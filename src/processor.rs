use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use postgres_replication::protocol::LogicalReplicationMessage;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
// Removed unused File import
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, info, warn, error};
use uuid::Uuid;
use std::num::ParseIntError;
use std::sync::Arc;

/// Configuration for the data processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorConfig {
    /// Output directory for Parquet files
    pub output_dir: String,
    
    /// Maximum number of rows in a batch before writing
    pub max_batch_size: usize,
    
    /// Flush interval in milliseconds (even if batch not full)
    pub flush_interval_ms: u64,
    
    /// Maximum file size in bytes before rolling to a new file
    pub max_file_size: u64,
    
    /// Pattern for output filenames
    pub file_name_pattern: String,
    
    /// S3 configuration for MinIO storage
    pub s3_config: Option<S3Config>,
}

/// Configuration for S3/MinIO integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// MinIO/S3 endpoint URL
    pub endpoint: String,
    
    /// S3 bucket name
    pub bucket: String,
    
    /// Access key for authentication
    pub access_key: String,
    
    /// Secret key for authentication
    pub secret_key: String,
    
    /// Region (optional, defaults to "us-east-1")
    pub region: Option<String>,
    
    /// Path prefix for S3 objects
    pub prefix: Option<String>,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            output_dir: "data".to_string(),
            max_batch_size: 1000, // Updated to 1000 rows as requested
            flush_interval_ms: 5000, // 5s timer
            max_file_size: 100 * 1024 * 1024, // 100MB default
            file_name_pattern: "logroll_changes_{timestamp}_{uuid}.parquet".to_string(),
            s3_config: None,
        }
    }
}

/// Represents a CDC change record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    /// Timestamp when the change was processed
    #[serde(with = "chrono::serde::ts_microseconds")]
    pub timestamp: DateTime<Utc>,
    
    /// Log Sequence Number of the change as a u64
    /// This is the native representation in PostgreSQL
    pub lsn: u64,
    
    /// Table name
    pub table: String,
    
    /// Operation type (INSERT, UPDATE, DELETE, TRUNCATE)
    pub op: String,
    
    /// Before state for UPDATE and DELETE (null for INSERT)
    pub before: Option<JsonValue>,
    
    /// After state for INSERT and UPDATE (null for DELETE)
    pub after: Option<JsonValue>,
    
    /// Additional metadata
    pub metadata: Option<JsonValue>,
}

/// Manages batched writing of change records to Parquet files
#[derive(Clone)]
pub struct ChangeProcessor {
    /// Configuration
    config: ProcessorConfig,
    
    /// Shared processor state
    state: Arc<ProcessorState>,
}

/// Shared state for the change processor
struct ProcessorState {
    /// Table of relation IDs to table names and column names (for lookups)
    relation_map: Mutex<HashMap<u32, (String, Vec<String>)>>,
    
    /// Current file size estimate
    current_file_size: AtomicU64,
    
    /// Current filename
    current_file: Mutex<Option<String>>,
    
    /// Current batch of records
    batch: Mutex<Vec<ChangeRecord>>,
    
    /// S3 client for MinIO operations
    s3_client: Mutex<Option<S3Client>>,
}



impl ChangeProcessor {
    /// Create a new change processor
    pub fn new(config: ProcessorConfig) -> Self {
        let processor = Self {
            config: config.clone(),
            state: Arc::new(ProcessorState {
                relation_map: Mutex::new(HashMap::new()),
                current_file_size: AtomicU64::new(0),
                current_file: Mutex::new(None),
                batch: Mutex::new(Vec::with_capacity(1000)),
                s3_client: Mutex::new(None),
            }),
        };
        
        // Initialize S3 client if S3 config is provided
        if let Some(s3_config) = &config.s3_config {
            let processor_clone = processor.clone();
            let s3_config_clone = s3_config.clone();
            tokio::spawn(async move {
                if let Err(e) = processor_clone.initialize_s3_client(s3_config_clone).await {
                    tracing::error!("Failed to initialize S3 client: {}", e);
                }
            });
        }
        
        processor
    }
    
    /// Register a relation (table) with its ID and extract table name and column names
    pub async fn register_relation(&self, id: u32, relation: LogicalReplicationMessage) {
        let mut map = self.state.relation_map.lock().await;
        // Store the table name and column names
        if let LogicalReplicationMessage::Relation(rel) = relation {
            let table_name = format!(
                "{}.{}",
                rel.namespace().unwrap_or("public"),
                rel.name().unwrap_or("unknown_table")
            );
            let column_names = rel.columns().iter()
                .map(|col| col.name().unwrap_or("unknown").to_string())
                .collect();
            map.insert(id, (table_name, column_names));
        }
    }
    
    /// Get relation column names by ID
    pub async fn get_relation_columns(&self, id: u32) -> Option<Vec<String>> {
        let map = self.state.relation_map.lock().await;
        map.get(&id).map(|(_, columns)| columns.clone())
    }
    
    /// Get table name from relation ID
    pub async fn get_table_name(&self, id: u32) -> Option<String> {
        let map = self.state.relation_map.lock().await;
        map.get(&id).map(|(table_name, _)| table_name.clone())
    }
    
    /// Generate a new output filename using date-based hierarchical format with LSN
    /// Uses timestamp from the records rather than current time
    fn generate_filename(&self, records: &[ChangeRecord]) -> String {
        if records.is_empty() {
            // Fallback if no records - this should rarely happen
            let now = chrono::Utc::now();
            return format!("{}/{}/{}/initial.parquet", 
                now.format("%Y"), 
                now.format("%m"), 
                now.format("%d")
            );
        }
        
        // Get the last record in the batch for timestamp and LSN
        let last_record = records.last().unwrap(); // Safe because we checked is_empty
        
        // Extract date components from the record's timestamp
        // This ensures partitioning based on when the change actually occurred in the database
        let year = last_record.timestamp.format("%Y");
        let month = last_record.timestamp.format("%m");
        let day = last_record.timestamp.format("%d");
        
        // Use the LSN directly from the record (already a u64)
        let lsn = last_record.lsn;
        
        // Format: YYYY/MM/DD/{LSN}.parquet
        format!("{}/{}/{}/{}.parquet", year, month, day, lsn)
    }
    
    /// Initialize the S3 client for MinIO
    async fn initialize_s3_client(&self, s3_config: S3Config) -> Result<()> {
        info!("Initializing S3 client for MinIO at {}", s3_config.endpoint);
        
        // Create credentials provider for the static credentials
        let credentials_provider = SharedCredentialsProvider::new(
            aws_credential_types::Credentials::new(
                s3_config.access_key.clone(),
                s3_config.secret_key.clone(),
                None, // session token
                None, // expiry
                "logroll-minio-provider",
            )
        );
        
        // Determine region
        let region_str = s3_config.region.unwrap_or_else(|| "us-east-1".to_string());
        let region_provider = RegionProviderChain::first_try(aws_sdk_s3::config::Region::new(region_str));
        
        // Configure S3 client
        let endpoint_url = s3_config.endpoint.clone();
        let s3_config_aws = aws_sdk_s3::config::Builder::new()
            .region(region_provider.region().await)
            .credentials_provider(credentials_provider)
            .endpoint_url(endpoint_url)
            .force_path_style(true) // Required for MinIO
            .behavior_version_latest() // Required for AWS SDK 1.87.0+
            .build();
        
        // Create S3 client
        let client = S3Client::from_conf(s3_config_aws.clone());
        
        // Store client in shared state
        let mut s3_client_guard = self.state.s3_client.lock().await;
        *s3_client_guard = Some(client.clone());
        
        // Check if bucket exists, create it if it doesn't
        let bucket_name = s3_config.bucket.clone();
        match client.head_bucket().bucket(&bucket_name).send().await {
            Ok(_) => {
                info!("Bucket '{}' exists", bucket_name);
            },
            Err(_) => {
                info!("Bucket '{}' does not exist, creating it", bucket_name);
                // Create the bucket
                match client.create_bucket().bucket(&bucket_name).send().await {
                    Ok(_) => info!("Successfully created bucket: {}", bucket_name),
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to create S3 bucket: {}", e));
                    }
                }
            }
        }
        
        info!("S3 client for MinIO initialized successfully");
        
        Ok(())
    }
    
    /// Direct streaming upload of data to S3/MinIO
    async fn upload_to_s3_direct(&self, data: Vec<u8>, filename: &str) -> Result<Option<String>> {
        // Check if S3 is configured
        let s3_config = match &self.config.s3_config {
            Some(config) => config,
            None => return Ok(None),
        };
        
        // Get S3 client
        let s3_client_guard = self.state.s3_client.lock().await;
        let client = match &*s3_client_guard {
            Some(client) => client,
            None => {
                info!("S3 client not initialized, skipping upload");
                return Ok(None);
            }
        };
        
        // Create S3 key with optional prefix
        let key = if let Some(prefix) = &s3_config.prefix {
            format!("{}/{}", prefix.trim_end_matches('/'), filename)
        } else {
            filename.to_string()
        };
        
        debug!("Directly uploading {} bytes to S3 bucket {} with key {}", data.len(), s3_config.bucket, key);
        
        // Create byte stream directly from the in-memory data
        let body = ByteStream::new(data.into());
        
        // Upload to S3
        info!("Uploading to bucket: '{}' with key: '{}'", s3_config.bucket, key);
        let put_object_request = client.put_object()
            .bucket(&s3_config.bucket)
            .key(&key)
            .body(body)
            .content_type("application/octet-stream");
            
        match put_object_request.send().await {
            Ok(_) => {
                info!("Successfully uploaded data to S3: s3://{}/{}", s3_config.bucket, key);
            },
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to upload data to S3: {}", e));
            }
        };
        
        // Verify the upload with a head request
        match client.head_object()
            .bucket(&s3_config.bucket)
            .key(&key)
            .send()
            .await {
            Ok(_) => info!("Verified object exists in S3"),
            Err(e) => warn!("Object verification failed: {}", e)
        }
        
        Ok(Some(format!("s3://{}/{}", s3_config.bucket, key)))
    }
    
    /// Add a record to the batch
    pub async fn add_record(&self, record: ChangeRecord) -> Result<bool> {
        let mut batch = self.state.batch.lock().await;
        batch.push(record);
        
        // Return true if batch is full
        let is_full = batch.len() >= self.config.max_batch_size;
        if is_full {
            self.flush_batch().await?;
        }
        
        Ok(is_full)
    }
    
    /// Flush the current batch to disk
    pub async fn flush_batch(&self) -> Result<()> {
        let mut batch = self.state.batch.lock().await;
        if batch.is_empty() {
            return Ok(());
        }
        
        let records_to_write = std::mem::replace(&mut *batch, Vec::with_capacity(self.config.max_batch_size));
        drop(batch); // Release the lock
        
        self.write_batch_to_parquet(&records_to_write).await?;
        Ok(())
    }
    
    /// Convert a batch of records to Arrow RecordBatch
    fn records_to_arrow_batch(&self, records: &[ChangeRecord]) -> Result<RecordBatch> {
        if records.is_empty() {
            return Err(anyhow::anyhow!("Empty batch"));
        }
        
        // Prepare arrays for each column
        let timestamps: Vec<i64> = records.iter()
            .map(|r| r.timestamp.timestamp_micros())
            .collect();
        
        let lsns: Vec<u64> = records.iter()
            .map(|r| r.lsn)
            .collect();
        
        let tables: Vec<String> = records.iter()
            .map(|r| r.table.clone())
            .collect();
        
        let ops: Vec<String> = records.iter()
            .map(|r| r.op.clone())
            .collect();
        
        // Create 'before' and 'after' JSON columns
        let before_jsons: Vec<String> = records.iter()
            .map(|r| match &r.before {
                Some(data) => serde_json::to_string(data).unwrap_or_else(|_| "null".to_string()),
                None => "null".to_string()
            })
            .collect();
        
        let after_jsons: Vec<String> = records.iter()
            .map(|r| match &r.after {
                Some(data) => serde_json::to_string(data).unwrap_or_else(|_| "null".to_string()),
                None => "null".to_string()
            })
            .collect();
        
        // Create metadata JSON column
        let metadata_jsons: Vec<String> = records.iter()
            .map(|r| match &r.metadata {
                Some(data) => serde_json::to_string(data).unwrap_or_else(|_| "null".to_string()),
                None => "null".to_string()
            })
            .collect();
        
        // Create Arrow schema
        let schema = ArrowSchema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("lsn", DataType::UInt64, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("before", DataType::Utf8, true),
            Field::new("after", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
        ]);
    
        // Create arrays
        let timestamp_array = TimestampMicrosecondArray::from(timestamps);
        let lsn_array = UInt64Array::from(lsns);
        let table_array = StringArray::from(tables);
        let op_array = StringArray::from(ops);
        let before_array = StringArray::from(before_jsons);
        let after_array = StringArray::from(after_jsons);
        let metadata_array = StringArray::from(metadata_jsons);
        
        // Create record batch
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(timestamp_array),
                std::sync::Arc::new(lsn_array),
                std::sync::Arc::new(table_array),
                std::sync::Arc::new(op_array),
                std::sync::Arc::new(before_array),
                std::sync::Arc::new(after_array),
                std::sync::Arc::new(metadata_array),
            ],
        )?;
        
        Ok(batch)
    }
    
    /// Write a batch of records to Parquet format and directly upload to S3
    async fn write_batch_to_parquet(&self, records: &[ChangeRecord]) -> Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }
        
        // Generate filename with the hierarchical date structure and LSN from last event
        let filename = self.generate_filename(records);
        
        // Convert records to Arrow format
        let record_batch = self.records_to_arrow_batch(records)
            .context("Failed to convert records to Arrow format")?;
        
        // Set up writer properties with zstd level 3 compression
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap_or_default()))
            .build();
        
        // Create an in-memory buffer to hold the Parquet data
        let cursor = std::io::Cursor::new(Vec::new());
        
        // Create the Arrow Parquet writer with ZSTD compression
        let mut writer = match ArrowWriter::try_new(cursor, record_batch.schema(), Some(props)) {
            Ok(writer) => writer,
            Err(e) => return Err(anyhow::anyhow!("Failed to create Arrow writer: {}", e)),
        };
        
        // Write the batch to the in-memory buffer
        if let Err(e) = writer.write(&record_batch) {
            return Err(anyhow::anyhow!("Failed to write records to Parquet format: {}", e));
        }
        
        // Important: into_inner() must be called before close() since close() takes ownership
        // Get the inner cursor from the writer
        let cursor = match writer.into_inner() {
            Ok(c) => c,
            Err(e) => return Err(anyhow::anyhow!("Failed to extract Parquet data: {}", e)),
        };
            
        // Get the Vec<u8> from the cursor
        let parquet_data = cursor.into_inner();
        
        // Measure the data size
        let file_size = parquet_data.len() as u64;
        
        // Update current file size metrics
        self.state.current_file_size.fetch_add(file_size, Ordering::Relaxed);
        
        info!("Serialized {} records to Parquet format, size: {} bytes", records.len(), file_size);
        
        // If S3 is configured, upload directly from memory
        if let Some(s3_config) = &self.config.s3_config {
            // S3 is configured - upload directly and propagate any errors
            match self.upload_to_s3_direct(parquet_data, &filename).await {
                Ok(Some(s3_uri)) => {
                    info!("Directly uploaded Parquet data to {}", s3_uri);
                },
                Ok(None) => {
                    // This shouldn't happen if S3 is properly configured
                    return Err(anyhow::anyhow!("S3 upload returned None when S3 was configured"));
                },
                Err(e) => {
                    // Propagate the error upward - never try to write locally as a fallback
                    return Err(anyhow::anyhow!("S3 upload failed to bucket '{}' with key '{}': {}", 
                                               s3_config.bucket, filename, e));
                }
            }
        } else {
            // Only allow local storage if S3 is explicitly not configured
            let absolute_output_dir = if Path::new(&self.config.output_dir).is_absolute() {
                self.config.output_dir.clone()
            } else {
                // Get the current directory and join it with the relative path
                std::env::current_dir()
                    .context("Failed to get current directory")?
                    .join(&self.config.output_dir)
                    .to_string_lossy()
                    .to_string()
            };
            
            // Create the output directory if it doesn't exist
            tokio::fs::create_dir_all(&absolute_output_dir).await
                .context(format!("Failed to create output directory: {}", absolute_output_dir))?;
            
            // Create an absolute path for the output file
            let output_path = Path::new(&absolute_output_dir).join(&filename);
            
            // Write the Parquet data to the local file - propagate any errors
            tokio::fs::write(&output_path, &parquet_data).await
                .context(format!("Failed to write local file: {}", output_path.display()))?;
                
            info!("Wrote {} records to local Parquet file: {}", records.len(), output_path.display());
        }
        
        Ok(file_size)
    }
    
    /// Start a processor task that consumes from a channel
    pub async fn start_processing(self, mut rx: mpsc::Receiver<ChangeRecord>) -> Result<()> {
        let processor = self.clone();
        
        // Start the background task that processes records and flushes periodically
        tokio::spawn(async move {
            info!("Starting change processor with max_batch_size={}, flush_interval={}ms", 
                 processor.config.max_batch_size, processor.config.flush_interval_ms);
                 
            // Set up flush timer
            let interval_ms = processor.config.flush_interval_ms;
            let mut timer = IntervalStream::new(interval(Duration::from_millis(interval_ms)));
            
            // Create metrics for batch sizes and processing latency
            let mut last_flush_time = std::time::Instant::now();
            let mut total_records_processed: u64 = 0;
            
            // Process records
            loop {
                tokio::select! {
                    // Timer triggered, flush whatever we have
                    Some(_) = timer.next() => {
                        let batch_size = {
                            let batch = processor.state.batch.lock().await;
                            batch.len()
                        };
                        
                        if batch_size > 0 {
                            let elapsed = last_flush_time.elapsed();
                            info!("Flushing batch of {} records due to timer ({}ms since last flush)", 
                                batch_size, elapsed.as_millis());
                                
                            match processor.flush_batch().await {
                                Ok(_) => {
                                    total_records_processed += batch_size as u64;
                                    info!("Total records processed: {}", total_records_processed);
                                },
                                Err(e) => {
                                    // Use structured error logging
                                    error!(error=?e, batch_size=batch_size, "Failed to flush batch");
                                    
                                    // Try with backoff, but abort processing if all retries fail
                                    if let Err(retry_err) = processor.retry_flush_with_backoff().await {
                                        error!(error=?retry_err, "Failed all retry attempts, stopping processor");
                                        break;
                                    }
                                }
                            }
                            last_flush_time = std::time::Instant::now();
                        }
                    }
                    
                    // Received a record from the channel
                    Some(record) = rx.recv() => {
                        match processor.add_record(record).await {
                            Ok(flushed) => {
                                if flushed {
                                    // Batch was flushed because it reached max_batch_size
                                    total_records_processed += processor.config.max_batch_size as u64;
                                    info!("Flushed batch due to size limit. Total records processed: {}", 
                                          total_records_processed);
                                    last_flush_time = std::time::Instant::now();
                                }
                            },
                            Err(e) => {
                                error!(error=?e, "Error adding record to batch");
                            }
                        }
                    }
                    
                    // Channel closed, we're done
                    else => {
                        info!("Channel closed, flushing final batch");
                        if let Err(e) = processor.flush_batch().await {
                            error!(error=?e, "Error flushing final batch");
                        }
                        info!("Completed processing with {} total records", total_records_processed);
                        break;
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Retry flushing the batch with exponential backoff
    /// If all retries fail, returns the error to the caller
    async fn retry_flush_with_backoff(&self) -> Result<()> {
        use backoff::{ExponentialBackoff, Error as BackoffError};
        
        // Create a more aggressive backoff strategy with fewer retries
        // We don't want to keep retrying indefinitely for S3 failures
        let mut backoff = ExponentialBackoff {
            max_elapsed_time: Some(std::time::Duration::from_secs(30)),  // Only retry for 30 seconds max
            max_interval: std::time::Duration::from_secs(5),           // Cap max delay at 5 seconds
            multiplier: 2.0,                                           // Double delay each time
            ..ExponentialBackoff::default()
        };
        
        let operation = || async {
            match self.flush_batch().await {
                Ok(_) => Ok(()),
                Err(e) => {
                    // If this is an S3 error, we should fail fast
                    if e.to_string().contains("S3 upload failed") {
                        error!(error=?e, "S3 upload error detected, failing immediately");
                        return Err(BackoffError::permanent(e));
                    }
                    
                    // For other errors, allow limited retries
                    error!(error=?e, "Retry attempt failed");
                    Err(BackoffError::transient(e))
                }
            }
        };
        
        let result = backoff::future::retry(backoff, operation).await;
        
        match &result {
            Ok(_) => info!("Successfully flushed batch after retries"),
            Err(e) => error!(error=?e, "Failed to flush batch after multiple retries")
        }
        
        result
    }
}