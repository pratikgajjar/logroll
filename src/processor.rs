use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use postgres_replication::protocol::LogicalReplicationMessage;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::fs::File;
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, error, info};
use uuid::Uuid;
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
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            output_dir: "data".to_string(),
            max_batch_size: 10000,
            flush_interval_ms: 5000,
            max_file_size: 100 * 1024 * 1024, // 100MB default
            file_name_pattern: "logroll_changes_{timestamp}_{uuid}.parquet".to_string(),
        }
    }
}

/// Represents a CDC change record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    /// Timestamp when the change was processed
    #[serde(with = "chrono::serde::ts_microseconds")]
    pub timestamp: DateTime<Utc>,
    
    /// Log Sequence Number of the change
    pub lsn: String,
    
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
}

impl ChangeProcessor {
    /// Create a new change processor
    pub fn new(config: ProcessorConfig) -> Self {
        Self {
            config,
            state: Arc::new(ProcessorState {
                relation_map: Mutex::new(HashMap::new()),
                current_file_size: AtomicU64::new(0),
                current_file: Mutex::new(None),
                batch: Mutex::new(Vec::with_capacity(1000)),
            }),
        }
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
    
    /// Generate a new output filename
    fn generate_filename(&self) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let uuid = Uuid::new_v4();
        
        self.config.file_name_pattern
            .replace("{timestamp}", &timestamp.to_string())
            .replace("{uuid}", &uuid.to_string())
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
        
        let lsns: Vec<String> = records.iter()
            .map(|r| r.lsn.clone())
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
            Field::new("lsn", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("before", DataType::Utf8, true),
            Field::new("after", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
        ]);
    
        // Create arrays
        let timestamp_array = TimestampMicrosecondArray::from(timestamps);
        let lsn_array = StringArray::from(lsns);
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
    
    /// Write a batch of records to a Parquet file
    async fn write_batch_to_parquet(&self, records: &[ChangeRecord]) -> Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }
        
        // Create the output directory if it doesn't exist
        tokio::fs::create_dir_all(&self.config.output_dir).await?;
        
        // Generate a new filename if needed
        let filename = {
            let mut current_file = self.state.current_file.lock().await;
            if current_file.is_none() || self.state.current_file_size.load(Ordering::Relaxed) >= self.config.max_file_size {
                let new_filename = self.generate_filename();
                *current_file = Some(new_filename);
                self.state.current_file_size.store(0, Ordering::Relaxed);
            }
            current_file.clone().unwrap()
        };
        
        let output_path = Path::new(&self.config.output_dir).join(&filename);
        
        // Convert records to Arrow format
        let record_batch = self.records_to_arrow_batch(records)
            .context("Failed to convert records to Arrow format")?;
        
        // Set up writer properties
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();
        
        // Create a temporary file first
        let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = temp_file.path().to_path_buf();
        
        // Write to the temporary file
        {
            let file = File::create(&temp_path).await?;
            let file = file.into_std().await;
            
            let mut writer = ArrowWriter::try_new(file, record_batch.schema(), Some(props))?;
            writer.write(&record_batch)?;
            writer.close()?;
        }
        
        // Get file size
        let metadata = tokio::fs::metadata(&temp_path).await?;
        let file_size = metadata.len();
        
        // Move the temporary file to the final location
        tokio::fs::copy(&temp_path, &output_path).await?;
        
        // Update current file size
        self.state.current_file_size.fetch_add(file_size, Ordering::Relaxed);
        
        info!(
            "Wrote {} records to Parquet file {}, size: {} bytes", 
            records.len(), 
            output_path.display(),
            file_size
        );
        
        Ok(file_size)
    }
    
    /// Start a processor task that consumes from a channel
    pub async fn start_processing(self, mut rx: mpsc::Receiver<ChangeRecord>) -> Result<()> {
        // Create flush interval timer
        let flush_interval = Duration::from_millis(self.config.flush_interval_ms);
        let interval_stream = IntervalStream::new(interval(flush_interval));
        tokio::pin!(interval_stream);
        
        info!(
            "Starting change processor with max_batch_size={}, flush_interval={}ms", 
            self.config.max_batch_size,
            self.config.flush_interval_ms
        );
        
        loop {
            tokio::select! {
                // Process incoming records
                Some(record) = rx.recv() => {
                    self.add_record(record).await?;
                }
                
                // Flush on interval
                _ = interval_stream.next() => {
                    let batch_len = {
                        let batch = self.state.batch.lock().await;
                        batch.len()
                    };
                    
                    if batch_len > 0 {
                        debug!("Flushing {} records due to interval timer", batch_len);
                        self.flush_batch().await?;
                    }
                }
                
                // Exit if sender is closed
                else => {
                    info!("Channel closed, flushing remaining records");
                    self.flush_batch().await?;
                    break;
                }
            }
        }
        
        info!("Change processor stopped");
        Ok(())
    }
}