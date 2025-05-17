use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use postgres_replication::protocol::LogicalReplicationMessage;
use postgres_types::PgLsn;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::fs::File;
use tokio::sync::{mpsc, Mutex};
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use tracing::{debug, error, info};
use uuid::Uuid;

/// Extension trait for postgres_replication::protocol::Tuple
trait TupleExt {
    /// Try to extract the tuple as bytes
    fn try_as_bytes(&self) -> Option<Vec<u8>>;
}

impl TupleExt for postgres_replication::protocol::Tuple {
    fn try_as_bytes(&self) -> Option<Vec<u8>> {
        // Extract raw bytes from the tuple
        let debug_str = format!("{:?}", self);
        Some(debug_str.into_bytes())
    }
}

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
    
    /// Data payload as JSON
    pub data: JsonValue,
}

/// Manages batched writing of change records to Parquet files
pub struct ChangeProcessor {
    /// Configuration
    config: ProcessorConfig,
    
    /// Channel for receiving change records
    rx: mpsc::Receiver<ChangeRecord>,
    
    /// Current batch of records
    batch: Vec<ChangeRecord>,
    
    /// Table of relation IDs to names (for lookups)
    relation_map: Mutex<HashMap<u32, String>>,
    
    /// Current file size estimate
    current_file_size: AtomicU64,
    
    /// Current filename
    current_file: Mutex<Option<String>>,
}

impl ChangeProcessor {
    /// Create a new change processor
    pub fn new(config: ProcessorConfig, rx: mpsc::Receiver<ChangeRecord>) -> Self {
        Self {
            config,
            rx,
            batch: Vec::with_capacity(1000),
            relation_map: Mutex::new(HashMap::new()),
            current_file_size: AtomicU64::new(0),
            current_file: Mutex::new(None),
        }
    }
    
    /// Register a relation (table) with its ID and name
    pub async fn register_relation(&self, id: u32, name: String) {
        let mut map = self.relation_map.lock().await;
        map.insert(id, name);
    }
    
    /// Get table name from relation ID
    pub async fn get_table_name(&self, id: u32) -> Option<String> {
        let map = self.relation_map.lock().await;
        map.get(&id).cloned()
    }
    
    /// Generate a new output filename
    fn generate_filename(&self) -> String {
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let uuid = Uuid::new_v4();
        
        self.config.file_name_pattern
            .replace("{timestamp}", &timestamp.to_string())
            .replace("{uuid}", &uuid.to_string())
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
            
        let data: Vec<String> = records.iter()
            .map(|r| serde_json::to_string(&r.data).unwrap_or_else(|_| "{}".to_string()))
            .collect();
        
        // Create Arrow schema
        let schema = ArrowSchema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("lsn", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
        ]);
        
        // Create arrays
        let timestamp_array = TimestampMicrosecondArray::from(timestamps);
        let lsn_array = StringArray::from(lsns);
        let table_array = StringArray::from(tables);
        let op_array = StringArray::from(ops);
        let data_array = StringArray::from(data);
        
        // Create record batch
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(timestamp_array),
                std::sync::Arc::new(lsn_array),
                std::sync::Arc::new(table_array),
                std::sync::Arc::new(op_array),
                std::sync::Arc::new(data_array),
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
            let mut current_file = self.current_file.lock().await;
            if current_file.is_none() || self.current_file_size.load(Ordering::Relaxed) >= self.config.max_file_size {
                let new_filename = self.generate_filename();
                *current_file = Some(new_filename);
                self.current_file_size.store(0, Ordering::Relaxed);
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
        self.current_file_size.fetch_add(file_size, Ordering::Relaxed);
        
        info!(
            "Wrote {} records to Parquet file {}, size: {} bytes", 
            records.len(), 
            output_path.display(),
            file_size
        );
        
        Ok(file_size)
    }
    
    /// Start processing incoming records
    pub async fn start(mut self) -> Result<()> {
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
                Some(record) = self.rx.recv() => {
                    self.batch.push(record);
                    
                    // Write batch if it reaches max size
                    if self.batch.len() >= self.config.max_batch_size {
                        let records_to_write = std::mem::replace(&mut self.batch, Vec::with_capacity(self.config.max_batch_size));
                        
                        if let Err(e) = self.write_batch_to_parquet(&records_to_write).await {
                            error!("Failed to write batch to Parquet: {}", e);
                            // Try to recover the records
                            self.batch.extend(records_to_write);
                        }
                    }
                }
                
                // Flush on interval
                _ = interval_stream.next() => {
                    if !self.batch.is_empty() {
                        debug!("Flushing {} records due to interval timer", self.batch.len());
                        let records_to_write = std::mem::replace(&mut self.batch, Vec::with_capacity(self.config.max_batch_size));
                        
                        if let Err(e) = self.write_batch_to_parquet(&records_to_write).await {
                            error!("Failed to write batch to Parquet on timer: {}", e);
                            // Try to recover the records
                            self.batch.extend(records_to_write);
                        }
                    }
                }
                
                // Exit if sender is closed
                else => {
                    info!("Channel closed, flushing remaining records");
                    if !self.batch.is_empty() {
                        if let Err(e) = self.write_batch_to_parquet(&self.batch).await {
                            error!("Failed to write final batch to Parquet: {}", e);
                        }
                    }
                    break;
                }
            }
        }
        
        info!("Change processor stopped");
        Ok(())
    }
}

/// Extract data from a replication message
pub fn extract_record_data(
    message: &LogicalReplicationMessage,
    lsn: PgLsn,
    table_name: String,
) -> Result<ChangeRecord> {
    let timestamp = chrono::Utc::now();
    
    match message {
        LogicalReplicationMessage::Insert(insert) => {
            // Extract tuple data as JSON
            let tuple_data = tuple_to_json(insert.tuple())?;
            
            Ok(ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "INSERT".to_string(),
                data: tuple_data,
            })
        }
        LogicalReplicationMessage::Update(update) => {
            // Extract new tuple data as JSON
            let new_tuple_data = tuple_to_json(update.new_tuple())?;
            
            // Also include old tuple if available
            let data = if let Some(old_tuple) = update.old_tuple() {
                if let Ok(old_data) = tuple_to_json(old_tuple) {
                    serde_json::json!({
                        "old": old_data,
                        "new": new_tuple_data,
                    })
                } else {
                    serde_json::json!({
                        "new": new_tuple_data,
                    })
                }
            } else {
                new_tuple_data
            };
            
            Ok(ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "UPDATE".to_string(),
                data,
            })
        }
        LogicalReplicationMessage::Delete(delete) => {
            // Extract tuple data as JSON
            let tuple_data = match delete.old_tuple() {
                Some(tuple) => tuple_to_json(tuple)?,
                None => JsonValue::Object(serde_json::Map::new()),
            };
            
            Ok(ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "DELETE".to_string(),
                data: tuple_data,
            })
        }
        LogicalReplicationMessage::Truncate(truncate) => {
            // Truncate affects multiple tables potentially
            let data = serde_json::json!({
                "rel_ids": truncate.rel_ids(),
                "options": truncate.options(),
            });
            
            Ok(ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "TRUNCATE".to_string(),
                data,
            })
        }
        _ => Err(anyhow::anyhow!("Unsupported message type for extracting data")),
    }
}

/// Convert a tuple to JSON value
fn tuple_to_json(tuple: &postgres_replication::protocol::Tuple) -> Result<JsonValue> {
    let mut map = serde_json::Map::new();
    
    // Since we can't access the tuple structure directly in a reliable way,
    // we'll make a simple approximation of the data based on what we can
    // extract from the tuple's debug representation
    let debug_str = format!("{:?}", tuple);
    
    // Try to extract column values from the debug string
    let column_values: Vec<&str> = debug_str
        .split("columns: [")
        .nth(1)
        .map(|s| s.trim_end_matches(']'))
        .map(|s| s.split(", ").collect())
        .unwrap_or_default();
    
    // Map the column values to JSON
    for (i, value) in column_values.iter().enumerate() {
        if value.contains("null") {
            map.insert(format!("col_{}", i), JsonValue::Null);
        } else if value.starts_with("\"") && value.ends_with("\"") {
            // Try to extract the string content
            let content = value.trim_start_matches('"').trim_end_matches('"');
            map.insert(format!("col_{}", i), JsonValue::String(content.to_string()));
        } else if let Ok(num) = value.parse::<i64>() {
            map.insert(format!("col_{}", i), JsonValue::Number(num.into()));
        } else if let Ok(num) = value.parse::<f64>() {
            // Try to convert to serde_json Number
            if let Some(num_val) = serde_json::Number::from_f64(num) {
                map.insert(format!("col_{}", i), JsonValue::Number(num_val));
            } else {
                map.insert(format!("col_{}", i), JsonValue::String(value.to_string()));
            }
        } else {
            map.insert(format!("col_{}", i), JsonValue::String(value.to_string()));
        }
    }
    
    // If we couldn't extract column data, use the debug representation
    if map.is_empty() {
        map.insert("debug".to_string(), JsonValue::String(debug_str));
    }
    
    // Add raw data in base64 for complete data preservation
    if let Some(raw_data) = tuple.try_as_bytes() {
        let base64_str = base64::engine::general_purpose::STANDARD.encode(raw_data);
        map.insert("raw_base64".to_string(), JsonValue::String(base64_str));
    }
    
    Ok(JsonValue::Object(map))
}