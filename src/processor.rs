use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray, StringBuilder, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
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
    /// Get the column names based on column count or from debug output
    fn extract_column_names(&self, relation_columns: Option<Vec<String>>) -> Vec<String>;
    
    /// Extract values from tuple as strings
    fn extract_values(&self) -> Vec<Option<String>>;
    
    /// Count the columns in a tuple
    fn count_columns(&self) -> usize;
    
    /// Extract column values at an index
    fn extract_column_at(&self, index: usize) -> Option<String>;
}

impl TupleExt for postgres_replication::protocol::Tuple {
    fn extract_column_names(&self, relation_columns: Option<Vec<String>>) -> Vec<String> {
        if let Some(columns) = relation_columns {
            columns
        } else {
            // Fallback to generic column names
            let count = self.count_columns();
            (0..count).map(|i| format!("col_{}", i)).collect()
        }
    }
    
    fn extract_values(&self) -> Vec<Option<String>> {
        let mut values = Vec::new();
        let column_count = self.count_columns();
        
        for i in 0..column_count {
            if let Some(value) = self.extract_column_at(i) {
                values.push(Some(value));
            } else {
                values.push(None);
            }
        }
        
        values
    }
    
    fn count_columns(&self) -> usize {
        // Parse from debug representation
        let debug_str = format!("{:?}", self);
        if debug_str.contains("Tuple([") {
            // Count the number of Text or Binary entries
            let count = debug_str.matches("Text(").count() + debug_str.matches("Binary(").count() + debug_str.matches("Null").count();
            count
        } else {
            0
        }
    }
    
    fn extract_column_at(&self, index: usize) -> Option<String> {
        // Parse from debug representation
        let debug_str = format!("{:?}", self);
        
        // Very basic parsing from debug output
        if let Some(content) = debug_str.strip_prefix("Tuple([").and_then(|s| s.strip_suffix("])")) {
            let columns: Vec<&str> = content.split(", ").collect();
            if index < columns.len() {
                let col_text = columns[index];
                if col_text.starts_with("Text(") {
                    // Extract data between b"..." from Text(b"...")
                    if let Some(content) = col_text.strip_prefix("Text(b\"").and_then(|s| s.strip_suffix("\")")) {
                        return Some(content.to_string());
                    }
                } else if col_text.starts_with("Binary(") {
                    // Extract data between b"..." from Binary(b"...")
                    if let Some(content) = col_text.strip_prefix("Binary(b\"").and_then(|s| s.strip_suffix("\")")) {
                        return Some(content.to_string());
                    }
                } else if col_text == "Null" {
                    return None;
                }
            }
        }
        
        None
    }
}

// Removed TupleColumn as it's no longer needed

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
    
    /// Convert a batch of records to Arrow RecordBatch with MapType for data
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
        
        // Create map arrays for data
        let data_maps = self.create_map_arrays_from_records(records)?;
        
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
        
        // Create record batch
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(timestamp_array),
                std::sync::Arc::new(lsn_array),
                std::sync::Arc::new(table_array),
                std::sync::Arc::new(op_array),
                data_maps,
            ],
        )?;
        
        Ok(batch)
    }
    
    /// Create map arrays from record data
    fn create_map_arrays_from_records(&self, records: &[ChangeRecord]) -> Result<ArrayRef> {
        // Create key and value builders
        let mut keys_builder = StringBuilder::new();
        let mut values_builder = StringBuilder::new();
        let mut offsets = Vec::new();
        
        let mut current_offset = 0;
        offsets.push(current_offset);
        
        for record in records {
            if let JsonValue::Object(map) = &record.data {
                for (key, value) in map {
                    keys_builder.append_value(key);
                    
                    // Handle different value types
                    match value {
                        JsonValue::Null => values_builder.append_null(),
                        JsonValue::Bool(b) => values_builder.append_value(b.to_string()),
                        JsonValue::Number(n) => values_builder.append_value(n.to_string()),
                        JsonValue::String(s) => values_builder.append_value(s),
                        _ => values_builder.append_value(value.to_string()),
                    }
                    
                    current_offset += 1;
                }
            }
            
            offsets.push(current_offset);
        }
        
        // For simplicity, serialize the data to JSON string instead of using MapArray
        // This is a workaround until we can properly set up the complex MapArray type
        let data_json = records.iter()
            .map(|r| serde_json::to_string(&r.data).unwrap_or_else(|_| "{}".to_string()))
            .collect::<Vec<String>>();
        
        let data_array = StringArray::from(data_json);
        
        Ok(std::sync::Arc::new(data_array))
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
    relation_map: &HashMap<u32, LogicalReplicationMessage>,
) -> Result<ChangeRecord> {
    let timestamp = chrono::Utc::now();
    
    match message {
        LogicalReplicationMessage::Insert(insert) => {
            // Get relation for column names
            let column_names = extract_column_names_from_relation_map(relation_map, insert.rel_id());
            
            // Extract tuple data as JSON with column names
            let tuple_data = tuple_to_json(insert.tuple(), column_names)?;
            
            Ok(ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "INSERT".to_string(),
                data: tuple_data,
            })
        }
        LogicalReplicationMessage::Update(update) => {
            // Get relation for column names
            let column_names = extract_column_names_from_relation_map(relation_map, update.rel_id());
            
            // Extract new tuple data as JSON with column names
            let new_tuple_data = tuple_to_json(update.new_tuple(), column_names.clone())?;
            
            // Also include old tuple if available
            let data = if let Some(old_tuple) = update.old_tuple() {
                if let Ok(old_data) = tuple_to_json(old_tuple, column_names) {
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
            // Get relation for column names
            let column_names = extract_column_names_from_relation_map(relation_map, delete.rel_id());
            
            // Extract tuple data as JSON with column names
            let tuple_data = match delete.old_tuple() {
                Some(tuple) => tuple_to_json(tuple, column_names)?,
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

/// Helper function to extract column names from the relation map
fn extract_column_names_from_relation_map(relation_map: &HashMap<u32, LogicalReplicationMessage>, rel_id: u32) -> Option<Vec<String>> {
    if let Some(LogicalReplicationMessage::Relation(relation)) = relation_map.get(&rel_id) {
        let column_names = relation.columns().iter()
            .map(|col| col.name().unwrap_or("unknown").to_string())
            .collect();
        Some(column_names)
    } else {
        None
    }
}

/// Convert a tuple to JSON value with column names
fn tuple_to_json(
    tuple: &postgres_replication::protocol::Tuple, 
    relation_columns: Option<Vec<String>>
) -> Result<JsonValue> {
    let mut map = serde_json::Map::new();
    
    // Get column names from relation if possible
    let column_names = tuple.extract_column_names(relation_columns);
    
    // Get column values
    let column_values = tuple.extract_values();
    
    // Map column names to values
    for (name, value) in column_names.iter().zip(column_values.iter()) {
        match value {
            Some(val_str) => {
                // Try to parse as number or boolean first
                if val_str == "t" || val_str == "true" {
                    map.insert(name.clone(), JsonValue::Bool(true));
                } else if val_str == "f" || val_str == "false" {
                    map.insert(name.clone(), JsonValue::Bool(false));
                } else if let Ok(num) = val_str.parse::<i64>() {
                    map.insert(name.clone(), JsonValue::Number(num.into()));
                } else if let Ok(num) = val_str.parse::<f64>() {
                    if let Some(num_val) = serde_json::Number::from_f64(num) {
                        map.insert(name.clone(), JsonValue::Number(num_val));
                    } else {
                        map.insert(name.clone(), JsonValue::String(val_str.clone()));
                    }
                } else {
                    // Try to parse as JSON
                    if (val_str.starts_with('{') && val_str.ends_with('}')) || 
                       (val_str.starts_with('[') && val_str.ends_with(']')) {
                        if let Ok(json_val) = serde_json::from_str(val_str) {
                            map.insert(name.clone(), json_val);
                        } else {
                            map.insert(name.clone(), JsonValue::String(val_str.clone()));
                        }
                    } else {
                        map.insert(name.clone(), JsonValue::String(val_str.clone()));
                    }
                }
            }
            None => {
                map.insert(name.clone(), JsonValue::Null);
            }
        }
    }
    
    // If there are more values than names, add them with generic names
    if column_values.len() > column_names.len() {
        for i in column_names.len()..column_values.len() {
            if let Some(val) = &column_values[i] {
                map.insert(format!("col_{}", i), JsonValue::String(val.clone()));
            } else {
                map.insert(format!("col_{}", i), JsonValue::Null);
            }
        }
    }
    
    Ok(JsonValue::Object(map))
}