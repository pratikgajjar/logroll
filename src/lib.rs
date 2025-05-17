//! Logroll - High-Performance PostgreSQL CDC Streaming Library
//!
//! Logroll is a library for streaming changes from PostgreSQL using
//! logical replication. It provides a modular, high-performance way
//! to consume database changes for building CDC (Change Data Capture)
//! pipelines, event sourcing systems, and more.

// Module declarations
pub mod processor;

/// Version of the library
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Re-export types and functions from the underlying libraries
pub use postgres_replication::{
    protocol::{LogicalReplicationMessage, ReplicationMessage},
    LogicalReplicationStream,
};
pub use postgres_types::PgLsn;
pub use tokio_postgres::{config::ReplicationMode, Client, Config, NoTls};

// Re-export our processor types
pub use processor::{ChangeProcessor, ChangeRecord, ProcessorConfig};
pub use std::collections::HashMap;

use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value as JsonValue};

/// Convert a tuple to a JSON object with proper column names
fn tuple_to_json(tuple: &postgres_replication::protocol::Tuple, relation: Option<&LogicalReplicationMessage>) -> JsonValue {
    let mut map = Map::new();
    
    // Get column names if relation is available
    let column_names = if let Some(LogicalReplicationMessage::Relation(rel)) = relation {
        rel.columns().iter()
            .map(|col| col.name().unwrap_or("unknown").to_string())
            .collect::<Vec<_>>()
    } else {
        // Default to generic column names
        let debug_str = format!("{:?}", tuple);
        let count = debug_str.matches("Text(").count() + debug_str.matches("Binary(").count();
        (0..count).map(|i| format!("col_{}", i)).collect()
    };
    
    // Parse the tuple's debug representation to extract values
    let debug_str = format!("{:?}", tuple);
    
    // Basic parsing from debug output
    if let Some(content) = debug_str.strip_prefix("Tuple([").and_then(|s| s.strip_suffix("])")) {
        let columns: Vec<&str> = content.split(", ").collect();
        for (i, col) in columns.iter().enumerate() {
            let name = if i < column_names.len() {
                column_names[i].clone()
            } else {
                format!("col_{}", i)
            };

            // Extract value from debug string
            let value = if *col == "Null" {
                JsonValue::Null
            } else if col.starts_with("Text(") {
                // Extract data between b"..." from Text(b"...")
                if let Some(content) = col.strip_prefix("Text(b\"").and_then(|s| s.strip_suffix("\")")) {
                    // Try to parse as JSON or convert to appropriate type
                    if (content.starts_with('{') && content.ends_with('}')) || 
                    (content.starts_with('[') && content.ends_with(']')) {
                        serde_json::from_str(content).unwrap_or(JsonValue::String(content.to_string()))
                    } else if content == "t" || content == "true" {
                        JsonValue::Bool(true)
                    } else if content == "f" || content == "false" {
                        JsonValue::Bool(false)
                    } else if let Ok(num) = content.parse::<i64>() {
                        JsonValue::Number(num.into())
                    } else if let Ok(num) = content.parse::<f64>() {
                        serde_json::Number::from_f64(num)
                            .map(JsonValue::Number)
                            .unwrap_or_else(|| JsonValue::String(content.to_string()))
                    } else {
                        JsonValue::String(content.to_string())
                    }
                } else {
                    JsonValue::String("<text data>".to_string())
                }
            } else if col.starts_with("Binary(") {
                // Binary data
                JsonValue::String("<binary data>".to_string())
            } else {
                JsonValue::String(col.to_string())
            };
            
            map.insert(name, value);
        }
    }
    
    JsonValue::Object(map)
}

/// Extract data from a replication message
pub fn extract_record_data(
    message: &LogicalReplicationMessage,
    lsn: PgLsn,
    table_name: String,
    relation_id: Option<u32>,
) -> Result<processor::ChangeRecord> {
    let timestamp = Utc::now();
    
    match message {
        LogicalReplicationMessage::Insert(insert) => {
            // Extract tuple data using native structs
            let tuple_data = tuple_to_json(insert.tuple(), None);
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "INSERT".to_string(),
                before: None,
                after: Some(tuple_data),
                metadata: None,
            })
        }
        LogicalReplicationMessage::Update(update) => {
            // Extract new tuple data
            let new_tuple_data = tuple_to_json(update.new_tuple(), None);
            
            // Extract old tuple data if available
            let old_tuple_data = update.old_tuple().map(|old_tuple| tuple_to_json(old_tuple, None));
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "UPDATE".to_string(),
                before: old_tuple_data,
                after: Some(new_tuple_data),
                metadata: None,
            })
        }
        LogicalReplicationMessage::Delete(delete) => {
            // Extract tuple data
            let old_tuple_data = delete.old_tuple().map(|tuple| tuple_to_json(tuple, None));
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "DELETE".to_string(),
                before: old_tuple_data,
                after: None,
                metadata: None,
            })
        }
        LogicalReplicationMessage::Truncate(truncate) => {
            // Truncate affects multiple tables potentially
            let metadata = serde_json::json!({
                "rel_ids": truncate.rel_ids(),
                "options": truncate.options(),
            });
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: lsn.to_string(),
                table: table_name,
                op: "TRUNCATE".to_string(),
                before: None,
                after: None,
                metadata: Some(metadata),
            })
        }
        _ => Err(anyhow::anyhow!("Unsupported message type for extracting data")),
    }
}