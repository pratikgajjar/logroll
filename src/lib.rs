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
pub use processor::{ChangeProcessor, ChangeRecord, ProcessorConfig, S3Config};
pub use std::collections::HashMap;

use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value as JsonValue};
use postgres_replication::protocol::Tuple;

/// Convert a tuple to a JSON object with proper column names using direct access to tuple data
fn tuple_to_json(tuple: &Tuple, column_names: Option<&[String]>) -> JsonValue {
    let mut map = Map::new();
    
    // Get tuple data directly from the tuple using the tuple_data() method
    let tuple_data = tuple.tuple_data();
    
    // Debug output for troubleshooting
    tracing::debug!(tuple_data_len = tuple_data.len(), "Tuple data values");
    
    // Empty tuple handling
    if tuple_data.is_empty() {
        return JsonValue::Object(map);
    }

    // Process each column's data
    for (i, data) in tuple_data.iter().enumerate() {
        // Determine column name
        let column_name = if let Some(names) = column_names {
            if i < names.len() {
                names[i].clone()
            } else {
                format!("col_{}", i)
            }
        } else {
            format!("col_{}", i)
        };
        
        // Convert the tuple data to appropriate JSON value
        let json_value = match data {
            postgres_replication::protocol::TupleData::Null => JsonValue::Null,
            postgres_replication::protocol::TupleData::Text(bytes) => {
                // Handle text values
                let text = String::from_utf8_lossy(bytes);
                parse_text_value(&text)
            },
            // The imor fork doesn't have TupleData::Binary, adapt to available variants
            postgres_replication::protocol::TupleData::UnchangedToast => {
                // For unchanged TOAST data, provide a placeholder
                JsonValue::String("<unchanged toast>".to_string())
            },
        };
        
        map.insert(column_name, json_value);
    }
    
    JsonValue::Object(map)
}

/// Parse a PostgreSQL text value into an appropriate JSON value
fn parse_text_value(text: &str) -> JsonValue {
    // Debug the text value
    tracing::debug!(text = %text, "Parsing text value");
    
    // Handle NULL values
    if text.is_empty() || text == "NULL" || text.eq_ignore_ascii_case("null") {
        return JsonValue::Null;
    }
    
    // Handle boolean values
    if text == "t" || text.eq_ignore_ascii_case("true") {
        return JsonValue::Bool(true);
    } else if text == "f" || text.eq_ignore_ascii_case("false") {
        return JsonValue::Bool(false);
    }
    
    // Try to parse as JSON
    if (text.starts_with('{') && text.ends_with('}')) ||
       (text.starts_with('[') && text.ends_with(']')) {
        if let Ok(json) = serde_json::from_str(text) {
            return json;
        }
    }
    
    // Try numeric values
    if let Ok(i) = text.parse::<i64>() {
        return JsonValue::Number(i.into());
    }
    
    if let Ok(f) = text.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return JsonValue::Number(n);
        }
    }
    
    // Default to string
    JsonValue::String(text.to_string())
}

// Note: Array parsing is now handled directly within parse_text_value when needed

/// Extract data from a replication message
pub fn extract_record_data(
    message: &LogicalReplicationMessage,
    lsn: PgLsn,
    table_name: String,
    _relation_id: Option<u32>,
    column_names: Option<Vec<String>>,
) -> Result<processor::ChangeRecord> {
    let timestamp = Utc::now();
    
    match message {
        LogicalReplicationMessage::Insert(insert) => {
            // Extract tuple data using native structs with column names
            let tuple_data = tuple_to_json(insert.tuple(), column_names.as_deref());
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: u64::from(lsn),
                table: table_name,
                op: "INSERT".to_string(),
                before: None,
                after: Some(tuple_data),
                metadata: None,
            })
        }
        LogicalReplicationMessage::Update(update) => {
            // Extract new tuple data with column names
            let new_tuple_data = tuple_to_json(update.new_tuple(), column_names.as_deref());
            
            // Extract old tuple data if available
            let old_tuple_data = update.old_tuple()
                .map(|old_tuple| tuple_to_json(old_tuple, column_names.as_deref()));
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: u64::from(lsn),
                table: table_name,
                op: "UPDATE".to_string(),
                before: old_tuple_data,
                after: Some(new_tuple_data),
                metadata: None,
            })
        }
        LogicalReplicationMessage::Delete(delete) => {
            // First try to extract complete tuple data if available (REPLICA IDENTITY FULL)
            let old_tuple_data = if let Some(tuple) = delete.old_tuple() {
                // We have the complete old tuple
                Some(tuple_to_json(tuple, column_names.as_deref()))
            } else {
                // Handle REPLICA IDENTITY DEFAULT/KEYS/NOTHING
                // For 'keys only', we need to extract the key fields manually
                let key_fields = delete.key_tuple();
                
                if let Some(key_tuple) = key_fields {
                    // We have key fields only, which typically contains just the primary key
                    // This happens when replica identity is set to 'keys'
                    
                    // Extract the raw key tuple data to create a compact representation with only primary keys
                    let tuple_data = key_tuple.tuple_data();
                    let mut map = serde_json::Map::new();
                    
                    // Process each key column's data, skipping nulls
                    for (i, data) in tuple_data.iter().enumerate() {
                        // Skip null values entirely to save space
                        if let postgres_replication::protocol::TupleData::Text(bytes) = data {
                            // Only process non-empty text values
                            if !bytes.is_empty() {
                                // Determine column name
                                let column_name = if let Some(names) = column_names.as_deref() {
                                    if i < names.len() {
                                        names[i].clone()
                                    } else {
                                        format!("col_{}", i)
                                    }
                                } else {
                                    format!("col_{}", i)
                                };
                                
                                // Parse value while preserving data type
                                let text = String::from_utf8_lossy(bytes);
                                let json_value = parse_text_value(&text);
                                
                                // Only add non-null values
                                if !json_value.is_null() {
                                    map.insert(column_name, json_value);
                                }
                            }
                        }
                    }
                    
                    Some(JsonValue::Object(map))
                } else {
                    // No identity information available at all (REPLICA IDENTITY NOTHING)
                    tracing::warn!("No tuple data available for DELETE operation on table {}", table_name);
                    None
                }
            };
            
            Ok(processor::ChangeRecord {
                timestamp,
                lsn: u64::from(lsn),
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
                lsn: u64::from(lsn),
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