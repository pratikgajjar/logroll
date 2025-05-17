use std::env;
use serde::{Deserialize, Serialize};
use thiserror::Error;


use crate::error::{ReplicationError, Result};

/// Configuration errors that can occur when parsing or validating config
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("missing required configuration: {0}")]
    MissingConfig(String),

    #[error("invalid configuration value: {0}")]
    InvalidValue(String),

    #[error("environment variable error: {0}")]
    EnvVarError(#[from] env::VarError),
}

/// Serializable representation of ReplicationMode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableReplicationMode {
    /// Logical replication
    Logical,
    /// Physical replication
    Physical,
}

impl From<tokio_postgres::config::ReplicationMode> for SerializableReplicationMode {
    fn from(mode: tokio_postgres::config::ReplicationMode) -> Self {
        match mode {
            tokio_postgres::config::ReplicationMode::Logical => SerializableReplicationMode::Logical,
            tokio_postgres::config::ReplicationMode::Physical => SerializableReplicationMode::Physical,
            _ => SerializableReplicationMode::Logical, // Default for any future additions
        }
    }
}

impl From<SerializableReplicationMode> for tokio_postgres::config::ReplicationMode {
    fn from(mode: SerializableReplicationMode) -> Self {
        match mode {
            SerializableReplicationMode::Logical => tokio_postgres::config::ReplicationMode::Logical,
            SerializableReplicationMode::Physical => tokio_postgres::config::ReplicationMode::Physical,
        }
    }
}

/// Replication configuration for connecting to PostgreSQL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Connection string for PostgreSQL
    pub connection_string: String,
    
    /// Name of the replication slot
    pub slot_name: String,
    
    /// Name of the publication to use
    pub publication_name: String,
    
    /// Whether to create a temporary replication slot
    pub create_temp_slot: bool,
    
    /// Whether to create the publication
    pub create_publication: bool,
    
    /// Replication mode (logical or physical) - not serialized
    #[serde(skip)]
    pub replication_mode: tokio_postgres::config::ReplicationMode,
    
    /// Serializable replication mode - this is the serialized representation
    #[serde(rename = "replication_mode")]
    pub serializable_replication_mode: SerializableReplicationMode,
    
    /// Interval in milliseconds for status updates
    pub status_interval_ms: u64,
    
    /// Buffer capacity for the replication stream
    pub buffer_capacity: usize,
    
    /// Timeout for receive operations in milliseconds
    pub receive_timeout_ms: u64,
    
    /// Whether to automatically acknowledge messages
    pub auto_ack: bool,
}



impl Default for ReplicationConfig {
    fn default() -> Self {
        let mode = tokio_postgres::config::ReplicationMode::Logical;
        Self {
            connection_string: "postgres://postgres:postgres@localhost:5432/logroll?replication=database".to_string(),
            slot_name: "logroll_slot".to_string(),
            publication_name: "logroll_publication".to_string(),
            create_temp_slot: false,
            create_publication: true,
            replication_mode: mode,
            serializable_replication_mode: SerializableReplicationMode::from(mode),
            status_interval_ms: 10000, // 10 seconds
            buffer_capacity: 1024,
            receive_timeout_ms: 10000, // 10 seconds
            auto_ack: true,
        }
    }
}

impl ReplicationConfig {
    /// Create a new config from environment variables
    pub fn from_env() -> Result<Self> {
        let connection_string = env::var("POSTGRES_CONNECTION_STRING")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/logroll?replication=database".to_string());
        
        let slot_name = env::var("REPLICATION_SLOT_NAME")
            .unwrap_or_else(|_| "logroll_slot".to_string());
        
        let publication_name = env::var("PUBLICATION_NAME")
            .unwrap_or_else(|_| "logroll_publication".to_string());
        
        let create_temp_slot = env::var("CREATE_TEMP_SLOT")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .map_err(|_| ReplicationError::ConfigError("Invalid CREATE_TEMP_SLOT value".to_string()))?;
        
        let create_publication = env::var("CREATE_PUBLICATION")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .map_err(|_| ReplicationError::ConfigError("Invalid CREATE_PUBLICATION value".to_string()))?;
        
        let status_interval_ms = env::var("STATUS_INTERVAL_MS")
            .unwrap_or_else(|_| "10000".to_string())
            .parse::<u64>()
            .map_err(|_| ReplicationError::ConfigError("Invalid STATUS_INTERVAL_MS value".to_string()))?;
        
        let buffer_capacity = env::var("BUFFER_CAPACITY")
            .unwrap_or_else(|_| "1024".to_string())
            .parse::<usize>()
            .map_err(|_| ReplicationError::ConfigError("Invalid BUFFER_CAPACITY value".to_string()))?;
        
        let receive_timeout_ms = env::var("RECEIVE_TIMEOUT_MS")
            .unwrap_or_else(|_| "10000".to_string())
            .parse::<u64>()
            .map_err(|_| ReplicationError::ConfigError("Invalid RECEIVE_TIMEOUT_MS value".to_string()))?;
        
        let auto_ack = env::var("AUTO_ACK")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .map_err(|_| ReplicationError::ConfigError("Invalid AUTO_ACK value".to_string()))?;

        let replication_mode = tokio_postgres::config::ReplicationMode::Logical;
        let serializable_replication_mode = SerializableReplicationMode::from(replication_mode);

        Ok(Self {
            connection_string,
            slot_name,
            publication_name,
            create_temp_slot,
            create_publication,
            replication_mode,
            serializable_replication_mode,
            status_interval_ms,
            buffer_capacity,
            receive_timeout_ms,
            auto_ack,
        })
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.connection_string.is_empty() {
            return Err(ReplicationError::ConfigError("Connection string cannot be empty".to_string()));
        }
        
        if self.slot_name.is_empty() {
            return Err(ReplicationError::ConfigError("Replication slot name cannot be empty".to_string()));
        }
        
        if self.publication_name.is_empty() {
            return Err(ReplicationError::ConfigError("Publication name cannot be empty".to_string()));
        }
        
        Ok(())
    }
}