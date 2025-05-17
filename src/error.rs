use std::io;
use thiserror::Error;
use tokio_postgres::Error as PgError;

/// ReplicationError represents all possible errors that can occur during replication.
#[derive(Error, Debug)]
pub enum ReplicationError {
    #[error("failed to connect to database: {0}")]
    ConnectionFailed(#[from] PgError),

    #[error("replication protocol error: {0}")]
    ReplicationProtocolError(String),

    #[error("error parsing replication message: {0}")]
    MessageParsingError(String),

    #[error("invalid message type received: {0}")]
    InvalidMessageType(String),

    #[error("timeline ended, next timeline: {next_timeline}, starting at position: {next_timeline_start_pos}")]
    EndTimeline {
        next_timeline: i32,
        next_timeline_start_pos: String,
    },

    #[error("I/O error: {0}")]
    IOError(#[from] io::Error),

    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("shutdown requested")]
    ShutdownRequested,

    #[error("validation error: {0}")]
    ValidationError(String),

    #[error("postgres replication error: {0}")]
    PgReplicationError(String),

    #[error("generic error: {0}")]
    Generic(String),

    #[error("configuration error: {0}")]
    ConfigError(String),
}

/// Convenient Result type for replication operations
pub type Result<T> = std::result::Result<T, ReplicationError>;