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
pub use processor::{ChangeProcessor, ChangeRecord, ProcessorConfig, extract_record_data};