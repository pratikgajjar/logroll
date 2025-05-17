use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::StreamExt;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use postgres_replication::LogicalReplicationStream;
use postgres_types::PgLsn;
use tokio::sync::{broadcast, mpsc};
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, error, info, warn};

use crate::config::ReplicationConfig;
use crate::error::{ReplicationError, Result};
use crate::models::{convert_to_operation, Operation, Relation, ReplicationState, Transaction};

/// ReplicationManager handles setting up and managing the Postgres logical replication process
pub struct ReplicationManager {
    /// Configuration for replication
    config: ReplicationConfig,
    
    /// Replication state
    state: ReplicationState,
    
    /// Channel for sending operations to downstream consumers
    tx: Option<mpsc::Sender<Operation>>,
    
    /// Shutdown signal
    shutdown: Option<broadcast::Receiver<()>>,
}

impl ReplicationManager {
    /// Create a new replication manager with the given configuration
    pub fn new(
        config: ReplicationConfig,
        start_lsn: Option<PgLsn>,
        tx: Option<mpsc::Sender<Operation>>,
        shutdown: Option<broadcast::Receiver<()>>,
    ) -> Self {
        let start_lsn = start_lsn.unwrap_or_else(|| PgLsn::from(0));
        
        Self {
            config,
            state: ReplicationState::new(start_lsn),
            tx,
            shutdown,
        }
    }
    
    /// Set up the replication slot and publication
    pub async fn setup(&self) -> Result<Client> {
        // Connect to PostgreSQL for setup operations
        info!("Connecting to PostgreSQL at {}", self.config.connection_string);
        let config = self.config.connection_string.parse::<Config>()
            .map_err(|e| ReplicationError::ConfigError(format!("Failed to parse connection string: {}", e)))?;
        
        let (client, connection) = config.connect(NoTls).await
            .map_err(|e| ReplicationError::ConnectionFailed(e))?;
        
        // Spawn connection handling task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Connection error: {}", e);
            }
        });
        
        // Create publication if needed
        if self.config.create_publication {
            info!("Creating publication {}", self.config.publication_name);
            client
                .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", self.config.publication_name))
                .await
                .map_err(|e| ReplicationError::Generic(format!("Failed to drop existing publication: {}", e)))?;
                
            client
                .simple_query(&format!(
                    "CREATE PUBLICATION {} FOR ALL TABLES",
                    self.config.publication_name
                ))
                .await
                .map_err(|e| ReplicationError::Generic(format!("Failed to create publication: {}", e)))?;
        }
        
        // Drop existing replication slot if it exists
        info!("Dropping existing replication slot if exists: {}", self.config.slot_name);
        let drop_slot_query = format!(r#"SELECT pg_drop_replication_slot('{}')"#, self.config.slot_name);
        let _ = client.simple_query(&drop_slot_query).await;
        
        Ok(client)
    }
    
    /// Start the replication process
    pub async fn start_replication(&mut self) -> Result<()> {
        // Setup initial connection
        let _setup_client = self.setup().await?;
        
        // Create a new connection specifically for replication
        let mut config = self.config.connection_string.parse::<Config>()
            .map_err(|e| ReplicationError::ConfigError(format!("Failed to parse connection string: {}", e)))?;
            
        config.replication_mode(self.config.replication_mode);
        
        info!("Config: {:?}", config);
        let (repl_client, repl_connection) = config.connect(NoTls).await
            .map_err(|e| ReplicationError::ConnectionFailed(e))?;
            
        tokio::spawn(async move {
            if let Err(e) = repl_connection.await {
                error!("Replication connection error: {}", e);
            }
        });
        
        // Create replication slot
        let create_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {} {} pgoutput"#,
            self.config.slot_name,
            if self.config.create_temp_slot { "TEMPORARY" } else { "LOGICAL" }
        );
        
        // Read the response to get the LSN
        let slot_query = repl_client.simple_query(&create_slot_query).await
            .map_err(|e| ReplicationError::Generic(format!("Failed to create replication slot: {}", e)))?;
            
        let lsn = if let tokio_postgres::SimpleQueryMessage::Row(row) = &slot_query[1] {
            row.get("consistent_point").unwrap_or("0/0")
        } else {
            return Err(ReplicationError::ReplicationProtocolError(
                "Unexpected query message when creating replication slot".to_string(),
            ));
        };
        
        info!("Replication slot created with LSN: {}", lsn);
        self.state.last_received_lsn = lsn.parse::<PgLsn>()
            .map_err(|_| ReplicationError::ReplicationProtocolError("Invalid LSN format".to_string()))?;
        self.state.last_flushed_lsn = self.state.last_received_lsn;
        self.state.last_applied_lsn = self.state.last_received_lsn;
        
        // Start replication
        let options = format!(
            r#""proto_version" '1', "publication_names" '{}'"#,
            self.config.publication_name
        );
        
        let query = format!(
            r#"START_REPLICATION SLOT {} LOGICAL {} ({})"#,
            self.config.slot_name, lsn, options
        );
        
        let copy_stream = repl_client.copy_both_simple::<Bytes>(&query).await
            .map_err(|e| ReplicationError::Generic(format!("Failed to start replication: {}", e)))?;
            
        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);
        
        // Set up status interval timer
        let status_interval = Duration::from_millis(self.config.status_interval_ms);
        let mut current_transaction: Option<Transaction> = None;
        
        // Process replication messages
        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = async { 
                    if let Some(shutdown) = self.shutdown.as_mut() {
                        shutdown.recv().await.ok()
                    } else {
                        std::future::pending().await
                    }
                }, if self.shutdown.is_some() => {
                    info!("Shutdown signal received, stopping replication");
                    break;
                }
                
                // Process next message from stream
                message = stream.next() => {
                    match message {
                        Some(Ok(ReplicationMessage::XLogData(body))) => {
                            // Get WAL position from the message
                            // Since direct access to raw WAL data is abstracted away,
                            // we'll use a simpler approach to track the LSN
                            self.state.last_received_lsn = PgLsn::from(body.wal_start() as u64);
                            self.state.serializable_last_received_lsn = crate::models::SerializablePgLsn::from(self.state.last_received_lsn);
                            
                            match body.into_data() {
                                LogicalReplicationMessage::Begin(begin) => {
                                    debug!("BEGIN: xid={:?}, lsn={:?}", begin.xid(), begin.final_lsn());
                                    
                                    // Start a new transaction
                                    let final_lsn = begin.final_lsn();
                                    current_transaction = Some(Transaction {
                                        xid: begin.xid() as u32,
                                        final_lsn: PgLsn::from(final_lsn as u64),
                                        serializable_final_lsn: crate::models::SerializablePgLsn::from(PgLsn::from(final_lsn as u64)),
                                        commit_time: std::time::UNIX_EPOCH + std::time::Duration::from_micros(begin.timestamp() as u64),
                                        operations: Vec::new(),
                                    });
                                }
                                
                                LogicalReplicationMessage::Commit(_commit) => {
                                    debug!("COMMIT");
                                    
                                    // Process the completed transaction
                                    if let Some(transaction) = current_transaction.take() {
                                        self.state.last_applied_lsn = transaction.final_lsn;
                                        
                                        // Send each operation to the channel
                                        if let Some(tx) = &self.tx {
                                            for op in transaction.operations {
                                                if tx.send(op).await.is_err() {
                                                    warn!("Channel closed, stopping replication");
                                                    return Err(ReplicationError::ShutdownRequested);
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                LogicalReplicationMessage::Relation(relation) => {
                                    debug!(
                                        "RELATION: id={:?}, name={:?}, columns={:?}",
                                        relation.rel_id(), relation.name(), relation.columns()
                                    );
                                    
                                    // Store relation in the state
                                    let rel = Relation {
                                        id: relation.rel_id() as u32,
                                        namespace: relation.namespace().unwrap_or("").to_string(),
                                        name: relation.name().unwrap_or("").to_string(),
                                        replica_identity: u8::from_be_bytes([0]), // Default value since we can't safely cast
                                        columns: relation.columns().iter().map(|col| {
                                            crate::models::RelationColumn {
                                                flags: col.flags() as u8,
                                                name: col.name().unwrap_or("").to_string(),
                                                data_type: col.type_id() as u32,
                                                type_modifier: col.type_modifier(),
                                            }
                                        }).collect(),
                                    };
                                    
                                    self.state.relations.insert(relation.rel_id(), Arc::new(rel));
                                }
                                
                                // Process DML operations
                                msg @ (
                                    LogicalReplicationMessage::Insert(_) |
                                    LogicalReplicationMessage::Update(_) |
                                    LogicalReplicationMessage::Delete(_) |
                                    LogicalReplicationMessage::Truncate(_)
                                ) => {
                                    // Convert to our operation model
                                    if let Some(operation) = convert_to_operation(msg) {
                                        // Add to current transaction if exists
                                        if let Some(transaction) = &mut current_transaction {
                                            transaction.operations.push(operation);
                                        } else {
                                            warn!("Received operation outside of transaction context");
                                        }
                                    }
                                }
                                
                                _ => {
                                    // Other message types (Origin, Type, etc.) are not handled currently
                                    debug!("Unhandled LogicalReplicationMessage variant");
                                }
                            }
                            
                            // Send status update if needed and auto_ack is enabled
                            if self.config.auto_ack && self.state.is_status_update_due(status_interval) {
                                if let Err(e) = self.send_status_update(&mut stream).await {
                                    error!("Failed to send status update: {:?}", e);
                                }
                            }
                        }
                        
                        Some(Ok(ReplicationMessage::PrimaryKeepAlive(keepalive))) => {
                            debug!("KEEPALIVE received");
                                
                            // Send status update if requested
                            if keepalive.reply() != 0 {
                                if let Err(e) = self.send_status_update(&mut stream).await {
                                    error!("Failed to send status update: {}", e);
                                }
                            }
                        }
                        
                        Some(Ok(_)) => {}
                        
                        Some(Err(e)) => {
                            error!("Replication error: {}", e);
                            return Err(ReplicationError::PgReplicationError(e.to_string()));
                        }
                        
                        None => {
                            info!("Replication stream ended");
                            break;
                        }
                    }
                }
                
                // Periodic status update timer
                _ = tokio::time::sleep(status_interval), if !self.config.auto_ack => {
                    if let Err(e) = self.send_status_update(&mut stream).await {
                        error!("Failed to send status update: {}", e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Send a status update to the server
    async fn send_status_update(
        &mut self,
        stream: &mut tokio::pin::Pin<&mut LogicalReplicationStream>,
    ) -> Result<()> {
        debug!(
            "Sending status update: received={}, flushed={}, applied={}",
            self.state.last_received_lsn, self.state.last_flushed_lsn, self.state.last_applied_lsn
        );
        
        // Update the last flushed and applied LSNs
        self.state.last_flushed_lsn = self.state.last_received_lsn;
        
        // Send the status update
        stream.standby_status_update(
            self.state.last_received_lsn,
            self.state.last_flushed_lsn,
            self.state.last_applied_lsn,
            SystemTime::now(),
            false,
        ).await.map_err(|e| ReplicationError::PgReplicationError(e.to_string()))?;
        
        // Update last status time
        self.state.last_status_time = SystemTime::now();
        
        Ok(())
    }
}