use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::env;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;

use logroll_cdc::{
    ChangeProcessor, ChangeRecord, Config, LogicalReplicationMessage, LogicalReplicationStream, 
    NoTls, PgLsn, ProcessorConfig, ReplicationMessage, ReplicationMode, extract_record_data,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with additional information
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    info!("Starting Logroll CDC v{}", logroll_cdc::VERSION);

    // Get configuration from environment variables
    let conninfo = env::var("POSTGRES_CONNECTION_STRING").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/logroll?replication=database".to_string()
    });
    let slot_name =
        env::var("REPLICATION_SLOT_NAME").unwrap_or_else(|_| "logroll_slot".to_string());
    let publication_name =
        env::var("PUBLICATION_NAME").unwrap_or_else(|_| "logroll_publication".to_string());
    let _create_temp_slot = env::var("CREATE_TEMP_SLOT")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);
    let create_publication = env::var("CREATE_PUBLICATION")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);
        
    // Configure the processor
    let processor_config = ProcessorConfig {
        output_dir: env::var("OUTPUT_DIR").unwrap_or_else(|_| "data".to_string()),
        max_batch_size: env::var("MAX_BATCH_SIZE")
            .unwrap_or_else(|_| "10000".to_string())
            .parse()
            .unwrap_or(10000),
        flush_interval_ms: env::var("FLUSH_INTERVAL_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000),
        max_file_size: env::var("MAX_FILE_SIZE")
            .unwrap_or_else(|_| "104857600".to_string()) // 100MB default
            .parse()
            .unwrap_or(104857600),
        file_name_pattern: env::var("FILE_NAME_PATTERN")
            .unwrap_or_else(|_| "logroll_changes_{timestamp}_{uuid}.parquet".to_string()),
    };
    
    // Set up channel for change records
    let (tx, rx) = mpsc::channel::<ChangeRecord>(1000);

    // Connect to PostgreSQL
    info!("Connecting to PostgreSQL at {}", conninfo);
    let config = conninfo.parse::<Config>()?;
    let (client, connection) = config.connect(NoTls).await?;

    // Spawn connection handling task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {}", e);
        }
    });

    // Create publication if needed
    if create_publication {
        info!("Creating publication {}", publication_name);
        client
            .simple_query(&format!("DROP PUBLICATION IF EXISTS {}", publication_name))
            .await
            .context("Failed to drop existing publication")?;
        client
            .simple_query(&format!(
                "CREATE PUBLICATION {} FOR ALL TABLES",
                publication_name
            ))
            .await
            .context("Failed to create publication")?;
    }

    // Create replication slot
    info!(
        "Dropping existing replication slot if exists: {}",
        slot_name
    );
    let drop_slot_query = format!(r#"SELECT pg_drop_replication_slot('{}')"#, slot_name);
    let _ = client.simple_query(&drop_slot_query).await;
    
    // Initialize the processor
    let processor = ChangeProcessor::new(processor_config, rx);
    
    // Start the processor in a separate task
    let processor_handle = tokio::spawn(async move {
        if let Err(e) = processor.start().await {
            error!("Processor error: {}", e);
        }
    });

    // Create a new connection specifically for replication
    let mut config = conninfo.parse::<Config>()?;
    config.replication_mode(ReplicationMode::Logical);
    tracing::info!("Config: {:?}", config);
    let (repl_client, repl_connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = repl_connection.await {
            error!("Replication connection error: {}", e);
        }
    });

    // Create replication slot using replication protocol
    let create_slot_query = format!(r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput"#, slot_name);
    // Read the response to get the LSN
    let slot_query = repl_client.simple_query(&create_slot_query).await?;
    let lsn = if let tokio_postgres::SimpleQueryMessage::Row(row) = &slot_query[1] {
        row.get("consistent_point").unwrap()
    } else {
        panic!("unexpected query message");
    };
    info!("Replication slot created with LSN: {}", lsn);

    let options = format!(
        r#""proto_version" '1', "publication_names" '{}'"#,
        publication_name
    );
    let query = format!(
        r#"START_REPLICATION SLOT {} LOGICAL {} ({})"#,
        slot_name, lsn, options
    );
    let copy_stream = repl_client.copy_both_simple::<bytes::Bytes>(&query).await?;

    let stream = LogicalReplicationStream::new(copy_stream);
    tokio::pin!(stream);

        // Map to store relation_id -> relation object mappings for column info
        let mut relation_map = std::collections::HashMap::<u32, logroll_cdc::LogicalReplicationMessage>::new();
    
    // Process replication messages
    while let Some(message) = stream.next().await {
        match message {
            Ok(ReplicationMessage::XLogData(body)) => {
                let lsn = PgLsn::from(body.wal_start() as u64);
                
                match body.into_data() {
                    LogicalReplicationMessage::Begin(begin) => {
                        info!("BEGIN: xid={:?}, lsn={:?}", begin.xid(), begin.final_lsn());
                    }
                    LogicalReplicationMessage::Commit(commit) => {
                        info!("COMMIT: {:?}", commit);
                    }
                    LogicalReplicationMessage::Relation(relation) => {
                        let rel_id = relation.rel_id();
                        let table_name = format!(
                            "{}.{}",
                            relation.namespace().unwrap_or("public"),
                            relation.name().unwrap_or("unknown_table")
                        );
                        
                        info!(
                            "RELATION: id={:?}, name={:?}, columns={:?}",
                            rel_id,
                            table_name,
                            relation.columns().len()
                        );
                        
                        // Store relation mapping with full relation message
                        relation_map.insert(rel_id, LogicalReplicationMessage::Relation(relation));
                    }
                    msg @ LogicalReplicationMessage::Insert(_) |
                    msg @ LogicalReplicationMessage::Update(_) |
                    msg @ LogicalReplicationMessage::Delete(_) |
                    msg @ LogicalReplicationMessage::Truncate(_) => {
                        // Get relation ID and table name
                        let rel_id = match &msg {
                            LogicalReplicationMessage::Insert(insert) => Some(insert.rel_id()),
                            LogicalReplicationMessage::Update(update) => Some(update.rel_id()),
                            LogicalReplicationMessage::Delete(delete) => Some(delete.rel_id()),
                            LogicalReplicationMessage::Truncate(_) => None,
                            _ => None,
                        };
                        
                        // Get table name from relation ID
                        let table_name = if let Some(id) = rel_id {
                            match relation_map.get(&id) {
                                Some(LogicalReplicationMessage::Relation(rel)) => format!(
                                    "{}.{}",
                                    rel.namespace().unwrap_or("public"),
                                    rel.name().unwrap_or("unknown_table")
                                ),
                                _ => format!("unknown_table_{}", id)
                            }
                        } else {
                            "multiple_tables".to_string()
                        };
                        
                        // Log the operation
                        match &msg {
                            LogicalReplicationMessage::Insert(insert) => {
                                info!("INSERT: table={}, rel_id={}", table_name, insert.rel_id());
                            }
                            LogicalReplicationMessage::Update(update) => {
                                info!("UPDATE: table={}, rel_id={}", table_name, update.rel_id());
                            }
                            LogicalReplicationMessage::Delete(delete) => {
                                info!("DELETE: table={}, rel_id={}", table_name, delete.rel_id());
                            }
                            LogicalReplicationMessage::Truncate(truncate) => {
                                info!("TRUNCATE: rel_ids={:?}", truncate.rel_ids());
                            }
                            _ => {}
                        }
                        
                        // Extract data and send to processor
                        match extract_record_data(&msg, lsn, table_name, &relation_map) {
                            Ok(record) => {
                                if let Err(e) = tx.send(record).await {
                                    warn!("Failed to send record to processor: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to extract data from message: {}", e);
                            }
                        }
                    }
                    LogicalReplicationMessage::Type(type_msg) => {
                        info!(
                            "TYPE: id={:?}, name={:?}, namespace={:?}",
                            type_msg.id(),
                            type_msg.name(),
                            type_msg.namespace()
                        );
                    }
                    LogicalReplicationMessage::Origin(origin) => {
                        info!("ORIGIN: {:?}", origin);
                    }
                    _ => {
                        info!("Unhandled LogicalReplicationMessage variant");
                    }
                }
            },
            Ok(ReplicationMessage::PrimaryKeepAlive(_keepalive)) => {
                debug!("KEEPALIVE received");
                // We'll just log the keepalive messages for now
                // Status updates would normally be sent here if required
            }
            Ok(_) => {}
            Err(e) => {
                error!("Replication error: {}", e);
                break;
            }
        }
    }

    info!("Logroll CDC shutting down, waiting for processor to finish");
    
    // Drop the sender to signal the processor to finish
    drop(tx);
    
    // Wait for the processor to finish
    if let Err(e) = processor_handle.await {
        error!("Processor task panicked: {}", e);
    }
    
    info!("Logroll CDC shutdown complete");
    Ok(())
}


