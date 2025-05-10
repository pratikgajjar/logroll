use std::time::{Duration, UNIX_EPOCH};
use std::env;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use postgres_replication::protocol::LogicalReplicationMessage;
use postgres_replication::protocol::ReplicationMessage;
use postgres_replication::LogicalReplicationStream;
use postgres_types::PgLsn;
use tokio_postgres::{NoTls, Config, config::ReplicationMode};
use tracing::{info, error};
use tokio_postgres::SimpleQueryMessage::Row;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Get configuration from environment variables
    let conninfo = env::var("POSTGRES_CONNECTION_STRING")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/logroll?replication=database".to_string());
    let slot_name = env::var("REPLICATION_SLOT_NAME")
        .unwrap_or_else(|_| "logroll_slot".to_string());
    let publication_name = env::var("PUBLICATION_NAME")
        .unwrap_or_else(|_| "logroll_publication".to_string());
    let create_temp_slot = env::var("CREATE_TEMP_SLOT")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);
    let create_publication = env::var("CREATE_PUBLICATION")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .unwrap_or(true);

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
            .simple_query(&format!("CREATE PUBLICATION {} FOR ALL TABLES", publication_name))
            .await
            .context("Failed to create publication")?;
    }

    // Create replication slot
    info!("Dropping existing replication slot if exists: {}", slot_name);
    let drop_slot_query = format!(
        r#"SELECT pg_drop_replication_slot('{}')"#,
        slot_name
    );
    let _ = client
        .simple_query(&drop_slot_query)
        .await;

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
    let create_slot_query = format!(
        r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput"#,
        slot_name
    );
    // Read the response to get the LSN
    let slot_query = repl_client.simple_query(&create_slot_query).await.unwrap();
    let lsn = if let Row(row) = &slot_query[1] {
        row.get("consistent_point").unwrap()
    } else {
        panic!("unexpeced query message");
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
    let copy_stream = repl_client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    let stream = LogicalReplicationStream::new(copy_stream);
    tokio::pin!(stream);

    // Process replication messages
    while let Some(message) = stream.next().await {
        match message {
            Ok(ReplicationMessage::XLogData(body)) => {
                match body.into_data() {
                    LogicalReplicationMessage::Begin(begin) => {
                        info!("BEGIN: xid={:?}, lsn={:?}", begin.xid(), begin.final_lsn());
                    }
                    LogicalReplicationMessage::Commit(commit) => {
                        info!("COMMIT: {:?}", commit);
                    }
                    LogicalReplicationMessage::Insert(insert) => {
                        info!(
                            "INSERT: rel_id={:?}, tuple={:?}",
                            insert.rel_id(),
                            insert.tuple()
                        );
                    }
                    LogicalReplicationMessage::Update(update) => {
                        info!(
                            "UPDATE: rel_id={:?}, old_tuple={:?}, new_tuple={:?}",
                            update.rel_id(),
                            update.old_tuple(),
                            update.new_tuple()
                        );
                    }
                    LogicalReplicationMessage::Delete(delete) => {
                        info!(
                            "DELETE: rel_id={:?}, tuple={:?}",
                            delete.rel_id(),
                            delete.old_tuple()
                        );
                    }
                    LogicalReplicationMessage::Relation(relation) => {
                        info!(
                            "RELATION: id={:?}, name={:?}, columns={:?}",
                            relation.rel_id(),
                            relation.name(),
                            relation.columns()
                        );
                    }
                    LogicalReplicationMessage::Truncate(truncate) => {
                        info!(
                            "TRUNCATE: rel_ids={:?}, options={:?}",
                            truncate.rel_ids(),
                            truncate.options()
                        );
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
            }
            Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                info!("KEEPALIVE: {:?}", keepalive);
                // TODO: Send status update if required by your library's API
            }
            Ok(_) => {}
            Err(e) => {
                error!("Replication error: {}", e);
                break;
            }
        }
    }

    Ok(())
} 