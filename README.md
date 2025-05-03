# Logroll

A high-performance, resilient system that reads PostgreSQL CDC logs, rolls them into compressed parquet files, and pushes them to S3.

## Components

### CDC Reader

The CDC reader component streams database changes in real-time using PostgreSQL logical replication with the `pgoutput` plugin and outputs them in structured log format. It's built with resilience and performance in mind following SOLID principles.

#### Features

- Streams PostgreSQL database changes in real-time using logical replication with the `pgoutput` plugin
- Handles binary data from the replication slot
- Outputs changes in logfmt format for easy integration with log processing tools
- Automatically creates required PostgreSQL publications and replication slots
- Resilient to connection failures with exponential backoff retry mechanism
- Handles graceful shutdowns
- Comprehensive logging and metrics via Prometheus
- Follows SOLID principles for maintainability and extensibility

#### Usage (CDC Reader)

```bash
# With default settings
cargo run --release

# With custom settings
cargo run --release -- --pg-connection-string="postgres://user:password@localhost:5432/dbname" --replication-slot="my_slot" --publication-name="my_pub"
```

#### Command Line Options (CDC Reader)

```
OPTIONS:
    --pg-connection-string <PG_CONNECTION_STRING>
        PostgreSQL connection string [default: postgres://postgres:postgres@localhost:5432/postgres]
        
    --replication-slot <REPLICATION_SLOT>
        Replication slot name [default: logroll_slot]
        
    --publication-name <PUBLICATION_NAME>
        Publication name [default: logroll_pub]
        
    --retry-interval-secs <RETRY_INTERVAL_SECS>
        Retry interval in seconds for connection failures [default: 5]
```

### Main Application

```bash
logroll --config config.yaml
```

### Configuration

```yaml
logroll:
  postgres:
    uri: postgres://user:password@host:port/database
    slot: logroll
    tick: 10s
  s3:
    endpoint: s3.amazonaws.com
    bucket: logroll
    prefix: logroll
    region: us-east-1
    access_key: AKIAIOSFODNN7EXAMPLE
    secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    