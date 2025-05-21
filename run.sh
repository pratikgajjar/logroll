#!/bin/bash

# Default environment variables
export RUST_LOG=${RUST_LOG:-info}
export POSTGRES_CONNECTION_STRING=${POSTGRES_CONNECTION_STRING:-"postgres://postgres:postgres@localhost:5432/logroll"}
export REPLICATION_SLOT_NAME=${REPLICATION_SLOT_NAME:-"logroll_slot"}
export OUTPUT_PLUGIN=${OUTPUT_PLUGIN:-"pgoutput"}
export PUBLICATION_NAME=${PUBLICATION_NAME:-"logroll_publication"}
export PROTOCOL_VERSION=${PROTOCOL_VERSION:-"2"}
export CREATE_TEMP_SLOT=${CREATE_TEMP_SLOT:-"false"}
export CREATE_PUBLICATION=${CREATE_PUBLICATION:-"true"}
export ENABLE_S3=true
export S3_ENDPOINT=http://localhost:9000
export S3_BUCKET=logroll
export S3_ACCESS_KEY=minio
export S3_SECRET_KEY=minio123
export S3_REGION=us-east-1
export S3_PREFIX=cdc-data/

# Build and run
cd "$(dirname "$0")"
cargo build && cargo run
