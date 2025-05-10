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

# Build and run
cd "$(dirname "$0")"
cargo build && cargo run
