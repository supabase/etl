#![cfg(feature = "test-utils")]

mod migrations;
mod pipeline;
mod pipeline_read_replica;
mod pipeline_replica_identity;
#[cfg(feature = "failpoints")]
mod pipeline_with_failpoints;
mod pipeline_with_partitioned_table;
mod pipelines_with_schema_changes;
mod postgres_store;
mod replication;
mod replication_stream;
mod support;
