#![cfg(feature = "test-utils")]

mod pipeline;
mod pipeline_replica_identity;
#[cfg(feature = "failpoints")]
mod pipeline_with_failpoints;
mod pipeline_with_partitioned_table;
mod pipelines_with_schema_changes;
mod postgres_store;
mod replication;
