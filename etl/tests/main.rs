#![cfg(feature = "test-utils")]

#[cfg(feature = "failpoints")]
mod failpoints_pipeline;
mod pipeline;
mod pipeline_replica_identity;
mod pipeline_with_partitioned_table;
mod postgres_store;
mod replication;
