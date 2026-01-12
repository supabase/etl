//! Worker implementations for concurrent replication tasks.
//!
//! Contains worker types for handling different aspects of replication: apply workers process
//! replication streams, table sync workers handle initial data copying, heartbeat workers send
//! periodic heartbeats to primary when streaming from replicas, and worker pools manage
//! concurrent execution and lifecycle coordination.

pub mod apply;
pub mod base;
pub mod heartbeat;
pub mod pool;
pub mod table_sync;
