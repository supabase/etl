//! Worker implementations for concurrent replication tasks.
//!
//! Contains worker types for handling different aspects of replication: apply workers process
//! replication streams, table sync workers handle initial data copying, and worker pools manage
//! concurrent execution and lifecycle coordination.

pub mod apply;
pub mod base;
pub mod pool;
pub mod table_sync;
