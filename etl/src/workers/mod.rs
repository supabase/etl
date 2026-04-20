//! Worker implementations for concurrent replication tasks.
//!
//! Contains worker types for handling different aspects of replication: apply workers process
//! replication streams, table sync workers handle initial data copying, and worker pools manage
//! concurrent execution and lifecycle coordination.

mod apply;
mod policy;
mod pool;
mod table_sync;
mod table_sync_copy;

pub(crate) use apply::{ApplyWorker, ApplyWorkerHandle};
pub(crate) use policy::ErrorHandlingPolicy;
pub(crate) use pool::TableSyncWorkerPool;
pub(crate) use table_sync::{TableSyncWorker, TableSyncWorkerState};
pub(crate) use table_sync_copy::{TableCopyResult, table_copy};
