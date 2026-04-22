//! Postgres logical replication protocol implementation.
//!
//! Handles the Postgres logical replication protocol including slot management,
//! streaming changes, and maintaining replication consistency.

mod apply;
pub mod client;
mod stream;
mod table_cache;
mod table_sync;

pub(crate) use apply::{
    ApplyLoop, ApplyLoopResult, ApplyWorkerContext, TableSyncWorkerContext, WorkerContext,
};
pub(crate) use stream::{EventsStream, StatusUpdateType, TableCopyStream};
pub(crate) use table_cache::SharedTableCache;
pub(crate) use table_sync::{TableSyncResult, start_table_sync};
