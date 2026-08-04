//! Postgres logical replication protocol implementation.
//!
//! Handles the Postgres logical replication protocol including slot management,
//! streaming changes, and maintaining replication consistency.

mod apply;
mod decoding_state;
pub(crate) mod state;
mod table_sync;
mod worker_type;

pub(crate) use apply::{
    ApplyLoop, ApplyLoopResult, ApplyWorkerContext, TableSyncWorkerContext, WorkerContext,
};
pub(crate) use decoding_state::TableDecodingState;
pub(crate) use table_sync::{TableSyncResult, start_table_sync};
pub use worker_type::WorkerType;
