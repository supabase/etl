//! Table synchronization runtime worker support.

mod pool;
mod slot;
mod worker;

pub(crate) use pool::{TableSyncWorkerId, TableSyncWorkerPool};
pub(crate) use slot::TableSyncSlot;
pub(crate) use worker::{
    TableSyncWorker, TableSyncWorkerHandle, TableSyncWorkerResult, TableSyncWorkerState,
};
