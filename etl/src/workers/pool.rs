use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, warn};

use crate::error::EtlResult;
use crate::workers::base::WorkerHandle;
use crate::workers::table_sync::{TableSyncWorkerHandle, TableSyncWorkerState};

/// Internal state for [`TableSyncWorkerPool`].
#[derive(Debug)]
pub struct TableSyncWorkerPoolInner {
    /// Currently active table sync workers indexed by table ID.
    active: HashMap<TableId, TableSyncWorkerHandle>,
    /// Completed or failed table sync workers, preserving history for inspection.
    finished: HashMap<TableId, Vec<TableSyncWorkerHandle>>,
    /// Notification mechanism for pool state changes.
    pool_update: Arc<Notify>,
}

impl TableSyncWorkerPoolInner {
    /// Creates a new empty table sync worker pool inner state.
    ///
    /// This constructor initializes the pool with empty collections for active
    /// and finished workers, with no notification mechanism initially configured.
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            finished: HashMap::new(),
            pool_update: Arc::new(Notify::new()),
        }
    }

    /// Marks a worker as finished and moves it from active to finished state.
    ///
    /// This method handles worker completion by transferring the worker handle
    /// from the active pool to the finished pool. It also notifies any waiting processes
    /// about the pool state change.
    pub fn mark_worker_finished(&mut self, table_id: TableId) {
        let removed_worker = self.active.remove(&table_id);

        self.pool_update.notify_waiters();

        if let Some(removed_worker) = removed_worker {
            self.finished
                .entry(table_id)
                .or_default()
                .push(removed_worker);
        }
    }

    /// Retrieves the state handle for an active worker by table ID.
    ///
    /// This method provides access to the state management structure of an
    /// active worker, enabling coordination and monitoring of the worker's
    /// synchronization progress.
    pub fn get_active_worker_state(&self, table_id: TableId) -> Option<TableSyncWorkerState> {
        let state = self.active.get(&table_id)?.state().clone();

        debug!(%table_id, "retrieved active worker state");

        Some(state)
    }

    /// Checks if an active worker exists for the given table.
    pub fn has_active_worker(&self, table_id: TableId) -> bool {
        self.active.contains_key(&table_id)
    }

    /// Inserts a worker handle into the pool.
    ///
    /// If a worker for the table already exists, logs a warning, aborts the new handle,
    /// and skips insertion. Callers should check [`has_active_worker`] before calling.
    pub fn insert_handle(&mut self, table_id: TableId, mut handle: TableSyncWorkerHandle) {
        match self.active.entry(table_id) {
            Entry::Vacant(entry) => {
                entry.insert(handle);
                debug!(%table_id, "added worker to pool");
            }
            Entry::Occupied(_) => {
                warn!(%table_id, "worker already exists in pool, aborting new handle");
                handle.abort();
            }
        }
    }

    /// Waits for all workers to complete or returns a notification for active workers.
    ///
    /// This method implements a non-blocking wait strategy for worker completion.
    /// If active workers remain, it returns a notification handle that callers can
    /// use to wait for state changes. If all workers are finished, it processes
    /// their results and reports any errors.
    ///
    /// Returns `Ok(Some(notify))` when active workers remain, `Ok(None)` when all
    /// workers have completed successfully, or an error if any worker failed.
    pub async fn wait_all(&mut self) -> EtlResult<Option<Arc<Notify>>> {
        // If there are active workers, we return the notify, signaling that not all of them are
        // ready.
        if !self.active.is_empty() {
            return Ok(Some(self.pool_update.clone()));
        }

        let mut errors = Vec::new();
        for (_, workers) in mem::take(&mut self.finished) {
            for worker in workers {
                // The `wait` method will return either an error due to a caught panic or the error
                // returned by the worker.
                if let Err(err) = worker.wait().await {
                    errors.push(err);
                }
            }
        }

        if !errors.is_empty() {
            return Err(errors.into());
        }

        Ok(None)
    }
}

/// Pool for managing multiple table synchronization workers.
///
/// [`TableSyncWorkerPool`] coordinates the execution of multiple table sync workers
/// that run in parallel during the initial synchronization phase of ETL pipelines.
/// It provides methods for spawning workers, tracking their progress, and waiting
/// for completion of all synchronization operations.
#[derive(Debug, Clone)]
pub struct TableSyncWorkerPool {
    inner: Arc<Mutex<TableSyncWorkerPoolInner>>,
}

impl TableSyncWorkerPool {
    /// Creates a new empty table sync worker pool.
    ///
    /// The pool starts with no active workers and can accept new workers
    /// as tables need to be synchronized.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TableSyncWorkerPoolInner::new())),
        }
    }

    /// Waits for all active table sync workers to complete.
    ///
    /// This method blocks until all workers in the pool have finished their
    /// synchronization tasks. If any workers encounter errors, those errors
    /// are collected and returned.
    pub async fn wait_all(&self) -> EtlResult<()> {
        loop {
            // We try first to wait for all workers to be finished, in case there are still active
            // workers, we get back a `Notify` which we will use to try again once new workers reported
            // their finished status.
            let notify = {
                let mut workers = self.inner.lock().await;
                let Some(notify) = workers.wait_all().await? else {
                    return Ok(());
                };

                notify
            };

            notify.notified().await;
        }
    }
}

impl Default for TableSyncWorkerPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for TableSyncWorkerPool {
    type Target = Mutex<TableSyncWorkerPoolInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
