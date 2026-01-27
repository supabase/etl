use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, error, warn};

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::workers::table_sync::{TableSyncWorkerHandle, TableSyncWorkerState};

/// Internal state for [`TableSyncWorkerPool`].
#[derive(Debug)]
pub struct TableSyncWorkerPoolInner {
    /// Currently active table sync workers indexed by table ID.
    active: HashMap<TableId, TableSyncWorkerHandle>,
    /// Owns all spawned worker tasks.
    join_set: JoinSet<(TableId, EtlResult<()>)>,
}

impl TableSyncWorkerPoolInner {
    /// Creates a new empty table sync worker pool inner state.
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            join_set: JoinSet::new(),
        }
    }

    /// Spawns and inserts a worker into the pool.
    ///
    /// If a worker for the table already exists and is still running, logs a warning
    /// and skips insertion. Callers should check [`has_active_worker`] before calling.
    pub fn spawn<F>(&mut self, table_id: TableId, state: TableSyncWorkerState, future: F)
    where
        F: Future<Output = EtlResult<()>> + Send + 'static,
    {
        match self.active.entry(table_id) {
            Entry::Vacant(entry) => {
                let abort_handle = self.join_set.spawn(async move {
                    let result = future.await;
                    (table_id, result)
                });

                let handle = TableSyncWorkerHandle::new(state, abort_handle);
                entry.insert(handle);

                debug!(%table_id, "spawned worker in pool");
            }
            Entry::Occupied(entry) => {
                if entry.get().is_finished() {
                    let abort_handle = self.join_set.spawn(async move {
                        let result = future.await;
                        (table_id, result)
                    });

                    let handle = TableSyncWorkerHandle::new(state, abort_handle);
                    entry.remove();
                    self.active.insert(table_id, handle);

                    debug!(%table_id, "replaced finished worker in pool");
                } else {
                    warn!(%table_id, "worker already exists in pool and is still running");
                }
            }
        }
    }

    /// Retrieves the state handle for an active worker by table ID.
    ///
    /// Returns `None` if no worker exists for the table or if the worker has finished.
    pub fn get_active_worker_state(&self, table_id: TableId) -> Option<TableSyncWorkerState> {
        let handle = self.active.get(&table_id)?;

        // Check if the worker is still running.
        if handle.is_finished() {
            return None;
        }

        debug!(%table_id, "retrieved active worker state");

        Some(handle.state())
    }

    /// Checks if an active worker exists for the given table.
    pub fn has_active_worker(&self, table_id: TableId) -> bool {
        self.active
            .get(&table_id)
            .is_some_and(|handle| !handle.is_finished())
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
        let mut errors = Vec::new();

        loop {
            let result = {
                let mut inner = self.inner.lock().await;
                inner.join_set.join_next().await
            };

            let Some(result) = result else {
                // JoinSet is empty, all workers have completed.
                break;
            };

            match result {
                Ok((table_id, worker_result)) => {
                    // Remove from active map.
                    let mut inner = self.inner.lock().await;
                    inner.active.remove(&table_id);

                    if let Err(err) = worker_result {
                        error!(%table_id, error = %err, "worker completed with error");
                        errors.push(err);
                    }
                }
                Err(join_err) => {
                    if join_err.is_cancelled() {
                        debug!("worker task was cancelled");
                    } else {
                        errors.push(etl_error!(
                            ErrorKind::TableSyncWorkerPanic,
                            "Table sync worker panicked",
                            join_err
                        ));
                    }
                }
            }
        }

        // Clean up any remaining entries in active map (shouldn't happen normally).
        {
            let mut inner = self.inner.lock().await;
            inner.active.clear();
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.into())
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
