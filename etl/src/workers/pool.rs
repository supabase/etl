use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::{debug, error, warn};

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::workers::table_sync::{TableSyncWorkerHandle, TableSyncWorkerState};

/// Unique identifier for a table sync worker run.
///
/// Each spawned worker is identified by its table ID and a monotonically
/// increasing run ID. This allows tracking all worker runs across restarts
/// for the same table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableSyncWorkerId {
    /// Identifier of the table being synchronized by this worker.
    pub table_id: TableId,
    /// Monotonically increasing identifier distinguishing individual worker runs for the same table.
    pub run_id: u64,
}

impl std::fmt::Display for TableSyncWorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_id, self.run_id)
    }
}

/// Pool for managing multiple table synchronization workers.
///
/// [`TableSyncWorkerPool`] coordinates the execution of multiple table sync workers
/// that run in parallel during the initial synchronization phase of ETL pipelines.
/// It provides methods for spawning workers, tracking their progress, and waiting
/// for completion of all synchronization operations.
///
/// The pool uses a two-lock design:
/// - A [`Mutex`] protects the [`JoinSet`] for task spawning and waiting.
/// - An [`RwLock`] protects the workers map for cheap read access to worker state.
///
/// Locking order during spawn is: join_set lock -> workers write lock. This ensures
/// that [`wait_all`] (which holds the join_set lock) blocks any new spawns until
/// all existing workers have completed.
#[derive(Debug)]
pub struct TableSyncWorkerPool {
    /// Monotonically increasing counter for generating unique run IDs.
    next_run_id: AtomicU64,
    /// Owns all spawned worker tasks. Locked first during spawn operations.
    workers_join_set: Mutex<JoinSet<(TableSyncWorkerId, EtlResult<()>)>>,
    /// Last known worker handle per table. Uses RwLock for cheap read access.
    workers: RwLock<HashMap<TableId, TableSyncWorkerHandle>>,
}

impl TableSyncWorkerPool {
    /// Creates a new empty table sync worker pool.
    pub fn new() -> Self {
        Self {
            next_run_id: AtomicU64::new(0),
            workers_join_set: Mutex::new(JoinSet::new()),
            workers: RwLock::new(HashMap::new()),
        }
    }

    /// Spawns a new worker into the pool if no active worker exists for the table.
    ///
    /// If a worker for the given table already exists and is still running, logs
    /// a warning and skips spawning. If no worker exists or the previous worker
    /// has finished, spawns a new worker with a unique run ID.
    ///
    /// The locking order is: join_set -> workers (write). This ensures that if
    /// [`wait_all`] is in progress, this method blocks until it completes.
    pub async fn spawn<F>(&self, table_id: TableId, state: TableSyncWorkerState, future: F)
    where
        F: Future<Output = EtlResult<()>> + Send + 'static,
    {
        // Lock join_set first to ensure we block if wait_all is in progress.
        let mut workers_join_set = self.workers_join_set.lock().await;
        let mut workers = self.workers.write().await;

        // Check if a worker already exists and is still running.
        if let Some(handle) = workers.get(&table_id) {
            if !handle.is_finished() {
                warn!(%table_id, "worker already exists in pool and is still running");
                return;
            }
        }

        // Generate a unique run ID for this worker.
        let run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let worker_id = TableSyncWorkerId { table_id, run_id };

        // Spawn the worker task.
        let abort_handle = workers_join_set.spawn(async move {
            let result = future.await;
            (worker_id, result)
        });

        // Create and store the handle.
        let handle = TableSyncWorkerHandle::new(worker_id, state, abort_handle);
        workers.insert(table_id, handle);

        debug!(%worker_id, "spawned worker in pool");
    }

    /// Retrieves the state handle for an active worker by table ID.
    ///
    /// Returns `None` if no worker exists for the table or if the worker has finished.
    /// This method only acquires a read lock on the workers map.
    pub async fn get_active_worker_state(&self, table_id: TableId) -> Option<TableSyncWorkerState> {
        let workers = self.workers.read().await;
        let handle = workers.get(&table_id)?;

        // Check if the worker is still running.
        if handle.is_finished() {
            return None;
        }

        debug!(%table_id, "retrieved active worker state");

        Some(handle.state())
    }

    /// Checks if an active worker exists for the given table.
    ///
    /// This method only acquires a read lock on the workers map.
    pub async fn has_active_worker(&self, table_id: TableId) -> bool {
        let workers = self.workers.read().await;
        workers
            .get(&table_id)
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Waits for all workers in the pool to complete.
    ///
    /// This method holds the join_set lock while draining all tasks, which blocks
    /// any new spawn attempts. For each completed task, it briefly acquires a write
    /// lock on the workers map to remove the entry only if the worker_id matches.
    ///
    /// If any workers encounter errors, those errors are collected and returned.
    pub async fn wait_all(&self) -> EtlResult<()> {
        let mut errors = Vec::new();
        let mut workers_join_set = self.workers_join_set.lock().await;

        while let Some(result) = workers_join_set.join_next().await {
            match result {
                Ok((worker_id, worker_result)) => {
                    // Only remove from workers map if the worker_id matches.
                    // A new worker with the same table_id but different run_id
                    // may have been spawned, so we must not remove it.
                    //
                    // We lock only after the join was completed, since we want to allow the active
                    // workers to be read while waiting for all to complete.
                    {
                        let mut workers = self.workers.write().await;
                        if let Some(handle) = workers.get(&worker_id.table_id) {
                            if handle.worker_id() == worker_id {
                                workers.remove(&worker_id.table_id);
                            }
                        }
                    }

                    if let Err(err) = worker_result {
                        error!(%worker_id, error = %err, "worker completed with error");
                        errors.push(err);
                    } else {
                        debug!(%worker_id, "worker completed successfully");
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
