use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::FutureExt;
use tokio::{
    sync::{Mutex, RwLock, watch},
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    runtime::table_sync::{
        TableSyncWorkerHandle, TableSyncWorkerResult, TableSyncWorkerState,
        worker::table_sync_worker_panic_error,
    },
    schema::TableId,
};

/// Unique identifier for a table sync worker run.
///
/// Each spawned worker is identified by its table ID and a monotonically
/// increasing run ID. This allows tracking all worker runs across restarts for
/// the same table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TableSyncWorkerId {
    /// Identifier of the table being synchronized by this worker.
    pub(crate) table_id: TableId,
    /// Monotonically increasing identifier distinguishing individual worker
    /// runs for the same table.
    pub(crate) run_id: u64,
}

impl std::fmt::Display for TableSyncWorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table_id, self.run_id)
    }
}

/// Pool for managing multiple table synchronization workers.
///
/// [`TableSyncWorkerPool`] coordinates the execution of multiple table sync
/// workers that run in parallel during the initial synchronization stage of ETL
/// pipelines. It provides methods for spawning workers, tracking their
/// progress, and waiting for completion of all synchronization operations.
///
/// The pool uses a two-lock design:
/// - A [`Mutex`] protects the [`JoinSet`] for task spawning and waiting.
/// - An [`RwLock`] protects the workers map for cheap read access to worker
///   state.
///
/// Locking order during spawn is: workers_join_set lock -> workers write lock.
/// This ensures that [`Self::wait_all`] (which holds the workers_join_set lock)
/// blocks any new spawns until all existing workers have completed.
#[derive(Debug)]
pub(crate) struct TableSyncWorkerPool {
    /// Monotonically increasing counter for generating unique run IDs.
    next_run_id: AtomicU64,
    /// Owns all spawned worker tasks. Locked first during spawn operations.
    workers_join_set: Mutex<JoinSet<(TableSyncWorkerId, EtlResult<TableSyncWorkerResult>)>>,
    /// Last known worker handle per table. Uses RwLock for cheap read access.
    workers: RwLock<HashMap<TableId, TableSyncWorkerHandle>>,
    /// First fatal failure, retained even before pipeline supervision waits.
    failure: watch::Sender<Option<EtlError>>,
    /// Stops intake when a worker can no longer safely manage its table.
    shutdown_token: CancellationToken,
}

impl TableSyncWorkerPool {
    /// Creates a new empty table sync worker pool.
    pub(crate) fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            next_run_id: AtomicU64::new(0),
            workers_join_set: Mutex::new(JoinSet::new()),
            workers: RwLock::new(HashMap::new()),
            failure: watch::channel(None).0,
            shutdown_token,
        }
    }

    /// Spawns a new worker into the pool if no active worker exists for the
    /// table.
    ///
    /// If a worker for the given table already exists and is still running,
    /// logs a warning and skips spawning. If no worker exists or the
    /// previous worker has finished, spawns a new worker with a unique run
    /// ID. Completed run results remain tracked until [`Self::wait_all`].
    /// Fatal errors and panics also stop intake and wake
    /// [`Self::wait_for_failure`].
    ///
    /// The locking order is: workers_join_set -> workers (write). This ensures
    /// that if [`Self::wait_all`] is in progress, this method blocks until it
    /// completes.
    pub(crate) async fn spawn<F>(&self, table_id: TableId, state: TableSyncWorkerState, future: F)
    where
        F: Future<Output = EtlResult<TableSyncWorkerResult>> + Send + 'static,
    {
        // Lock workers_join_set first to ensure we block if wait_all is in
        // progress.
        let mut workers_join_set = self.workers_join_set.lock().await;
        let mut workers = self.workers.write().await;

        // Check if a worker already exists and is still running.
        if let Some(handle) = workers.get(&table_id)
            && !handle.is_finished()
        {
            warn!(
                table_id = table_id.0,
                "table sync worker already exists in pool and is still running"
            );

            return;
        }

        // Generate a unique run ID for this worker.
        let run_id = self.next_run_id.fetch_add(1, Ordering::Relaxed);
        let worker_id = TableSyncWorkerId { table_id, run_id };

        // Report fatal failures independently of draining the pool: apply may
        // otherwise keep running indefinitely without joining this worker.
        let failure = self.failure.clone();
        let shutdown_token = self.shutdown_token.clone();
        let abort_handle = workers_join_set.spawn(async move {
            let result = AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .unwrap_or_else(|payload| Err(table_sync_worker_panic_error(payload)));
            if let Err(error) = &result {
                failure.send_if_modified(|first| {
                    if first.is_some() {
                        return false;
                    }
                    *first = Some(error.clone());
                    true
                });
                shutdown_token.cancel();
            }
            (worker_id, result)
        });

        // Create and store the handle.
        let handle = TableSyncWorkerHandle::new(worker_id, state, abort_handle);
        workers.insert(table_id, handle);

        debug!(%worker_id, "spawned table sync worker in pool");
    }

    /// Retrieves the state handle for an active worker by table ID.
    ///
    /// Returns `None` if no worker exists for the table or if the worker has
    /// finished. This method only acquires a read lock on the workers map.
    pub(crate) async fn get_active_worker_state(
        &self,
        table_id: TableId,
    ) -> Option<TableSyncWorkerState> {
        let workers = self.workers.read().await;
        let handle = workers.get(&table_id)?;

        // Check if the worker is still running.
        if handle.is_finished() {
            return None;
        }

        debug!(table_id = table_id.0, "retrieved active table sync worker state");

        Some(handle.state())
    }

    /// Observes the first fatal worker failure without locking the task set or
    /// waiting for apply to stop. Ordinary table errors remain worker outcomes.
    pub(crate) async fn wait_for_failure(&self) -> EtlError {
        let mut failure = self.failure.subscribe();
        // The pool retains the sender, and failures are never cleared.
        let error = failure.wait_for(Option::is_some).await.unwrap();
        error.as_ref().unwrap().clone()
    }

    /// Waits for all workers in the pool to complete.
    ///
    /// This method holds the workers_join_set lock while draining all tasks,
    /// which blocks any new spawn attempts. For each completed task, it briefly
    /// acquires a write lock on the workers map to remove the entry only if the
    /// worker_id matches.
    ///
    /// The first supervision error returns immediately and drops remaining
    /// task handles to request cancellation without waiting.
    ///
    /// When the drain budget expires, aborts and joins remaining workers.
    /// Only owner-requested cancellations are accepted; failures still
    /// propagate.
    pub(crate) async fn wait_all(&self, force_stop: &CancellationToken) -> EtlResult<()> {
        let mut workers_join_set = self.workers_join_set.lock().await;
        let mut stopping = false;

        loop {
            let result = tokio::select! {
                biased;

                result = workers_join_set.join_next() => result,

                () = force_stop.cancelled(), if !stopping => {
                    stopping = true;
                    workers_join_set.abort_all();
                    continue;
                }
            };
            let Some(result) = result else { break };
            if stopping && matches!(&result, Err(error) if error.is_cancelled()) {
                continue;
            }
            let mut workers = self.workers.write().await;
            if let Err(err) = Self::handle_worker_result(&mut workers, result) {
                *workers_join_set = JoinSet::new();
                workers.clear();

                return Err(err);
            }
        }

        self.workers.write().await.clear();
        Ok(())
    }

    /// Releases completed worker state and reports supervision failures.
    fn handle_worker_result(
        workers: &mut HashMap<TableId, TableSyncWorkerHandle>,
        result: Result<(TableSyncWorkerId, EtlResult<TableSyncWorkerResult>), JoinError>,
    ) -> EtlResult<()> {
        match result {
            Ok((worker_id, result)) => {
                // Remove the worker handle for the specific worker id.
                if workers
                    .get(&worker_id.table_id)
                    .is_some_and(|handle| handle.worker_id() == worker_id)
                {
                    workers.remove(&worker_id.table_id);
                }

                match result {
                    Ok(result) => debug!(%worker_id, ?result, "table sync worker completed"),
                    Err(err) => {
                        debug!(%worker_id, error = %err, "table sync worker completed with error");

                        return Err(err);
                    }
                }
            }
            Err(err) if err.is_cancelled() => {
                return Err(
                    etl_error!(ErrorKind::TableSyncWorkerCancelled, "Table sync worker was cancelled", source: err),
                );
            }
            Err(err) => {
                return Err(
                    etl_error!(ErrorKind::TableSyncWorkerPanic, "Table sync worker panicked", source: err),
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use crate::{
        error::ErrorKind,
        etl_error,
        runtime::table_sync::{TableSyncWorkerId, TableSyncWorkerPool, TableSyncWorkerState},
        schema::TableId,
        store::TableState,
    };

    /// Fatal failures stop intake and remain observable by a late supervisor,
    /// without requiring the apply worker or pool drain to finish first.
    #[tokio::test]
    async fn fatal_failure_reaches_supervision_before_pool_drain() {
        let shutdown = CancellationToken::new();
        let pool = TableSyncWorkerPool::new(shutdown.clone());
        let table_id = TableId::new(1);
        pool.spawn(table_id, TableSyncWorkerState::new(table_id, TableState::Init), async {
            Err(etl_error!(ErrorKind::SourceQueryFailed, "Test persistence failure"))
        })
        .await;

        tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled()).await.unwrap();
        let error = pool.wait_for_failure().await;
        assert_eq!(error.kind(), ErrorKind::SourceQueryFailed);
        assert_eq!(pool.workers_join_set.lock().await.len(), 1);
        assert_eq!(
            pool.wait_all(&CancellationToken::new()).await.unwrap_err().kind(),
            error.kind()
        );
    }

    /// Panics outside the recoverable attempt path are fatal supervision
    /// failures too, including when they abandon an unconfirmed slot lease.
    #[tokio::test]
    async fn supervision_panic_stops_intake() {
        let shutdown = CancellationToken::new();
        let pool = TableSyncWorkerPool::new(shutdown.clone());
        let table_id = TableId::new(1);
        pool.spawn(table_id, TableSyncWorkerState::new(table_id, TableState::Init), async {
            panic!("Test supervision panic");
        })
        .await;

        tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled()).await.unwrap();
        assert_eq!(pool.wait_for_failure().await.kind(), ErrorKind::TableSyncWorkerPanic);
        assert_eq!(
            pool.wait_all(&CancellationToken::new()).await.unwrap_err().kind(),
            ErrorKind::TableSyncWorkerPanic
        );
    }

    /// The escalation path joins every cancelled worker and preserves genuine
    /// failures instead of treating them as successful cancellation.
    #[tokio::test]
    async fn shutdown_deadline_joins_workers_and_preserves_errors() {
        let shutdown = CancellationToken::new();
        let pool = TableSyncWorkerPool::new(shutdown.clone());
        let (lifetime, dropped) = oneshot::channel::<()>();
        let table_id = TableId::new(1);
        pool.spawn(table_id, TableSyncWorkerState::new(table_id, TableState::Init), async move {
            let _lifetime = lifetime;
            std::future::pending().await
        })
        .await;
        let force_stop = CancellationToken::new();
        force_stop.cancel();
        pool.wait_all(&force_stop).await.unwrap();
        assert!(dropped.await.is_err());
        assert!(pool.workers_join_set.lock().await.is_empty());
        assert!(pool.failure.borrow().is_none());
        assert!(!shutdown.is_cancelled());

        let task = pool.workers_join_set.lock().await.spawn(async {
            (
                TableSyncWorkerId { table_id: TableId::new(1), run_id: 1 },
                Err(etl_error!(ErrorKind::DestinationError, "Test worker failure")),
            )
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            pool.wait_all(&force_stop).await.unwrap_err().kind(),
            ErrorKind::DestinationError
        );
    }
}
