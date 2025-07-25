use postgres::schema::TableId;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, warn};

use crate::concurrency::future::ReactiveFutureCallback;
use crate::destination::base::Destination;
use crate::state::store::base::StateStore;
use crate::workers::base::{Worker, WorkerHandle, WorkerWaitError, WorkerWaitErrors};
use crate::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerError, TableSyncWorkerHandle, TableSyncWorkerState,
};

#[derive(Debug)]
pub enum TableSyncWorkerInactiveReason {
    Completed,
    Errored(String),
}

#[derive(Debug)]
pub struct TableSyncWorkerPoolInner {
    /// The table sync workers that are currently active.
    active: HashMap<TableId, TableSyncWorkerHandle>,
    /// The table sync workers that are inactive, meaning that they are completed or errored.
    ///
    /// Having the state of finished workers gives us the power to reschedule failed table sync
    /// workers very cheaply since the state can be fed into a new table worker future as if it was
    /// read initially from the state store.
    // TODO: make it a bounded history to guard memory usage.
    inactive: HashMap<TableId, Vec<(TableSyncWorkerInactiveReason, TableSyncWorkerHandle)>>,
    waiting: Option<Arc<Notify>>,
}

impl TableSyncWorkerPoolInner {
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            inactive: HashMap::new(),
            waiting: None,
        }
    }

    pub async fn start_worker<S, D>(
        &mut self,
        worker: TableSyncWorker<S, D>,
    ) -> Result<bool, TableSyncWorkerError>
    where
        S: StateStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        let table_id = worker.table_id();
        if self.active.contains_key(&table_id) {
            warn!("worker for table {} already exists in the pool", table_id);
            return Ok(false);
        }

        let handle = worker.start().await?;
        self.active.insert(table_id, handle);
        debug!(
            "successfully added worker for table {} to the pool",
            table_id
        );

        Ok(true)
    }

    pub fn get_active_worker_state(&self, table_id: TableId) -> Option<TableSyncWorkerState> {
        let state = self.active.get(&table_id)?.state().clone();
        debug!("retrieved worker state for table {table_id}");

        Some(state)
    }

    pub fn set_worker_finished(
        &mut self,
        table_id: TableId,
        reason: TableSyncWorkerInactiveReason,
    ) {
        let removed_worker = self.active.remove(&table_id);

        if let Some(waiting) = self.waiting.take() {
            waiting.notify_one();
        }

        if let Some(removed_worker) = removed_worker {
            debug!("table sync worker finished with reason: {reason:?}",);

            self.inactive
                .entry(table_id)
                .or_default()
                .push((reason, removed_worker));
        }
    }

    pub async fn wait_all(&mut self) -> Result<Option<Arc<Notify>>, WorkerWaitErrors> {
        // If there are active workers, we return the notify, signaling that not all of them are
        // ready.
        //
        // This is done since if we wait on active workers, there will be a deadlock because the
        // worker within the `ReactiveFuture` will not be able to hold the lock onto the pool to
        // mark itself as finished.
        if !self.active.is_empty() {
            let notify = Arc::new(Notify::new());
            self.waiting = Some(notify.clone());

            return Ok(Some(notify));
        }

        let mut errors = Vec::new();
        for (_, workers) in mem::take(&mut self.inactive) {
            for (finish, worker) in workers {
                // If there is an error while waiting for the task, we can assume that there was un
                // uncaught panic or a propagated error.
                if let Err(err) = worker.wait().await {
                    errors.push(err);
                    continue;
                }

                // If we arrive here, it means that the worker task did fail but silently, since
                // the error we see here was reported by the `ReactiveFuture` and swallowed.
                // This should not happen since right now the `ReactiveFuture` is configured to
                // re-propagate the error after marking a table sync worker as finished.
                if let TableSyncWorkerInactiveReason::Errored(err) = finish {
                    errors.push(WorkerWaitError::WorkerSilentlyFailed(err));
                }
            }
        }

        if errors.is_empty() {
            Ok(None)
        } else {
            Err(WorkerWaitErrors(errors))
        }
    }
}

impl ReactiveFutureCallback<TableId> for TableSyncWorkerPoolInner {
    fn on_complete(&mut self, id: TableId) {
        self.set_worker_finished(id, TableSyncWorkerInactiveReason::Completed);
    }

    fn on_error(&mut self, id: TableId, error: String) {
        self.set_worker_finished(id, TableSyncWorkerInactiveReason::Errored(error));
    }
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerPool {
    inner: Arc<Mutex<TableSyncWorkerPoolInner>>,
}

impl TableSyncWorkerPool {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TableSyncWorkerPoolInner::new())),
        }
    }

    pub fn get_inner(&self) -> Arc<Mutex<TableSyncWorkerPoolInner>> {
        self.inner.clone()
    }

    pub async fn wait_all(&self) -> Result<(), WorkerWaitErrors> {
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
