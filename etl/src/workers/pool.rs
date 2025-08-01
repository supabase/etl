use postgres::schema::TableId;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, warn};

use crate::destination::Destination;
use crate::error::EtlResult;
use crate::state::store::StateStore;
use crate::workers::base::{Worker, WorkerHandle};
use crate::workers::table_sync::{TableSyncWorker, TableSyncWorkerHandle, TableSyncWorkerState};

#[derive(Debug)]
pub struct TableSyncWorkerPoolInner {
    /// The table sync workers that are currently active.
    active: HashMap<TableId, TableSyncWorkerHandle>,
    /// The table sync workers that are finished, meaning that either they completed or errored.
    finished: HashMap<TableId, Vec<TableSyncWorkerHandle>>,
    /// A [`Notify`] instance which notifies subscribers when there is a change in the pool (e.g.
    /// a new worker changes from active to inactive).
    pool_update: Option<Arc<Notify>>,
}

impl TableSyncWorkerPoolInner {
    fn new() -> Self {
        Self {
            active: HashMap::new(),
            finished: HashMap::new(),
            pool_update: None,
        }
    }

    pub async fn start_worker<S, D>(&mut self, worker: TableSyncWorker<S, D>) -> EtlResult<bool>
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

    pub fn mark_worker_finished(&mut self, table_id: TableId) {
        let removed_worker = self.active.remove(&table_id);

        if let Some(waiting) = self.pool_update.take() {
            waiting.notify_one();
        }

        if let Some(removed_worker) = removed_worker {
            self.finished
                .entry(table_id)
                .or_default()
                .push(removed_worker);
        }
    }

    pub fn get_active_worker_state(&self, table_id: TableId) -> Option<TableSyncWorkerState> {
        let state = self.active.get(&table_id)?.state().clone();

        debug!("retrieved active worker state for table {table_id}");

        Some(state)
    }

    pub async fn wait_all(&mut self) -> EtlResult<Option<Arc<Notify>>> {
        // If there are active workers, we return the notify, signaling that not all of them are
        // ready.
        //
        // This is done since if we wait on active workers, there will be a deadlock because the
        // worker within the `ReactiveFuture` will not be able to hold the lock onto the pool to
        // mark itself as finished.
        if !self.active.is_empty() {
            let notify = Arc::new(Notify::new());
            self.pool_update = Some(notify.clone());

            return Ok(Some(notify));
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
