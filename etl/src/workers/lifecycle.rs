use postgres::schema::TableId;
use tracing::error;

use crate::concurrency::future::ReactiveFutureCallback;
use crate::state::store::base::StateStore;
use crate::state::table::TableReplicationPhase;
use crate::workers::pool::{TableSyncWorkerInactiveReason, TableSyncWorkerPool};
use crate::workers::table_sync::TableSyncWorkerState;

#[derive(Debug, Clone)]
pub struct WorkerLifecycleObserver<S> {
    pool: TableSyncWorkerPool,
    state_store: S,
}

impl<S> WorkerLifecycleObserver<S>
where
    S: StateStore,
{
    pub fn new(pool: TableSyncWorkerPool, state_store: S) -> Self {
        Self { pool, state_store }
    }
}

impl<S> ReactiveFutureCallback<TableId> for WorkerLifecycleObserver<S>
where
    S: StateStore + Send + Sync,
{
    async fn on_complete(&mut self, id: TableId) {
        let mut pool = self.pool.lock().await;
        pool.mark_worker_finished(id, TableSyncWorkerInactiveReason::Completed);
    }

    async fn on_error(&mut self, id: TableId, error: String, is_panic: bool) {
        let mut pool = self.pool.lock().await;

        // We mark the worker as finished with a specific reason.
        let reason = match is_panic {
            true => TableSyncWorkerInactiveReason::Panicked(error),
            false => TableSyncWorkerInactiveReason::Errored(error),
        };
        pool.mark_worker_finished(id, reason);

        // We update the worker state with the new phase and store it in the state store.
        //
        // In case we fail while handling the error, we don't want to return another error, otherwise
        // the `ReactiveFuture` will have to merge both the future error and the state store update
        // error. If we see the need, we might want to merge them into a `Many` error instance.
        if let Err(err) = TableSyncWorkerState::set_and_store(
            &pool,
            &self.state_store,
            id,
            TableReplicationPhase::Skipped,
        )
        .await
        {
            error!("an error occurred while marking table {id} as failed: {err}");
        };
    }
}
