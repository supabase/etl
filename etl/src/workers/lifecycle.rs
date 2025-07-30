use config::shared::PipelineConfig;
use postgres::schema::TableId;
use std::ops::Deref;
use std::sync::Arc;
use tracing::error;

use crate::concurrency::future::ReactiveFutureCallback;
use crate::error::EtlError;
use crate::state::retries::RetriesOrchestrator;
use crate::state::store::base::StateStore;
use crate::state::table::{RetryPolicy, TableReplicationError};
use crate::workers::pool::{TableSyncWorkerPool, TableSyncWorkerPoolInner};
use crate::workers::table_sync::TableSyncWorkerState;

/// Observes and manages the lifecycle of table sync workers.
///
/// Handles completion, errors, and panics from table sync workers by updating
/// worker pool state and persisting error information through the state store.
/// Integrates with the retries orchestrator to determine appropriate retry policies.
#[derive(Debug, Clone)]
pub struct WorkerLifecycleObserver<S> {
    /// Pipeline configuration used for error processing.
    config: Arc<PipelineConfig>,
    /// Pool of table synchronization workers being observed.
    pool: TableSyncWorkerPool,
    /// Orchestrator for handling retry logic and policies.
    retries_orchestrator: RetriesOrchestrator<S>,
    /// State store for persisting worker and table states.
    state_store: S,
}

impl<S> WorkerLifecycleObserver<S>
where
    S: StateStore + Clone + Send + 'static,
{
    /// Creates a new worker lifecycle observer.
    ///
    /// Initializes the observer with the necessary components to monitor and
    /// manage table sync worker lifecycles, including error handling and state persistence.
    pub fn new(
        config: Arc<PipelineConfig>,
        pool: TableSyncWorkerPool,
        retries_orchestrator: RetriesOrchestrator<S>,
        state_store: S,
    ) -> Self {
        Self {
            config,
            pool,
            retries_orchestrator,
            state_store,
        }
    }

    /// Marks a table as errored and persists the error state.
    ///
    /// Processes the error through the retries orchestrator to determine retry policy,
    /// then updates and stores the worker state. Logs any failures that occur during
    /// state persistence to avoid propagating errors to the reactive future system.
    async fn mark_table_errored<P>(
        &self,
        pool: &P,
        table_id: TableId,
        table_replication_error: TableReplicationError,
    ) where
        P: Deref<Target = TableSyncWorkerPoolInner>,
    {
        // We update the worker state with the new phase and store it in the state store.
        //
        // In case we fail while handling the error, we don't want to return another error, otherwise
        // the `ReactiveFuture` will have to merge both the future error and the state store update
        // error. If we see the need, we might want to merge them into a `Many` error instance.
        let table_replication_error = table_replication_error
            .process(&self.retries_orchestrator)
            .await;
        if let Err(err) = TableSyncWorkerState::set_and_store(
            pool,
            &self.state_store,
            table_id,
            table_replication_error.into(),
        )
        .await
        {
            error!("an error occurred while marking table {table_id} as failed: {err}");
        };
    }
}

impl<S> ReactiveFutureCallback<TableId, EtlError> for WorkerLifecycleObserver<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    async fn on_complete(&mut self, id: TableId) {
        let mut pool = self.pool.lock().await;
        pool.mark_worker_finished(id);
    }

    async fn on_error(&mut self, id: TableId, error: EtlError) {
        let mut pool = self.pool.lock().await;

        // We first mark the worker as finished in the pool, in this way we know that a failed worker
        // can never have an active worker state.
        pool.mark_worker_finished(id);

        // We mark the table as errored properly persisting the error state.
        let table_error = TableReplicationError::from_etl_error(&self.config, id, error);
        self.mark_table_errored(&pool, id, table_error).await;
    }

    async fn on_panic(&mut self, id: TableId, panic: String) {
        let mut pool = self.pool.lock().await;

        // We first mark the worker as finished in the pool, in this way we know that a failed worker
        // can never have an active worker state.
        pool.mark_worker_finished(id);

        // We mark the table as errored properly persisting the error state.
        let error = TableReplicationError::without_solution(
            id,
            format!("The table sync worker has experienced a panic: {panic}"),
            RetryPolicy::NoRetry,
        );
        self.mark_table_errored(&pool, id, error).await;
    }
}
