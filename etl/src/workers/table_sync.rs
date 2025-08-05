use chrono::Utc;
use config::shared::PipelineConfig;
use postgres::schema::TableId;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, debug, error, info, warn};

use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::concurrency::signal::SignalTx;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::replication::apply::{ApplyLoopHook, start_apply_loop};
use crate::replication::client::PgReplicationClient;
use crate::replication::slot::get_slot_name;
use crate::replication::table_sync::{TableSyncResult, start_table_sync};
use crate::schema::SchemaCache;
use crate::state::store::StateStore;
use crate::state::table::{
    RetryPolicy, TableReplicationError, TableReplicationPhase, TableReplicationPhaseType,
};
use crate::types::PipelineId;
use crate::workers::base::{Worker, WorkerHandle, WorkerType};
use crate::workers::pool::{TableSyncWorkerPool, TableSyncWorkerPoolInner};
use crate::{bail, etl_error};

/// Maximum time to wait for a phase change before trying again.
const PHASE_CHANGE_REFRESH_FREQUENCY: Duration = Duration::from_millis(100);

/// Maximum time to wait for the slot deletion call to complete.
///
/// The reason for setting a timer on deletion is that we wait for the slot to become unused before
/// deleting it. We want to avoid an infinite wait in case the slot fails to be released,
/// as this could result in a connection being held indefinitely, potentially stalling the processing
/// of new tables.
const MAX_DELETE_SLOT_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct TableSyncWorkerStateInner {
    table_id: TableId,
    table_replication_phase: TableReplicationPhase,
    phase_change: Arc<Notify>,
}

impl TableSyncWorkerStateInner {
    pub fn set(&mut self, phase: TableReplicationPhase) {
        info!(
            "table phase changing from '{:?}' to '{:?}'",
            self.table_replication_phase, phase
        );

        self.table_replication_phase = phase;
        // We want to notify all waiters that there was a phase change.
        //
        // Note that this notify will not wake up waiters that will be coming in the future since
        // no permit is stored, only active listeners will be notified.
        self.phase_change.notify_waiters();
    }

    pub async fn set_and_store<S: StateStore>(
        &mut self,
        phase: TableReplicationPhase,
        state_store: &S,
    ) -> EtlResult<()> {
        self.set(phase.clone());

        // If we should store this phase change, we want to do it via the supplied state store.
        if phase.as_type().should_store() {
            info!(
                "storing phase change '{:?}' for table {}",
                phase, self.table_id
            );

            state_store
                .update_table_replication_state(self.table_id, phase)
                .await?;
        }

        Ok(())
    }

    pub async fn rollback<S: StateStore>(&mut self, state_store: &S) -> EtlResult<()> {
        // We rollback the state in the store and then also set the rolled back state in memory.
        let previous_phase = state_store
            .rollback_table_replication_state(self.table_id)
            .await?;
        self.set(previous_phase);

        Ok(())
    }

    pub fn replication_phase(&self) -> TableReplicationPhase {
        self.table_replication_phase.clone()
    }
}

#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<Mutex<TableSyncWorkerStateInner>>,
}

impl TableSyncWorkerState {
    fn new(table_id: TableId, table_replication_phase: TableReplicationPhase) -> Self {
        let inner = TableSyncWorkerStateInner {
            table_id,
            table_replication_phase,
            phase_change: Arc::new(Notify::new()),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn set_and_store<P, S>(
        pool: &P,
        state_store: &S,
        table_id: TableId,
        table_replication_phase: TableReplicationPhase,
    ) -> EtlResult<()>
    where
        P: Deref<Target = TableSyncWorkerPoolInner>,
        S: StateStore,
    {
        let table_sync_worker_state = pool.get_active_worker_state(table_id);

        // In case we have the state in memory, we will atomically update the memory and state store
        // states. Otherwise, we just update the state store.
        if let Some(table_sync_worker_state) = table_sync_worker_state {
            let mut inner = table_sync_worker_state.lock().await;
            inner
                .set_and_store(table_replication_phase, state_store)
                .await?;
        } else {
            state_store
                .update_table_replication_state(table_id, table_replication_phase)
                .await?;
        }

        Ok(())
    }

    pub async fn wait_for_phase_type(
        &self,
        phase_type: TableReplicationPhaseType,
        mut shutdown_rx: ShutdownRx,
    ) -> ShutdownResult<MutexGuard<'_, TableSyncWorkerStateInner>, ()> {
        let table_id = {
            let inner = self.inner.lock().await;
            inner.table_id
        };
        info!(
            "waiting for table replication phase '{:?}' for table {:?}",
            phase_type, table_id
        );

        loop {
            tokio::select! {
                biased;

                // Shutdown signal received, exit loop.
                _ = shutdown_rx.changed() => {
                    info!("shutdown signal received, cancelling the wait for phase type {:?}", phase_type);

                    return ShutdownResult::Shutdown(());
                }

                // Try to wait for the phase type.
                acquired = self.wait(phase_type) => {
                    if let Some(acquired) = acquired {
                        return ShutdownResult::Ok(acquired);
                    }
                }
            }
        }
    }

    async fn wait(
        &self,
        phase_type: TableReplicationPhaseType,
    ) -> Option<MutexGuard<'_, TableSyncWorkerStateInner>> {
        // We grab hold of the phase change notify in case we don't immediately have the state
        // that we want.
        let phase_change = {
            let inner = self.inner.lock().await;
            if inner.table_replication_phase.as_type() == phase_type {
                info!(
                    "table replication phase '{:?}' was already set, no need to wait",
                    phase_type
                );
                return Some(inner);
            }

            inner.phase_change.clone()
        };

        // We wait for a state change within a timeout. This is done since it might be that a
        // notification is missed and in that case we want to avoid blocking indefinitely.
        let _ = tokio::time::timeout(PHASE_CHANGE_REFRESH_FREQUENCY, phase_change.notified()).await;

        // We read the state and return the lock to the state.
        let inner = self.inner.lock().await;
        if inner.table_replication_phase.as_type() == phase_type {
            info!(
                "table replication phase '{:?}' was reached for table {:?}",
                phase_type, inner.table_id
            );
            return Some(inner);
        }

        None
    }
}

impl Deref for TableSyncWorkerState {
    type Target = Mutex<TableSyncWorkerStateInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct TableSyncWorkerHandle {
    state: TableSyncWorkerState,
    handle: Option<JoinHandle<EtlResult<()>>>,
}

impl WorkerHandle<TableSyncWorkerState> for TableSyncWorkerHandle {
    fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    async fn wait(mut self) -> EtlResult<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await.map_err(|err| {
            etl_error!(
                ErrorKind::TableSyncWorkerPanic,
                "A panic occurred in the table sync worker",
                err
            )
        })??;

        Ok(())
    }
}

#[derive(Debug)]
pub struct TableSyncWorker<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    pool: TableSyncWorkerPool,
    table_id: TableId,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    force_syncing_tables_tx: SignalTx,
    run_permit: Arc<Semaphore>,
}

impl<S, D> TableSyncWorker<S, D> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: TableSyncWorkerPool,
        table_id: TableId,
        schema_cache: SchemaCache,
        state_store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        force_syncing_tables_tx: SignalTx,
        run_permit: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            pool,
            table_id,
            schema_cache,
            state_store,
            destination,
            shutdown_rx,
            force_syncing_tables_tx,
            run_permit,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl<S, D> TableSyncWorker<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    async fn guarded_run_table_sync_worker(self, state: TableSyncWorkerState) -> EtlResult<()> {
        let table_id = self.table_id;
        let pool = self.pool.clone();
        let state_store = self.state_store.clone();
        let config = self.config.clone();

        // Clone all the fields we need for retries
        let pipeline_id = self.pipeline_id;
        let schema_cache = self.schema_cache.clone();
        let destination = self.destination.clone();
        let shutdown_rx = self.shutdown_rx.clone();
        let force_syncing_tables_tx = self.force_syncing_tables_tx.clone();
        let run_permit = self.run_permit.clone();

        loop {
            // Recreate the worker for each attempt
            let worker = TableSyncWorker {
                pipeline_id,
                config: config.clone(),
                pool: pool.clone(),
                table_id,
                schema_cache: schema_cache.clone(),
                state_store: state_store.clone(),
                destination: destination.clone(),
                shutdown_rx: shutdown_rx.clone(),
                force_syncing_tables_tx: force_syncing_tables_tx.clone(),
                run_permit: run_permit.clone(),
            };

            let result = worker.run_table_sync_worker(state.clone()).await;

            match result {
                Ok(_) => {
                    // Worker completed successfully, mark as finished
                    let mut pool = pool.lock().await;
                    pool.mark_worker_finished(table_id);

                    return Ok(());
                }
                Err(err) => {
                    error!("table sync worker failed for table {}: {}", table_id, err);

                    // Convert error to table replication error to determine retry policy
                    let table_error =
                        TableReplicationError::from_etl_error(&config, table_id, &err);
                    let retry_policy = table_error.retry_policy().clone();

                    // We lock both the pool and the table sync worker state to be consistent
                    let mut pool_guard = pool.lock().await;
                    let mut state_guard = state.lock().await;

                    // Update the state and store with the error
                    if let Err(err) = state_guard
                        .set_and_store(table_error.into(), &state_store)
                        .await
                    {
                        error!(
                            "failed to update table sync worker state for table {}: {}",
                            table_id, err
                        );

                        pool_guard.mark_worker_finished(table_id);

                        return Err(err);
                    };

                    match retry_policy {
                        RetryPolicy::TimedRetry { next_retry } => {
                            let now = Utc::now();
                            if now < next_retry {
                                let sleep_duration = (next_retry - now)
                                    .to_std()
                                    .unwrap_or(Duration::from_secs(0));

                                info!(
                                    "retrying table sync worker for table {} in {:?}",
                                    table_id, sleep_duration
                                );

                                // We drop the lock on the pool while waiting. We do not do the same
                                // for the state guard since we want to hold the lock for that state
                                // since when we are waiting to retry, nobody should be allowed to
                                // modify it.
                                drop(pool_guard);

                                tokio::time::sleep(sleep_duration).await;
                            } else {
                                info!(
                                    "retrying table sync worker for table {} immediately",
                                    table_id
                                );
                            }

                            // Before rolling back, we acquire the pool lock again for consistency
                            let mut pool_guard = pool.lock().await;

                            // After sleeping, we rollback to the previous state and retry
                            if let Err(err) = state_guard.rollback(&state_store).await {
                                error!(
                                    "failed to rollback table sync worker state for table {}: {}",
                                    table_id, err
                                );

                                pool_guard.mark_worker_finished(table_id);

                                return Err(err);
                            };

                            continue;
                        }
                        RetryPolicy::NoRetry | RetryPolicy::ManualRetry => {
                            pool_guard.mark_worker_finished(table_id);

                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    async fn run_table_sync_worker(mut self, state: TableSyncWorkerState) -> EtlResult<()> {
        debug!(
            "waiting to acquire a running permit for table sync worker for table {}",
            self.table_id
        );

        // We acquire a permit to run the table sync worker. This helps us limit the number
        // of table sync workers running in parallel which in turn helps limit the max
        // number of cocurrent connections to the source database.
        let permit = tokio::select! {
            _ = self.shutdown_rx.changed() => {
                info!("shutting down table sync worker for table {} while waiting for a run permit", self.table_id);
                return Ok(());
            }

            permit = self.run_permit.acquire() => {
                permit
            }
        };

        info!(
            "acquired running permit for table sync worker for table {}",
            self.table_id
        );

        // We create a new replication connection specifically for this table sync worker.
        //
        // Note that this connection must be tied to the lifetime of this worker, otherwise
        // there will be problems when cleaning up the replication slot.
        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;

        let result = start_table_sync(
            self.pipeline_id,
            self.config.clone(),
            replication_client.clone(),
            self.table_id,
            state.clone(),
            self.schema_cache.clone(),
            self.state_store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
            self.force_syncing_tables_tx,
        )
        .await;

        let start_lsn = match result {
            Ok(TableSyncResult::SyncCompleted { start_lsn }) => start_lsn,
            Ok(TableSyncResult::SyncStopped | TableSyncResult::SyncNotRequired) => {
                return Ok(());
            }
            Err(err) => {
                error!("table sync failed for table {}: {}", self.table_id, err);
                return Err(err);
            }
        };

        start_apply_loop(
            self.pipeline_id,
            start_lsn,
            self.config,
            replication_client.clone(),
            self.schema_cache,
            self.destination,
            TableSyncWorkerHook::new(self.table_id, state, self.state_store),
            self.shutdown_rx,
            None,
        )
        .await?;

        // We delete the replication slot used by this table sync worker.
        //
        // Note that if the deletion fails, the slot will remain in the database and will not be
        // removed later, so manual intervention will be required. The reason for not implementing
        // an automatic cleanup mechanism is that it would introduce performance overhead,
        // and we expect this call to fail only rarely.
        let worker_type = WorkerType::TableSync {
            table_id: self.table_id,
        };
        let slot_name = get_slot_name(self.pipeline_id, worker_type)?;
        let result = tokio::time::timeout(
            MAX_DELETE_SLOT_WAIT,
            replication_client.delete_slot(&slot_name),
        )
        .await;
        if result.is_err() {
            warn!(
                "failed to delete the replication slot {slot_name} of the table sync worker {} due to timeout",
                self.table_id
            );
        }

        // This explicit drop is not strictly necessary but is added to make it extra clear
        // that the scope of the run permit is needed upto here to avoid multiple parallel
        // connections.
        drop(permit);

        info!("table sync worker {} completed successfully", self.table_id);

        Ok(())
    }
}

impl<S, D> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = EtlError;

    async fn start(self) -> EtlResult<TableSyncWorkerHandle> {
        info!("starting table sync worker for table {}", self.table_id);

        let Some(table_replication_phase) = self
            .state_store
            .get_table_replication_state(self.table_id)
            .await?
        else {
            error!(
                "no replication state found for table {}, cannot start sync worker",
                self.table_id
            );

            bail!(
                ErrorKind::InvalidState,
                "Replication state missing",
                format!(
                    "The replication state is missing for table {}",
                    self.table_id
                )
            );
        };

        info!(
            "loaded table sync worker state for table {}: {:?}",
            self.table_id, table_replication_phase
        );

        let state = TableSyncWorkerState::new(self.table_id, table_replication_phase);

        let table_sync_worker_span = tracing::info_span!(
            "table_sync_worker",
            pipeline_id = self.pipeline_id,
            publication_name = self.config.publication_name,
            table_id = %self.table_id,
        );
        let table_sync_worker = self.guarded_run_table_sync_worker(state.clone());

        let fut = table_sync_worker.instrument(table_sync_worker_span);
        let handle = tokio::spawn(fut);

        Ok(TableSyncWorkerHandle {
            state,
            handle: Some(handle),
        })
    }
}

#[derive(Debug)]
struct TableSyncWorkerHook<S> {
    table_id: TableId,
    table_sync_worker_state: TableSyncWorkerState,
    state_store: S,
}

impl<S> TableSyncWorkerHook<S> {
    fn new(
        table_id: TableId,
        table_sync_worker_state: TableSyncWorkerState,
        state_store: S,
    ) -> Self {
        Self {
            table_id,
            table_sync_worker_state,
            state_store,
        }
    }
}

impl<S> TableSyncWorkerHook<S>
where
    S: StateStore + Clone,
{
    /// Tries to advance the [`TableReplicationPhase`] of this table based on the current lsn.
    ///
    /// Returns `Ok(false)` when the worker is done with its work, signaling the caller that the apply
    /// loop should be stopped.
    async fn try_advance_phase(&self, current_lsn: PgLsn, update_state: bool) -> EtlResult<bool> {
        let mut inner = self.table_sync_worker_state.lock().await;

        // If we caught up with the lsn, we mark this table as `SyncDone` and stop the worker.
        if let TableReplicationPhase::Catchup { lsn } = inner.replication_phase() {
            if current_lsn >= lsn {
                // If we are told to update the state, we mark the phase as actually changes. We do
                // this because we want to update the actual state only when we are sure that the
                // progress has been persisted to the destination. When `update_state` is `false` this
                // function is used as a lookahead, to determine whether the worker should be stopped.
                if update_state {
                    inner
                        .set_and_store(
                            TableReplicationPhase::SyncDone { lsn: current_lsn },
                            &self.state_store,
                        )
                        .await?;

                    info!(
                        "table sync worker for table {} is in sync with the apply worker, the worker will terminate",
                        self.table_id
                    );
                }

                return Ok(false);
            }
        }

        Ok(true)
    }
}

impl<S> ApplyLoopHook for TableSyncWorkerHook<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    async fn before_loop(&self, start_lsn: PgLsn) -> EtlResult<bool> {
        info!("checking if the table sync worker is already caught up with the apply worker");

        self.try_advance_phase(start_lsn, true).await
    }

    /// This function compares `current_lsn` against the table's catch up lsn
    /// and if it is greater than or equal to the `Catchup` `lsn`:
    ///
    /// * Marks the table as sync done in state store if `update_state` is true.
    /// * Returns Ok(false) to indicate to the callers that this table has been marked sync done,
    ///   meaning that the apply loop should not continue.
    ///
    /// In all other cases it returns Ok(true)
    async fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> EtlResult<bool> {
        info!(
            "processing syncing tables for table sync worker with lsn {}",
            current_lsn
        );

        self.try_advance_phase(current_lsn, update_state).await
    }

    async fn mark_table_errored(
        &self,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<bool> {
        if self.table_id != table_replication_error.table_id() {
            // If the table is not the same as the one handled by this table sync worker, marking
            // the table will be a noop, and we want to continue the loop.
            return Ok(true);
        }

        // Since we already have access to the table sync worker state, we can avoid going through
        // the pool, and we just modify the state here and also update the state store.
        let mut inner = self.table_sync_worker_state.lock().await;
        inner
            .set_and_store(table_replication_error.into(), &self.state_store)
            .await?;

        // If we mark a table as errored in a table sync worker, the worker will stop here, thus we
        // signal the loop to stop.
        Ok(false)
    }

    async fn should_apply_changes(
        &self,
        table_id: TableId,
        _remote_final_lsn: PgLsn,
    ) -> EtlResult<bool> {
        let inner = self.table_sync_worker_state.lock().await;
        let is_errored = matches!(
            inner.table_replication_phase.as_type(),
            TableReplicationPhaseType::Errored
        );

        let should_apply_changes = !is_errored && self.table_id == table_id;

        Ok(should_apply_changes)
    }

    fn worker_type(&self) -> WorkerType {
        WorkerType::TableSync {
            table_id: self.table_id,
        }
    }
}
