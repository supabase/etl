use config::shared::PipelineConfig;
use postgres::schema::TableId;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{AcquireError, Notify, RwLock, RwLockReadGuard, Semaphore};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, debug, error, info, warn};

use crate::concurrency::future::ReactiveFuture;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::destination::base::Destination;
use crate::pipeline::PipelineId;
use crate::replication::apply::{ApplyLoopError, ApplyLoopHook, start_apply_loop};
use crate::replication::client::{PgReplicationClient, PgReplicationError};
use crate::replication::slot::get_slot_name;
use crate::replication::table_sync::{TableSyncError, TableSyncResult, start_table_sync};
use crate::schema::cache::SchemaCache;
use crate::state::store::base::{StateStore, StateStoreError};
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::workers::base::{Worker, WorkerHandle, WorkerType, WorkerWaitError};
use crate::workers::pool::TableSyncWorkerPool;

/// Maximum time to wait for a phase change before trying again.
const PHASE_CHANGE_REFRESH_FREQUENCY: Duration = Duration::from_millis(100);

/// Maximum time to wait for the slot deletion call to complete.
///
/// The reason for setting a timer on deletion is that we wait for the slot to become unused before
/// deleting it. We want to avoid an infinite wait in case the slot fails to be released,
/// as this could result in a connection being held indefinitely, potentially stalling the processing
/// of new tables.
const MAX_DELETE_SLOT_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum TableSyncWorkerError {
    #[error("An error occurred while syncing a table: {0}")]
    TableSync(#[from] TableSyncError),

    #[error("The replication state is missing for table {0}")]
    ReplicationStateMissing(TableId),

    #[error("An error occurred while interacting with the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error occurred in the apply loop: {0}")]
    ApplyLoop(#[from] ApplyLoopError),

    #[error("Failed to acquire a permit to run a table sync worker")]
    PermitAcquire(#[from] AcquireError),

    #[error("A Postgres replication error occurred in the table sync worker: {0}")]
    PgReplication(#[from] PgReplicationError),
}

#[derive(Debug, Error)]
pub enum TableSyncWorkerHookError {
    #[error("An error occurred while updating the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),

    #[error("A Postgres replication error occurred in the table sync worker: {0}")]
    PgReplication(#[from] PgReplicationError),
}

#[derive(Debug, Error)]
pub enum TableSyncWorkerStateError {
    #[error("An error occurred while interacting with the state store: {0}")]
    StateStore(#[from] StateStoreError),
}

#[derive(Debug)]
pub struct TableSyncWorkerStateInner {
    table_id: TableId,
    table_replication_phase: TableReplicationPhase,
    phase_change: Arc<Notify>,
}

impl TableSyncWorkerStateInner {
    pub fn set_phase(&mut self, phase: TableReplicationPhase) {
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

    // TODO: investigate whether we want to just keep the syncwait and catchup special states in
    //  the table sync worker state for the sake of simplicity.
    pub async fn set_phase_with<S: StateStore>(
        &mut self,
        phase: TableReplicationPhase,
        state_store: S,
    ) -> Result<(), TableSyncWorkerStateError> {
        self.set_phase(phase);

        // If we should store this phase change, we want to do it via the supplied state store.
        if phase.as_type().should_store() {
            info!("storing phase change '{:?}' for table", phase);

            state_store
                .update_table_replication_state(self.table_id, phase)
                .await?;
        }

        Ok(())
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn replication_phase(&self) -> TableReplicationPhase {
        self.table_replication_phase
    }
}

// TODO: we would like to put the state of tables in a shared state structure which can be referenced
//  by table sync workers.
#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<RwLock<TableSyncWorkerStateInner>>,
}

impl TableSyncWorkerState {
    fn new(table_id: TableId, table_replication_phase: TableReplicationPhase) -> Self {
        let inner = TableSyncWorkerStateInner {
            table_id,
            table_replication_phase,
            phase_change: Arc::new(Notify::new()),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn get_inner(&self) -> &RwLock<TableSyncWorkerStateInner> {
        &self.inner
    }

    async fn wait(
        &self,
        phase_type: TableReplicationPhaseType,
    ) -> Option<RwLockReadGuard<'_, TableSyncWorkerStateInner>> {
        // We grab hold of the phase change notify in case we don't immediately have the state
        // that we want.
        let phase_change = {
            let inner = self.inner.read().await;
            if inner.table_replication_phase.as_type() == phase_type {
                info!(
                    "phase type '{:?}' was already set, no need to wait",
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
        let inner = self.inner.read().await;
        if inner.table_replication_phase.as_type() == phase_type {
            info!(
                "phase type '{:?}' was reached for table {:?}",
                phase_type, inner.table_id
            );
            return Some(inner);
        }

        None
    }

    pub async fn wait_for_phase_type(
        &self,
        phase_type: TableReplicationPhaseType,
        mut shutdown_rx: ShutdownRx,
    ) -> ShutdownResult<RwLockReadGuard<'_, TableSyncWorkerStateInner>, ()> {
        let table_id = {
            let inner = self.inner.read().await;
            inner.table_id
        };
        info!(
            "waiting for phase type '{:?}' for table {:?}",
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
}

#[derive(Debug)]
pub struct TableSyncWorkerHandle {
    state: TableSyncWorkerState,
    handle: Option<JoinHandle<Result<(), TableSyncWorkerError>>>,
}

impl WorkerHandle<TableSyncWorkerState> for TableSyncWorkerHandle {
    fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    async fn wait(mut self) -> Result<(), WorkerWaitError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await??;

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
            run_permit,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl<S, D> Worker<TableSyncWorkerHandle, TableSyncWorkerState> for TableSyncWorker<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = TableSyncWorkerError;

    async fn start(mut self) -> Result<TableSyncWorkerHandle, Self::Error> {
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

            return Err(TableSyncWorkerError::ReplicationStateMissing(self.table_id));
        };

        info!(
            "loaded table sync worker state for table {}: {:?}",
            self.table_id, table_replication_phase
        );

        let state = TableSyncWorkerState::new(self.table_id, table_replication_phase);

        let state_clone = state.clone();
        let table_sync_worker_span =
            tracing::info_span!("table_sync_worker", table_id = self.table_id);
        let table_sync_worker = async move {
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
                state_clone.clone(),
                self.schema_cache.clone(),
                self.state_store.clone(),
                self.destination.clone(),
                self.shutdown_rx.clone(),
            )
            .await;

            let start_lsn = match result {
                Ok(TableSyncResult::SyncStopped | TableSyncResult::SyncNotRequired) => {
                    return Ok(());
                }
                Ok(TableSyncResult::SyncCompleted { start_lsn }) => start_lsn,
                Err(err) => {
                    error!("table sync failed for table {}: {}", self.table_id, err);

                    return Err(err.into());
                }
            };

            start_apply_loop(
                self.pipeline_id,
                start_lsn,
                self.config.clone(),
                replication_client.clone(),
                self.schema_cache,
                self.destination,
                TableSyncWorkerHook::new(self.table_id, state_clone, self.state_store),
                self.shutdown_rx,
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
            let slot_name = get_slot_name(self.pipeline_id, worker_type).unwrap();
            let result = tokio::time::timeout(
                MAX_DELETE_SLOT_WAIT,
                replication_client.delete_slot(&slot_name),
            )
            .await;
            match result {
                Ok(Ok(())) => {
                    info!(
                        "successfully deleted replication slot '{}' for table {}",
                        slot_name, self.table_id
                    );
                }
                Ok(Err(err)) => {
                    warn!(
                        "failed to delete the replication slot {slot_name} of the table sync worker {}: {err}",
                        self.table_id
                    )
                }
                Err(_) => {
                    warn!(
                        "failed to delete the replication slot {slot_name} of the table sync worker {} due to timeout",
                        self.table_id
                    );
                }
            }

            // This explicit drop is not strictly necessary but is added to make it extra clear
            // that the scope of the run permit is needed upto here to avoid multiple parallel
            // connections.
            drop(permit);

            info!("table sync worker {} completed successfully", self.table_id);

            Ok(())
        };

        // We spawn the table sync worker with a safe future, so that we can have controlled teardown
        // on completion or error.
        let fut = ReactiveFuture::new(table_sync_worker, self.table_id, self.pool.workers())
            .instrument(table_sync_worker_span);
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

impl<S> ApplyLoopHook for TableSyncWorkerHook<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    type Error = TableSyncWorkerHookError;

    async fn initialize(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// This function compares `current_lsn` against the table's catch up lsn
    /// and if it is greater than or equal to the catch up `lsn`:
    ///
    /// * Marks the table as sync done in state store if `update_state` is true.
    /// * Returns Ok(false) to indicate to the callers that this table has been marked sync done.
    ///
    /// In all other cases it returns Ok(true)
    async fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> Result<bool, Self::Error> {
        info!(
            "processing syncing tables for table sync worker with lsn {}",
            current_lsn
        );

        let mut inner = self.table_sync_worker_state.get_inner().write().await;

        // If we caught up with the lsn, we mark this table as `SyncDone` and stop the worker.
        if let TableReplicationPhase::Catchup { lsn } = inner.replication_phase() {
            // TODO: there is currently a correctness bug in which we mark this table as sync done
            //  before we actually acknowledged writes to dis  (since they are acknowledged after the
            //  batch is processed). This is fine for apply workers since progress is tracked after
            //  the batch is written, but for table sync workers this causes the problem where we might
            //  crash after marking ourselves as `SynCdONE` and we end up not actually sending that data
            //  and the apply worker still assumes that data is there so it will be lost forever.
            //  Postgres doesn't have this problem since they process and acknowledge each commit message
            //  individually.
            if current_lsn >= lsn {
                if update_state {
                    inner
                        .set_phase_with(
                            TableReplicationPhase::SyncDone { lsn: current_lsn },
                            self.state_store.clone(),
                        )
                        .await?;

                    info!(
                        "table sync worker for table {} has caught up with the apply worker, shutting down",
                        self.table_id
                    );
                }

                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn skip_table(&self, table_id: TableId) -> Result<bool, Self::Error> {
        if self.table_id != table_id {
            return Ok(true);
        }

        let mut inner = self.table_sync_worker_state.get_inner().write().await;
        inner
            .set_phase_with(TableReplicationPhase::Skipped, self.state_store.clone())
            .await?;

        Ok(false)
    }

    async fn should_apply_changes(
        &self,
        table_id: TableId,
        _remote_final_lsn: PgLsn,
    ) -> Result<bool, Self::Error> {
        let inner = self.table_sync_worker_state.get_inner().write().await;
        let is_skipped = matches!(
            inner.table_replication_phase.as_type(),
            TableReplicationPhaseType::Skipped
        );

        let should_apply_changes = !is_skipped && self.table_id == table_id;

        Ok(should_apply_changes)
    }

    fn worker_type(&self) -> WorkerType {
        WorkerType::TableSync {
            table_id: self.table_id,
        }
    }
}
