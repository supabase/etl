use crate::v2::concurrency::shutdown::ShutdownRx;
use crate::v2::destination::base::Destination;
use crate::v2::pipeline::PipelineId;
use crate::v2::replication::apply::{ApplyLoopError, ApplyLoopHook, start_apply_loop};
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{SlotError, get_slot_name};
use crate::v2::schema::cache::SchemaCache;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerType, WorkerWaitError};
use crate::v2::workers::pool::TableSyncWorkerPool;
use crate::v2::workers::table_sync::{
    TableSyncWorker, TableSyncWorkerError, TableSyncWorkerState, TableSyncWorkerStateError,
};
use config::shared::PipelineConfig;
use postgres::schema::TableId;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum ApplyWorkerError {
    #[error("An error occurred while interacting with the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error occurred in the apply loop: {0}")]
    ApplyLoop(#[from] ApplyLoopError),

    #[error("A Postgres replication error occurred in the apply loop: {0}")]
    PgReplication(#[from] PgReplicationError),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),
}

#[derive(Debug, Error)]
pub enum ApplyWorkerHookError {
    #[error("An error occurred while interacting with the state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("An error occurred while interacting with the table sync worker state: {0}")]
    TableSyncWorkerState(#[from] TableSyncWorkerStateError),

    #[error("An error occurred while trying to start the table sync worker: {0}")]
    TableSyncWorkerStartedFailed(#[from] TableSyncWorkerError),

    #[error("A Postgres replication error occurred in the apply worker: {0}")]
    PgReplication(#[from] PgReplicationError),
}

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<Result<(), ApplyWorkerError>>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) {}

    async fn wait(mut self) -> Result<(), WorkerWaitError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await??;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    table_sync_worker_permits: Arc<Semaphore>,
}

impl<S, D> ApplyWorker<S, D> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        schema_cache: SchemaCache,
        state_store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        table_sync_worker_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            replication_client,
            pool,
            schema_cache,
            state_store,
            destination,
            shutdown_rx,
            table_sync_worker_permits,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerError;

    async fn start(self) -> Result<ApplyWorkerHandle, Self::Error> {
        info!("Starting apply worker");

        let apply_worker = async move {
            let start_lsn = get_start_lsn(self.pipeline_id, &self.replication_client).await?;

            start_apply_loop(
                self.pipeline_id,
                start_lsn,
                self.config.clone(),
                self.replication_client.clone(),
                self.schema_cache.clone(),
                self.destination.clone(),
                ApplyWorkerHook::new(
                    self.pipeline_id,
                    self.config,
                    self.pool,
                    self.schema_cache,
                    self.state_store,
                    self.destination,
                    self.shutdown_rx.clone(),
                    self.table_sync_worker_permits.clone(),
                ),
                self.shutdown_rx,
            )
            .await?;

            Ok(())
        };

        let handle = tokio::spawn(apply_worker);

        Ok(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }
}

async fn get_start_lsn(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
) -> Result<PgLsn, ApplyWorkerError> {
    let slot_name = get_slot_name(pipeline_id, WorkerType::Apply)?;
    // TODO: validate that we only create the slot when we first start replication which
    // means when all tables are in the Init state. In any other case we should raise an
    // error because that means the apply slot was deleted and creating a fresh slot now
    // could cause inconsistent data to be read.
    // Addendum: this might be hard to detect in all cases. E.g. what if the apply worker
    // starts bunch of table sync wokers and before creating a slot the process crashes?
    // In this case, the apply worker slot is missing not because someone deleted it but
    // because it was never created in the first place. The answer here might be to create
    // the apply worker slot as the first thing, before starting table sync workers.
    let slot = replication_client.get_or_create_slot(&slot_name).await?;
    Ok(slot.get_start_lsn())
}

#[derive(Debug)]
struct ApplyWorkerHook<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    pool: TableSyncWorkerPool,
    schema_cache: SchemaCache,
    state_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    table_sync_worker_permits: Arc<Semaphore>,
}

impl<S, D> ApplyWorkerHook<S, D> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: TableSyncWorkerPool,
        schema_cache: SchemaCache,
        state_store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        table_sync_worker_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            pool,
            schema_cache,
            state_store,
            destination,
            shutdown_rx,
            table_sync_worker_permits,
        }
    }
}

impl<S, D> ApplyWorkerHook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    async fn start_table_sync_worker(&self, table_id: TableId) -> Result<(), ApplyWorkerHookError> {
        let worker = TableSyncWorker::new(
            self.pipeline_id,
            self.config.clone(),
            self.pool.clone(),
            table_id,
            self.schema_cache.clone(),
            self.state_store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
            self.table_sync_worker_permits.clone(),
        );

        let mut pool = self.pool.write().await;
        if let Err(err) = pool.start_worker(worker).await {
            // TODO: check if we want to build a backoff mechanism for retrying the
            //  spawning of new table sync workers.
            error!("Failed to start table sync worker: {}", err);

            return Err(err.into());
        }

        Ok(())
    }

    async fn handle_syncing_table(
        &self,
        table_id: TableId,
        current_lsn: PgLsn,
    ) -> Result<bool, ApplyWorkerHookError> {
        let table_sync_worker_state = {
            let pool = self.pool.read().await;
            pool.get_active_worker_state(table_id)
        };

        let Some(table_sync_worker_state) = table_sync_worker_state else {
            info!("Creating new sync worker for table {}", table_id);
            self.start_table_sync_worker(table_id).await?;

            return Ok(true);
        };

        self.handle_existing_worker(table_id, table_sync_worker_state, current_lsn)
            .await
    }

    async fn handle_existing_worker(
        &self,
        table_id: TableId,
        table_sync_worker_state: TableSyncWorkerState,
        current_lsn: PgLsn,
    ) -> Result<bool, ApplyWorkerHookError> {
        let mut catchup_started = false;
        {
            let mut inner = table_sync_worker_state.get_inner().write().await;
            if inner.replication_phase().as_type() == TableReplicationPhaseType::SyncWait {
                inner
                    .set_phase_with(
                        TableReplicationPhase::Catchup { lsn: current_lsn },
                        self.state_store.clone(),
                    )
                    .await?;

                catchup_started = true;
            }
        }

        if catchup_started {
            let result = table_sync_worker_state
                .wait_for_phase_type(
                    TableReplicationPhaseType::SyncDone,
                    self.shutdown_rx.clone(),
                )
                .await;

            // If we are told to shut down while waiting for a phase change, we will signal this to
            // the caller.
            if result.should_shutdown() {
                return Ok(false);
            }

            info!("Sync completed for table {}", table_id);
        }

        Ok(true)
    }

    async fn active_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, ApplyWorkerHookError> {
        let mut table_replication_states = self.state_store.get_table_replication_states().await?;
        table_replication_states.retain(|_table_id, state| !state.as_type().is_done());

        Ok(table_replication_states)
    }
}

impl<S, D> ApplyLoopHook for ApplyWorkerHook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = ApplyWorkerHookError;

    async fn initialize(&self) -> Result<(), Self::Error> {
        let table_replication_states = self.active_table_replication_states().await?;

        for table_id in table_replication_states.keys() {
            let table_sync_worker_state = {
                let pool = self.pool.read().await;
                pool.get_active_worker_state(*table_id)
            };

            if table_sync_worker_state.is_none() {
                if let Err(err) = self.start_table_sync_worker(*table_id).await {
                    error!(
                        "Error starting table sync worker for table {}: {}",
                        table_id, err
                    );
                }
            }
        }

        Ok(())
    }

    async fn process_syncing_tables(&self, current_lsn: PgLsn) -> Result<bool, Self::Error> {
        let table_replication_states = self.active_table_replication_states().await?;
        info!(
            "Processing syncing tables for apply worker with LSN {}",
            current_lsn
        );

        for (table_id, table_replication_phase) in table_replication_states {
            // We read the state store state first, if we don't find `SyncDone` we will attempt to
            // read the shared state which can contain also non-persisted states.
            match table_replication_phase {
                // It is important that the `if current_lsn >= lsn` is inside the match arm rather than
                // as a guard on it because while we do want to mark any tables in sync done state as
                // ready, we do not want to call the `handle_syncing_table` method on such tables because
                // it will unnecessarily lauch a table sync worker for it which will anyway exit because
                // table sync workers do not process tables in either sync done or ready states. Preventing
                // launch of such workers has become even more important after we started calling
                // `process_syncing_tables` at regular intervals, because now such spurious launches
                // have become extremely common, which is just wasteful.
                TableReplicationPhase::SyncDone { lsn } => {
                    if current_lsn >= lsn {
                        info!(
                            "Table {} is ready, its events are now processed by the main apply worker",
                            table_id
                        );
                        self.state_store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    }
                }
                _ => {
                    if let Err(err) = self.handle_syncing_table(table_id, current_lsn).await {
                        error!("Error handling syncing table {}: {}", table_id, err);
                    }
                }
            }
        }

        Ok(true)
    }

    async fn skip_table(&self, table_id: TableId) -> Result<bool, Self::Error> {
        let table_sync_worker_state = {
            let pool = self.pool.read().await;
            pool.get_active_worker_state(table_id)
        };

        // In case we have the state in memory, we will also update that.
        if let Some(table_sync_worker_state) = table_sync_worker_state {
            let mut inner = table_sync_worker_state.get_inner().write().await;
            inner.set_phase(TableReplicationPhase::Skipped);
        }

        // We store the new skipped state in the state store, since we want to still skip a table in
        // case of pipeline restarts.
        self.state_store
            .update_table_replication_state(table_id, TableReplicationPhase::Skipped)
            .await?;

        Ok(true)
    }

    async fn should_apply_changes(
        &self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> Result<bool, Self::Error> {
        let pool = self.pool.read().await;

        // We try to load the state first from memory, if we don't find it, we try to load from the
        // state store.
        let replication_phase = match pool.get_active_worker_state(table_id) {
            Some(state) => {
                let inner = state.get_inner().read().await;
                inner.replication_phase()
            }
            None => {
                let Some(state) = self
                    .state_store
                    .get_table_replication_state(table_id)
                    .await?
                else {
                    // If we don't even find the state for this table, we skip the event entirely.
                    return Ok(false);
                };

                state
            }
        };

        let should_apply_changes = match replication_phase {
            TableReplicationPhase::Ready => true,
            TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
            _ => false,
        };

        Ok(should_apply_changes)
    }

    fn worker_type(&self) -> WorkerType {
        WorkerType::Apply
    }
}
