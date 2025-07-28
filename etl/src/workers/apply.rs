use crate::error::{ErrorKind, EtlError, EtlResult};
use config::shared::PipelineConfig;
use postgres::schema::TableId;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, debug, error, info};

use crate::concurrency::shutdown::ShutdownRx;
use crate::concurrency::signal::{SignalTx, create_signal};
use crate::destination::base::Destination;
use crate::etl_error;
use crate::pipeline::PipelineId;
use crate::replication::apply::{ApplyLoopHook, start_apply_loop};
use crate::replication::client::PgReplicationClient;
use crate::replication::common::get_table_replication_states;
use crate::replication::slot::get_slot_name;
use crate::schema::cache::SchemaCache;
use crate::state::store::base::StateStore;
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::workers::base::{Worker, WorkerHandle, WorkerType};
use crate::workers::pool::TableSyncWorkerPool;
use crate::workers::table_sync::{TableSyncWorker, TableSyncWorkerState};

#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<EtlResult<()>>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    fn state(&self) {}

    async fn wait(mut self) -> EtlResult<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await.map_err(|err| {
            etl_error!(
                ErrorKind::ApplyWorkerPanic,
                "A panic occurred in the apply worker",
                err
            )
        })??;

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
    type Error = EtlError;

    async fn start(self) -> EtlResult<ApplyWorkerHandle> {
        info!("starting apply worker");

        let apply_worker_span = tracing::info_span!(
            "apply_worker",
            pipeline_id = self.pipeline_id,
            publication_name = self.config.publication_name
        );
        let apply_worker = async move {
            let start_lsn = get_start_lsn(self.pipeline_id, &self.replication_client).await?;

            // We create the signal used to notify the apply worker that it should force syncing tables.
            let (force_syncing_tables_tx, force_syncing_tables_rx) = create_signal();

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
                    force_syncing_tables_tx,
                    self.table_sync_worker_permits.clone(),
                ),
                self.shutdown_rx,
                Some(force_syncing_tables_rx),
            )
            .await?;

            info!("apply worker completed successfully");

            Ok(())
        }
        .instrument(apply_worker_span.or_current());

        let handle = tokio::spawn(apply_worker);

        Ok(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }
}

async fn get_start_lsn(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
) -> EtlResult<PgLsn> {
    let slot_name = get_slot_name(pipeline_id, WorkerType::Apply)?;
    // TODO: validate that we only create the slot when we first start replication which
    //  means when all tables are in the Init state. In any other case we should raise an
    //  error because that means the apply slot was deleted and creating a fresh slot now
    //  could cause inconsistent data to be read.
    //  Addendum: this might be hard to detect in all cases. E.g. what if the apply worker
    //  starts bunch of table sync workers and before creating a slot the process crashes?
    //  In this case, the apply worker slot is missing not because someone deleted it but
    //  because it was never created in the first place. The answer here might be to create
    //  the apply worker slot as the first thing, before starting table sync workers.
    let slot = replication_client.get_or_create_slot(&slot_name).await?;
    let start_lsn = slot.get_start_lsn();

    Ok(start_lsn)
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
    force_syncing_tables_tx: SignalTx,
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
        force_syncing_tables_tx: SignalTx,
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
            force_syncing_tables_tx,
            table_sync_worker_permits,
        }
    }
}

impl<S, D> ApplyWorkerHook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    async fn build_table_sync_worker(&self, table_id: TableId) -> TableSyncWorker<S, D> {
        info!("creating a new table sync worker for table {}", table_id);

        TableSyncWorker::new(
            self.pipeline_id,
            self.config.clone(),
            self.pool.clone(),
            table_id,
            self.schema_cache.clone(),
            self.state_store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
            self.force_syncing_tables_tx.clone(),
            self.table_sync_worker_permits.clone(),
        )
    }

    async fn handle_syncing_table(&self, table_id: TableId, current_lsn: PgLsn) -> EtlResult<bool> {
        let mut pool = self.pool.lock().await;
        let table_sync_worker_state = pool.get_active_worker_state(table_id);

        // If there is no worker state, we start a new worker.
        let Some(table_sync_worker_state) = table_sync_worker_state else {
            let table_sync_worker = self.build_table_sync_worker(table_id).await;
            pool.start_worker(table_sync_worker).await?;

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
    ) -> EtlResult<bool> {
        let mut catchup_started = false;
        {
            let mut inner = table_sync_worker_state.get_inner().lock().await;
            if inner.replication_phase().as_type() == TableReplicationPhaseType::SyncWait {
                info!(
                    "table sync worker {} is waiting to catchup, starting catchup at lsn {}",
                    table_id, current_lsn
                );

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
            info!(
                "catchup was started, waiting for table sync worker {} to complete sync",
                table_id
            );

            let result = table_sync_worker_state
                .wait_for_phase_type(
                    TableReplicationPhaseType::SyncDone,
                    self.shutdown_rx.clone(),
                )
                .await;

            info!("the table sync worker {} has finished syncing", table_id);

            // If we are told to shut down while waiting for a phase change, we will signal this to
            // the caller.
            if result.should_shutdown() {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

impl<S, D> ApplyLoopHook for ApplyWorkerHook<S, D>
where
    S: StateStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    async fn before_loop(&self, _start_lsn: PgLsn) -> EtlResult<bool> {
        info!("starting table sync workers before the main apply loop");

        let active_table_replication_states =
            get_table_replication_states(&self.state_store, false).await?;

        for (table_id, table_replication_phase) in active_table_replication_states {
            // A table in `SyncDone` doesn't need to have its worker started, since the main apply
            // worker will move it into `Ready` state automatically once the condition is met.
            if let TableReplicationPhaseType::SyncDone = table_replication_phase.as_type() {
                continue;
            }

            // If there is already an active worker for this table in the pool, we can avoid starting
            // it.
            let mut pool = self.pool.lock().await;
            if pool.get_active_worker_state(table_id).is_some() {
                continue;
            }

            // If we fail, we just show an error, and hopefully we will succeed when starting it
            // during syncing tables.
            let table_sync_worker = self.build_table_sync_worker(table_id).await;
            if let Err(err) = pool.start_worker(table_sync_worker).await {
                error!(
                    "error starting table sync worker for table {} during initialization: {}",
                    table_id, err
                );
            }
        }

        Ok(true)
    }

    async fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> EtlResult<bool> {
        let active_table_replication_states =
            get_table_replication_states(&self.state_store, false).await?;
        debug!(
            "processing syncing tables for apply worker with lsn {}",
            current_lsn
        );

        for (table_id, table_replication_phase) in active_table_replication_states {
            // We read the state store state first, if we don't find `SyncDone` we will attempt to
            // read the shared state which can contain also non-persisted states.
            match table_replication_phase {
                // It is important that the `if current_lsn >= lsn` is inside the match arm rather than
                // as a guard on it because while we do want to mark any tables in sync done state as
                // ready, we do not want to call the `handle_syncing_table` method on such tables because
                // it will unnecessarily launch a table sync worker for it which will anyway exit because
                // table sync workers do not process tables in either sync done or ready states. Preventing
                // launch of such workers has become even more important after we started calling
                // `process_syncing_tables` at regular intervals, because now such spurious launches
                // have become extremely common, which is just wasteful.
                TableReplicationPhase::SyncDone { lsn } => {
                    if current_lsn >= lsn && update_state {
                        info!(
                            "table {} is ready, its events will now be processed by the main apply worker",
                            table_id
                        );

                        self.state_store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    }
                }
                _ => match self.handle_syncing_table(table_id, current_lsn).await {
                    Ok(continue_loop) => {
                        if !continue_loop {
                            return Ok(false);
                        }
                    }
                    Err(err) => {
                        error!("error handling syncing for table {}: {}", table_id, err);
                    }
                },
            }
        }

        Ok(true)
    }

    async fn skip_table(&self, table_id: TableId) -> EtlResult<bool> {
        let table_sync_worker_state = {
            let pool = self.pool.lock().await;
            pool.get_active_worker_state(table_id)
        };

        // In case we have the state in memory, we will also update that.
        if let Some(table_sync_worker_state) = table_sync_worker_state {
            let mut inner = table_sync_worker_state.get_inner().lock().await;
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
    ) -> EtlResult<bool> {
        let pool = self.pool.lock().await;

        // We try to load the state first from memory, if we don't find it, we try to load from the
        // state store.
        let replication_phase = match pool.get_active_worker_state(table_id) {
            Some(state) => {
                let inner = state.get_inner().lock().await;
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
