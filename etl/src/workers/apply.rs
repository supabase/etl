use etl_config::shared::PipelineConfig;
use etl_postgres::schema::TableId;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, debug, error, info};

use crate::concurrency::shutdown::ShutdownRx;
use crate::concurrency::signal::SignalTx;
use crate::concurrency::signal::create_signal;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::replication::apply::{ApplyLoopHook, start_apply_loop};
use crate::replication::client::PgReplicationClient;
use crate::replication::common::get_table_replication_states;
use crate::state::table::{
    TableReplicationError, TableReplicationPhase, TableReplicationPhaseType,
};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::base::{Worker, WorkerHandle};
use crate::workers::pool::TableSyncWorkerPool;
use crate::workers::table_sync::{TableSyncWorker, TableSyncWorkerState};
use etl_postgres::replication::slots::get_slot_name;
use etl_postgres::replication::worker::WorkerType;

/// Handle for monitoring and controlling the apply worker.
///
/// [`ApplyWorkerHandle`] provides control over the apply worker that processes
/// replication stream events and coordinates with table sync workers. The handle
/// enables waiting for worker completion and checking final results.
#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<EtlResult<()>>>,
}

impl WorkerHandle<()> for ApplyWorkerHandle {
    /// Returns the current state of the apply worker.
    ///
    /// Since the apply worker doesn't expose detailed state information,
    /// this method returns unit type and serves as a placeholder.
    fn state(&self) {}

    /// Waits for the apply worker to complete execution.
    ///
    /// This method blocks until the apply worker finishes processing, either
    /// due to successful completion, shutdown signal, or error. It properly
    /// handles panics that might occur within the worker task.
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

/// Worker that applies replication stream events to destinations.
///
/// [`ApplyWorker`] is the core worker responsible for processing Postgres logical
/// replication events and applying them to the configured destination. It coordinates
/// with table sync workers during initial synchronization and handles the continuous
/// replication stream during normal operation.
///
/// The worker manages transaction boundaries, coordinates table synchronization,
/// and ensures data consistency throughout the replication process.
#[derive(Debug)]
pub struct ApplyWorker<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    pool: TableSyncWorkerPool,
    store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    table_sync_worker_permits: Arc<Semaphore>,
}

impl<S, D> ApplyWorker<S, D> {
    /// Creates a new apply worker with the given configuration and dependencies.
    ///
    /// The worker will use the provided replication client to read the Postgres
    /// replication stream and coordinate with the table sync worker pool for
    /// initial synchronization operations.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        pool: TableSyncWorkerPool,
        store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        table_sync_worker_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            replication_client,
            pool,
            store,
            destination,
            shutdown_rx,
            table_sync_worker_permits,
        }
    }
}

impl<S, D> Worker<ApplyWorkerHandle, ()> for ApplyWorker<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    type Error = EtlError;

    /// Starts the apply worker and returns a handle for monitoring.
    ///
    /// This method initializes the apply worker by determining the starting LSN,
    /// creating coordination signals, and launching the main apply loop. The worker
    /// runs asynchronously and can be monitored through the returned handle.
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
                self.store.clone(),
                self.destination.clone(),
                ApplyWorkerHook::new(
                    self.pipeline_id,
                    self.config,
                    self.pool,
                    self.store,
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

/// Determines the LSN position from which the apply worker should start reading the replication stream.
///
/// This function implements critical replication consistency logic by managing the apply worker's
/// replication slot. The slot serves as a persistent marker in Postgres's WAL (Write-Ahead Log)
/// that tracks the apply worker's progress and prevents WAL deletion of unreplicated data.
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

/// Internal coordination hook that implements apply loop integration with table sync workers.
///
/// [`ApplyWorkerHook`] serves as the critical coordination layer between the main apply loop
/// that processes replication events and the table sync worker pool that handles initial data
/// copying. This hook implements the [`ApplyLoopHook`] trait to provide custom logic for
/// managing table lifecycle transitions and worker coordination.
#[derive(Debug)]
struct ApplyWorkerHook<S, D> {
    /// Unique identifier for the pipeline this hook serves.
    pipeline_id: PipelineId,
    /// Shared configuration for all coordinated operations.
    config: Arc<PipelineConfig>,
    /// Pool of table sync workers that this hook coordinates.
    pool: TableSyncWorkerPool,
    /// State store for tracking table replication progress.
    store: S,
    /// Destination where replicated data is written.
    destination: D,
    /// Shutdown signal receiver for graceful termination.
    shutdown_rx: ShutdownRx,
    /// Signal transmitter for triggering table sync operations.
    force_syncing_tables_tx: SignalTx,
    /// Semaphore controlling maximum concurrent table sync workers.
    table_sync_worker_permits: Arc<Semaphore>,
}

impl<S, D> ApplyWorkerHook<S, D> {
    /// Creates a new apply worker hook with the given dependencies.
    ///
    /// This constructor initializes the hook with all necessary components
    /// for coordinating between the apply loop and table sync workers.
    #[expect(clippy::too_many_arguments)]
    fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: TableSyncWorkerPool,
        store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        force_syncing_tables_tx: SignalTx,
        table_sync_worker_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            pool,
            store,
            destination,
            shutdown_rx,
            force_syncing_tables_tx,
            table_sync_worker_permits,
        }
    }
}

impl<S, D> ApplyWorkerHook<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Creates a new table sync worker for the specified table.
    ///
    /// This method constructs a fully configured table sync worker that can
    /// handle the initial data synchronization for the given table. The worker
    /// inherits the hook's configuration and coordination channels.
    async fn build_table_sync_worker(&self, table_id: TableId) -> TableSyncWorker<S, D> {
        info!("creating a new table sync worker for table {}", table_id);

        TableSyncWorker::new(
            self.pipeline_id,
            self.config.clone(),
            self.pool.clone(),
            table_id,
            self.store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
            self.force_syncing_tables_tx.clone(),
            self.table_sync_worker_permits.clone(),
        )
    }

    /// Handles the lifecycle of a syncing table and its associated worker.
    ///
    /// This method checks if a table sync worker exists for the given table and
    /// either creates a new worker or coordinates with the existing one. It manages
    /// the transition from sync to catchup phases based on the current LSN.
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

    /// Coordinates with an existing table sync worker to manage state transitions.
    ///
    /// This method handles workers that are in the `SyncWait` state by transitioning
    /// them to `Catchup` and then waiting for them to reach `SyncDone`. It ensures
    /// proper synchronization between the apply worker and table sync workers.
    async fn handle_existing_worker(
        &self,
        table_id: TableId,
        table_sync_worker_state: TableSyncWorkerState,
        current_lsn: PgLsn,
    ) -> EtlResult<bool> {
        let mut catchup_started = false;
        {
            let mut inner = table_sync_worker_state.lock().await;
            if inner.replication_phase().as_type() == TableReplicationPhaseType::SyncWait {
                info!(
                    "table sync worker {} is waiting to catchup, starting catchup at lsn {}",
                    table_id, current_lsn
                );

                inner
                    .set_and_store(
                        TableReplicationPhase::Catchup { lsn: current_lsn },
                        &self.store,
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

            // If we are told to shut down while waiting for a phase change, we will signal this to
            // the caller which will result in the apply loop being cancelled.
            if result.should_shutdown() {
                info!(
                    "the table sync worker {} didn't manage to finish syncing because it was instructed to shutdown",
                    table_id
                );

                return Ok(false);
            }

            info!("the table sync worker {} has finished syncing", table_id);
        }

        Ok(true)
    }
}

impl<S, D> ApplyLoopHook for ApplyWorkerHook<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Initializes table sync workers before the main apply loop begins.
    ///
    /// This hook method starts table sync workers for all tables that need
    /// initial synchronization. It excludes tables already in `SyncDone` state
    /// and avoids starting duplicate workers for tables that already have
    /// active workers in the pool.
    async fn before_loop(&self, _start_lsn: PgLsn) -> EtlResult<bool> {
        info!("starting table sync workers before the main apply loop");

        let active_table_replication_states =
            get_table_replication_states(&self.store, false).await?;

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

    /// Processes all tables currently in synchronization phases.
    ///
    /// This method coordinates the lifecycle of syncing tables by promoting
    /// `SyncDone` tables to `Ready` state when the apply worker catches up
    /// to their sync LSN. For other tables, it handles the typical sync process.
    async fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> EtlResult<bool> {
        let active_table_replication_states =
            get_table_replication_states(&self.store, false).await?;
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

                        self.store
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

    /// Handles table replication errors by updating the table's state.
    ///
    /// This method processes errors that occur during table replication by
    /// converting them to appropriate error states and persisting the updated
    /// state. The apply loop continues processing other tables after handling
    /// the error.
    async fn mark_table_errored(
        &self,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<bool> {
        let pool = self.pool.lock().await;

        // Convert the table replication error directly to a phase.
        let table_id = table_replication_error.table_id();
        TableSyncWorkerState::set_and_store(
            &pool,
            &self.store,
            table_id,
            table_replication_error.into(),
        )
        .await?;

        // We want to always continue the loop, since we have to deal with the events of other
        // tables.
        Ok(true)
    }

    /// Determines whether changes should be applied for a given table.
    ///
    /// This method evaluates the table's replication state to decide if events
    /// should be processed by the apply worker. It considers both in-memory
    /// worker states and persistent storage states to make the decision.
    ///
    /// Tables in `Ready` state always have changes applied. Tables in `SyncDone`
    /// state only apply changes if the sync LSN is at or before the current
    /// transaction's final LSN.
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
                let inner = state.lock().await;
                inner.replication_phase()
            }
            None => {
                let Some(state) = self.store.get_table_replication_state(table_id).await? else {
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

    /// Returns the worker type for this hook.
    ///
    /// This method identifies this hook as belonging to an apply worker,
    /// which is used for coordination and logging purposes throughout
    /// the replication system.
    fn worker_type(&self) -> WorkerType {
        WorkerType::Apply
    }
}
