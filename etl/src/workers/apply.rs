use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::EtlReplicationSlot;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::types::TableId;
use std::sync::Arc;
use tokio::sync::{MutexGuard, Semaphore};
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, debug, error, info};

use crate::bail;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::replication::apply::{ApplyLoopAction, ApplyLoopHook, start_apply_loop};
use crate::replication::client::{GetOrCreateSlotResult, PgReplicationClient};
use crate::state::table::{
    TableReplicationError, TableReplicationPhase, TableReplicationPhaseType,
};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::base::{Worker, WorkerHandle};
use crate::workers::pool::{TableSyncWorkerPool, TableSyncWorkerPoolInner};
use crate::workers::table_sync::{TableSyncWorker, TableSyncWorkerState};

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
            etl_error!(ErrorKind::ApplyWorkerPanic, "Apply worker panicked", err)
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
            let start_lsn =
                get_start_lsn(self.pipeline_id, &self.replication_client, &self.store).await?;

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
                    self.table_sync_worker_permits.clone(),
                ),
                self.shutdown_rx,
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
///
/// When creating a new slot, this function validates that all tables are in the Init state.
/// If any table is not in Init state when creating a new slot, it indicates that data was
/// synchronized based on a different apply worker lineage, which would break replication
/// correctness.
async fn get_start_lsn<S: StateStore>(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    store: &S,
) -> EtlResult<PgLsn> {
    let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into()?;

    // We try to get or create the slot. Both operations will return an LSN that we can use to start
    // streaming events.
    let slot = replication_client.get_or_create_slot(&slot_name).await?;

    // When creating a new apply worker slot, all tables must be in the `Init` state. If any table
    // is not in Init state, it means the table was synchronized based on another apply worker
    // lineage (different slot) which will break correctness.
    if let GetOrCreateSlotResult::CreateSlot(_) = &slot {
        if let Err(err) = validate_tables_in_init_state(store).await {
            // Delete the slot before failing, otherwise the system will restart and skip validation
            // since the slot will already exist.
            replication_client.delete_slot(&slot_name).await?;

            return Err(err);
        }
    }

    // We return the LSN from which we will start streaming events.
    Ok(slot.get_start_lsn())
}

/// Validates that all tables are in the Init state.
///
/// This validation is required when creating a new apply worker slot to ensure replication
/// correctness. If any table has progressed beyond Init state, it indicates the table was
/// synchronized based on a different apply worker lineage.
async fn validate_tables_in_init_state<S: StateStore>(store: &S) -> EtlResult<()> {
    let table_states = store.get_table_replication_states().await?;

    let non_init_tables: Vec<_> = table_states
        .iter()
        .filter(|(_, phase)| phase.as_type() != TableReplicationPhaseType::Init)
        .map(|(table_id, phase)| (*table_id, phase.as_type()))
        .collect();

    if non_init_tables.is_empty() {
        return Ok(());
    }

    let table_details: Vec<String> = non_init_tables
        .iter()
        .map(|(id, phase)| format!("table {id} in state {phase}"))
        .collect();

    bail!(
        ErrorKind::InvalidState,
        "Cannot create apply worker slot when tables are not in Init state",
        format!(
            "Creating a new apply worker replication slot requires all tables to be in Init state, \
            but found {} table(s) in non-Init states: {}. This indicates that tables were \
            synchronized based on a different apply worker lineage. To fix this, either restore \
            the original apply worker slot or reset all tables to Init state.",
            non_init_tables.len(),
            table_details.join(", ")
        )
    );
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
    /// Semaphore controlling maximum concurrent table sync workers.
    table_sync_worker_permits: Arc<Semaphore>,
}

impl<S, D> ApplyWorkerHook<S, D> {
    /// Creates a new apply worker hook with the given dependencies.
    ///
    /// This constructor initializes the hook with all necessary components
    /// for coordinating between the apply loop and table sync workers.
    fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: TableSyncWorkerPool,
        store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        table_sync_worker_permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            pool,
            store,
            destination,
            shutdown_rx,
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
        info!(%table_id, "creating table sync worker");

        TableSyncWorker::new(
            self.pipeline_id,
            self.config.clone(),
            self.pool.clone(),
            table_id,
            self.store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
            self.table_sync_worker_permits.clone(),
        )
    }

    /// Processes a single syncing table during the apply loop iteration.
    ///
    /// This method handles one table's synchronization lifecycle by checking if an active
    /// worker exists, resolving the effective replication phase, and taking appropriate
    /// action based on the phase. The pool lock must be held by the caller.
    ///
    /// Returns an action that indicates whether the apply loop should continue, pause, or
    /// terminate based on the table's processing outcome.
    async fn process_single_syncing_table(
        &self,
        pool: &mut MutexGuard<'_, TableSyncWorkerPoolInner>,
        table_id: TableId,
        store_phase: TableReplicationPhase,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> EtlResult<ApplyLoopAction> {
        // Request the active worker state once per table. This is the single source of
        // truth for the current state if a worker exists.
        let active_worker_state = pool.get_active_worker_state(table_id);

        // If an active worker exists, lock its state once to read the phase for this
        // loop cycle. This ensures we make all decisions based on a consistent snapshot
        // of the state at this moment.
        if let Some(worker_state) = active_worker_state {
            // Lock the worker state once and read the effective phase.
            let mut worker_state_guard = worker_state.lock().await;
            let active_phase = worker_state_guard.replication_phase();

            // Handle SyncDone tables by promoting them to Ready when the apply worker
            // has caught up to their sync LSN.
            match active_phase {
                TableReplicationPhase::SyncDone { lsn } => {
                    if current_lsn >= lsn && update_state {
                        info!(
                            %table_id,
                            "table ready, events will be processed by apply worker",
                        );

                        self.store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    }
                }
                TableReplicationPhase::SyncWait => {
                    // Transition worker from SyncWait to Catchup.
                    info!(
                        %table_id,
                        %current_lsn,
                        "table sync worker is waiting to catchup, starting catchup",
                    );

                    worker_state_guard
                        .set_and_store(
                            TableReplicationPhase::Catchup { lsn: current_lsn },
                            &self.store,
                        )
                        .await?;

                    info!(
                        %table_id,
                        "catchup was started, waiting for table sync worker to complete sync",
                    );

                    // We drop the guard before waiting for a phase, to let the workers make progress.
                    drop(worker_state_guard);

                    // Wait for the table to be in `SyncDone` or `Errored`.
                    let result = worker_state
                        .wait_for_phase_type(
                            &[
                                TableReplicationPhaseType::SyncDone,
                                TableReplicationPhaseType::Errored,
                            ],
                            self.shutdown_rx.clone(),
                        )
                        .await;

                    // If we are told to shut down while waiting, signal to cancel the apply loop.
                    if result.should_shutdown() {
                        info!(
                            %table_id,
                            "table sync worker interrupted by shutdown signal",
                        );

                        return Ok(ApplyLoopAction::Pause);
                    }

                    // If the table sync worker errored, skip this table and continue.
                    if let ShutdownResult::Ok(inner) = &result {
                        if inner.replication_phase().as_type().is_errored() {
                            info!(
                                %table_id,
                                "table sync worker errored, skipping table",
                            );

                            return Ok(ApplyLoopAction::Continue);
                        }
                    }

                    info!(%table_id, "table sync worker finished syncing");
                }
                _ => {
                    // For other phases, no action needed in this loop cycle.
                }
            }
        } else {
            // No active worker exists, use the store phase and potentially start a new worker.
            match store_phase {
                TableReplicationPhase::SyncDone { lsn } => {
                    if current_lsn >= lsn && update_state {
                        info!(
                            %table_id,
                            "table ready, events will be processed by apply worker",
                        );

                        self.store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    }
                }
                _ => {
                    // Start a new worker for this table for any other state.
                    let table_sync_worker = self.build_table_sync_worker(table_id).await;
                    if let Err(err) = pool.start_worker(table_sync_worker).await {
                        error!(
                            %table_id,
                            error = %err,
                            "failed to start table sync worker",
                        );
                    }
                }
            }
        }

        Ok(ApplyLoopAction::Continue)
    }
}

impl<S, D> ApplyLoopHook for ApplyWorkerHook<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Processes all tables currently in synchronization phases.
    ///
    /// This method coordinates the lifecycle of syncing tables by promoting
    /// `SyncDone` tables to `Ready` state when the apply worker catches up
    /// to their sync LSN. For other tables, it handles the typical sync process.
    async fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> EtlResult<ApplyLoopAction> {
        // TODO: this is a very hot path and we have to optimize it as much as possible.
        //
        // We fetch the tables that are in active syncing state. For more predictability and performance
        // we leverage the order of the BTreeMap.
        //
        // This load takes a snapshot of the current table states, independently of whether a table
        // sync worker is currently active or not.
        let active_table_replication_states = self
            .store
            .get_table_replication_states()
            .await?
            .into_iter()
            .filter(|(_, state)| !state.as_type().is_done());

        // Lock the pool before iteration to ensure consistency. This prevents race conditions
        // where a worker finishes between loading states and processing them. The lock is
        // held for the entire iteration to maintain a consistent view of worker states.
        let mut pool = self.pool.lock().await;

        for (table_id, store_phase) in active_table_replication_states {
            let action = self
                .process_single_syncing_table(
                    &mut pool,
                    table_id,
                    store_phase,
                    current_lsn,
                    update_state,
                )
                .await?;

            // If the action is terminating, stop iterating and return immediately.
            if action.is_terminating() {
                return Ok(action);
            }
        }

        Ok(ApplyLoopAction::Continue)
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
    ) -> EtlResult<ApplyLoopAction> {
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
        Ok(ApplyLoopAction::Continue)
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
                    debug!(
                        %table_id,
                        worker_type = %self.worker_type(),
                        should_apply_changes = false,
                        "evaluated whether table should apply changes",
                    );

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

        debug!(
            %table_id,
            worker_type = %self.worker_type(),
            should_apply_changes = should_apply_changes,
            "evaluated whether table should apply changes",
        );

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
