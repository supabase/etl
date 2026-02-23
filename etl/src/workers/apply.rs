use etl_config::shared::{InvalidatedSlotBehavior, PipelineConfig};
use etl_postgres::replication::slots::EtlReplicationSlot;
use metrics::counter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, error, info, warn};

use crate::bail;
use crate::concurrency::batch_budget::BatchBudgetController;
use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::concurrency::shutdown::ShutdownRx;
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::metrics::{
    ERROR_TYPE_LABEL, ETL_SLOT_INVALIDATIONS_TOTAL, ETL_WORKER_ERRORS_TOTAL, PIPELINE_ID_LABEL,
    WORKER_TYPE_LABEL,
};
use crate::replication::apply::{ApplyLoop, ApplyWorkerContext, WorkerContext};
use crate::replication::client::{GetOrCreateSlotResult, PgReplicationClient, SlotState};
use crate::replication::masks::ReplicationMasksCache;
use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::policy::build_error_handling_policy;
use crate::workers::pool::TableSyncWorkerPool;

/// Handle for monitoring and controlling the apply worker.
///
/// [`ApplyWorkerHandle`] provides control over the apply worker that processes
/// replication stream events and coordinates with table sync workers. The handle
/// enables waiting for worker completion and checking final results.
#[derive(Debug)]
pub struct ApplyWorkerHandle {
    handle: Option<JoinHandle<EtlResult<()>>>,
}

impl ApplyWorkerHandle {
    /// Waits for the apply worker to complete execution.
    ///
    /// This method blocks until the apply worker finishes processing, either
    /// due to successful completion, shutdown signal, or error. It properly
    /// handles panics that might occur within the worker task.
    pub async fn wait(mut self) -> EtlResult<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await.map_err(|err| {
            if err.is_cancelled() {
                etl_error!(
                    ErrorKind::ApplyWorkerCancelled,
                    "Apply worker was cancelled",
                    err
                )
            } else {
                etl_error!(ErrorKind::ApplyWorkerPanic, "Apply worker panicked", err)
            }
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
    pool: Arc<TableSyncWorkerPool>,
    store: S,
    destination: D,
    replication_masks: ReplicationMasksCache,
    shutdown_rx: ShutdownRx,
    table_sync_worker_permits: Arc<Semaphore>,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
}

impl<S, D> ApplyWorker<S, D> {
    /// Creates a new apply worker with the given configuration and dependencies.
    ///
    /// The worker creates a fresh replication connection for each run attempt and
    /// coordinates with the table sync worker pool for initial synchronization operations.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: Arc<TableSyncWorkerPool>,
        store: S,
        destination: D,
        replication_masks: ReplicationMasksCache,
        shutdown_rx: ShutdownRx,
        table_sync_worker_permits: Arc<Semaphore>,
        memory_monitor: MemoryMonitor,
    ) -> Self {
        let batch_budget = BatchBudgetController::new(
            pipeline_id,
            memory_monitor.clone(),
            config.batch.memory_budget_ratio,
        );

        Self {
            pipeline_id,
            config,
            pool,
            store,
            destination,
            replication_masks,
            shutdown_rx,
            table_sync_worker_permits,
            memory_monitor,
            batch_budget,
        }
    }
}

impl<S, D> ApplyWorker<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Handles apply worker errors using policy-based retry and backoff.
    ///
    /// Returns `Ok(true)` if shutdown was requested while waiting to retry, `Ok(false)` if
    /// execution should continue retrying, or `Err` when the failure should be propagated.
    ///
    /// Errors that happen while handling the worker error in this function are immediately propagated.
    async fn handle_apply_worker_error(
        pipeline_id: PipelineId,
        config: &PipelineConfig,
        shutdown_rx: &mut ShutdownRx,
        retry_attempts: &mut u32,
        err: EtlError,
    ) -> EtlResult<bool> {
        error!(error = %err, "apply worker failed");

        let policy = build_error_handling_policy(&err);
        counter!(
            ETL_WORKER_ERRORS_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string(),
            WORKER_TYPE_LABEL => "apply",
            ERROR_TYPE_LABEL => policy.retry_directive().to_string(),
        )
        .increment(1);

        // If the error is not retriable, we should just propagate it.
        if !policy.should_retry() {
            return Err(err);
        }

        // If we reached the max attempts, we propagate the last known error.
        if *retry_attempts >= config.table_error_retry_max_attempts {
            error!(
                max_attempts = config.table_error_retry_max_attempts,
                error = %err,
                "apply worker timed retry limit reached, stopping worker",
            );

            return Err(err);
        }

        *retry_attempts = retry_attempts.saturating_add(1);
        let sleep_duration = Duration::from_millis(config.table_error_retry_delay_ms);

        info!(
            retry_attempt = *retry_attempts,
            max_attempts = config.table_error_retry_max_attempts,
            sleep_duration = ?sleep_duration,
            "retrying apply worker after timed-retriable error",
        );

        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                info!("shutting down apply worker while waiting to retry");
                Ok(true)
            }

            _ = tokio::time::sleep(sleep_duration) => Ok(false)
        }
    }

    /// Spawns the apply worker and returns a handle for monitoring.
    ///
    /// This method initializes the apply worker by determining the starting LSN,
    /// creating coordination signals, and launching the main apply loop. The worker
    /// runs asynchronously and can be monitored through the returned handle.
    pub async fn spawn(self) -> EtlResult<ApplyWorkerHandle> {
        info!("starting apply worker");

        let apply_worker_span = tracing::info_span!(
            "apply_worker",
            pipeline_id = self.pipeline_id,
            publication_name = self.config.publication_name
        );
        let apply_worker = self
            .guarded_run_apply_worker()
            .instrument(apply_worker_span.or_current());

        let handle = tokio::spawn(apply_worker);

        Ok(ApplyWorkerHandle {
            handle: Some(handle),
        })
    }

    /// Runs the apply worker with retry handling for timed-retriable errors.
    ///
    /// Timed retry scheduling intentionally reuses the same settings used by table sync workers
    /// (`table_error_retry_delay_ms` and `table_error_retry_max_attempts`) so retry behavior is
    /// coherent across worker types.
    async fn guarded_run_apply_worker(self) -> EtlResult<()> {
        let pipeline_id = self.pipeline_id;
        let config = self.config.clone();
        let pool = self.pool.clone();
        let store = self.store.clone();
        let destination = self.destination.clone();
        let replication_masks = self.replication_masks.clone();
        let table_sync_worker_permits = self.table_sync_worker_permits.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut retry_attempts: u32 = 0;

        loop {
            let worker = ApplyWorker {
                pipeline_id,
                config: config.clone(),
                pool: pool.clone(),
                store: store.clone(),
                destination: destination.clone(),
                replication_masks: replication_masks.clone(),
                shutdown_rx: shutdown_rx.clone(),
                table_sync_worker_permits: table_sync_worker_permits.clone(),
                memory_monitor: self.memory_monitor.clone(),
                batch_budget: self.batch_budget.clone(),
            };

            let result = worker.run_apply_worker().await;
            match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    let should_shutdown = Self::handle_apply_worker_error(
                        pipeline_id,
                        config.as_ref(),
                        &mut shutdown_rx,
                        &mut retry_attempts,
                        err,
                    )
                    .await?;

                    if should_shutdown {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Runs a single apply worker attempt.
    async fn run_apply_worker(self) -> EtlResult<()> {
        let replication_client =
            PgReplicationClient::connect(self.config.pg_connection.clone()).await?;
        let _apply_loop_stream_guard = self.batch_budget.register_stream_load(1);

        let start_lsn = get_start_lsn(
            self.pipeline_id,
            &replication_client,
            &self.store,
            &self.config.invalidated_slot_behavior,
        )
        .await?;

        let worker_context = WorkerContext::Apply(ApplyWorkerContext {
            pipeline_id: self.pipeline_id,
            config: self.config.clone(),
            pool: self.pool,
            store: self.store.clone(),
            destination: self.destination.clone(),
            replication_masks: self.replication_masks.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            table_sync_worker_permits: self.table_sync_worker_permits,
            memory_monitor: self.memory_monitor.clone(),
            batch_budget: self.batch_budget.clone(),
        });

        ApplyLoop::start(
            self.pipeline_id,
            start_lsn,
            self.config,
            replication_client,
            self.store,
            self.destination,
            self.replication_masks,
            worker_context,
            self.shutdown_rx,
            self.memory_monitor,
            self.batch_budget,
        )
        .await?;

        info!("apply worker completed successfully");

        Ok(())
    }
}

/// Determines the LSN position from which the apply worker should start reading the replication stream.
///
/// This function implements critical replication consistency logic by managing the apply worker's
/// replication slot. The slot serves as a persistent marker in Postgres's WAL (Write-Ahead Log)
/// that tracks the apply worker's progress and prevents WAL deletion of unreplicated data.
///
/// When an existing slot is found, this function checks if it's been invalidated. If so, it handles
/// the situation according to the configured [`InvalidatedSlotBehavior`]:
/// - [`InvalidatedSlotBehavior::Error`]: Returns an error requiring manual intervention
/// - [`InvalidatedSlotBehavior::Recreate`]: Deletes the slot, resets all tables to Init, and creates a new slot
///
/// When creating a new slot, this function validates that all tables are in the Init state.
/// If any table is not in Init state when creating a new slot, it indicates that data was
/// synchronized based on a different apply worker lineage, which would break replication
/// correctness.
async fn get_start_lsn<S: StateStore>(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    store: &S,
    invalidated_slot_behavior: &InvalidatedSlotBehavior,
) -> EtlResult<PgLsn> {
    let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into()?;

    // We try to get or create the slot. Both operations will return an LSN that we can use to start
    // streaming events.
    let slot = replication_client.get_or_create_slot(&slot_name).await?;

    // When we get an existing slot, check if it's been invalidated
    if let GetOrCreateSlotResult::GetSlot(_) = &slot {
        let slot_state = replication_client.get_slot_state(&slot_name).await?;

        if slot_state == SlotState::Invalidated {
            return handle_invalidated_slot(
                pipeline_id,
                replication_client,
                store,
                &slot_name,
                invalidated_slot_behavior,
            )
            .await;
        }
    }

    // When creating a new apply worker slot, all tables must be in the `Init` state. If any table
    // is not in Init state, it means the table was synchronized based on another apply worker
    // lineage (different slot) which will break correctness.
    if let GetOrCreateSlotResult::CreateSlot(_) = &slot
        && let Err(err) = validate_tables_in_init_state(store).await
    {
        // Delete the slot before failing, otherwise the system will restart and skip validation
        // since the slot will already exist.
        replication_client.delete_slot_if_exists(&slot_name).await?;

        return Err(err);
    }

    // We return the LSN from which we will start streaming events.
    Ok(slot.get_start_lsn())
}

/// Handles the case when the apply worker slot is found to be invalidated.
///
/// Depending on the configured behavior:
/// - [`InvalidatedSlotBehavior::Error`]: Returns an error with details about the invalidation
/// - [`InvalidatedSlotBehavior::Recreate`]: Deletes the slot, resets all table states to Init,
///   and creates a new slot, returning its consistent point LSN
async fn handle_invalidated_slot<S: StateStore>(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    store: &S,
    slot_name: &str,
    behavior: &InvalidatedSlotBehavior,
) -> EtlResult<PgLsn> {
    counter!(
        ETL_SLOT_INVALIDATIONS_TOTAL,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
    )
    .increment(1);

    match behavior {
        InvalidatedSlotBehavior::Error => {
            bail!(
                ErrorKind::ReplicationSlotInvalidated,
                "Replication slot has been invalidated",
                format!(
                    "The replication slot '{}' for pipeline {} has been invalidated. \
                    This typically happens when the slot falls too far behind and PostgreSQL \
                    removes the required WAL segments. To recover, delete the apply replication slot, \
                    reset all table states, and start/restart the pipeline.",
                    slot_name, pipeline_id
                )
            );
        }
        InvalidatedSlotBehavior::Recreate => {
            warn!(
                slot_name,
                pipeline_id,
                "replication slot is invalidated, resetting all table states and recreating slot"
            );

            // We update all tables to Init to reset their state, but no slots are deleted for table
            // sync workers since the deletion will be handled by the worker itself when starting up
            // again.
            let table_states_updates: Vec<_> = store
                .get_table_replication_states()
                .await?
                .keys()
                .map(|table_id| (*table_id, TableReplicationPhase::Init))
                .collect();
            let reset_count = table_states_updates.len();
            store
                .update_table_replication_states(table_states_updates)
                .await?;

            info!(
                reset_count,
                "reset table replication states to init for resync"
            );

            // We delete and recreate the main apply worker slot.
            replication_client.delete_slot_if_exists(slot_name).await?;
            let create_result = replication_client.create_slot(slot_name).await?;

            info!(
                slot_name,
                consistent_point = %create_result.consistent_point,
                "created new apply worker replication slot after invalidation recovery"
            );

            Ok(create_result.consistent_point)
        }
    }
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
