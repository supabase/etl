use chrono::{Duration as ChronoDuration, Utc};
use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::EtlReplicationSlot;
use etl_postgres::types::TableId;
use metrics::counter;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard, Notify, Semaphore};
use tokio::task::AbortHandle;
use tracing::{Instrument, debug, error, info};

use crate::bail;
use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::metrics::{
    ERROR_TYPE_LABEL, ETL_WORKER_ERRORS_TOTAL, PIPELINE_ID_LABEL, WORKER_TYPE_LABEL,
};
use crate::replication::apply::{
    ApplyLoop, ApplyLoopResult, TableSyncWorkerContext, WorkerContext,
};
use crate::replication::client::PgReplicationClient;
use crate::replication::table_sync::{TableSyncResult, start_table_sync};
use crate::state::table::{
    RetryPolicy, TableReplicationError, TableReplicationPhase, TableReplicationPhaseType,
};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::PipelineId;
use crate::workers::policy::{RetryDirective, build_error_handling_policy};
use crate::workers::pool::{TableSyncWorkerId, TableSyncWorkerPool};

/// Internal state of [`TableSyncWorkerState`].
#[derive(Debug)]
pub struct TableSyncWorkerStateInner {
    /// Unique identifier for the table whose state this structure tracks.
    table_id: TableId,
    /// Current replication phase, this is the authoritative in-memory state.
    table_replication_phase: TableReplicationPhase,
    /// Notification mechanism for notifying state changes to waiting workers.
    phase_change: Arc<Notify>,
    /// Number of consecutive automatic retry attempts.
    retry_attempts: u32,
}

impl TableSyncWorkerStateInner {
    /// Updates the table's replication phase and notifies all waiting workers.
    ///
    /// This method provides the core state transition mechanism for table synchronization.
    /// It atomically updates the in-memory state and broadcasts the change to any workers
    /// that may be waiting for state transitions.
    pub fn set(&mut self, phase: TableReplicationPhase) {
        info!(
            table_id = self.table_id.0,
            from_phase = %self.table_replication_phase,
            to_phase = %phase,
            "table phase changing",
        );

        self.table_replication_phase = phase;

        // Broadcast notification to all active waiters.
        //
        // Note that this notify will not wake up waiters that will be coming in the future since
        // no permit is stored, only active listeners will be notified.
        self.phase_change.notify_waiters();
    }

    /// Returns the number of consecutive automatic retry attempts.
    pub fn retry_attempts(&self) -> u32 {
        self.retry_attempts
    }

    /// Increments the retry attempt counter and returns the updated value.
    pub fn increment_retry_attempts(&mut self) -> u32 {
        self.retry_attempts = self.retry_attempts.saturating_add(1);
        self.retry_attempts
    }

    /// Resets the automatic retry attempt counter to zero.
    pub fn reset_retry_attempts(&mut self) {
        self.retry_attempts = 0;
    }

    /// Updates the table's replication phase with conditional persistence to external storage.
    ///
    /// This method extends the basic [`TableSyncWorkerStateInner::set()`] method with durable persistence capabilities,
    /// ensuring that important state transitions survive process restarts and failures.
    ///
    /// The persistence behavior is controlled by the phase type's storage requirements.
    pub async fn set_and_store<S: StateStore>(
        &mut self,
        phase: TableReplicationPhase,
        state_store: &S,
    ) -> EtlResult<()> {
        // Apply in-memory state change first for immediate visibility
        self.set(phase.clone());

        // Conditionally persist based on phase type requirements
        if phase.as_type().should_store() {
            info!(
                table_id = self.table_id.0,
                %phase,
                "storing phase change",
            );

            // Persist to external storage - this may fail without affecting in-memory state
            state_store
                .update_table_replication_state(self.table_id, phase)
                .await?;
        }

        Ok(())
    }

    /// Rolls back the table's replication state to the previous version.
    ///
    /// This method coordinates rollback operations between persistent storage and
    /// in-memory state. It first queries the state store to retrieve the previous
    /// state, then applies that state to the in-memory representation and notifies
    /// any waiting workers of the change.
    pub async fn rollback<S: StateStore>(&mut self, state_store: &S) -> EtlResult<()> {
        // We rollback the state in the store and then also set the rolled back state in memory.
        let previous_phase = state_store
            .rollback_table_replication_state(self.table_id)
            .await?;
        self.set(previous_phase);

        Ok(())
    }

    /// Returns the current replication phase for this table.
    ///
    /// This method provides access to the authoritative in-memory state without
    /// requiring coordination with external storage. The returned phase represents
    /// the most current state as seen by the local worker.
    pub fn replication_phase(&self) -> TableReplicationPhase {
        self.table_replication_phase.clone()
    }
}

/// Thread-safe handle for table synchronization worker state management.
///
/// [`TableSyncWorkerState`] provides a thread-safe wrapper around table synchronization
/// state, enabling multiple workers to coordinate and react to state changes. It serves
/// as the primary coordination mechanism between table sync workers and apply workers.
///
/// The state handle supports atomic updates, notifications, and blocking waits for
/// specific phase transitions, making it suitable for complex multi-worker scenarios.
#[derive(Debug, Clone)]
pub struct TableSyncWorkerState {
    inner: Arc<Mutex<TableSyncWorkerStateInner>>,
}

impl TableSyncWorkerState {
    /// Creates a new table sync worker state with the given initial phase.
    ///
    /// This constructor initializes the state management structure with the
    /// specified table ID and replication phase. It sets up the notification
    /// mechanism for coordinating state changes between workers.
    fn new(table_id: TableId, table_replication_phase: TableReplicationPhase) -> Self {
        let inner = TableSyncWorkerStateInner {
            table_id,
            table_replication_phase,
            phase_change: Arc::new(Notify::new()),
            retry_attempts: 0,
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Updates table replication state in both memory and persistent storage.
    ///
    /// This static method provides a unified interface for updating table state
    /// regardless of whether the table has an active worker in the pool.
    pub async fn set_and_store<S>(
        pool: &TableSyncWorkerPool,
        state_store: &S,
        table_id: TableId,
        table_replication_phase: TableReplicationPhase,
    ) -> EtlResult<()>
    where
        S: StateStore,
    {
        let table_sync_worker_state = pool.get_active_worker_state(table_id).await;

        // In case we have the state in memory, we will atomically update the memory and state store
        // states. Otherwise, we just update the state store.
        //
        // Note: There is a potential race condition here where `get_active_worker_state` returns
        // `None` (worker finished), then a new worker starts and loads old state from the store,
        // and finally we update the store with the new state, leaving the new worker with stale
        // in-memory state. However, this is not a problem in practice because if we are calling
        // this from the table sync worker, the apply worker sees this worker as active and will not
        // launch any new worker and if we are the apply worker, no one can start a table sync worker
        // besides the apply worker itself, which is running this code.
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

    /// Waits for the table to reach a specific replication phase type.
    ///
    /// This method blocks until either the table reaches one of the desired phases or
    /// a shutdown signal is received. It uses an efficient notification system
    /// to avoid polling and provides immediate response to state changes.
    pub async fn wait_for_phase_type(
        &self,
        phase_types: &[TableReplicationPhaseType],
        mut shutdown_rx: ShutdownRx,
    ) -> ShutdownResult<MutexGuard<'_, TableSyncWorkerStateInner>, ()> {
        loop {
            let inner = self.inner.lock().await;

            let current_phase = inner.table_replication_phase.as_type();
            if phase_types.contains(&current_phase) {
                info!(
                    table_id = inner.table_id.0,
                    %current_phase,
                    "table replication phase reached",
                );

                return ShutdownResult::Ok(inner);
            }

            info!(
                table_id = inner.table_id.0,
                %current_phase,
                phase_types = ?phase_types,
                "waiting for table replication phase",
            );

            // We listen for the phase change while holding the lock to avoid the race condition which
            // occurs when we release the lock, the value changes, and then we wait for a value change,
            // in that case, we will miss the notification and the system will stall.
            let phase_change = inner.phase_change.clone();
            let phase_change_notified = phase_change.notified();

            // We must drop the lock here so that state changes can actually happen.
            drop(inner);

            tokio::select! {
                biased;

                _ = shutdown_rx.changed() => {
                    info!(phase_types = ?phase_types, "shutdown signal received, cancelling wait for phase");

                    return ShutdownResult::Shutdown(());
                }

                _ = phase_change_notified => {
                    // Phase changed, loop to check if it's the desired phase.
                }
            }
        }
    }
}

impl Deref for TableSyncWorkerState {
    type Target = Mutex<TableSyncWorkerStateInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Handle for monitoring and controlling table sync workers.
///
/// [`TableSyncWorkerHandle`] provides control and observability for table
/// synchronization workers. It exposes both the worker's state and completion
/// status, enabling coordination with other parts of the system.
#[derive(Debug)]
pub struct TableSyncWorkerHandle {
    worker_id: TableSyncWorkerId,
    state: TableSyncWorkerState,
    abort_handle: AbortHandle,
}

impl TableSyncWorkerHandle {
    /// Creates a new handle with the given worker ID, state, and abort handle.
    pub fn new(
        worker_id: TableSyncWorkerId,
        state: TableSyncWorkerState,
        abort_handle: AbortHandle,
    ) -> Self {
        Self {
            worker_id,
            state,
            abort_handle,
        }
    }

    /// Returns the unique identifier for this worker run.
    pub fn worker_id(&self) -> TableSyncWorkerId {
        self.worker_id
    }

    /// Returns the worker's state.
    pub fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    /// Checks if the worker task has finished.
    pub fn is_finished(&self) -> bool {
        self.abort_handle.is_finished()
    }
}

/// Worker responsible for synchronizing individual tables from Postgres to destinations.
///
/// [`TableSyncWorker`] handles the complete lifecycle of table synchronization, including
/// initial data copying, incremental catchup, and coordination with apply workers. Each
/// worker is responsible for a single table and manages its own replication slot.
///
/// The worker coordinates with the apply worker through shared state and implements
/// sophisticated retry and error handling logic to ensure robust operation.
#[derive(Debug)]
pub struct TableSyncWorker<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    pool: Arc<TableSyncWorkerPool>,
    table_id: TableId,
    store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    run_permit: Arc<Semaphore>,
    memory_monitor: MemoryMonitor,
}

impl<S, D> TableSyncWorker<S, D> {
    /// Creates a new table sync worker with the given configuration and dependencies.
    ///
    /// This constructor initializes all necessary components for table synchronization,
    /// including coordination channels, resource permits, and storage interfaces.
    /// The worker is ready to start synchronization upon creation.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: Arc<TableSyncWorkerPool>,
        table_id: TableId,
        store: S,
        destination: D,
        shutdown_rx: ShutdownRx,
        run_permit: Arc<Semaphore>,
        memory_monitor: MemoryMonitor,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            pool,
            table_id,
            store,
            destination,
            shutdown_rx,
            run_permit,
            memory_monitor,
        }
    }

    /// Returns the ID of the table this worker is responsible for synchronizing.
    ///
    /// This method provides access to the table identifier, which is used for
    /// logging, coordination, and state management throughout the synchronization
    /// process.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl<S, D> TableSyncWorker<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Handles table sync worker errors using policy-based retry and backoff.
    ///
    /// Returns `Ok(true)` if shutdown was requested while waiting to retry, `Ok(false)` if
    /// execution should continue retrying, or `Err` when the failure should be propagated.
    ///
    /// Errors that happen while handling the worker error in this function are immediately propagated.
    async fn handle_table_sync_worker_error(
        pipeline_id: PipelineId,
        table_id: TableId,
        config: &PipelineConfig,
        state: &TableSyncWorkerState,
        store: &S,
        shutdown_rx: &mut ShutdownRx,
        err: EtlError,
    ) -> EtlResult<bool> {
        error!(table_id = table_id.0, error = %err, "table sync worker failed");

        // Build a retry policy from the shared classifier. The concrete retry timestamp is
        // computed in the worker from config so both table sync and apply worker use the
        // same retry timing settings.
        let policy = build_error_handling_policy(&err);
        counter!(
            ETL_WORKER_ERRORS_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string(),
            WORKER_TYPE_LABEL => "table_sync",
            ERROR_TYPE_LABEL => policy.retry_directive().to_string(),
        )
        .increment(1);

        let mut retry_policy = match policy.retry_directive() {
            RetryDirective::Timed => RetryPolicy::retry_in(ChronoDuration::milliseconds(
                config.table_error_retry_delay_ms as i64,
            )),
            RetryDirective::Manual => RetryPolicy::ManualRetry,
            RetryDirective::NoRetry => RetryPolicy::NoRetry,
        };
        let mut table_error =
            TableReplicationError::from_error_policy(table_id, &err, &policy, retry_policy.clone());

        let mut state_guard = state.lock().await;

        // If we should retry this error, we want to see if we reached the maximum number of attempts
        // before trying again. If we did, we switch to a manual retry policy.
        if policy.should_retry()
            && state_guard.retry_attempts() >= config.table_error_retry_max_attempts
        {
            info!(
                table_id = table_id.0,
                max_attempts = config.table_error_retry_max_attempts,
                "max automatic retry attempts reached, switching to manual retry"
            );

            table_error = table_error.with_retry_policy(RetryPolicy::ManualRetry);
            retry_policy = table_error.retry_policy().clone();
        }

        // Update the state and store with the error. This way the user is notified about
        // the current error state.
        state_guard.set_and_store(table_error.into(), store).await?;

        match retry_policy {
            RetryPolicy::TimedRetry { next_retry } => {
                let now = Utc::now();
                let mut should_shutdown = false;

                if now < next_retry {
                    let sleep_duration = (next_retry - now)
                        .to_std()
                        .unwrap_or(Duration::from_secs(0));

                    info!(
                        table_id = table_id.0,
                        sleep_duration = ?sleep_duration,
                        "retrying table sync worker",
                    );

                    // We drop the state guard lock before sleeping to avoid stalling
                    // the apply worker while the worker is waiting to retry.
                    drop(state_guard);

                    // Stop retrying immediately on shutdown instead of sleeping through it.
                    tokio::select! {
                        biased;

                        _ = shutdown_rx.changed() => {
                            info!(table_id = table_id.0, "shutting down table sync worker while waiting to retry");
                            should_shutdown = true;
                        }

                        _ = tokio::time::sleep(sleep_duration) => {}
                    }

                    // We lock the state again after sleeping.
                    state_guard = state.lock().await;
                } else {
                    info!(table_id = table_id.0, "retrying table sync worker");
                }

                // If we should shutdown because we got a shutdown request during retry we
                // just want to immediately return.
                if should_shutdown {
                    state_guard.reset_retry_attempts();

                    return Ok(true);
                }

                // We mark that we attempted a retry.
                state_guard.increment_retry_attempts();

                // After sleeping, we rollback to the previous state and retry.
                //
                // Note that this rollback is one state before which works only if we are
                // in a table sync worker, this is why it's not in the apply worker:
                // - Errored -> Init: okay since it will restart from scratch.
                // - Errored -> DataSync: okay since it will restart the copy from a new slot.
                // - Errored -> FinishedCopy: okay since the table was already copied, so it resumes
                //   streaming from the `confirmed_flush_lsn`.
                // - Errored -> SyncDone: okay since the table sync will immediately stop.
                // - Errored -> Ready: same as SyncDone.
                //
                // The in-memory states like SyncWait and Catchup won't ever be in a rollback
                // since they are just states used for synchronization and never saved in the
                // state store.
                state_guard.rollback(store).await?;

                Ok(false)
            }
            RetryPolicy::NoRetry | RetryPolicy::ManualRetry => {
                state_guard.reset_retry_attempts();

                Err(err)
            }
        }
    }

    /// Spawns the table sync worker into the pool.
    ///
    /// This method initializes the worker by loading its replication state from
    /// storage, creating the state management structure, and spawning the
    /// synchronization process into the pool.
    pub async fn spawn_into_pool(self, pool: &TableSyncWorkerPool) -> EtlResult<()> {
        info!(table_id = self.table_id.0, "starting table sync worker");

        let Some(table_replication_phase) = self
            .store
            .get_table_replication_state(self.table_id)
            .await?
        else {
            error!(
                table_id = self.table_id.0,
                "no replication state found, cannot start sync worker"
            );

            bail!(
                ErrorKind::InvalidState,
                "Table replication state not found",
                format!("Replication state missing for table {}", self.table_id)
            );
        };

        info!(
            table_id = self.table_id.0,
            %table_replication_phase,
            "loaded table sync worker state",
        );

        let state = TableSyncWorkerState::new(self.table_id, table_replication_phase);
        let table_id = self.table_id;

        let table_sync_worker_span = tracing::info_span!(
            "table_sync_worker",
            pipeline_id = self.pipeline_id,
            publication_name = self.config.publication_name,
            table_id = self.table_id.0,
        );

        let fut = self
            .guarded_run_table_sync_worker(state.clone())
            .instrument(table_sync_worker_span);

        pool.spawn(table_id, state, fut).await;

        Ok(())
    }

    /// Runs the table sync worker with retry logic and error handling.
    ///
    /// This method implements the retry loop for table synchronization, handling
    /// different error scenarios according to their retry policies. It manages
    /// worker lifecycle, state transitions, and cleanup operations while providing
    /// robust error recovery capabilities.
    async fn guarded_run_table_sync_worker(self, state: TableSyncWorkerState) -> EtlResult<()> {
        let table_id = self.table_id;
        let pool = self.pool.clone();
        let store = self.store.clone();
        let config = self.config.clone();
        let pipeline_id = self.pipeline_id;
        let destination = self.destination.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let run_permit = self.run_permit.clone();

        loop {
            let worker = TableSyncWorker {
                pipeline_id,
                config: config.clone(),
                pool: pool.clone(),
                table_id,
                store: store.clone(),
                destination: destination.clone(),
                shutdown_rx: shutdown_rx.clone(),
                run_permit: run_permit.clone(),
                memory_monitor: self.memory_monitor.clone(),
            };

            let result = worker.run_table_sync_worker(state.clone()).await;
            match result {
                Ok(_) => {
                    let mut state_guard = state.lock().await;
                    state_guard.reset_retry_attempts();

                    return Ok(());
                }
                Err(err) => {
                    let should_shutdown = Self::handle_table_sync_worker_error(
                        pipeline_id,
                        table_id,
                        config.as_ref(),
                        &state,
                        &store,
                        &mut shutdown_rx,
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

    /// Executes the core table synchronization process.
    ///
    /// This method orchestrates the complete table sync workflow: acquiring run
    /// permits, establishing replication connections, performing initial data sync,
    /// running catchup replication, and cleaning up resources. It handles both
    /// the bulk data copy phase and the incremental replication phase.
    async fn run_table_sync_worker(mut self, state: TableSyncWorkerState) -> EtlResult<()> {
        debug!(
            table_id = self.table_id.0,
            "waiting to acquire a running permit for table sync worker"
        );

        // We acquire a permit to run the table sync worker. This helps us limit the number
        // of table sync workers running in parallel which in turn helps limit the max
        // number of concurrent connections to the source database.
        let permit = tokio::select! {
            biased;

            _ = self.shutdown_rx.changed() => {
                info!(table_id = self.table_id.0, "shutting down table sync worker while waiting for a run permit");
                return Ok(());
            }

            permit = self.run_permit.acquire() => {
                permit
            }
        };

        info!(
            table_id = self.table_id.0,
            "acquired running permit for table sync worker"
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
            self.store.clone(),
            self.destination.clone(),
            self.shutdown_rx.clone(),
            self.memory_monitor.clone(),
        )
        .await;

        let start_lsn = match result {
            Ok(TableSyncResult::SyncCompleted { start_lsn }) => start_lsn,
            Ok(TableSyncResult::SyncStopped | TableSyncResult::SyncNotRequired) => {
                return Ok(());
            }
            Err(err) => {
                error!(table_id = self.table_id.0, error = %err, "table sync failed");
                return Err(err);
            }
        };

        let worker_context = WorkerContext::TableSync(TableSyncWorkerContext {
            table_id: self.table_id,
            table_sync_worker_state: state,
            state_store: self.store.clone(),
        });

        let result = ApplyLoop::start(
            self.pipeline_id,
            start_lsn,
            self.config,
            replication_client.clone(),
            self.store,
            self.destination,
            worker_context,
            self.shutdown_rx,
            self.memory_monitor.clone(),
        )
        .await?;

        // If the apply loop was completed, we perform cleanup since resources are not needed anymore.
        if let ApplyLoopResult::Completed = result {
            // We delete the replication slot used by this table sync worker.
            //
            // Note that if the deletion fails, the slot will remain in the database and will not be
            // removed later, so manual intervention will be required. The reason for not implementing
            // an automatic cleanup mechanism is that it would introduce performance overhead,
            // and we expect this call to fail only rarely.
            let slot_name: String =
                EtlReplicationSlot::for_table_sync_worker(self.pipeline_id, self.table_id)
                    .try_into()?;
            replication_client.delete_slot_if_exists(&slot_name).await?;
        }

        // This explicit drop is not strictly necessary but is added to make it extra clear
        // that the scope of the run permit is needed upto here to avoid multiple parallel
        // connections.
        drop(permit);

        info!(
            table_id = self.table_id.0,
            "table sync worker completed successfully"
        );

        Ok(())
    }
}
