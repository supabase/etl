use std::{any::Any, ops::Deref, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use etl_config::shared::PipelineConfig;
use etl_postgres::slots::EtlReplicationSlot;
use futures::FutureExt;
use metrics::counter;
use tokio::{
    sync::{Mutex, MutexGuard, Notify, Semaphore},
    task::AbortHandle,
};
use tracing::{Instrument, debug, error, info, warn};

#[cfg(feature = "failpoints")]
use crate::failpoints::{TABLE_SYNC_WORKER_BEFORE_STREAMING_FP, etl_fail_point};
use crate::{
    bail,
    destination::PipelineDestination,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    observability::{ERROR_TYPE_LABEL, ETL_WORKER_ERRORS_TOTAL, WORKER_TYPE_LABEL},
    pipeline::PipelineId,
    postgres::{OutOfBandSourcePool, client::PgReplicationClient},
    replication::{
        ApplyLoop, ApplyLoopResult, SharedTableCache, TableSyncResult, TableSyncWorkerContext,
        WorkerContext, WorkerType, start_table_sync,
        state::{TableError, TableRetryPolicy, TableState, TableStateType},
    },
    runtime::{
        BatchBudgetController, MemoryMonitor, TableSyncWorkerPool,
        concurrency::{ShutdownResult, ShutdownRx},
        error_policy::{RetryDirective, build_error_handling_policy},
        table_sync::TableSyncWorkerId,
    },
    schema::TableId,
    store::{PipelineStore, StateStore},
};

/// Formats state types without using debug output.
fn format_state_types(state_types: &[TableStateType]) -> String {
    state_types.iter().copied().map(Into::into).collect::<Vec<&'static str>>().join(",")
}

/// Result for a table sync worker task.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum TableSyncWorkerResult {
    /// The worker completed the sync flow successfully.
    Completed,
    /// The worker stopped because shutdown was requested.
    Shutdown,
    /// The worker stopped after persisting the table error state.
    Errored,
}

/// Builds an error from a panic caught inside the table sync worker.
fn table_sync_worker_panic_error(payload: Box<dyn Any + Send>) -> EtlError {
    let detail = if let Some(message) = payload.downcast_ref::<&'static str>() {
        message.to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "Table sync worker panicked with non-string payload".to_owned()
    };

    etl_error!(ErrorKind::TableSyncWorkerPanic, "Table sync worker panicked", detail)
}

/// Internal state of [`TableSyncWorkerState`].
#[derive(Debug)]
pub(crate) struct TableSyncWorkerStateInner {
    /// Unique identifier for the table whose state this structure tracks.
    table_id: TableId,
    /// Current table state, this is the authoritative in-memory state.
    table_state: TableState,
    /// Notification mechanism for notifying state changes to waiting workers.
    state_change: Arc<Notify>,
    /// Number of consecutive automatic retry attempts.
    retry_attempts: u32,
}

impl TableSyncWorkerStateInner {
    /// Updates the table's table state and notifies all waiting workers.
    ///
    /// This method provides the core state transition mechanism for table
    /// synchronization. It atomically updates the in-memory state and
    /// broadcasts the change to any workers that may be waiting for state
    /// transitions.
    pub(crate) fn set(&mut self, state: TableState) {
        info!(
            table_id = self.table_id.0,
            from_state = %self.table_state,
            to_state = %state,
            "table state changing",
        );

        self.table_state = state;

        // Broadcast notification to all active waiters.
        //
        // Note that this notify will not wake up waiters that will be coming in the
        // future since no permit is stored, only active listeners will be
        // notified.
        self.state_change.notify_waiters();
    }

    /// Returns the number of consecutive automatic retry attempts.
    pub(crate) fn retry_attempts(&self) -> u32 {
        self.retry_attempts
    }

    /// Increments the retry attempt counter and returns the updated value.
    pub(crate) fn increment_retry_attempts(&mut self) -> u32 {
        self.retry_attempts = self.retry_attempts.saturating_add(1);
        self.retry_attempts
    }

    /// Resets the automatic retry attempt counter to zero.
    pub(crate) fn reset_retry_attempts(&mut self) {
        self.retry_attempts = 0;
    }

    /// Updates the table's table state with conditional persistence to
    /// external storage.
    ///
    /// This method extends the basic [`TableSyncWorkerStateInner::set()`]
    /// method with durable persistence capabilities, ensuring that
    /// important state transitions survive process restarts and failures.
    ///
    /// The persistence behavior is controlled by the state type's storage
    /// requirements.
    pub(crate) async fn set_and_store<S: StateStore>(
        &mut self,
        state: TableState,
        state_store: &S,
    ) -> EtlResult<()> {
        // Apply in-memory state change first for immediate visibility
        self.set(state.clone());

        // Conditionally persist based on state type requirements
        if state.as_type().should_store() {
            // Persist to external storage - this may fail without affecting in-memory state
            state_store.update_table_state(self.table_id, state).await?;
        }

        Ok(())
    }

    /// Rolls back the table's table state to the previous version.
    ///
    /// This method coordinates rollback operations between persistent storage
    /// and in-memory state. It first queries the state store to retrieve
    /// the previous state, then applies that state to the in-memory
    /// representation and notifies any waiting workers of the change.
    pub(crate) async fn rollback<S: StateStore>(&mut self, state_store: &S) -> EtlResult<()> {
        // We rollback the state in the store and then also set the rolled back state in
        // memory.
        let previous_state = state_store.rollback_table_state(self.table_id).await?;
        self.set(previous_state);

        Ok(())
    }

    /// Returns the current table state for this table.
    ///
    /// This method provides access to the authoritative in-memory state without
    /// requiring coordination with external storage. The returned state
    /// represents the most current state as seen by the local worker.
    pub(crate) fn table_state(&self) -> TableState {
        self.table_state.clone()
    }
}

/// Thread-safe handle for table synchronization worker state management.
///
/// [`TableSyncWorkerState`] provides a thread-safe wrapper around table
/// synchronization state, enabling multiple workers to coordinate and react to
/// state changes. It serves as the primary coordination mechanism between table
/// sync workers and apply workers.
///
/// The state handle supports atomic updates, notifications, and blocking waits
/// for specific state transitions, making it suitable for complex multi-worker
/// scenarios.
#[derive(Debug, Clone)]
pub(crate) struct TableSyncWorkerState {
    inner: Arc<Mutex<TableSyncWorkerStateInner>>,
}

impl TableSyncWorkerState {
    /// Creates a new table sync worker state with the given initial state.
    ///
    /// This constructor initializes the state management structure with the
    /// specified table ID and table state. It sets up the notification
    /// mechanism for coordinating state changes between workers.
    fn new(table_id: TableId, table_state: TableState) -> Self {
        let inner = TableSyncWorkerStateInner {
            table_id,
            table_state,
            state_change: Arc::new(Notify::new()),
            retry_attempts: 0,
        };

        Self { inner: Arc::new(Mutex::new(inner)) }
    }

    /// Waits for the table to reach a specific table state type.
    ///
    /// This method blocks until either the table reaches one of the desired
    /// states or a shutdown signal is received. It uses an efficient
    /// notification system to avoid polling and provides immediate response
    /// to state changes.
    pub(crate) async fn wait_for_state_type(
        &self,
        target_state_types: &[TableStateType],
        mut shutdown_rx: ShutdownRx,
    ) -> ShutdownResult<MutexGuard<'_, TableSyncWorkerStateInner>, ()> {
        loop {
            let inner = self.inner.lock().await;

            let current_state = inner.table_state.as_type();
            if target_state_types.contains(&current_state) {
                info!(
                    table_id = inner.table_id.0,
                    current_table_state_type = %current_state,
                    "table state reached",
                );

                return ShutdownResult::Ok(inner);
            }

            info!(
                table_id = inner.table_id.0,
                current_table_state_type = %current_state,
                target_table_state_types = %format_state_types(target_state_types),
                "waiting for table state",
            );

            // We listen for the state change while holding the lock to avoid the race
            // condition which occurs when we release the lock, the value
            // changes, and then we wait for a value change, in that case, we
            // will miss the notification and the system will stall.
            let state_change = Arc::clone(&inner.state_change);
            let state_change_notified = state_change.notified();

            // We must drop the lock here so that state changes can actually happen.
            drop(inner);

            tokio::select! {
                biased;

                _ = shutdown_rx.changed() => {
                    info!(
                        target_table_state_types = %format_state_types(target_state_types),
                        "shutdown signal received, cancelling wait for state",
                    );

                    return ShutdownResult::Shutdown(());
                }

                _ = state_change_notified => {
                    // State changed, loop to check if it's the desired state.
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
pub(crate) struct TableSyncWorkerHandle {
    worker_id: TableSyncWorkerId,
    state: TableSyncWorkerState,
    abort_handle: AbortHandle,
}

impl TableSyncWorkerHandle {
    /// Creates a new handle with the given worker ID, state, and abort handle.
    pub(crate) fn new(
        worker_id: TableSyncWorkerId,
        state: TableSyncWorkerState,
        abort_handle: AbortHandle,
    ) -> Self {
        Self { worker_id, state, abort_handle }
    }

    /// Returns the unique identifier for this worker run.
    pub(crate) fn worker_id(&self) -> TableSyncWorkerId {
        self.worker_id
    }

    /// Returns the worker's state.
    pub(crate) fn state(&self) -> TableSyncWorkerState {
        self.state.clone()
    }

    /// Checks if the worker task has finished.
    pub(crate) fn is_finished(&self) -> bool {
        self.abort_handle.is_finished()
    }

    /// Aborts the worker task.
    pub(crate) fn abort(&self) {
        self.abort_handle.abort();
    }
}

/// Worker responsible for synchronizing individual tables from Postgres to
/// destinations.
///
/// [`TableSyncWorker`] handles the complete lifecycle of table synchronization,
/// including initial data copying, incremental catchup, and coordination with
/// apply workers. Each worker is responsible for a single table and manages its
/// own replication slot.
///
/// The worker coordinates with the apply worker through shared state and
/// implements sophisticated retry and error handling logic to ensure robust
/// operation.
#[derive(Debug)]
pub(crate) struct TableSyncWorker<S, D> {
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    pool: Arc<TableSyncWorkerPool>,
    table_id: TableId,
    store: S,
    destination: D,
    shared_table_cache: SharedTableCache,
    out_of_band_source_pool: OutOfBandSourcePool,
    shutdown_rx: ShutdownRx,
    run_permit: Arc<Semaphore>,
    memory_monitor: MemoryMonitor,
    batch_budget: BatchBudgetController,
}

impl<S, D> TableSyncWorker<S, D> {
    /// Creates a new table sync worker with the given configuration and
    /// dependencies.
    ///
    /// This constructor initializes all necessary components for table
    /// synchronization, including coordination channels, resource permits,
    /// and storage interfaces. The worker is ready to start synchronization
    /// upon creation.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        pipeline_id: PipelineId,
        config: Arc<PipelineConfig>,
        pool: Arc<TableSyncWorkerPool>,
        table_id: TableId,
        store: S,
        destination: D,
        shared_table_cache: SharedTableCache,
        out_of_band_source_pool: OutOfBandSourcePool,
        shutdown_rx: ShutdownRx,
        run_permit: Arc<Semaphore>,
        memory_monitor: MemoryMonitor,
        batch_budget: BatchBudgetController,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            pool,
            table_id,
            store,
            destination,
            shared_table_cache,
            out_of_band_source_pool,
            shutdown_rx,
            run_permit,
            memory_monitor,
            batch_budget,
        }
    }
}

impl<S, D> TableSyncWorker<S, D>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    /// Handles a table sync worker failure using the configured retry policy.
    ///
    /// Returns [`Some(TableSyncWorkerResult)`] when error handling terminates
    /// the worker, [`None`] when the worker should retry, or [`Err`] when
    /// the failure cannot be handled and must be propagated.
    ///
    /// Any errors encountered while persisting error state or preparing a retry
    /// are propagated immediately.
    async fn handle_table_sync_worker_error(
        table_id: TableId,
        config: &PipelineConfig,
        state: &TableSyncWorkerState,
        store: &S,
        shutdown_rx: &mut ShutdownRx,
        err: EtlError,
    ) -> EtlResult<Option<TableSyncWorkerResult>> {
        error!(table_id = table_id.0, error = %err, "table sync worker failed");

        // Build a retry policy from the shared classifier. The concrete retry timestamp
        // is computed in the worker from config so both table sync and apply
        // worker use the same retry timing settings.
        let policy = build_error_handling_policy(&err);
        let mut retry_policy = match policy.retry_directive() {
            RetryDirective::Timed => TableRetryPolicy::retry_in(ChronoDuration::milliseconds(
                config.table_error_retry_delay_ms as i64,
            )),
            RetryDirective::Manual => TableRetryPolicy::ManualRetry,
            RetryDirective::NoRetry => TableRetryPolicy::NoRetry,
        };
        let mut table_error = TableError::from_error_policy(&err, &policy, retry_policy.clone());

        let mut state_guard = state.lock().await;

        // If we should retry this error, we want to see if we reached the maximum
        // number of attempts before trying again. If we did, we switch to a
        // manual retry policy.
        if policy.should_retry()
            && state_guard.retry_attempts() >= config.table_error_retry_max_attempts
        {
            warn!(
                table_id = table_id.0,
                max_attempts = config.table_error_retry_max_attempts,
                "max automatic retry attempts reached, switching to manual retry"
            );

            table_error = table_error.with_retry_policy(TableRetryPolicy::ManualRetry);
            retry_policy = table_error.retry_policy().clone();
        }

        let error_type = match retry_policy {
            TableRetryPolicy::TimedRetry { .. } => "timed",
            TableRetryPolicy::ManualRetry => "manual",
            TableRetryPolicy::NoRetry => "no_retry",
        };
        counter!(
            ETL_WORKER_ERRORS_TOTAL,
            WORKER_TYPE_LABEL => "table_sync",
            ERROR_TYPE_LABEL => error_type,
        )
        .increment(1);

        // Update the state and store with the error. This way the user is notified
        // about the current error state.
        //
        // Errors from persisting this state must still propagate: a table sync error is
        // only considered handled once it has been durably reflected in the
        // state store.
        state_guard.set_and_store(table_error.into(), store).await?;

        match retry_policy {
            TableRetryPolicy::TimedRetry { next_retry } => {
                let now = Utc::now();
                let mut should_shutdown = false;

                if now < next_retry {
                    let sleep_duration =
                        (next_retry - now).to_std().unwrap_or(Duration::from_secs(0));

                    info!(
                        table_id = table_id.0,
                        sleep_duration_ms = sleep_duration.as_millis(),
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

                    return Ok(Some(TableSyncWorkerResult::Shutdown));
                }

                // We mark that we attempted a retry.
                state_guard.increment_retry_attempts();

                // After sleeping, we rollback to the previous state and retry.
                // Rollback failures must propagate because they leave retry state inconsistent.
                //
                // Note that this rollback is one state before which works only if we are
                // in a table sync worker, this is why it's not in the apply worker:
                // - Errored -> Init: okay since it will restart from scratch.
                // - Errored -> DataSync: okay since it will restart the copy from a new slot.
                // - Errored -> FinishedCopy: okay since table sync startup treats it as a clean
                //   copy restart.
                // - Errored -> SyncDone: okay since the table sync will immediately stop.
                // - Errored -> Ready: same as SyncDone.
                //
                // The in-memory states like SyncWait and Catchup won't ever be in a rollback
                // since they are just states used for synchronization and never saved in the
                // state store.
                state_guard.rollback(store).await?;

                Ok(None)
            }
            TableRetryPolicy::NoRetry | TableRetryPolicy::ManualRetry => {
                state_guard.reset_retry_attempts();

                info!(
                    table_id = table_id.0,
                    "table sync worker stopped after persisting terminal error state"
                );

                Ok(Some(TableSyncWorkerResult::Errored))
            }
        }
    }

    /// Spawns the table sync worker into the pool.
    ///
    /// This method initializes the worker by loading its table state from
    /// storage, creating the state management structure, and spawning the
    /// synchronization process into the pool.
    pub(crate) async fn spawn_into_pool(self, pool: &TableSyncWorkerPool) -> EtlResult<()> {
        debug!(table_id = self.table_id.0, "starting table sync worker");

        let Some(table_state) = self.store.get_table_state(self.table_id).await? else {
            bail!(
                ErrorKind::InvalidState,
                "Table state not found",
                format!("Table state missing for table {}", self.table_id)
            );
        };

        info!(
            table_id = self.table_id.0,
            %table_state,
            "loaded table sync worker state",
        );

        let state = TableSyncWorkerState::new(self.table_id, table_state);
        let table_id = self.table_id;

        let table_sync_worker_span = tracing::info_span!(
            "table_sync_worker",
            pipeline_id = self.pipeline_id,
            publication_name = self.config.publication_name,
            table_id = self.table_id.0,
        );

        let fut = self.run_table_sync_worker_task(state.clone(), table_sync_worker_span);

        pool.spawn(table_id, state, fut).await;

        Ok(())
    }

    /// Runs the table sync worker task and converts panics to table errors.
    async fn run_table_sync_worker_task(
        self,
        state: TableSyncWorkerState,
        table_sync_worker_span: tracing::Span,
    ) -> EtlResult<TableSyncWorkerResult> {
        let table_id = self.table_id;
        let config = Arc::clone(&self.config);
        let store = self.store.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        let result = AssertUnwindSafe(
            self.guarded_run_table_sync_worker(state.clone()).instrument(table_sync_worker_span),
        )
        .catch_unwind()
        .await;

        match result {
            Ok(result) => result,
            Err(payload) => {
                let err = table_sync_worker_panic_error(payload);
                Self::handle_table_sync_worker_error(
                    table_id,
                    config.as_ref(),
                    &state,
                    &store,
                    &mut shutdown_rx,
                    err,
                )
                .await?
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Table sync worker panic requested retry after unwinding"
                    )
                })
            }
        }
    }

    /// Runs the table sync worker with retry logic and error handling.
    ///
    /// This method implements the retry loop for table synchronization,
    /// handling different error scenarios according to their retry
    /// policies. It manages worker lifecycle, state transitions, and
    /// cleanup operations while providing robust error recovery
    /// capabilities.
    async fn guarded_run_table_sync_worker(
        self,
        state: TableSyncWorkerState,
    ) -> EtlResult<TableSyncWorkerResult> {
        let table_id = self.table_id;
        let pool = Arc::clone(&self.pool);
        let store = self.store.clone();
        let config = Arc::clone(&self.config);
        let pipeline_id = self.pipeline_id;
        let destination = self.destination.clone();
        let shared_table_cache = self.shared_table_cache.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let run_permit = Arc::clone(&self.run_permit);

        loop {
            let worker = TableSyncWorker {
                pipeline_id,
                config: Arc::clone(&config),
                pool: Arc::clone(&pool),
                table_id,
                store: store.clone(),
                destination: destination.clone(),
                shared_table_cache: shared_table_cache.clone(),
                out_of_band_source_pool: self.out_of_band_source_pool.clone(),
                shutdown_rx: shutdown_rx.clone(),
                run_permit: Arc::clone(&run_permit),
                memory_monitor: self.memory_monitor.clone(),
                batch_budget: self.batch_budget.clone(),
            };

            let result = worker.run_table_sync_worker(state.clone()).await;

            match result {
                Ok(result) => {
                    let mut state_guard = state.lock().await;
                    state_guard.reset_retry_attempts();

                    return Ok(result);
                }
                Err(err) => {
                    let result = Self::handle_table_sync_worker_error(
                        table_id,
                        config.as_ref(),
                        &state,
                        &store,
                        &mut shutdown_rx,
                        err,
                    )
                    .await?;

                    if let Some(result) = result {
                        return Ok(result);
                    }
                }
            }
        }
    }

    /// Executes the core table synchronization process.
    ///
    /// This method orchestrates the complete table sync workflow: acquiring run
    /// permits, establishing replication connections, performing initial data
    /// sync, running catchup replication, and cleaning up resources. It
    /// handles both the bulk data copy state and the incremental
    /// table state.
    async fn run_table_sync_worker(
        mut self,
        state: TableSyncWorkerState,
    ) -> EtlResult<TableSyncWorkerResult> {
        debug!(
            table_id = self.table_id.0,
            "waiting to acquire a running permit for table sync worker"
        );

        // We acquire a permit to run the table sync worker. This helps us limit the
        // number of table sync workers running in parallel which in turn helps
        // limit the max number of concurrent connections to the source
        // database.
        let _permit = tokio::select! {
            biased;

            _ = self.shutdown_rx.changed() => {
                info!(table_id = self.table_id.0, "shutting down table sync worker while waiting for a run permit");

                return Ok(TableSyncWorkerResult::Shutdown);
            }

            // We use `acquired_owned` for better semantics over `acquire` since we want to own the
            // permit in this future.
            permit = Arc::clone(&self.run_permit).acquire_owned() => {
                permit
            }
        }
        .map_err(|err| {
            etl_error!(
                ErrorKind::InvalidState,
                "Table sync worker semaphore closed while acquiring run permit",
                err
            )
        })?;

        info!(table_id = self.table_id.0, "acquired running permit for table sync worker");

        // Keep the owned permit alive for the full worker run so concurrency stays
        // bounded until the worker fully exits.

        // We create a new replication connection specifically for this table sync
        // worker.
        //
        // Note that this connection must be tied to the lifetime of this worker,
        // otherwise there will be problems when cleaning up the replication
        // slot.
        let mut replication_client = PgReplicationClient::connect_for_table_sync_worker(
            self.config.pg_connection.clone(),
            self.pipeline_id,
            self.table_id,
        )
        .await?;

        let table_sync_result = start_table_sync(
            self.pipeline_id,
            Arc::clone(&self.config),
            &mut replication_client,
            self.table_id,
            state.clone(),
            self.store.clone(),
            self.destination.clone(),
            &self.shared_table_cache,
            self.out_of_band_source_pool.clone(),
            self.shutdown_rx.clone(),
            self.memory_monitor.clone(),
            self.batch_budget.clone(),
        )
        .await;

        let start_lsn = match table_sync_result {
            Ok(TableSyncResult::Completed { start_lsn }) => start_lsn,
            Ok(TableSyncResult::Stopped) => {
                info!(table_id = self.table_id.0, "table sync was stopped");

                return Ok(TableSyncWorkerResult::Shutdown);
            }
            Ok(TableSyncResult::NotRequired) => {
                info!(table_id = self.table_id.0, "table sync was not required");

                return Ok(TableSyncWorkerResult::Completed);
            }
            Err(err) => {
                return Err(err);
            }
        };

        // Let tests fail after the apply worker has entered `Catchup`, so they
        // cover the apply worker unblocking on `Errored`.
        #[cfg(feature = "failpoints")]
        etl_fail_point(TABLE_SYNC_WORKER_BEFORE_STREAMING_FP)?;

        let worker_context = WorkerContext::TableSync(TableSyncWorkerContext {
            table_id: self.table_id,
            table_sync_worker_state: state,
            state_store: self.store.clone(),
        });

        let _apply_loop_stream_guard = self.batch_budget.register_stream_load(1);
        let apply_loop_result = ApplyLoop::start(
            self.pipeline_id,
            start_lsn,
            Arc::clone(&self.config),
            &replication_client,
            self.store.clone(),
            self.destination.clone(),
            self.shared_table_cache.clone(),
            self.out_of_band_source_pool.clone(),
            worker_context,
            self.shutdown_rx.clone(),
            self.memory_monitor.clone(),
            self.batch_budget.clone(),
        )
        .await?;

        match apply_loop_result {
            ApplyLoopResult::Completed => {
                info!(
                    table_id = self.table_id.0,
                    "table sync apply loop completed successfully, deleting slot"
                );

                // Catchup has completed, so cleanup failures should not turn a
                // completed table sync into a replication failure.
                if let Err(err) =
                    self.cleanup_resources(&replication_client, self.store.clone()).await
                {
                    warn!(
                        table_id = self.table_id.0,
                        error = %err,
                        "failed to clean up table sync resources after completion"
                    );
                }

                Ok(TableSyncWorkerResult::Completed)
            }
            ApplyLoopResult::Paused => {
                info!(table_id = self.table_id.0, "table sync apply loop paused for shutdown");

                Ok(TableSyncWorkerResult::Shutdown)
            }
        }
    }

    /// Cleans up resources owned by this table sync worker.
    ///
    /// Once the table sync apply loop completes, its durable progress row is no
    /// longer needed for resume and the replication slot should be removed so
    /// it stops retaining WAL.
    ///
    /// Progress is deleted first so a slot deletion failure leaves only a stale
    /// slot behind. That can retain WAL and may require manual cleanup, but it
    /// does not leave stale progress for a completed table sync worker.
    async fn cleanup_resources(
        &self,
        replication_client: &PgReplicationClient,
        store: S,
    ) -> EtlResult<()> {
        store
            .delete_replication_progress(WorkerType::TableSync { table_id: self.table_id })
            .await?;

        let slot_name: String =
            EtlReplicationSlot::for_table_sync_worker(self.pipeline_id, self.table_id)
                .try_into()?;
        replication_client.delete_slot_if_exists(&slot_name).await?;

        Ok(())
    }
}
