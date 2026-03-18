//! Apply loop implementation for processing PostgreSQL logical replication events.
//!
//! This module provides the core apply loop that processes replication events from
//! PostgreSQL and coordinates table synchronization. It uses a [`WorkerContext`] enum
//! to enable different behavior based on the worker type (apply worker vs table sync
//! worker) at various points in the replication cycle.

use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::EtlReplicationSlot;
use etl_postgres::types::TableId;
use futures::StreamExt;
use metrics::{counter, histogram};
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use tokio::pin;
use tokio::sync::Semaphore;
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info, warn};

use crate::concurrency::batch_budget::{BatchBudgetController, CachedBatchBudget};
use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::concurrency::stream::BackpressureStream;
use crate::conversions::event::{
    parse_event_from_begin_message, parse_event_from_commit_message,
    parse_event_from_delete_message, parse_event_from_insert_message,
    parse_event_from_relation_message, parse_event_from_truncate_message,
    parse_event_from_update_message,
};
use crate::destination::{
    BatchFlushMetrics, BatchFlushResult, CompletedBatchFlushResult, Destination,
    PendingBatchFlushResult,
};
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::metrics::{
    ACTION_LABEL, DESTINATION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
    ETL_EVENTS_PROCESSED_TOTAL, ETL_TRANSACTION_DURATION_SECONDS, ETL_TRANSACTION_SIZE,
    ETL_TRANSACTIONS_TOTAL, PIPELINE_ID_LABEL, WORKER_TYPE_LABEL,
};
use crate::replication::client::{PgReplicationClient, PostgresConnectionUpdate};
use crate::replication::stream::{EventsStream, StatusUpdateType};
use crate::state::table::{
    RetryPolicy, TableReplicationError, TableReplicationPhase, TableReplicationPhaseType,
};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::{Event, PipelineId, SizeHint};
use crate::workers::pool::TableSyncWorkerPool;
use crate::workers::table_sync::{TableSyncWorker, TableSyncWorkerState};
use crate::{bail, etl_error};

/// Type of worker driving the apply loop.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    /// The main apply worker that coordinates table sync workers.
    Apply,
    /// A table sync worker that synchronizes a specific table.
    TableSync {
        /// The table being synchronized.
        table_id: TableId,
    },
}

impl WorkerType {
    /// Builds an [`EtlReplicationSlot`] for this worker type.
    pub fn build_etl_replication_slot(&self, pipeline_id: u64) -> EtlReplicationSlot {
        match self {
            Self::Apply => EtlReplicationSlot::Apply { pipeline_id },
            Self::TableSync { table_id } => EtlReplicationSlot::TableSync {
                pipeline_id,
                table_id: *table_id,
            },
        }
    }
}

impl Display for WorkerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Apply => write!(f, "apply"),
            Self::TableSync { table_id } => write!(f, "table_sync({table_id})"),
        }
    }
}

/// Result type for the apply loop execution.
///
/// Indicates the reason why the apply loop terminated, enabling appropriate
/// cleanup and error handling by the caller.
#[derive(Debug, Copy, Clone)]
pub enum ApplyLoopResult {
    /// The apply loop was paused and could be resumed in the future.
    Paused,
    /// The apply loop was completed and will never be invoked again.
    Completed,
}

/// Action that should be taken during the apply loop.
///
/// An action defines what to do after one iteration of the apply loop.
#[derive(Debug, Copy, Clone, Default)]
pub enum ApplyLoopAction {
    /// The apply loop can continue on the next element.
    #[default]
    Continue,
    /// The apply loop should pause processing.
    Pause,
    /// The apply loop should stop processing because it has completed.
    Complete,
}

impl ApplyLoopAction {
    /// Builds an [`ApplyLoopResult`] given this [`ApplyLoopAction`].
    ///
    /// Returns [`Some`] if the action can lead to a result of the loop, [`None`]
    /// otherwise.
    pub fn to_result(self) -> Option<ApplyLoopResult> {
        match self {
            Self::Continue => None,
            Self::Pause => Some(ApplyLoopResult::Paused),
            Self::Complete => Some(ApplyLoopResult::Completed),
        }
    }

    /// Returns `true` if the apply loop action is terminating the loop, `false` otherwise.
    pub fn is_terminating(&self) -> bool {
        match self {
            Self::Continue => false,
            Self::Pause | Self::Complete => true,
        }
    }

    /// Merges two [`ApplyLoopAction`]s with the following priorities:
    /// 1. Complete
    /// 2. Pause
    /// 3. Continue
    pub fn merge(self, other: Self) -> Self {
        match self {
            Self::Continue => match other {
                Self::Continue => Self::Continue,
                Self::Pause => Self::Pause,
                Self::Complete => Self::Complete,
            },
            Self::Pause => match other {
                Self::Continue => Self::Pause,
                Self::Pause => Self::Pause,
                Self::Complete => Self::Complete,
            },
            Self::Complete => Self::Complete,
        }
    }
}

/// Represents the shutdown state of the apply loop.
///
/// Tracks the progress of a graceful shutdown, ensuring that PostgreSQL acknowledges
/// our flush position before we terminate to prevent data duplication on restart.
#[derive(Debug, Clone)]
pub enum ShutdownState {
    /// No shutdown requested, normal operation.
    NoShutdown,
    /// Shutdown in progress, waiting for PostgreSQL to acknowledge our flush position.
    /// The loop will only process keepalive messages until one arrives with `wal_end >= acked_flush_lsn`.
    WaitingForPrimaryKeepAlive {
        /// The LSN we sent in the status update that PostgreSQL should acknowledge.
        acked_flush_lsn: PgLsn,
    },
}

impl ShutdownState {
    /// Returns `true` if a shutdown has been requested.
    pub fn is_requested(&self) -> bool {
        !matches!(self, Self::NoShutdown)
    }
}

/// Resources for the apply worker during the apply loop.
///
/// Contains all state and dependencies needed by the apply worker to coordinate
/// with table sync workers and manage table lifecycle transitions.
pub struct ApplyWorkerContext<S, D> {
    /// Unique identifier for the pipeline.
    pub pipeline_id: PipelineId,
    /// Shared configuration for all coordinated operations.
    pub config: Arc<PipelineConfig>,
    /// Pool of table sync workers that this worker coordinates.
    pub pool: Arc<TableSyncWorkerPool>,
    /// State store for tracking table replication progress.
    pub store: S,
    /// Destination where replicated data is written.
    pub destination: D,
    /// Shutdown signal receiver for graceful termination.
    pub shutdown_rx: ShutdownRx,
    /// Semaphore controlling maximum concurrent table sync workers.
    pub table_sync_worker_permits: Arc<Semaphore>,
    /// Shared memory backpressure controller.
    pub memory_monitor: MemoryMonitor,
    /// Shared batch budget controller.
    pub batch_budget: BatchBudgetController,
}

/// Resources for the table sync worker during the apply loop.
///
/// Contains state and dependencies needed by a table sync worker to track
/// its synchronization progress and coordinate with the apply worker.
pub struct TableSyncWorkerContext<S> {
    /// Unique identifier for the table being synchronized.
    pub table_id: TableId,
    /// Thread-safe state management for this worker.
    pub table_sync_worker_state: TableSyncWorkerState,
    /// State store for persisting replication progress.
    pub state_store: S,
}

/// Context for the worker driving the apply loop.
///
/// This enum replaces the [`ApplyLoopHook`] trait, providing direct access to
/// worker-specific resources and enabling different behavior based on the
/// worker type at various points in the replication cycle.
pub enum WorkerContext<S, D> {
    /// Context for the apply worker.
    Apply(ApplyWorkerContext<S, D>),
    /// Context for a table sync worker.
    TableSync(TableSyncWorkerContext<S>),
}

impl<S, D> WorkerContext<S, D> {
    /// Returns the [`WorkerType`] for this context.
    pub fn worker_type(&self) -> WorkerType {
        match self {
            Self::Apply(_) => WorkerType::Apply,
            Self::TableSync(ctx) => WorkerType::TableSync {
                table_id: ctx.table_id,
            },
        }
    }
}

/// Tracks the progress of logical replication from PostgreSQL.
#[derive(Debug, Clone)]
struct ReplicationProgress {
    /// The highest LSN received from PostgreSQL so far.
    last_received_lsn: PgLsn,
    /// The highest commit LSN that has been durably flushed to the destination.
    last_flush_lsn: PgLsn,
}

impl ReplicationProgress {
    /// Updates the last received LSN to a higher value if the new LSN is greater.
    fn update_last_received_lsn(&mut self, new_lsn: PgLsn) {
        if new_lsn <= self.last_received_lsn {
            return;
        }

        debug_assert!(self.last_received_lsn >= self.last_flush_lsn);

        self.last_received_lsn = new_lsn;
    }

    /// Updates the last flush LSN to a higher value if the new LSN is greater.
    fn update_last_flush_lsn(&mut self, new_lsn: PgLsn) {
        if new_lsn <= self.last_flush_lsn {
            return;
        }

        debug_assert!(self.last_received_lsn >= self.last_flush_lsn);

        self.last_flush_lsn = new_lsn;
    }
}

/// An enum representing if the batch should be ended or not.
#[derive(Debug)]
enum EndBatch {
    /// The batch should include the last processed event and end.
    Inclusive,
    /// The batch should exclude the last processed event and end.
    Exclusive,
}

/// Result returned from [`ApplyLoop::handle_replication_message`] and related functions.
#[derive(Debug, Default)]
struct HandleMessageResult {
    /// The event converted from the replication message.
    event: Option<Event>,
    /// Set to a commit message's end_lsn value, [`None`] otherwise.
    end_lsn: Option<PgLsn>,
    /// Set when a batch should be ended earlier than the normal batching parameters.
    end_batch: Option<EndBatch>,
    /// Set when the table has encountered an error.
    table_replication_error: Option<TableReplicationError>,
    /// The action that this event should have on the loop.
    action: ApplyLoopAction,
}

impl HandleMessageResult {
    /// Creates a result with no event and no side effects.
    fn no_event() -> Self {
        Self::default()
    }

    /// Creates a result that returns an event without affecting batch state.
    fn return_event(event: Event) -> Self {
        Self {
            event: Some(event),
            ..Default::default()
        }
    }

    /// Creates a result that excludes the current event and requests batch termination.
    fn finish_batch_and_exclude_event(error: TableReplicationError) -> Self {
        Self {
            end_batch: Some(EndBatch::Exclusive),
            table_replication_error: Some(error),
            ..Default::default()
        }
    }
}

/// A shared state that is used throughout the apply loop to track progress.
#[derive(Debug)]
struct ApplyLoopState {
    /// The highest LSN received from the [`end_lsn`] field of a [`Commit`] message.
    last_commit_end_lsn: Option<PgLsn>,
    /// The LSN of the commit WAL entry of the transaction that is currently being processed.
    remote_final_lsn: Option<PgLsn>,
    /// The current replication progress tracking received and flushed LSN positions.
    replication_progress: ReplicationProgress,
    /// A batch of events to send to the destination.
    events_batch: Vec<Event>,
    /// Approximate total size in bytes of events currently in the batch.
    events_batch_bytes: usize,
    /// Instant from when a transaction began.
    current_tx_begin_ts: Option<Instant>,
    /// Number of events observed in the current transaction (excluding BEGIN/COMMIT).
    current_tx_events: u64,
    /// Next zero-based ordinal to assign to transaction-scoped events.
    next_tx_ordinal: u64,
    /// The current shutdown state tracking graceful shutdown progress.
    shutdown_state: ShutdownState,
    /// The deadline by which the current batch must be flushed.
    flush_deadline: Option<Instant>,
    /// The maximum duration to wait before forcibly flushing a batch.
    max_batch_fill_duration: Duration,
    /// Destination write result waiting to be applied to replication progress.
    pending_flush_result: Option<PendingBatchFlushResult<()>>,
    /// The action that is being accumulated from multiple code paths until we are ready to act on
    /// it.
    pending_action: Option<ApplyLoopAction>,
    /// Set to `true` after a terminating action so the loop never accepts new messages again.
    ///
    /// This tracks both [`ApplyLoopAction::Pause`] and [`ApplyLoopAction::Complete`], since both
    /// require stopping message intake forever.
    processing_stopped: bool,
    /// Set to `true` when a flush was deferred because another flush result was still in flight.
    processing_paused: bool,
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial replication progress and event batch.
    fn new(replication_progress: ReplicationProgress, max_batch_fill_duration: Duration) -> Self {
        Self {
            last_commit_end_lsn: None,
            remote_final_lsn: None,
            replication_progress,
            events_batch: Vec::new(),
            events_batch_bytes: 0,
            current_tx_begin_ts: None,
            current_tx_events: 0,
            next_tx_ordinal: 0,
            shutdown_state: ShutdownState::NoShutdown,
            flush_deadline: None,
            max_batch_fill_duration,
            pending_flush_result: None,
            pending_action: None,
            processing_stopped: false,
            processing_paused: false,
        }
    }

    /// Adds an event to the batch.
    fn add_event_to_batch(&mut self, event: Event) {
        // We track the current number of bytes in the batch.
        self.events_batch_bytes = self.events_batch_bytes.saturating_add(event.size_hint());

        // We add the element to the pending batch.
        self.events_batch.push(event);
    }

    /// Takes the events batch for further processing. Replacing it with a new empty batch.
    fn take_events_batch(&mut self) -> (Vec<Event>, usize) {
        let events_batch = std::mem::take(&mut self.events_batch);
        let events_batch_bytes = self.events_batch_bytes;
        self.events_batch_bytes = 0;

        (events_batch, events_batch_bytes)
    }

    /// Sets the batch flush deadline, if not already set.
    fn set_flush_deadline_if_needed(&mut self) {
        if self.flush_deadline.is_some() {
            return;
        }

        self.flush_deadline = Some(Instant::now() + self.max_batch_fill_duration);

        debug!("started batch flush timer");
    }

    /// Resets the batch flush deadline.
    fn reset_flush_deadline(&mut self) {
        self.flush_deadline = None;

        debug!("reset batch flush timer");
    }

    /// Updates the last commit end LSN to track transaction boundaries.
    fn update_last_commit_end_lsn(&mut self, end_lsn: Option<PgLsn>) {
        match (self.last_commit_end_lsn, end_lsn) {
            (None, Some(end_lsn)) => {
                self.last_commit_end_lsn = Some(end_lsn);
            }
            (Some(old_last_commit_end_lsn), Some(end_lsn)) => {
                if end_lsn > old_last_commit_end_lsn {
                    self.last_commit_end_lsn = Some(end_lsn);
                }
            }
            (_, None) => {}
        }
    }

    /// Returns the last received LSN that should be reported as written to the PostgreSQL server.
    fn last_received_lsn(&self) -> PgLsn {
        self.replication_progress.last_received_lsn
    }

    /// Returns `true` if the apply loop is totally idle.
    fn is_idle(&self) -> bool {
        !self.handling_transaction()
            && !self.has_pending_batch()
            && !self.has_pending_flush_result()
    }

    /// Returns the effective flush LSN to report to PostgreSQL.
    ///
    /// When idle, returns the last received LSN since no actual flushes occur. Otherwise,
    /// returns the last flush LSN from completed transactions.
    ///
    /// Note that when a new transaction starts, the last flush LSN will be used and it may appear
    /// to move backward relative to the last received LSN reported earlier. This is fine because
    /// the status update logic guarantees monotonically increasing LSNs.
    fn effective_flush_lsn(&self) -> PgLsn {
        if self.is_idle() {
            self.replication_progress.last_received_lsn
        } else {
            self.replication_progress.last_flush_lsn
        }
    }

    /// Returns true if the apply loop is in the middle of processing a transaction.
    fn handling_transaction(&self) -> bool {
        self.remote_final_lsn.is_some()
    }

    /// Resets transaction-local ordinal assignment.
    fn reset_tx_ordinal(&mut self) {
        self.next_tx_ordinal = 0;
    }

    /// Returns and advances the next transaction-local ordinal.
    fn next_tx_ordinal(&mut self) -> u64 {
        let tx_ordinal = self.next_tx_ordinal;
        self.next_tx_ordinal = match self.next_tx_ordinal.checked_add(1) {
            Some(next_tx_ordinal) => next_tx_ordinal,
            None => {
                warn!(
                    current_tx_ordinal = self.next_tx_ordinal,
                    "transaction-local ordinal overflow detected; subsequent events may reuse the same ordinal"
                );

                self.next_tx_ordinal
            }
        };

        tx_ordinal
    }

    /// Returns `true` if there is a pending batch of events waiting to be flushed.
    fn has_pending_batch(&self) -> bool {
        !self.events_batch.is_empty()
    }

    /// Returns `true` if there is a batch flush in flight whose result has not yet resolved.
    fn has_pending_flush_result(&self) -> bool {
        self.pending_flush_result.is_some()
    }

    /// Merges a new action into the current pending action, if any.
    fn merge_into_pending_action(&mut self, new_pending_action: ApplyLoopAction) {
        self.pending_action = match self.pending_action {
            Some(pending_action) => Some(pending_action.merge(new_pending_action)),
            None => Some(new_pending_action),
        };
    }

    /// Records the latest action so the loop can stop ingesting messages after termination.
    fn note_action(&mut self, action: ApplyLoopAction) {
        self.processing_stopped |= action.is_terminating();
    }

    /// Returns `true` when the apply loop may still accept new replication messages.
    fn can_process_messages(&self) -> bool {
        !self.processing_stopped && !self.processing_paused
    }

    /// Marks the current pending batch as paused behind an in-flight flush.
    fn pause_processing(&mut self) {
        self.processing_paused = true;
    }

    /// Clears the pause caused by an in-flight flush and returns whether it had been set.
    fn resume_processing(&mut self) -> bool {
        std::mem::replace(&mut self.processing_paused, false)
    }
}

/// Main apply loop implementation that processes replication events.
///
/// [`ApplyLoop`] encapsulates all state and logic for the apply loop, providing
/// a struct-based approach instead of the previous function-based approach.
pub struct ApplyLoop<S, D> {
    /// Unique identifier for the pipeline.
    pipeline_id: PipelineId,
    /// Shared configuration.
    config: Arc<PipelineConfig>,
    /// Schema store for table schemas.
    schema_store: S,
    /// Destination where replicated data is written.
    destination: D,
    /// Shutdown signal receiver.
    shutdown_rx: ShutdownRx,
    /// Worker context for worker-specific behavior.
    worker_context: WorkerContext<S, D>,
    /// Shared memory backpressure controller.
    memory_monitor: MemoryMonitor,
    /// Cached dynamic batch budget used to decide flushes by bytes.
    cached_batch_budget: CachedBatchBudget,
    /// Internal loop state.
    state: ApplyLoopState,
}

impl<S, D> ApplyLoop<S, D>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    /// Starts the apply loop for processing replication events.
    ///
    /// This is the main entry point that creates the loop instance and runs it.
    #[expect(clippy::too_many_arguments)]
    pub async fn start(
        pipeline_id: PipelineId,
        start_lsn: PgLsn,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        schema_store: S,
        destination: D,
        worker_context: WorkerContext<S, D>,
        shutdown_rx: ShutdownRx,
        memory_monitor: MemoryMonitor,
        batch_budget: BatchBudgetController,
    ) -> EtlResult<ApplyLoopResult> {
        info!(
            worker_type = %worker_context.worker_type(),
            %start_lsn,
            "starting apply loop",
        );

        let initial_progress = ReplicationProgress {
            last_received_lsn: start_lsn,
            last_flush_lsn: start_lsn,
        };

        let state = ApplyLoopState::new(
            initial_progress,
            Duration::from_millis(config.batch.max_fill_ms),
        );

        let cached_batch_budget = batch_budget.cached();

        let mut apply_loop = Self {
            pipeline_id,
            config: config.clone(),
            schema_store,
            destination,
            shutdown_rx,
            worker_context,
            memory_monitor,
            cached_batch_budget,
            state,
        };

        apply_loop.run(replication_client, start_lsn).await
    }

    /// Runs the main event processing loop.
    async fn run(
        &mut self,
        replication_client: PgReplicationClient,
        start_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopResult> {
        let slot_name: String = self
            .worker_context
            .worker_type()
            .build_etl_replication_slot(self.pipeline_id)
            .try_into()?;

        let logical_replication_stream = replication_client
            .start_logical_replication(&self.config.publication_name, &slot_name, start_lsn)
            .await?;

        let events_stream = EventsStream::wrap(logical_replication_stream, self.pipeline_id);
        let events_stream =
            BackpressureStream::wrap(events_stream, self.memory_monitor.subscribe());
        pin!(events_stream);

        let mut connection_updates_rx = replication_client.connection_updates_rx();

        loop {
            // If waiting for shutdown acknowledgement, the loop is now just waiting for the keep alive
            // message before shutting off.
            if let ShutdownState::WaitingForPrimaryKeepAlive { acked_flush_lsn } =
                self.state.shutdown_state
            {
                let message = events_stream.next().await;
                if self.try_complete_shutdown(message, acked_flush_lsn)? {
                    // Return the pending result (constructed from the action, if any), otherwise default
                    // to pausing.
                    return Ok(self
                        .state
                        .pending_action
                        .unwrap_or(ApplyLoopAction::Pause)
                        .to_result()
                        .unwrap_or(ApplyLoopResult::Paused));
                }

                continue;
            }

            tokio::select! {
                biased;

                // PRIORITY 1: Handle shutdown signals.
                // Shutdown takes highest priority to ensure graceful termination. When received,
                // we immediately send the final status update and wait for PostgreSQL
                // acknowledgement, regardless of the current transaction state.
                _ = self.shutdown_rx.changed() => {
                    self.handle_shutdown_signal(events_stream.as_mut()).await?;
                }

                // PRIORITY 2: Handle PostgreSQL connection lifecycle updates.
                // Closed connections stop the loop and surface a source connection error.
                changed = connection_updates_rx.changed() => {
                    if changed.is_err() {
                        return Err(etl_error!(
                            ErrorKind::SourceConnectionFailed,
                            "postgresql connection updates ended during the apply loop"
                        ));
                    }

                    // We use `borrow_and_update` to avoid the race condition in which there is a change
                    // between the `changed()` notification and the borrow. So that if there was, we mark
                    // the value as seen avoiding `changed()` to be called again in the next iteration.
                    let update = connection_updates_rx.borrow_and_update().clone();
                    match update {
                        PostgresConnectionUpdate::Running => {}
                        PostgresConnectionUpdate::Terminated => {
                            return Err(etl_error!(
                                ErrorKind::SourceConnectionFailed,
                                "postgresql connection terminated during the apply loop"
                            ));
                        }
                        PostgresConnectionUpdate::Errored { error } => {
                            return Err(etl_error!(
                                ErrorKind::SourceConnectionFailed,
                                "postgresql connection errored during the apply loop",
                                error.to_string()
                            ));
                        }
                    }
                }

                // PRIORITY 3: Handle the pending destination write result.
                // This branch polls the stored future by mutable reference, so if another branch wins
                // the select first we keep the pending result in place for the next iteration.
                // After resolving, any paused flush is retried and message processing only resumes
                // if no terminating action has already stopped the loop permanently.
                apply_result = Self::wait_for_flush_result(self.state.pending_flush_result.as_mut()), if self.state.pending_flush_result.is_some() => {
                    let action = self
                        .handle_flush_result(apply_result)
                        .await?;
                    self.state.note_action(action);

                    // If there is a pending flush, we need that to be done before deciding what
                    // to do with the action. Otherwise, we just return the value.
                    if self.state.has_pending_flush_result() {
                        self.state.merge_into_pending_action(action);
                    } else if let Some(result) = action.to_result() {
                        return Ok(result);
                    }
                }

                // PRIORITY 4: Handle batch flush timer expiry.
                // Ensures batches don't wait indefinitely when message rate is low. The timer
                // is started when the first event enters the batch and reset after each flush.
                _ = Self::wait_for_batch_deadline(self.state.flush_deadline), if !self.state.processing_paused => {
                    self.flush_batch("flush deadline reached").await?;
                }

                // PRIORITY 5: Process incoming replication messages from PostgreSQL.
                // This is the main work of the apply loop - receiving WAL events and converting
                // them to destination writes. Has lowest priority so shutdown and flush timer
                // are always handled promptly. Disabled both when a terminating action has already
                // stopped the loop and when a pending batch is waiting behind an in-flight flush.
                maybe_message = events_stream.next(), if self.state.can_process_messages() => {
                    let action = self
                        .handle_stream_message(
                            events_stream.as_mut(),
                            maybe_message,
                            &replication_client,
                        )
                        .await?;
                    self.state.note_action(action);

                    // If there is a pending flush, we need that to be done before deciding what
                    // to do with the action. Otherwise, we just return the value.
                    if self.state.has_pending_flush_result() {
                        self.state.merge_into_pending_action(action);
                    } else if let Some(result) = action.to_result() {
                        return Ok(result);
                    }
                }
            }
        }
    }

    /// Waits for the batch flush deadline if one is set.
    async fn wait_for_batch_deadline(deadline: Option<Instant>) {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
            None => std::future::pending().await,
        }
    }

    /// Checks if a message completes the pending shutdown.
    ///
    /// Returns `true` if the message is a keepalive with `wal_end >= acked_flush_lsn`,
    /// `false` if still waiting for the right keepalive.
    fn try_complete_shutdown(
        &self,
        message: Option<EtlResult<ReplicationMessage<LogicalReplicationMessage>>>,
        acked_flush_lsn: PgLsn,
    ) -> EtlResult<bool> {
        let worker_type = self.worker_context.worker_type();

        let Some(message) = message else {
            warn!(
                %worker_type,
                "replication stream ended while waiting for keepalive acknowledgement",
            );

            bail!(
                ErrorKind::SourceConnectionFailed,
                "Replication stream ended while waiting for keepalive acknowledgement"
            )
        };

        match message? {
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = PgLsn::from(keepalive.wal_end());

                if wal_end >= acked_flush_lsn {
                    info!(
                        %worker_type,
                        %wal_end,
                        %acked_flush_lsn,
                        "received keepalive acknowledgement, safe to shutdown",
                    );
                    return Ok(true);
                }

                // This shouldn't happen in practice because if we did process up to LSN `x` it means
                // next keep alive messages from that must be at least `x`.
                debug!(
                    %worker_type,
                    %wal_end,
                    %acked_flush_lsn,
                    "received keepalive but wal_end < acked_flush_lsn, continuing to wait",
                );
            }
            _ => {
                // Ignore non-keepalive messages while waiting for shutdown acknowledgement.
                // These events will be replayed on restart from the confirmed LSN.
                debug!(
                    %worker_type,
                    "ignoring non-keepalive message while waiting for shutdown acknowledgement",
                );
            }
        }

        Ok(false)
    }

    /// Handles a shutdown signal by transitioning to [`ShutdownState::WaitingForPrimaryKeepAlive`].
    ///
    /// If an in-flight flush result is already ready, it is processed first without waiting.
    ///
    /// The goal of the shutdown procedure is to reduce duplicates on restart as much as possible
    /// by ensuring the replication slot's confirmed LSN reflects the latest flushed data.
    ///
    /// Note: the shutdown system is best-effort. Graceful shutdown may not complete if we are
    /// blocked on non-interruptible code or if keepalive messages never arrive from the server.
    /// It is the responsibility of the caller to forcefully kill the process if shutdown does
    /// not complete within an acceptable timeframe.
    async fn handle_shutdown_signal(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
    ) -> EtlResult<()> {
        let worker_type = self.worker_context.worker_type();

        // If the shutdown was already requested, we silently skip it.
        if self.state.shutdown_state.is_requested() {
            info!(
                %worker_type,
                shutdown_state = ?self.state.shutdown_state,
                "shutdown signal received but already in shutdown state, continuing",
            );

            return Ok(());
        }

        // We try to take the pending flush result and handle it if it's immediately available.
        if let Some(flush_result) = self.try_take_pending_flush_result() {
            info!(
                %worker_type,
                "shutdown signal received with ready in-flight flush result, processing it before shutdown",
            );

            // We handle the result and merge it into the pending action, so that it can be properly
            // returned once the shutdown is completed.
            let action = self.handle_flush_result(flush_result).await?;
            self.state.merge_into_pending_action(action);
        }

        info!(
            %worker_type,
            "shutdown signal received, sending status update and waiting for acknowledgement",
        );

        self.initiate_graceful_shutdown(events_stream.as_mut())
            .await
    }

    /// Initiates graceful shutdown by sending a status update and transitioning to
    /// [`ShutdownState::WaitingForPrimaryKeepAlive`].
    async fn initiate_graceful_shutdown(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
    ) -> EtlResult<()> {
        // Use effective flush LSN to report last received LSN when idle, since
        // last flush LSN only advances during actual flushes.
        let flush_lsn = self.state.effective_flush_lsn();
        events_stream
            .as_mut()
            .stream_mut()
            .send_status_update(
                self.state.last_received_lsn(),
                flush_lsn,
                true,
                StatusUpdateType::ShutdownFlush,
            )
            .await?;

        self.state.shutdown_state = ShutdownState::WaitingForPrimaryKeepAlive {
            acked_flush_lsn: flush_lsn,
        };

        Ok(())
    }

    /// Waits for the pending flush result, if any.
    async fn wait_for_flush_result(
        pending_flush_result: Option<&mut PendingBatchFlushResult<()>>,
    ) -> CompletedBatchFlushResult<()> {
        match pending_flush_result {
            Some(flush_result) => flush_result.await,
            None => std::future::pending().await,
        }
    }

    /// Tries to take a ready pending flush result without waiting.
    fn try_take_pending_flush_result(&mut self) -> Option<CompletedBatchFlushResult<()>> {
        self.state
            .pending_flush_result
            .as_mut()
            .and_then(PendingBatchFlushResult::try_recv)
    }

    /// Handles a completed batch flush result.
    async fn handle_flush_result(
        &mut self,
        flush_result: CompletedBatchFlushResult<()>,
    ) -> EtlResult<ApplyLoopAction> {
        // We clear the state up front because this flush is no longer in flight.
        let processing_paused = self.state.resume_processing();
        self.state.pending_flush_result = None;

        let (commit_end_lsn, metrics, result) = flush_result.into_parts();

        // If there was an error in the flushing, we return it immediately.
        result?;

        counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            WORKER_TYPE_LABEL => "apply",
            ACTION_LABEL => "table_streaming",
            PIPELINE_ID_LABEL => self.pipeline_id.to_string(),
            DESTINATION_LABEL => D::name(),
        )
        .increment(metrics.events_count as u64);

        histogram!(
            ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
            WORKER_TYPE_LABEL => "apply",
            ACTION_LABEL => "table_streaming",
            PIPELINE_ID_LABEL => self.pipeline_id.to_string(),
            DESTINATION_LABEL => D::name(),
        )
        .record(metrics.dispatched_at.elapsed().as_secs_f64());

        // We take the current pending action, or we default to continue.
        let mut action = self
            .state
            .pending_action
            .take()
            .unwrap_or(ApplyLoopAction::Continue);

        // We process the syncing tables with the last end lsn that the batch contains.
        //
        // Note that it could be that there is no end lsn for a specific batch, which could happen
        // if we process a huge transaction, and we don't reach the commit before flushing. In that
        // case, we don't process syncing tables, meaning that progress it not tracked, since it's
        // not going to do anything because we can only track progress at commit boundaries.
        if let Some(commit_end_lsn) = commit_end_lsn {
            let synchronize_action = self
                .process_syncing_tables_after_flush(commit_end_lsn)
                .await?;
            action = action.merge(synchronize_action);
        }

        let synchronize_action = self.process_syncing_tables_when_idle().await?;
        action = action.merge(synchronize_action);

        // If processing was paused, there must be a batch that still needs to be flushed now that
        // the previous in-flight result has resolved.
        if processing_paused {
            self.flush_batch("pending flush result received").await?;
        }

        Ok(action)
    }

    /// Handles a message from the replication stream.
    ///
    /// Processes the message and manages batch timing.
    async fn handle_stream_message(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        maybe_message: Option<EtlResult<ReplicationMessage<LogicalReplicationMessage>>>,
        replication_client: &PgReplicationClient,
    ) -> EtlResult<ApplyLoopAction> {
        // If there is no message anymore, it means that the connection has been closed or had some
        // issues, we must handle this case.
        let Some(message) = maybe_message else {
            return Err(self.build_stream_ended_error(replication_client));
        };

        // If the Postgres had an error, we want to raise it immediately.
        let message = message?;

        let action = self
            .handle_replication_message_and_flush(events_stream.as_mut(), message)
            .await?;

        Ok(action)
    }

    /// Creates an error for when the replication stream ends unexpectedly.
    fn build_stream_ended_error(&self, replication_client: &PgReplicationClient) -> EtlError {
        let worker_type = self.worker_context.worker_type();

        if replication_client.is_closed() {
            warn!(
                %worker_type,
                "replication stream ended: postgresql connection closed",
            );

            etl_error!(
                ErrorKind::SourceConnectionFailed,
                "PostgreSQL connection has been closed during the apply loop"
            )
        } else {
            warn!(
                %worker_type,
                "replication stream ended unexpectedly",
            );

            etl_error!(
                ErrorKind::SourceConnectionFailed,
                "Replication stream ended unexpectedly during the apply loop"
            )
        }
    }

    /// Handles a replication message and flushes the batch if necessary.
    async fn handle_replication_message_and_flush(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        message: ReplicationMessage<LogicalReplicationMessage>,
    ) -> EtlResult<ApplyLoopAction> {
        let result = self
            .handle_replication_message(events_stream.as_mut(), message)
            .await?;

        let should_include_event = matches!(result.end_batch, None | Some(EndBatch::Inclusive));
        if let Some(event) = result.event
            && should_include_event
        {
            // We add the element to the pending batch.
            self.state.add_event_to_batch(event);

            // We update the last end lsn of the commit that we encountered, if any.
            self.state.update_last_commit_end_lsn(result.end_lsn);

            // We start the batch timer for the flushing. This timer is needed to control force
            // flushing of a batch if its size is not reached in time.
            self.state.set_flush_deadline_if_needed();
        }

        let batch_size_reached =
            self.state.events_batch_bytes >= self.cached_batch_budget.current_batch_size_bytes();
        let early_flush_requested = result.end_batch.is_some();
        let should_flush = batch_size_reached || early_flush_requested;

        if should_flush && self.state.has_pending_batch() {
            let reason = if batch_size_reached {
                "max batch bytes reached"
            } else {
                "early flush requested"
            };

            self.flush_batch(reason).await?;
        }

        let mut action = result.action;

        if let Some(error) = result.table_replication_error {
            let mark_table_errored_action = self.mark_table_errored(error).await?;
            action = action.merge(mark_table_errored_action);
        }

        let synchronize_action = self.process_syncing_tables_when_idle().await?;
        action = action.merge(synchronize_action);

        Ok(action)
    }

    /// Flushes the current batch of events to the destination.
    ///
    /// If a flush is already in flight, this pauses the loop and leaves the current batch queued
    /// until the pending flush result has been processed.
    async fn flush_batch(&mut self, reason: &str) -> EtlResult<()> {
        // If the batch is empty, we don't need to do anything.
        if !self.state.has_pending_batch() {
            return Ok(());
        }

        // A flush is already in flight. Pause processing until the result resolves, at which
        // point the loop will resume and dispatch this batch.
        if self.state.has_pending_flush_result() {
            self.state.pause_processing();

            return Ok(());
        }

        // We replace the existing vector with a new one and reset the accumulated batch bytes size.
        let (events_batch, events_batch_bytes) = self.state.take_events_batch();

        let events_batch_size = events_batch.len();
        info!(
            worker_type = %self.worker_context.worker_type(),
            batch_size = events_batch_size,
            batch_size_bytes = events_batch_bytes,
            %reason,
            "flushing batch to destination",
        );

        // Capture dispatch-time metrics; they are carried through the result channel and
        // recorded once the destination acknowledges the batch.
        let metrics = BatchFlushMetrics {
            events_count: events_batch_size,
            dispatched_at: Instant::now(),
        };

        // Create the flush result channel: the sender is handed to the destination and the
        // pending receiver is stored on the loop state until the destination signals completion.
        let (flush_result, pending_flush_result) =
            BatchFlushResult::new(self.state.last_commit_end_lsn.take(), metrics);
        self.destination
            .write_events(events_batch, flush_result)
            .await;
        self.state.pending_flush_result = Some(pending_flush_result);

        // We reset the deadline for the batch, since we are now flushing a new batch. The new deadline
        // will start as soon as we process a new element.
        //
        // It's important to note that the deadline is removed only when the batch is flushed and not before
        // this way, if a batch fails to flush due to inflight, it will be re-tried indefinitely until
        // that finishes.
        self.state.reset_flush_deadline();

        Ok(())
    }

    /// Dispatches replication protocol messages to appropriate handlers.
    async fn handle_replication_message(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        message: ReplicationMessage<LogicalReplicationMessage>,
    ) -> EtlResult<HandleMessageResult> {
        match message {
            ReplicationMessage::XLogData(message) => {
                let start_lsn = PgLsn::from(message.wal_start());
                self.state
                    .replication_progress
                    .update_last_received_lsn(start_lsn);

                let end_lsn = PgLsn::from(message.wal_end());
                self.state
                    .replication_progress
                    .update_last_received_lsn(end_lsn);

                debug!(
                    %start_lsn,
                    %end_lsn,
                    "handling logical replication data message",
                );

                self.handle_logical_replication_message(start_lsn, message.into_data())
                    .await
            }
            ReplicationMessage::PrimaryKeepAlive(message) => {
                let end_lsn = PgLsn::from(message.wal_end());
                self.state
                    .replication_progress
                    .update_last_received_lsn(end_lsn);

                debug!(
                    wal_end = %end_lsn,
                    reply_requested = message.reply() == 1,
                    "received keep alive",
                );

                events_stream
                    .as_mut()
                    .stream_mut()
                    .send_status_update(
                        self.state.last_received_lsn(),
                        // Use effective flush LSN to report last received LSN when idle, since
                        // last flush LSN only advances during actual flushes.
                        self.state.effective_flush_lsn(),
                        message.reply() == 1,
                        StatusUpdateType::KeepAlive,
                    )
                    .await?;

                Ok(HandleMessageResult::no_event())
            }
            _ => Ok(HandleMessageResult::no_event()),
        }
    }

    /// Processes logical replication messages and converts them to typed events.
    async fn handle_logical_replication_message(
        &mut self,
        start_lsn: PgLsn,
        message: LogicalReplicationMessage,
    ) -> EtlResult<HandleMessageResult> {
        self.state.current_tx_events += 1;

        match &message {
            LogicalReplicationMessage::Begin(begin_body) => {
                self.handle_begin_message(start_lsn, begin_body)
            }
            LogicalReplicationMessage::Commit(commit_body) => {
                self.handle_commit_message(start_lsn, commit_body).await
            }
            LogicalReplicationMessage::Relation(relation_body) => {
                self.handle_relation_message(start_lsn, relation_body).await
            }
            LogicalReplicationMessage::Insert(insert_body) => {
                self.handle_insert_message(start_lsn, insert_body).await
            }
            LogicalReplicationMessage::Update(update_body) => {
                self.handle_update_message(start_lsn, update_body).await
            }
            LogicalReplicationMessage::Delete(delete_body) => {
                self.handle_delete_message(start_lsn, delete_body).await
            }
            LogicalReplicationMessage::Truncate(truncate_body) => {
                self.handle_truncate_message(start_lsn, truncate_body).await
            }
            LogicalReplicationMessage::Origin(_) => {
                debug!("received unsupported ORIGIN message");
                Ok(HandleMessageResult::default())
            }
            LogicalReplicationMessage::Type(_) => {
                debug!("received unsupported TYPE message");
                Ok(HandleMessageResult::default())
            }
            _ => Ok(HandleMessageResult::default()),
        }
    }

    /// Handles Postgres BEGIN messages.
    fn handle_begin_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::BeginBody,
    ) -> EtlResult<HandleMessageResult> {
        let final_lsn = PgLsn::from(message.final_lsn());
        self.state.remote_final_lsn = Some(final_lsn);

        self.state.current_tx_begin_ts = Some(Instant::now());
        self.state.current_tx_events = 0;
        self.state.reset_tx_ordinal();

        let tx_ordinal = self.state.next_tx_ordinal();
        let event = parse_event_from_begin_message(start_lsn, final_lsn, tx_ordinal, message);

        Ok(HandleMessageResult::return_event(Event::Begin(event)))
    }

    /// Handles Postgres COMMIT messages.
    async fn handle_commit_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::CommitBody,
    ) -> EtlResult<HandleMessageResult> {
        let Some(remote_final_lsn) = self.state.remote_final_lsn.take() else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "Transaction must be active before processing COMMIT message"
            );
        };

        let commit_lsn = PgLsn::from(message.commit_lsn());
        if commit_lsn != remote_final_lsn {
            bail!(
                ErrorKind::ValidationError,
                "Invalid commit LSN",
                format!(
                    "Incorrect commit LSN {} in COMMIT message (expected {})",
                    commit_lsn, remote_final_lsn
                )
            );
        }

        if let Some(begin_ts) = self.state.current_tx_begin_ts.take() {
            let now = Instant::now();
            let duration_seconds = (now - begin_ts).as_secs_f64();
            histogram!(
                ETL_TRANSACTION_DURATION_SECONDS,
                PIPELINE_ID_LABEL => self.pipeline_id.to_string()
            )
            .record(duration_seconds);

            counter!(
                ETL_TRANSACTIONS_TOTAL,
                PIPELINE_ID_LABEL => self.pipeline_id.to_string()
            )
            .increment(1);

            histogram!(
                ETL_TRANSACTION_SIZE,
                PIPELINE_ID_LABEL => self.pipeline_id.to_string()
            )
            .record((self.state.current_tx_events - 1) as f64);

            self.state.current_tx_events = 0;
        }

        let end_lsn = PgLsn::from(message.end_lsn());

        // Process syncing tables after commit (worker-specific behavior).
        let action = self
            .process_syncing_tables_after_commit_event(end_lsn)
            .await?;

        let tx_ordinal = self.state.next_tx_ordinal();
        let event = parse_event_from_commit_message(start_lsn, commit_lsn, tx_ordinal, message);

        let mut result = HandleMessageResult {
            event: Some(Event::Commit(event)),
            end_lsn: Some(end_lsn),
            action,
            ..Default::default()
        };

        // If the action results in the apply loop is terminating, we want to end the batch forcibly,
        // including the commit event itself.
        if action.is_terminating() {
            result.end_batch = Some(EndBatch::Inclusive);
        }

        Ok(result)
    }

    /// Handles Postgres RELATION messages.
    async fn handle_relation_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::RelationBody,
    ) -> EtlResult<HandleMessageResult> {
        let Some(remote_final_lsn) = self.state.remote_final_lsn else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "Transaction must be active before processing RELATION message"
            );
        };

        let table_id = TableId::new(message.rel_id());
        let tx_ordinal = self.state.next_tx_ordinal();

        if !self
            .should_apply_changes(table_id, remote_final_lsn)
            .await?
        {
            return Ok(HandleMessageResult::no_event());
        }

        let existing_table_schema = self
            .schema_store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found in cache",
                    format!("Table schema for table {} not found in cache", table_id)
                )
            })?;

        let event =
            parse_event_from_relation_message(start_lsn, remote_final_lsn, tx_ordinal, message)?;

        if !existing_table_schema.partial_eq(&event.table_schema) {
            let error = TableReplicationError::with_solution(
                table_id,
                format!("The schema for table {table_id} has changed during streaming"),
                "ETL doesn't support schema changes at this point in time, rollback the schema"
                    .into(),
                RetryPolicy::ManualRetry,
                etl_error!(
                    ErrorKind::SourceSchemaError,
                    "table schema changed during streaming",
                    format!("table schema for table {table_id} changed during streaming")
                ),
            );

            return Ok(HandleMessageResult::finish_batch_and_exclude_event(error));
        }

        Ok(HandleMessageResult::return_event(Event::Relation(event)))
    }

    /// Handles Postgres INSERT messages.
    async fn handle_insert_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::InsertBody,
    ) -> EtlResult<HandleMessageResult> {
        let Some(remote_final_lsn) = self.state.remote_final_lsn else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "Transaction must be active before processing INSERT message"
            );
        };

        let tx_ordinal = self.state.next_tx_ordinal();

        if !self
            .should_apply_changes(TableId::new(message.rel_id()), remote_final_lsn)
            .await?
        {
            return Ok(HandleMessageResult::no_event());
        }

        let event = parse_event_from_insert_message(
            &self.schema_store,
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
            self.pipeline_id,
        )
        .await?;

        Ok(HandleMessageResult::return_event(Event::Insert(event)))
    }

    /// Handles Postgres UPDATE messages.
    async fn handle_update_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::UpdateBody,
    ) -> EtlResult<HandleMessageResult> {
        let Some(remote_final_lsn) = self.state.remote_final_lsn else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "Transaction must be active before processing UPDATE message"
            );
        };

        let tx_ordinal = self.state.next_tx_ordinal();

        if !self
            .should_apply_changes(TableId::new(message.rel_id()), remote_final_lsn)
            .await?
        {
            return Ok(HandleMessageResult::no_event());
        }

        let event = parse_event_from_update_message(
            &self.schema_store,
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
            self.pipeline_id,
        )
        .await?;

        Ok(HandleMessageResult::return_event(Event::Update(event)))
    }

    /// Handles Postgres DELETE messages.
    async fn handle_delete_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::DeleteBody,
    ) -> EtlResult<HandleMessageResult> {
        let Some(remote_final_lsn) = self.state.remote_final_lsn else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "Transaction must be active before processing DELETE message"
            );
        };

        let tx_ordinal = self.state.next_tx_ordinal();

        if !self
            .should_apply_changes(TableId::new(message.rel_id()), remote_final_lsn)
            .await?
        {
            return Ok(HandleMessageResult::no_event());
        }

        let event = parse_event_from_delete_message(
            &self.schema_store,
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
            self.pipeline_id,
        )
        .await?;

        Ok(HandleMessageResult::return_event(Event::Delete(event)))
    }

    /// Handles Postgres TRUNCATE messages.
    async fn handle_truncate_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::TruncateBody,
    ) -> EtlResult<HandleMessageResult> {
        let Some(remote_final_lsn) = self.state.remote_final_lsn else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "Transaction must be active before processing TRUNCATE message"
            );
        };

        let tx_ordinal = self.state.next_tx_ordinal();

        let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
        for &table_id in message.rel_ids().iter() {
            let should_apply_truncate = self
                .should_apply_changes(TableId::new(table_id), remote_final_lsn)
                .await?;
            if should_apply_truncate {
                rel_ids.push(table_id)
            }
        }

        if rel_ids.is_empty() {
            return Ok(HandleMessageResult::no_event());
        }

        let event = parse_event_from_truncate_message(
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
            rel_ids,
        );

        Ok(HandleMessageResult::return_event(Event::Truncate(event)))
    }

    /// Determines whether changes should be applied for a given table.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn should_apply_changes(
        &self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<bool> {
        match &self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::should_apply_changes(ctx, table_id, remote_final_lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                Ok(table_sync_worker::should_apply_changes(ctx, table_id))
            }
        }
    }

    /// Processes syncing tables after a commit message.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn process_syncing_tables_after_commit_event(
        &mut self,
        lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction> {
        match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_after_commit_event(ctx, lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_after_commit_event(ctx, lsn).await
            }
        }
    }

    /// Processes syncing tables after a batch has been flushed.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn process_syncing_tables_after_flush(
        &mut self,
        last_commit_end_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction> {
        // Update replication progress to notify PostgreSQL of durable flush. Only reports progress
        // up to the last completed transaction, which may cause duplicates on restart for partial
        // transactions. Destinations must handle at-least-once delivery semantics.
        self.state
            .replication_progress
            .update_last_flush_lsn(last_commit_end_lsn);

        let current_lsn = self.state.replication_progress.last_flush_lsn;
        info!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables after batch flush"
        );

        match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_after_flush(ctx, current_lsn).await?;
                Ok(ApplyLoopAction::Continue)
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_after_flush(ctx, current_lsn).await
            }
        }
    }

    /// Processes syncing tables when the apply loop is idle.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn process_syncing_tables_when_idle(&mut self) -> EtlResult<ApplyLoopAction> {
        if !self.state.is_idle() {
            debug!("skipping table sync processing because apply loop is not idle");

            return Ok(ApplyLoopAction::Continue);
        }

        // Use effective flush LSN to report last received LSN when idle.
        let current_lsn = self.state.effective_flush_lsn();

        debug!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables while idle"
        );

        match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_when_idle(ctx, current_lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_when_idle(ctx, current_lsn).await
            }
        }
    }

    /// Marks a table as errored.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn mark_table_errored(
        &mut self,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<ApplyLoopAction> {
        match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::mark_table_errored(ctx, table_replication_error).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::mark_table_errored(ctx, table_replication_error).await
            }
        }
    }
}

/// Returns an iterator over tables that are still synchronizing.
async fn get_syncing_tables<S>(
    store: &S,
) -> EtlResult<impl Iterator<Item = (TableId, TableReplicationPhase)> + use<S>>
where
    S: StateStore,
{
    Ok(store
        .get_table_replication_states()
        .await?
        .into_iter()
        .filter(|(_, state)| !state.as_type().is_done()))
}

/// Functions specific to the apply worker.
mod apply_worker {
    use super::*;

    /// Determines whether changes should be applied for a given table.
    ///
    /// If an active worker exists for the table, its state is checked while holding
    /// the lock. Otherwise, the replication phase is read from the store.
    pub(super) async fn should_apply_changes<S, D>(
        ctx: &ApplyWorkerContext<S, D>,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<bool>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        fn is_phase_ready_for_changes(
            phase: TableReplicationPhase,
            remote_final_lsn: PgLsn,
        ) -> bool {
            match phase {
                TableReplicationPhase::Ready => true,
                TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
                _ => false,
            }
        }

        let active_worker_state = ctx.pool.get_active_worker_state(table_id).await;

        if let Some(active_worker_state) = active_worker_state {
            let inner = active_worker_state.lock().await;
            return Ok(is_phase_ready_for_changes(
                inner.replication_phase(),
                remote_final_lsn,
            ));
        }

        // If we didn't find an active worker, we need to read the replication phase from the store. This
        // could happen if the event is from a table that has to be synced, or it was synced.
        let Some(phase) = ctx.store.get_table_replication_state(table_id).await? else {
            return Ok(false);
        };

        Ok(is_phase_ready_for_changes(phase, remote_final_lsn))
    }

    /// Processes syncing tables after commit.
    ///
    /// Spawns new table sync workers and triggers catchup when encountering SyncWait.
    /// Does NOT perform SyncDone → Ready transitions.
    pub(super) async fn process_syncing_tables_after_commit_event<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        for (table_id, table_replication_phase) in get_syncing_tables(&ctx.store).await? {
            let action = process_single_syncing_table_after_commit(
                ctx,
                table_id,
                table_replication_phase,
                current_lsn,
            )
            .await?;

            if action.is_terminating() {
                return Ok(action);
            }
        }

        Ok(ApplyLoopAction::Continue)
    }

    /// Processes a single syncing table after commit.
    ///
    /// Handles SyncWait → Catchup transitions and spawns new workers.
    /// Does NOT handle SyncDone → Ready transitions.
    async fn process_single_syncing_table_after_commit<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        table_id: TableId,
        table_replication_phase: TableReplicationPhase,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        if let Some(worker_state) = worker_state {
            let mut worker_state_guard = worker_state.lock().await;
            let phase = worker_state_guard.replication_phase();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?phase,
                %current_lsn,
                "checking table with active worker after commit",
            );

            match phase {
                TableReplicationPhase::SyncWait { lsn: snapshot_lsn } => {
                    // The catchup lsn is determined via max since it could be that the table sync worker
                    // is started from a lsn which is far in the future compared to where the apply worker
                    // is.
                    let catchup_lsn = snapshot_lsn.max(current_lsn);

                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %current_lsn,
                        %snapshot_lsn,
                        %catchup_lsn,
                        "transitioning sync_wait -> catchup",
                    );

                    worker_state_guard
                        .set_and_store(
                            TableReplicationPhase::Catchup { lsn: catchup_lsn },
                            &ctx.store,
                        )
                        .await?;

                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %catchup_lsn,
                        "table sync worker entered catchup phase, apply worker blocking until table sync worker reaches sync_done",
                    );

                    // It's important to drop the state guard before waiting, otherwise we deadlock.
                    drop(worker_state_guard);

                    let result = worker_state
                        .wait_for_phase_type(
                            &[
                                TableReplicationPhaseType::SyncDone,
                                TableReplicationPhaseType::Errored,
                            ],
                            ctx.shutdown_rx.clone(),
                        )
                        .await;

                    match result {
                        ShutdownResult::Ok(result) => {
                            let final_phase = result.replication_phase();
                            if final_phase.as_type().is_errored() {
                                info!(
                                    worker_type = %WorkerType::Apply,
                                    table_id = table_id.0,
                                    ?final_phase,
                                    "apply worker unblocked: table sync worker errored, skipping table",
                                );
                                return Ok(ApplyLoopAction::Continue);
                            }

                            info!(
                                worker_type = %WorkerType::Apply,
                                table_id = table_id.0,
                                ?final_phase,
                                "apply worker unblocked: table sync worker reached sync_done",
                            );
                        }
                        ShutdownResult::Shutdown(_) => {
                            info!(
                                worker_type = %WorkerType::Apply,
                                table_id = table_id.0,
                                "apply worker unblocked: shutdown signal received while waiting for table sync worker",
                            );
                            return Ok(ApplyLoopAction::Pause);
                        }
                    }
                }
                TableReplicationPhase::SyncDone { lsn } => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        sync_done_lsn = %lsn,
                        "table in sync_done state, will transition to ready after batch flush",
                    );
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        ?phase,
                        "no action needed for current phase after commit",
                    );
                }
            }
        } else {
            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?table_replication_phase,
                "checking table without active worker after commit",
            );

            // No active worker exists, potentially start a new worker.
            match table_replication_phase {
                TableReplicationPhase::SyncDone { lsn } => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        sync_done_lsn = %lsn,
                        "table in sync_done state, will transition to ready after batch flush",
                    );
                }
                _ => {
                    debug!(worker_type = %WorkerType::Apply, table_id = table_id.0, ?table_replication_phase, "spawning new table sync worker");
                    // Start a new worker for this table.
                    let table_sync_worker = build_table_sync_worker(ctx, table_id);
                    if let Err(err) =
                        start_table_sync_worker(ctx.pool.clone(), table_sync_worker).await
                    {
                        error!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            error = %err,
                            "failed to start table sync worker",
                        );
                    }
                }
            }
        }

        Ok(ApplyLoopAction::Continue)
    }

    /// Processes syncing tables after a batch flush.
    ///
    /// Handles `SyncDone → Ready` transitions and spawns new workers.
    pub(super) async fn process_syncing_tables_after_flush<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
    ) -> EtlResult<()>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        for (table_id, table_replication_phase) in get_syncing_tables(&ctx.store).await? {
            process_single_syncing_table_after_flush(
                ctx,
                table_id,
                table_replication_phase,
                current_lsn,
            )
            .await?;
        }

        Ok(())
    }

    /// Processes a single syncing table after batch flush.
    ///
    /// Handles `SyncDone → Ready` transitions and spawns new workers.
    async fn process_single_syncing_table_after_flush<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        table_id: TableId,
        table_replication_phase: TableReplicationPhase,
        current_lsn: PgLsn,
    ) -> EtlResult<()>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        // If there is an active worker, we want to see if we can switch it to the ready state.
        // If there isn't an active worker, we just try to see if we can switch the table to ready
        // state or start a new worker for that table.
        if let Some(worker_state) = worker_state {
            let worker_state_guard = worker_state.lock().await;
            let phase = worker_state_guard.replication_phase();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?phase,
                %current_lsn,
                "checking table with active worker after batch flush",
            );

            if let TableReplicationPhase::SyncDone { lsn: sync_done_lsn } = phase {
                if current_lsn >= sync_done_lsn {
                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %sync_done_lsn,
                        %current_lsn,
                        "transitioning sync_done -> ready",
                    );

                    ctx.store
                        .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                        .await?;
                } else {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %sync_done_lsn,
                        %current_lsn,
                        "table not yet ready, current lsn below sync done lsn",
                    );
                }
            }
        } else {
            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?table_replication_phase,
                "checking table without active worker after batch flush",
            );

            match table_replication_phase {
                TableReplicationPhase::SyncDone { lsn: sync_done_lsn } => {
                    if current_lsn >= sync_done_lsn {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    } else {
                        debug!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "table not yet ready, current lsn below sync done lsn",
                        );
                    }
                }
                _ => {
                    debug!(worker_type = %WorkerType::Apply, table_id = table_id.0, ?table_replication_phase, "spawning new table sync worker");

                    // Start a new worker for this table.
                    let table_sync_worker = build_table_sync_worker(ctx, table_id);
                    if let Err(err) =
                        start_table_sync_worker(ctx.pool.clone(), table_sync_worker).await
                    {
                        error!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            error = %err,
                            "failed to start table sync worker",
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes syncing tables outside transaction.
    ///
    /// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, and spawns workers.
    /// Only called when outside a transaction and the batch is empty.
    pub(super) async fn process_syncing_tables_when_idle<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        for (table_id, table_replication_phase) in get_syncing_tables(&ctx.store).await? {
            let action = process_single_syncing_table_when_idle(
                ctx,
                table_id,
                table_replication_phase,
                current_lsn,
            )
            .await?;

            if action.is_terminating() {
                return Ok(action);
            }
        }

        Ok(ApplyLoopAction::Continue)
    }

    /// Processes a single syncing table outside transaction.
    ///
    /// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, and spawns workers.
    /// Only called when outside a transaction and the batch is empty.
    async fn process_single_syncing_table_when_idle<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        table_id: TableId,
        table_replication_phase: TableReplicationPhase,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        // If there is an active worker, we want to see if we can start the catchup or if we can
        // switch it to ready state.
        // If there isn't an active worker, we just try to see if we can switch the table to ready
        // state or start a new worker for that table.
        if let Some(worker_state) = worker_state {
            let mut worker_state_guard = worker_state.lock().await;
            let phase = worker_state_guard.replication_phase();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?phase,
                %current_lsn,
                "checking table with active worker when idle",
            );

            match phase {
                TableReplicationPhase::SyncWait { lsn: snapshot_lsn } => {
                    // The catchup lsn is determined via max since it could be that the table sync worker
                    // is started from a lsn which is far in the future compared to where the apply worker
                    // is.
                    let catchup_lsn = snapshot_lsn.max(current_lsn);

                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %current_lsn,
                        %snapshot_lsn,
                        %catchup_lsn,
                        "transitioning sync_wait -> catchup",
                    );

                    worker_state_guard
                        .set_and_store(
                            TableReplicationPhase::Catchup { lsn: catchup_lsn },
                            &ctx.store,
                        )
                        .await?;

                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %catchup_lsn,
                        "table sync worker entered catchup phase, apply worker blocking until table sync worker reaches sync_done",
                    );

                    // It's important to drop the state guard before waiting, otherwise we deadlock.
                    drop(worker_state_guard);

                    let result = worker_state
                        .wait_for_phase_type(
                            &[
                                TableReplicationPhaseType::SyncDone,
                                TableReplicationPhaseType::Errored,
                            ],
                            ctx.shutdown_rx.clone(),
                        )
                        .await;

                    match result {
                        ShutdownResult::Ok(result) => {
                            let final_phase = result.replication_phase();
                            if final_phase.as_type().is_errored() {
                                info!(
                                    worker_type = %WorkerType::Apply,
                                    table_id = table_id.0,
                                    ?final_phase,
                                    "apply worker unblocked: table sync worker errored, skipping table",
                                );

                                return Ok(ApplyLoopAction::Continue);
                            }

                            info!(
                                worker_type = %WorkerType::Apply,
                                table_id = table_id.0,
                                ?final_phase,
                                "apply worker unblocked: table sync worker reached sync_done",
                            );
                        }
                        ShutdownResult::Shutdown(_) => {
                            info!(
                                worker_type = %WorkerType::Apply,
                                table_id = table_id.0,
                                "apply worker unblocked: shutdown signal received while waiting for table sync worker",
                            );

                            return Ok(ApplyLoopAction::Pause);
                        }
                    }
                }
                TableReplicationPhase::SyncDone { lsn: sync_done_lsn } => {
                    if current_lsn >= sync_done_lsn {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    } else {
                        debug!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "table not yet ready, current lsn below sync done lsn",
                        );
                    }
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        ?phase,
                        "no action needed for current phase when idle",
                    );
                }
            }
        } else {
            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?table_replication_phase,
                "checking table without active worker when idle",
            );

            match table_replication_phase {
                TableReplicationPhase::SyncDone { lsn: sync_done_lsn } => {
                    if current_lsn >= sync_done_lsn {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store
                            .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                            .await?;
                    }
                }
                _ => {
                    debug!(worker_type = %WorkerType::Apply, table_id = table_id.0, ?table_replication_phase, "spawning new table sync worker");

                    // Start a new worker for this table.
                    let table_sync_worker = build_table_sync_worker(ctx, table_id);
                    if let Err(err) =
                        start_table_sync_worker(ctx.pool.clone(), table_sync_worker).await
                    {
                        error!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            error = %err,
                            "failed to start table sync worker",
                        );
                    }
                }
            }
        }

        Ok(ApplyLoopAction::Continue)
    }

    /// Marks a table as errored.
    ///
    /// Updates the state store and continues the loop.
    pub(super) async fn mark_table_errored<S, D>(
        ctx: &ApplyWorkerContext<S, D>,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        let table_id = table_replication_error.table_id();
        TableSyncWorkerState::set_and_store(
            &ctx.pool,
            &ctx.store,
            table_id,
            table_replication_error.into(),
        )
        .await?;

        Ok(ApplyLoopAction::Continue)
    }

    /// Creates a new table sync worker for the specified table.
    fn build_table_sync_worker<S, D>(
        ctx: &ApplyWorkerContext<S, D>,
        table_id: TableId,
    ) -> TableSyncWorker<S, D>
    where
        S: Clone,
        D: Clone,
    {
        info!(table_id = table_id.0, "creating table sync worker");

        TableSyncWorker::new(
            ctx.pipeline_id,
            ctx.config.clone(),
            ctx.pool.clone(),
            table_id,
            ctx.store.clone(),
            ctx.destination.clone(),
            ctx.shutdown_rx.clone(),
            ctx.table_sync_worker_permits.clone(),
            ctx.memory_monitor.clone(),
            ctx.batch_budget.clone(),
        )
    }

    /// Starts a table sync worker and adds it to the pool.
    ///
    /// We optimistically start the worker without checking if another one already exists since
    /// it's highly likely that if we didn't find the worker state during process syncing table, then
    /// the worker doesn't exist. If it were to exist, the pool itself performs de-duplication in a
    /// consistent way.
    ///
    /// This helper function uses type erasure via [`Box::pin`] to enforce `Send` bounds
    /// on the future. Without this, the compiler cannot verify that the recursive async
    /// call chain (ApplyLoop -> TableSyncWorker -> ApplyLoop for catchup) satisfies `Send`.
    fn start_table_sync_worker<S, D>(
        pool: Arc<TableSyncWorkerPool>,
        worker: TableSyncWorker<S, D>,
    ) -> Pin<Box<dyn Future<Output = EtlResult<()>> + Send>>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        Box::pin(async move { worker.spawn_into_pool(&pool).await })
    }
}

/// Functions specific to the table sync worker.
mod table_sync_worker {
    use super::*;

    /// Determines whether changes should be applied for a given table.
    ///
    /// For table sync workers, changes are only applied if the table matches the worker's assigned
    /// table.
    pub(super) fn should_apply_changes<S>(
        ctx: &TableSyncWorkerContext<S>,
        table_id: TableId,
    ) -> bool {
        ctx.table_id == table_id
    }

    /// Processes syncing tables after commit.
    ///
    /// Validates whether catchup position has been reached.
    /// If so, returns Complete to signal end batch.
    /// Does NOT update state (that happens after flush).
    pub(super) async fn process_syncing_tables_after_commit_event<S>(
        ctx: &TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction> {
        let worker_type = WorkerType::TableSync {
            table_id: ctx.table_id,
        };

        // Check if catchup position reached, if so, signal end batch but don't update the state yet.
        let inner = ctx.table_sync_worker_state.lock().await;
        if let TableReplicationPhase::Catchup { lsn: catchup_lsn } = inner.replication_phase() {
            if current_lsn >= catchup_lsn {
                info!(
                    %worker_type,
                    %catchup_lsn,
                    %current_lsn,
                    "catchup target lsn reached after commit, requesting early batch flush before transitioning to sync_done",
                );

                return Ok(ApplyLoopAction::Complete);
            }

            debug!(
                %worker_type,
                %catchup_lsn,
                %current_lsn,
                remaining_lsn = %(u64::from(catchup_lsn) - u64::from(current_lsn)),
                "catchup in progress, target lsn not yet reached",
            );
        }

        Ok(ApplyLoopAction::Continue)
    }

    /// Processes syncing tables after batch flush.
    ///
    /// Validates whether catchup position has been reached.
    /// If so, transitions to SyncDone and returns Complete.
    pub(super) async fn process_syncing_tables_after_flush<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        try_complete_catchup(ctx, current_lsn).await
    }

    /// Processes syncing tables outside transaction.
    ///
    /// If catchup position reached, transitions to SyncDone.
    /// Only called when outside a transaction and the batch is empty.
    pub(super) async fn process_syncing_tables_when_idle<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        try_complete_catchup(ctx, current_lsn).await
    }

    /// Attempts to complete catchup and transition to SyncDone.
    ///
    /// If catchup position has been reached, transitions to SyncDone and returns Complete.
    async fn try_complete_catchup<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        let worker_type = WorkerType::TableSync {
            table_id: ctx.table_id,
        };
        let mut inner = ctx.table_sync_worker_state.lock().await;
        let phase = inner.replication_phase();

        if let TableReplicationPhase::Catchup { lsn: catchup_lsn } = phase {
            if current_lsn >= catchup_lsn {
                info!(
                    %worker_type,
                    %catchup_lsn,
                    %current_lsn,
                    "catchup target lsn reached, transitioning catchup -> sync_done",
                );

                inner
                    .set_and_store(
                        TableReplicationPhase::SyncDone { lsn: current_lsn },
                        &ctx.state_store,
                    )
                    .await?;

                info!(
                    %worker_type,
                    %current_lsn,
                    "table sync worker completed: now in sync_done state, apply worker will be unblocked",
                );

                return Ok(ApplyLoopAction::Complete);
            }

            debug!(
                %worker_type,
                %catchup_lsn,
                %current_lsn,
                remaining_lsn = %(u64::from(catchup_lsn) - u64::from(current_lsn)),
                "catchup in progress, target lsn not yet reached",
            );
        }

        Ok(ApplyLoopAction::Continue)
    }

    /// Marks a table as errored.
    ///
    /// Updates the state and returns Complete if the table matches this worker.
    pub(super) async fn mark_table_errored<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<ApplyLoopAction>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        if ctx.table_id != table_replication_error.table_id() {
            return Ok(ApplyLoopAction::Continue);
        }

        let mut inner = ctx.table_sync_worker_state.lock().await;
        inner
            .set_and_store(table_replication_error.into(), &ctx.state_store)
            .await?;

        Ok(ApplyLoopAction::Complete)
    }
}
