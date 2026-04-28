//! Apply loop implementation for processing PostgreSQL logical replication
//! events.
//!
//! This module provides the core apply loop that processes replication events
//! from PostgreSQL and coordinates table synchronization. It uses a
//! [`WorkerContext`] enum to enable different behavior based on the worker type
//! (apply worker vs table sync worker) at various points in the replication
//! cycle.

use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use etl_config::shared::{PgConnectionConfig, PipelineConfig};
use etl_postgres::{
    replication::slots::EtlReplicationSlot,
    types::{
        IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId, TableSchema,
    },
};
use futures::StreamExt;
use metrics::{counter, histogram};
use postgres_replication::{
    protocol,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};
use tokio::{
    pin,
    sync::{Mutex, Semaphore, watch},
    task::JoinHandle,
};
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info, warn};

#[cfg(feature = "failpoints")]
use crate::failpoints::{
    FORCE_SCHEMA_CLEANUP_CONFIRMED_FLUSH_LSN_FP, FORCE_SCHEMA_CLEANUP_FP, SEND_STATUS_UPDATE_FP,
    etl_fail_point_active,
};
use crate::{
    bail,
    concurrency::{
        BackpressureStream, BatchBudgetController, CachedBatchBudget, MemoryMonitor,
        ShutdownResult, ShutdownRx, apply_worker_apply_stream_id,
        table_sync_worker_apply_stream_id,
    },
    conversions::{
        DDL_MESSAGE_PREFIX, SchemaChangeMessage, parse_event_from_begin_message,
        parse_event_from_commit_message, parse_event_from_delete_message,
        parse_event_from_insert_message, parse_event_from_truncate_message,
        parse_event_from_update_message, parse_replica_identity_column_names,
        parse_replicated_column_names,
    },
    destination::{
        Destination,
        async_result::{
            ApplyLoopAsyncResultMetadata, CompletedWriteEventsResult, DispatchMetrics,
            PendingWriteEventsResult, WriteEventsResult,
        },
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    metrics::{
        ACTION_LABEL, COMMAND_TAG_LABEL, DESTINATION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
        ETL_DDL_SCHEMA_CHANGE_COLUMNS, ETL_DDL_SCHEMA_CHANGES_TOTAL, ETL_EVENTS_PROCESSED_TOTAL,
        ETL_REPLICATION_MESSAGES_TOTAL, ETL_SCHEMA_CLEANUP_ERRORS_TOTAL, ETL_SCHEMA_CLEANUPS_TOTAL,
        ETL_TRANSACTION_DURATION_SECONDS, ETL_TRANSACTION_SIZE, ETL_TRANSACTIONS_TOTAL,
        OUTCOME_LABEL, WORKER_TYPE_LABEL,
    },
    replication::{
        EventsStream, SharedTableCache, StatusUpdateResult, StatusUpdateType,
        client::{PgReplicationClient, PostgresConnectionUpdate},
    },
    state::table::{TableReplicationError, TableReplicationPhase, TableReplicationPhaseType},
    store::{schema::SchemaStore, state::StateStore},
    types::{Event, PipelineId, RelationEvent, SizeHint},
    workers::{TableSyncWorker, TableSyncWorkerPool, TableSyncWorkerState},
};

/// Default keep alive value if it can't be fetched from Postgres.
///
/// PostgreSQL defaults `wal_sender_timeout` to 60 seconds, so we will use the
/// same.
const DEFAULT_KEEP_ALIVE_DURATION: Duration = Duration::from_secs(60);
/// Fraction of `wal_sender_timeout` used for the proactive keep alive deadline.
///
/// PostgreSQL normally emits an idle keep alive around `wal_sender_timeout /
/// 2`. We wait a bit longer than that, using `60%` of the full timeout, so
/// normal server keep alives still win most of the time while the client still
/// has room to send its own status update if that keep alive is delayed by
/// network, scheduling, or local processing latency. This is intentionally a
/// last-resort fallback: in normal operation, progress should still be driven
/// by PostgreSQL's primary keep alive messages rather than by the client
/// timeout path.
const KEEP_ALIVE_DEADLINE_FRACTION: f64 = 0.6;
/// Minimum client-side deadline for proactive keep alive retries.
///
/// PostgreSQL exposes `wal_sender_timeout` in millisecond units and `0`
/// disables it, so the smallest enabled value is effectively `1ms`. A raw `60%`
/// deadline at that scale would make the apply loop spin sending forced keep
/// alives, which is not operationally useful. We clamp to `100ms`.
const MIN_KEEP_ALIVE_DEADLINE_DURATION: Duration = Duration::from_millis(100);
/// Minimum interval between best-effort schema cleanup tasks during normal
/// replication.
///
/// Cleanup is considered only after status updates, because those are the
/// points where the slot's confirmed flush LSN may have advanced. The next
/// deadline is scheduled when the previous cleanup task finishes.
const SCHEMA_CLEANUP_INTERVAL: Duration = Duration::from_hours(1);

/// Type of worker driving the apply loop.
#[derive(Debug, Copy, Clone)]
pub(crate) enum WorkerType {
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
    pub(crate) fn build_etl_replication_slot(&self, pipeline_id: u64) -> EtlReplicationSlot {
        match self {
            Self::Apply => EtlReplicationSlot::Apply { pipeline_id },
            Self::TableSync { table_id } => {
                EtlReplicationSlot::TableSync { pipeline_id, table_id: *table_id }
            }
        }
    }

    /// Returns a low-cardinality worker type label for metrics and tags.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::TableSync { .. } => "table_sync",
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
pub(crate) enum ApplyLoopResult {
    /// The apply loop was paused and could be resumed in the future.
    Paused,
    /// The apply loop was completed and will never be invoked again.
    Completed,
}

/// Final exit that the current apply loop invocation should eventually take.
#[derive(Debug, Copy, Clone)]
enum ExitIntent {
    /// Stop the current invocation and allow it to be resumed later.
    Pause,
    /// Stop the current invocation permanently.
    Complete,
}

impl ExitIntent {
    /// Returns the stronger of two exit intents.
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Complete, _) | (_, Self::Complete) => Self::Complete,
            (Self::Pause, Self::Pause) => Self::Pause,
        }
    }

    /// Builds the final loop result for this exit intent.
    fn to_result(self) -> ApplyLoopResult {
        match self {
            Self::Pause => ApplyLoopResult::Paused,
            Self::Complete => ApplyLoopResult::Completed,
        }
    }
}

/// Represents the shutdown state of the apply loop.
///
/// Tracks the progress of a graceful shutdown, ensuring that PostgreSQL
/// acknowledges our flush position before we terminate to prevent data
/// duplication on restart.
#[derive(Debug, Clone)]
pub(crate) enum ShutdownState {
    /// Normal operation.
    NoShutdown,
    /// Shutdown requested.
    ///
    /// No new WAL is accepted, but buffered or in-flight destination work is
    /// still allowed to drain.
    DrainingForShutdown,
    /// Shutdown drain completed.
    ///
    /// The loop now waits only for PostgreSQL to acknowledge the shutdown
    /// flush position.
    WaitingForPrimaryKeepAlive {
        /// The LSN we sent in the status update that PostgreSQL should
        /// acknowledge.
        acked_flush_lsn: PgLsn,
    },
}

impl ShutdownState {
    /// Returns `true` if a shutdown has been requested.
    pub(crate) fn is_requested(&self) -> bool {
        !matches!(self, Self::NoShutdown)
    }
}

/// Resources for the apply worker during the apply loop.
///
/// Contains all state and dependencies needed by the apply worker to coordinate
/// with table sync workers and manage table lifecycle transitions.
#[derive(Debug, Clone)]
pub(crate) struct ApplyWorkerContext<S, D> {
    /// Unique identifier for the pipeline.
    pub(crate) pipeline_id: PipelineId,
    /// Shared configuration for all coordinated operations.
    pub(crate) config: Arc<PipelineConfig>,
    /// Pool of table sync workers that this worker coordinates.
    pub(crate) pool: Arc<TableSyncWorkerPool>,
    /// State store for tracking table replication progress.
    pub(crate) store: S,
    /// Destination where replicated data is written.
    pub(crate) destination: D,
    /// Shared per-table protocol state used to decode relation and row
    /// messages.
    pub(crate) shared_table_cache: SharedTableCache,
    /// Shutdown signal receiver for graceful termination.
    pub(crate) shutdown_rx: ShutdownRx,
    /// Semaphore controlling maximum concurrent table sync workers.
    pub(crate) table_sync_worker_permits: Arc<Semaphore>,
    /// Shared memory backpressure controller.
    pub(crate) memory_monitor: MemoryMonitor,
    /// Shared batch budget controller.
    pub(crate) batch_budget: BatchBudgetController,
}

/// Resources for the table sync worker during the apply loop.
///
/// Contains state and dependencies needed by a table sync worker to track
/// its synchronization progress and coordinate with the apply worker.
#[derive(Debug, Clone)]
pub(crate) struct TableSyncWorkerContext<S> {
    /// Unique identifier for the table being synchronized.
    pub(crate) table_id: TableId,
    /// Thread-safe state management for this worker.
    pub(crate) table_sync_worker_state: TableSyncWorkerState,
    /// State store for persisting replication progress.
    pub(crate) state_store: S,
}

/// Context for the worker driving the apply loop.
///
/// This enum replaces the [`ApplyLoopHook`] trait, providing direct access to
/// worker-specific resources and enabling different behavior based on the
/// worker type at various points in the replication cycle.
#[derive(Debug, Clone)]
pub(crate) enum WorkerContext<S, D> {
    /// Context for the apply worker.
    Apply(ApplyWorkerContext<S, D>),
    /// Context for a table sync worker.
    TableSync(TableSyncWorkerContext<S>),
}

impl<S, D> WorkerContext<S, D> {
    /// Returns the [`WorkerType`] for this context.
    pub(crate) fn worker_type(&self) -> WorkerType {
        match self {
            Self::Apply(_) => WorkerType::Apply,
            Self::TableSync(ctx) => WorkerType::TableSync { table_id: ctx.table_id },
        }
    }

    /// Builds the logical apply-stream id for this worker context.
    pub(crate) fn apply_stream_id(&self) -> String {
        match self {
            Self::Apply(_) => apply_worker_apply_stream_id(),
            Self::TableSync(ctx) => table_sync_worker_apply_stream_id(ctx.table_id),
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
    /// Updates the last received LSN to a higher value if the new LSN is
    /// greater.
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

/// Result returned from [`ApplyLoop::handle_replication_message`] and related
/// functions.
#[derive(Debug, Default)]
struct HandleMessageResult {
    /// The event converted from the replication message.
    event: Option<Event>,
    /// Set to a commit message's end_lsn value, [`None`] otherwise.
    end_lsn: Option<PgLsn>,
    /// Set when a batch should be ended earlier than the normal batching
    /// parameters.
    end_batch: bool,
    /// Set when the table has encountered an error.
    table_replication_error: Option<TableReplicationError>,
}

impl HandleMessageResult {
    /// Creates a result with no event and no side effects.
    fn no_event() -> Self {
        Self::default()
    }

    /// Creates a result that returns an event without affecting batch state.
    fn return_event(event: Event) -> Self {
        Self { event: Some(event), ..Default::default() }
    }
}

/// Mutable runtime state that evolves throughout the apply loop.
#[derive(Debug)]
struct ApplyLoopState {
    /// The highest LSN received from the [`end_lsn`] field of a [`Commit`]
    /// message.
    last_commit_end_lsn: Option<PgLsn>,
    /// The LSN of the commit WAL entry of the transaction that is currently
    /// being processed.
    remote_final_lsn: Option<PgLsn>,
    /// The current replication progress tracking received and flushed LSN
    /// positions.
    replication_progress: ReplicationProgress,
    /// A batch of events to send to the destination.
    events_batch: Vec<Event>,
    /// Approximate total size in bytes of events currently in the batch.
    events_batch_bytes: usize,
    /// Instant from when a transaction began.
    current_tx_begin_ts: Option<Instant>,
    /// Number of events observed in the current transaction (excluding
    /// BEGIN/COMMIT).
    current_tx_events: u64,
    /// Next zero-based ordinal to assign to transaction-scoped events.
    next_tx_ordinal: u64,
    /// The current shutdown state tracking graceful shutdown progress.
    shutdown_state: ShutdownState,
    /// The deadline by which the current batch must be flushed.
    flush_deadline: Option<Instant>,
    /// The deadline for the next proactive keep alive status update.
    keep_alive_deadline: Instant,
    /// Destination write result waiting to be applied to replication progress.
    pending_flush_result: Option<PendingWriteEventsResult<()>>,
    /// The strongest exit that this apply loop invocation should eventually
    /// return.
    ///
    /// Once set, the loop stops ingesting new replication messages and instead
    /// drains any in-flight flushes and shutdown barriers before returning
    /// the final result.
    exit_intent: Option<ExitIntent>,
    /// Set to `true` when a flush was deferred because another flush result was
    /// still in flight.
    ///
    /// While this is set, the loop stops both deadline-driven flush attempts
    /// and new message intake until the in-flight flush resolves and the
    /// queued batch can be retried.
    processing_paused: bool,
    /// Fallback snapshot used before a table has established shared protocol
    /// state.
    ///
    /// This is seeded from the worker start LSN so a first `RELATION` message
    /// can always resolve the latest schema version whose snapshot is less
    /// than or equal to the worker's start point.
    bootstrap_snapshot_id: SnapshotId,
    /// Replication slot name used by this loop.
    slot_name: String,
    /// Shared schema cleanup deadline.
    ///
    /// `None` means a cleanup task is already running. The background cleanup
    /// task resets this after it finishes.
    schema_cleanup_deadline: Arc<Mutex<Option<Instant>>>,
    /// Background schema cleanup task owned by this apply loop.
    schema_cleanup_task: Option<JoinHandle<()>>,
}

/// Running schema cleanup marker.
///
/// Finishing the marker schedules the next cleanup deadline. This keeps the
/// deadline mutex private while allowing the background task to reset it.
#[derive(Debug)]
struct SchemaCleanupRun {
    /// Shared cleanup deadline owned by [`ApplyLoopState`].
    deadline: Arc<Mutex<Option<Instant>>>,
}

impl SchemaCleanupRun {
    /// Schedules the next cleanup after the current run finishes.
    async fn finish(self) {
        let mut deadline = self.deadline.lock().await;
        *deadline = Some(Instant::now() + SCHEMA_CLEANUP_INTERVAL);
    }
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial replication progress.
    fn new(
        replication_progress: ReplicationProgress,
        keep_alive_deadline_duration: Duration,
        bootstrap_snapshot_id: SnapshotId,
        slot_name: String,
    ) -> Self {
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
            keep_alive_deadline: Instant::now() + keep_alive_deadline_duration,
            pending_flush_result: None,
            exit_intent: None,
            processing_paused: false,
            bootstrap_snapshot_id,
            slot_name,
            schema_cleanup_deadline: Arc::new(Mutex::new(Some(
                Instant::now() + SCHEMA_CLEANUP_INTERVAL,
            ))),
            schema_cleanup_task: None,
        }
    }

    /// Adds an event to the batch.
    fn add_event_to_batch(&mut self, event: Event) {
        // We track the current number of bytes in the batch.
        self.events_batch_bytes = self.events_batch_bytes.saturating_add(event.size_hint());

        // We add the element to the pending batch.
        self.events_batch.push(event);
    }

    /// Takes the events batch for further processing. Replacing it with a new
    /// empty batch.
    fn take_events_batch(&mut self) -> (Vec<Event>, usize) {
        let events_batch = std::mem::take(&mut self.events_batch);
        let events_batch_bytes = self.events_batch_bytes;
        self.events_batch_bytes = 0;

        (events_batch, events_batch_bytes)
    }

    /// Returns the bootstrap snapshot used before a table has shared protocol
    /// state.
    fn bootstrap_snapshot_id(&self) -> SnapshotId {
        self.bootstrap_snapshot_id
    }

    /// Returns the replication slot name used by this loop.
    fn slot_name(&self) -> &str {
        &self.slot_name
    }

    /// Sets the batch flush deadline, if not already set.
    ///
    /// The deadline stays armed until a flush is actually dispatched. If a
    /// flush attempt is deferred because another flush is still in flight,
    /// the deadline is intentionally preserved.
    fn set_flush_deadline_if_needed(&mut self, max_batch_fill_duration: Duration) {
        if self.flush_deadline.is_some() {
            return;
        }

        self.flush_deadline = Some(Instant::now() + max_batch_fill_duration);

        debug!("started batch flush timer");
    }

    /// Resets the batch flush deadline after a batch has been dispatched.
    fn reset_flush_deadline(&mut self) {
        self.flush_deadline = None;

        debug!("reset batch flush timer");
    }

    /// Resets the keep alive deadline using the configured duration.
    fn reset_keep_alive_deadline(&mut self, keep_alive_deadline_duration: Duration) {
        self.keep_alive_deadline = Instant::now() + keep_alive_deadline_duration;
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

    /// Returns the last received LSN that should be reported as written to the
    /// PostgreSQL server.
    fn last_received_lsn(&self) -> PgLsn {
        self.replication_progress.last_received_lsn
    }

    /// Returns `true` if the apply loop is totally idle.
    fn is_idle(&self) -> bool {
        !self.handling_transaction() && !self.has_unresolved_batch_work()
    }

    /// Returns the effective flush LSN to report to PostgreSQL.
    ///
    /// When idle, returns the last received LSN since no actual flushes occur.
    /// Otherwise, returns the last flush LSN from completed transactions.
    ///
    /// Note that when a transaction is now started, the last flush lsn will be
    /// used, and it might jump back compared to the last received lsn that
    /// we sent before, however this is fine since the status update logic
    /// guarantees monotonically increasing LSNs.
    fn effective_flush_lsn(&self) -> PgLsn {
        if self.is_idle() {
            self.replication_progress.last_received_lsn
        } else {
            self.replication_progress.last_flush_lsn
        }
    }

    /// Tries to mark schema cleanup as running if the deadline has elapsed.
    async fn try_start_schema_cleanup(&self) -> Option<SchemaCleanupRun> {
        let mut schema_cleanup_deadline = self.schema_cleanup_deadline.lock().await;
        let Some(deadline) = *schema_cleanup_deadline else {
            return None;
        };

        #[cfg(feature = "failpoints")]
        if etl_fail_point_active(FORCE_SCHEMA_CLEANUP_FP) {
            *schema_cleanup_deadline = None;
            return Some(SchemaCleanupRun { deadline: Arc::clone(&self.schema_cleanup_deadline) });
        }

        if Instant::now() < deadline {
            return None;
        }

        *schema_cleanup_deadline = None;
        Some(SchemaCleanupRun { deadline: Arc::clone(&self.schema_cleanup_deadline) })
    }

    /// Moves the schema cleanup deadline into the past for tests.
    #[cfg(test)]
    async fn expire_schema_cleanup_deadline(&self) {
        let mut schema_cleanup_deadline = self.schema_cleanup_deadline.lock().await;
        *schema_cleanup_deadline =
            Some(Instant::now() - SCHEMA_CLEANUP_INTERVAL - Duration::from_secs(1));
    }

    /// Returns `true` if a schema cleanup task is still recorded.
    fn has_schema_cleanup_task(&self) -> bool {
        self.schema_cleanup_task.is_some()
    }

    /// Takes the schema cleanup task if it has finished.
    fn take_finished_schema_cleanup_task(&mut self) -> Option<JoinHandle<()>> {
        if self.schema_cleanup_task.as_ref().is_some_and(|task| task.is_finished()) {
            self.schema_cleanup_task.take()
        } else {
            None
        }
    }

    /// Takes the schema cleanup task, whether it has finished or not.
    fn take_schema_cleanup_task(&mut self) -> Option<JoinHandle<()>> {
        self.schema_cleanup_task.take()
    }

    /// Sets the currently running schema cleanup task.
    fn set_schema_cleanup_task(&mut self, task: JoinHandle<()>) {
        self.schema_cleanup_task = Some(task);
    }

    /// Schedules schema cleanup again after the configured interval.
    async fn reset_schema_cleanup_deadline(&self) {
        let mut schema_cleanup_deadline = self.schema_cleanup_deadline.lock().await;
        *schema_cleanup_deadline = Some(Instant::now() + SCHEMA_CLEANUP_INTERVAL);
    }

    /// Returns true if the apply loop is in the middle of processing a
    /// transaction.
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
                    "transaction-local ordinal overflow detected; subsequent events may reuse the \
                     same ordinal"
                );

                self.next_tx_ordinal
            }
        };

        tx_ordinal
    }

    /// Returns `true` if there is a pending batch of events waiting to be
    /// flushed.
    fn has_pending_batch(&self) -> bool {
        !self.events_batch.is_empty()
    }

    /// Returns `true` if there is a batch flush in flight whose result has not
    /// yet resolved.
    fn has_pending_flush_result(&self) -> bool {
        self.pending_flush_result.is_some()
    }

    /// Returns `true` if any buffered or in-flight destination batch work is
    /// still unresolved.
    fn has_unresolved_batch_work(&self) -> bool {
        self.has_pending_batch() || self.has_pending_flush_result()
    }

    /// Records a new exit intent if one was produced.
    fn record_exit_intent(&mut self, exit_intent: Option<ExitIntent>) {
        let Some(exit_intent) = exit_intent else {
            return;
        };

        self.exit_intent = match self.exit_intent {
            Some(current_exit_intent) => Some(current_exit_intent.merge(exit_intent)),
            None => Some(exit_intent),
        };
    }

    /// Returns `true` when the apply loop may still accept new replication
    /// messages.
    fn can_process_messages(&self) -> bool {
        self.exit_intent.is_none() && !self.processing_paused
    }

    /// Returns `true` when the batch deadline timer may still trigger a flush
    /// for buffered work.
    fn can_wait_for_deadline(&self) -> bool {
        !self.processing_paused && self.has_pending_batch()
    }

    /// Marks the current pending batch as paused behind an in-flight flush.
    fn pause_processing(&mut self) {
        self.processing_paused = true;
    }

    /// Resumes processing by clearing the existing pending flush result and
    /// enabling processing.
    fn resume_processing(&mut self) -> bool {
        let prev_processing_paused = std::mem::replace(&mut self.processing_paused, false);
        self.pending_flush_result = None;

        prev_processing_paused
    }

    /// Returns the final result requested by this loop, if any.
    fn exit_result(&self) -> Option<ApplyLoopResult> {
        self.exit_intent.map(ExitIntent::to_result)
    }
}

/// Main apply loop implementation that processes replication events.
///
/// [`ApplyLoop`] encapsulates the apply loop's immutable dependencies plus its
/// mutable runtime state.
pub(crate) struct ApplyLoop<S, D> {
    /// Shared immutable configuration.
    config: Arc<PipelineConfig>,
    /// Schema store for table schemas.
    schema_store: S,
    /// Destination where replicated data is written.
    destination: D,
    /// Shared per-table protocol state used to decode relation and row
    /// messages.
    shared_table_cache: SharedTableCache,
    /// Shutdown signal receiver.
    shutdown_rx: ShutdownRx,
    /// Worker-specific dependencies and coordination hooks.
    worker_context: WorkerContext<S, D>,
    /// Shared memory backpressure controller.
    memory_monitor: MemoryMonitor,
    /// Cached dynamic batch budget used to decide flushes by bytes.
    cached_batch_budget: CachedBatchBudget,
    /// Maximum duration to wait before forcibly flushing a batch.
    max_batch_fill_duration: Duration,
    /// Deadline duration used before proactively sending a periodic status
    /// update.
    keep_alive_deadline_duration: Duration,
    /// Mutable loop state.
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
    pub(crate) async fn start(
        pipeline_id: PipelineId,
        start_lsn: PgLsn,
        config: Arc<PipelineConfig>,
        replication_client: PgReplicationClient,
        schema_store: S,
        destination: D,
        shared_table_cache: SharedTableCache,
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

        let worker_type = worker_context.worker_type();
        let keep_alive_deadline_duration = match replication_client.get_wal_sender_timeout().await {
            Ok(Some(wal_sender_timeout)) => {
                Self::compute_keep_alive_deadline_duration(wal_sender_timeout)
            }
            Ok(None) => {
                warn!(
                    %worker_type,
                    "wal sender timeout is disabled; using heuristic keep alive deadline",
                );

                Self::compute_keep_alive_deadline_duration(DEFAULT_KEEP_ALIVE_DURATION)
            }
            Err(error) => {
                warn!(
                    %worker_type,
                    error = %error,
                    "failed to read wal sender timeout; using heuristic keep alive deadline",
                );

                Self::compute_keep_alive_deadline_duration(DEFAULT_KEEP_ALIVE_DURATION)
            }
        };
        let bootstrap_snapshot_id: SnapshotId = start_lsn.into();

        let initial_progress =
            ReplicationProgress { last_received_lsn: start_lsn, last_flush_lsn: start_lsn };

        let slot_name: String = worker_type.build_etl_replication_slot(pipeline_id).try_into()?;

        let state = ApplyLoopState::new(
            initial_progress,
            keep_alive_deadline_duration,
            bootstrap_snapshot_id,
            slot_name,
        );

        let mut apply_loop = Self {
            config: Arc::clone(&config),
            schema_store,
            destination,
            shared_table_cache,
            shutdown_rx,
            worker_context,
            memory_monitor,
            cached_batch_budget: batch_budget.cached(),
            max_batch_fill_duration: Duration::from_millis(config.batch.max_fill_ms),
            keep_alive_deadline_duration,
            state,
        };

        apply_loop.run_with_teardown(replication_client, start_lsn).await
    }

    /// Runs the apply loop and performs teardown work before returning.
    async fn run_with_teardown(
        &mut self,
        replication_client: PgReplicationClient,
        start_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopResult> {
        let result = self.run(replication_client, start_lsn).await;
        self.drain_schema_cleanup_task().await;

        result
    }

    /// Runs the main event processing loop.
    async fn run(
        &mut self,
        replication_client: PgReplicationClient,
        start_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopResult> {
        let logical_replication_stream = replication_client
            .start_logical_replication(
                &self.config.publication_name,
                self.state.slot_name(),
                start_lsn,
            )
            .await?;

        let events_stream = EventsStream::wrap(logical_replication_stream);
        let events_stream = BackpressureStream::wrap(
            events_stream,
            self.worker_context.apply_stream_id(),
            self.memory_monitor.subscribe(),
        );
        pin!(events_stream);
        let mut connection_updates_rx = replication_client.connection_updates_rx();

        loop {
            #[cfg(feature = "failpoints")]
            if matches!(self.state.shutdown_state, ShutdownState::WaitingForPrimaryKeepAlive { .. })
                && etl_fail_point_active(SEND_STATUS_UPDATE_FP)
            {
                warn!("not waiting for primary keep alive on shutdown due to active failpoint");

                return Ok(self.finish_shutdown());
            }

            let iteration_result = match &self.state.shutdown_state {
                ShutdownState::NoShutdown => {
                    self.run_active_iteration(
                        events_stream.as_mut(),
                        &replication_client,
                        &mut connection_updates_rx,
                    )
                    .await
                }
                ShutdownState::DrainingForShutdown => {
                    self.run_draining_shutdown_iteration(
                        events_stream.as_mut(),
                        &mut connection_updates_rx,
                    )
                    .await
                }
                ShutdownState::WaitingForPrimaryKeepAlive { acked_flush_lsn } => {
                    self.run_shutdown_wait_iteration(
                        events_stream.as_mut(),
                        &mut connection_updates_rx,
                        *acked_flush_lsn,
                    )
                    .await
                }
            };
            let result = iteration_result?;

            if let Some(result) = result {
                return Ok(result);
            }
        }
    }

    /// Runs one normal apply-loop iteration while the worker is still actively
    /// processing.
    ///
    /// This keeps the priority order explicit:
    /// 1. Shutdown requests.
    /// 2. PostgreSQL connection lifecycle updates.
    /// 3. Pending destination flush results.
    /// 4. Completed background schema cleanup tasks.
    /// 5. Batch flush deadline expiry.
    /// 6. Incoming replication messages.
    /// 7. Periodic heartbeats once the computed keep alive deadline expires.
    ///
    /// PostgreSQL normally sends keep alives at roughly half of
    /// `wal_sender_timeout`. We wait a little longer than that before
    /// proactively emitting our own status update so that normal PostgreSQL
    /// keep alives still drive the loop, but long stalls can still be recovered
    /// without waiting for the full server timeout. This timeout branch is
    /// therefore a last-resort mechanism, not the primary source of
    /// progress. It matters when the loop is healthy but effectively stuck
    /// on in-flight work, such as waiting for an older batch to flush before a
    /// queued batch can be dispatched, or when keep alives are temporarily not
    /// reaching the loop because the stream is backpressured or the source
    /// is not sending them promptly.
    ///
    /// Each branch performs its work and then relies on
    /// [`Self::try_finish_active_iteration`] to decide whether the loop may
    /// return yet.
    async fn run_active_iteration(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        replication_client: &PgReplicationClient,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
    ) -> EtlResult<Option<ApplyLoopResult>> {
        tokio::select! {
            biased;

            // PRIORITY 1: Handle shutdown signals.
            // Shutdown stops new intake first and then lets the loop drain or wait as needed.
            _ = self.shutdown_rx.changed() => {
                self.handle_shutdown_signal(events_stream.as_mut()).await?;
            }

            // PRIORITY 2: Handle PostgreSQL connection lifecycle updates.
            // A closed or errored source connection always stops the loop immediately.
            changed = connection_updates_rx.changed() => {
                Self::handle_connection_update(changed, connection_updates_rx)?;
            }

            // PRIORITY 3: Handle the pending destination write result.
            // Finishing an in-flight flush may advance progress and unblock a queued batch.
            apply_result = Self::wait_for_flush_result(self.state.pending_flush_result.as_mut()), if self.state.pending_flush_result.is_some() => {
                self.handle_flush_result(apply_result)
                    .await?;
            }

            // PRIORITY 4: Handle batch flush timer expiry.
            // This prevents buffered work from waiting forever when traffic is low.
            _ = Self::wait_for_batch_deadline(self.state.flush_deadline), if self.state.can_wait_for_deadline() => {
                self.flush_batch("flush deadline reached").await?;
            }

            // PRIORITY 5: Process incoming replication messages from PostgreSQL.
            // New WAL messages are only accepted while the loop is still actively ingesting.
            maybe_message = events_stream.next(), if self.state.can_process_messages() => {
                self.handle_stream_message(
                    events_stream.as_mut(),
                    maybe_message,
                    replication_client,
                )
                .await?;
            }

            // PRIORITY 6: Emit a periodic status update once the computed keep alive deadline
            // expires. This intentionally resends the same effective flush LSN so PostgreSQL keeps
            // the standby connection open during long stalls, including cases where the loop is
            // paused behind an in-flight flush and therefore not making visible progress yet. This
            // is only a fallback path: most status updates should still be triggered by incoming
            // PostgreSQL primary keep alive messages during normal operation.
            _ = Self::wait_for_keep_alive_deadline(self.state.keep_alive_deadline) => {
                self.send_status_update(
                    events_stream.as_mut(),
                    self.state.effective_flush_lsn(),
                    true,
                    StatusUpdateType::PeriodicKeepAlive,
                )
                .await?;

                self.state
                    .reset_keep_alive_deadline(self.keep_alive_deadline_duration);
            }
        }

        // We try to process syncing tables when the system is idle to make sure the
        // system advances synchronization state even when no data is being
        // actively transferred. This is especially important to have the tables
        // converge to `Ready` or `SyncDone` even if there is no traffic on that
        // specific slot. In that case, the process is driven by keep alive messages
        // which will advance the `last_received_lsn` and properly use that for
        // the syncing LSN to establish progress.
        self.maybe_process_syncing_tables_when_idle().await?;

        Ok(self.try_finish_active_iteration())
    }

    /// Runs one iteration of the shutdown drain phase.
    ///
    /// This mirrors the active phase but omits shutdown handling and new WAL
    /// intake.
    ///
    /// Priority order:
    /// 1. PostgreSQL connection lifecycle updates.
    /// 2. Pending destination flush results.
    /// 3. Batch flush deadline expiry.
    /// 4. Periodic keep alive status updates.
    ///
    /// After the selected branch runs, the loop advances idle syncing state.
    /// Once buffered or in-flight destination work is resolved, it sends the
    /// final shutdown status update and transitions to
    /// [`ShutdownState::WaitingForPrimaryKeepAlive`].
    async fn run_draining_shutdown_iteration(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
    ) -> EtlResult<Option<ApplyLoopResult>> {
        tokio::select! {
            biased;

            // PRIORITY 1: Handle PostgreSQL connection lifecycle updates.
            changed = connection_updates_rx.changed() => {
                Self::handle_connection_update(changed, connection_updates_rx)?;
            }

            // PRIORITY 2: Handle the pending destination write result.
            apply_result = Self::wait_for_flush_result(self.state.pending_flush_result.as_mut()), if self.state.pending_flush_result.is_some() => {
                self.handle_flush_result(apply_result)
                    .await?;
            }

            // PRIORITY 3: Handle batch flush timer expiry.
            _ = Self::wait_for_batch_deadline(self.state.flush_deadline), if self.state.can_wait_for_deadline() => {
                self.flush_batch("flush deadline reached during shutdown drain").await?;
            }

            // PRIORITY 4: Emit a periodic status update while shutdown is draining.
            _ = Self::wait_for_keep_alive_deadline(self.state.keep_alive_deadline) => {
                self.send_status_update(
                    events_stream.as_mut(),
                    self.state.effective_flush_lsn(),
                    true,
                    StatusUpdateType::PeriodicKeepAlive,
                )
                .await?;

                self.state
                    .reset_keep_alive_deadline(self.keep_alive_deadline_duration);
            }
        }

        // Try to keep advancing syncing tables whenever the system becomes idle.
        self.maybe_process_syncing_tables_when_idle().await?;

        // Once the drain is complete, start waiting for PostgreSQL to
        // acknowledge the final flush position.
        if !self.state.has_unresolved_batch_work() {
            self.initiate_graceful_shutdown(events_stream.as_mut()).await?;
        }

        Ok(None)
    }

    /// Runs one iteration of the final shutdown acknowledgement phase.
    ///
    /// In this phase the loop no longer accepts new replication messages and
    /// no longer waits for destination flush results.
    ///
    /// Priority order:
    /// 1. PostgreSQL connection lifecycle updates.
    /// 2. PostgreSQL keepalives that may acknowledge `acked_flush_lsn`.
    /// 3. Completed background schema cleanup tasks.
    /// 4. Periodic keep alive status updates.
    ///
    /// Once the acknowledgement arrives, unresolved work causes
    /// [`ApplyLoopResult::Paused`]; otherwise the recorded exit result may be
    /// reused.
    async fn run_shutdown_wait_iteration(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
        acked_flush_lsn: PgLsn,
    ) -> EtlResult<Option<ApplyLoopResult>> {
        tokio::select! {
            biased;

            // PRIORITY 1: Handle PostgreSQL connection lifecycle updates.
            // A closed or errored source connection always stops the loop immediately.
            changed = connection_updates_rx.changed() => {
                Self::handle_connection_update(changed, connection_updates_rx)?;
            }

            // PRIORITY 2: Wait for the keepalive that acknowledges the shutdown flush LSN.
            // Once this barrier is reached, shutdown may finish.
            message = events_stream.next() => {
                if self
                    .handle_shutdown_keepalive_wait_message(events_stream.as_mut(), message, acked_flush_lsn)
                    .await?
                {
                    return Ok(Some(self.finish_shutdown()));
                }
            }

            // PRIORITY 3: Resend a heartbeat once the computed keep alive deadline expires while
            // shutdown is waiting for the primary keep alive acknowledgement barrier. This is a
            // last-resort safeguard for cases where PostgreSQL keep alives stop reaching the loop,
            // for example because the source has gone quiet, the stream is backpressured, or the
            // network is delayed. In normal operation, PostgreSQL's own keep alives should drive
            // this wait.
            _ = Self::wait_for_keep_alive_deadline(self.state.keep_alive_deadline) => {
                self.send_status_update(
                    events_stream.as_mut(),
                    self.state.effective_flush_lsn(),
                    true,
                    StatusUpdateType::PeriodicKeepAlive,
                )
                .await?;

                self.state
                    .reset_keep_alive_deadline(self.keep_alive_deadline_duration);
            }
        }

        Ok(None)
    }

    /// Returns the final loop result after PostgreSQL has acknowledged the
    /// shutdown status update.
    ///
    /// By the time shutdown reaches this point, the drain phase should have
    /// resolved any buffered or in-flight destination work. The loop therefore
    /// reuses the recorded exit result and falls back to pausing only if no
    /// exit intent was recorded unexpectedly.
    fn finish_shutdown(&self) -> ApplyLoopResult {
        debug_assert!(!self.state.has_unresolved_batch_work());

        self.state.exit_result().unwrap_or(ApplyLoopResult::Paused)
    }

    /// Returns the final loop result for the active phase if all exit barriers
    /// have been resolved.
    fn try_finish_active_iteration(&self) -> Option<ApplyLoopResult> {
        if self.state.shutdown_state.is_requested() || self.state.has_unresolved_batch_work() {
            return None;
        }

        self.state.exit_result()
    }

    /// Processes one PostgreSQL connection lifecycle notification.
    ///
    /// `changed()` only tells us that some update exists, so we must still read
    /// the latest value with `borrow_and_update()` to consume it without
    /// missing races between notification and observation.
    fn handle_connection_update(
        changed: Result<(), watch::error::RecvError>,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
    ) -> EtlResult<()> {
        if changed.is_err() {
            return Err(etl_error!(
                ErrorKind::SourceConnectionFailed,
                "postgresql connection updates ended during the apply loop"
            ));
        }

        let update = connection_updates_rx.borrow_and_update().clone();
        match update {
            PostgresConnectionUpdate::Running => Ok(()),
            PostgresConnectionUpdate::Terminated => Err(etl_error!(
                ErrorKind::SourceConnectionFailed,
                "postgresql connection terminated during the apply loop"
            )),
            PostgresConnectionUpdate::Errored { error } => Err(etl_error!(
                ErrorKind::SourceConnectionFailed,
                "postgresql connection errored during the apply loop",
                error.to_string()
            )),
        }
    }

    /// Waits for the batch flush deadline if one is set.
    async fn wait_for_batch_deadline(deadline: Option<Instant>) {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
            None => std::future::pending().await,
        }
    }

    /// Waits until the keep alive deadline expires.
    async fn wait_for_keep_alive_deadline(deadline: Instant) {
        tokio::time::sleep_until(deadline.into()).await;
    }

    /// Computes the keep alive deadline from PostgreSQL's `wal_sender_timeout`.
    ///
    /// PostgreSQL normally sends a keep alive after roughly half of this
    /// timeout. We therefore use `60%` of the configured timeout to allow
    /// normal server keep alives to arrive first while still leaving
    /// comfortable room for network and processing latency before the full
    /// timeout.
    fn compute_keep_alive_deadline_duration(wal_sender_timeout: Duration) -> Duration {
        wal_sender_timeout
            .mul_f64(KEEP_ALIVE_DEADLINE_FRACTION)
            .max(MIN_KEEP_ALIVE_DEADLINE_DURATION)
    }

    /// Processes a replication message while shutdown is waiting for PostgreSQL
    /// to acknowledge the requested flush LSN.
    ///
    /// Returns `true` if the message is a keepalive with `wal_end >=
    /// acked_flush_lsn`, `false` if still waiting for the right keepalive.
    async fn handle_shutdown_keepalive_wait_message(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        message: Option<EtlResult<ReplicationMessage<LogicalReplicationMessage>>>,
        acked_flush_lsn: PgLsn,
    ) -> EtlResult<bool> {
        let worker_type = self.worker_context.worker_type();

        let Some(message) = message else {
            warn!(
                %worker_type,
                "replication stream ended while waiting for keep alive acknowledgement",
            );

            bail!(
                ErrorKind::SourceConnectionFailed,
                "Replication stream ended while waiting for keep alive acknowledgement"
            )
        };

        match message? {
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = PgLsn::from(keepalive.wal_end());

                // `acked_flush_lsn` is the effective flush LSN included in the status update
                // for shutdown. We only treat a keepalive as the shutdown
                // acknowledgement once its `wal_end` has reached or passed that
                // barrier, which guarantees the reply was produced after
                // PostgreSQL observed at least that replication position.
                if wal_end >= acked_flush_lsn {
                    info!(
                        %worker_type,
                        %wal_end,
                        %acked_flush_lsn,
                        "received keep alive acknowledgement, safe to shutdown",
                    );

                    self.state.reset_keep_alive_deadline(self.keep_alive_deadline_duration);

                    return Ok(true);
                }

                // This shouldn't happen in practice because if we did process up to LSN `x` it
                // means next keep alive messages from that must be at least
                // `x`.
                debug!(
                    %worker_type,
                    %wal_end,
                    %acked_flush_lsn,
                    "received keep alive but wal_end < acked_flush_lsn, continuing to wait",
                );

                self.send_status_update(
                    events_stream.as_mut(),
                    self.state.effective_flush_lsn(),
                    true,
                    StatusUpdateType::KeepAlive,
                )
                .await?;

                self.state.reset_keep_alive_deadline(self.keep_alive_deadline_duration);
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

    /// Handles a shutdown signal by transitioning to
    /// [`ShutdownState::DrainingForShutdown`] or
    /// [`ShutdownState::WaitingForPrimaryKeepAlive`].
    ///
    /// Shutdown stops new message intake immediately. If there is already
    /// buffered or in-flight destination work, the loop first drains that work
    /// so the best durable position can advance before sending the final
    /// shutdown status update. Otherwise it transitions directly into waiting
    /// for PostgreSQL to acknowledge the current flush position.
    ///
    /// Note: the shutdown system is best-effort. Graceful shutdown may not
    /// complete if we are blocked on non-interruptible code or if keepalive
    /// messages never arrive from the server. It is the responsibility of
    /// the caller to forcefully kill the process if shutdown does
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

        // Shutdown always means this apply loop invocation will eventually return, even
        // if a later quiescent state upgrades the final result from pause to
        // complete.
        self.state.record_exit_intent(Some(ExitIntent::Pause));

        // If there is unresolved work, we want to drain it before shutting down.
        if self.state.has_unresolved_batch_work() {
            info!(
                %worker_type,
                pending_flush_result = self.state.has_pending_flush_result(),
                pending_batch = self.state.has_pending_batch(),
                processing_paused = self.state.processing_paused,
                "shutdown signal received, stopping new intake and entering shutdown drain",
            );

            self.state.shutdown_state = ShutdownState::DrainingForShutdown;

            return Ok(());
        }

        info!(
            %worker_type,
            "shutdown signal received, no unresolved work left, entering final acknowledgement wait",
        );

        self.initiate_graceful_shutdown(events_stream.as_mut()).await
    }

    /// Initiates graceful shutdown by sending a status update and transitioning
    /// to [`ShutdownState::WaitingForPrimaryKeepAlive`].
    ///
    /// The status update uses the best durable position currently known by the
    /// loop.
    async fn initiate_graceful_shutdown(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
    ) -> EtlResult<()> {
        let worker_type = self.worker_context.worker_type();

        // Use effective flush LSN to report last received LSN when idle, since
        // last flush LSN only advances during actual flushes.
        let flush_lsn = self.state.effective_flush_lsn();

        info!(
            %worker_type,
            %flush_lsn,
            "sending shutdown status update and waiting for primary keep alive acknowledgement",
        );

        self.send_status_update(
            events_stream.as_mut(),
            flush_lsn,
            true,
            StatusUpdateType::ShutdownFlush,
        )
        .await?;

        self.state.shutdown_state =
            ShutdownState::WaitingForPrimaryKeepAlive { acked_flush_lsn: flush_lsn };

        Ok(())
    }

    /// Sends a status update to PostgreSQL using the current write and flush
    /// positions.
    ///
    /// Some callers intentionally resend the same effective flush LSN with
    /// `force = true`. Those updates are about keeping the replication
    /// connection alive while the system is idle, not about advertising
    /// newly acknowledged progress. Keepalive replies from the main replication
    /// stream and the final shutdown acknowledgement path still use this same
    /// helper.
    async fn send_status_update(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        flush_lsn: PgLsn,
        force: bool,
        status_update_type: StatusUpdateType,
    ) -> EtlResult<()> {
        let status_update_result = events_stream
            .as_mut()
            .stream_mut()
            .send_status_update(
                self.state.last_received_lsn(),
                flush_lsn,
                force,
                status_update_type,
            )
            .await?;

        // If we sent a status update, we check if we can perform schema cleanup for old
        // table schema snapshots.
        if let StatusUpdateResult::Sent { flush_lsn } = status_update_result {
            self.maybe_spawn_schema_cleanup(flush_lsn).await?;
        }

        Ok(())
    }

    /// Spawns a best-effort task that prunes obsolete schema versions.
    ///
    /// Cleanup is interval-gated on the status-update path, but the task reads
    /// the slot's actual `confirmed_flush_lsn` before deleting anything. This
    /// avoids using the optimistic status update boundary, which is acceptable
    /// for replaying data but can break decoding if schema versions are pruned
    /// before PostgreSQL stores the acknowledgement.
    async fn maybe_spawn_schema_cleanup(&mut self, reported_flush_lsn: PgLsn) -> EtlResult<()> {
        self.collect_finished_schema_cleanup_task().await;

        if self.state.has_schema_cleanup_task() {
            return Ok(());
        }

        let Some(schema_cleanup_run) = self.state.try_start_schema_cleanup().await else {
            return Ok(());
        };

        let schema_store = self.schema_store.clone();
        let pg_connection = self.config.pg_connection.clone();
        let slot_name = self.state.slot_name().to_owned();
        let shared_table_cache = self.shared_table_cache.clone();
        let worker_type = self.worker_context.worker_type();
        let worker_context = self.worker_context.clone();

        let cleanup_fut = async move {
            let confirmed_flush_lsn = match Self::get_actual_confirmed_flush_lsn(
                pg_connection,
                &slot_name,
                reported_flush_lsn,
            )
            .await
            {
                Ok(confirmed_flush_lsn) => confirmed_flush_lsn,
                Err(err) => {
                    warn!(
                        %worker_type,
                        %reported_flush_lsn,
                        error = %err,
                        "skipping schema cleanup because slot progress could not be confirmed"
                    );

                    return;
                }
            };

            let table_ids = match Self::get_schema_cleanup_table_ids(
                &worker_context,
                &shared_table_cache,
                confirmed_flush_lsn,
            )
            .await
            {
                Ok(table_ids) => table_ids,
                Err(err) => {
                    error!(
                        %worker_type,
                        error = %err,
                        "failed to determine schema cleanup ownership"
                    );

                    return;
                }
            };

            if table_ids.is_empty() {
                return;
            }

            match schema_store.prune_table_schemas(table_ids, confirmed_flush_lsn).await {
                Ok(deleted_count) => {
                    counter!(
                        ETL_SCHEMA_CLEANUPS_TOTAL,
                        WORKER_TYPE_LABEL => worker_type.as_str(),
                    )
                    .increment(1);

                    if deleted_count > 0 {
                        info!(
                            %worker_type,
                            deleted_count,
                            "completed obsolete table schema cleanup"
                        );
                    }
                }
                Err(err) => {
                    counter!(
                        ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
                        WORKER_TYPE_LABEL => worker_type.as_str(),
                    )
                    .increment(1);

                    error!(
                        %worker_type,
                        error = %err,
                        "failed to clean up obsolete table schemas"
                    );
                }
            }
        };

        self.state.set_schema_cleanup_task(tokio::spawn(async move {
            cleanup_fut.await;
            schema_cleanup_run.finish().await;
        }));

        Ok(())
    }

    /// Returns the slot-confirmed flush LSN to use as the cleanup boundary.
    ///
    /// Streaming status updates are optimistic: after sending one, the client
    /// can keep processing and tolerate replay if PostgreSQL never stores the
    /// acknowledgement. Schema cleanup is different because deleting a schema
    /// that may still be needed on replay can stop decoding, so cleanup reads
    /// `confirmed_flush_lsn` from the replication slot.
    async fn get_actual_confirmed_flush_lsn(
        pg_connection: PgConnectionConfig,
        slot_name: &str,
        reported_flush_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        #[cfg(feature = "failpoints")]
        if etl_fail_point_active(FORCE_SCHEMA_CLEANUP_CONFIRMED_FLUSH_LSN_FP) {
            return Ok(reported_flush_lsn);
        }

        let replication_client = PgReplicationClient::connect(pg_connection).await?;
        let slot = replication_client.get_slot(&slot_name).await?;

        Ok(slot.confirmed_flush_lsn)
    }

    /// Returns table ids this worker may clean up at the confirmed LSN.
    ///
    /// The shared cache is used only to find active tables. The schema store
    /// computes the retained snapshot for each table from the confirmed flush
    /// LSN before deleting anything.
    async fn get_schema_cleanup_table_ids(
        worker_context: &WorkerContext<S, D>,
        shared_table_cache: &SharedTableCache,
        confirmed_flush_lsn: PgLsn,
    ) -> EtlResult<HashSet<TableId>> {
        let active_table_ids = shared_table_cache.active_table_ids().await;
        let mut table_ids = HashSet::with_capacity(active_table_ids.len());

        for table_id in active_table_ids {
            // Only prune snapshots for tables this worker would apply at this
            // flush position. This keeps table sync workers limited to their
            // assigned table while preserving apply worker ownership rules.
            let should_apply_changes = Self::should_apply_changes_for_context(
                worker_context,
                table_id,
                confirmed_flush_lsn,
            )
            .await?;

            if should_apply_changes {
                table_ids.insert(table_id);
            }
        }

        Ok(table_ids)
    }

    /// Records the result of a completed background schema cleanup task.
    async fn handle_schema_cleanup_task_result(
        &mut self,
        cleanup_result: Result<(), tokio::task::JoinError>,
    ) {
        if let Err(err) = cleanup_result {
            self.state.reset_schema_cleanup_deadline().await;

            let worker_type = self.worker_context.worker_type();
            counter!(
                ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
                WORKER_TYPE_LABEL => worker_type.as_str(),
            )
            .increment(1);

            error!(
                %worker_type,
                error = %err,
                "schema cleanup task failed before completing"
            );
        }
    }

    /// Collects the schema cleanup task if it has completed.
    async fn collect_finished_schema_cleanup_task(&mut self) {
        if let Some(cleanup_task) = self.state.take_finished_schema_cleanup_task() {
            self.handle_schema_cleanup_task_result(cleanup_task.await).await;
        }
    }

    /// Joins the background schema cleanup task before the apply loop exits.
    async fn drain_schema_cleanup_task(&mut self) {
        if let Some(cleanup_task) = self.state.take_schema_cleanup_task() {
            self.handle_schema_cleanup_task_result(cleanup_task.await).await;
        }
    }

    /// Waits for the pending flush result, if any.
    async fn wait_for_flush_result(
        pending_flush_result: Option<&mut PendingWriteEventsResult<()>>,
    ) -> CompletedWriteEventsResult<()> {
        match pending_flush_result {
            Some(flush_result) => flush_result.await,
            None => std::future::pending().await,
        }
    }

    /// Handles a completed batch flush result.
    async fn handle_flush_result(
        &mut self,
        flush_result: CompletedWriteEventsResult<()>,
    ) -> EtlResult<()> {
        // We clear the state up front because this flush is no longer in flight.
        let processing_paused = self.state.resume_processing();

        // Explode the result into parts which are used for handling the flush result.
        let (metadata, result) = flush_result.into_parts();

        // If there was an error in the flushing, we return it immediately.
        result?;

        if let Some(metadata) = metadata {
            counter!(
                ETL_EVENTS_PROCESSED_TOTAL,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                ACTION_LABEL => "table_streaming",
                DESTINATION_LABEL => D::name(),
            )
            .increment(metadata.metrics.items_count as u64);

            histogram!(
                ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                ACTION_LABEL => "table_streaming",
                DESTINATION_LABEL => D::name(),
            )
            .record(metadata.metrics.dispatched_at.elapsed().as_secs_f64());

            // We process the syncing tables with the last end lsn that the batch contains.
            //
            // Note that it could be that there is no end lsn for a specific batch, which
            // could happen if we process a huge transaction, and we don't reach the
            // commit before flushing. In that case, we don't process syncing
            // tables, meaning that progress it not tracked, since it's not going to
            // do anything because we can only track progress at commit boundaries.
            if let Some(commit_end_lsn) = metadata.commit_end_lsn {
                self.process_syncing_tables_after_flush(commit_end_lsn).await?;
            }
        }

        // If processing was paused, there must be a queued batch that still needs to be
        // flushed now that the previous in-flight result has resolved.
        if processing_paused {
            self.flush_batch("pending flush result received").await?;
        }

        Ok(())
    }

    /// Handles a message from the replication stream.
    ///
    /// Processes the message and manages batch timing.
    async fn handle_stream_message(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        maybe_message: Option<EtlResult<ReplicationMessage<LogicalReplicationMessage>>>,
        replication_client: &PgReplicationClient,
    ) -> EtlResult<()> {
        // If there is no message anymore, it means that the connection has been closed
        // or had some issues, we must handle this case.
        let Some(message) = maybe_message else {
            return Err(self.build_stream_ended_error(replication_client));
        };

        // If the Postgres had an error, we want to raise it immediately.
        let message = message?;

        self.handle_replication_message_and_flush(events_stream.as_mut(), message).await
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
    ) -> EtlResult<()> {
        let result = self.handle_replication_message(events_stream.as_mut(), message).await?;

        if let Some(event) = result.event {
            // We add the element to the pending batch.
            self.state.add_event_to_batch(event);

            // We update the last end lsn of the commit that we encountered, if any.
            self.state.update_last_commit_end_lsn(result.end_lsn);

            // We start the batch timer for the flushing. This timer is needed to control
            // force flushing of a batch if its size is not reached in time.
            self.state.set_flush_deadline_if_needed(self.max_batch_fill_duration);
        }

        // We check for the batch flushing conditions before deciding whether to flush
        // or not.
        let batch_size_reached =
            self.state.events_batch_bytes >= self.cached_batch_budget.current_batch_size_bytes();
        let early_flush_requested = result.end_batch;
        let should_flush = batch_size_reached || early_flush_requested;

        if should_flush {
            let reason = if batch_size_reached {
                "max batch bytes reached"
            } else {
                "early flush requested"
            };

            self.flush_batch(reason).await?;
        }

        if let Some(error) = result.table_replication_error {
            self.mark_table_errored(error).await?;
        }

        Ok(())
    }

    /// Flushes the current batch of events to the destination.
    ///
    /// If a flush is already in flight, this pauses the loop and leaves the
    /// current batch queued until the pending flush result has been
    /// processed. The queued batch is then retried from
    /// [`Self::handle_flush_result`] when that in-flight flush resolves.
    async fn flush_batch(&mut self, reason: &str) -> EtlResult<()> {
        // If the batch is empty, we don't need to do anything.
        if !self.state.has_pending_batch() {
            return Ok(());
        }

        // A flush is already in flight. Pause processing until the result resolves, at
        // which point the loop will resume and dispatch this batch.
        if self.state.has_pending_flush_result() {
            self.state.pause_processing();
            return Ok(());
        }

        let (events_batch, events_batch_bytes) = self.state.take_events_batch();
        let events_batch_size = events_batch.len();
        info!(
            worker_type = %self.worker_context.worker_type(),
            batch_size = events_batch_size,
            batch_size_bytes = events_batch_bytes,
            %reason,
            "flushing batch to destination",
        );

        // Capture dispatch-time metrics; they are carried through the result channel
        // and recorded once the destination acknowledges the batch.
        let metadata = ApplyLoopAsyncResultMetadata {
            commit_end_lsn: self.state.last_commit_end_lsn.take(),
            metrics: DispatchMetrics {
                items_count: events_batch_size,
                dispatched_at: Instant::now(),
            },
        };

        // Create the flush result channel: the sender is handed to the destination and
        // the pending receiver is stored on the loop state until the
        // destination signals completion.
        let (flush_result, pending_flush_result) = WriteEventsResult::new(metadata);
        self.destination.write_events(events_batch, flush_result).await?;
        self.state.pending_flush_result = Some(pending_flush_result);

        // We reset the deadline for the batch, since we are now flushing a new batch.
        // The new deadline will start as soon as we process a new element.
        //
        // It's important to note that the deadline is removed only when the batch is
        // flushed and not before this way, if a batch fails to flush due to
        // inflight, it will be re-tried indefinitely until that finishes.
        self.state.reset_flush_deadline();

        Ok(())
    }

    /// Dispatches replication protocol messages to appropriate handlers.
    async fn handle_replication_message(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        message: ReplicationMessage<LogicalReplicationMessage>,
    ) -> EtlResult<HandleMessageResult> {
        counter!(
            ETL_REPLICATION_MESSAGES_TOTAL,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
        )
        .increment(1);

        match message {
            ReplicationMessage::XLogData(message) => {
                let start_lsn = PgLsn::from(message.wal_start());
                self.state.replication_progress.update_last_received_lsn(start_lsn);

                let end_lsn = PgLsn::from(message.wal_end());
                self.state.replication_progress.update_last_received_lsn(end_lsn);

                debug!(
                    %start_lsn,
                    %end_lsn,
                    "handling logical replication data message",
                );

                self.handle_logical_replication_message(start_lsn, message.into_data()).await
            }
            ReplicationMessage::PrimaryKeepAlive(message) => {
                let end_lsn = PgLsn::from(message.wal_end());
                self.state.replication_progress.update_last_received_lsn(end_lsn);

                debug!(
                    wal_end = %end_lsn,
                    reply_requested = message.reply() == 1,
                    "received keep alive",
                );

                self.send_status_update(
                    events_stream.as_mut(),
                    // Use effective flush LSN to report last received LSN when idle, since
                    // last flush LSN only advances during actual flushes.
                    self.state.effective_flush_lsn(),
                    message.reply() == 1,
                    StatusUpdateType::KeepAlive,
                )
                .await?;

                self.state.reset_keep_alive_deadline(self.keep_alive_deadline_duration);

                Ok(HandleMessageResult::no_event())
            }
            _ => Ok(HandleMessageResult::no_event()),
        }
    }

    /// Processes logical replication messages and converts them to typed
    /// events.
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
            LogicalReplicationMessage::Message(message_body) => {
                self.handle_message(start_lsn, message_body).await
            }
            _ => Ok(HandleMessageResult::default()),
        }
    }

    /// Handles Postgres MESSAGE messages (pg_logical_emit_message).
    ///
    /// For `supabase_etl_ddl`, we persist the new table schema as soon as the
    /// logical message is decoded and invalidate any cached replication mask
    /// for that table before more changes in the same transaction are
    /// processed.
    ///
    /// This ordering matches how `pgoutput` produces the stream:
    /// - `pgoutput_message()` writes logical `Message` records directly and
    ///   does not inject `Relation` metadata.
    /// - `Relation` records are synthesized lazily by `maybe_send_schema()`
    ///   only when `pgoutput_change()` is about to emit a DML change.
    /// - relcache invalidation from the DDL resets `schema_sent`, so the first
    ///   post-DDL DML for the relation gets a fresh `Relation` message just
    ///   before the row event.
    ///
    /// In other words, the protocol variant this code relies on is:
    /// `... -> ddl Message -> Relation(new schema) -> Insert/Update/Delete
    /// ...`. Because the DDL message itself is not a DML event, we must
    /// update the stored schema and drop the old mask here, so that the
    /// very next `Relation` rebuilds the mask against the new schema
    /// snapshot.
    async fn handle_message(
        &mut self,
        start_lsn: PgLsn,
        message: &protocol::MessageBody,
    ) -> EtlResult<HandleMessageResult> {
        // If the prefix is unknown, we don't want to process it.
        let prefix = message.prefix()?;
        if prefix != DDL_MESSAGE_PREFIX {
            info!(
                prefix = %prefix,
                "received logical message with unknown prefix, discarding"
            );

            return Ok(HandleMessageResult::no_event());
        }

        // DDL messages must be transactional.
        let Some(remote_final_lsn) = self.state.remote_final_lsn else {
            bail!(
                ErrorKind::InvalidState,
                "Invalid transaction state",
                "DDL schema change messages must be transactional (transactional=true). Received \
                 a DDL message outside of a transaction boundary."
            );
        };

        let content = message.content()?;
        let schema_change_message = match SchemaChangeMessage::from_str(content) {
            Ok(schema_change_message) => schema_change_message,
            Err(err) => {
                counter!(
                    ETL_DDL_SCHEMA_CHANGES_TOTAL,
                    WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                    COMMAND_TAG_LABEL => "unknown",
                    OUTCOME_LABEL => "failed_parse",
                )
                .increment(1);

                return Err(err);
            }
        };

        let table_id = schema_change_message.table_id();
        let command_tag = schema_change_message.command_tag.clone();
        let columns_count = schema_change_message.columns.len();

        // Exactly one worker owns protocol interpretation for a table at a time. If
        // this worker is not the owner, it must skip the DDL so the owning
        // worker is solely responsible for advancing the shared per-table
        // protocol state.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            counter!(
                ETL_DDL_SCHEMA_CHANGES_TOTAL,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                COMMAND_TAG_LABEL => command_tag,
                OUTCOME_LABEL => "skipped",
            )
            .increment(1);

            return Ok(HandleMessageResult::no_event());
        }

        info!(
            table_id = schema_change_message.oid,
            table_name = %schema_change_message.relname,
            schema_name = %schema_change_message.nspname,
            event = %schema_change_message.command_tag,
            columns = schema_change_message.columns.len(),
            "received ddl schema change message"
        );

        // Build table schema from DDL message with start_lsn as the snapshot_id.
        let snapshot_id: SnapshotId = start_lsn.into();
        let table_schema = schema_change_message.into_table_schema(snapshot_id);

        // Store the new schema version in the store.
        if let Err(err) = self.schema_store.store_table_schema(table_schema).await {
            counter!(
                ETL_DDL_SCHEMA_CHANGES_TOTAL,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                COMMAND_TAG_LABEL => command_tag.clone(),
                OUTCOME_LABEL => "failed_store",
            )
            .increment(1);

            return Err(err);
        }
        // The next post-DDL DML will cause pgoutput to synthesize a fresh `RELATION`
        // message for this table. Record the new snapshot and clear any cached
        // mask now so relation handling rebuilds it from the schema version we
        // just stored rather than reusing pre-DDL state.
        self.shared_table_cache.note_waiting_for_relation(table_id, snapshot_id).await;

        let table_id_u32: u32 = table_id.into();
        counter!(
            ETL_DDL_SCHEMA_CHANGES_TOTAL,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
            COMMAND_TAG_LABEL => command_tag.clone(),
            OUTCOME_LABEL => "applied",
        )
        .increment(1);

        histogram!(
            ETL_DDL_SCHEMA_CHANGE_COLUMNS,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
            COMMAND_TAG_LABEL => command_tag.clone(),
        )
        .record(columns_count as f64);

        info!(
            table_id = table_id_u32,
            %snapshot_id,
            "stored new schema version from ddl message"
        );

        Ok(HandleMessageResult::no_event())
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
            histogram!(ETL_TRANSACTION_DURATION_SECONDS).record(duration_seconds);

            counter!(ETL_TRANSACTIONS_TOTAL).increment(1);

            histogram!(ETL_TRANSACTION_SIZE).record((self.state.current_tx_events - 1) as f64);

            self.state.current_tx_events = 0;
        }

        let end_lsn = PgLsn::from(message.end_lsn());

        // Process syncing tables after commit (worker-specific behavior).
        let should_end_batch = self.process_syncing_tables_after_commit_event(end_lsn).await?;

        let tx_ordinal = self.state.next_tx_ordinal();
        let event = parse_event_from_commit_message(start_lsn, commit_lsn, tx_ordinal, message);

        let mut result = HandleMessageResult {
            event: Some(Event::Commit(event)),
            end_lsn: Some(end_lsn),
            ..Default::default()
        };

        // Any requested exit forces the current commit batch to end, including the
        // commit event itself. For shutdown, this is mainly the catch-up wait
        // path requesting a pause exit, which lets that case reuse the normal
        // commit flush flow.
        if should_end_batch {
            result.end_batch = true;
        }

        Ok(result)
    }

    /// Handles Postgres RELATION messages.
    ///
    /// Builds a replication mask from the relation message and stores it for
    /// use by DML handlers.
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

        // Exactly one worker owns protocol interpretation for a table at a time.
        // Non-owning workers skip `RELATION` handling and rely on the owner to
        // refresh shared table state.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        // We extract the columns from the message that are needed to build the masks.
        // The masks themselves are built by name, so relation-message order is
        // not needed to decide membership: PostgreSQL live column names are
        // unique within a table schema version. The order matters after the
        // masks are applied: PostgreSQL writes RELATION columns and tuple data
        // in the same physical `pg_attribute.attnum` order, skipping
        // unpublished columns. Because stored TableSchema values use that same
        // order, ReplicatedTableSchema becomes the positional view used to
        // decode later tuple payloads.
        let replicated_columns = parse_replicated_column_names(message)?;
        let identity_columns = parse_replica_identity_column_names(message)?;

        info!(
            table_id = %table_id,
            replicated_columns = ?replicated_columns,
            identity_columns = ?identity_columns,
            "received relation message, building replication mask"
        );

        // Build the replication mask by validating that all replicated columns exist in
        // the schema.
        let shared_table_state = self.shared_table_cache.get(&table_id).await;
        let used_bootstrap_snapshot = shared_table_state.is_none();
        let table_snapshot_id = shared_table_state
            .map_or_else(|| self.state.bootstrap_snapshot_id(), |state| state.snapshot_id());
        let table_schema = get_table_schema(
            &self.schema_store,
            &table_id,
            table_snapshot_id,
            used_bootstrap_snapshot,
        )
        .await?;
        let replication_mask = ReplicationMask::try_build(&table_schema, &replicated_columns)?;
        let identity_mask = IdentityMask::try_build(&table_schema, &identity_columns)?;

        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);

        self.shared_table_cache.note_ready(table_id, replicated_table_schema.clone()).await;

        let relation_event = RelationEvent {
            start_lsn,
            commit_lsn: remote_final_lsn,
            tx_ordinal,
            replicated_table_schema,
        };

        Ok(HandleMessageResult::return_event(Event::Relation(relation_event)))
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

        let table_id = TableId::new(message.rel_id());
        let tx_ordinal = self.state.next_tx_ordinal();

        // Exactly one worker owns protocol interpretation for a table at a time, so
        // non-owning workers skip row decoding and leave the shared table state
        // untouched.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            get_replicated_table_schema(&table_id, &self.shared_table_cache).await?;

        let event = parse_event_from_insert_message(
            replicated_table_schema,
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
        )?;

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

        let table_id = TableId::new(message.rel_id());
        let tx_ordinal = self.state.next_tx_ordinal();

        // Exactly one worker owns protocol interpretation for a table at a time, so
        // non-owning workers skip row decoding and leave the shared table state
        // untouched.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            get_replicated_table_schema(&table_id, &self.shared_table_cache).await?;

        let event = parse_event_from_update_message(
            replicated_table_schema,
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
        )?;

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

        let table_id = TableId::new(message.rel_id());
        let tx_ordinal = self.state.next_tx_ordinal();

        // Exactly one worker owns protocol interpretation for a table at a time, so
        // non-owning workers skip row decoding and leave the shared table state
        // untouched.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            get_replicated_table_schema(&table_id, &self.shared_table_cache).await?;

        let event = parse_event_from_delete_message(
            replicated_table_schema,
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
        )?;

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

        // Collect the replicated schemas for tables this worker currently owns.
        let mut truncated_tables = Vec::with_capacity(message.rel_ids().len());
        for &rel_id in message.rel_ids() {
            let table_id = TableId::new(rel_id);

            // Exactly one worker owns protocol interpretation for a table at a time, so
            // non-owning workers skip truncation handling for that table as well.
            if self.should_apply_changes(table_id, remote_final_lsn).await? {
                let replicated_table_schema =
                    get_replicated_table_schema(&table_id, &self.shared_table_cache).await?;
                truncated_tables.push(replicated_table_schema);
            }
        }

        // If nothing to apply, skip conversion entirely.
        if truncated_tables.is_empty() {
            return Ok(HandleMessageResult::no_event());
        }

        let event = parse_event_from_truncate_message(
            start_lsn,
            remote_final_lsn,
            tx_ordinal,
            message,
            truncated_tables,
        );

        Ok(HandleMessageResult::return_event(Event::Truncate(event)))
    }

    /// Determines whether this worker currently owns protocol interpretation
    /// for a table.
    ///
    /// Exactly one worker owns DDL, `RELATION`, and DML handling for a table at
    /// a time. When this returns `false`, the caller must skip the message
    /// and leave the shared per-table protocol state untouched so the
    /// owning worker remains the single writer for that table.
    async fn should_apply_changes(
        &self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<bool> {
        Self::should_apply_changes_for_context(&self.worker_context, table_id, remote_final_lsn)
            .await
    }

    /// Determines whether a worker context owns changes for a table.
    async fn should_apply_changes_for_context(
        worker_context: &WorkerContext<S, D>,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<bool> {
        match worker_context {
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
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
    async fn process_syncing_tables_after_commit_event(&mut self, lsn: PgLsn) -> EtlResult<bool> {
        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_after_commit_event(ctx, lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_after_commit_event(ctx, lsn).await
            }
        }?;

        let should_end_batch = exit_intent.is_some();
        self.state.record_exit_intent(exit_intent);

        Ok(should_end_batch)
    }

    /// Processes syncing tables after a batch has been flushed.
    ///
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
    async fn process_syncing_tables_after_flush(
        &mut self,
        last_commit_end_lsn: PgLsn,
    ) -> EtlResult<()> {
        // Update replication progress to notify PostgreSQL of durable flush. Only
        // reports progress up to the last completed transaction, which may
        // cause duplicates on restart for partial transactions. Destinations
        // must handle at-least-once delivery semantics.
        self.state.replication_progress.update_last_flush_lsn(last_commit_end_lsn);

        let current_lsn = self.state.replication_progress.last_flush_lsn;
        info!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables after batch flush"
        );

        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_after_flush(ctx, current_lsn).await?;

                None
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_after_flush(ctx, current_lsn).await?
            }
        };

        self.state.record_exit_intent(exit_intent);

        Ok(())
    }

    /// Processes syncing tables outside a transaction.
    ///
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
    ///
    /// Once an exit has already been requested we intentionally skip this class
    /// of work so draining stays focused on already-started flushes and
    /// shutdown barriers.
    async fn maybe_process_syncing_tables_when_idle(&mut self) -> EtlResult<()> {
        if self.state.exit_intent.is_some() {
            return Ok(());
        }

        self.process_syncing_tables_when_idle().await
    }

    /// Processes syncing tables when the apply loop is idle.
    ///
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
    async fn process_syncing_tables_when_idle(&mut self) -> EtlResult<()> {
        if !self.state.is_idle() {
            debug!("skipping table sync processing because apply loop is not idle");

            return Ok(());
        }

        // Use effective flush LSN to report last received LSN when idle.
        let current_lsn = self.state.effective_flush_lsn();

        debug!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables outside transaction"
        );

        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_when_idle(ctx, current_lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_when_idle(ctx, current_lsn).await
            }
        }?;

        self.state.record_exit_intent(exit_intent);

        Ok(())
    }

    /// Marks a table as errored.
    ///
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
    async fn mark_table_errored(
        &mut self,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<()> {
        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::mark_table_errored(ctx, table_replication_error).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::mark_table_errored(ctx, table_replication_error).await
            }
        }?;

        self.state.record_exit_intent(exit_intent);

        Ok(())
    }
}

/// Returns tables that are still synchronizing.
async fn get_syncing_tables<S>(store: &S) -> EtlResult<Vec<(TableId, TableReplicationPhase)>>
where
    S: StateStore,
{
    let states = store.get_table_replication_states().await?;
    Ok(states
        .iter()
        .filter(|(_, state)| !state.as_type().is_done())
        .map(|(id, state)| (*id, state.clone()))
        .collect())
}

/// Functions specific to the apply worker.
mod apply_worker {
    use super::*;

    /// Determines whether changes should be applied for a given table.
    ///
    /// If an active worker exists for the table, its state is checked while
    /// holding the lock. Otherwise, the replication phase is read from the
    /// store.
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
            return Ok(is_phase_ready_for_changes(inner.replication_phase(), remote_final_lsn));
        }

        // If we didn't find an active worker, we need to read the replication phase
        // from the store. This could happen if the event is from a table that
        // has to be synced, or it was synced.
        let Some(phase) = ctx.store.get_table_replication_state(table_id).await? else {
            return Ok(false);
        };

        Ok(is_phase_ready_for_changes(phase, remote_final_lsn))
    }

    /// Processes syncing tables after commit.
    ///
    /// Spawns new table sync workers and triggers catchup when encountering
    /// SyncWait. Does NOT perform SyncDone → Ready transitions.
    pub(super) async fn process_syncing_tables_after_commit_event<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        for (table_id, table_replication_phase) in get_syncing_tables(&ctx.store).await? {
            let exit_intent = process_single_syncing_table_after_commit(
                ctx,
                table_id,
                table_replication_phase,
                current_lsn,
            )
            .await?;

            if exit_intent.is_some() {
                return Ok(exit_intent);
            }
        }

        Ok(None)
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
    ) -> EtlResult<Option<ExitIntent>>
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
                    // The catchup lsn is determined via max since it could be that the table sync
                    // worker is started from a lsn which is far in the future
                    // compared to where the apply worker is.
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

                                return Ok(None);
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

                            return Ok(Some(ExitIntent::Pause));
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
                        start_table_sync_worker(Arc::clone(&ctx.pool), table_sync_worker).await
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

        Ok(None)
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

        // If there is an active worker, we want to see if we can switch it to the ready
        // state. If there isn't an active worker, we just try to see if we can
        // switch the table to ready state or start a new worker for that table.
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
                        start_table_sync_worker(Arc::clone(&ctx.pool), table_sync_worker).await
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
    /// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, and
    /// spawns workers. Only called when outside a transaction and the batch
    /// is empty.
    pub(super) async fn process_syncing_tables_when_idle<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        for (table_id, table_replication_phase) in get_syncing_tables(&ctx.store).await? {
            let exit_intent = process_single_syncing_table_when_idle(
                ctx,
                table_id,
                table_replication_phase,
                current_lsn,
            )
            .await?;

            if exit_intent.is_some() {
                return Ok(exit_intent);
            }
        }

        Ok(None)
    }

    /// Processes a single syncing table outside transaction.
    ///
    /// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, and
    /// spawns workers. Only called when outside a transaction and the batch
    /// is empty.
    async fn process_single_syncing_table_when_idle<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        table_id: TableId,
        table_replication_phase: TableReplicationPhase,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
        D: Destination + Clone + Send + Sync + 'static,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        // If there is an active worker, we want to see if we can start the catchup or
        // if we can switch it to ready state.
        // If there isn't an active worker, we just try to see if we can switch the
        // table to ready state or start a new worker for that table.
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
                    // The catchup lsn is determined via max since it could be that the table sync
                    // worker is started from a lsn which is far in the future
                    // compared to where the apply worker is.
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

                                return Ok(None);
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

                            return Ok(Some(ExitIntent::Pause));
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
                        start_table_sync_worker(Arc::clone(&ctx.pool), table_sync_worker).await
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

        Ok(None)
    }

    /// Marks a table as errored.
    ///
    /// Updates the state store and continues the loop.
    pub(super) async fn mark_table_errored<S, D>(
        ctx: &ApplyWorkerContext<S, D>,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        let table_id = table_replication_error.table_id();
        let state = ctx.pool.get_active_worker_state(table_id).await.ok_or_else(|| {
            etl_error!(
                ErrorKind::InvalidState,
                "table sync worker state missing while marking table errored",
                format!("table_id={table_id}")
            )
        })?;
        let mut state_guard = state.lock().await;
        state_guard.set_and_store(table_replication_error.into(), &ctx.store).await?;

        Ok(None)
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
            Arc::clone(&ctx.config),
            Arc::clone(&ctx.pool),
            table_id,
            ctx.store.clone(),
            ctx.destination.clone(),
            ctx.shared_table_cache.clone(),
            ctx.shutdown_rx.clone(),
            Arc::clone(&ctx.table_sync_worker_permits),
            ctx.memory_monitor.clone(),
            ctx.batch_budget.clone(),
        )
    }

    /// Starts a table sync worker and adds it to the pool.
    ///
    /// We optimistically start the worker without checking if another one
    /// already exists since it's highly likely that if we didn't find the
    /// worker state during process syncing table, then the worker doesn't
    /// exist. If it were to exist, the pool itself performs de-duplication in a
    /// consistent way.
    ///
    /// This helper function uses type erasure via [`Box::pin`] to enforce
    /// `Send` bounds on the future. Without this, the compiler cannot
    /// verify that the recursive async call chain (ApplyLoop ->
    /// TableSyncWorker -> ApplyLoop for catchup) satisfies `Send`.
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
    /// For table sync workers, changes are only applied if the table matches
    /// the worker's assigned table.
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
    ) -> EtlResult<Option<ExitIntent>> {
        let worker_type = WorkerType::TableSync { table_id: ctx.table_id };

        // Check if catchup position reached, if so, signal end batch but don't update
        // the state yet.
        let inner = ctx.table_sync_worker_state.lock().await;
        if let TableReplicationPhase::Catchup { lsn: catchup_lsn } = inner.replication_phase() {
            if current_lsn >= catchup_lsn {
                info!(
                    %worker_type,
                    %catchup_lsn,
                    %current_lsn,
                    "catchup target lsn reached after commit, requesting early batch flush before transitioning to sync_done",
                );

                return Ok(Some(ExitIntent::Complete));
            }

            debug!(
                %worker_type,
                %catchup_lsn,
                %current_lsn,
                remaining_lsn = %(u64::from(catchup_lsn) - u64::from(current_lsn)),
                "catchup in progress, target lsn not yet reached",
            );
        }

        Ok(None)
    }

    /// Processes syncing tables after batch flush.
    ///
    /// Validates whether catchup position has been reached.
    /// If so, transitions to SyncDone and returns Complete.
    pub(super) async fn process_syncing_tables_after_flush<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
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
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        try_complete_catchup(ctx, current_lsn).await
    }

    /// Attempts to complete catchup and transition to SyncDone.
    ///
    /// If catchup position has been reached, transitions to SyncDone and
    /// returns Complete.
    async fn try_complete_catchup<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        let worker_type = WorkerType::TableSync { table_id: ctx.table_id };
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

                return Ok(Some(ExitIntent::Complete));
            }

            debug!(
                %worker_type,
                %catchup_lsn,
                %current_lsn,
                remaining_lsn = %(u64::from(catchup_lsn) - u64::from(current_lsn)),
                "catchup in progress, target lsn not yet reached",
            );
        }

        Ok(None)
    }
    /// Marks a table as errored.
    ///
    /// Updates the state and returns Complete if the table matches this worker.
    pub(super) async fn mark_table_errored<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        table_replication_error: TableReplicationError,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: StateStore + Clone + Send + Sync + 'static,
    {
        if ctx.table_id != table_replication_error.table_id() {
            return Ok(None);
        }

        let mut inner = ctx.table_sync_worker_state.lock().await;
        inner.set_and_store(table_replication_error.into(), &ctx.state_store).await?;

        Ok(Some(ExitIntent::Complete))
    }
}

/// Retrieves a table schema from the schema store by table ID and snapshot.
///
/// When `used_bootstrap_snapshot` is `false`, the returned schema must match
/// the requested snapshot exactly. When it is `true`, the lookup is allowed to
/// resolve to an older schema version because the first `RELATION` message may
/// arrive before shared per-table protocol state has been established, but it
/// must never resolve to a newer schema than requested.
async fn get_table_schema<S>(
    schema_store: &S,
    table_id: &TableId,
    snapshot_id: SnapshotId,
    used_bootstrap_snapshot: bool,
) -> EtlResult<Arc<TableSchema>>
where
    S: SchemaStore,
{
    let table_schema =
        schema_store.get_table_schema(table_id, snapshot_id).await?.ok_or_else(|| {
            etl_error!(
                ErrorKind::MissingTableSchema,
                "Table schema not found",
                format!(
                    "Table schema for table {} at snapshot {} not found",
                    table_id, snapshot_id
                )
            )
        })?;

    if used_bootstrap_snapshot {
        if table_schema.snapshot_id > snapshot_id {
            bail!(
                ErrorKind::InvalidState,
                "Bootstrap table schema snapshot exceeded requested snapshot",
                format!(
                    "Bootstrap schema lookup for table {} resolved to snapshot {} which is newer \
                     than requested snapshot {}",
                    table_id, table_schema.snapshot_id, snapshot_id
                )
            );
        }

        if table_schema.snapshot_id != snapshot_id {
            info!(
                table_id = %table_id,
                requested_snapshot_id = %snapshot_id,
                resolved_snapshot_id = %table_schema.snapshot_id,
                "schema lookup returned an older schema because the first relation message used the bootstrap snapshot"
            );
        }
    } else if table_schema.snapshot_id != snapshot_id {
        bail!(
            ErrorKind::InvalidState,
            "Table schema snapshot mismatch",
            format!(
                "Table schema for table {} resolved to snapshot {} when snapshot {} was required",
                table_id, table_schema.snapshot_id, snapshot_id
            )
        );
    }

    Ok(table_schema)
}

/// Retrieves a [`ReplicatedTableSchema`] for the given table from the shared
/// table state.
///
/// Relation handling and table copy both materialize the same runtime schema
/// shape into the shared cache, so row-event decoding can read that exact
/// schema directly without reconstructing masks on demand.
async fn get_replicated_table_schema(
    table_id: &TableId,
    shared_table_cache: &SharedTableCache,
) -> EtlResult<ReplicatedTableSchema> {
    let Some(shared_table_state) = shared_table_cache.get(table_id).await else {
        bail!(
            ErrorKind::InvalidState,
            "Missing shared table state",
            format!(
                "No shared replicated table schema found for table {}, this event can't be \
                 processed",
                table_id
            )
        );
    };

    let Some(replicated_table_schema) = shared_table_state.replicated_table_schema().cloned()
    else {
        bail!(
            ErrorKind::InvalidState,
            "Waiting for relation state cannot decode row event",
            format!(
                "Table {} is waiting for a relation refresh before row events can be decoded",
                table_id
            )
        );
    };

    Ok(replicated_table_schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply_loop_state() -> ApplyLoopState {
        let start_lsn = PgLsn::from(100u64);
        let replication_progress =
            ReplicationProgress { last_received_lsn: start_lsn, last_flush_lsn: start_lsn };

        ApplyLoopState::new(
            replication_progress,
            Duration::from_secs(1),
            SnapshotId::from(start_lsn),
            "test_slot".to_string(),
        )
    }

    #[tokio::test]
    async fn schema_cleanup_uses_deadline_between_runs() {
        let state = apply_loop_state();

        state.expire_schema_cleanup_deadline().await;

        let schema_cleanup_run = state.try_start_schema_cleanup().await.unwrap();

        assert!(state.try_start_schema_cleanup().await.is_none());

        schema_cleanup_run.finish().await;

        assert!(state.try_start_schema_cleanup().await.is_none());
    }
}
