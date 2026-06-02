//! Apply loop implementation for processing PostgreSQL logical replication
//! events.
//!
//! This module provides the core apply loop that processes replication events
//! from PostgreSQL and coordinates table synchronization. It uses a
//! [`WorkerContext`] enum to enable different behavior based on the worker type
//! (apply worker vs table sync worker) at various points in the replication
//! cycle.

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::array::UInt64Array;
use etl_config::shared::PipelineConfig;
use etl_postgres::types::{
    IdentityMask, ReplicatedTableSchema, ReplicationMask, SchemaError, SnapshotId, TableId,
    TableSchema,
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
    FORCE_SCHEMA_CLEANUP_FP, STORE_REPLICATION_PROGRESS_FP, etl_fail_point_active,
};
use crate::{
    bail,
    concurrency::{
        BackpressureStream, BatchBudgetController, CachedBatchBudget, MemoryMonitor,
        ShutdownResult, ShutdownRx, apply_worker_apply_stream_id,
        table_sync_worker_apply_stream_id,
    },
    conversions::{
        arrow::{PgOutputArrowAppendResult, TableArrowBatchBuilder, table_rows_to_arrow_batch},
        event::{
            DDL_MESSAGE_PREFIX, SchemaChangeMessage, calculate_tuple_bytes,
            parse_event_from_begin_message, parse_event_from_commit_message,
            parse_event_from_delete_message, parse_event_from_insert_message,
            parse_event_from_truncate_message, parse_event_from_update_message,
            parse_replica_identity_column_names, parse_replicated_column_names,
            tuple_bytes_to_size_hint,
        },
    },
    destination::{
        PipelineDestination,
        async_result::{
            ApplyLoopAsyncResultMetadata, CompletedWriteStreamBatchesResult, DispatchMetrics,
            PendingWriteStreamBatchesResult, WriteStreamBatchesResult,
        },
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    metrics::{
        ACTION_LABEL, COMMAND_TAG_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
        ETL_BYTES_PROCESSED_TOTAL, ETL_DDL_SCHEMA_CHANGE_COLUMNS, ETL_DDL_SCHEMA_CHANGES_TOTAL,
        ETL_EVENTS_PROCESSED_TOTAL, ETL_REPLICATION_MESSAGES_TOTAL, ETL_ROW_SIZE_BYTES,
        ETL_SCHEMA_CLEANUP_ERRORS_TOTAL, ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL,
        ETL_SCHEMA_CLEANUP_TABLES_TOTAL, ETL_SCHEMA_CLEANUPS_TOTAL,
        ETL_TRANSACTION_DURATION_SECONDS, ETL_TRANSACTION_SIZE, ETL_TRANSACTIONS_TOTAL,
        EVENT_TYPE_LABEL, OUTCOME_LABEL, WORKER_TYPE_LABEL,
    },
    replication::{
        SharedTableCache, WorkerType,
        client::{PgReplicationClient, PostgresConnectionUpdate},
        stream::{EventsStream, StatusUpdateResult, StatusUpdateType},
    },
    state::{TableError, TableState, TableStateType},
    store::{PipelineStore, SharedStateStore, schema::TableSchemaRetention, state::StateStore},
    types::{
        ChangeArrowBatch, ChangeKind, Event, OldTableRow, PipelineId, RelationEvent, RowImage,
        SizeHint, StreamBatch, TableChangeSet, TableRow, TruncateBatch, UpdatedTableRow,
    },
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
/// Cleanup is considered only after status updates, because those are natural
/// progress points where durable ETL progress may have advanced. The next
/// deadline is scheduled when the previous cleanup task finishes.
const SCHEMA_CLEANUP_INTERVAL: Duration = Duration::from_hours(1);

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
#[derive(Debug, Clone)]
pub(crate) enum ShutdownState {
    /// Normal operation.
    NoShutdown,
    /// Shutdown requested.
    ///
    /// No new WAL is accepted, but buffered or in-flight destination work is
    /// still allowed to drain.
    DrainingForShutdown,
}

impl ShutdownState {
    /// Returns `true` if a shutdown has been requested.
    pub(crate) fn is_requested(&self) -> bool {
        !matches!(self, Self::NoShutdown)
    }
}

impl Display for ShutdownState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoShutdown => write!(f, "no_shutdown"),
            Self::DrainingForShutdown => write!(f, "draining_for_shutdown"),
        }
    }
}

/// Resources for the apply worker during the apply loop.
///
/// Contains all state and dependencies needed by the apply worker to coordinate
/// with table sync workers and manage table lifecycle transitions.
#[derive(Debug)]
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
#[derive(Debug)]
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
#[derive(Debug)]
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

/// An enum representing if the batch should be ended or not.
#[derive(Debug)]
enum EndBatch {
    /// The batch should include the last processed event and end.
    Inclusive,
}

/// A buffered item waiting to be normalized for the destination.
enum PendingBatchItem {
    /// A decoded logical replication event.
    Event(Event),
    /// Pgoutput change rows that were appended directly to Arrow.
    PgOutputChange(PendingPgOutputChange),
}

impl PendingBatchItem {
    /// Returns the approximate size of this pending item.
    fn size_hint(&self) -> usize {
        match self {
            Self::Event(event) => event.size_hint(),
            Self::PgOutputChange(change) => change.size_hint,
        }
    }

    /// Returns the number of logical items represented by this pending item.
    fn item_count(&self) -> usize {
        match self {
            Self::Event(_) => 1,
            Self::PgOutputChange(change) => change.event_count,
        }
    }
}

/// A pgoutput change event or coalesced sequence of compatible events.
struct PendingPgOutputChange {
    groups: Vec<PendingPgOutputRows>,
    event_count: usize,
    size_hint: usize,
}

impl PendingPgOutputChange {
    /// Creates a direct change with a single row group.
    fn new_single_group(group: PendingPgOutputRows) -> Self {
        Self { event_count: group.row_count, size_hint: group.size_hint, groups: vec![group] }
    }

    /// Creates a direct change with multiple row-image groups for one event.
    fn new_multi_group(groups: Vec<PendingPgOutputRows>) -> Self {
        let size_hint = groups.iter().map(|group| group.size_hint).sum();

        Self { groups, event_count: 1, size_hint }
    }

    /// Returns whether this change can append to its only row group.
    fn can_append_single_group(
        &self,
        table_id: TableId,
        change: ChangeKind,
        row_image: RowImage,
        table_schema: &TableSchema,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> bool {
        let [group] = self.groups.as_slice() else {
            return false;
        };

        group.can_append(table_id, change, row_image, table_schema, replicated_table_schema)
    }

    /// Appends one tuple to this change's single row group.
    fn append_single_tuple(
        &mut self,
        tuple_data: &[protocol::TupleData],
        meta: PendingPgOutputRowMeta,
    ) -> EtlResult<()> {
        let [group] = self.groups.as_mut_slice() else {
            bail!(
                ErrorKind::ConversionError,
                "pgoutput tuple cannot be appended to a multi-group direct change"
            );
        };

        group.append_tuple(tuple_data, meta)?;
        self.event_count += 1;
        self.size_hint = self.size_hint.saturating_add(meta.size_hint);

        Ok(())
    }
}

/// Destination group targeted by a direct pgoutput row append.
struct PendingPgOutputRowsTarget {
    table_id: TableId,
    change: ChangeKind,
    row_image: RowImage,
    table_schema: Arc<TableSchema>,
    replicated_table_schema: ReplicatedTableSchema,
}

impl PendingPgOutputRowsTarget {
    /// Creates a direct row group target.
    fn new(
        table_id: TableId,
        change: ChangeKind,
        row_image: RowImage,
        table_schema: Arc<TableSchema>,
        replicated_table_schema: ReplicatedTableSchema,
    ) -> Self {
        Self { table_id, change, row_image, table_schema, replicated_table_schema }
    }
}

/// Replication metadata attached to a direct pgoutput row.
#[derive(Clone, Copy)]
struct PendingPgOutputRowMeta {
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    size_hint: usize,
}

impl PendingPgOutputRowMeta {
    /// Creates row metadata for a direct pgoutput row.
    fn new(commit_lsn: PgLsn, tx_ordinal: u64, size_hint: usize) -> Self {
        Self { commit_lsn, tx_ordinal, size_hint }
    }
}

/// Pgoutput tuples appended directly to one Arrow change group.
struct PendingPgOutputRows {
    table_id: TableId,
    change: ChangeKind,
    row_image: RowImage,
    table_schema: Arc<TableSchema>,
    replicated_table_schema: ReplicatedTableSchema,
    rows_builder: TableArrowBatchBuilder,
    commit_lsns: Vec<u64>,
    tx_ordinals: Vec<u64>,
    row_count: usize,
    size_hint: usize,
}

impl PendingPgOutputRows {
    /// Creates rows from the first direct pgoutput tuple.
    fn try_new(
        target: PendingPgOutputRowsTarget,
        tuple_data: &[protocol::TupleData],
        meta: PendingPgOutputRowMeta,
    ) -> EtlResult<Self> {
        let rows_builder = TableArrowBatchBuilder::try_new(Arc::clone(&target.table_schema), 1)
            .expect("Arrow batch builder supports all PostgreSQL table columns");

        let mut rows = Self {
            table_id: target.table_id,
            change: target.change,
            row_image: target.row_image,
            table_schema: target.table_schema,
            replicated_table_schema: target.replicated_table_schema,
            rows_builder,
            commit_lsns: Vec::new(),
            tx_ordinals: Vec::new(),
            row_count: 0,
            size_hint: 0,
        };
        rows.append_tuple(tuple_data, meta)?;

        Ok(rows)
    }

    /// Creates rows from selected fields in the first direct pgoutput tuple.
    fn try_new_projected(
        target: PendingPgOutputRowsTarget,
        tuple_data: &[protocol::TupleData],
        projection: &[usize],
        meta: PendingPgOutputRowMeta,
    ) -> EtlResult<Self> {
        let rows_builder = TableArrowBatchBuilder::try_new(Arc::clone(&target.table_schema), 1)
            .expect("Arrow batch builder supports all PostgreSQL table columns");

        let mut rows = Self {
            table_id: target.table_id,
            change: target.change,
            row_image: target.row_image,
            table_schema: target.table_schema,
            replicated_table_schema: target.replicated_table_schema,
            rows_builder,
            commit_lsns: Vec::new(),
            tx_ordinals: Vec::new(),
            row_count: 0,
            size_hint: 0,
        };
        rows.append_projected_tuple(tuple_data, projection, meta)?;

        Ok(rows)
    }

    /// Returns whether this row group can accept another tuple.
    fn can_append(
        &self,
        table_id: TableId,
        change: ChangeKind,
        row_image: RowImage,
        table_schema: &TableSchema,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> bool {
        self.table_id == table_id
            && self.change == change
            && self.row_image == row_image
            && self.table_schema.snapshot_id == table_schema.snapshot_id
            && self.table_schema.column_schemas.iter().eq(table_schema.column_schemas.iter())
            && replicated_table_schemas_are_compatible(
                &self.replicated_table_schema,
                replicated_table_schema,
            )
    }

    /// Appends another borrowed pgoutput tuple directly into the Arrow builder.
    fn append_tuple(
        &mut self,
        tuple_data: &[protocol::TupleData],
        meta: PendingPgOutputRowMeta,
    ) -> EtlResult<()> {
        match self.rows_builder.append_pgoutput_tuple(tuple_data)? {
            PgOutputArrowAppendResult::Appended => {}
            PgOutputArrowAppendResult::Unsupported => {
                bail!(
                    ErrorKind::ConversionError,
                    "pgoutput tuple cannot be appended directly to Arrow"
                );
            }
        }

        self.commit_lsns.push(u64::from(meta.commit_lsn));
        self.tx_ordinals.push(meta.tx_ordinal);
        self.row_count += 1;
        self.size_hint = self.size_hint.saturating_add(meta.size_hint);

        Ok(())
    }

    /// Appends selected fields from another borrowed pgoutput tuple.
    fn append_projected_tuple(
        &mut self,
        tuple_data: &[protocol::TupleData],
        projection: &[usize],
        meta: PendingPgOutputRowMeta,
    ) -> EtlResult<()> {
        match self.rows_builder.append_projected_pgoutput_tuple(tuple_data, projection)? {
            PgOutputArrowAppendResult::Appended => {}
            PgOutputArrowAppendResult::Unsupported => {
                bail!(
                    ErrorKind::ConversionError,
                    "pgoutput tuple cannot be appended directly to Arrow"
                );
            }
        }

        self.commit_lsns.push(u64::from(meta.commit_lsn));
        self.tx_ordinals.push(meta.tx_ordinal);
        self.row_count += 1;
        self.size_hint = self.size_hint.saturating_add(meta.size_hint);

        Ok(())
    }
}

/// Result returned from [`ApplyLoop::handle_replication_message`] and related
/// functions.
#[derive(Default)]
struct HandleMessageResult {
    /// The item converted from the replication message.
    batch_item: Option<PendingBatchItem>,
    /// Set to a commit message's end_lsn value, [`None`] otherwise.
    end_lsn: Option<PgLsn>,
    /// Set when a batch should be ended earlier than the normal batching
    /// parameters.
    end_batch: Option<EndBatch>,
    /// Set when the table has encountered an error.
    table_replication_error: Option<(TableId, TableError)>,
    /// Set when a handler already added directly to the pending batch.
    batch_updated: bool,
}

impl HandleMessageResult {
    /// Creates a result with no event and no side effects.
    fn no_event() -> Self {
        Self::default()
    }

    /// Creates a result that returns an event without affecting batch state.
    fn return_event(event: Event) -> Self {
        Self::return_batch_item(PendingBatchItem::Event(event))
    }

    /// Creates a result that returns a pending batch item.
    fn return_batch_item(batch_item: PendingBatchItem) -> Self {
        Self { batch_item: Some(batch_item), ..Default::default() }
    }

    /// Creates a result for handlers that already updated the pending batch.
    fn batch_updated() -> Self {
        Self { batch_updated: true, ..Default::default() }
    }
}

/// Running schema cleanup marker.
///
/// Finishing the marker schedules the next cleanup deadline. This keeps the
/// deadline mutex private while allowing a cleanup run to reset it after it
/// either finishes or decides not to spawn background work.
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

/// Mutable runtime state that evolves throughout the apply loop.
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
    /// A batch of items to send to the destination.
    pending_batch: Vec<PendingBatchItem>,
    /// Approximate total size in bytes of items currently in the batch.
    pending_batch_bytes: usize,
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
    pending_flush_result: Option<PendingWriteStreamBatchesResult<()>>,
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
    /// Shared schema cleanup deadline.
    ///
    /// `None` means a cleanup run has claimed the deadline. The run resets this
    /// after it either finishes or decides not to spawn background work.
    schema_cleanup_deadline: Arc<Mutex<Option<Instant>>>,
    /// Background schema cleanup task owned by this apply loop.
    schema_cleanup_task: Option<JoinHandle<()>>,
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial replication progress.
    fn new(
        replication_progress: ReplicationProgress,
        keep_alive_deadline_duration: Duration,
    ) -> Self {
        Self {
            last_commit_end_lsn: None,
            remote_final_lsn: None,
            replication_progress,
            pending_batch: Vec::new(),
            pending_batch_bytes: 0,
            current_tx_begin_ts: None,
            current_tx_events: 0,
            next_tx_ordinal: 0,
            shutdown_state: ShutdownState::NoShutdown,
            flush_deadline: None,
            keep_alive_deadline: Instant::now() + keep_alive_deadline_duration,
            pending_flush_result: None,
            exit_intent: None,
            processing_paused: false,
            schema_cleanup_deadline: Arc::new(Mutex::new(Some(
                Instant::now() + SCHEMA_CLEANUP_INTERVAL,
            ))),
            schema_cleanup_task: None,
        }
    }

    /// Adds an item to the batch.
    fn add_item_to_batch(&mut self, item: PendingBatchItem) {
        // We track the current number of bytes in the batch.
        self.pending_batch_bytes = self.pending_batch_bytes.saturating_add(item.size_hint());

        // We add the element to the pending batch.
        self.pending_batch.push(item);
    }

    /// Appends a borrowed pgoutput tuple directly into the pending Arrow batch.
    fn add_pgoutput_tuple_to_batch(
        &mut self,
        target: PendingPgOutputRowsTarget,
        tuple_data: &[protocol::TupleData],
        meta: PendingPgOutputRowMeta,
    ) -> EtlResult<()> {
        if let Some(PendingBatchItem::PgOutputChange(direct_change)) = self.pending_batch.last_mut()
            && direct_change.can_append_single_group(
                target.table_id,
                target.change,
                target.row_image,
                &target.table_schema,
                &target.replicated_table_schema,
            )
        {
            direct_change.append_single_tuple(tuple_data, meta)?;
            self.pending_batch_bytes = self.pending_batch_bytes.saturating_add(meta.size_hint);

            return Ok(());
        }

        let rows = PendingPgOutputRows::try_new(target, tuple_data, meta)?;
        let direct_change = PendingPgOutputChange::new_single_group(rows);
        self.pending_batch_bytes = self.pending_batch_bytes.saturating_add(meta.size_hint);
        self.pending_batch.push(PendingBatchItem::PgOutputChange(direct_change));

        Ok(())
    }

    /// Adds a direct pgoutput change containing prebuilt Arrow row groups.
    fn add_pgoutput_change_to_batch(&mut self, direct_change: PendingPgOutputChange) {
        self.pending_batch_bytes = self.pending_batch_bytes.saturating_add(direct_change.size_hint);
        self.pending_batch.push(PendingBatchItem::PgOutputChange(direct_change));
    }

    /// Takes the pending batch for further processing. Replacing it with a new
    /// empty batch.
    fn take_pending_batch(&mut self) -> (Vec<PendingBatchItem>, usize) {
        let pending_batch = std::mem::take(&mut self.pending_batch);
        let pending_batch_bytes = self.pending_batch_bytes;
        self.pending_batch_bytes = 0;

        (pending_batch, pending_batch_bytes)
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
    /// Note that when a new transaction starts, the last flush LSN will be used
    /// and it may appear to move backward relative to the last received LSN
    /// reported earlier. This is fine because the status update logic
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
        let deadline = (*schema_cleanup_deadline)?;

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

    /// Returns `true` if a schema cleanup task is still recorded.
    fn has_schema_cleanup_task(&self) -> bool {
        self.schema_cleanup_task.is_some()
    }

    /// Takes the schema cleanup task if it has finished.
    fn take_finished_schema_cleanup_task(&mut self) -> Option<JoinHandle<()>> {
        if self.schema_cleanup_task.as_ref().is_some_and(JoinHandle::is_finished) {
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
        !self.pending_batch.is_empty()
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

/// Storage for rows gathered into one destination change group.
enum PendingChangeRowsStorage {
    /// Decoded rows waiting for Arrow conversion.
    Rows(Vec<TableRow>),
    /// Rows being appended directly to Arrow.
    Arrow(TableArrowBatchBuilder),
}

/// Rows and metadata for one destination change group.
struct PendingChangeRows {
    change: ChangeKind,
    row_image: RowImage,
    table_schema: Arc<TableSchema>,
    replicated_table_schema: ReplicatedTableSchema,
    partial_update_missing_column_indexes: Option<Arc<[usize]>>,
    storage: PendingChangeRowsStorage,
    commit_lsns: Vec<u64>,
    tx_ordinals: Vec<u64>,
}

impl PendingChangeRows {
    /// Creates an empty pending change group.
    fn new(
        change: ChangeKind,
        row_image: RowImage,
        table_schema: Arc<TableSchema>,
        replicated_table_schema: ReplicatedTableSchema,
        partial_update_missing_column_indexes: Option<Arc<[usize]>>,
    ) -> Self {
        let storage = TableArrowBatchBuilder::try_new(Arc::clone(&table_schema), 0).map_or_else(
            || PendingChangeRowsStorage::Rows(Vec::new()),
            PendingChangeRowsStorage::Arrow,
        );

        Self {
            change,
            row_image,
            table_schema,
            replicated_table_schema,
            partial_update_missing_column_indexes,
            storage,
            commit_lsns: Vec::new(),
            tx_ordinals: Vec::new(),
        }
    }

    /// Returns whether this group can accept another row.
    fn can_append(
        &self,
        change: ChangeKind,
        row_image: RowImage,
        table_schema: &TableSchema,
        replicated_table_schema: &ReplicatedTableSchema,
        partial_update_missing_column_indexes: Option<&Arc<[usize]>>,
    ) -> bool {
        self.change == change
            && self.row_image == row_image
            && table_schemas_are_compatible(&self.table_schema, table_schema)
            && replicated_table_schemas_are_compatible(
                &self.replicated_table_schema,
                replicated_table_schema,
            )
            && self.partial_update_missing_column_indexes.as_ref()
                == partial_update_missing_column_indexes
    }

    /// Creates pending rows from prebuilt direct pgoutput Arrow rows.
    fn from_pgoutput_rows(rows: PendingPgOutputRows) -> Self {
        Self {
            change: rows.change,
            row_image: rows.row_image,
            table_schema: rows.table_schema,
            replicated_table_schema: rows.replicated_table_schema,
            partial_update_missing_column_indexes: None,
            storage: PendingChangeRowsStorage::Arrow(rows.rows_builder),
            commit_lsns: rows.commit_lsns,
            tx_ordinals: rows.tx_ordinals,
        }
    }

    /// Appends a decoded table row to this change group.
    fn push_row(
        &mut self,
        table_row: TableRow,
        commit_lsn: PgLsn,
        tx_ordinal: u64,
    ) -> EtlResult<()> {
        match &mut self.storage {
            PendingChangeRowsStorage::Rows(table_rows) => table_rows.push(table_row),
            PendingChangeRowsStorage::Arrow(builder) => builder.append_trusted_row(&table_row)?,
        }

        self.commit_lsns.push(u64::from(commit_lsn));
        self.tx_ordinals.push(tx_ordinal);

        Ok(())
    }

    /// Converts this group into a destination-facing Arrow change batch.
    fn into_change_arrow_batch(self) -> EtlResult<ChangeArrowBatch> {
        let rows = match self.storage {
            PendingChangeRowsStorage::Rows(table_rows) => {
                table_rows_to_arrow_batch(self.table_schema, table_rows.as_slice())?
            }
            PendingChangeRowsStorage::Arrow(builder) => builder.finish()?,
        }
        .with_replicated_table_schema(self.replicated_table_schema);

        Ok(ChangeArrowBatch {
            rows,
            change: self.change,
            row_image: self.row_image,
            partial_update_missing_column_indexes: self.partial_update_missing_column_indexes,
            commit_lsns: UInt64Array::from(self.commit_lsns),
            tx_ordinals: UInt64Array::from(self.tx_ordinals),
        })
    }
}

/// Pending change groups for one table.
#[derive(Default)]
struct PendingTableChangeSet {
    groups: Vec<PendingChangeRows>,
}

/// Builds a batch schema containing the replicated columns for a table.
fn replicated_arrow_table_schema(
    replicated_table_schema: &ReplicatedTableSchema,
) -> Arc<TableSchema> {
    Arc::new(TableSchema::with_snapshot_id(
        replicated_table_schema.id(),
        replicated_table_schema.name().clone(),
        replicated_table_schema.column_schemas().cloned().collect(),
        replicated_table_schema.inner().snapshot_id,
    ))
}

/// Builds a batch schema containing the replicated identity columns for a
/// table.
fn identity_arrow_table_schema(
    replicated_table_schema: &ReplicatedTableSchema,
) -> Arc<TableSchema> {
    Arc::new(TableSchema::with_snapshot_id(
        replicated_table_schema.id(),
        replicated_table_schema.name().clone(),
        replicated_table_schema.identity_column_schemas().cloned().collect(),
        replicated_table_schema.inner().snapshot_id,
    ))
}

/// Builds a batch schema containing present columns for a partial update row.
fn partial_arrow_table_schema(
    replicated_table_schema: &ReplicatedTableSchema,
    missing_column_indexes: &[usize],
) -> Arc<TableSchema> {
    Arc::new(TableSchema::with_snapshot_id(
        replicated_table_schema.id(),
        replicated_table_schema.name().clone(),
        replicated_table_schema
            .column_schemas()
            .enumerate()
            .filter(|(index, _)| !missing_column_indexes.contains(index))
            .map(|(_, column_schema)| column_schema.clone())
            .collect(),
        replicated_table_schema.inner().snapshot_id,
    ))
}

/// Returns whether two table schemas describe the same Arrow batch shape.
fn table_schemas_are_compatible(left: &TableSchema, right: &TableSchema) -> bool {
    left.snapshot_id == right.snapshot_id
        && left.column_schemas.iter().eq(right.column_schemas.iter())
}

/// Returns whether two replicated schemas describe the same source shape and
/// masks.
fn replicated_table_schemas_are_compatible(
    left: &ReplicatedTableSchema,
    right: &ReplicatedTableSchema,
) -> bool {
    left.inner().snapshot_id == right.inner().snapshot_id
        && left.inner().column_schemas.iter().eq(right.inner().column_schemas.iter())
        && left.replication_mask().as_slice() == right.replication_mask().as_slice()
        && left.identity_mask().as_slice() == right.identity_mask().as_slice()
}

/// Returns whether an Arrow table schema matches replicated table columns.
fn table_schema_matches_replicated(
    table_schema: &TableSchema,
    replicated_table_schema: &ReplicatedTableSchema,
) -> bool {
    table_schema.snapshot_id == replicated_table_schema.inner().snapshot_id
        && table_schema.column_schemas.iter().eq(replicated_table_schema.column_schemas())
}

/// Destination group targeted by a decoded change row.
#[derive(Clone)]
struct PendingChangeTarget {
    table_id: TableId,
    change: ChangeKind,
    row_image: RowImage,
    replicated_table_schema: ReplicatedTableSchema,
    partial_update_missing_column_indexes: Option<Arc<[usize]>>,
}

impl PendingChangeTarget {
    /// Creates a decoded change row target.
    fn new(
        table_id: TableId,
        change: ChangeKind,
        row_image: RowImage,
        replicated_table_schema: ReplicatedTableSchema,
        partial_update_missing_column_indexes: Option<Arc<[usize]>>,
    ) -> Self {
        Self {
            table_id,
            change,
            row_image,
            replicated_table_schema,
            partial_update_missing_column_indexes,
        }
    }
}

/// Replication metadata attached to a decoded change row.
#[derive(Clone, Copy)]
struct PendingChangeMeta {
    commit_lsn: PgLsn,
    tx_ordinal: u64,
}

impl PendingChangeMeta {
    /// Creates row metadata for a decoded change row.
    fn new(commit_lsn: PgLsn, tx_ordinal: u64) -> Self {
        Self { commit_lsn, tx_ordinal }
    }
}

fn push_pending_change(
    table_changes: &mut HashMap<TableId, PendingTableChangeSet>,
    table_order: &mut Vec<TableId>,
    target: PendingChangeTarget,
    table_row: TableRow,
    meta: PendingChangeMeta,
    table_schema: impl FnOnce() -> Arc<TableSchema>,
) -> EtlResult<()> {
    let table_schema = table_schema();
    let entry = table_changes.entry(target.table_id).or_insert_with(|| {
        table_order.push(target.table_id);
        PendingTableChangeSet::default()
    });

    let group = match entry.groups.last_mut() {
        Some(group)
            if group.can_append(
                target.change,
                target.row_image,
                &table_schema,
                &target.replicated_table_schema,
                target.partial_update_missing_column_indexes.as_ref(),
            ) =>
        {
            group
        }
        _ => {
            let group = PendingChangeRows::new(
                target.change,
                target.row_image,
                table_schema,
                target.replicated_table_schema,
                target.partial_update_missing_column_indexes,
            );
            entry.groups.push(group);
            entry.groups.last_mut().expect("pushed group must exist")
        }
    };

    group.push_row(table_row, meta.commit_lsn, meta.tx_ordinal)?;

    Ok(())
}

/// Pushes direct pgoutput rows into pending table changes.
fn push_pending_pgoutput_rows(
    table_changes: &mut HashMap<TableId, PendingTableChangeSet>,
    table_order: &mut Vec<TableId>,
    rows: PendingPgOutputRows,
) -> EtlResult<()> {
    let entry = table_changes.entry(rows.table_id).or_insert_with(|| {
        table_order.push(rows.table_id);
        PendingTableChangeSet::default()
    });

    entry.groups.push(PendingChangeRows::from_pgoutput_rows(rows));

    Ok(())
}

/// Pushes direct pgoutput change groups into pending table changes.
fn push_pending_pgoutput_change(
    table_changes: &mut HashMap<TableId, PendingTableChangeSet>,
    table_order: &mut Vec<TableId>,
    direct_change: PendingPgOutputChange,
) -> EtlResult<()> {
    for rows in direct_change.groups {
        push_pending_pgoutput_rows(table_changes, table_order, rows)?;
    }

    Ok(())
}

/// Records CDC tuple byte metrics for a direct pgoutput tuple.
fn record_cdc_tuple_metrics(event_type: &'static str, row_size_bytes: u64) {
    counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => event_type).increment(row_size_bytes);

    histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => event_type).record(row_size_bytes as f64);
}

/// Returns whether a tuple can be buffered for direct Arrow append.
fn can_append_pgoutput_tuple_directly(tuple_data: &[protocol::TupleData]) -> bool {
    !tuple_data.iter().any(|tuple_data| matches!(tuple_data, protocol::TupleData::UnchangedToast))
}

/// Builds a direct Arrow row group for a full-width tuple.
fn pending_pgoutput_full_row_group(
    replicated_table_schema: &ReplicatedTableSchema,
    change: ChangeKind,
    row_image: RowImage,
    tuple_data: &[protocol::TupleData],
    meta: PendingPgOutputRowMeta,
) -> EtlResult<PendingPgOutputRows> {
    let target = PendingPgOutputRowsTarget::new(
        replicated_table_schema.id(),
        change,
        row_image,
        replicated_arrow_table_schema(replicated_table_schema),
        replicated_table_schema.clone(),
    );
    PendingPgOutputRows::try_new(target, tuple_data, meta)
}

/// Builds a direct Arrow row group for a key-image tuple.
fn pending_pgoutput_key_row_group(
    replicated_table_schema: &ReplicatedTableSchema,
    change: ChangeKind,
    row_image: RowImage,
    tuple_data: &[protocol::TupleData],
    meta: PendingPgOutputRowMeta,
) -> EtlResult<PendingPgOutputRows> {
    let table_schema = identity_arrow_table_schema(replicated_table_schema);
    let target = PendingPgOutputRowsTarget::new(
        replicated_table_schema.id(),
        change,
        row_image,
        table_schema,
        replicated_table_schema.clone(),
    );

    match pgoutput_key_tuple_projection(replicated_table_schema, tuple_data)? {
        Some(projection) => {
            PendingPgOutputRows::try_new_projected(target, tuple_data, &projection, meta)
        }
        None => PendingPgOutputRows::try_new(target, tuple_data, meta),
    }
}

/// Returns the projection needed to append a key tuple to identity columns.
fn pgoutput_key_tuple_projection(
    replicated_table_schema: &ReplicatedTableSchema,
    tuple_data: &[protocol::TupleData],
) -> EtlResult<Option<Vec<usize>>> {
    let identity_column_count = replicated_table_schema.identity_column_schemas().len();
    let replicated_column_count = replicated_table_schema.column_schemas().len();

    if identity_column_count == 0 {
        bail!(
            ErrorKind::ConversionError,
            "Replica-identity tuple missing key columns",
            "Key-image row was received for a table without replicated replica-identity columns"
        );
    }

    match tuple_data.len() {
        len if len == identity_column_count => Ok(None),
        len if len == replicated_column_count => {
            let mut projection = Vec::with_capacity(identity_column_count);
            let mut identity_columns = replicated_table_schema.identity_column_schemas().peekable();

            for (index, column_schema) in replicated_table_schema.column_schemas().enumerate() {
                if identity_columns.peek().is_some_and(|identity_column| {
                    identity_column.ordinal_position == column_schema.ordinal_position
                }) {
                    projection.push(index);
                    let _ = identity_columns.next();
                }
            }

            Ok(Some(projection))
        }
        _ => {
            bail!(
                ErrorKind::ConversionError,
                "Replica-identity tuple shape does not match schema",
                format!(
                    "Expected {} key values or {} replicated values for key image, got {}",
                    identity_column_count,
                    replicated_column_count,
                    tuple_data.len()
                )
            );
        }
    }
}

#[derive(Debug)]
struct HomogeneousFullNewBatchPlan {
    table_id: TableId,
    change: ChangeKind,
    table_schema: Arc<TableSchema>,
    replicated_table_schema: ReplicatedTableSchema,
    row_count: usize,
}

fn update_homogeneous_full_new_batch_plan(
    plan_table_id: &mut Option<TableId>,
    plan_change: &mut Option<ChangeKind>,
    plan_table_schema: &mut Option<Arc<TableSchema>>,
    plan_replicated_table_schema: &mut Option<ReplicatedTableSchema>,
    table_id: TableId,
    change: ChangeKind,
    replicated_table_schema: &ReplicatedTableSchema,
) -> bool {
    match (
        *plan_table_id,
        *plan_change,
        plan_table_schema.as_deref(),
        plan_replicated_table_schema.as_ref(),
    ) {
        (
            Some(existing_table_id),
            Some(existing_change),
            Some(existing_table_schema),
            Some(existing_replicated_table_schema),
        ) => {
            existing_table_id == table_id
                && existing_change == change
                && table_schema_matches_replicated(existing_table_schema, replicated_table_schema)
                && replicated_table_schemas_are_compatible(
                    existing_replicated_table_schema,
                    replicated_table_schema,
                )
        }
        (None, None, None, None) => {
            *plan_table_id = Some(table_id);
            *plan_change = Some(change);
            *plan_table_schema = Some(replicated_arrow_table_schema(replicated_table_schema));
            *plan_replicated_table_schema = Some(replicated_table_schema.clone());
            true
        }
        _ => false,
    }
}

fn homogeneous_full_new_batch_plan(
    pending_items: &[PendingBatchItem],
) -> EtlResult<Option<HomogeneousFullNewBatchPlan>> {
    let mut table_id = None;
    let mut change = None;
    let mut table_schema = None;
    let mut replicated_table_schema = None;
    let mut first_data_event_index = None;
    let mut row_count = 0;

    for (event_index, item) in pending_items.iter().enumerate() {
        match item {
            PendingBatchItem::Event(Event::Insert(insert)) => {
                if !update_homogeneous_full_new_batch_plan(
                    &mut table_id,
                    &mut change,
                    &mut table_schema,
                    &mut replicated_table_schema,
                    insert.replicated_table_schema.id(),
                    ChangeKind::Insert,
                    &insert.replicated_table_schema,
                ) {
                    return Ok(None);
                }
                first_data_event_index.get_or_insert(event_index);
                row_count += 1;
            }
            PendingBatchItem::Event(Event::Update(update)) => {
                if update.old_table_row.is_some()
                    || !matches!(update.updated_table_row, UpdatedTableRow::Full(_))
                {
                    return Ok(None);
                }

                if !update_homogeneous_full_new_batch_plan(
                    &mut table_id,
                    &mut change,
                    &mut table_schema,
                    &mut replicated_table_schema,
                    update.replicated_table_schema.id(),
                    ChangeKind::Update,
                    &update.replicated_table_schema,
                ) {
                    return Ok(None);
                }
                first_data_event_index.get_or_insert(event_index);
                row_count += 1;
            }
            PendingBatchItem::PgOutputChange(_) => return Ok(None),
            PendingBatchItem::Event(
                Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported,
            ) => {}
            PendingBatchItem::Event(Event::Delete(_) | Event::Truncate(_)) => return Ok(None),
        }
    }

    if first_data_event_index.is_none() {
        return Ok(None);
    }

    Ok(Some(HomogeneousFullNewBatchPlan {
        table_id: table_id.expect("data event must set table id"),
        change: change.expect("data event must set change kind"),
        table_schema: table_schema.expect("data event must set table schema"),
        replicated_table_schema: replicated_table_schema
            .expect("data event must set replicated table schema"),
        row_count,
    }))
}

fn normalize_homogeneous_full_new_batch(
    pending_items: Vec<PendingBatchItem>,
    plan: HomogeneousFullNewBatchPlan,
) -> EtlResult<Vec<StreamBatch>> {
    if let Some(rows_builder) =
        TableArrowBatchBuilder::try_new(Arc::clone(&plan.table_schema), plan.row_count)
    {
        return normalize_homogeneous_full_new_batch_with_builder(
            pending_items,
            plan,
            rows_builder,
        );
    }

    let mut table_rows = Vec::with_capacity(plan.row_count);
    let mut commit_lsns = Vec::with_capacity(plan.row_count);
    let mut tx_ordinals = Vec::with_capacity(plan.row_count);

    for item in pending_items {
        match item {
            PendingBatchItem::Event(Event::Insert(insert)) => {
                debug_assert_eq!(plan.change, ChangeKind::Insert);
                table_rows.push(insert.table_row);
                commit_lsns.push(u64::from(insert.commit_lsn));
                tx_ordinals.push(insert.tx_ordinal);
            }
            PendingBatchItem::Event(Event::Update(update)) => {
                debug_assert_eq!(plan.change, ChangeKind::Update);
                let UpdatedTableRow::Full(row) = update.updated_table_row else {
                    unreachable!("homogeneous plan already rejected partial updates");
                };
                table_rows.push(row);
                commit_lsns.push(u64::from(update.commit_lsn));
                tx_ordinals.push(update.tx_ordinal);
            }
            PendingBatchItem::PgOutputChange(_) => {
                unreachable!("homogeneous plan rejected direct pgoutput row groups");
            }
            PendingBatchItem::Event(
                Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported,
            ) => {}
            PendingBatchItem::Event(Event::Delete(_) | Event::Truncate(_)) => {
                unreachable!("homogeneous plan already rejected deletes and truncates");
            }
        }
    }

    let rows = table_rows_to_arrow_batch(plan.table_schema, table_rows.as_slice())?;
    let rows = rows.with_replicated_table_schema(plan.replicated_table_schema);
    Ok(vec![StreamBatch::Changes(TableChangeSet {
        table_id: plan.table_id,
        groups: vec![ChangeArrowBatch {
            rows,
            change: plan.change,
            row_image: RowImage::New,
            partial_update_missing_column_indexes: None,
            commit_lsns: UInt64Array::from(commit_lsns),
            tx_ordinals: UInt64Array::from(tx_ordinals),
        }],
    })])
}

fn normalize_homogeneous_full_new_batch_with_builder(
    pending_items: Vec<PendingBatchItem>,
    plan: HomogeneousFullNewBatchPlan,
    mut rows_builder: TableArrowBatchBuilder,
) -> EtlResult<Vec<StreamBatch>> {
    let mut commit_lsns = Vec::with_capacity(plan.row_count);
    let mut tx_ordinals = Vec::with_capacity(plan.row_count);

    for item in pending_items {
        match item {
            PendingBatchItem::Event(Event::Insert(insert)) => {
                debug_assert_eq!(plan.change, ChangeKind::Insert);
                rows_builder.append_trusted_row(&insert.table_row)?;
                commit_lsns.push(u64::from(insert.commit_lsn));
                tx_ordinals.push(insert.tx_ordinal);
            }
            PendingBatchItem::Event(Event::Update(update)) => {
                debug_assert_eq!(plan.change, ChangeKind::Update);
                let UpdatedTableRow::Full(row) = update.updated_table_row else {
                    unreachable!("homogeneous plan already rejected partial updates");
                };
                rows_builder.append_trusted_row(&row)?;
                commit_lsns.push(u64::from(update.commit_lsn));
                tx_ordinals.push(update.tx_ordinal);
            }
            PendingBatchItem::PgOutputChange(_) => {
                unreachable!("homogeneous plan rejected direct pgoutput row groups");
            }
            PendingBatchItem::Event(
                Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported,
            ) => {}
            PendingBatchItem::Event(Event::Delete(_) | Event::Truncate(_)) => {
                unreachable!("homogeneous plan already rejected deletes and truncates");
            }
        }
    }

    let rows = rows_builder.finish()?;
    let rows = rows.with_replicated_table_schema(plan.replicated_table_schema);
    Ok(vec![StreamBatch::Changes(TableChangeSet {
        table_id: plan.table_id,
        groups: vec![ChangeArrowBatch {
            rows,
            change: plan.change,
            row_image: RowImage::New,
            partial_update_missing_column_indexes: None,
            commit_lsns: UInt64Array::from(commit_lsns),
            tx_ordinals: UInt64Array::from(tx_ordinals),
        }],
    })])
}

/// Normalizes pending replication items into destination-facing Arrow batches.
fn normalize_pending_batch_items(
    pending_items: Vec<PendingBatchItem>,
) -> EtlResult<Vec<StreamBatch>> {
    if let Some(plan) = homogeneous_full_new_batch_plan(&pending_items)? {
        return normalize_homogeneous_full_new_batch(pending_items, plan);
    }

    let mut batches = Vec::new();
    let mut item_iter = pending_items.into_iter().peekable();

    while item_iter.peek().is_some() {
        let mut table_changes: HashMap<TableId, PendingTableChangeSet> = HashMap::new();
        let mut table_order = Vec::new();

        while let Some(item) = item_iter.peek() {
            if matches!(item, PendingBatchItem::Event(Event::Truncate(_))) {
                break;
            }

            let item = item_iter.next().expect("peeked item must exist");
            match item {
                PendingBatchItem::Event(Event::Insert(insert)) => {
                    let table_id = insert.replicated_table_schema.id();
                    push_pending_change(
                        &mut table_changes,
                        &mut table_order,
                        PendingChangeTarget::new(
                            table_id,
                            ChangeKind::Insert,
                            RowImage::New,
                            insert.replicated_table_schema.clone(),
                            None,
                        ),
                        insert.table_row,
                        PendingChangeMeta::new(insert.commit_lsn, insert.tx_ordinal),
                        || replicated_arrow_table_schema(&insert.replicated_table_schema),
                    )?;
                }
                PendingBatchItem::Event(Event::Update(update)) => {
                    let table_id = update.replicated_table_schema.id();

                    if let Some(old_table_row) = update.old_table_row {
                        let (key_only, table_schema, table_row) = match old_table_row {
                            OldTableRow::Full(row) => (false, None, row),
                            OldTableRow::Key(row) => (
                                true,
                                Some(identity_arrow_table_schema(&update.replicated_table_schema)),
                                row,
                            ),
                        };

                        push_pending_change(
                            &mut table_changes,
                            &mut table_order,
                            PendingChangeTarget::new(
                                table_id,
                                ChangeKind::Update,
                                RowImage::Old { key_only },
                                update.replicated_table_schema.clone(),
                                None,
                            ),
                            table_row,
                            PendingChangeMeta::new(update.commit_lsn, update.tx_ordinal),
                            || {
                                table_schema.unwrap_or_else(|| {
                                    replicated_arrow_table_schema(&update.replicated_table_schema)
                                })
                            },
                        )?;
                    }

                    let (table_row, table_schema, partial_update_missing_column_indexes) =
                        match update.updated_table_row {
                            UpdatedTableRow::Full(row) => (
                                row,
                                replicated_arrow_table_schema(&update.replicated_table_schema),
                                None,
                            ),
                            UpdatedTableRow::Partial(partial_row) => {
                                let (row, missing_column_indexes) = partial_row.into_parts();
                                let missing_column_indexes: Arc<[usize]> =
                                    missing_column_indexes.into();
                                let table_schema = partial_arrow_table_schema(
                                    &update.replicated_table_schema,
                                    &missing_column_indexes,
                                );

                                (row, table_schema, Some(missing_column_indexes))
                            }
                        };

                    push_pending_change(
                        &mut table_changes,
                        &mut table_order,
                        PendingChangeTarget::new(
                            table_id,
                            ChangeKind::Update,
                            RowImage::New,
                            update.replicated_table_schema.clone(),
                            partial_update_missing_column_indexes,
                        ),
                        table_row,
                        PendingChangeMeta::new(update.commit_lsn, update.tx_ordinal),
                        || table_schema,
                    )?;
                }
                PendingBatchItem::Event(Event::Delete(delete)) => {
                    let Some(old_table_row) = delete.old_table_row else {
                        bail!(
                            ErrorKind::ConversionError,
                            "Delete event missing old row image",
                            format!(
                                "Table {} emitted a delete without an old row image",
                                delete.replicated_table_schema.name()
                            )
                        );
                    };
                    let table_id = delete.replicated_table_schema.id();
                    let (key_only, table_schema, table_row) = match old_table_row {
                        OldTableRow::Full(row) => (
                            false,
                            replicated_arrow_table_schema(&delete.replicated_table_schema),
                            row,
                        ),
                        OldTableRow::Key(row) => (
                            true,
                            identity_arrow_table_schema(&delete.replicated_table_schema),
                            row,
                        ),
                    };

                    push_pending_change(
                        &mut table_changes,
                        &mut table_order,
                        PendingChangeTarget::new(
                            table_id,
                            ChangeKind::Delete,
                            RowImage::Old { key_only },
                            delete.replicated_table_schema.clone(),
                            None,
                        ),
                        table_row,
                        PendingChangeMeta::new(delete.commit_lsn, delete.tx_ordinal),
                        || table_schema,
                    )?;
                }
                PendingBatchItem::PgOutputChange(direct_change) => {
                    push_pending_pgoutput_change(
                        &mut table_changes,
                        &mut table_order,
                        direct_change,
                    )?;
                }
                PendingBatchItem::Event(
                    Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported,
                ) => {}
                PendingBatchItem::Event(Event::Truncate(_)) => {
                    unreachable!("truncate events are handled separately");
                }
            }
        }

        for table_id in table_order {
            let pending = table_changes
                .remove(&table_id)
                .expect("table order must only include known tables");
            let mut groups = Vec::with_capacity(pending.groups.len());

            for pending_group in pending.groups {
                groups.push(pending_group.into_change_arrow_batch()?);
            }

            batches.push(StreamBatch::Changes(TableChangeSet { table_id, groups }));
        }

        while let Some(PendingBatchItem::Event(Event::Truncate(_))) = item_iter.peek() {
            if let Some(PendingBatchItem::Event(Event::Truncate(truncate))) = item_iter.next() {
                batches.push(StreamBatch::Truncate(TruncateBatch {
                    rel_ids: truncate
                        .truncated_tables
                        .into_iter()
                        .map(|schema| schema.id())
                        .collect(),
                    commit_lsn: truncate.commit_lsn,
                    tx_ordinal: truncate.tx_ordinal,
                    options: truncate.options,
                }));
            }
        }
    }

    Ok(batches)
}

/// Main apply loop implementation that processes replication events.
///
/// [`ApplyLoop`] encapsulates the apply loop's immutable dependencies plus its
/// mutable runtime state.
pub(crate) struct ApplyLoop<S, D> {
    /// Unique identifier for the pipeline.
    pipeline_id: PipelineId,
    /// Shared immutable configuration.
    config: Arc<PipelineConfig>,
    /// Schema store for table schemas.
    schema_store: S,
    /// Destination where replicated data is written.
    destination: D,
    /// Shared per-table protocol state used by apply and table-sync workers.
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
    S: PipelineStore,
    D: PipelineDestination,
{
    /// Starts the apply loop for processing replication events.
    ///
    /// This is the main entry point that creates the loop instance and runs it.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn start(
        pipeline_id: PipelineId,
        start_lsn: PgLsn,
        config: Arc<PipelineConfig>,
        replication_client: &PgReplicationClient,
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

        let initial_progress =
            ReplicationProgress { last_received_lsn: start_lsn, last_flush_lsn: start_lsn };

        let state = ApplyLoopState::new(initial_progress, keep_alive_deadline_duration);

        let mut apply_loop = Self {
            pipeline_id,
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
        replication_client: &PgReplicationClient,
        start_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopResult> {
        let result = self.run(replication_client, start_lsn).await;

        self.drain_schema_cleanup_task().await;

        result
    }

    /// Runs the main event processing loop.
    async fn run(
        &mut self,
        replication_client: &PgReplicationClient,
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

        let events_stream = EventsStream::wrap(logical_replication_stream);
        let stream_id = self.worker_context.apply_stream_id();
        let events_stream =
            BackpressureStream::wrap(events_stream, stream_id, self.memory_monitor.subscribe());
        pin!(events_stream);

        let mut connection_updates_rx = replication_client.connection_updates_rx();

        loop {
            let result = match &self.state.shutdown_state {
                ShutdownState::NoShutdown => {
                    self.run_active_iteration(
                        events_stream.as_mut(),
                        replication_client,
                        &mut connection_updates_rx,
                    )
                    .await?
                }
                ShutdownState::DrainingForShutdown => {
                    self.run_draining_shutdown_iteration(
                        events_stream.as_mut(),
                        &mut connection_updates_rx,
                    )
                    .await?
                }
            };

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
    /// 4. Batch flush deadline expiry.
    /// 5. Incoming replication messages.
    /// 6. Periodic heartbeats once the computed keep alive deadline expires.
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
        self.maybe_process_syncing_tables_when_idle().await?;

        // We try to finish the active iteration even before starting it, since we might
        // be able to finish earlier, without having to process any signal from
        // the following `select!`.
        if let Some(result) = self.try_finish_active_iteration() {
            return Ok(Some(result));
        }

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

    /// Runs one iteration of the shutdown drain state.
    ///
    /// This mirrors the active state but omits shutdown handling and new WAL
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
    /// final shutdown status update and exits.
    async fn run_draining_shutdown_iteration(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
    ) -> EtlResult<Option<ApplyLoopResult>> {
        if !self.state.has_unresolved_batch_work() {
            self.initiate_graceful_shutdown(events_stream.as_mut()).await?;

            return Ok(Some(self.finish_shutdown()));
        }

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

        if !self.state.has_unresolved_batch_work() {
            self.initiate_graceful_shutdown(events_stream.as_mut()).await?;

            return Ok(Some(self.finish_shutdown()));
        }

        Ok(None)
    }

    /// Returns the final loop result after all buffered destination work has
    /// resolved.
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
                "PostgreSQL connection updates ended during the apply loop"
            ));
        }

        let update = connection_updates_rx.borrow_and_update().clone();
        match update {
            PostgresConnectionUpdate::Running => Ok(()),
            PostgresConnectionUpdate::Terminated => Err(etl_error!(
                ErrorKind::SourceConnectionFailed,
                "PostgreSQL connection terminated during the apply loop"
            )),
            PostgresConnectionUpdate::Errored { error } => Err(etl_error!(
                ErrorKind::SourceConnectionFailed,
                "PostgreSQL connection errored during the apply loop",
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

    /// Handles a shutdown signal.
    ///
    /// Shutdown stops new message intake immediately. If there is already
    /// buffered or in-flight destination work, the loop first drains that work
    /// so the best durable position can advance before sending the final
    /// shutdown status update. Otherwise it sends that update immediately and
    /// exits the loop.
    ///
    /// Note: the shutdown system is best-effort. Graceful shutdown may not
    /// complete if we are blocked on non-interruptible code. It is the
    /// responsibility of the caller to forcefully kill the process if shutdown
    /// does not complete within an acceptable timeframe.
    async fn handle_shutdown_signal(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
    ) -> EtlResult<()> {
        let worker_type = self.worker_context.worker_type();

        // If the shutdown was already requested, we silently skip it.
        if self.state.shutdown_state.is_requested() {
            info!(
                %worker_type,
                shutdown_state = %self.state.shutdown_state,
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
            "shutdown signal received, no unresolved work left, sending final status update",
        );

        self.initiate_graceful_shutdown(events_stream.as_mut()).await
    }

    /// Initiates graceful shutdown by sending a final status update just to
    /// make sure that Postgres can advance its state once more.
    ///
    /// The status update uses the loop's effective flush position. When idle,
    /// this may include received keepalive progress that is intentionally not
    /// persisted as durable ETL progress.
    async fn initiate_graceful_shutdown(
        &mut self,
        mut events_stream: Pin<&mut BackpressureStream<EventsStream>>,
    ) -> EtlResult<()> {
        let worker_type = self.worker_context.worker_type();

        let flush_lsn = self.state.effective_flush_lsn();

        info!(
            %worker_type,
            %flush_lsn,
            "sending shutdown status update",
        );

        self.send_status_update(
            events_stream.as_mut(),
            flush_lsn,
            true,
            StatusUpdateType::ShutdownFlush,
        )
        .await?;

        self.state.shutdown_state = ShutdownState::NoShutdown;

        Ok(())
    }

    /// Sends a status update to PostgreSQL using the current write and flush
    /// positions.
    ///
    /// Some callers intentionally resend the same effective flush LSN with
    /// `force = true`. Those updates are about keeping the replication
    /// connection alive while the system is idle, not about advertising
    /// newly flushed progress. Keepalive replies from the main replication
    /// stream and the final shutdown update still use this same helper.
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
        if let StatusUpdateResult::Sent = status_update_result {
            self.maybe_spawn_schema_cleanup().await?;
        }

        Ok(())
    }

    /// Attempts to spawn a best-effort task that prunes obsolete schema
    /// versions.
    ///
    /// Cleanup uses ETL-owned durable replication progress. If no progress row
    /// exists yet, pruning is skipped instead of relying on PostgreSQL slot
    /// state.
    async fn maybe_spawn_schema_cleanup(&mut self) -> EtlResult<()> {
        self.collect_finished_schema_cleanup_task().await;

        if self.state.has_schema_cleanup_task() {
            return Ok(());
        }

        let Some(schema_cleanup_run) = self.state.try_start_schema_cleanup().await else {
            return Ok(());
        };

        let worker_type = self.worker_context.worker_type();

        let durable_flush_lsn = match self.schema_store.get_replication_progress(worker_type).await
        {
            Ok(Some(durable_flush_lsn)) => durable_flush_lsn,
            Ok(None) => {
                debug!(
                    %worker_type,
                    "skipping schema cleanup because durable replication progress is not available"
                );

                schema_cleanup_run.finish().await;

                return Ok(());
            }
            Err(err) => {
                warn!(
                    %worker_type,
                    error = %err,
                    "skipping schema cleanup because durable replication progress could not be loaded"
                );

                schema_cleanup_run.finish().await;

                return Ok(());
            }
        };

        // We get the table retention boundaries that this apply loop instance
        // can safely prune up to. The map is frozen before the background task
        // is spawned, so any concurrent progress can only make this cleanup
        // conservative.
        let table_schema_retentions =
            match self.get_table_schema_retentions(durable_flush_lsn).await {
                Ok(table_schema_retentions) => table_schema_retentions,
                Err(err) => {
                    error!(
                        %worker_type,
                        error = %err,
                        "failed to determine schema cleanup ownership"
                    );

                    schema_cleanup_run.finish().await;

                    return Ok(());
                }
            };

        // If there are no tables to try to prune, we don't want to attempt it.
        if table_schema_retentions.is_empty() {
            schema_cleanup_run.finish().await;

            return Ok(());
        }

        let schema_store = self.schema_store.clone();
        let table_count = table_schema_retentions.len() as u64;

        // At this point we know the retention boundaries to check for pruning,
        // so we can offload this to a background task to not stall the worker.
        // This is fine since those boundaries are frozen, so this task can take
        // its time to do the job without interfering with the apply loop. Also,
        // no more than one pruning task can occur at the same time within an
        // apply loop instance, so this makes this even safer.
        let cleanup_fut = async move {
            match schema_store.prune_table_schemas(table_schema_retentions).await {
                Ok(deleted_count) => {
                    counter!(
                        ETL_SCHEMA_CLEANUPS_TOTAL,
                        WORKER_TYPE_LABEL => worker_type.as_str(),
                    )
                    .increment(1);

                    counter!(
                        ETL_SCHEMA_CLEANUP_TABLES_TOTAL,
                        WORKER_TYPE_LABEL => worker_type.as_str(),
                    )
                    .increment(table_count);

                    counter!(
                        ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL,
                        WORKER_TYPE_LABEL => worker_type.as_str(),
                    )
                    .increment(deleted_count);

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
            };
        };

        let cleanup_task = tokio::spawn(async move {
            cleanup_fut.await;
            schema_cleanup_run.finish().await;
        });
        self.state.set_schema_cleanup_task(cleanup_task);

        Ok(())
    }

    /// Returns schema retention boundaries for tables this worker may clean up.
    ///
    /// The shared cache is used only to find active tables, and the worker's
    /// normal ownership check decides which of those tables can be considered.
    /// A table's cleanup boundary is capped by durable ETL replication progress
    /// and by the earliest destination metadata snapshot that may still be
    /// needed.
    async fn get_table_schema_retentions(
        &self,
        durable_flush_lsn: PgLsn,
    ) -> EtlResult<HashMap<TableId, TableSchemaRetention>> {
        let active_table_ids = self.shared_table_cache.active_table_ids().await;
        let mut table_schema_retentions = HashMap::with_capacity(active_table_ids.len());

        for table_id in active_table_ids {
            // Only prune snapshots for tables this worker would apply at this
            // flush position. This keeps table sync workers limited to their
            // assigned table while preserving apply worker ownership rules.
            let should_apply_changes =
                self.should_apply_changes(table_id, durable_flush_lsn).await?;

            if !should_apply_changes {
                continue;
            }

            // We try to load the destination table metadata to see if it is referencing a
            // snapshot id that would be cleaned up if we were to just use the
            // durable flush lsn as the boundary.
            //
            // If there is no metadata, we play it safe and skip the pruning for this table.
            let Some(destination_table_metadata) =
                self.schema_store.get_destination_table_metadata(table_id).await?
            else {
                debug!(
                    %table_id,
                    "skipping schema cleanup for table without destination metadata"
                );

                continue;
            };

            // We take the earliest snapshot id that is still needed by the destination
            // table metadata.
            let destination_retention_snapshot_id = destination_table_metadata
                .previous_snapshot_id
                .unwrap_or(destination_table_metadata.snapshot_id)
                .min(destination_table_metadata.snapshot_id);

            // We determine whether the durable flush lsn or the snapshot id is the new
            // retention limit. We could use a normal PgLsn type for handling
            // this, but to make the implementation more explicit we use an enum.
            let retention = if durable_flush_lsn <= destination_retention_snapshot_id.into_inner() {
                TableSchemaRetention::DurableFlushLsn(durable_flush_lsn)
            } else {
                TableSchemaRetention::SnapshotId(destination_retention_snapshot_id)
            };

            table_schema_retentions.insert(table_id, retention);
        }

        Ok(table_schema_retentions)
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

    /// Normalizes raw logical-replication events into destination-facing Arrow
    /// batches.
    fn normalize_pending_batch(
        &self,
        pending_items: Vec<PendingBatchItem>,
    ) -> EtlResult<Vec<StreamBatch>> {
        normalize_pending_batch_items(pending_items)
    }

    /// Waits for the pending flush result, if any.
    async fn wait_for_flush_result(
        pending_flush_result: Option<&mut PendingWriteStreamBatchesResult<()>>,
    ) -> CompletedWriteStreamBatchesResult<()> {
        match pending_flush_result {
            Some(flush_result) => flush_result.await,
            None => std::future::pending().await,
        }
    }

    /// Handles a completed batch flush result.
    async fn handle_flush_result(
        &mut self,
        flush_result: CompletedWriteStreamBatchesResult<()>,
    ) -> EtlResult<()> {
        // We clear the state up front because this flush is no longer in flight.
        let processing_paused = self.state.resume_processing();

        // Explode the result into parts which are used for handling the flush result.
        let (metadata, result) = flush_result.into_parts();

        // If there was an error in the flushing, we return it immediately.
        result?;

        counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
            ACTION_LABEL => "table_streaming",
        )
        .increment(metadata.metrics.items_count as u64);

        histogram!(
            ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
            ACTION_LABEL => "table_streaming",
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

        let should_include_event = matches!(result.end_batch, None | Some(EndBatch::Inclusive));
        let mut batch_updated = result.batch_updated;
        if let Some(batch_item) = result.batch_item
            && should_include_event
        {
            // We add the element to the pending batch.
            self.state.add_item_to_batch(batch_item);
            batch_updated = true;
        }

        if batch_updated {
            // We update the last end lsn of the commit that we encountered, if any.
            self.state.update_last_commit_end_lsn(result.end_lsn);

            // We start the batch timer for the flushing. This timer is needed to control
            // force flushing of a batch if its size is not reached in time.
            self.state.set_flush_deadline_if_needed(self.max_batch_fill_duration);
        }

        // We check for the batch flushing conditions before deciding whether to flush
        // or not.
        let batch_size_reached =
            self.state.pending_batch_bytes >= self.cached_batch_budget.current_batch_size_bytes();
        let early_flush_requested = result.end_batch.is_some();
        let should_flush = batch_size_reached || early_flush_requested;

        if should_flush {
            let reason = if batch_size_reached {
                "max batch bytes reached"
            } else {
                "early flush requested"
            };

            self.flush_batch(reason).await?;
        }

        if let Some((table_id, error)) = result.table_replication_error {
            self.mark_table_errored(table_id, error).await?;
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

        // We replace the existing vector with a new one and reset the accumulated batch
        // bytes size.
        let (pending_batch, pending_batch_bytes) = self.state.take_pending_batch();

        let pending_batch_size = pending_batch.iter().map(PendingBatchItem::item_count).sum();
        info!(
            worker_type = %self.worker_context.worker_type(),
            batch_size = pending_batch_size,
            batch_size_bytes = pending_batch_bytes,
            %reason,
            "flushing batch to destination",
        );

        // Capture dispatch-time metrics; they are carried through the result channel
        // and recorded once the destination acknowledges the batch.
        let metadata = ApplyLoopAsyncResultMetadata {
            commit_end_lsn: self.state.last_commit_end_lsn.take(),
            metrics: DispatchMetrics {
                items_count: pending_batch_size,
                dispatched_at: Instant::now(),
            },
        };

        // Create the flush result channel: the sender is handed to the destination and
        // the pending receiver is stored on the loop state until the
        // destination signals completion.
        let stream_batches = self.normalize_pending_batch(pending_batch)?;
        let (flush_result, pending_flush_result) = WriteStreamBatchesResult::new(metadata);
        self.destination.write_stream_batches(stream_batches, flush_result).await?;
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

    /// Handles Postgres MESSAGE messages (`pg_logical_emit_message`).
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

        // Build table schema from DDL message with start_lsn as the snapshot ID.
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
            batch_item: Some(PendingBatchItem::Event(Event::Commit(event))),
            end_lsn: Some(end_lsn),
            ..Default::default()
        };

        // Any requested exit forces the current commit batch to end, including the
        // commit event itself. For shutdown, this is mainly the catch-up wait
        // path requesting a pause exit, which lets that case reuse the normal
        // commit flush flow.
        if should_end_batch {
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

        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let table_schema = self
            .schema_store
            .get_table_schema(&table_id, SnapshotId::from(remote_final_lsn))
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found in cache",
                    format!("Table schema for table {} not found in cache", table_id)
                )
            })?;

        let replicated_column_names = parse_replicated_column_names(message)?;
        let identity_column_names = parse_replica_identity_column_names(message)?;
        let replication_mask = ReplicationMask::try_build(&table_schema, &replicated_column_names)
            .map_err(|err: SchemaError| {
                etl_error!(
                    ErrorKind::SourceSchemaError,
                    "Relation message did not match stored table schema",
                    source: err
                )
            })?;
        let identity_mask = IdentityMask::try_build(&table_schema, &identity_column_names)
            .map_err(|err: SchemaError| {
                etl_error!(
                    ErrorKind::SourceSchemaError,
                    "Relation identity did not match stored table schema",
                    source: err
                )
            })?;
        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);
        self.shared_table_cache.note_ready(table_id, replicated_table_schema.clone()).await;

        let event = RelationEvent {
            start_lsn,
            commit_lsn: remote_final_lsn,
            tx_ordinal,
            replicated_table_schema,
        };

        Ok(HandleMessageResult::return_event(Event::Relation(event)))
    }

    /// Returns the ready replicated schema needed to decode a row message.
    async fn get_ready_replicated_table_schema(
        &self,
        table_id: TableId,
        action: &'static str,
    ) -> EtlResult<ReplicatedTableSchema> {
        let Some(shared_state) = self.shared_table_cache.get(&table_id).await else {
            bail!(
                ErrorKind::InvalidState,
                "relation state missing while decoding row message",
                format!("No relation state found for table {table_id} while decoding {action}")
            );
        };

        shared_state.replicated_table_schema().cloned().ok_or_else(|| {
            etl_error!(
                ErrorKind::InvalidState,
                "relation state is not ready while decoding row message",
                format!(
                    "Table {table_id} is waiting for relation state for snapshot {} while \
                     decoding {action}",
                    shared_state.snapshot_id()
                )
            )
        })
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
        let table_id = TableId::new(message.rel_id());

        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            self.get_ready_replicated_table_schema(table_id, "insert").await?;
        let tuple_data = message.tuple().tuple_data();
        if can_append_pgoutput_tuple_directly(tuple_data) {
            let row_size_bytes = calculate_tuple_bytes(tuple_data);
            record_cdc_tuple_metrics("insert", row_size_bytes);

            let target = PendingPgOutputRowsTarget::new(
                table_id,
                ChangeKind::Insert,
                RowImage::New,
                replicated_arrow_table_schema(&replicated_table_schema),
                replicated_table_schema.clone(),
            );
            let meta = PendingPgOutputRowMeta::new(
                remote_final_lsn,
                tx_ordinal,
                tuple_bytes_to_size_hint(row_size_bytes),
            );
            self.state.add_pgoutput_tuple_to_batch(target, tuple_data, meta)?;

            return Ok(HandleMessageResult::batch_updated());
        }

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

        let tx_ordinal = self.state.next_tx_ordinal();
        let table_id = TableId::new(message.rel_id());

        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            self.get_ready_replicated_table_schema(table_id, "update").await?;
        let new_tuple_data = message.new_tuple().tuple_data();
        if can_append_pgoutput_tuple_directly(new_tuple_data) {
            match (message.old_tuple(), message.key_tuple()) {
                (None, None) => {
                    let row_size_bytes = calculate_tuple_bytes(new_tuple_data);
                    record_cdc_tuple_metrics("update", row_size_bytes);

                    let target = PendingPgOutputRowsTarget::new(
                        table_id,
                        ChangeKind::Update,
                        RowImage::New,
                        replicated_arrow_table_schema(&replicated_table_schema),
                        replicated_table_schema.clone(),
                    );
                    let meta = PendingPgOutputRowMeta::new(
                        remote_final_lsn,
                        tx_ordinal,
                        tuple_bytes_to_size_hint(row_size_bytes),
                    );
                    self.state.add_pgoutput_tuple_to_batch(target, new_tuple_data, meta)?;

                    return Ok(HandleMessageResult::batch_updated());
                }
                (Some(old_tuple), None)
                    if can_append_pgoutput_tuple_directly(old_tuple.tuple_data()) =>
                {
                    let old_tuple_data = old_tuple.tuple_data();
                    let old_tuple_bytes = calculate_tuple_bytes(old_tuple_data);
                    let new_tuple_bytes = calculate_tuple_bytes(new_tuple_data);
                    record_cdc_tuple_metrics("update", old_tuple_bytes + new_tuple_bytes);

                    let old_rows = pending_pgoutput_full_row_group(
                        &replicated_table_schema,
                        ChangeKind::Update,
                        RowImage::Old { key_only: false },
                        old_tuple_data,
                        PendingPgOutputRowMeta::new(
                            remote_final_lsn,
                            tx_ordinal,
                            tuple_bytes_to_size_hint(old_tuple_bytes),
                        ),
                    )?;
                    let new_rows = pending_pgoutput_full_row_group(
                        &replicated_table_schema,
                        ChangeKind::Update,
                        RowImage::New,
                        new_tuple_data,
                        PendingPgOutputRowMeta::new(
                            remote_final_lsn,
                            tx_ordinal,
                            tuple_bytes_to_size_hint(new_tuple_bytes),
                        ),
                    )?;
                    let direct_change =
                        PendingPgOutputChange::new_multi_group(vec![old_rows, new_rows]);
                    self.state.add_pgoutput_change_to_batch(direct_change);

                    return Ok(HandleMessageResult::batch_updated());
                }
                (None, Some(key_tuple))
                    if can_append_pgoutput_tuple_directly(key_tuple.tuple_data()) =>
                {
                    let key_tuple_data = key_tuple.tuple_data();
                    let key_tuple_bytes = calculate_tuple_bytes(key_tuple_data);
                    let new_tuple_bytes = calculate_tuple_bytes(new_tuple_data);
                    record_cdc_tuple_metrics("update", key_tuple_bytes + new_tuple_bytes);

                    let old_rows = pending_pgoutput_key_row_group(
                        &replicated_table_schema,
                        ChangeKind::Update,
                        RowImage::Old { key_only: true },
                        key_tuple_data,
                        PendingPgOutputRowMeta::new(
                            remote_final_lsn,
                            tx_ordinal,
                            tuple_bytes_to_size_hint(key_tuple_bytes),
                        ),
                    )?;
                    let new_rows = pending_pgoutput_full_row_group(
                        &replicated_table_schema,
                        ChangeKind::Update,
                        RowImage::New,
                        new_tuple_data,
                        PendingPgOutputRowMeta::new(
                            remote_final_lsn,
                            tx_ordinal,
                            tuple_bytes_to_size_hint(new_tuple_bytes),
                        ),
                    )?;
                    let direct_change =
                        PendingPgOutputChange::new_multi_group(vec![old_rows, new_rows]);
                    self.state.add_pgoutput_change_to_batch(direct_change);

                    return Ok(HandleMessageResult::batch_updated());
                }
                _ => {}
            }
        }

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

        let tx_ordinal = self.state.next_tx_ordinal();
        let table_id = TableId::new(message.rel_id());

        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            self.get_ready_replicated_table_schema(table_id, "delete").await?;
        let is_key = message.old_tuple().is_none();
        let old_tuple = message.old_tuple().or(message.key_tuple());
        if let Some(old_tuple) = old_tuple
            && can_append_pgoutput_tuple_directly(old_tuple.tuple_data())
        {
            let old_tuple_data = old_tuple.tuple_data();
            let old_tuple_bytes = calculate_tuple_bytes(old_tuple_data);
            record_cdc_tuple_metrics("delete", old_tuple_bytes);

            let rows = if is_key {
                pending_pgoutput_key_row_group(
                    &replicated_table_schema,
                    ChangeKind::Delete,
                    RowImage::Old { key_only: true },
                    old_tuple_data,
                    PendingPgOutputRowMeta::new(
                        remote_final_lsn,
                        tx_ordinal,
                        tuple_bytes_to_size_hint(old_tuple_bytes),
                    ),
                )?
            } else {
                pending_pgoutput_full_row_group(
                    &replicated_table_schema,
                    ChangeKind::Delete,
                    RowImage::Old { key_only: false },
                    old_tuple_data,
                    PendingPgOutputRowMeta::new(
                        remote_final_lsn,
                        tx_ordinal,
                        tuple_bytes_to_size_hint(old_tuple_bytes),
                    ),
                )?
            };
            self.state.add_pgoutput_change_to_batch(PendingPgOutputChange::new_single_group(rows));

            return Ok(HandleMessageResult::batch_updated());
        }

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

        let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
        for &table_id in message.rel_ids() {
            let should_apply_truncate =
                self.should_apply_changes(TableId::new(table_id), remote_final_lsn).await?;
            if should_apply_truncate {
                rel_ids.push(table_id);
            }
        }

        if rel_ids.is_empty() {
            return Ok(HandleMessageResult::no_event());
        }

        let mut truncated_tables = Vec::with_capacity(rel_ids.len());
        for rel_id in rel_ids {
            let table_id = TableId::new(rel_id);
            let table_schema = self
                .schema_store
                .get_table_schema(&table_id, SnapshotId::from(remote_final_lsn))
                .await?
                .ok_or_else(|| {
                    etl_error!(
                        ErrorKind::MissingTableSchema,
                        "Table schema not found while decoding truncate",
                        format!("No schema found for table {table_id}")
                    )
                })?;
            truncated_tables.push(ReplicatedTableSchema::all(table_schema));
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

    /// Determines whether changes should be applied for a given table.
    ///
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
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
        // Store durable replication progress at commit boundaries after the
        // destination acknowledges the batch. This gives restarts a durable
        // lower-bound resume point: once startup chooses this point, no event
        // older than it will be emitted. It is not meant to eliminate every
        // duplicate, and idle keepalive-only progress is intentionally left out
        // to avoid writing to the customer database on every quiet heartbeat.
        let durable_flush_lsn =
            self.upsert_durable_replication_progress(last_commit_end_lsn).await?;
        self.state.replication_progress.update_last_flush_lsn(durable_flush_lsn);

        let current_lsn = self.state.replication_progress.last_flush_lsn;
        info!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables after durable batch flush"
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

    /// Stores durable worker progress unless fault injection asks us to skip
    /// it.
    async fn upsert_durable_replication_progress(&self, flush_lsn: PgLsn) -> EtlResult<PgLsn> {
        let worker_type = self.worker_context.worker_type();

        #[cfg(feature = "failpoints")]
        if etl_fail_point_active(STORE_REPLICATION_PROGRESS_FP) {
            warn!(
                %worker_type,
                %flush_lsn,
                "not storing durable replication progress due to active failpoint"
            );

            return Ok(flush_lsn);
        }

        self.schema_store.upsert_replication_progress(worker_type, flush_lsn).await
    }

    /// Processes syncing tables when the apply loop is idle.
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
            "processing syncing tables while idle"
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
        table_id: TableId,
        table_replication_error: TableError,
    ) -> EtlResult<()> {
        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::mark_table_errored(ctx, table_id, table_replication_error).await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::mark_table_errored(ctx, table_id, table_replication_error).await
            }
        }?;

        self.state.record_exit_intent(exit_intent);

        Ok(())
    }
}

/// Returns an iterator over tables that are still synchronizing.
async fn get_syncing_tables<S>(
    store: &S,
) -> EtlResult<impl Iterator<Item = (TableId, TableState)> + use<S>>
where
    S: StateStore,
{
    let states = store.get_table_states().await?;
    let syncing_states = states
        .iter()
        .filter(|(_, state)| !state.as_type().is_done())
        .map(|(table_id, state)| (*table_id, state.clone()))
        .collect::<Vec<_>>();

    Ok(syncing_states.into_iter())
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
        S: SharedStateStore,
    {
        fn is_phase_ready_for_changes(phase: TableState, remote_final_lsn: PgLsn) -> bool {
            match phase {
                TableState::Ready => true,
                TableState::SyncDone { lsn } => lsn <= remote_final_lsn,
                _ => false,
            }
        }

        let active_worker_state = ctx.pool.get_active_worker_state(table_id).await;

        if let Some(active_worker_state) = active_worker_state {
            let inner = active_worker_state.lock().await;
            return Ok(is_phase_ready_for_changes(inner.table_state(), remote_final_lsn));
        }

        // If we didn't find an active worker, we need to read the replication phase
        // from the store. This could happen if the event is from a table that
        // has to be synced, or it was synced.
        let Some(phase) = ctx.store.get_table_state(table_id).await? else {
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
        S: PipelineStore,
        D: PipelineDestination,
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
        table_replication_phase: TableState,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        if let Some(worker_state) = worker_state {
            let mut worker_state_guard = worker_state.lock().await;
            let phase = worker_state_guard.table_state();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?phase,
                %current_lsn,
                "checking table with active worker after commit",
            );

            match phase {
                TableState::SyncWait { lsn: snapshot_lsn } => {
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
                        .set_and_store(TableState::Catchup { lsn: catchup_lsn }, &ctx.store)
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
                        .wait_for_state_type(
                            &[TableStateType::SyncDone, TableStateType::Errored],
                            ctx.shutdown_rx.clone(),
                        )
                        .await;

                    match result {
                        ShutdownResult::Ok(result) => {
                            let final_phase = result.table_state();
                            if final_phase.is_errored() {
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
                TableState::SyncDone { lsn } => {
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
                TableState::SyncDone { lsn } => {
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
        S: PipelineStore,
        D: PipelineDestination,
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
        table_replication_phase: TableState,
        current_lsn: PgLsn,
    ) -> EtlResult<()>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        // If there is an active worker, we want to see if we can switch it to the ready
        // state. If there isn't an active worker, we just try to see if we can
        // switch the table to ready state or start a new worker for that table.
        if let Some(worker_state) = worker_state {
            let worker_state_guard = worker_state.lock().await;
            let phase = worker_state_guard.table_state();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?phase,
                %current_lsn,
                "checking table with active worker after batch flush",
            );

            if let TableState::SyncDone { lsn: sync_done_lsn } = phase {
                if current_lsn >= sync_done_lsn {
                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %sync_done_lsn,
                        %current_lsn,
                        "transitioning sync_done -> ready",
                    );

                    ctx.store.update_table_state(table_id, TableState::Ready).await?;
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
                TableState::SyncDone { lsn: sync_done_lsn } => {
                    if current_lsn >= sync_done_lsn {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store.update_table_state(table_id, TableState::Ready).await?;
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
        S: PipelineStore,
        D: PipelineDestination,
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
        table_replication_phase: TableState,
        current_lsn: PgLsn,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        // If there is an active worker, we want to see if we can start the catchup or
        // if we can switch it to ready state.
        // If there isn't an active worker, we just try to see if we can switch the
        // table to ready state or start a new worker for that table.
        if let Some(worker_state) = worker_state {
            let mut worker_state_guard = worker_state.lock().await;
            let phase = worker_state_guard.table_state();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                ?phase,
                %current_lsn,
                "checking table with active worker when idle",
            );

            match phase {
                TableState::SyncWait { lsn: snapshot_lsn } => {
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
                        .set_and_store(TableState::Catchup { lsn: catchup_lsn }, &ctx.store)
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
                        .wait_for_state_type(
                            &[TableStateType::SyncDone, TableStateType::Errored],
                            ctx.shutdown_rx.clone(),
                        )
                        .await;

                    match result {
                        ShutdownResult::Ok(result) => {
                            let final_phase = result.table_state();
                            if final_phase.is_errored() {
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
                TableState::SyncDone { lsn: sync_done_lsn } => {
                    if current_lsn >= sync_done_lsn {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store.update_table_state(table_id, TableState::Ready).await?;
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
                TableState::SyncDone { lsn: sync_done_lsn } => {
                    if current_lsn >= sync_done_lsn {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store.update_table_state(table_id, TableState::Ready).await?;
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
        table_id: TableId,
        table_replication_error: TableError,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: SharedStateStore,
    {
        ctx.store.update_table_state(table_id, table_replication_error.into()).await?;

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
        S: PipelineStore,
        D: PipelineDestination,
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
        if let TableState::Catchup { lsn: catchup_lsn } = inner.table_state() {
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
        S: SharedStateStore,
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
        S: SharedStateStore,
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
        S: SharedStateStore,
    {
        let worker_type = WorkerType::TableSync { table_id: ctx.table_id };
        let mut inner = ctx.table_sync_worker_state.lock().await;
        let phase = inner.table_state();

        if let TableState::Catchup { lsn: catchup_lsn } = phase {
            if current_lsn >= catchup_lsn {
                info!(
                    %worker_type,
                    %catchup_lsn,
                    %current_lsn,
                    "catchup target lsn reached, transitioning catchup -> sync_done",
                );

                inner
                    .set_and_store(TableState::SyncDone { lsn: current_lsn }, &ctx.state_store)
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
        table_id: TableId,
        table_replication_error: TableError,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: SharedStateStore,
    {
        if ctx.table_id != table_id {
            return Ok(None);
        }

        let mut inner = ctx.table_sync_worker_state.lock().await;
        inner.set_and_store(table_replication_error.into(), &ctx.state_store).await?;

        Ok(Some(ExitIntent::Complete))
    }
}

#[cfg(test)]
mod tests {
    use etl_postgres::types::{ColumnSchema, TableName};
    use postgres_replication::protocol::TupleData;

    use super::*;
    use crate::{
        conversions::arrow::record_batch_to_table_rows,
        types::{Cell, Type},
    };

    fn column(name: &str, typ: Type, ordinal_position: i32, primary_key: bool) -> ColumnSchema {
        ColumnSchema::new(
            name.to_owned(),
            typ,
            -1,
            ordinal_position,
            if primary_key { Some(1) } else { None },
            false,
        )
    }

    fn scalar_table_schema() -> Arc<TableSchema> {
        Arc::new(TableSchema::new(
            TableId::new(777),
            TableName::new("public".to_owned(), "direct_pgoutput".to_owned()),
            vec![
                column("id", Type::INT8, 1, true),
                column("active", Type::BOOL, 2, false),
                column("payload", Type::JSONB, 3, false),
            ],
        ))
    }

    fn scalar_table_schema_with_snapshot(
        snapshot_id: u64,
        columns: Vec<ColumnSchema>,
    ) -> Arc<TableSchema> {
        Arc::new(TableSchema::with_snapshot_id(
            TableId::new(777),
            TableName::new("public".to_owned(), "direct_pgoutput".to_owned()),
            columns,
            SnapshotId::from(snapshot_id),
        ))
    }

    fn insert_event(
        replicated_table_schema: ReplicatedTableSchema,
        tx_ordinal: u64,
        table_row: TableRow,
    ) -> PendingBatchItem {
        PendingBatchItem::Event(Event::Insert(crate::types::InsertEvent {
            start_lsn: PgLsn::from(1),
            commit_lsn: PgLsn::from(99),
            tx_ordinal,
            replicated_table_schema,
            table_row,
        }))
    }

    #[test]
    fn normalizer_preserves_direct_pgoutput_arrow_rows() {
        let replicated_table_schema = ReplicatedTableSchema::all(scalar_table_schema());
        let tuple_data = vec![
            TupleData::Text(b"42".to_vec().into()),
            TupleData::Text(b"t".to_vec().into()),
            TupleData::Text(br#"{"k": 1}"#.to_vec().into()),
        ];
        let row_size_bytes = calculate_tuple_bytes(&tuple_data);
        let pending_items =
            vec![PendingBatchItem::PgOutputChange(PendingPgOutputChange::new_single_group(
                pending_pgoutput_full_row_group(
                    &replicated_table_schema,
                    ChangeKind::Insert,
                    RowImage::New,
                    &tuple_data,
                    PendingPgOutputRowMeta::new(
                        PgLsn::from(99),
                        3,
                        tuple_bytes_to_size_hint(row_size_bytes),
                    ),
                )
                .unwrap(),
            ))];

        let batches = normalize_pending_batch_items(pending_items).unwrap();

        let [StreamBatch::Changes(change_set)] = batches.as_slice() else {
            panic!("expected one change batch");
        };
        assert_eq!(change_set.table_id, TableId::new(777));
        let [group] = change_set.groups.as_slice() else {
            panic!("expected one change group");
        };
        assert_eq!(group.change, ChangeKind::Insert);
        assert_eq!(group.row_image, RowImage::New);
        assert_eq!(group.commit_lsns.value(0), 99);
        assert_eq!(group.tx_ordinals.value(0), 3);
        assert_eq!(
            record_batch_to_table_rows(&group.rows.batch),
            vec![TableRow::new(vec![
                Cell::I64(42),
                Cell::Bool(true),
                Cell::String(r#"{"k": 1}"#.to_owned()),
            ])]
        );
    }

    #[test]
    fn normalizer_keeps_homogeneous_schema_versions_separate() {
        let schema_v1 = ReplicatedTableSchema::all(scalar_table_schema_with_snapshot(
            10,
            vec![column("id", Type::INT8, 1, true)],
        ));
        let schema_v2 = ReplicatedTableSchema::all(scalar_table_schema_with_snapshot(
            20,
            vec![column("id", Type::INT8, 1, true), column("name", Type::TEXT, 2, false)],
        ));
        let pending_items = vec![
            insert_event(schema_v1, 0, TableRow::new(vec![Cell::I64(1)])),
            insert_event(
                schema_v2,
                1,
                TableRow::new(vec![Cell::I64(2), Cell::String("two".to_owned())]),
            ),
        ];

        let batches = normalize_pending_batch_items(pending_items).unwrap();

        let [StreamBatch::Changes(change_set)] = batches.as_slice() else {
            panic!("expected one change batch");
        };
        assert_eq!(change_set.groups.len(), 2);
        assert_eq!(
            record_batch_to_table_rows(&change_set.groups[0].rows.batch),
            vec![TableRow::new(vec![Cell::I64(1)])]
        );
        assert_eq!(
            record_batch_to_table_rows(&change_set.groups[1].rows.batch),
            vec![TableRow::new(vec![Cell::I64(2), Cell::String("two".to_owned())])]
        );
    }

    #[test]
    fn normalizer_preserves_direct_pgoutput_projected_key_rows() {
        let replicated_table_schema = ReplicatedTableSchema::all(scalar_table_schema());
        let tuple_data = vec![
            TupleData::Text(b"42".to_vec().into()),
            TupleData::Text(b"t".to_vec().into()),
            TupleData::Text(br#"{"k": 1}"#.to_vec().into()),
        ];
        let row_size_bytes = calculate_tuple_bytes(&tuple_data);
        let pending_items =
            vec![PendingBatchItem::PgOutputChange(PendingPgOutputChange::new_single_group(
                pending_pgoutput_key_row_group(
                    &replicated_table_schema,
                    ChangeKind::Delete,
                    RowImage::Old { key_only: true },
                    &tuple_data,
                    PendingPgOutputRowMeta::new(
                        PgLsn::from(101),
                        5,
                        tuple_bytes_to_size_hint(row_size_bytes),
                    ),
                )
                .unwrap(),
            ))];

        let batches = normalize_pending_batch_items(pending_items).unwrap();

        let [StreamBatch::Changes(change_set)] = batches.as_slice() else {
            panic!("expected one change batch");
        };
        assert_eq!(change_set.table_id, TableId::new(777));
        let [group] = change_set.groups.as_slice() else {
            panic!("expected one change group");
        };
        assert_eq!(group.change, ChangeKind::Delete);
        assert_eq!(group.row_image, RowImage::Old { key_only: true });
        assert_eq!(group.commit_lsns.value(0), 101);
        assert_eq!(group.tx_ordinals.value(0), 5);
        assert_eq!(
            record_batch_to_table_rows(&group.rows.batch),
            vec![TableRow::new(vec![Cell::I64(42)])]
        );
    }
}
