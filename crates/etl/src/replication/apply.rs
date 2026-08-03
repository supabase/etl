//! Apply loop implementation for processing PostgreSQL logical replication
//! events.
//!
//! This module provides the core apply loop that processes replication events
//! from PostgreSQL and coordinates table synchronization. It uses a
//! [`WorkerContext`] enum to enable different behavior based on the worker type
//! (apply worker vs table sync worker) at various points in the replication
//! cycle.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use etl_config::shared::PipelineConfig;
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use postgres_replication::{
    protocol,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};
use tokio::{
    pin,
    sync::{Semaphore, mpsc, watch},
    task::JoinHandle,
    time::MissedTickBehavior,
};
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info, warn};

#[cfg(feature = "failpoints")]
use crate::failpoints::{STORE_REPLICATION_CHECKPOINT_FP, etl_fail_point_active_for_parameter};
use crate::{
    bail,
    data::SizeHint,
    destination::{
        ApplyLoopAsyncResultMetadata, CompletedWriteEventsResult, DestinationWriteStatus,
        PendingWriteEventsResult, PipelineDestination, WriteEventsDurability, WriteEventsResult,
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::{Event, RelationEvent},
    observability::{
        ACTION_LABEL, COMMAND_TAG_LABEL, CONFIRMATION_LABEL,
        ETL_APPLY_LOOP_EFFECTIVE_FLUSH_LAG_BYTES, ETL_APPLY_LOOP_END_TO_END_LAG_BYTES,
        ETL_APPLY_LOOP_FLUSH_LAG_BYTES, ETL_APPLY_LOOP_RECEIVED_LAG_BYTES,
        ETL_BATCH_ITEMS_DURABLE_DURATION_SECONDS, ETL_BATCH_ITEMS_DURABLE_WAIT_DURATION_SECONDS,
        ETL_BATCH_ITEMS_SEND_DURATION_SECONDS, ETL_DDL_SCHEMA_CHANGE_COLUMNS,
        ETL_DDL_SCHEMA_CHANGES_TOTAL, ETL_EVENTS_PROCESSED_TOTAL, ETL_EVENTS_RECEIVED_TOTAL,
        ETL_REPLICATION_MESSAGES_TOTAL, ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
        ETL_SCHEMA_CLEANUP_PRUNED_VERSIONS_TOTAL, ETL_SCHEMA_CLEANUP_TABLES_TOTAL,
        ETL_SCHEMA_CLEANUPS_TOTAL, ETL_TRANSACTION_DURATION_SECONDS, ETL_TRANSACTION_SIZE,
        ETL_TRANSACTIONS_TOTAL, OUTCOME_LABEL, WORKER_TYPE_LABEL,
    },
    pipeline::PipelineId,
    postgres::{
        OutOfBandSourcePool, ReplicationMessageStream, StatusUpdateType,
        client::{PgReplicationClient, PostgresConnectionUpdate},
        codec::{
            DDL_MESSAGE_PREFIX, SchemaChangeMessage, delete_message_payload_bytes,
            insert_message_payload_bytes, parse_event_from_begin_message,
            parse_event_from_commit_message, parse_event_from_delete_message,
            parse_event_from_insert_message, parse_event_from_truncate_message,
            parse_event_from_update_message, parse_replica_identity_column_names,
            parse_replicated_column_names, schema_snapshot_id_from_message,
            update_message_payload_bytes,
        },
    },
    replication::{
        TableDecodingState, WorkerType,
        state::{TableState, TableStateType},
    },
    runtime::{
        BatchBudgetController, CachedBatchBudget, MemoryMonitor, TableSyncWorker,
        TableSyncWorkerPool, TableSyncWorkerState,
        concurrency::{
            BackpressureStream, ShutdownResult, ShutdownRx, apply_worker_apply_stream_id,
            table_sync_worker_apply_stream_id,
        },
    },
    schema::{
        IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId, TableSchema,
    },
    source_payload_metadata::StreamingPayloadMetadata,
    store::{PipelineStore, SchemaStore, SharedStateStore, StateStore, TableSchemaRetention},
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
/// Maximum accepted-but-not-durable streaming writes tracked per apply loop.
const MAX_PENDING_DURABLE_DISPATCHES: usize = 1024;
/// Maximum number of table schema cleanups buffered per apply loop.
///
/// Each queue entry contains one table identifier and one frozen retention
/// boundary. A capacity of 1024 accommodates large bursts of relation messages
/// while keeping queue memory bounded. Queueing is non-blocking, so additional
/// candidates remain pending in the apply loop and are retried after a later
/// durable flush result.
const SCHEMA_CLEANUP_QUEUE_TABLE_CAPACITY: usize = 1024;

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
    /// State store for tracking table state and persisted checkpoints.
    pub(crate) store: S,
    /// Destination where replicated data is written.
    pub(crate) destination: D,
    /// Shared pool for out-of-band source database queries.
    pub(crate) out_of_band_source_pool: OutOfBandSourcePool,
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
    /// State store for persisting replication checkpoints.
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

/// Tracks the LSNs observed by the logical replication apply loop.
///
/// These values describe in-memory loop progress. They are distinct from the
/// checkpoint selected for PostgreSQL feedback and from the checkpoint stored
/// durably for restart.
#[derive(Debug, Clone, Copy)]
struct ReplicationProgress {
    /// The highest LSN received from PostgreSQL so far.
    last_received_lsn: PgLsn,
    /// The highest LSN whose destination write has completed durably.
    last_flush_lsn: PgLsn,
}

impl ReplicationProgress {
    /// Creates replication progress initialized to the given LSN.
    fn new(initial_lsn: PgLsn) -> Self {
        Self { last_received_lsn: initial_lsn, last_flush_lsn: initial_lsn }
    }

    /// Returns the highest LSN received from PostgreSQL so far.
    fn last_received_lsn(&self) -> PgLsn {
        self.last_received_lsn
    }

    /// Returns the highest LSN whose destination write completed durably.
    fn last_flush_lsn(&self) -> PgLsn {
        self.last_flush_lsn
    }

    /// Updates the last received LSN if it advanced.
    fn update_last_received_lsn(&mut self, lsn: PgLsn) {
        self.last_received_lsn = self.last_received_lsn.max(lsn);

        debug_assert!(self.last_received_lsn >= self.last_flush_lsn);
    }

    /// Updates the last flush LSN if it advanced.
    fn update_last_flush_lsn(&mut self, lsn: PgLsn) {
        self.last_flush_lsn = self.last_flush_lsn.max(lsn);

        debug_assert!(self.last_received_lsn >= self.last_flush_lsn);
    }
}

/// Tracks replication lag measurements shared with the sampler task.
#[derive(Debug, Clone)]
struct ReplicationLagMetrics {
    /// Shared atomic LSN positions used for lag gauges.
    inner: Arc<ReplicationLagMetricsInner>,
}

/// Atomic replication lag positions shared by the apply loop and sampler task.
#[derive(Debug)]
struct ReplicationLagMetricsInner {
    /// Last source WAL LSN observed by the out-of-band sampler.
    last_source_current_lsn: AtomicU64,
    /// The highest LSN received from PostgreSQL so far.
    last_received_lsn: AtomicU64,
    /// The highest LSN whose destination write completed durably.
    last_flush_lsn: AtomicU64,
    /// The highest safe frontier selected from received-or-flushed progress.
    ///
    /// PostgreSQL feedback uses this quiescent-or-flushed selection rule.
    /// Durable ETL checkpoints advance only after destination flushes.
    last_checkpoint_lsn: AtomicU64,
}

impl ReplicationLagMetrics {
    /// Creates replication lag metrics initialized to the given LSN.
    fn new(initial_lsn: PgLsn) -> Self {
        let initial_lsn = u64::from(initial_lsn);

        Self {
            inner: Arc::new(ReplicationLagMetricsInner {
                last_source_current_lsn: AtomicU64::new(initial_lsn),
                last_received_lsn: AtomicU64::new(initial_lsn),
                last_flush_lsn: AtomicU64::new(initial_lsn),
                last_checkpoint_lsn: AtomicU64::new(initial_lsn),
            }),
        }
    }

    /// Updates the last source current LSN if it advanced.
    fn update_last_source_current_lsn(&self, lsn: PgLsn) {
        Self::update_lsn(&self.inner.last_source_current_lsn, lsn);
    }

    /// Updates lag metric positions derived from apply-loop progress.
    fn update_from_progress(&self, progress: ReplicationProgress, checkpoint_lsn: PgLsn) {
        Self::update_lsn(&self.inner.last_received_lsn, progress.last_received_lsn());
        Self::update_lsn(&self.inner.last_flush_lsn, progress.last_flush_lsn());
        Self::update_lsn(&self.inner.last_checkpoint_lsn, checkpoint_lsn);
    }

    /// Emits lag gauges from the current atomic progress positions.
    fn emit_lag_metrics(&self, worker_type: WorkerType) {
        let last_source_current_lsn = self.inner.last_source_current_lsn.load(Ordering::Relaxed);
        let last_received_lsn = self.inner.last_received_lsn.load(Ordering::Relaxed);
        let last_flush_lsn = self.inner.last_flush_lsn.load(Ordering::Relaxed);
        let last_checkpoint_lsn = self.inner.last_checkpoint_lsn.load(Ordering::Relaxed);

        let worker_type = worker_type.as_str();

        gauge!(
            ETL_APPLY_LOOP_RECEIVED_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_source_current_lsn.saturating_sub(last_received_lsn) as f64);
        gauge!(
            ETL_APPLY_LOOP_EFFECTIVE_FLUSH_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_received_lsn.saturating_sub(last_checkpoint_lsn) as f64);
        gauge!(
            ETL_APPLY_LOOP_FLUSH_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_received_lsn.saturating_sub(last_flush_lsn) as f64);
        gauge!(
            ETL_APPLY_LOOP_END_TO_END_LAG_BYTES,
            WORKER_TYPE_LABEL => worker_type
        )
        .set(last_source_current_lsn.saturating_sub(last_checkpoint_lsn) as f64);
    }

    /// Updates a stored LSN monotonically.
    fn update_lsn(stored_lsn: &AtomicU64, lsn: PgLsn) {
        let new_lsn = u64::from(lsn);
        let _ = stored_lsn.try_update(Ordering::Relaxed, Ordering::Relaxed, |current_lsn| {
            (new_lsn > current_lsn).then_some(new_lsn)
        });
    }
}

/// Result returned from [`ApplyLoop::handle_replication_message`] and related
/// functions.
#[derive(Debug, Default)]
struct HandleMessageResult {
    /// The event converted from the replication message.
    event: Option<Event>,
    /// PostgreSQL source metadata represented by the returned event.
    streaming_payload_metadata: StreamingPayloadMetadata,
    /// Set to a commit message's end_lsn value, [`None`] otherwise.
    end_lsn: Option<PgLsn>,
    /// Set when a batch should be ended earlier than the normal batching
    /// parameters.
    end_batch: bool,
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

    /// Creates a result containing a row event and its source metadata.
    fn return_row_event(
        event: Event,
        streaming_payload_metadata: StreamingPayloadMetadata,
    ) -> Self {
        Self { event: Some(event), streaming_payload_metadata, ..Default::default() }
    }
}

/// Immutable retention boundary for one asynchronous table schema cleanup.
///
/// The apply loop builds this request immediately after persisting
/// commit-boundary progress and reading destination metadata. The background
/// worker must use this frozen boundary rather than reloading newer state:
/// arbitrary queue delay can then only make the request conservative.
///
/// Concurrent schema insertion is also safe: ordered replication cannot later
/// introduce a schema at or below persisted progress, and pruning always
/// preserves every snapshot newer than the frozen boundary.
///
/// Pruning is idempotent and does not rely on request order. Each boundary
/// preserves the greatest schema snapshot at or below it and every newer
/// snapshot, so replaying a request—or processing an older request after a
/// newer one—cannot remove a schema retained by the newer boundary.
#[derive(Debug)]
struct SchemaCleanupRequest {
    /// Table whose obsolete schema versions may be pruned.
    table_id: TableId,
    /// Retention boundary captured at the durable flush result.
    retention: TableSchemaRetention,
}

/// Background tasks owned by an apply loop invocation.
#[derive(Debug)]
struct ApplyLoopTasks {
    /// Sender for serialized background schema cleanup requests.
    schema_cleanup_tx: Option<mpsc::Sender<SchemaCleanupRequest>>,
    /// Background worker that serially processes schema cleanup requests.
    schema_cleanup_worker_task: JoinHandle<()>,
    /// Background replication lag sampler task owned by this apply loop.
    replication_lag_sampler_task: JoinHandle<()>,
}

impl ApplyLoopTasks {
    /// Creates task ownership and starts background workers.
    fn start<S>(
        schema_store: S,
        out_of_band_source_pool: OutOfBandSourcePool,
        replication_lag_metrics: ReplicationLagMetrics,
        worker_type: WorkerType,
        replication_lag_refresh_interval: Duration,
    ) -> Self
    where
        S: SchemaStore + Send + Sync + 'static,
    {
        let (schema_cleanup_tx, schema_cleanup_rx) =
            mpsc::channel(SCHEMA_CLEANUP_QUEUE_TABLE_CAPACITY);
        let schema_cleanup_worker_task =
            Self::spawn_schema_cleanup_worker(schema_store, schema_cleanup_rx, worker_type);

        let replication_lag_sampler_task = Self::spawn_replication_lag_sampler(
            out_of_band_source_pool,
            replication_lag_metrics,
            worker_type,
            replication_lag_refresh_interval,
        );

        Self {
            schema_cleanup_tx: Some(schema_cleanup_tx),
            schema_cleanup_worker_task,
            replication_lag_sampler_task,
        }
    }

    /// Tries to queue a schema cleanup request for one table.
    ///
    /// Returns `false` when the bounded queue is full or the background worker
    /// has stopped. This method never waits for queue capacity.
    fn try_queue_schema_cleanup(&self, table_id: TableId, retention: TableSchemaRetention) -> bool {
        let Some(schema_cleanup_tx) = &self.schema_cleanup_tx else {
            return false;
        };

        let request = SchemaCleanupRequest { table_id, retention };
        match schema_cleanup_tx.try_send(request) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => false,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("schema cleanup worker stopped before accepting cleanup request");

                false
            }
        }
    }

    /// Starts the worker that serially prunes requested table schema versions.
    fn spawn_schema_cleanup_worker<S>(
        schema_store: S,
        mut schema_cleanup_rx: mpsc::Receiver<SchemaCleanupRequest>,
        worker_type: WorkerType,
    ) -> JoinHandle<()>
    where
        S: SchemaStore + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            while let Some(request) = schema_cleanup_rx.recv().await {
                // Coalesce up to one bounded batch of available requests to
                // retain batched store cleanup without making the apply loop
                // wait for it.
                let mut table_schema_retentions =
                    HashMap::with_capacity(schema_cleanup_rx.len().saturating_add(1));
                table_schema_retentions.insert(request.table_id, request.retention);

                for _ in 1..SCHEMA_CLEANUP_QUEUE_TABLE_CAPACITY {
                    let Ok(request) = schema_cleanup_rx.try_recv() else {
                        break;
                    };

                    table_schema_retentions.insert(request.table_id, request.retention);
                }

                let table_count = table_schema_retentions.len() as u64;
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
                        // Cleanup is best-effort. Do not block later requests behind a
                        // permanently failing one: a later relation for the table,
                        // including its first relation after restart, will enqueue a
                        // fresh cleanup attempt.
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
            }
        })
    }

    /// Aborts and joins the replication lag sampler task.
    async fn handle_replication_lag_sampler_task_result(&mut self) {
        // We abort the task, so that awaiting on the handle is as quick as possible.
        //
        // It's fine to abort this task midway, since it's not going to affect
        // consistency.
        self.replication_lag_sampler_task.abort();

        if let Err(err) = (&mut self.replication_lag_sampler_task).await
            && !err.is_cancelled()
        {
            warn!(
                error = %err,
                "replication lag sampler task failed before completing"
            );
        }
    }

    async fn handle_schema_cleanup_task_result(&mut self, worker_type: WorkerType) {
        // Closing the sender lets the cleanup worker finish every accepted
        // request before the apply loop returns.
        //
        // We don't want to call abort on the task, just to prevent possible errors from
        // partial completion around await points. In practice, it could be suspendable
        // midway, but it's safer to avoid it, also since the pruning of schemas
        // should be relatively quick.
        self.schema_cleanup_tx.take();

        if let Err(err) = (&mut self.schema_cleanup_worker_task).await {
            counter!(
                ETL_SCHEMA_CLEANUP_ERRORS_TOTAL,
                WORKER_TYPE_LABEL => worker_type.as_str(),
            )
            .increment(1);

            error!(
                %worker_type,
                error = %err,
                "schema cleanup worker task failed before completing"
            );
        }
    }

    /// Stops and joins all owned background tasks.
    async fn teardown(&mut self, worker_type: WorkerType) {
        self.handle_replication_lag_sampler_task_result().await;
        self.handle_schema_cleanup_task_result(worker_type).await;
    }

    /// Starts the replication lag sampler for an apply loop.
    fn spawn_replication_lag_sampler(
        out_of_band_source_pool: OutOfBandSourcePool,
        replication_lag_metrics: ReplicationLagMetrics,
        worker_type: WorkerType,
        replication_lag_refresh_interval: Duration,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self::run_replication_lag_sampler(
                out_of_band_source_pool,
                replication_lag_metrics,
                worker_type,
                replication_lag_refresh_interval,
            )
            .await;
        })
    }

    /// Runs the best-effort replication lag sampler.
    async fn run_replication_lag_sampler(
        out_of_band_source_pool: OutOfBandSourcePool,
        replication_lag_metrics: ReplicationLagMetrics,
        worker_type: WorkerType,
        replication_lag_refresh_interval: Duration,
    ) {
        let mut interval = tokio::time::interval(replication_lag_refresh_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            match out_of_band_source_pool.get_current_wal_lsn().await {
                Ok(source_current_lsn) => {
                    replication_lag_metrics.update_last_source_current_lsn(source_current_lsn);
                    replication_lag_metrics.emit_lag_metrics(worker_type);
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        "replication lag sampler failed to poll source database"
                    );
                }
            }
        }
    }
}

/// A buffered batch of events waiting to be sent to the destination.
#[derive(Debug, Default)]
struct EventBatch {
    /// Events accumulated in the batch.
    events: Vec<Event>,
    /// Tables whose schemas are communicated by relation events in the batch.
    relation_table_ids: HashSet<TableId>,
    /// Decoded in-memory size estimate used only to decide when to flush.
    size_hint_bytes: usize,
    /// PostgreSQL tuple bytes used for source metrics and usage accounting.
    ///
    /// These are independent from the decoded [`SizeHint`] used to determine
    /// when the batch is dispatched.
    streaming_payload_metadata: StreamingPayloadMetadata,
}

impl EventBatch {
    /// Creates an empty event batch with the specified event capacity.
    fn with_capacity(capacity: usize) -> Self {
        Self {
            events: Vec::with_capacity(capacity),
            relation_table_ids: HashSet::new(),
            size_hint_bytes: 0,
            streaming_payload_metadata: StreamingPayloadMetadata::default(),
        }
    }

    /// Adds an event and its source payload metadata to the batch.
    fn push(&mut self, event: Event, streaming_payload_metadata: StreamingPayloadMetadata) {
        if let Event::Relation(relation) = &event {
            self.relation_table_ids.insert(relation.replicated_table_schema.id());
        }

        self.size_hint_bytes = self.size_hint_bytes.saturating_add(event.size_hint());
        self.streaming_payload_metadata.merge(streaming_payload_metadata);
        self.events.push(event);
    }

    /// Returns the number of events in the batch.
    fn len(&self) -> usize {
        self.events.len()
    }

    /// Returns whether the batch contains no events.
    fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Returns the decoded in-memory size estimate for the batch.
    fn size_hint_bytes(&self) -> usize {
        self.size_hint_bytes
    }

    /// Takes the current batch and leaves one with the same event capacity.
    fn take(&mut self) -> Self {
        debug_assert!(!self.is_empty());

        // Steady-state streaming usually produces similarly sized batches, so retain
        // enough event capacity for the next batch.
        let replacement_capacity = self.len();
        std::mem::replace(self, Self::with_capacity(replacement_capacity))
    }

    /// Splits the batch into its events and dispatch metadata.
    fn into_parts(self) -> (Vec<Event>, usize, HashSet<TableId>, StreamingPayloadMetadata) {
        let event_count = self.events.len();

        (self.events, event_count, self.relation_table_ids, self.streaming_payload_metadata)
    }
}

/// Schema lookup required to materialize a relation message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationSchemaSelection {
    /// Resolve an exact schema snapshot established by connection-local state.
    Exact(SnapshotId),
    /// Resolve the newest stored schema at or before an ownership-safe bound.
    AtOrBefore(SnapshotId),
}

/// Timing anchors for one accepted-but-not-durable destination write.
#[derive(Debug, Clone, Copy)]
struct PendingDurableDispatch {
    /// Instant at which the write was handed off to the destination.
    dispatched_at: Instant,
    /// Instant at which the apply loop processed the accepted result.
    accepted_at: Instant,
}

/// Mutable runtime state that evolves throughout the apply loop.
#[derive(Debug)]
struct ApplyLoopState {
    /// The highest commit end LSN that should be attached to the next
    /// destination write.
    ///
    /// This usually comes from the [`end_lsn`] field of a [`Commit`] message.
    /// If a destination accepts a write without making it durable, the write's
    /// commit end LSN is re-attached here so a later durable write can advance
    /// through it.
    last_commit_end_lsn: Option<PgLsn>,
    /// Timing anchors of accepted-but-not-durable destination writes.
    ///
    /// A durable write result confirms all earlier accepted writes in the same
    /// ordered apply-loop stream, so their durable durations are recorded once
    /// the next durable result arrives. The queue is capped at
    /// [`MAX_PENDING_DURABLE_DISPATCHES`]. Timing entries are metric-only
    /// state, so overflow skips new entries instead of affecting replication.
    pending_durable_dispatches: VecDeque<PendingDurableDispatch>,
    /// Relation tables not yet covered by persisted commit-boundary progress.
    ///
    /// This includes relations from `Accepted` writes and from durable
    /// mid-transaction writes that carry no commit end LSN. A later durable
    /// commit-bearing result covers them cumulatively and makes their tables
    /// candidates for obsolete schema cleanup.
    pending_relation_table_ids: HashSet<TableId>,
    /// The LSN of the commit WAL entry of the transaction that is currently
    /// being processed.
    remote_final_lsn: Option<PgLsn>,
    /// The current replication progress tracking received and flushed LSN
    /// positions.
    replication_progress: ReplicationProgress,
    /// Shared replication lag metrics derived from apply-loop progress.
    replication_lag_metrics: ReplicationLagMetrics,
    /// Events and associated batching information waiting for dispatch.
    event_batch: EventBatch,
    /// Instant from when a transaction began.
    current_tx_begin_ts: Option<Instant>,
    /// Number of events observed in the current transaction (excluding
    /// BEGIN/COMMIT).
    current_tx_events: u64,
    /// Next zero-based ordinal to assign to transaction-scoped events.
    next_tx_ordinal: u64,
    /// Whether the loop is draining buffered destination work for shutdown.
    draining_for_shutdown: bool,
    /// The deadline by which the current batch must be flushed.
    flush_deadline: Option<Instant>,
    /// The deadline for the next proactive keep alive status update.
    keep_alive_deadline: Instant,
    /// Destination write result waiting to advance the last flush LSN.
    pending_flush_result: Option<PendingWriteEventsResult>,
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
    /// Fallback snapshot used before a table establishes connection-local
    /// protocol state or receives stored table decoding state.
    ///
    /// This is seeded from the worker start LSN so a first `RELATION` message
    /// can always resolve the latest schema version whose snapshot is less
    /// than or equal to the worker's start point.
    bootstrap_snapshot_id: SnapshotId,
    /// Replication slot name used by this loop.
    slot_name: String,
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial replication progress.
    fn new(
        replication_progress: ReplicationProgress,
        replication_lag_metrics: ReplicationLagMetrics,
        keep_alive_deadline_duration: Duration,
        bootstrap_snapshot_id: SnapshotId,
        slot_name: String,
    ) -> Self {
        Self {
            last_commit_end_lsn: None,
            pending_durable_dispatches: VecDeque::new(),
            pending_relation_table_ids: HashSet::new(),
            remote_final_lsn: None,
            replication_progress,
            replication_lag_metrics,
            event_batch: EventBatch::default(),
            current_tx_begin_ts: None,
            current_tx_events: 0,
            next_tx_ordinal: 0,
            draining_for_shutdown: false,
            flush_deadline: None,
            keep_alive_deadline: Instant::now() + keep_alive_deadline_duration,
            pending_flush_result: None,
            exit_intent: None,
            processing_paused: false,
            bootstrap_snapshot_id,
            slot_name,
        }
    }

    /// Returns the bootstrap snapshot used before a table has local decoding
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

    /// Updates the last received LSN and snapshots replication lag metrics.
    fn update_last_received_lsn(&mut self, lsn: PgLsn) {
        self.replication_progress.update_last_received_lsn(lsn);
        self.update_replication_lag_metrics_from_progress();
    }

    /// Updates the last destination flush LSN and snapshots lag metrics.
    fn update_last_flush_lsn(&mut self, lsn: PgLsn) {
        self.replication_progress.update_last_flush_lsn(lsn);
        self.update_replication_lag_metrics_from_progress();
    }

    /// Snapshots apply-loop progress into the replication lag metrics.
    fn update_replication_lag_metrics_from_progress(&self) {
        self.replication_lag_metrics
            .update_from_progress(self.replication_progress, self.checkpoint_lsn());
    }

    /// Returns the last received LSN that should be reported as written to the
    /// PostgreSQL server.
    fn last_received_lsn(&self) -> PgLsn {
        self.replication_progress.last_received_lsn()
    }

    /// Returns `true` if the apply loop is quiescent.
    ///
    /// A quiescent loop has no open transaction, buffered or in-flight
    /// destination work, or carried durability obligation.
    ///
    /// A carried commit end LSN from an accepted-but-not-durable write keeps
    /// the loop non-quiescent for status updates. This is conservative:
    /// PostgreSQL feedback keeps reporting the last flush LSN until a later
    /// durable write proves the carried LSN safe.
    ///
    /// Ordinarily, if no later batch arrives, durability for the accepted
    /// write is deferred until replay or the next batch. A terminal table-sync
    /// catchup is the exception: once keepalive progress reaches its target,
    /// ETL dispatches an empty required-durability write through the same
    /// pending-result path.
    fn is_quiescent(&self) -> bool {
        !self.handling_transaction()
            && !self.has_unresolved_batch_work()
            && self.last_commit_end_lsn.is_none()
    }

    /// Returns the checkpoint LSN to report to PostgreSQL.
    ///
    /// When the loop is quiescent, every emitted message has been fully
    /// handled, so the last received LSN is a safe replay frontier even if
    /// no destination write occurred. A logical keepalive can cross an open
    /// transaction still buffered by PostgreSQL; the slot retains its
    /// earlier `restart_lsn` and rebuilds transactions that commit after
    /// the confirmed checkpoint. While any client-side transaction, batch,
    /// or destination write is unresolved, the checkpoint remains at the
    /// last completed destination flush.
    ///
    /// Starting new work after a quiescent checkpoint can make this computed
    /// value lower than a value already reported on the connection. The
    /// replication stream keeps PostgreSQL feedback monotonic, so the
    /// wire-level checkpoint never moves backward.
    fn checkpoint_lsn(&self) -> PgLsn {
        if self.is_quiescent() {
            self.replication_progress.last_received_lsn()
        } else {
            self.replication_progress.last_flush_lsn()
        }
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
        !self.event_batch.is_empty()
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
        debug_assert!(self.has_pending_flush_result());
        debug_assert!(self.has_pending_batch());

        self.processing_paused = true;
    }

    /// Resumes processing by clearing the existing pending flush result and
    /// enabling processing.
    fn resume_processing(&mut self) -> bool {
        debug_assert!(self.has_pending_flush_result());

        let prev_processing_paused = std::mem::replace(&mut self.processing_paused, false);
        self.pending_flush_result = None;

        debug_assert!(!prev_processing_paused || self.has_pending_batch());

        prev_processing_paused
    }

    /// Returns the final result requested by this loop, if any.
    fn exit_result(&self) -> Option<ApplyLoopResult> {
        self.exit_intent.map(ExitIntent::to_result)
    }

    /// Returns `true` when active intake has stopped to drain shutdown work.
    fn is_draining_for_shutdown(&self) -> bool {
        self.draining_for_shutdown
    }

    /// Stops active intake and starts draining buffered shutdown work.
    fn start_draining_for_shutdown(&mut self) {
        self.draining_for_shutdown = true;
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
    /// Connection-local per-table protocol state used to decode relation and
    /// row messages.
    table_decoding_states: HashMap<TableId, TableDecodingState>,
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
    /// Background tasks owned by this apply loop.
    tasks: ApplyLoopTasks,
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
        out_of_band_source_pool: OutOfBandSourcePool,
        worker_context: WorkerContext<S, D>,
        shutdown_rx: ShutdownRx,
        memory_monitor: MemoryMonitor,
        batch_budget: BatchBudgetController,
        initial_replicated_table_schema: Option<ReplicatedTableSchema>,
    ) -> EtlResult<ApplyLoopResult> {
        info!(
            worker_type = %worker_context.worker_type(),
            %start_lsn,
            "starting apply loop",
        );

        let worker_type = worker_context.worker_type();
        let wal_sender_timeout_result = replication_client.get_wal_sender_timeout().await;
        let keep_alive_deadline_duration = match wal_sender_timeout_result {
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

        let replication_progress = ReplicationProgress::new(start_lsn);
        let replication_lag_metrics = ReplicationLagMetrics::new(start_lsn);

        let slot_name: String = worker_type.build_etl_replication_slot(pipeline_id).try_into()?;

        let replication_lag_refresh_interval =
            Duration::from_millis(config.replication_lag_refresh_interval_ms);
        let tasks = ApplyLoopTasks::start(
            schema_store.clone(),
            out_of_band_source_pool,
            replication_lag_metrics.clone(),
            worker_type,
            replication_lag_refresh_interval,
        );

        let state = ApplyLoopState::new(
            replication_progress,
            replication_lag_metrics,
            keep_alive_deadline_duration,
            bootstrap_snapshot_id,
            slot_name,
        );

        let mut table_decoding_states = HashMap::new();
        if let Some(replicated_table_schema) = initial_replicated_table_schema {
            // Only the table-sync worker supplies this schema. It seeds catchup
            // with the same snapshot `0/0` and publication/identity masks used
            // by the initial copy before later relation messages replace it.
            table_decoding_states.insert(
                replicated_table_schema.id(),
                TableDecodingState::WithSchema(replicated_table_schema),
            );
        }

        let mut apply_loop = Self {
            config: Arc::clone(&config),
            schema_store,
            destination,
            table_decoding_states,
            shutdown_rx,
            worker_context,
            memory_monitor,
            cached_batch_budget: batch_budget.cached(),
            max_batch_fill_duration: Duration::from_millis(config.batch.max_fill_ms),
            keep_alive_deadline_duration,
            tasks,
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

        self.tasks.teardown(self.worker_context.worker_type()).await;

        result
    }

    /// Runs the main event processing loop.
    async fn run(
        &mut self,
        replication_client: &PgReplicationClient,
        start_lsn: PgLsn,
    ) -> EtlResult<ApplyLoopResult> {
        let logical_replication_stream = replication_client
            .start_logical_replication(
                &self.config.publication_name,
                self.state.slot_name(),
                start_lsn,
            )
            .await?;

        let replication_message_stream = ReplicationMessageStream::wrap(logical_replication_stream);
        let replication_message_stream = BackpressureStream::wrap(
            replication_message_stream,
            self.worker_context.apply_stream_id(),
            self.memory_monitor.subscribe(),
        );
        pin!(replication_message_stream);
        let mut connection_updates_rx = replication_client.connection_updates_rx();

        loop {
            let iteration_result = if self.state.is_draining_for_shutdown() {
                self.run_draining_shutdown_iteration(
                    replication_message_stream.as_mut(),
                    &mut connection_updates_rx,
                )
                .await
            } else {
                self.run_active_iteration(
                    replication_message_stream.as_mut(),
                    replication_client,
                    &mut connection_updates_rx,
                )
                .await
            };

            // If we have a result from the apply loop, we should stop the loop.
            if let Some(result) = iteration_result? {
                return Ok(result);
            }
        }
    }

    /// Runs one normal apply-loop iteration while the worker is still actively
    /// processing.
    ///
    /// Each active iteration first advances quiescent table-sync coordination
    /// before it can read more WAL. This keeps restarted apply loops from
    /// streaming while a table-sync worker is already in `Catchup`.
    ///
    /// After that preflight, this keeps the priority order explicit:
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
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
        replication_client: &PgReplicationClient,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
    ) -> EtlResult<Option<ApplyLoopResult>> {
        // Process quiescent coordination before waiting for another signal. The
        // next loop iteration observes progress made by whichever branch runs below.
        self.maybe_process_syncing_tables_when_quiescent().await?;

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
                self.handle_shutdown_signal(replication_message_stream.as_mut()).await?;
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
            maybe_message = replication_message_stream.next(), if self.state.can_process_messages() => {
                self.handle_stream_message(
                    replication_message_stream.as_mut(),
                    maybe_message,
                    replication_client,
                )
                .await?;
            }

            // PRIORITY 6: Emit a periodic status update once the computed keep alive deadline
            // expires. This intentionally resends the same checkpoint LSN so PostgreSQL keeps
            // the standby connection open during long stalls, including cases where the loop is
            // paused behind an in-flight flush and therefore not making visible progress yet. This
            // is only a fallback path: most status updates should still be triggered by incoming
            // PostgreSQL primary keep alive messages during normal operation.
            _ = Self::wait_for_keep_alive_deadline(self.state.keep_alive_deadline) => {
                self.send_status_update(
                    replication_message_stream.as_mut(),
                    true,
                    StatusUpdateType::PeriodicKeepAlive,
                )
                .await?;

                self.state
                    .reset_keep_alive_deadline(self.keep_alive_deadline_duration);
            }
        }

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
    /// Shutdown drain does not start new quiescent table-sync coordination. If
    /// permanent completion already started a required-durability barrier,
    /// the result handler completes the table-sync handoff before this drain
    /// is allowed to return.
    ///
    /// A write that already resolved as [`DestinationWriteStatus::Accepted`] is
    /// no longer pending work. If no later write makes it durable, the final
    /// status update stays at the last durable flush LSN and replay handles it
    /// after restart.
    async fn run_draining_shutdown_iteration(
        &mut self,
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
        connection_updates_rx: &mut watch::Receiver<PostgresConnectionUpdate>,
    ) -> EtlResult<Option<ApplyLoopResult>> {
        // If we are done with unresolved work, we can finish the shutdown.
        if !self.state.has_unresolved_batch_work() {
            self.send_shutdown_flush_status_update(replication_message_stream.as_mut()).await?;

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
                    replication_message_stream.as_mut(),
                    true,
                    StatusUpdateType::PeriodicKeepAlive,
                )
                .await?;

                self.state
                    .reset_keep_alive_deadline(self.keep_alive_deadline_duration);
            }
        }

        // If we are done with unresolved work, we can finish the shutdown.
        if !self.state.has_unresolved_batch_work() {
            self.send_shutdown_flush_status_update(replication_message_stream.as_mut()).await?;

            return Ok(Some(self.finish_shutdown()));
        }

        // If the batch work is not completed, we return `None` to continue the draining
        // in the next iteration.
        Ok(None)
    }

    /// Returns the final loop result after all buffered destination work has
    /// resolved.
    fn finish_shutdown(&self) -> ApplyLoopResult {
        debug_assert!(!self.state.has_unresolved_batch_work());

        // We try to honor the existing exit result, otherwise we just mark it as
        // `Paused` since shutting down is effectively pausing a loop.
        self.state.exit_result().unwrap_or(ApplyLoopResult::Paused)
    }

    /// Returns the final loop result for the active state if all exit barriers
    /// have been resolved.
    fn try_finish_active_iteration(&self) -> Option<ApplyLoopResult> {
        if self.state.is_draining_for_shutdown() || self.state.has_unresolved_batch_work() {
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
    /// shutdown status update. A write that has already completed as
    /// [`DestinationWriteStatus::Accepted`] is not waited on again; without a
    /// later durable write, the final status update remains at the last durable
    /// flush LSN. Otherwise, the loop sends that update immediately and exits.
    ///
    /// Note: the shutdown system is best-effort. Graceful shutdown may not
    /// complete if we are blocked on non-interruptible code. It is the
    /// responsibility of the caller to forcefully kill the process if shutdown
    /// does not complete within an acceptable timeframe.
    async fn handle_shutdown_signal(
        &mut self,
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
    ) -> EtlResult<()> {
        let worker_type = self.worker_context.worker_type();

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

            self.state.start_draining_for_shutdown();

            return Ok(());
        }

        info!(
            %worker_type,
            "shutdown signal received, no unresolved work left, sending final status update",
        );

        self.send_shutdown_flush_status_update(replication_message_stream.as_mut()).await
    }

    /// Sends the final shutdown status update to let Postgres advance its
    /// replication state once more.
    ///
    /// The update uses the loop's [`ApplyLoopState::checkpoint_lsn`], which may
    /// select received progress while quiescent or flushed progress while work
    /// is unresolved.
    async fn send_shutdown_flush_status_update(
        &mut self,
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
    ) -> EtlResult<()> {
        self.send_status_update(
            replication_message_stream.as_mut(),
            true,
            StatusUpdateType::ShutdownFlush,
        )
        .await?;

        Ok(())
    }

    /// Sends a status update to PostgreSQL using the current write and flush
    /// positions.
    ///
    /// Some callers intentionally resend the same checkpoint LSN with
    /// `force = true`. Those updates are about keeping the replication
    /// connection alive while the system is idle, not about advertising
    /// newly flushed progress. Keepalive replies from the main replication
    /// stream and the final shutdown update still use this same helper.
    async fn send_status_update(
        &mut self,
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
        force: bool,
        status_update_type: StatusUpdateType,
    ) -> EtlResult<()> {
        replication_message_stream
            .as_mut()
            .stream_mut()
            .send_status_update(
                self.state.last_received_lsn(),
                self.state.checkpoint_lsn(),
                force,
                status_update_type,
            )
            .await?;

        Ok(())
    }

    /// Tries to queue best-effort cleanup for relation tables covered by
    /// durable progress.
    ///
    /// The persisted checkpoint row is deliberately reloaded here instead of
    /// using the apply loop's in-memory flush position. Cleanup removes replay
    /// state, so its boundary must survive a crash independently of this
    /// process.
    ///
    /// Pending candidates remain in [`ApplyLoopState`] when a transient failure
    /// prevents evaluation or queueing, so the next durable result retries
    /// them. They are cleared only after successful evaluation and, when
    /// needed, successful queueing.
    async fn try_queue_schema_cleanup(&mut self) {
        if self.state.pending_relation_table_ids.is_empty() {
            return;
        }

        let worker_type = self.worker_context.worker_type();
        let persisted_checkpoint_lsn = match self
            .schema_store
            .get_replication_checkpoint(worker_type)
            .await
        {
            Ok(Some(persisted_checkpoint_lsn)) => persisted_checkpoint_lsn,
            Ok(None) => {
                debug!(
                    %worker_type,
                    "skipping schema cleanup because a persisted replication checkpoint is not available"
                );

                return;
            }
            Err(err) => {
                warn!(
                    %worker_type,
                    error = %err,
                    "skipping schema cleanup because the persisted replication checkpoint could not be loaded"
                );

                return;
            }
        };

        // We get the table retention boundaries that this apply loop instance
        // can safely prune up to. The map is frozen before the request is
        // queued, so any concurrent progress can only make this cleanup
        // conservative.
        let table_schema_retentions =
            match self.get_table_schema_retentions(persisted_checkpoint_lsn).await {
                Ok(table_schema_retentions) => table_schema_retentions,
                Err(err) => {
                    error!(
                        %worker_type,
                        error = %err,
                        "failed to determine schema cleanup ownership"
                    );

                    return;
                }
            };

        // If there are no tables to try to prune, we don't want to attempt it.
        if table_schema_retentions.is_empty() {
            self.state.pending_relation_table_ids.clear();

            return;
        }

        // Queue each frozen table boundary without waiting for capacity. The
        // cleanup worker coalesces available entries into batched store calls.
        // Candidates that do not fit remain pending for the next durable result.
        let mut deferred_table_ids = HashSet::new();
        let mut table_schema_retentions = table_schema_retentions.into_iter();

        while let Some((table_id, retention)) = table_schema_retentions.next() {
            if !self.tasks.try_queue_schema_cleanup(table_id, retention) {
                deferred_table_ids.insert(table_id);
                deferred_table_ids.extend(table_schema_retentions.map(|(table_id, _)| table_id));

                break;
            }
        }

        self.state
            .pending_relation_table_ids
            .retain(|table_id| deferred_table_ids.contains(table_id));
    }

    /// Returns schema retention boundaries for tables this worker may clean up.
    ///
    /// The candidates come from relation events covered by a durable
    /// destination result. The worker's normal ownership check decides which
    /// of those tables can be considered.
    ///
    /// The cleanup LSN is the minimum of the persisted commit-boundary
    /// checkpoint and the earliest snapshot still referenced by destination
    /// metadata. A persisted checkpoint is an arbitrary LSN and need not
    /// identify a schema version. The schema store resolves it to the greatest
    /// snapshot at or below that LSN, preserving that snapshot and every newer
    /// version.
    ///
    /// Progress and metadata do not need to be read in one transaction. During
    /// normal replication both safe boundaries move forward, so taking their
    /// minimum from a mixed observation is conservative. Table lifecycle resets
    /// remove the prior schemas before rebuilding them, and rebuilt nonzero
    /// snapshots occur after the frozen checkpoint boundary.
    async fn get_table_schema_retentions(
        &self,
        persisted_checkpoint_lsn: PgLsn,
    ) -> EtlResult<HashMap<TableId, TableSchemaRetention>> {
        let mut table_schema_retentions =
            HashMap::with_capacity(self.state.pending_relation_table_ids.len());

        for &table_id in &self.state.pending_relation_table_ids {
            // Only prune snapshots for tables this worker would apply at this
            // checkpoint. This keeps table sync workers limited to their
            // assigned table while preserving apply worker ownership rules.
            let should_apply_changes =
                self.should_apply_changes(table_id, persisted_checkpoint_lsn).await?;

            if !should_apply_changes {
                continue;
            }

            // We try to load the destination table metadata to see if it is referencing a
            // snapshot ID that would be cleaned up if we were to just use the
            // persisted checkpoint as the boundary.
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

            // An applying schema change may still need both endpoints for recovery, so
            // retain from the earlier destination snapshot.
            let destination_retention_snapshot_id = destination_table_metadata
                .previous_snapshot_id
                .unwrap_or(destination_table_metadata.snapshot_id)
                .min(destination_table_metadata.snapshot_id);

            // Choose the smaller LSN while retaining why it bounded cleanup. Unlike the
            // destination value, a persisted checkpoint need not coincide with a schema
            // snapshot.
            let retention =
                if persisted_checkpoint_lsn <= destination_retention_snapshot_id.into_inner() {
                    TableSchemaRetention::PersistedCheckpointLsn(persisted_checkpoint_lsn)
                } else {
                    TableSchemaRetention::SnapshotId(destination_retention_snapshot_id)
                };

            table_schema_retentions.insert(table_id, retention);
        }

        Ok(table_schema_retentions)
    }

    /// Waits for the pending flush result, if any.
    async fn wait_for_flush_result(
        pending_flush_result: Option<&mut PendingWriteEventsResult>,
    ) -> CompletedWriteEventsResult {
        match pending_flush_result {
            Some(flush_result) => flush_result.await,
            None => std::future::pending().await,
        }
    }

    /// Handles a completed batch flush result.
    async fn handle_flush_result(
        &mut self,
        flush_result: CompletedWriteEventsResult,
    ) -> EtlResult<()> {
        // We clear the state up front because this flush is no longer in flight.
        let processing_paused = self.state.resume_processing();

        // Explode the result into parts which are used for handling the flush result.
        let (metadata, completed_at, result) = flush_result.into_parts_with_completion();

        // If there was an error in the flushing, we return it immediately.
        let status = result?;

        if let Some(metadata) = metadata.as_ref() {
            // Relation events are optimistic cleanup signals: the first relation after
            // startup need not represent a schema change, while a real DDL is
            // communicated downstream through one. Accumulate them until a durable
            // result also carries a commit end LSN, because only then can persisted
            // progress cover every preceding relation in this ordered apply-loop
            // stream. This also makes cleanup self-healing across restarts: the first
            // later relation rebuilds the candidate even though this in-memory set was
            // lost.
            self.state
                .pending_relation_table_ids
                .extend(metadata.relation_table_ids.iter().copied());

            if metadata.durability == WriteEventsDurability::RequireDurable
                && status == DestinationWriteStatus::Accepted
            {
                bail!(
                    ErrorKind::DestinationError,
                    "Destination was expected to durably persist last batch but it didn't do it"
                );
            }

            counter!(
                ETL_EVENTS_PROCESSED_TOTAL,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                ACTION_LABEL => "table_streaming",
            )
            .increment(metadata.event_count as u64);

            histogram!(
                ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                ACTION_LABEL => "table_streaming",
            )
            .record(metadata.dispatched_at.elapsed().as_secs_f64());

            metadata.streaming_payload_metadata.record_processed(D::name());

            match status {
                DestinationWriteStatus::Accepted => {
                    // Remember when this write was dispatched and accepted so
                    // its durable durations can be recorded once a later
                    // durable result covers it. The queue is metric-only
                    // state, so overflow must never fail the apply loop; the
                    // write's samples are skipped instead.
                    if metadata.event_count > 0 {
                        if self.state.pending_durable_dispatches.len()
                            < MAX_PENDING_DURABLE_DISPATCHES
                        {
                            self.state.pending_durable_dispatches.push_back(
                                PendingDurableDispatch {
                                    dispatched_at: metadata.dispatched_at,
                                    accepted_at: completed_at,
                                },
                            );
                        } else {
                            warn!(
                                pending_durable_dispatches = MAX_PENDING_DURABLE_DISPATCHES,
                                "pending durable dispatch queue is full, skipping durable \
                                 duration samples for this write",
                            );
                        }
                    }

                    self.state.update_last_commit_end_lsn(metadata.commit_end_lsn);
                }
                DestinationWriteStatus::Durable => {
                    // A durable result also confirms every earlier accepted
                    // write in the same ordered apply-loop stream.
                    if !self.state.pending_durable_dispatches.is_empty() {
                        let deferred_durable_histogram = histogram!(
                            ETL_BATCH_ITEMS_DURABLE_DURATION_SECONDS,
                            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                            ACTION_LABEL => "table_streaming",
                            CONFIRMATION_LABEL => "deferred",
                        );
                        let durable_wait_histogram = histogram!(
                            ETL_BATCH_ITEMS_DURABLE_WAIT_DURATION_SECONDS,
                            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                            ACTION_LABEL => "table_streaming",
                        );
                        for dispatch in self.state.pending_durable_dispatches.drain(..) {
                            deferred_durable_histogram.record(
                                completed_at
                                    .saturating_duration_since(dispatch.dispatched_at)
                                    .as_secs_f64(),
                            );
                            durable_wait_histogram.record(
                                completed_at
                                    .saturating_duration_since(dispatch.accepted_at)
                                    .as_secs_f64(),
                            );
                        }
                    }

                    // Empty durability barriers carry no items, so only writes
                    // with items are recorded for the current result.
                    if metadata.event_count > 0 {
                        histogram!(
                            ETL_BATCH_ITEMS_DURABLE_DURATION_SECONDS,
                            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                            ACTION_LABEL => "table_streaming",
                            CONFIRMATION_LABEL => "direct",
                        )
                        .record(
                            completed_at
                                .saturating_duration_since(metadata.dispatched_at)
                                .as_secs_f64(),
                        );
                    }

                    // We process the syncing tables with the last end lsn that the batch contains.
                    //
                    // Note that it could be that there is no end lsn for a specific batch, which
                    // could happen if we process a huge transaction, and we don't reach the
                    // commit before flushing. In that case, we don't process syncing
                    // tables, meaning that progress is not tracked, since it's not going to
                    // do anything because we can only track progress at commit boundaries.
                    if let Some(commit_end_lsn) = metadata.commit_end_lsn {
                        self.process_syncing_tables_after_flush(commit_end_lsn).await?;

                        // Progress and table-sync state are now durable. Freeze cleanup
                        // boundaries for all cumulatively covered relations before
                        // allowing their database deletion to run asynchronously.
                        self.try_queue_schema_cleanup().await;
                    }
                }
            }

            // A keepalive-only table-sync completion records `Complete` before
            // dispatching its empty durability barrier. The normal quiescent hook
            // skips once an exit is requested, and shutdown drain does not run that
            // hook. Complete the handoff here after the barrier has settled the
            // carried commit LSN and the loop is quiescent again.
            if metadata.event_count == 0
                && metadata.durability == WriteEventsDurability::RequireDurable
                && status == DestinationWriteStatus::Durable
                && matches!(self.state.exit_intent, Some(ExitIntent::Complete))
                && self.state.is_quiescent()
            {
                self.process_syncing_tables_when_quiescent().await?;
            }
        }

        // If processing was paused, there must be a queued batch that still needs to be
        // flushed now that the previous in-flight result has resolved.
        if processing_paused {
            if let Some(metadata) = metadata.as_ref() {
                // A required-durability write is terminal, so `Complete` must have
                // stopped intake before a successor batch could be queued behind it.
                debug_assert_ne!(
                    metadata.durability,
                    WriteEventsDurability::RequireDurable,
                    "required-durability write must not have a queued successor batch"
                );
            }

            self.flush_batch("pending flush result received").await?;
        }

        Ok(())
    }

    /// Handles a message from the replication stream.
    ///
    /// Processes the message and manages batch timing.
    async fn handle_stream_message(
        &mut self,
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
        maybe_message: Option<EtlResult<ReplicationMessage<LogicalReplicationMessage>>>,
        replication_client: &PgReplicationClient,
    ) -> EtlResult<()> {
        // If there is no message anymore, it means that the connection has been closed
        // or had some issues, we must handle this case.
        let Some(message) = maybe_message else {
            let is_closed = replication_client.is_closed();
            return Err(self.build_stream_ended_error(is_closed));
        };

        // If the Postgres had an error, we want to raise it immediately.
        let message = message?;

        self.handle_replication_message_and_flush(replication_message_stream.as_mut(), message)
            .await
    }

    /// Creates an error for when the replication stream ends unexpectedly.
    fn build_stream_ended_error(&self, is_closed: bool) -> EtlError {
        let worker_type = self.worker_context.worker_type();

        if is_closed {
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
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
        message: ReplicationMessage<LogicalReplicationMessage>,
    ) -> EtlResult<()> {
        let result =
            self.handle_replication_message(replication_message_stream.as_mut(), message).await?;

        if let Some(event) = result.event {
            // We add the element to the pending batch.
            self.state.event_batch.push(event, result.streaming_payload_metadata);

            // We update the last end lsn of the commit that we encountered, if any.
            self.state.update_last_commit_end_lsn(result.end_lsn);

            // We start the batch timer for the flushing. This timer is needed to control
            // force flushing if the batch-size hint threshold is not reached in time.
            self.state.set_flush_deadline_if_needed(self.max_batch_fill_duration);
        }

        // We check for the batch flushing conditions before deciding whether to flush
        // or not.
        let batch_size_hint_bytes_reached = self.state.event_batch.size_hint_bytes()
            >= self.cached_batch_budget.current_batch_size_bytes();
        let early_flush_requested = result.end_batch;
        let should_flush = batch_size_hint_bytes_reached || early_flush_requested;

        if should_flush {
            let reason = if batch_size_hint_bytes_reached {
                "max batch size hint reached"
            } else {
                "early flush requested"
            };

            self.flush_batch(reason).await?;
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

        let event_batch = self.state.event_batch.take();

        self.dispatch_write_events(event_batch, reason).await
    }

    /// Dispatches one streaming write through the shared async-result path.
    async fn dispatch_write_events(
        &mut self,
        event_batch: EventBatch,
        reason: &str,
    ) -> EtlResult<()> {
        debug_assert!(!self.state.has_pending_flush_result());

        let (events, event_count, relation_table_ids, streaming_payload_metadata) =
            event_batch.into_parts();
        // `Complete` is terminal, so no later write is guaranteed to settle an
        // `Accepted` result. Its final batch must confirm cumulative durability
        // before the apply loop can complete.
        let durability = match self.state.exit_intent {
            Some(ExitIntent::Complete) => WriteEventsDurability::RequireDurable,
            Some(ExitIntent::Pause) | None => WriteEventsDurability::MayDefer,
        };
        debug!(
            worker_type = %self.worker_context.worker_type(),
            event_count,
            %reason,
            "flushing batch to destination",
        );

        // Capture dispatch-time metrics; they are carried through the result channel
        // and recorded once the destination acknowledges the batch.
        let metadata = ApplyLoopAsyncResultMetadata {
            commit_end_lsn: self.state.last_commit_end_lsn.take(),
            durability,
            event_count,
            relation_table_ids,
            streaming_payload_metadata,
            dispatched_at: Instant::now(),
        };

        // Create the flush result channel: the sender is handed to the destination and
        // the pending receiver is stored on the loop state until the
        // destination signals completion.
        let (flush_result, pending_flush_result) = WriteEventsResult::new(metadata);
        self.destination.write_events(events, durability, flush_result).await?;
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
        mut replication_message_stream: Pin<&mut BackpressureStream<ReplicationMessageStream>>,
        message: ReplicationMessage<LogicalReplicationMessage>,
    ) -> EtlResult<HandleMessageResult> {
        counter!(
            ETL_REPLICATION_MESSAGES_TOTAL,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
        )
        .increment(1);

        match message {
            ReplicationMessage::XLogData(message) => {
                // These positions have a narrower meaning than in a physical
                // WAL stream. PostgreSQL's logical walsender copies the output
                // callback LSN into both fields: BEGIN uses the first change
                // LSN, each pgoutput row change uses its own change LSN, COMMIT
                // uses the transaction end LSN, and a logical message uses its
                // message LSN. Auxiliary TYPE and RELATION messages are
                // intermediate writes, so both fields are zero.
                // This is a consumed logical-stream frontier, not the
                // primary's unrelated current WAL end; the monotonic updates
                // intentionally ignore the zero-valued envelopes.
                let start_lsn = PgLsn::from(message.wal_start());
                self.state.update_last_received_lsn(start_lsn);

                let end_lsn = PgLsn::from(message.wal_end());
                self.state.update_last_received_lsn(end_lsn);

                debug!(
                    %start_lsn,
                    %end_lsn,
                    "handling logical replication data message",
                );

                self.handle_logical_replication_message(message.into_data()).await
            }
            ReplicationMessage::PrimaryKeepAlive(message) => {
                // A primary keepalive has only `wal_end`, not `wal_start`.
                // This field predates PostgreSQL 15 and normally contains the
                // logical walsender's `sentPtr`. PostgreSQL 15 added the
                // empty-transaction optimization: in synchronous replication,
                // a skipped transaction can send a keepalive with its decoded
                // write location before `sentPtr` has been updated:
                // https://github.com/postgres/postgres/commit/d5a9d86d8ffcadc52ff3729cd00fbd83bc38643c
                // pgoutput skips a transaction this way when it reaches commit
                // without emitting any published change, for example after a
                // write to a table outside the publication. A burst of such
                // commits can therefore produce a burst of primary keepalives.
                // We attempt a response to each one, but the stream-level
                // status-update debounce suppresses redundant optional replies.
                // In either case this is a decoded-WAL frontier, not necessarily
                // an emitted-event frontier: it may advance through an
                // uncommitted transaction whose changes remain in the server's
                // reorder buffer. The slot retains the earlier `restart_lsn`
                // needed to rebuild such transactions after a reconnect.
                // ETL uses this position only while the apply loop is quiescent;
                // while emitted work is unresolved, `checkpoint_lsn()` remains
                // at the completed destination flush frontier. This relies on
                // the current non-streaming pgoutput session and must be
                // re-audited if transaction streaming is enabled.
                let end_lsn = PgLsn::from(message.wal_end());
                self.state.update_last_received_lsn(end_lsn);

                debug!(
                    wal_end = %end_lsn,
                    reply_requested = message.reply() == 1,
                    "received keep alive",
                );

                self.send_status_update(
                    replication_message_stream.as_mut(),
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
        message: LogicalReplicationMessage,
    ) -> EtlResult<HandleMessageResult> {
        self.record_streaming_event_received();

        match &message {
            LogicalReplicationMessage::Begin(begin_body) => self.handle_begin_message(begin_body),
            LogicalReplicationMessage::Commit(commit_body) => {
                self.handle_commit_message(commit_body).await
            }
            LogicalReplicationMessage::Relation(relation_body) => {
                self.handle_relation_message(relation_body).await
            }
            LogicalReplicationMessage::Insert(insert_body) => {
                self.state.current_tx_events += 1;
                self.handle_insert_message(insert_body).await
            }
            LogicalReplicationMessage::Update(update_body) => {
                self.state.current_tx_events += 1;
                self.handle_update_message(update_body).await
            }
            LogicalReplicationMessage::Delete(delete_body) => {
                self.state.current_tx_events += 1;
                self.handle_delete_message(delete_body).await
            }
            LogicalReplicationMessage::Truncate(truncate_body) => {
                self.state.current_tx_events += 1;
                self.handle_truncate_message(truncate_body).await
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
                self.handle_message(message_body).await
            }
            _ => Ok(HandleMessageResult::default()),
        }
    }

    /// Records a source event received by the table streaming path.
    fn record_streaming_event_received(&self) {
        counter!(
            ETL_EVENTS_RECEIVED_TOTAL,
            WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
            ACTION_LABEL => "table_streaming",
        )
        .increment(1);
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
    /// record the new schema cursor here so the next `Relation` rebuilds the
    /// masks against that exact snapshot. A table-sync worker cannot hand over
    /// this incomplete state until the relation provides both masks.
    async fn handle_message(
        &mut self,
        message: &protocol::MessageBody,
    ) -> EtlResult<HandleMessageResult> {
        // If the prefix is unknown, we don't want to process it.
        let prefix = message.prefix()?;
        if prefix != DDL_MESSAGE_PREFIX {
            warn!(
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
        let column_count = schema_change_message.columns.len();

        if !schema_change_message.applies_to_publication(&self.config.publication_name) {
            debug!(
                table_id = %table_id,
                message_publication = %
                    schema_change_message.publication_name.as_deref().unwrap_or("<missing>"),
                configured_publication = %self.config.publication_name,
                "skipping ddl schema change message for another publication"
            );

            counter!(
                ETL_DDL_SCHEMA_CHANGES_TOTAL,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                COMMAND_TAG_LABEL => command_tag,
                OUTCOME_LABEL => "skipped_publication",
            )
            .increment(1);

            return Ok(HandleMessageResult::no_event());
        }

        let table_name = schema_change_message.relname.clone();
        let schema_name = schema_change_message.nspname.clone();

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

        let snapshot_id = schema_snapshot_id_from_message(message);
        let table_schema = Arc::new(schema_change_message.into_table_schema(snapshot_id));
        self.table_decoding_states
            .insert(table_id, TableDecodingState::WaitingForRelation { snapshot_id });

        info!(
            table_id = %table_id,
            table_name = %table_name,
            schema_name = %schema_name,
            event = %command_tag,
            columns = column_count,
            "received ddl schema change message"
        );

        // Store the new schema version in the store.
        if let Err(err) = self.schema_store.store_table_schema((*table_schema).clone()).await {
            counter!(
                ETL_DDL_SCHEMA_CHANGES_TOTAL,
                WORKER_TYPE_LABEL => self.worker_context.worker_type().as_str(),
                COMMAND_TAG_LABEL => command_tag.clone(),
                OUTCOME_LABEL => "failed_store",
            )
            .increment(1);

            return Err(err);
        }

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
        .record(column_count as f64);

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
        message: &protocol::BeginBody,
    ) -> EtlResult<HandleMessageResult> {
        let final_lsn = PgLsn::from(message.final_lsn());
        self.state.remote_final_lsn = Some(final_lsn);

        self.state.current_tx_begin_ts = Some(Instant::now());
        self.state.current_tx_events = 0;
        self.state.reset_tx_ordinal();

        let tx_ordinal = self.state.next_tx_ordinal();
        let event = parse_event_from_begin_message(final_lsn, tx_ordinal, message);

        Ok(HandleMessageResult::return_event(Event::Begin(event)))
    }

    /// Handles Postgres COMMIT messages.
    async fn handle_commit_message(
        &mut self,
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
            histogram!(ETL_TRANSACTION_SIZE).record(self.state.current_tx_events as f64);

            self.state.current_tx_events = 0;
        }

        let end_lsn = PgLsn::from(message.end_lsn());

        // Process syncing tables after commit (worker-specific behavior).
        let should_end_batch = self.process_syncing_tables_after_commit_event(end_lsn).await?;

        let tx_ordinal = self.state.next_tx_ordinal();
        let event = parse_event_from_commit_message(commit_lsn, tx_ordinal, message);

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

        fn format_column_names(column_names: &HashSet<String>) -> String {
            let mut column_names = column_names.iter().map(String::as_str).collect::<Vec<_>>();
            column_names.sort_unstable();
            column_names.join(",")
        }

        let schema_selection =
            self.resolve_relation_schema_selection(table_id, remote_final_lsn).await?;
        let replicated_table_schema = self
            .materialize_relation_schema(
                table_id,
                schema_selection,
                &replicated_columns,
                &identity_columns,
            )
            .await?;

        info!(
            table_id = %table_id,
            replicated_columns = %format_column_names(&replicated_columns),
            identity_columns = %format_column_names(&identity_columns),
            "received relation message, building replication mask"
        );

        let relation_event = RelationEvent { replicated_table_schema };

        Ok(HandleMessageResult::return_event(Event::Relation(relation_event)))
    }

    /// Returns the schema lookup selected for a table's next relation message.
    ///
    /// The apply worker clears its local decoder before starting table
    /// synchronization. Therefore, an empty entry after the ownership boundary
    /// means the relation must rebuild the connection-local decoder.
    ///
    /// A preceding DDL message installs `WaitingForRelation`, which takes
    /// priority because it identifies a newer exact snapshot observed by this
    /// connection. Otherwise the schema lookup uses the later of the connection
    /// bootstrap and an applicable `SyncDone.lsn`. This lets the same
    /// connection include schemas skipped before handover, while a restarted
    /// connection can include schemas stored after handover. For `Ready`, the
    /// persisted-checkpoint condition guarantees that bootstrap is already at
    /// or beyond the discarded `SyncDone` boundary. The relation supplies
    /// fresh publication and identity masks, so its lookup needs only the
    /// `SyncDone` boundary, not the stored decoding payload.
    async fn resolve_relation_schema_selection(
        &self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<RelationSchemaSelection> {
        let decoding_state = self.table_decoding_states.get(&table_id).cloned();
        let sync_done_lsn = if decoding_state.is_none() {
            match self.get_table_state_for_decoding(table_id).await? {
                Some(TableState::SyncDone { lsn: sync_done_lsn, .. })
                    if sync_done_lsn <= remote_final_lsn =>
                {
                    Some(sync_done_lsn)
                }
                _ => None,
            }
        } else {
            None
        };

        Ok(self.select_relation_schema(decoding_state, sync_done_lsn))
    }

    /// Selects the schema lookup for a relation message.
    ///
    /// Under normal non-streaming pgoutput ordering, a relation arrives either
    /// with no connection-local state or after a DDL message has installed
    /// [`TableDecodingState::WaitingForRelation`]. A table-sync worker can
    /// instead start with a materialized bootstrap schema before its first
    /// catch-up relation arrives. A materialized state also covers repeated
    /// relation metadata.
    ///
    /// Without local state, the newest schema is bounded by the later of the
    /// connection bootstrap and `SyncDone`. The bootstrap prevents a fresh
    /// connection from selecting a schema it has not reached. Raising that
    /// bound to `SyncDone` lets a connection which started earlier see schema
    /// changes skipped while table sync owned the table. A `Ready` table no
    /// longer retains `SyncDone`, but its readiness invariant guarantees that
    /// a restarted connection's bootstrap is at or beyond the discarded
    /// boundary.
    fn select_relation_schema(
        &self,
        decoding_state: Option<TableDecodingState>,
        sync_done_lsn: Option<PgLsn>,
    ) -> RelationSchemaSelection {
        match decoding_state {
            Some(TableDecodingState::WaitingForRelation { snapshot_id }) => {
                RelationSchemaSelection::Exact(snapshot_id)
            }
            Some(TableDecodingState::WithSchema(schema)) => {
                RelationSchemaSelection::Exact(schema.inner().snapshot_id)
            }
            None => {
                let bootstrap_snapshot_id = self.state.bootstrap_snapshot_id();
                let schema_upper_bound = sync_done_lsn
                    .map_or(bootstrap_snapshot_id, |sync_done_lsn| {
                        bootstrap_snapshot_id.max(SnapshotId::from(sync_done_lsn))
                    });

                RelationSchemaSelection::AtOrBefore(schema_upper_bound)
            }
        }
    }

    /// Returns the apply worker's current state for a table decoder lookup.
    ///
    /// Active table-sync state is authoritative while its worker remains in
    /// the pool. After a restart or worker cleanup, the durable store supplies
    /// the same `SyncDone` state. A table-sync apply loop never consumes
    /// another worker's decoding state.
    async fn get_table_state_for_decoding(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableState>> {
        let WorkerContext::Apply(ctx) = &self.worker_context else {
            return Ok(None);
        };

        if let Some(worker_state) = ctx.pool.get_active_worker_state(table_id).await {
            let worker_state = worker_state.lock().await;
            Ok(Some(worker_state.table_state()))
        } else {
            ctx.store.get_table_state(table_id).await
        }
    }

    /// Materializes relation metadata against this attempt's selected table
    /// schema.
    async fn materialize_relation_schema(
        &mut self,
        table_id: TableId,
        schema_selection: RelationSchemaSelection,
        replicated_columns: &HashSet<String>,
        identity_columns: &HashSet<String>,
    ) -> EtlResult<ReplicatedTableSchema> {
        let table_schema = self.get_table_schema_for_relation(table_id, schema_selection).await?;

        let replication_mask = ReplicationMask::try_build(&table_schema, replicated_columns)?;
        let identity_mask = IdentityMask::try_build(&table_schema, identity_columns)?;
        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);

        self.table_decoding_states
            .insert(table_id, TableDecodingState::WithSchema(replicated_table_schema.clone()));

        Ok(replicated_table_schema)
    }

    /// Resolves a relation's schema according to its selected snapshot
    /// constraint.
    ///
    /// Connection-local state identifies exact snapshots established by a DDL
    /// message or durable `SyncDone` decoding state. A relation without local
    /// state instead uses an upper bound derived from the connection bootstrap
    /// and any applicable `SyncDone` boundary.
    async fn get_table_schema_for_relation(
        &self,
        table_id: TableId,
        schema_selection: RelationSchemaSelection,
    ) -> EtlResult<Arc<TableSchema>> {
        let requested_snapshot_id = match schema_selection {
            RelationSchemaSelection::Exact(snapshot_id)
            | RelationSchemaSelection::AtOrBefore(snapshot_id) => snapshot_id,
        };

        let table_schema = self
            .schema_store
            .get_table_schema(&table_id, requested_snapshot_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!(
                        "Table schema for table {} at snapshot {} not found",
                        table_id, requested_snapshot_id
                    )
                )
            })?;

        match schema_selection {
            RelationSchemaSelection::Exact(snapshot_id) => {
                if table_schema.snapshot_id != snapshot_id {
                    bail!(
                        ErrorKind::InvalidState,
                        "Table schema snapshot mismatch",
                        format!(
                            "Table schema for table {} resolved to snapshot {} when snapshot {} \
                             was required",
                            table_id, table_schema.snapshot_id, snapshot_id
                        )
                    );
                }
            }
            RelationSchemaSelection::AtOrBefore(snapshot_upper_bound) => {
                // Schema stores implement newest-at-or-before lookup. Keep this
                // validation at the decoding boundary because using a future
                // schema would corrupt the positional interpretation of row
                // tuples.
                if table_schema.snapshot_id > snapshot_upper_bound {
                    bail!(
                        ErrorKind::InvalidState,
                        "Bounded table schema lookup exceeded its upper bound",
                        format!(
                            "Schema lookup for table {} resolved snapshot {} beyond upper bound {}",
                            table_id, table_schema.snapshot_id, snapshot_upper_bound
                        )
                    );
                }

                if table_schema.snapshot_id != snapshot_upper_bound {
                    info!(
                        table_id = %table_id,
                        requested_snapshot_id = %snapshot_upper_bound,
                        resolved_snapshot_id = %table_schema.snapshot_id,
                        "bounded relation schema lookup resolved an older stored snapshot"
                    );
                }
            }
        }

        Ok(table_schema)
    }

    /// Installs missing stored decoding state when row decoding first needs it.
    ///
    /// Before the ownership boundary, rows remain owned by table sync and no
    /// apply decoding state is needed. At or after the boundary, the first
    /// relation-less DML can use the stored snapshot and masks because
    /// PostgreSQL may have sent the relevant relation while table sync still
    /// owned the table.
    ///
    /// Existing local state is authoritative. A materialized schema needs no
    /// recovery. [`TableDecodingState::WaitingForRelation`] means a DDL message
    /// invalidated the decoder but PostgreSQL did not send the required
    /// relation before DML, which is an invalid protocol sequence.
    ///
    /// `SyncDone` rows written before durable decoders were introduced
    /// deserialize without a stored snapshot or masks. ETL cannot safely infer
    /// that historical decoder from the physical schema, so it waits for a
    /// later relation and rejects relation-less DML.
    async fn install_sync_done_decoding_state_if_missing(
        &mut self,
        table_id: TableId,
        current_lsn: PgLsn,
    ) -> EtlResult<()> {
        match self.table_decoding_states.get(&table_id) {
            Some(TableDecodingState::WithSchema(_)) => return Ok(()),
            Some(TableDecodingState::WaitingForRelation { snapshot_id }) => {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "Relation message missing after schema change",
                    format!(
                        "Table {} received a row event while waiting for relation snapshot {}",
                        table_id, snapshot_id
                    )
                ));
            }
            None => {}
        }

        let Some(table_state) = self.get_table_state_for_decoding(table_id).await? else {
            return Ok(());
        };

        // DML ownership normally establishes the boundary in
        // `should_apply_changes`. Recheck it here because decoder recovery
        // independently reads durable table state across an async boundary.
        let TableState::SyncDone { lsn: sync_done_lsn, table_decoding_state } = table_state else {
            return Ok(());
        };

        if sync_done_lsn > current_lsn {
            return Ok(());
        }

        let Some(table_decoding_state) = table_decoding_state else {
            return Ok(());
        };

        let table_schema = self
            .get_table_schema_for_relation(
                table_id,
                RelationSchemaSelection::Exact(table_decoding_state.snapshot_id()),
            )
            .await?;
        let replicated_table_schema =
            table_decoding_state.materialize(table_schema, sync_done_lsn)?;
        self.table_decoding_states
            .insert(table_id, TableDecodingState::WithSchema(replicated_table_schema));

        info!(
            worker_type = %WorkerType::Apply,
            table_id = table_id.0,
            %sync_done_lsn,
            %current_lsn,
            "installed missing sync_done table decoding state",
        );

        Ok(())
    }

    /// Returns the materialized decoding state for a row event.
    ///
    /// The normal first-use sequence is `RELATION → DML`, so the relation
    /// handler materializes this state. PostgreSQL does not resend relation
    /// metadata when it already sent it earlier on the same connection,
    /// however. If apply skipped that relation while table sync still owned the
    /// table, the first owned DML at or after `SyncDone.lsn` arrives with no
    /// local decoder. In that case the compact `SyncDone` state supplies the
    /// exact schema snapshot and both masks on demand.
    ///
    /// A missing decoder without an applicable complete `SyncDone` state is an
    /// error: unlike a relation message, DML does not contain the column and
    /// identity metadata needed to reconstruct the masks from a bootstrap
    /// schema alone.
    async fn get_replicated_table_schema(
        &mut self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<ReplicatedTableSchema> {
        self.install_sync_done_decoding_state_if_missing(table_id, remote_final_lsn).await?;

        let replicated_table_schema = match self.table_decoding_states.get(&table_id).cloned() {
            Some(TableDecodingState::WithSchema(schema)) => schema,
            Some(TableDecodingState::WaitingForRelation { snapshot_id }) => {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "Relation state is waiting for refresh",
                    format!(
                        "Table {} requires relation snapshot {} before row decoding",
                        table_id, snapshot_id
                    )
                ));
            }
            None => {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "Relation state missing for row event",
                    format!(
                        "Table {} has no materialized relation or complete SyncDone decoding state",
                        table_id
                    )
                ));
            }
        };

        Ok(replicated_table_schema)
    }

    /// Handles Postgres INSERT messages.
    async fn handle_insert_message(
        &mut self,
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

        // Capture the source payload metadata and emit the initial metrics.
        let streaming_payload_metadata =
            StreamingPayloadMetadata::insert(insert_message_payload_bytes(message));
        streaming_payload_metadata.record_received();
        streaming_payload_metadata.record_row_size();

        // Exactly one worker owns protocol interpretation for a table at a time, so
        // Non-owning workers skip row decoding and leave their connection-local
        // decoding state untouched.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            self.get_replicated_table_schema(table_id, remote_final_lsn).await?;

        let event = parse_event_from_insert_message(
            replicated_table_schema,
            remote_final_lsn,
            tx_ordinal,
            message,
        )?;

        Ok(HandleMessageResult::return_row_event(Event::Insert(event), streaming_payload_metadata))
    }

    /// Handles Postgres UPDATE messages.
    async fn handle_update_message(
        &mut self,
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

        // Capture the source payload metadata and emit the initial metrics.
        let streaming_payload_metadata =
            StreamingPayloadMetadata::update(update_message_payload_bytes(message));
        streaming_payload_metadata.record_received();
        streaming_payload_metadata.record_row_size();

        // Exactly one worker owns protocol interpretation for a table at a time, so
        // Non-owning workers skip row decoding and leave their connection-local
        // decoding state untouched.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            self.get_replicated_table_schema(table_id, remote_final_lsn).await?;

        let event = parse_event_from_update_message(
            replicated_table_schema,
            remote_final_lsn,
            tx_ordinal,
            message,
        )?;

        Ok(HandleMessageResult::return_row_event(Event::Update(event), streaming_payload_metadata))
    }

    /// Handles Postgres DELETE messages.
    async fn handle_delete_message(
        &mut self,
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

        // Capture the source payload metadata and emit the initial metrics.
        let streaming_payload_metadata =
            StreamingPayloadMetadata::delete(delete_message_payload_bytes(message));
        streaming_payload_metadata.record_received();
        streaming_payload_metadata.record_row_size();

        // Exactly one worker owns protocol interpretation for a table at a time, so
        // Non-owning workers skip row decoding and leave their connection-local
        // decoding state untouched.
        if !self.should_apply_changes(table_id, remote_final_lsn).await? {
            return Ok(HandleMessageResult::no_event());
        }

        let replicated_table_schema =
            self.get_replicated_table_schema(table_id, remote_final_lsn).await?;

        let event = parse_event_from_delete_message(
            replicated_table_schema,
            remote_final_lsn,
            tx_ordinal,
            message,
        )?;

        Ok(HandleMessageResult::return_row_event(Event::Delete(event), streaming_payload_metadata))
    }

    /// Handles Postgres TRUNCATE messages.
    async fn handle_truncate_message(
        &mut self,
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
                    self.get_replicated_table_schema(table_id, remote_final_lsn).await?;
                truncated_tables.push(replicated_table_schema);
            }
        }

        // If nothing to apply, skip conversion entirely.
        if truncated_tables.is_empty() {
            return Ok(HandleMessageResult::no_event());
        }

        let event = parse_event_from_truncate_message(
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
    /// without changing its connection-local protocol state.
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
        let table_decoding_states = &mut self.table_decoding_states;
        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_after_commit_event(
                    ctx,
                    lsn,
                    table_decoding_states,
                )
                .await
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
    async fn process_syncing_tables_after_flush(&mut self, current_lsn: PgLsn) -> EtlResult<()> {
        debug!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables after destination batch flush"
        );

        // The caller invokes this only after the destination reports the
        // batch durable, so record completed write progress before performing
        // worker-specific table-state bookkeeping.
        self.state.update_last_flush_lsn(current_lsn);

        let table_sync_decoding_state = self.table_sync_decoding_state();
        let table_decoding_states = &mut self.table_decoding_states;

        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_after_flush(
                    ctx,
                    current_lsn,
                    table_decoding_states,
                )
                .await?;

                None
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_after_flush(
                    ctx,
                    current_lsn,
                    table_sync_decoding_state.as_ref(),
                )
                .await?
            }
        };

        self.state.record_exit_intent(exit_intent);

        // Persist progress only after worker-specific state processing succeeds.
        //
        // The apply-worker hook above intentionally reads the progress that was
        // durable before this flush. If this flush is the first one to reach a
        // SyncDone boundary, that hook leaves the table in SyncDone, this write
        // advances the persisted checkpoint, and the next quiescent or
        // after-flush evaluation may move the table to Ready once this
        // connection has also materialized its decoder. This one-evaluation
        // delay establishes
        // `Ready(H) => persisted apply checkpoint >= H` without an atomic
        // state/checkpoint write.
        //
        // This per-flush write deliberately minimizes the replay window when
        // PostgreSQL slot feedback has not advanced yet. It could be coalesced
        // across durable flushes if production metrics show that checkpoint
        // write amplification is material, at the cost of replaying more
        // already-flushed events after a crash. Any such optimization must keep
        // Ready transitions and schema cleanup gated on the checkpoint that is
        // actually persisted, never on a deferred in-memory candidate.
        let persisted_checkpoint_lsn = self.persist_replication_checkpoint(current_lsn).await?;
        debug_assert!(persisted_checkpoint_lsn >= current_lsn);

        Ok(())
    }

    /// Persists a worker checkpoint unless fault injection asks us to skip it.
    ///
    /// The caller passes a completed destination flush boundary. The store
    /// applies a monotonic update and returns the checkpoint that remains
    /// persisted.
    ///
    /// Test failpoints deliberately return the candidate without writing it so
    /// recovery tests can model a lost checkpoint write.
    async fn persist_replication_checkpoint(&self, checkpoint_lsn: PgLsn) -> EtlResult<PgLsn> {
        let worker_type = self.worker_context.worker_type();

        #[cfg(feature = "failpoints")]
        if etl_fail_point_active_for_parameter(
            STORE_REPLICATION_CHECKPOINT_FP,
            worker_type.as_str(),
        ) {
            warn!(
                %worker_type,
                %checkpoint_lsn,
                "not persisting replication checkpoint due to active failpoint"
            );

            return Ok(checkpoint_lsn);
        }

        self.schema_store.upsert_replication_checkpoint(worker_type, checkpoint_lsn).await
    }

    /// Returns whether this table-sync worker has received its catchup target.
    async fn table_sync_catchup_target_reached(&self) -> bool {
        let WorkerContext::TableSync(ctx) = &self.worker_context else {
            return false;
        };

        table_sync_worker::catchup_target_reached(ctx, self.state.last_received_lsn()).await
    }

    /// Processes syncing tables when the apply loop is quiescent.
    ///
    /// Once an exit has already been requested we intentionally skip this class
    /// of work so draining stays focused on already-started flushes and
    /// shutdown barriers.
    async fn maybe_process_syncing_tables_when_quiescent(&mut self) -> EtlResult<()> {
        if self.state.exit_intent.is_some() {
            return Ok(());
        }

        // Catchup can reach its target through a keepalive after the last event batch
        // returned `Accepted`. Once no transaction, buffered batch, or in-flight write
        // remains, no terminal batch exists to carry `RequireDurable`, so dispatch an
        // empty durability barrier.
        if !self.state.handling_transaction()
            && !self.state.has_unresolved_batch_work()
            && self.state.last_commit_end_lsn.is_some()
            && self.table_sync_catchup_target_reached().await
        {
            // Record completion before dispatch so intake stops and the empty write
            // requires durability.
            self.state.record_exit_intent(Some(ExitIntent::Complete));
            self.dispatch_write_events(
                EventBatch::default(),
                "table sync catchup reached without a terminal event batch",
            )
            .await?;

            return Ok(());
        }

        self.process_syncing_tables_when_quiescent().await
    }

    /// Processes syncing tables when the apply loop is quiescent.
    ///
    /// Dispatches to worker-specific implementation based on the worker
    /// context.
    ///
    /// Quiescent syncing uses the last received LSN so table handoff can make
    /// progress even when no new transactions arrive. This is made possible
    /// thanks to keepalive messages that carry the logical walsender's sent
    /// position. That position advances through decoded WAL even when the
    /// output plugin emits no events for tables this pipeline is interested
    /// in.
    async fn process_syncing_tables_when_quiescent(&mut self) -> EtlResult<()> {
        if !self.state.is_quiescent() {
            debug!("skipping table sync processing because apply loop is not quiescent");

            return Ok(());
        }

        let current_lsn = self.state.last_received_lsn();

        debug!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables while apply loop is quiescent"
        );

        let table_sync_decoding_state = self.table_sync_decoding_state();
        let table_decoding_states = &mut self.table_decoding_states;

        let exit_intent = match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                apply_worker::process_syncing_tables_when_quiescent(
                    ctx,
                    current_lsn,
                    table_decoding_states,
                )
                .await
            }
            WorkerContext::TableSync(ctx) => {
                table_sync_worker::process_syncing_tables_when_quiescent(
                    ctx,
                    current_lsn,
                    table_sync_decoding_state.as_ref(),
                )
                .await
            }
        }?;

        self.state.record_exit_intent(exit_intent);

        Ok(())
    }

    /// Returns the table-sync worker's current row-decoding state.
    fn table_sync_decoding_state(&self) -> Option<TableDecodingState> {
        let WorkerContext::TableSync(ctx) = &self.worker_context else {
            return None;
        };

        self.table_decoding_states.get(&ctx.table_id).cloned()
    }
}

/// Returns tables that are still synchronizing.
async fn get_syncing_tables<S>(store: &S) -> EtlResult<Vec<(TableId, TableState)>>
where
    S: StateStore,
{
    let states = store.get_table_states().await?;
    Ok(states
        .iter()
        .filter(|(_, state)| state.as_type().is_syncing())
        .map(|(id, state)| (*id, state.clone()))
        .collect())
}

/// Functions specific to the apply worker.
mod apply_worker {
    use super::*;

    /// Why the apply worker is waiting for table-sync catchup.
    #[derive(Debug)]
    enum CatchupWaitReason {
        /// The apply worker just moved the table-sync worker into catchup.
        EnteredCatchup,
        /// The apply worker found the table-sync worker already in catchup.
        AlreadyInCatchup,
    }

    /// Clears apply-side decoding state before a table-sync worker starts.
    ///
    /// While table sync owns the table, the apply worker skips its DDL,
    /// relation, and DML messages. Removing the previous decoder makes any
    /// state observed after ownership returns meaningful: it was created from
    /// `SyncDone` on demand or from protocol messages handled by this apply
    /// loop after the handover boundary.
    ///
    /// This also covers explicit table resets. Resetting the durable table
    /// state to `Init` immediately stops apply ownership. Before the
    /// replacement copy begins, this removal prevents an earlier
    /// `WithSchema` from satisfying the later `SyncDone → Ready` condition.
    /// The table-sync worker then drops the old destination object, clears
    /// the table's stored schemas and metadata, and stores a fresh
    /// initial-copy schema.
    fn reset_table_decoding_state_for_sync(
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
        table_id: TableId,
    ) {
        if table_decoding_states.remove(&table_id).is_some() {
            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                "cleared apply decoding state before starting table sync worker",
            );
        }
    }

    /// Determines whether changes should be applied for a given table.
    ///
    /// If an active worker exists for the table, its state is checked while
    /// holding the lock. Otherwise, the table state is read from the
    /// store.
    pub(super) async fn should_apply_changes<S, D>(
        ctx: &ApplyWorkerContext<S, D>,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> EtlResult<bool>
    where
        S: SharedStateStore,
    {
        fn is_state_ready_for_changes(state: TableState, remote_final_lsn: PgLsn) -> bool {
            match state {
                TableState::Ready => true,
                // Match PostgreSQL's table-sync boundary rule. SyncDone may
                // point one byte past the initial slot's consistent-point WAL
                // record, which is also the start of a COMMIT record whose
                // transaction was excluded from the copied snapshot. BEGIN's
                // final LSN can therefore equal SyncDone even though the apply
                // worker, not the table-sync worker, must apply that
                // transaction.
                TableState::SyncDone { lsn, .. } => lsn <= remote_final_lsn,
                _ => false,
            }
        }

        let active_worker_state = ctx.pool.get_active_worker_state(table_id).await;

        if let Some(active_worker_state) = active_worker_state {
            let inner = active_worker_state.lock().await;
            return Ok(is_state_ready_for_changes(inner.table_state(), remote_final_lsn));
        }

        // If we didn't find an active worker, we need to read the table state
        // from the store. This could happen if the event is from a table that
        // has to be synced, or it was synced.
        let Some(state) = ctx.store.get_table_state(table_id).await? else {
            return Ok(false);
        };

        Ok(is_state_ready_for_changes(state, remote_final_lsn))
    }

    /// Processes syncing tables after commit.
    ///
    /// Spawns new table sync workers, triggers catchup when encountering
    /// SyncWait, and waits for workers already in Catchup.
    /// Does NOT perform SyncDone → Ready transitions.
    pub(super) async fn process_syncing_tables_after_commit_event<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        for (table_id, table_state) in get_syncing_tables(&ctx.store).await? {
            let exit_intent = process_single_syncing_table_after_commit_event(
                ctx,
                table_id,
                table_state,
                current_lsn,
                table_decoding_states,
            )
            .await?;

            if exit_intent.is_some() {
                return Ok(exit_intent);
            }
        }

        Ok(None)
    }

    /// Waits for a table-sync worker in catchup to hand the table back.
    async fn wait_for_table_sync_worker_catchup<S, D>(
        ctx: &ApplyWorkerContext<S, D>,
        table_id: TableId,
        worker_state: &TableSyncWorkerState,
        catchup_lsn: PgLsn,
        wait_reason: CatchupWaitReason,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: SchemaStore,
    {
        // The table-sync worker owns the table through `SyncDone`. Reaching that
        // state releases the apply loop. An owned relation can rebuild the
        // decoder using the handover boundary, while relation-less DML can
        // restore the complete stored decoder on demand.
        let catchup_state = match wait_reason {
            CatchupWaitReason::EnteredCatchup => "entered",
            CatchupWaitReason::AlreadyInCatchup => "already_in",
        };

        info!(
            worker_type = %WorkerType::Apply,
            table_id = table_id.0,
            %catchup_lsn,
            catchup_state,
            "apply worker blocking until table sync worker reaches sync_done",
        );

        // We wait for both states since if the table sync worker errors, we don't want
        // to stall forever.
        let result = worker_state
            .wait_for_state_type(
                &[TableStateType::SyncDone, TableStateType::Errored],
                ctx.shutdown_rx.clone(),
            )
            .await;

        match result {
            ShutdownResult::Ok(result) => {
                let final_state = result.table_state();
                if final_state.as_type().is_errored() {
                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %final_state.as_type(),
                        "apply worker unblocked: table sync worker errored, skipping table",
                    );

                    return Ok(None);
                }

                info!(
                    worker_type = %WorkerType::Apply,
                    table_id = table_id.0,
                    table_state_type = %final_state.as_type(),
                    "apply worker unblocked: table sync worker reached sync_done",
                );

                Ok(None)
            }
            ShutdownResult::Shutdown(_) => {
                info!(
                    worker_type = %WorkerType::Apply,
                    table_id = table_id.0,
                    "apply worker unblocked: shutdown signal received while waiting for table sync worker",
                );

                Ok(Some(ExitIntent::Pause))
            }
        }
    }

    /// Processes a single syncing table after commit.
    ///
    /// Handles SyncWait → Catchup transitions, waits for workers already in
    /// Catchup, and spawns new workers.
    /// Does NOT handle SyncDone → Ready transitions.
    async fn process_single_syncing_table_after_commit_event<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        table_id: TableId,
        table_state: TableState,
        current_lsn: PgLsn,
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        let worker_state = ctx.pool.get_active_worker_state(table_id).await;

        if let Some(worker_state) = worker_state {
            let mut worker_state_guard = worker_state.lock().await;
            let state = worker_state_guard.table_state();

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                table_state_type = %state.as_type(),
                %current_lsn,
                "checking table with active worker after commit",
            );

            match state {
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

                    // It's important to drop the state guard before waiting, otherwise we deadlock.
                    drop(worker_state_guard);

                    if let Some(exit_intent) = wait_for_table_sync_worker_catchup(
                        ctx,
                        table_id,
                        &worker_state,
                        catchup_lsn,
                        CatchupWaitReason::EnteredCatchup,
                    )
                    .await?
                    {
                        return Ok(Some(exit_intent));
                    }
                }
                TableState::SyncDone { lsn, .. } => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        sync_done_lsn = %lsn,
                        "table in sync_done state, waiting for readiness evaluation",
                    );
                }
                TableState::Catchup { lsn: catchup_lsn } => {
                    drop(worker_state_guard);

                    if let Some(exit_intent) = wait_for_table_sync_worker_catchup(
                        ctx,
                        table_id,
                        &worker_state,
                        catchup_lsn,
                        CatchupWaitReason::AlreadyInCatchup,
                    )
                    .await?
                    {
                        return Ok(Some(exit_intent));
                    }
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %state.as_type(),
                        "no action needed for current state after commit",
                    );
                }
            }
        } else {
            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                table_state_type = %table_state.as_type(),
                "checking table without active worker after commit",
            );

            // No active worker exists, potentially start a new worker.
            match table_state {
                TableState::SyncDone { lsn, .. } => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        sync_done_lsn = %lsn,
                        "table in sync_done state, waiting for readiness evaluation",
                    );
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %table_state.as_type(),
                        "spawning new table sync worker",
                    );
                    // Start a new worker for this table.
                    reset_table_decoding_state_for_sync(table_decoding_states, table_id);
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
    /// Handles `SyncDone → Ready` transitions only after the persisted apply
    /// checkpoint reaches `SyncDone.lsn` and this connection has materialized
    /// the table decoder, and spawns new workers.
    ///
    /// The caller invokes this before persisting `current_lsn`, so
    /// `persisted_checkpoint_lsn` is the restart frontier from an earlier
    /// evaluation. When the current flush first reaches `SyncDone.lsn`, this
    /// pass keeps the table in `SyncDone`; the caller then persists the new
    /// checkpoint, and the next quiescent or after-flush pass can store `Ready`
    /// if the decoder is also materialized. The deliberate delay makes every
    /// persisted `Ready` imply both that a checkpoint at or beyond
    /// `SyncDone.lsn` was already durable and that the current connection no
    /// longer needs the stored decoding payload.
    pub(super) async fn process_syncing_tables_after_flush<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
    ) -> EtlResult<()>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        let syncing_tables = get_syncing_tables(&ctx.store).await?;
        if syncing_tables.is_empty() {
            return Ok(());
        }

        // This apply loop serializes checkpoint persistence. No apply
        // checkpoint write can race this read before the caller finishes this
        // state evaluation and persists its next candidate.
        let persisted_checkpoint_lsn =
            ctx.store.get_replication_checkpoint(WorkerType::Apply).await?;
        for (table_id, table_state) in syncing_tables {
            process_single_syncing_table_after_flush(
                ctx,
                table_id,
                table_state,
                current_lsn,
                persisted_checkpoint_lsn,
                table_decoding_states,
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
        table_state: TableState,
        current_lsn: PgLsn,
        persisted_checkpoint_lsn: Option<PgLsn>,
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
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
            let mut worker_state_guard = worker_state.lock().await;
            let state = worker_state_guard.table_state();
            let has_materialized_decoding_state = matches!(
                table_decoding_states.get(&table_id),
                Some(TableDecodingState::WithSchema(_))
            );

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                table_state_type = %state.as_type(),
                %current_lsn,
                "checking table with active worker after batch flush",
            );

            if let TableState::SyncDone { lsn: sync_done_lsn, .. } = state {
                if can_transition_to_ready(
                    current_lsn,
                    persisted_checkpoint_lsn,
                    sync_done_lsn,
                    has_materialized_decoding_state,
                ) {
                    info!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %sync_done_lsn,
                        %current_lsn,
                        ?persisted_checkpoint_lsn,
                        "transitioning sync_done -> ready",
                    );

                    worker_state_guard.set_and_store(TableState::Ready, &ctx.store).await?;
                } else {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %sync_done_lsn,
                        %current_lsn,
                        ?persisted_checkpoint_lsn,
                        has_materialized_decoding_state,
                        "table not yet ready, checkpoint or local decoding schema unavailable",
                    );
                }
            }
        } else {
            let has_materialized_decoding_state = matches!(
                table_decoding_states.get(&table_id),
                Some(TableDecodingState::WithSchema(_))
            );

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                table_state_type = %table_state.as_type(),
                "checking table without active worker after batch flush",
            );

            match table_state {
                TableState::SyncDone { lsn: sync_done_lsn, .. } => {
                    if can_transition_to_ready(
                        current_lsn,
                        persisted_checkpoint_lsn,
                        sync_done_lsn,
                        has_materialized_decoding_state,
                    ) {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            ?persisted_checkpoint_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store.update_table_state(table_id, TableState::Ready).await?;
                    } else {
                        debug!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            ?persisted_checkpoint_lsn,
                            has_materialized_decoding_state,
                            "table not yet ready, checkpoint or local decoding schema unavailable",
                        );
                    }
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %table_state.as_type(),
                        "spawning new table sync worker",
                    );

                    // Start a new worker for this table.
                    reset_table_decoding_state_for_sync(table_decoding_states, table_id);
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

    /// Processes syncing tables while the apply loop is quiescent.
    ///
    /// Handles `SyncWait → Catchup`, waits for workers already in Catchup,
    /// transitions `SyncDone → Ready` only after the persisted apply checkpoint
    /// reaches `SyncDone.lsn` and this connection has materialized the table
    /// decoder, and spawns workers.
    ///
    /// This path does not advance the apply checkpoint because no destination
    /// flush completed. A quiet table may therefore remain in `SyncDone` until
    /// a later durable apply flush advances the persisted checkpoint.
    pub(super) async fn process_syncing_tables_when_quiescent<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        current_lsn: PgLsn,
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: PipelineStore,
        D: PipelineDestination,
    {
        let syncing_tables = get_syncing_tables(&ctx.store).await?;
        if syncing_tables.is_empty() {
            return Ok(None);
        }

        // This apply loop serializes checkpoint persistence. No apply
        // checkpoint write can race this read before the caller finishes this
        // state evaluation and persists its next candidate.
        let persisted_checkpoint_lsn =
            ctx.store.get_replication_checkpoint(WorkerType::Apply).await?;
        for (table_id, table_state) in syncing_tables {
            let exit_intent = process_single_syncing_table_when_quiescent(
                ctx,
                table_id,
                table_state,
                current_lsn,
                persisted_checkpoint_lsn,
                table_decoding_states,
            )
            .await?;

            if exit_intent.is_some() {
                return Ok(exit_intent);
            }
        }

        Ok(None)
    }

    /// Processes a single syncing table while the apply loop is quiescent.
    ///
    /// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, waits
    /// for workers already in Catchup, and spawns workers.
    async fn process_single_syncing_table_when_quiescent<S, D>(
        ctx: &mut ApplyWorkerContext<S, D>,
        table_id: TableId,
        table_state: TableState,
        current_lsn: PgLsn,
        persisted_checkpoint_lsn: Option<PgLsn>,
        table_decoding_states: &mut HashMap<TableId, TableDecodingState>,
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
            let state = worker_state_guard.table_state();
            let has_materialized_decoding_state = matches!(
                table_decoding_states.get(&table_id),
                Some(TableDecodingState::WithSchema(_))
            );

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                table_state_type = %state.as_type(),
                %current_lsn,
                "checking table with active worker when quiescent",
            );

            match state {
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

                    // It's important to drop the state guard before waiting, otherwise we deadlock.
                    drop(worker_state_guard);

                    if let Some(exit_intent) = wait_for_table_sync_worker_catchup(
                        ctx,
                        table_id,
                        &worker_state,
                        catchup_lsn,
                        CatchupWaitReason::EnteredCatchup,
                    )
                    .await?
                    {
                        return Ok(Some(exit_intent));
                    }
                }
                TableState::SyncDone { lsn: sync_done_lsn, .. } => {
                    if can_transition_to_ready(
                        current_lsn,
                        persisted_checkpoint_lsn,
                        sync_done_lsn,
                        has_materialized_decoding_state,
                    ) {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            ?persisted_checkpoint_lsn,
                            "transitioning sync_done -> ready",
                        );

                        worker_state_guard.set_and_store(TableState::Ready, &ctx.store).await?;

                        return Ok(None);
                    }

                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        %sync_done_lsn,
                        %current_lsn,
                        ?persisted_checkpoint_lsn,
                        has_materialized_decoding_state,
                        "table not yet ready, checkpoint or local decoding schema unavailable",
                    );
                }
                TableState::Catchup { lsn: catchup_lsn } => {
                    drop(worker_state_guard);

                    if let Some(exit_intent) = wait_for_table_sync_worker_catchup(
                        ctx,
                        table_id,
                        &worker_state,
                        catchup_lsn,
                        CatchupWaitReason::AlreadyInCatchup,
                    )
                    .await?
                    {
                        return Ok(Some(exit_intent));
                    }
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %state.as_type(),
                        "no action needed for current state when quiescent",
                    );
                }
            }
        } else {
            let has_materialized_decoding_state = matches!(
                table_decoding_states.get(&table_id),
                Some(TableDecodingState::WithSchema(_))
            );

            debug!(
                worker_type = %WorkerType::Apply,
                table_id = table_id.0,
                table_state_type = %table_state.as_type(),
                "checking table without active worker when quiescent",
            );

            match table_state {
                TableState::SyncDone { lsn: sync_done_lsn, .. } => {
                    if can_transition_to_ready(
                        current_lsn,
                        persisted_checkpoint_lsn,
                        sync_done_lsn,
                        has_materialized_decoding_state,
                    ) {
                        info!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            ?persisted_checkpoint_lsn,
                            "transitioning sync_done -> ready",
                        );

                        ctx.store.update_table_state(table_id, TableState::Ready).await?;
                    } else {
                        debug!(
                            worker_type = %WorkerType::Apply,
                            table_id = table_id.0,
                            %sync_done_lsn,
                            %current_lsn,
                            ?persisted_checkpoint_lsn,
                            has_materialized_decoding_state,
                            "table not yet ready, checkpoint or local decoding schema unavailable",
                        );
                    }
                }
                _ => {
                    debug!(
                        worker_type = %WorkerType::Apply,
                        table_id = table_id.0,
                        table_state_type = %table_state.as_type(),
                        "spawning new table sync worker",
                    );

                    // Start a new worker for this table.
                    reset_table_decoding_state_for_sync(table_decoding_states, table_id);
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

    /// Returns whether stored `SyncDone` decoding state can be discarded as
    /// `Ready`.
    ///
    /// The three checks cover separate safety obligations:
    ///
    /// - `current_lsn >= sync_done_lsn` proves that this connection reached the
    ///   boundary where apply ownership resumes.
    /// - `persisted_checkpoint_lsn >= sync_done_lsn` proves restart safety.
    ///   Requiring the stored value—not the apply loop's in-memory checkpoint—
    ///   makes `Ready(H) => persisted apply checkpoint >= H` hold across
    ///   crashes and checkpoint-write failures. Startup selects the later of
    ///   the replication-slot position and this checkpoint, so a later
    ///   connection has a bootstrap snapshot at or beyond `SyncDone`. A fresh
    ///   pgoutput connection emits relation metadata before its first row
    ///   change, so that relation can resolve the newest stored schema at or
    ///   before the bootstrap.
    /// - `has_materialized_decoding_state` proves current-connection safety.
    ///   PostgreSQL may have sent the relation before `SyncDone`, while table
    ///   sync still owned the table. Apply skipped that relation, and
    ///   PostgreSQL need not send it again on the same connection. If DML then
    ///   arrives without a local decoder, apply can recover only while
    ///   `SyncDone` still retains the exact snapshot and masks.
    ///
    /// Apply-side decoding state is cleared before table sync starts.
    /// Consequently, a later [`TableDecodingState::WithSchema`] proves that
    /// this connection now has a complete decoder: relation-less DML restored
    /// it from `SyncDone`, or an owned relation materialized it. Only then may
    /// `Ready` discard the durable fallback.
    ///
    /// The `WithSchema` check does not make a table that is already `Ready`
    /// safer across restart. The persisted-checkpoint condition provides that
    /// guarantee. A restarted loop which still finds `SyncDone` applies this
    /// same check only because the restarted loop is now the current
    /// connection whose decoder must be established before it stores `Ready`.
    ///
    /// A handled DDL replaces `WithSchema` with `WaitingForRelation`,
    /// preventing `Ready` until PostgreSQL's following relation
    /// materializes the new exact snapshot and masks.
    ///
    /// A table which lacks local decoding state remains in `SyncDone`; this
    /// does not block owned changes at or after the boundary. Its first
    /// relation or DML can materialize the decoder on demand.
    fn can_transition_to_ready(
        current_lsn: PgLsn,
        persisted_checkpoint_lsn: Option<PgLsn>,
        sync_done_lsn: PgLsn,
        has_materialized_decoding_state: bool,
    ) -> bool {
        has_materialized_decoding_state
            && current_lsn >= sync_done_lsn
            && persisted_checkpoint_lsn.is_some_and(|lsn| lsn >= sync_done_lsn)
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
            ctx.out_of_band_source_pool.clone(),
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

    /// Returns whether the worker's catchup target has been received.
    pub(super) async fn catchup_target_reached<S>(
        ctx: &TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
    ) -> bool {
        let inner = ctx.table_sync_worker_state.lock().await;

        matches!(
            inner.table_state(),
            TableState::Catchup { lsn: catchup_lsn } if current_lsn >= catchup_lsn
        )
    }

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
        table_decoding_state: Option<&TableDecodingState>,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: SharedStateStore,
    {
        try_complete_catchup(ctx, current_lsn, table_decoding_state).await
    }

    /// Processes syncing tables while the apply loop is quiescent.
    ///
    /// If catchup position reached, transitions to SyncDone.
    pub(super) async fn process_syncing_tables_when_quiescent<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
        table_decoding_state: Option<&TableDecodingState>,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: SharedStateStore,
    {
        try_complete_catchup(ctx, current_lsn, table_decoding_state).await
    }

    /// Attempts to complete catchup and transition to SyncDone.
    ///
    /// If catchup position has been reached, transitions to SyncDone and
    /// returns Complete.
    async fn try_complete_catchup<S>(
        ctx: &mut TableSyncWorkerContext<S>,
        current_lsn: PgLsn,
        table_decoding_state: Option<&TableDecodingState>,
    ) -> EtlResult<Option<ExitIntent>>
    where
        S: SharedStateStore,
    {
        let worker_type = WorkerType::TableSync { table_id: ctx.table_id };
        let mut inner = ctx.table_sync_worker_state.lock().await;
        let state = inner.table_state();

        if let TableState::Catchup { lsn: catchup_lsn } = state {
            if current_lsn >= catchup_lsn {
                let sync_done_state =
                    build_sync_done_state(ctx.table_id, current_lsn, table_decoding_state)?;

                info!(
                    %worker_type,
                    %catchup_lsn,
                    %current_lsn,
                    "catchup target lsn reached, transitioning catchup -> sync_done",
                );

                inner.set_and_store(sync_done_state, &ctx.state_store).await?;

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

    /// Builds `SyncDone` with the complete table-sync decoder state.
    fn build_sync_done_state(
        table_id: TableId,
        sync_done_lsn: PgLsn,
        table_decoding_state: Option<&TableDecodingState>,
    ) -> EtlResult<TableState> {
        match table_decoding_state {
            Some(TableDecodingState::WithSchema(replicated_table_schema)) => {
                Ok(TableState::sync_done(sync_done_lsn, replicated_table_schema))
            }
            // TODO: Make this handover recoverable when catch-up observes a
            //  schema or publication change without a following relation,
            //  without guessing the historical publication and identity masks.
            Some(TableDecodingState::WaitingForRelation { snapshot_id }) => Err(etl_error!(
                ErrorKind::InvalidState,
                "Table-sync decoding state is incomplete",
                format!(
                    "Table {} loaded schema snapshot {}, but PostgreSQL did not emit the Relation \
                     message required to build its publication and identity masks before SyncDone",
                    table_id, snapshot_id
                )
            )),
            None => Err(etl_error!(
                ErrorKind::InvalidState,
                "Table-sync decoding state is missing",
                format!("Table {} has no row-decoding state at SyncDone", table_id)
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_postgres::types::Type;

    use super::*;
    use crate::{
        replication::state::StoredTableDecodingState,
        schema::{ColumnSchema, TableName},
    };

    fn replicated_schema(snapshot_id: SnapshotId) -> ReplicatedTableSchema {
        let table_schema = TableSchema::with_snapshot_id(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1)],
            snapshot_id,
        );

        ReplicatedTableSchema::from_masks(
            Arc::new(table_schema),
            ReplicationMask::from_bytes(vec![1]),
            IdentityMask::from_bytes(vec![1]),
        )
    }

    /// Returns the complete decoder captured by a test `SyncDone` state.
    fn sync_done_decoding_state(table_state: &TableState) -> (PgLsn, &StoredTableDecodingState) {
        let TableState::SyncDone { lsn, table_decoding_state: Some(table_decoding_state) } =
            table_state
        else {
            panic!("test table state should contain SyncDone decoding state");
        };

        (*lsn, table_decoding_state)
    }

    #[test]
    fn stored_sync_done_decoder_materializes_schema_and_masks() {
        let snapshot_id = SnapshotId::new(20.into());
        let replicated_table_schema = replicated_schema(snapshot_id);
        let table_state = TableState::sync_done(20.into(), &replicated_table_schema);
        let (sync_done_lsn, table_decoding_state) = sync_done_decoding_state(&table_state);

        let loaded_schema = table_decoding_state
            .materialize(Arc::new(replicated_table_schema.inner().clone()), sync_done_lsn)
            .unwrap();
        assert_eq!(loaded_schema.inner().snapshot_id, snapshot_id);
        assert_eq!(loaded_schema.replication_mask(), replicated_table_schema.replication_mask());
        assert_eq!(loaded_schema.identity_mask(), replicated_table_schema.identity_mask());
    }
}
