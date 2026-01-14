use etl_config::shared::PipelineConfig;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::types::TableId;
use futures::StreamExt;
use metrics::{counter, histogram};
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, warn};

use crate::concurrency::shutdown::ShutdownRx;
use crate::conversions::event::{
    parse_event_from_begin_message, parse_event_from_commit_message,
    parse_event_from_delete_message, parse_event_from_insert_message,
    parse_event_from_relation_message, parse_event_from_truncate_message,
    parse_event_from_update_message,
};
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
use crate::metrics::{
    ACTION_LABEL, DESTINATION_LABEL, ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
    ETL_EVENTS_PROCESSED_TOTAL, ETL_TRANSACTION_DURATION_SECONDS, ETL_TRANSACTION_SIZE,
    ETL_TRANSACTIONS_TOTAL, PIPELINE_ID_LABEL, WORKER_TYPE_LABEL,
};
use crate::replication::client::PgReplicationClient;
use crate::replication::stream::{EventsStream, StatusUpdateType};
use crate::state::table::{RetryPolicy, TableReplicationError};
use crate::store::schema::SchemaStore;
use crate::types::{Event, PipelineId};
use crate::{bail, etl_error};

/// Result type for the apply loop execution.
///
/// [`ApplyLoopResult`] indicates the reason why the apply loop terminated,
/// enabling appropriate cleanup and error handling by the caller.
#[derive(Debug, Copy, Clone)]
pub enum ApplyLoopResult {
    /// The apply loop was paused.
    ///
    /// When the apply loop is paused, the system assumes that it could be resumed in the future.
    Paused,
    /// The apply loop was completed.
    ///
    /// When the apply loop is completed, the system assumes that it will never be invoked again.
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
    /// Returns `Some(result)` if the action can lead to a result of the loop, `None`
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
    ///
    /// The rationale for these priorities is that we want to give priority to terminating
    /// actions and when choosing between two terminating actions, the most restrictive is the one
    /// we want to honor.
    pub fn merge(self, other: Self) -> Self {
        match self {
            Self::Continue => match other {
                Self::Continue => Self::Continue,
                Self::Pause => Self::Pause,
                Self::Complete => Self::Complete,
            },
            Self::Pause => {
                match other {
                    // If we should pause, but we are told to continue, we will prioritize pause.
                    Self::Continue => Self::Pause,
                    Self::Pause => Self::Pause,
                    // If we should pause, but we are told to complete, we will prioritize complete.
                    Self::Complete => Self::Complete,
                }
            }
            Self::Complete => {
                match other {
                    // If we should complete, but we are told to continue, we will prioritize complete.
                    Self::Continue => Self::Complete,
                    // If we should complete, but we are told to pause, we will prioritize complete.
                    Self::Pause => Self::Complete,
                    Self::Complete => Self::Complete,
                }
            }
        }
    }
}

/// Hook trait for customizing apply loop behavior.
///
/// [`ApplyLoopHook`] allows external components to inject custom logic into
/// the apply loop processing cycle.
pub trait ApplyLoopHook {
    /// Called to process tables that are currently synchronizing.
    ///
    /// This hook coordinates with table sync workers and manages the transition
    /// between initial sync and continuous replication
    fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> impl Future<Output = EtlResult<ApplyLoopAction>> + Send;

    /// Called when a table encounters an error during replication.
    ///
    /// This hook handles error reporting and retry logic for failed tables.
    fn mark_table_errored(
        &self,
        table_replication_error: TableReplicationError,
    ) -> impl Future<Output = EtlResult<ApplyLoopAction>> + Send;

    /// Called to check if the events processed by this apply loop should be applied in the destination.
    ///
    /// Returns `true` if the event should be applied, `false` otherwise.
    fn should_apply_changes(
        &self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> impl Future<Output = EtlResult<bool>> + Send;

    /// Returns the [`WorkerType`] driving this instance of the apply loop.
    fn worker_type(&self) -> WorkerType;
}

/// Tracks the progress of logical replication from PostgreSQL.
///
/// This struct maintains the LSN positions used for sending status updates to PostgreSQL
/// and for coordinating table synchronization. PostgreSQL uses these positions to determine
/// which WAL segments can be safely pruned.
#[derive(Debug, Clone)]
struct ReplicationProgress {
    /// The highest LSN received from PostgreSQL so far.
    ///
    /// This value is updated for every received replication message. For each message,
    /// it is set to the maximum of `start_lsn` and `end_lsn`, where:
    /// * `start_lsn` is the LSN at which PostgreSQL started decoding the message
    ///   (Keep alive messages do not have a `start_lsn` field).
    /// * `end_lsn` is the LSN immediately after the last WAL record decoded for the message.
    ///
    /// This LSN represents the furthest WAL position that has been successfully received
    /// by the replicator. It is reported to PostgreSQL as the `write` position in status updates.
    last_received_lsn: PgLsn,
    /// The highest commit LSN that has been durably flushed to the destination.
    ///
    /// This value is updated only when a batch is flushed. When flushing, it is set to the
    /// last `end_lsn` among all `Commit` messages contained in the batch. If a batch
    /// contains no `Commit` messages, the previously stored value is retained.
    ///
    /// This LSN is reported to PostgreSQL as the `flush` position in status updates to indicate
    /// that all WAL positions strictly less than this LSN have been safely persisted. PostgreSQL
    /// may then consider those WAL segments eligible for cleanup and vacuuming.
    last_flush_lsn: PgLsn,
}

impl ReplicationProgress {
    /// Updates the last received LSN to a higher value if the new LSN is greater.
    fn update_last_received_lsn(&mut self, new_lsn: PgLsn) {
        if new_lsn <= self.last_received_lsn {
            return;
        }

        // This invariant is important since if `last_flush_lsn` becomes bigger, it means that there
        // was a problem during replication.
        debug_assert!(self.last_received_lsn >= self.last_flush_lsn);

        self.last_received_lsn = new_lsn;
    }

    /// Updates the last flush LSN to a higher value if the new LSN is greater.
    fn update_last_flush_lsn(&mut self, new_lsn: PgLsn) {
        if new_lsn <= self.last_flush_lsn {
            return;
        }

        // This invariant is important since if `last_flush_lsn` becomes bigger, it means that there
        // was a problem during replication.
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

/// Result returned from `handle_replication_message` and related functions
#[derive(Debug, Default)]
struct HandleMessageResult {
    /// The event converted from the replication message.
    /// Could be None if this event should not be added to the batch
    /// Will be None in the following cases:
    ///
    /// * When the apply worker receives an event for a table which is not ready.
    /// * When the apply or table sync workers receive an event from a table which is errored.
    /// * When the table sync worker receives an event from a table other than its own.
    /// * When the message is a primary keepalive message.
    event: Option<Event>,
    /// Set to a commit message's end_lsn value, `None` otherwise.
    ///
    /// This value is used to update the last commit end lsn in the [`ApplyLoopState`] which is
    /// helpful to track progress at transaction boundaries.
    end_lsn: Option<PgLsn>,
    /// Set when a batch should be ended earlier than the normal batching parameters of
    /// max size and max fill duration. Currently, this will be set in the following
    /// conditions:
    ///
    /// * Set to [`EndBatch::Inclusive`]` when a commit message indicates that it will
    ///   mark the table sync worker as caught up. We want to end the batch in this
    ///   case because we do not want to sent events after this commit message because
    ///   these events will also be sent by the apply worker later, leading to
    ///   duplicate events being sent. The commit event will be included in the
    ///   batch.
    /// * Set to [`EndBatch::Exclusive`] when a replication message indicates a change
    ///   in schema. Since currently we are not handling any changes in schema, we
    ///   mark the table as skipped in this case. The replication event will be excluded
    ///   from the batch.
    end_batch: Option<EndBatch>,
    /// Set when the table has encountered an error, and it should consequently be marked as errored
    /// in the state store.
    ///
    /// This error is a "caught" error, meaning that it doesn't crash the apply loop, but it makes it
    /// continue or gracefully stop based on the worker type that runs the loop.
    ///
    /// Other errors that make the apply loop fail, will be propagated to the caller and handled differently
    /// based on the worker that runs the loop:
    /// - Apply worker -> the error will make the apply loop crash, which will be propagated to the
    ///   worker and up if the worker is awaited.
    /// - Table sync worker -> the error will make the apply loop crash, which will be propagated
    ///   to the worker, however the error will be caught and persisted via the observer mechanism
    ///   in place for the table sync workers.
    table_replication_error: Option<TableReplicationError>,
    /// The action that this event should have on the loop.
    ///
    /// Note that this action might be overridden by operations that are happening when a batch is flushed
    /// and that can also return loop actions. Those actions will be merged with this action using the
    /// [`ApplyLoopAction::merge`] method.
    action: ApplyLoopAction,
}

impl HandleMessageResult {
    /// Creates a result with no event and no side effects.
    ///
    /// Use this when a replication message should be ignored or has been
    /// fully handled without producing an [`Event`].
    fn no_event() -> Self {
        Self::default()
    }

    /// Creates a result that returns an event without affecting batch state.
    ///
    /// The returned event will be appended to the current batch by the caller.
    fn return_event(event: Event) -> Self {
        Self {
            event: Some(event),
            ..Default::default()
        }
    }

    /// Creates a result that excludes the current event and requests batch termination.
    ///
    /// Used when the current message triggers a recoverable table-level error.
    /// The error is propagated to be handled by the apply loop hook.
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
    /// The highest LSN received from the `end_lsn` field of a `Commit` message.
    ///
    /// This LSN is used to determine the next WAL entry that we should receive from Postgres in case
    /// of restarts and allows Postgres to determine whether some old entries could be pruned from the
    /// WAL.
    last_commit_end_lsn: Option<PgLsn>,
    /// The LSN of the commit WAL entry of the transaction that is currently being processed.
    ///
    /// This LSN is set at every `BEGIN` of a new transaction, and it's used to know the `commit_lsn`
    /// of the transaction which is currently being processed.
    remote_final_lsn: Option<PgLsn>,
    /// The current replication progress tracking received and flushed LSN positions.
    replication_progress: ReplicationProgress,
    /// A batch of events to send to the destination.
    events_batch: Vec<Event>,
    /// Instant from when a transaction began.
    current_tx_begin_ts: Option<Instant>,
    /// Number of events observed in the current transaction (excluding BEGIN/COMMIT).
    current_tx_events: u64,
    /// Boolean representing whether the shutdown was requested but could not be processed because
    /// a transaction was running.
    ///
    /// When a shutdown has been deferred, the apply loop will continue processing events until a
    /// transaction boundary is found. If not found, the process will continue until it is killed via
    /// a `SIGKILL`.
    shutdown_deferred: bool,
    /// The deadline by which the current batch must be flushed.
    ///
    /// This deadline is set when the first message of a batch is received. When the deadline
    /// expires, the batch is unconditionally flushed regardless of size. The deadline is reset
    /// (set to `None`) after each flush.
    batch_flush_deadline: Option<Instant>,
    /// The maximum duration to wait before forcibly flushing a batch.
    max_batch_fill_duration: Duration,
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial replication progress and event batch.
    ///
    /// This constructor initializes the state tracking structure used throughout
    /// the apply loop to maintain replication progress and coordinate batching.
    fn new(
        replication_progress: ReplicationProgress,
        max_batch_size: usize,
        max_batch_fill_duration: Duration,
    ) -> Self {
        Self {
            last_commit_end_lsn: None,
            remote_final_lsn: None,
            replication_progress,
            events_batch: Vec::with_capacity(max_batch_size),
            current_tx_begin_ts: None,
            current_tx_events: 0,
            shutdown_deferred: false,
            batch_flush_deadline: None,
            max_batch_fill_duration,
        }
    }

    /// Starts the batch flush timer if not already running.
    ///
    /// This method is called when the first event of a batch is received. If a deadline
    /// is already set, this is a no-op.
    fn start_batch_timer_if_needed(&mut self) {
        if self.batch_flush_deadline.is_some() {
            return;
        }

        self.batch_flush_deadline = Some(Instant::now() + self.max_batch_fill_duration);

        debug!("started batch flush timer");
    }

    /// Resets the batch flush deadline.
    ///
    /// This method is called after a batch is flushed.
    fn reset_batch_deadline(&mut self) {
        self.batch_flush_deadline = None;

        debug!("reset batch flush timer");
    }

    /// Updates the last commit end LSN to track transaction boundaries.
    ///
    /// This method maintains the highest commit end LSN seen, which represents
    /// the next position to resume from after a transaction completes. Only
    /// advances the LSN forward to ensure progress monotonicity.
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

    /// Returns the effective flush LSN that should be reported to the PostgreSQL server.
    ///
    /// This method computes the correct flush LSN based on the current replication
    /// state and the presence of in-flight transactions and pending events.
    ///
    /// When the replicator is idle, meaning no active transaction and no buffered
    /// events, the flush LSN is advanced to the last received LSN. This prevents WAL
    /// retention for replicated tables that are not receiving writes.
    ///
    /// # Advancing the Flush LSN for Idle Replication
    ///
    /// If there are no outstanding changes to flush, we safely report `last_received_lsn`
    /// as flushed. Doing so allows PostgreSQL to advance the replication slot's
    /// `restart_lsn`, even if replicated tables are idle.
    ///
    /// Consider the following scenario:
    /// - Table A is replicated but idle
    /// - Table B is not replicated and receives continuous writes
    ///
    /// Without advancing the flush LSN, the slot's `restart_lsn` would remain pinned
    /// at Table A's last activity, despite WAL continuing to grow due to Table B.
    /// This can lead to unnecessary WAL accumulation and, in extreme cases, slot
    /// invalidation when `max_slot_wal_keep_size` is exceeded.
    ///
    /// By advancing the flush LSN when idle, we explicitly acknowledge that all
    /// replicated changes up to `last_received_lsn` have been processed, allowing PostgreSQL
    /// to safely reclaim WAL.
    ///
    /// # Use During Table Sync Outside Transactions
    ///
    /// This method is also used when syncing tables outside an explicit
    /// transaction. In that case, events are driven by keepalive messages.
    ///
    /// When processing keepalives with no active transaction, we advance the flush
    /// LSN to the value reported by the server. This ensures forward progress of the
    /// slot while remaining consistent with what has actually been flushed.
    ///
    /// If a batch of events is non-empty, we do not advance the flush LSN to
    /// `last_received_lsn`. Reporting a higher LSN than what has been flushed would
    /// incorrectly mark WAL segments as processed even though they are not.
    ///
    /// # Returns
    ///
    /// The highest LSN for which all replicated changes have been flushed and can be
    /// safely discarded by PostgreSQL.
    fn effective_flush_lsn(&self) -> PgLsn {
        if !self.handling_transaction() && self.events_batch.is_empty() {
            // Report last_received_lsn as flush LSN when idle to prevent WAL buildup. Since the
            // `send_status_update` method tracks the last sent data, we should not worry about the
            // LSN being reported first at a higher position and then later at a lower position.
            self.replication_progress.last_received_lsn
        } else {
            self.replication_progress.last_flush_lsn
        }
    }

    /// Returns true if the apply loop is in the middle of processing a transaction, false otherwise.
    ///
    /// This method checks whether a transaction is currently active by examining
    /// if `remote_final_lsn` is set, which indicates a `BEGIN` message was processed
    /// but the corresponding `COMMIT` has not yet been handled.
    fn handling_transaction(&self) -> bool {
        self.remote_final_lsn.is_some()
    }
}

/// Starts the main apply loop for processing replication events.
///
/// This function implements the core replication processing algorithm that maintains
/// consistency between Postgres and destination systems. It orchestrates multiple
/// concurrent operations while ensuring ACID properties are preserved.
///
/// # Algorithm Overview
///
/// The apply loop processes three types of events:
/// 1. **Replication messages** - DDL/DML events from Postgres logical replication
/// 2. **Table sync signals** - Coordination events from table synchronization workers
/// 3. **Shutdown signals** - Graceful termination requests from the pipeline
///
/// # Processing Phases
///
/// ## 1. Initialization Phase
/// - Establishes logical replication stream from Postgres.
/// - Initializes batch processing state and status tracking.
///
/// ## 2. Event Processing Phase
/// - **Message handling**: Processes replication messages in transaction-aware batches.
/// - **Batch management**: Accumulates events until batch size/time limits are reached.
/// - **Status updates**: Periodically reports progress back to Postgres.
/// - **Coordination**: Manages table sync worker lifecycle and state transitions.
///
/// # Concurrency Model
///
/// The loop uses `tokio::select!` to handle multiple asynchronous operations:
/// - **Priority handling**: Shutdown signals have the highest priority (biased select)
/// - **Message streaming**: Continuously processes replication stream
/// - **Periodic operations**: Status updates and housekeeping every 1 second
/// - **External coordination**: Responds to table sync worker signals
#[expect(clippy::too_many_arguments)]
pub async fn start_apply_loop<S, D, T>(
    pipeline_id: PipelineId,
    start_lsn: PgLsn,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    schema_store: S,
    destination: D,
    hook: T,
    mut shutdown_rx: ShutdownRx,
) -> EtlResult<ApplyLoopResult>
where
    S: SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    info!(
        worker_type = %hook.worker_type(),
        %start_lsn,
        "starting apply loop",
    );

    // The first status update is defaulted from the start lsn since at this point we haven't
    // processed anything.
    let initial_progress = ReplicationProgress {
        last_received_lsn: start_lsn,
        last_flush_lsn: start_lsn,
    };

    // We compute the slot name for the replication slot that we are going to use for the logical
    // replication. At this point we assume that the slot already exists.
    let slot_name: String = hook
        .worker_type()
        .build_etl_replication_slot(pipeline_id)
        .try_into()?;

    // We start the logical replication stream with the supplied parameters at a given lsn. That
    // lsn is the last lsn from which we need to start fetching events.
    let logical_replication_stream = replication_client
        .start_logical_replication(&config.publication_name, &slot_name, start_lsn)
        .await?;

    // We wrap the logical replication stream with EventsStream to expose special status update
    // methods on the stream.
    let events_stream = EventsStream::wrap(logical_replication_stream, pipeline_id);

    pin!(events_stream);

    // We initialize the shared state used throughout the loop to track progress.
    // The state includes a timer registry that persists across loop iterations.
    let mut state = ApplyLoopState::new(
        initial_progress,
        config.batch.max_size,
        Duration::from_millis(config.batch.max_fill_ms),
    );

    // Main event processing loop, continues until shutdown or fatal error
    loop {
        tokio::select! {
            // Use biased selection to enforce priority ordering:
            // 1. Shutdown (highest priority)
            // 2. Batch flush timer expiry
            // 3. Incoming events (lowest priority)
            biased;

            // PRIORITY 1: Handle shutdown signals.
            // When shutdown is requested, we check if we are in a transaction, if we are, we discard
            // the shutdown and gracefully stop when a transaction boundary is found.
            _ = shutdown_rx.changed() => {
                // If the shutdown is being deferred, we don't want to handle it again.
                if state.shutdown_deferred {
                    info!("shutdown already requested, continuing due to running transaction");

                    continue;
                }

                // If we are not inside a transaction, we can cleanly stop streaming and return.
                if !state.handling_transaction() {
                    info!("shutting down apply loop outside transaction");

                    return Ok(ApplyLoopResult::Paused);
                }

                info!("deferring shutdown until transaction boundary");

                state.shutdown_deferred = true;
            }

            // PRIORITY 2: Handle batch flush timer expiry.
            _ = async {
                match state.batch_flush_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            } => {
                info!(
                    batch_size = state.events_batch.len(),
                    "batch flush timer expired, flushing batch",
                );

                let action = flush_batch(
                    &mut state,
                    &destination,
                    &hook,
                    config.batch.max_size,
                    pipeline_id,
                )
                .await?;

                if let Some(result) = action.to_result() {
                    return Ok(result);
                }
            }

            // PRIORITY 3: Process incoming replication messages from Postgres.
            // This is the primary data flow, converts replication protocol messages
            // into typed events and accumulates them into batches for efficient processing.
            message = events_stream.next() => {
                let Some(message) = message else {
                    // The stream returned None, which should never happen for logical replication
                    // since it runs indefinitely. This indicates either the connection was closed
                    // or something unexpected occurred.
                    if replication_client.is_closed() {
                        warn!("replication stream ended due to closed postgres connection");

                        bail!(
                            ErrorKind::SourceConnectionFailed,
                            "PostgreSQL connection has been closed during the apply loop"
                        )
                    } else {
                        warn!("replication stream ended unexpectedly");

                        bail!(
                            ErrorKind::SourceConnectionFailed,
                            "Replication stream ended unexpectedly during the apply loop"
                        )
                    }
                };

                let action = handle_replication_message_and_flush(
                    &mut state,
                    events_stream.as_mut(),
                    message?,
                    &schema_store,
                    &destination,
                    &hook,
                    config.batch.max_size,
                    pipeline_id
                )
                .await?;

                // Start the batch timer if we have events and no timer is running.
                // If the batch was flushed, reset_batch_deadline already canceled the timer.
                if !state.events_batch.is_empty() {
                    state.start_batch_timer_if_needed();
                }

                if let Some(result) = action.to_result() {
                    return Ok(result);
                }
            }
        }
    }
}

/// Handles a replication message and flushes the batch if necessary.
///
/// This function processes a replication message, adds it to the batch if applicable,
/// and flushes the batch when the size limit is reached or when `end_batch` is signaled.
/// After processing each message, it processes syncing tables outside of transactions.
#[expect(clippy::too_many_arguments)]
async fn handle_replication_message_and_flush<S, D, T>(
    state: &mut ApplyLoopState,
    mut events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_store: &S,
    destination: &D,
    hook: &T,
    max_batch_size: usize,
    pipeline_id: PipelineId,
) -> EtlResult<ApplyLoopAction>
where
    S: SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let result = handle_replication_message(
        state,
        events_stream.as_mut(),
        message,
        schema_store,
        hook,
        pipeline_id,
    )
    .await?;

    // If we have an event, and we want to keep it, we add it to the batch and update the last
    // commit lsn (if any).
    let should_include_event = matches!(result.end_batch, None | Some(EndBatch::Inclusive));
    if let Some(event) = result.event
        && should_include_event
    {
        state.events_batch.push(event);
        state.update_last_commit_end_lsn(result.end_lsn);
    }

    // The action that we want to perform is by default the action attached to the message.
    let mut action = result.action;

    // Check if we should flush the batch due to the size limit or `end_batch` signal.
    let should_flush = state.events_batch.len() >= max_batch_size || result.end_batch.is_some();
    if should_flush && !state.events_batch.is_empty() {
        info!(
            batch_size = state.events_batch.len(),
            "batch flush condition reached, flushing batch"
        );

        let flush_batch_action =
            flush_batch(state, destination, hook, max_batch_size, pipeline_id).await?;

        action = action.merge(flush_batch_action);
    }

    // If we have a caught table error, we want to mark the table as errored.
    //
    // Note that if we have a failure after marking a table as errored and events will
    // be reprocessed, even the events before the failure will be skipped.
    //
    // Usually in the apply loop, errors are propagated upstream and handled based on if
    // we are in a table sync worker or apply worker, however we have an edge case (for
    // relation messages that change the schema) where we want to mark a table as errored
    // manually, not propagating the error outside the loop, which is going to be handled
    // differently based on the worker:
    // - Apply worker -> will continue the loop skipping the table.
    // - Table sync worker -> will stop the work (as if it had a normal uncaught error).
    // Ideally we would get rid of this since it's an anomalous case which adds unnecessary
    // complexity.
    if let Some(error) = result.table_replication_error {
        let mark_table_errored_action = hook.mark_table_errored(error).await?;
        action = action.merge(mark_table_errored_action);
    }

    // After processing each message, process syncing tables if we are outside a transaction.
    let synchronize_action = try_synchronize_outside_transaction(state, hook).await?;
    action = action.merge(synchronize_action);

    Ok(action)
}

/// Flushes the current batch of events to the destination.
///
/// This is the unified flush method that handles all flush cases:
/// - Batch size limit reached
/// - `end_batch` signal received
/// - Timer expired
///
/// After flushing, it resets the batch timer and performs synchronization.
async fn flush_batch<D, T>(
    state: &mut ApplyLoopState,
    destination: &D,
    hook: &T,
    max_batch_size: usize,
    pipeline_id: PipelineId,
) -> EtlResult<ApplyLoopAction>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    send_batch_to_destination(state, destination, max_batch_size, pipeline_id).await?;

    // Reset the batch deadline after flushing (the timer will be cancelled in the main loop).
    state.reset_batch_deadline();

    // Once the batch is sent, we have the guarantee that all events up to this point have
    // been durably persisted, so we do synchronization.
    //
    // If we were to synchronize for every event, we would risk data loss since we would notify
    // Postgres about our progress of events processing without having those events durably
    // persisted in the destination.
    try_synchronize_after_batch(state, hook).await
}

/// Sends the current batch of events to the destination and updates metrics.
///
/// Swaps out the in-memory batch to avoid reallocations, persists the events
/// via [`Destination::write_events`], and records counters and timings.
async fn send_batch_to_destination<D>(
    state: &mut ApplyLoopState,
    destination: &D,
    max_batch_size: usize,
    pipeline_id: PipelineId,
) -> EtlResult<()>
where
    D: Destination + Clone + Send + 'static,
{
    // If there are no events in the batch, we can skip sending them to the destination.
    if state.events_batch.is_empty() {
        return Ok(());
    }

    // TODO: figure out if we can send a slice to the destination instead of a vec
    //  that would allow use to avoid new allocations of the `events_batch` vec and
    //  we could just call clear() on it.
    let events_batch =
        std::mem::replace(&mut state.events_batch, Vec::with_capacity(max_batch_size));

    let batch_size = events_batch.len();
    info!(batch_size = batch_size, "sending batch to destination");

    let before_sending = Instant::now();

    destination.write_events(events_batch).await?;

    counter!(
        ETL_EVENTS_PROCESSED_TOTAL,
        WORKER_TYPE_LABEL => "apply",
        ACTION_LABEL => "table_streaming",
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
    )
    .increment(batch_size as u64);

    let send_duration_seconds = before_sending.elapsed().as_secs_f64();
    histogram!(
        ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
        WORKER_TYPE_LABEL => "apply",
        ACTION_LABEL => "table_streaming",
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DESTINATION_LABEL => D::name(),
    )
    .record(send_duration_seconds);

    Ok(())
}

/// Forces synchronization outside normal `COMMIT` boundaries.
///
/// The forcing of synchronization is necessary for the progress of the system when there are no
/// transactions flowing through.
async fn try_synchronize_outside_transaction<T>(
    state: &mut ApplyLoopState,
    hook: &T,
) -> EtlResult<ApplyLoopAction>
where
    T: ApplyLoopHook,
{
    // Table state transitions can only occur at transaction boundaries to maintain consistency, thus
    // only if we are not handling a transaction, we force the synchronization of tables, in all the
    // other cases we defer the sync processing until the current transaction completes.
    if state.handling_transaction() {
        debug!("skipping table sync processing because of in progress transaction");

        return Ok(ApplyLoopAction::Continue);
    }

    let current_lsn = state.effective_flush_lsn();
    info!(worker_type = %hook.worker_type(), %current_lsn, "processing syncing tables outside transaction");

    // With this synchronization, we use `effective_flush_lsn()`, which returns the highest LSN that we can
    // safely treat as “durably flushed” given the current state of the loop.
    //
    // We use this method because we want to notify workers with the most advanced LSN that is still
    // guaranteed to have been persisted. In particular, Postgres keepalives may carry a WAL position
    // that is ahead of the last commit we have actually flushed, so we must be careful not to report
    // progress too early.
    //
    // Example timeline ([x] is LSN x and data is accumulated in memory before flushing):
    //   [1] BEGIN
    //   [2] INSERT
    //   [3] INSERT
    //   [4] COMMIT
    //   [5] KEEPALIVE
    //   (flush)
    //   [6] KEEPALIVE
    //
    // Assume the INSERTs have not been flushed yet. When we receive the first KEEPALIVE, we have:
    //   last_received_lsn = 5
    //   last_flush_lsn = 0 (assuming 0 is the start_lsn when the apply loop is called)
    //
    // Even though we have received WAL up to 5, we have not durably flushed the transaction yet, so
    // the safe LSN to expose to syncing tables is still 0.
    //
    // Now we flush the data for the transaction. This updates `last_flush_lsn` to 4 (simplified: in
    // practice the bookkeeping is slightly more involved).
    //
    // When the next KEEPALIVE arrives, we may observe something like:
    //   last_received_lsn = 6
    //   last_flush_lsn = 4 (now points to the LSN of the commit)
    //
    // At this point there is no transaction in progress, and we have flushed everything up to the last
    // commit. In this “outside a transaction” state, it is safe for `effective_flush_lsn()` to return
    // the keepalive LSN (6) as the effective point of durable progress, even though `last_flush_lsn`
    // itself only advances on commit boundaries after flushing.
    //
    // This syncing step is crucial for the system to make progress when we are not actively inside
    // a transaction. Without it, the system could stall since it will never process syncing tables
    // if no transactions are being replicated.
    let action = hook.process_syncing_tables(current_lsn, true).await?;

    Ok(action)
}

/// Performs post-batch synchronization and progress reporting.
///
/// Updates the next status update LSNs (flush and apply) after a batch has
/// been durably written, and calls [`ApplyLoopHook::process_syncing_tables`]
/// to advance table synchronization state. Returns `true` if the caller
/// should terminate the loop based on hook feedback.
async fn try_synchronize_after_batch<T>(
    state: &mut ApplyLoopState,
    hook: &T,
) -> EtlResult<ApplyLoopAction>
where
    T: ApplyLoopHook,
{
    // At this point, the `last_commit_end_lsn` will contain the LSN of the next byte in the WAL after
    // the last `COMMIT` message that was processed in this batch or in the previous ones.
    //
    // We take the entry here, since we want to avoid issuing `process_syncing_tables` multiple times
    // with the same LSN, with `take` this can't happen under the assumption that the next LSN will be
    // strictly greater.
    let Some(last_commit_end_lsn) = state.last_commit_end_lsn.take() else {
        return Ok(ApplyLoopAction::Continue);
    };

    // We also prepare the next status update for Postgres, where we will confirm that we flushed
    // data up to this LSN to allow for WAL pruning on the database side.
    //
    // Note that we do this ONLY once a batch is fully saved, since that is the only place where
    // we are guaranteed that data has been safely persisted. In all the other cases, we just update
    // the `last_received_lsn` which Postgres uses to get an acknowledgement of how far we have
    // processed messages but not flushed them.
    state
        .replication_progress
        .update_last_flush_lsn(last_commit_end_lsn);

    // We call `process_syncing_tables` with `update_state` set to true here *after* we've received
    // and ack for the batch from the destination. This is important to keep a consistent state.
    // Without this order it could happen that the table's state was updated but sending the batch
    // to the destination failed.
    //
    // For this loop, we use the `last_flush_lsn` as LSN instead of the `last_commit_end_lsn` just
    // because we want to semantically process syncing tables with the same LSN that we tell
    // Postgres that we flushed durably to disk. In practice, `last_flush_lsn` and `last_commit_end_lsn`
    // will be always equal, since LSNs are guaranteed to be monotonically increasing.
    let current_lsn = state.replication_progress.last_flush_lsn;
    info!(worker_type = %hook.worker_type(), %current_lsn, "processing syncing tables after batch flush");
    hook.process_syncing_tables(current_lsn, true).await
}

/// Dispatches replication protocol messages to appropriate handlers.
///
/// This function serves as the main routing mechanism for Postgres replication
/// messages, distinguishing between XLogData (containing actual logical replication
/// events) and PrimaryKeepAlive (heartbeat and status) messages.
///
/// For XLogData messages, it extracts LSN boundaries and delegates to logical
/// replication processing. For keepalive messages, it responds with status updates
/// to maintain the replication connection and inform Postgres of progress.
async fn handle_replication_message<S, T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_store: &S,
    hook: &T,
    pipeline_id: PipelineId,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    match message {
        ReplicationMessage::XLogData(message) => {
            let start_lsn = PgLsn::from(message.wal_start());
            state
                .replication_progress
                .update_last_received_lsn(start_lsn);

            // The `end_lsn` here is the LSN of the last byte in the WAL that was processed by the
            // server, and it's different from the `end_lsn` found in the `Commit` message.
            let end_lsn = PgLsn::from(message.wal_end());
            state.replication_progress.update_last_received_lsn(end_lsn);

            debug!(
                %start_lsn,
                %end_lsn,
                "handling logical replication data message",
            );

            handle_logical_replication_message(
                state,
                start_lsn,
                message.into_data(),
                schema_store,
                hook,
                pipeline_id,
            )
            .await
        }
        ReplicationMessage::PrimaryKeepAlive(message) => {
            let end_lsn = PgLsn::from(message.wal_end());
            state.replication_progress.update_last_received_lsn(end_lsn);

            debug!(
                message = ?message,
                "handling logical replication keep alive message",
            );

            events_stream
                .send_status_update(
                    state.last_received_lsn(),
                    state.effective_flush_lsn(),
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
///
/// This function handles the core logic of transforming Postgres's logical
/// replication protocol messages into strongly-typed [`Event`] instances. It
/// determines transaction boundaries, validates message ordering, and routes
/// each message type to its specialized handler.
///
/// The function ensures proper LSN tracking by combining start LSN (for WAL
/// position) with commit LSN (for transaction ordering) to maintain both
/// temporal and transactional consistency in the event stream.
async fn handle_logical_replication_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: LogicalReplicationMessage,
    schema_store: &S,
    hook: &T,
    pipeline_id: PipelineId,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    state.current_tx_events += 1;

    match &message {
        LogicalReplicationMessage::Begin(begin_body) => {
            handle_begin_message(state, start_lsn, begin_body).await
        }
        LogicalReplicationMessage::Commit(commit_body) => {
            handle_commit_message(state, start_lsn, commit_body, hook, pipeline_id).await
        }
        LogicalReplicationMessage::Relation(relation_body) => {
            handle_relation_message(state, start_lsn, relation_body, schema_store, hook).await
        }
        LogicalReplicationMessage::Insert(insert_body) => {
            handle_insert_message(
                state,
                start_lsn,
                insert_body,
                hook,
                schema_store,
                pipeline_id,
            )
            .await
        }
        LogicalReplicationMessage::Update(update_body) => {
            handle_update_message(
                state,
                start_lsn,
                update_body,
                hook,
                schema_store,
                pipeline_id,
            )
            .await
        }
        LogicalReplicationMessage::Delete(delete_body) => {
            handle_delete_message(
                state,
                start_lsn,
                delete_body,
                hook,
                schema_store,
                pipeline_id,
            )
            .await
        }
        LogicalReplicationMessage::Truncate(truncate_body) => {
            handle_truncate_message(state, start_lsn, truncate_body, hook).await
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

/// Handles Postgres BEGIN messages that mark transaction boundaries.
///
/// This function processes transaction start events by validating the event type
/// and storing the final LSN for the transaction. The final LSN represents where
/// the transaction will commit in the WAL, enabling proper transaction ordering
/// and consistency maintenance.
///
/// The stored `remote_final_lsn` is used by subsequent message handlers to ensure
/// all events within the transaction share the same commit boundary identifier.
async fn handle_begin_message(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::BeginBody,
) -> EtlResult<HandleMessageResult> {
    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    let final_lsn = PgLsn::from(message.final_lsn());
    state.remote_final_lsn = Some(final_lsn);

    // Track begin instant and reset tx event count.
    state.current_tx_begin_ts = Some(Instant::now());
    state.current_tx_events = 0;

    // Convert event from the protocol message.
    let event = parse_event_from_begin_message(start_lsn, final_lsn, message);

    Ok(HandleMessageResult::return_event(Event::Begin(event)))
}

/// Handles Postgres COMMIT messages that complete transactions.
///
/// This function processes transaction completion events by validating LSN
/// consistency, coordinating with table synchronization workers, and preparing
/// the transaction boundary information for batch processing.
///
/// The function ensures the commit LSN matches the expected final LSN from the
/// corresponding BEGIN message, maintaining transaction integrity. It also
/// determines whether this commit should trigger worker termination (for table
/// sync workers reaching their target LSN).
async fn handle_commit_message<T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::CommitBody,
    hook: &T,
    pipeline_id: PipelineId,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    // We take the LSN that belongs to the current transaction, however, if there is no
    // LSN, it means that a `Begin` message was not received before this `Commit` which means
    // we are in an inconsistent state.
    let Some(remote_final_lsn) = state.remote_final_lsn.take() else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction state",
            "Transaction must be active before processing COMMIT message"
        );
    };

    // If the commit lsn of the message is different from the remote final lsn, it means that the
    // transaction that was started expect a different commit lsn in the commit message. In this case,
    // we want to bail assuming we are in an inconsistent state.
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

    let end_lsn = PgLsn::from(message.end_lsn());

    // Track metrics after the end of the transaction. If we arrive here, we assume that the begin
    // ts was active.
    if let Some(begin_ts) = state.current_tx_begin_ts.take() {
        let now = Instant::now();
        let duration_seconds = (now - begin_ts).as_secs_f64();
        histogram!(
            ETL_TRANSACTION_DURATION_SECONDS,
            PIPELINE_ID_LABEL => pipeline_id.to_string()
        )
        .record(duration_seconds);

        counter!(
            ETL_TRANSACTIONS_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string()
        )
        .increment(1);

        // We do - 1 since we exclude this COMMIT event from the count.
        histogram!(
            ETL_TRANSACTION_SIZE,
            PIPELINE_ID_LABEL => pipeline_id.to_string()
        )
        .record((state.current_tx_events - 1) as f64);

        state.current_tx_events = 0;
    }

    // We call `process_syncing_tables` with `update_state` set to false here because we do not yet want
    // to update the table state. This function will be called again in `handle_replication_message_batch`
    // with `update_state` set to true *after* sending the batch to the destination. This order is needed
    // for consistency because otherwise we might update the table state before receiving an ack from the
    // destination.
    let mut action = hook.process_syncing_tables(end_lsn, false).await?;

    // If the shutdown was deferred, and we have just processed a `COMMIT` message, we now want to pause
    // the apply loop.
    if state.shutdown_deferred {
        action = action.merge(ApplyLoopAction::Pause);
    }

    // Convert event from the protocol message.
    let event = parse_event_from_commit_message(start_lsn, commit_lsn, message);

    let mut result = HandleMessageResult {
        event: Some(Event::Commit(event)),
        // The rationale for using only the `end_lsn` of the `COMMIT` message is that once we found a
        // commit and successfully processed it, we can say that the next byte we want is the next transaction
        // since if we were to store an intermediate `end_lsn` (from a dml operation within a transaction)
        // the replication will still start from a transaction boundary, that is, a `Begin` statement in
        // our case.
        end_lsn: Some(end_lsn),
        action,
        ..Default::default()
    };

    // If we are told to stop/pause the loop, it means we reached the end of processing for this specific
    // worker, so we gracefully stop processing the batch, but we include in the batch the last processed
    // element, in this case the `COMMIT` message.
    if action.is_terminating() {
        result.end_batch = Some(EndBatch::Inclusive);
    }

    Ok(result)
}

/// Handles Postgres RELATION messages that describe table schemas.
///
/// This function processes schema definition messages by validating that table
/// schemas haven't changed unexpectedly during replication. Schema stability
/// is critical for maintaining data consistency between source and destination.
///
/// When schema changes are detected, the function creates appropriate error
/// conditions and signals batch termination to prevent processing of events
/// with mismatched schemas. This protection mechanism ensures data integrity
/// by failing fast on incompatible schema evolution.
async fn handle_relation_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::RelationBody,
    schema_store: &S,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction state",
            "Transaction must be active before processing RELATION message"
        );
    };

    let table_id = TableId::new(message.rel_id());

    if !hook
        .should_apply_changes(table_id, remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::no_event());
    }

    // If no table schema is found, it means that something went wrong since we should have schemas
    // ready before starting the apply loop.
    let existing_table_schema =
        schema_store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found in cache",
                    format!("Table schema for table {} not found in cache", table_id)
                )
            })?;

    // Convert event from the protocol message.
    let event = parse_event_from_relation_message(start_lsn, remote_final_lsn, message)?;

    // We compare the table schema from the relation message with the existing schema (if any).
    // The purpose of this comparison is that we want to throw an error and stop the processing
    // of any table that incurs in a schema change after the initial table sync is performed.
    if !existing_table_schema.partial_eq(&event.table_schema) {
        let error = TableReplicationError::with_solution(
            table_id,
            format!("The schema for table {table_id} has changed during streaming"),
            "ETL doesn't support schema changes at this point in time, rollback the schema",
            RetryPolicy::ManualRetry,
        );

        return Ok(HandleMessageResult::finish_batch_and_exclude_event(error));
    }

    Ok(HandleMessageResult::return_event(Event::Relation(event)))
}

/// Handles Postgres INSERT messages for row insertion events.
async fn handle_insert_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::InsertBody,
    hook: &T,
    schema_store: &S,
    pipeline_id: PipelineId,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction state",
            "Transaction must be active before processing INSERT message"
        );
    };

    if !hook
        .should_apply_changes(TableId::new(message.rel_id()), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::no_event());
    }

    // Convert event from the protocol message.
    let event = parse_event_from_insert_message(
        schema_store,
        start_lsn,
        remote_final_lsn,
        message,
        pipeline_id,
    )
    .await?;

    Ok(HandleMessageResult::return_event(Event::Insert(event)))
}

/// Handles Postgres UPDATE messages for row modification events.
async fn handle_update_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::UpdateBody,
    hook: &T,
    schema_store: &S,
    pipeline_id: PipelineId,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction state",
            "Transaction must be active before processing UPDATE message"
        );
    };

    if !hook
        .should_apply_changes(TableId::new(message.rel_id()), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::no_event());
    }

    // Convert event from the protocol message.
    let event = parse_event_from_update_message(
        schema_store,
        start_lsn,
        remote_final_lsn,
        message,
        pipeline_id,
    )
    .await?;

    Ok(HandleMessageResult::return_event(Event::Update(event)))
}

/// Handles Postgres DELETE messages for row removal events.
async fn handle_delete_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::DeleteBody,
    hook: &T,
    schema_store: &S,
    pipeline_id: PipelineId,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction state",
            "Transaction must be active before processing DELETE message"
        );
    };

    if !hook
        .should_apply_changes(TableId::new(message.rel_id()), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::no_event());
    }

    // Convert event from the protocol message.
    let event = parse_event_from_delete_message(
        schema_store,
        start_lsn,
        remote_final_lsn,
        message,
        pipeline_id,
    )
    .await?;

    Ok(HandleMessageResult::return_event(Event::Delete(event)))
}

/// Handles Postgres TRUNCATE messages for bulk table clearing operations.
///
/// This function processes table truncation events by validating the event type,
/// ensuring transaction context, and filtering the affected table list based on
/// hook decisions. Since TRUNCATE can affect multiple tables simultaneously,
/// it evaluates each table individually.
async fn handle_truncate_message<T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::TruncateBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction state",
            "Transaction must be active before processing TRUNCATE message"
        );
    };

    // We collect only the relation ids for which we are allow to apply changes, thus in this case
    // the truncation.
    let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
    for &table_id in message.rel_ids().iter() {
        if hook
            .should_apply_changes(TableId::new(table_id), remote_final_lsn)
            .await?
        {
            rel_ids.push(table_id)
        }
    }
    // If nothing to apply, skip conversion entirely
    if rel_ids.is_empty() {
        return Ok(HandleMessageResult::no_event());
    }

    // Convert event from the protocol message.
    let event = parse_event_from_truncate_message(start_lsn, remote_final_lsn, message, rel_ids);

    Ok(HandleMessageResult::return_event(Event::Truncate(event)))
}
