use etl_config::shared::PipelineConfig;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::types::TableId;
use futures::StreamExt;
use metrics::histogram;
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::concurrency::shutdown::ShutdownRx;
use crate::concurrency::signal::SignalRx;
use crate::concurrency::stream::{TimeoutStream, TimeoutStreamResult};
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
    ETL_EVENTS_PROCESSED_TOTAL, ETL_EVENTS_RECEIVED_TOTAL, ETL_TRANSACTION_DURATION_SECONDS,
    ETL_TRANSACTION_SIZE, PIPELINE_ID_LABEL, WORKER_TYPE_LABEL,
};
use crate::replication::client::PgReplicationClient;
use crate::replication::stream::EventsStream;
use crate::state::table::{RetryPolicy, TableReplicationError};
use crate::store::schema::SchemaStore;
use crate::types::{Event, PipelineId};
use crate::{bail, etl_error};

/// The minimum interval (in milliseconds) between consecutive status updates.
///
/// This value must be less than the `wal_sender_timeout` configured in the Postgres instance.
/// If set too high, Postgres may timeout before the next status update is sent.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

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
#[derive(Debug, Copy, Clone)]
pub enum ApplyLoopAction {
    /// The apply loop can continue on the next element.
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

impl Default for ApplyLoopAction {
    fn default() -> Self {
        Self::Continue
    }
}

/// Hook trait for customizing apply loop behavior.
///
/// [`ApplyLoopHook`] allows external components to inject custom logic into
/// the apply loop processing cycle.
pub trait ApplyLoopHook {
    /// Called before the main apply loop begins processing.
    fn before_loop(
        &self,
        start_lsn: PgLsn,
    ) -> impl Future<Output = EtlResult<ApplyLoopAction>> + Send;

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

/// The status update that is sent to Postgres to report progress.
///
/// The status update is a crucial part since it enables Postgres to know our replication process
/// and is required for WAL pruning.
#[derive(Debug, Clone)]
struct StatusUpdate {
    write_lsn: PgLsn,
    flush_lsn: PgLsn,
}

impl StatusUpdate {
    /// Updates the write LSN to a higher value if the new LSN is greater.
    fn update_write_lsn(&mut self, new_write_lsn: PgLsn) {
        if new_write_lsn <= self.write_lsn {
            return;
        }

        self.write_lsn = new_write_lsn;
    }

    /// Updates the flush LSN to a higher value if the new LSN is greater.
    fn update_flush_lsn(&mut self, flush_lsn: PgLsn) {
        if flush_lsn <= self.flush_lsn {
            return;
        }

        self.flush_lsn = flush_lsn;
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
    ///and that can also return loop actions. Those actions will be merged with this action using the
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
    /// The LSNs of the status update that we want to send to Postgres.
    next_status_update: StatusUpdate,
    /// A batch of events to send to the destination.
    events_batch: Vec<Event>,
    /// Deadline for when the next status update must be dispatched.
    status_update_deadline: Instant,
    /// Instant from when a transaction began.
    current_tx_begin_ts: Option<Instant>,
    /// Number of events observed in the current transaction (excluding BEGIN/COMMIT).
    current_tx_events: u64,
    /// Boolean representing whether the shutdown was requested but could not be processed because
    /// a transaction was running.
    ///
    /// When a shutdown has been discarded, the apply loop will continue processing events until a
    /// transaction boundary is found. If not found, the process will continue until it is killed via
    /// a `SIGKILL`.
    shutdown_discarded: bool,
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial status update and event batch.
    ///
    /// This constructor initializes the state tracking structure used throughout
    /// the apply loop to maintain replication progress and coordinate batching.
    fn new(next_status_update: StatusUpdate, events_batch: Vec<Event>) -> Self {
        Self {
            last_commit_end_lsn: None,
            remote_final_lsn: None,
            next_status_update,
            events_batch,
            status_update_deadline: Instant::now() + STATUS_UPDATE_INTERVAL,
            current_tx_begin_ts: None,
            current_tx_events: 0,
            shutdown_discarded: false,
        }
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

    /// Returns the LSN that should be reported as written to the PostgresSQL server.
    fn write_lsn(&self) -> PgLsn {
        self.next_status_update.write_lsn
    }

    /// Returns the LSN that should be reported as flushed to the PostgreSQL server.
    ///
    /// This method determines the appropriate flush LSN based on the current replication state.
    /// When no transaction is being processed and no events are pending, it advances the flush
    /// LSN to match the write LSN, preventing WAL buildup on idle replicated tables.
    ///
    /// # WAL Advancement for Idle Tables
    ///
    /// When there are no outstanding transactions to flush (i.e., we're not inside a transaction
    /// and have no pending events), we advance flush_lsn to match write_lsn. This allows the
    /// replication slot's restart_lsn to advance even when replicated tables are idle.
    ///
    /// Without this advancement, consider a scenario where:
    /// - Table A (replicated): idle, receiving no writes
    /// - Table B (not replicated): actively receiving writes
    ///
    /// The restart_lsn would remain stalled at Table A's position despite Table B generating
    /// new WAL entries. This stall could cause:
    /// 1. WAL accumulation on the primary
    /// 2. Potential slot invalidation if WAL exceeds `max_slot_wal_keep_size`
    ///
    /// By advancing flush_lsn, we signal that all replicated data up to write_lsn has been
    /// processed, allowing PostgreSQL to safely advance restart_lsn and reclaim WAL.
    ///
    /// # Returns
    ///
    /// The LSN up to which all changes have been flushed and can be safely discarded by
    /// the PostgreSQL server.
    fn flush_lsn(&self) -> PgLsn {
        if !self.handling_transaction() && self.events_batch.is_empty() {
            // Advance flush_lsn to write_lsn when idle to prevent WAL buildup.
            self.next_status_update.write_lsn
        } else {
            self.next_status_update.flush_lsn
        }
    }

    /// Records that a status update was sent and schedules the next refresh deadline.
    fn mark_status_update_sent(&mut self) {
        self.status_update_deadline = Instant::now() + STATUS_UPDATE_INTERVAL;
    }

    /// Returns true when the status update deadline has elapsed.
    fn status_update_due(&self) -> bool {
        Instant::now() >= self.status_update_deadline
    }

    /// Returns the deadline for the next status update as a Tokio instant.
    fn next_status_update_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::from_std(self.status_update_deadline)
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

/// An [`EventsStream`] which is wrapped inside a [`TimeoutStream`] for timing purposes.
type TimeoutEventsStream =
    TimeoutStream<EtlResult<ReplicationMessage<LogicalReplicationMessage>>, EventsStream>;

/// Result type for reading from [`TimeoutEventsStream`].
///
/// Wraps either a replication message value or a timeout marker used to
/// trigger batch flushes when no new events arrive within the deadline.
type TimeoutEventsStreamResult =
    TimeoutStreamResult<EtlResult<ReplicationMessage<LogicalReplicationMessage>>>;

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
/// - Validates hook requirements via `before_loop()` callback.
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
    mut force_syncing_tables_rx: Option<SignalRx>,
) -> EtlResult<ApplyLoopResult>
where
    S: SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    info!(
        "starting apply loop in worker '{:?}' from lsn {}",
        hook.worker_type(),
        start_lsn
    );

    // We call the `before_loop` hook and stop the loop immediately in case we are told to stop.
    let action = hook.before_loop(start_lsn).await?;
    if let Some(result) = action.to_result() {
        info!(
            "no need to run apply loop for worker '{:?}', the loop will terminate",
            hook.worker_type()
        );
        return Ok(result);
    }

    // The first status update is defaulted from the start lsn since at this point we haven't
    // processed anything.
    let first_status_update = StatusUpdate {
        write_lsn: start_lsn,
        flush_lsn: start_lsn,
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

    // Maximum time to wait for additional events when batching (prevents indefinite delays)
    let max_batch_fill_duration = Duration::from_millis(config.batch.max_fill_ms);

    // We wrap the logical replication stream with multiple streams:
    // - EventsStream -> used to expose special status updates methods on the stream.
    // - TimeoutStream -> adds a timeout mechanism that detects when no data has been going through
    //   the stream for a while, and returns a special marker to signal that.
    let logical_replication_stream = EventsStream::wrap(logical_replication_stream);
    let logical_replication_stream =
        TimeoutStream::wrap(logical_replication_stream, max_batch_fill_duration);

    pin!(logical_replication_stream);

    // We initialize the shared state used throughout the loop to track progress.
    let mut state = ApplyLoopState::new(
        first_status_update,
        Vec::with_capacity(config.batch.max_size),
    );

    // Main event processing loop - continues until shutdown or fatal error
    loop {
        tokio::select! {
            // Use biased selection to prioritize shutdown signals over other operations
            // This ensures graceful shutdown takes precedence over event processing
            biased;

            // PRIORITY 1: Handle shutdown signals.
            // When shutdown is requested, we check if we are in a transaction, if we are, we discard
            // the shutdown and gracefully stop when a transaction boundary is found.
            _ = shutdown_rx.changed() => {
                // If the shutdown is being discarded, we don't want to handle it again.
                if state.shutdown_discarded {
                    info!("shutdown was already requested but it has been discarded because of a running transaction");
                    continue;
                }

                // If we are not inside a transaction, we can cleanly stop streaming and return.
                if !state.handling_transaction() {
                    info!("shutting down apply worker while waiting for incoming events outside of a transaction");
                    return Ok(ApplyLoopResult::Paused);
                }

                info!("discarding shutdown because of a running transaction, the apply loop will shut down on the next transaction boundary");

                state.shutdown_discarded = true;
            }

            // PRIORITY 2: Process incoming replication messages from Postgres.
            // This is the primary data flow, converts replication protocol messages
            // into typed events and accumulates them into batches for efficient processing.
            Some(message) = logical_replication_stream.next() => {
                let action = handle_replication_message_with_timeout(
                    &mut state,
                    logical_replication_stream.as_mut(),
                    message,
                    &schema_store,
                    &destination,
                    &hook,
                    config.batch.max_size,
                    pipeline_id
                )
                .await?;

                // After processing each message, we explicitly check for the status update deadline.
                // This is necessary because message processing time is unbounded, so Postgres could
                // timeout before we receive a primary keep-alive message in the stream.
                //
                // By performing this check here, we minimize the risk of missing the status update.
                // However, in rare cases where a single message's processing exceeds the timeout,
                // the error could still occur but this scenario is highly unlikely.
                if state.status_update_due() {
                    logical_replication_stream
                        .as_mut()
                        .get_inner()
                        .send_status_update(
                            state.write_lsn(),
                            state.flush_lsn(),
                            false
                        )
                        .await?;
                    state.mark_status_update_sent();
                }

                if let Some(result) = action.to_result() {
                    return Ok(result);
                }
            }

            // PRIORITY 3: Handle table synchronization coordination signals.
            // Table sync workers signal when they complete initial data copying and are ready
            // to transition to continuous replication mode. Guard the branch so it stays
            // dormant if no signal receiver was provided.
            _ = async {
                if let Some(rx) = force_syncing_tables_rx.as_mut() {
                    let _ = rx.changed().await;
                }
            }, if force_syncing_tables_rx.is_some() => {
                // Table state transitions can only occur at transaction boundaries to maintain consistency.
                // If we're in the middle of processing a transaction (`remote_final_lsn` is set),
                // we defer the sync processing until the current transaction completes.
                if !state.handling_transaction() {
                    debug!("forcefully processing syncing tables");

                    let action = hook.process_syncing_tables(state.next_status_update.flush_lsn, true).await?;
                    if let Some(result) = action.to_result() {
                        return Ok(result);
                    }
                } else {
                    debug!("skipping forced table sync processing because of in progress transaction");
                }
            }

            // PRIORITY 4: Periodic housekeeping and Postgres status updates.
            // Every REFRESH_INTERVAL (1 second), send progress updates back to Postgres
            // This serves multiple purposes:
            // 1. Keeps Postgres informed of our processing progress
            // 2. Allows Postgres to clean up old WAL files based on our progress
            // 3. Provides a heartbeat mechanism to detect connection issues
            _ = tokio::time::sleep_until(state.next_status_update_deadline()) => {
                logical_replication_stream
                    .as_mut()
                    .get_inner()
                    .send_status_update(
                        state.write_lsn(),
                        state.flush_lsn(),
                        false
                    )
                    .await?;
                state.mark_status_update_sent();
            }
        }
    }
}

/// Handles a replication message or a timeout.
///
/// The rationale for having a value or timeout is to handle for the cases where a batch can't
/// be filled within a reasonable time bound. In that case, the stream will timeout, signaling a
/// force flush of the current events in the batch.
///
/// This function performs synchronization under the assumption that transaction boundary events are
/// always processed and never skipped.
#[expect(clippy::too_many_arguments)]
async fn handle_replication_message_with_timeout<S, D, T>(
    state: &mut ApplyLoopState,
    mut events_stream: Pin<&mut TimeoutEventsStream>,
    result: TimeoutEventsStreamResult,
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
    match result {
        TimeoutStreamResult::Value(message) => {
            let result = handle_replication_message(
                state,
                events_stream.as_mut(),
                message?,
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

                metrics::counter!(
                    ETL_EVENTS_RECEIVED_TOTAL,
                    WORKER_TYPE_LABEL => "apply",
                    ACTION_LABEL => "table_streaming",
                    PIPELINE_ID_LABEL => pipeline_id.to_string(),
                    DESTINATION_LABEL => D::name(),
                )
                .increment(1);
            }

            // TODO: check if this is what we want.
            // The action that we want to perform is by default the action attached to the message.
            let mut action = result.action;

            // If we have elements in the batch, and we have reached the max batch size, or we are told
            // to end the batch, we send it.
            if state.events_batch.len() >= max_batch_size || result.end_batch.is_some() {
                // We check if the batch has elements. It can be that a batch has no elements when
                // the batch is ended prematurely, and it contains only skipped events. In this case,
                // we don't produce any events to the destination but downstream code treats it as if
                // those evets are "persisted".
                if !state.events_batch.is_empty() {
                    send_batch(
                        state,
                        events_stream.as_mut(),
                        destination,
                        max_batch_size,
                        pipeline_id,
                    )
                    .await?;
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
                    action = action.merge(hook.mark_table_errored(error).await?);
                }

                // Once the batch is sent, we have the guarantee that all events up to this point have
                // been durably persisted, so we do synchronization.
                //
                // If we were to synchronize for every event, we would risk data loss since we would notify
                // Postgres about our progress of events processing without having those events durably
                // persisted in the destination.
                action = action.merge(synchronize(state, hook).await?);
            }

            Ok(action)
        }
        TimeoutStreamResult::Timeout => {
            debug!(
                "the events stream timed out before reaching batch size of {}, ready to flush batch of {} events",
                max_batch_size,
                state.events_batch.len()
            );

            if !state.events_batch.is_empty() {
                // We send the non-empty batch.
                send_batch(
                    state,
                    events_stream.as_mut(),
                    destination,
                    max_batch_size,
                    pipeline_id,
                )
                .await?;
            }

            // We perform synchronization to make sure that tables are synced.
            synchronize(state, hook).await
        }
    }
}

/// Sends the current batch of events to the destination and updates metrics.
///
/// Swaps out the in-memory batch to avoid reallocations, persists the events
/// via [`Destination::write_events`], records counters and timings, and resets
/// the stream timeout to continue batching.
async fn send_batch<D>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut TimeoutEventsStream>,
    destination: &D,
    max_batch_size: usize,
    pipeline_id: PipelineId,
) -> EtlResult<()>
where
    D: Destination + Clone + Send + 'static,
{
    // TODO: figure out if we can send a slice to the destination instead of a vec
    //  that would allow use to avoid new allocations of the `events_batch` vec and
    //  we could just call clear() on it.
    let events_batch =
        std::mem::replace(&mut state.events_batch, Vec::with_capacity(max_batch_size));

    let batch_size = events_batch.len();
    info!("sending batch of {} events to destination", batch_size);

    let before_sending = Instant::now();

    destination.write_events(events_batch).await?;

    metrics::counter!(
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

    // We tell the stream to reset the timer when it is polled the next time, this way the deadline
    // is restarted.
    events_stream.mark_reset_timer();

    Ok(())
}

/// Performs post-batch synchronization and progress reporting.
///
/// Updates the next status update LSNs (flush and apply) after a batch has
/// been durably written, and calls [`ApplyLoopHook::process_syncing_tables`]
/// to advance table synchronization state. Returns `true` if the caller
/// should terminate the loop based on hook feedback.
async fn synchronize<T>(state: &mut ApplyLoopState, hook: &T) -> EtlResult<ApplyLoopAction>
where
    T: ApplyLoopHook,
{
    // At this point, the `last_commit_end_lsn` will contain the LSN of the next byte in the WAL after
    // the last `Commit` message that was processed in this batch or in the previous ones.
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
    // the `write_lsn` which is used by Postgres to get an acknowledgement of how far we have processed
    // messages but not flushed them.
    debug!(
        "updating lsn for next status update to {}",
        last_commit_end_lsn
    );
    state
        .next_status_update
        .update_flush_lsn(last_commit_end_lsn);

    // We call `process_syncing_tables` with `update_state` set to true here *after* we've received
    // and ack for the batch from the destination. This is important to keep a consistent state.
    // Without this order it could happen that the table's state was updated but sending the batch
    // to the destination failed.
    //
    // For this loop, we use the `flush_lsn` as LSN instead of the `last_commit_end_lsn` just
    // because we want to semantically process syncing tables with the same LSN that we tell
    // Postgres that we flushed durably to disk. In practice, `flush_lsn` and `last_commit_end_lsn`
    // will be always equal, since LSNs are guaranteed to be monotonically increasing.
    hook.process_syncing_tables(state.next_status_update.flush_lsn, true)
        .await
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
    events_stream: Pin<&mut TimeoutEventsStream>,
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
            state.next_status_update.update_write_lsn(start_lsn);

            // The `end_lsn` here is the LSN of the last byte in the WAL that was processed by the
            // server, and it's different from the `end_lsn` found in the `Commit` message.
            let end_lsn = PgLsn::from(message.wal_end());
            state.next_status_update.update_write_lsn(end_lsn);

            debug!(
                "handling logical replication data message (start_lsn: {}, end_lsn: {})",
                start_lsn, end_lsn
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
            state.next_status_update.update_write_lsn(end_lsn);

            debug!(
                "handling logical replication status update message (end_lsn: {})",
                end_lsn
            );

            events_stream
                .get_inner()
                .send_status_update(state.write_lsn(), state.flush_lsn(), message.reply() == 1)
                .await?;

            state.mark_status_update_sent();

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
            handle_insert_message(state, start_lsn, insert_body, hook, schema_store).await
        }
        LogicalReplicationMessage::Update(update_body) => {
            handle_update_message(state, start_lsn, update_body, hook, schema_store).await
        }
        LogicalReplicationMessage::Delete(delete_body) => {
            handle_delete_message(state, start_lsn, delete_body, hook, schema_store).await
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

    // If we are told to continue processing but the shutdown was discarded, it means that we should
    // stop the apply loop. On the other hand, if we are already told to either stop or complete,
    // we will honor that decision.
    if let ApplyLoopAction::Continue = action
        && state.shutdown_discarded
    {
        action = ApplyLoopAction::Pause;
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

    // If we are told to stop the loop, it means we reached the end of processing for this specific
    // worker, so we gracefully stop processing the batch, but we include in the batch the last processed
    // element, in this case the `Commit` message.
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
    let event =
        parse_event_from_insert_message(schema_store, start_lsn, remote_final_lsn, message).await?;

    Ok(HandleMessageResult::return_event(Event::Insert(event)))
}

/// Handles Postgres UPDATE messages for row modification events.
async fn handle_update_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::UpdateBody,
    hook: &T,
    schema_store: &S,
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
    let event =
        parse_event_from_update_message(schema_store, start_lsn, remote_final_lsn, message).await?;

    Ok(HandleMessageResult::return_event(Event::Update(event)))
}

/// Handles Postgres DELETE messages for row removal events.
async fn handle_delete_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: &protocol::DeleteBody,
    hook: &T,
    schema_store: &S,
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
    let event =
        parse_event_from_delete_message(schema_store, start_lsn, remote_final_lsn, message).await?;

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
