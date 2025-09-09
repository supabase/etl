use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::get_slot_name;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::types::TableId;
use futures::{FutureExt, StreamExt};
use metrics::{counter, gauge, histogram};
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::{Future, pending};
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
    APPLY, ETL_BATCH_SEND_DURATION_SECONDS, ETL_BATCH_SIZE, ETL_ITEMS_COPIED_TOTAL, MILLIS_PER_SEC,
    PHASE, PIPELINE_ID,
};
use crate::replication::client::PgReplicationClient;
use crate::replication::stream::EventsStream;
use crate::state::table::{RetryPolicy, TableReplicationError};
use crate::store::schema::SchemaStore;
use crate::types::{Event, PipelineId};
use crate::{bail, etl_error};

/// The amount of milliseconds that pass between one refresh and the other of the system, in case no
/// events or shutdown signal are received.
const REFRESH_INTERVAL: Duration = Duration::from_millis(1000);

/// Result type for the apply loop execution.
///
/// [`ApplyLoopResult`] indicates the reason why the apply loop terminated,
/// enabling appropriate cleanup and error handling by the caller.
#[derive(Debug, Copy, Clone)]
pub enum ApplyLoopResult {
    /// The apply loop stopped processing, either due to shutdown or completion
    ApplyStopped,
}

/// Hook trait for customizing apply loop behavior.
///
/// [`ApplyLoopHook`] allows external components to inject custom logic into
/// the apply loop processing cycle.
pub trait ApplyLoopHook {
    /// Called before the main apply loop begins processing.
    ///
    /// This hook allows initialization logic to run before replication starts.
    /// Return `false` to signal that the loop should terminate early.
    fn before_loop(&self, start_lsn: PgLsn) -> impl Future<Output = EtlResult<bool>> + Send;

    /// Called to process tables that are currently synchronizing.
    ///
    /// This hook coordinates with table sync workers and manages the transition
    /// between initial sync and continuous replication. Return `false` to signal
    /// that the loop should terminate.
    fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> impl Future<Output = EtlResult<bool>> + Send;

    /// Called when a table encounters an error during replication.
    ///
    /// This hook handles error reporting and retry logic for failed tables.
    /// Return `false` to signal that the loop should terminate due to the error.
    fn mark_table_errored(
        &self,
        table_replication_error: TableReplicationError,
    ) -> impl Future<Output = EtlResult<bool>> + Send;

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
    apply_lsn: PgLsn,
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

    /// Updates the apply LSN to a higher value if the new LSN is greater.
    fn update_apply_lsn(&mut self, apply_lsn: PgLsn) {
        if apply_lsn <= self.apply_lsn {
            return;
        }

        self.apply_lsn = apply_lsn;
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

    /// Creates a result that returns an event and marks a transaction boundary.
    ///
    /// Sets `end_lsn` to the provided value so the caller can update
    /// [`ApplyLoopState`] progress at the end of a transaction.
    fn return_boundary_event(event: Event, end_lsn: PgLsn) -> Self {
        Self {
            event: Some(event),
            end_lsn: Some(end_lsn),
            ..Default::default()
        }
    }

    /// Creates a result that returns an event and requests batch termination.
    ///
    /// The event is included in the batch (`Inclusive`) and `end_lsn` is set
    /// to signal a transaction boundary.
    fn finish_batch_and_return_boundary_event(event: Event, end_lsn: PgLsn) -> Self {
        Self {
            event: Some(event),
            end_lsn: Some(end_lsn),
            end_batch: Some(EndBatch::Inclusive),
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
#[derive(Debug, Clone)]
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
    let continue_loop = hook.before_loop(start_lsn).await?;
    if !continue_loop {
        info!(
            "no need to run apply loop for worker '{:?}', the loop will terminate",
            hook.worker_type()
        );

        return Ok(ApplyLoopResult::ApplyStopped);
    }

    // The first status update is defaulted from the start lsn since at this point we haven't
    // processed anything.
    let first_status_update = StatusUpdate {
        write_lsn: start_lsn,
        flush_lsn: start_lsn,
        apply_lsn: start_lsn,
    };

    // We compute the slot name for the replication slot that we are going to use for the logical
    // replication. At this point we assume that the slot already exists.
    let slot_name = get_slot_name(pipeline_id, hook.worker_type())?;

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

    // We initialize the shared state that is used throughout the loop to track progress.
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

            // PRIORITY 1: Handle shutdown signals immediately
            // When shutdown is requested, we stop processing new events and terminate gracefully
            // This allows current transactions to complete but prevents new ones from starting
            _ = shutdown_rx.changed() => {
                info!("shutting down apply worker while waiting for incoming events");

                return Ok(ApplyLoopResult::ApplyStopped);
            }

            // PRIORITY 2: Process incoming replication messages from Postgres
            // This is the primary data flow - converts replication protocol messages
            // into typed events and accumulates them into batches for efficient processing
            Some(message) = logical_replication_stream.next() => {
                let continue_loop = handle_replication_message_with_timeout(
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
                if !continue_loop {
                    return Ok(ApplyLoopResult::ApplyStopped);
                }
            }

            // PRIORITY 3: Handle table synchronization coordination signals
            // Table sync workers signal when they complete initial data copying and are ready
            // to transition to continuous replication mode. We use map_or_else with pending()
            // to make this branch optional - if no signal receiver exists, this branch never fires.
            _ = force_syncing_tables_rx.as_mut().map_or_else(|| pending().boxed(), |rx| rx.changed().boxed()) => {
                // Table state transitions can only occur at transaction boundaries to maintain consistency.
                // If we're in the middle of processing a transaction (remote_final_lsn is set),
                // we defer the sync processing until the current transaction completes.
                if !state.handling_transaction() {
                    debug!("forcefully processing syncing tables");

                    // Delegate to hook for actual table sync processing
                    // Pass current flush LSN to ensure sync operations use consistent state
                    let continue_loop = hook.process_syncing_tables(state.next_status_update.flush_lsn, true).await?;
                    if !continue_loop {
                        return Ok(ApplyLoopResult::ApplyStopped);
                    }
                } else {
                    debug!("skipping table sync processing - transaction in progress");
                }
            }

            // PRIORITY 4: Periodic housekeeping and Postgres status updates
            // Every REFRESH_INTERVAL (1 second), send progress updates back to Postgres
            // This serves multiple purposes:
            // 1. Keeps Postgres informed of our processing progress
            // 2. Allows Postgres to clean up old WAL files based on our progress
            // 3. Provides a heartbeat mechanism to detect connection issues
            _ = tokio::time::sleep(REFRESH_INTERVAL) => {
                logical_replication_stream
                    .as_mut()
                    .get_inner()
                    .send_status_update(
                        state.next_status_update.write_lsn,
                        state.next_status_update.flush_lsn,
                        state.next_status_update.apply_lsn,
                        false
                    )
                    .await?;
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
) -> EtlResult<bool>
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

            let mut continue_loop = true;

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
                    continue_loop &= hook.mark_table_errored(error).await?;
                }

                // Once the batch is sent, we have the guarantee that all events up to this point have
                // been durably persisted, so we do synchronization.
                //
                // If we were to synchronize for every event, we would risk data loss since we would notify
                // Postgres about our progress of events processing without having those events durably
                // persisted in the destination.
                continue_loop &= synchronize(state, hook).await?;
            }

            Ok(continue_loop)
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

            // We perform synchronization, to make sure that tables are synced.
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

    counter!(ETL_ITEMS_COPIED_TOTAL, PIPELINE_ID => pipeline_id.to_string(), PHASE => APPLY)
        .increment(batch_size as u64);
    gauge!(ETL_BATCH_SIZE, PIPELINE_ID => pipeline_id.to_string()).set(batch_size as f64);

    let send_duration_secs = before_sending.elapsed().as_millis() as f64 / MILLIS_PER_SEC;
    histogram!(ETL_BATCH_SEND_DURATION_SECONDS, PIPELINE_ID => pipeline_id.to_string(), PHASE => APPLY).record(send_duration_secs);

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
async fn synchronize<T>(state: &mut ApplyLoopState, hook: &T) -> EtlResult<bool>
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
        return Ok(true);
    };

    // We also prepare the next status update for Postgres, where we will confirm that we flushed
    // data up to this LSN to allow for WAL pruning on the database side.
    //
    // Note that we do this ONLY once a batch is fully saved, since that is the only place where
    // we are guaranteed that data has been safely persisted. In all the other cases, we just update
    // the `write_lsn` which is used by Postgres to get an acknowledgement of how far we have processed
    // messages but not flushed them.
    // TODO: maybe we want to send `apply_lsn` as a different value.
    debug!(
        "updating lsn for next status update to {}",
        last_commit_end_lsn
    );
    state
        .next_status_update
        .update_flush_lsn(last_commit_end_lsn);
    state
        .next_status_update
        .update_apply_lsn(last_commit_end_lsn);

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
                .send_status_update(
                    state.next_status_update.write_lsn,
                    state.next_status_update.flush_lsn,
                    state.next_status_update.apply_lsn,
                    message.reply() == 1,
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
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let commit_lsn = get_commit_lsn(state, &message)?;

    match &message {
        LogicalReplicationMessage::Begin(begin_body) => {
            handle_begin_message(state, start_lsn, commit_lsn, begin_body).await
        }
        LogicalReplicationMessage::Commit(commit_body) => {
            handle_commit_message(state, start_lsn, commit_lsn, commit_body, hook).await
        }
        LogicalReplicationMessage::Relation(relation_body) => {
            handle_relation_message(
                state,
                start_lsn,
                commit_lsn,
                relation_body,
                schema_store,
                hook,
            )
            .await
        }
        LogicalReplicationMessage::Insert(insert_body) => {
            handle_insert_message(
                state,
                start_lsn,
                commit_lsn,
                insert_body,
                hook,
                schema_store,
            )
            .await
        }
        LogicalReplicationMessage::Update(update_body) => {
            handle_update_message(
                state,
                start_lsn,
                commit_lsn,
                update_body,
                hook,
                schema_store,
            )
            .await
        }
        LogicalReplicationMessage::Delete(delete_body) => {
            handle_delete_message(
                state,
                start_lsn,
                commit_lsn,
                delete_body,
                hook,
                schema_store,
            )
            .await
        }
        LogicalReplicationMessage::Truncate(truncate_body) => {
            handle_truncate_message(state, start_lsn, commit_lsn, truncate_body, hook).await
        }
        LogicalReplicationMessage::Origin(_) => Ok(HandleMessageResult::default()),
        LogicalReplicationMessage::Type(_) => Ok(HandleMessageResult::default()),
        _ => Ok(HandleMessageResult::default()),
    }
}

/// Determines the commit LSN for a replication message based on transaction state.
///
/// This function extracts the appropriate commit LSN depending on the message type.
/// For `BEGIN` messages, it uses the final LSN from the message payload. For all
/// other message types, it retrieves the previously stored `remote_final_lsn`
/// that was set when the transaction began.
fn get_commit_lsn(state: &ApplyLoopState, message: &LogicalReplicationMessage) -> EtlResult<PgLsn> {
    // If we are in a `Begin` message, the `commit_lsn` is the `final_lsn` of the payload, in all the
    // other cases we read the `remote_final_lsn` which should be always set in case we are within or
    // at the end of a transaction (meaning that the event type is different from `Begin`).
    if let LogicalReplicationMessage::Begin(message) = message {
        Ok(PgLsn::from(message.final_lsn()))
    } else {
        state.remote_final_lsn.ok_or_else(|| {
            etl_error!(
                ErrorKind::InvalidState,
                "Invalid transaction",
                "A transaction should have started for get_commit_lsn to be performed"
            )
        })
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
    commit_lsn: PgLsn,
    message: &protocol::BeginBody,
) -> EtlResult<HandleMessageResult> {
    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    let final_lsn = PgLsn::from(message.final_lsn());
    state.remote_final_lsn = Some(final_lsn);

    // Convert event from the protocol message.
    let event = parse_event_from_begin_message(start_lsn, commit_lsn, message);

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
    commit_lsn: PgLsn,
    message: &protocol::CommitBody,
    hook: &T,
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
            "Invalid transaction",
            "A transaction should have started for handle_commit_message to be performed"
        );
    };

    // If the commit lsn of the message is different from the remote final lsn, it means that the
    // transaction that was started expect a different commit lsn in the commit message. In this case,
    // we want to bail assuming we are in an inconsistent state.
    let commit_lsn_msg = PgLsn::from(message.commit_lsn());
    if commit_lsn_msg != remote_final_lsn {
        bail!(
            ErrorKind::ValidationError,
            "Invalid commit LSN",
            format!(
                "Incorrect commit LSN {} in COMMIT message (expected {})",
                commit_lsn_msg, remote_final_lsn
            )
        );
    }

    let end_lsn = PgLsn::from(message.end_lsn());

    // We call `process_syncing_tables` with `update_state` set to false here because we do not yet want
    // to update the table state. This function will be called again in `handle_replication_message_batch`
    // with `update_state` set to true *after* sending the batch to the destination. This order is needed
    // for consistency because otherwise we might update the table state before receiving an ack from the
    // destination.
    let continue_loop = hook.process_syncing_tables(end_lsn, false).await?;

    // Convert event from the protocol message.
    let event = parse_event_from_commit_message(start_lsn, commit_lsn, message);

    // We mark this as the last commit end LSN since we want to be able to track from the outside
    // what was the biggest transaction boundary LSN which was successfully applied.
    //
    // The rationale for using only the `end_lsn` of the `Commit` message is that once we found a
    // commit and successfully processed it, we can say that the next byte we want is the next transaction
    // since if we were to store an intermediate `end_lsn` (from a dml operation within a transaction)
    // the replication will still start from a transaction boundary, that is, a `Begin` statement in
    // our case.
    if continue_loop {
        Ok(HandleMessageResult::return_boundary_event(
            Event::Commit(event),
            end_lsn,
        ))
    } else {
        // If we are told to stop the loop, it means we reached the end of processing for this specific
        // worker, so we gracefully stop processing the batch, but we include in the batch the last processed
        // element, in this case the `Commit` message.
        Ok(HandleMessageResult::finish_batch_and_return_boundary_event(
            Event::Commit(event),
            end_lsn,
        ))
    }
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
    commit_lsn: PgLsn,
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
            "Invalid transaction",
            "A transaction should have started for handle_relation_message to be performed"
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
                    "Table not found in the schema cache",
                    format!("The table schema for table {table_id} was not found in the cache")
                )
            })?;

    // Convert event from the protocol message.
    let event = parse_event_from_relation_message(start_lsn, commit_lsn, message)?;

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
    commit_lsn: PgLsn,
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
            "Invalid transaction",
            "A transaction should have started for handle_insert_message to be performed"
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
        parse_event_from_insert_message(schema_store, start_lsn, commit_lsn, message).await?;

    Ok(HandleMessageResult::return_event(Event::Insert(event)))
}

/// Handles Postgres UPDATE messages for row modification events.
async fn handle_update_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
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
            "Invalid transaction",
            "A transaction should have started for handle_update_message to be performed"
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
        parse_event_from_update_message(schema_store, start_lsn, commit_lsn, message).await?;

    Ok(HandleMessageResult::return_event(Event::Update(event)))
}

/// Handles Postgres DELETE messages for row removal events.
async fn handle_delete_message<S, T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
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
            "Invalid transaction",
            "A transaction should have started for handle_delete_message to be performed"
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
        parse_event_from_delete_message(schema_store, start_lsn, commit_lsn, message).await?;

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
    commit_lsn: PgLsn,
    message: &protocol::TruncateBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction",
            "A transaction should have started for handle_truncate_message to be performed"
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
    let event = parse_event_from_truncate_message(start_lsn, commit_lsn, message, rel_ids);

    Ok(HandleMessageResult::return_event(Event::Truncate(event)))
}
