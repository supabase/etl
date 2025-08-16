use etl_config::shared::PipelineConfig;
use etl_postgres::replication::slots::get_slot_name;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::schema::TableId;
use futures::{FutureExt, StreamExt};
use metrics::{counter, gauge};
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
use crate::conversions::event::{Event, EventType, convert_message_to_event};
use crate::destination::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::metrics::{
    ETL_APPLY_EVENTS_COPIED_TOTAL, ETL_BATCH_SEND_MILLISECONDS_TOTAL, ETL_BATCH_SIZE,
};
use crate::replication::client::PgReplicationClient;
use crate::replication::stream::EventsStream;
use crate::state::table::{RetryPolicy, TableReplicationError};
use crate::store::schema::SchemaStore;
use crate::types::PipelineId;
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

/// The status update that is sent to PostgreSQL to report progress.
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
    ///
    /// This method ensures LSN values only advance forward, preventing
    /// regression in replication progress reporting to PostgreSQL.
    fn update_write_lsn(&mut self, new_write_lsn: PgLsn) {
        if new_write_lsn <= self.write_lsn {
            return;
        }

        self.write_lsn = new_write_lsn;
    }

    /// Updates the flush LSN to a higher value if the new LSN is greater.
    ///
    /// This method tracks the highest LSN for data that has been durably
    /// written to the destination, enabling PostgreSQL WAL cleanup.
    fn update_flush_lsn(&mut self, flush_lsn: PgLsn) {
        if flush_lsn <= self.flush_lsn {
            return;
        }

        self.flush_lsn = flush_lsn;
    }

    /// Updates the apply LSN to a higher value if the new LSN is greater.
    ///
    /// This method tracks the highest LSN for data that has been successfully
    /// applied to the destination system with all constraints satisfied.
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
    /// Last time when the batch was sent (or since when the apply loop started).
    last_batch_send_time: Instant,
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
            last_batch_send_time: Instant::now(),
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

/// Starts the main apply loop for processing replication events.
///
/// This function implements the core replication processing algorithm that maintains
/// consistency between PostgreSQL and destination systems. It orchestrates multiple
/// concurrent operations while ensuring ACID properties are preserved.
///
/// # Algorithm Overview
///
/// The apply loop processes three types of events:
/// 1. **Replication messages** - DDL/DML events from PostgreSQL logical replication
/// 2. **Table sync signals** - Coordination events from table synchronization workers  
/// 3. **Shutdown signals** - Graceful termination requests from the pipeline
///
/// # Processing Phases
///
/// ## 1. Initialization Phase
/// - Validates hook requirements via `before_loop()` callback.
/// - Establishes logical replication stream from PostgreSQL.
/// - Initializes batch processing state and status tracking.
///
/// ## 2. Event Processing Phase
/// - **Message handling**: Processes replication messages in transaction-aware batches.
/// - **Batch management**: Accumulates events until batch size/time limits are reached.
/// - **Status updates**: Periodically reports progress back to PostgreSQL.
/// - **Coordination**: Manages table sync worker lifecycle and state transitions.
///
/// # Concurrency Model
///
/// The loop uses `tokio::select!` to handle multiple asynchronous operations:
/// - **Priority handling**: Shutdown signals have highest priority (biased select)
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
    let logical_replication_stream = EventsStream::wrap(logical_replication_stream);

    pin!(logical_replication_stream);

    // We initialize the shared state that is used throughout the loop to track progress.
    let mut state = ApplyLoopState::new(
        first_status_update,
        Vec::with_capacity(config.batch.max_size),
    );

    // Maximum time to wait for additional events when batching (prevents indefinite delays)
    let max_batch_fill_duration = Duration::from_millis(config.batch.max_fill_ms);

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

            // PRIORITY 2: Process incoming replication messages from PostgreSQL
            // This is the primary data flow - converts replication protocol messages
            // into typed events and accumulates them into batches for efficient processing
            Some(message) = logical_replication_stream.next() => {
                let end_loop = handle_replication_message_batch(
                    &mut state,
                    logical_replication_stream.as_mut(),
                    message?,
                    &schema_store,
                    &destination,
                    &hook,
                    config.batch.max_size,
                    max_batch_fill_duration,
                )
                .await?;

                // If the message handler indicates we should terminate (e.g., due to hook decision)/
                if end_loop {
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

            // PRIORITY 4: Periodic housekeeping and PostgreSQL status updates
            // Every REFRESH_INTERVAL (1 second), send progress updates back to PostgreSQL
            // This serves multiple purposes:
            // 1. Keeps PostgreSQL informed of our processing progress
            // 2. Allows PostgreSQL to clean up old WAL files based on our progress
            // 3. Provides a heartbeat mechanism to detect connection issues
            _ = tokio::time::sleep(REFRESH_INTERVAL) => {
                logical_replication_stream.as_mut()
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

/// Processes a replication message and manages batch accumulation and sending.
#[expect(clippy::too_many_arguments)]
async fn handle_replication_message_batch<S, D, T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_store: &S,
    destination: &D,
    hook: &T,
    max_batch_size: usize,
    max_batch_fill_duration: Duration,
) -> EtlResult<bool>
where
    S: SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let result =
        handle_replication_message(state, events_stream, message, schema_store, hook).await?;

    if let Some(event) = result.event
        && matches!(result.end_batch, None | Some(EndBatch::Inclusive))
    {
        state.events_batch.push(event);
        state.update_last_commit_end_lsn(result.end_lsn);
    }

    try_send_batch(
        state,
        result.end_batch,
        result.table_replication_error,
        destination,
        hook,
        max_batch_size,
        max_batch_fill_duration,
    )
    .await
}

/// Evaluates whether to send the current event batch and executes the send operation.
///
/// This function implements the batching strategy by checking multiple conditions:
/// size limits, timing constraints, and forced batch completion signals. When any
/// condition is met, it sends the accumulated events to the destination and updates
/// the replication state accordingly.
///
/// After confirming that events are durably persisted in the destination, it performs synchronization
/// between all workers and notifies Postgres about the progress.
async fn try_send_batch<D, T>(
    state: &mut ApplyLoopState,
    end_batch: Option<EndBatch>,
    table_replication_error: Option<TableReplicationError>,
    destination: &D,
    hook: &T,
    max_batch_size: usize,
    max_batch_fill_duration: Duration,
) -> EtlResult<bool>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let elapsed = state.last_batch_send_time.elapsed();
    // `elapsed` could be zero in case current time is earlier than `last_batch_send_time`.
    // We send the batch even in this case to make sure `last_batch_send_time` is reset to
    // a new value and to avoid getting stuck with some events in the batch.
    let time_to_send_batch = elapsed.is_zero() || elapsed > max_batch_fill_duration;

    if time_to_send_batch || state.events_batch.len() >= max_batch_size || end_batch.is_some() {
        if !state.events_batch.is_empty() {
            // TODO: figure out if we can send a slice to the destination instead of a vec
            // that would allow use to avoid new allocations of the `events_batch` vec and
            // we could just call clear() on it.
            let events_batch =
                std::mem::replace(&mut state.events_batch, Vec::with_capacity(max_batch_size));

            let num_events = events_batch.len();
            info!("sending batch of {} events to destination", num_events);
            let before_sending = Instant::now();

            destination.write_events(events_batch).await?;

            counter!(ETL_APPLY_EVENTS_COPIED_TOTAL).increment(num_events as u64);
            gauge!(ETL_BATCH_SIZE).set(num_events as f64);
            let time_taken_to_send = before_sending.elapsed().as_millis();
            gauge!(ETL_BATCH_SEND_MILLISECONDS_TOTAL).set(time_taken_to_send as f64);

            state.last_batch_send_time = Instant::now();
        }

        let mut end_loop = false;
        // We handle the action related to a table error. This is done after the batch is written
        // to the destination to make sure that the data is durable before assuming things about a
        // table.
        if let Some(table_replication_error) = table_replication_error {
            end_loop |= !hook.mark_table_errored(table_replication_error).await?;
        }

        // At this point, the `last_commit_end_lsn` will contain the LSN of the next byte in the WAL after
        // the last `Commit` message that was processed in this batch or in the previous ones.
        if let Some(last_commit_end_lsn) = state.last_commit_end_lsn.take() {
            // We also prepare the next status update for Postgres, where we will confirm that we flushed
            // data up to this LSN to allow for WAL pruning on the database side.
            //
            // Note that we do this ONLY once a batch is fully saved, since that is the only place where
            // we are guaranteed that data has been safely persisted. In all the other cases, we just update
            // the `write_lsn` which is used by Postgres to get an acknowledgement of how far we have processed
            // messages but not flushed them.
            // TODO: check if we want to send `apply_lsn` as a different value.
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
            end_loop |= !hook
                .process_syncing_tables(state.next_status_update.flush_lsn, true)
                .await?;
        }

        return Ok(end_loop);
    }

    Ok(false)
}

/// Dispatches replication protocol messages to appropriate handlers.
///
/// This function serves as the main routing mechanism for PostgreSQL replication
/// messages, distinguishing between XLogData (containing actual logical replication
/// events) and PrimaryKeepAlive (heartbeat and status) messages.
///
/// For XLogData messages, it extracts LSN boundaries and delegates to logical
/// replication processing. For keepalive messages, it responds with status updates
/// to maintain the replication connection and inform PostgreSQL of progress.
async fn handle_replication_message<S, T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
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
                .send_status_update(
                    state.next_status_update.write_lsn,
                    state.next_status_update.flush_lsn,
                    state.next_status_update.apply_lsn,
                    message.reply() == 1,
                )
                .await?;

            Ok(HandleMessageResult::default())
        }
        _ => Ok(HandleMessageResult::default()),
    }
}

/// Processes logical replication messages and converts them to typed events.
///
/// This function handles the core logic of transforming PostgreSQL's logical
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
    // We perform the conversion of the message to our own event format which is used downstream
    // by the destination.
    //
    // It's important to note that we use the `start_lsn` and `commit_lsn` as LSNs for tracking the
    // position of the event in the WAL. The `start_lsn` defines total order within the WAL but with
    // `commit_lsn` we can also encode information about the transaction order since we might have
    // an entry with `start_lsn` greater than another but because logical replication sends transactions
    // in the order of commit, the actual insert could happen before.
    let commit_lsn = get_commit_lsn(state, &message)?;
    let event = convert_message_to_event(schema_store, start_lsn, commit_lsn, &message).await?;

    let event_type = EventType::from(&event);
    debug!("message converted to event type {}", event_type);

    match message {
        LogicalReplicationMessage::Begin(message) => {
            handle_begin_message(state, event, &message).await
        }
        LogicalReplicationMessage::Commit(message) => {
            handle_commit_message(state, event, &message, hook).await
        }
        LogicalReplicationMessage::Relation(message) => {
            handle_relation_message(state, event, &message, schema_store, hook).await
        }
        LogicalReplicationMessage::Insert(message) => {
            handle_insert_message(state, event, &message, hook).await
        }
        LogicalReplicationMessage::Update(message) => {
            handle_update_message(state, event, &message, hook).await
        }
        LogicalReplicationMessage::Delete(message) => {
            handle_delete_message(state, event, &message, hook).await
        }
        LogicalReplicationMessage::Truncate(message) => {
            handle_truncate_message(state, event, &message, hook).await
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

/// Handles PostgreSQL BEGIN messages that mark transaction boundaries.
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
    event: Event,
    message: &protocol::BeginBody,
) -> EtlResult<HandleMessageResult> {
    let EventType::Begin = event.event_type() else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Begin
            )
        );
    };

    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    let final_lsn = PgLsn::from(message.final_lsn());
    state.remote_final_lsn = Some(final_lsn);

    Ok(HandleMessageResult {
        event: Some(event),
        end_lsn: None,
        end_batch: None,
        table_replication_error: None,
    })
}

/// Handles PostgreSQL COMMIT messages that complete transactions.
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
    event: Event,
    message: &protocol::CommitBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let EventType::Commit = event.event_type() else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Commit
            )
        );
    };

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

    // We call `process_syncing_tables` with `update_state` set to false here because we do not yet want
    // to update the table state. This function will be called again in `handle_replication_message_batch`
    // with `update_state` set to true *after* sending the batch to the destination. This order is needed
    // for consistency because otherwise we might update the table state before receiving an ack from the
    // destination.
    let continue_loop = hook.process_syncing_tables(end_lsn, false).await?;

    let mut result = HandleMessageResult {
        event: Some(event),
        // We mark this as the last commit end LSN since we want to be able to track from the outside
        // what was the biggest transaction boundary LSN which was successfully applied.
        //
        // The rationale for using only the `end_lsn` of the `Commit` message is that once we found a
        // commit and successfully processed it, we can say that the next byte we want is the next transaction
        // since if we were to store an intermediate `end_lsn` (from a dml operation within a transaction)
        // the replication will still start from a transaction boundary, that is, a `Begin` statement in
        // our case.
        end_lsn: Some(end_lsn),
        ..Default::default()
    };

    // If we are told to stop the loop, it means we reached the end of processing for this specific
    // worker, so we gracefully stop processing the batch, but we include in the batch the last processed
    // element, in this case the `Commit` message.
    if !continue_loop {
        result.end_batch = Some(EndBatch::Inclusive);
    }

    Ok(result)
}

/// Handles PostgreSQL RELATION messages that describe table schemas.
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
    event: Event,
    message: &protocol::RelationBody,
    schema_store: &S,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    S: SchemaStore + Clone + Send + 'static,
    T: ApplyLoopHook,
{
    let Event::Relation(event) = event else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Relation
            )
        );
    };

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
        return Ok(HandleMessageResult::default());
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

    // We compare the table schema from the relation message with the existing schema (if any).
    // The purpose of this comparison is that we want to throw an error and stop the processing
    // of any table that incurs in a schema change after the initial table sync is performed.
    if !existing_table_schema.partial_eq(&event.table_schema) {
        let table_error = TableReplicationError::with_solution(
            table_id,
            format!("The schema for table {table_id} has changed during streaming"),
            "ETL doesn't support schema changes at this point in time, rollback the schema",
            RetryPolicy::ManualRetry,
        );

        return Ok(HandleMessageResult {
            end_batch: Some(EndBatch::Exclusive),
            table_replication_error: Some(table_error),
            ..Default::default()
        });
    }

    Ok(HandleMessageResult {
        event: Some(Event::Relation(event)),
        end_lsn: None,
        end_batch: None,
        table_replication_error: None,
    })
}

/// Handles PostgreSQL INSERT messages for row insertion events.
async fn handle_insert_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::InsertBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let Event::Insert(event) = event else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Insert
            )
        );
    };

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
        return Ok(HandleMessageResult::default());
    }

    Ok(HandleMessageResult {
        event: Some(Event::Insert(event)),
        end_lsn: None,
        end_batch: None,
        table_replication_error: None,
    })
}

/// Handles PostgreSQL UPDATE messages for row modification events.
async fn handle_update_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::UpdateBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let Event::Update(event) = event else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Update
            )
        );
    };

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
        return Ok(HandleMessageResult::default());
    }

    Ok(HandleMessageResult {
        event: Some(Event::Update(event)),
        end_lsn: None,
        end_batch: None,
        table_replication_error: None,
    })
}

/// Handles PostgreSQL DELETE messages for row removal events.
async fn handle_delete_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::DeleteBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let Event::Delete(event) = event else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Delete
            )
        );
    };

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
        return Ok(HandleMessageResult::default());
    }

    Ok(HandleMessageResult {
        event: Some(Event::Delete(event)),
        end_lsn: None,
        end_batch: None,
        table_replication_error: None,
    })
}

/// Handles PostgreSQL TRUNCATE messages for bulk table clearing operations.
///
/// This function processes table truncation events by validating the event type,
/// ensuring transaction context, and filtering the affected table list based on
/// hook decisions. Since TRUNCATE can affect multiple tables simultaneously,
/// it evaluates each table individually.
async fn handle_truncate_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::TruncateBody,
    hook: &T,
) -> EtlResult<HandleMessageResult>
where
    T: ApplyLoopHook,
{
    let Event::Truncate(mut event) = event else {
        bail!(
            ErrorKind::ValidationError,
            "Invalid event",
            format!(
                "An invalid event {event:?} was received (expected {:?})",
                EventType::Truncate
            )
        );
    };

    let Some(remote_final_lsn) = state.remote_final_lsn else {
        bail!(
            ErrorKind::InvalidState,
            "Invalid transaction",
            "A transaction should have started for handle_truncate_message to be performed"
        );
    };

    let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
    for &table_id in message.rel_ids().iter() {
        if hook
            .should_apply_changes(TableId::new(table_id), remote_final_lsn)
            .await?
        {
            rel_ids.push(table_id)
        }
    }
    event.rel_ids = rel_ids;

    Ok(HandleMessageResult {
        event: Some(Event::Truncate(event)),
        end_lsn: None,
        end_batch: None,
        table_replication_error: None,
    })
}
