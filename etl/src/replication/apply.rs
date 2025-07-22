use crate::concurrency::shutdown::ShutdownRx;
use crate::conversions::event::{Event, EventConversionError, EventType, convert_message_to_event};
use crate::destination::base::{Destination, DestinationError};
use crate::pipeline::PipelineId;
use crate::replication::client::{PgReplicationClient, PgReplicationError};
use crate::replication::slot::{SlotError, get_slot_name};
use crate::replication::stream::{EventsStream, EventsStreamError};
use crate::schema::cache::SchemaCache;
use crate::workers::apply::ApplyWorkerHookError;
use crate::workers::base::WorkerType;
use crate::workers::table_sync::TableSyncWorkerHookError;

use crate::concurrency::signal::SignalRx;
use config::shared::PipelineConfig;
use futures::{FutureExt, StreamExt};
use postgres::schema::TableId;
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info};

/// The amount of milliseconds that pass between one refresh and the other of the system, in case no
/// events or shutdown signal are received.
const REFRESH_INTERVAL: Duration = Duration::from_millis(1000);

// TODO: figure out how to break the cycle and remove `Box`.
#[derive(Debug, Error)]
pub enum ApplyLoopError {
    #[error("Apply worker hook operation failed: {0}")]
    ApplyWorkerHook(Box<ApplyWorkerHookError>),

    #[error("Table sync worker hook operation failed: {0}")]
    TableSyncWorkerHook(Box<TableSyncWorkerHookError>),

    #[error("A Postgres replication error occurred in the apply loop: {0}")]
    PgReplication(#[from] PgReplicationError),

    #[error("An error occurred while streaming logical replication changes in the apply loop: {0}")]
    LogicalReplicationStreamFailed(#[from] EventsStreamError),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),

    #[error("An error occurred while building an event from a message in the apply loop: {0}")]
    EventConversion(#[from] EventConversionError),

    #[error("An error occurred when interacting with the destination in the apply loop: {0}")]
    Destination(#[from] DestinationError),

    #[error("A transaction should have started for the action ({0}) to be performed")]
    InvalidTransaction(String),

    #[error("Incorrect commit LSN {0} in COMMIT message (expected {1})")]
    InvalidCommitLsn(PgLsn, PgLsn),

    #[error("An invalid event {0} was received (expected {1})")]
    InvalidEvent(EventType, EventType),

    #[error("The table schema for table {0} was not found in the cache")]
    MissingTableSchema(TableId),

    #[error("The received table schema doesn't match the table schema loaded during table sync")]
    MismatchedTableSchema,
}

impl From<ApplyWorkerHookError> for ApplyLoopError {
    fn from(err: ApplyWorkerHookError) -> Self {
        ApplyLoopError::ApplyWorkerHook(Box::new(err))
    }
}

impl From<TableSyncWorkerHookError> for ApplyLoopError {
    fn from(err: TableSyncWorkerHookError) -> Self {
        ApplyLoopError::TableSyncWorkerHook(Box::new(err))
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ApplyLoopResult {
    ApplyStopped,
}

pub trait ApplyLoopHook {
    type Error: Into<ApplyLoopError>;

    fn before_loop(
        &self,
        start_lsn: PgLsn,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
        update_state: bool,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn skip_table(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn should_apply_changes(
        &self,
        table_id: TableId,
        remote_final_lsn: PgLsn,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn worker_type(&self) -> WorkerType;
}

#[derive(Debug, Clone)]
struct StatusUpdate {
    write_lsn: PgLsn,
    flush_lsn: PgLsn,
    apply_lsn: PgLsn,
}

impl StatusUpdate {
    fn update_write_lsn(&mut self, new_write_lsn: PgLsn) {
        if new_write_lsn <= self.write_lsn {
            return;
        }

        self.write_lsn = new_write_lsn;
    }

    fn update_flush_lsn(&mut self, flush_lsn: PgLsn) {
        if flush_lsn <= self.flush_lsn {
            return;
        }

        self.flush_lsn = flush_lsn;
    }

    fn update_apply_lsn(&mut self, apply_lsn: PgLsn) {
        if apply_lsn <= self.apply_lsn {
            return;
        }

        self.apply_lsn = apply_lsn;
    }
}

/// An enum representing if the batch should be ended or not
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
    /// * When the apply worker receives an event for a table which is not ready
    /// * When the apply or table sync workers receive an event from a table which was skipped
    /// * When the table sync worker receives an event from a table other than its own
    /// * When the message is a primary keepalive message
    ///
    event: Option<Event>,

    /// Set to a commit message's end_lsn value, None otherwise
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
    ///
    end_batch: Option<EndBatch>,

    /// Set when a replication message indicates a change in schema, otherwise None
    skip_table: Option<TableId>,
}

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

    /// Last time when the batch was sent (or since when the apply loop started)
    last_batch_send_time: Instant,

    /// A batch of events to send to the destination
    events_batch: Vec<Event>,
}

impl ApplyLoopState {
    fn new(next_status_update: StatusUpdate, events_batch: Vec<Event>) -> Self {
        Self {
            last_commit_end_lsn: None,
            remote_final_lsn: None,
            next_status_update,
            last_batch_send_time: Instant::now(),
            events_batch,
        }
    }

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
    fn handling_transaction(&self) -> bool {
        self.remote_final_lsn.is_some()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_apply_loop<D, T>(
    pipeline_id: PipelineId,
    start_lsn: PgLsn,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    schema_cache: SchemaCache,
    destination: D,
    hook: T,
    mut shutdown_rx: ShutdownRx,
    mut force_syncing_tables_rx: Option<SignalRx>,
) -> Result<ApplyLoopResult, ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
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

    let max_batch_fill_duration = Duration::from_millis(config.batch.max_fill_ms);

    loop {
        tokio::select! {
            biased;

            // Shutdown signal received, exit loop.
            _ = shutdown_rx.changed() => {
                info!("shutting down apply worker while waiting for incoming events");

                return Ok(ApplyLoopResult::ApplyStopped);
            }

            Some(message) = logical_replication_stream.next() => {
                let end_loop = handle_replication_message_batch(
                    &mut state,
                    logical_replication_stream.as_mut(),
                    message?,
                    &schema_cache,
                    &destination,
                    &hook,
                    config.batch.max_size,
                    max_batch_fill_duration,
                )
                .await?;
                if end_loop {
                    return Ok(ApplyLoopResult::ApplyStopped);
                }
            }

            // If we are given a signal which tells us when to forcefully perform table syncing, we
            // will subscribe to it.
            _ = force_syncing_tables_rx.as_mut().map_or_else(|| pending().boxed(), |rx| rx.changed().boxed()) => {
                // If we are told to force syncing tables, call the hook's `process_syncing_tables`
                // method so that we can advance the state of tables.
                //
                // Note that for consistency we can perform table syncing only when we are not in
                // a transaction, meaning that if we get a signal while in the middle of a transaction
                // it will be received but no syncing will happen. We are fine with that since we assume
                // that if we are in the middle of a transaction, Postgres will send us the remaining
                // events of the transaction within a reasonable amount of time and that will drive the
                // sync at the next transaction boundary.
                if !state.handling_transaction() {
                    debug!("forcefully processing syncing tables");
                    let continue_loop = hook.process_syncing_tables(state.next_status_update.flush_lsn, true).await?;
                    if !continue_loop {
                        return Ok(ApplyLoopResult::ApplyStopped);
                    }
                }
            }

            // At regular intervals, if nothing happens, perform housekeeping and send status updates
            // to Postgres.
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

#[expect(clippy::too_many_arguments)]
async fn handle_replication_message_batch<D, T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_cache: &SchemaCache,
    destination: &D,
    hook: &T,
    max_batch_size: usize,
    max_batch_fill_duration: Duration,
) -> Result<bool, ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let result =
        handle_replication_message(state, events_stream, message, schema_cache, hook).await?;

    if let Some(event) = result.event
        && matches!(result.end_batch, None | Some(EndBatch::Inclusive))
    {
        state.events_batch.push(event);
        state.update_last_commit_end_lsn(result.end_lsn);
    }

    try_send_batch(
        state,
        result.end_batch,
        result.skip_table,
        destination,
        hook,
        max_batch_size,
        max_batch_fill_duration,
    )
    .await
}

async fn try_send_batch<D, T>(
    state: &mut ApplyLoopState,
    end_batch: Option<EndBatch>,
    skip_table: Option<TableId>,
    destination: &D,
    hook: &T,
    max_batch_size: usize,
    max_batch_fill_duration: Duration,
) -> Result<bool, ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
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

            info!(
                "sending batch of {} events to destination",
                events_batch.len()
            );

            destination.write_events(events_batch).await?;
            state.last_batch_send_time = Instant::now();
        }

        let mut end_loop = false;
        if let Some(table_id) = skip_table {
            info!("skipping table {} due to schema change", table_id);
            end_loop |= !hook.skip_table(table_id).await?;
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

async fn handle_replication_message<T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
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
                schema_cache,
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

async fn handle_logical_replication_message<T>(
    state: &mut ApplyLoopState,
    start_lsn: PgLsn,
    message: LogicalReplicationMessage,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
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
    let event = convert_message_to_event(schema_cache, start_lsn, commit_lsn, &message).await?;

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
            handle_relation_message(state, event, &message, schema_cache, hook).await
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

#[allow(clippy::result_large_err)]
fn get_commit_lsn(
    state: &ApplyLoopState,
    message: &LogicalReplicationMessage,
) -> Result<PgLsn, ApplyLoopError> {
    // If we are in a `Begin` message, the `commit_lsn` is the `final_lsn` of the payload, in all the
    // other cases we read the `remote_final_lsn` which should be always set in case we are within or
    // at the end of a transaction (meaning that the event type is different from `Begin`).
    if let LogicalReplicationMessage::Begin(message) = message {
        Ok(PgLsn::from(message.final_lsn()))
    } else {
        state
            .remote_final_lsn
            .ok_or_else(|| ApplyLoopError::InvalidTransaction("get_commit_lsn".to_owned()))
    }
}

async fn handle_begin_message(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::BeginBody,
) -> Result<HandleMessageResult, ApplyLoopError> {
    let EventType::Begin = event.event_type() else {
        return Err(ApplyLoopError::InvalidEvent(event.into(), EventType::Begin));
    };

    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    let final_lsn = PgLsn::from(message.final_lsn());
    state.remote_final_lsn = Some(final_lsn);

    Ok(HandleMessageResult {
        event: Some(event),
        end_lsn: None,
        end_batch: None,
        skip_table: None,
    })
}

async fn handle_commit_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::CommitBody,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let EventType::Commit = event.event_type() else {
        return Err(ApplyLoopError::InvalidEvent(
            event.into(),
            EventType::Commit,
        ));
    };

    // We take the LSN that belongs to the current transaction, however, if there is no
    // LSN, it means that a `Begin` message was not received before this `Commit` which means
    // we are in an inconsistent state.
    let Some(remote_final_lsn) = state.remote_final_lsn.take() else {
        return Err(ApplyLoopError::InvalidTransaction(
            "handle_commit_message".to_owned(),
        ));
    };

    // If the commit lsn of the message is different from the remote final lsn, it means that the
    // transaction that was started expect a different commit lsn in the commit message. In this case,
    // we want to bail assuming we are in an inconsistent state.
    let commit_lsn = PgLsn::from(message.commit_lsn());
    if commit_lsn != remote_final_lsn {
        return Err(ApplyLoopError::InvalidCommitLsn(
            commit_lsn,
            remote_final_lsn,
        ));
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

async fn handle_relation_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::RelationBody,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let Event::Relation(event) = event else {
        return Err(ApplyLoopError::InvalidEvent(
            event.into(),
            EventType::Relation,
        ));
    };

    let Some(remote_final_lsn) = state.remote_final_lsn else {
        return Err(ApplyLoopError::InvalidTransaction(
            "handle_relation_message".to_owned(),
        ));
    };

    if !hook
        .should_apply_changes(message.rel_id(), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::default());
    }

    // If no table schema is found, it means that something went wrong and we throw an error, which is
    // dealt with differently based on the worker type.
    // TODO: explore how to deal with applying relation messages to the schema (creating it if missing).
    let schema_cache = schema_cache.lock_inner().await;
    let Some(existing_table_schema) = schema_cache.get_table_schema_ref(&message.rel_id()) else {
        return Err(ApplyLoopError::MissingTableSchema(message.rel_id()));
    };

    // We compare the table schema from the relation message with the existing schema (if any).
    // The purpose of this comparison is that we want to throw an error and stop the processing
    // of any table that incurs in a schema change after the initial table sync is performed.
    if !existing_table_schema.partial_eq(&event.table_schema) {
        return Ok(HandleMessageResult {
            end_batch: Some(EndBatch::Exclusive),
            skip_table: Some(message.rel_id()),
            ..Default::default()
        });
    }

    Ok(HandleMessageResult {
        event: Some(Event::Relation(event)),
        end_lsn: None,
        end_batch: None,
        skip_table: None,
    })
}

async fn handle_insert_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::InsertBody,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let Event::Insert(event) = event else {
        return Err(ApplyLoopError::InvalidEvent(
            event.into(),
            EventType::Insert,
        ));
    };

    let Some(remote_final_lsn) = state.remote_final_lsn else {
        return Err(ApplyLoopError::InvalidTransaction(
            "handle_insert_message".to_owned(),
        ));
    };

    if !hook
        .should_apply_changes(message.rel_id(), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::default());
    }

    Ok(HandleMessageResult {
        event: Some(Event::Insert(event)),
        end_lsn: None,
        end_batch: None,
        skip_table: None,
    })
}

async fn handle_update_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::UpdateBody,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let Event::Update(event) = event else {
        return Err(ApplyLoopError::InvalidEvent(
            event.into(),
            EventType::Update,
        ));
    };

    let Some(remote_final_lsn) = state.remote_final_lsn else {
        return Err(ApplyLoopError::InvalidTransaction(
            "handle_update_message".to_owned(),
        ));
    };

    if !hook
        .should_apply_changes(message.rel_id(), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::default());
    }

    Ok(HandleMessageResult {
        event: Some(Event::Update(event)),
        end_lsn: None,
        end_batch: None,
        skip_table: None,
    })
}

async fn handle_delete_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::DeleteBody,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let Event::Delete(event) = event else {
        return Err(ApplyLoopError::InvalidEvent(
            event.into(),
            EventType::Delete,
        ));
    };

    let Some(remote_final_lsn) = state.remote_final_lsn else {
        return Err(ApplyLoopError::InvalidTransaction(
            "handle_delete_message".to_owned(),
        ));
    };

    if !hook
        .should_apply_changes(message.rel_id(), remote_final_lsn)
        .await?
    {
        return Ok(HandleMessageResult::default());
    }

    Ok(HandleMessageResult {
        event: Some(Event::Delete(event)),
        end_lsn: None,
        end_batch: None,
        skip_table: None,
    })
}

async fn handle_truncate_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::TruncateBody,
    hook: &T,
) -> Result<HandleMessageResult, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let Event::Truncate(mut event) = event else {
        return Err(ApplyLoopError::InvalidEvent(
            event.into(),
            EventType::Truncate,
        ));
    };

    let Some(remote_final_lsn) = state.remote_final_lsn else {
        return Err(ApplyLoopError::InvalidTransaction(
            "handle_truncate_message".to_owned(),
        ));
    };

    let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
    for &table_id in message.rel_ids().iter() {
        if hook
            .should_apply_changes(table_id, remote_final_lsn)
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
        skip_table: None,
    })
}
