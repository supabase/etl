use crate::v2::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use crate::v2::concurrency::stream::BatchStream;
use crate::v2::config::pipeline::PipelineConfig;
use crate::v2::conversions::event::{
    convert_message_to_event, Event, EventConversionError, EventType,
};
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::pipeline::PipelineIdentity;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::slot::{get_slot_name, SlotError};
use crate::v2::replication::stream::{EventsStream, EventsStreamError};
use crate::v2::schema::cache::SchemaCache;
use crate::v2::state::store::base::StateStoreError;
use crate::v2::workers::apply::ApplyWorkerHookError;
use crate::v2::workers::base::WorkerType;
use crate::v2::workers::table_sync::TableSyncWorkerHookError;

use futures::StreamExt;
use postgres::schema::Oid;
use postgres_replication::protocol;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

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

    #[error(
        "An error occurred while streaming logical replication changes in the apply loop: {0}"
    )]
    LogicalReplicationStreamFailed(#[from] EventsStreamError),

    #[error("Could not generate slot name in the apply loop: {0}")]
    Slot(#[from] SlotError),

    #[error("An error happened in the state store while in the apply loop: {0}")]
    StateStore(#[from] StateStoreError),

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
    MissingTableSchema(Oid),

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
    ApplyCompleted,
}

pub trait ApplyLoopHook {
    type Error: Into<ApplyLoopError>;

    fn initialize(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn process_syncing_tables(
        &self,
        current_lsn: PgLsn,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn skip_table(&self, table_id: Oid) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn should_apply_changes(
        &self,
        table_id: Oid,
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

#[derive(Debug, Clone)]
enum BatchEarlyBreak {
    Break,
    BreakAndDiscard,
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
    /// An enum indicating whether the processing of a batch should be terminated early, with a consequent
    /// termination of the main apply loop too.
    early_break: Option<BatchEarlyBreak>,
}

impl ApplyLoopState {
    fn new(next_status_update: StatusUpdate) -> Self {
        Self {
            last_commit_end_lsn: None,
            remote_final_lsn: None,
            next_status_update,
            early_break: None,
        }
    }

    fn update_last_commit_end_lsn(&mut self, new_last_commit_end_lsn: PgLsn) {
        let Some(last_commit_end_lsn) = &mut self.last_commit_end_lsn else {
            self.last_commit_end_lsn = Some(new_last_commit_end_lsn);
            return;
        };

        if new_last_commit_end_lsn <= *last_commit_end_lsn {
            return;
        }

        *last_commit_end_lsn = new_last_commit_end_lsn;
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn start_apply_loop<D, T>(
    identity: PipelineIdentity,
    start_lsn: PgLsn,
    config: Arc<PipelineConfig>,
    replication_client: PgReplicationClient,
    schema_cache: SchemaCache,
    destination: D,
    hook: T,
    mut shutdown_rx: ShutdownRx,
) -> Result<ApplyLoopResult, ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // The first status update is defaulted from the start lsn since at this point we haven't
    // processed anything.
    let first_status_update = StatusUpdate {
        write_lsn: start_lsn,
        flush_lsn: start_lsn,
        apply_lsn: start_lsn,
    };

    // We initialize the shared state that is used throughout the loop to track progress.
    let mut state = ApplyLoopState::new(first_status_update);

    // We initialize the apply loop which is based on the hook implementation.
    hook.initialize().await?;

    // We compute the slot name for the replication slot that we are going to use for the logical
    // replication. At this point we assume that the slot already exists.
    let slot_name = get_slot_name(identity.id(), hook.worker_type())?;

    // We start the logical replication stream with the supplied parameters at a given lsn. That
    // lsn is the last lsn from which we need to start fetching events.
    let logical_replication_stream = replication_client
        .start_logical_replication(identity.publication_name(), &slot_name, start_lsn)
        .await?;
    let logical_replication_stream = EventsStream::wrap(logical_replication_stream);
    let logical_replication_stream = BatchStream::wrap(
        logical_replication_stream,
        config.batch.clone(),
        shutdown_rx.clone(),
    );
    pin!(logical_replication_stream);

    loop {
        tokio::select! {
            biased;

            // Shutdown signal received, exit loop.
            _ = shutdown_rx.changed() => {
                info!("Shutting down apply worker while waiting for incoming events");
                return Ok(ApplyLoopResult::ApplyStopped);
            }

            // Process a batch of replication messages.
            Some(result) = logical_replication_stream.next() => {
                let logical_replication_stream = logical_replication_stream.as_mut();
                let events_stream = unsafe {
                    Pin::new_unchecked(
                        logical_replication_stream
                            .get_unchecked_mut()
                            .get_inner_mut()
                    )
                };

                match result {
                    ShutdownResult::Ok(messages_batch) => {
                        let stop_apply_loop = handle_replication_message_batch(
                            &mut state,
                            events_stream,
                            messages_batch,
                            &schema_cache,
                            &destination,
                            &hook,
                        )
                        .await?;

                        // If we are told to stop the apply loop, we will do it.
                        if stop_apply_loop {
                            return Ok(ApplyLoopResult::ApplyStopped);
                        }
                    }
                    ShutdownResult::Shutdown(_) => {
                        // If we incurred in a shutdown within the stream, we also return that we
                        // stopped.
                        // This branch is technically not really needed since we have the shutdown
                        // handler also in the `select!`, however this code path could react faster
                        // in case we have a shutdown signal sent while we are running the blocking
                        // loop in the stream.
                        info!("Shutting down apply worker before processing batch");
                        return Ok(ApplyLoopResult::ApplyStopped);
                    }
                }
            }

            // At regular intervals, if nothing happens, perform housekeeping and send status updates
            // to Postgres.
            _ = tokio::time::sleep(REFRESH_INTERVAL) => {
                // TODO: implement housekeeping like slot deletion.
                let logical_replication_stream = logical_replication_stream.as_mut();
                let events_stream = unsafe {
                    Pin::new_unchecked(
                        logical_replication_stream
                            .get_unchecked_mut()
                            .get_inner_mut()
                    )
                };

                events_stream
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

#[allow(clippy::too_many_arguments)]
async fn handle_replication_message_batch<D, T>(
    state: &mut ApplyLoopState,
    mut stream: Pin<&mut EventsStream>,
    messages_batch: Vec<Result<ReplicationMessage<LogicalReplicationMessage>, EventsStreamError>>,
    schema_cache: &SchemaCache,
    destination: &D,
    hook: &T,
) -> Result<bool, ApplyLoopError>
where
    D: Destination + Clone + Send + 'static,
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let mut stop_apply_loop = false;
    let mut events_batch = Vec::with_capacity(messages_batch.len());

    for message in messages_batch {
        // We store the previous state to use it in case we have to restore it because we processed
        // a message that lead to an early break and discard.
        //
        // Note that the `shared` part of the state will be shared amongst the clones, so you have to
        // make sure the data there is expected to be shared.
        let previous_state = state.clone();

        // An error while processing a message in a batch will lead to the entire batch being discarded.
        let event =
            handle_replication_message(state, stream.as_mut(), message?, schema_cache, hook)
                .await?;

        if !matches!(state.early_break, Some(BatchEarlyBreak::BreakAndDiscard)) {
            if let Some(event) = event {
                events_batch.push(event);
            }
        }

        // If we should break early after processing a message, we can do this in many ways:
        // - break -> this breaks out of the loop and assumes that the last processed message was
        //  successfully processed, so we apply all the messages up to this one.
        // - break and discard -> this breaks out of the loop and assumes that the last processed
        //  message was not fully processed, so we reset the `last_end_lsn` to the one of the message
        //  before this one, so that when we apply events and notify Postgres, it's done as if the
        //  last message was not processed.
        //
        // Early breaking can happen for example when a table sync worker has caught up with the apply worker
        // but its batch contained more elements after the caught up element, in that case we don't
        // want to process those elements, otherwise if we do, the apply worker will process them too
        // causing duplicate data.
        match state.early_break {
            Some(BatchEarlyBreak::Break) => {
                stop_apply_loop = true;

                break;
            }
            Some(BatchEarlyBreak::BreakAndDiscard) => {
                *state = previous_state;
                stop_apply_loop = true;

                break;
            }
            None => {}
        }
    }

    if events_batch.is_empty() {
        return Ok(stop_apply_loop);
    }

    // We apply the batch of events to the destination.
    destination.write_events(events_batch).await?;

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
        state
            .next_status_update
            .update_flush_lsn(last_commit_end_lsn);
        state
            .next_status_update
            .update_apply_lsn(last_commit_end_lsn);
    }

    Ok(stop_apply_loop)
}

async fn handle_replication_message<T>(
    state: &mut ApplyLoopState,
    events_stream: Pin<&mut EventsStream>,
    message: ReplicationMessage<LogicalReplicationMessage>,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
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

            handle_logical_replication_message(state, message.into_data(), schema_cache, hook).await
        }
        ReplicationMessage::PrimaryKeepAlive(message) => {
            let end_lsn = PgLsn::from(message.wal_end());
            state.next_status_update.update_write_lsn(end_lsn);

            events_stream
                .send_status_update(
                    state.next_status_update.write_lsn,
                    state.next_status_update.flush_lsn,
                    state.next_status_update.apply_lsn,
                    message.reply() == 1,
                )
                .await?;

            Ok(None)
        }
        _ => Ok(None),
    }
}

async fn handle_logical_replication_message<T>(
    state: &mut ApplyLoopState,
    message: LogicalReplicationMessage,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    // We perform the conversion of the message to our own event format which is used downstream
    // by the destination.
    let event = convert_message_to_event(schema_cache, &message).await?;

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
        LogicalReplicationMessage::Origin(_) => Ok(None),
        LogicalReplicationMessage::Type(_) => Ok(None),
        _ => Ok(None),
    }
}

async fn handle_begin_message(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::BeginBody,
) -> Result<Option<Event>, ApplyLoopError> {
    let Event::Begin(event) = event else {
        return Err(ApplyLoopError::InvalidEvent(event.into(), EventType::Begin));
    };

    // We track the final lsn of this transaction, which should be equal to the `commit_lsn` of the
    // `Commit` message.
    let final_lsn = PgLsn::from(message.final_lsn());
    state.remote_final_lsn = Some(final_lsn);

    Ok(Some(Event::Begin(event)))
}

async fn handle_commit_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::CommitBody,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
where
    T: ApplyLoopHook,
    ApplyLoopError: From<<T as ApplyLoopHook>::Error>,
{
    let Event::Commit(event) = event else {
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

    // We mark this as the last commit end LSN since we want to be able to track from the outside
    // what was the biggest transaction boundary LSN which was successfully applied.
    //
    // The rationale for using only the `end_lsn` of the `Commit` message is that once we found a
    // commit and successfully processed it, we can say that the next byte we want is the next transaction
    // since if we were to store an intermediate `end_lsn` (from a dml operation within a transaction)
    // the replication will still start from a transaction boundary, that is, a `Begin` statement in
    // our case.
    state.update_last_commit_end_lsn(end_lsn);

    // We process syncing tables since we just arrived at the end of a transaction, and we want to
    // synchronize all the workers.
    //
    // The `end_lsn` here refers to the LSN of the record right after the commit record.
    let continue_loop = hook.process_syncing_tables(end_lsn).await?;

    // If we are told to stop the loop, it means we reached the end of processing for this specific
    // worker, so we gracefully stop processing the batch, but we include in the batch the last processed
    // element, in this case the `Commit` message.
    if !continue_loop {
        state.early_break = Some(BatchEarlyBreak::Break);
    }

    Ok(Some(Event::Commit(event)))
}

async fn handle_relation_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::RelationBody,
    schema_cache: &SchemaCache,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
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
        return Ok(None);
    }

    // If no table schema is found, it means that something went wrong and we throw an error, which is
    // dealt with differently based on the worker type.
    // TODO: explore how to deal with applying relation messages to the schema (creating it if missing).
    let Some(existing_table_schema) = schema_cache.get_table_schema(&message.rel_id()).await else {
        return Err(ApplyLoopError::MissingTableSchema(message.rel_id()));
    };

    // We compare the table schema from the relation message with the existing schema (if any).
    // The purpose of this comparison is that we want to throw an error and stop the processing
    // of any table that incurs in a schema change after the initial table sync is performed.
    if !existing_table_schema.partial_eq(&event.table_schema) {
        let continue_loop = hook.skip_table(message.rel_id()).await?;

        // If we are told to stop the loop, we want to break the batch processing and discard the
        // current element being processed, in this case the `Relation` message.
        if !continue_loop {
            state.early_break = Some(BatchEarlyBreak::BreakAndDiscard);
        }

        return Ok(None);
    }

    Ok(Some(Event::Relation(event)))
}

async fn handle_insert_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::InsertBody,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
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
        return Ok(None);
    }

    Ok(Some(Event::Insert(event)))
}

async fn handle_update_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::UpdateBody,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
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
        return Ok(None);
    }

    Ok(Some(Event::Update(event)))
}

async fn handle_delete_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::DeleteBody,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
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
        return Ok(None);
    }

    Ok(Some(Event::Delete(event)))
}

async fn handle_truncate_message<T>(
    state: &mut ApplyLoopState,
    event: Event,
    message: &protocol::TruncateBody,
    hook: &T,
) -> Result<Option<Event>, ApplyLoopError>
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

    Ok(Some(Event::Truncate(event)))
}
