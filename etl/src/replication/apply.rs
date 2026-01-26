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

use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
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
use crate::state::table::{
    RetryPolicy, TableReplicationError, TableReplicationPhase, TableReplicationPhaseType,
};
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;
use crate::types::{Event, PipelineId};
use crate::workers::base::Worker;
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
    pub pool: TableSyncWorkerPool,
    /// State store for tracking table replication progress.
    pub store: S,
    /// Destination where replicated data is written.
    pub destination: D,
    /// Shutdown signal receiver for graceful termination.
    pub shutdown_rx: ShutdownRx,
    /// Semaphore controlling maximum concurrent table sync workers.
    pub table_sync_worker_permits: Arc<Semaphore>,
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
/// This enum replaces the `ApplyLoopHook` trait, providing direct access to
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

/// Result returned from `handle_replication_message` and related functions.
#[derive(Debug, Default)]
struct HandleMessageResult {
    /// The event converted from the replication message.
    event: Option<Event>,
    /// Set to a commit message's end_lsn value, `None` otherwise.
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
    /// The highest LSN received from the `end_lsn` field of a `Commit` message.
    last_commit_end_lsn: Option<PgLsn>,
    /// The LSN of the commit WAL entry of the transaction that is currently being processed.
    remote_final_lsn: Option<PgLsn>,
    /// The current replication progress tracking received and flushed LSN positions.
    replication_progress: ReplicationProgress,
    /// A batch of events to send to the destination.
    events_batch: Vec<Event>,
    /// Instant from when a transaction began.
    current_tx_begin_ts: Option<Instant>,
    /// Number of events observed in the current transaction (excluding BEGIN/COMMIT).
    current_tx_events: u64,
    /// Boolean representing whether the shutdown was requested but could not be processed.
    shutdown_deferred: bool,
    /// The deadline by which the current batch must be flushed.
    batch_flush_deadline: Option<Instant>,
    /// The maximum duration to wait before forcibly flushing a batch.
    max_batch_fill_duration: Duration,
}

impl ApplyLoopState {
    /// Creates a new [`ApplyLoopState`] with initial replication progress and event batch.
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
    fn start_batch_timer_if_needed(&mut self) {
        if self.batch_flush_deadline.is_some() {
            return;
        }

        self.batch_flush_deadline = Some(Instant::now() + self.max_batch_fill_duration);

        debug!("started batch flush timer");
    }

    /// Resets the batch flush deadline.
    fn reset_batch_deadline(&mut self) {
        self.batch_flush_deadline = None;

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

    /// Returns the effective flush LSN that should be reported to the PostgreSQL server.
    fn effective_flush_lsn(&self) -> PgLsn {
        if !self.handling_transaction() && self.events_batch.is_empty() {
            self.replication_progress.last_received_lsn
        } else {
            self.replication_progress.last_flush_lsn
        }
    }

    /// Returns true if the apply loop is in the middle of processing a transaction.
    fn handling_transaction(&self) -> bool {
        self.remote_final_lsn.is_some()
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
            config.batch.max_size,
            Duration::from_millis(config.batch.max_fill_ms),
        );

        let mut apply_loop = Self {
            pipeline_id,
            config: config.clone(),
            schema_store,
            destination,
            shutdown_rx,
            worker_context,
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
        pin!(events_stream);

        loop {
            tokio::select! {
                biased;

                // PRIORITY 1: Handle shutdown signals.
                _ = self.shutdown_rx.changed() => {
                    if self.state.shutdown_deferred {
                        info!("shutdown already requested, continuing due to running transaction");
                        continue;
                    }

                    if !self.state.handling_transaction() {
                        info!("shutting down apply loop outside transaction");
                        return Ok(ApplyLoopResult::Paused);
                    }

                    info!("deferring shutdown until transaction boundary");
                    self.state.shutdown_deferred = true;
                }

                // PRIORITY 2: Handle batch flush timer expiry.
                _ = async {
                    match self.state.batch_flush_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                        None => std::future::pending().await,
                    }
                } => {
                    info!(
                        batch_size = self.state.events_batch.len(),
                        "batch flush timer expired, flushing batch",
                    );

                    let action = self.flush_batch().await?;

                    if let Some(result) = action.to_result() {
                        return Ok(result);
                    }
                }

                // PRIORITY 3: Process incoming replication messages from Postgres.
                message = events_stream.next() => {
                    let Some(message) = message else {
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

                    let action = self
                        .handle_replication_message_and_flush(events_stream.as_mut(), message?)
                        .await?;

                    if !self.state.events_batch.is_empty() {
                        self.state.start_batch_timer_if_needed();
                    }

                    if let Some(result) = action.to_result() {
                        return Ok(result);
                    }
                }
            }
        }
    }

    /// Handles a replication message and flushes the batch if necessary.
    async fn handle_replication_message_and_flush(
        &mut self,
        mut events_stream: Pin<&mut EventsStream>,
        message: ReplicationMessage<LogicalReplicationMessage>,
    ) -> EtlResult<ApplyLoopAction> {
        let result = self
            .handle_replication_message(events_stream.as_mut(), message)
            .await?;

        let should_include_event = matches!(result.end_batch, None | Some(EndBatch::Inclusive));
        if let Some(event) = result.event
            && should_include_event
        {
            self.state.events_batch.push(event);
            self.state.update_last_commit_end_lsn(result.end_lsn);
        }

        let mut action = result.action;

        let should_flush = self.state.events_batch.len() >= self.config.batch.max_size
            || result.end_batch.is_some();
        if should_flush && !self.state.events_batch.is_empty() {
            info!(
                batch_size = self.state.events_batch.len(),
                "batch flush condition reached, flushing batch"
            );

            let flush_batch_action = self.flush_batch().await?;
            action = action.merge(flush_batch_action);
        }

        if let Some(error) = result.table_replication_error {
            let mark_table_errored_action = self.mark_table_errored(error).await?;
            action = action.merge(mark_table_errored_action);
        }

        let synchronize_action = self.process_syncing_tables_outside_transaction().await?;
        action = action.merge(synchronize_action);

        Ok(action)
    }

    /// Flushes the current batch of events to the destination.
    async fn flush_batch(&mut self) -> EtlResult<ApplyLoopAction> {
        self.state.reset_batch_deadline();

        self.send_batch_to_destination().await?;
        self.process_syncing_tables_after_batch().await
    }

    /// Sends the current batch of events to the destination and updates metrics.
    async fn send_batch_to_destination(&mut self) -> EtlResult<()> {
        if self.state.events_batch.is_empty() {
            return Ok(());
        }

        let events_batch = std::mem::replace(
            &mut self.state.events_batch,
            Vec::with_capacity(self.config.batch.max_size),
        );

        let batch_size = events_batch.len();
        info!(batch_size = batch_size, "sending batch to destination");

        let before_sending = Instant::now();

        // TODO: in the future we want to investigate how to perform the writing asynchronously
        //  to avoid stalling the apply loop.
        self.destination.write_events(events_batch).await?;

        counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            WORKER_TYPE_LABEL => "apply",
            ACTION_LABEL => "table_streaming",
            PIPELINE_ID_LABEL => self.pipeline_id.to_string(),
            DESTINATION_LABEL => D::name(),
        )
        .increment(batch_size as u64);

        let send_duration_seconds = before_sending.elapsed().as_secs_f64();
        histogram!(
            ETL_BATCH_ITEMS_SEND_DURATION_SECONDS,
            WORKER_TYPE_LABEL => "apply",
            ACTION_LABEL => "table_streaming",
            PIPELINE_ID_LABEL => self.pipeline_id.to_string(),
            DESTINATION_LABEL => D::name(),
        )
        .record(send_duration_seconds);

        Ok(())
    }

    /// Dispatches replication protocol messages to appropriate handlers.
    async fn handle_replication_message(
        &mut self,
        events_stream: Pin<&mut EventsStream>,
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
                    .send_status_update(
                        self.state.last_received_lsn(),
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

        let event = parse_event_from_begin_message(start_lsn, final_lsn, message);

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

        let end_lsn = PgLsn::from(message.end_lsn());

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

        // Process syncing tables after commit (worker-specific behavior).
        let mut action = self.process_syncing_tables_after_commit(end_lsn).await?;

        // If the shutdown was deferred, we want to pause the loop since now we have reached the end
        // of a transaction, which is the best point where we can consistently and gracefully stop.
        if self.state.shutdown_deferred {
            action = action.merge(ApplyLoopAction::Pause);
        }

        let event = parse_event_from_commit_message(start_lsn, commit_lsn, message);

        let mut result = HandleMessageResult {
            event: Some(Event::Commit(event)),
            end_lsn: Some(end_lsn),
            action,
            ..Default::default()
        };

        // If the action results in the apply loop terminating, we want to end the batch forcibly
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

        let event = parse_event_from_relation_message(start_lsn, remote_final_lsn, message)?;

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

        let mut rel_ids = Vec::with_capacity(message.rel_ids().len());
        for &table_id in message.rel_ids().iter() {
            if self
                .should_apply_changes(TableId::new(table_id), remote_final_lsn)
                .await?
            {
                rel_ids.push(table_id)
            }
        }

        if rel_ids.is_empty() {
            return Ok(HandleMessageResult::no_event());
        }

        let event =
            parse_event_from_truncate_message(start_lsn, remote_final_lsn, message, rel_ids);

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
                should_apply_changes_apply(ctx, table_id, remote_final_lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                Ok(should_apply_changes_table_sync(ctx, table_id).await)
            }
        }
    }

    /// Processes syncing tables after a commit message.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn process_syncing_tables_after_commit(
        &mut self,
        lsn: PgLsn,
    ) -> EtlResult<ApplyLoopAction> {
        match &mut self.worker_context {
            WorkerContext::Apply(ctx) => process_syncing_tables_after_commit_apply(ctx, lsn).await,
            WorkerContext::TableSync(ctx) => {
                process_syncing_tables_after_commit_table_sync(ctx, lsn).await
            }
        }
    }

    /// Processes syncing tables after a batch has been flushed.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    async fn process_syncing_tables_after_batch(&mut self) -> EtlResult<ApplyLoopAction> {
        let Some(last_commit_end_lsn) = self.state.last_commit_end_lsn.take() else {
            return Ok(ApplyLoopAction::Continue);
        };

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
                process_syncing_tables_after_batch_apply(ctx, current_lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                process_syncing_tables_after_batch_table_sync(ctx, current_lsn).await
            }
        }
    }

    /// Processes syncing tables outside a transaction.
    ///
    /// Dispatches to worker-specific implementation based on the worker context.
    /// Only processes when outside a transaction and the batch is empty.
    async fn process_syncing_tables_outside_transaction(&mut self) -> EtlResult<ApplyLoopAction> {
        if self.state.handling_transaction() {
            debug!("skipping table sync processing because of in progress transaction");
            return Ok(ApplyLoopAction::Continue);
        }

        if !self.state.events_batch.is_empty() {
            debug!("skipping table sync processing because batch is not empty");
            return Ok(ApplyLoopAction::Continue);
        }

        let current_lsn = self.state.effective_flush_lsn();

        info!(
            worker_type = %self.worker_context.worker_type(),
            %current_lsn,
            "processing syncing tables outside transaction"
        );

        match &mut self.worker_context {
            WorkerContext::Apply(ctx) => {
                process_syncing_tables_outside_transaction_apply(ctx, current_lsn).await
            }
            WorkerContext::TableSync(ctx) => {
                process_syncing_tables_outside_transaction_table_sync(ctx, current_lsn).await
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
                mark_table_errored_apply(ctx, table_replication_error).await
            }
            WorkerContext::TableSync(ctx) => {
                mark_table_errored_table_sync(ctx, table_replication_error).await
            }
        }
    }
}

/// Checks whether changes should be applied based on the replication phase.
///
/// Returns `true` for `Ready` state or `SyncDone` when the sync LSN is at or
/// before the remote final LSN.
fn is_phase_ready_for_changes(phase: TableReplicationPhase, remote_final_lsn: PgLsn) -> bool {
    match phase {
        TableReplicationPhase::Ready => true,
        TableReplicationPhase::SyncDone { lsn } => lsn <= remote_final_lsn,
        _ => false,
    }
}

/// Determines whether changes should be applied for a given table (apply worker).
///
/// If an active worker exists for the table, its state is checked while holding
/// the lock. Otherwise, the replication phase is read from the store.
async fn should_apply_changes_apply<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    table_id: TableId,
    remote_final_lsn: PgLsn,
) -> EtlResult<bool>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let pool_guard = ctx.pool.lock().await;
    let active_worker_state = pool_guard.get_active_worker_state(table_id);

    if let Some(active_worker_state) = active_worker_state {
        let inner = active_worker_state.lock().await;
        return Ok(is_phase_ready_for_changes(
            inner.replication_phase(),
            remote_final_lsn,
        ));
    }

    let Some(phase) = ctx.store.get_table_replication_state(table_id).await? else {
        return Ok(false);
    };

    Ok(is_phase_ready_for_changes(phase, remote_final_lsn))
}

/// Determines whether changes should be applied for a given table (table sync worker).
///
/// For table sync workers, changes are only applied if the table matches
/// the worker's assigned table and the table is not in an error state.
async fn should_apply_changes_table_sync<S>(
    ctx: &TableSyncWorkerContext<S>,
    table_id: TableId,
) -> bool {
    let inner = ctx.table_sync_worker_state.lock().await;
    let is_errored = inner.replication_phase().as_type() == TableReplicationPhaseType::Errored;

    !is_errored && ctx.table_id == table_id
}

/// Processes syncing tables after commit (apply worker).
///
/// Spawns new table sync workers and triggers catchup when encountering SyncWait.
/// Does NOT perform SyncDone → Ready transitions.
async fn process_syncing_tables_after_commit_apply<S, D>(
    ctx: &mut ApplyWorkerContext<S, D>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let active_table_replication_states = ctx
        .store
        .get_table_replication_states()
        .await?
        .into_iter()
        .filter(|(_, state)| !state.as_type().is_done());

    for (table_id, table_replication_phase) in active_table_replication_states {
        let action = process_single_syncing_table_after_commit_apply(
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

/// Processes a single syncing table after commit (apply worker).
///
/// Handles SyncWait → Catchup transitions and spawns new workers.
/// Does NOT handle SyncDone → Ready transitions.
async fn process_single_syncing_table_after_commit_apply<S, D>(
    ctx: &mut ApplyWorkerContext<S, D>,
    table_id: TableId,
    table_replication_phase: TableReplicationPhase,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let worker_state = {
        let pool_guard = ctx.pool.lock().await;
        pool_guard.get_active_worker_state(table_id)
    };

    if let Some(worker_state) = worker_state {
        let mut worker_state_guard = worker_state.lock().await;

        match worker_state_guard.replication_phase() {
            TableReplicationPhase::SyncWait { lsn: snapshot_lsn } => {
                // The catchup lsn is determined via max since it could be that the table sync worker
                // is started from a lsn which is far in the future compared to where the apply worker
                // is.
                let catchup_lsn = snapshot_lsn.max(current_lsn);

                info!(
                    %table_id,
                    %current_lsn,
                    %snapshot_lsn,
                    %catchup_lsn,
                    "table sync worker is waiting to catchup, starting catchup",
                );

                worker_state_guard
                    .set_and_store(
                        TableReplicationPhase::Catchup { lsn: catchup_lsn },
                        &ctx.store,
                    )
                    .await?;

                info!(
                    %table_id,
                    "catchup was started, waiting for table sync worker to complete sync",
                );

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
                        if result.replication_phase().as_type().is_errored() {
                            info!(
                                %table_id,
                                "table sync worker errored, skipping table",
                            );
                            return Ok(ApplyLoopAction::Continue);
                        }
                    }
                    ShutdownResult::Shutdown(_) => {
                        info!(
                            %table_id,
                            "table sync worker interrupted by shutdown signal",
                        );
                        return Ok(ApplyLoopAction::Pause);
                    }
                }

                info!(%table_id, "table sync worker finished syncing");
            }
            TableReplicationPhase::SyncDone { .. } => {
                // Do NOT transition to Ready after commit - that happens after batch flush.
            }
            _ => {
                // For other phases, no action needed in this loop cycle.
            }
        }
    } else {
        // No active worker exists, potentially start a new worker.
        match table_replication_phase {
            TableReplicationPhase::SyncDone { .. } => {
                // Do NOT transition to Ready after commit.
            }
            _ => {
                // Start a new worker for this table.
                let table_sync_worker = build_table_sync_worker(ctx, table_id);
                if let Err(err) = start_table_sync_worker(ctx.pool.clone(), table_sync_worker).await
                {
                    error!(
                        %table_id,
                        error = %err,
                        "failed to start table sync worker",
                    );
                }
            }
        }
    }

    Ok(ApplyLoopAction::Continue)
}

/// Processes syncing tables after commit (table sync worker).
///
/// Validates whether catchup position has been reached.
/// If so, returns Complete to signal end batch.
/// Does NOT update state (that happens after flush).
async fn process_syncing_tables_after_commit_table_sync<S>(
    ctx: &TableSyncWorkerContext<S>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction> {
    debug!(
        table_id = %ctx.table_id,
        %current_lsn,
        "processing syncing tables after commit for table sync worker",
    );

    let inner = ctx.table_sync_worker_state.lock().await;

    // Check if catchup position reached - if so, signal end batch but don't update state yet.
    if let TableReplicationPhase::Catchup { lsn } = inner.replication_phase() {
        if current_lsn >= lsn {
            return Ok(ApplyLoopAction::Complete);
        }
    }

    Ok(ApplyLoopAction::Continue)
}

/// Processes syncing tables after batch flush (apply worker).
///
/// Handles `SyncDone → Ready` transitions and spawns new workers.
async fn process_syncing_tables_after_batch_apply<S, D>(
    ctx: &mut ApplyWorkerContext<S, D>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let active_table_replication_states = ctx
        .store
        .get_table_replication_states()
        .await?
        .into_iter()
        .filter(|(_, state)| !state.as_type().is_done());

    for (table_id, table_replication_phase) in active_table_replication_states {
        let action = process_single_syncing_table_after_batch_apply(
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

/// Processes a single syncing table after batch flush (apply worker).
///
/// Handles `SyncDone → Ready` transitions and spawns new workers.
async fn process_single_syncing_table_after_batch_apply<S, D>(
    ctx: &mut ApplyWorkerContext<S, D>,
    table_id: TableId,
    table_replication_phase: TableReplicationPhase,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let worker_state = {
        let pool_guard = ctx.pool.lock().await;
        pool_guard.get_active_worker_state(table_id)
    };

    if let Some(worker_state) = worker_state {
        let worker_state_guard = worker_state.lock().await;

        if let TableReplicationPhase::SyncDone { lsn: sync_done_lsn } =
            worker_state_guard.replication_phase()
        {
            if current_lsn >= sync_done_lsn {
                info!(
                    %table_id,
                    %sync_done_lsn,
                    %current_lsn,
                    "table transitioned to ready",
                );

                ctx.store
                    .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                    .await?;
            }
        }
    } else {
        // No active worker exists.
        match table_replication_phase {
            TableReplicationPhase::SyncDone { lsn: sync_done_lsn } => {
                if current_lsn >= sync_done_lsn {
                    info!(
                        %table_id,
                        %sync_done_lsn,
                        %current_lsn,
                        "table transitioned to ready",
                    );

                    ctx.store
                        .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                        .await?;
                }
            }
            _ => {
                // Start a new worker for this table.
                let table_sync_worker = build_table_sync_worker(ctx, table_id);
                if let Err(err) = start_table_sync_worker(ctx.pool.clone(), table_sync_worker).await
                {
                    error!(
                        %table_id,
                        error = %err,
                        "failed to start table sync worker",
                    );
                }
            }
        }
    }

    Ok(ApplyLoopAction::Continue)
}

/// Processes syncing tables after batch flush (table sync worker).
///
/// Validates whether catchup position has been reached.
/// If so, transitions to SyncDone and returns Complete.
async fn process_syncing_tables_after_batch_table_sync<S>(
    ctx: &mut TableSyncWorkerContext<S>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    try_complete_table_sync_catchup(ctx, current_lsn).await
}

/// Processes syncing tables outside transaction (apply worker).
///
/// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, and spawns workers.
/// Only called when outside a transaction and the batch is empty.
async fn process_syncing_tables_outside_transaction_apply<S, D>(
    ctx: &mut ApplyWorkerContext<S, D>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let active_table_replication_states = ctx
        .store
        .get_table_replication_states()
        .await?
        .into_iter()
        .filter(|(_, state)| !state.as_type().is_done());

    for (table_id, table_replication_phase) in active_table_replication_states {
        let action = process_single_syncing_table_outside_transaction_apply(
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

/// Processes a single syncing table outside transaction (apply worker).
///
/// Handles `SyncWait → Catchup` and `SyncDone → Ready` transitions, and spawns workers.
/// Only called when outside a transaction and the batch is empty.
async fn process_single_syncing_table_outside_transaction_apply<S, D>(
    ctx: &mut ApplyWorkerContext<S, D>,
    table_id: TableId,
    table_replication_phase: TableReplicationPhase,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let worker_state = {
        let pool_guard = ctx.pool.lock().await;
        pool_guard.get_active_worker_state(table_id)
    };

    if let Some(worker_state) = worker_state {
        let mut worker_state_guard = worker_state.lock().await;

        match worker_state_guard.replication_phase() {
            TableReplicationPhase::SyncWait { lsn: snapshot_lsn } => {
                // The catchup lsn is determined via max since it could be that the table sync worker
                // is started from a lsn which is far in the future compared to where the apply worker
                // is.
                let catchup_lsn = snapshot_lsn.max(current_lsn);

                info!(
                    %table_id,
                    %current_lsn,
                    %snapshot_lsn,
                    %catchup_lsn,
                    "table sync worker is waiting to catchup, starting catchup",
                );

                worker_state_guard
                    .set_and_store(
                        TableReplicationPhase::Catchup { lsn: catchup_lsn },
                        &ctx.store,
                    )
                    .await?;

                info!(
                    %table_id,
                    "catchup was started, waiting for table sync worker to complete sync",
                );

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
                        if result.replication_phase().as_type().is_errored() {
                            info!(
                                %table_id,
                                "table sync worker errored, skipping table",
                            );
                            return Ok(ApplyLoopAction::Continue);
                        }
                    }
                    ShutdownResult::Shutdown(_) => {
                        info!(
                            %table_id,
                            "table sync worker interrupted by shutdown signal",
                        );
                        return Ok(ApplyLoopAction::Pause);
                    }
                }

                info!(%table_id, "table sync worker finished syncing");
            }
            TableReplicationPhase::SyncDone { lsn: sync_done_lsn } => {
                if current_lsn >= sync_done_lsn {
                    info!(
                        %table_id,
                        %sync_done_lsn,
                        %current_lsn,
                        "table transitioned to ready",
                    );

                    ctx.store
                        .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                        .await?;
                }
            }
            _ => {}
        }
    } else {
        // No active worker exists.
        match table_replication_phase {
            TableReplicationPhase::SyncDone { lsn: sync_done_lsn } => {
                if current_lsn >= sync_done_lsn {
                    info!(
                        %table_id,
                        %sync_done_lsn,
                        %current_lsn,
                        "table transitioned to ready",
                    );

                    ctx.store
                        .update_table_replication_state(table_id, TableReplicationPhase::Ready)
                        .await?;
                }
            }
            _ => {
                // Start a new worker for this table.
                let table_sync_worker = build_table_sync_worker(ctx, table_id);
                if let Err(err) = start_table_sync_worker(ctx.pool.clone(), table_sync_worker).await
                {
                    error!(
                        %table_id,
                        error = %err,
                        "failed to start table sync worker",
                    );
                }
            }
        }
    }

    Ok(ApplyLoopAction::Continue)
}

/// Processes syncing tables outside transaction (table sync worker).
///
/// If catchup position reached, transitions to SyncDone.
/// Only called when outside a transaction and the batch is empty.
async fn process_syncing_tables_outside_transaction_table_sync<S>(
    ctx: &mut TableSyncWorkerContext<S>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    try_complete_table_sync_catchup(ctx, current_lsn).await
}

/// Attempts to complete catchup and transition to SyncDone.
///
/// If catchup position has been reached, transitions to SyncDone and returns Complete.
async fn try_complete_table_sync_catchup<S>(
    ctx: &mut TableSyncWorkerContext<S>,
    current_lsn: PgLsn,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let mut inner = ctx.table_sync_worker_state.lock().await;

    if let TableReplicationPhase::Catchup { lsn } = inner.replication_phase() {
        if current_lsn >= lsn {
            inner
                .set_and_store(
                    TableReplicationPhase::SyncDone { lsn: current_lsn },
                    &ctx.state_store,
                )
                .await?;

            info!(
                table_id = %ctx.table_id,
                "table sync worker is in sync with apply worker, terminating",
            );

            return Ok(ApplyLoopAction::Complete);
        }
    }

    Ok(ApplyLoopAction::Continue)
}

/// Marks a table as errored (apply worker).
///
/// Updates the state store and continues the loop.
async fn mark_table_errored_apply<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    table_replication_error: TableReplicationError,
) -> EtlResult<ApplyLoopAction>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let pool = ctx.pool.lock().await;

    let table_id = table_replication_error.table_id();
    TableSyncWorkerState::set_and_store(
        &pool,
        &ctx.store,
        table_id,
        table_replication_error.into(),
    )
    .await?;

    Ok(ApplyLoopAction::Continue)
}

/// Marks a table as errored (table sync worker).
///
/// Updates the state and returns Complete if the table matches this worker.
async fn mark_table_errored_table_sync<S>(
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

/// Creates a new table sync worker for the specified table.
fn build_table_sync_worker<S, D>(
    ctx: &ApplyWorkerContext<S, D>,
    table_id: TableId,
) -> TableSyncWorker<S, D>
where
    S: Clone,
    D: Clone,
{
    info!(%table_id, "creating table sync worker");

    TableSyncWorker::new(
        ctx.pipeline_id,
        ctx.config.clone(),
        ctx.pool.clone(),
        table_id,
        ctx.store.clone(),
        ctx.destination.clone(),
        ctx.shutdown_rx.clone(),
        ctx.table_sync_worker_permits.clone(),
    )
}

/// Starts a table sync worker and adds it to the pool.
///
/// This helper function breaks the recursive type cycle between [`ApplyLoop`]
/// and [`TableSyncWorker`] by returning an explicitly boxed future. The `dyn Future`
/// erases the concrete type, preventing the compiler from trying to compute the
/// infinite size of the recursive future chain.
fn start_table_sync_worker<S, D>(
    pool: TableSyncWorkerPool,
    worker: TableSyncWorker<S, D>,
) -> Pin<Box<dyn Future<Output = EtlResult<bool>> + Send>>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    D: Destination + Clone + Send + Sync + 'static,
{
    let fut = async move {
        let table_id = worker.table_id();
        let mut pool_guard = pool.lock().await;

        if pool_guard.has_active_worker(table_id) {
            warn!(%table_id, "worker already exists in pool");
            return Ok(false);
        }

        let handle = worker.start().await?;
        pool_guard.try_insert_handle(table_id, handle);

        Ok(true)
    };

    Box::pin(fut)
}
