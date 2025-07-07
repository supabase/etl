use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{error, warn};

use crate::v2::concurrency::shutdown::ShutdownRx;
use crate::v2::pipeline::PipelineId;
use crate::v2::replication::apply::ApplyLoopError;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::common::get_table_replication_states;
use crate::v2::replication::slot::{get_slot_name, SlotError};
use crate::v2::state::store::base::StateStore;
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerType, WorkerWaitError};

/// The interval between background work tasks when no messages are received.
const BACKGROUND_WORK_INTERVAL: Duration = Duration::from_secs(1);

/// The maximum amount of time to wait for a slot to be deleted.
const MAX_DELETE_SLOT_WAIT: Duration = Duration::from_secs(10);

/// Errors that can occur during background worker operations.
#[derive(Debug, Error)]
pub enum BackgroundWorkerError {
    #[error("Could not send message to the background worker")]
    SendMessageFailed,

    #[error("Could not wait for a response, likely because the background worker crashed")]
    WaitingForResponseFailed,

    #[error("Could not delete the replication slot within the expected time")]
    DeleteSlotTimeout,

    #[error("Could not generate slot name in the background worker: {0}")]
    Slot(#[from] SlotError),

    #[error("PostgreSQL replication operation failed: {0}")]
    PgReplicationClient(#[from] PgReplicationError),
}

/// Messages that can be sent to the background worker.
#[derive(Debug)]
pub enum BackgroundWorkerMessage {
    /// Request to delete a replication slot for the specified worker type.
    DeleteReplicationSlot(WorkerType),
}

/// Internal wrapper for background worker messages that includes an optional response channel.
#[derive(Debug)]
struct BackgroundWorkerMessageWrapper {
    message: BackgroundWorkerMessage,
    response_tx: Option<oneshot::Sender<Result<(), BackgroundWorkerError>>>,
}

impl BackgroundWorkerMessageWrapper {
    /// Creates a new message wrapper with a response channel.
    fn wrap_with_response(
        message: BackgroundWorkerMessage,
        response_tx: oneshot::Sender<Result<(), BackgroundWorkerError>>,
    ) -> Self {
        Self {
            message,
            response_tx: Some(response_tx),
        }
    }

    /// Sends a response back to the caller if a response channel is available.
    fn respond(mut self, result: Result<(), BackgroundWorkerError>) {
        if let Some(response_tx) = self.response_tx.take() {
            let _ = response_tx.send(result);
        }
    }
}

/// State handle for communicating with the background worker.
#[derive(Debug, Clone)]
pub struct BackgroundWorkerState {
    tx: mpsc::UnboundedSender<BackgroundWorkerMessageWrapper>,
}

impl BackgroundWorkerState {
    /// Sends a message to the background worker and waits for a response.
    ///
    /// This method ensures that the requested operation completes before returning,
    /// providing consistency guarantees to the caller.
    pub async fn send_sync_message(
        &self,
        message: BackgroundWorkerMessage,
    ) -> Result<(), BackgroundWorkerError> {
        // We send the wrapped message.
        let (response_tx, response_rx) = oneshot::channel();
        let message = BackgroundWorkerMessageWrapper::wrap_with_response(message, response_tx);
        self.tx
            .send(message)
            .map_err(|_| BackgroundWorkerError::SendMessageFailed)?;

        // We wait for a response from the worker and in case we don't get any, we propagate the
        // error, since not getting a response and silently failing, might cause consistency issues
        // since the caller uses a sync message exactly to have a guarantee that something happened.
        let result = response_rx
            .await
            .map_err(|_| BackgroundWorkerError::WaitingForResponseFailed)?;

        result
    }
}

/// Handle for managing the background worker lifecycle.
#[derive(Debug)]
pub struct BackgroundWorkerHandle {
    state: BackgroundWorkerState,
    handle: Option<JoinHandle<Result<(), BackgroundWorkerError>>>,
}

impl WorkerHandle<BackgroundWorkerState> for BackgroundWorkerHandle {
    /// Returns a clone of the background worker state for communication.
    fn state(&self) -> BackgroundWorkerState {
        self.state.clone()
    }

    /// Waits for the background worker to complete and returns its result.
    async fn wait(mut self) -> Result<(), WorkerWaitError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await??;

        Ok(())
    }
}

/// Background worker that performs periodic maintenance tasks.
///
/// This worker runs continuously in the background, handling cleanup operations like removing
/// unused replication slots.
#[derive(Debug)]
pub struct BackgroundWorker<S> {
    pipeline_id: PipelineId,
    replication_client: PgReplicationClient,
    state_store: S,
    shutdown_rx: ShutdownRx,
}

impl<S> BackgroundWorker<S> {
    /// Creates a new background worker with the specified components.
    pub fn new(
        pipeline_id: PipelineId,
        replication_client: PgReplicationClient,
        state_store: S,
        shutdown_rx: ShutdownRx,
    ) -> Self {
        Self {
            pipeline_id,
            replication_client,
            state_store,
            shutdown_rx,
        }
    }
}

impl<S> Worker<BackgroundWorkerHandle, BackgroundWorkerState> for BackgroundWorker<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    type Error = BackgroundWorkerError;

    /// Starts the background worker and returns a handle for communication.
    ///
    /// The worker runs in a loop, prioritizing message handling over periodic tasks,
    /// and responds to shutdown signals gracefully.
    async fn start(mut self) -> Result<BackgroundWorkerHandle, Self::Error> {
        // We do use an unbounded channel since we expect in the future to have the system react to
        // memory usage.
        let (tx, mut rx) = mpsc::unbounded_channel();

        let background_worker = async move {
            loop {
                tokio::select! {
                    biased;

                    // We prioritize the handling of incoming messages, since in case we are told
                    // to shutdown, we want to go through all remaining messages to make sure we do
                    // not miss any important work.
                    //
                    // This mechanism can work only if the producers of messages for this worker stop
                    // sending messages when they also receive a shutdown signal.
                    Some(message) = rx.recv() => {
                        handle_message(self.pipeline_id, &self.replication_client, message).await;
                    }

                    // Shutdown signal received, exit loop.
                    _ = self.shutdown_rx.changed() => {
                        return Ok(());
                    }

                    // At regular intervals, we perform background operations.
                    _ = tokio::time::sleep(BACKGROUND_WORK_INTERVAL) => {
                        perform_periodic_background_work(self.pipeline_id, &self.replication_client, &self.state_store).await;
                    }
                }
            }
        };

        let handle = tokio::spawn(background_worker);

        Ok(BackgroundWorkerHandle {
            state: BackgroundWorkerState { tx },
            handle: Some(handle),
        })
    }
}

/// Handles incoming messages from clients and sends responses back.
async fn handle_message(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    message: BackgroundWorkerMessageWrapper,
) {
    match &message.message {
        BackgroundWorkerMessage::DeleteReplicationSlot(worker_type) => {
            let result =
                delete_replication_slot(pipeline_id, replication_client, *worker_type).await;
            message.respond(result);
        }
    }
}

/// Deletes a replication slot for the specified worker type.
///
/// If the slot is not found, logs a warning but returns success since the
/// desired outcome (slot not existing) is achieved.
async fn delete_replication_slot(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    worker_type: WorkerType,
) -> Result<(), BackgroundWorkerError> {
    let slot_name = get_slot_name(pipeline_id, worker_type)?;
    // Since we are using the `WAIT` option while deleting a slot, there is a non-zero chance that we
    // will be blocked indefinitely. To avoid this, we want to put an upper bound in time when executing
    // this query. If we fail to execute it, we return an error.
    let Ok(result) = timeout(
        MAX_DELETE_SLOT_WAIT,
        replication_client.delete_slot(&slot_name),
    )
    .await
    else {
        warn!("Could not delete replication slot {slot_name} within the timeout, maybe a connection is still holding it");
        return Err(BackgroundWorkerError::DeleteSlotTimeout);
    };

    if let Err(PgReplicationError::SlotNotFound(_)) = result {
        warn!(
            "Attempted to delete replication slot {} but it was not found",
            slot_name
        );

        return Ok(());
    }

    result?;

    Ok(())
}

/// Performs periodic background maintenance tasks.
///
/// Currently, handles cleanup of unused table sync worker replication slots.
/// Errors are logged but do not stop the background worker.
async fn perform_periodic_background_work<S>(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    state_store: &S,
) where
    S: StateStore + Clone + Send + Sync + 'static,
{
    if let Err(err) =
        cleanup_unused_table_sync_worker_slots(pipeline_id, replication_client, state_store).await
    {
        error!(
            "An error occurred while cleaning up unsued table sync worker replication slots: {err}"
        );
    }
}

/// Cleans up replication slots for table sync workers that have completed their work.
///
/// Retrieves all completed table replication states and attempts to delete their
/// corresponding replication slots. Individual deletion failures are logged but
/// do not prevent cleanup of other slots.
async fn cleanup_unused_table_sync_worker_slots<S>(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    state_store: &S,
) -> Result<(), ApplyLoopError>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    let done_table_replication_states = get_table_replication_states(state_store, true).await?;

    for table_id in done_table_replication_states.keys() {
        // In case we fail the deletion, we emit a warning but do not fail the entire worker.
        let worker_type = WorkerType::TableSync {
            table_id: *table_id,
        };
        if let Err(err) =
            delete_replication_slot(pipeline_id, replication_client, worker_type).await
        {
            warn!("Could not delete replication slot in the background: {err}");
        }
    }

    Ok(())
}
