use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, warn};

use crate::v2::concurrency::shutdown::ShutdownRx;
use crate::v2::pipeline::PipelineId;
use crate::v2::replication::apply::ApplyLoopError;
use crate::v2::replication::client::{PgReplicationClient, PgReplicationError};
use crate::v2::replication::common::get_table_replication_states;
use crate::v2::replication::slot::{get_slot_name, SlotError};
use crate::v2::state::store::base::StateStore;
use crate::v2::workers::base::{Worker, WorkerHandle, WorkerType, WorkerWaitError};

/// The interval between background work tasks, assuming no messages are received by the background
/// worker.
const BACKGROUND_WORK_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum BackgroundWorkerError {
    #[error("Could not send message to the background worker")]
    SendMessageFailed,

    #[error("Could not wait for a response, likely because the background worker crashed")]
    WaitingForResponseFailed,

    #[error("Could not generate slot name in the background worker: {0}")]
    Slot(#[from] SlotError),

    #[error("PostgreSQL replication operation failed: {0}")]
    PgReplicationClient(#[from] PgReplicationError),
}

#[derive(Debug)]
pub enum BackgroundWorkerMessage {
    DeleteReplicationSlot(WorkerType),
}

#[derive(Debug)]
struct BackgroundWorkerMessageWrapper {
    message: BackgroundWorkerMessage,
    response_tx: Option<oneshot::Sender<Result<(), BackgroundWorkerError>>>,
}

impl BackgroundWorkerMessageWrapper {
    fn wrap_with_response(
        message: BackgroundWorkerMessage,
        response_tx: oneshot::Sender<Result<(), BackgroundWorkerError>>,
    ) -> Self {
        Self {
            message,
            response_tx: Some(response_tx),
        }
    }

    fn respond(mut self, result: Result<(), BackgroundWorkerError>) {
        if let Some(response_tx) = self.response_tx.take() {
            let _ = response_tx.send(result);
        }
    }
}

#[derive(Debug, Clone)]
pub struct BackgroundWorkerState {
    tx: mpsc::UnboundedSender<BackgroundWorkerMessageWrapper>,
}

impl BackgroundWorkerState {
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

#[derive(Debug)]
pub struct BackgroundWorkerHandle {
    state: BackgroundWorkerState,
    handle: Option<JoinHandle<Result<(), BackgroundWorkerError>>>,
}

impl WorkerHandle<BackgroundWorkerState> for BackgroundWorkerHandle {
    fn state(&self) -> BackgroundWorkerState {
        self.state.clone()
    }

    async fn wait(mut self) -> Result<(), WorkerWaitError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };

        handle.await??;

        Ok(())
    }
}

#[derive(Debug)]
pub struct BackgroundWorker<S> {
    pipeline_id: PipelineId,
    replication_client: PgReplicationClient,
    state_store: S,
    shutdown_rx: ShutdownRx,
}

impl<S> BackgroundWorker<S> {
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

    async fn start(mut self) -> Result<BackgroundWorkerHandle, Self::Error> {
        // We do use an unbounded channel since we expect in the future to have the system react to
        // memory usage.
        let (tx, mut rx) = mpsc::unbounded_channel();

        let background_worker = async move {
            loop {
                tokio::select! {
                    biased;

                    // Shutdown signal received, exit loop.
                    _ = self.shutdown_rx.changed() => {
                        return Ok(());
                    }

                    // Incoming message.
                    Some(message) = rx.recv() => {
                        handle_message(self.pipeline_id, &self.replication_client, message).await;
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

async fn delete_replication_slot(
    pipeline_id: PipelineId,
    replication_client: &PgReplicationClient,
    worker_type: WorkerType,
) -> Result<(), BackgroundWorkerError> {
    let slot_name = get_slot_name(pipeline_id, worker_type)?;
    replication_client.delete_slot(&slot_name).await?;

    Ok(())
}

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
        let slot_name = get_slot_name(
            pipeline_id,
            WorkerType::TableSync {
                table_id: *table_id,
            },
        )?;

        // In case we fail the deletion, we emit a warning but do not fail the entire worker.
        if let Err(err) = replication_client.delete_slot(&slot_name).await {
            warn!("Could not delete replication slot {slot_name} in the background: {err}")
        }
    }

    Ok(())
}
