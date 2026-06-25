//! Pipeline runtime helpers.

use etl::{destination::PipelineDestination, pipeline::Pipeline, store::PipelineStore};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info, warn};

use crate::{error::ReplicatorResult, metrics};

/// Starts a pipeline and handles graceful shutdown signals.
///
/// Launches the pipeline, sets up signal handlers for SIGTERM and SIGINT,
/// and ensures proper cleanup on shutdown. The pipeline will attempt to
/// finish processing current batches before terminating.
#[tracing::instrument(skip(pipeline))]
pub(super) async fn start<S, D>(mut pipeline: Pipeline<S, D>) -> ReplicatorResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    // Start the pipeline.
    pipeline.start().await?;

    // We spawn metrics collection after the pipeline was started, so that if we
    // crash before starting we don't keep emitting metrics that make it look as
    // if the system is running.
    let metrics_tasks = metrics::spawn_metrics_tasks();

    // Spawn a task to listen for shutdown signals and trigger shutdown.
    let shutdown_tx = pipeline.shutdown_tx();
    let shutdown_handle = tokio::spawn(async move {
        // Listen for SIGTERM, sent by Kubernetes before SIGKILL during pod termination.
        //
        // If the process is killed before shutdown completes, the pipeline may become
        // corrupted, depending on the store and destination
        // implementations.
        let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
            error!("failed to register sigterm handler, shutting down pipeline");

            if let Err(err) = shutdown_tx.shutdown() {
                warn!(error = %err, "failed to send shutdown signal");
            }

            return;
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("sigint (ctrl+c) received, shutting down pipeline");
            }
            _ = sigterm.recv() => {
                info!("sigterm received, shutting down pipeline");
            }
        }

        if let Err(err) = shutdown_tx.shutdown() {
            warn!(error = %err, "failed to send shutdown signal");
        }
    });

    // Wait for the pipeline to finish (either normally or via shutdown).
    let result = pipeline.wait().await;

    // Ensure the shutdown task is finished before returning.
    // If the pipeline finished before Ctrl+C, we want to abort the shutdown task.
    // If Ctrl+C was pressed, the shutdown task will have already triggered
    // shutdown. We don't care about the result of the shutdown_handle, but we
    // should abort it if it's still running.
    shutdown_handle.abort();
    let _ = shutdown_handle.await;
    metrics_tasks.abort_and_wait().await;

    // Propagate any pipeline error.
    result?;

    Ok(())
}
