//! Pipeline execution and graceful shutdown owned by the replicator runner.

use etl::{destination::PipelineDestination, pipeline::Pipeline, store::PipelineStore};
use tokio::{
    signal::unix::{Signal, SignalKind, signal},
    sync::watch,
};
use tracing::{error, info};

use crate::{core::PipelineState, error::ReplicatorResult, metrics};

/// Waits for a process termination request.
async fn shutdown_requested(sigterm: &mut Signal) {
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                error!(error = %error, "failed to listen for sigint, shutting down pipeline");
            } else {
                info!(signal = "sigint", "pipeline shutdown requested");
            }
        }

        _ = sigterm.recv() => {
            info!(signal = "sigterm", "pipeline shutdown requested");
        }
    }
}

/// Runs the pipeline, publishing lifecycle observations and draining on
/// termination.
#[tracing::instrument(skip(pipeline, pipeline_state_tx))]
pub(super) async fn start<S, D>(
    mut pipeline: Pipeline<S, D>,
    pipeline_state_tx: watch::Sender<PipelineState>,
) -> ReplicatorResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    // Register before startup so failure cannot leave started workers behind.
    let mut sigterm = signal(SignalKind::terminate())?;

    pipeline.start().await?;

    pipeline_state_tx.send_replace(PipelineState::Running);

    // Report runtime metrics only after workers have started.
    let metrics_tasks = metrics::spawn_metrics_tasks();

    let pipeline_shutdown_tx = pipeline.shutdown_tx();

    // We prepare the future to wait on the pipeline completion.
    let pipeline_completion = pipeline.wait();
    tokio::pin!(pipeline_completion);

    let pipeline_result = tokio::select! {
        pipeline_result = &mut pipeline_completion => {
            pipeline_state_tx.send_replace(PipelineState::Stopping);

            pipeline_result
        }

        _ = shutdown_requested(&mut sigterm) => {
            pipeline_state_tx.send_replace(PipelineState::Stopping);

            let _ = pipeline_shutdown_tx.shutdown();

            pipeline_completion.await
        }
    };

    metrics_tasks.abort_and_wait().await;

    pipeline_result?;

    info!("pipeline stopped");

    Ok(())
}
