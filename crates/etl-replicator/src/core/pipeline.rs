//! Pipeline execution and graceful shutdown owned by the replicator runner.

use etl::{
    destination::PipelineDestination, error::EtlResult, pipeline::Pipeline, store::PipelineStore,
};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tracing::{error, info};

use crate::{core::ReplicatorState, error::ReplicatorResult, health::ReplicatorHealth, metrics};

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

/// Starts workers unless termination is requested, returning whether startup
/// completed.
async fn start_pipeline<S, D>(
    pipeline: &mut Pipeline<S, D>,
    replicator_health: &ReplicatorHealth,
    sigterm: &mut Signal,
) -> EtlResult<bool>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    tokio::select! {
        biased;

        _ = shutdown_requested(sigterm) => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);

            // Pipeline startup spawns workers only after its final await, so
            // cancelling pending initialization cannot leave workers behind.
            Ok(false)
        }

        result = pipeline.start() => {
            result?;
            replicator_health.set_replicator_state(ReplicatorState::Running);

            Ok(true)
        }
    }
}

/// Waits for running workers, requesting graceful shutdown on termination.
async fn wait_for_pipeline<S, D>(
    pipeline: Pipeline<S, D>,
    replicator_health: &ReplicatorHealth,
    sigterm: &mut Signal,
) -> EtlResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    let pipeline_shutdown_tx = pipeline.shutdown_tx();
    let pipeline_completion = pipeline.wait();
    tokio::pin!(pipeline_completion);

    tokio::select! {
        biased;

        _ = shutdown_requested(sigterm) => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);

            let _ = pipeline_shutdown_tx.shutdown();

            pipeline_completion.await
        }

        pipeline_result = &mut pipeline_completion => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);

            pipeline_result
        }
    }
}

/// Runs the pipeline, updating lifecycle observations and draining on
/// termination.
#[tracing::instrument(skip(pipeline, replicator_health))]
pub(super) async fn start<S, D>(
    mut pipeline: Pipeline<S, D>,
    replicator_health: ReplicatorHealth,
) -> ReplicatorResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    // Register before startup so failure cannot leave started workers behind.
    let mut sigterm = signal(SignalKind::terminate())?;

    // Try to start the pipeline.
    if !start_pipeline(&mut pipeline, &replicator_health, &mut sigterm).await? {
        return Ok(());
    }

    // Report runtime metrics only after workers have started.
    let metrics_tasks = metrics::spawn_metrics_tasks();

    // Wait for the pipeline to stop or be terminated.
    let pipeline_result = wait_for_pipeline(pipeline, &replicator_health, &mut sigterm).await;

    metrics_tasks.abort_and_wait().await;

    pipeline_result?;

    info!("pipeline stopped");

    Ok(())
}
