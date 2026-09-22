//! Pipeline initialization and graceful shutdown owned by the replicator.

use std::future::Future;

use etl::{destination::PipelineDestination, pipeline::Pipeline, store::PipelineStore};

use crate::{
    core::{
        ReplicatorState,
        shutdown::{ShutdownSignal, with_shutdown},
    },
    error::ReplicatorResult,
    health::ReplicatorHealth,
    metrics,
};

/// Initializes a destination and pipeline, then drains running workers on
/// termination. Initialization is cancellable; a running completion future
/// must remain alive until graceful teardown finishes.
///
/// The factory keeps only its captures in the async argument storage carried
/// through the tracing wrapper. Current compiler layouts can reserve separate
/// slots for a future argument and the child being awaited; constructing the
/// initialization future inside avoids the extra large argument slot.
#[tracing::instrument(skip(initialize, shutdown_signal, replicator_health))]
pub(super) async fn start<S, D, F, Fut>(
    initialize: F,
    shutdown_signal: &mut ShutdownSignal,
    replicator_health: &ReplicatorHealth,
) -> ReplicatorResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
    F: FnOnce() -> Fut,
    Fut: Future<Output = ReplicatorResult<Pipeline<S, D>>>,
{
    let Some(result) = with_shutdown!(initialize(), shutdown_signal.wait()) else {
        return Ok(());
    };
    let mut pipeline = result?;

    let startup_result = with_shutdown!(pipeline.start(), shutdown_signal.wait());

    match startup_result {
        Some(Ok(())) => replicator_health.set_replicator_state(ReplicatorState::Running),
        result => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);

            // Startup failures return immediately. A signal is an orderly
            // stop, so close the destination constructed before cancellation.
            result.unwrap_or(Ok(()))?;

            pipeline.shutdown_and_wait().await?;

            return Ok(());
        }
    }

    // Runtime metrics begin only once the pipeline has started successfully.
    let metrics_tasks = metrics::spawn_metrics_tasks();

    let pipeline_wait = pipeline.wait();
    tokio::pin!(pipeline_wait);

    let pipeline_result = with_shutdown!(&mut pipeline_wait, shutdown_signal.wait());
    replicator_health.set_replicator_state(ReplicatorState::Stopping);

    match pipeline_result {
        Some(result) => result?,
        None => {
            // Kubernetes enforces the grace period with SIGKILL. Do not drop
            // the completion future while transactions and writes drain.
            pipeline.shutdown();
            pipeline_wait.await?;
        }
    }

    metrics_tasks.abort_and_wait().await?;

    Ok(())
}
