//! Pipeline initialization and graceful shutdown owned by the replicator.

use std::future::Future;

use etl::{
    destination::PipelineDestination, error::EtlError, pipeline::Pipeline, store::PipelineStore,
};

use crate::{
    core::{ReplicatorState, shutdown::ShutdownSignal},
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
    let mut pipeline = tokio::select! {
        biased;

        _ = shutdown_signal.wait() => return Ok(()),

        result = initialize() => result?,
    };

    let startup_result = tokio::select! {
        biased;

        _ = shutdown_signal.wait() => None,

        result = pipeline.start() => Some(result),
    };
    match startup_result {
        Some(Ok(())) => replicator_health.set_replicator_state(ReplicatorState::Running),
        result => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);

            // Pipeline startup spawns workers only after its final await.
            // Cancel pending initialization, then close the constructed
            // destination even when startup failed before workers existed.
            let cleanup_result = pipeline.shutdown_and_wait().await;
            let errors: Vec<_> = [result.unwrap_or(Ok(())), cleanup_result]
                .into_iter()
                .filter_map(Result::err)
                .collect();
            return if errors.is_empty() { Ok(()) } else { Err(EtlError::from(errors).into()) };
        }
    }

    // Runtime metrics begin only once the pipeline has started successfully.
    let metrics_tasks = metrics::spawn_metrics_tasks();
    let pipeline_wait = pipeline.wait();
    tokio::pin!(pipeline_wait);
    let result = tokio::select! {
        biased;

        _ = shutdown_signal.wait() => {
            // Kubernetes enforces the grace period with SIGKILL. Do not drop
            // the completion future while transactions and writes drain.
            replicator_health.set_replicator_state(ReplicatorState::Stopping);
            pipeline.shutdown();
            pipeline_wait.await
        }

        result = &mut pipeline_wait => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);
            result
        }
    };
    let metrics_result = metrics_tasks.abort_and_wait().await;
    let errors: Vec<_> = [result, metrics_result].into_iter().filter_map(Result::err).collect();
    if errors.is_empty() { Ok(()) } else { Err(EtlError::from(errors).into()) }
}
