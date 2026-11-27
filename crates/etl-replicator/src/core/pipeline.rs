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
    source_repair::SourceRepair,
};

/// Initializes a destination and pipeline, then drains running workers on
/// termination. Initialization is cancellable; a running completion future
/// must remain alive until graceful teardown finishes.
///
/// The factory keeps only its captures in the async argument storage carried
/// through the tracing wrapper. Current compiler layouts can reserve separate
/// slots for a future argument and the child being awaited; constructing the
/// initialization future inside avoids the extra large argument slot.
#[tracing::instrument(skip_all)]
pub(super) async fn start<S, D, F, Fut>(
    initialize: F,
    shutdown_signal: &mut ShutdownSignal,
    replicator_health: &ReplicatorHealth,
    source_repair: Option<&SourceRepair<'_>>,
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

    let failures =
        source_repair.map(|repair| (repair, pipeline.subscribe_source_network_failures()));
    let startup_result = with_shutdown!(
        async {
            let result = pipeline.start().await;
            if let Err(error) = &result
                && let Some(repair) = source_repair
            {
                repair.startup(error).await;
            }
            result
        },
        shutdown_signal.wait()
    );

    match startup_result {
        Some(Ok(())) => replicator_health.set_replicator_state(ReplicatorState::Running),
        result => {
            replicator_health.set_replicator_state(ReplicatorState::Stopping);

            // A signal is an orderly stop, including while waiting for repair.
            // Close the destination constructed before cancellation.
            result.unwrap_or(Ok(()))?;

            pipeline.shutdown_and_wait().await?;

            return Ok(());
        }
    }

    // Runtime metrics begin only once the pipeline has started successfully.
    let metrics_tasks = metrics::spawn_metrics_tasks();

    let pipeline_wait = pipeline.wait();
    tokio::pin!(pipeline_wait);

    let pipeline_result = with_shutdown!(
        async {
            if let Some((repair, failures)) = failures {
                tokio::select! {
                    biased;

                    result = &mut pipeline_wait => return result,

                    () = repair.run(failures) => {}
                }
            }
            (&mut pipeline_wait).await
        },
        shutdown_signal.wait()
    );
    replicator_health.set_replicator_state(ReplicatorState::Stopping);

    match pipeline_result {
        Some(result) => result?,
        None => {
            // The pipeline owns bounded draining and escalation. Keep its
            // completion future alive so cancelled workers and native writes
            // are joined before the process exits.
            pipeline.shutdown();
            pipeline_wait.await?;
        }
    }

    metrics_tasks.abort_and_wait().await?;

    Ok(())
}
