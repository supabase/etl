//! Tokio runtime metrics for Prometheus monitoring.
//!
//! Exposes the stable Tokio runtime metrics provided by [`tokio::runtime::Handle::metrics`]
//! as Prometheus gauges. Metrics are polled on a fixed interval from a background task
//! to match the existing jemalloc reporting pattern.

use std::time::Duration;

use super::{APP_TYPE_LABEL, APP_TYPE_VALUE, PIPELINE_ID_LABEL};
use metrics::{Unit, describe_gauge, gauge};
use tracing::debug;

/// Current number of worker threads used by the runtime.
const TOKIO_METRICS_WORKERS: &str = "tokio_metrics_workers";

/// Current number of alive tasks tracked by the runtime.
const TOKIO_METRICS_ALIVE_TASKS: &str = "tokio_metrics_alive_tasks";

/// Current number of tasks waiting in the runtime global queue.
const TOKIO_METRICS_GLOBAL_QUEUE_DEPTH: &str = "tokio_metrics_global_queue_depth";

/// Total time a worker has spent busy since runtime startup.
#[cfg(target_has_atomic = "64")]
const TOKIO_METRICS_WORKER_BUSY_DURATION_SECONDS: &str =
    "tokio_metrics_worker_busy_duration_seconds";

/// Total number of times a worker has parked since runtime startup.
#[cfg(target_has_atomic = "64")]
const TOKIO_METRICS_WORKER_PARK_TOTAL: &str = "tokio_metrics_worker_park_total";

/// Total number of worker park and unpark transitions since runtime startup.
#[cfg(target_has_atomic = "64")]
const TOKIO_METRICS_WORKER_PARK_UNPARK_TOTAL: &str = "tokio_metrics_worker_park_unpark_total";

/// Polling interval for Tokio runtime statistics.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Label key for worker identifier.
#[cfg(target_has_atomic = "64")]
const WORKER_ID_LABEL: &str = "worker_id";

/// Registers Tokio runtime metric descriptions with the global metrics recorder.
fn register_metrics() {
    describe_gauge!(
        TOKIO_METRICS_WORKERS,
        Unit::Count,
        "Current number of worker threads used by the tokio runtime"
    );
    describe_gauge!(
        TOKIO_METRICS_ALIVE_TASKS,
        Unit::Count,
        "Current number of alive tasks tracked by the tokio runtime"
    );
    describe_gauge!(
        TOKIO_METRICS_GLOBAL_QUEUE_DEPTH,
        Unit::Count,
        "Current number of tasks pending in the tokio runtime global queue"
    );

    #[cfg(target_has_atomic = "64")]
    describe_gauge!(
        TOKIO_METRICS_WORKER_BUSY_DURATION_SECONDS,
        Unit::Seconds,
        "Total time a tokio runtime worker has spent busy since runtime startup"
    );
    #[cfg(target_has_atomic = "64")]
    describe_gauge!(
        TOKIO_METRICS_WORKER_PARK_TOTAL,
        Unit::Count,
        "Total number of times a tokio runtime worker has parked since runtime startup"
    );
    #[cfg(target_has_atomic = "64")]
    describe_gauge!(
        TOKIO_METRICS_WORKER_PARK_UNPARK_TOTAL,
        Unit::Count,
        "Total number of park and unpark transitions for a tokio runtime worker since runtime startup"
    );
}

/// Spawns a background task that periodically polls Tokio runtime statistics.
///
/// This function should be called after the metrics recorder is installed and from
/// within the target Tokio runtime so the task observes the correct runtime handle.
pub fn spawn_tokio_metrics_task(pipeline_id: u64) {
    register_metrics();

    let handle = tokio::runtime::Handle::current();
    let pipeline_id_str = pipeline_id.to_string();

    tokio::spawn(async move {
        loop {
            let runtime_metrics = handle.metrics();
            let num_workers = runtime_metrics.num_workers();
            let alive_tasks = runtime_metrics.num_alive_tasks() as f64;
            let global_queue_depth = runtime_metrics.global_queue_depth() as f64;

            gauge!(
                TOKIO_METRICS_WORKERS,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(num_workers as f64);
            gauge!(
                TOKIO_METRICS_ALIVE_TASKS,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(alive_tasks);
            gauge!(
                TOKIO_METRICS_GLOBAL_QUEUE_DEPTH,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(global_queue_depth);

            #[cfg(target_has_atomic = "64")]
            for worker_id in 0..num_workers {
                let worker_id_str = worker_id.to_string();

                gauge!(
                    TOKIO_METRICS_WORKER_BUSY_DURATION_SECONDS,
                    PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    WORKER_ID_LABEL => worker_id_str.clone(),
                )
                .set(
                    runtime_metrics
                        .worker_total_busy_duration(worker_id)
                        .as_secs_f64(),
                );
                gauge!(
                    TOKIO_METRICS_WORKER_PARK_TOTAL,
                    PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    WORKER_ID_LABEL => worker_id_str.clone(),
                )
                .set(runtime_metrics.worker_park_count(worker_id) as f64);
                gauge!(
                    TOKIO_METRICS_WORKER_PARK_UNPARK_TOTAL,
                    PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    WORKER_ID_LABEL => worker_id_str,
                )
                .set(runtime_metrics.worker_park_unpark_count(worker_id) as f64);
            }

            debug!(
                num_workers,
                alive_tasks, global_queue_depth, "tokio runtime stats updated"
            );

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}
