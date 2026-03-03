//! Tokio runtime metrics for Prometheus monitoring.
//!
//! Exposes Tokio runtime metrics provided by [`tokio::runtime::Handle::metrics`] as
//! Prometheus gauges. This includes stable metrics and additional unstable metrics
//! when compiled with `tokio_unstable`.
//!
//! Metrics are polled on a fixed interval from a background task to match the existing
//! jemalloc reporting pattern. Monotonic runtime totals are exported as gauges because
//! Tokio exposes them as absolute snapshots rather than deltas.

use std::time::Duration;

use metrics::{Unit, describe_gauge, gauge};
use tracing::debug;

use crate::metrics::{APP_TYPE_LABEL, APP_TYPE_VALUE, PIPELINE_ID_LABEL};

/// Current number of worker threads used by the runtime.
const TOKIO_METRICS_WORKERS: &str = "tokio_metrics_workers";

/// Current number of alive tasks tracked by the runtime.
const TOKIO_METRICS_ALIVE_TASKS: &str = "tokio_metrics_alive_tasks";

/// Current number of tasks waiting in the runtime global queue.
const TOKIO_METRICS_GLOBAL_QUEUE_DEPTH: &str = "tokio_metrics_global_queue_depth";

/// Current number of tasks waiting in a worker local queue.
#[cfg(tokio_unstable)]
const TOKIO_METRICS_WORKER_LOCAL_QUEUE_DEPTH: &str = "tokio_metrics_worker_local_queue_depth";

/// Current number of threads in the blocking thread pool.
#[cfg(tokio_unstable)]
const TOKIO_METRICS_BLOCKING_THREADS: &str = "tokio_metrics_blocking_threads";

/// Current number of idle threads in the blocking thread pool.
#[cfg(tokio_unstable)]
const TOKIO_METRICS_IDLE_BLOCKING_THREADS: &str = "tokio_metrics_idle_blocking_threads";

/// Current number of tasks waiting in the blocking thread pool queue.
#[cfg(tokio_unstable)]
const TOKIO_METRICS_BLOCKING_QUEUE_DEPTH: &str = "tokio_metrics_blocking_queue_depth";

/// Total number of tasks scheduled from outside the runtime since startup.
#[cfg(all(tokio_unstable, target_has_atomic = "64"))]
const TOKIO_METRICS_REMOTE_SCHEDULE_TOTAL: &str = "tokio_metrics_remote_schedule_total";

/// Total number of forced cooperative yields since startup.
#[cfg(all(tokio_unstable, target_has_atomic = "64"))]
const TOKIO_METRICS_BUDGET_FORCED_YIELD_TOTAL: &str = "tokio_metrics_budget_forced_yield_total";

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

/// Total number of tasks stolen by a worker since runtime startup.
#[cfg(all(tokio_unstable, target_has_atomic = "64"))]
const TOKIO_METRICS_WORKER_STEAL_TOTAL: &str = "tokio_metrics_worker_steal_total";

/// Total number of steal operations performed by a worker since runtime startup.
#[cfg(all(tokio_unstable, target_has_atomic = "64"))]
const TOKIO_METRICS_WORKER_STEAL_OPERATIONS_TOTAL: &str =
    "tokio_metrics_worker_steal_operations_total";

/// Polling interval for Tokio runtime statistics.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Label key for worker identifier.
const WORKER_ID_LABEL: &str = "worker_id";

/// Emits a structured debug log for the current Tokio runtime metrics snapshot.
#[cfg(all(tokio_unstable, target_has_atomic = "64"))]
fn log_runtime_metrics(
    num_workers: usize,
    alive_tasks: f64,
    global_queue_depth: f64,
    blocking_threads: f64,
    idle_blocking_threads: f64,
    blocking_queue_depth: f64,
    remote_schedule_total: f64,
    budget_forced_yield_total: f64,
) {
    debug!(
        num_workers,
        alive_tasks,
        global_queue_depth,
        blocking_threads,
        idle_blocking_threads,
        blocking_queue_depth,
        remote_schedule_total,
        budget_forced_yield_total,
        "tokio runtime stats updated"
    );
}

/// Emits a structured debug log for the current Tokio runtime metrics snapshot.
#[cfg(all(tokio_unstable, not(target_has_atomic = "64")))]
fn log_runtime_metrics(
    num_workers: usize,
    alive_tasks: f64,
    global_queue_depth: f64,
    blocking_threads: f64,
    idle_blocking_threads: f64,
    blocking_queue_depth: f64,
) {
    debug!(
        num_workers,
        alive_tasks,
        global_queue_depth,
        blocking_threads,
        idle_blocking_threads,
        blocking_queue_depth,
        "tokio runtime stats updated"
    );
}

/// Emits a structured debug log for the current Tokio runtime metrics snapshot.
#[cfg(all(not(tokio_unstable), target_has_atomic = "64"))]
fn log_runtime_metrics(num_workers: usize, alive_tasks: f64, global_queue_depth: f64) {
    debug!(
        num_workers,
        alive_tasks, global_queue_depth, "tokio runtime stats updated"
    );
}

/// Emits a structured debug log for the current Tokio runtime metrics snapshot.
#[cfg(all(not(tokio_unstable), not(target_has_atomic = "64")))]
fn log_runtime_metrics(num_workers: usize, alive_tasks: f64, global_queue_depth: f64) {
    debug!(
        num_workers,
        alive_tasks, global_queue_depth, "tokio runtime stats updated"
    );
}

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
    #[cfg(tokio_unstable)]
    describe_gauge!(
        TOKIO_METRICS_WORKER_LOCAL_QUEUE_DEPTH,
        Unit::Count,
        "Current number of tasks pending in a tokio runtime worker local queue"
    );
    #[cfg(tokio_unstable)]
    describe_gauge!(
        TOKIO_METRICS_BLOCKING_THREADS,
        Unit::Count,
        "Current number of threads in the tokio blocking thread pool"
    );
    #[cfg(tokio_unstable)]
    describe_gauge!(
        TOKIO_METRICS_IDLE_BLOCKING_THREADS,
        Unit::Count,
        "Current number of idle threads in the tokio blocking thread pool"
    );
    #[cfg(tokio_unstable)]
    describe_gauge!(
        TOKIO_METRICS_BLOCKING_QUEUE_DEPTH,
        Unit::Count,
        "Current number of tasks pending in the tokio blocking thread pool queue"
    );
    #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
    describe_gauge!(
        TOKIO_METRICS_REMOTE_SCHEDULE_TOTAL,
        Unit::Count,
        "Total number of tasks scheduled from outside the tokio runtime since startup"
    );
    #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
    describe_gauge!(
        TOKIO_METRICS_BUDGET_FORCED_YIELD_TOTAL,
        Unit::Count,
        "Total number of forced cooperative yields in the tokio runtime since startup"
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
    #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
    describe_gauge!(
        TOKIO_METRICS_WORKER_STEAL_TOTAL,
        Unit::Count,
        "Total number of tasks stolen by a tokio runtime worker since startup"
    );
    #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
    describe_gauge!(
        TOKIO_METRICS_WORKER_STEAL_OPERATIONS_TOTAL,
        Unit::Count,
        "Total number of steal operations performed by a tokio runtime worker since startup"
    );
}

/// Spawns a background task that periodically polls Tokio runtime statistics.
///
/// This function should be called after the metrics recorder is installed and from
/// within the target Tokio runtime so the task observes the correct runtime handle.
///
/// Stable metrics are always exported. Additional metrics guarded by `tokio_unstable`
/// are exported only when the binary is compiled with that cfg enabled.
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
            #[cfg(tokio_unstable)]
            let blocking_threads = runtime_metrics.num_blocking_threads() as f64;
            #[cfg(tokio_unstable)]
            let idle_blocking_threads = runtime_metrics.num_idle_blocking_threads() as f64;
            #[cfg(tokio_unstable)]
            let blocking_queue_depth = runtime_metrics.blocking_queue_depth() as f64;
            #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
            let remote_schedule_total = runtime_metrics.remote_schedule_count() as f64;
            #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
            let budget_forced_yield_total = runtime_metrics.budget_forced_yield_count() as f64;
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
            #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
            gauge!(
                TOKIO_METRICS_REMOTE_SCHEDULE_TOTAL,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(remote_schedule_total);
            #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
            gauge!(
                TOKIO_METRICS_BUDGET_FORCED_YIELD_TOTAL,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(budget_forced_yield_total);
            #[cfg(tokio_unstable)]
            gauge!(
                TOKIO_METRICS_BLOCKING_THREADS,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(blocking_threads);
            #[cfg(tokio_unstable)]
            gauge!(
                TOKIO_METRICS_IDLE_BLOCKING_THREADS,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(idle_blocking_threads);
            #[cfg(tokio_unstable)]
            gauge!(
                TOKIO_METRICS_BLOCKING_QUEUE_DEPTH,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(blocking_queue_depth);

            #[cfg(target_has_atomic = "64")]
            for worker_id in 0..num_workers {
                let worker_id_str = worker_id.to_string();
                let worker_busy_duration = runtime_metrics
                    .worker_total_busy_duration(worker_id)
                    .as_secs_f64();
                let worker_park_count = runtime_metrics.worker_park_count(worker_id) as f64;
                let worker_park_unpark_count =
                    runtime_metrics.worker_park_unpark_count(worker_id) as f64;

                gauge!(
                    TOKIO_METRICS_WORKER_BUSY_DURATION_SECONDS,
                    PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    WORKER_ID_LABEL => worker_id_str.clone(),
                )
                .set(worker_busy_duration);
                gauge!(
                    TOKIO_METRICS_WORKER_PARK_TOTAL,
                    PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    WORKER_ID_LABEL => worker_id_str.clone(),
                )
                .set(worker_park_count);
                gauge!(
                    TOKIO_METRICS_WORKER_PARK_UNPARK_TOTAL,
                    PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    WORKER_ID_LABEL => worker_id_str,
                )
                .set(worker_park_unpark_count);
                #[cfg(tokio_unstable)]
                {
                    let local_queue_depth =
                        runtime_metrics.worker_local_queue_depth(worker_id) as f64;
                    gauge!(
                        TOKIO_METRICS_WORKER_LOCAL_QUEUE_DEPTH,
                        PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                        APP_TYPE_LABEL => APP_TYPE_VALUE,
                        WORKER_ID_LABEL => worker_id.to_string(),
                    )
                    .set(local_queue_depth);
                    #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
                    {
                        let steal_total = runtime_metrics.worker_steal_count(worker_id) as f64;
                        let steal_operations_total =
                            runtime_metrics.worker_steal_operations(worker_id) as f64;
                        gauge!(
                            TOKIO_METRICS_WORKER_STEAL_TOTAL,
                            PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                            APP_TYPE_LABEL => APP_TYPE_VALUE,
                            WORKER_ID_LABEL => worker_id.to_string(),
                        )
                        .set(steal_total);
                        gauge!(
                            TOKIO_METRICS_WORKER_STEAL_OPERATIONS_TOTAL,
                            PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                            APP_TYPE_LABEL => APP_TYPE_VALUE,
                            WORKER_ID_LABEL => worker_id.to_string(),
                        )
                        .set(steal_operations_total);
                    }
                }
            }

            #[cfg(all(tokio_unstable, target_has_atomic = "64"))]
            log_runtime_metrics(
                num_workers,
                alive_tasks,
                global_queue_depth,
                blocking_threads,
                idle_blocking_threads,
                blocking_queue_depth,
                remote_schedule_total,
                budget_forced_yield_total,
            );
            #[cfg(all(tokio_unstable, not(target_has_atomic = "64")))]
            log_runtime_metrics(
                num_workers,
                alive_tasks,
                global_queue_depth,
                blocking_threads,
                idle_blocking_threads,
                blocking_queue_depth,
            );
            #[cfg(all(not(tokio_unstable), target_has_atomic = "64"))]
            log_runtime_metrics(num_workers, alive_tasks, global_queue_depth);
            #[cfg(all(not(tokio_unstable), not(target_has_atomic = "64")))]
            log_runtime_metrics(num_workers, alive_tasks, global_queue_depth);

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}
