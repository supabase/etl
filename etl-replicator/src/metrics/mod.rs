//! Metrics collection tasks for the replicator.

#[cfg(not(target_env = "msvc"))]
mod jemalloc;
mod tokio;

/// Label key for pipeline identifier.
const PIPELINE_ID_LABEL: &str = "pipeline_id";

/// Label key for application type.
const APP_TYPE_LABEL: &str = "app_type";

/// Application type value for the replicator.
const APP_TYPE_VALUE: &str = "etl-replicator";

/// Starts background metrics collection tasks for the replicator runtime.
pub fn spawn_metrics_tasks(pipeline_id: u64) {
    tokio::spawn_tokio_metrics_task(pipeline_id);

    #[cfg(not(target_env = "msvc"))]
    jemalloc::spawn_jemalloc_metrics_task(pipeline_id);
}
