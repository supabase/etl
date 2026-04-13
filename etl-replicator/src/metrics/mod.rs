//! Metrics collection tasks for the replicator.

#[cfg(not(target_env = "msvc"))]
mod jemalloc;
mod tokio;

/// Label key for application type.
const APP_TYPE_LABEL: &str = "app_type";

/// Application type value for the replicator.
const APP_TYPE_VALUE: &str = "etl-replicator";

/// Starts background metrics collection tasks for the replicator runtime.
pub fn spawn_metrics_tasks() {
    tokio::spawn_tokio_metrics_task();

    #[cfg(not(target_env = "msvc"))]
    jemalloc::spawn_jemalloc_metrics_task();
}
