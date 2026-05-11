//! Metrics collection tasks for the replicator.

#[cfg(not(target_env = "msvc"))]
mod jemalloc;
mod tokio;

use ::tokio::task::JoinHandle;
use tracing::warn;

/// Label key for application type.
const APP_TYPE_LABEL: &str = "app_type";

/// Application type value for the replicator.
const APP_TYPE_VALUE: &str = "etl-replicator";

/// Handles for replicator-owned metrics collection tasks.
#[derive(Debug)]
pub(crate) struct MetricsTaskHandles {
    /// Spawned metrics collection tasks.
    handles: Vec<JoinHandle<()>>,
}

impl MetricsTaskHandles {
    /// Aborts all metrics tasks and waits for them to stop.
    pub(crate) async fn abort_and_wait(self) {
        for handle in &self.handles {
            handle.abort();
        }

        for handle in self.handles {
            if let Err(err) = handle.await
                && !err.is_cancelled()
            {
                warn!(error = %err, "metrics task failed while shutting down");
            }
        }
    }
}

/// Starts background metrics collection tasks for the replicator runtime.
pub(crate) fn spawn_metrics_tasks() -> MetricsTaskHandles {
    let mut handles = vec![tokio::spawn_tokio_metrics_task()];

    #[cfg(not(target_env = "msvc"))]
    handles.push(jemalloc::spawn_jemalloc_metrics_task());

    MetricsTaskHandles { handles }
}
