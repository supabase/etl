//! Metrics collection tasks for the replicator.

#[cfg(not(target_env = "msvc"))]
mod jemalloc;
mod tokio;

use etl::{error::EtlResult, task::abort_and_join};
use tokio_util::task::AbortOnDropHandle;

/// Label key for application type.
const APP_TYPE_LABEL: &str = "app_type";

/// Application type value for the replicator.
const APP_TYPE_VALUE: &str = "etl-replicator";

/// Handles for replicator-owned metrics collection tasks.
#[derive(Debug)]
pub(crate) struct MetricsTaskHandles {
    /// Spawned metrics collection tasks.
    handles: Vec<AbortOnDropHandle<()>>,
}

impl MetricsTaskHandles {
    /// Aborts all metrics tasks and waits for them to stop.
    pub(crate) async fn abort_and_wait(self) -> EtlResult<()> {
        for handle in &self.handles {
            handle.abort();
        }

        let mut errors = Vec::new();
        for handle in self.handles {
            if let Err(error) = abort_and_join(handle).await {
                errors.push(error);
            }
        }

        if errors.is_empty() { Ok(()) } else { Err(errors.into()) }
    }
}

/// Starts background metrics collection tasks for the replicator runtime.
pub(crate) fn spawn_metrics_tasks() -> MetricsTaskHandles {
    let mut handles = vec![tokio::spawn_tokio_metrics_task()];

    #[cfg(not(target_env = "msvc"))]
    handles.push(jemalloc::spawn_jemalloc_metrics_task());

    MetricsTaskHandles { handles }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use tokio_util::task::AbortOnDropHandle;

    use crate::metrics::MetricsTaskHandles;

    /// A panic is returned after the other metrics tasks have stopped.
    #[tokio::test]
    async fn metrics_shutdown_reports_panic_after_joining_siblings() {
        let (panic_tx, panic_rx) = oneshot::channel::<()>();
        let panicking = tokio::spawn(async move {
            let _lifetime = panic_tx;
            panic!("Test metrics task panic");
        });
        assert!(panic_rx.await.is_err());
        let (lifetime_tx, mut lifetime_rx) = oneshot::channel::<()>();
        let pending = tokio::spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending::<()>().await;
        });
        let tasks = MetricsTaskHandles {
            handles: vec![AbortOnDropHandle::new(panicking), AbortOnDropHandle::new(pending)],
        };

        assert!(tasks.abort_and_wait().await.is_err());
        assert_eq!(lifetime_rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    }
}
