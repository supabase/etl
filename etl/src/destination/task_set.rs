use std::future::Future;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, warn};

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;

/// Reap completed destination tasks once the tracked set grows past this threshold.
///
/// A value of 32 keeps memory bounded while avoiding a lock-and-reap pass for every single write.
const DESTINATION_TASK_REAP_THRESHOLD: usize = 32;

/// Tracks destination-owned background tasks and reaps them safely.
///
/// This helper is for destinations that want to offload accepted work to background tasks. The
/// main use case today is the [`write_events`] method.
///
/// It intentionally does not add its own concurrency control. ETL already limits concurrent
/// destination work through the apply worker and table sync workers, so this type only manages the
/// lifecycle of spawned destination tasks: reap completed work during normal operation and shut
/// down outstanding tasks safely.
#[derive(Debug)]
struct DestinationTaskSetInner {
    join_set: JoinSet<()>,
}

/// Shared handle used by destinations to manage spawned background tasks.
///
/// [`DestinationTaskSet`] is a small lifecycle primitive, not a scheduler.
#[derive(Debug, Clone)]
pub struct DestinationTaskSet {
    inner: Arc<Mutex<DestinationTaskSetInner>>,
}

impl DestinationTaskSet {
    /// Creates a new task set for a destination.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(DestinationTaskSetInner {
                join_set: JoinSet::new(),
            })),
        }
    }

    /// Spawns a new tracked background task.
    pub async fn spawn<Fut>(&self, task: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().await;
        inner.join_set.spawn(task);
    }

    /// Reaps completed tasks once enough of them may have accumulated to justify the lock.
    pub async fn try_reap(&self) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        if inner.join_set.len() <= DESTINATION_TASK_REAP_THRESHOLD {
            return Ok(());
        }

        while let Some(result) = inner.join_set.try_join_next() {
            self.handle_task_result(result)?;
        }

        Ok(())
    }

    /// Reaps all remaining tasks during shutdown.
    ///
    /// Destination shutdown is a one-shot lifecycle operation. We therefore intentionally keep the
    /// mutex locked while draining the remaining tasks so no new work can be submitted
    /// concurrently once shutdown has begun. We abort all remaining tasks first so shutdown does
    /// not wait for background writes that are no longer needed.
    pub async fn shutdown(&self) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        inner.join_set.abort_all();

        while let Some(result) = inner.join_set.join_next().await {
            self.handle_task_result(result)?;
        }

        Ok(())
    }

    /// Handles the outcome of a completed task.
    ///
    /// If a task has panicked, we want to return the error immediately to avoid having invariants
    /// being violated by the fact that a panic occurred but was swallowed.
    fn handle_task_result(&self, result: Result<(), tokio::task::JoinError>) -> EtlResult<()> {
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if err.is_cancelled() {
                    warn!(
                        error = %err,
                        "destination background task was cancelled"
                    );

                    return Ok(());
                }

                error!(
                    error = %err,
                    "destination background task panicked"
                );

                Err(etl_error!(
                    ErrorKind::DestinationError,
                    "Destination background task panicked",
                    err.to_string()
                ))
            }
        }
    }
}

impl Default for DestinationTaskSet {
    fn default() -> Self {
        Self::new()
    }
}
