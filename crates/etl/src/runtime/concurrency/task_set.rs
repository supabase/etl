use std::{future::Future, sync::Arc};

use tokio::{
    sync::{Mutex, OwnedMutexGuard},
    task::JoinSet,
};
use tracing::{error, warn};

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

/// Reap completed tasks once the tracked set grows past this threshold.
///
/// A value of 32 keeps memory bounded while avoiding a lock-and-reap pass for
/// every single spawn.
const TASK_REAP_THRESHOLD: usize = 32;

/// Tracks background tasks and reaps them safely.
///
/// This helper is for components that want to offload accepted work to
/// background tasks while still keeping task handles owned and observable.
///
/// This type does not impose a work-concurrency policy, higher layers decide
/// when work is submitted and how much may run concurrently. It coordinates
/// task registration with reaping, draining, and shutdown.
#[derive(Debug)]
struct TaskSetInner {
    join_set: JoinSet<()>,
}

/// Shared handle used to manage spawned background tasks.
///
/// [`TaskSet`] is a small lifecycle primitive, not a scheduler.
#[derive(Debug, Clone)]
pub struct TaskSet {
    inner: Arc<Mutex<TaskSetInner>>,
}

/// Exclusive task-registration boundary retained after a [`TaskSet`] drains.
///
/// While this guard is alive, calls that access the same task registry wait for
/// it to be dropped. Dropping the guard restores access without aborting any
/// task.
#[must_use = "dropping this guard allows new tasks to register"]
#[derive(Debug)]
pub struct TaskSetDrainGuard {
    /// Retained lock for the task registry.
    _guard: OwnedMutexGuard<TaskSetInner>,
}

impl TaskSet {
    /// Creates a new task set.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(TaskSetInner { join_set: JoinSet::new() })) }
    }

    /// Spawns a new tracked background task.
    pub async fn spawn<Fut>(&self, task: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().await;
        inner.join_set.spawn(task);
    }

    /// Constructs and spawns a tracked background task after registration is
    /// admitted.
    ///
    /// Unlike [`Self::spawn`], this method does not retain the constructed task
    /// future while waiting for access to the registry. The factory runs
    /// synchronously while the registry is locked and should only construct the
    /// returned future. This keeps large task futures out of callers' async
    /// state while they wait for registration.
    pub async fn spawn_with<F, Fut>(&self, task_factory: F)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().await;
        inner.join_set.spawn(task_factory());
    }

    /// Reaps completed tasks once enough of them may have accumulated to
    /// justify the lock.
    pub async fn try_reap(&self) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        if inner.join_set.len() <= TASK_REAP_THRESHOLD {
            return Ok(());
        }

        while let Some(result) = inner.join_set.try_join_next() {
            self.handle_task_result(result)?;
        }

        Ok(())
    }

    /// Drains the task set and retains exclusive access to its task registry.
    ///
    /// Use this when resources used by registered tasks must be changed after
    /// all previously registered work has finished and before later work can
    /// start. The returned guard blocks [`TaskSet::spawn`],
    /// [`TaskSet::spawn_with`], and other registry operations until dropped.
    ///
    /// This method neither aborts tasks nor imposes a timeout. Cancelling it
    /// releases the registry and leaves unfinished tasks tracked.
    ///
    /// The registry remains locked while registered tasks are awaited. Such
    /// tasks must not directly or indirectly wait for an operation that
    /// accesses this [`TaskSet`]. The caller must likewise not await any
    /// operation that accesses this task registry while holding the returned
    /// guard.
    ///
    /// If a tracked task panics, this method returns an error without a guard.
    /// Tasks not yet joined remain tracked and continue running.
    pub async fn drain(&self) -> EtlResult<TaskSetDrainGuard> {
        let mut inner = Arc::clone(&self.inner).lock_owned().await;

        while let Some(result) = inner.join_set.join_next().await {
            self.handle_task_result(result)?;
        }

        Ok(TaskSetDrainGuard { _guard: inner })
    }

    /// Aborts and reaps all remaining tasks during shutdown.
    ///
    /// Shutdown is a one-shot lifecycle operation. We therefore intentionally
    /// keep the mutex locked while draining the remaining tasks so no new work
    /// can be submitted concurrently once shutdown has begun.
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
    /// If a task has panicked, we want to return the error immediately to avoid
    /// having invariants being violated by the fact that a panic occurred
    /// but was swallowed.
    fn handle_task_result(&self, result: Result<(), tokio::task::JoinError>) -> EtlResult<()> {
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if err.is_cancelled() {
                    warn!(
                        error = %err,
                        "background task was cancelled"
                    );

                    return Ok(());
                }

                error!(
                    error = %err,
                    "background task panicked"
                );

                Err(etl_error!(
                    ErrorKind::InvalidState,
                    "Background task panicked",
                    source: err
                ))
            }
        }
    }
}

impl Default for TaskSet {
    fn default() -> Self {
        Self::new()
    }
}
