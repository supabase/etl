//! Shared registration and teardown for background tasks.

use std::{future::Future, sync::Arc};

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{error::EtlResult, task::TaskGroup};

/// Reap completed tasks once the tracked set grows past this threshold.
///
/// A value of 32 keeps memory bounded while avoiding a lock-and-reap pass for
/// every single spawn.
const TASK_REAP_THRESHOLD: usize = 32;

/// Shared handle used to manage spawned background tasks.
///
/// Dropping the last owner requests abort of registered tasks. Tasks capturing
/// the registry, including through destination clones, create ownership cycles
/// that require explicit shutdown. Some built-in destinations retain such
/// cycles.
///
/// A failed operation discards the tracked group and requests cancellation.
/// Reusing the registry does not wait for previously discarded tasks to stop;
/// resource reuse must account for cancellation and any native or remote work.
#[derive(Debug, Clone)]
pub struct TaskRegistry {
    inner: Arc<Mutex<TaskGroup<()>>>,
}

/// Exclusive task-registration boundary retained after a [`TaskRegistry`]
/// drains.
///
/// While this guard is alive, calls that access the same task registry wait for
/// it to be dropped. Dropping the guard restores access without aborting any
/// task.
#[must_use = "dropping this guard allows new tasks to register"]
#[derive(Debug)]
pub struct TaskRegistryDrainGuard {
    /// Retained lock for the task registry.
    _guard: OwnedMutexGuard<TaskGroup<()>>,
}

impl TaskRegistry {
    /// Creates a new task set.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(TaskGroup::new())) }
    }

    /// Spawns a new tracked background task.
    ///
    /// # Panics
    ///
    /// Panics when polled outside a Tokio runtime.
    pub async fn spawn<Fut>(&self, task: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().await;

        inner.spawn(async move {
            task.await;

            Ok(())
        });
    }

    /// Constructs and spawns a tracked background task after registration is
    /// admitted.
    ///
    /// Unlike [`Self::spawn`], this method does not retain the constructed task
    /// future while waiting for access to the registry. The factory runs
    /// synchronously while the registry is locked and should only construct the
    /// returned future. This keeps large task futures out of callers' async
    /// state while they wait for registration.
    ///
    /// Use [`Self::spawn`] when the future is already constructed. Capturing an
    /// existing future in the factory does not provide this size benefit.
    ///
    /// # Panics
    ///
    /// Panics when polled outside a Tokio runtime or if the factory panics.
    pub async fn spawn_with<F, Fut>(&self, task_factory: F)
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().await;
        let task = task_factory();

        inner.spawn(async move {
            task.await;

            Ok(())
        });
    }

    /// Reaps completed tasks once enough of them may have accumulated to
    /// justify the lock.
    pub async fn try_reap(&self) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        if inner.len() <= TASK_REAP_THRESHOLD {
            return Ok(());
        }

        inner.try_reap()
    }

    /// Waits for tracked tasks and retains exclusive access to the registry.
    ///
    /// The returned guard blocks registration and other registry operations
    /// while the caller changes resources used by tasks. Both the tasks being
    /// drained and the guard holder must avoid awaiting registry access, which
    /// would deadlock.
    ///
    /// A panic or unexpected cancellation returns an error without a guard and
    /// drops remaining tasks to request abort without waiting. No timeout is
    /// imposed. Cancelling this method releases the lock but leaves unfinished
    /// tasks tracked, including any cancellation already requested.
    pub async fn drain(&self) -> EtlResult<TaskRegistryDrainGuard> {
        let mut inner = Arc::clone(&self.inner).lock_owned().await;

        inner.wait().await?;

        Ok(TaskRegistryDrainGuard { _guard: inner })
    }

    /// Aborts and reaps all remaining tasks during shutdown.
    ///
    /// The caller must stop producers first. The registry stays locked until
    /// every task is reaped or a failure is observed, then admits submissions
    /// again. Requested cancellations are silently accepted. A panic returns
    /// immediately and drops the remaining tasks without awaiting them.
    pub async fn shutdown(&self) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        inner.shutdown().await
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Waker},
    };

    use tokio::sync::oneshot;

    use crate::{
        error::ErrorKind,
        task::{TaskRegistry, registry::TASK_REAP_THRESHOLD},
    };

    /// Polls once to verify that the operation cannot complete before release.
    fn assert_pending(future: Pin<&mut impl Future>) {
        let mut context = Context::from_waker(Waker::noop());
        assert!(future.poll(&mut context).is_pending());
    }

    /// Registers a task and waits until its panic has dropped its captures.
    async fn spawn_panicked_task(tasks: &TaskRegistry) {
        let (lifetime_tx, lifetime_rx) = oneshot::channel::<()>();
        tasks
            .spawn(async move {
                let _lifetime = lifetime_tx;
                panic!("Test background task panic");
            })
            .await;
        assert!(lifetime_rx.await.is_err());
    }

    /// Registers a task that stays pending until released or aborted.
    async fn spawn_parked_task(tasks: &TaskRegistry) -> oneshot::Sender<()> {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        tasks
            .spawn(async move {
                started_tx.send(()).unwrap();
                let _ = release_rx.await;
            })
            .await;
        started_rx.await.unwrap();
        release_tx
    }

    /// Operational reaping reports a completed panic once the threshold is
    /// exceeded.
    #[tokio::test]
    async fn try_reap_reports_task_panic_past_the_reap_threshold() {
        let tasks = TaskRegistry::new();
        spawn_panicked_task(&tasks).await;
        let mut release_senders = Vec::new();
        for _ in 0..TASK_REAP_THRESHOLD {
            release_senders.push(spawn_parked_task(&tasks).await);
        }

        let error = tasks.try_reap().await.unwrap_err();

        assert_eq!(error.kind(), ErrorKind::TaskPanic);
        assert!(tasks.inner.lock().await.is_empty());
        for sender in &mut release_senders {
            sender.closed().await;
        }
    }

    /// A below-threshold panic is reported by drain once without poisoning the
    /// registry.
    #[tokio::test]
    async fn drain_reports_panic_deferred_by_try_reap() {
        let tasks = TaskRegistry::new();
        spawn_panicked_task(&tasks).await;

        tasks.try_reap().await.unwrap();
        let error = tasks.drain().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TaskPanic);

        let _guard = tasks.drain().await.unwrap();
    }

    /// Drain waits for admitted work before returning exclusive access.
    #[tokio::test]
    async fn drain_joins_admitted_tasks_before_returning_the_guard() {
        let tasks = TaskRegistry::new();
        let release = spawn_parked_task(&tasks).await;
        let drain = tasks.drain();
        tokio::pin!(drain);

        assert_pending(drain.as_mut());
        release.send(()).unwrap();
        let _guard = drain.await.unwrap();
    }

    /// The drain guard blocks registration and factory execution until dropped.
    #[tokio::test]
    async fn drain_guard_blocks_registration_until_dropped() {
        for use_factory in [false, true] {
            let tasks = TaskRegistry::new();
            let guard = tasks.drain().await.unwrap();
            let (factory_tx, mut factory_rx) = oneshot::channel();
            let register = async {
                if use_factory {
                    tasks
                        .spawn_with(|| {
                            factory_tx.send(()).unwrap();
                            async {}
                        })
                        .await;
                } else {
                    tasks.spawn(async {}).await;
                }
            };
            tokio::pin!(register);

            assert_pending(register.as_mut());
            if use_factory {
                assert_eq!(factory_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
            }

            drop(guard);
            register.await;
            if use_factory {
                factory_rx.await.unwrap();
            }
            let _guard = tasks.drain().await.unwrap();
        }
    }

    /// Owner-requested cancellation joins running tasks without returning an
    /// error.
    #[tokio::test]
    async fn shutdown_aborts_running_tasks_and_swallows_cancellation() {
        let tasks = TaskRegistry::new();
        let release = spawn_parked_task(&tasks).await;

        tasks.shutdown().await.unwrap();

        assert!(release.is_closed());
        assert!(tasks.inner.lock().await.is_empty());
    }

    /// An earlier panic drops remaining tasks even while the registry is
    /// retained.
    #[tokio::test]
    async fn teardown_failure_drops_remaining_tasks() {
        for drain in [false, true] {
            let tasks = TaskRegistry::new();
            spawn_panicked_task(&tasks).await;

            let (lifetime_tx, lifetime_rx) = oneshot::channel::<()>();
            tasks
                .spawn(async move {
                    let _lifetime = lifetime_tx;
                    std::future::pending::<()>().await;
                })
                .await;

            let error = if drain {
                tasks.drain().await.unwrap_err()
            } else {
                tasks.shutdown().await.unwrap_err()
            };
            assert_eq!(error.kind(), ErrorKind::TaskPanic);
            assert!(tasks.inner.lock().await.is_empty());
            assert!(lifetime_rx.await.is_err());
        }
    }
}
