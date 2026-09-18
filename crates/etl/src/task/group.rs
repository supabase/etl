//! Fallible child tasks scoped to one operation.

use std::future::Future;

use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::error::{EtlError, EtlResult};

/// Owned group of fallible tasks belonging to one operation.
///
/// Task failures retain their original kinds. Unexpected cancellation and panic
/// use [`crate::error::ErrorKind::TaskCancelled`] and
/// [`crate::error::ErrorKind::TaskPanic`]. Failure drops the remaining tasks,
/// requesting cancellation without waiting. On success, [`Self::wait`] joins
/// every child; [`Self::shutdown`] also requests cancellation before joining.
#[derive(Debug)]
pub struct TaskGroup<T: 'static> {
    /// Tasks whose results have not yet been consumed.
    tasks: JoinSet<EtlResult<T>>,
}

impl<T: 'static> TaskGroup<T> {
    /// Creates a new task group.
    pub fn new() -> Self {
        Self { tasks: JoinSet::new() }
    }

    /// Spawns one child and returns a handle for explicitly cancelling it.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    pub fn spawn<F>(&mut self, future: F) -> AbortHandle
    where
        F: Future<Output = EtlResult<T>> + Send + 'static,
        T: Send,
    {
        self.tasks.spawn(future)
    }

    /// Joins the next child, returning its failure immediately and dropping
    /// remaining tasks to request cancellation without waiting.
    pub async fn join_next(&mut self) -> Option<EtlResult<T>> {
        let result = self.tasks.join_next().await?;

        Some(self.handle_result(result))
    }

    /// Aborts and joins remaining children until the first failure.
    ///
    /// Owner-requested cancellation is silently accepted as successful cleanup.
    /// A completed error or panic returns immediately and drops the remaining
    /// tasks. Cancelling this future leaves unfinished tasks tracked with
    /// cancellation requested.
    pub async fn shutdown(&mut self) -> EtlResult<()> {
        self.tasks.abort_all();

        while let Some(result) = self.tasks.join_next().await {
            if matches!(&result, Err(error) if error.is_cancelled()) {
                continue;
            }

            self.handle_result(result)?;
        }

        Ok(())
    }

    /// Returns whether no tasks remain tracked.
    ///
    /// After a failure, cancellation of dropped tasks may still be in progress.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns the number of tasks awaiting collection.
    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Reaps ready children, returning the first failure without waiting for
    /// cancellation of remaining tasks.
    pub(super) fn try_reap(&mut self) -> EtlResult<()> {
        while let Some(result) = self.tasks.try_join_next() {
            self.handle_result(result)?;
        }

        Ok(())
    }

    /// Classifies one completion and drops remaining tasks on failure.
    fn handle_result(&mut self, result: Result<EtlResult<T>, JoinError>) -> EtlResult<T> {
        let result = result.map_err(EtlError::from).and_then(std::convert::identity);
        if result.is_err() {
            // The group may remain owned by a registry after this call returns.
            // Drop its tasks now rather than waiting for the owner to
            // disappear.
            self.tasks = JoinSet::new();
        }

        result
    }
}

impl TaskGroup<()> {
    /// Waits for all children, returning the first failure immediately.
    pub async fn wait(&mut self) -> EtlResult<()> {
        while let Some(result) = self.join_next().await {
            result?;
        }

        Ok(())
    }
}

impl<T: 'static> Default for TaskGroup<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use tokio_util::task::AbortOnDropHandle;

    use crate::{
        error::ErrorKind,
        etl_error,
        task::{TaskGroup, abort_and_join, abort_and_join_result},
    };

    /// Failure must return even while a sibling cannot yet observe abort.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failure_returns_without_waiting_for_sibling_cancellation() {
        for shutdown in [false, true] {
            let mut tasks = TaskGroup::<()>::new();
            let failed = tasks.spawn(async {
                Err(etl_error!(ErrorKind::DestinationError, "Write failed", source: std::io::Error::other("Test write failure")))
            });
            while !failed.is_finished() {
                tokio::task::yield_now().await;
            }
            let (started_tx, started_rx) = oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let (lifetime_tx, lifetime_rx) = oneshot::channel::<()>();
            tasks.spawn(async move {
                let _lifetime = lifetime_tx;
                started_tx.send(()).unwrap();
                // Model a child inside synchronous work. Dropping release_tx
                // also releases it if this test fails before normal release.
                let _ = release_rx.recv();
                Ok(())
            });
            started_rx.await.unwrap();

            let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                if shutdown { tasks.shutdown().await } else { tasks.wait().await }
            })
            .await
            .unwrap();
            let error = result.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DestinationError);
            assert!(std::error::Error::source(&error).is_some());
            assert!(tasks.is_empty());
            release_tx.send(()).unwrap();
            assert!(lifetime_rx.await.is_err());
        }
    }

    /// Unexpected cancellation fails the operation; owner shutdown is
    /// successful cleanup.
    #[tokio::test]
    async fn distinguishes_unexpected_cancellation_from_shutdown() {
        let mut tasks = TaskGroup::<()>::new();
        let child = tasks.spawn(std::future::pending());
        child.abort();
        let error = tasks.wait().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TaskCancelled);
        assert!(std::error::Error::source(&error).is_some());
        assert!(tasks.is_empty());

        tasks.spawn(std::future::pending());
        tasks.shutdown().await.unwrap();
        assert!(tasks.is_empty());
    }

    /// Owner-requested cancellation succeeds after releasing resources, while
    /// completed failures remain visible.
    #[tokio::test]
    async fn abort_joins_cancelled_tasks_and_preserves_failures() {
        let (lifetime_tx, mut lifetime_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending::<()>().await;
        });
        assert_eq!(abort_and_join(AbortOnDropHandle::new(task)).await.unwrap(), None);
        assert_eq!(lifetime_rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));

        let task = tokio::spawn(std::future::pending());
        abort_and_join_result(AbortOnDropHandle::new(task)).await.unwrap();

        for result in
            [Ok(()), Err(etl_error!(ErrorKind::DestinationError, "Completed write failed"))]
        {
            let task_result = result.clone();
            let task = tokio::spawn(async move { task_result });
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
            let joined = abort_and_join_result(AbortOnDropHandle::new(task)).await;
            assert_eq!(joined.map_err(|error| error.kind()), result.map_err(|error| error.kind()));
        }

        let task = tokio::spawn(async { panic!("Test sampler panic") });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let error = abort_and_join_result(AbortOnDropHandle::new(task)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TaskPanic);
        assert!(std::error::Error::source(&error).is_some());
    }
}
