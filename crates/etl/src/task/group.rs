//! Fallible child tasks scoped to one operation.

use std::future::Future;

use tokio::task::{AbortHandle, JoinError, JoinSet};

use crate::error::{EtlError, EtlResult};

/// Owned group of fallible tasks belonging to one operation.
///
/// Task failures retain their original kinds. Unexpected cancellation and panic
/// use [`crate::error::ErrorKind::TaskCancelled`] and
/// [`crate::error::ErrorKind::TaskPanic`]. Dropping the group aborts remaining
/// tasks; await [`Self::shutdown`] to also join them and collect failures.
#[derive(Debug)]
pub struct TaskGroup<T: 'static> {
    /// Tasks whose results have not yet been consumed.
    tasks: JoinSet<EtlResult<T>>,
    /// Failures retained across cancellation of a join or shutdown future.
    errors: Vec<EtlError>,
}

impl<T: 'static> TaskGroup<T> {
    /// Creates an empty group.
    pub fn new() -> Self {
        Self { tasks: JoinSet::new(), errors: Vec::new() }
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

    /// Joins the next child, stopping and joining all siblings on failure.
    ///
    /// Cancelling this future keeps unreported failures in the group. Resume
    /// joining or call [`Self::shutdown`] to complete cleanup and observe them.
    pub async fn join_next(&mut self) -> Option<EtlResult<T>> {
        if !self.errors.is_empty() {
            return self.shutdown().await.err().map(Err);
        }

        let result = self.tasks.join_next().await?;

        self.handle_result(result).await
    }

    /// Aborts and joins every remaining child, preserving completed failures.
    ///
    /// Cancellation requested here is expected cleanup. Panics and errors
    /// returned by children are collected even after an earlier failure.
    /// Cancelling this future retains the remaining tasks and collected errors.
    pub async fn shutdown(&mut self) -> EtlResult<()> {
        self.tasks.abort_all();

        while let Some(result) = self.tasks.join_next().await {
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => self.errors.push(error),
                Err(error) if error.is_cancelled() => {}
                Err(error) => self.errors.push(error.into()),
            }
        }

        if self.errors.is_empty() { Ok(()) } else { Err(std::mem::take(&mut self.errors).into()) }
    }

    /// Returns whether every child has been joined and every error reported.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty() && self.errors.is_empty()
    }

    /// Returns the number of tasks awaiting collection.
    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Reaps ready children, aborting and joining siblings on failure.
    pub(super) async fn try_reap(&mut self) -> EtlResult<()> {
        if !self.errors.is_empty() {
            return self.shutdown().await;
        }

        while let Some(result) = self.tasks.try_join_next() {
            if let Some(result) = self.handle_result(result).await {
                result?;
            }
        }
        
        Ok(())
    }

    /// Classifies one completion and retains failures while siblings stop.
    async fn handle_result(
        &mut self,
        result: Result<EtlResult<T>, JoinError>,
    ) -> Option<EtlResult<T>> {
        match result.map_err(EtlError::from).and_then(std::convert::identity) {
            Ok(value) => Some(Ok(value)),
            Err(error) => {
                self.errors.push(error);
                self.shutdown().await.err().map(Err)
            }
        }
    }
}

impl TaskGroup<()> {
    /// Waits for all children, aborting and joining siblings on the first
    /// error.
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
    use futures::FutureExt;
    use tokio::sync::oneshot;
    use tokio_util::task::AbortOnDropHandle;

    use crate::{
        error::ErrorKind,
        etl_error,
        task::{TaskGroup, abort_and_join},
    };

    /// Failure stops siblings while retaining domain errors, panics, and
    /// sources.
    #[tokio::test]
    async fn failure_joins_siblings_and_preserves_errors() {
        let mut tasks = TaskGroup::<()>::new();
        let failed = tasks.spawn(async {
            Err(etl_error!(ErrorKind::DestinationError, "Write failed", source: std::io::Error::other("Test write failure")))
        });
        let panicked = tasks.spawn(async { panic!("Test child panic") });
        while !failed.is_finished() || !panicked.is_finished() {
            tokio::task::yield_now().await;
        }
        let (lifetime_tx, mut lifetime_rx) = oneshot::channel::<()>();
        tasks.spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending().await
        });
        let error = tasks.wait().await.unwrap_err();
        let errors = error.errors().unwrap();
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().any(|error| error.kind() == ErrorKind::DestinationError));
        assert!(errors.iter().any(|error| error.kind() == ErrorKind::TaskPanic));
        assert!(errors.iter().all(|error| std::error::Error::source(error).is_some()));
        assert!(tasks.is_empty());
        assert_eq!(lifetime_rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
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

    /// Cancelling a join during sibling cleanup must not lose its original
    /// error.
    #[tokio::test]
    async fn interrupted_cleanup_retains_original_failure() {
        let mut tasks = TaskGroup::<()>::new();
        let failed = tasks
            .spawn(async { Err(etl_error!(ErrorKind::DestinationError, "Original write failed")) });
        while !failed.is_finished() {
            tokio::task::yield_now().await;
        }
        let (lifetime_tx, mut lifetime_rx) = oneshot::channel::<()>();
        tasks.spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending().await
        });
        assert!(tasks.wait().now_or_never().is_none());
        let error = tasks.shutdown().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DestinationError);
        assert_eq!(error.description(), Some("Original write failed"));
        assert!(tasks.is_empty());
        assert_eq!(lifetime_rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
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

        let task = tokio::spawn(async {
            Err::<(), _>(etl_error!(ErrorKind::DestinationError, "Completed write failed"))
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let result = abort_and_join(AbortOnDropHandle::new(task)).await.unwrap().unwrap();
        assert_eq!(result.unwrap_err().kind(), ErrorKind::DestinationError);

        let task = tokio::spawn(async { panic!("Test sampler panic") });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let error = abort_and_join(AbortOnDropHandle::new(task)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::TaskPanic);
        assert!(std::error::Error::source(&error).is_some());
    }
}
