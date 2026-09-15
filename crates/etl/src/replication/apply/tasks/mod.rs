//! Ownership and lifecycle of background work for one apply-loop invocation.

use std::{future::Future, time::Duration};

use tokio::{sync::mpsc, task::JoinHandle};
use tokio_postgres::types::PgLsn;

use crate::{
    error::EtlResult,
    postgres::{FeedbackHandle, OutOfBandSourcePool},
    replication::{WorkerType, apply::tasks::schema_cleanup::SchemaCleanupRequest},
    schema::{SnapshotId, TableId},
    store::SchemaStore,
};

mod feedback;
mod replication_lag;
mod schema_cleanup;

pub(super) use replication_lag::ReplicationLagMetrics;

/// Background tasks owned by an apply loop invocation.
#[derive(Debug)]
pub(super) struct ApplyLoopTasks {
    /// Independent channel handle for submitting safe replication progress.
    feedback_handle: FeedbackHandle,
    /// Sender for serialized background schema cleanup requests.
    schema_cleanup_tx: Option<mpsc::Sender<SchemaCleanupRequest>>,
    /// Background worker that serially processes schema cleanup requests.
    schema_cleanup_worker_task: JoinHandle<()>,
    /// Background replication lag sampler task owned by this apply loop.
    replication_lag_sampler_task: JoinHandle<()>,
    /// Sole feedback writer owned for the entire apply-loop invocation.
    feedback_sender_task: JoinHandle<()>,
}

impl ApplyLoopTasks {
    /// Creates task ownership and starts background workers.
    pub(super) fn start<S, F>(
        schema_store: S,
        out_of_band_source_pool: OutOfBandSourcePool,
        replication_lag_metrics: ReplicationLagMetrics,
        worker_type: WorkerType,
        table_sync_monitor_refresh_interval: Duration,
        feedback_handle: FeedbackHandle,
        feedback_sender_future: F,
    ) -> Self
    where
        S: SchemaStore + Send + 'static,
        F: Future<Output = EtlResult<()>> + Send + 'static,
    {
        let (schema_cleanup_tx, schema_cleanup_worker_task) =
            schema_cleanup::spawn(schema_store, worker_type);

        let replication_lag_sampler_task = replication_lag::spawn(
            out_of_band_source_pool,
            replication_lag_metrics,
            worker_type,
            table_sync_monitor_refresh_interval,
        );

        let feedback_sender_task = feedback::spawn(feedback_sender_future);

        Self {
            feedback_handle,
            schema_cleanup_tx: Some(schema_cleanup_tx),
            schema_cleanup_worker_task,
            replication_lag_sampler_task,
            feedback_sender_task,
        }
    }

    /// Queues safe feedback, failing with a retryable error if its sender
    /// stopped.
    pub(super) async fn enqueue_status_update(
        &self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        force: bool,
    ) -> EtlResult<()> {
        self.feedback_handle.enqueue_status_update(write_lsn, flush_lsn, force).await
    }

    /// Tries to queue a frozen cleanup boundary without blocking the apply
    /// loop.
    pub(super) fn try_queue_schema_cleanup(
        &self,
        table_id: TableId,
        retention_snapshot_id: SnapshotId,
    ) -> bool {
        schema_cleanup::try_queue(self.schema_cleanup_tx.as_ref(), table_id, retention_snapshot_id)
    }

    /// Stops and joins all owned background tasks, logging any failures.
    ///
    /// Feedback and sampling can stop immediately. Closing the cleanup queue
    /// lets the worker finish accepted requests before it exits. Task failures
    /// never replace the apply loop's result, including during error recovery.
    pub(super) async fn teardown(&mut self, worker_type: WorkerType) {
        // No final feedback is needed: persisted checkpoints govern replay.
        self.feedback_sender_task.abort();
        self.replication_lag_sampler_task.abort();
        self.schema_cleanup_tx.take();

        feedback::join(&mut self.feedback_sender_task).await;
        replication_lag::join(&mut self.replication_lag_sampler_task).await;
        schema_cleanup::join(&mut self.schema_cleanup_worker_task, worker_type).await;
    }
}

impl Drop for ApplyLoopTasks {
    fn drop(&mut self) {
        // Cancellation or panic can skip or interrupt async teardown. Abort
        // every task; interrupted cleanup only prunes obsolete schemas and
        // can be retried.
        self.feedback_sender_task.abort();
        self.replication_lag_sampler_task.abort();
        self.schema_cleanup_worker_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures::FutureExt;
    use tokio::{
        sync::{mpsc, oneshot},
        task::JoinHandle,
    };

    use crate::{
        error::{ErrorKind, EtlResult},
        etl_error,
        postgres::FeedbackHandle,
        replication::{
            WorkerType,
            apply::tasks::{ApplyLoopTasks, feedback},
        },
        schema::{SnapshotId, TableId},
    };

    /// Supplies an unused feedback handle for tests of task cancellation.
    fn closed_feedback_handle() -> FeedbackHandle {
        let sink = Box::pin(futures::sink::unfold((), |(), _: Bytes| {
            std::future::pending::<EtlResult<()>>()
        }));
        let (handle, _) = FeedbackHandle::create(sink, Duration::from_secs(10));
        handle
    }

    /// Creates background tasks for testing apply-loop ownership and teardown.
    fn test_apply_loop_tasks(
        feedback_handle: FeedbackHandle,
        feedback_sender_task: JoinHandle<()>,
    ) -> ApplyLoopTasks {
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel(1);
        ApplyLoopTasks {
            feedback_handle,
            schema_cleanup_tx: Some(cleanup_tx),
            schema_cleanup_worker_task: tokio::spawn(async move {
                while cleanup_rx.recv().await.is_some() {}
            }),
            replication_lag_sampler_task: tokio::spawn(std::future::pending()),
            feedback_sender_task,
        }
    }

    /// Spawns a task whose cancellation is observable without timing
    /// assumptions.
    fn pending_background_task() -> (JoinHandle<()>, oneshot::Receiver<()>) {
        let (lifetime_tx, lifetime_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending::<()>().await;
        });
        (task, lifetime_rx)
    }

    /// Cancellation must not detach any background task from its apply loop.
    #[tokio::test]
    async fn dropping_apply_loop_tasks_aborts_all_background_tasks() {
        let (schema_cleanup_worker_task, cleanup_lifetime_rx) = pending_background_task();
        let (replication_lag_sampler_task, sampler_lifetime_rx) = pending_background_task();
        let (lifetime_tx, lifetime_rx) = oneshot::channel::<()>();
        let feedback_sender_task = feedback::spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending().await
        });
        let tasks = ApplyLoopTasks {
            feedback_handle: closed_feedback_handle(),
            schema_cleanup_tx: None,
            schema_cleanup_worker_task,
            replication_lag_sampler_task,
            feedback_sender_task,
        };
        drop(tasks);
        assert!(lifetime_rx.await.is_err());
        assert!(cleanup_lifetime_rx.await.is_err());
        assert!(sampler_lifetime_rx.await.is_err());
    }

    /// Teardown joins cancelled feedback and sampler tasks and drains cleanup.
    #[tokio::test]
    async fn apply_loop_teardown_joins_background_tasks() {
        let (feedback_sender_task, feedback_lifetime_rx) = pending_background_task();
        let (replication_lag_sampler_task, sampler_lifetime_rx) = pending_background_task();
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel(1);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (completed_tx, completed_rx) = oneshot::channel();
        let schema_cleanup_worker_task = tokio::spawn(async move {
            // Accepted cleanup must finish even when teardown closes the queue.
            cleanup_rx.recv().await.unwrap();
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            assert!(cleanup_rx.recv().await.is_none());
            completed_tx.send(()).unwrap();
        });
        let mut tasks = ApplyLoopTasks {
            feedback_handle: closed_feedback_handle(),
            schema_cleanup_tx: Some(cleanup_tx),
            schema_cleanup_worker_task,
            replication_lag_sampler_task,
            feedback_sender_task,
        };
        assert!(
            tasks
                .try_queue_schema_cleanup(TableId::new(1), SnapshotId::new(100.into(), 90.into()),)
        );
        entered_rx.await.unwrap();

        let mut teardown = Box::pin(tasks.teardown(WorkerType::Apply));
        assert!(teardown.as_mut().now_or_never().is_none());
        assert!(feedback_lifetime_rx.await.is_err());
        assert!(sampler_lifetime_rx.await.is_err());
        assert!(teardown.as_mut().now_or_never().is_none());
        release_tx.send(()).unwrap();
        teardown.await;
        completed_rx.await.unwrap();

        assert!(tasks.feedback_sender_task.is_finished());
        assert!(tasks.replication_lag_sampler_task.is_finished());
        assert!(tasks.schema_cleanup_worker_task.is_finished());
    }

    /// Cancellation during graceful teardown must still abort unfinished
    /// cleanup.
    #[tokio::test]
    async fn dropping_apply_loop_tasks_during_teardown_aborts_cleanup() {
        let (feedback_sender_task, feedback_lifetime_rx) = pending_background_task();
        let (replication_lag_sampler_task, sampler_lifetime_rx) = pending_background_task();
        let (schema_cleanup_worker_task, cleanup_lifetime_rx) = pending_background_task();
        let mut tasks = ApplyLoopTasks {
            feedback_handle: closed_feedback_handle(),
            schema_cleanup_tx: None,
            schema_cleanup_worker_task,
            replication_lag_sampler_task,
            feedback_sender_task,
        };

        let mut teardown = Box::pin(tasks.teardown(WorkerType::Apply));
        assert!(teardown.as_mut().now_or_never().is_none());
        assert!(feedback_lifetime_rx.await.is_err());
        assert!(sampler_lifetime_rx.await.is_err());
        assert!(teardown.as_mut().now_or_never().is_none());
        drop(teardown);
        drop(tasks);
        assert!(cleanup_lifetime_rx.await.is_err());
    }

    /// Feedback failure makes subsequent submissions retryable without failing
    /// teardown or preventing it from joining the other tasks.
    #[tokio::test]
    async fn apply_loop_teardown_tolerates_feedback_failures() {
        let (closed_tx, closed_rx) = mpsc::channel::<()>(1);
        let sink = Box::pin(futures::sink::unfold(closed_rx, |receiver, _: Bytes| async move {
            let _receiver = receiver;
            Err(etl_error!(ErrorKind::SourceConnectionFailed, "Test feedback failure"))
        }));
        let (handle, sender) = FeedbackHandle::create(sink, Duration::from_secs(10));
        let mut tasks = test_apply_loop_tasks(handle, feedback::spawn(sender));
        tasks.enqueue_status_update(100.into(), 80.into(), true).await.unwrap();
        closed_tx.closed().await;
        assert_eq!(
            tasks.enqueue_status_update(100.into(), 80.into(), true).await.unwrap_err().kind(),
            ErrorKind::ReplicationFeedbackUnavailable,
        );
        tasks.teardown(WorkerType::Apply).await;
        assert!(tasks.feedback_sender_task.is_finished());
        assert!(tasks.replication_lag_sampler_task.is_finished());
        assert!(tasks.schema_cleanup_worker_task.is_finished());
    }

    /// A feedback task panic does not make teardown fail or skip other tasks.
    #[tokio::test]
    async fn apply_loop_teardown_tolerates_feedback_panic() {
        let (closed_tx, closed_rx) = mpsc::channel::<()>(1);
        let task = feedback::spawn(async move {
            let _receiver = closed_rx;
            panic!("Test feedback task panic");
        });
        let mut tasks = test_apply_loop_tasks(closed_feedback_handle(), task);
        closed_tx.closed().await;
        tasks.teardown(WorkerType::Apply).await;
        assert!(tasks.feedback_sender_task.is_finished());
        assert!(tasks.replication_lag_sampler_task.is_finished());
        assert!(tasks.schema_cleanup_worker_task.is_finished());
    }
}
