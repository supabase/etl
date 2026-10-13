//! Ownership and lifecycle of background work for one apply-loop invocation.

use std::{future::Future, time::Duration};

use tokio::sync::mpsc;
use tokio_postgres::types::PgLsn;
use tokio_util::task::AbortOnDropHandle;

use crate::{
    error::{EtlError, EtlResult},
    postgres::{FeedbackHandle, OutOfBandSourcePool},
    replication::{WorkerType, apply::tasks::schema_cleanup::SchemaCleanupRequest},
    schema::{SnapshotId, TableId},
    store::SchemaStore,
    task::{abort_and_join, abort_and_join_result},
};

mod replication_lag;
mod schema_cleanup;

pub(super) use replication_lag::ReplicationLagMetrics;

/// Background tasks owned by an apply loop invocation.
#[derive(Debug)]
pub(super) struct ApplyLoopTasks {
    /// Independent channel handle for submitting safe replication progress.
    feedback_handle: FeedbackHandle,
    /// Sender for serialized background schema cleanup requests.
    schema_cleanup_tx: mpsc::Sender<SchemaCleanupRequest>,
    /// Background worker that serially processes schema cleanup requests.
    schema_cleanup_worker_task: AbortOnDropHandle<()>,
    /// Background replication lag sampler task owned by this apply loop.
    replication_lag_metrics_task: AbortOnDropHandle<()>,
    /// Sole feedback writer owned for the entire apply-loop invocation.
    feedback_sender_task: AbortOnDropHandle<EtlResult<()>>,
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
            schema_cleanup::spawn_schema_cleanup_task(schema_store, worker_type);

        let replication_lag_metrics_task = replication_lag::spawn_replication_lag_metrics_task(
            out_of_band_source_pool,
            replication_lag_metrics,
            worker_type,
            table_sync_monitor_refresh_interval,
        );

        let feedback_sender_task = AbortOnDropHandle::new(tokio::spawn(feedback_sender_future));

        Self {
            feedback_handle,
            schema_cleanup_tx,
            schema_cleanup_worker_task,
            replication_lag_metrics_task,
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
        schema_cleanup::try_queue(&self.schema_cleanup_tx, table_id, retention_snapshot_id)
    }

    /// Stops and joins owned background tasks until the first failure.
    ///
    /// Feedback and sampling can stop immediately. Closing the cleanup queue
    /// lets the worker finish accepted requests before it exits. A failure
    /// returns immediately; remaining handles request cancellation on drop.
    pub(super) async fn teardown(self) -> EtlResult<()> {
        // No final feedback is needed: persisted checkpoints govern replay.
        self.feedback_sender_task.abort();
        self.replication_lag_metrics_task.abort();
        drop(self.schema_cleanup_tx);

        abort_and_join_result(self.feedback_sender_task).await?;
        abort_and_join(self.replication_lag_metrics_task).await?;
        self.schema_cleanup_worker_task.await.map_err(EtlError::from)?;

        Ok(())
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
    use tokio_util::task::AbortOnDropHandle;

    use crate::{
        error::{ErrorKind, EtlResult},
        etl_error,
        postgres::FeedbackHandle,
        replication::apply::tasks::ApplyLoopTasks,
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
        feedback_sender_task: JoinHandle<EtlResult<()>>,
    ) -> ApplyLoopTasks {
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel(1);
        ApplyLoopTasks {
            feedback_handle,
            schema_cleanup_tx: cleanup_tx,
            schema_cleanup_worker_task: AbortOnDropHandle::new(tokio::spawn(async move {
                while cleanup_rx.recv().await.is_some() {}
            })),
            replication_lag_metrics_task: AbortOnDropHandle::new(tokio::spawn(
                std::future::pending(),
            )),
            feedback_sender_task: AbortOnDropHandle::new(feedback_sender_task),
        }
    }

    /// Spawns a task whose cancellation is observable without timing
    /// assumptions.
    fn pending_background_task<T: Send + 'static>() -> (JoinHandle<T>, oneshot::Receiver<()>) {
        let (lifetime_tx, lifetime_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _lifetime = lifetime_tx;
            std::future::pending::<T>().await
        });
        (task, lifetime_rx)
    }

    /// Teardown joins cancelled feedback and sampler tasks and drains cleanup.
    #[tokio::test]
    async fn apply_loop_teardown_joins_background_tasks() {
        let (feedback_sender_task, feedback_lifetime_rx) = pending_background_task();
        let (replication_lag_metrics_task, sampler_lifetime_rx) = pending_background_task();
        let (cleanup_tx, mut cleanup_rx) = mpsc::channel(1);
        let (release_tx, release_rx) = oneshot::channel();
        let schema_cleanup_worker_task = tokio::spawn(async move {
            // Accepted cleanup must finish even when teardown closes the queue.
            cleanup_rx.recv().await.unwrap();
            release_rx.await.unwrap();
            assert!(cleanup_rx.recv().await.is_none());
        });
        let tasks = ApplyLoopTasks {
            feedback_handle: closed_feedback_handle(),
            schema_cleanup_tx: cleanup_tx,
            schema_cleanup_worker_task: AbortOnDropHandle::new(schema_cleanup_worker_task),
            replication_lag_metrics_task: AbortOnDropHandle::new(replication_lag_metrics_task),
            feedback_sender_task: AbortOnDropHandle::new(feedback_sender_task),
        };
        assert!(
            tasks
                .try_queue_schema_cleanup(TableId::new(1), SnapshotId::new(100.into(), 90.into()),)
        );
        let task_handles = [
            tasks.feedback_sender_task.abort_handle(),
            tasks.replication_lag_metrics_task.abort_handle(),
            tasks.schema_cleanup_worker_task.abort_handle(),
        ];
        let mut teardown = Box::pin(tasks.teardown());
        assert!(teardown.as_mut().now_or_never().is_none());
        assert!(feedback_lifetime_rx.await.is_err());
        assert!(sampler_lifetime_rx.await.is_err());
        assert!(teardown.as_mut().now_or_never().is_none());
        release_tx.send(()).unwrap();
        teardown.await.unwrap();
        assert!(task_handles.iter().all(tokio::task::AbortHandle::is_finished));
    }

    /// Cancellation during graceful teardown must still abort unfinished
    /// cleanup.
    #[tokio::test]
    async fn dropping_apply_loop_tasks_during_teardown_aborts_cleanup() {
        let (feedback_sender_task, feedback_lifetime_rx) = pending_background_task();
        let (replication_lag_metrics_task, sampler_lifetime_rx) = pending_background_task();
        let (schema_cleanup_worker_task, cleanup_lifetime_rx) = pending_background_task();
        let tasks = ApplyLoopTasks {
            feedback_handle: closed_feedback_handle(),
            schema_cleanup_tx: mpsc::channel(1).0,
            schema_cleanup_worker_task: AbortOnDropHandle::new(schema_cleanup_worker_task),
            replication_lag_metrics_task: AbortOnDropHandle::new(replication_lag_metrics_task),
            feedback_sender_task: AbortOnDropHandle::new(feedback_sender_task),
        };

        let mut teardown = Box::pin(tasks.teardown());
        assert!(teardown.as_mut().now_or_never().is_none());
        assert!(feedback_lifetime_rx.await.is_err());
        assert!(sampler_lifetime_rx.await.is_err());
        assert!(teardown.as_mut().now_or_never().is_none());
        drop(teardown);
        assert!(cleanup_lifetime_rx.await.is_err());
    }

    /// Feedback failure propagates while remaining background tasks are
    /// dropped.
    #[tokio::test]
    async fn apply_loop_teardown_propagates_feedback_failures() {
        let (closed_tx, closed_rx) = mpsc::channel::<()>(1);
        let sink = Box::pin(futures::sink::unfold(closed_rx, |receiver, _: Bytes| async move {
            let _receiver = receiver;
            Err(etl_error!(ErrorKind::SourceConnectionFailed, "Test feedback failure"))
        }));
        let (handle, sender) = FeedbackHandle::create(sink, Duration::from_secs(10));
        let tasks = test_apply_loop_tasks(handle, tokio::spawn(sender));
        tasks.enqueue_status_update(100.into(), 80.into(), true).await.unwrap();
        closed_tx.closed().await;
        assert_eq!(
            tasks.enqueue_status_update(100.into(), 80.into(), true).await.unwrap_err().kind(),
            ErrorKind::ReplicationFeedbackUnavailable,
        );
        let task_handles = [
            tasks.feedback_sender_task.abort_handle(),
            tasks.replication_lag_metrics_task.abort_handle(),
            tasks.schema_cleanup_worker_task.abort_handle(),
        ];
        assert!(tasks.teardown().await.is_err());
        tokio::time::timeout(Duration::from_secs(5), async {
            while !task_handles.iter().all(tokio::task::AbortHandle::is_finished) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    /// A feedback task panic drops the remaining background tasks.
    #[tokio::test]
    async fn apply_loop_teardown_propagates_feedback_panic() {
        let (closed_tx, closed_rx) = mpsc::channel::<()>(1);
        let task = tokio::spawn(async move {
            let _receiver = closed_rx;
            panic!("Test feedback task panic");
        });
        let tasks = test_apply_loop_tasks(closed_feedback_handle(), task);
        closed_tx.closed().await;
        let task_handles = [
            tasks.feedback_sender_task.abort_handle(),
            tasks.replication_lag_metrics_task.abort_handle(),
            tasks.schema_cleanup_worker_task.abort_handle(),
        ];
        assert!(tasks.teardown().await.is_err());
        tokio::time::timeout(Duration::from_secs(5), async {
            while !task_handles.iter().all(tokio::task::AbortHandle::is_finished) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
