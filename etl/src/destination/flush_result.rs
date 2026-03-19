use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use pin_project_lite::pin_project;
use tokio::sync::oneshot;
use tokio_postgres::types::PgLsn;

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;

/// Metrics captured at batch dispatch time and carried through to result processing.
#[derive(Debug, Clone, Copy)]
pub struct BatchFlushMetrics {
    /// Number of events in the dispatched batch.
    pub events_count: usize,
    /// Instant at which the batch was handed off to the destination.
    pub dispatched_at: Instant,
}

/// Sender half of an asynchronous batch flush result.
///
/// This handle is passed to [`crate::destination::Destination::write_events`] so
/// destinations can report the final outcome of a flushed event batch without
/// coupling apply-loop progress to the method's return value.
///
/// If the apply loop has already dropped the receiver, sending the result fails and the caller
/// gets the original payload back. Destinations may treat that as an implicit cancellation signal.
#[derive(Debug)]
pub struct BatchFlushResult<T> {
    tx: Option<oneshot::Sender<EtlResult<T>>>,
}

impl<T> BatchFlushResult<T> {
    /// Creates a new batch flush result channel and its pending counterpart.
    pub fn new(
        commit_end_lsn: Option<PgLsn>,
        metrics: BatchFlushMetrics,
    ) -> (Self, PendingBatchFlushResult<T>) {
        let (tx, rx) = oneshot::channel();

        (
            Self { tx: Some(tx) },
            PendingBatchFlushResult {
                commit_end_lsn,
                metrics,
                rx,
            },
        )
    }

    /// Sends a completed result to the apply loop.
    ///
    /// If the receiver has already been dropped, returns the original result to the caller.
    pub fn send(mut self, result: EtlResult<T>) -> Result<(), EtlResult<T>> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };

        tx.send(result)
    }
}

impl<T> Drop for BatchFlushResult<T> {
    fn drop(&mut self) {
        let Some(tx) = self.tx.take() else {
            return;
        };

        let _ = tx.send(Err(etl_error!(
            ErrorKind::DestinationError,
            "Batch flush result dropped without sending"
        )));
    }
}

pin_project! {
    /// Receiver half of an asynchronous batch flush result.
    ///
    /// Stored by the apply loop while the destination processes the batch. Resolves
    /// once the destination calls [`BatchFlushResult::send`].
    #[must_use = "pending batch flush results do nothing unless polled"]
    #[derive(Debug)]
    pub struct PendingBatchFlushResult<T> {
        commit_end_lsn: Option<PgLsn>,
        metrics: BatchFlushMetrics,
        #[pin]
        rx: oneshot::Receiver<EtlResult<T>>,
    }
}

impl<T> PendingBatchFlushResult<T> {
    /// Returns the commit end LSN associated with this result, if any.
    pub fn commit_end_lsn(&self) -> Option<PgLsn> {
        self.commit_end_lsn
    }

    /// Returns the metrics captured when the batch was dispatched.
    pub fn metrics(&self) -> BatchFlushMetrics {
        self.metrics
    }
}

impl<T> Future for PendingBatchFlushResult<T> {
    type Output = CompletedBatchFlushResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.rx.poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(CompletedBatchFlushResult {
                commit_end_lsn: *this.commit_end_lsn,
                metrics: *this.metrics,
                result,
            }),
            Poll::Ready(Err(_)) => Poll::Ready(CompletedBatchFlushResult {
                commit_end_lsn: *this.commit_end_lsn,
                metrics: *this.metrics,
                result: Err(etl_error!(
                    ErrorKind::DestinationError,
                    "Batch flush result channel closed before sending"
                )),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Completed batch flush result returned by [`PendingBatchFlushResult`].
#[derive(Debug)]
pub struct CompletedBatchFlushResult<T> {
    commit_end_lsn: Option<PgLsn>,
    metrics: BatchFlushMetrics,
    result: EtlResult<T>,
}

impl<T> CompletedBatchFlushResult<T> {
    /// Returns the commit end LSN associated with this result, if any.
    pub fn commit_end_lsn(&self) -> Option<PgLsn> {
        self.commit_end_lsn
    }

    /// Returns the metrics captured when the batch was dispatched.
    pub fn metrics(&self) -> BatchFlushMetrics {
        self.metrics
    }

    /// Returns the final result.
    pub fn into_result(self) -> EtlResult<T> {
        self.result
    }

    /// Returns the LSN, metrics, and final result.
    pub fn into_parts(self) -> (Option<PgLsn>, BatchFlushMetrics, EtlResult<T>) {
        (self.commit_end_lsn, self.metrics, self.result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn batch_flush_result_round_trips_success() {
        let metrics = BatchFlushMetrics {
            events_count: 1,
            dispatched_at: Instant::now(),
        };
        let (flush_result, pending_result) = BatchFlushResult::new(Some(PgLsn::from(42)), metrics);

        flush_result.send(Ok(7_u64)).unwrap();

        let completed = pending_result.await;
        let (commit_end_lsn, _metrics, result) = completed.into_parts();

        assert_eq!(commit_end_lsn, Some(PgLsn::from(42)));
        assert_eq!(result.unwrap(), 7);
    }

    #[tokio::test]
    async fn dropping_batch_flush_result_surfaces_error() {
        let metrics = BatchFlushMetrics {
            events_count: 0,
            dispatched_at: Instant::now(),
        };
        let (flush_result, pending_result) = BatchFlushResult::<()>::new(None, metrics);
        drop(flush_result);

        let err = pending_result.await.into_result().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationError);
        assert_eq!(
            err.description(),
            Some("batch flush result dropped without sending")
        );
    }
}
