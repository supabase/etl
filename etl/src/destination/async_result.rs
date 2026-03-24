use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use pin_project_lite::pin_project;
use tokio::sync::oneshot;
use tokio_postgres::types::PgLsn;
use tracing::warn;

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;

/// Async completion handle used for [`crate::destination::Destination::write_table_rows`].
///
/// ETL waits for this result before requesting the next row batch for the same copy partition.
/// The handle exists mostly to keep the destination API aligned with
/// [`WriteEventsResult`], so destinations may still choose to structure their internal write
/// paths in a similar way.
pub type WriteTableRowsResult<T = ()> = AsyncResult<T>;
/// Pending async completion used for `Destination::write_table_rows`.
pub type PendingWriteTableRowsResult<T = ()> = PendingAsyncResult<T, ()>;
/// Completed async completion used for `Destination::write_table_rows`.
pub type CompletedWriteTableRowsResult<T = ()> = CompletedAsyncResult<T, ()>;

/// Async completion handle used for [`crate::destination::Destination::truncate_table`].
///
/// ETL waits for this result immediately. It is primarily an API consistency hook rather than a
/// mechanism for overlapping more ETL work with truncation.
pub type TruncateTableResult<T = ()> = AsyncResult<T>;
/// Pending async completion used for `Destination::truncate_table`.
pub type PendingTruncateTableResult<T = ()> = PendingAsyncResult<T, ()>;
/// Completed async completion used for `Destination::truncate_table`.
pub type CompletedTruncateTableResult<T = ()> = CompletedAsyncResult<T, ()>;

/// Async completion handle used for [`crate::destination::Destination::write_events`].
///
/// This is the path where asynchronous completion changes ETL behavior the most: once dispatch
/// succeeds, the apply loop may continue other work while the destination finishes the batch.
pub type WriteEventsResult<T = ()> = AsyncResult<T>;
/// Pending async completion used for `Destination::write_events`.
pub type PendingWriteEventsResult<T = ()> = PendingAsyncResult<T, ApplyLoopAsyncResultMetadata>;
/// Completed async completion used for `Destination::write_events`.
pub type CompletedWriteEventsResult<T = ()> = CompletedAsyncResult<T, ApplyLoopAsyncResultMetadata>;

/// Dispatch-time metrics carried through an asynchronous completion result.
#[derive(Debug, Clone, Copy)]
pub struct DispatchMetrics {
    /// Number of items in the dispatched batch.
    pub items_count: usize,
    /// Instant at which the batch was handed off to the destination.
    pub dispatched_at: Instant,
}

/// Metadata carried by apply-loop event write completions.
#[derive(Debug, Clone, Copy)]
pub struct ApplyLoopAsyncResultMetadata {
    /// Commit end LSN associated with the dispatched batch, if any.
    pub commit_end_lsn: Option<PgLsn>,
    /// Dispatch-time metrics for the batch.
    pub metrics: DispatchMetrics,
}

/// Sender half of a typed asynchronous completion result.
///
/// Destinations receive this handle from ETL and complete it once the operation is truly done.
/// The method return value is reserved for immediate dispatch or setup failures before work has
/// been accepted. ETL may wait on the pending side immediately or later, depending on the method.
#[derive(Debug)]
pub struct AsyncResult<T> {
    tx: Option<oneshot::Sender<EtlResult<T>>>,
}

impl<T> AsyncResult<T> {
    /// Creates a new asynchronous completion result and its pending counterpart.
    ///
    /// The metadata is stored only on the pending/completed side so ETL can carry method-specific
    /// context across the asynchronous boundary.
    pub fn new<M>(metadata: M) -> (Self, PendingAsyncResult<T, M>) {
        let (tx, rx) = oneshot::channel();

        (
            Self { tx: Some(tx) },
            PendingAsyncResult {
                metadata: Some(metadata),
                rx,
            },
        )
    }

    /// Sends the final result to the waiting receiver.
    pub fn send(mut self, result: EtlResult<T>) {
        let Some(tx) = self.tx.take() else {
            return;
        };

        if tx.send(result).is_err() {
            warn!("could not send async result because receiver was already closed");
        }
    }
}

impl<T> Drop for AsyncResult<T> {
    fn drop(&mut self) {
        let Some(tx) = self.tx.take() else {
            return;
        };

        let _ = tx.send(Err(etl_error!(
            ErrorKind::DestinationError,
            "Async result dropped without sending"
        )));
    }
}

pin_project! {
    /// Receiver half of a typed asynchronous completion result.
    #[must_use = "pending async results do nothing unless polled"]
    #[derive(Debug)]
    pub struct PendingAsyncResult<T, M> {
        metadata: Option<M>,
        #[pin]
        rx: oneshot::Receiver<EtlResult<T>>,
    }
}

impl<T, M> PendingAsyncResult<T, M> {
    /// Returns the metadata attached to this pending result.
    pub fn metadata(&self) -> &M {
        self.metadata
            .as_ref()
            .expect("pending async result metadata is always present until completion")
    }
}

impl<T, M> Future for PendingAsyncResult<T, M> {
    type Output = CompletedAsyncResult<T, M>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.rx.poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(CompletedAsyncResult {
                metadata: this
                    .metadata
                    .take()
                    .expect("pending async result metadata must be present on completion"),
                result,
            }),
            Poll::Ready(Err(_)) => Poll::Ready(CompletedAsyncResult {
                metadata: this
                    .metadata
                    .take()
                    .expect("pending async result metadata must be present on completion"),
                result: Err(etl_error!(
                    ErrorKind::DestinationError,
                    "Async result channel closed before sending"
                )),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Completed typed asynchronous result.
#[derive(Debug)]
pub struct CompletedAsyncResult<T, M> {
    metadata: M,
    result: EtlResult<T>,
}

impl<T, M> CompletedAsyncResult<T, M> {
    /// Returns the metadata attached to this result.
    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    /// Returns the final result.
    pub fn into_result(self) -> EtlResult<T> {
        self.result
    }

    /// Returns the metadata and final result.
    pub fn into_parts(self) -> (M, EtlResult<T>) {
        (self.metadata, self.result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn async_result_round_trips_success() {
        let metadata = ApplyLoopAsyncResultMetadata {
            commit_end_lsn: Some(PgLsn::from(42)),
            metrics: DispatchMetrics {
                items_count: 1,
                dispatched_at: Instant::now(),
            },
        };
        let (result_tx, pending_result) = WriteEventsResult::new(metadata);

        result_tx.send(Ok(7_u64));

        let completed = pending_result.await;
        let (metadata, result) = completed.into_parts();

        assert_eq!(metadata.commit_end_lsn, Some(PgLsn::from(42)));
        assert_eq!(result.unwrap(), 7);
    }

    #[tokio::test]
    async fn dropping_async_result_surfaces_error() {
        let (result_tx, pending_result) = WriteTableRowsResult::<()>::new(());
        drop(result_tx);

        let err = pending_result.await.into_result().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationError);
        assert_eq!(
            err.description(),
            Some("Async result dropped without sending")
        );
    }
}
