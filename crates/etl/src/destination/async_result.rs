use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use pin_project_lite::pin_project;
use tokio::sync::oneshot;
use tokio_postgres::types::PgLsn;
use tracing::warn;

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
};

/// Durability status reported by streaming and table-copy destination writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationWriteStatus {
    /// The destination accepted ownership of the write, but ETL must not
    /// consider it durable yet.
    ///
    /// Completing a result as `Accepted` consumes that result. After that
    /// point, ETL cannot wait on the same result for a later durability signal.
    ///
    /// For streaming writes through
    /// [`crate::destination::Destination::write_events`], ETL does not advance
    /// the batch's commit end LSN. It carries that LSN into the next streaming
    /// write and advances durable progress only when a later cumulative
    /// [`DestinationWriteStatus::Durable`] result covers it. If no later write
    /// is dispatched before shutdown, ETL leaves progress at the last durable
    /// checkpoint so restart can replay the accepted write.
    ///
    /// For table-copy writes through
    /// [`crate::destination::Destination::write_table_rows`], ETL may request
    /// the next batch for the same copy partition, but records that the table
    /// requires a terminal durability barrier. After all copy workers finish,
    /// ETL sends an empty table-copy write and refuses to store `FinishedCopy`
    /// until that barrier returns [`DestinationWriteStatus::Durable`].
    /// Returning `Accepted` from the terminal barrier fails the table copy.
    Accepted,
    /// The destination confirms the write is durable.
    ///
    /// For streaming writes through
    /// [`crate::destination::Destination::write_events`], this write and all
    /// earlier `Accepted` writes in the same ordered apply-loop stream are
    /// durable. ETL may advance through the greatest commit end LSN carried by
    /// this write or reattached from those earlier writes.
    ///
    /// For a nonempty table-copy write through
    /// [`crate::destination::Destination::write_table_rows`], this status
    /// confirms that batch is durable. If any table-copy batch returned
    /// `Accepted`, ETL still sends a terminal empty write after every copy
    /// worker finishes. `Durable` from that barrier must cover every accepted
    /// write in the current table-copy attempt before ETL stores
    /// `FinishedCopy`. Empty and skipped tables must also return `Durable`
    /// from their empty initialization write before the copy can finish.
    Durable,
}

/// Async completion handle used for
/// [`crate::destination::Destination::write_table_rows`].
///
/// ETL waits for this result before requesting the next row batch for the same
/// copy partition. A destination may report
/// [`DestinationWriteStatus::Accepted`] to continue copying before the batch is
/// durable. ETL then issues a terminal empty write as a table-wide durability
/// barrier and requires [`DestinationWriteStatus::Durable`] before completing
/// the copy.
pub type WriteTableRowsResult<T = DestinationWriteStatus> = AsyncResult<T>;

/// Async completion handle used for
/// [`crate::destination::Destination::drop_table_for_copy`].
///
/// ETL waits for this result immediately before clearing stored table-copy
/// metadata and starting a fresh copy.
pub type DropTableForCopyResult<T = ()> = AsyncResult<T>;

/// Async completion handle used for
/// [`crate::destination::Destination::write_events`].
///
/// This is the path where asynchronous completion changes ETL behavior the
/// most: once dispatch succeeds, the apply loop may continue other work while
/// the destination finishes the batch.
pub type WriteEventsResult<T = DestinationWriteStatus> = AsyncResult<T>;
/// Pending async completion used for `Destination::write_events`.
pub(crate) type PendingWriteEventsResult<T = DestinationWriteStatus> =
    PendingAsyncResult<T, ApplyLoopAsyncResultMetadata>;
/// Completed async completion used for `Destination::write_events`.
pub(crate) type CompletedWriteEventsResult<T = DestinationWriteStatus> =
    CompletedAsyncResult<T, ApplyLoopAsyncResultMetadata>;

/// Dispatch-time metrics carried through an asynchronous completion result.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DispatchMetrics {
    /// Number of items in the dispatched batch.
    pub items_count: usize,
    /// Instant at which the batch was handed off to the destination.
    pub dispatched_at: Instant,
}

/// Metadata carried by apply-loop event write completions.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ApplyLoopAsyncResultMetadata {
    /// Commit end LSN associated with the dispatched batch, if any.
    ///
    /// For immediate destinations this becomes durable progress when the write
    /// result returns [`DestinationWriteStatus::Durable`]. For deferred
    /// destinations, the apply loop carries this LSN across
    /// [`DestinationWriteStatus::Accepted`] results and advances only when a
    /// later cumulative durable result covers the carried LSN.
    pub commit_end_lsn: Option<PgLsn>,
    /// Dispatch-time metrics for the batch.
    pub metrics: DispatchMetrics,
}

/// Sender half of a typed asynchronous completion result.
///
/// Destinations receive this handle from ETL and complete it once the operation
/// is truly done. The method return value is reserved for immediate dispatch or
/// setup failures before work has been accepted. ETL may wait on the pending
/// side immediately or later, depending on the method.
#[derive(Debug)]
pub struct AsyncResult<T> {
    tx: Option<oneshot::Sender<EtlResult<T>>>,
}

impl<T> AsyncResult<T> {
    /// Creates a new asynchronous completion result and its pending
    /// counterpart.
    ///
    /// The metadata is stored only on the pending/completed side so ETL can
    /// carry method-specific context across the asynchronous boundary.
    pub(crate) fn new<M>(metadata: M) -> (Self, PendingAsyncResult<T, M>) {
        let (tx, rx) = oneshot::channel();

        (Self { tx: Some(tx) }, PendingAsyncResult { metadata: Some(metadata), rx })
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
    pub(crate) struct PendingAsyncResult<T, M> {
        metadata: Option<M>,
        #[pin]
        rx: oneshot::Receiver<EtlResult<T>>,
    }
}

impl<T, M> Future for PendingAsyncResult<T, M> {
    type Output = CompletedAsyncResult<T, M>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.rx.poll(cx) {
            Poll::Ready(Ok(result)) => {
                Poll::Ready(CompletedAsyncResult { metadata: this.metadata.take(), result })
            }
            Poll::Ready(Err(_)) => Poll::Ready(CompletedAsyncResult {
                metadata: this.metadata.take(),
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
pub(crate) struct CompletedAsyncResult<T, M> {
    metadata: Option<M>,
    result: EtlResult<T>,
}

impl<T, M> CompletedAsyncResult<T, M> {
    /// Returns the final result.
    pub(crate) fn into_result(self) -> EtlResult<T> {
        self.result
    }

    /// Returns the metadata and final result.
    pub(crate) fn into_parts(self) -> (Option<M>, EtlResult<T>) {
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
            metrics: DispatchMetrics { items_count: 1, dispatched_at: Instant::now() },
        };
        let (result_tx, pending_result) = WriteEventsResult::new(metadata);

        result_tx.send(Ok(7_u64));

        let completed = pending_result.await;
        let (metadata, result) = completed.into_parts();

        let metadata = metadata.expect("metadata should be present");
        assert_eq!(metadata.commit_end_lsn, Some(PgLsn::from(42)));
        assert_eq!(result.unwrap(), 7);
    }

    #[tokio::test]
    async fn dropping_async_result_surfaces_error() {
        let (result_tx, pending_result) = WriteTableRowsResult::<()>::new(());
        drop(result_tx);

        let err = pending_result.await.into_result().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationError);
        assert_eq!(err.description(), Some("Async result dropped without sending"));
    }

    #[tokio::test]
    async fn completed_async_result_can_surface_missing_metadata_without_panicking() {
        let (result_tx, rx) = oneshot::channel();
        let mut pending_result =
            PendingAsyncResult::<u64, ApplyLoopAsyncResultMetadata> { metadata: None, rx };

        result_tx.send(Ok(7)).unwrap();

        let completed = std::future::poll_fn(|cx| Pin::new(&mut pending_result).poll(cx)).await;
        let (metadata, result) = completed.into_parts();

        assert!(metadata.is_none());
        assert_eq!(result.unwrap(), 7);
    }
}
