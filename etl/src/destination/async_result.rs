use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::sync::oneshot;
use tokio_postgres::types::PgLsn;

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;

/// Sender half of an asynchronous apply result.
///
/// This handle is passed to [`crate::destination::Destination::write_events`] so
/// destinations can report the final outcome of a flushed event batch without
/// coupling apply-loop progress to the method's return value.
#[derive(Debug)]
pub struct ApplyAsyncResult<T> {
    tx: Option<oneshot::Sender<EtlResult<T>>>,
}

impl<T> ApplyAsyncResult<T> {
    /// Creates a new apply result channel and its pending counterpart.
    pub fn new(commit_end_lsn: Option<PgLsn>) -> (Self, PendingApplyAsyncResult<T>) {
        let (tx, rx) = oneshot::channel();

        (
            Self { tx: Some(tx) },
            PendingApplyAsyncResult { commit_end_lsn, rx },
        )
    }

    /// Sends a completed result to the apply loop.
    pub fn send_result(mut self, result: EtlResult<T>) -> Result<(), EtlResult<T>> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };

        tx.send(result)
    }

    /// Sends a successful result to the apply loop.
    pub fn send_ok(self, value: T) -> Result<(), T> {
        match self.send_result(Ok(value)) {
            Ok(()) => Ok(()),
            Err(Ok(value)) => Err(value),
            Err(Err(_)) => unreachable!("successful apply results cannot return errors"),
        }
    }

    /// Sends a failed result to the apply loop.
    pub fn send_err(self, error: EtlError) -> Result<(), EtlError> {
        match self.send_result(Err(error)) {
            Ok(()) => Ok(()),
            Err(Err(error)) => Err(error),
            Err(Ok(_)) => unreachable!("failed apply results cannot return success values"),
        }
    }
}

impl<T> Drop for ApplyAsyncResult<T> {
    fn drop(&mut self) {
        let Some(tx) = self.tx.take() else {
            return;
        };

        let _ = tx.send(Err(etl_error!(
            ErrorKind::DestinationError,
            "apply async result dropped without sending"
        )));
    }
}

pin_project! {
    /// Receiver half of an asynchronous apply result.
    ///
    /// The apply loop stores these in a [`futures::stream::FuturesOrdered`] so
    /// destination acknowledgements are processed in the same order batches were
    /// written, even if the destination completes them out of order.
    #[must_use = "pending apply results do nothing unless polled"]
    #[derive(Debug)]
    pub struct PendingApplyAsyncResult<T> {
        commit_end_lsn: Option<PgLsn>,
        #[pin]
        rx: oneshot::Receiver<EtlResult<T>>,
    }
}

impl<T> PendingApplyAsyncResult<T> {
    /// Returns the commit end LSN associated with this result, if any.
    pub fn commit_end_lsn(&self) -> Option<PgLsn> {
        self.commit_end_lsn
    }
}

/// Completed asynchronous apply result returned by [`PendingApplyAsyncResult`].
#[derive(Debug)]
pub struct CompletedApplyAsyncResult<T> {
    commit_end_lsn: Option<PgLsn>,
    result: EtlResult<T>,
}

impl<T> CompletedApplyAsyncResult<T> {
    /// Returns the commit end LSN associated with this result, if any.
    pub fn commit_end_lsn(&self) -> Option<PgLsn> {
        self.commit_end_lsn
    }

    /// Returns the final result.
    pub fn into_result(self) -> EtlResult<T> {
        self.result
    }

    /// Returns the LSN and final result.
    pub fn into_parts(self) -> (Option<PgLsn>, EtlResult<T>) {
        (self.commit_end_lsn, self.result)
    }
}

impl<T> Future for PendingApplyAsyncResult<T> {
    type Output = CompletedApplyAsyncResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.rx.poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(CompletedApplyAsyncResult {
                commit_end_lsn: *this.commit_end_lsn,
                result,
            }),
            Poll::Ready(Err(_)) => Poll::Ready(CompletedApplyAsyncResult {
                commit_end_lsn: *this.commit_end_lsn,
                result: Err(etl_error!(
                    ErrorKind::DestinationError,
                    "apply async result channel closed before sending"
                )),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn apply_async_result_round_trips_success() {
        let (apply_result, pending_result) = ApplyAsyncResult::new(Some(PgLsn::from(42)));

        apply_result.send_ok(7_u64).unwrap();

        let completed = pending_result.await;
        let (commit_end_lsn, result) = completed.into_parts();

        assert_eq!(commit_end_lsn, Some(PgLsn::from(42)));
        assert_eq!(result.unwrap(), 7);
    }

    #[tokio::test]
    async fn dropping_apply_async_result_surfaces_error() {
        let (apply_result, pending_result) = ApplyAsyncResult::<()>::new(None);
        drop(apply_result);

        let err = pending_result.await.into_result().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationError);
        assert_eq!(
            err.description(),
            Some("apply async result dropped without sending")
        );
    }
}
