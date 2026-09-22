//! Cooperative cancellation of individual waits.

/// Polls a future until completion or cancellation, with shutdown taking
/// priority when both are ready. Expands to an expression in an async context;
/// the future's output is preserved inside [`ShutdownResult::Ok`], including
/// any error it returns.
///
/// Both arguments are evaluated once. Cancellation drops the supplied future,
/// so use this only where dropping that future is safe. It does not abort or
/// join spawned work, undo remote effects, or perform graceful cleanup.
macro_rules! with_shutdown {
    ($future:expr, $shutdown_token:expr $(,)?) => {{
        let shutdown_token = &$shutdown_token;
        ::tokio::select! {
            biased;

            _ = shutdown_token.cancelled() => {
                $crate::runtime::concurrency::ShutdownResult::Shutdown(())
            }

            output = $future => $crate::runtime::concurrency::ShutdownResult::Ok(output),
        }
    }};
}

pub(crate) use with_shutdown;

/// Result type that distinguishes between normal operation and shutdown
/// scenarios.
///
/// [`ShutdownResult`] is used by operations that can be interrupted by shutdown
/// signals. It preserves both successful results and any partial data that was
/// being processed when shutdown was requested.
pub(crate) enum ShutdownResult<T, I> {
    /// Normal completion with the future's output, which may itself be an
    /// error.
    Ok(T),
    /// Operation was interrupted by shutdown, with any partial data preserved.
    Shutdown(I),
}

impl<T, I> ShutdownResult<T, I> {
    /// Returns true if this result represents a shutdown scenario.
    pub(crate) fn should_shutdown(&self) -> bool {
        matches!(self, ShutdownResult::Shutdown(_))
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use crate::runtime::concurrency::ShutdownResult;

    /// Fallible output is preserved without conflating errors with shutdown.
    #[tokio::test]
    async fn completion_preserves_success_and_failure() {
        let token = CancellationToken::new();
        for output in [Ok(7), Err("Test failure")] {
            let ShutdownResult::Ok(actual) = with_shutdown!(async { output }, token) else {
                panic!("Unexpected shutdown");
            };
            assert_eq!(actual, output);
        }
    }

    /// A retained request wins even when the supplied future is already ready.
    #[tokio::test]
    async fn cancellation_takes_priority_without_polling_ready_work() {
        let token = CancellationToken::new();
        token.cancel();
        for _ in 0..2 {
            let result =
                with_shutdown!(async { panic!("Cancelled work must not be polled") }, token);
            assert!(matches!(result, ShutdownResult::Shutdown(())));
        }
    }

    /// Cancelling an active wait drops its future before reporting shutdown.
    #[tokio::test]
    async fn cancellation_drops_pending_work() {
        let token = CancellationToken::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (lifetime_tx, mut lifetime_rx) = oneshot::channel::<()>();
        let future = async move {
            let _lifetime = lifetime_tx;
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        };
        let waiting = async { with_shutdown!(future, token) };
        tokio::pin!(waiting);
        tokio::select! {
            _ = &mut waiting => panic!("Wait completed before cancellation"),

            result = started_rx => result.unwrap(),
        }
        token.cancel();
        assert!(matches!(waiting.await, ShutdownResult::Shutdown(())));
        assert_eq!(lifetime_rx.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    }
}
