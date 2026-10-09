//! Exponential backoff for destination-owned retry loops.
//!
//! This module owns attempt counting, delay growth, and sleeping, while
//! logging and metrics stay at the destination call site. A loop retries only
//! errors that are [`Retryability::Retryable`], so local retries follow the
//! same classification as the pipeline's retry policy.

use std::{future::Future, time::Duration};

use etl::error::Retryability;

use crate::retry::ClassifiedError;

/// Retry policy for one destination-owned operation.
///
/// `max_retries` counts retries after the initial attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryPolicy {
    /// Maximum number of retries after the first attempt.
    pub(crate) max_retries: u32,
    /// Delay before the first retry.
    pub(crate) initial_delay: Duration,
    /// Upper bound for the exponential backoff base delay.
    pub(crate) max_delay: Duration,
}

/// Retry metadata emitted before one sleep.
#[derive(Debug)]
pub(crate) struct RetryAttempt<'a, E> {
    /// One-based retry number.
    pub(crate) retry_index: u32,
    /// Configured retry limit.
    pub(crate) max_retries: u32,
    /// Exponential backoff delay before caller-specific shaping.
    #[cfg_attr(not(test), expect(dead_code, reason = "base delay is inspected by retry tests"))]
    pub(crate) base_delay: Duration,
    /// Final delay that will be slept.
    #[cfg_attr(
        all(not(test), not(any(feature = "bigquery", feature = "snowflake"))),
        expect(dead_code, reason = "DuckLake logs its own retry delay")
    )]
    pub(crate) sleep_delay: Duration,
    /// Error that triggered the retry.
    pub(crate) error: &'a E,
}

/// Final failure after the retry helper stops.
#[derive(Debug)]
pub(crate) struct RetryFailure<E> {
    /// Total attempts including the initial attempt.
    #[cfg_attr(not(test), expect(dead_code, reason = "attempt count is inspected by retry tests"))]
    pub(crate) total_attempts: u32,
    /// Last error returned by the operation.
    pub(crate) last_error: E,
}

/// Executes an async operation with exponential backoff while it fails with a
/// retryable error.
///
/// The helper owns attempt counting, delay growth, and sleeping, and retries an
/// error only when [`ClassifiedError::retryability`] is
/// [`Retryability::Retryable`]. `stop_early` can end retries sooner, for
/// example during shutdown or when resending in place is unsafe; it cannot
/// retry a permanent error. Callers retain delay shaping, logging, and metrics.
pub(crate) async fn retry_with_backoff<
    T,
    E,
    AttemptFn,
    AttemptFut,
    StopEarly,
    TransformDelay,
    OnRetry,
>(
    policy: RetryPolicy,
    mut stop_early: StopEarly,
    mut transform_delay: TransformDelay,
    mut on_retry: OnRetry,
    mut attempt_fn: AttemptFn,
) -> Result<T, RetryFailure<E>>
where
    E: ClassifiedError,
    AttemptFn: FnMut() -> AttemptFut,
    AttemptFut: Future<Output = Result<T, E>>,
    StopEarly: FnMut(&E) -> bool,
    TransformDelay: FnMut(Duration) -> Duration,
    OnRetry: FnMut(RetryAttempt<'_, E>),
{
    let mut total_attempts = 0_u32;
    let mut base_delay = policy.initial_delay.min(policy.max_delay);

    loop {
        total_attempts = total_attempts.saturating_add(1);

        match attempt_fn().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retry_index = total_attempts;
                if retry_index > policy.max_retries
                    || error.retryability() == Retryability::Permanent
                    || stop_early(&error)
                {
                    return Err(RetryFailure { total_attempts, last_error: error });
                }

                let sleep_delay = transform_delay(base_delay);
                on_retry(RetryAttempt {
                    retry_index,
                    max_retries: policy.max_retries,
                    base_delay,
                    sleep_delay,
                    error: &error,
                });

                tokio::time::sleep(sleep_delay).await;
                base_delay =
                    base_delay.checked_mul(2).unwrap_or(Duration::MAX).min(policy.max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    use super::*;

    /// Error returned by test operations.
    #[derive(Debug, PartialEq, Eq)]
    struct TestError {
        /// Zero-based attempt that failed.
        attempt: usize,
        /// Whether the failed attempt may succeed again.
        retryability: Retryability,
    }

    impl ClassifiedError for TestError {
        fn retryability(&self) -> Retryability {
            self.retryability
        }
    }

    /// Returns a retryable failure of `attempt`.
    fn retryable(attempt: usize) -> TestError {
        TestError { attempt, retryability: Retryability::Retryable }
    }

    /// Retries until the operation succeeds.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (seen_retries_tx, seen_retries) = mpsc::channel();

        let attempts_for_task = Arc::clone(&attempts);
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 3,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(20),
                },
                |_| false,
                |delay| delay,
                move |attempt: RetryAttempt<'_, TestError>| {
                    seen_retries_tx
                        .send((attempt.retry_index, attempt.base_delay, attempt.sleep_delay))
                        .unwrap();
                },
                move || {
                    let attempts = Arc::clone(&attempts_for_task);
                    async move {
                        let current = attempts.fetch_add(1, Ordering::SeqCst);
                        if current < 2 { Err(retryable(current)) } else { Ok("done") }
                    }
                },
            )
            .await
        });

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        assert_eq!(handle.await.unwrap().unwrap(), "done");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(
            seen_retries.try_iter().collect::<Vec<_>>(),
            vec![
                (1, Duration::from_millis(5), Duration::from_millis(5),),
                (2, Duration::from_millis(10), Duration::from_millis(10),),
            ]
        );
    }

    /// Caps exponential delay growth at the configured maximum.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_caps_delay_growth() {
        let (base_delays_tx, base_delays) = mpsc::channel();
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 3,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(8),
                },
                |_| false,
                |delay| delay,
                move |attempt: RetryAttempt<'_, TestError>| {
                    base_delays_tx.send(attempt.base_delay).unwrap();
                },
                || async { Err::<(), _>(retryable(0)) },
            )
            .await
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(8)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(8)).await;
        tokio::task::yield_now().await;

        let failure = handle.await.unwrap().unwrap_err();
        assert_eq!(failure.total_attempts, 4);
        assert_eq!(failure.last_error, retryable(0));
        assert_eq!(
            base_delays.try_iter().collect::<Vec<_>>(),
            vec![Duration::from_millis(5), Duration::from_millis(8), Duration::from_millis(8),]
        );
    }

    /// Stops immediately on a permanent error, and when `stop_early` asks to,
    /// even though the error is retryable.
    #[tokio::test]
    async fn retry_with_backoff_stops_on_permanent_error_or_early_stop() {
        let on_retry_calls = Arc::new(AtomicUsize::new(0));
        let policy = RetryPolicy {
            max_retries: 5,
            initial_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(20),
        };
        let permanent = TestError { attempt: 0, retryability: Retryability::Permanent };

        let on_retry_calls_for_permanent = Arc::clone(&on_retry_calls);
        let failure = retry_with_backoff(
            policy,
            |_| false,
            |delay| delay,
            move |_attempt: RetryAttempt<'_, TestError>| {
                on_retry_calls_for_permanent.fetch_add(1, Ordering::SeqCst);
            },
            || async {
                Err::<(), _>(TestError { attempt: 0, retryability: Retryability::Permanent })
            },
        )
        .await
        .unwrap_err();
        assert_eq!(failure.total_attempts, 1);
        assert_eq!(failure.last_error, permanent);

        let on_retry_calls_for_stop = Arc::clone(&on_retry_calls);
        let failure = retry_with_backoff(
            policy,
            |_| true,
            |delay| delay,
            move |_attempt: RetryAttempt<'_, TestError>| {
                on_retry_calls_for_stop.fetch_add(1, Ordering::SeqCst);
            },
            || async { Err::<(), _>(retryable(0)) },
        )
        .await
        .unwrap_err();
        assert_eq!(failure.total_attempts, 1);
        assert_eq!(failure.last_error, retryable(0));

        assert_eq!(on_retry_calls.load(Ordering::SeqCst), 0);
    }

    /// Applies caller-provided delay shaping before sleeping.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_applies_transformed_delay() {
        let (seen_sleep_delays_tx, seen_sleep_delays) = mpsc::channel();
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 1,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(20),
                },
                |_| false,
                |delay| delay + Duration::from_millis(3),
                move |attempt: RetryAttempt<'_, TestError>| {
                    seen_sleep_delays_tx.send(attempt.sleep_delay).unwrap();
                },
                {
                    let attempts = Arc::new(AtomicUsize::new(0));
                    move || {
                        let attempts = Arc::clone(&attempts);
                        async move {
                            let current = attempts.fetch_add(1, Ordering::SeqCst);
                            if current == 0 { Err(retryable(current)) } else { Ok("done") }
                        }
                    }
                },
            )
            .await
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(8)).await;
        tokio::task::yield_now().await;

        assert_eq!(handle.await.unwrap().unwrap(), "done");
        assert_eq!(
            seen_sleep_delays.try_iter().collect::<Vec<_>>(),
            vec![Duration::from_millis(8)]
        );
    }

    /// Returns the last error after exhausting retries.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_returns_last_error_after_exhaustion() {
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 2,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(20),
                },
                |_| false,
                |delay| delay,
                |_attempt: RetryAttempt<'_, TestError>| {},
                {
                    let attempts = Arc::new(AtomicUsize::new(0));
                    move || {
                        let attempts = Arc::clone(&attempts);
                        async move { Err::<(), _>(retryable(attempts.fetch_add(1, Ordering::SeqCst))) }
                    }
                },
            )
            .await
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        let failure = handle.await.unwrap().unwrap_err();
        assert_eq!(failure.total_attempts, 3);
        assert_eq!(failure.last_error, retryable(2));
    }
}
