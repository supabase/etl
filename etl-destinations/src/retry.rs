//! Shared retry helpers for destination-owned retry loops.
//!
//! This module centralizes exponential backoff mechanics while leaving retry
//! classification, logging, and metrics at the destination call site.

use std::future::Future;
use std::time::Duration;

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

/// Retry decision for one failed attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryDecision {
    /// Retry the operation after the computed delay.
    Retry,
    /// Stop retrying and return the error immediately.
    Stop,
}

/// Retry metadata emitted before one sleep.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RetryAttempt<'a, E> {
    /// One-based retry number.
    pub(crate) retry_index: u32,
    /// Configured retry limit.
    pub(crate) max_retries: u32,
    /// Exponential backoff delay before caller-specific shaping.
    pub(crate) base_delay: Duration,
    /// Final delay that will be slept.
    pub(crate) sleep_delay: Duration,
    /// Error that triggered the retry.
    pub(crate) error: &'a E,
}

/// Final failure after the retry helper stops.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RetryFailure<E> {
    /// Total attempts including the initial attempt.
    pub(crate) total_attempts: u32,
    /// Last error returned by the operation.
    pub(crate) last_error: E,
}

/// Executes an async operation with exponential backoff.
///
/// The helper owns attempt counting, delay growth, and sleeping. Callers retain
/// control over retry classification, delay shaping, logging, and metrics.
pub(crate) async fn retry_with_backoff<
    T,
    E,
    AttemptFn,
    AttemptFut,
    ShouldRetry,
    TransformDelay,
    OnRetry,
>(
    policy: RetryPolicy,
    mut should_retry: ShouldRetry,
    mut transform_delay: TransformDelay,
    mut on_retry: OnRetry,
    mut attempt_fn: AttemptFn,
) -> Result<T, RetryFailure<E>>
where
    AttemptFn: FnMut() -> AttemptFut,
    AttemptFut: Future<Output = Result<T, E>>,
    ShouldRetry: FnMut(&E) -> RetryDecision,
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
                if retry_index > policy.max_retries || should_retry(&error) == RetryDecision::Stop {
                    return Err(RetryFailure {
                        total_attempts,
                        last_error: error,
                    });
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
                base_delay = base_delay
                    .checked_mul(2)
                    .unwrap_or(Duration::MAX)
                    .min(policy.max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// Retries until the operation succeeds.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let seen_retries = Arc::new(Mutex::new(Vec::new()));

        let attempts_for_task = Arc::clone(&attempts);
        let seen_retries_for_task = Arc::clone(&seen_retries);
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 3,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(20),
                },
                |_| RetryDecision::Retry,
                |delay| delay,
                move |attempt: RetryAttempt<'_, &'static str>| {
                    seen_retries_for_task.lock().unwrap().push((
                        attempt.retry_index,
                        attempt.base_delay,
                        attempt.sleep_delay,
                    ));
                },
                move || {
                    let attempts = Arc::clone(&attempts_for_task);
                    async move {
                        let current = attempts.fetch_add(1, Ordering::SeqCst);
                        if current < 2 {
                            Err("retry me")
                        } else {
                            Ok("done")
                        }
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
            *seen_retries.lock().unwrap(),
            vec![
                (1, Duration::from_millis(5), Duration::from_millis(5),),
                (2, Duration::from_millis(10), Duration::from_millis(10),),
            ]
        );
    }

    /// Caps exponential delay growth at the configured maximum.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_caps_delay_growth() {
        let base_delays = Arc::new(Mutex::new(Vec::new()));
        let base_delays_for_task = Arc::clone(&base_delays);
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 3,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(8),
                },
                |_| RetryDecision::Retry,
                |delay| delay,
                move |attempt: RetryAttempt<'_, &'static str>| {
                    base_delays_for_task
                        .lock()
                        .unwrap()
                        .push(attempt.base_delay);
                },
                || async { Err::<(), _>("still failing") },
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
        assert_eq!(failure.last_error, "still failing");
        assert_eq!(
            *base_delays.lock().unwrap(),
            vec![
                Duration::from_millis(5),
                Duration::from_millis(8),
                Duration::from_millis(8),
            ]
        );
    }

    /// Stops immediately when the caller marks an error as non-retriable.
    #[tokio::test]
    async fn retry_with_backoff_stops_on_non_retriable_error() {
        let on_retry_calls = Arc::new(AtomicUsize::new(0));
        let on_retry_calls_for_task = Arc::clone(&on_retry_calls);

        let failure = retry_with_backoff(
            RetryPolicy {
                max_retries: 5,
                initial_delay: Duration::from_millis(5),
                max_delay: Duration::from_millis(20),
            },
            |_| RetryDecision::Stop,
            |delay| delay,
            move |_attempt: RetryAttempt<'_, &'static str>| {
                on_retry_calls_for_task.fetch_add(1, Ordering::SeqCst);
            },
            || async { Err::<(), _>("stop now") },
        )
        .await
        .unwrap_err();

        assert_eq!(failure.total_attempts, 1);
        assert_eq!(failure.last_error, "stop now");
        assert_eq!(on_retry_calls.load(Ordering::SeqCst), 0);
    }

    /// Applies caller-provided delay shaping before sleeping.
    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_applies_transformed_delay() {
        let seen_sleep_delays = Arc::new(Mutex::new(Vec::new()));
        let seen_sleep_delays_for_task = Arc::clone(&seen_sleep_delays);
        let handle = tokio::spawn(async move {
            retry_with_backoff(
                RetryPolicy {
                    max_retries: 1,
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(20),
                },
                |_| RetryDecision::Retry,
                |delay| delay + Duration::from_millis(3),
                move |attempt: RetryAttempt<'_, &'static str>| {
                    seen_sleep_delays_for_task
                        .lock()
                        .unwrap()
                        .push(attempt.sleep_delay);
                },
                {
                    let attempts = Arc::new(AtomicUsize::new(0));
                    move || {
                        let attempts = Arc::clone(&attempts);
                        async move {
                            let current = attempts.fetch_add(1, Ordering::SeqCst);
                            if current == 0 {
                                Err("retry once")
                            } else {
                                Ok("done")
                            }
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
            *seen_sleep_delays.lock().unwrap(),
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
                |_| RetryDecision::Retry,
                |delay| delay,
                |_attempt: RetryAttempt<'_, usize>| {},
                {
                    let attempts = Arc::new(AtomicUsize::new(0));
                    move || {
                        let attempts = Arc::clone(&attempts);
                        async move { Err::<(), _>(attempts.fetch_add(1, Ordering::SeqCst)) }
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
        assert_eq!(failure.last_error, 2);
    }
}
