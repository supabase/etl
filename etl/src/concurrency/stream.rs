use core::pin::Pin;
use core::task::{Context, Poll};
use etl_config::shared::BatchConfig;
use futures::{Future, Stream, ready};
use pin_project_lite::pin_project;
use std::time::Duration;
use tracing::info;

use crate::concurrency::batch_budget::CachedBatchBudget;
use crate::concurrency::memory_monitor::MemoryMonitorSubscription;
use crate::types::SizeHint;

pin_project! {
    /// A stream adapter that pauses polling when memory monitor reports pressure.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
pub struct BackpressureStream<S: Stream> {
        #[pin]
        stream: S,
        memory_subscription: Option<MemoryMonitorSubscription>,
        paused_for_memory: bool,
    }
}

impl<S: Stream> BackpressureStream<S> {
    /// Creates a new [`BackpressureStream`] wrapping `stream`.
    pub fn wrap(stream: S, memory_subscription: Option<MemoryMonitorSubscription>) -> Self {
        Self {
            stream,
            memory_subscription,
            paused_for_memory: false,
        }
    }

    /// Returns a pinned mutable reference to the wrapped stream.
    pub fn stream_mut(self: Pin<&mut Self>) -> Pin<&mut S> {
        self.project().stream
    }
}

impl<S: Stream> Stream for BackpressureStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let was_paused = *this.paused_for_memory;

        if let Some(memory_subscription) = this.memory_subscription.as_mut() {
            // Drain all currently queued watch updates and only stop at `Pending`.
            // Hitting `Pending` is important because it registers this task's waker for the next
            // backpressure transition, so returning `Pending` below cannot miss a wakeup.
            loop {
                match Pin::new(&mut *memory_subscription).poll_next(cx) {
                    Poll::Ready(Some(backpressure_active)) => {
                        *this.paused_for_memory = backpressure_active;
                    }
                    Poll::Ready(None) => {
                        // If the was channel was dropped, we assume that memory is fine, to be resilient.
                        *this.paused_for_memory = false;

                        break;
                    }
                    Poll::Pending => {
                        // If the memory state didn't change, we just use the current state that is on the
                        // watch.
                        let currently_backpressure_active =
                            memory_subscription.current_backpressure_active();
                        if *this.paused_for_memory != currently_backpressure_active {
                            *this.paused_for_memory = currently_backpressure_active;
                        }

                        break;
                    }
                }
            }
        } else {
            *this.paused_for_memory = false;
        }

        if !was_paused && *this.paused_for_memory {
            info!("backpressure active, stream paused");
        } else if was_paused && !*this.paused_for_memory {
            info!("backpressure released, stream resumed");
        }

        if *this.paused_for_memory {
            return Poll::Pending;
        }

        this.stream.as_mut().poll_next(cx)
    }
}

// Implementation adapted from:
//  https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs.
pin_project! {
    /// A stream adapter that batches items based on size limits and timeouts.
    ///
    /// This stream collects items from the underlying stream into batches, emitting them when either:
    /// - The batch reaches its current byte budget
    /// - A timeout occurs
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
pub struct BatchBackpressureStream<B, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        items: Vec<S::Item>,
        current_batch_bytes: usize,
        cached_batch_budget: CachedBatchBudget,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
        memory_subscription: Option<MemoryMonitorSubscription>,
        paused_for_memory: bool,
    }
}

impl<B, S: Stream<Item = B>> BatchBackpressureStream<B, S>
where
    B: SizeHint,
{
    /// Creates a new [`BatchBackpressureStream`].
    pub fn wrap(
        stream: S,
        batch_config: BatchConfig,
        memory_subscription: Option<MemoryMonitorSubscription>,
        cached_batch_budget: CachedBatchBudget,
    ) -> Self {
        BatchBackpressureStream {
            stream,
            deadline: None,
            items: Vec::with_capacity(batch_config.max_size),
            current_batch_bytes: 0,
            cached_batch_budget,
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            memory_subscription,
            paused_for_memory: false,
        }
    }
}

impl<B, S: Stream<Item = B>> Stream for BatchBackpressureStream<B, S>
where
    B: SizeHint,
{
    type Item = Vec<S::Item>;

    /// Polls the stream for the next batch of items using a complex state machine.
    ///
    /// This method implements a batching algorithm that balances throughput
    /// and latency by collecting items into batches based on both size and time constraints.
    /// The polling state machine handles multiple concurrent conditions while preserving bounded
    /// buffering behavior.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // Fast path: if the inner stream has already ended, we're done.
        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            // PRIORITY 1: Memory backpressure.
            // If memory backpressure is active and there are buffered items, flush immediately to avoid
            // accumulating more memory in this stream.
            let was_paused = *this.paused_for_memory;
            if let Some(memory_subscription) = this.memory_subscription.as_mut() {
                // Drain all currently queued watch updates and only stop at `Pending`.
                // Hitting `Pending` is important because it registers this task's waker for the next
                // backpressure transition, so returning `Pending` below cannot miss a wakeup.
                loop {
                    match Pin::new(&mut *memory_subscription).poll_next(cx) {
                        Poll::Ready(Some(backpressure_active)) => {
                            *this.paused_for_memory = backpressure_active;
                        }
                        Poll::Ready(None) => {
                            // If the was channel was dropped, we assume that memory is fine, to be resilient.
                            *this.paused_for_memory = false;

                            break;
                        }
                        Poll::Pending => {
                            // If the memory state didn't change, we just use the current state that is on the
                            // watch.
                            let currently_backpressure_active =
                                memory_subscription.current_backpressure_active();
                            if *this.paused_for_memory != currently_backpressure_active {
                                *this.paused_for_memory = currently_backpressure_active;
                            }

                            break;
                        }
                    }
                }
            } else {
                *this.paused_for_memory = false;
            }

            if !was_paused && *this.paused_for_memory {
                info!("backpressure active, batch stream paused");
            } else if was_paused && !*this.paused_for_memory {
                info!("backpressure released, batch stream resumed");
            }

            if *this.paused_for_memory {
                if !this.items.is_empty() {
                    info!(
                        buffered_items = this.items.len(),
                        buffered_bytes = *this.current_batch_bytes,
                        "backpressure active, flushing buffered batch"
                    );
                    *this.reset_timer = true;
                    *this.current_batch_bytes = 0;

                    return Poll::Ready(Some(std::mem::take(this.items)));
                }

                return Poll::Pending;
            }

            // PRIORITY 2: Timer management.
            // Reset the timeout timer when starting a new batch or after emitting a batch
            if *this.reset_timer {
                this.deadline
                    .set(Some(tokio::time::sleep(Duration::from_millis(
                        this.batch_config.max_fill_ms,
                    ))));
                *this.reset_timer = false;
            }

            // PRIORITY 3: Memory optimization.
            // Pre-allocate batch capacity when starting to collect items
            // This avoids reallocations during batch collection.
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_size);
            }

            // PRIORITY 4: Poll underlying stream for new items.
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => {
                    // No more items available right now, check if we should emit due to timeout.
                    break;
                }
                Poll::Ready(Some(item)) => {
                    // New item available - add to current batch.
                    *this.current_batch_bytes =
                        this.current_batch_bytes.saturating_add(item.size_hint());
                    this.items.push(item);

                    // If byte budget is reached, emit immediately.
                    let max_batch_bytes_reached = *this.current_batch_bytes
                        >= this.cached_batch_budget.current_batch_size_bytes();
                    if max_batch_bytes_reached {
                        *this.reset_timer = true;
                        *this.current_batch_bytes = 0;

                        return Poll::Ready(Some(std::mem::take(this.items)));
                    }

                    // Continue loop to collect more items or check other conditions.
                }
                Poll::Ready(None) => {
                    // Underlying stream finished.
                    // Return final batch if we have items, otherwise signal completion.
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true;
                        *this.current_batch_bytes = 0;

                        Some(std::mem::take(this.items))
                    };

                    // Mark stream as permanently ended. We do this, since we might be returning
                    // the last accumulated data, and the future will be polled again and we need
                    // to return `Poll::Ready(None)`.
                    *this.inner_stream_ended = true;

                    return Poll::Ready(last);
                }
            }
        }

        // PRIORITY 5: Time-based emission check
        // If we have items and the timeout has expired, emit the current batch
        // This provides latency bounds to prevent indefinite delays in low-volume scenarios.
        if !this.items.is_empty()
            && let Some(deadline) = this.deadline.as_pin_mut()
        {
            // Check if timeout has elapsed.
            ready!(deadline.poll(cx));
            // Schedule timer reset for next batch.
            *this.reset_timer = true;
            *this.current_batch_bytes = 0;

            return Poll::Ready(Some(std::mem::take(this.items)));
        }

        // No conditions met for batch emission, wait for more items or timeout.
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::batch_budget::BatchBudgetController;
    use crate::concurrency::memory_monitor::MemoryMonitor;
    use crate::types::SizeHint;
    use core::task::Poll;
    use futures::StreamExt;
    use futures::future::poll_fn;
    use pin_project_lite::pin_project;

    pin_project! {
        struct TwoThenPending {
            emitted: usize,
        }
    }

    impl TwoThenPending {
        fn new() -> Self {
            Self { emitted: 0 }
        }
    }

    impl Stream for TwoThenPending {
        type Item = i32;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.emitted {
                0 => {
                    self.emitted = 1;
                    Poll::Ready(Some(1))
                }
                1 => {
                    self.emitted = 2;
                    Poll::Ready(Some(2))
                }
                _ => Poll::Pending,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct SizedToken {
        value: i32,
        bytes: usize,
    }

    impl SizeHint for SizedToken {
        fn size_hint(&self) -> usize {
            self.bytes
        }
    }

    impl SizeHint for i32 {
        fn size_hint(&self) -> usize {
            size_of::<Self>()
        }
    }

    /// Returns a cached budget with a very high limit so byte-based flushes do not interfere.
    fn test_cached_budget(memory_monitor: &MemoryMonitor) -> CachedBatchBudget {
        memory_monitor.set_total_memory_bytes_for_test(10_000_000_000);

        BatchBudgetController::new(1, memory_monitor.clone(), 0.2).cached()
    }

    /// Returns a cached budget and the computed byte limit for assertions in byte-based tests.
    fn test_cached_budget_with_limit(memory_monitor: &MemoryMonitor) -> (CachedBatchBudget, usize) {
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        let mut cached_budget = BatchBudgetController::new(1, memory_monitor.clone(), 0.2).cached();
        let limit = cached_budget.current_batch_size_bytes();

        (cached_budget, limit)
    }

    // BackpressureStream tests.
    #[tokio::test]
    async fn backpressure_stream_pauses_while_blocked_then_resumes() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_backpressure_active_for_test(true);
        let memory_sub = memory.subscribe();

        // When backpressure is active, wrapped stream stays pending even if it has data.
        let mut stream = Box::pin(BackpressureStream::wrap(
            futures::stream::iter(vec![10]),
            memory_sub,
        ));

        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending while backpressure is active"),
        })
        .await;

        memory.set_backpressure_active_for_test(false);

        // Once unblocked, wrapper yields underlying item.
        let item = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(item, Some(10));
    }

    #[tokio::test]
    async fn backpressure_stream_uses_current_state_when_no_new_update() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();
        let mut stream = Box::pin(BackpressureStream::wrap(
            futures::stream::iter(vec![11]),
            memory_sub,
        ));

        // Activate backpressure after subscribe and before next poll.
        memory.set_backpressure_active_for_test(true);

        // Even if the updates stream is pending, wrapper falls back to current state.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending based on current backpressure state"),
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn backpressure_stream_first_poll_blocked_update_waits_until_unblock() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();
        let stream = BackpressureStream::wrap(futures::stream::iter(vec![14]), memory_sub);

        // Set blocked before first poll so the first poll observes an immediate update.
        memory.set_backpressure_active_for_test(true);

        let unblocker = {
            let memory = memory.clone();
            tokio::spawn(async move {
                // We wait a second before unblocking, to avoid the stream being polled with `false`
                // already there.
                tokio::time::sleep(Duration::from_secs(1)).await;
                memory.set_backpressure_active_for_test(false);
            })
        };

        let waiter = tokio::spawn(async move {
            futures::pin_mut!(stream);
            stream.next().await
        });

        // This is a scheduling hint, not a strict guarantee, but it gives the spawned waiter a
        // chance to poll once before we advance virtual time.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let result = waiter.await.unwrap();

        assert_eq!(result, Some(14));
        unblocker.await.unwrap();
    }

    // BatchBackpressureStream tests.
    #[tokio::test]
    async fn flushes_buffered_items_immediately_when_memory_blocks() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            TwoThenPending::new(),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        ));

        // First the first poll, we are pending since we are waiting for 10 elements but the stream
        // only yields 2 and then suspends.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending"),
        })
        .await;

        memory.set_backpressure_active_for_test(true);

        // Now backpressure is active, so the system is expected to flush its existing state.
        let batch = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(batch, Some(vec![1, 2]));
    }

    #[tokio::test]
    async fn returns_pending_while_blocked_then_resumes_after_unblock() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_backpressure_active_for_test(true);
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 1,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            futures::stream::iter(vec![1]),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        ));

        // Memory is full, so we block any poll.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending while backpressure is active"),
        })
        .await;

        memory.set_backpressure_active_for_test(false);

        // Memory is now back, so we should get the batch of 1 element.
        let batch = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(batch, Some(vec![1]));
    }

    #[tokio::test(start_paused = true)]
    async fn batch_stream_first_poll_blocked_update_waits_until_unblock() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 1,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let stream = BatchBackpressureStream::wrap(
            futures::stream::iter(vec![2]),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        );

        // Set blocked before first poll so the first poll observes an immediate update.
        memory.set_backpressure_active_for_test(true);

        let unblocker = {
            let memory = memory.clone();
            tokio::spawn(async move {
                // We wait a second before unblocking, to avoid the stream being polled with `false`
                // already there.
                tokio::time::sleep(Duration::from_secs(1)).await;
                memory.set_backpressure_active_for_test(false);
            })
        };

        let waiter = tokio::spawn(async move {
            futures::pin_mut!(stream);
            stream.next().await
        });

        // This is a scheduling hint, not a strict guarantee, but it gives the spawned waiter a
        // chance to poll once before we advance virtual time.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let result = waiter.await.unwrap();

        assert_eq!(result, Some(vec![2]));
        unblocker.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_flush_when_only_max_size_is_reached() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 2,
            max_fill_ms: 100,
            memory_budget_ratio: 0.2,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            TwoThenPending::new(),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        ));

        // Even though max_size is 2 and stream emits exactly two items, we do not flush by item count.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending before timeout when only item count threshold is met"),
        })
        .await;

        tokio::time::advance(Duration::from_millis(120)).await;
        let flushed = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(flushed, Some(vec![1, 2]));
    }

    #[tokio::test]
    async fn flushes_when_batch_reaches_max_bytes_before_max_items() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let (cached_budget, byte_limit) = test_cached_budget_with_limit(&memory);
        let byte_size = (byte_limit / 2).max(1);
        let items = vec![
            SizedToken {
                value: 1,
                bytes: byte_size,
            },
            SizedToken {
                value: 2,
                bytes: byte_size,
            },
            SizedToken {
                value: 3,
                bytes: byte_size,
            },
        ];
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            futures::stream::iter(items.clone()),
            batch_config,
            memory_sub,
            cached_budget,
        ));

        let first = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(first, Some(items[..2].to_vec()));

        let second = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(second, Some(items[2..].to_vec()));
    }

    #[tokio::test]
    async fn max_bytes_uses_cumulative_size_hint_summation() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let (cached_budget, byte_limit) = test_cached_budget_with_limit(&memory);
        let first = (byte_limit / 4).max(1);
        let second = (byte_limit / 4).max(1);
        let third = byte_limit
            .saturating_sub(first.saturating_add(second))
            .max(1);
        let items = vec![
            SizedToken {
                value: 1,
                bytes: first,
            },
            SizedToken {
                value: 2,
                bytes: second,
            },
            SizedToken {
                value: 3,
                bytes: third,
            },
        ];
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            futures::stream::iter(items.clone()),
            batch_config,
            memory_sub,
            cached_budget,
        ));

        let first = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(first, Some(items));
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_buffered_items_when_timeout_elapses() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 100,
            memory_budget_ratio: 0.2,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            TwoThenPending::new(),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        ));

        // The stream has buffered items but not enough to reach max_size, so it should wait.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending before timeout"),
        })
        .await;

        // Advancing past the deadline should trigger timeout-based flush to bound latency.
        tokio::time::advance(Duration::from_millis(120)).await;

        let flushed = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(flushed, Some(vec![1, 2]));
    }

    #[tokio::test]
    async fn emits_final_partial_batch_then_returns_none() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            futures::stream::iter(vec![7, 8]),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        ));

        // End-of-stream with buffered items must emit one final batch before completion.
        let last = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(last, Some(vec![7, 8]));

        // A subsequent poll must return None, proving the stream transitions to ended state.
        let done = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(done, None);
    }

    #[tokio::test]
    async fn returns_none_immediately_for_empty_inner_stream() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 10_000,
            memory_budget_ratio: 0.2,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            futures::stream::empty::<i32>(),
            batch_config,
            memory_sub,
            test_cached_budget(&memory),
        ));

        // Empty streams should complete immediately without emitting empty batches.
        let result = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(result, None);
    }
}
