use core::pin::Pin;
use core::task::{Context, Poll};
use etl_config::shared::BatchConfig;
use futures::{Future, Stream, ready};
use pin_project_lite::pin_project;
use std::time::Duration;
use tracing::info;

use crate::concurrency::memory_monitor::MemoryMonitorSubscription;
use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};

pin_project! {
    /// A stream adapter that pauses polling when memory monitor reports pressure.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BackpressureStream<S: Stream> {
        #[pin]
        stream: S,
        memory_monitor: MemoryMonitorSubscription,
        paused_for_memory: bool,
    }
}

impl<S: Stream> BackpressureStream<S> {
    /// Creates a new [`BackpressureStream`] wrapping `stream`.
    pub fn wrap(stream: S, memory_monitor: MemoryMonitorSubscription) -> Self {
        Self {
            stream,
            memory_monitor,
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

        match this.memory_monitor.poll_update(cx) {
            Poll::Ready(Some(blocked)) => {
                *this.paused_for_memory = blocked;
            }
            Poll::Ready(None) => {
                *this.paused_for_memory = false;
            }
            Poll::Pending => {
                let currently_blocked = this.memory_monitor.current_blocked();
                if *this.paused_for_memory != currently_blocked {
                    *this.paused_for_memory = currently_blocked;
                }
            }
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
    /// - The batch reaches its maximum size
    /// - A timeout occurs
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BatchBackpressureStream<B, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        shutdown_rx: ShutdownRx,
        items: Vec<S::Item>,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
        stream_stopped: bool,
        memory_monitor: MemoryMonitorSubscription,
        paused_for_memory: bool,
    }
}

impl<B, S: Stream<Item = B>> BatchBackpressureStream<B, S> {
    /// Creates a new [`BatchBackpressureStream`].
    pub fn wrap(
        stream: S,
        batch_config: BatchConfig,
        shutdown_rx: ShutdownRx,
        memory_monitor: MemoryMonitorSubscription,
    ) -> Self {
        BatchBackpressureStream {
            stream,
            deadline: None,
            shutdown_rx,
            items: Vec::with_capacity(batch_config.max_size),
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            stream_stopped: false,
            memory_monitor,
            paused_for_memory: false,
        }
    }
}

impl<B, S: Stream<Item = B>> Stream for BatchBackpressureStream<B, S> {
    type Item = ShutdownResult<Vec<S::Item>, Vec<S::Item>>;

    /// Polls the stream for the next batch of items using a complex state machine.
    ///
    /// This method implements a batching algorithm that balances throughput
    /// and latency by collecting items into batches based on both size and time constraints.
    /// The polling state machine handles multiple concurrent conditions and ensures proper
    /// resource cleanup during shutdown scenarios.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // Fast path: if the inner stream has already ended, we're done.
        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            // Fast path: if we've been marked as stopped, terminate immediately.
            if *this.stream_stopped {
                return Poll::Ready(None);
            }

            // PRIORITY 1: Check for shutdown signal
            // Shutdown handling takes priority over all other operations to ensure
            // graceful termination. We return any accumulated items with shutdown indication.
            if this.shutdown_rx.has_changed().unwrap_or(false) {
                info!("stream forcefully stopped due to shutdown signal");

                // Mark stream as permanently stopped to prevent further polling.
                *this.stream_stopped = true;

                // Acknowledge that we've seen the shutdown signal to maintain watch semantics.
                this.shutdown_rx.mark_unchanged();

                // Return accumulated items (if any) with shutdown indication.
                // Even empty batches are returned to signal shutdown occurred.
                return Poll::Ready(Some(ShutdownResult::Shutdown(std::mem::take(this.items))));
            }

            // PRIORITY 2: Memory backpressure.
            // If memory is blocked and there are buffered items, flush immediately to avoid
            // accumulating more memory in this stream.
            let was_paused = *this.paused_for_memory;
            match this.memory_monitor.poll_update(cx) {
                Poll::Ready(Some(blocked)) => {
                    *this.paused_for_memory = blocked;
                }
                Poll::Ready(None) => {
                    *this.paused_for_memory = false;
                }
                Poll::Pending => {
                    let currently_blocked = this.memory_monitor.current_blocked();
                    if *this.paused_for_memory != currently_blocked {
                        *this.paused_for_memory = currently_blocked;
                    }
                }
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
                        "backpressure active, flushing buffered batch"
                    );
                    *this.reset_timer = true;

                    return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
                }

                return Poll::Pending;
            }

            // PRIORITY 3: Timer management
            // Reset the timeout timer when starting a new batch or after emitting a batch
            if *this.reset_timer {
                this.deadline
                    .set(Some(tokio::time::sleep(Duration::from_millis(
                        this.batch_config.max_fill_ms,
                    ))));
                *this.reset_timer = false;
            }

            // PRIORITY 4: Memory optimization
            // Pre-allocate batch capacity when starting to collect items
            // This avoids reallocations during batch collection.
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_size);
            }

            // PRIORITY 5: Poll underlying stream for new items
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => {
                    // No more items available right now, check if we should emit due to timeout.
                    break;
                }
                Poll::Ready(Some(item)) => {
                    // New item available - add to current batch.
                    this.items.push(item);

                    // SIZE-BASED EMISSION: If batch is full, emit immediately.
                    // This provides throughput optimization for high-volume streams.
                    if this.items.len() >= this.batch_config.max_size {
                        *this.reset_timer = true; // Schedule timer reset for next batch.
                        return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
                    }
                    // Continue loop to collect more items or check other conditions.
                }
                Poll::Ready(None) => {
                    // STREAM END: Underlying stream finished.
                    // Return final batch if we have items, otherwise signal completion.
                    let last = if this.items.is_empty() {
                        None // No final batch needed.
                    } else {
                        *this.reset_timer = true; // Clean up timer state.
                        Some(ShutdownResult::Ok(std::mem::take(this.items)))
                    };

                    *this.inner_stream_ended = true; // Mark stream as permanently ended.

                    return Poll::Ready(last);
                }
            }
        }

        // PRIORITY 6: Time-based emission check
        // If we have items and the timeout has expired, emit the current batch
        // This provides latency bounds to prevent indefinite delays in low-volume scenarios.
        if !this.items.is_empty()
            && let Some(deadline) = this.deadline.as_pin_mut()
        {
            // Check if timeout has elapsed (this will register waker if not ready).
            ready!(deadline.poll(cx));
            // Schedule timer reset for next batch.
            *this.reset_timer = true;

            return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
        }

        // No conditions met for batch emission, wait for more items or timeout.
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::memory_monitor::MemoryMonitor;
    use crate::concurrency::shutdown::create_shutdown_channel;
    use core::task::Poll;
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

    #[tokio::test]
    async fn flushes_buffered_items_immediately_when_memory_blocks() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();
        let (_, shutdown_rx) = create_shutdown_channel();

        let batch_config = BatchConfig {
            max_size: 10,
            max_fill_ms: 10_000,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            TwoThenPending::new(),
            batch_config,
            shutdown_rx,
            memory_sub,
        ));

        // First the first poll, we are pending since we are waiting for 10 elements but the stream
        // only yields 2 and then suspends.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending"),
        })
        .await;

        memory.set_blocked_for_test(true);

        // Now the memory is blocked, so the system is expected to flush its existing state.
        let batch = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        match batch {
            Some(ShutdownResult::Ok(items)) => assert_eq!(items, vec![1, 2]),
            _ => panic!("expected flushed batch"),
        }
    }

    #[tokio::test]
    async fn returns_pending_while_blocked_then_resumes_after_unblock() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_blocked_for_test(true);
        let memory_sub = memory.subscribe();
        let (_, shutdown_rx) = create_shutdown_channel();

        let batch_config = BatchConfig {
            max_size: 1,
            max_fill_ms: 10_000,
        };
        let mut stream = Box::pin(BatchBackpressureStream::wrap(
            futures::stream::iter(vec![1]),
            batch_config,
            shutdown_rx,
            memory_sub,
        ));

        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending while blocked"),
        })
        .await;

        memory.set_blocked_for_test(false);
        let batch = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;

        match batch {
            Some(ShutdownResult::Ok(items)) => assert_eq!(items, vec![1]),
            _ => panic!("expected resumed batch"),
        }
    }

    #[tokio::test]
    async fn backpressure_stream_pauses_while_blocked_then_resumes() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_blocked_for_test(true);
        let memory_sub = memory.subscribe();

        // Expectation: when blocked, wrapped stream stays pending even if it has data.
        let mut stream = Box::pin(BackpressureStream::wrap(
            futures::stream::iter(vec![10]),
            memory_sub,
        ));

        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending while blocked"),
        })
        .await;

        // Expectation: once unblocked, wrapper yields underlying item.
        memory.set_blocked_for_test(false);
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

        // Set blocked after subscribe and before next poll.
        // Expectation: even if `poll_update` is pending, wrapper falls back to current state.
        memory.set_blocked_for_test(true);
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending based on current blocked state"),
        })
        .await;
    }
}
