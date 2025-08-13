use crate::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use core::pin::Pin;
use core::task::{Context, Poll};
use etl_config::shared::BatchConfig;
use futures::{Future, Stream, ready};
use pin_project_lite::pin_project;
use std::time::Duration;
use tracing::info;

// Implementation adapted from:
//  https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs
pin_project! {
    /// A stream adapter that batches items based on size limits and timeouts.
    ///
    /// This stream collects items from the underlying stream into batches, emitting them when either:
    /// - The batch reaches its maximum size
    /// - A timeout occurs
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BatchStream<B, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        shutdown_rx: ShutdownRx,
        items: Vec<S::Item>,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
        stream_stopped: bool
    }
}

impl<B, S: Stream<Item = B>> BatchStream<B, S> {
    /// Creates a new [`BatchStream`] with the given configuration.
    ///
    /// The stream will batch items according to the provided `batch_config` and can be
    /// stopped using the `shutdown_rx` watch channel.
    pub fn wrap(stream: S, batch_config: BatchConfig, shutdown_rx: ShutdownRx) -> Self {
        BatchStream {
            stream,
            deadline: None,
            shutdown_rx,
            items: Vec::with_capacity(batch_config.max_size),
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            stream_stopped: false,
        }
    }
}

impl<B, S: Stream<Item = B>> Stream for BatchStream<B, S> {
    type Item = ShutdownResult<Vec<S::Item>, Vec<S::Item>>;

    /// Polls the stream for the next batch of items using a complex state machine.
    ///
    /// This method implements a batching algorithm that balances throughput
    /// and latency by collecting items into batches based on both size and time constraints.
    /// The polling state machine handles multiple concurrent conditions and ensures proper
    /// resource cleanup during shutdown scenarios.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        // Fast path: if the inner stream has already ended, we're done
        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            // Fast path: if we've been marked as stopped, terminate immediately
            if *this.stream_stopped {
                return Poll::Ready(None);
            }

            // PRIORITY 1: Check for shutdown signal
            // Shutdown handling takes priority over all other operations to ensure
            // graceful termination. We return any accumulated items with shutdown indication.
            if this.shutdown_rx.has_changed().unwrap_or(false) {
                info!("the stream has been forcefully stopped");

                // Mark stream as permanently stopped to prevent further polling
                *this.stream_stopped = true;

                // Acknowledge that we've seen the shutdown signal to maintain watch semantics
                this.shutdown_rx.mark_unchanged();

                // Return accumulated items (if any) with shutdown indication
                // Even empty batches are returned to signal shutdown occurred
                return Poll::Ready(Some(ShutdownResult::Shutdown(std::mem::take(this.items))));
            }

            // PRIORITY 2: Timer management
            // Reset the timeout timer when starting a new batch or after emitting a batch
            if *this.reset_timer {
                this.deadline
                    .set(Some(tokio::time::sleep(Duration::from_millis(
                        this.batch_config.max_fill_ms,
                    ))));
                *this.reset_timer = false;
            }

            // PRIORITY 3: Memory optimization
            // Pre-allocate batch capacity when starting to collect items
            // This avoids reallocations during batch collection
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_size);
            }

            // PRIORITY 4: Poll underlying stream for new items
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => {
                    // No more items available right now, check if we should emit due to timeout
                    break;
                }
                Poll::Ready(Some(item)) => {
                    // New item available - add to current batch
                    this.items.push(item);

                    // SIZE-BASED EMISSION: If batch is full, emit immediately
                    // This provides throughput optimization for high-volume streams
                    if this.items.len() >= this.batch_config.max_size {
                        *this.reset_timer = true; // Schedule timer reset for next batch
                        return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
                    }
                    // Continue loop to collect more items or check other conditions
                }
                Poll::Ready(None) => {
                    // STREAM END: Underlying stream finished
                    // Return final batch if we have items, otherwise signal completion
                    let last = if this.items.is_empty() {
                        None // No final batch needed
                    } else {
                        *this.reset_timer = true; // Clean up timer state
                        Some(ShutdownResult::Ok(std::mem::take(this.items)))
                    };

                    *this.inner_stream_ended = true; // Mark stream as permanently ended

                    return Poll::Ready(last);
                }
            }
        }

        // PRIORITY 5: Time-based emission check
        // If we have items and the timeout has expired, emit the current batch
        // This provides latency bounds to prevent indefinite delays in low-volume scenarios
        if !this.items.is_empty()
            && let Some(deadline) = this.deadline.as_pin_mut()
        {
            // Check if timeout has elapsed (this will register waker if not ready)
            ready!(deadline.poll(cx));

            *this.reset_timer = true; // Schedule timer reset for next batch

            return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
        }

        // No conditions met for batch emission - wait for more items or timeout
        Poll::Pending
    }
}
