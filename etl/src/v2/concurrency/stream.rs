use crate::v2::concurrency::shutdown::{ShutdownResult, ShutdownRx};
use config::shared::BatchConfig;
use core::pin::Pin;
use core::task::{Context, Poll};
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

    /// Returns a mutable reference to the underlying stream.
    ///
    /// This allows for direct manipulation of the inner stream if needed.
    pub fn get_inner_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<B, S: Stream<Item = B>> Stream for BatchStream<B, S> {
    type Item = ShutdownResult<Vec<S::Item>, Vec<S::Item>>;

    /// Polls the stream for the next batch of items.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(batch))` when a complete batch is available
    /// - `Poll::Ready(None)` when the stream has ended
    /// - `Poll::Pending` when more items are needed to form a batch
    ///
    /// The stream will emit a batch when:
    /// - The batch reaches maximum size
    /// - A timeout occurs
    /// - The stream is forcefully stopped
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            if *this.stream_stopped {
                return Poll::Ready(None);
            }

            // If the stream has been asked to stop, we mark the stream as stopped and return the
            // remaining elements, irrespectively of boundaries.
            if this.shutdown_rx.has_changed().unwrap_or(false) {
                info!("the stream has been forcefully stopped");
                *this.stream_stopped = true;

                // Even if we have no items, we return this result, since we signal that a shutdown
                // signal was received and the consumer side of the stream, can decide what to do.
                return Poll::Ready(Some(ShutdownResult::Shutdown(std::mem::take(this.items))));
            }

            if *this.reset_timer {
                this.deadline
                    .set(Some(tokio::time::sleep(Duration::from_millis(
                        this.batch_config.max_fill_ms,
                    ))));
                *this.reset_timer = false;
            }

            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_size);
            }

            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(item)) => {
                    this.items.push(item);

                    // If we reached the `max_batch_size` we want to return the batch and reset the
                    // timer.
                    if this.items.len() >= this.batch_config.max_size {
                        *this.reset_timer = true;
                        return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
                    }
                }
                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true;
                        Some(ShutdownResult::Ok(std::mem::take(this.items)))
                    };

                    *this.inner_stream_ended = true;

                    return Poll::Ready(last);
                }
            }
        }

        // If there are items, we want to check the deadline, if it's met, we return the batch
        // we currently have in memory, otherwise, we return.
        if !this.items.is_empty() {
            if let Some(deadline) = this.deadline.as_pin_mut() {
                ready!(deadline.poll(cx));
                *this.reset_timer = true;

                return Poll::Ready(Some(ShutdownResult::Ok(std::mem::take(this.items))));
            }
        }

        Poll::Pending
    }
}
