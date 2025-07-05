use futures::{Future, Stream, ready};
use pin_project_lite::pin_project;
use tokio::time::{Sleep, sleep};

use super::{BatchBoundaryV1, BatchConfig};
use core::pin::Pin;
use core::task::{Context, Poll};
use tokio::sync::futures::Notified;
use tracing::info;

// Implementation adapted from https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs
pin_project! {
    /// Adapter stream which batches the items of the underlying stream when it
    /// reaches max_size or when a timeout expires. The underlying streams items
    /// must implement [`BatchBoundaryV1`]. A batch is guaranteed to end on an
    /// item which returns true from [`BatchBoundaryV1::is_last_in_batch`] unless the
    /// stream is forcefully stopped.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BatchTimeoutStream<'a, B: BatchBoundaryV1, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<Sleep>,
        #[pin]
        stream_stop: Notified<'a>,
        items: Vec<S::Item>,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
        stream_stopped: bool
    }
}

impl<'a, B: BatchBoundaryV1, S: Stream<Item = B>> BatchTimeoutStream<'a, B, S> {
    pub fn new(stream: S, batch_config: BatchConfig, stream_stop: Notified<'a>) -> Self {
        BatchTimeoutStream {
            stream,
            deadline: None,
            stream_stop,
            items: Vec::with_capacity(batch_config.max_batch_size),
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            stream_stopped: false,
        }
    }

    pub fn get_inner_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<'a, B: BatchBoundaryV1, S: Stream<Item = B>> Stream for BatchTimeoutStream<'a, B, S> {
    type Item = Vec<S::Item>;

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
            if this.stream_stop.as_mut().poll(cx).is_ready() {
                info!("the stream has been forcefully stopped");
                *this.stream_stopped = true;
                return if !this.items.is_empty() {
                    Poll::Ready(Some(std::mem::take(this.items)))
                } else {
                    Poll::Ready(None)
                };
            }

            if *this.reset_timer {
                this.deadline
                    .set(Some(sleep(this.batch_config.max_batch_fill_time)));
                *this.reset_timer = false;
            }
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_batch_size);
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => break,
                Poll::Ready(Some(item)) => {
                    let is_last_in_batch = item.is_last_in_batch();
                    this.items.push(item);
                    if this.items.len() >= this.batch_config.max_batch_size && is_last_in_batch {
                        *this.reset_timer = true;
                        return Poll::Ready(Some(std::mem::take(this.items)));
                    }
                }
                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true;
                        Some(std::mem::take(this.items))
                    };

                    *this.inner_stream_ended = true;

                    return Poll::Ready(last);
                }
            }
        }

        if !this.items.is_empty() {
            if let Some(deadline) = this.deadline.as_pin_mut() {
                ready!(deadline.poll(cx));
            }

            let last_item = this.items.last().expect("missing last item");
            if last_item.is_last_in_batch() {
                *this.reset_timer = true;
                return Poll::Ready(Some(std::mem::take(this.items)));
            }
        }

        Poll::Pending
    }
}
