use core::{
    pin::Pin,
    task::{Context, Poll},
};
use std::time::Duration;

use etl_config::shared::BatchConfig;
use futures::{Stream, ready};
use pin_project_lite::pin_project;
use tracing::info;

use crate::{
    data::SizeHint,
    runtime::{BatchMemoryGovernor, MemoryMonitorSubscription},
    schema::TableId,
};

/// Builds the stream id for a table sync worker's initial table-copy stream.
pub(crate) fn table_sync_worker_copy_stream_id(table_id: TableId) -> String {
    format!("table_sync_worker_copy_{}_stream", table_id.0)
}

/// Builds the stream id for a table sync worker's apply stream.
pub(crate) fn table_sync_worker_apply_stream_id(table_id: TableId) -> String {
    format!("table_sync_worker_apply_{}_stream", table_id.0)
}

/// Builds the stream id for the apply worker's apply stream.
pub(crate) fn apply_worker_apply_stream_id() -> String {
    "apply_worker_apply_stream".to_owned()
}

/// Takes the current batch and resets its decoded-size estimate.
fn take_batch<T>(items: &mut Vec<T>, size_hint_bytes: &mut usize) -> Vec<T> {
    *size_hint_bytes = 0;
    std::mem::take(items)
}

pin_project! {
    /// A stream adapter that pauses polling when memory monitor reports pressure.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub(crate) struct MemoryBackpressureStream<S: Stream> {
        // Source stream whose polling is controlled.
        #[pin]
        stream: S,
        // Stable identifier included in state-transition logs.
        stream_id: String,
        // Optional emergency backpressure subscription.
        memory_subscription: Option<MemoryMonitorSubscription>,
        // Last observed emergency backpressure state.
        paused_for_memory: bool,
    }
}

impl<S: Stream> MemoryBackpressureStream<S> {
    /// Creates a new [`MemoryBackpressureStream`] wrapping `stream`.
    pub(crate) fn wrap(
        stream: S,
        stream_id: impl Into<String>,
        memory_subscription: Option<MemoryMonitorSubscription>,
    ) -> Self {
        Self { stream, stream_id: stream_id.into(), memory_subscription, paused_for_memory: false }
    }

    /// Returns a pinned mutable reference to the wrapped stream.
    pub(crate) fn stream_mut(self: Pin<&mut Self>) -> Pin<&mut S> {
        self.project().stream
    }
}

impl<S: Stream> Stream for MemoryBackpressureStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let was_paused = *this.paused_for_memory;

        if let Some(memory_subscription) = this.memory_subscription.as_mut() {
            // Drain all currently queued watch updates and only stop at `Pending`.
            // Hitting `Pending` is important because it registers this task's waker for the
            // next backpressure transition, so returning `Pending` below cannot
            // miss a wakeup.
            loop {
                match Pin::new(&mut *memory_subscription).poll_next(cx) {
                    Poll::Ready(Some(backpressure_active)) => {
                        *this.paused_for_memory = backpressure_active;
                    }
                    Poll::Ready(None) => {
                        // If the was channel was dropped, we assume that memory is fine, to be
                        // resilient.
                        *this.paused_for_memory = false;

                        break;
                    }
                    Poll::Pending => {
                        // If the memory state didn't change, we just use the current state that is
                        // on the watch.
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
            info!(stream_id = %this.stream_id, "backpressure active, stream paused");
        } else if was_paused && !*this.paused_for_memory {
            info!(stream_id = %this.stream_id, "backpressure released, stream resumed");
        }

        if *this.paused_for_memory {
            return Poll::Pending;
        }

        this.stream.as_mut().poll_next(cx)
    }
}

pin_project! {
    /// A stream adapter that batches fallible items based on a size target and timeout.
    ///
    /// This stream buffers successful values and yields their decoded values. It
    /// avoids buffering `Result<B, E>` values and then allocating a second
    /// vector to extract successful entries.
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub(crate) struct MemoryBatchStream<B, E, S: Stream<Item = Result<B, E>>> {
        // Fallible source stream being batched.
        #[pin]
        stream: S,
        // Stable identifier included in state-transition logs.
        stream_id: String,
        // Deadline for the currently accumulating non-empty batch.
        #[pin]
        deadline: Option<tokio::time::Sleep>,
        // Successfully decoded items in the current batch.
        items: Vec<B>,
        // Decoded in-memory size estimate for the accumulating batch.
        batch_size_hint_bytes: usize,
        // Governor supplying the shared advisory batch-size target.
        batch_memory_governor: BatchMemoryGovernor,
        // Batch configuration supplying the fill timeout.
        batch_config: BatchConfig,
        // Whether the next item should start a new fill deadline.
        reset_timer: bool,
        // Whether the source stream has permanently ended.
        inner_stream_ended: bool,
        // Optional emergency backpressure subscription.
        memory_subscription: Option<MemoryMonitorSubscription>,
        // Last observed emergency backpressure state.
        paused_for_memory: bool,
    }
}

impl<B, E, S: Stream<Item = Result<B, E>>> MemoryBatchStream<B, E, S>
where
    B: SizeHint,
{
    /// Creates a new [`MemoryBatchStream`].
    pub(crate) fn wrap(
        stream: S,
        stream_id: impl Into<String>,
        batch_config: BatchConfig,
        memory_subscription: Option<MemoryMonitorSubscription>,
        batch_memory_governor: BatchMemoryGovernor,
    ) -> Self {
        Self {
            stream,
            stream_id: stream_id.into(),
            deadline: None,
            items: Vec::new(),
            batch_size_hint_bytes: 0,
            batch_memory_governor,
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
            memory_subscription,
            paused_for_memory: false,
        }
    }
}

impl<B, E, S: Stream<Item = Result<B, E>>> Stream for MemoryBatchStream<B, E, S>
where
    B: SizeHint,
{
    type Item = Result<Vec<B>, E>;

    /// Polls the stream for the next batch of successful values while
    /// preserving backpressure, timeout behavior, byte-target checks, and
    /// immediate error propagation.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Fast path: if the inner stream has already ended, we're done.
        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        // PRIORITY 1: Memory backpressure.
        // If memory backpressure is active and there are buffered items, flush
        // immediately to avoid accumulating more memory in this stream.
        //
        // The subscription stream is polled once per outer poll rather than
        // once per item. The hot loop below still reads the current watch value
        // after each item so a memory transition during a ready-drain can flush
        // the partial batch without paying watch-stream polling overhead for
        // every row.
        let was_paused = *this.paused_for_memory;
        if let Some(memory_subscription) = this.memory_subscription.as_mut() {
            // Drain all currently queued watch updates and only stop at `Pending`.
            // Hitting `Pending` is important because it registers this task's waker for the
            // next backpressure transition, so returning `Pending` below
            // cannot miss a wakeup.
            loop {
                match Pin::new(&mut *memory_subscription).poll_next(cx) {
                    Poll::Ready(Some(backpressure_active)) => {
                        *this.paused_for_memory = backpressure_active;
                    }
                    Poll::Ready(None) => {
                        // If the was channel was dropped, we assume that memory is fine, to be
                        // resilient.
                        *this.paused_for_memory = false;

                        break;
                    }
                    Poll::Pending => {
                        // If the memory state didn't change, we just use the current state that
                        // is on the watch.
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

        // We log the backpressure state changes, for better observability.
        if !was_paused && *this.paused_for_memory {
            info!(
                stream_id = %this.stream_id,
                "backpressure active, batch stream paused"
            );
        } else if was_paused && !*this.paused_for_memory {
            info!(
                stream_id = %this.stream_id,
                "backpressure released, batch stream resumed"
            );
        }

        // If the stream is paused due to memory backpressure, we want to return the
        // accumulated data.
        if *this.paused_for_memory {
            if !this.items.is_empty() {
                info!(
                    stream_id = %this.stream_id,
                    buffered_items = this.items.len(),
                    buffered_size_hint_bytes = *this.batch_size_hint_bytes,
                    "backpressure active, flushing buffered batch"
                );
                *this.reset_timer = true;

                // If we are paused for memory, we don't want to reallocate a batch of the same
                // size as before, to avoid increasing memory usage even more.
                let batch = take_batch(this.items, this.batch_size_hint_bytes);

                return Poll::Ready(Some(Ok(batch)));
            }

            return Poll::Pending;
        }

        // PRIORITY 2: Poll underlying stream for new items.
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => {
                    // Start the fill timer only when a batch becomes non-empty.
                    // Otherwise, an idle stream could expire the timer before
                    // the first item arrives and flush that item immediately.
                    if this.items.is_empty() && *this.reset_timer {
                        this.deadline.set(Some(tokio::time::sleep(Duration::from_millis(
                            this.batch_config.max_fill_ms,
                        ))));
                        *this.reset_timer = false;
                    }

                    // Add the new item to the batch and track its size.
                    let item_size_hint_bytes = item.size_hint();
                    *this.batch_size_hint_bytes =
                        this.batch_size_hint_bytes.saturating_add(item_size_hint_bytes);
                    this.items.push(item);

                    // If backpressure activated while the source was ready, flush the
                    // accumulated data before pausing source intake.
                    if let Some(memory_subscription) = this.memory_subscription.as_mut()
                        && memory_subscription.current_backpressure_active()
                    {
                        *this.paused_for_memory = true;
                        *this.reset_timer = true;

                        // If we are paused for memory, we don't want to reallocate a batch of the
                        // same size as before, to avoid increasing memory
                        // usage even more.
                        let batch = take_batch(this.items, this.batch_size_hint_bytes);

                        return Poll::Ready(Some(Ok(batch)));
                    }

                    // Consult the shared target after every decoded item. A PostgreSQL COPY
                    // stream can yield many ready rows during one outer poll, so checking only
                    // once per poll would delay adaptation to a changed cgroup limit or active
                    // batch-slot count.
                    if *this.batch_size_hint_bytes
                        >= this.batch_memory_governor.batch_size_target_bytes()
                    {
                        *this.reset_timer = true;

                        // COPY consumers wait for destination-result completion before polling
                        // again, so retaining another full outer-vector allocation
                        // here would violate the stream's one-batch-slot model.
                        let batch = take_batch(this.items, this.batch_size_hint_bytes);

                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                Poll::Ready(Some(Err(err))) => {
                    *this.inner_stream_ended = true;
                    *this.reset_timer = true;
                    this.deadline.set(None);

                    // Successful values buffered before the error will never be
                    // emitted, so release them immediately.
                    drop(std::mem::take(this.items));
                    *this.batch_size_hint_bytes = 0;

                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true;

                        // The inner stream is ending, so no future batch can
                        // reuse retained capacity. `mem::take` replaces the
                        // buffer with `Vec::new()`, which keeps zero capacity
                        // and avoids allocating a replacement vector.
                        let batch = take_batch(this.items, this.batch_size_hint_bytes);

                        Some(Ok(batch))
                    };

                    *this.inner_stream_ended = true;

                    return Poll::Ready(last);
                }
                Poll::Pending => {
                    // No more items available right now, check if we should emit due to timeout.
                    break;
                }
            }
        }

        // PRIORITY 3: Time-based emission check.
        if !this.items.is_empty()
            && let Some(deadline) = this.deadline.as_pin_mut()
        {
            ready!(deadline.poll(cx));
            *this.reset_timer = true;

            // Do not preallocate the next outer row buffer while this batch is
            // still awaiting destination-result completion.
            let batch = take_batch(this.items, this.batch_size_hint_bytes);

            return Poll::Ready(Some(Ok(batch)));
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use core::task::Poll;

    use futures::{StreamExt, future::poll_fn};
    use pin_project_lite::pin_project;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{
        data::SizeHint,
        runtime::{BatchMemoryGovernor, MemoryMonitor},
        schema::TableId,
    };

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
        type Item = Result<i32, &'static str>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.emitted {
                0 => {
                    self.emitted = 1;
                    Poll::Ready(Some(Ok(1)))
                }
                1 => {
                    self.emitted = 2;
                    Poll::Ready(Some(Ok(2)))
                }
                _ => Poll::Pending,
            }
        }
    }

    pin_project! {
        struct ActivatesBackpressureAfterFirst {
            emitted: usize,
            memory: MemoryMonitor,
        }
    }

    impl ActivatesBackpressureAfterFirst {
        fn new(memory: MemoryMonitor) -> Self {
            Self { emitted: 0, memory }
        }
    }

    impl Stream for ActivatesBackpressureAfterFirst {
        type Item = Result<i32, &'static str>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.emitted {
                0 => {
                    self.emitted = 1;
                    self.memory.set_backpressure_active_for_test(true);
                    Poll::Ready(Some(Ok(1)))
                }
                1 => {
                    self.emitted = 2;
                    Poll::Ready(Some(Ok(2)))
                }
                _ => Poll::Pending,
            }
        }
    }

    pin_project! {
        struct ShrinksMemoryBetweenReadyItems {
            emitted: usize,
            memory: MemoryMonitor,
        }
    }

    impl ShrinksMemoryBetweenReadyItems {
        fn new(memory: MemoryMonitor) -> Self {
            Self { emitted: 0, memory }
        }
    }

    impl Stream for ShrinksMemoryBetweenReadyItems {
        type Item = Result<SizedToken, &'static str>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let item = match self.emitted {
                0 => SizedToken { value: 1, bytes: 400 },
                1 => {
                    self.memory.set_total_memory_bytes_for_test(500);
                    // Publishing a new coherent memory snapshot must refresh the shared target
                    // while the outer poll continues draining ready rows.
                    SizedToken { value: 2, bytes: 100 }
                }
                2 => SizedToken { value: 3, bytes: 100 },
                3 => SizedToken { value: 4, bytes: 100 },
                _ => return Poll::Ready(None),
            };
            self.emitted += 1;

            Poll::Ready(Some(Ok(item)))
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

    /// Returns a governor with a very high target so byte-based flushes do
    /// not interfere.
    fn test_batch_memory_governor(memory_monitor: &MemoryMonitor) -> BatchMemoryGovernor {
        memory_monitor.set_total_memory_bytes_for_test(10_000_000_000);

        BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 8 * 1024 * 1024)
    }

    /// Returns a governor derived from a small synthetic memory capacity.
    fn test_small_batch_memory_governor(memory_monitor: &MemoryMonitor) -> BatchMemoryGovernor {
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, BatchConfig::DEFAULT_MAX_BYTES)
    }

    /// Returns a test batch config with the supplied fill timeout.
    fn test_batch_config(max_fill_ms: u64) -> BatchConfig {
        BatchConfig {
            max_fill_ms,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
            max_bytes: BatchConfig::DEFAULT_MAX_BYTES,
        }
    }

    #[test]
    fn builds_worker_stream_ids() {
        assert_eq!(
            table_sync_worker_copy_stream_id(TableId::new(123)),
            "table_sync_worker_copy_123_stream"
        );
        assert_eq!(
            table_sync_worker_apply_stream_id(TableId::new(456)),
            "table_sync_worker_apply_456_stream"
        );
        assert_eq!(apply_worker_apply_stream_id(), "apply_worker_apply_stream");
    }

    #[tokio::test]
    async fn backpressure_stream_pauses_while_blocked_then_resumes() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_backpressure_active_for_test(true);
        let memory_sub = memory.subscribe();

        // When backpressure is active, wrapped stream stays pending even if it has
        // data.
        let mut stream = Box::pin(MemoryBackpressureStream::wrap(
            futures::stream::iter(vec![10]),
            "test_stream",
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
        let mut stream = Box::pin(MemoryBackpressureStream::wrap(
            futures::stream::iter(vec![11]),
            "test_stream",
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
        let stream = MemoryBackpressureStream::wrap(
            futures::stream::iter(vec![14]),
            "test_stream",
            memory_sub,
        );

        // Set blocked before first poll so the first poll observes an immediate update.
        memory.set_backpressure_active_for_test(true);

        let unblocker = {
            let memory = memory.clone();
            tokio::spawn(async move {
                // We wait a second before unblocking, to avoid the stream being polled with
                // `false` already there.
                tokio::time::sleep(Duration::from_secs(1)).await;
                memory.set_backpressure_active_for_test(false);
            })
        };

        let waiter = tokio::spawn(async move {
            futures::pin_mut!(stream);
            stream.next().await
        });

        // This is a scheduling hint, not a strict guarantee, but it gives the spawned
        // waiter a chance to poll once before we advance virtual time.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let result = waiter.await.unwrap();

        assert_eq!(result, Some(14));
        unblocker.await.unwrap();
    }

    #[tokio::test]
    async fn flushes_buffered_items_immediately_when_memory_blocks() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            TwoThenPending::new(),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        ));

        // First the first poll, we are pending since we are waiting for 10 elements but
        // the stream only yields 2 and then suspends.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending"),
        })
        .await;

        memory.set_backpressure_active_for_test(true);

        // Now backpressure is active, so the system is expected to flush its existing
        // state.
        let batch = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(batch, Some(Ok(vec![1, 2])));
    }

    #[tokio::test(start_paused = true)]
    async fn pressure_flushes_the_batch_and_resumes() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_memory_snapshot_for_test(7_500, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory.clone(), 1.0, 10_000);
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            ReceiverStream::new(rx),
            "test_stream",
            test_batch_config(100),
            memory.subscribe(),
            governor.clone(),
        ));

        tx.send(Ok::<_, &'static str>(SizedToken { value: 1, bytes: 100 })).await.unwrap();
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected the partial batch to remain buffered"),
        })
        .await;

        memory.set_backpressure_active_for_test(true);
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch, vec![SizedToken { value: 1, bytes: 100 }]);

        // The stream remains paused until the memory monitor releases it.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected source intake to remain paused"),
        })
        .await;
        memory.set_backpressure_active_for_test(false);
        tx.send(Ok(SizedToken { value: 2, bytes: 100 })).await.unwrap();
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected the resumed batch to wait for its fill deadline"),
        })
        .await;
        tokio::time::advance(Duration::from_millis(120)).await;

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch, vec![SizedToken { value: 2, bytes: 100 }]);
    }

    #[tokio::test]
    async fn flushes_current_batch_when_memory_blocks_during_ready_drain() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        // This covers the ready-drain path specifically: the inner stream does
        // not return `Pending` between items, so memory pressure must be
        // observed from the current watch value inside the drain loop.
        let batch_config = test_batch_config(10_000);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            ActivatesBackpressureAfterFirst::new(memory.clone()),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        ));

        let batch = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(batch, Some(Ok(vec![1])));

        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending while backpressure is active"),
        })
        .await;
    }

    #[tokio::test]
    async fn returns_pending_while_blocked_then_resumes_after_unblock() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_backpressure_active_for_test(true);
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            futures::stream::iter(vec![Ok::<i32, &'static str>(1)]),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
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
        assert_eq!(batch, Some(Ok(vec![1])));
    }

    #[tokio::test(start_paused = true)]
    async fn batch_stream_first_poll_blocked_update_waits_until_unblock() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let stream = MemoryBatchStream::wrap(
            futures::stream::iter(vec![Ok::<i32, &'static str>(2)]),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        );

        // Set blocked before first poll so the first poll observes an immediate update.
        memory.set_backpressure_active_for_test(true);

        let unblocker = {
            let memory = memory.clone();
            tokio::spawn(async move {
                // We wait a second before unblocking, to avoid the stream being polled with
                // `false` already there.
                tokio::time::sleep(Duration::from_secs(1)).await;
                memory.set_backpressure_active_for_test(false);
            })
        };

        let waiter = tokio::spawn(async move {
            futures::pin_mut!(stream);
            stream.next().await
        });

        // This is a scheduling hint, not a strict guarantee, but it gives the spawned
        // waiter a chance to poll once before we advance virtual time.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let result = waiter.await.unwrap();

        assert_eq!(result, Some(Ok(vec![2])));
        unblocker.await.unwrap();
    }

    #[tokio::test]
    async fn flushes_when_batch_reaches_max_bytes_before_max_items() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let governor = test_small_batch_memory_governor(&memory);
        let byte_target = governor.batch_size_target_bytes();
        let byte_size = (byte_target / 2).max(1);
        let items = vec![
            SizedToken { value: 1, bytes: byte_size },
            SizedToken { value: 2, bytes: byte_size },
            SizedToken { value: 3, bytes: byte_size },
        ];
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            futures::stream::iter(items.clone().into_iter().map(Ok::<SizedToken, &'static str>)),
            "test_stream",
            batch_config,
            memory_sub,
            governor,
        ));

        let first = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(first, Some(Ok(items[..2].to_vec())));

        let second = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(second, Some(Ok(items[2..].to_vec())));
    }

    #[tokio::test]
    async fn max_bytes_uses_cumulative_size_hint_summation() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let governor = test_small_batch_memory_governor(&memory);
        let byte_target = governor.batch_size_target_bytes();
        let first = (byte_target / 4).max(1);
        let second = (byte_target / 4).max(1);
        let third = byte_target.saturating_sub(first.saturating_add(second)).max(1);
        let items = vec![
            SizedToken { value: 1, bytes: first },
            SizedToken { value: 2, bytes: second },
            SizedToken { value: 3, bytes: third },
        ];
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            futures::stream::iter(items.clone().into_iter().map(Ok::<SizedToken, &'static str>)),
            "test_stream",
            batch_config,
            memory_sub,
            governor,
        ));

        let first = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(first, Some(Ok(items)));
    }

    #[tokio::test]
    async fn refreshes_shared_target_between_ready_items() {
        let memory = MemoryMonitor::new_for_test();
        memory.set_total_memory_bytes_for_test(5_000);
        let governor = BatchMemoryGovernor::new(1, memory.clone(), 0.2, 10_000);
        let batch_config = test_batch_config(10_000);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            ShrinksMemoryBetweenReadyItems::new(memory),
            "test_stream",
            batch_config,
            None,
            governor,
        ));

        // The initial global target is 1,000 bytes. While the same outer poll
        // drains ready rows, total memory shrinks to 500 bytes. The refreshed
        // global target is 100 bytes, so the second row immediately flushes the
        // already oversized batch and the third row remains unread.
        let first = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(
            first,
            Some(Ok(vec![
                SizedToken { value: 1, bytes: 400 },
                SizedToken { value: 2, bytes: 100 },
            ]))
        );
    }

    #[tokio::test]
    async fn source_error_discards_buffered_items() {
        let memory = MemoryMonitor::new_for_test();
        let governor = test_batch_memory_governor(&memory);
        let input =
            futures::stream::iter([Ok(SizedToken { value: 1, bytes: 100 }), Err("source error")]);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            input,
            "test_stream",
            test_batch_config(10_000),
            None,
            governor,
        ));

        assert_eq!(stream.next().await, Some(Err("source error")));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn flushes_buffered_items_when_timeout_elapses() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(100);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            TwoThenPending::new(),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        ));

        // The stream has buffered items but not enough to reach its byte target, so it
        // should wait.
        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending before timeout"),
        })
        .await;

        // Advancing past the deadline should trigger timeout-based flush to bound
        // latency.
        tokio::time::advance(Duration::from_millis(120)).await;

        let flushed = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(flushed, Some(Ok(vec![1, 2])));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_starts_when_first_item_arrives_after_idle() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        // This guards against arming the fill timer while the stream is idle.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let batch_config = test_batch_config(100);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            ReceiverStream::new(rx),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        ));

        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending while idle"),
        })
        .await;

        tokio::time::advance(Duration::from_millis(120)).await;
        tx.send(Ok::<i32, &'static str>(1)).await.unwrap();

        poll_fn(|cx| match stream.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("expected pending before first item's timeout elapses"),
        })
        .await;

        tokio::time::advance(Duration::from_millis(120)).await;
        let flushed = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(flushed, Some(Ok(vec![1])));
    }

    #[tokio::test]
    async fn emits_final_partial_batch_then_returns_none() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            futures::stream::iter(vec![Ok::<i32, &'static str>(7), Ok(8)]),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        ));

        // End-of-stream with buffered items must emit one final batch before
        // completion.
        let last = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert_eq!(last, Some(Ok(vec![7, 8])));

        // A subsequent poll must return None, proving the stream transitions to ended
        // state.
        let done = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert!(done.is_none());
    }

    #[tokio::test]
    async fn returns_none_immediately_for_empty_inner_stream() {
        let memory = MemoryMonitor::new_for_test();
        let memory_sub = memory.subscribe();

        let batch_config = test_batch_config(10_000);
        let mut stream = Box::pin(MemoryBatchStream::wrap(
            futures::stream::empty::<Result<i32, &'static str>>(),
            "test_stream",
            batch_config,
            memory_sub,
            test_batch_memory_governor(&memory),
        ));

        // Empty streams should complete immediately without emitting empty batches.
        let result = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert!(result.is_none());
    }
}
