use std::sync::Arc;

use etl::{
    data::TableRow,
    destination::{
        Destination, DestinationWriteStatus, DropTableForCopyResult, PipelineDestination,
        WriteEventsResult, WriteTableRowsResult,
    },
    error::EtlResult,
    event::Event,
    schema::ReplicatedTableSchema,
    test_utils::notify::TimedNotify,
};
use tokio::sync::{Mutex, Notify};

/// State used by [`DeferredCopyDestination`] to control copy durability.
#[derive(Default)]
struct DeferredCopyDestinationState {
    /// Number of nonempty copy writes received.
    nonempty_writes: usize,
    /// Rows retained after the first copy write is accepted.
    accepted_rows: Vec<TableRow>,
    /// Held terminal durability barrier result.
    barrier_result: Option<WriteTableRowsResult>,
}

/// Destination test double that defers the first copy write's durability.
#[derive(Clone)]
pub(crate) struct DeferredCopyDestination<D> {
    /// Immediate destination used for writes after the first copy batch.
    inner: D,
    /// Shared durability-control state.
    state: Arc<Mutex<DeferredCopyDestinationState>>,
    /// Notification emitted after the terminal barrier is held.
    barrier_reached: Arc<Notify>,
}

impl<D> DeferredCopyDestination<D> {
    /// Wraps an immediate destination with deferred copy durability behavior.
    pub(crate) fn wrap(inner: D) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(DeferredCopyDestinationState::default())),
            barrier_reached: Arc::new(Notify::new()),
        }
    }

    /// Returns a notification for the next terminal durability barrier.
    pub(crate) fn notify_on_barrier(&self) -> TimedNotify {
        TimedNotify::new(Arc::clone(&self.barrier_reached))
    }

    /// Returns the number of nonempty copy writes received.
    pub(crate) async fn nonempty_writes(&self) -> usize {
        self.state.lock().await.nonempty_writes
    }

    /// Completes the held terminal durability barrier with `status`.
    pub(crate) async fn complete_barrier(&self, status: DestinationWriteStatus) {
        let barrier_result = self
            .state
            .lock()
            .await
            .barrier_result
            .take()
            .expect("terminal copy durability barrier should be held");
        barrier_result.send(Ok(status));
    }
}

impl<D> Destination for DeferredCopyDestination<D>
where
    D: PipelineDestination,
{
    fn name() -> &'static str {
        "deferred_copy"
    }

    async fn startup(&self) -> EtlResult<()> {
        self.inner.startup().await
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.inner.shutdown().await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.inner.drop_table_for_copy(replicated_table_schema, async_result).await
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        mut table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            // Hold the terminal empty write so the test can inspect table state
            // before durability is confirmed.
            let mut state = self.state.lock().await;
            assert!(
                state.barrier_result.is_none(),
                "only one terminal copy durability barrier should be pending"
            );
            state.barrier_result = Some(async_result);
            drop(state);

            self.barrier_reached.notify_one();

            return Ok(());
        }

        let table_rows = {
            let mut state = self.state.lock().await;
            state.nonempty_writes += 1;

            if state.nonempty_writes == 1 {
                // Take ownership of the first batch without making it durable.
                state.accepted_rows = table_rows;
                None
            } else {
                // Make the accepted rows durable with a later batch. ETL must
                // still remember that the terminal barrier is required.
                state.accepted_rows.append(&mut table_rows);
                Some(std::mem::take(&mut state.accepted_rows))
            }
        };

        let Some(table_rows) = table_rows else {
            async_result.send(Ok(DestinationWriteStatus::Accepted));

            return Ok(());
        };

        self.inner.write_table_rows(replicated_table_schema, table_rows, async_result).await
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.inner.write_events(events, async_result).await
    }
}
