use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

use std::time::Instant;

use crate::conversions::arrow::record_batch_to_table_rows;
use crate::destination::Destination;
use crate::destination::async_result::{
    ApplyLoopAsyncResultMetadata, DispatchMetrics, TruncateTableResult, WriteSnapshotBatchResult,
    WriteStreamBatchesResult,
};
use crate::error::EtlResult;
use crate::test_utils::event::{check_all_events_count, check_events_count, deduplicate_events};
use crate::test_utils::notify::TimedNotify;
use crate::types::{
    ChangeKind, DeleteEvent, Event, EventType, InsertEvent, RowImage, StreamBatch, TableArrowBatch,
    TableChangeSet, TableRow, TruncateBatch, UpdateEvent,
};
use tokio_postgres::types::PgLsn;

type EventCondition = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type TableRowCondition = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;
type CombinedCondition =
    Box<dyn Fn(&[Event], &HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

fn snapshot_batch_to_table_rows(batch: &TableArrowBatch) -> Vec<TableRow> {
    record_batch_to_table_rows(&batch.batch)
}

fn table_change_set_to_events(change_set: &TableChangeSet) -> Vec<Event> {
    let mut events = Vec::new();
    let mut index = 0;

    while index < change_set.groups.len() {
        let group = &change_set.groups[index];
        let rows = record_batch_to_table_rows(&group.rows.batch);

        match (group.change, group.row_image) {
            (ChangeKind::Insert, RowImage::New) => {
                for (row_index, row) in rows.into_iter().enumerate() {
                    let commit_lsn = PgLsn::from(group.commit_lsns.values()[row_index]);
                    let tx_ordinal = group.tx_ordinals.values()[row_index];
                    events.push(Event::Insert(InsertEvent {
                        start_lsn: commit_lsn,
                        commit_lsn,
                        tx_ordinal,
                        table_id: change_set.table_id,
                        table_row: row,
                    }));
                }
                index += 1;
            }
            (ChangeKind::Update, RowImage::Old { key_only }) => {
                if let Some(next_group) = change_set.groups.get(index + 1)
                    && next_group.change == ChangeKind::Update
                    && next_group.row_image == RowImage::New
                {
                    let new_rows = record_batch_to_table_rows(&next_group.rows.batch);
                    for row_index in 0..rows.len() {
                        let commit_lsn = PgLsn::from(group.commit_lsns.values()[row_index]);
                        let tx_ordinal = group.tx_ordinals.values()[row_index];
                        events.push(Event::Update(UpdateEvent {
                            start_lsn: commit_lsn,
                            commit_lsn,
                            tx_ordinal,
                            table_id: change_set.table_id,
                            table_row: new_rows[row_index].clone(),
                            old_table_row: Some((key_only, rows[row_index].clone())),
                        }));
                    }
                    index += 2;
                } else {
                    for (row_index, row) in rows.into_iter().enumerate() {
                        let commit_lsn = PgLsn::from(group.commit_lsns.values()[row_index]);
                        let tx_ordinal = group.tx_ordinals.values()[row_index];
                        events.push(Event::Delete(DeleteEvent {
                            start_lsn: commit_lsn,
                            commit_lsn,
                            tx_ordinal,
                            table_id: change_set.table_id,
                            old_table_row: Some((key_only, row)),
                        }));
                    }
                    index += 1;
                }
            }
            (ChangeKind::Update, RowImage::New) => {
                for (row_index, row) in rows.into_iter().enumerate() {
                    let commit_lsn = PgLsn::from(group.commit_lsns.values()[row_index]);
                    let tx_ordinal = group.tx_ordinals.values()[row_index];
                    events.push(Event::Update(UpdateEvent {
                        start_lsn: commit_lsn,
                        commit_lsn,
                        tx_ordinal,
                        table_id: change_set.table_id,
                        table_row: row,
                        old_table_row: None,
                    }));
                }
                index += 1;
            }
            (ChangeKind::Delete, RowImage::Old { key_only }) => {
                for (row_index, row) in rows.into_iter().enumerate() {
                    let commit_lsn = PgLsn::from(group.commit_lsns.values()[row_index]);
                    let tx_ordinal = group.tx_ordinals.values()[row_index];
                    events.push(Event::Delete(DeleteEvent {
                        start_lsn: commit_lsn,
                        commit_lsn,
                        tx_ordinal,
                        table_id: change_set.table_id,
                        old_table_row: Some((key_only, row)),
                    }));
                }
                index += 1;
            }
            (ChangeKind::Delete, RowImage::New) | (ChangeKind::Insert, RowImage::Old { .. }) => {
                index += 1;
            }
        }
    }

    events
}

fn stream_batches_to_events(batches: &[StreamBatch]) -> Vec<Event> {
    let mut events = Vec::new();

    for batch in batches {
        match batch {
            StreamBatch::Changes(change_set) => {
                events.extend(table_change_set_to_events(change_set));
            }
            StreamBatch::Truncate(TruncateBatch {
                rel_ids,
                commit_lsn,
                tx_ordinal,
                options,
            }) => {
                events.push(Event::Truncate(crate::types::TruncateEvent {
                    start_lsn: *commit_lsn,
                    commit_lsn: *commit_lsn,
                    tx_ordinal: *tx_ordinal,
                    options: *options,
                    rel_ids: rel_ids.iter().map(|table_id| table_id.0).collect(),
                }));
            }
        }
    }

    events
}

struct Inner<D> {
    wrapped_destination: D,
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    event_conditions: Vec<(EventCondition, Arc<Notify>)>,
    table_row_conditions: Vec<(TableRowCondition, Arc<Notify>)>,
    combined_conditions: Vec<(CombinedCondition, Arc<Notify>)>,
    write_table_rows_called: u64,
    shutdown_called: bool,
}

impl<D> Inner<D> {
    fn check_conditions(&mut self) {
        // Check event conditions
        let events = self.events.clone();
        self.event_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&events);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        // Check table row conditions
        let table_rows = self.table_rows.clone();
        self.table_row_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&table_rows);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        // Check combined conditions
        let events = self.events.clone();
        let table_rows = self.table_rows.clone();
        self.combined_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&events, &table_rows);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });
    }
}

/// Test wrapper for [`Destination`] implementations that tracks all operations.
///
/// [`TestDestinationWrapper`] wraps any destination implementation and records all
/// method calls and data flowing through it. This enables test assertions on the
/// behavior of ETL pipelines without requiring complex destination setup.
///
/// The wrapper supports waiting for specific conditions to be met, making it ideal
/// for testing asynchronous ETL operations with deterministic assertions.
#[derive(Clone)]
pub struct TestDestinationWrapper<D> {
    inner: Arc<RwLock<Inner<D>>>,
}

impl<D: fmt::Debug> fmt::Debug for TestDestinationWrapper<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestDestinationWrapper")
            .field("wrapped_destination", &inner.wrapped_destination)
            .field("events", &inner.events)
            .field("table_rows", &inner.table_rows)
            .finish()
    }
}

impl<D> TestDestinationWrapper<D> {
    /// Creates a new test wrapper around any destination implementation.
    ///
    /// The wrapper will track all method calls and data operations performed
    /// on the destination, enabling comprehensive testing and verification.
    pub fn wrap(destination: D) -> Self {
        let inner = Inner {
            wrapped_destination: destination,
            events: Vec::new(),
            table_rows: HashMap::new(),
            event_conditions: Vec::new(),
            table_row_conditions: Vec::new(),
            combined_conditions: Vec::new(),
            write_table_rows_called: 0,
            shutdown_called: false,
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Get all table rows that have been written
    pub async fn get_table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    /// Get all events that have been written
    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
    }

    /// Get all events that have been written, de-duplicated by full event equality.
    pub async fn get_events_deduped(&self) -> Vec<Event> {
        let events = self.inner.read().await.events.clone();
        deduplicate_events(&events)
    }

    /// Registers a notification that fires when events match a specific condition.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// condition is not met. This prevents tests from hanging indefinitely.
    pub async fn notify_on_events<F>(&self, condition: F) -> TimedNotify
    where
        F: Fn(&[Event]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .event_conditions
            .push((Box::new(condition), notify.clone()));

        // Check conditions immediately in case they're already satisfied
        inner.check_conditions();

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when a specific number of events of given types are received.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// expected event count is not reached. This prevents tests from hanging indefinitely.
    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> TimedNotify {
        self.notify_on_events(move |events| check_events_count(events, conditions.clone()))
            .await
    }

    /// Registers a notification that fires when a specific number of events of given types are received after de-duplicating.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// expected event count is not reached. This prevents tests from hanging indefinitely.
    pub async fn wait_for_events_count_deduped(
        &self,
        conditions: Vec<(EventType, u64)>,
    ) -> TimedNotify {
        self.notify_on_events(move |events| {
            let deduped = deduplicate_events(events);
            check_events_count(&deduped, conditions.clone())
        })
        .await
    }

    /// Registers a notification that fires when a specific number of events are received,
    /// counting both insert events from streaming and table rows from the initial copy phase.
    ///
    /// This is useful for tests that need to verify all data was captured regardless of
    /// whether it arrived during table copy or streaming replication.
    ///
    /// Counts are aggregated across all tables for the specified event types.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after 30 seconds if the
    /// expected count is not reached. This prevents tests from hanging indefinitely.
    pub async fn wait_for_all_events(&self, conditions: Vec<(EventType, u64)>) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;

        let condition: CombinedCondition = Box::new(move |events, table_rows| {
            check_all_events_count(events, table_rows, conditions.clone())
        });

        inner.combined_conditions.push((condition, notify.clone()));

        // Check conditions immediately in case they're already satisfied.
        inner.check_conditions();

        TimedNotify::new(notify)
    }

    pub async fn clear_table_rows(&self) {
        let mut inner = self.inner.write().await;
        inner.table_rows.clear();
    }

    pub async fn clear_events(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
    }

    pub async fn write_table_rows_called(&self) -> u64 {
        self.inner.read().await.write_table_rows_called
    }

    /// Returns whether the shutdown method was called on the destination.
    pub async fn shutdown_called(&self) -> bool {
        self.inner.read().await.shutdown_called
    }
}

impl<D> Destination for TestDestinationWrapper<D>
where
    D: Destination + Send + Sync + Clone + 'static,
{
    fn name() -> &'static str {
        "wrapper"
    }

    async fn truncate_table(
        &self,
        table_id: TableId,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let (wrapped_truncate_result, pending_result) = TruncateTableResult::new(());
        destination
            .truncate_table(table_id, wrapped_truncate_result)
            .await?;

        let result = pending_result.await.into_result();

        let mut inner = self.inner.write().await;

        inner.table_rows.remove(&table_id);
        inner.events.retain_mut(|event| {
            let has_table_id = event.has_table_id(&table_id);
            if let Event::Truncate(event) = event
                && has_table_id
            {
                let Some(index) = event.rel_ids.iter().position(|&id| table_id.0 == id) else {
                    return true;
                };

                event.rel_ids.remove(index);
                if event.rel_ids.is_empty() {
                    return false;
                }

                return true;
            }

            !has_table_id
        });

        async_result.send(result);

        Ok(())
    }

    async fn write_snapshot_batch(
        &self,
        batch: TableArrowBatch,
        async_result: WriteSnapshotBatchResult<()>,
    ) -> EtlResult<()> {
        let destination = {
            let mut inner = self.inner.write().await;
            inner.write_table_rows_called += 1;
            inner.wrapped_destination.clone()
        };
        let table_id = batch.table_id;
        let table_rows = snapshot_batch_to_table_rows(&batch);

        let (wrapped_flush_result, pending_result) = WriteSnapshotBatchResult::new(());
        destination
            .write_snapshot_batch(batch, wrapped_flush_result)
            .await?;

        let result = pending_result.await.into_result();

        {
            let mut inner = self.inner.write().await;
            if result.is_ok() {
                inner
                    .table_rows
                    .entry(table_id)
                    .or_default()
                    .extend(table_rows);
            }

            inner.check_conditions();
        }

        async_result.send(result);

        Ok(())
    }

    async fn write_stream_batches(
        &self,
        batches: Vec<StreamBatch>,
        async_result: WriteStreamBatchesResult<()>,
    ) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };
        let events = stream_batches_to_events(&batches);

        let (wrapped_flush_result, pending_result) =
            WriteStreamBatchesResult::new(ApplyLoopAsyncResultMetadata {
                commit_end_lsn: None,
                metrics: DispatchMetrics {
                    items_count: events.len(),
                    dispatched_at: Instant::now(),
                },
            });
        destination
            .write_stream_batches(batches, wrapped_flush_result)
            .await?;

        // We spawn a task to handle the result, this way the wrapper behaves like a transparent
        // layer that doesn't block on the result of the inner destination, effectively exhibiting
        // the fully asynchronous behavior that a destination could have.
        //
        // For the other destination methods with async result this is not needed since the methods
        // on the outside block on the result right after calling the method, so it's not needed to
        // simulate asynchronous work to make the code continue and do something else in the
        // meanwhile.
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let result = pending_result.await.into_result();

            {
                let mut inner = inner.write().await;
                if result.is_ok() {
                    inner.events.extend(events);
                }

                inner.check_conditions();
            }

            async_result.send(result);
        });

        Ok(())
    }

    async fn shutdown(&self) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let result = destination.shutdown().await;

        {
            let mut inner = self.inner.write().await;
            inner.shutdown_called = true;
        }

        result
    }
}
