use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
    time::Instant,
};

use etl_postgres::types::{ReplicatedTableSchema, TableId};
use tokio::{
    runtime::Handle,
    sync::{Notify, RwLock},
};

use crate::{
    destination::{
        Destination,
        async_result::{
            ApplyLoopAsyncResultMetadata, DispatchMetrics, TruncateTableResult, WriteEventsResult,
            WriteTableRowsResult,
        },
    },
    error::EtlResult,
    test_utils::{
        event::{EventCondition, check_all_events_count, group_events_by_type},
        notify::TimedNotify,
    },
    types::{Event, EventType, TableRow},
};

type EventCheckFn = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type TableRowCheckFn = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;
type CombinedCheckFn =
    Box<dyn Fn(&[Event], &HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

struct Inner<D> {
    wrapped_destination: D,
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    truncated_tables: HashSet<TableId>,
    event_conditions: Vec<(EventCheckFn, Arc<Notify>)>,
    table_row_conditions: Vec<(TableRowCheckFn, Arc<Notify>)>,
    combined_conditions: Vec<(CombinedCheckFn, Arc<Notify>)>,
    write_table_rows_called: u64,
    shutdown_called: bool,
}

impl<D> Inner<D> {
    fn check_conditions(&mut self) {
        // Check event conditions.
        let events = self.events.clone();
        self.event_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&events);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        // Check table row conditions.
        let table_rows = self.table_rows.clone();
        self.table_row_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&table_rows);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        // Check combined conditions.
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
/// [`TestDestinationWrapper`] wraps any destination implementation and records
/// all rows, events, truncations, and shutdown calls flowing through it. This
/// enables test assertions on pipeline behavior without requiring complex
/// destination setup.
///
/// Notification helpers only observe writes that happen after registration.
/// Register the returned [`TimedNotify`] before starting the producer that is
/// expected to satisfy it.
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
    /// The wrapper forwards every operation to the wrapped destination and
    /// keeps an in-memory history for assertions.
    pub fn wrap(destination: D) -> Self {
        let inner = Inner {
            wrapped_destination: destination,
            events: Vec::new(),
            table_rows: HashMap::new(),
            truncated_tables: HashSet::new(),
            event_conditions: Vec::new(),
            table_row_conditions: Vec::new(),
            combined_conditions: Vec::new(),
            write_table_rows_called: 0,
            shutdown_called: false,
        };

        Self { inner: Arc::new(RwLock::new(inner)) }
    }

    /// Returns all table rows written through the wrapper.
    pub async fn get_table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    /// Returns all events written through the wrapper.
    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
    }

    /// Registers a notification that fires when future events match a
    /// condition.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the condition is not met. This prevents tests
    /// from hanging indefinitely.
    pub async fn notify_on_events<F>(&self, condition: F) -> TimedNotify
    where
        F: Fn(&[Event]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.event_conditions.push((Box::new(condition), Arc::clone(&notify)));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when future events exactly match the
    /// requested per-type counts.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected event count is not reached. This
    /// prevents tests from hanging indefinitely.
    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> TimedNotify {
        self.notify_on_events(move |events| {
            let grouped_events = group_events_by_type(events);

            conditions.iter().all(|(event_type, count)| {
                grouped_events.get(event_type).is_some_and(|inner| inner.len() == *count as usize)
            })
        })
        .await
    }

    /// Registers a notification that fires when future events and table rows
    /// satisfy all requested conditions.
    ///
    /// Supports two condition types:
    /// - [`EventCondition::Any`]: counts events across all tables
    /// - [`EventCondition::Table`]: counts events for a specific table only
    ///
    /// For insert events, both streaming events and table copy rows are
    /// counted.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected count is not reached.
    pub async fn wait_for_all_events(&self, conditions: Vec<EventCondition>) -> TimedNotify {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;

        let condition: CombinedCheckFn = Box::new(move |events, table_rows| {
            check_all_events_count(events, table_rows, conditions.clone())
        });

        inner.combined_conditions.push((condition, Arc::clone(&notify)));

        TimedNotify::new(notify)
    }

    /// Clears the recorded table rows without touching the wrapped destination.
    pub async fn clear_table_rows(&self) {
        let mut inner = self.inner.write().await;
        inner.table_rows.clear();
    }

    /// Clears the recorded events without touching the wrapped destination.
    pub async fn clear_events(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
    }

    /// Returns whether the table was truncated through the wrapper.
    pub async fn was_table_truncated(&self, table_id: TableId) -> bool {
        self.inner.read().await.truncated_tables.contains(&table_id)
    }

    /// Returns how many times [`Destination::write_table_rows`] was called.
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
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let (wrapped_truncate_result, pending_result) = TruncateTableResult::new(());
        destination.truncate_table(replicated_table_schema, wrapped_truncate_result).await?;

        // We send the result back before doing the internal checks for this utility, to
        // avoid checking before the apply loop received the result.
        let result = pending_result.await.into_result();
        async_result.send(result);

        let mut inner = self.inner.write().await;

        let table_id = replicated_table_schema.id();
        inner.truncated_tables.insert(table_id);
        inner.table_rows.remove(&table_id);
        inner.events.retain_mut(|event| {
            let has_table_id = event.has_table_id(&table_id);
            if let Event::Truncate(truncate_event) = event
                && has_table_id
            {
                truncate_event.truncated_tables.retain(|s| s.id() != table_id);
                if truncate_event.truncated_tables.is_empty() {
                    return false;
                }

                return true;
            }

            !has_table_id
        });

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let destination = {
            let mut inner = self.inner.write().await;
            inner.write_table_rows_called += 1;
            inner.wrapped_destination.clone()
        };

        let (wrapped_flush_result, pending_result) = WriteTableRowsResult::new(());
        destination
            .write_table_rows(replicated_table_schema, table_rows.clone(), wrapped_flush_result)
            .await?;

        // We send the result back before doing the internal checks for this utility, to
        // avoid checking before the apply loop received the result.
        let result = pending_result.await.into_result();
        let should_record_table_rows = result.is_ok();
        async_result.send(result);

        {
            let table_id = replicated_table_schema.id();
            let mut inner = self.inner.write().await;
            if should_record_table_rows {
                inner.table_rows.entry(table_id).or_default().extend(table_rows);
            }

            inner.check_conditions();
        }

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let (wrapped_flush_result, pending_result) =
            WriteEventsResult::new(ApplyLoopAsyncResultMetadata {
                commit_end_lsn: None,
                metrics: DispatchMetrics {
                    items_count: events.len(),
                    dispatched_at: Instant::now(),
                },
            });
        destination.write_events(events.clone(), wrapped_flush_result).await?;

        // We spawn a task to handle the result, this way the wrapper behaves like a
        // transparent layer that doesn't block on the result of the inner
        // destination, effectively exhibiting the fully asynchronous behavior
        // that a destination could have.
        //
        // For the other destination methods with async result this is not needed since
        // the methods on the outside block on the result right after calling
        // the method, so it's not needed to simulate asynchronous work to make
        // the code continue and do something else in the meanwhile.
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            // We send the result back before doing the internal checks for this utility, to
            // avoid checking before the apply loop received the result.
            let result = pending_result.await.into_result();
            let should_record_events = result.is_ok();
            async_result.send(result);

            {
                let mut inner = inner.write().await;
                if should_record_events {
                    inner.events.extend(events);
                }

                inner.check_conditions();
            }
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
