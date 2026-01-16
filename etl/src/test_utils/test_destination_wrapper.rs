use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

use crate::destination::Destination;
use crate::error::EtlResult;
use crate::test_utils::event::{check_all_events_count, check_events_count, deduplicate_events};
use crate::test_utils::notify::TimedNotify;
use crate::types::{Event, EventType, TableRow};

type EventCondition = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type TableRowCondition = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;
type CombinedCondition =
    Box<dyn Fn(&[Event], &HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

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
    async fn check_conditions(&mut self) {
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
        inner.check_conditions().await;

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
        inner.check_conditions().await;

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
    D: Destination + Send + Sync + Clone,
{
    fn name() -> &'static str {
        "wrapper"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let result = destination.truncate_table(table_id).await;

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

        result
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let destination = {
            let mut inner = self.inner.write().await;
            inner.write_table_rows_called += 1;
            inner.wrapped_destination.clone()
        };

        let result = destination
            .write_table_rows(table_id, table_rows.clone())
            .await;

        {
            let mut inner = self.inner.write().await;
            if result.is_ok() {
                inner
                    .table_rows
                    .entry(table_id)
                    .or_default()
                    .extend(table_rows);
            }

            inner.check_conditions().await;
        }

        result
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let result = destination.write_events(events.clone()).await;

        {
            let mut inner = self.inner.write().await;
            if result.is_ok() {
                inner.events.extend(events);
            }

            inner.check_conditions().await;
        }

        result
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
