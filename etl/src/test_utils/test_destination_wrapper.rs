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
use tracing::info;

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
        event::{EventCondition, check_all_events_count, check_events_count, deduplicate_events},
        notify::TimedNotify,
    },
    types::{Event, EventType, TableRow},
};

type EventCheckFn = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type TableRowCheckFn = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;
type CombinedCheckFn =
    Box<dyn Fn(&[Event], &HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

type EventConditionEntry = (String, EventCheckFn, Arc<Notify>);
type TableRowConditionEntry = (String, TableRowCheckFn, Arc<Notify>);
type CombinedConditionEntry = (String, CombinedCheckFn, Arc<Notify>);

struct Inner<D> {
    wrapped_destination: D,
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    truncated_tables: HashSet<TableId>,
    event_conditions: Vec<EventConditionEntry>,
    table_row_conditions: Vec<TableRowConditionEntry>,
    combined_conditions: Vec<CombinedConditionEntry>,
    write_table_rows_called: u64,
    shutdown_called: bool,
}

impl<D> Inner<D> {
    fn check_conditions(&mut self) {
        // Check event conditions
        let events = self.events.clone();
        let total_table_row_count = total_table_rows(&self.table_rows);
        self.event_conditions.retain(|(description, condition, notify)| {
            let should_retain = !condition(&events);
            if !should_retain {
                info!(
                    description,
                    events_len = events.len(),
                    total_table_rows = total_table_row_count,
                    "destination event wait satisfied",
                );
                notify.notify_one();
            }
            should_retain
        });

        // Check table row conditions
        let table_rows = self.table_rows.clone();
        self.table_row_conditions.retain(|(description, condition, notify)| {
            let should_retain = !condition(&table_rows);
            if !should_retain {
                info!(
                    description,
                    total_table_rows = total_table_rows(&table_rows),
                    "destination table row wait satisfied",
                );
                notify.notify_one();
            }
            should_retain
        });

        // Check combined conditions
        let events = self.events.clone();
        let table_rows = self.table_rows.clone();
        self.combined_conditions.retain(|(description, condition, notify)| {
            let should_retain = !condition(&events, &table_rows);
            if !should_retain {
                info!(
                    description,
                    events_len = events.len(),
                    total_table_rows = total_table_rows(&table_rows),
                    "destination combined wait satisfied",
                );
                notify.notify_one();
            }
            should_retain
        });
    }
}

/// Test wrapper for [`Destination`] implementations that tracks all operations.
///
/// [`TestDestinationWrapper`] wraps any destination implementation and records
/// all method calls and data flowing through it. This enables test assertions
/// on the behavior of ETL pipelines without requiring complex destination
/// setup.
///
/// The wrapper supports waiting for specific conditions to be met, making it
/// ideal for testing asynchronous ETL operations with deterministic assertions.
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
            truncated_tables: HashSet::new(),
            event_conditions: Vec::new(),
            table_row_conditions: Vec::new(),
            combined_conditions: Vec::new(),
            write_table_rows_called: 0,
            shutdown_called: false,
        };

        Self { inner: Arc::new(RwLock::new(inner)) }
    }

    /// Get all table rows that have been written
    pub async fn get_table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    /// Get all events that have been written
    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
    }

    /// Get all events that have been written, de-duplicated by full event
    /// equality.
    pub async fn get_events_deduped(&self) -> Vec<Event> {
        let events = self.inner.read().await.events.clone();
        deduplicate_events(&events)
    }

    /// Registers a notification that fires when events match a specific
    /// condition after this method is called.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the condition is not met. This prevents tests
    /// from hanging indefinitely.
    pub async fn notify_on_events<F>(&self, condition: F) -> TimedNotify
    where
        F: Fn(&[Event]) -> bool + Send + Sync + 'static,
    {
        self.notify_on_events_with_description("custom event condition", condition).await
    }

    /// Registers a notification that fires when events match a specific
    /// condition after this method is called.
    async fn notify_on_events_with_description<F>(
        &self,
        description: impl Into<String>,
        condition: F,
    ) -> TimedNotify
    where
        F: Fn(&[Event]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let description = description.into();
        let mut inner = self.inner.write().await;
        info!(
            description,
            current_events_len = inner.events.len(),
            current_total_table_rows = total_table_rows(&inner.table_rows),
            "registered destination event wait",
        );
        inner.event_conditions.push((description.clone(), Box::new(condition), notify.clone()));

        TimedNotify::with_description(
            notify,
            crate::test_utils::notify::DEFAULT_NOTIFY_TIMEOUT,
            description,
        )
    }

    /// Registers a notification that fires when a specific number of events of
    /// given types are received.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected event count is not reached. This
    /// prevents tests from hanging indefinitely.
    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> TimedNotify {
        let description = format!("event counts {:?}", conditions);
        self.notify_on_events_with_description(description, move |events| {
            check_events_count(events, conditions.clone())
        })
        .await
    }

    /// Registers a notification that fires when a specific number of events of
    /// given types are received after de-duplicating.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected event count is not reached. This
    /// prevents tests from hanging indefinitely.
    pub async fn wait_for_events_count_deduped(
        &self,
        conditions: Vec<(EventType, u64)>,
    ) -> TimedNotify {
        let description = format!("deduped event counts {:?}", conditions);
        self.notify_on_events_with_description(description, move |events| {
            let deduped = deduplicate_events(events);
            check_events_count(&deduped, conditions.clone())
        })
        .await
    }

    /// Registers a notification that fires when event conditions are met after
    /// this method is called.
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
        let description = format!("all event conditions {:?}", conditions);
        let mut inner = self.inner.write().await;
        info!(
            description,
            current_events_len = inner.events.len(),
            current_total_table_rows = total_table_rows(&inner.table_rows),
            "registered destination combined wait",
        );

        let condition: CombinedCheckFn = Box::new(move |events, table_rows| {
            check_all_events_count(events, table_rows, conditions.clone())
        });

        inner.combined_conditions.push((description.clone(), condition, notify.clone()));

        TimedNotify::with_description(
            notify,
            crate::test_utils::notify::DEFAULT_NOTIFY_TIMEOUT,
            description,
        )
    }

    pub async fn clear_table_rows(&self) {
        let mut inner = self.inner.write().await;
        inner.table_rows.clear();
    }

    pub async fn clear_events(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
    }

    pub async fn was_table_truncated(&self, table_id: TableId) -> bool {
        self.inner.read().await.truncated_tables.contains(&table_id)
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

        info!(
            table_id = %table_id,
            remaining_events_len = inner.events.len(),
            remaining_total_table_rows = total_table_rows(&inner.table_rows),
            "destination truncate recorded",
        );

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
            let row_count = table_rows.len();
            let mut inner = self.inner.write().await;
            if should_record_table_rows {
                inner.table_rows.entry(table_id).or_default().extend(table_rows);
            }

            info!(
                table_id = %table_id,
                row_count,
                recorded = should_record_table_rows,
                write_table_rows_called = inner.write_table_rows_called,
                total_table_rows = total_table_rows(&inner.table_rows),
                "destination table rows completed",
            );
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
        let inner = self.inner.clone();
        let event_batch_summary = summarize_events(&events);
        let event_count = events.len();
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

                info!(
                    event_count,
                    event_batch_summary,
                    recorded = should_record_events,
                    total_events_len = inner.events.len(),
                    total_table_rows = total_table_rows(&inner.table_rows),
                    "destination events completed",
                );
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
            info!(
                total_events_len = inner.events.len(),
                total_table_rows = total_table_rows(&inner.table_rows),
                "destination shutdown recorded",
            );
        }

        result
    }
}

fn total_table_rows(table_rows: &HashMap<TableId, Vec<TableRow>>) -> usize {
    table_rows.values().map(Vec::len).sum()
}

fn summarize_events(events: &[Event]) -> String {
    let mut counts = HashMap::new();
    for event in events {
        let key = match event {
            Event::Begin(_) => "begin",
            Event::Commit(_) => "commit",
            Event::Relation(_) => "relation",
            Event::Insert(_) => "insert",
            Event::Update(_) => "update",
            Event::Delete(_) => "delete",
            Event::Truncate(_) => "truncate",
            Event::Unsupported => "unsupported",
        };
        *counts.entry(key).or_insert(0usize) += 1;
    }

    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_unstable_by_key(|(name, _)| *name);

    counts.into_iter().map(|(name, count)| format!("{name}={count}")).collect::<Vec<_>>().join(",")
}
