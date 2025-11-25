use etl_config::shared::SchemaCreationMode;
use etl_postgres::types::TableId;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

use crate::destination::Destination;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::test_utils::event::{check_events_count, deduplicate_events};
use crate::types::{Event, EventType, TableRow, TableSchema};

type EventCondition = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type TableRowCondition = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

struct Inner<D> {
    wrapped_destination: D,
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    event_conditions: Vec<(EventCondition, Arc<Notify>)>,
    table_row_conditions: Vec<(TableRowCondition, Arc<Notify>)>,
    write_table_rows_called: u64,
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
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
            write_table_rows_called: 0,
            table_schemas: HashMap::new(),
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

    /// Wait for a specific condition on events
    pub async fn notify_on_events<F>(&self, condition: F) -> Arc<Notify>
    where
        F: Fn(&[Event]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .event_conditions
            .push((Box::new(condition), notify.clone()));

        notify
    }

    /// Wait for a specific number of events of given types.
    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> Arc<Notify> {
        self.notify_on_events(move |events| check_events_count(events, conditions.clone()))
            .await
    }

    /// Wait for a specific number of events of given types after de-duplicating by full event equality.
    pub async fn wait_for_events_count_deduped(
        &self,
        conditions: Vec<(EventType, u64)>,
    ) -> Arc<Notify> {
        self.notify_on_events(move |events| {
            let deduped = deduplicate_events(events);
            check_events_count(&deduped, conditions.clone())
        })
        .await
    }

    pub async fn clear_events(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
    }

    /// Clears the cached table schemas for this wrapped destination.
    pub async fn clear_table_schemas(&self) {
        let mut inner = self.inner.write().await;
        inner.table_schemas.clear();
    }

    pub async fn write_table_rows_called(&self) -> u64 {
        self.inner.read().await.write_table_rows_called
    }

    fn ensure_schema_exists(inner: &Inner<D>, table_id: &TableId) -> EtlResult<()> {
        if inner.table_schemas.contains_key(table_id) {
            return Ok(());
        }

        Err(etl_error!(
            ErrorKind::MissingTableSchema,
            "Table schema not found",
            format!("Table schema for table {table_id} was not created before writing data")
        ))
    }

    fn extract_table_id(event: &Event) -> Option<TableId> {
        match event {
            Event::Insert(e) => Some(e.table_id),
            Event::Update(e) => Some(e.table_id),
            Event::Delete(e) => Some(e.table_id),
            Event::Relation(e) => Some(e.table_schema.id),
            Event::Truncate(e) => e.rel_ids.first().map(|id| TableId::new(*id)),
            _ => None,
        }
    }
}

impl<D> Destination for TestDestinationWrapper<D>
where
    D: Destination + Send + Sync + Clone,
{
    fn name() -> &'static str {
        "wrapper"
    }

    async fn create_table_schema(&self, table_schema: Arc<TableSchema>) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let result = destination.create_table_schema(table_schema.clone()).await;

        if result.is_ok() {
            let mut inner = self.inner.write().await;
            inner
                .table_schemas
                .insert(table_schema.id, table_schema.clone());
            inner.check_conditions().await;
        }

        result
    }

    async fn truncate_table(
        &self,
        table_id: TableId,
        schema_creation_mode: SchemaCreationMode,
    ) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let result = destination
            .truncate_table(table_id, schema_creation_mode)
            .await;

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
        schema_creation_mode: SchemaCreationMode,
    ) -> EtlResult<()> {
        let destination = {
            let mut inner = self.inner.write().await;
            inner.write_table_rows_called += 1;
            Self::ensure_schema_exists(&inner, &table_id)?;
            inner.wrapped_destination.clone()
        };

        let result = destination
            .write_table_rows(table_id, table_rows.clone(), schema_creation_mode)
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

    async fn write_events(
        &self,
        events: Vec<Event>,
        schema_creation_mode: SchemaCreationMode,
    ) -> EtlResult<()> {
        let destination = {
            let inner = self.inner.read().await;
            for event in &events {
                if let Some(table_id) = Self::extract_table_id(event) {
                    Self::ensure_schema_exists(&inner, &table_id)?;
                }
            }
            inner.wrapped_destination.clone()
        };

        let result = destination
            .write_events(events.clone(), schema_creation_mode)
            .await;

        {
            let mut inner = self.inner.write().await;
            if result.is_ok() {
                inner.events.extend(events);
            }

            inner.check_conditions().await;
        }

        result
    }
}
