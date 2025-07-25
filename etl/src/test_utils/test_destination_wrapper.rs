use postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

use crate::conversions::event::{Event, EventType};
use crate::conversions::table_row::TableRow;
use crate::destination::base::{Destination, DestinationError};
use crate::schema::cache::SchemaCache;
use crate::test_utils::event::check_events_count;

type EventCondition = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type SchemaCondition = Box<dyn Fn(&[TableSchema]) -> bool + Send + Sync>;
type TableRowCondition = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

struct Inner<D> {
    wrapped_destination: D,
    events: Vec<Event>,
    table_schemas: Vec<TableSchema>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    event_conditions: Vec<(EventCondition, Arc<Notify>)>,
    table_schema_conditions: Vec<(SchemaCondition, Arc<Notify>)>,
    table_row_conditions: Vec<(TableRowCondition, Arc<Notify>)>,
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

        // Check schema conditions
        let schemas = self.table_schemas.clone();
        self.table_schema_conditions.retain(|(condition, notify)| {
            let should_retain = !condition(&schemas);
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

/// A test wrapper that can wrap any destination and track method calls and data
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
            .field("schemas", &inner.table_schemas)
            .field("table_rows", &inner.table_rows)
            .finish()
    }
}

impl<D> TestDestinationWrapper<D> {
    /// Create a new test wrapper around any destination
    pub fn wrap(destination: D) -> Self {
        let inner = Inner {
            wrapped_destination: destination,
            events: Vec::new(),
            table_schemas: Vec::new(),
            table_rows: HashMap::new(),
            event_conditions: Vec::new(),
            table_schema_conditions: Vec::new(),
            table_row_conditions: Vec::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Get all table schemas that have been written
    pub async fn get_table_schemas(&self) -> Vec<TableSchema> {
        let mut table_schemas = self.inner.read().await.table_schemas.clone();
        table_schemas.sort();
        table_schemas
    }

    /// Get all table rows that have been written
    pub async fn get_table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    /// Get all events that have been written
    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
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

    /// Wait for a specific number of events of given types
    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> Arc<Notify> {
        self.notify_on_events(move |events| check_events_count(events, conditions.clone()))
            .await
    }

    /// Wait for a specific condition on schemas
    pub async fn notify_on_schemas<F>(&self, condition: F) -> Arc<Notify>
    where
        F: Fn(&[TableSchema]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner
            .table_schema_conditions
            .push((Box::new(condition), notify.clone()));

        notify
    }

    /// Wait for a specific number of schemas
    pub async fn wait_for_n_schemas(&self, n: usize) -> Arc<Notify> {
        self.notify_on_schemas(move |schemas| schemas.len() == n)
            .await
    }
}

impl<D: Destination + Send + Sync + Clone> Destination for TestDestinationWrapper<D> {
    async fn inject(&self, schema_cache: SchemaCache) -> Result<(), DestinationError> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        destination.inject(schema_cache).await
    }

    async fn write_table_schema(&self, table_schema: TableSchema) -> Result<(), DestinationError> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let result = destination.write_table_schema(table_schema.clone()).await;

        {
            let mut inner = self.inner.write().await;
            if result.is_ok() {
                inner.table_schemas.push(table_schema);
            }

            inner.check_conditions().await;
        }

        result
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, DestinationError> {
        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        destination.load_table_schemas().await
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> Result<(), DestinationError> {
        let destination = {
            let inner = self.inner.read().await;
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

    async fn write_events(&self, events: Vec<Event>) -> Result<(), DestinationError> {
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
}
