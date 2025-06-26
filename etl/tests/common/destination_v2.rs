use etl::conversions::table_row::TableRow;
use etl::v2::conversions::event::{Event, EventType};
use etl::v2::destination::base::{Destination, DestinationError};
use postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{Notify, RwLock};

use crate::common::event::check_events_count;

type EventCondition = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type SchemaCondition = Box<dyn Fn(&[TableSchema]) -> bool + Send + Sync>;
type TableRowCondition = Box<dyn Fn(&HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

struct Inner {
    events: Vec<Event>,
    table_schemas: Vec<TableSchema>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    event_conditions: Vec<(EventCondition, Arc<Notify>)>,
    table_schema_conditions: Vec<(SchemaCondition, Arc<Notify>)>,
    table_row_conditions: Vec<(TableRowCondition, Arc<Notify>)>,
}

impl Inner {
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

#[derive(Clone)]
pub struct TestDestination {
    inner: Arc<RwLock<Inner>>,
}

impl fmt::Debug for TestDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestDestination")
            .field("events", &inner.events)
            .field("schemas", &inner.table_schemas)
            .field("table_rows", &inner.table_rows)
            .finish()
    }
}

impl TestDestination {
    pub fn new() -> Self {
        let inner = Inner {
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

    pub async fn get_table_schemas(&self) -> Vec<TableSchema> {
        let mut table_schemas = self.inner.read().await.table_schemas.clone();
        table_schemas.sort();

        table_schemas
    }

    pub async fn get_table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
    }

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

    pub async fn wait_for_events_count(&self, conditions: Vec<(EventType, u64)>) -> Arc<Notify> {
        self.notify_on_events(move |events| check_events_count(events, conditions.clone()))
            .await
    }

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

    pub async fn wait_for_n_schemas(&self, n: usize) -> Arc<Notify> {
        self.notify_on_schemas(move |schemas| schemas.len() == n)
            .await
    }
}

impl Default for TestDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for TestDestination {
    async fn write_table_schema(&self, schema: TableSchema) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.table_schemas.push(schema);
        inner.check_conditions().await;

        Ok(())
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, DestinationError> {
        let inner = self.inner.read().await;
        let table_schemas = inner.table_schemas.to_vec();

        Ok(table_schemas)
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.table_rows.entry(table_id).or_default().extend(rows);
        inner.check_conditions().await;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.events.extend(events);
        inner.check_conditions().await;

        Ok(())
    }
}
