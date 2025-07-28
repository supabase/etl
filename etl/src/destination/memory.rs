use postgres::schema::{TableId, TableSchema};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::conversions::event::Event;
use crate::conversions::table_row::TableRow;
use crate::destination::base::Destination;
use crate::error::EtlResult;

#[derive(Debug)]
struct Inner {
    events: Vec<Event>,
    table_schemas: Vec<TableSchema>,
    table_rows: Vec<(TableId, Vec<TableRow>)>,
}

#[derive(Debug, Clone)]
pub struct MemoryDestination {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryDestination {
    pub fn new() -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_schemas: Vec::new(),
            table_rows: Vec::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl Default for MemoryDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for MemoryDestination {
    async fn write_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        info!("writing table schema:");
        info!("{:?}", table_schema);
        inner.table_schemas.push(table_schema);

        Ok(())
    }

    async fn load_table_schemas(&self) -> EtlResult<Vec<TableSchema>> {
        let inner = self.inner.lock().await;
        let schemas = inner.table_schemas.to_vec();
        info!("loaded {} table schemas:", schemas.len());
        info!("{:?}", schemas);

        Ok(schemas)
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        info!("writing a batch of {} table rows:", table_rows.len());
        for table_row in &table_rows {
            info!("  {:?}", table_row);
        }
        inner.table_rows.push((table_id, table_rows));

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        info!("writing a batch of {} events:", events.len());
        for event in &events {
            info!("  {:?}", event);
        }
        inner.events.extend(events);

        Ok(())
    }
}
