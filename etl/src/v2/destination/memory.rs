use postgres::schema::{TableId, TableSchema};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::conversions::table_row::TableRow;
use crate::v2::conversions::event::Event;
use crate::v2::destination::base::{Destination, DestinationError};

#[derive(Debug)]
struct Inner {
    events: Vec<Event>,
    table_schemas: Vec<TableSchema>,
    table_rows: Vec<(TableId, Vec<TableRow>)>,
}

#[derive(Debug, Clone)]
pub struct MemoryDestination {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryDestination {
    pub fn new() -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_schemas: Vec::new(),
            table_rows: Vec::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Default for MemoryDestination {
    fn default() -> Self {
        Self::new()
    }
}

impl Destination for MemoryDestination {
    async fn write_table_schema(&self, table_schema: TableSchema) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        info!("Writing table schema:");
        info!("{:?}", table_schema);
        inner.table_schemas.push(table_schema);
        Ok(())
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, DestinationError> {
        let inner = self.inner.read().await;
        let schemas = inner.table_schemas.to_vec();
        info!("Loaded {} table schemas:", schemas.len());
        info!("{:?}", schemas);
        Ok(schemas)
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        info!(
            "Writing batch of {} table rows for table id {:?}:",
            table_rows.len(),
            table_id
        );
        for table_row in &table_rows {
            info!("  {:?}", table_row);
        }
        inner.table_rows.push((table_id, table_rows));
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        info!("Writing batch of {} events:", events.len());
        for event in &events {
            info!("  {:?}", event);
        }
        inner.events.extend(events);
        Ok(())
    }
}
