use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::destination::Destination;
use crate::error::EtlResult;
use crate::types::{Event, TableId, TableRow};

#[derive(Debug)]
struct Inner {
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
}

#[derive(Debug, Clone)]
pub struct MemoryDestination {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryDestination {
    pub fn new() -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_rows: HashMap::new(),
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
    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        // For truncation, we simulate removing all table rows for a specific table and also the events
        // of that table.
        let mut inner = self.inner.lock().await;

        info!("truncating table {}", table_id);

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

        Ok(())
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
        inner.table_rows.insert(table_id, table_rows);

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
