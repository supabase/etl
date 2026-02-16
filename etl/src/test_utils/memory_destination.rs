use etl_postgres::types::TableId;
use std::collections::HashSet;
use tracing::info;

use crate::destination::Destination;
use crate::error::EtlResult;
use crate::store::state::StateStore;
use crate::types::{Event, TableRow};

/// In-memory destination for tests that only logs incoming data.
#[derive(Debug, Clone)]
pub struct MemoryDestination<S> {
    state_store: S,
}

impl<S> MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    /// Creates a new memory destination.
    pub fn new(state_store: S) -> Self {
        Self { state_store }
    }
}

impl<S> Destination for MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    fn name() -> &'static str {
        "memory"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!(table_id = table_id.0, "truncating table");
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        info!(
            table_id = table_id.0,
            row_count = table_rows.len(),
            "writing table rows"
        );

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut table_ids = HashSet::new();
        for event in &events {
            match event {
                Event::Insert(event) => {
                    table_ids.insert(event.table_id);
                }
                Event::Update(event) => {
                    table_ids.insert(event.table_id);
                }
                Event::Delete(event) => {
                    table_ids.insert(event.table_id);
                }
                Event::Relation(event) => {
                    table_ids.insert(event.table_schema.id);
                }
                Event::Truncate(event) => {
                    for table_id in &event.rel_ids {
                        table_ids.insert(TableId::new(*table_id));
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }
        }

        for table_id in table_ids {
            self.state_store
                .store_table_mapping(table_id, format!("memory_destination_table_{}", table_id.0))
                .await?;
        }

        info!(event_count = events.len(), "writing events");

        Ok(())
    }
}
