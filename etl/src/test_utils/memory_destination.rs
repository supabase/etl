use etl_postgres::types::TableId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::destination::Destination;
use crate::error::EtlResult;
use crate::store::state::StateStore;
use crate::types::{Event, TableRow};

#[derive(Debug)]
struct Inner {
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    table_mappings: HashSet<TableId>,
}

/// In-memory destination for tests.
///
/// [`MemoryDestination`] stores all replicated data in memory and also writes
/// table mappings into the provided state store so mappings exist during tests.
#[derive(Debug, Clone)]
pub struct MemoryDestination<S> {
    inner: Arc<Mutex<Inner>>,
    state_store: S,
}

impl<S> MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync + 'static,
{
    /// Creates a new empty memory destination using the provided state store.
    pub fn new(state_store: S) -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_rows: HashMap::new(),
            table_mappings: HashSet::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
            state_store,
        }
    }

    /// Returns a copy of all events stored in this destination.
    pub async fn events(&self) -> Vec<Event> {
        let inner = self.inner.lock().await;
        inner.events.clone()
    }

    /// Returns a copy of all table rows stored in this destination.
    pub async fn table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        let inner = self.inner.lock().await;
        inner.table_rows.clone()
    }

    /// Clears all stored events and table rows.
    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        inner.events.clear();
        inner.table_rows.clear();
    }

    async fn store_table_mapping(&self, table_id: TableId) -> EtlResult<()> {
        {
            let mut inner = self.inner.lock().await;
            inner.table_mappings.insert(table_id);
        }

        self.state_store
            .store_table_mapping(table_id, format!("memory_destination_table_{}", table_id.0))
            .await
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
        let mut inner = self.inner.lock().await;

        info!(table_id = table_id.0, "truncating table");

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
        self.store_table_mapping(table_id).await?;

        let mut inner = self.inner.lock().await;

        info!(
            table_id = table_id.0,
            row_count = table_rows.len(),
            "writing table rows"
        );
        inner.table_rows.insert(table_id, table_rows);

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
            self.store_table_mapping(table_id).await?;
        }

        let mut inner = self.inner.lock().await;

        info!(event_count = events.len(), "writing events");
        inner.events.extend(events);

        Ok(())
    }
}
