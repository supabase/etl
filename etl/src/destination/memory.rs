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

/// In-memory destination for testing and development purposes.
///
/// [`MemoryDestination`] stores all replicated data in memory, making it ideal for
/// testing ETL pipelines, debugging replication behavior, and development workflows.
/// All data is held in memory and will be lost when the process terminates.
///
/// # Examples
///
/// ```rust,no_run
/// use etl::destination::memory::MemoryDestination;
/// use etl::store::both::memory::MemoryStore;
/// use etl::prelude::*;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a memory destination for testing
/// let destination = MemoryDestination::new();
///
/// // Set up basic pipeline configuration
/// let pipeline_config = PipelineConfig {
///     id: 1,
///     pg_connection: PgConnectionConfig {
///         host: "localhost".to_string(),
///         port: 5432,
///         name: "testdb".to_string(),
///         username: "postgres".to_string(),
///         password: None,
///         tls: TlsConfig {
///             enabled: false,
///             trusted_root_certs: String::new(),
///         },
///     },
///     publication_name: "test_publication".to_string(),
///     batch: BatchConfig::default(),
///     table_error_retry_delay_ms: 1000,
///     max_table_sync_workers: 2,
/// };
///
/// // Memory destinations are perfect for integration tests
/// // as you can inspect the captured data afterward
/// let memory_store = MemoryStore::new();
/// let mut pipeline = Pipeline::new(
///     1, // PipelineId
///     pipeline_config,
///     memory_store,
///     destination.clone()
/// );
///
/// pipeline.start().await?;
/// // ... pipeline processing ...
/// pipeline.shutdown_and_wait().await?;
///
/// // Access captured data for verification
/// let events = destination.events().await;
/// let table_data = destination.table_rows().await;
///
/// println!("Captured {} events", events.len());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MemoryDestination {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryDestination {
    /// Creates a new empty memory destination.
    ///
    /// The destination starts with no stored data and will accumulate
    /// events and table rows as the pipeline processes replication data.
    pub fn new() -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_rows: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Returns a copy of all events stored in this destination.
    ///
    /// This method is useful for testing and verification of pipeline behavior.
    /// It provides access to all replication events that have been written
    /// to this destination since creation or the last clear operation.
    pub async fn events(&self) -> Vec<Event> {
        let inner = self.inner.lock().await;
        inner.events.clone()
    }

    /// Returns a copy of all table rows stored in this destination.
    ///
    /// This method is useful for testing and verification of pipeline behavior.
    /// It provides access to all table row data that has been written
    /// to this destination, organized by table ID.
    pub async fn table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        let inner = self.inner.lock().await;
        inner.table_rows.clone()
    }

    /// Clears all stored events and table rows.
    ///
    /// This method is useful for resetting the destination state between tests
    /// or during development workflows.
    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        inner.events.clear();
        inner.table_rows.clear();
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
