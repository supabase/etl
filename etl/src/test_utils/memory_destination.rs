use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::destination::Destination;
use crate::destination::async_result::{
    TruncateTableResult, WriteEventsResult, WriteTableRowsResult,
};
use crate::error::EtlResult;
use crate::state::destination_metadata::DestinationTableMetadata;
use crate::store::state::StateStore;
use crate::types::{Event, ReplicatedTableSchema, TableId, TableRow};

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
/// Like real destinations (BigQuery, Iceberg), this destination tracks table metadata
/// (snapshot IDs and replication masks) in a state store to support features like
/// table truncation during state resets.
#[derive(Clone)]
pub struct MemoryDestination<S> {
    inner: Arc<Mutex<Inner>>,
    store: S,
}

impl<S> MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync,
{
    /// Creates a new memory destination with a state store.
    ///
    /// The state store is used to track table metadata (snapshot IDs and replication masks),
    /// mirroring the behavior of real destinations like BigQuery and Iceberg.
    pub fn new(store: S) -> Self {
        let inner = Inner {
            events: Vec::new(),
            table_rows: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
            store,
        }
    }

    /// Returns a copy of all events stored in this destination.
    ///
    /// This method is useful for testing and verification of pipeline behavior.
    /// It provides access to all replication events that have been written
    /// to this destination since creation or the last clear operation.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn events(&self) -> Vec<Event> {
        let inner = self.inner.lock().await;
        inner.events.clone()
    }

    /// Returns a copy of all table rows stored in this destination.
    ///
    /// This method is useful for testing and verification of pipeline behavior.
    /// It provides access to all table row data that has been written
    /// to this destination, organized by table ID.
    #[cfg(any(test, feature = "test-utils"))]
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

    /// Stores destination metadata for a table if it has not been seen before.
    async fn ensure_destination_table_metadata(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();
        let existing_metadata = self.store.get_destination_table_metadata(table_id).await?;
        if existing_metadata.is_none() {
            let metadata = Self::build_destination_table_metadata(replicated_table_schema);
            self.store
                .store_destination_table_metadata(table_id, metadata)
                .await?;
        }

        Ok(())
    }

    /// Builds applied destination metadata for a memory-backed table.
    fn build_destination_table_metadata(
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> DestinationTableMetadata {
        let table_id = replicated_table_schema.id();
        let snapshot_id = replicated_table_schema.inner().snapshot_id;
        let replication_mask = replicated_table_schema.replication_mask().clone();
        let destination_table_id = format!("memory_{}", table_id.into_inner());

        DestinationTableMetadata::new_applied(destination_table_id, snapshot_id, replication_mask)
    }
}

impl<S> Destination for MemoryDestination<S>
where
    S: StateStore + Clone + Send + Sync,
{
    fn name() -> &'static str {
        "memory"
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        // For truncation, we simulate removing all table rows for a specific table and also the events
        // of that table.
        let mut inner = self.inner.lock().await;

        let table_id = replicated_table_schema.id();
        info!(%table_id, "truncating table");

        inner.table_rows.remove(&table_id);
        inner.events.retain_mut(|event| {
            let has_table_id = event.has_table_id(&table_id);
            if let Event::Truncate(truncate_event) = event
                && has_table_id
            {
                truncate_event
                    .truncated_tables
                    .retain(|s| s.id() != table_id);
                if truncate_event.truncated_tables.is_empty() {
                    return false;
                }

                return true;
            }

            !has_table_id
        });

        async_result.send(Ok(()));

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let table_id = replicated_table_schema.id();

        // Store destination metadata on first write, like real destinations (BigQuery, Iceberg) do.
        self.ensure_destination_table_metadata(replicated_table_schema)
            .await?;

        let mut inner = self.inner.lock().await;
        info!(%table_id, row_count = table_rows.len(), "writing table rows");
        inner.table_rows.insert(table_id, table_rows);

        async_result.send(Ok(()));

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let mut table_schemas = HashMap::new();
        for event in &events {
            match event {
                Event::Insert(event) => {
                    table_schemas.insert(
                        event.replicated_table_schema.id(),
                        event.replicated_table_schema.clone(),
                    );
                }
                Event::Update(event) => {
                    table_schemas.insert(
                        event.replicated_table_schema.id(),
                        event.replicated_table_schema.clone(),
                    );
                }
                Event::Delete(event) => {
                    table_schemas.insert(
                        event.replicated_table_schema.id(),
                        event.replicated_table_schema.clone(),
                    );
                }
                Event::Relation(event) => {
                    table_schemas.insert(
                        event.replicated_table_schema.id(),
                        event.replicated_table_schema.clone(),
                    );
                }
                Event::Truncate(event) => {
                    for replicated_table_schema in &event.truncated_tables {
                        table_schemas.insert(
                            replicated_table_schema.id(),
                            replicated_table_schema.clone(),
                        );
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }
        }

        for replicated_table_schema in table_schemas.into_values() {
            self.ensure_destination_table_metadata(&replicated_table_schema)
                .await?;
        }

        let mut inner = self.inner.lock().await;
        info!(event_count = events.len(), "writing events");
        inner.events.extend(events);

        async_result.send(Ok(()));

        Ok(())
    }
}
