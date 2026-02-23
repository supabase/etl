use etl_postgres::types::{SnapshotId, TableId, TableSchema};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::state::destination_metadata::DestinationTableMetadata;
use crate::state::table::TableReplicationPhase;
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;

/// Inner state of [`MemoryStore`]
#[derive(Debug)]
struct Inner {
    /// Current replication state for each table - this is the authoritative source of truth
    /// for table states. Every table being replicated must have an entry here.
    table_replication_states: BTreeMap<TableId, TableReplicationPhase>,
    /// Complete history of state transitions for each table, used for debugging and auditing.
    /// This is an append-only log that grows over time and provides visibility into
    /// table state evolution. Entries are chronologically ordered.
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    /// Cached table schemas keyed by (TableId, SnapshotId) for versioning support.
    table_schemas: HashMap<(TableId, SnapshotId), Arc<TableSchema>>,
    /// Cached destination table metadata indexed by table ID.
    destination_tables_metadata: HashMap<TableId, DestinationTableMetadata>,
}

/// In-memory storage for ETL pipeline state and schema information.
///
/// [`MemoryStore`] implements both [`StateStore`] and [`SchemaStore`] traits,
/// providing a complete storage solution that keeps all data in memory. This is
/// ideal for testing, development, and scenarios where persistence is not required.
///
/// All state information including table replication phases, schema definitions,
/// and table mappings are stored in memory and will be lost on process restart.
#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    /// Creates a new empty memory store.
    ///
    /// The store initializes with empty collections for all state and schema data.
    /// As the pipeline runs, it will populate these collections with replication
    /// state and schema information.
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: BTreeMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
            destination_tables_metadata: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.len())
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        for (table_id, state) in updates {
            // Store the current state in history before updating
            if let Some(current_state) = inner.table_replication_states.get(&table_id).cloned() {
                inner
                    .table_state_history
                    .entry(table_id)
                    .or_insert_with(Vec::new)
                    .push(current_state);
            }

            inner.table_replication_states.insert(table_id, state);
        }

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let mut inner = self.inner.lock().await;

        // Get the previous state from history
        let previous_state = inner
            .table_state_history
            .get_mut(&table_id)
            .and_then(|history| history.pop())
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::StateRollbackError,
                    "No previous state available to roll back to"
                )
            })?;

        // Update the current state to the previous state
        inner
            .table_replication_states
            .insert(table_id, previous_state.clone());

        Ok(previous_state)
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        let inner = self.inner.lock().await;

        Ok(inner.destination_tables_metadata.get(&table_id).cloned())
    }

    async fn load_destination_tables_metadata(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.destination_tables_metadata.len())
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner.destination_tables_metadata.insert(table_id, metadata);

        Ok(())
    }
}

impl SchemaStore for MemoryStore {
    /// Returns the table schema for the given table at the specified snapshot point.
    ///
    /// Returns the schema version with the largest snapshot_id <= the requested snapshot_id.
    /// For MemoryStore, this only looks in the in-memory cache.
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        // Find the best matching schema (largest snapshot_id <= requested).
        let best_match = inner
            .table_schemas
            .iter()
            .filter(|((tid, sid), _)| *tid == *table_id && *sid <= snapshot_id)
            .max_by_key(|((_, sid), _)| *sid)
            .map(|(_, schema)| schema.clone());

        Ok(best_match)
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.len())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        let mut inner = self.inner.lock().await;

        let key = (table_schema.id, table_schema.snapshot_id);
        let table_schema = Arc::new(table_schema);
        inner.table_schemas.insert(key, table_schema.clone());
        Ok(table_schema)
    }
}

impl CleanupStore for MemoryStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        inner.table_replication_states.remove(&table_id);
        inner.table_state_history.remove(&table_id);
        // Remove all schema versions for this table
        inner.table_schemas.retain(|(tid, _), _| *tid != table_id);
        inner.destination_tables_metadata.remove(&table_id);

        Ok(())
    }
}
