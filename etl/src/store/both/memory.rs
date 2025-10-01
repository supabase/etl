use etl_postgres::types::{SchemaVersion, TableId, TableSchema, TableSchemaDraft};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::state::table::TableReplicationPhase;
use crate::store::cleanup::CleanupStore;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;

/// The initial schema version number.
const STARTING_SCHEMA_VERSION: u64 = 0;

/// Inner state of [`MemoryStore`]
#[derive(Debug)]
struct Inner {
    /// Current replication state for each table.
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
    /// Complete history of state transitions for each table which is used for state rollbacks.
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    /// Table schema definitions for each table. Each schema has multiple versions which are created
    /// when new schema changes are detected.
    table_schemas: HashMap<TableId, BTreeMap<SchemaVersion, Arc<TableSchema>>>,
    /// Mappings between source and destination tables.
    table_mappings: HashMap<TableId, String>,
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
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
            table_mappings: HashMap::new(),
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
    ) -> EtlResult<HashMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.len())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        // Store the current state in history before updating
        if let Some(current_state) = inner.table_replication_states.get(&table_id).cloned() {
            inner
                .table_state_history
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(current_state);
        }

        inner.table_replication_states.insert(table_id, state);

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
                    "There is no state in memory to rollback to"
                )
            })?;

        // Update the current state to the previous state
        inner
            .table_replication_states
            .insert(table_id, previous_state.clone());

        Ok(previous_state)
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.get(source_table_id).cloned())
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.clone())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_mappings.len())
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner
            .table_mappings
            .insert(source_table_id, destination_table_id);

        Ok(())
    }
}

impl SchemaStore for MemoryStore {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        version: SchemaVersion,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner
            .table_schemas
            .get(table_id)
            .and_then(|table_schemas| table_schemas.get(&version).cloned()))
    }

    async fn get_latest_table_schema(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.get(table_id).and_then(|table_schemas| {
            table_schemas
                .iter()
                .next_back()
                .map(|(_, schema)| schema.clone())
        }))
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner
            .table_schemas
            .values()
            .map(|table_schemas| table_schemas.len())
            .sum())
    }

    async fn store_table_schema(
        &self,
        table_schema: TableSchemaDraft,
    ) -> EtlResult<Arc<TableSchema>> {
        let mut inner = self.inner.lock().await;
        let table_schemas = inner
            .table_schemas
            .entry(table_schema.id)
            .or_insert_with(BTreeMap::new);

        let next_version = table_schemas
            .keys()
            .next_back()
            .map(|version| version + 1)
            .unwrap_or(STARTING_SCHEMA_VERSION);

        let table_schema = Arc::new(table_schema.into_table_schema(next_version));
        table_schemas.insert(next_version, table_schema.clone());

        Ok(table_schema)
    }
}

impl CleanupStore for MemoryStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        inner.table_replication_states.remove(&table_id);
        inner.table_state_history.remove(&table_id);
        inner.table_schemas.remove(&table_id);
        inner.table_mappings.remove(&table_id);

        Ok(())
    }
}
