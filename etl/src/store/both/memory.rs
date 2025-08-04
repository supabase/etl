use postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::state::table::TableReplicationPhase;
use crate::store::schema::SchemaStore;
use crate::store::state::StateStore;

#[derive(Debug)]
struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
}

#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
            table_schemas: HashMap::new(),
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
}

impl SchemaStore for MemoryStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.get(table_id).cloned())
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;

        Ok(inner.table_schemas.len())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner
            .table_schemas
            .insert(table_schema.id, Arc::new(table_schema));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres::schema::{ColumnSchema, TableName};
    use tokio_postgres::types::Type;

    #[tokio::test]
    async fn test_schema_storage_operations() {
        let store = MemoryStore::new();

        // Create a test table schema
        let table_id = TableId::new(123);
        let table_name = TableName::new("test_schema".to_string(), "test_table".to_string());
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT8, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ];
        let table_schema = TableSchema::new(table_id, table_name, columns);

        // Test storing schema
        store
            .store_table_schema(table_schema.clone())
            .await
            .unwrap();

        // Test retrieving schema
        let retrieved = store.get_table_schema(&table_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, table_schema.id);
        assert_eq!(retrieved.name, table_schema.name);
        assert_eq!(
            retrieved.column_schemas.len(),
            table_schema.column_schemas.len()
        );

        // Test get_table_schemas
        let schemas = store.get_table_schemas().await.unwrap();
        assert_eq!(schemas.len(), 1);

        // Test load_table_schemas (for memory store, it just returns the count)
        let count = store.load_table_schemas().await.unwrap();
        assert_eq!(count, 1);
    }
}
