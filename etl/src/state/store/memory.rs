use postgres::schema::TableId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::state::store::base::{StateStore, StateStoreError};
use crate::state::table::TableReplicationPhase;

#[derive(Debug)]
struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
}

#[derive(Debug, Clone)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.clone())
    }

    async fn load_table_replication_states(&self) -> Result<usize, StateStoreError> {
        let inner = self.inner.lock().await;

        Ok(inner.table_replication_states.len())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> Result<(), StateStoreError> {
        let mut inner = self.inner.lock().await;
        inner.table_replication_states.insert(table_id, state);

        Ok(())
    }
}
