use postgres::schema::TableId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::etl_error;
use crate::state::store::StateStore;
use crate::state::table::TableReplicationPhase;

#[derive(Debug)]
struct Inner {
    table_replication_states: HashMap<TableId, TableReplicationPhase>,
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
}

#[derive(Debug, Clone)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
            table_state_history: HashMap::new(),
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
        if let Some(current_state) = inner.table_replication_states.get(&table_id).copied() {
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
            .insert(table_id, previous_state);

        Ok(previous_state)
    }
}
