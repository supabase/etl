use postgres::schema::Oid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::v2::pipeline::PipelineId;
use crate::v2::state::store::base::{StateStore, StateStoreError};
use crate::v2::state::table::TableReplicationState;

#[derive(Debug)]
struct Inner {
    table_replication_states: HashMap<(PipelineId, Oid), TableReplicationState>,
}

#[derive(Debug, Clone)]
pub struct MemoryStateStore {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        let inner = Inner {
            table_replication_states: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStore for MemoryStateStore {
    async fn load_table_replication_state(
        &self,
        pipeline_id: PipelineId,
        table_id: Oid,
    ) -> Result<Option<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner
            .table_replication_states
            .get(&(pipeline_id, table_id))
            .cloned())
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<Vec<TableReplicationState>, StateStoreError> {
        let inner = self.inner.read().await;

        Ok(inner.table_replication_states.values().cloned().collect())
    }

    async fn store_table_replication_state(
        &self,
        state: TableReplicationState,
        overwrite: bool,
    ) -> Result<bool, StateStoreError> {
        let mut inner = self.inner.write().await;
        let key = (state.pipeline_id, state.table_id);

        if !overwrite && inner.table_replication_states.contains_key(&key) {
            return Ok(false);
        }

        inner.table_replication_states.insert(key, state);

        Ok(true)
    }
}
