use std::{collections::HashMap, sync::Arc};

use postgres::schema::TableId;

use crate::v2::state::{
    store::base::{StateStore, StateStoreError},
    table::TableReplicationPhase,
};

/// A state store to allow different state store implementations to be
/// used in the same pipeline. This might have dynamic dispatch overhead
/// in which case we'll go to a statically dispatched impl if confirmed
/// by a performance benchmark.
#[derive(Clone)]
pub struct BoxedStateStore {
    inner: Arc<dyn StateStore + Send + Sync>,
}

impl BoxedStateStore {
    pub fn new<S: StateStore + Send + Sync + 'static>(store: S) -> Self {
        BoxedStateStore {
            inner: Arc::new(store),
        }
    }
}

#[async_trait::async_trait]
impl StateStore for BoxedStateStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError> {
        self.inner.get_table_replication_state(table_id).await
    }

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        self.inner.get_table_replication_states().await
    }

    async fn load_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError> {
        self.inner.load_table_replication_states().await
    }

    async fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> Result<(), StateStoreError> {
        self.inner
            .store_table_replication_state(table_id, state)
            .await
    }
}
