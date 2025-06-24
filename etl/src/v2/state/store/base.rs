use postgres::schema::TableId;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

use crate::v2::{
    replication::slot::SlotError,
    state::{store::postgres::ToTableStateError, table::TableReplicationPhase},
};

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Sqlx error in state store: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Slot error in state store: {0}")]
    Slot(#[from] SlotError),

    #[error("Invalid confirmed flush lsn value in state store: {0}")]
    InvalidConfirmedFlushLsn(String),

    #[error("Missing slot in state store: {0}")]
    MissingSlot(String),

    #[error("Error converting from table replication phase to table state")]
    ToTableState(#[from] ToTableStateError),
}

#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> Result<Option<TableReplicationPhase>, StateStoreError>;

    async fn get_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError>;

    async fn load_table_replication_states(
        &self,
    ) -> Result<HashMap<TableId, TableReplicationPhase>, StateStoreError>;

    async fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> Result<(), StateStoreError>;
}

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
