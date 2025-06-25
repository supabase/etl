use postgres::schema::TableId;
use std::{collections::HashMap, future::Future};
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

pub trait StateStore: Send + Sync {
    fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = Result<Option<TableReplicationPhase>, StateStoreError>> + Send;

    fn get_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<HashMap<TableId, TableReplicationPhase>, StateStoreError>> + Send;

    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<HashMap<TableId, TableReplicationPhase>, StateStoreError>> + Send;

    fn store_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send;
}
