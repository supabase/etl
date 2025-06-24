use postgres::schema::TableId;
use std::{collections::HashMap, future::Future};
use thiserror::Error;

use crate::v2::state::table::TableReplicationPhase;

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Table replication state not found in store")]
    TableReplicationStateNotFound,
}

pub trait StateStore {
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
