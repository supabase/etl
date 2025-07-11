use postgres::schema::TableId;
use std::{collections::HashMap, future::Future};
use thiserror::Error;

use crate::{
    replication::slot::SlotError,
    state::{
        store::postgres::{FromTableStateError, ToTableStateError},
        table::TableReplicationPhase,
    },
};

#[derive(Debug, Error)]
pub enum StateStoreError {
    #[error("Sqlx error in state store: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Slot error in state store: {0}")]
    Slot(#[from] SlotError),

    #[error("Invalid confirmed flush lsn value in state store: {0}")]
    InvalidConfirmedFlushLsn(String),

    #[error("Missing slot in state store: {0}")]
    MissingSlot(String),

    #[error("Error converting from table replication phase to table state")]
    ToTableState(#[from] ToTableStateError),

    #[error("Error converting from table state to table replication phase")]
    FromTableState(#[from] FromTableStateError),
}

/// This trait represents a state store for the replication state of all tables.
/// It assumes that the implementers keep a cache of the state to avoid having
/// to keep reading from the backing persistent store again and again.
pub trait StateStore {
    /// Returns table replication state for table with id `table_id` from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = Result<Option<TableReplicationPhase>, StateStoreError>> + Send;

    /// Returns the table replication states for all the tables from the cache.
    /// Does not read from the persistent store.
    fn get_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<HashMap<TableId, TableReplicationPhase>, StateStoreError>> + Send;

    /// Loads the table replication states from the persistent state into the cache.
    /// This should called once at program start to load the state into the cache
    /// and then use only the `get_X` methods to access the state. Updating the state
    /// by calling the `update_table_replication_state` updates in both the cache and
    /// the persistent store, so no need to ever load the state again.
    fn load_table_replication_states(
        &self,
    ) -> impl Future<Output = Result<usize, StateStoreError>> + Send;

    /// Updates the table replicate state for a table with `table_id` in both the cache as well as
    /// the persistent store.
    fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send;
}
