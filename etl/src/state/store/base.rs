use postgres::schema::TableId;
use std::{collections::HashMap, future::Future};

use crate::error::EtlResult;
use crate::state::table::TableReplicationPhase;

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
    ) -> impl Future<Output = EtlResult<Option<TableReplicationPhase>>> + Send;

    /// Returns the table replication states for all the tables from the cache.
    /// Does not read from the persistent store.
    fn get_table_replication_states(
        &self,
    ) -> impl Future<Output = EtlResult<HashMap<TableId, TableReplicationPhase>>> + Send;

    /// Loads the table replication states from the persistent state into the cache.
    /// This should be called once at program start to load the state into the cache
    /// and then use only the `get_X` methods to access the state. Updating the state
    /// by calling the `update_table_replication_state` updates in both the cache and
    /// the persistent store, so no need to ever load the state again.
    fn load_table_replication_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Updates the table replicate state for a table with `table_id` in both the cache and
    /// the persistent store.
    fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
