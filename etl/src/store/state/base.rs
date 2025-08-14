use etl_postgres::schema::TableId;
use std::{collections::HashMap, future::Future};

use crate::error::EtlResult;
use crate::state::table::TableReplicationPhase;

/// Trait for storing and retrieving table replication state and mapping information.
///
/// [`StateStore`] implementations are responsible for defining how table replication states and
/// table mappings are stored and retrieved. Table mappings define the relationship between
/// source table identifiers and destination table names.
///
/// Implementations should ensure thread-safety and handle concurrent access to the data.
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

    /// Rolls back to the previous replication state.
    fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<TableReplicationPhase>> + Send;

    /// Returns table mapping for a specific source table ID from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_table_mapping(
        &self,
        source_table_id: &TableId,
    ) -> impl Future<Output = EtlResult<Option<String>>> + Send;

    /// Returns all table mappings from the cache.
    ///
    /// Does not read from the persistent store.
    fn get_table_mappings(
        &self,
    ) -> impl Future<Output = EtlResult<HashMap<TableId, String>>> + Send;

    /// Loads all table mappings from the persistent state into the cache.
    ///
    /// This can be called lazily when table mappings are needed by the destination.
    fn load_table_mappings(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores a table mapping in both the cache and the persistent store.
    fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
