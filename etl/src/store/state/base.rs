use etl_postgres::types::TableId;
use std::collections::BTreeMap;
use std::future::Future;

use crate::error::EtlResult;
use crate::state::destination_metadata::DestinationTableMetadata;
use crate::state::table::TableReplicationPhase;

/// Trait for storing and retrieving table replication state and destination metadata.
///
/// [`StateStore`] implementations are responsible for defining how table replication states
/// and destination table metadata are stored and retrieved.
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
    ///
    /// Does not read from the persistent store.
    fn get_table_replication_states(
        &self,
    ) -> impl Future<Output = EtlResult<BTreeMap<TableId, TableReplicationPhase>>> + Send;

    /// Loads the table replication states from the persistent state into the cache.
    ///
    /// This should be called once at program start to load the state into the cache
    /// and then use only the `get_X` methods to access the state. Updating the state
    /// by calling the `update_table_replication_state` updates in both the cache and
    /// the persistent store, so no need to ever load the state again.
    fn load_table_replication_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Updates multiple table replication states atomically in both the cache and
    /// the persistent store.
    ///
    /// All state updates are applied as a single atomic operation. If any update fails,
    /// the entire operation is rolled back.
    fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Updates the table replicate state for a table with `table_id` in both the cache and
    /// the persistent store.
    fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        self.update_table_replication_states(vec![(table_id, state)])
    }

    /// Rolls back to the previous replication state.
    fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<TableReplicationPhase>> + Send;

    /// Returns destination table metadata for a specific table from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<Option<DestinationTableMetadata>>> + Send;

    /// Loads all destination table metadata from the persistent state into the cache.
    ///
    /// This should be called during startup to load the metadata into the cache.
    fn load_destination_tables_metadata(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores destination table metadata in both the cache and persistent store.
    ///
    /// This performs a full upsert. For updates, get the current metadata, modify
    /// the fields you need to change, and store it back.
    fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
