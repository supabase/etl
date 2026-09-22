use std::{collections::BTreeMap, future::Future, sync::Arc};

use crate::{
    destination::DestinationTableMetadata,
    error::EtlResult,
    schema::{PgLsn, TableId},
    store::{TableState, WorkerType},
};

/// Arc-wrapped dictionary of table states.
pub type TableStates = Arc<BTreeMap<TableId, TableState>>;

/// Arc-wrapped dictionary of destination table metadata.
pub(crate) type DestinationTablesMetadata = Arc<BTreeMap<TableId, DestinationTableMetadata>>;

/// Stores table states, replication checkpoints, and destination metadata.
///
/// Implementations follow the shared-cache contract in [`crate::store`].
pub trait StateStore {
    /// Returns table state for table with id `table_id` from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_table_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<Option<TableState>>> + Send;

    /// Returns the table states for all the tables from the cache.
    ///
    /// Does not read from the persistent store.
    fn get_table_states(&self) -> impl Future<Output = EtlResult<TableStates>> + Send;

    /// Loads the table states from the persistent store into the cache.
    ///
    /// Call once at startup; subsequent writes maintain the cache.
    fn load_table_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Persists state updates atomically, then updates the cache.
    fn update_table_states(
        &self,
        updates: Vec<(TableId, TableState)>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Updates the table state for a table with `table_id` in both the cache
    /// and the persistent store.
    fn update_table_state(
        &self,
        table_id: TableId,
        state: TableState,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        self.update_table_states(vec![(table_id, state)])
    }

    /// Rolls back to the previous table state.
    fn rollback_table_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<TableState>> + Send;

    /// Loads all worker checkpoints from persistent storage into the cache.
    ///
    /// Call once at startup before workers run. Later writes and resets keep
    /// the cache current. Returns the number of checkpoints loaded.
    fn load_replication_checkpoints(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Returns the worker's cached durable replay checkpoint, or `None`.
    ///
    /// Call [`Self::load_replication_checkpoints`] at startup. Reads never
    /// query storage; in-memory apply progress alone does not establish a
    /// checkpoint.
    fn get_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> impl Future<Output = EtlResult<Option<PgLsn>>> + Send;

    /// Monotonically persists a checkpoint LSN for a replication worker.
    ///
    /// Cache and return the confirmed stored value, which may exceed the
    /// supplied LSN. Failed writes must not advance the cache.
    fn upsert_replication_checkpoint(
        &self,
        worker_type: WorkerType,
        checkpoint_lsn: PgLsn,
    ) -> impl Future<Output = EtlResult<PgLsn>> + Send;

    /// Deletes the persisted checkpoint for a replication worker.
    ///
    /// Used when resetting slot lineage. Invalidate the cached checkpoint
    /// before database work so cancellation cannot leave a stale boundary.
    fn delete_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Returns destination table metadata for a specific table from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<Option<DestinationTableMetadata>>> + Send;

    /// Loads all destination table metadata from the persistent store into the
    /// cache.
    ///
    /// This should be called during startup to load the metadata into the
    /// cache.
    fn load_destination_tables_metadata(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores destination table metadata in both the cache and persistent
    /// store.
    ///
    /// This performs a full upsert. For updates, get the current metadata,
    /// modify the fields you need to change, and store it back.
    fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
