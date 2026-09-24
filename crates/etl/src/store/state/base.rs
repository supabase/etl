use std::{collections::BTreeMap, future::Future, sync::Arc};

use crate::{
    destination::DestinationTableMetadata,
    error::EtlResult,
    schema::{PgLsn, TableId},
    store::{CachedStore, TableState, WorkerType},
};

/// Arc-wrapped dictionary of table states.
pub type TableStates = Arc<BTreeMap<TableId, TableState>>;

/// Arc-wrapped dictionary of destination table metadata.
pub(crate) type DestinationTablesMetadata = Arc<BTreeMap<TableId, DestinationTableMetadata>>;

/// Stores table states, replication checkpoints, and destination metadata.
///
/// Implementations follow the shared-cache contract in [`crate::store`].
pub trait StateStore: CachedStore {
    /// Returns table state for table with id `table_id` from the cache.
    ///
    /// Implementations may refresh an uninitialized or invalidated cache.
    fn get_table_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<Option<TableState>>> + Send;

    /// Returns the table states for all the tables from the cache.
    ///
    /// Implementations may refresh an uninitialized or invalidated cache.
    fn get_table_states(&self) -> impl Future<Output = EtlResult<TableStates>> + Send;

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

    /// Returns the worker's cached durable replay checkpoint, or `None`.
    ///
    /// Implementations may refresh an uninitialized or invalidated cache.
    /// In-memory apply progress alone does not establish a checkpoint.
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
    /// Used when resetting slot lineage. Remove the cached checkpoint only
    /// after persistence succeeds. Cancellation must not expose a stale cache
    /// while the deletion can still finish in the database.
    fn delete_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Returns destination table metadata for a specific table from the cache.
    ///
    /// Implementations may refresh an uninitialized or invalidated cache.
    fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<Option<DestinationTableMetadata>>> + Send;

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
