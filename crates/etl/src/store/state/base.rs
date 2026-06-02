use std::{collections::BTreeMap, future::Future, sync::Arc};

use crate::{
    error::EtlResult,
    replication::WorkerType,
    state::{
        TableState,
        destination_table_metadata::{AppliedDestinationTableMetadata, DestinationTableMetadata},
    },
    types::{PgLsn, TableId},
};

/// Arc-wrapped dictionary of table states.
pub type TableStates = Arc<BTreeMap<TableId, TableState>>;

/// Arc-wrapped dictionary of destination table metadata.
pub(crate) type DestinationTablesMetadata = Arc<BTreeMap<TableId, DestinationTableMetadata>>;

/// Trait for storing and retrieving table states, durable replication progress,
/// and destination metadata.
///
/// [`StateStore`] implementations are responsible for defining how table
/// states, replication progress, and destination table metadata are stored and
/// retrieved.
///
/// Implementations should ensure thread-safety and handle concurrent access to
/// the data.
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
    /// This should be called once at program start to load the state into the
    /// cache and then use only the `get_X` methods to access the states.
    /// Updating the state by calling the `update_table_state`
    /// updates in both the cache and the persistent store, so no need to
    /// ever load the states again.
    fn load_table_states(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Updates multiple table states atomically in both the cache and the
    /// persistent store.
    ///
    /// All state updates are applied as a single atomic operation. If any
    /// update fails, the entire operation is rolled back.
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

    /// Returns the durable flush LSN for a replication worker, if one has been
    /// stored.
    fn get_replication_progress(
        &self,
        worker_type: WorkerType,
    ) -> impl Future<Output = EtlResult<Option<PgLsn>>> + Send;

    /// Monotonically upserts the durable flush LSN for a replication worker.
    ///
    /// Implementations must never move stored progress backward. The returned
    /// value is the LSN stored after applying the monotonic update.
    fn upsert_replication_progress(
        &self,
        worker_type: WorkerType,
        flush_lsn: PgLsn,
    ) -> impl Future<Output = EtlResult<PgLsn>> + Send;

    /// Deletes durable replication progress for a replication worker.
    ///
    /// This is used when the worker's slot lineage is intentionally reset.
    fn delete_replication_progress(
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

    /// Returns destination table metadata only when the schema is fully
    /// applied.
    ///
    /// This is the preferred accessor for normal application code. It converts
    /// the raw metadata into [`AppliedDestinationTableMetadata`] and returns an
    /// error if the stored metadata exists but is not in the applied state.
    ///
    /// Recovery and DDL transition code should continue to use
    /// [`Self::get_destination_table_metadata`] directly so it can reason about
    /// `Applying` metadata explicitly.
    fn get_applied_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<Option<AppliedDestinationTableMetadata>>> + Send;

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
