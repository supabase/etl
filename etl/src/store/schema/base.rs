use std::{collections::HashSet, sync::Arc};

use crate::{
    error::EtlResult,
    types::{SnapshotId, TableId, TableSchema},
};

/// Trait for storing and retrieving database table schema information.
///
/// [`SchemaStore`] implementations are responsible for defining how the schema
/// information is stored and retrieved. The store supports schema versioning
/// where each schema version is identified by a snapshot_id (the start_lsn of
/// the DDL message that created it). Stores may prune obsolete versions after
/// replication progress has been acknowledged.
///
/// Implementations should ensure thread-safety and handle concurrent access to
/// the data.
pub trait SchemaStore {
    /// Returns the table schema for the given table at the specified snapshot
    /// point.
    ///
    /// Returns the schema version with the largest snapshot_id that is <= the
    /// requested snapshot_id. If not found in cache, loads from the
    /// persistent store. As an optimization, also loads the latest schema
    /// version when fetching from the database.
    ///
    /// Returns `None` if no schema version exists for the table at or before
    /// the given snapshot.
    fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;

    /// Returns all cached table schemas.
    ///
    /// Does not read from the persistent store.
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;

    /// Loads table schemas from the persistent state into the cache.
    ///
    /// This should be called once the program starts to load the schemas into
    /// the cache.
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores a table schema in both the cache and the persistent store.
    ///
    /// The schema's `snapshot_id` field determines which version this schema
    /// represents.
    fn store_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send;

    /// Prunes obsolete table schema versions for tables at a confirmed LSN.
    ///
    /// For each table id, implementations should find the newest schema
    /// version at or before `confirmed_flush_lsn`, preserve it, and remove
    /// older versions. The LSN must come from the replication slot's actual
    /// `confirmed_flush_lsn`, not an optimistic status update, because cleanup
    /// must not delete schemas PostgreSQL may still replay. Versions newer than
    /// `confirmed_flush_lsn` must also be preserved because PostgreSQL may
    /// replay the DDL that created them.
    ///
    /// Returns the number of schema versions removed.
    fn prune_table_schemas(
        &self,
        _table_ids: HashSet<TableId>,
        _confirmed_flush_lsn: SnapshotId,
    ) -> impl Future<Output = EtlResult<u64>> + Send {
        async { Ok(0) }
    }
}
