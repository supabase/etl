use std::{collections::BTreeMap, sync::Arc};

use crate::{
    error::EtlResult,
    schema::{SnapshotId, TableId, TableSchema},
    store::CachedStore,
};

/// Stores table schemas versioned by commit LSN, then message LSN.
///
/// Implementations follow the shared-cache contract in [`crate::store`].
pub trait SchemaStore: CachedStore {
    /// Returns the newest cached schema at or before the requested snapshot,
    /// or `None` if no version qualifies.
    ///
    /// The cache must contain all retained versions to avoid selecting an
    /// outdated schema. Implementations may refresh it when uninitialized or
    /// invalidated.
    fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;

    /// Returns all cached table schemas.
    ///
    /// Implementations may refresh an uninitialized or invalidated cache.
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;

    /// Stores a table schema in both the cache and the persistent store.
    ///
    /// The schema's `snapshot_id` field determines which version this schema
    /// represents.
    fn store_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send;

    /// Prunes obsolete table schema versions for tables at cleanup points.
    ///
    /// For each table id, implementations should find the newest schema version
    /// at or before that table's retention boundary, preserve it, and remove
    /// older versions. Versions newer than the boundary must also be preserved
    /// because PostgreSQL may replay them, or the destination may need them for
    /// schema application. The ordered map keeps per-table cleanup iteration
    /// deterministic.
    ///
    /// Implementations may skip completed boundaries unless schema writes or
    /// lifecycle changes require another pass.
    ///
    /// Returns the number of schema versions removed.
    fn prune_table_schemas(
        &self,
        _retention_snapshot_ids: BTreeMap<TableId, SnapshotId>,
    ) -> impl Future<Output = EtlResult<u64>> + Send;
}
