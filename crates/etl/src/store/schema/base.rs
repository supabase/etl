use std::{collections::BTreeMap, sync::Arc};

use crate::{
    bail,
    error::{ErrorKind, EtlResult},
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
    /// Use this to resolve the schema in effect at a replication boundary,
    /// such as a checkpoint or connection bootstrap, where no exact version is
    /// known. Use [`SchemaStore::get_exact_table_schema`] when the snapshot
    /// identifies a stored version, since this method may return an older one.
    ///
    /// The cache must contain all retained versions to avoid selecting an
    /// outdated schema. Implementations may refresh it when uninitialized or
    /// invalidated.
    ///
    /// Use [`SnapshotId::before_lsn`] to convert an exclusive replication
    /// checkpoint into this inclusive snapshot bound.
    fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;

    /// Returns the schema stored at exactly the requested snapshot.
    ///
    /// Use this when the snapshot identifies a stored version, such as one
    /// recorded in destination metadata or announced by a DDL message. Unlike
    /// [`SchemaStore::get_table_schema`], it never substitutes an older version
    /// and fails with [`ErrorKind::MissingTableSchema`] when the requested
    /// version is not stored.
    fn get_exact_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send
    where
        Self: Sync,
    {
        async move {
            match self.get_table_schema(table_id, snapshot_id).await? {
                Some(table_schema) if table_schema.snapshot_id == snapshot_id => Ok(table_schema),
                Some(table_schema) => bail!(
                    ErrorKind::MissingTableSchema,
                    "Exact table schema snapshot not found",
                    format!(
                        "Table {table_id} needs schema snapshot {snapshot_id}, but the newest \
                         stored snapshot at or before it is {}",
                        table_schema.snapshot_id
                    )
                ),
                None => bail!(
                    ErrorKind::MissingTableSchema,
                    "Exact table schema snapshot not found",
                    format!(
                        "Table {table_id} needs schema snapshot {snapshot_id}, but no snapshot at \
                         or before it is stored"
                    )
                ),
            }
        }
    }

    /// Returns every retained schema version for every table.
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
    /// Checkpoint-derived bounds use [`SnapshotId::before_lsn`], additionally
    /// limited by any older snapshot needed for destination recovery.
    ///
    /// Returns the number of schema versions removed.
    fn prune_table_schemas(
        &self,
        _retention_snapshot_ids: BTreeMap<TableId, SnapshotId>,
    ) -> impl Future<Output = EtlResult<u64>> + Send;
}
