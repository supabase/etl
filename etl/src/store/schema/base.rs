use std::{collections::HashMap, sync::Arc};

use tokio_postgres::types::PgLsn;

use crate::{
    error::EtlResult,
    types::{SnapshotId, TableId, TableSchema},
};

/// Per-table schema cleanup retention boundary.
///
/// Retention can be bounded by a stored schema snapshot that the destination
/// still needs, or by the replication slot's confirmed flush LSN. Both are LSN
/// values, but the variant records why that boundary was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableSchemaRetention {
    /// Retain schemas according to a destination-useful schema snapshot.
    SnapshotId(SnapshotId),
    /// Retain schemas according to the replication slot's confirmed flush LSN.
    ConfirmedFlushLsn(PgLsn),
}

impl TableSchemaRetention {
    /// Returns the underlying LSN used as the retention boundary.
    pub fn to_lsn(self) -> PgLsn {
        match self {
            Self::SnapshotId(snapshot_id) => snapshot_id.into(),
            Self::ConfirmedFlushLsn(lsn) => lsn,
        }
    }
}

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

    /// Prunes obsolete table schema versions for tables at safe cleanup points.
    ///
    /// For each table id, implementations should find the newest schema version
    /// at or before that table's retention LSN, preserve it, and remove older
    /// versions. Versions newer than the retention LSN must also be preserved
    /// because PostgreSQL may replay them, or the destination may need them for
    /// schema application.
    ///
    /// Returns the number of schema versions removed.
    fn prune_table_schemas(
        &self,
        _table_schema_retentions: HashMap<TableId, TableSchemaRetention>,
    ) -> impl Future<Output = EtlResult<u64>> + Send {
        async { Ok(0) }
    }
}
