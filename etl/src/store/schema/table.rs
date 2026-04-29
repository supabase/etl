use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::types::{PgLsn, SnapshotId, TableId, TableSchema};

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

/// In-memory index of table schema snapshots grouped by table.
///
/// This type owns the rules for snapshot lookup and cleanup so store
/// implementations do not each reimplement retention behavior.
#[derive(Clone, Debug, Default)]
pub(crate) struct TableSchemaSnapshots {
    /// Schema versions keyed first by table and then by snapshot.
    table_schemas: BTreeMap<TableId, BTreeMap<SnapshotId, Arc<TableSchema>>>,
}

impl TableSchemaSnapshots {
    /// Returns the total number of stored schema snapshots.
    pub(crate) fn len(&self) -> usize {
        self.table_schemas.values().map(BTreeMap::len).sum()
    }

    /// Returns the number of stored snapshots for a table.
    pub(crate) fn table_len(&self, table_id: TableId) -> usize {
        self.table_schemas.get(&table_id).map_or(0, BTreeMap::len)
    }

    /// Returns all stored schema snapshots.
    pub(crate) fn all(&self) -> Vec<Arc<TableSchema>> {
        self.table_schemas.values().flat_map(|schemas| schemas.values().map(Arc::clone)).collect()
    }

    /// Returns the newest schema snapshot at or before `snapshot_id`.
    pub(crate) fn get_at_or_before(
        &self,
        table_id: TableId,
        snapshot_id: SnapshotId,
    ) -> Option<Arc<TableSchema>> {
        self.table_schemas
            .get(&table_id)?
            .range(..=snapshot_id)
            .next_back()
            .map(|(_, schema)| Arc::clone(schema))
    }

    /// Inserts or replaces a schema snapshot.
    pub(crate) fn insert(&mut self, table_schema: TableSchema) -> Arc<TableSchema> {
        let table_id = table_schema.id;
        let snapshot_id = table_schema.snapshot_id;
        let table_schema = Arc::new(table_schema);

        self.table_schemas
            .entry(table_id)
            .or_default()
            .insert(snapshot_id, Arc::clone(&table_schema));

        table_schema
    }

    /// Inserts a schema snapshot and keeps at most `max_snapshots` for its
    /// table.
    pub(crate) fn insert_with_eviction(
        &mut self,
        table_schema: TableSchema,
        max_snapshots: usize,
    ) -> Arc<TableSchema> {
        let table_id = table_schema.id;
        let table_schema = self.insert(table_schema);

        if max_snapshots == 0 {
            self.table_schemas.remove(&table_id);
            return table_schema;
        }

        if let Some(schemas) = self.table_schemas.get_mut(&table_id) {
            while schemas.len() > max_snapshots {
                let Some(oldest_snapshot_id) = schemas.keys().next().copied() else {
                    break;
                };

                schemas.remove(&oldest_snapshot_id);
            }
        }

        table_schema
    }

    /// Replaces all stored schema snapshots with the supplied values.
    pub(crate) fn replace_all(&mut self, table_schemas: impl IntoIterator<Item = TableSchema>) {
        self.table_schemas.clear();
        for table_schema in table_schemas {
            self.insert(table_schema);
        }
    }

    /// Removes all schema snapshots for a table.
    pub(crate) fn remove_table(&mut self, table_id: TableId) {
        self.table_schemas.remove(&table_id);
    }

    /// Prunes obsolete schema snapshots according to per-table retention
    /// limits.
    ///
    /// For each table, this preserves the newest schema at or before the
    /// retention LSN and every newer schema. Older schemas are removed.
    pub(crate) fn prune(
        &mut self,
        table_schema_retentions: &HashMap<TableId, TableSchemaRetention>,
    ) -> u64 {
        let mut removed_count = 0u64;

        for (table_id, schemas) in &mut self.table_schemas {
            let Some(retention) = table_schema_retentions.get(table_id) else {
                continue;
            };

            // We find the biggest snapshot id <= than the retention one. We can rely on the
            // sorted properties of the BTreeMap to perform this traversal from
            // the right without finding a maximum by scanning the whole list.
            let retention_snapshot_id = SnapshotId::from(retention.to_lsn());
            let retained_snapshot_id =
                schemas.keys().rfind(|snapshot_id| **snapshot_id <= retention_snapshot_id).copied();

            let Some(retained_snapshot_id) = retained_snapshot_id else {
                continue;
            };

            // We keep all snapshots that are >= than the retained one.
            let before_count = schemas.len();
            schemas.retain(|snapshot_id, _| *snapshot_id >= retained_snapshot_id);
            removed_count =
                removed_count.saturating_add(before_count.saturating_sub(schemas.len()) as u64);
        }

        removed_count
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::Type;

    use super::*;
    use crate::types::{ColumnSchema, TableName};

    /// Builds a schema with a snapshot-specific non-key column.
    fn test_schema(table_id: TableId, snapshot_id: u64) -> TableSchema {
        TableSchema::with_snapshot_id(
            table_id,
            TableName::new("public".to_string(), format!("table_{table_id}")),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT8, -1, 1, Some(1), false),
                ColumnSchema::new(format!("col_at_{snapshot_id}"), Type::TEXT, -1, 2, None, true),
            ],
            SnapshotId::from(snapshot_id),
        )
    }

    #[test]
    fn get_at_or_before_returns_newest_eligible_snapshot() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();

        snapshots.insert(test_schema(table_id, 100));
        snapshots.insert(test_schema(table_id, 300));

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::from(250))
            .expect("schema should exist");
        assert_eq!(schema.snapshot_id, SnapshotId::from(100));

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::from(300))
            .expect("schema should exist");
        assert_eq!(schema.snapshot_id, SnapshotId::from(300));

        assert!(snapshots.get_at_or_before(table_id, SnapshotId::from(50)).is_none());
    }

    #[test]
    fn insert_with_eviction_keeps_newest_snapshots_for_table() {
        let table_id = TableId::new(10);
        let other_table_id = TableId::new(20);
        let mut snapshots = TableSchemaSnapshots::default();

        snapshots.insert_with_eviction(test_schema(table_id, 100), 2);
        snapshots.insert_with_eviction(test_schema(table_id, 200), 2);
        snapshots.insert_with_eviction(test_schema(table_id, 300), 2);
        snapshots.insert_with_eviction(test_schema(other_table_id, 50), 2);

        assert!(snapshots.get_at_or_before(table_id, SnapshotId::from(100)).is_none());
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, SnapshotId::from(250))
                .expect("schema should exist")
                .snapshot_id,
            SnapshotId::from(200)
        );
        assert_eq!(snapshots.table_len(table_id), 2);
        assert_eq!(snapshots.table_len(other_table_id), 1);
    }

    #[test]
    fn prune_preserves_retained_snapshot_and_newer_versions() {
        let table_id = TableId::new(10);
        let other_table_id = TableId::new(20);
        let untouched_table_id = TableId::new(30);
        let mut snapshots = TableSchemaSnapshots::default();

        for snapshot_id in [0, 100, 200, 300] {
            snapshots.insert(test_schema(table_id, snapshot_id));
        }
        for snapshot_id in [0, 150] {
            snapshots.insert(test_schema(other_table_id, snapshot_id));
        }
        snapshots.insert(test_schema(untouched_table_id, 0));

        let removed = snapshots.prune(&HashMap::from([
            (table_id, TableSchemaRetention::SnapshotId(SnapshotId::from(250))),
            (other_table_id, TableSchemaRetention::ConfirmedFlushLsn(SnapshotId::from(150).into())),
        ]));

        assert_eq!(removed, 3);
        assert!(snapshots.get_at_or_before(table_id, SnapshotId::from(100)).is_none());
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, SnapshotId::from(250))
                .expect("schema should exist")
                .snapshot_id,
            SnapshotId::from(200)
        );
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, SnapshotId::from(400))
                .expect("schema should exist")
                .snapshot_id,
            SnapshotId::from(300)
        );
        assert_eq!(snapshots.table_len(other_table_id), 1);
        assert_eq!(snapshots.table_len(untouched_table_id), 1);
    }

    #[test]
    fn prune_skips_table_when_no_snapshot_is_before_retention() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();

        snapshots.insert(test_schema(table_id, 200));

        let removed = snapshots.prune(&HashMap::from([(
            table_id,
            TableSchemaRetention::SnapshotId(SnapshotId::from(100)),
        )]));

        assert_eq!(removed, 0);
        assert_eq!(snapshots.table_len(table_id), 1);
    }
}
