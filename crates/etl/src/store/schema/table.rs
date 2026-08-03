use std::{collections::BTreeMap, sync::Arc};

use crate::schema::{SnapshotId, TableId, TableSchema};

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
    pub(crate) fn total_snapshots_count(&self) -> usize {
        self.table_schemas.values().map(BTreeMap::len).sum()
    }

    /// Returns the number of stored schema snapshots for a table.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn snapshots_count(&self, table_id: TableId) -> usize {
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
    /// retention snapshot and every newer schema. Older schemas are removed.
    pub(crate) fn prune(&mut self, retention_snapshot_ids: &BTreeMap<TableId, SnapshotId>) -> u64 {
        let mut removed_count = 0u64;

        for (table_id, schemas) in &mut self.table_schemas {
            let Some(retention_snapshot_id) = retention_snapshot_ids.get(table_id) else {
                continue;
            };

            // The map is ordered by `(commit_lsn, message_lsn)`, so the last
            // entry at or below the inclusive boundary is the schema active at
            // that point.
            let retained_snapshot_id = schemas
                .range(..=retention_snapshot_id)
                .next_back()
                .map(|(snapshot_id, _)| *snapshot_id);

            let Some(retained_snapshot_id) = retained_snapshot_id else {
                continue;
            };

            // Keep the retained snapshot and every newer snapshot.
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
    use tokio_postgres::types::{PgLsn, Type};

    use super::*;
    use crate::schema::{ColumnSchema, TableName};

    /// Creates a synthetic composite snapshot ID for tests.
    fn test_snapshot_id(commit_lsn: u64, message_lsn: u64) -> SnapshotId {
        SnapshotId::new(PgLsn::from(commit_lsn), PgLsn::from(message_lsn))
    }

    /// Builds a schema with a snapshot-specific non-key column.
    fn test_schema(table_id: TableId, snapshot_id: u64) -> TableSchema {
        test_schema_at(table_id, test_snapshot_id(snapshot_id, snapshot_id))
    }

    /// Builds a schema at an exact composite snapshot.
    fn test_schema_at(table_id: TableId, snapshot_id: SnapshotId) -> TableSchema {
        TableSchema::with_snapshot_id(
            table_id,
            TableName::new("public".to_owned(), format!("table_{table_id}")),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 2, true),
            ],
            snapshot_id,
        )
    }

    #[test]
    fn get_at_or_before_returns_newest_eligible_snapshot() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();

        snapshots.insert(test_schema(table_id, 100));
        snapshots.insert(test_schema(table_id, 300));

        let schema = snapshots
            .get_at_or_before(table_id, test_snapshot_id(250, 250))
            .expect("schema should exist");
        assert_eq!(schema.snapshot_id, test_snapshot_id(100, 100));

        let schema = snapshots
            .get_at_or_before(table_id, test_snapshot_id(300, 300))
            .expect("schema should exist");
        assert_eq!(schema.snapshot_id, test_snapshot_id(300, 300));

        assert!(snapshots.get_at_or_before(table_id, test_snapshot_id(50, 50)).is_none());
    }

    #[test]
    fn restart_frontier_selects_latest_schema_in_latest_committed_transaction() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();
        let first_snapshot_id = SnapshotId::new(PgLsn::from(300), PgLsn::from(100));
        let second_snapshot_id = SnapshotId::new(PgLsn::from(300), PgLsn::from(200));
        let next_commit_snapshot_id = SnapshotId::new(PgLsn::from(500), PgLsn::from(150));

        snapshots.insert(test_schema(table_id, 0));
        snapshots.insert(test_schema_at(table_id, first_snapshot_id));
        snapshots.insert(test_schema_at(table_id, second_snapshot_id));
        snapshots.insert(test_schema_at(table_id, next_commit_snapshot_id));
        snapshots.insert(test_schema_at(table_id, SnapshotId::max()));

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::at_lsn(PgLsn::from(200)))
            .expect("initial schema should remain eligible");
        assert_eq!(schema.snapshot_id, SnapshotId::initial());

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::at_lsn(PgLsn::from(300)))
            .expect("committed schema should be eligible");
        assert_eq!(schema.snapshot_id, second_snapshot_id);

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::at_lsn(PgLsn::from(499)))
            .expect("previous committed schema should remain active between commits");
        assert_eq!(schema.snapshot_id, second_snapshot_id);

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::at_lsn(PgLsn::from(500)))
            .expect("next committed schema should be eligible");
        assert_eq!(schema.snapshot_id, next_commit_snapshot_id);

        let schema = snapshots
            .get_at_or_before(table_id, SnapshotId::at_lsn(PgLsn::from(u64::MAX)))
            .expect("maximum snapshot should be eligible at the maximum WAL frontier");
        assert_eq!(schema.snapshot_id, SnapshotId::max());
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

        assert!(snapshots.get_at_or_before(table_id, test_snapshot_id(100, 100)).is_none());
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, test_snapshot_id(250, 250))
                .expect("schema should exist")
                .snapshot_id,
            test_snapshot_id(200, 200)
        );
        assert_eq!(snapshots.snapshots_count(table_id), 2);
        assert_eq!(snapshots.snapshots_count(other_table_id), 1);
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

        let removed = snapshots.prune(&BTreeMap::from([
            (table_id, test_snapshot_id(250, 250)),
            (other_table_id, SnapshotId::at_lsn(PgLsn::from(150))),
        ]));

        assert_eq!(removed, 3);
        assert!(snapshots.get_at_or_before(table_id, test_snapshot_id(100, 100)).is_none());
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, test_snapshot_id(250, 250))
                .expect("schema should exist")
                .snapshot_id,
            test_snapshot_id(200, 200)
        );
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, test_snapshot_id(400, 400))
                .expect("schema should exist")
                .snapshot_id,
            test_snapshot_id(300, 300)
        );
        assert_eq!(snapshots.snapshots_count(other_table_id), 1);
        assert_eq!(snapshots.snapshots_count(untouched_table_id), 1);
    }

    #[test]
    fn prune_skips_table_when_no_snapshot_is_before_retention() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();

        snapshots.insert(test_schema(table_id, 200));

        let removed = snapshots.prune(&BTreeMap::from([(table_id, test_snapshot_id(100, 100))]));

        assert_eq!(removed, 0);
        assert_eq!(snapshots.snapshots_count(table_id), 1);
    }

    #[test]
    fn prune_exact_boundary_preserves_earlier_message_in_same_commit() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();
        let first_snapshot_id = SnapshotId::new(PgLsn::from(300), PgLsn::from(100));
        let second_snapshot_id = SnapshotId::new(PgLsn::from(300), PgLsn::from(200));

        snapshots.insert(test_schema(table_id, 0));
        snapshots.insert(test_schema_at(table_id, first_snapshot_id));
        snapshots.insert(test_schema_at(table_id, second_snapshot_id));

        let removed = snapshots.prune(&BTreeMap::from([(table_id, first_snapshot_id)]));

        assert_eq!(removed, 1);
        assert_eq!(snapshots.snapshots_count(table_id), 2);
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, first_snapshot_id)
                .expect("destination snapshot should remain")
                .snapshot_id,
            first_snapshot_id
        );
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, SnapshotId::at_lsn(PgLsn::from(300)))
                .expect("later same-commit snapshot should remain")
                .snapshot_id,
            second_snapshot_id
        );
    }

    #[test]
    fn prune_zero_boundary_preserves_initial_and_newer_snapshots() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();

        snapshots.insert(test_schema_at(table_id, SnapshotId::initial()));
        snapshots.insert(test_schema_at(table_id, test_snapshot_id(2, 2)));

        let removed = snapshots.prune(&BTreeMap::from([(table_id, SnapshotId::initial())]));

        assert_eq!(removed, 0);
        assert_eq!(snapshots.snapshots_count(table_id), 2);
        assert!(snapshots.get_at_or_before(table_id, SnapshotId::initial()).is_some());
    }

    #[test]
    fn prune_is_idempotent_for_repeated_and_out_of_order_boundaries() {
        let table_id = TableId::new(10);
        let mut snapshots = TableSchemaSnapshots::default();

        for snapshot_id in [100, 200, 300] {
            snapshots.insert(test_schema(table_id, snapshot_id));
        }

        let newer_retention = BTreeMap::from([(table_id, SnapshotId::at_lsn(PgLsn::from(350)))]);
        assert_eq!(snapshots.prune(&newer_retention), 2);
        assert_eq!(snapshots.prune(&newer_retention), 0);

        let older_retention = BTreeMap::from([(table_id, SnapshotId::at_lsn(PgLsn::from(250)))]);
        assert_eq!(snapshots.prune(&older_retention), 0);
        assert_eq!(snapshots.snapshots_count(table_id), 1);
        assert_eq!(
            snapshots
                .get_at_or_before(table_id, SnapshotId::max())
                .expect("newest schema should remain")
                .snapshot_id,
            test_snapshot_id(300, 300)
        );
    }
}
