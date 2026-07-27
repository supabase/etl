//! Shared helpers for recovering interrupted destination schema changes.

#[cfg(any(feature = "clickhouse", feature = "ducklake"))]
use etl::schema::{ReplicationMask, TableSchema};

/// Builds a conservative previous replication mask for interrupted DDL.
///
/// [`etl::destination::DestinationTableMetadata`] stores the target mask, not
/// the previous mask. Treat every previous-schema column as potentially
/// present so an idempotent DDL planner removes target-excluded columns if they
/// exist. Callers must separately reconcile target columns that may have been
/// absent from the previous physical schema.
#[cfg(any(feature = "clickhouse", feature = "ducklake"))]
pub(crate) fn conservative_previous_replication_mask(
    previous_schema: &TableSchema,
) -> ReplicationMask {
    ReplicationMask::all(previous_schema)
}

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "clickhouse", feature = "ducklake"))]
    use std::sync::Arc;

    #[cfg(any(feature = "clickhouse", feature = "ducklake"))]
    use etl::schema::{
        ColumnSchema, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId, TableName,
        TableSchema, Type,
    };

    #[cfg(any(feature = "clickhouse", feature = "ducklake"))]
    use crate::recovery::conservative_previous_replication_mask;

    #[cfg(any(feature = "clickhouse", feature = "ducklake"))]
    #[test]
    fn conservative_previous_mask_exposes_filter_contraction_to_diff() {
        let previous_schema = Arc::new(TableSchema::new(
            TableId::new(5),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("hidden".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let target_schema = Arc::new(TableSchema::with_snapshot_id(
            previous_schema.id,
            previous_schema.name.clone(),
            previous_schema.column_schemas.clone(),
            SnapshotId::from(42_u64),
        ));
        let previous_mask = conservative_previous_replication_mask(&previous_schema);
        assert_eq!(previous_mask.as_slice(), &[1, 1]);
        let previous = ReplicatedTableSchema::from_mask(previous_schema, previous_mask);
        let target = ReplicatedTableSchema::from_mask(
            target_schema,
            ReplicationMask::from_bytes(vec![1, 0]),
        );

        let diff = previous.diff(&target);

        assert_eq!(
            diff.columns_to_remove.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["hidden"]
        );
    }

    #[cfg(any(feature = "clickhouse", feature = "ducklake"))]
    #[test]
    fn conservative_previous_mask_exposes_removed_source_column_to_diff() {
        let previous_schema = Arc::new(TableSchema::new(
            TableId::new(6),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("old_col".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let target_schema = Arc::new(TableSchema::with_snapshot_id(
            previous_schema.id,
            previous_schema.name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
            SnapshotId::from(43_u64),
        ));
        let previous_mask = conservative_previous_replication_mask(&previous_schema);
        let previous = ReplicatedTableSchema::from_mask(previous_schema, previous_mask);
        let target = ReplicatedTableSchema::all(target_schema);

        let diff = previous.diff(&target);

        assert_eq!(
            diff.columns_to_remove.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["old_col"]
        );
    }
}
