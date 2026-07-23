//! Shared helpers for recovering interrupted destination schema changes.

use etl::schema::{ReplicationMask, TableSchema};

/// Builds the best previous-schema replication mask available during recovery.
///
/// [`etl::destination::DestinationTableMetadata`] stores the target mask, not
/// the previous mask. For a source-schema change we project target bits back
/// by ordinal position. Columns that no longer exist in the target schema are
/// treated as previously replicated so the idempotent DDL planner can drop
/// them if they exist.
pub(crate) fn previous_replication_mask_for_recovery(
    previous_schema: &TableSchema,
    target_schema: &TableSchema,
    target_replication_mask: &ReplicationMask,
) -> ReplicationMask {
    let mask = previous_schema
        .column_schemas
        .iter()
        .map(|previous_column| {
            target_schema
                .column_schemas
                .iter()
                .position(|target_column| {
                    target_column.ordinal_position == previous_column.ordinal_position
                })
                .and_then(|index| target_replication_mask.as_slice().get(index).copied())
                .unwrap_or(1)
        })
        .collect();

    ReplicationMask::from_bytes(mask)
}

#[cfg(test)]
mod tests {
    use etl::schema::{
        ColumnSchema, ReplicationMask, SnapshotId, TableId, TableName, TableSchema, Type,
    };

    use crate::recovery::previous_replication_mask_for_recovery;

    #[test]
    fn previous_replication_mask_for_recovery_matches_previous_schema_width() {
        let previous_schema = TableSchema::new(
            TableId::new(4),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("hidden".to_owned(), Type::TEXT, -1, 2, true),
            ],
        );
        let target_schema = TableSchema::with_snapshot_id(
            previous_schema.id,
            previous_schema.name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("hidden".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
            ],
            SnapshotId::from(42_u64),
        );
        let target_mask = ReplicationMask::from_bytes(vec![1, 0, 1]);

        let previous_mask =
            previous_replication_mask_for_recovery(&previous_schema, &target_schema, &target_mask);

        assert_eq!(previous_mask.to_bytes(), vec![1, 0]);
    }

    #[test]
    fn previous_replication_mask_for_recovery_keeps_removed_columns_for_drop_diff() {
        let previous_schema = TableSchema::new(
            TableId::new(5),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("old_col".to_owned(), Type::TEXT, -1, 3, true),
            ],
        );
        let target_schema = TableSchema::with_snapshot_id(
            previous_schema.id,
            previous_schema.name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
            SnapshotId::from(43_u64),
        );
        let target_mask = ReplicationMask::from_bytes(vec![1, 1]);

        let previous_mask =
            previous_replication_mask_for_recovery(&previous_schema, &target_schema, &target_mask);

        assert_eq!(previous_mask.to_bytes(), vec![1, 1, 1]);
    }
}
