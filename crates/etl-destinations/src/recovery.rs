//! Shared helpers for destination schema-transition safety and recovery.

#[cfg(feature = "ducklake")]
use etl::schema::TableSchema;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::{ReplicationMask, SnapshotId, TableId},
};

/// Validates that a relation can advance an applied destination schema.
///
/// Supported schema and publication-mask changes receive a new [`SnapshotId`].
/// An equal snapshot with a different mask therefore has no valid ordering and
/// must fail closed instead of driving destination DDL.
pub(crate) fn ensure_relation_schema_transition(
    destination_name: &str,
    table_id: TableId,
    applied_snapshot_id: SnapshotId,
    applied_replication_mask: &ReplicationMask,
    received_snapshot_id: SnapshotId,
    received_replication_mask: &ReplicationMask,
) -> EtlResult<()> {
    if received_snapshot_id < applied_snapshot_id {
        return Err(etl_error!(
            ErrorKind::DestinationSchemaRewind,
            "Destination schema is newer than the replayed schema snapshot",
            format!(
                "{destination_name} table {} received schema snapshot {}, but the destination \
                 already applied snapshot {}. Reverse DDL is not executed because it could delete \
                 newer column data; resynchronize the table to recover.",
                table_id, received_snapshot_id, applied_snapshot_id,
            )
        ));
    }

    if received_snapshot_id == applied_snapshot_id
        && received_replication_mask != applied_replication_mask
    {
        return Err(etl_error!(
            ErrorKind::DestinationSchemaRewind,
            "Relation reused an applied schema snapshot with a different replication mask",
            format!(
                "{destination_name} table {} received schema snapshot {} with replication mask \
                 {}, but the destination already applied the same snapshot with replication mask \
                 {}. Supported publication column-list changes use a newer schema snapshot, and \
                 equal-snapshot relations have no ordering with which to choose a mask; \
                 resynchronize the table to recover.",
                table_id, received_snapshot_id, received_replication_mask, applied_replication_mask,
            )
        ));
    }

    Ok(())
}

/// Builds a conservative previous replication mask for interrupted DDL.
///
/// [`etl::destination::DestinationTableMetadata`] stores the target mask, not
/// the previous mask. Treat every previous-schema column as potentially
/// present so an idempotent DDL planner removes target-excluded columns if they
/// exist. Callers must separately reconcile target columns that may have been
/// absent from the previous physical schema.
#[cfg(feature = "ducklake")]
pub(crate) fn conservative_previous_replication_mask(
    previous_schema: &TableSchema,
) -> ReplicationMask {
    ReplicationMask::all(previous_schema)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "ducklake")]
    use std::sync::Arc;

    #[cfg(feature = "ducklake")]
    use etl::schema::{ColumnSchema, ReplicatedTableSchema, TableName, TableSchema, Type};
    use etl::{
        error::ErrorKind,
        schema::{ReplicationMask, SnapshotId, TableId},
    };

    #[cfg(feature = "ducklake")]
    use crate::recovery::conservative_previous_replication_mask;
    use crate::recovery::ensure_relation_schema_transition;

    #[test]
    fn relation_schema_transition_accepts_identical_or_newer_state() {
        let table_id = TableId::new(7);
        let applied_snapshot_id = SnapshotId::from(200_u64);
        let applied_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);

        ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            applied_snapshot_id,
            &applied_mask,
        )
        .expect("an identical applied schema should be accepted");
        ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            SnapshotId::from(300_u64),
            &ReplicationMask::from_bytes(vec![1, 0, 1]),
        )
        .expect("a newer schema snapshot should be accepted");
    }

    #[test]
    fn relation_schema_transition_rejects_older_snapshot() {
        let table_id = TableId::new(7);
        let applied_snapshot_id = SnapshotId::from(200_u64);
        let applied_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);
        let older_error = ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            SnapshotId::from(100_u64),
            &applied_mask,
        )
        .expect_err("an older schema snapshot should be rejected");
        assert_eq!(older_error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[test]
    fn relation_schema_transition_rejects_different_mask_at_equal_snapshot() {
        let table_id = TableId::new(7);
        let applied_snapshot_id = SnapshotId::from(200_u64);
        let applied_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);
        let ambiguous_error = ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            applied_snapshot_id,
            &ReplicationMask::from_bytes(vec![1, 0, 1]),
        )
        .expect_err("an equal snapshot with a different mask should be rejected");
        assert_eq!(ambiguous_error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[cfg(feature = "ducklake")]
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

    #[cfg(feature = "ducklake")]
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
