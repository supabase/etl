//! Shared helpers for destination schema-transition safety and recovery.

use std::fmt::Display;

use etl::{
    destination::DestinationTableMetadata,
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::{ColumnAlteration, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId},
};
use tracing::warn;

/// Warns that a destination is intentionally skipping a column type change.
pub(crate) fn warn_unsupported_column_type_change(
    destination_name: &str,
    destination_table_id: impl Display,
    alteration: &ColumnAlteration,
) {
    let before = alteration.before_column_schema();
    let after = alteration.after_column_schema();
    warn!(
        destination_name,
        destination_table_id = %destination_table_id,
        column_name = %before.name,
        before_data_type = before.typ.name(),
        before_type_modifier = before.modifier,
        after_data_type = after.typ.name(),
        after_type_modifier = after.modifier,
        "{destination_name} column type changes are currently unsupported; subsequent schema \
         changes and row writes may fail or behave unpredictably until type-change support is \
         implemented"
    );
}

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

/// Requires an arriving schema to match the exact endpoint in destination
/// metadata.
///
/// Caches may avoid remote work after this check, but they cannot replace it:
/// the durable snapshot ID and replication mask define the only row shape the
/// current destination table may accept.
pub(crate) fn ensure_destination_schema_matches_metadata(
    destination_name: &str,
    table_id: TableId,
    metadata: &DestinationTableMetadata,
    received_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    let received_snapshot_id = received_schema.inner().snapshot_id;
    let received_replication_mask = received_schema.replication_mask();
    if metadata.snapshot_id() == received_snapshot_id
        && metadata.replication_mask() == received_replication_mask
    {
        return Ok(());
    }

    Err(etl_error!(
        ErrorKind::DestinationSchemaRewind,
        "Destination metadata does not match the received schema",
        format!(
            "{destination_name} table {table_id} has destination metadata for snapshot {} and \
             replication mask {}, but received snapshot {received_snapshot_id} and replication \
             mask {received_replication_mask}. Recover the recorded destination operation or \
             resynchronize the table before retrying.",
            metadata.snapshot_id(),
            metadata.replication_mask(),
        )
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etl::{
        destination::DestinationTableMetadata,
        error::ErrorKind,
        schema::{
            ColumnSchema, PgLsn, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
            TableName, TableSchema, Type,
        },
    };

    use crate::recovery::{
        ensure_destination_schema_matches_metadata, ensure_relation_schema_transition,
    };

    /// Creates a synthetic composite snapshot ID for tests.
    fn test_snapshot_id(commit_lsn: u64, message_lsn: u64) -> SnapshotId {
        SnapshotId::new(PgLsn::from(commit_lsn), PgLsn::from(message_lsn))
    }

    #[test]
    fn relation_schema_transition_accepts_identical_or_newer_state() {
        let table_id = TableId::new(7);
        let applied_snapshot_id = test_snapshot_id(200_u64, 200_u64);
        let applied_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);

        ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            applied_snapshot_id,
            &applied_mask,
        )
        .unwrap();
        ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            test_snapshot_id(300_u64, 300_u64),
            &ReplicationMask::from_bytes(vec![1, 0, 1]),
        )
        .unwrap();
    }

    #[test]
    fn relation_schema_transition_rejects_older_snapshot() {
        let table_id = TableId::new(7);
        let applied_snapshot_id = test_snapshot_id(200_u64, 200_u64);
        let applied_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);
        let older_error = ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            test_snapshot_id(100_u64, 100_u64),
            &applied_mask,
        )
        .unwrap_err();
        assert_eq!(older_error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[test]
    fn relation_schema_transition_rejects_different_mask_at_equal_snapshot() {
        let table_id = TableId::new(7);
        let applied_snapshot_id = test_snapshot_id(200_u64, 200_u64);
        let applied_mask = ReplicationMask::from_bytes(vec![1, 1, 0]);
        let ambiguous_error = ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            applied_snapshot_id,
            &ReplicationMask::from_bytes(vec![1, 0, 1]),
        )
        .unwrap_err();
        assert_eq!(ambiguous_error.kind(), ErrorKind::DestinationSchemaRewind);
    }

    #[test]
    fn destination_schema_match_requires_snapshot_and_replication_mask() {
        let table_id = TableId::new(7);
        let snapshot_id = test_snapshot_id(200_u64, 200_u64);
        let table_schema = Arc::new(TableSchema::with_snapshot_id(
            table_id,
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false)],
            snapshot_id,
        ));
        let schema = ReplicatedTableSchema::all(table_schema);
        let metadata = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            snapshot_id,
            schema.replication_mask().clone(),
        );

        ensure_destination_schema_matches_metadata("Test", table_id, &metadata, &schema).unwrap();

        let different_snapshot = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            test_snapshot_id(300_u64, 300_u64),
            schema.replication_mask().clone(),
        );
        let snapshot_error = ensure_destination_schema_matches_metadata(
            "Test",
            table_id,
            &different_snapshot,
            &schema,
        )
        .unwrap_err();
        assert_eq!(snapshot_error.kind(), ErrorKind::DestinationSchemaRewind);

        let different_mask = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            snapshot_id,
            ReplicationMask::from_bytes(vec![0]),
        );
        let mask_error =
            ensure_destination_schema_matches_metadata("Test", table_id, &different_mask, &schema)
                .unwrap_err();
        assert_eq!(mask_error.kind(), ErrorKind::DestinationSchemaRewind);
    }
}
