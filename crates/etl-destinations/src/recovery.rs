//! Shared helpers for destination schema-transition safety and recovery.

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

#[cfg(test)]
mod tests {
    use etl::{
        error::ErrorKind,
        schema::{PgLsn, ReplicationMask, SnapshotId, TableId},
    };

    use crate::recovery::ensure_relation_schema_transition;

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
        .expect("an identical applied schema should be accepted");
        ensure_relation_schema_transition(
            "Test",
            table_id,
            applied_snapshot_id,
            &applied_mask,
            test_snapshot_id(300_u64, 300_u64),
            &ReplicationMask::from_bytes(vec![1, 0, 1]),
        )
        .expect("a newer schema snapshot should be accepted");
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
        .expect_err("an older schema snapshot should be rejected");
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
        .expect_err("an equal snapshot with a different mask should be rejected");
        assert_eq!(ambiguous_error.kind(), ErrorKind::DestinationSchemaRewind);
    }
}
