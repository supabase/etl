use crate::{
    error::{ErrorKind, EtlResult},
    schema::{ReplicationMask, SnapshotId},
};

/// Status of the schema at a destination.
///
/// Tracks whether a schema change is in progress or complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationTableSchemaStatus {
    /// A schema change is currently being applied.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

/// Unified metadata for a table at a destination.
///
/// Tracks all destination-related state for a replicated table in a single
/// structure. This structure is created atomically when a table is first
/// replicated to a destination, containing all the information needed to
/// track and manage that table's destination state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationTableMetadata {
    /// The name/identifier of the table in the destination system.
    pub destination_table_id: String,
    /// The snapshot_id of the schema currently applied at the destination.
    pub snapshot_id: SnapshotId,
    /// The schema version before the current change. None for initial schemas.
    ///
    /// Together with [`DestinationTableMetadata::previous_replication_mask`],
    /// this identifies the physical destination schema that was applied before
    /// the in-flight change. If `schema_status` is `Applying` on startup,
    /// destinations use this pair to reconstruct the previous endpoint and
    /// recover the interrupted DDL.
    pub previous_snapshot_id: Option<SnapshotId>,
    /// The replication mask before the current change. None for initial
    /// schemas.
    ///
    /// The mask can change without a new schema snapshot (for example when
    /// the publication's column list changes), so `previous_snapshot_id`
    /// alone cannot identify the previously applied destination schema. Both
    /// previous fields are set and cleared together.
    pub previous_replication_mask: Option<ReplicationMask>,
    /// Status of the current schema change operation.
    ///
    /// If `Applying` is found on startup, the destination schema may be in
    /// an unknown state and recovery may be needed depending on the
    /// destination.
    pub schema_status: DestinationTableSchemaStatus,
    /// The replication mask indicating which columns are replicated.
    ///
    /// Each byte is 0 (not replicated) or 1 (replicated), with the index
    /// corresponding to the column's ordinal position in the schema.
    pub replication_mask: ReplicationMask,
}

impl DestinationTableMetadata {
    /// Creates new metadata for a table being created at the destination.
    ///
    /// Initializes with `Applying` status since the table creation is in
    /// progress. For initial table creation, `previous_snapshot_id` is
    /// None.
    pub fn new_applying(
        destination_table_id: String,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
    ) -> Self {
        Self {
            destination_table_id,
            snapshot_id,
            previous_snapshot_id: None,
            previous_replication_mask: None,
            schema_status: DestinationTableSchemaStatus::Applying,
            replication_mask,
        }
    }

    /// Creates new metadata for a table that has been successfully created.
    ///
    /// Initializes with `Applied` status.
    pub fn new_applied(
        destination_table_id: String,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
    ) -> Self {
        Self {
            destination_table_id,
            snapshot_id,
            previous_snapshot_id: None,
            previous_replication_mask: None,
            schema_status: DestinationTableSchemaStatus::Applied,
            replication_mask,
        }
    }

    /// Returns true if a schema change is in progress.
    pub fn is_applying(&self) -> bool {
        self.schema_status == DestinationTableSchemaStatus::Applying
    }

    /// Returns true if the schema has been applied.
    pub fn is_applied(&self) -> bool {
        self.schema_status == DestinationTableSchemaStatus::Applied
    }

    /// Transitions this metadata to applied status.
    ///
    /// Clears both previous fields since the change completed successfully.
    pub fn to_applied(mut self) -> Self {
        self.schema_status = DestinationTableSchemaStatus::Applied;
        self.previous_snapshot_id = None;
        self.previous_replication_mask = None;
        self
    }

    /// Updates the schema state for a new schema change.
    ///
    /// Sets `previous_snapshot_id` and `previous_replication_mask` to the
    /// current values before updating, enabling recovery of the interrupted
    /// DDL if the change fails partway through.
    pub fn with_schema_change(
        mut self,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
        status: DestinationTableSchemaStatus,
    ) -> Self {
        self.previous_snapshot_id = Some(self.snapshot_id);
        self.previous_replication_mask = Some(self.replication_mask.clone());
        self.snapshot_id = snapshot_id;
        self.replication_mask = replication_mask;
        self.schema_status = status;
        self
    }

    /// Converts this metadata into [`AppliedDestinationTableMetadata`],
    /// returning an error if the schema is not in
    /// [`DestinationTableSchemaStatus::Applied`] state.
    ///
    /// Use this at any point where downstream code must guarantee that the
    /// destination DDL completed successfully before proceeding. The caller
    /// decides whether to propagate the error or handle it (e.g. warn and
    /// skip an optional operation).
    pub fn into_applied(self) -> EtlResult<AppliedDestinationTableMetadata> {
        if !self.is_applied() {
            return Err(crate::etl_error!(
                ErrorKind::InvalidState,
                "Destination table schema is not in applied state",
                format!(
                    "table '{}' has schema_status '{:?}'; the DDL may not have completed — manual \
                     intervention may be required",
                    self.destination_table_id, self.schema_status
                )
            ));
        }
        Ok(AppliedDestinationTableMetadata {
            destination_table_id: self.destination_table_id,
            snapshot_id: self.snapshot_id,
            replication_mask: self.replication_mask,
        })
    }
}

/// Destination table metadata guaranteed to be in
/// [`DestinationTableSchemaStatus::Applied`] state.
///
/// Can only be constructed via [`DestinationTableMetadata::into_applied`],
/// which returns an error if the underlying metadata is not fully applied. Code
/// that accepts this type has a static guarantee that the destination DDL
/// completed successfully and the table is ready for reads and writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedDestinationTableMetadata {
    /// The name/identifier of the table in the destination system.
    pub destination_table_id: String,
    /// The snapshot_id of the schema applied at the destination.
    pub snapshot_id: SnapshotId,
    /// The replication mask indicating which columns are replicated.
    pub replication_mask: ReplicationMask,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn applied_metadata() -> DestinationTableMetadata {
        DestinationTableMetadata::new_applied(
            "dest_table".to_owned(),
            SnapshotId::from(100_u64),
            ReplicationMask::from_bytes(vec![1, 1, 0]),
        )
    }

    #[test]
    fn with_schema_change_captures_previous_snapshot_and_mask() {
        let metadata = applied_metadata().with_schema_change(
            SnapshotId::from(100_u64),
            ReplicationMask::from_bytes(vec![1, 0, 1]),
            DestinationTableSchemaStatus::Applying,
        );

        assert!(metadata.is_applying());
        assert_eq!(metadata.snapshot_id, SnapshotId::from(100_u64));
        assert_eq!(metadata.replication_mask, ReplicationMask::from_bytes(vec![1, 0, 1]));
        assert_eq!(metadata.previous_snapshot_id, Some(SnapshotId::from(100_u64)));
        assert_eq!(
            metadata.previous_replication_mask,
            Some(ReplicationMask::from_bytes(vec![1, 1, 0]))
        );
    }

    #[test]
    fn to_applied_clears_both_previous_fields() {
        let metadata = applied_metadata()
            .with_schema_change(
                SnapshotId::from(101_u64),
                ReplicationMask::from_bytes(vec![1, 0, 1]),
                DestinationTableSchemaStatus::Applying,
            )
            .to_applied();

        assert!(metadata.is_applied());
        assert_eq!(metadata.previous_snapshot_id, None);
        assert_eq!(metadata.previous_replication_mask, None);
    }

    #[test]
    fn new_metadata_has_no_previous_fields() {
        let applying = DestinationTableMetadata::new_applying(
            "dest_table".to_owned(),
            SnapshotId::initial(),
            ReplicationMask::from_bytes(vec![1]),
        );
        assert_eq!(applying.previous_snapshot_id, None);
        assert_eq!(applying.previous_replication_mask, None);

        let applied = applied_metadata();
        assert_eq!(applied.previous_snapshot_id, None);
        assert_eq!(applied.previous_replication_mask, None);
    }
}
