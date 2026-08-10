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
    /// The replication mask indicating which columns are replicated.
    ///
    /// Each byte is 0 (not replicated) or 1 (replicated), with the index
    /// corresponding to the column's ordinal position in the schema.
    pub replication_mask: ReplicationMask,
    /// The schema version before the current change. None for initial schemas.
    ///
    /// Destinations can use this for recovery when `schema_status` is
    /// `Applying`. The physical DDL may have committed, rolled back, or only
    /// partially completed, so recovery must reconcile the destination
    /// idempotently rather than assuming one outcome.
    pub previous_snapshot_id: Option<SnapshotId>,
    /// The replication mask before the current change. None for initial
    /// schemas.
    ///
    /// A snapshot ID alone does not identify the replicated destination
    /// endpoint because publication column lists can change independently of
    /// the physical table schema. Destinations use this together with
    /// [`Self::previous_snapshot_id`] when recovering an interrupted change.
    pub previous_replication_mask: Option<ReplicationMask>,
    /// Status of the current schema change operation.
    ///
    /// If `Applying` is found on startup, the destination schema may be in
    /// an unknown state and recovery may be needed depending on the
    /// destination.
    pub schema_status: DestinationTableSchemaStatus,
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
            replication_mask,
            previous_snapshot_id: None,
            previous_replication_mask: None,
            schema_status: DestinationTableSchemaStatus::Applying,
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
            replication_mask,
            previous_snapshot_id: None,
            previous_replication_mask: None,
            schema_status: DestinationTableSchemaStatus::Applied,
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

    /// Validates the durable schema-recovery endpoint.
    ///
    /// Initial creation has neither previous field, an in-progress schema
    /// change has both, and completed metadata has neither. Half-populated or
    /// stale recovery state is rejected instead of being inferred.
    pub(crate) fn validate_recovery_endpoint(&self) -> EtlResult<()> {
        let has_previous_snapshot = self.previous_snapshot_id.is_some();
        let has_previous_mask = self.previous_replication_mask.is_some();
        if has_previous_snapshot != has_previous_mask {
            return Err(crate::etl_error!(
                ErrorKind::InvalidState,
                "Destination table recovery endpoint is incomplete",
                format!(
                    "Table '{}' must store the previous snapshot and replication mask together",
                    self.destination_table_id
                )
            ));
        }
        if self.is_applied() && has_previous_snapshot {
            return Err(crate::etl_error!(
                ErrorKind::InvalidState,
                "Applied destination table metadata contains stale recovery state",
                format!(
                    "Table '{}' is applied but still stores a previous schema endpoint",
                    self.destination_table_id
                )
            ));
        }

        Ok(())
    }

    /// Transitions this metadata to applied status.
    ///
    /// Clears the previous endpoint since the change completed successfully.
    pub fn to_applied(mut self) -> Self {
        self.schema_status = DestinationTableSchemaStatus::Applied;
        self.previous_snapshot_id = None;
        self.previous_replication_mask = None;
        self
    }

    /// Updates the schema state for a new schema change.
    ///
    /// Stores the current snapshot and replication mask as the previous
    /// endpoint before updating, enabling destination-specific recovery if the
    /// change is interrupted.
    pub fn with_schema_change(
        mut self,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
        status: DestinationTableSchemaStatus,
    ) -> Self {
        let previous_snapshot_id = self.snapshot_id;
        let previous_replication_mask = self.replication_mask.clone();

        self.snapshot_id = snapshot_id;
        self.replication_mask = replication_mask;
        self.previous_snapshot_id = Some(previous_snapshot_id);
        self.previous_replication_mask = Some(previous_replication_mask);
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
        self.validate_recovery_endpoint()?;
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
    use crate::schema::PgLsn;

    #[test]
    fn schema_change_metadata_preserves_both_logical_endpoints() {
        let target_snapshot_id = SnapshotId::new(PgLsn::from(20), PgLsn::from(21));
        let target_mask = ReplicationMask::from_bytes(vec![1, 1, 1]);
        let previous_snapshot_id = SnapshotId::new(PgLsn::from(10), PgLsn::from(11));
        let previous_mask = ReplicationMask::from_bytes(vec![1, 0, 1]);

        let metadata = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            previous_snapshot_id,
            previous_mask.clone(),
        )
        .with_schema_change(
            target_snapshot_id,
            target_mask.clone(),
            DestinationTableSchemaStatus::Applying,
        );

        assert_eq!(metadata.snapshot_id, target_snapshot_id);
        assert_eq!(metadata.replication_mask, target_mask);
        assert_eq!(metadata.previous_snapshot_id, Some(previous_snapshot_id));
        assert_eq!(metadata.previous_replication_mask, Some(previous_mask));
        assert!(metadata.is_applying());
    }

    #[test]
    fn applied_metadata_clears_the_recovery_endpoint() {
        let metadata = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            SnapshotId::initial(),
            ReplicationMask::from_bytes(vec![1, 0]),
        )
        .with_schema_change(
            SnapshotId::new(PgLsn::from(20), PgLsn::from(21)),
            ReplicationMask::from_bytes(vec![1, 1]),
            DestinationTableSchemaStatus::Applying,
        )
        .to_applied();

        assert_eq!(metadata.previous_snapshot_id, None);
        assert_eq!(metadata.previous_replication_mask, None);
        assert!(metadata.is_applied());
    }

    #[test]
    fn recovery_endpoint_rejects_half_populated_and_stale_states() {
        let mut metadata = DestinationTableMetadata::new_applying(
            "users".to_owned(),
            SnapshotId::initial(),
            ReplicationMask::from_bytes(vec![1]),
        );
        metadata.previous_snapshot_id = Some(SnapshotId::initial());
        assert_eq!(
            metadata.validate_recovery_endpoint().unwrap_err().kind(),
            ErrorKind::InvalidState
        );

        metadata.previous_replication_mask = Some(ReplicationMask::from_bytes(vec![1]));
        metadata.previous_snapshot_id = None;
        assert_eq!(
            metadata.validate_recovery_endpoint().unwrap_err().kind(),
            ErrorKind::InvalidState
        );

        metadata.previous_snapshot_id = Some(SnapshotId::initial());
        metadata.schema_status = DestinationTableSchemaStatus::Applied;
        assert_eq!(
            metadata.validate_recovery_endpoint().unwrap_err().kind(),
            ErrorKind::InvalidState
        );
    }
}
