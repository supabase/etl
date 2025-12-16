use etl_postgres::types::{ReplicationMask, SnapshotId};

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
    /// Status of the current schema change operation.
    ///
    /// If `Applying` is found on startup, the destination schema may be in
    /// an unknown state and recovery is needed.
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
    /// Initializes with `Applying` status since the table creation is in progress.
    pub fn new_applying(
        destination_table_id: String,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
    ) -> Self {
        Self {
            destination_table_id,
            snapshot_id,
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
    pub fn to_applied(mut self) -> Self {
        self.schema_status = DestinationTableSchemaStatus::Applied;
        self
    }

    /// Updates the schema state for a new schema change.
    pub fn with_schema_change(
        mut self,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
        status: DestinationTableSchemaStatus,
    ) -> Self {
        self.snapshot_id = snapshot_id;
        self.replication_mask = replication_mask;
        self.schema_status = status;
        self
    }
}
