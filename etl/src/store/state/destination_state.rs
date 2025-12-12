use etl_postgres::types::{ReplicationMask, SnapshotId};

/// The state of a schema change operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationSchemaStateType {
    /// A schema change is currently being applied.
    ///
    /// If the system restarts and finds this state, it indicates that a previous
    /// schema change was interrupted and manual intervention may be required.
    /// The previous valid snapshot_id can be derived from table_schemas table.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

/// Represents the state of the schema at a destination.
///
/// Used to track which schema version is currently applied at a destination
/// and to detect interrupted schema changes that require recovery.
///
/// This structure tracks both the snapshot_id and the replication mask, which
/// is needed to correctly reconstruct the [`ReplicatedTableSchema`] for diffing
/// when schema changes occur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationSchemaState {
    /// The current state of the schema change operation.
    pub state: DestinationSchemaStateType,
    /// The current snapshot_id at the destination.
    pub snapshot_id: SnapshotId,
    /// The replication mask indicating which columns are replicated.
    ///
    /// This is stored alongside the snapshot_id so that when a schema change
    /// occurs, we can reconstruct the old [`ReplicatedTableSchema`] with the
    /// correct mask for accurate diffing.
    pub replication_mask: ReplicationMask,
}

impl DestinationSchemaState {
    /// Creates a new state indicating a schema change is being applied.
    pub fn applying(snapshot_id: SnapshotId, replication_mask: ReplicationMask) -> Self {
        Self {
            state: DestinationSchemaStateType::Applying,
            snapshot_id,
            replication_mask,
        }
    }

    /// Creates a new state indicating a schema has been successfully applied.
    pub fn applied(snapshot_id: SnapshotId, replication_mask: ReplicationMask) -> Self {
        Self {
            state: DestinationSchemaStateType::Applied,
            snapshot_id,
            replication_mask,
        }
    }

    /// Returns true if the state indicates a schema change is in progress.
    pub fn is_applying(&self) -> bool {
        self.state == DestinationSchemaStateType::Applying
    }

    /// Returns true if the state indicates the schema has been applied.
    pub fn is_applied(&self) -> bool {
        self.state == DestinationSchemaStateType::Applied
    }

    /// Transitions this state to applied, keeping the same snapshot_id and mask.
    pub fn to_applied(self) -> Self {
        Self {
            state: DestinationSchemaStateType::Applied,
            ..self
        }
    }
}
