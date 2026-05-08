use crate::types::{SnapshotId, TableId};

/// Database enum type for destination table schema status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationTableSchemaStatus {
    /// A schema change is currently being applied.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

impl DestinationTableSchemaStatus {
    /// Returns the Postgres enum value for this status.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applying => "applying",
            Self::Applied => "applied",
        }
    }
}

/// Database row representation of destination table metadata.
#[derive(Debug, Clone)]
pub struct DestinationTableMetadataRow {
    /// Source table ID.
    pub table_id: TableId,
    /// Destination table ID.
    pub destination_table_id: String,
    /// Current schema snapshot ID.
    pub snapshot_id: SnapshotId,
    /// Previous schema snapshot ID.
    pub previous_snapshot_id: Option<SnapshotId>,
    /// Destination schema status.
    pub schema_status: DestinationTableSchemaStatus,
    /// Replication mask bytes.
    pub replication_mask: Vec<u8>,
}
