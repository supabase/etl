use thiserror::Error;

use crate::types::{SnapshotId, TableId};

/// Errors from parsing destination table schema status values.
#[derive(Debug, Error)]
pub enum DestinationTableSchemaStatusParseError {
    /// The stored schema status value is not known.
    #[error("Unknown destination schema status '{value}'")]
    Unknown {
        /// Unknown stored schema status value.
        value: String,
    },
}

/// Database enum type for destination table schema status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationTableSchemaStatus {
    /// A schema change is currently being applied.
    Applying,
    /// The schema has been successfully applied.
    Applied,
}

impl TryFrom<&str> for DestinationTableSchemaStatus {
    type Error = DestinationTableSchemaStatusParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "applying" => Ok(Self::Applying),
            "applied" => Ok(Self::Applied),
            _ => Err(DestinationTableSchemaStatusParseError::Unknown { value: value.to_owned() }),
        }
    }
}

impl From<DestinationTableSchemaStatus> for &'static str {
    fn from(value: DestinationTableSchemaStatus) -> Self {
        match value {
            DestinationTableSchemaStatus::Applying => "applying",
            DestinationTableSchemaStatus::Applied => "applied",
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
