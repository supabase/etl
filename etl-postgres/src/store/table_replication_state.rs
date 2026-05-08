use thiserror::Error;

use crate::types::TableId;

/// Errors from parsing table replication state values.
#[derive(Debug, Error)]
pub enum TableReplicationStateTypeParseError {
    /// The stored state value is not a known table replication state.
    #[error("Unknown table replication state '{value}'")]
    Unknown {
        /// Unknown stored state value.
        value: String,
    },
}

/// Database enum type for table replication states.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableReplicationStateType {
    /// Initial state.
    Init,
    /// Data sync state.
    DataSync,
    /// Finished copy state.
    FinishedCopy,
    /// Sync done state.
    SyncDone,
    /// Ready state.
    Ready,
    /// Errored state.
    Errored,
}

impl TryFrom<&str> for TableReplicationStateType {
    type Error = TableReplicationStateTypeParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "init" => Ok(Self::Init),
            "data_sync" => Ok(Self::DataSync),
            "finished_copy" => Ok(Self::FinishedCopy),
            "sync_done" => Ok(Self::SyncDone),
            "ready" => Ok(Self::Ready),
            "errored" => Ok(Self::Errored),
            _ => Err(TableReplicationStateTypeParseError::Unknown { value: value.to_owned() }),
        }
    }
}

impl From<TableReplicationStateType> for &'static str {
    fn from(value: TableReplicationStateType) -> Self {
        match value {
            TableReplicationStateType::Init => "init",
            TableReplicationStateType::DataSync => "data_sync",
            TableReplicationStateType::FinishedCopy => "finished_copy",
            TableReplicationStateType::SyncDone => "sync_done",
            TableReplicationStateType::Ready => "ready",
            TableReplicationStateType::Errored => "errored",
        }
    }
}

/// Database row representation of table replication state.
#[derive(Debug, Clone)]
pub struct TableReplicationStateRow {
    /// Row ID.
    pub id: i64,
    /// Pipeline ID.
    pub pipeline_id: i64,
    /// Source table ID.
    pub table_id: TableId,
    /// Stored replication state.
    pub state: TableReplicationStateType,
    /// Stored state metadata.
    pub metadata: Option<serde_json::Value>,
    /// Previous state row ID.
    pub prev: Option<i64>,
    /// Whether this is the current state row.
    pub is_current: bool,
}

impl TableReplicationStateRow {
    /// Returns the state type without deserializing metadata.
    #[must_use]
    pub fn state_type(&self) -> TableReplicationStateType {
        self.state
    }
}
