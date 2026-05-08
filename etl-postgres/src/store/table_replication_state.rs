use crate::types::TableId;

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

impl TableReplicationStateType {
    /// Returns the Postgres enum value for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::DataSync => "data_sync",
            Self::FinishedCopy => "finished_copy",
            Self::SyncDone => "sync_done",
            Self::Ready => "ready",
            Self::Errored => "errored",
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
