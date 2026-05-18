use std::fmt::{Display, Formatter};

use etl_postgres::{replication::slots::EtlReplicationSlot, types::TableId};

/// Type of worker driving replication.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WorkerType {
    /// The main apply worker that coordinates table sync workers.
    Apply,
    /// A table sync worker that synchronizes a specific table.
    TableSync {
        /// The table being synchronized.
        table_id: TableId,
    },
}

impl WorkerType {
    /// Builds an [`EtlReplicationSlot`] for this worker type.
    pub(crate) fn build_etl_replication_slot(&self, pipeline_id: u64) -> EtlReplicationSlot {
        match self {
            Self::Apply => EtlReplicationSlot::Apply { pipeline_id },
            Self::TableSync { table_id } => {
                EtlReplicationSlot::TableSync { pipeline_id, table_id: *table_id }
            }
        }
    }

    /// Returns a low-cardinality worker type label for metrics and tags.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::TableSync { .. } => "table_sync",
        }
    }

    /// Returns the durable progress table ID used by the state store.
    pub(crate) fn progress_table_id(self) -> Option<TableId> {
        match self {
            Self::Apply => None,
            Self::TableSync { table_id } => Some(table_id),
        }
    }
}

impl Display for WorkerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
