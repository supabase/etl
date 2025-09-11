use std::fmt;
use std::fmt::Formatter;

use crate::types::TableId;

/// Enum representing the types of workers that can be involved with a replication task.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    Apply,
    TableSync { table_id: TableId },
}

impl fmt::Display for WorkerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            WorkerType::Apply => write!(f, "apply"),
            WorkerType::TableSync { .. } => write!(f, "table_sync"),
        }
    }
}
