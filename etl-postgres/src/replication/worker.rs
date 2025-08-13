use crate::schema::TableId;

/// Enum representing the types of workers that can be involved with a replication task.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    Apply,
    TableSync { table_id: TableId },
}
