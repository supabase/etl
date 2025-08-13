use crate::schema::TableId;

/// Worker type for slot name generation.
#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    Apply,
    TableSync { table_id: TableId },
}