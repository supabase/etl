use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use tokio_postgres::types::PgLsn;

use crate::types::{SizeHint, TableId, TableSchema};

/// Arrow batch for one source table.
#[derive(Debug)]
pub struct TableArrowBatch {
    /// Source table identifier.
    pub table_id: TableId,
    /// Source schema retained for destination-specific policies.
    pub table_schema: Arc<TableSchema>,
    /// User columns only.
    pub batch: RecordBatch,
    /// Approximate in-memory size of the batch payload.
    pub approx_bytes: usize,
}

impl TableArrowBatch {
    /// Creates a new table Arrow batch.
    pub fn new(table_id: TableId, table_schema: Arc<TableSchema>, batch: RecordBatch) -> Self {
        let approx_bytes = batch.get_array_memory_size();

        Self {
            table_id,
            table_schema,
            batch,
            approx_bytes,
        }
    }

    /// Returns the number of rows in the batch.
    pub fn row_count(&self) -> usize {
        self.batch.num_rows()
    }
}

impl SizeHint for TableArrowBatch {
    fn size_hint(&self) -> usize {
        size_of::<Self>() + self.approx_bytes
    }
}

/// Change kind represented by a CDC Arrow batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    /// Inserted rows.
    Insert,
    /// Updated rows.
    Update,
    /// Deleted rows.
    Delete,
}

/// Row image carried by a CDC Arrow batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowImage {
    /// New row image.
    New,
    /// Old row image.
    Old {
        /// Whether the old image only includes key columns.
        key_only: bool,
    },
}

/// CDC Arrow batch plus sequence metadata.
#[derive(Debug)]
pub struct ChangeArrowBatch {
    /// Row payload.
    pub rows: TableArrowBatch,
    /// Change kind for all rows.
    pub change: ChangeKind,
    /// Row image carried by the batch.
    pub row_image: RowImage,
    /// Commit LSNs aligned to batch rows.
    pub commit_lsns: UInt64Array,
    /// Transaction ordinals aligned to batch rows.
    pub tx_ordinals: UInt64Array,
}

impl SizeHint for ChangeArrowBatch {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
            + self.rows.size_hint()
            + self.commit_lsns.get_array_memory_size()
            + self.tx_ordinals.get_array_memory_size()
    }
}

/// Ordered CDC groups for one table.
#[derive(Debug)]
pub struct TableChangeSet {
    /// Table identifier shared by all groups.
    pub table_id: TableId,
    /// Ordered groups preserving source order for this table.
    pub groups: Vec<ChangeArrowBatch>,
}

impl SizeHint for TableChangeSet {
    fn size_hint(&self) -> usize {
        size_of::<Self>() + self.groups.iter().map(SizeHint::size_hint).sum::<usize>()
    }
}

/// Normalized truncate event for destinations.
#[derive(Debug)]
pub struct TruncateBatch {
    /// Truncated tables.
    pub rel_ids: Vec<TableId>,
    /// Commit LSN of the truncate event.
    pub commit_lsn: PgLsn,
    /// Transaction-local ordinal.
    pub tx_ordinal: u64,
    /// PostgreSQL truncate flags.
    pub options: i8,
}

impl SizeHint for TruncateBatch {
    fn size_hint(&self) -> usize {
        size_of::<Self>() + self.rel_ids.len() * size_of::<TableId>()
    }
}

/// Destination-facing streaming batch.
#[derive(Debug)]
pub enum StreamBatch {
    /// Ordered table-local changes.
    Changes(TableChangeSet),
    /// Truncate control event.
    Truncate(TruncateBatch),
}

impl SizeHint for StreamBatch {
    fn size_hint(&self) -> usize {
        match self {
            Self::Changes(changes) => changes.size_hint(),
            Self::Truncate(truncate) => truncate.size_hint(),
        }
    }
}
