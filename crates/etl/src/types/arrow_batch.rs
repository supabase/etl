use std::{
    mem::{size_of, size_of_val},
    sync::Arc,
};

use arrow::{
    array::{Array, UInt64Array},
    record_batch::RecordBatch,
};
use tokio_postgres::types::PgLsn;

use crate::types::{ReplicatedTableSchema, SizeHint, TableId, TableSchema};

/// Arrow batch for one source table.
#[derive(Debug)]
pub struct TableArrowBatch {
    /// Source table identifier.
    pub table_id: TableId,
    /// Arrow row schema retained for destination-specific policies.
    pub table_schema: Arc<TableSchema>,
    /// Source schema and masks represented by this batch.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// User columns only.
    pub batch: RecordBatch,
    /// Approximate in-memory size of the batch payload.
    pub approx_bytes: usize,
}

impl TableArrowBatch {
    /// Creates a new table Arrow batch.
    pub fn new(table_id: TableId, table_schema: Arc<TableSchema>, batch: RecordBatch) -> Self {
        let replicated_table_schema = ReplicatedTableSchema::all(Arc::clone(&table_schema));
        Self::new_with_replicated_table_schema(
            table_id,
            table_schema,
            replicated_table_schema,
            batch,
        )
    }

    /// Creates a new table Arrow batch with explicit replicated schema
    /// metadata.
    pub fn new_with_replicated_table_schema(
        table_id: TableId,
        table_schema: Arc<TableSchema>,
        replicated_table_schema: ReplicatedTableSchema,
        batch: RecordBatch,
    ) -> Self {
        debug_assert_eq!(table_id, table_schema.id);
        debug_assert_eq!(table_id, replicated_table_schema.id());

        let approx_bytes = batch.get_array_memory_size();

        Self { table_id, table_schema, replicated_table_schema, batch, approx_bytes }
    }

    /// Returns this batch with replaced replicated schema metadata.
    pub fn with_replicated_table_schema(
        self,
        replicated_table_schema: ReplicatedTableSchema,
    ) -> Self {
        Self::new_with_replicated_table_schema(
            self.table_id,
            self.table_schema,
            replicated_table_schema,
            self.batch,
        )
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
    /// Missing replicated-column indexes for partial update new-row images.
    pub partial_update_missing_column_indexes: Option<Arc<[usize]>>,
    /// Commit LSNs aligned to batch rows.
    pub commit_lsns: UInt64Array,
    /// Transaction ordinals aligned to batch rows.
    pub tx_ordinals: UInt64Array,
}

impl SizeHint for ChangeArrowBatch {
    fn size_hint(&self) -> usize {
        size_of::<Self>()
            + self.rows.size_hint()
            + self
                .partial_update_missing_column_indexes
                .as_ref()
                .map_or(0, |indexes| size_of_val(indexes.as_ref()))
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
