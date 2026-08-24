use std::fmt;

use uuid::Uuid;

use crate::data::TableRow;

/// Identifies one execution of an initial table copy against one source
/// snapshot.
///
/// ETL abandons interrupted table-copy attempts instead of resuming them. A
/// restarted copy receives a fresh attempt ID and begins from a new snapshot.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableCopyAttemptId(Uuid);

impl TableCopyAttemptId {
    /// Generates a fresh table-copy attempt ID.
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a table-copy attempt ID from its numeric representation.
    pub const fn from_u128(id: u128) -> Self {
        Self(Uuid::from_u128(id))
    }
}

impl fmt::Display for TableCopyAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// An idempotency key for one logical batch in an initial table copy.
///
/// Redeliveries of the same batch retain the same ID. Distinct batches have
/// distinct IDs even when their rows are identical or belong to different copy
/// attempts. Each ID combines a fresh [`TableCopyAttemptId`] with an
/// attempt-local `u64` sequence allocated across all copy workers. The ID is
/// opaque to destinations and does not define ordering or resumable source
/// progress.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableCopyBatchId {
    /// The initial-copy execution that emitted the batch.
    attempt_id: TableCopyAttemptId,
    /// The attempt-local batch sequence allocated across all copy workers.
    sequence: u64,
}

impl TableCopyBatchId {
    /// Creates a table-copy batch ID from an attempt ID and sequence.
    pub const fn new(attempt_id: TableCopyAttemptId, sequence: u64) -> Self {
        Self { attempt_id, sequence }
    }
}

impl fmt::Display for TableCopyBatchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.attempt_id, self.sequence)
    }
}

/// One nonempty batch emitted during an initial table copy.
#[derive(Debug)]
pub struct TableCopyBatch {
    /// The batch's idempotency key.
    id: TableCopyBatchId,
    /// The copied source rows.
    rows: Vec<TableRow>,
}

impl TableCopyBatch {
    /// Creates a table-copy batch.
    ///
    /// # Panics
    ///
    /// Panics if `rows` is empty. Terminal copy coordination uses
    /// [`TableCopyWrite::Finish`] instead.
    pub fn new(id: TableCopyBatchId, rows: Vec<TableRow>) -> Self {
        assert!(!rows.is_empty(), "table-copy batches must contain rows");

        Self { id, rows }
    }

    /// Returns the batch's idempotency key.
    pub fn id(&self) -> &TableCopyBatchId {
        &self.id
    }

    /// Returns the copied source rows.
    pub fn rows(&self) -> &[TableRow] {
        &self.rows
    }

    /// Splits the batch into its idempotency key and rows.
    pub fn into_parts(self) -> (TableCopyBatchId, Vec<TableRow>) {
        (self.id, self.rows)
    }
}

/// Data or terminal coordination sent during an initial table copy.
#[derive(Debug)]
pub enum TableCopyWrite {
    /// A nonempty batch of copied rows.
    Batch(TableCopyBatch),
    /// The terminal call for an empty copy or a deferred-durability barrier.
    Finish,
}

impl TableCopyWrite {
    /// Returns the copied rows, or an empty vector for [`Self::Finish`].
    pub fn into_rows(self) -> Vec<TableRow> {
        match self {
            Self::Batch(batch) => batch.into_parts().1,
            Self::Finish => Vec::new(),
        }
    }
}
