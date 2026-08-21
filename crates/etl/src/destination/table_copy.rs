use std::fmt;

use crate::data::TableRow;

/// An idempotency key for one logical batch in a table-copy attempt.
///
/// Redeliveries of the same batch retain the same ID. Distinct batches have
/// distinct IDs even when their rows are identical. The ID is opaque to
/// destinations and does not define ordering or resumable source progress.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TableCopyBatchId(Box<str>);

impl TableCopyBatchId {
    /// Creates an opaque table-copy batch ID.
    pub fn new(id: impl Into<Box<str>>) -> Self {
        Self(id.into())
    }

    /// Returns the opaque ID as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableCopyBatchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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
