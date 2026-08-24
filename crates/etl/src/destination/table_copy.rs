use std::fmt;

use uuid::Uuid;

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
