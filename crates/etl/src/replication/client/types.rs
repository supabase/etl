use std::{fmt, sync::Arc};

use crate::types::PgLsn;

/// Updates emitted by the background task driving the PostgreSQL connection.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum PostgresConnectionUpdate {
    /// The connection task is running.
    Running,
    /// The connection task terminated cleanly.
    Terminated,
    /// The connection task exited due to an error.
    Errored { error: Arc<str> },
}

impl fmt::Debug for PostgresConnectionUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => f.write_str("Running"),
            Self::Terminated => f.write_str("Terminated"),
            Self::Errored { .. } => f.debug_struct("Errored").finish_non_exhaustive(),
        }
    }
}

impl PostgresConnectionUpdate {
    /// Creates an error update from an error message.
    pub(crate) fn errored(error: impl Into<Arc<str>>) -> Self {
        Self::Errored { error: error.into() }
    }
}

impl fmt::Display for PostgresConnectionUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Terminated => write!(f, "terminated"),
            Self::Errored { .. } => write!(f, "errored"),
        }
    }
}

/// Internal snapshot action for `CREATE_REPLICATION_SLOT`.
#[derive(Debug)]
pub(super) enum SnapshotAction {
    /// `USE_SNAPSHOT` - uses the snapshot in the current transaction.
    Use,
    /// `NOEXPORT_SNAPSHOT` - neither exports nor uses the snapshot.
    NoExport,
}

/// Result returned when creating a new replication slot.
///
/// Contains the consistent point LSN that should be used as the starting point
/// for logical replication.
#[derive(Debug, Clone)]
pub struct CreateSlotResult {
    /// The LSN at which the slot was created, representing a consistent point
    /// in the WAL.
    pub consistent_point: PgLsn,
}

/// Result returned when retrieving an existing replication slot.
///
/// Contains the confirmed flush LSN indicating how far replication has
/// progressed.
#[derive(Debug, Clone)]
pub struct GetSlotResult {
    /// The LSN up to which changes have been confirmed as processed by ETL.
    pub confirmed_flush_lsn: PgLsn,
}

/// The current state of a replication slot.
///
/// Represents whether a slot is valid and can be used for replication, or has
/// been invalidated by PostgreSQL (e.g., due to exceeding
/// `max_slot_wal_keep_size`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotState {
    /// The slot is valid and can be used for replication.
    Valid,
    /// The slot has been invalidated and cannot be used for replication.
    ///
    /// This typically occurs when the slot falls too far behind the current WAL
    /// position and PostgreSQL removes the required WAL segments.
    Invalidated,
}

/// Result type for operations that either get an existing slot or create a new
/// one.
///
/// This enum distinguishes between whether a slot was newly created or already
/// existed, providing appropriate result data for each case.
#[derive(Debug, Clone)]
pub(crate) enum GetOrCreateSlotResult {
    /// A new slot was created with the given consistent point.
    CreateSlot(CreateSlotResult),
    /// An existing slot was found with the given confirmed flush LSN.
    GetSlot(GetSlotResult),
}

impl GetOrCreateSlotResult {
    /// Returns the lsn that should be used as starting LSN during events
    /// replication.
    pub(crate) fn get_start_lsn(&self) -> PgLsn {
        match self {
            GetOrCreateSlotResult::CreateSlot(result) => result.consistent_point,
            GetOrCreateSlotResult::GetSlot(result) => result.confirmed_flush_lsn,
        }
    }
}

/// A ctid-based partition range for parallel copy.
#[derive(Debug)]
pub enum CtidPartition {
    /// A range with an open lower bound and an exclusive upper bound.
    ///
    /// Matches rows where `ctid < end_tid`.
    OpenStart { end_tid: String },
    /// A range with an inclusive lower bound and an exclusive upper bound.
    ///
    /// Matches rows where `ctid >= start_tid and ctid < end_tid`.
    Closed { start_tid: String, end_tid: String },
    /// A range with an inclusive lower bound and an open upper bound.
    ///
    /// Matches rows where `ctid >= start_tid`.
    OpenEnd { start_tid: String },
}

#[cfg(test)]
mod tests {
    use super::{CreateSlotResult, GetOrCreateSlotResult, GetSlotResult};
    use crate::types::PgLsn;

    #[test]
    fn existing_logical_slot_resumes_from_confirmed_flush_lsn() {
        let confirmed_flush_lsn = PgLsn::from(100u64);
        let slot = GetOrCreateSlotResult::GetSlot(GetSlotResult { confirmed_flush_lsn });

        assert_eq!(slot.get_start_lsn(), confirmed_flush_lsn);
    }

    #[test]
    fn new_logical_slot_starts_from_consistent_point() {
        let consistent_point = PgLsn::from(200u64);
        let slot = GetOrCreateSlotResult::CreateSlot(CreateSlotResult { consistent_point });

        assert_eq!(slot.get_start_lsn(), consistent_point);
    }
}
