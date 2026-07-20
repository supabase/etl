//! Replication events emitted by the ETL pipeline.
//!
//! This module contains transaction, row, truncate, and relation events passed
//! to destinations during streaming replication. Each data-bearing event
//! carries the [`crate::schema::ReplicatedTableSchema`] needed to interpret its
//! row payload.

use std::{fmt, mem::size_of};

use crate::{
    data::{OldTableRow, SizeHint, TableRow, UpdatedTableRow},
    schema::{PgLsn, ReplicatedTableSchema, TableId},
};

/// Transaction begin event from Postgres logical replication.
///
/// [`BeginEvent`] marks the start of a new transaction in the replication
/// stream. It contains metadata about the transaction including LSN positions
/// and timing information for proper sequencing and recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct BeginEvent {
    /// LSN position where the transaction started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction will commit.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within the transaction.
    pub tx_ordinal: u64,
    /// Transaction start timestamp in Postgres format.
    pub timestamp: i64,
    /// Transaction ID for tracking and coordination.
    pub xid: u32,
}

impl BeginEvent {
    /// Returns the sequence key for this event.
    pub fn event_sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Transaction commit event from Postgres logical replication.
///
/// [`CommitEvent`] marks the successful completion of a transaction in the
/// replication stream. It provides final metadata about the transaction
/// including timing and LSN positions for maintaining consistency and ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitEvent {
    /// LSN position where the transaction started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction committed.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within the transaction.
    pub tx_ordinal: u64,
    /// Transaction commit flags from Postgres.
    pub flags: i8,
    /// Final LSN position after the transaction.
    pub end_lsn: PgLsn,
    /// Transaction commit timestamp in Postgres format.
    pub timestamp: i64,
}

impl CommitEvent {
    /// Returns the sequence key for this event.
    pub fn event_sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Row insertion event from Postgres logical replication.
///
/// [`InsertEvent`] represents a new row being added to a table. It contains
/// the complete row data for the inserted source row.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct InsertEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within the transaction.
    pub tx_ordinal: u64,
    /// The replicated table schema for this event.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// Complete row data for the inserted row.
    pub table_row: TableRow,
}

impl InsertEvent {
    /// Returns the sequence key for this event.
    pub fn event_sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Row update event from Postgres logical replication.
///
/// [`UpdateEvent`] represents an existing row being modified.
///
/// The updated row is the authoritative post-update payload. The old row, when
/// present, is auxiliary context from PostgreSQL's replica identity and may be
/// a full old image or only replica-identity columns.
///
/// A few details matter for consumers:
///
/// - `old_table_row == None` is valid for updates under key-based replica
///   identities when PostgreSQL determines no old-side tuple is needed.
/// - [`OldTableRow::Key`] contains replica-identity columns only. That is not
///   necessarily the source primary key.
/// - [`UpdatedTableRow::Partial`] can occur when PostgreSQL emits
///   `UnchangedToast` for columns ETL cannot reconstruct from the available
///   old-side row image.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct UpdateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within the transaction.
    pub tx_ordinal: u64,
    /// The replicated table schema for this event.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// New row data after the update.
    ///
    /// [`UpdatedTableRow::Full`] means ETL knows every replicated column value.
    /// [`UpdatedTableRow::Partial`] means some replicated columns were emitted
    /// by PostgreSQL as `UnchangedToast` and could not be reconstructed
    /// safely from the old-side row image.
    pub updated_table_row: UpdatedTableRow,
    /// Previous row data before the update, when PostgreSQL emitted one.
    ///
    /// For `REPLICA IDENTITY FULL`, this is a full row image. For `DEFAULT`
    /// with a primary key or `USING INDEX`, this may be a key image or may be
    /// absent entirely when PostgreSQL did not need to log an old-side tuple.
    pub old_table_row: Option<OldTableRow>,
}

impl UpdateEvent {
    /// Returns the sequence key for this event.
    pub fn event_sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Row deletion event from Postgres logical replication.
///
/// [`DeleteEvent`] represents a row being removed from a table.
///
/// Unlike updates, deletes carry only old-side data.
/// [`DeleteEvent::old_table_row`] is the payload used to identify the removed
/// source row.
///
/// PostgreSQL pgoutput sends an old-side tuple for every published delete:
/// [`OldTableRow::Full`] for `REPLICA IDENTITY FULL`, otherwise
/// [`OldTableRow::Key`] for key-based replica identity. The field remains
/// optional at the Rust API boundary for malformed or non-pgoutput inputs.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct DeleteEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within the transaction.
    pub tx_ordinal: u64,
    /// The replicated table schema for this event.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// Old-side payload from the deleted row.
    pub old_table_row: Option<OldTableRow>,
}

impl DeleteEvent {
    /// Returns the sequence key for this event.
    pub fn event_sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Table truncation event from Postgres logical replication.
///
/// [`TruncateEvent`] represents one or more tables being truncated (all rows
/// deleted). This is a bulk operation that clears entire tables and may affect
/// multiple tables in a single operation when using cascading truncates.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct TruncateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within the transaction.
    pub tx_ordinal: u64,
    /// Truncate operation options from Postgres.
    pub options: i8,
    /// List of schemas for tables that were truncated in this operation.
    pub truncated_tables: Vec<ReplicatedTableSchema>,
}

impl TruncateEvent {
    /// Returns the sequence key for this event.
    pub fn event_sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }
}

/// Relation (schema) event from Postgres logical replication.
///
/// [`RelationEvent`] represents a table schema notification in the replication
/// stream. It is emitted when a RELATION message is received, containing the
/// current replication mask for the table. This event notifies downstream
/// consumers about which columns are being replicated for a table.
///
/// PostgreSQL generates relation messages at runtime from `pgoutput`'s
/// session-local schema cache; they are not WAL-backed changes. Which relation
/// messages appear is therefore session-dependent: a fresh session resets the
/// cache and can re-emit schema metadata during replay. This event
/// intentionally has no LSN, transaction ordinal, or sequence key because such
/// metadata would not be a durable replay identity. Consumers should instead
/// treat it as an ordered schema barrier for the row events that follow it.
///
/// PostgreSQL emits relation-message columns in `pg_attribute.attnum` order,
/// skipping unpublished columns, and sends tuple data in that same order. The
/// masks are built by unique column name, so ordering is not needed for mask
/// membership. Applying those masks to ETL's `attnum`-ordered stored table
/// schema creates a [`ReplicatedTableSchema`] whose column order matches the
/// tuple payloads. Event conversion can then decode row values by
/// replicated-column position.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub struct RelationEvent {
    /// The replicated table schema containing the table schema, replication
    /// mask, and identity mask.
    pub replicated_table_schema: ReplicatedTableSchema,
}

/// Represents a single replication event from Postgres logical replication.
///
/// [`Event`] encapsulates all possible events that can occur in a Postgres
/// replication stream, including data modification events and transaction
/// control events. Each event type corresponds to specific operations in the
/// source database.
#[derive(Debug)]
#[cfg_attr(any(test, feature = "test-utils"), derive(Clone))]
pub enum Event {
    /// Transaction begin event marking the start of a new transaction.
    Begin(BeginEvent),
    /// Transaction commit event marking successful transaction completion.
    Commit(CommitEvent),
    /// Row insertion event with new row data.
    Insert(InsertEvent),
    /// Row update event with old and new row data.
    Update(UpdateEvent),
    /// Row deletion event with deleted row data.
    Delete(DeleteEvent),
    /// Table truncation event clearing all rows from tables.
    Truncate(TruncateEvent),
    /// Relation (schema) event notifying about table schema and replication
    /// mask.
    Relation(RelationEvent),
    /// Unsupported event type that cannot be processed.
    Unsupported,
}

impl Event {
    /// Returns the [`EventType`] that corresponds to this event.
    ///
    /// This provides a lightweight way to identify the event type without
    /// pattern matching on the full event structure.
    pub fn event_type(&self) -> EventType {
        self.into()
    }

    /// Returns true if the event is associated with the specified table.
    ///
    /// This method checks whether the event operates on the given table ID.
    /// Transaction control events (Begin/Commit) are not associated with
    /// specific tables and will always return false.
    pub fn has_table_id(&self, table_id: &TableId) -> bool {
        match self {
            Event::Insert(event) => event.replicated_table_schema.id() == *table_id,
            Event::Update(event) => event.replicated_table_schema.id() == *table_id,
            Event::Delete(event) => event.replicated_table_schema.id() == *table_id,
            Event::Truncate(event) => event.truncated_tables.iter().any(|s| s.id() == *table_id),
            Event::Relation(event) => event.replicated_table_schema.id() == *table_id,
            _ => false,
        }
    }
}

impl SizeHint for Event {
    fn size_hint(&self) -> usize {
        match self {
            Self::Begin(_) => size_of::<BeginEvent>(),
            Self::Commit(_) => size_of::<CommitEvent>(),
            Self::Insert(event) => size_of::<InsertEvent>() + event.table_row.size_hint(),
            Self::Update(event) => {
                let old_row_size =
                    event.old_table_row.as_ref().map(SizeHint::size_hint).unwrap_or_default();
                size_of::<UpdateEvent>() + event.updated_table_row.size_hint() + old_row_size
            }
            Self::Delete(event) => {
                let old_row_size =
                    event.old_table_row.as_ref().map(SizeHint::size_hint).unwrap_or_default();
                size_of::<DeleteEvent>() + old_row_size
            }
            Self::Truncate(event) => {
                size_of::<TruncateEvent>()
                    + event.truncated_tables.len() * size_of::<ReplicatedTableSchema>()
            }
            Self::Relation(_) => size_of::<RelationEvent>(),
            Self::Unsupported => 0,
        }
    }
}

/// Pair used to build a CDC sequence key for destinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventSequenceKey {
    /// Commit LSN identifying transaction order across transactions.
    pub commit_lsn: PgLsn,
    /// Zero-based ordinal identifying protocol order within the same
    /// transaction. Runtime-generated relation events may reserve ordinal
    /// slots for compatibility, but do not expose sequence keys themselves.
    pub tx_ordinal: u64,
}

impl EventSequenceKey {
    /// Creates a new sequence key from commit LSN and transaction-local
    /// ordinal.
    pub fn new(commit_lsn: PgLsn, tx_ordinal: u64) -> Self {
        Self { commit_lsn, tx_ordinal }
    }

    /// Returns the canonical packed `u128` form: `commit_lsn` in the high
    /// 64 bits, `tx_ordinal` in the low 64 bits. Used by destinations that
    /// need a single totally-ordered numeric key for CDC dedup (e.g.
    /// ClickHouse's `ReplacingMergeTree` version column).
    pub fn as_u128(self) -> u128 {
        (u128::from(u64::from(self.commit_lsn)) << 64) | u128::from(self.tx_ordinal)
    }
}

impl fmt::Display for EventSequenceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let commit_lsn = u64::from(self.commit_lsn);
        write!(f, "{commit_lsn:016x}/{:016x}", self.tx_ordinal)
    }
}

/// Creates a hex-encoded sequence number from Postgres LSNs to ensure correct
/// event ordering.
///
/// Creates a hex-encoded sequence number that ensures events are processed in
/// the correct order even when they have the same system time. The format is
/// compatible with BigQuery's `_CHANGE_SEQUENCE_NUMBER` column requirements.
///
/// The rationale for using the LSN is that downstream systems will preserve the
/// highest sequence number in case of equal primary key, which is what we want
/// since in case of updates, we want the latest update in Postgres order to be
/// the winner. We have first the `commit_lsn` in the key so that operations are
/// first ordered based on the LSN at which the transaction committed,
/// and if two operations belong to the same transaction (meaning they have the
/// same `commit_lsn`), the `start_lsn` will be used as a tiebreaker. We first
/// order by `commit_lsn` to preserve the order in which operations are received
/// by the pipeline since transactions are ordered by commit time
/// and not interleaved.
pub fn generate_sequence_number(start_lsn: PgLsn, commit_lsn: PgLsn) -> String {
    let start_lsn = u64::from(start_lsn);
    let commit_lsn = u64::from(commit_lsn);

    format!("{commit_lsn:016x}/{start_lsn:016x}")
}

/// Classification of Postgres replication event types.
///
/// [`EventType`] provides a lightweight enumeration of possible replication
/// events without carrying the associated data. This is useful for filtering,
/// routing, and processing decisions based on event type alone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    /// Transaction begin marker.
    Begin,
    /// Transaction commit marker.
    Commit,
    /// Row insertion operation.
    Insert,
    /// Row update operation.
    Update,
    /// Row deletion operation.
    Delete,
    /// Table schema definition.
    Relation,
    /// Table truncation operation.
    Truncate,
    /// Unsupported or unknown event.
    Unsupported,
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Begin => write!(f, "Begin"),
            Self::Commit => write!(f, "Commit"),
            Self::Insert => write!(f, "Insert"),
            Self::Update => write!(f, "Update"),
            Self::Delete => write!(f, "Delete"),
            Self::Relation => write!(f, "Relation"),
            Self::Truncate => write!(f, "Truncate"),
            Self::Unsupported => write!(f, "Unsupported"),
        }
    }
}

impl From<&Event> for EventType {
    fn from(event: &Event) -> Self {
        match event {
            Event::Begin(_) => EventType::Begin,
            Event::Commit(_) => EventType::Commit,
            Event::Insert(_) => EventType::Insert,
            Event::Update(_) => EventType::Update,
            Event::Delete(_) => EventType::Delete,
            Event::Truncate(_) => EventType::Truncate,
            Event::Relation(_) => EventType::Relation,
            Event::Unsupported => EventType::Unsupported,
        }
    }
}

impl From<Event> for EventType {
    fn from(event: Event) -> Self {
        (&event).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_sequence_number_fn() {
        assert_eq!(
            generate_sequence_number(PgLsn::from(0), PgLsn::from(0)),
            "0000000000000000/0000000000000000"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(1), PgLsn::from(0)),
            "0000000000000000/0000000000000001"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(255), PgLsn::from(0)),
            "0000000000000000/00000000000000ff"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(65535), PgLsn::from(0)),
            "0000000000000000/000000000000ffff"
        );
        assert_eq!(
            generate_sequence_number(PgLsn::from(u64::MAX), PgLsn::from(0)),
            "0000000000000000/ffffffffffffffff"
        );
    }
}
