use etl_postgres::types::{ReplicatedTableSchema, TableId};
use std::fmt;
use tokio_postgres::types::PgLsn;

use crate::types::TableRow;

/// Transaction begin event from Postgres logical replication.
///
/// [`BeginEvent`] marks the start of a new transaction in the replication stream.
/// It contains metadata about the transaction including LSN positions and timing
/// information for proper sequencing and recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct BeginEvent {
    /// LSN position where the transaction started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction will commit.
    pub commit_lsn: PgLsn,
    /// Transaction start timestamp in Postgres format.
    pub timestamp: i64,
    /// Transaction ID for tracking and coordination.
    pub xid: u32,
}

/// Transaction commit event from Postgres logical replication.
///
/// [`CommitEvent`] marks the successful completion of a transaction in the replication
/// stream. It provides final metadata about the transaction including timing and
/// LSN positions for maintaining consistency and ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitEvent {
    /// LSN position where the transaction started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction committed.
    pub commit_lsn: PgLsn,
    /// Transaction commit flags from Postgres.
    pub flags: i8,
    /// Final LSN position after the transaction.
    pub end_lsn: u64,
    /// Transaction commit timestamp in Postgres format.
    pub timestamp: i64,
}

/// Row insertion event from Postgres logical replication.
///
/// [`InsertEvent`] represents a new row being added to a table. It contains
/// the complete row data for insertion into the destination system.
#[derive(Debug, Clone)]
pub struct InsertEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// The replicated table schema for this event.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// Complete row data for the inserted row.
    pub table_row: TableRow,
}

/// Row update event from Postgres logical replication.
///
/// [`UpdateEvent`] represents an existing row being modified. It contains
/// both the new row data and optionally the old row data for comparison
/// and conflict resolution in the destination system.
#[derive(Debug, Clone)]
pub struct UpdateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// The replicated table schema for this event.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// New row data after the update.
    pub table_row: TableRow,
    /// Previous row data before the update.
    ///
    /// The boolean indicates whether the row contains only key columns (`true`)
    /// or the complete row data (`false`). This depends on the Postgres
    /// `REPLICA IDENTITY` setting for the table.
    pub old_table_row: Option<(bool, TableRow)>,
}

/// Row deletion event from Postgres logical replication.
///
/// [`DeleteEvent`] represents a row being removed from a table. It contains
/// information about the deleted row for proper cleanup in the destination system.
#[derive(Debug, Clone)]
pub struct DeleteEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// The replicated table schema for this event.
    pub replicated_table_schema: ReplicatedTableSchema,
    /// Data from the deleted row.
    ///
    /// The boolean indicates whether the row contains only key columns (`true`)
    /// or the complete row data (`false`). This depends on the Postgres
    /// `REPLICA IDENTITY` setting for the table.
    pub old_table_row: Option<(bool, TableRow)>,
}

/// Table truncation event from Postgres logical replication.
///
/// [`TruncateEvent`] represents one or more tables being truncated (all rows deleted).
/// This is a bulk operation that clears entire tables and may affect multiple tables
/// in a single operation when using cascading truncates.
#[derive(Debug, Clone)]
pub struct TruncateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// Truncate operation options from Postgres.
    pub options: i8,
    /// List of schemas for tables that were truncated in this operation.
    pub truncated_tables: Vec<ReplicatedTableSchema>,
}

/// Represents a single replication event from Postgres logical replication.
///
/// [`Event`] encapsulates all possible events that can occur in a Postgres replication
/// stream, including data modification events and transaction control events. Each event
/// type corresponds to specific operations in the source database.
#[derive(Debug, Clone)]
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
            Event::Insert(e) => e.replicated_table_schema.id() == *table_id,
            Event::Update(e) => e.replicated_table_schema.id() == *table_id,
            Event::Delete(e) => e.replicated_table_schema.id() == *table_id,
            Event::Truncate(e) => e.truncated_tables.iter().any(|s| s.id() == *table_id),
            _ => false,
        }
    }
}

/// Classification of Postgres replication event types.
///
/// [`EventType`] provides a lightweight enumeration of possible replication events
/// without carrying the associated data. This is useful for filtering, routing,
/// and processing decisions based on event type alone.
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
            Event::Unsupported => EventType::Unsupported,
        }
    }
}

impl From<Event> for EventType {
    fn from(event: Event) -> Self {
        (&event).into()
    }
}
