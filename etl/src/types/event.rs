use etl_postgres::types::{ColumnSchema, SchemaVersion, TableId, TableSchema};
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
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

/// A change in a relation.
#[derive(Debug, Clone, PartialEq)]
pub enum RelationChange {
    /// A change that describes adding a new column.
    AddColumn(ColumnSchema),
    /// A change that describes dropping an existing column.
    DropColumn(ColumnSchema),
    /// A change that describes altering an existing column.
    ///
    /// The alteration of a column is defined as any modifications to a column
    /// while keeping the same name.
    AlterColumn(ColumnSchema, ColumnSchema),
}

/// Table schema definition event from Postgres logical replication.
///
/// [`RelationEvent`] provides schema information for tables involved in replication.
/// It contains complete column definitions and metadata needed to interpret
/// subsequent data modification events for the table.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// ID of the table of which this is a schema change.
    pub table_id: TableId,
    /// The old table schema.
    pub old_table_schema: Arc<TableSchema>,
    /// The new table schema.
    pub new_table_schema: Arc<TableSchema>,
}

impl RelationEvent {
    /// Builds a list of [`RelationChange`]s that describe the changes between the old and new table
    /// schemas.
    pub fn build_changes(&self) -> Vec<RelationChange> {
        // We build a lookup set for the new column schemas for quick change detection.
        let mut new_indexed_column_schemas = self
            .new_table_schema
            .column_schemas
            .iter()
            .cloned()
            .map(IndexedColumnSchema)
            .collect::<HashSet<_>>();

        // We process all the changes that we want to dispatch to the destination.
        let mut changes = vec![];
        for column_schema in self.old_table_schema.column_schemas.iter() {
            let column_schema = IndexedColumnSchema(column_schema.clone());
            let latest_column_schema = new_indexed_column_schemas.take(&column_schema);
            match latest_column_schema {
                Some(latest_column_schema) => {
                    let column_schema = column_schema.into_inner();
                    let latest_column_schema = latest_column_schema.into_inner();

                    if column_schema.name != latest_column_schema.name {
                        // If we find a column with the same name but different fields, we assume it was changed. The only changes
                        // that we detect are changes to the column but with preserved name.
                        changes.push(RelationChange::AlterColumn(
                            column_schema,
                            latest_column_schema,
                        ));
                    }
                }
                None => {
                    // If we don't find the column in the latest schema, we assume it was dropped even
                    // though it could be renamed.
                    changes.push(RelationChange::DropColumn(column_schema.into_inner()));
                }
            }
        }

        // For the remaining columns that didn't match, we assume they were added.
        for column_schema in new_indexed_column_schemas {
            changes.push(RelationChange::AddColumn(column_schema.into_inner()));
        }

        changes
    }
}

/// Row insertion event from Postgres logical replication.
///
/// [`InsertEvent`] represents a new row being added to a table. It contains
/// the complete row data for insertion into the destination system.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// ID of the table where the row was inserted.
    pub table_id: TableId,
    /// Schema version that should be used to interpret this row.
    pub schema_version: SchemaVersion,
    /// Complete row data for the inserted row.
    pub table_row: TableRow,
}

/// Row update event from Postgres logical replication.
///
/// [`UpdateEvent`] represents an existing row being modified. It contains
/// both the new row data and optionally the old row data for comparison
/// and conflict resolution in the destination system.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// ID of the table where the row was updated.
    pub table_id: TableId,
    /// Schema version that should be used to interpret this row.
    pub schema_version: SchemaVersion,
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
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// ID of the table where the row was deleted.
    pub table_id: TableId,
    /// Schema version that should be used to interpret this row.
    pub schema_version: SchemaVersion,
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
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the transaction of this event will commit.
    pub commit_lsn: PgLsn,
    /// Truncate operation options from Postgres.
    pub options: i8,
    /// List of table IDs that were truncated in this operation.
    pub table_ids: Vec<(TableId, SchemaVersion)>,
}

/// Represents a single replication event from Postgres logical replication.
///
/// [`Event`] encapsulates all possible events that can occur in a Postgres replication
/// stream, including data modification events and transaction control events. Each event
/// type corresponds to specific operations in the source database.
#[derive(Debug, Clone, PartialEq)]
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
    /// Relation schema information event describing table structure.
    Relation(RelationEvent),
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
            Event::Insert(insert_event) => insert_event.table_id == *table_id,
            Event::Update(update_event) => update_event.table_id == *table_id,
            Event::Delete(delete_event) => delete_event.table_id == *table_id,
            Event::Relation(relation_event) => relation_event.table_id == *table_id,
            Event::Truncate(event) => event.table_ids.iter().any(|(id, _)| id == table_id),
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
            Event::Relation(_) => EventType::Relation,
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

#[derive(Debug, Clone)]
struct IndexedColumnSchema(ColumnSchema);

impl IndexedColumnSchema {
    fn into_inner(self) -> ColumnSchema {
        self.0
    }
}

impl Eq for IndexedColumnSchema {}

impl PartialEq for IndexedColumnSchema {
    fn eq(&self, other: &Self) -> bool {
        self.0.name == other.0.name
    }
}

impl Hash for IndexedColumnSchema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.name.hash(state);
    }
}
