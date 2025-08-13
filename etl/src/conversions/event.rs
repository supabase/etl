use core::str;
use etl_postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use etl_postgres::types::convert_type_oid_to_type;
use postgres_replication::protocol;
use postgres_replication::protocol::LogicalReplicationMessage;
use std::fmt;
use std::sync::Arc;
use tokio_postgres::types::PgLsn;

use crate::conversions::Cell;
use crate::conversions::table_row::TableRow;
use crate::conversions::text::TextFormatConverter;
use crate::error::EtlError;
use crate::error::{ErrorKind, EtlResult};
use crate::store::schema::SchemaStore;
use crate::{bail, etl_error};

/// Transaction begin event from PostgreSQL logical replication.
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
    /// Transaction start timestamp in PostgreSQL format.
    pub timestamp: i64,
    /// Transaction ID for tracking and coordination.
    pub xid: u32,
}

impl BeginEvent {
    /// Creates a [`BeginEvent`] from PostgreSQL protocol data.
    ///
    /// This method parses the replication protocol begin message and extracts
    /// transaction metadata for use in the ETL pipeline.
    pub fn from_protocol(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        begin_body: &protocol::BeginBody,
    ) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            timestamp: begin_body.timestamp(),
            xid: begin_body.xid(),
        }
    }
}

/// Transaction commit event from PostgreSQL logical replication.
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
    /// Transaction commit flags from PostgreSQL.
    pub flags: i8,
    /// Final LSN position after the transaction.
    pub end_lsn: u64,
    /// Transaction commit timestamp in PostgreSQL format.
    pub timestamp: i64,
}

impl CommitEvent {
    /// Creates a [`CommitEvent`] from PostgreSQL protocol data.
    ///
    /// This method parses the replication protocol commit message and extracts
    /// transaction completion metadata for use in the ETL pipeline.
    pub fn from_protocol(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        commit_body: &protocol::CommitBody,
    ) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            flags: commit_body.flags(),
            end_lsn: commit_body.end_lsn(),
            timestamp: commit_body.timestamp(),
        }
    }
}

/// Table schema definition event from PostgreSQL logical replication.
///
/// [`RelationEvent`] provides schema information for tables involved in replication.
/// It contains complete column definitions and metadata needed to interpret
/// subsequent data modification events for the table.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the event will commit.
    pub commit_lsn: PgLsn,
    /// Complete table schema including columns and types.
    pub table_schema: TableSchema,
}

impl RelationEvent {
    /// Creates a [`RelationEvent`] from PostgreSQL protocol data.
    ///
    /// This method parses the replication protocol relation message and builds
    /// a complete table schema for use in interpreting subsequent data events.
    pub fn from_protocol(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        relation_body: &protocol::RelationBody,
    ) -> EtlResult<Self> {
        let table_name = TableName::new(
            relation_body.namespace()?.to_string(),
            relation_body.name()?.to_string(),
        );
        let column_schemas = relation_body
            .columns()
            .iter()
            .map(Self::build_column_schema)
            .collect::<Result<Vec<ColumnSchema>, _>>()?;
        let table_schema = TableSchema::new(
            TableId::new(relation_body.rel_id()),
            table_name,
            column_schemas,
        );

        Ok(Self {
            start_lsn,
            commit_lsn,
            table_schema,
        })
    }

    /// Constructs a [`ColumnSchema`] from PostgreSQL protocol column data.
    ///
    /// This helper method extracts column metadata from the replication protocol
    /// and converts it into the internal column schema representation. Some fields
    /// like nullable status have default values due to protocol limitations.
    fn build_column_schema(column: &protocol::Column) -> EtlResult<ColumnSchema> {
        Ok(ColumnSchema::new(
            column.name()?.to_string(),
            convert_type_oid_to_type(column.type_id() as u32),
            column.type_modifier(),
            // We do not have access to this information, so we default it to `false`.
            // TODO: figure out how to fill this value correctly or how to handle the missing value
            //  better.
            false,
            // Currently 1 means that the column is part of the primary key.
            column.flags() == 1,
        ))
    }
}

/// Row insertion event from PostgreSQL logical replication.
///
/// [`InsertEvent`] represents a new row being added to a table. It contains
/// the complete row data for insertion into the destination system.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the event will commit.
    pub commit_lsn: PgLsn,
    /// ID of the table where the row was inserted.
    pub table_id: TableId,
    /// Complete row data for the inserted row.
    pub table_row: TableRow,
}

/// Row update event from PostgreSQL logical replication.
///
/// [`UpdateEvent`] represents an existing row being modified. It contains
/// both the new row data and optionally the old row data for comparison
/// and conflict resolution in the destination system.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the event will commit.
    pub commit_lsn: PgLsn,
    /// ID of the table where the row was updated.
    pub table_id: TableId,
    /// New row data after the update.
    pub table_row: TableRow,
    /// Previous row data before the update.
    ///
    /// The boolean indicates whether the row contains only key columns (`true`)
    /// or the complete row data (`false`). This depends on the PostgreSQL
    /// `REPLICA IDENTITY` setting for the table.
    pub old_table_row: Option<(bool, TableRow)>,
}

/// Row deletion event from PostgreSQL logical replication.
///
/// [`DeleteEvent`] represents a row being removed from a table. It contains
/// information about the deleted row for proper cleanup in the destination system.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteEvent {
    /// LSN position where the event started
    pub start_lsn: PgLsn,
    /// LSN position where the event will commit
    pub commit_lsn: PgLsn,
    /// ID of the table where the row was deleted
    pub table_id: TableId,
    /// Data from the deleted row.
    ///
    /// The boolean indicates whether the row contains only key columns (`true`)
    /// or the complete row data (`false`). This depends on the PostgreSQL
    /// `REPLICA IDENTITY` setting for the table.
    pub old_table_row: Option<(bool, TableRow)>,
}

/// Table truncation event from PostgreSQL logical replication.
///
/// [`TruncateEvent`] represents one or more tables being truncated (all rows deleted).
/// This is a bulk operation that clears entire tables and may affect multiple tables
/// in a single operation when using cascading truncates.
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateEvent {
    /// LSN position where the event started.
    pub start_lsn: PgLsn,
    /// LSN position where the event will commit.
    pub commit_lsn: PgLsn,
    /// Truncate operation options from PostgreSQL.
    pub options: i8,
    /// List of table IDs that were truncated in this operation.
    pub rel_ids: Vec<u32>,
}

impl TruncateEvent {
    /// Creates a [`TruncateEvent`] from PostgreSQL protocol data.
    ///
    /// This method parses the replication protocol truncate message and extracts
    /// information about which tables were truncated and with what options.
    pub fn from_protocol(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        truncate_body: &protocol::TruncateBody,
    ) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            options: truncate_body.options(),
            rel_ids: truncate_body.rel_ids().to_vec(),
        }
    }
}

/// Represents a single replication event from PostgreSQL logical replication.
///
/// [`Event`] encapsulates all possible events that can occur in a PostgreSQL replication
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
            Event::Relation(relation_event) => relation_event.table_schema.id == *table_id,
            Event::Truncate(event) => {
                let Some(_) = event.rel_ids.iter().find(|&&id| table_id.0 == id) else {
                    return false;
                };

                true
            }
            _ => false,
        }
    }
}

/// Classification of PostgreSQL replication event types.
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

/// Retrieves a table schema from the schema store by table ID.
///
/// This function looks up the table schema for the specified table ID in the
/// schema store. If the schema is not found, it returns an error indicating
/// that the table is missing from the cache.
async fn get_table_schema<S>(schema_store: &S, table_id: TableId) -> EtlResult<Arc<TableSchema>>
where
    S: SchemaStore,
{
    schema_store
        .get_table_schema(&table_id)
        .await?
        .ok_or_else(|| {
            etl_error!(
                ErrorKind::MissingTableSchema,
                "Table not found in the schema cache",
                format!("The table schema for table {table_id} was not found in the cache")
            )
        })
}

/// Converts PostgreSQL tuple data into a [`TableRow`] using column schemas.
///
/// This function transforms raw tuple data from the replication protocol into
/// a structured row representation. It handles null values, unchanged TOAST data,
/// and binary data according to PostgreSQL semantics. For unchanged TOAST values,
/// it attempts to reuse data from the old row if available.
///
/// # Panics
///
/// Panics if a required (non-nullable) column receives null data and
/// `use_default_for_missing_cols` is false, as this indicates protocol-level
/// corruption that should not be handled gracefully.
fn convert_tuple_to_row(
    column_schemas: &[ColumnSchema],
    tuple_data: &[protocol::TupleData],
    old_table_row: &mut Option<TableRow>,
    use_default_for_missing_cols: bool,
) -> EtlResult<TableRow> {
    let mut values = Vec::with_capacity(column_schemas.len());

    for (i, column_schema) in column_schemas.iter().enumerate() {
        // We are expecting that for each column, there is corresponding tuple data, even for null
        // values.
        let Some(tuple_data) = &tuple_data.get(i) else {
            bail!(
                ErrorKind::ConversionError,
                "Tuple data does not contain data at the specified index"
            );
        };

        let cell = match tuple_data {
            protocol::TupleData::Null => {
                if column_schema.nullable {
                    Cell::Null
                } else if use_default_for_missing_cols {
                    TextFormatConverter::default_value(&column_schema.typ)?
                } else {
                    // This is protocol level error, so we panic instead of carrying on
                    // with incorrect data to avoid corruption downstream.
                    panic!(
                        "A required column {} was missing from the tuple",
                        column_schema.name
                    );
                }
            }
            protocol::TupleData::UnchangedToast => {
                // For unchanged toast values we try to use the value from the old row if it is present
                // but only if it is not null. In all other cases we send the default value for
                // consistency. As a bit of a practical hack we take the value out of the old row and
                // move a null value in its place to avoid a clone because toast values tend to be large.
                if let Some(row) = old_table_row {
                    let old_row_value = std::mem::replace(&mut row.values[i], Cell::Null);
                    if old_row_value == Cell::Null {
                        TextFormatConverter::default_value(&column_schema.typ)?
                    } else {
                        old_row_value
                    }
                } else {
                    TextFormatConverter::default_value(&column_schema.typ)?
                }
            }
            protocol::TupleData::Binary(_) => {
                bail!(
                    ErrorKind::ConversionError,
                    "Binary format is not supported in tuple data"
                );
            }
            protocol::TupleData::Text(bytes) => {
                let str = str::from_utf8(&bytes[..])?;
                TextFormatConverter::try_from_str(&column_schema.typ, str)?
            }
        };

        values.push(cell);
    }

    Ok(TableRow { values })
}

/// Converts a PostgreSQL insert message into an [`InsertEvent`].
///
/// This function processes an insert operation from the replication stream,
/// retrieves the table schema from the store, and constructs a complete
/// insert event with the new row data ready for ETL processing.
async fn convert_insert_to_event<S>(
    schema_store: &S,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    insert_body: &protocol::InsertBody,
) -> EtlResult<InsertEvent>
where
    S: SchemaStore,
{
    let table_id = insert_body.rel_id();
    let table_schema = get_table_schema(schema_store, TableId::new(table_id)).await?;

    let table_row = convert_tuple_to_row(
        &table_schema.column_schemas,
        insert_body.tuple().tuple_data(),
        &mut None,
        false,
    )?;

    Ok(InsertEvent {
        start_lsn,
        commit_lsn,
        table_id: TableId::new(table_id),
        table_row,
    })
}

/// Converts a PostgreSQL update message into an [`UpdateEvent`].
///
/// This function processes an update operation from the replication stream,
/// handling both the old and new row data. The old row data may be either
/// the complete row or just the key columns, depending on the table's
/// `REPLICA IDENTITY` setting in PostgreSQL.
async fn convert_update_to_event<S>(
    schema_store: &S,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    update_body: &protocol::UpdateBody,
) -> EtlResult<UpdateEvent>
where
    S: SchemaStore,
{
    let table_id = update_body.rel_id();
    let table_schema = get_table_schema(schema_store, TableId::new(table_id)).await?;

    // We try to extract the old tuple by either taking the entire old tuple or the key of the old
    // tuple.
    let is_key = update_body.old_tuple().is_none();
    let old_tuple = update_body.old_tuple().or(update_body.key_tuple());
    let old_table_row = match old_tuple {
        Some(identity) => Some(convert_tuple_to_row(
            &table_schema.column_schemas,
            identity.tuple_data(),
            &mut None,
            true,
        )?),
        None => None,
    };

    let mut old_table_row_mut = old_table_row;
    let table_row = convert_tuple_to_row(
        &table_schema.column_schemas,
        update_body.new_tuple().tuple_data(),
        &mut old_table_row_mut,
        false,
    )?;

    let old_table_row = old_table_row_mut.map(|row| (is_key, row));

    Ok(UpdateEvent {
        start_lsn,
        commit_lsn,
        table_id: TableId::new(table_id),
        table_row,
        old_table_row,
    })
}

/// Converts a PostgreSQL delete message into a [`DeleteEvent`].
///
/// This function processes a delete operation from the replication stream,
/// extracting the old row data that was deleted. The old row data may be
/// either the complete row or just the key columns, depending on the table's
/// `REPLICA IDENTITY` setting in PostgreSQL.
async fn convert_delete_to_event<S>(
    schema_store: &S,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    delete_body: &protocol::DeleteBody,
) -> EtlResult<DeleteEvent>
where
    S: SchemaStore,
{
    let table_id = delete_body.rel_id();
    let table_schema = get_table_schema(schema_store, TableId::new(table_id)).await?;

    // We try to extract the old tuple by either taking the entire old tuple or the key of the old
    // tuple.
    let is_key = delete_body.old_tuple().is_none();
    let old_tuple = delete_body.old_tuple().or(delete_body.key_tuple());
    let old_table_row = match old_tuple {
        Some(identity) => Some(convert_tuple_to_row(
            &table_schema.column_schemas,
            identity.tuple_data(),
            &mut None,
            true,
        )?),
        None => None,
    }
    .map(|row| (is_key, row));

    Ok(DeleteEvent {
        start_lsn,
        commit_lsn,
        table_id: TableId::new(table_id),
        old_table_row,
    })
}

/// Converts a PostgreSQL logical replication message into an [`Event`].
///
/// This is the main entry point for converting raw replication protocol messages
/// into strongly-typed events for ETL processing. It dispatches to specialized
/// conversion functions based on the message type and handles unsupported message
/// types by returning [`Event::Unsupported`].
pub async fn convert_message_to_event<S>(
    schema_store: &S,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    message: &LogicalReplicationMessage,
) -> EtlResult<Event>
where
    S: SchemaStore,
{
    match message {
        LogicalReplicationMessage::Begin(begin_body) => Ok(Event::Begin(
            BeginEvent::from_protocol(start_lsn, commit_lsn, begin_body),
        )),
        LogicalReplicationMessage::Commit(commit_body) => Ok(Event::Commit(
            CommitEvent::from_protocol(start_lsn, commit_lsn, commit_body),
        )),
        LogicalReplicationMessage::Relation(relation_body) => Ok(Event::Relation(
            RelationEvent::from_protocol(start_lsn, commit_lsn, relation_body)?,
        )),
        LogicalReplicationMessage::Insert(insert_body) => {
            let insert_event =
                convert_insert_to_event(schema_store, start_lsn, commit_lsn, insert_body).await?;
            Ok(Event::Insert(insert_event))
        }
        LogicalReplicationMessage::Update(update_body) => {
            let update_event =
                convert_update_to_event(schema_store, start_lsn, commit_lsn, update_body).await?;
            Ok(Event::Update(update_event))
        }
        LogicalReplicationMessage::Delete(delete_body) => {
            let delete_event =
                convert_delete_to_event(schema_store, start_lsn, commit_lsn, delete_body).await?;
            Ok(Event::Delete(delete_event))
        }
        LogicalReplicationMessage::Truncate(truncate_body) => Ok(Event::Truncate(
            TruncateEvent::from_protocol(start_lsn, commit_lsn, truncate_body),
        )),
        LogicalReplicationMessage::Origin(_) | LogicalReplicationMessage::Type(_) => {
            Ok(Event::Unsupported)
        }
        _ => bail!(
            ErrorKind::ConversionError,
            "Replication message not supported",
            format!("The replication message {:?} is not supported", message)
        ),
    }
}
