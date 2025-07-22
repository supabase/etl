use crate::conversions::Cell;
use crate::conversions::table_row::TableRow;
use crate::conversions::text::{FromTextError, TextFormatConverter};
use crate::schema::cache::SchemaCache;
use crate::state::store::base::StateStoreError;
use core::str;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres::types::convert_type_oid_to_type;
use postgres_replication::protocol;
use postgres_replication::protocol::LogicalReplicationMessage;
use std::{fmt, io, str::Utf8Error};
use thiserror::Error;
use tokio_postgres::types::PgLsn;

#[derive(Debug, Error)]
pub enum EventConversionError {
    #[error("An unknown replication message type was encountered")]
    UnknownReplicationMessage,

    #[error("Binary format is not supported for data conversion")]
    BinaryFormatNotSupported,

    #[error("The tuple data was not found for column {0} at index {0}")]
    TupleDataNotFound(String, usize),

    #[error("Missing tuple data in delete body")]
    MissingTupleInDeleteBody,

    #[error("Table schema not found for table id {0}")]
    MissingSchema(TableId),

    #[error("Error converting from bytes: {0}")]
    FromBytes(#[from] FromTextError),

    #[error("Invalid string value encountered: {0}")]
    InvalidStr(#[from] Utf8Error),

    #[error("IO error encountered: {0}")]
    Io(#[from] io::Error),

    #[error("An error occurred in the state store: {0}")]
    StateStore(#[from] StateStoreError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BeginEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub timestamp: i64,
    pub xid: u32,
}

impl BeginEvent {
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

#[derive(Debug, Clone, PartialEq)]
pub struct CommitEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub flags: i8,
    pub end_lsn: u64,
    pub timestamp: i64,
}

impl CommitEvent {
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

#[derive(Debug, Clone, PartialEq)]
pub struct RelationEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_schema: TableSchema,
}

impl RelationEvent {
    pub fn from_protocol(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        relation_body: &protocol::RelationBody,
    ) -> Result<Self, EventConversionError> {
        let table_name = TableName::new(
            relation_body.namespace()?.to_string(),
            relation_body.name()?.to_string(),
        );
        let column_schemas = relation_body
            .columns()
            .iter()
            .map(Self::build_column_schema)
            .collect::<Result<Vec<ColumnSchema>, _>>()?;
        let table_schema = TableSchema::new(relation_body.rel_id(), table_name, column_schemas);

        Ok(Self {
            start_lsn,
            commit_lsn,
            table_schema,
        })
    }

    fn build_column_schema(
        column: &protocol::Column,
    ) -> Result<ColumnSchema, EventConversionError> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct InsertEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_id: TableId,
    pub table_row: TableRow,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_id: TableId,
    pub table_row: TableRow,
    /// Represents the old table row that was deleted.
    ///
    /// The boolean represents whether the row contains only the `key` columns or not.
    pub old_table_row: Option<(bool, TableRow)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub table_id: TableId,
    /// Represents the old table row that was deleted.
    ///
    /// The boolean represents whether the row contains only the `key` columns or not.
    pub old_table_row: Option<(bool, TableRow)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TruncateEvent {
    pub start_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub options: i8,
    pub rel_ids: Vec<u32>,
}

impl TruncateEvent {
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

#[derive(Debug, Clone, PartialEq)]
pub struct KeepAliveEvent {
    pub reply: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Begin(BeginEvent),
    Commit(CommitEvent),
    Insert(InsertEvent),
    Update(UpdateEvent),
    Delete(DeleteEvent),
    Relation(RelationEvent),
    Truncate(TruncateEvent),
    Unsupported,
}

impl Event {
    /// Returns the [`EventType`] that corresponds to this event.
    pub fn event_type(&self) -> EventType {
        self.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    Begin,
    Commit,
    Insert,
    Update,
    Delete,
    Relation,
    Truncate,
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

async fn get_table_schema(
    schema_cache: &SchemaCache,
    table_id: TableId,
) -> Result<TableSchema, EventConversionError> {
    schema_cache
        .get_table_schema(&table_id)
        .await
        .ok_or(EventConversionError::MissingSchema(table_id))
}

fn convert_tuple_to_row(
    column_schemas: &[ColumnSchema],
    tuple_data: &[protocol::TupleData],
) -> Result<TableRow, EventConversionError> {
    let mut values = Vec::with_capacity(column_schemas.len());

    for (i, column_schema) in column_schemas.iter().enumerate() {
        // We are expecting that for each column, there is corresponding tuple data, even for null
        // values.
        let Some(tuple_data) = &tuple_data.get(i) else {
            return Err(EventConversionError::TupleDataNotFound(
                column_schema.name.clone(),
                i,
            ));
        };

        let cell = match tuple_data {
            // In case of a null value, we store the type information since that will be used to
            // correctly compute default values when needed.
            protocol::TupleData::Null => Cell::Null(column_schema.typ.clone()),
            protocol::TupleData::UnchangedToast => {
                TextFormatConverter::default_value(&column_schema.typ)
            }
            protocol::TupleData::Binary(_) => {
                return Err(EventConversionError::BinaryFormatNotSupported);
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

async fn convert_insert_to_event(
    schema_cache: &SchemaCache,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    insert_body: &protocol::InsertBody,
) -> Result<InsertEvent, EventConversionError> {
    let table_id = insert_body.rel_id();
    let table_schema = get_table_schema(schema_cache, table_id).await?;

    let table_row = convert_tuple_to_row(
        &table_schema.column_schemas,
        insert_body.tuple().tuple_data(),
    )?;

    Ok(InsertEvent {
        start_lsn,
        commit_lsn,
        table_id,
        table_row,
    })
}

async fn convert_update_to_event(
    schema_cache: &SchemaCache,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    update_body: &protocol::UpdateBody,
) -> Result<UpdateEvent, EventConversionError> {
    let table_id = update_body.rel_id();
    let table_schema = get_table_schema(schema_cache, table_id).await?;

    let table_row = convert_tuple_to_row(
        &table_schema.column_schemas,
        update_body.new_tuple().tuple_data(),
    )?;

    // We try to extract the old tuple by either taking the entire old tuple or the key of the old
    // tuple.
    let is_key = update_body.old_tuple().is_none();
    let old_tuple = update_body.old_tuple().or(update_body.key_tuple());
    let old_table_row = match old_tuple {
        Some(identity) => Some(convert_tuple_to_row(
            &table_schema.column_schemas,
            identity.tuple_data(),
        )?),
        None => None,
    }
    .map(|row| (is_key, row));

    Ok(UpdateEvent {
        start_lsn,
        commit_lsn,
        table_id,
        table_row,
        old_table_row,
    })
}

async fn convert_delete_to_event(
    schema_cache: &SchemaCache,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    delete_body: &protocol::DeleteBody,
) -> Result<DeleteEvent, EventConversionError> {
    let table_id = delete_body.rel_id();
    let table_schema = get_table_schema(schema_cache, table_id).await?;

    // We try to extract the old tuple by either taking the entire old tuple or the key of the old
    // tuple.
    let is_key = delete_body.old_tuple().is_none();
    let old_tuple = delete_body.old_tuple().or(delete_body.key_tuple());
    let old_table_row = match old_tuple {
        Some(identity) => Some(convert_tuple_to_row(
            &table_schema.column_schemas,
            identity.tuple_data(),
        )?),
        None => None,
    }
    .map(|row| (is_key, row));

    Ok(DeleteEvent {
        start_lsn,
        commit_lsn,
        table_id,
        old_table_row,
    })
}

pub async fn convert_message_to_event(
    schema_cache: &SchemaCache,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    message: &LogicalReplicationMessage,
) -> Result<Event, EventConversionError> {
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
                convert_insert_to_event(schema_cache, start_lsn, commit_lsn, insert_body).await?;
            Ok(Event::Insert(insert_event))
        }
        LogicalReplicationMessage::Update(update_body) => {
            let update_event =
                convert_update_to_event(schema_cache, start_lsn, commit_lsn, update_body).await?;
            Ok(Event::Update(update_event))
        }
        LogicalReplicationMessage::Delete(delete_body) => {
            let delete_event =
                convert_delete_to_event(schema_cache, start_lsn, commit_lsn, delete_body).await?;
            Ok(Event::Delete(delete_event))
        }
        LogicalReplicationMessage::Truncate(truncate_body) => Ok(Event::Truncate(
            TruncateEvent::from_protocol(start_lsn, commit_lsn, truncate_body),
        )),
        LogicalReplicationMessage::Origin(_) | LogicalReplicationMessage::Type(_) => {
            Ok(Event::Unsupported)
        }
        _ => Err(EventConversionError::UnknownReplicationMessage),
    }
}
