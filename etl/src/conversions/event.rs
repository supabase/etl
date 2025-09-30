use core::str;
use etl_postgres::types::{ColumnSchema, TableId, TableSchema, convert_type_oid_to_type};
use postgres_replication::protocol;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tokio_postgres::types::PgLsn;

use crate::conversions::text::{default_value_for_type, parse_cell_from_postgres_text};
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::store::schema::SchemaStore;
use crate::types::{
    BeginEvent, Cell, CommitEvent, DeleteEvent, InsertEvent, RelationChange, RelationEvent,
    TableRow, TruncateEvent, UpdateEvent,
};
use crate::{bail, etl_error};

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

impl Ord for IndexedColumnSchema {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.name.cmp(&other.0.name)
    }
}

impl PartialOrd for IndexedColumnSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.name.cmp(&other.0.name))
    }
}

impl Hash for IndexedColumnSchema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.name.hash(state);
    }
}

/// Creates a [`BeginEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol begin message and extracts
/// transaction metadata for use in the ETL pipeline.
pub fn parse_event_from_begin_message(
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    begin_body: &protocol::BeginBody,
) -> BeginEvent {
    BeginEvent {
        start_lsn,
        commit_lsn,
        timestamp: begin_body.timestamp(),
        xid: begin_body.xid(),
    }
}

/// Creates a [`CommitEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol commit message and extracts
/// transaction completion metadata for use in the ETL pipeline.
pub fn parse_event_from_commit_message(
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    commit_body: &protocol::CommitBody,
) -> CommitEvent {
    CommitEvent {
        start_lsn,
        commit_lsn,
        flags: commit_body.flags(),
        end_lsn: commit_body.end_lsn(),
        timestamp: commit_body.timestamp(),
    }
}

/// Creates a [`RelationEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol relation message and builds
/// a complete table schema for use in interpreting subsequent data events.
pub async fn parse_event_from_relation_message<S>(
    schema_store: &S,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    relation_body: &protocol::RelationBody,
) -> EtlResult<RelationEvent>
where
    S: SchemaStore,
{
    let table_id = TableId::new(relation_body.rel_id());

    let Some(table_schema) = schema_store.get_table_schema(&table_id).await? else {
        bail!(
            ErrorKind::MissingTableSchema,
            "Table not found in the schema cache",
            format!("The table schema for table {table_id} was not found in the cache")
        )
    };

    // We build a set of the new column chemas from the relation message. The rationale for using a
    // BTreeSet is that we want to preserve the order of columns in the schema, which is important,
    // and also we can reuse the same set to build the vector of column schemas needed for the table
    // schema.
    let mut latest_column_schemas = relation_body
        .columns()
        .iter()
        .map(build_indexed_column_schema)
        .collect::<Result<BTreeSet<IndexedColumnSchema>, EtlError>>()?;

    // We build the updated table schema to store in the schema store.
    let mut latest_table_schema = TableSchema::new(
        table_schema.id,
        table_schema.name.clone(),
        Vec::with_capacity(latest_column_schemas.len()),
    );
    for column_schema in latest_column_schemas.iter() {
        latest_table_schema.add_column_schema(column_schema.clone().into_inner());
    }
    schema_store.store_table_schema(latest_table_schema).await?;

    // We process all the changes that we want to dispatch to the destination.
    let mut changes = vec![];
    for column_schema in table_schema.column_schemas.iter() {
        let column_schema = IndexedColumnSchema(column_schema.clone());
        let latest_column_schema = latest_column_schemas.take(&column_schema);
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
    for column_schema in latest_column_schemas {
        changes.push(RelationChange::AddColumn(column_schema.into_inner()));
    }

    Ok(RelationEvent {
        start_lsn,
        commit_lsn,
        changes,
        table_id,
    })
}

/// Converts a Postgres insert message into an [`InsertEvent`].
///
/// This function processes an insert operation from the replication stream,
/// retrieves the table schema from the store, and constructs a complete
/// insert event with the new row data ready for ETL processing.
pub async fn parse_event_from_insert_message<S>(
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

/// Converts a Postgres update message into an [`UpdateEvent`].
///
/// This function processes an update operation from the replication stream,
/// handling both the old and new row data. The old row data may be either
/// the complete row or just the key columns, depending on the table's
/// `REPLICA IDENTITY` setting in Postgres.
pub async fn parse_event_from_update_message<S>(
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

/// Converts a Postgres delete message into a [`DeleteEvent`].
///
/// This function processes a delete operation from the replication stream,
/// extracting the old row data that was deleted. The old row data may be
/// either the complete row or just the key columns, depending on the table's
/// `REPLICA IDENTITY` setting in Postgres.
pub async fn parse_event_from_delete_message<S>(
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

/// Creates a [`TruncateEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol truncate message and extracts
/// information about which tables were truncated and with what options.
pub fn parse_event_from_truncate_message(
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    truncate_body: &protocol::TruncateBody,
    overridden_rel_ids: Vec<u32>,
) -> TruncateEvent {
    TruncateEvent {
        start_lsn,
        commit_lsn,
        options: truncate_body.options(),
        rel_ids: overridden_rel_ids,
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

/// Constructs a [`IndexedColumnSchema`] from Postgres protocol column data.
///
/// This helper method extracts column metadata from the replication protocol
/// and converts it into the internal column schema representation. Some fields
/// like nullable status have default values due to protocol limitations.
fn build_indexed_column_schema(column: &protocol::Column) -> EtlResult<IndexedColumnSchema> {
    let column_schema = ColumnSchema::new(
        column.name()?.to_string(),
        convert_type_oid_to_type(column.type_id() as u32),
        column.type_modifier(),
        // Currently 1 means that the column is part of the primary key.
        column.flags() == 1,
    );

    Ok(IndexedColumnSchema(column_schema))
}

/// Converts Postgres tuple data into a [`TableRow`] using column schemas.
///
/// This function transforms raw tuple data from the replication protocol into
/// a structured row representation. It handles null values, unchanged TOAST data,
/// and binary data according to Postgres semantics. For unchanged TOAST values,
/// it attempts to reuse data from the old row if available.
///
/// # Panics
///
/// Panics if a required (non-nullable) column receives null data and
/// `use_default_for_missing_cols` is false, as this indicates protocol-level
/// corruption that should not be handled gracefully.
pub fn convert_tuple_to_row(
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
                    default_value_for_type(&column_schema.typ)?
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
                        default_value_for_type(&column_schema.typ)?
                    } else {
                        old_row_value
                    }
                } else {
                    default_value_for_type(&column_schema.typ)?
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
                parse_cell_from_postgres_text(&column_schema.typ, str)?
            }
        };

        values.push(cell);
    }

    Ok(TableRow { values })
}
