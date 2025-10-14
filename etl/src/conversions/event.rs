use core::str;
use etl_postgres::types::{
    ColumnSchema, SchemaVersion, TableId, TableName, TableSchema, VersionedTableSchema,
    convert_type_oid_to_type,
};
use postgres_replication::protocol;
use std::sync::Arc;
use tokio_postgres::types::PgLsn;

use crate::conversions::text::{default_value_for_type, parse_cell_from_postgres_text};
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::types::{
    BeginEvent, Cell, CommitEvent, DeleteEvent, InsertEvent, TableRow, TruncateEvent, UpdateEvent,
};
use crate::{bail, etl_error};

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

/// Creates a [`TableSchema`] from Postgres protocol data.
///
/// This method parses the replication protocol relation message and builds the table schema that
/// it represents.
pub async fn parse_table_schema_from_relation_message(
    relation_body: &protocol::RelationBody,
) -> EtlResult<TableSchema> {
    let table_id = TableId::new(relation_body.rel_id());
    let schema = relation_body.namespace().map_err(|error| {
        etl_error!(
            ErrorKind::InvalidData,
            "Invalid namespace in relation message",
            format!(
                "Failed to decode namespace for relation {}: {error}",
                relation_body.rel_id()
            )
        )
    })?;
    let table = relation_body.name().map_err(|error| {
        etl_error!(
            ErrorKind::InvalidData,
            "Invalid table name in relation message",
            format!(
                "Failed to decode table name for relation {}: {error}",
                relation_body.rel_id()
            )
        )
    })?;

    let table_name = TableName::new(schema.to_string(), table.to_string());

    // We construct the new column schemas in order. The order is important since the table schema
    // relies on the right ordering to interpret the Postgres correctly.
    let new_column_schemas = relation_body
        .columns()
        .iter()
        .map(build_column_schema)
        .collect::<Result<Vec<_>, EtlError>>()?;

    Ok(TableSchema::new(table_id, table_name, new_column_schemas))
}

/// Converts a Postgres insert message into an [`InsertEvent`].
///
/// This function processes an insert operation from the replication stream
/// using the supplied table schema version to build the resulting event.
pub fn parse_event_from_insert_message(
    table_schema: Arc<VersionedTableSchema>,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    insert_body: &protocol::InsertBody,
) -> EtlResult<InsertEvent> {
    let table_row = convert_tuple_to_row(
        &table_schema.column_schemas,
        insert_body.tuple().tuple_data(),
        &mut None,
        false,
    )?;

    Ok(InsertEvent {
        start_lsn,
        commit_lsn,
        schema_version: table_schema.version,
        table_id: table_schema.id,
        table_row,
    })
}

/// Converts a Postgres update message into an [`UpdateEvent`].
///
/// This function processes an update operation from the replication stream,
/// handling both the old and new row data. The old row data may be either
/// the complete row or just the key columns, depending on the table's
/// `REPLICA IDENTITY` setting in Postgres.
pub fn parse_event_from_update_message(
    table_schema: Arc<VersionedTableSchema>,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    update_body: &protocol::UpdateBody,
) -> EtlResult<UpdateEvent> {
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
        schema_version: table_schema.version,
        table_id: table_schema.id,
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
pub fn parse_event_from_delete_message(
    table_schema: Arc<VersionedTableSchema>,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    delete_body: &protocol::DeleteBody,
) -> EtlResult<DeleteEvent> {
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
        schema_version: table_schema.version,
        table_id: table_schema.id,
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
    table_ids: Vec<(TableId, SchemaVersion)>,
) -> TruncateEvent {
    TruncateEvent {
        start_lsn,
        commit_lsn,
        options: truncate_body.options(),
        table_ids,
    }
}

/// Constructs a [`IndexedColumnSchema`] from Postgres protocol column data.
///
/// This helper method extracts column metadata from the replication protocol
/// and converts it into the internal column schema representation. Some fields
/// like nullable status have default values due to protocol limitations.
fn build_column_schema(column: &protocol::Column) -> EtlResult<ColumnSchema> {
    let column_schema = ColumnSchema::new(
        column.name()?.to_string(),
        convert_type_oid_to_type(column.type_id() as u32),
        column.type_modifier(),
        // Currently 1 means that the column is part of the primary key.
        column.flags() == 1,
    );

    Ok(column_schema)
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
                    // This is a protocol level error, so we panic instead of carrying on
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
                // consistency. As a bit of a practical hack, we take the value out of the old row and
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
