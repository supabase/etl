use core::str;
use std::collections::HashSet;

use etl_postgres::types::{
    ColumnSchema, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableSchema,
    convert_type_oid_to_type,
};
use postgres_replication::protocol;
use serde::Deserialize;
use tokio_postgres::types::PgLsn;

use crate::conversions::text::{default_value_for_type, parse_cell_from_postgres_text};
use crate::error::{ErrorKind, EtlResult};
use crate::types::{
    BeginEvent, Cell, CommitEvent, DeleteEvent, InsertEvent, TableRow, TruncateEvent, UpdateEvent,
};
use crate::{bail, etl_error};

/// The prefix used for DDL schema change messages emitted by the `etl.emit_schema_change_messages`
/// event trigger. Messages with this prefix contain JSON-encoded schema information.
pub const DDL_MESSAGE_PREFIX: &str = "supabase_etl_ddl";

/// Represents a schema change message emitted by Postgres event trigger.
///
/// This message is emitted when ALTER TABLE commands are executed on tables
/// that are part of a publication.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaChangeMessage {
    /// The DDL command that triggered this message (e.g., "ALTER TABLE").
    pub event: String,
    /// The schema name of the affected table.
    pub schema_name: String,
    /// The name of the affected table.
    pub table_name: String,
    /// The OID of the affected table.
    pub table_id: i64,
    /// The columns of the table after the schema change.
    pub columns: Vec<ColumnSchemaMessage>,
}

impl SchemaChangeMessage {
    /// Converts a [`SchemaChangeMessage`] to a [`TableSchema`] with a specific snapshot ID.
    ///
    /// This is used to update the stored table schema when a DDL change is detected.
    /// The snapshot_id should be the start_lsn of the DDL message.
    pub fn into_table_schema(self, snapshot_id: SnapshotId) -> TableSchema {
        let table_name = TableName::new(self.schema_name, self.table_name);
        let column_schemas = self
            .columns
            .into_iter()
            .map(|column| {
                let typ = convert_type_oid_to_type(column.type_oid);
                ColumnSchema::new(
                    column.name,
                    typ,
                    column.type_modifier,
                    column.ordinal_position,
                    column.primary_key_ordinal_position,
                    column.nullable,
                )
            })
            .collect();

        TableSchema::with_snapshot_id(
            TableId::new(self.table_id as u32),
            table_name,
            column_schemas,
            snapshot_id,
        )
    }
}

/// Represents a column schema in a schema change message.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnSchemaMessage {
    /// The name of the column.
    pub name: String,
    /// The OID of the column's data type.
    pub type_oid: u32,
    /// Type-specific modifier value (e.g., length for varchar).
    pub type_modifier: i32,
    /// The 1-based ordinal position of the column in the table.
    pub ordinal_position: i32,
    /// The 1-based ordinal position of this column in the primary key, or null if not a primary key.
    pub primary_key_ordinal_position: Option<i32>,
    /// Whether the column can contain NULL values.
    pub nullable: bool,
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

/// Returns the set of column names to replicate from a relation message.
pub fn parse_replicated_column_names(
    relation_body: &protocol::RelationBody,
) -> EtlResult<HashSet<String>> {
    let column_names = relation_body
        .columns()
        .iter()
        .map(parse_column_name_from_column)
        .collect::<Result<HashSet<String>, _>>()?;

    Ok(column_names)
}

/// Extracts the column name from a [`protocol::Column`] object.
fn parse_column_name_from_column(column: &protocol::Column) -> EtlResult<String> {
    let column_name = column.name()?.to_string();

    Ok(column_name)
}

/// Converts a Postgres insert message into an [`InsertEvent`].
///
/// This function processes an insert operation from the replication stream
/// and constructs an insert event with the new row data ready for ETL processing.
pub fn parse_event_from_insert_message(
    replicated_table_schema: ReplicatedTableSchema,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    insert_body: &protocol::InsertBody,
) -> EtlResult<InsertEvent> {
    let table_row = convert_tuple_to_row(
        replicated_table_schema.column_schemas(),
        insert_body.tuple().tuple_data(),
        &mut None,
        false,
    )?;

    Ok(InsertEvent {
        start_lsn,
        commit_lsn,
        replicated_table_schema,
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
    replicated_table_schema: ReplicatedTableSchema,
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
            replicated_table_schema.column_schemas(),
            identity.tuple_data(),
            &mut None,
            true,
        )?),
        None => None,
    };

    let mut old_table_row_mut = old_table_row;
    let table_row = convert_tuple_to_row(
        replicated_table_schema.column_schemas(),
        update_body.new_tuple().tuple_data(),
        &mut old_table_row_mut,
        false,
    )?;

    let old_table_row = old_table_row_mut.map(|row| (is_key, row));

    Ok(UpdateEvent {
        start_lsn,
        commit_lsn,
        replicated_table_schema,
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
    replicated_table_schema: ReplicatedTableSchema,
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
            replicated_table_schema.column_schemas(),
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
        replicated_table_schema,
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
    truncated_tables: Vec<ReplicatedTableSchema>,
) -> TruncateEvent {
    TruncateEvent {
        start_lsn,
        commit_lsn,
        options: truncate_body.options(),
        truncated_tables,
    }
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
pub fn convert_tuple_to_row<'a>(
    column_schemas: impl Iterator<Item = &'a ColumnSchema>,
    tuple_data: &[protocol::TupleData],
    old_table_row: &mut Option<TableRow>,
    use_default_for_missing_cols: bool,
) -> EtlResult<TableRow> {
    let mut values = Vec::with_capacity(tuple_data.len());

    for (i, column_schema) in column_schemas.enumerate() {
        // We are expecting that for each column, there is corresponding tuple data, even for null
        // values.
        let Some(tuple_data) = &tuple_data.get(i) else {
            bail!(
                ErrorKind::ConversionError,
                "Tuple data missing value at index"
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
                    "Binary format not supported in tuple data"
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

/// Parses a DDL schema change message from its JSON content.
///
/// Returns the parsed message if successful, or an error if the JSON is malformed.
pub fn parse_schema_change_message(content: &str) -> EtlResult<SchemaChangeMessage> {
    serde_json::from_str(content).map_err(|e| {
        etl_error!(
            ErrorKind::ConversionError,
            "Failed to parse schema change message",
            format!("Invalid JSON in schema change message: {}", e)
        )
    })
}
