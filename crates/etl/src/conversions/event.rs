use core::str;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use etl_postgres::types::{
    ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
    TableName, TableSchema, convert_type_oid_to_type,
};
use metrics::{counter, histogram};
use postgres_replication::protocol;
use serde::Deserialize;
use tokio_postgres::types::PgLsn;

use crate::{
    bail,
    conversions::text::parse_cell_from_postgres_text,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    metrics::{ETL_BYTES_PROCESSED_TOTAL, ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL},
    types::{
        BeginEvent, Cell, CommitEvent, DeleteEvent, InsertEvent, OldTableRow, PartialTableRow,
        TableRow, TruncateEvent, UpdateEvent, UpdatedTableRow,
    },
};

/// The prefix used for DDL schema change messages emitted by the
/// `etl.emit_schema_change_messages` event trigger. Messages with this prefix
/// contain JSON-encoded schema information.
pub(crate) const DDL_MESSAGE_PREFIX: &str = "supabase_etl_ddl";

/// Represents a schema change message emitted by Postgres event trigger.
///
/// This message is emitted when ALTER TABLE commands are executed on tables
/// that are part of a publication.
///
/// Unknown fields are ignored on purpose so the SQL payload can grow richer
/// without forcing a synchronized rollout of every consumer.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SchemaChangeMessage {
    /// The command tag from `pg_event_trigger_ddl_commands().command_tag`.
    pub(crate) command_tag: String,
    /// The schema name from `pg_namespace.nspname`.
    pub(crate) nspname: String,
    /// The table name from `pg_class.relname`.
    pub(crate) relname: String,
    /// The table OID from `pg_class.oid`.
    ///
    /// PostgreSQL table OIDs are `u32` values, but JSON serialization from the
    /// event trigger uses `bigint` (i64) for transmission. The cast back to
    /// `u32` in [`into_table_schema`] is safe because PostgreSQL OIDs are
    /// always within the `u32` range.
    pub(crate) oid: i64,
    /// The identity metadata emitted by Postgres for this table snapshot.
    pub(crate) identity: IdentityMessage,
    /// The columns of the table after the schema change.
    pub(crate) columns: Vec<ColumnSchemaMessage>,
}

impl SchemaChangeMessage {
    /// Returns the table identifier as [`TableId`].
    pub(crate) fn table_id(&self) -> TableId {
        TableId::new(self.oid as u32)
    }

    /// Converts a [`SchemaChangeMessage`] to a [`TableSchema`] with a specific
    /// snapshot ID.
    ///
    /// This is used to update the stored table schema when a DDL change is
    /// detected. The snapshot_id should be the start_lsn of the DDL
    /// message.
    pub(crate) fn into_table_schema(self, snapshot_id: SnapshotId) -> TableSchema {
        let table_id = self.table_id();
        build_table_schema(
            table_id,
            TableName::new(self.nspname, self.relname),
            self.columns,
            self.identity.primary_key_attnums,
            snapshot_id,
        )
    }
}

impl FromStr for SchemaChangeMessage {
    type Err = EtlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| {
            etl_error!(
                ErrorKind::ConversionError,
                "Failed to parse schema change message",
                format!("Invalid JSON in schema change message: {}", e)
            )
        })
    }
}

/// The identity metadata emitted by Postgres.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct IdentityMessage {
    /// The primary key columns in key order, expressed as `pg_attribute.attnum`
    /// values.
    pub(crate) primary_key_attnums: Vec<i32>,
    /// The replica-identity mode from `pg_class.relreplident`.
    pub(crate) relreplident: String,
    /// The replica-identity index columns in key order, expressed as
    /// `pg_attribute.attnum` values.
    pub(crate) replica_identity_index_attnums: Vec<i32>,
}

impl IdentityMessage {
    /// Builds the runtime identity mask for a replicated table schema.
    ///
    /// The returned mask is expressed in full table-schema width so it can be
    /// combined with the replication mask held by [`ReplicatedTableSchema`].
    ///
    /// `REPLICA IDENTITY FULL` uses the replicated columns themselves as the
    /// row identity. `DEFAULT` uses the primary key, `USING INDEX` uses the
    /// configured replica-identity index, and `NOTHING` produces an empty
    /// identity mask.
    pub(crate) fn build_identity_mask(
        &self,
        table_schema: &TableSchema,
        replication_mask: &ReplicationMask,
    ) -> EtlResult<IdentityMask> {
        match self.relreplident.as_str() {
            "f" => Ok(IdentityMask::from_bytes(replication_mask.as_slice().to_vec())),
            "d" => {
                Ok(Self::build_identity_mask_from_attnums(table_schema, &self.primary_key_attnums))
            }
            "i" => Ok(Self::build_identity_mask_from_attnums(
                table_schema,
                &self.replica_identity_index_attnums,
            )),
            "n" => Ok(IdentityMask::from_bytes(vec![0; table_schema.column_schemas.len()])),
            relreplident => {
                bail!(
                    ErrorKind::ConversionError,
                    "Invalid replica identity metadata",
                    format!(
                        "Unsupported replica identity mode '{}' for table '{}'",
                        relreplident, table_schema.name
                    )
                );
            }
        }
    }

    /// Builds an identity mask from ordered attribute numbers.
    fn build_identity_mask_from_attnums(
        table_schema: &TableSchema,
        attnums: &[i32],
    ) -> IdentityMask {
        let attnums: HashSet<i32> = attnums.iter().copied().collect();

        IdentityMask::from_bytes(
            table_schema
                .column_schemas
                .iter()
                .map(|column_schema| u8::from(attnums.contains(&column_schema.ordinal_position)))
                .collect(),
        )
    }
}

/// The column schema shape emitted by Postgres.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ColumnSchemaMessage {
    /// The column name from `pg_attribute.attname`.
    pub(crate) attname: String,
    /// The type OID from `pg_attribute.atttypid`.
    pub(crate) atttypid: u32,
    /// The type modifier from `pg_attribute.atttypmod`.
    pub(crate) atttypmod: i32,
    /// The physical column number from `pg_attribute.attnum`.
    pub(crate) attnum: i32,
    /// Whether the column is marked `NOT NULL` in `pg_attribute.attnotnull`.
    pub(crate) attnotnull: bool,
}

/// Builds [`ColumnSchema`] values from PostgreSQL-native schema and identity
/// snapshots.
///
/// The resulting columns are always sorted by `attnum`, preserving physical
/// table order, while `primary_key_ordinal_position` stays tied to the order of
/// `primary_key_attnums`.
pub(crate) fn build_column_schemas(
    mut columns: Vec<ColumnSchemaMessage>,
    primary_key_attnums: Vec<i32>,
) -> Vec<ColumnSchema> {
    let primary_key_positions: HashMap<i32, i32> = primary_key_attnums
        .into_iter()
        .enumerate()
        .map(|(index, attnum)| (attnum, (index + 1) as i32))
        .collect();

    // We sort columns by their ordinal position to keep the ordering consistent
    // within the application.
    columns.sort_by_key(|column| column.attnum);

    columns
        .into_iter()
        .map(|column| {
            let typ = convert_type_oid_to_type(column.atttypid);
            ColumnSchema::new(
                column.attname,
                typ,
                column.atttypmod,
                column.attnum,
                primary_key_positions.get(&column.attnum).copied(),
                !column.attnotnull,
            )
        })
        .collect()
}

/// Builds a [`TableSchema`] from PostgreSQL-native schema and identity
/// snapshots.
///
/// This is shared by bootstrap schema loading and DDL message handling so both
/// paths produce the exact same [`TableSchema`] representation.
pub(crate) fn build_table_schema(
    table_id: TableId,
    table_name: TableName,
    columns: Vec<ColumnSchemaMessage>,
    primary_key_attnums: Vec<i32>,
    snapshot_id: SnapshotId,
) -> TableSchema {
    TableSchema::with_snapshot_id(
        table_id,
        table_name,
        build_column_schemas(columns, primary_key_attnums),
        snapshot_id,
    )
}

/// Calculates the total payload size of tuple data from a replication message.
///
/// This is used only for coarse event-size metrics, so `NULL` and
/// `UnchangedToast` fields contribute zero bytes.
fn calculate_tuple_bytes(tuple_data: &[protocol::TupleData]) -> u64 {
    tuple_data
        .iter()
        .map(|data| match data {
            protocol::TupleData::Null | protocol::TupleData::UnchangedToast => 0,
            protocol::TupleData::Text(bytes) | protocol::TupleData::Binary(bytes) => {
                bytes.len() as u64
            }
        })
        .sum()
}

/// Creates a [`BeginEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol begin message and extracts
/// transaction metadata for use in the ETL pipeline.
pub(crate) fn parse_event_from_begin_message(
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    begin_body: &protocol::BeginBody,
) -> BeginEvent {
    BeginEvent {
        start_lsn,
        commit_lsn,
        tx_ordinal,
        timestamp: begin_body.timestamp(),
        xid: begin_body.xid(),
    }
}

/// Creates a [`CommitEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol commit message and extracts
/// transaction completion metadata for use in the ETL pipeline.
pub(crate) fn parse_event_from_commit_message(
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    commit_body: &protocol::CommitBody,
) -> CommitEvent {
    CommitEvent {
        start_lsn,
        commit_lsn,
        tx_ordinal,
        flags: commit_body.flags(),
        end_lsn: commit_body.end_lsn().into(),
        timestamp: commit_body.timestamp(),
    }
}

/// Returns the set of column names to replicate from a relation message.
///
/// PostgreSQL pgoutput emits relation columns by iterating the relation
/// `TupleDesc` in physical `pg_attribute.attnum` order and skipping columns
/// that are not published.
///
/// This function intentionally returns a [`HashSet`]: mask membership is built
/// by column name, and PostgreSQL requires live column names to be unique
/// within one table schema version. Relation-message order is therefore not
/// needed to decide which columns are included. The order matters later, when
/// tuple data is decoded, because tuple fields are sent in the same order as
/// the relation message. Applying the name-based masks to ETL's stored,
/// `attnum`-ordered [`TableSchema`] creates the [`ReplicatedTableSchema`] view
/// used for positional tuple decoding.
pub(crate) fn parse_replicated_column_names(
    relation_body: &protocol::RelationBody,
) -> EtlResult<HashSet<String>> {
    let column_names = relation_body
        .columns()
        .iter()
        .map(|column| column.name().map(ToString::to_string))
        .collect::<Result<HashSet<String>, _>>()?;

    Ok(column_names)
}

/// Returns the set of relation-message key column names.
///
/// PostgreSQL exposes replica-identity mode on the relation itself and
/// key-column membership on each [`protocol::RelationBody`] column. For
/// `REPLICA IDENTITY FULL`, every replicated column belongs to the old-row
/// identity. Otherwise the low bit of the column flags marks identity
/// membership. The column order is the same `attnum` order described in
/// [`parse_replicated_column_names`]. This returns names because identity-mask
/// membership is also name-based; tuple interpretation relies on the resulting
/// replicated schema preserving relation-message order after the masks are
/// applied.
pub(crate) fn parse_replica_identity_column_names(
    relation_body: &protocol::RelationBody,
) -> EtlResult<HashSet<String>> {
    let column_names = match relation_body.replica_identity() {
        protocol::ReplicaIdentity::Full => relation_body
            .columns()
            .iter()
            .map(|column| column.name().map(ToString::to_string))
            .collect::<Result<HashSet<String>, _>>()?,
        _ => relation_body
            .columns()
            .iter()
            .filter(|column| column.flags() & 1 == 1)
            .map(|column| column.name().map(ToString::to_string))
            .collect::<Result<HashSet<String>, _>>()?,
    };

    Ok(column_names)
}

/// Converts a Postgres insert message into an [`InsertEvent`].
///
/// This function processes an insert operation from the replication stream
/// and constructs an insert event with the new row data ready for ETL
/// processing.
pub(crate) fn parse_event_from_insert_message(
    replicated_table_schema: ReplicatedTableSchema,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    insert_body: &protocol::InsertBody,
) -> EtlResult<InsertEvent> {
    let tuple_data = insert_body.tuple().tuple_data();
    let row_size_bytes = calculate_tuple_bytes(tuple_data);

    counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => "insert").increment(row_size_bytes);

    histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => "insert").record(row_size_bytes as f64);

    let table_row = convert_tuple_to_row(replicated_table_schema.column_schemas(), tuple_data)?;

    Ok(InsertEvent { start_lsn, commit_lsn, tx_ordinal, replicated_table_schema, table_row })
}

/// Converts a Postgres update message into an [`UpdateEvent`].
///
/// This function preserves pgoutput's update tuple markers:
///
/// - `REPLICA IDENTITY FULL` emits `old_tuple`, which we decode as
///   [`OldTableRow::Full`] for every published update.
/// - `REPLICA IDENTITY DEFAULT` with a primary key and `USING INDEX` may emit
///   `key_tuple`, which we decode as [`OldTableRow::Key`]. PostgreSQL does this
///   when any replica-identity column changed, or when a replica-identity
///   column has external data that must be available from the old tuple.
/// - `REPLICA IDENTITY DEFAULT` with a primary key and `USING INDEX` otherwise
///   emit no old-side tuple and `old_table_row` stays `None`.
/// - `REPLICA IDENTITY NOTHING`, and `DEFAULT` without a primary key, do not
///   produce published update messages when the table publishes updates. The
///   source `UPDATE` is rejected before pgoutput emits an event.
/// - `REPLICA IDENTITY FULL` updates with no old row are not emitted by
///   PostgreSQL pgoutput.
///
/// The new tuple is decoded separately. `UnchangedToast` values are recovered
/// from the old-side row image when possible; otherwise the result is
/// [`UpdatedTableRow::Partial`].
pub(crate) fn parse_event_from_update_message(
    replicated_table_schema: ReplicatedTableSchema,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    update_body: &protocol::UpdateBody,
) -> EtlResult<UpdateEvent> {
    // PostgreSQL can attach either a full old tuple (`old_tuple`) or only the
    // replica-identity columns (`key_tuple`) to an update. If neither is
    // present, the publisher determined that no old-side image was required
    // for this key-based replica-identity update. We preserve that shape in
    // `OldTableRow`. The new tuple is decoded separately below, where the old
    // row acts only as a source for resolving `UnchangedToast`.
    let is_key = update_body.old_tuple().is_none();
    let old_tuple = update_body.old_tuple().or(update_body.key_tuple());

    // Calculate total bytes from both old and new tuple data.
    let new_tuple_data = update_body.new_tuple().tuple_data();
    let mut total_bytes = calculate_tuple_bytes(new_tuple_data);
    if let Some(identity) = &old_tuple {
        total_bytes += calculate_tuple_bytes(identity.tuple_data());
    }
    counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => "update").increment(total_bytes);

    let old_table_row = match old_tuple {
        Some(identity) if is_key => Some(OldTableRow::Key(normalize_key_tuple_to_row(
            &replicated_table_schema,
            identity.tuple_data(),
        )?)),
        Some(identity) => Some(OldTableRow::Full(convert_tuple_to_row(
            replicated_table_schema.column_schemas(),
            identity.tuple_data(),
        )?)),
        None => None,
    };

    // Old-row shape matters only for `UnchangedToast` resolution:
    // - full rows can recover any unchanged column by index.
    // - key rows can recover only unchanged key columns, consuming key values in
    //   replicated table order as we walk the schema below.
    let table_row = convert_update_tuple_to_updated_table_row(
        &replicated_table_schema,
        new_tuple_data,
        old_table_row.as_ref(),
    )?;

    histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => "update").record(total_bytes as f64);

    Ok(UpdateEvent {
        start_lsn,
        commit_lsn,
        tx_ordinal,
        replicated_table_schema,
        updated_table_row: table_row,
        old_table_row,
    })
}

/// Converts a Postgres delete message into a [`DeleteEvent`].
///
/// Delete messages carry only an old-side row image:
///
/// - `REPLICA IDENTITY FULL` emits `old_tuple`, which we decode as
///   [`OldTableRow::Full`].
/// - `REPLICA IDENTITY DEFAULT` with a primary key and `USING INDEX` emit
///   `key_tuple`, which we decode as [`OldTableRow::Key`].
/// - `REPLICA IDENTITY NOTHING`, and `DEFAULT` without a primary key, do not
///   produce published delete messages when the table publishes deletes. The
///   source `DELETE` is rejected before pgoutput emits an event.
///
/// PostgreSQL pgoutput sends an old-side tuple for every published delete. This
/// decoder preserves that tuple shape, normalizing key tuples into dense
/// replica-identity rows in replicated table order.
pub(crate) fn parse_event_from_delete_message(
    replicated_table_schema: ReplicatedTableSchema,
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    delete_body: &protocol::DeleteBody,
) -> EtlResult<DeleteEvent> {
    // Published delete messages carry an old-side image. PostgreSQL sends
    // either the full old row or only the replica-identity columns, and we
    // preserve that shape here.
    let is_key = delete_body.old_tuple().is_none();
    let old_tuple = delete_body.old_tuple().or(delete_body.key_tuple());

    if let Some(identity) = &old_tuple {
        let row_size_bytes = calculate_tuple_bytes(identity.tuple_data());

        counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => "delete").increment(row_size_bytes);

        histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => "delete").record(row_size_bytes as f64);
    }

    let old_table_row = match old_tuple {
        Some(identity) if is_key => Some(OldTableRow::Key(normalize_key_tuple_to_row(
            &replicated_table_schema,
            identity.tuple_data(),
        )?)),
        Some(identity) => Some(OldTableRow::Full(convert_tuple_to_row(
            replicated_table_schema.column_schemas(),
            identity.tuple_data(),
        )?)),
        None => None,
    };

    Ok(DeleteEvent { start_lsn, commit_lsn, tx_ordinal, replicated_table_schema, old_table_row })
}

/// Creates a [`TruncateEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol truncate message and extracts
/// information about which tables were truncated and with what options.
pub(crate) fn parse_event_from_truncate_message(
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    truncate_body: &protocol::TruncateBody,
    truncated_tables: Vec<ReplicatedTableSchema>,
) -> TruncateEvent {
    TruncateEvent {
        start_lsn,
        commit_lsn,
        tx_ordinal,
        options: truncate_body.options(),
        truncated_tables,
    }
}

/// Converts a full tuple image into a dense [`TableRow`].
///
/// The tuple width must exactly match the number of provided column schemas.
/// Every field must decode to a concrete [`Cell`]; `UnchangedToast` is
/// therefore rejected here because full row images must be self-contained.
fn convert_tuple_to_row<'a>(
    column_schemas: impl ExactSizeIterator<Item = &'a ColumnSchema>,
    tuple_data: &[protocol::TupleData],
) -> EtlResult<TableRow> {
    let column_count = column_schemas.len();
    if tuple_data.len() != column_count {
        bail!(
            ErrorKind::ConversionError,
            "Tuple data field count does not match schema",
            format!("Expected {} tuple values, got {}", column_count, tuple_data.len())
        );
    }

    let mut values = Vec::with_capacity(column_count);
    for (index, (column_schema, tuple_data)) in column_schemas.zip(tuple_data.iter()).enumerate() {
        let ConvertedTupleCell::Present(value) =
            convert_tuple_data_to_cell(index, column_schema, tuple_data, None)?
        else {
            bail!(
                ErrorKind::ConversionError,
                "Tuple missing source value for full row image",
                format!(
                    "Column '{}' at position {} was omitted from a row that requires a full image",
                    column_schema.name,
                    index + 1
                )
            );
        };

        values.push(value);
    }

    Ok(TableRow::new(values))
}

/// Converts a Postgres update tuple into a full or partial new row image.
///
/// `old_table_row` preserves PostgreSQL's original old-row shape.
///
/// This function is update-specific: it decodes the new tuple and optionally
/// consults the old row to resolve `UnchangedToast` fields.
///
/// Full rows can resolve any `UnchangedToast` field by column index. Key rows
/// can resolve only unchanged key columns, consuming key values in replicated
/// table-column order as the decoder walks the schema. This means the shared
/// key-row decoding logic stays generic, while the update-only meaning of that
/// old row stays localized here.
///
/// When PostgreSQL emits `UnchangedToast` for a column that cannot be resolved
/// from the available old-row image, the column position is marked missing and
/// the result becomes [`UpdatedTableRow::Partial`].
fn convert_update_tuple_to_updated_table_row(
    replicated_table_schema: &ReplicatedTableSchema,
    tuple_data: &[protocol::TupleData],
    old_table_row: Option<&OldTableRow>,
) -> EtlResult<UpdatedTableRow> {
    let column_count = replicated_table_schema.column_schemas().len();
    if tuple_data.len() != column_count {
        bail!(
            ErrorKind::ConversionError,
            "Tuple data field count does not match schema",
            format!("Expected {} tuple values, got {}", column_count, tuple_data.len())
        );
    }

    let mut old_row_resolver = OldRowResolver::new(old_table_row, column_count)?;

    let mut full_values = Vec::with_capacity(column_count);
    let mut present_values = Vec::new();
    let mut missing_column_indexes = Vec::new();
    let mut partial_row = false;

    let mut identity_columns = replicated_table_schema.identity_column_schemas().peekable();
    for (index, (column_schema, tuple_data)) in
        replicated_table_schema.column_schemas().zip(tuple_data.iter()).enumerate()
    {
        // We try to resolve the old row so that we can use its values to avoid
        // submitting a partial table row due to unchanged toast values in the
        // tuple data.
        let is_identity = identity_columns.peek().is_some_and(|identity_column| {
            identity_column.ordinal_position == column_schema.ordinal_position
        });
        let old_value = old_row_resolver.value_for_column(is_identity)?;
        if is_identity {
            let _ = identity_columns.next();
        }
        match convert_tuple_data_to_cell(index, column_schema, tuple_data, old_value)? {
            ConvertedTupleCell::Present(value) if partial_row => {
                present_values.push(value);
            }
            ConvertedTupleCell::Present(value) => full_values.push(value),
            ConvertedTupleCell::Missing => {
                if !partial_row {
                    // This is the first column we cannot reconstruct. Up to this
                    // point `full_values` held a dense prefix of known values, so
                    // we move that prefix into `present_values` and continue
                    // collecting only the values we do know plus the indexes we
                    // do not.
                    present_values.reserve(column_count.saturating_sub(index));
                    present_values.append(&mut full_values);
                    partial_row = true;
                }
                // Missing indexes stay in replicated-column order so consumers
                // can align the sparse row against the attached replicated
                // schema without guessing positions.
                missing_column_indexes.push(index);
            }
        }
    }

    old_row_resolver.finish()?;

    if partial_row {
        Ok(UpdatedTableRow::Partial(PartialTableRow::new(
            column_count,
            TableRow::new(present_values),
            missing_column_indexes,
        )))
    } else {
        Ok(UpdatedTableRow::Full(TableRow::new(full_values)))
    }
}

/// Resolves old-row values while decoding an update new tuple.
///
/// This resolver exists only for update decoding.
///
/// Delete events can forward the old row as-is, but update decoding may need to
/// consult the old row to resolve `UnchangedToast` fields in the new tuple.
/// Key-row resolution consumes values in replicated table order as the decoder
/// walks the schema.
#[derive(Debug)]
enum OldRowResolver<'a> {
    None,
    Full { values: &'a [Cell], next_column_index: usize },
    Key { values: &'a [Cell], next_key_index: usize },
}

impl<'a> OldRowResolver<'a> {
    /// Creates a resolver for the provided old row image.
    ///
    /// Full rows must match the replicated schema width. Key rows are kept
    /// dense and are validated incrementally as key columns are consumed.
    fn new(old_table_row: Option<&'a OldTableRow>, column_count: usize) -> EtlResult<Self> {
        match old_table_row {
            Some(OldTableRow::Full(row)) => {
                if row.values().len() != column_count {
                    bail!(
                        ErrorKind::ConversionError,
                        "Old tuple row width does not match schema",
                        format!(
                            "Expected {} old row values, got {}",
                            column_count,
                            row.values().len()
                        )
                    );
                }

                Ok(Self::Full { values: row.values(), next_column_index: 0 })
            }
            Some(OldTableRow::Key(row)) => {
                Ok(Self::Key { values: row.values(), next_key_index: 0 })
            }
            None => Ok(Self::None),
        }
    }

    /// Returns the old value aligned with the next schema column when
    /// available.
    ///
    /// For full rows this advances one value per schema column. For key rows
    /// this advances only when the current schema column belongs to the key.
    fn value_for_column(&mut self, is_identity_column: bool) -> EtlResult<Option<&'a Cell>> {
        match self {
            Self::None => Ok(None),
            Self::Full { values, next_column_index } => {
                let Some(value) = values.get(*next_column_index) else {
                    bail!(
                        ErrorKind::ConversionError,
                        "Old tuple row width does not match schema",
                        format!(
                            "Expected at least {} old row values, got {}",
                            *next_column_index + 1,
                            values.len()
                        )
                    );
                };

                *next_column_index += 1;
                Ok(Some(value))
            }
            Self::Key { values, next_key_index } => {
                if !is_identity_column {
                    return Ok(None);
                }

                let Some(value) = values.get(*next_key_index) else {
                    bail!(
                        ErrorKind::ConversionError,
                        "Replica-identity tuple shape does not match schema",
                        format!(
                            "Expected at least {} key values, got {}",
                            *next_key_index + 1,
                            values.len()
                        )
                    );
                };

                *next_key_index += 1;
                Ok(Some(value))
            }
        }
    }

    /// Verifies that the key row width matched the number of key columns
    /// encountered during the schema walk.
    fn finish(&self) -> EtlResult<()> {
        match self {
            Self::Full { values, next_column_index } if *next_column_index != values.len() => {
                bail!(
                    ErrorKind::ConversionError,
                    "Old tuple row width does not match schema",
                    format!("Expected {} old row values, got {}", *next_column_index, values.len())
                );
            }
            Self::Key { values, next_key_index } if *next_key_index != values.len() => {
                bail!(
                    ErrorKind::ConversionError,
                    "Replica-identity tuple shape does not match schema",
                    format!("Expected {} key values, got {}", *next_key_index, values.len())
                );
            }
            _ => {}
        }

        Ok(())
    }
}

/// Converts a dense key-image tuple into a dense row containing only
/// replica-identity columns in replicated table-column order.
fn convert_dense_key_tuple_to_row(
    identity_column_schemas: &[&ColumnSchema],
    tuple_data: &[protocol::TupleData],
) -> EtlResult<TableRow> {
    let mut values = Vec::with_capacity(identity_column_schemas.len());

    for (i, (column_schema, tuple_data)) in
        identity_column_schemas.iter().zip(tuple_data.iter()).enumerate()
    {
        let ConvertedTupleCell::Present(value) =
            convert_tuple_data_to_cell(i, column_schema, tuple_data, None)?
        else {
            bail!(
                ErrorKind::ConversionError,
                "Replica-identity tuple missing source value",
                format!(
                    "Replica-identity column '{}' did not carry a concrete value",
                    column_schema.name
                )
            );
        };
        values.push(value);
    }

    Ok(TableRow::new(values))
}

/// Converts a full-width key-image tuple into a dense row containing only
/// replica-identity columns in replicated table-column order.
fn convert_full_width_key_tuple_to_row(
    identity_column_schemas: &[&ColumnSchema],
    replicated_column_schemas: &[&ColumnSchema],
    tuple_data: &[protocol::TupleData],
) -> EtlResult<TableRow> {
    let mut values = Vec::with_capacity(identity_column_schemas.len());
    let mut identity_columns = identity_column_schemas.iter().peekable();

    for (i, (column_schema, tuple_data)) in
        replicated_column_schemas.iter().zip(tuple_data.iter()).enumerate()
    {
        if !identity_columns.peek().is_some_and(|identity_column| {
            identity_column.ordinal_position == column_schema.ordinal_position
        }) {
            continue;
        }

        let Some(identity_column) = identity_columns.next() else {
            bail!(
                ErrorKind::ConversionError,
                "Replica-identity tuple shape does not match schema",
                "Replica-identity column iterator ended unexpectedly"
            );
        };
        let ConvertedTupleCell::Present(value) =
            convert_tuple_data_to_cell(i, identity_column, tuple_data, None)?
        else {
            bail!(
                ErrorKind::ConversionError,
                "Replica-identity tuple missing source value",
                format!(
                    "Replica-identity column '{}' did not carry a concrete value",
                    identity_column.name
                )
            );
        };
        values.push(value);
    }

    Ok(TableRow::new(values))
}

/// Normalizes a key-image tuple into a dense row containing only
/// replica-identity columns in replicated table-column order.
///
/// This function is shared by update and delete parsing because the decoding
/// rule for a PostgreSQL key image is the same in both cases. The semantic
/// difference between update and delete lives above this boundary:
/// - updates use the decoded key row only as an old-side helper for new-row
///   reconstruction.
/// - deletes forward the decoded key row as the entire old-side payload.
///
/// PostgreSQL may encode a key image either as:
/// - a dense tuple containing only replica-identity values, or
/// - a tuple aligned to the full replicated schema.
///
/// In both cases this function normalizes the result to the internal dense
/// key-row shape so downstream code does not need to reason about the wire
/// layout it came from.
fn normalize_key_tuple_to_row(
    replicated_table_schema: &ReplicatedTableSchema,
    tuple_data: &[protocol::TupleData],
) -> EtlResult<TableRow> {
    let identity_column_schemas: Vec<_> =
        replicated_table_schema.identity_column_schemas().collect();
    let identity_column_count = identity_column_schemas.len();
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    let replicated_column_count = replicated_column_schemas.len();

    if identity_column_count == 0 {
        bail!(
            ErrorKind::ConversionError,
            "Replica-identity tuple missing key columns",
            "Key-image row was received for a table without replicated replica-identity columns"
        );
    }

    match tuple_data.len() {
        len if len == identity_column_count => {
            convert_dense_key_tuple_to_row(&identity_column_schemas, tuple_data)
        }
        len if len == replicated_column_count => convert_full_width_key_tuple_to_row(
            &identity_column_schemas,
            &replicated_column_schemas,
            tuple_data,
        ),
        _ => {
            bail!(
                ErrorKind::ConversionError,
                "Replica-identity tuple shape does not match schema",
                format!(
                    "Expected {} key values or {} replicated values for key image, got {}",
                    identity_column_count,
                    replicated_column_count,
                    tuple_data.len()
                )
            );
        }
    }
}

/// Result of decoding a single tuple field.
#[derive(Debug)]
enum ConvertedTupleCell {
    /// The field decoded to a concrete [`Cell`] value.
    Present(Cell),
    /// The field was omitted by PostgreSQL and remains unknown.
    Missing,
}

/// Converts one Postgres tuple field into a [`Cell`] or a missing marker.
///
/// `old_value` is used only to resolve `UnchangedToast` values while decoding
/// update new-tuples.
fn convert_tuple_data_to_cell(
    _index: usize,
    column_schema: &ColumnSchema,
    tuple_data: &protocol::TupleData,
    old_value: Option<&Cell>,
) -> EtlResult<ConvertedTupleCell> {
    match tuple_data {
        protocol::TupleData::Null => {
            // If a column schema is nullable and there is no value, it's fine, but if it's
            // not nullable this is a problem, and we need to raise it.
            if column_schema.nullable {
                Ok(ConvertedTupleCell::Present(Cell::Null))
            } else {
                bail!(
                    ErrorKind::InvalidData,
                    "Required column missing from tuple",
                    format!(
                        "Non-nullable column '{}' received NULL value, indicating protocol-level \
                         corruption",
                        column_schema.name
                    )
                );
            }
        }
        protocol::TupleData::UnchangedToast => {
            // PostgreSQL always sends the update's new tuple, but unchanged
            // TOASTed values can be represented as `UnchangedToast` instead of
            // an actual value. The caller passes an aligned old value when it
            // can recover one from the available old-row image; otherwise we
            // surface the field as missing and let the caller produce a
            // partial updated row.
            if let Some(old_value) = old_value {
                Ok(ConvertedTupleCell::Present(old_value.clone()))
            } else {
                Ok(ConvertedTupleCell::Missing)
            }
        }
        protocol::TupleData::Text(bytes) => {
            let str = str::from_utf8(&bytes[..])?;
            parse_cell_from_postgres_text(&column_schema.typ, str).map(ConvertedTupleCell::Present)
        }
        protocol::TupleData::Binary(_) => {
            bail!(ErrorKind::ConversionError, "Binary format not supported in tuple data");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use etl_postgres::types::{ColumnSchema, IdentityType, ReplicationMask};
    use postgres_replication::protocol::{LogicalReplicationMessage, TupleData};
    use tokio_postgres::types::Type;

    use super::{
        IdentityMessage, convert_tuple_to_row, convert_update_tuple_to_updated_table_row,
        normalize_key_tuple_to_row, parse_event_from_delete_message,
        parse_event_from_update_message,
    };
    use crate::{
        error::ErrorKind,
        types::{
            Cell, DeleteEvent, OldTableRow, PartialTableRow, PgLsn, ReplicatedTableSchema, TableId,
            TableName, TableRow, TableSchema, UpdateEvent, UpdatedTableRow,
        },
    };

    #[derive(Clone, Copy)]
    enum TestTupleData<'a> {
        Null,
        UnchangedToast,
        Text(&'a str),
    }

    fn event_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            columns,
        ));

        ReplicatedTableSchema::all(Arc::clone(&table_schema))
    }

    fn composite_primary_key_schema() -> ReplicatedTableSchema {
        event_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(2), false),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, false),
            ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, Some(1), false),
            ColumnSchema::new("city".to_owned(), Type::TEXT, -1, 4, None, false),
            ColumnSchema::new("large_text".to_owned(), Type::TEXT, -1, 5, None, false),
        ])
    }

    fn alternative_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(43),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(2), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, Some(1), false),
                ColumnSchema::new("city".to_owned(), Type::TEXT, -1, 4, None, false),
            ],
        ));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            etl_postgres::types::IdentityMask::from_bytes(vec![0, 1, 1, 0]),
        )
    }

    fn full_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(44),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(2), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, Some(1), false),
                ColumnSchema::new("city".to_owned(), Type::TEXT, -1, 4, None, false),
            ],
        ));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            etl_postgres::types::IdentityMask::from_bytes(vec![1, 1, 1, 1]),
        )
    }

    fn encode_tuple(buf: &mut Vec<u8>, tuple_data: &[TestTupleData<'_>]) {
        buf.extend_from_slice(&(i16::try_from(tuple_data.len()).unwrap()).to_be_bytes());

        for cell in tuple_data {
            match cell {
                TestTupleData::Null => buf.push(b'n'),
                TestTupleData::UnchangedToast => buf.push(b'u'),
                TestTupleData::Text(value) => {
                    buf.push(b't');
                    buf.extend_from_slice(&(i32::try_from(value.len()).unwrap()).to_be_bytes());
                    buf.extend_from_slice(value.as_bytes());
                }
            }
        }
    }

    fn parse_update_body_from_parts(
        rel_id: u32,
        old_tuple: Option<&[TestTupleData<'_>]>,
        key_tuple: Option<&[TestTupleData<'_>]>,
        new_tuple: &[TestTupleData<'_>],
    ) -> postgres_replication::protocol::UpdateBody {
        let mut bytes = Vec::new();
        bytes.push(b'U');
        bytes.extend_from_slice(&rel_id.to_be_bytes());

        match (old_tuple, key_tuple) {
            (Some(old_tuple), None) => {
                bytes.push(b'O');
                encode_tuple(&mut bytes, old_tuple);
            }
            (None, Some(key_tuple)) => {
                bytes.push(b'K');
                encode_tuple(&mut bytes, key_tuple);
            }
            (None, None) => {}
            (Some(_), Some(_)) => panic!("update body cannot contain both old and key tuples"),
        }

        bytes.push(b'N');
        encode_tuple(&mut bytes, new_tuple);

        let message = LogicalReplicationMessage::parse(&Bytes::from(bytes)).unwrap();
        let LogicalReplicationMessage::Update(body) = message else {
            panic!("expected update body");
        };

        body
    }

    fn parse_delete_body_from_parts(
        rel_id: u32,
        old_tuple: Option<&[TestTupleData<'_>]>,
        key_tuple: Option<&[TestTupleData<'_>]>,
    ) -> postgres_replication::protocol::DeleteBody {
        let mut bytes = Vec::new();
        bytes.push(b'D');
        bytes.extend_from_slice(&rel_id.to_be_bytes());

        match (old_tuple, key_tuple) {
            (Some(old_tuple), None) => {
                bytes.push(b'O');
                encode_tuple(&mut bytes, old_tuple);
            }
            (None, Some(key_tuple)) => {
                bytes.push(b'K');
                encode_tuple(&mut bytes, key_tuple);
            }
            (None, None) => panic!("delete body requires either old or key tuple"),
            (Some(_), Some(_)) => panic!("delete body cannot contain both old and key tuples"),
        }

        let message = LogicalReplicationMessage::parse(&Bytes::from(bytes)).unwrap();
        let LogicalReplicationMessage::Delete(body) = message else {
            panic!("expected delete body");
        };

        body
    }

    fn parse_update_event(
        replicated_table_schema: ReplicatedTableSchema,
        update_body: &postgres_replication::protocol::UpdateBody,
    ) -> UpdateEvent {
        parse_event_from_update_message(
            replicated_table_schema,
            PgLsn::from(10),
            PgLsn::from(20),
            0,
            update_body,
        )
        .unwrap()
    }

    fn parse_delete_event(
        replicated_table_schema: ReplicatedTableSchema,
        delete_body: &postgres_replication::protocol::DeleteBody,
    ) -> DeleteEvent {
        parse_event_from_delete_message(
            replicated_table_schema,
            PgLsn::from(10),
            PgLsn::from(20),
            0,
            delete_body,
        )
        .unwrap()
    }

    #[test]
    fn build_identity_classifies_using_index_with_primary_key_columns_as_primary_key() {
        let table_schema = TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, None, false),
            ],
        );
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1],
            relreplident: "i".to_owned(),
            replica_identity_index_attnums: vec![1],
        };

        let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask).unwrap();
        let identity_type = ReplicatedTableSchema::from_masks(
            Arc::new(table_schema),
            replication_mask,
            identity_mask,
        )
        .identity_type();

        assert_eq!(identity_type, IdentityType::PrimaryKey);
    }

    #[test]
    fn build_identity_classifies_using_index_with_distinct_columns_as_alternative_key() {
        let table_schema = TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, None, false),
            ],
        );
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1],
            relreplident: "i".to_owned(),
            replica_identity_index_attnums: vec![2],
        };

        let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask).unwrap();
        let identity_type = ReplicatedTableSchema::from_masks(
            Arc::new(table_schema),
            replication_mask,
            identity_mask,
        )
        .identity_type();

        assert_eq!(identity_type, IdentityType::AlternativeKey);
    }

    #[test]
    fn convert_tuple_to_row_rejects_missing_non_nullable_columns_for_full_rows() {
        let column_schemas = [
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("d".to_owned(), Type::DATE, -1, 2, None, false),
        ];
        let tuple_data = [TupleData::Text(b"1".to_vec().into()), TupleData::Null];

        let err = convert_tuple_to_row(column_schemas.iter(), &tuple_data).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.description(), Some("Required column missing from tuple"));
    }

    #[test]
    fn convert_update_tuple_to_new_table_row_returns_partial_when_toast_cannot_be_recovered() {
        let replicated_table_schema = event_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, None, false),
        ]);
        let tuple_data = [TupleData::Text(b"1".to_vec().into()), TupleData::UnchangedToast];

        let row =
            convert_update_tuple_to_updated_table_row(&replicated_table_schema, &tuple_data, None)
                .unwrap();

        assert_eq!(
            row,
            UpdatedTableRow::Partial(PartialTableRow::new(
                2,
                TableRow::new(vec![Cell::I64(1)]),
                vec![1],
            ))
        );
    }

    #[test]
    fn convert_update_tuple_to_new_table_row_reuses_old_value_when_available() {
        let replicated_table_schema = event_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, None, false),
        ]);
        let tuple_data = [TupleData::Text(b"1".to_vec().into()), TupleData::UnchangedToast];
        let original_old_row = TableRow::new(vec![Cell::I64(1), Cell::String("toast".to_owned())]);
        let old_table_row = OldTableRow::Full(original_old_row.clone());

        let row = convert_update_tuple_to_updated_table_row(
            &replicated_table_schema,
            &tuple_data,
            Some(&old_table_row),
        )
        .unwrap();

        assert_eq!(
            row,
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I64(1),
                Cell::String("toast".to_owned())
            ]))
        );
        assert_eq!(old_table_row, OldTableRow::Full(original_old_row));
    }

    #[test]
    fn convert_update_tuple_to_new_table_row_reuses_key_value_when_column_is_in_key() {
        let replicated_table_schema = event_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(1), false),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, Some(2), false),
        ]);
        let tuple_data = [TupleData::Text(b"2".to_vec().into()), TupleData::UnchangedToast];
        let old_table_row =
            OldTableRow::Key(TableRow::new(vec![Cell::I64(1), Cell::String("toast".to_owned())]));

        let row = convert_update_tuple_to_updated_table_row(
            &replicated_table_schema,
            &tuple_data,
            Some(&old_table_row),
        )
        .unwrap();

        assert_eq!(
            row,
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I64(2),
                Cell::String("toast".to_owned())
            ]))
        );
    }

    #[test]
    fn normalize_key_tuple_to_row_accepts_full_width_tuple_and_filters_identity_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(2), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, Some(1), false),
                ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 4, None, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicatedTableSchema::all(Arc::clone(&table_schema)).replication_mask().clone(),
            etl_postgres::types::IdentityMask::from_bytes(vec![1, 0, 1, 0]),
        );
        let tuple_data = [
            TupleData::Text(b"1".to_vec().into()),
            TupleData::Text(b"alice".to_vec().into()),
            TupleData::Text(b"smith".to_vec().into()),
            TupleData::Text(b"toast".to_vec().into()),
        ];

        let row = normalize_key_tuple_to_row(&replicated_table_schema, &tuple_data).unwrap();

        assert_eq!(row, TableRow::new(vec![Cell::I64(1), Cell::String("smith".to_owned())]));
    }

    #[test]
    fn normalize_key_tuple_to_row_accepts_dense_key_tuple() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, Some(2), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, Some(1), false),
                ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 4, None, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicatedTableSchema::all(Arc::clone(&table_schema)).replication_mask().clone(),
            etl_postgres::types::IdentityMask::from_bytes(vec![1, 0, 1, 0]),
        );
        let tuple_data =
            [TupleData::Text(b"1".to_vec().into()), TupleData::Text(b"smith".to_vec().into())];

        let row = normalize_key_tuple_to_row(&replicated_table_schema, &tuple_data).unwrap();

        assert_eq!(row, TableRow::new(vec![Cell::I64(1), Cell::String("smith".to_owned())]));
    }

    #[test]
    fn parse_event_from_update_message_preserves_absent_old_row_for_non_identity_change() {
        let replicated_table_schema = composite_primary_key_schema();
        let update_body = parse_update_body_from_parts(
            42,
            None,
            None,
            &[
                TestTupleData::Text("1"),
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Text("vienna"),
                TestTupleData::Text("toast"),
            ],
        );

        let event = parse_update_event(replicated_table_schema, &update_body);

        assert_eq!(event.old_table_row, None);
        assert_eq!(
            event.updated_table_row,
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I64(1),
                Cell::String("alice".to_owned()),
                Cell::String("smith".to_owned()),
                Cell::String("vienna".to_owned()),
                Cell::String("toast".to_owned()),
            ]))
        );
    }

    #[test]
    fn parse_event_from_update_message_marks_unrecoverable_toast_as_partial_without_old_row() {
        let replicated_table_schema = composite_primary_key_schema();
        let update_body = parse_update_body_from_parts(
            42,
            None,
            None,
            &[
                TestTupleData::Text("1"),
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Text("vienna"),
                TestTupleData::UnchangedToast,
            ],
        );

        let event = parse_update_event(replicated_table_schema, &update_body);

        assert_eq!(event.old_table_row, None);
        assert_eq!(
            event.updated_table_row,
            UpdatedTableRow::Partial(PartialTableRow::new(
                5,
                TableRow::new(vec![
                    Cell::I64(1),
                    Cell::String("alice".to_owned()),
                    Cell::String("smith".to_owned()),
                    Cell::String("vienna".to_owned()),
                ]),
                vec![4],
            ))
        );
    }

    #[test]
    fn parse_event_from_update_message_preserves_key_tuple_for_identity_change() {
        let replicated_table_schema = composite_primary_key_schema();
        let update_body = parse_update_body_from_parts(
            42,
            None,
            Some(&[
                TestTupleData::Text("1"),
                TestTupleData::Null,
                TestTupleData::Text("smith"),
                TestTupleData::Null,
                TestTupleData::Null,
            ]),
            &[
                TestTupleData::Text("1"),
                TestTupleData::Text("alice"),
                TestTupleData::Text("smithers"),
                TestTupleData::Text("rome"),
                TestTupleData::Text("toast"),
            ],
        );

        let event = parse_update_event(replicated_table_schema, &update_body);

        assert_eq!(
            event.old_table_row,
            Some(OldTableRow::Key(TableRow::new(vec![
                Cell::I64(1),
                Cell::String("smith".to_owned()),
            ])))
        );
    }

    #[test]
    fn parse_event_from_update_message_preserves_key_tuple_when_old_identity_is_still_sent() {
        let replicated_table_schema = alternative_identity_schema();
        let update_body = parse_update_body_from_parts(
            43,
            None,
            Some(&[
                TestTupleData::Null,
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Null,
            ]),
            &[
                TestTupleData::Text("1"),
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Text("vienna"),
            ],
        );

        let event = parse_update_event(replicated_table_schema, &update_body);

        assert_eq!(
            event.old_table_row,
            Some(OldTableRow::Key(TableRow::new(vec![
                Cell::String("alice".to_owned()),
                Cell::String("smith".to_owned()),
            ])))
        );
        assert_eq!(
            event.updated_table_row,
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I64(1),
                Cell::String("alice".to_owned()),
                Cell::String("smith".to_owned()),
                Cell::String("vienna".to_owned()),
            ]))
        );
    }

    #[test]
    fn parse_event_from_update_message_preserves_full_old_tuple_for_full_identity() {
        let replicated_table_schema = full_identity_schema();
        let update_body = parse_update_body_from_parts(
            44,
            Some(&[
                TestTupleData::Text("1"),
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Text("rome"),
            ]),
            None,
            &[
                TestTupleData::Text("1"),
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Text("vienna"),
            ],
        );

        let event = parse_update_event(replicated_table_schema, &update_body);

        assert_eq!(
            event.old_table_row,
            Some(OldTableRow::Full(TableRow::new(vec![
                Cell::I64(1),
                Cell::String("alice".to_owned()),
                Cell::String("smith".to_owned()),
                Cell::String("rome".to_owned()),
            ])))
        );
    }

    #[test]
    fn parse_event_from_delete_message_preserves_key_tuple_for_delete() {
        let replicated_table_schema = alternative_identity_schema();
        let delete_body = parse_delete_body_from_parts(
            43,
            None,
            Some(&[
                TestTupleData::Null,
                TestTupleData::Text("alice"),
                TestTupleData::Text("smith"),
                TestTupleData::Null,
            ]),
        );

        let event = parse_delete_event(replicated_table_schema, &delete_body);

        assert_eq!(
            event.old_table_row,
            Some(OldTableRow::Key(TableRow::new(vec![
                Cell::String("alice".to_owned()),
                Cell::String("smith".to_owned()),
            ])))
        );
    }
}
