use core::str;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use etl_postgres::type_utils::convert_type_oid_to_type;
use postgres_replication::protocol;
use serde::Deserialize;
use tokio_postgres::types::PgLsn;

use crate::{
    bail,
    data::{Cell, OldTableRow, PartialTableRow, TableRow, UpdatedTableRow},
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::{BeginEvent, CommitEvent, DeleteEvent, InsertEvent, TruncateEvent, UpdateEvent},
    postgres::codec::text::parse_cell_from_postgres_text,
    schema::{
        ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
        TableName, TableSchema,
    },
};

/// The prefix used for DDL schema change messages emitted by the
/// `etl.emit_schema_change_messages` event trigger. Messages with this prefix
/// contain JSON-encoded schema information.
pub(crate) const DDL_MESSAGE_PREFIX: &str = "supabase_etl_ddl";

/// Represents a schema change message emitted by Postgres event trigger.
///
/// This message is emitted when supported table or publication commands affect
/// tables that are part of a publication.
///
/// Unknown fields are ignored on purpose so the SQL payload can grow richer
/// without forcing a synchronized rollout of every consumer.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SchemaChangeMessage {
    /// The command tag from `pg_event_trigger_ddl_commands().command_tag`.
    pub(crate) command_tag: String,
    /// The publication changed by an `ALTER PUBLICATION` command.
    ///
    /// This is absent for table DDL and payloads emitted before publication
    /// schema change messages were introduced. Publication DDL without this
    /// field is rejected by [`SchemaChangeMessage::applies_to_publication`].
    pub(crate) publication_name: Option<String>,
    /// Effective column attnums for each publication at this DDL boundary.
    ///
    /// A null entry means all supported columns. Explicit lists use stable
    /// PostgreSQL attnums, avoiding repeated name lookups in the source
    /// trigger. Older WAL payloads omit this field and require a protocol
    /// relation to supply the publication mask.
    pub(crate) publication_column_attnums: Option<HashMap<String, Option<HashSet<i32>>>>,
    /// The schema name from `pg_namespace.nspname`.
    pub(crate) nspname: String,
    /// The table name from `pg_class.relname`.
    pub(crate) relname: String,
    /// The table OID from `pg_class.oid`.
    pub(crate) oid: u32,
    /// The identity metadata emitted by Postgres for this table snapshot.
    pub(crate) identity: IdentityMessage,
    /// The columns of the table after the schema change.
    pub(crate) columns: Vec<ColumnSchemaMessage>,
}

impl SchemaChangeMessage {
    /// Returns the table identifier as [`TableId`].
    pub(crate) fn table_id(&self) -> TableId {
        TableId::new(self.oid)
    }

    /// Returns whether this message applies to `publication_name`.
    ///
    /// Table DDL is global to every publication containing the table.
    /// Publication DDL without an explicit publication is rejected so it cannot
    /// mutate another pipeline's schema state.
    pub(crate) fn applies_to_publication(&self, publication_name: &str) -> bool {
        if self.command_tag == "ALTER PUBLICATION"
            && self.publication_name.as_deref() != Some(publication_name)
        {
            return false;
        }

        self.publication_column_attnums
            .as_ref()
            .is_none_or(|publications| publications.contains_key(publication_name))
    }

    /// Converts a [`SchemaChangeMessage`] to a [`TableSchema`] with a specific
    /// snapshot ID.
    ///
    /// This is used to update the stored table schema when a DDL change is
    /// detected. The snapshot ID identifies both the transaction's COMMIT
    /// record start and the logical DDL message LSN within that transaction.
    pub(crate) fn into_table_schema(self, snapshot_id: SnapshotId) -> TableSchema {
        build_table_schema(
            self.table_id(),
            TableName::new(self.nspname, self.relname),
            self.columns,
            self.identity.primary_key_attnums,
            self.identity.primary_key_condeferrable,
            snapshot_id,
        )
    }
}

impl FromStr for SchemaChangeMessage {
    type Err = EtlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|error| {
            etl_error!(
                ErrorKind::ConversionError,
                "Failed to parse schema change message",
                source: error
            )
        })
    }
}

/// Returns the durable schema snapshot identifier for a logical message.
///
/// The commit LSN orders transactions because they are decoded in commit
/// order. The message LSN cannot provide that order across
/// overlapping transactions, but it does preserve the order of multiple DDL
/// messages delivered within the same commit.
pub(crate) fn schema_snapshot_id_from_message(
    commit_lsn: PgLsn,
    message: &protocol::MessageBody,
) -> SnapshotId {
    SnapshotId::new(commit_lsn, PgLsn::from(message.message_lsn()))
}

/// Source identity fields, including the legacy explicit-index representation.
#[derive(Deserialize)]
struct IdentityMessagePayload {
    /// The source primary-key columns.
    primary_key_attnums: Vec<i32>,
    /// Whether the source primary key is deferrable.
    #[serde(default)]
    primary_key_condeferrable: bool,
    /// The source replica-identity mode.
    relreplident: String,
    /// The resolved table-local replication index keys, when supplied.
    replica_identity_attnums: Option<Vec<i32>>,
    /// Explicit-index keys supplied by older source helpers.
    replica_identity_index_attnums: Option<Vec<i32>>,
}

/// The identity metadata emitted by Postgres.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "IdentityMessagePayload")]
pub struct IdentityMessage {
    /// The primary key columns in key order, expressed as `pg_attribute.attnum`
    /// values.
    pub(crate) primary_key_attnums: Vec<i32>,
    /// Whether the primary-key constraint is `DEFERRABLE`, from
    /// `pg_constraint.condeferrable`.
    ///
    /// Payloads emitted before the source helper reported it omit the field;
    /// they are read as not deferrable.
    pub(crate) primary_key_condeferrable: bool,
    /// The replica-identity mode from `pg_class.relreplident`.
    pub(crate) relreplident: String,
    /// The table-local replication index keys, expressed as
    /// `pg_attribute.attnum` values in index-key order.
    ///
    /// Both default and explicit-index identity use these resolved keys.
    /// Older payloads are normalized during deserialization.
    pub(crate) replica_identity_attnums: Vec<i32>,
}

impl IdentityMessage {
    /// Builds the runtime identity mask for a replicated table schema.
    ///
    /// The returned mask is expressed in full table-schema width so it can be
    /// combined with the replication mask held by [`ReplicatedTableSchema`].
    ///
    /// ETL stores runtime identity over replicated columns, matching relation
    /// messages where PostgreSQL can only flag columns present in the relation
    /// payload. PostgreSQL should reject update/delete publications whose
    /// column lists do not cover the source identity; insert-only publications
    /// do not use identity data.
    ///
    /// `REPLICA IDENTITY FULL` uses every replicated column as the runtime row
    /// identity. `DEFAULT` uses the table-local non-deferrable primary index,
    /// `USING INDEX` uses the eligible configured replica-identity index, and
    /// `NOTHING` produces an empty identity mask, each intersected with the
    /// replication mask. Both index-based modes exclude non-immediate indexes,
    /// including an explicit identity index later attached to a deferrable
    /// unique constraint.
    pub(crate) fn build_identity_mask(
        &self,
        table_schema: &TableSchema,
        replication_mask: &ReplicationMask,
    ) -> EtlResult<IdentityMask> {
        match self.relreplident.as_str() {
            "f" => Ok(IdentityMask::from_bytes(replication_mask.as_slice().to_vec())),
            "d" | "i" => Ok(Self::build_identity_mask_from_attnums(
                table_schema,
                replication_mask,
                &self.replica_identity_attnums,
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
    ///
    /// Initial schema metadata describes the source table identity, which can
    /// include columns omitted by an insert-only publication column list.
    /// Filter through the replication mask so initial copy matches streaming
    /// relation-message semantics.
    fn build_identity_mask_from_attnums(
        table_schema: &TableSchema,
        replication_mask: &ReplicationMask,
        attnums: &[i32],
    ) -> IdentityMask {
        let attnums: HashSet<i32> = attnums.iter().copied().collect();

        IdentityMask::from_bytes(
            table_schema
                .column_schemas
                .iter()
                .zip(replication_mask.as_slice().iter())
                .map(|(column_schema, &replicated)| {
                    u8::from(replicated == 1 && attnums.contains(&column_schema.ordinal_position))
                })
                .collect(),
        )
    }
}

impl TryFrom<IdentityMessagePayload> for IdentityMessage {
    type Error = &'static str;

    fn try_from(payload: IdentityMessagePayload) -> Result<Self, Self::Error> {
        // Resolve legacy payloads once so mask construction only uses protocol
        // keys. An explicit empty set is authoritative, not missing metadata.
        let replica_identity_attnums = match payload.replica_identity_attnums {
            Some(attnums) => attnums,
            None => match payload.relreplident.as_str() {
                "d" if !payload.primary_key_condeferrable => payload.primary_key_attnums.clone(),
                "i" => payload
                    .replica_identity_index_attnums
                    .ok_or("Missing replica identity index columns")?,
                _ => Vec::new(),
            },
        };

        if replica_identity_attnums
            .iter()
            .any(|attnum| !i16::try_from(*attnum).is_ok_and(|attnum| attnum > 0))
        {
            return Err("Invalid replica identity column number");
        }

        Ok(Self {
            primary_key_attnums: payload.primary_key_attnums,
            primary_key_condeferrable: payload.primary_key_condeferrable,
            relreplident: payload.relreplident,
            replica_identity_attnums,
        })
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
    /// The default expression from `pg_attrdef`, if one exists.
    pub(crate) default_expression: Option<String>,
    /// The captured missing-value literal, omitted by legacy payloads.
    pub(crate) missing_value_expression: Option<String>,
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
                !column.attnotnull,
            )
            .with_primary_key_ordinal_position(primary_key_positions.get(&column.attnum).copied())
            .with_default_expression_option(column.default_expression)
            .with_missing_value_expression(column.missing_value_expression)
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
    primary_key_deferrable: bool,
    snapshot_id: SnapshotId,
) -> TableSchema {
    TableSchema::with_snapshot_id(
        table_id,
        table_name,
        build_column_schemas(columns, primary_key_attnums),
        snapshot_id,
    )
    .with_primary_key_deferrable(primary_key_deferrable)
}

/// Calculates the uncompressed value bytes in a pgoutput tuple.
///
/// Text and binary values contribute their emitted byte length. `NULL` and
/// unchanged TOAST fields contribute zero bytes. Tuple markers, value-length
/// fields, column counts, relation IDs, and replication protocol framing are
/// excluded.
pub(crate) fn calculate_tuple_bytes(tuple_data: &[protocol::TupleData]) -> u64 {
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

/// Calculates the new-tuple value bytes in an insert message.
pub(crate) fn insert_message_payload_bytes(insert_body: &protocol::InsertBody) -> u64 {
    calculate_tuple_bytes(insert_body.tuple().tuple_data())
}

/// Calculates the new- and emitted old-identity-tuple value bytes in an update.
pub(crate) fn update_message_payload_bytes(update_body: &protocol::UpdateBody) -> u64 {
    let new_tuple_data = update_body.new_tuple().tuple_data();
    let mut total_bytes = calculate_tuple_bytes(new_tuple_data);

    if let Some(identity) = update_body.old_tuple().or(update_body.key_tuple()) {
        total_bytes += calculate_tuple_bytes(identity.tuple_data());
    }

    total_bytes
}

/// Calculates the emitted old-identity-tuple value bytes in a delete message.
pub(crate) fn delete_message_payload_bytes(delete_body: &protocol::DeleteBody) -> u64 {
    delete_body
        .old_tuple()
        .or(delete_body.key_tuple())
        .map(|identity| calculate_tuple_bytes(identity.tuple_data()))
        .unwrap_or_default()
}

/// Creates a [`BeginEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol begin message and extracts
/// transaction metadata for use in the ETL pipeline.
pub(crate) fn parse_event_from_begin_message(
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    begin_body: &protocol::BeginBody,
) -> BeginEvent {
    BeginEvent { commit_lsn, tx_ordinal, timestamp: begin_body.timestamp(), xid: begin_body.xid() }
}

/// Creates a [`CommitEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol commit message and extracts
/// transaction completion metadata for use in the ETL pipeline.
pub(crate) fn parse_event_from_commit_message(
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    commit_body: &protocol::CommitBody,
) -> CommitEvent {
    CommitEvent {
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
        .map(|column| column.name().map(str::to_owned))
        .collect::<Result<HashSet<String>, _>>()?;

    Ok(column_names)
}

/// Returns the set of relation-message key column names.
///
/// PostgreSQL exposes replica-identity mode on the relation itself and
/// key-column membership on each [`protocol::RelationBody`] column. For
/// `REPLICA IDENTITY FULL`, every relation-message column belongs to the
/// runtime old-row identity. Otherwise, the low bit of the column flags marks
/// identity membership. The column order is the same `attnum` order described
/// in [`parse_replicated_column_names`]. This returns names because
/// identity-mask membership is also name-based; tuple interpretation relies on
/// the resulting replicated schema preserving relation-message order after the
/// masks are applied.
pub(crate) fn parse_replica_identity_column_names(
    relation_body: &protocol::RelationBody,
) -> EtlResult<HashSet<String>> {
    let column_names = match relation_body.replica_identity() {
        protocol::ReplicaIdentity::Full => relation_body
            .columns()
            .iter()
            .map(|column| column.name().map(str::to_owned))
            .collect::<Result<HashSet<String>, _>>()?,
        _ => relation_body
            .columns()
            .iter()
            // PostgreSQL sends relation column flags as a bitmask. Bit 0 is
            // LOGICALREP_IS_REPLICA_IDENTITY, so `& 1` tests only that bit while ignoring any other
            // protocol flags that may be present.
            .filter(|column| column.flags() & 1 == 1)
            .map(|column| column.name().map(str::to_owned))
            .collect::<Result<HashSet<String>, _>>()?,
    };

    Ok(column_names)
}

/// Converts a Postgres insert message into an [`InsertEvent`].
///
/// This function processes an insert operation from the replication stream and
/// constructs an insert event with the new row data ready for ETL processing.
pub(crate) fn parse_event_from_insert_message(
    replicated_table_schema: ReplicatedTableSchema,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    insert_body: &protocol::InsertBody,
) -> EtlResult<InsertEvent> {
    let tuple_data = insert_body.tuple().tuple_data();
    let table_row = convert_tuple_to_row(replicated_table_schema.column_schemas(), tuple_data)?;

    Ok(InsertEvent { commit_lsn, tx_ordinal, replicated_table_schema, table_row })
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
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    update_body: &protocol::UpdateBody,
) -> EtlResult<UpdateEvent> {
    // PostgreSQL can attach either a full old tuple (`old_tuple`) or only the
    // replica-identity columns (`key_tuple`) to an update. If neither is
    // present, the publisher determined that no old-side image was required for
    // this key-based replica-identity update. We preserve that shape in
    // `OldTableRow`. The new tuple is decoded separately below, where the old
    // row acts only as a source for resolving `UnchangedToast`.
    let is_key = update_body.old_tuple().is_none();
    let old_tuple = update_body.old_tuple().or(update_body.key_tuple());
    let new_tuple_data = update_body.new_tuple().tuple_data();

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
    // - key rows can recover only unchanged key columns, consuming key values
    //   in replicated table order as we walk the schema below.
    let table_row = convert_update_tuple_to_updated_table_row(
        &replicated_table_schema,
        new_tuple_data,
        old_table_row.as_ref(),
    )?;

    Ok(UpdateEvent {
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
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    delete_body: &protocol::DeleteBody,
) -> EtlResult<DeleteEvent> {
    // Published delete messages carry an old-side image. PostgreSQL sends
    // either the full old row or only the replica-identity columns, and we
    // preserve that shape here.
    let is_key = delete_body.old_tuple().is_none();
    let old_tuple = delete_body.old_tuple().or(delete_body.key_tuple());

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

    Ok(DeleteEvent { commit_lsn, tx_ordinal, replicated_table_schema, old_table_row })
}

/// Creates a [`TruncateEvent`] from Postgres protocol data.
///
/// This method parses the replication protocol truncate message and extracts
/// information about which tables were truncated and with what options.
pub(crate) fn parse_event_from_truncate_message(
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    truncate_body: &protocol::TruncateBody,
    truncated_tables: Vec<ReplicatedTableSchema>,
) -> TruncateEvent {
    TruncateEvent { commit_lsn, tx_ordinal, options: truncate_body.options(), truncated_tables }
}

/// Converts a full tuple image into a dense [`TableRow`].
///
/// The tuple width must exactly match the number of provided column schemas.
/// Every field must decode to a concrete [`Cell`]; `UnchangedToast` is
/// therefore rejected here because full row images must be self-contained.
pub(crate) fn convert_tuple_to_row<'a>(
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
                    // This is the first column we cannot reconstruct. Up to
                    // this point `full_values` held a dense prefix of known
                    // values, so we move that prefix into `present_values` and
                    // continue collecting only the values we do know plus the
                    // indexes we do not.
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
fn convert_dense_key_tuple_to_row<'a>(
    identity_column_schemas: impl ExactSizeIterator<Item = &'a ColumnSchema>,
    tuple_data: &[protocol::TupleData],
) -> EtlResult<TableRow> {
    let identity_column_count = identity_column_schemas.len();
    let mut values = Vec::with_capacity(identity_column_count);

    for (i, (column_schema, tuple_data)) in
        identity_column_schemas.zip(tuple_data.iter()).enumerate()
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
fn convert_full_width_key_tuple_to_row<'a>(
    identity_column_schemas: impl ExactSizeIterator<Item = &'a ColumnSchema>,
    replicated_column_schemas: impl Iterator<Item = &'a ColumnSchema>,
    tuple_data: &[protocol::TupleData],
) -> EtlResult<TableRow> {
    let identity_column_count = identity_column_schemas.len();
    let mut values = Vec::with_capacity(identity_column_count);
    let mut identity_columns = identity_column_schemas.peekable();

    for (i, (column_schema, tuple_data)) in
        replicated_column_schemas.zip(tuple_data.iter()).enumerate()
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
    let identity_column_count = replicated_table_schema.identity_column_schemas().len();
    let replicated_column_count = replicated_table_schema.column_schemas().len();

    if identity_column_count == 0 {
        bail!(
            ErrorKind::ConversionError,
            "Replica-identity tuple missing key columns",
            "Key-image row was received for a table without replicated replica-identity columns"
        );
    }

    match tuple_data.len() {
        len if len == identity_column_count => convert_dense_key_tuple_to_row(
            replicated_table_schema.identity_column_schemas(),
            tuple_data,
        ),
        len if len == replicated_column_count => convert_full_width_key_tuple_to_row(
            replicated_table_schema.identity_column_schemas(),
            replicated_table_schema.column_schemas(),
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
            // If a column schema is nullable and there is no value, it's fine,
            // but if it's not nullable this is a problem, and we need to raise
            // it.
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
            // surface the field as missing and let the caller produce a partial
            // updated row.
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
    use std::{collections::HashSet, sync::Arc};

    use bytes::Bytes;
    use postgres_replication::protocol::{
        LogicalReplicationMessage, ReplicationMessage, TupleData,
    };
    #[cfg(feature = "test-utils")]
    use proptest::{arbitrary::any, collection, prop_assert_eq, strategy::Strategy};
    use tokio_postgres::types::{PgLsn, Type};

    #[cfg(feature = "test-utils")]
    use crate::test_utils::property::{pg_text, run_property};
    use crate::{
        data::{Cell, OldTableRow, PartialTableRow, TableRow, UpdatedTableRow},
        error::ErrorKind,
        event::{DeleteEvent, UpdateEvent},
        postgres::codec::event::{
            ColumnSchemaMessage, DDL_MESSAGE_PREFIX, IdentityMessage, SchemaChangeMessage,
            build_column_schemas, calculate_tuple_bytes, convert_tuple_to_row,
            convert_update_tuple_to_updated_table_row, delete_message_payload_bytes,
            insert_message_payload_bytes, normalize_key_tuple_to_row,
            parse_event_from_delete_message, parse_event_from_update_message,
            schema_snapshot_id_from_message, update_message_payload_bytes,
        },
        schema::{
            ColumnSchema, IdentityType, ReplicatedTableSchema, ReplicationMask, SnapshotId,
            TableId, TableName, TableSchema,
        },
    };

    #[derive(Clone, Copy)]
    enum TestTupleData<'a> {
        Null,
        UnchangedToast,
        Text(&'a str),
    }

    #[test]
    fn schema_snapshot_uses_logical_message_lsn_not_xlogdata_wal_start() {
        let wal_start = 100_u64;
        let message_lsn = 140_u64;

        let mut logical_message = vec![b'M', 1];
        logical_message.extend_from_slice(&message_lsn.to_be_bytes());
        logical_message.extend_from_slice(DDL_MESSAGE_PREFIX.as_bytes());
        logical_message.push(0);
        logical_message.extend_from_slice(&2_i32.to_be_bytes());
        logical_message.extend_from_slice(b"{}");

        let mut xlog_data = vec![b'w'];
        xlog_data.extend_from_slice(&wal_start.to_be_bytes());
        xlog_data.extend_from_slice(&200_u64.to_be_bytes());
        xlog_data.extend_from_slice(&0_i64.to_be_bytes());
        xlog_data.extend_from_slice(&logical_message);

        let replication_message = ReplicationMessage::parse(&Bytes::from(xlog_data)).unwrap();
        let ReplicationMessage::XLogData(xlog_data) = replication_message else {
            panic!("encoded replication message should be XLogData");
        };
        assert_eq!(xlog_data.wal_start(), wal_start);

        let logical_message = LogicalReplicationMessage::parse(xlog_data.data()).unwrap();
        let LogicalReplicationMessage::Message(message) = logical_message else {
            panic!("encoded logical message should be Message");
        };
        let commit_lsn = PgLsn::from(180);

        assert_eq!(
            schema_snapshot_id_from_message(commit_lsn, &message),
            SnapshotId::new(commit_lsn, PgLsn::from(message_lsn))
        );
        assert!(
            schema_snapshot_id_from_message(commit_lsn, &message)
                > SnapshotId::new(commit_lsn, PgLsn::from(wal_start))
        );
    }

    #[test]
    fn schema_change_message_validates_oid_and_publication_scope() {
        let table_ddl = r#"{
            "command_tag": "ALTER TABLE",
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [],
                "relreplident": "d",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#;
        let unscoped = table_ddl.parse::<SchemaChangeMessage>().unwrap();
        assert!(unscoped.applies_to_publication("pipeline_publication"));
        assert!(unscoped.publication_column_attnums.is_none());

        let captured_columns = table_ddl.replace(
            "\"columns\": []",
            "\"columns\": [], \"publication_column_attnums\": {\"pipeline_publication\": [1]}",
        );
        let captured = captured_columns.parse::<SchemaChangeMessage>().unwrap();
        assert!(captured.applies_to_publication("pipeline_publication"));
        assert!(!captured.applies_to_publication("other_publication"));
        assert_eq!(
            captured.publication_column_attnums.unwrap()["pipeline_publication"]
                .as_ref()
                .unwrap()
                .clone(),
            HashSet::from([1])
        );
        for invalid_attnums in ["[\"id\"]", "[2147483648]", "[{}]"] {
            assert!(
                captured_columns
                    .replace("[1]", invalid_attnums)
                    .parse::<SchemaChangeMessage>()
                    .is_err()
            );
        }

        let all_columns = captured_columns.replace("[1]", "null");
        let all_columns = all_columns.parse::<SchemaChangeMessage>().unwrap();
        assert!(all_columns.applies_to_publication("pipeline_publication"));
        assert!(all_columns.publication_column_attnums.unwrap()["pipeline_publication"].is_none());

        for invalid_oid in ["-1", "4294967296"] {
            let invalid_payload =
                table_ddl.replace("\"oid\": 42", &format!("\"oid\": {invalid_oid}"));
            assert!(invalid_payload.parse::<SchemaChangeMessage>().is_err());
        }

        let null_scoped_table = r#"{
            "command_tag": "ALTER TABLE",
            "publication_name": null,
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [],
                "relreplident": "d",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#
        .parse::<SchemaChangeMessage>()
        .unwrap();
        assert!(null_scoped_table.applies_to_publication("pipeline_publication"));

        let unscoped_publication = r#"{
            "command_tag": "ALTER PUBLICATION",
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [],
                "relreplident": "d",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#
        .parse::<SchemaChangeMessage>()
        .unwrap();
        assert!(!unscoped_publication.applies_to_publication("pipeline_publication"));

        let scoped = r#"{
            "command_tag": "ALTER PUBLICATION",
            "publication_name": "pipeline_publication",
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [],
                "relreplident": "d",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#
        .parse::<SchemaChangeMessage>()
        .unwrap();
        assert!(scoped.applies_to_publication("pipeline_publication"));
        assert!(!scoped.applies_to_publication("other_publication"));

        let malformed = r#"{
            "command_tag": "ALTER PUBLICATION",
            "publication_name": 42,
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [],
                "relreplident": "d",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#;
        assert!(malformed.parse::<SchemaChangeMessage>().is_err());
    }

    /// Malformed DDL payloads fail without panicking and retain their cause.
    #[test]
    fn schema_change_message_rejects_malformed_json_with_source() {
        for payload in ["", "{", "[]", "null", "{\"columns\": [", "{\"relname\": \"旧"] {
            let error = payload.parse::<SchemaChangeMessage>().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConversionError);
            assert!(std::error::Error::source(&error).is_some());
        }
    }

    /// Optional missing-value literals preserve decoding of older column
    /// payloads.
    #[test]
    fn column_schema_message_preserves_legacy_and_captured_missing_values() {
        let legacy = serde_json::json!({
            "attname": "score", "attnum": 2, "atttypid": Type::INT4.oid(),
            "atttypmod": -1, "attnotnull": false, "default_expression": "2"
        });
        let parsed: ColumnSchemaMessage = serde_json::from_value(legacy.clone()).unwrap();
        assert!(parsed.missing_value_expression.is_none());
        for expression in ["1", "null"] {
            let mut current = legacy.clone();
            current["missing_value_expression"] = serde_json::json!(expression);
            let parsed: ColumnSchemaMessage = serde_json::from_value(current).unwrap();
            let columns = build_column_schemas(vec![parsed], vec![]);
            assert_eq!(columns[0].default_expression.as_deref(), Some("2"));
            assert_eq!(columns[0].missing_value_expression.as_deref(), Some(expression));
        }
        for invalid in [serde_json::json!(1), serde_json::json!([]), serde_json::json!({})] {
            let mut current = legacy.clone();
            current["missing_value_expression"] = invalid;
            assert!(serde_json::from_value::<ColumnSchemaMessage>(current).is_err());
        }
    }

    #[test]
    fn schema_change_message_without_deferrability_reads_as_not_deferrable() {
        // Messages emitted before the source helper reported deferrability
        // omit the field.
        let schema = r#"{
            "command_tag": "ALTER TABLE",
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [1],
                "relreplident": "f",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#
        .parse::<SchemaChangeMessage>()
        .unwrap()
        .into_table_schema(SnapshotId::initial());

        assert!(!schema.primary_key_deferrable);
    }

    #[test]
    fn schema_change_message_carries_primary_key_deferrability() {
        let schema = r#"{
            "command_tag": "ALTER TABLE",
            "nspname": "public",
            "relname": "items",
            "oid": 42,
            "identity": {
                "primary_key_attnums": [1],
                "primary_key_condeferrable": true,
                "relreplident": "f",
                "replica_identity_index_attnums": []
            },
            "columns": []
        }"#
        .parse::<SchemaChangeMessage>()
        .unwrap()
        .into_table_schema(SnapshotId::initial());

        assert!(schema.primary_key_deferrable);
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
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(2),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
            ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, false).with_primary_key(1),
            ColumnSchema::new("city".to_owned(), Type::TEXT, -1, 4, false),
            ColumnSchema::new("large_text".to_owned(), Type::TEXT, -1, 5, false),
        ])
    }

    fn alternative_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(43),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, false)
                    .with_primary_key(1),
                ColumnSchema::new("city".to_owned(), Type::TEXT, -1, 4, false),
            ],
        ));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            crate::schema::IdentityMask::from_bytes(vec![0, 1, 1, 0]),
        )
    }

    fn full_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(44),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, false)
                    .with_primary_key(1),
                ColumnSchema::new("city".to_owned(), Type::TEXT, -1, 4, false),
            ],
        ));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            crate::schema::IdentityMask::from_bytes(vec![1, 1, 1, 1]),
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

    fn parse_insert_body_from_parts(
        rel_id: u32,
        tuple_data: &[TestTupleData<'_>],
    ) -> postgres_replication::protocol::InsertBody {
        let mut bytes = Vec::new();
        bytes.push(b'I');
        bytes.extend_from_slice(&rel_id.to_be_bytes());
        bytes.push(b'N');
        encode_tuple(&mut bytes, tuple_data);

        let message = LogicalReplicationMessage::parse(&Bytes::from(bytes)).unwrap();
        let LogicalReplicationMessage::Insert(body) = message else {
            panic!("expected insert body");
        };

        body
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
        parse_event_from_update_message(replicated_table_schema, PgLsn::from(20), 0, update_body)
            .unwrap()
    }

    fn parse_delete_event(
        replicated_table_schema: ReplicatedTableSchema,
        delete_body: &postgres_replication::protocol::DeleteBody,
    ) -> DeleteEvent {
        parse_event_from_delete_message(replicated_table_schema, PgLsn::from(20), 0, delete_body)
            .unwrap()
    }

    #[test]
    fn pgoutput_payload_sizes_count_values_without_protocol_framing() {
        let tuple_data = [
            TupleData::Null,
            TupleData::UnchangedToast,
            TupleData::Text(Bytes::from_static("é".as_bytes())),
            TupleData::Binary(Bytes::from_static(b"\x01\x02\x03")),
        ];
        assert_eq!(calculate_tuple_bytes(&tuple_data), 5);

        let insert =
            parse_insert_body_from_parts(42, &[TestTupleData::Text("é"), TestTupleData::Null]);
        assert_eq!(insert_message_payload_bytes(&insert), 2);

        let update = parse_update_body_from_parts(
            42,
            None,
            Some(&[TestTupleData::Text("old"), TestTupleData::Null]),
            &[TestTupleData::Text("é"), TestTupleData::UnchangedToast],
        );
        assert_eq!(update_message_payload_bytes(&update), 5);

        let delete = parse_delete_body_from_parts(
            42,
            Some(&[TestTupleData::Text("gone"), TestTupleData::Null]),
            None,
        );
        assert_eq!(delete_message_payload_bytes(&delete), 4);
    }

    /// Legacy WAL is normalized without letting fallback metadata replace
    /// explicit keys.
    #[test]
    fn identity_payload_normalizes_legacy_and_resolved_keys() {
        for (mode, resolved, expected) in [
            ("d", None, vec![1]),
            ("i", None, vec![2]),
            ("f", None, vec![]),
            ("n", None, vec![]),
            ("d", Some(vec![]), vec![]),
            ("i", Some(vec![]), vec![]),
            ("d", Some(vec![3]), vec![3]),
            ("i", Some(vec![3]), vec![3]),
        ] {
            let mut payload = serde_json::json!({
                "primary_key_attnums": [1],
                "relreplident": mode,
                "replica_identity_index_attnums": [2],
            });
            if let Some(resolved) = resolved {
                payload["replica_identity_attnums"] = serde_json::json!(resolved);
            }
            let identity: IdentityMessage = serde_json::from_value(payload).unwrap();
            assert_eq!(identity.replica_identity_attnums, expected);
            assert!(!identity.primary_key_condeferrable);
        }

        let identity: IdentityMessage = serde_json::from_value(serde_json::json!({
            "primary_key_attnums": [1],
            "primary_key_condeferrable": true,
            "relreplident": "d",
            "replica_identity_attnums": [],
        }))
        .unwrap();
        assert!(identity.primary_key_condeferrable);
        assert!(identity.replica_identity_attnums.is_empty());
    }

    /// Legacy deferrable primary keys cannot act as default replica identity.
    #[test]
    fn identity_payload_normalizes_legacy_deferrable_primary_key() {
        let identity: IdentityMessage = serde_json::from_value(serde_json::json!({
            "primary_key_attnums": [1],
            "primary_key_condeferrable": true,
            "relreplident": "d",
            "replica_identity_index_attnums": [],
        }))
        .unwrap();
        assert!(identity.replica_identity_attnums.is_empty());
    }

    /// Missing explicit-index metadata must not silently become an empty key.
    #[test]
    fn identity_payload_rejects_missing_or_malformed_keys() {
        for keys in [None, Some(serde_json::Value::Null)] {
            let mut payload = serde_json::json!({
                "primary_key_attnums": [1],
                "relreplident": "i",
            });
            if let Some(keys) = keys {
                payload["replica_identity_attnums"] = keys;
            }
            assert!(serde_json::from_value::<IdentityMessage>(payload).is_err());
        }
        for keys in [
            serde_json::json!(["bad"]),
            serde_json::json!([0]),
            serde_json::json!([-1]),
            serde_json::json!([32768]),
            serde_json::json!([2147483648_i64]),
        ] {
            let payload = serde_json::json!({
                "primary_key_attnums": [1],
                "relreplident": "i",
                "replica_identity_attnums": keys,
            });
            assert!(serde_json::from_value::<IdentityMessage>(payload).is_err());
        }
    }

    #[test]
    fn build_identity_classifies_using_index_with_primary_key_columns_as_primary_key() {
        let table_schema = TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, false),
            ],
        );
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1],
            primary_key_condeferrable: false,
            relreplident: "i".to_owned(),
            replica_identity_attnums: vec![1],
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
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, false),
            ],
        );
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1],
            primary_key_condeferrable: false,
            relreplident: "i".to_owned(),
            replica_identity_attnums: vec![2],
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
    fn build_identity_for_default_filters_to_replicated_columns() {
        let table_schema = TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, false),
            ],
        );
        let replication_mask = ReplicationMask::from_bytes(vec![1, 0, 1]);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1, 2],
            primary_key_condeferrable: false,
            relreplident: "d".to_owned(),
            replica_identity_attnums: vec![1, 2],
        };

        let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask).unwrap();

        assert_eq!(identity_mask.as_slice(), &[1, 0, 0]);
    }

    #[test]
    fn build_identity_for_using_index_filters_to_replicated_columns() {
        let table_schema = TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, false),
            ],
        );
        let replication_mask = ReplicationMask::from_bytes(vec![0, 1, 0]);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1],
            primary_key_condeferrable: false,
            relreplident: "i".to_owned(),
            replica_identity_attnums: vec![2, 3],
        };

        let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask).unwrap();

        assert_eq!(identity_mask.as_slice(), &[0, 1, 0]);
    }

    #[test]
    fn build_identity_for_full_uses_all_replicated_columns() {
        let table_schema = TableSchema::new(
            TableId::new(42),
            TableName::new("public".to_owned(), "test".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, false),
            ],
        );
        let replication_mask = ReplicationMask::from_bytes(vec![1, 0]);
        let identity = IdentityMessage {
            primary_key_attnums: vec![1],
            primary_key_condeferrable: false,
            relreplident: "f".to_owned(),
            replica_identity_attnums: Vec::new(),
        };

        let identity_mask = identity.build_identity_mask(&table_schema, &replication_mask).unwrap();

        assert_eq!(identity_mask.as_slice(), &[1, 0]);
    }

    #[test]
    fn convert_tuple_to_row_rejects_missing_non_nullable_columns_for_full_rows() {
        let column_schemas = [
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("d".to_owned(), Type::DATE, -1, 2, false),
        ];
        let tuple_data = [TupleData::Text(b"1".to_vec().into()), TupleData::Null];

        let err = convert_tuple_to_row(column_schemas.iter(), &tuple_data).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert_eq!(err.description(), Some("Required column missing from tuple"));
    }

    #[test]
    fn convert_update_tuple_to_new_table_row_returns_partial_when_toast_cannot_be_recovered() {
        let replicated_table_schema = event_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, false),
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
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, false),
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
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 2, false).with_primary_key(2),
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
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, false)
                    .with_primary_key(1),
                ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 4, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicatedTableSchema::all(Arc::clone(&table_schema)).replication_mask().clone(),
            crate::schema::IdentityMask::from_bytes(vec![1, 0, 1, 0]),
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
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, false),
                ColumnSchema::new("surname".to_owned(), Type::TEXT, -1, 3, false)
                    .with_primary_key(1),
                ColumnSchema::new("payload".to_owned(), Type::TEXT, -1, 4, false),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicatedTableSchema::all(Arc::clone(&table_schema)).replication_mask().clone(),
            crate::schema::IdentityMask::from_bytes(vec![1, 0, 1, 0]),
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

    /// Column type drawn by the update/delete tuple property.
    ///
    /// `int8` is fixed width and can never be TOASTed; `text` is varlena and is
    /// the type that can show up as an unchanged TOAST marker.
    #[cfg(feature = "test-utils")]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ModelColumnType {
        Int8,
        Text,
    }

    /// Non-null source value of one generated column.
    #[cfg(feature = "test-utils")]
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ModelValue {
        Int8(i64),
        Text(String),
    }

    #[cfg(feature = "test-utils")]
    impl ModelValue {
        /// Renders the value as pgoutput's text-format tuple payload.
        fn to_pg_text(&self) -> String {
            match self {
                Self::Int8(value) => value.to_string(),
                Self::Text(value) => value.clone(),
            }
        }
    }

    /// Returns the cell etl must decode for an optional model value.
    #[cfg(feature = "test-utils")]
    fn model_cell(value: Option<&ModelValue>) -> Cell {
        match value {
            None => Cell::Null,
            Some(ModelValue::Int8(value)) => Cell::I64(*value),
            Some(ModelValue::Text(value)) => Cell::String(value.clone()),
        }
    }

    /// One column of a generated table, plus its value before and after one
    /// `UPDATE` of one row.
    #[cfg(feature = "test-utils")]
    #[derive(Debug, Clone)]
    struct ModelColumn {
        typ: ModelColumnType,
        nullable: bool,
        replicated: bool,
        identity: bool,
        old: Option<ModelValue>,
        new: Option<ModelValue>,
        /// Whether pgoutput sends this column as `u` in the new tuple, which
        /// it does for unchanged values stored out of line in TOAST.
        unchanged_toast: bool,
    }

    #[cfg(feature = "test-utils")]
    impl ModelColumn {
        /// Returns the expected decoded cell for the old value.
        fn old_cell(&self) -> Cell {
            model_cell(self.old.as_ref())
        }

        /// Returns the expected decoded cell for the new value.
        fn new_cell(&self) -> Cell {
            model_cell(self.new.as_ref())
        }
    }

    /// Generated table schema, publication masks, replica identity, and one
    /// row update, constrained to what PostgreSQL can publish.
    #[cfg(feature = "test-utils")]
    #[derive(Debug, Clone)]
    struct UpdateDeleteCase {
        /// `REPLICA IDENTITY FULL` when set; otherwise a key identity
        /// (`DEFAULT` with a primary key, or `USING INDEX`).
        full_identity: bool,
        columns: Vec<ModelColumn>,
    }

    /// Raw per-column draw before [`UpdateDeleteCase`] normalization:
    /// `(is_text, nullable, replicated, identity)`, `(old_null, old_int,
    /// old_text)`, `(changed, new_null, new_int, new_text)`, `toasted`.
    #[cfg(feature = "test-utils")]
    type RawModelColumn =
        ((bool, bool, bool, bool), (bool, i64, String), (bool, bool, i64, String), bool);

    /// One field of a tuple as the model encodes it on the wire.
    #[cfg(feature = "test-utils")]
    #[derive(Debug, Clone)]
    enum ModelTupleCell {
        Null,
        UnchangedToast,
        Text(String),
    }

    #[cfg(feature = "test-utils")]
    impl ModelTupleCell {
        /// Encodes an optional model value as a text or null field.
        fn from_value(value: Option<&ModelValue>) -> Self {
            value.map_or(Self::Null, |value| Self::Text(value.to_pg_text()))
        }
    }

    #[cfg(feature = "test-utils")]
    impl UpdateDeleteCase {
        /// Builds a case that PostgreSQL could publish from raw draws.
        ///
        /// The rules mirror source constraints rather than narrowing the
        /// envelope: an update/delete publication must replicate every
        /// replica-identity column, a key identity has at least one column and
        /// its columns are `NOT NULL`, `FULL` flags every replicated column as
        /// identity, and only a non-null, unchanged varlena value can be sent
        /// as unchanged TOAST.
        fn from_raw(full_identity: bool, raw_columns: Vec<RawModelColumn>) -> Self {
            let mut columns: Vec<ModelColumn> = raw_columns
                .iter()
                .map(|((is_text, nullable, replicated, identity), ..)| ModelColumn {
                    typ: if *is_text { ModelColumnType::Text } else { ModelColumnType::Int8 },
                    nullable: *nullable,
                    replicated: *replicated,
                    identity: *replicated && (full_identity || *identity),
                    old: None,
                    new: None,
                    unchanged_toast: false,
                })
                .collect();

            if !columns.iter().any(|column| column.replicated) {
                columns[0].replicated = true;
                columns[0].identity = full_identity;
            }
            if !columns.iter().any(|column| column.identity) {
                columns.iter_mut().find(|column| column.replicated).unwrap().identity = true;
            }

            for (column, raw_column) in columns.iter_mut().zip(&raw_columns) {
                let (
                    _,
                    (old_null, old_int, old_text),
                    (changed, new_null, new_int, new_text),
                    toasted,
                ) = raw_column;
                // Key identities come from a primary key or a unique index over
                // `NOT NULL` columns.
                if column.identity && !full_identity {
                    column.nullable = false;
                }

                let typ = column.typ;
                let value = |int: &i64, text: &String| match typ {
                    ModelColumnType::Int8 => ModelValue::Int8(*int),
                    ModelColumnType::Text => ModelValue::Text(text.clone()),
                };
                column.old = (!(column.nullable && *old_null)).then(|| value(old_int, old_text));
                column.new = if !changed {
                    column.old.clone()
                } else if column.nullable && *new_null {
                    None
                } else {
                    Some(value(new_int, new_text))
                };
                column.unchanged_toast = *toasted
                    && typ == ModelColumnType::Text
                    && column.replicated
                    && column.old.is_some()
                    && column.new == column.old;
            }

            Self { full_identity, columns }
        }

        /// Builds the replicated schema etl would hold for this table.
        fn replicated_table_schema(&self) -> ReplicatedTableSchema {
            let mut primary_key_position = 0;
            let column_schemas = self
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let typ = match column.typ {
                        ModelColumnType::Int8 => Type::INT8,
                        ModelColumnType::Text => Type::TEXT,
                    };
                    let column_schema = ColumnSchema::new(
                        format!("c{index}"),
                        typ,
                        -1,
                        i32::try_from(index + 1).unwrap(),
                        column.nullable,
                    );
                    if column.identity && !self.full_identity {
                        primary_key_position += 1;
                        column_schema.with_primary_key(primary_key_position)
                    } else {
                        column_schema
                    }
                })
                .collect();
            let table_schema = Arc::new(TableSchema::new(
                TableId::new(42),
                TableName::new("public".to_owned(), "model".to_owned()),
                column_schemas,
            ));
            let mask = |flag: fn(&ModelColumn) -> bool| {
                self.columns.iter().map(|column| u8::from(flag(column))).collect::<Vec<_>>()
            };

            ReplicatedTableSchema::from_masks(
                table_schema,
                ReplicationMask::from_bytes(mask(|column| column.replicated)),
                crate::schema::IdentityMask::from_bytes(mask(|column| column.identity)),
            )
        }

        /// Returns the replicated columns in table order.
        fn replicated_columns(&self) -> impl Iterator<Item = &ModelColumn> {
            self.columns.iter().filter(|column| column.replicated)
        }

        /// Returns whether pgoutput attaches a `K` tuple to the update.
        ///
        /// `heap_update` logs the old key when a key column changed or when a
        /// key column is stored externally, which is the case for every key
        /// column the new tuple sends as unchanged TOAST.
        fn sends_key_tuple(&self) -> bool {
            !self.full_identity
                && self.replicated_columns().any(|column| {
                    column.identity && (column.old != column.new || column.unchanged_toast)
                })
        }

        /// Encodes the `O` tuple: the detoasted old row over replicated
        /// columns.
        fn full_old_tuple(&self) -> Vec<ModelTupleCell> {
            self.replicated_columns()
                .map(|column| ModelTupleCell::from_value(column.old.as_ref()))
                .collect()
        }

        /// Encodes the `K` tuple the way pgoutput writes it.
        ///
        /// `ExtractReplicaIdentity` forms a full-width heap tuple with nulls
        /// outside the key, and `logicalrep_write_tuple` writes every published
        /// column of it.
        fn key_tuple(&self) -> Vec<ModelTupleCell> {
            self.replicated_columns()
                .map(|column| {
                    if column.identity {
                        ModelTupleCell::from_value(column.old.as_ref())
                    } else {
                        ModelTupleCell::Null
                    }
                })
                .collect()
        }

        /// Encodes the `N` tuple, with `u` for unchanged TOAST columns.
        fn new_tuple(&self) -> Vec<ModelTupleCell> {
            self.replicated_columns()
                .map(|column| {
                    if column.unchanged_toast {
                        ModelTupleCell::UnchangedToast
                    } else {
                        ModelTupleCell::from_value(column.new.as_ref())
                    }
                })
                .collect()
        }

        /// Returns the old-side tuple for an update, or `None` when pgoutput
        /// sends neither `O` nor `K`.
        fn update_old_side_tuple(&self) -> Option<Vec<ModelTupleCell>> {
            if self.full_identity {
                Some(self.full_old_tuple())
            } else if self.sends_key_tuple() {
                Some(self.key_tuple())
            } else {
                None
            }
        }

        /// Returns the old-side tuple pgoutput sends for a delete.
        fn delete_old_side_tuple(&self) -> Vec<ModelTupleCell> {
            if self.full_identity { self.full_old_tuple() } else { self.key_tuple() }
        }

        /// Returns the old row etl must decode from a delete.
        fn expected_delete_old_row(&self) -> OldTableRow {
            if self.full_identity {
                OldTableRow::Full(TableRow::new(
                    self.replicated_columns().map(ModelColumn::old_cell).collect(),
                ))
            } else {
                OldTableRow::Key(TableRow::new(
                    self.replicated_columns()
                        .filter(|column| column.identity)
                        .map(ModelColumn::old_cell)
                        .collect(),
                ))
            }
        }

        /// Returns the old row etl must decode from an update.
        fn expected_update_old_row(&self) -> Option<OldTableRow> {
            (self.full_identity || self.sends_key_tuple()).then(|| self.expected_delete_old_row())
        }

        /// Returns the new row etl must decode from an update.
        ///
        /// An unchanged TOAST column is recoverable from a full old row, or
        /// from a key row when it is a key column; every other one is missing.
        fn expected_new_row(&self) -> UpdatedTableRow {
            let sends_key_tuple = self.sends_key_tuple();
            let mut present_values = Vec::new();
            let mut missing_column_indexes = Vec::new();
            for (index, column) in self.replicated_columns().enumerate() {
                let recoverable = self.full_identity || (sends_key_tuple && column.identity);
                if column.unchanged_toast && !recoverable {
                    missing_column_indexes.push(index);
                } else {
                    present_values.push(column.new_cell());
                }
            }

            if missing_column_indexes.is_empty() {
                UpdatedTableRow::Full(TableRow::new(present_values))
            } else {
                UpdatedTableRow::Partial(PartialTableRow::new(
                    self.replicated_columns().count(),
                    TableRow::new(present_values),
                    missing_column_indexes,
                ))
            }
        }
    }

    /// Borrows model tuple fields as the shared test encoder's input.
    #[cfg(feature = "test-utils")]
    fn test_tuple(cells: &[ModelTupleCell]) -> Vec<TestTupleData<'_>> {
        cells
            .iter()
            .map(|cell| match cell {
                ModelTupleCell::Null => TestTupleData::Null,
                ModelTupleCell::UnchangedToast => TestTupleData::UnchangedToast,
                ModelTupleCell::Text(value) => TestTupleData::Text(value),
            })
            .collect()
    }

    /// Truncates `cells` to `width` or pads it with nulls.
    #[cfg(feature = "test-utils")]
    fn resize_tuple(cells: &[ModelTupleCell], width: usize) -> Vec<ModelTupleCell> {
        let mut resized = cells.to_vec();
        resized.resize(width, ModelTupleCell::Null);
        resized
    }

    /// Encodes an update whose old side is `O` for full identity and `K`
    /// otherwise.
    #[cfg(feature = "test-utils")]
    fn model_update_body(
        full_identity: bool,
        old_side: Option<&[ModelTupleCell]>,
        new_tuple: &[ModelTupleCell],
    ) -> postgres_replication::protocol::UpdateBody {
        let old_side = old_side.map(test_tuple);
        let new_tuple = test_tuple(new_tuple);
        if full_identity {
            parse_update_body_from_parts(42, old_side.as_deref(), None, &new_tuple)
        } else {
            parse_update_body_from_parts(42, None, old_side.as_deref(), &new_tuple)
        }
    }

    /// Encodes a delete whose old side is `O` for full identity and `K`
    /// otherwise.
    #[cfg(feature = "test-utils")]
    fn model_delete_body(
        full_identity: bool,
        old_side: &[ModelTupleCell],
    ) -> postgres_replication::protocol::DeleteBody {
        let old_side = test_tuple(old_side);
        if full_identity {
            parse_delete_body_from_parts(42, Some(&old_side), None)
        } else {
            parse_delete_body_from_parts(42, None, Some(&old_side))
        }
    }

    /// Generates tables of 1 to 10 `int8`/`text` columns with random
    /// nullability, publication column list, replica identity, and one row
    /// update.
    #[cfg(feature = "test-utils")]
    fn update_delete_case() -> impl Strategy<Value = UpdateDeleteCase> {
        let raw_column = (
            (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()),
            (any::<bool>(), any::<i64>(), pg_text()),
            (any::<bool>(), any::<bool>(), any::<i64>(), pg_text()),
            any::<bool>(),
        );

        (any::<bool>(), collection::vec(raw_column, 1..=10)).prop_map(
            |(full_identity, raw_columns)| UpdateDeleteCase::from_raw(full_identity, raw_columns),
        )
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn update_and_delete_tuples_decode_like_pgoutput_reference_model() {
        run_property(
            "update_and_delete_tuples_decode_like_pgoutput_reference_model",
            &update_delete_case(),
            |case| {
                let update_body = model_update_body(
                    case.full_identity,
                    case.update_old_side_tuple().as_deref(),
                    &case.new_tuple(),
                );
                let update = parse_event_from_update_message(
                    case.replicated_table_schema(),
                    PgLsn::from(20),
                    0,
                    &update_body,
                )
                .unwrap();
                prop_assert_eq!(update.old_table_row, case.expected_update_old_row());
                prop_assert_eq!(update.updated_table_row, case.expected_new_row());

                let delete_body =
                    model_delete_body(case.full_identity, &case.delete_old_side_tuple());
                let delete = parse_event_from_delete_message(
                    case.replicated_table_schema(),
                    PgLsn::from(20),
                    0,
                    &delete_body,
                )
                .unwrap();
                prop_assert_eq!(delete.old_table_row, Some(case.expected_delete_old_row()));

                Ok(())
            },
        );
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn update_and_delete_tuples_with_wrong_width_are_rejected() {
        run_property(
            "update_and_delete_tuples_with_wrong_width_are_rejected",
            &update_delete_case(),
            |case| {
                let replicated_count = case.replicated_columns().count();
                let identity_count =
                    case.replicated_columns().filter(|column| column.identity).count();
                let new_tuple = case.new_tuple();
                let old_side = case.delete_old_side_tuple();

                for width in 0..=replicated_count + 1 {
                    // Key images are accepted dense or full width; any other
                    // width must fail rather than yield a short or shifted row.
                    let accepted = width == replicated_count
                        || (!case.full_identity && width == identity_count);
                    if !accepted {
                        let resized = resize_tuple(&old_side, width);

                        let update_body =
                            model_update_body(case.full_identity, Some(&resized), &new_tuple);
                        let update = parse_event_from_update_message(
                            case.replicated_table_schema(),
                            PgLsn::from(20),
                            0,
                            &update_body,
                        );
                        prop_assert_eq!(update.unwrap_err().kind(), ErrorKind::ConversionError);

                        let delete_body = model_delete_body(case.full_identity, &resized);
                        let delete = parse_event_from_delete_message(
                            case.replicated_table_schema(),
                            PgLsn::from(20),
                            0,
                            &delete_body,
                        );
                        prop_assert_eq!(delete.unwrap_err().kind(), ErrorKind::ConversionError);
                    }

                    if width != replicated_count {
                        let update_body = model_update_body(
                            case.full_identity,
                            case.update_old_side_tuple().as_deref(),
                            &resize_tuple(&new_tuple, width),
                        );
                        let update = parse_event_from_update_message(
                            case.replicated_table_schema(),
                            PgLsn::from(20),
                            0,
                            &update_body,
                        );
                        prop_assert_eq!(update.unwrap_err().kind(), ErrorKind::ConversionError);
                    }
                }

                Ok(())
            },
        );
    }
}
