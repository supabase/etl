//! Fuzz-only entrypoints for internal parsers.

use std::{collections::HashSet, str::FromStr, sync::Arc};

use bytes::Bytes;
use etl_postgres::{numeric::PgNumeric, type_utils::convert_type_oid_to_type};
use postgres_replication::protocol::{LogicalReplicationMessage, TupleData};
use serde_json::json;
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    error::EtlResult,
    postgres::codec::{self, RelationSchemaMismatchKind, SchemaChangeMessage},
    schema::{
        ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId,
        TableName, TableSchema,
    },
};

/// Postgres types the text cell fuzz targets cycle through.
///
/// Covers every dedicated codec branch, scalar and array, plus the generic
/// array fallback (`varchar[]`) and the scalar text fallback.
///
/// Hand-written corpus seeds use in-range selector bytes as direct indices into
/// this table. Preserve existing order and append new types so those seeds keep
/// their meaning; arbitrary selectors continue to wrap modulo the table length.
const TEXT_CELL_FUZZ_TYPES: &[Type] = &[
    Type::BOOL,
    Type::BOOL_ARRAY,
    Type::INT2,
    Type::INT2_ARRAY,
    Type::INT4,
    Type::INT4_ARRAY,
    Type::INT8,
    Type::INT8_ARRAY,
    Type::FLOAT4,
    Type::FLOAT4_ARRAY,
    Type::FLOAT8,
    Type::FLOAT8_ARRAY,
    Type::NUMERIC,
    Type::NUMERIC_ARRAY,
    Type::BYTEA,
    Type::BYTEA_ARRAY,
    Type::DATE,
    Type::DATE_ARRAY,
    Type::TIME,
    Type::TIME_ARRAY,
    Type::TIMETZ,
    Type::TIMETZ_ARRAY,
    Type::TIMESTAMP,
    Type::TIMESTAMP_ARRAY,
    Type::TIMESTAMPTZ,
    Type::TIMESTAMPTZ_ARRAY,
    Type::UUID,
    Type::UUID_ARRAY,
    Type::JSON,
    Type::JSON_ARRAY,
    Type::JSONB,
    Type::JSONB_ARRAY,
    Type::OID,
    Type::OID_ARRAY,
    Type::TEXT,
    Type::VARCHAR_ARRAY,
];

/// Postgres types the pgoutput tuple fuzz target cycles through.
///
/// Value codecs have their own targets, so this table stays small and favors
/// types that random bytes decode often, letting the structural checks run.
const PGOUTPUT_TUPLE_FUZZ_TYPES: &[Type] = &[Type::TEXT, Type::INT8, Type::BOOL, Type::JSONB];

/// Relation id written into assembled pgoutput messages.
const PGOUTPUT_TUPLE_FUZZ_REL_ID: u32 = 42;

/// Column names the relation fuzz target draws from.
///
/// Stored schemas use each name at most once, as Postgres requires. Relations
/// may repeat names, and a selector past the end sends a name that is not
/// valid UTF-8.
const RELATION_FUZZ_NAMES: [&str; 8] = ["id", "a", "b", "c", "d", "e", "f", "g"];

/// Built-in column types the relation fuzz target draws from: an integer
/// widening pair, the string types ETL decodes alike, a numeric type, and a
/// timestamp pair.
const RELATION_FUZZ_TYPES: [Type; 8] = [
    Type::INT4,
    Type::INT8,
    Type::TEXT,
    Type::VARCHAR,
    Type::BPCHAR,
    Type::NUMERIC,
    Type::TIMESTAMP,
    Type::TIMESTAMPTZ,
];

/// Type OID outside the built-in range, standing in for domains, enums, and
/// extension types, which ETL maps to text.
const RELATION_FUZZ_UNKNOWN_TYPE_OID: u32 = 16_500;

/// Replica-identity modes a schema change message can report, plus one that
/// Postgres never sends.
const RELATION_FUZZ_RELREPLIDENTS: [&str; 5] = ["d", "f", "i", "n", "x"];

/// Returns the fuzz type table entry for a selector byte.
fn text_cell_fuzz_type(selector: u8) -> &'static Type {
    &TEXT_CELL_FUZZ_TYPES[usize::from(selector) % TEXT_CELL_FUZZ_TYPES.len()]
}

/// Parses a Postgres bytea hex string through the internal parser.
pub fn parse_bytea_hex_string(value: &str) -> EtlResult<Vec<u8>> {
    codec::parse_bytea_hex_string_for_fuzzing(value)
}

/// Parses one Postgres text value as the type picked by `selector`.
pub fn parse_text_cell(selector: u8, value: &str) -> EtlResult<Cell> {
    codec::parse_cell_from_postgres_text(text_cell_fuzz_type(selector), value)
}

/// Parses one COPY text row against a schema derived from the input.
///
/// The first byte picks the column count (1 to 8), the following bytes pick
/// each column's type from the fuzz type table, and the remaining bytes are the
/// row payload.
pub fn parse_copy_row(data: &[u8]) -> EtlResult<TableRow> {
    let Some((&first, rest)) = data.split_first() else {
        return codec::parse_table_row_from_postgres_copy_bytes(&[], &[]);
    };

    let column_count = usize::from(first) % 8 + 1;
    let selectors = rest.get(..column_count).unwrap_or(rest);
    let row = rest.get(column_count..).unwrap_or(&[]);

    let column_schemas: Vec<ColumnSchema> = selectors
        .iter()
        .enumerate()
        .map(|(index, selector)| {
            ColumnSchema::new(
                format!("column_{index}"),
                text_cell_fuzz_type(*selector).clone(),
                -1,
                i32::try_from(index + 1).expect("column count is at most 8"),
                true,
            )
        })
        .collect();

    codec::parse_table_row_from_postgres_copy_bytes(row, &column_schemas)
}

/// Checks that the date, time and timestamp fast paths agree with chrono.
///
/// # Panics
///
/// Panics when a fast path accepts input that chrono rejects or decodes
/// differently; that panic is the fuzz target's finding signal.
pub fn check_time_fast_paths(value: &str) {
    codec::check_fast_paths_match_chrono(value);
}

/// Checks the parse, print, parse stability of a numeric text value.
///
/// # Panics
///
/// Panics when a value accepted from text does not reparse from its own text
/// form to the same value with the same rendering; that panic is the fuzz
/// target's finding signal.
pub fn check_numeric_text_roundtrip(value: &str) {
    let Ok(parsed) = value.parse::<PgNumeric>() else {
        return;
    };

    let printed = parsed.to_string();
    let reparsed: PgNumeric = printed
        .parse()
        .unwrap_or_else(|error| panic!("printed numeric {printed:?} failed to reparse: {error:?}"));

    assert_eq!(parsed, reparsed, "numeric value not stable across parse/print/parse of {value:?}");
    assert_eq!(printed, reparsed.to_string(), "numeric text form not stable for {value:?}");
}

/// Byte cursor over fuzz input that yields zero once input runs out.
struct FuzzBytes<'a> {
    data: &'a [u8],
}

impl<'a> FuzzBytes<'a> {
    /// Returns the next byte, or zero when the input is exhausted.
    fn byte(&mut self) -> u8 {
        let Some((&first, rest)) = self.data.split_first() else {
            return 0;
        };

        self.data = rest;
        first
    }

    /// Returns up to `len` next bytes.
    fn take(&mut self, len: usize) -> &'a [u8] {
        let (taken, rest) = self.data.split_at(len.min(self.data.len()));
        self.data = rest;
        taken
    }
}

/// Appends one pgoutput tuple (`int16` field count, then fields) to `message`.
///
/// The width byte selects the replicated column count most of the time and an
/// arbitrary width otherwise, so wrong-width tuples are exercised too. Each
/// field is `n`, `u`, `t`, or `b`, with a one-byte payload length. Framing is
/// always well formed: this target fuzzes etl's conversion, not the
/// `postgres_replication` framing parser.
fn append_fuzz_tuple(message: &mut Vec<u8>, input: &mut FuzzBytes<'_>, replicated_count: usize) {
    let width_selector = input.byte();
    let width =
        if width_selector < 0xc0 { replicated_count } else { usize::from(width_selector) % 12 };

    message.extend_from_slice(&u16::try_from(width).expect("tuple width is small").to_be_bytes());

    for _ in 0..width {
        let tag = match input.byte() % 8 {
            0 => b'n',
            1 => b'u',
            7 => b'b',
            _ => b't',
        };

        message.push(tag);

        if matches!(tag, b'b' | b't') {
            let requested_len = usize::from(input.byte());
            let payload = input.take(requested_len);
            let payload_len = u32::try_from(payload.len()).expect("payload is at most 255 bytes");
            message.extend_from_slice(&payload_len.to_be_bytes());
            message.extend_from_slice(payload);
        }
    }
}

/// Asserts the structural invariants of a decoded old-side row.
fn check_old_table_row(
    old_table_row: &OldTableRow,
    replicated_count: usize,
    identity_count: usize,
) {
    match old_table_row {
        OldTableRow::Full(row) => assert_eq!(row.values().len(), replicated_count),
        OldTableRow::Key(row) => assert_eq!(row.values().len(), identity_count),
    }
}

/// Returns the type OID for a relation fuzz type selector byte.
fn relation_fuzz_type_oid(selector: u8) -> u32 {
    RELATION_FUZZ_TYPES
        .get(usize::from(selector) % (RELATION_FUZZ_TYPES.len() + 1))
        .map_or(RELATION_FUZZ_UNKNOWN_TYPE_OID, Type::oid)
}

/// Builds a schema change message from fuzz input.
///
/// The first byte picks the column count (1 to 8). Each column then takes five
/// bytes: name, type, attnum step, typmod, and flags (bit 0 `NOT NULL`, bit 1
/// primary key, bit 2 replica-identity index). A final byte picks the
/// replica-identity mode. The message is serialized to JSON and parsed by the
/// same code that handles schema change messages from the replication stream.
fn fuzz_schema_change_message(input: &mut FuzzBytes<'_>) -> SchemaChangeMessage {
    let column_count = usize::from(input.byte()) % RELATION_FUZZ_NAMES.len() + 1;
    let mut used_names = [false; RELATION_FUZZ_NAMES.len()];
    let mut attnum = 0_i32;
    let mut columns = Vec::with_capacity(column_count);
    let mut primary_key_attnums = Vec::new();
    let mut replica_identity_index_attnums = Vec::new();

    for _ in 0..column_count {
        // Postgres column names are unique, so a taken name moves on to the
        // next free one.
        let name_selector = usize::from(input.byte());
        let name_index = (0..RELATION_FUZZ_NAMES.len())
            .map(|offset| (name_selector + offset) % RELATION_FUZZ_NAMES.len())
            .find(|&index| !used_names[index])
            .expect("a stored schema has at most as many columns as names");
        used_names[name_index] = true;
        let type_oid = relation_fuzz_type_oid(input.byte());
        // A zero step repeats the previous attnum, which Postgres never
        // sends but the parser must survive.
        let attnum_step = input.byte();
        attnum += if attnum_step == 0 { 0 } else { i32::from(attnum_step % 3) + 1 };
        let typmod = i32::from(input.byte()) - 1;
        let flags = input.byte();
        if flags & 2 != 0 {
            primary_key_attnums.push(attnum);
        }

        if flags & 4 != 0 {
            replica_identity_index_attnums.push(attnum);
        }

        columns.push(json!({
            "attname": RELATION_FUZZ_NAMES[name_index],
            "atttypid": type_oid,
            "atttypmod": typmod,
            "formatted_type": "fuzz",
            "attnum": attnum,
            "attnotnull": flags & 1 != 0,
            "default_expression": null,
        }));
    }

    let relreplident =
        RELATION_FUZZ_RELREPLIDENTS[usize::from(input.byte()) % RELATION_FUZZ_RELREPLIDENTS.len()];

    let message = json!({
        "command_tag": "ALTER TABLE",
        "nspname": "public",
        "relname": "fuzz",
        "oid": PGOUTPUT_TUPLE_FUZZ_REL_ID,
        "identity": {
            "primary_key_attnums": primary_key_attnums,
            "relreplident": relreplident,
            "replica_identity_index_attnums": replica_identity_index_attnums,
        },
        "columns": columns,
    });
    SchemaChangeMessage::from_str(&message.to_string())
        .expect("assembled schema change message must parse")
}

/// Appends a pgoutput RELATION message built from fuzz input to `message`.
///
/// One byte picks the replica identity mode and one the column count (0 to
/// 8). Each column then takes three bytes: name, type, and flags (bit 0 marks
/// a replica-identity column).
fn append_fuzz_relation(message: &mut Vec<u8>, input: &mut FuzzBytes<'_>) {
    let replica_identity = [b'd', b'f', b'i', b'n'][usize::from(input.byte()) % 4];
    let column_count = usize::from(input.byte()) % (RELATION_FUZZ_NAMES.len() + 1);

    message.push(b'R');
    message.extend_from_slice(&PGOUTPUT_TUPLE_FUZZ_REL_ID.to_be_bytes());
    message.extend_from_slice(b"public\0fuzz\0");
    message.push(replica_identity);
    message.extend_from_slice(
        &u16::try_from(column_count).expect("relation width is small").to_be_bytes(),
    );

    for _ in 0..column_count {
        let name_selector = usize::from(input.byte()) % (RELATION_FUZZ_NAMES.len() + 1);
        let type_oid = relation_fuzz_type_oid(input.byte());
        let flags = input.byte();
        message.push(flags & 1);

        match RELATION_FUZZ_NAMES.get(name_selector) {
            Some(name) => message.extend_from_slice(name.as_bytes()),
            None => message.push(0xff),
        }

        message.push(0);
        message.extend_from_slice(&type_oid.to_be_bytes());
        message.extend_from_slice(&(-1_i32).to_be_bytes());
    }
}

/// Decodes a pgoutput insert, update, or delete against a schema derived from
/// the input and asserts structural invariants on every decoded row.
///
/// The first byte picks the column count (1 to 8). One descriptor byte per
/// column follows: bit 0 marks it replicated, bit 1 marks it a replica-identity
/// column (only when replicated), bit 2 marks it nullable, and the higher bits
/// pick its type. The next byte picks the message kind and which old-side tuple
/// (`K`, `O`, or none) is attached. The rest encodes the tuples.
///
/// # Panics
///
/// Panics when a successfully decoded row breaks an invariant: row widths must
/// match the replicated or identity column count, a partial row's missing
/// indexes must be strictly increasing, in range, complementary to its present
/// values, and limited to fields sent as unchanged TOAST, a full old row must
/// leave nothing missing, and an explicit null field must decode to
/// [`Cell::Null`]. That panic is the fuzz target's finding signal.
pub fn check_pgoutput_tuple_decoding(data: &[u8]) {
    let mut input = FuzzBytes { data };
    let column_count = usize::from(input.byte()) % 8 + 1;

    let mut column_schemas = Vec::with_capacity(column_count);
    let mut replication_mask = Vec::with_capacity(column_count);
    let mut identity_mask = Vec::with_capacity(column_count);

    for index in 0..column_count {
        let descriptor = input.byte();
        let replicated = descriptor & 1 != 0;
        let identity = replicated && descriptor & 2 != 0;
        let typ = &PGOUTPUT_TUPLE_FUZZ_TYPES
            [usize::from(descriptor >> 3) % PGOUTPUT_TUPLE_FUZZ_TYPES.len()];
        column_schemas.push(ColumnSchema::new(
            format!("column_{index}"),
            typ.clone(),
            -1,
            i32::try_from(index + 1).expect("column count is at most 8"),
            descriptor & 4 != 0,
        ));
        replication_mask.push(u8::from(replicated));
        identity_mask.push(u8::from(identity));
    }

    let replicated_count = replication_mask.iter().filter(|&&flag| flag == 1).count();
    let identity_count = identity_mask.iter().filter(|&&flag| flag == 1).count();
    let table_schema = Arc::new(TableSchema::new(
        TableId::new(PGOUTPUT_TUPLE_FUZZ_REL_ID),
        TableName::new("public".to_owned(), "fuzz".to_owned()),
        column_schemas,
    ));
    let replicated_table_schema = ReplicatedTableSchema::from_masks(
        table_schema,
        ReplicationMask::from_bytes(replication_mask),
        IdentityMask::from_bytes(identity_mask),
    );

    let kind = input.byte();
    let mut message = Vec::new();

    match kind % 3 {
        0 => message.push(b'I'),
        1 => message.push(b'U'),
        _ => message.push(b'D'),
    }

    message.extend_from_slice(&PGOUTPUT_TUPLE_FUZZ_REL_ID.to_be_bytes());

    let old_side_tag = match (kind % 3, (kind / 3) % 3) {
        (0, _) | (1, 0) => None,
        (_, 1) => Some(b'O'),
        _ => Some(b'K'),
    };

    if let Some(old_side_tag) = old_side_tag {
        message.push(old_side_tag);
        append_fuzz_tuple(&mut message, &mut input, replicated_count);
    }

    if kind % 3 != 2 {
        message.push(b'N');
        append_fuzz_tuple(&mut message, &mut input, replicated_count);
    }

    let message = LogicalReplicationMessage::parse(&Bytes::from(message))
        .expect("assembled pgoutput message must parse");
    let commit_lsn = PgLsn::from(0);

    match message {
        LogicalReplicationMessage::Insert(body) => {
            let Ok(event) = codec::parse_event_from_insert_message(
                replicated_table_schema,
                commit_lsn,
                0,
                &body,
            ) else {
                return;
            };

            assert_eq!(event.table_row.values().len(), replicated_count);
        }
        LogicalReplicationMessage::Update(body) => {
            let Ok(event) = codec::parse_event_from_update_message(
                replicated_table_schema,
                commit_lsn,
                0,
                &body,
            ) else {
                return;
            };

            if let Some(old_table_row) = &event.old_table_row {
                check_old_table_row(old_table_row, replicated_count, identity_count);
            }

            let new_tuple = body.new_tuple().tuple_data();

            let (present_values, missing_column_indexes) = match &event.updated_table_row {
                UpdatedTableRow::Full(row) => (row.values(), &[][..]),
                UpdatedTableRow::Partial(row) => {
                    assert_eq!(row.total_columns(), replicated_count);
                    (row.values(), row.missing_column_indexes())
                }
            };

            assert_eq!(present_values.len() + missing_column_indexes.len(), replicated_count);
            assert!(missing_column_indexes.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(
                missing_column_indexes.iter().all(|&index| {
                    matches!(new_tuple.get(index), Some(TupleData::UnchangedToast))
                })
            );

            match &event.old_table_row {
                // Without an old-side image no unchanged TOAST value can be
                // recovered, so every such field must be reported missing.
                None => assert!(new_tuple.iter().enumerate().all(|(index, field)| {
                    !matches!(field, TupleData::UnchangedToast)
                        || missing_column_indexes.contains(&index)
                })),
                Some(OldTableRow::Full(_)) => assert!(missing_column_indexes.is_empty()),
                Some(OldTableRow::Key(_)) => {}
            }

            let mut present_values = present_values.iter();

            for (index, field) in new_tuple.iter().enumerate() {
                if missing_column_indexes.contains(&index) {
                    continue;
                }

                let value = present_values.next().expect("present values cover the row");

                if matches!(field, TupleData::Null) {
                    assert_eq!(value, &Cell::Null);
                }
            }
        }
        LogicalReplicationMessage::Delete(body) => {
            let Ok(event) = codec::parse_event_from_delete_message(
                replicated_table_schema,
                commit_lsn,
                0,
                &body,
            ) else {
                return;
            };

            let old_table_row = event.old_table_row.expect("deletes always carry an old row");
            check_old_table_row(&old_table_row, replicated_count, identity_count);
        }
        _ => unreachable!("assembled message is an insert, update, or delete"),
    }
}

/// Applies a fuzzed RELATION message to a stored schema from a fuzzed schema
/// change message, then decodes an insert with the resulting decoder.
///
/// The input encodes, in order, the schema change message (see
/// `fuzz_schema_change_message`), the RELATION message (see
/// `append_fuzz_relation`), and one insert tuple. The relation is applied
/// with `codec::replicated_table_schema_from_relation`, the function
/// `ApplyLoop::materialize_relation_schema` uses.
///
/// # Panics
///
/// Panics when any of these breaks: the relation is accepted exactly when
/// every relation column name is UTF-8 and present in the stored schema; the
/// decoder keeps exactly the stored columns the relation names, in stored
/// order; identity columns are replicated; the schema change identity builds a
/// mask for every mode Postgres sends, covering only replicated columns;
/// mismatch detection reports nothing exactly when names and mapped types
/// agree position by position, and reports `name_order` exactly when the first
/// disagreement is a name; a decoded insert has one value per replicated
/// column. That panic is the fuzz target's finding signal.
pub fn check_pgoutput_relation_materialization(data: &[u8]) {
    let mut input = FuzzBytes { data };

    let schema_change = fuzz_schema_change_message(&mut input);
    let column_count = schema_change.columns.len();
    let primary_key_attnums: HashSet<i32> =
        schema_change.identity.primary_key_attnums.iter().copied().collect();
    let identity = schema_change.identity.clone();
    let table_schema =
        Arc::new(schema_change.into_table_schema(SnapshotId::new(PgLsn::from(1), PgLsn::from(1))));
    assert_eq!(table_schema.column_schemas.len(), column_count);
    assert!(
        table_schema
            .column_schemas
            .windows(2)
            .all(|pair| pair[0].ordinal_position <= pair[1].ordinal_position)
    );
    assert!(table_schema.column_schemas.iter().all(|column| {
        column.primary_key_ordinal_position.is_some()
            == primary_key_attnums.contains(&column.ordinal_position)
    }));

    let mut message = Vec::new();
    append_fuzz_relation(&mut message, &mut input);

    let LogicalReplicationMessage::Relation(relation) =
        LogicalReplicationMessage::parse(&Bytes::from(message))
            .expect("assembled relation message must parse")
    else {
        unreachable!("assembled message is a relation");
    };

    // Production must accept exactly the relations whose column names are all
    // UTF-8 and present in the stored schema.
    let relation_names: Option<HashSet<&str>> =
        relation.columns().iter().map(|column| column.name().ok()).collect();
    let names_known = relation_names.as_ref().is_some_and(|names| {
        names
            .iter()
            .all(|name| table_schema.column_schemas.iter().any(|column| column.name == *name))
    });
    let materialized =
        codec::replicated_table_schema_from_relation(Arc::clone(&table_schema), &relation);
    assert_eq!(materialized.is_ok(), names_known);

    let (Ok((replicated_table_schema, mismatch)), Some(replicated_names)) =
        (materialized, relation_names)
    else {
        return;
    };

    let replication_mask = replicated_table_schema.replication_mask();

    let valid_relreplident = ["d", "f", "i", "n"].contains(&identity.relreplident.as_str());

    match identity.build_identity_mask(&table_schema, replication_mask) {
        Ok(mask) => {
            assert!(valid_relreplident);
            assert_eq!(mask.as_slice().len(), column_count);
            assert!(
                mask.as_slice()
                    .iter()
                    .zip(replication_mask.as_slice())
                    .all(|(&identity, &replicated)| identity <= replicated)
            );
        }
        Err(_) => assert!(!valid_relreplident),
    }

    let decoder_names: Vec<&str> =
        replicated_table_schema.column_schemas().map(|column| column.name.as_str()).collect();
    let stored_names: Vec<&str> = table_schema
        .column_schemas
        .iter()
        .filter(|column| replicated_names.contains(column.name.as_str()))
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(decoder_names, stored_names);
    assert!(
        replicated_table_schema
            .identity_column_schemas()
            .all(|column| replicated_names.contains(column.name.as_str()))
    );

    let decoder_columns: Vec<(&str, Option<Type>)> = replicated_table_schema
        .column_schemas()
        .map(|column| (column.name.as_str(), Some(column.typ.clone())))
        .collect();
    let relation_columns: Vec<(&str, Option<Type>)> = relation
        .columns()
        .iter()
        .map(|column| {
            let name = column.name().expect("relation names parsed above");
            (name, u32::try_from(column.type_id()).ok().map(convert_type_oid_to_type))
        })
        .collect();

    match mismatch.map(|mismatch| mismatch.kind()) {
        None => assert_eq!(decoder_columns, relation_columns),
        Some(RelationSchemaMismatchKind::ColumnCount) => {
            assert_ne!(decoder_columns.len(), relation_columns.len());
        }
        Some(kind) => {
            assert_eq!(decoder_columns.len(), relation_columns.len());
            let (decoder_column, relation_column) = decoder_columns
                .iter()
                .zip(&relation_columns)
                .find(|(decoder_column, relation_column)| decoder_column != relation_column)
                .expect("a reported mismatch has a disagreeing position");
            assert_eq!(
                kind == RelationSchemaMismatchKind::NameOrder,
                decoder_column.0 != relation_column.0
            );
        }
    }

    let replicated_count = replicated_table_schema.column_schemas().len();
    let mut message = vec![b'I'];
    message.extend_from_slice(&PGOUTPUT_TUPLE_FUZZ_REL_ID.to_be_bytes());
    message.push(b'N');
    append_fuzz_tuple(&mut message, &mut input, replicated_count);

    let LogicalReplicationMessage::Insert(insert) =
        LogicalReplicationMessage::parse(&Bytes::from(message))
            .expect("assembled insert message must parse")
    else {
        unreachable!("assembled message is an insert");
    };

    if let Ok(event) =
        codec::parse_event_from_insert_message(replicated_table_schema, PgLsn::from(0), 0, &insert)
    {
        assert_eq!(event.table_row.values().len(), replicated_count);
    }
}
