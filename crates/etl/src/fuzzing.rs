//! Fuzz-only entrypoints for internal parsers.

use std::sync::Arc;

use bytes::Bytes;
use etl_postgres::numeric::PgNumeric;
use postgres_replication::protocol::{LogicalReplicationMessage, TupleData};
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    error::EtlResult,
    postgres::codec,
    schema::{
        ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, TableId, TableName,
        TableSchema,
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
