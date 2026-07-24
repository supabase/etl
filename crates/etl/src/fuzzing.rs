//! Fuzz-only entrypoints for internal parsers.

use etl_postgres::numeric::PgNumeric;
use tokio_postgres::types::Type;

use crate::{
    data::{Cell, TableRow},
    error::EtlResult,
    postgres::codec,
    schema::ColumnSchema,
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
/// each column's type from the fuzz type table, and the remaining bytes are
/// the row payload.
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
