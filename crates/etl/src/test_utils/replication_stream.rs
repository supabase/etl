//! Helpers for asserting replication stream conversions in integration tests.

use postgres_replication::protocol::TupleData;
use tokio_postgres::types::Type;

use crate::{
    data::{Cell, TableRow},
    error::EtlResult,
    postgres::codec::{
        convert_tuple_to_row, parse_cell_from_postgres_text,
        parse_table_row_from_postgres_copy_bytes,
    },
    schema::ColumnSchema,
};

/// Parses a raw Postgres COPY row with the production conversion path.
pub fn parse_copy_row(row: &[u8], column_schemas: &[ColumnSchema]) -> EtlResult<TableRow> {
    parse_table_row_from_postgres_copy_bytes(row, column_schemas)
}

/// Parses a logical replication tuple with the production conversion path.
pub fn parse_tuple(
    tuple_data: &[TupleData],
    column_schemas: &[ColumnSchema],
) -> EtlResult<TableRow> {
    convert_tuple_to_row(column_schemas.iter(), tuple_data)
}

/// Parses a single Postgres text-format value with the production conversion
/// path.
pub fn parse_text_cell(typ: &Type, value: &str) -> EtlResult<Cell> {
    parse_cell_from_postgres_text(typ, value)
}
