//! Helpers for asserting replication stream conversions in integration tests.

use postgres_replication::protocol::TupleData;

use crate::{
    data::TableRow,
    error::EtlResult,
    postgres::codec::{convert_tuple_to_row, parse_table_row_from_postgres_copy_bytes},
    schema::ColumnSchema,
};

/// Parses a raw Postgres COPY row with the production conversion path.
pub fn parse_copy_row(row: &[u8], column_schemas: &[ColumnSchema]) -> EtlResult<TableRow> {
    parse_table_row_from_postgres_copy_bytes(row, column_schemas.iter())
}

/// Parses a logical replication tuple with the production conversion path.
pub fn parse_tuple(
    tuple_data: &[TupleData],
    column_schemas: &[ColumnSchema],
) -> EtlResult<TableRow> {
    convert_tuple_to_row(column_schemas.iter(), tuple_data)
}
