//! Benchmark-only entrypoints for ClickHouse hot paths.

use etl::{data::TableRow, error::EtlResult};
use etl_config::shared::ClickHouseEngine;

use crate::clickhouse::{
    core::clickhouse_copy_row,
    encoding::{ColumnEncoding, encode_row},
};

/// Encodes copied rows into RowBinary and appends the bytes to `output`.
///
/// This is the per-row work of one table-copy INSERT statement: each row is
/// converted, encoded into a reused row buffer, and appended to the statement
/// body. `column_types` are the ClickHouse types of every column, the
/// trailing CDC columns included.
pub fn encode_copy_rows(
    table_rows: impl IntoIterator<Item = TableRow>,
    column_types: &[&str],
    engine: ClickHouseEngine,
    output: &mut Vec<u8>,
) -> EtlResult<()> {
    let column_encodings: Vec<ColumnEncoding> =
        column_types.iter().map(|type_name| ColumnEncoding::from_type_name(type_name)).collect();
    let mut row_buf = Vec::new();
    let mut scratch = String::new();

    for table_row in table_rows {
        let row = clickhouse_copy_row(table_row, engine)?;
        row_buf.clear();
        encode_row(&row, &column_encodings, &mut scratch, &mut row_buf)?;
        output.extend_from_slice(&row_buf);
    }

    Ok(())
}
