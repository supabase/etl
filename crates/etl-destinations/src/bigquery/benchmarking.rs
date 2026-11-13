//! Benchmark-only entrypoints for BigQuery hot paths.

use etl::{data::TableRow, error::EtlResult};
use prost::Message;

use crate::bigquery::encoding::BigQueryTableRow;

/// Encodes one copied row and appends its Protocol Buffer bytes to `output`.
///
/// This is the per-row work of a table-copy write: the row is encoded once
/// into its own buffer, then appended to an append request.
pub fn encode_table_row(table_row: TableRow, output: &mut Vec<u8>) -> EtlResult<()> {
    BigQueryTableRow::try_from(table_row)?.encode_raw(output);

    Ok(())
}
