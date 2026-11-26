//! Benchmark-only entrypoints for BigQuery hot paths.

use etl::{data::TableRow, error::EtlResult, schema::ReplicatedTableSchema};
use prost::Message;

use crate::bigquery::core::{BIGQUERY_COPY_SEQUENCE_NUMBER, bigquery_upsert_row};

/// Appends the Protocol Buffer bytes of one table-copy UPSERT to `output`.
pub fn encode_table_row(
    schema: &ReplicatedTableSchema,
    table_row: TableRow,
    output: &mut Vec<u8>,
) -> EtlResult<()> {
    bigquery_upsert_row(schema, table_row, BIGQUERY_COPY_SEQUENCE_NUMBER.to_owned())?
        .encode_raw(output);

    Ok(())
}
