//! Benchmark-only entrypoints for internal hot paths.
//!
//! Each entrypoint runs the code a production path runs once per row or once
//! per replication message, so `etl-benchmarks` can count its instructions and
//! allocations without a Postgres connection.

use bytes::Bytes;
use postgres_replication::protocol::LogicalReplicationMessage;
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    bail,
    data::{Cell, TableRow},
    error::{ErrorKind, EtlResult},
    event::Event,
    postgres::codec,
    replication::{CdcIngestionMetrics, WorkerType},
    schema::{ColumnSchema, ReplicatedTableSchema},
    source_payload_metadata::{
        StreamingPayloadMetadata, TableCopyIngestionMetrics, TableCopyPayloadMetadata,
    },
};

/// Parses one Postgres text value as `typ`.
pub fn parse_text_cell(typ: &Type, value: &str) -> EtlResult<Cell> {
    codec::parse_cell_from_postgres_text(typ, value)
}

/// Parses COPY text rows as the table-copy stream does, appending each decoded
/// row to `table_rows`.
///
/// Like the stream, one unescape buffer serves every row.
pub fn parse_copy_rows<'a>(
    rows: impl IntoIterator<Item = &'a [u8]>,
    column_schemas: &[ColumnSchema],
    table_rows: &mut Vec<TableRow>,
) -> EtlResult<()> {
    let mut field_buffer = String::new();
    for row in rows {
        table_rows.push(codec::parse_table_row_from_postgres_copy_bytes(
            row,
            column_schemas,
            &mut field_buffer,
        )?);
    }

    Ok(())
}

/// Records the row-size observation the copy path emits for each COPY row.
///
/// Like one table-copy stream, the rows share one set of metric handles.
pub fn record_copy_rows_received(copy_row_bytes: &[u64]) {
    let mut ingestion_metrics = TableCopyIngestionMetrics::new();
    for copy_bytes in copy_row_bytes {
        ingestion_metrics.record_row_size(TableCopyPayloadMetadata::new(*copy_bytes));
    }
}

/// Parses one pgoutput insert, update, or delete message and decodes it into
/// an event, as the apply loop does for each row change.
pub fn decode_row_change(
    message: &Bytes,
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<Event> {
    let commit_lsn = PgLsn::from(0);

    match LogicalReplicationMessage::parse(message)? {
        LogicalReplicationMessage::Insert(body) => codec::parse_event_from_insert_message(
            replicated_table_schema.clone(),
            commit_lsn,
            0,
            &body,
        )
        .map(Event::Insert),
        LogicalReplicationMessage::Update(body) => codec::parse_event_from_update_message(
            replicated_table_schema.clone(),
            commit_lsn,
            0,
            &body,
        )
        .map(Event::Update),
        LogicalReplicationMessage::Delete(body) => codec::parse_event_from_delete_message(
            replicated_table_schema.clone(),
            commit_lsn,
            0,
            &body,
        )
        .map(Event::Delete),
        _ => bail!(
            ErrorKind::InvalidData,
            "Benchmark message is not a row change",
            "Only insert, update, and delete messages decode into row-change events"
        ),
    }
}

/// Records the source metrics the apply loop emits for each pgoutput insert
/// message, given the tuple-data bytes each message carries.
///
/// This mirrors the per-message recording in the apply loop: the replication
/// message counter, the received event counter, and the payload byte and
/// row-size observations. Like one apply loop, the messages share one set of
/// metric handles.
pub fn record_cdc_inserts_received(insert_message_bytes: &[u64]) {
    let mut ingestion_metrics = CdcIngestionMetrics::new(WorkerType::Apply);
    for insert_bytes in insert_message_bytes {
        ingestion_metrics.record_replication_message();
        ingestion_metrics.record_event_received();
        ingestion_metrics.record_row_event(StreamingPayloadMetadata::insert(*insert_bytes));
    }
}
