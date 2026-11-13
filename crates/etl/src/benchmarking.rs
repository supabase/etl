//! Benchmark-only entrypoints for internal hot paths.
//!
//! Each entrypoint runs the code a production path runs once per row or once
//! per replication message, so `etl-benchmarks` can count its instructions and
//! allocations without a Postgres connection.

use bytes::Bytes;
use metrics::counter;
use postgres_replication::protocol::LogicalReplicationMessage;
use tokio_postgres::types::{PgLsn, Type};

use crate::{
    bail,
    data::{Cell, TableRow},
    error::{ErrorKind, EtlResult},
    event::Event,
    observability::{
        CDC_REPLICATION_PATH, ETL_EVENTS_RECEIVED_TOTAL, ETL_REPLICATION_MESSAGES_TOTAL,
        REPLICATION_PATH_LABEL, WORKER_TYPE_LABEL,
    },
    postgres::codec,
    replication::WorkerType,
    schema::{ColumnSchema, ReplicatedTableSchema},
    source_payload_metadata::{StreamingPayloadMetadata, TableCopyPayloadMetadata},
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

/// Records the row-size observation the copy path emits for one COPY row.
pub fn record_copy_row_received(copy_bytes: u64) {
    TableCopyPayloadMetadata::new(copy_bytes).record_row_size();
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

/// Records the source metrics the apply loop emits for one pgoutput insert
/// message carrying `insert_bytes` of tuple data.
///
/// This mirrors the per-message recording in the apply loop: the replication
/// message counter, the received event counter, and the payload byte and
/// row-size observations.
pub fn record_cdc_insert_received(insert_bytes: u64) {
    let worker_type = WorkerType::Apply.as_str();

    counter!(ETL_REPLICATION_MESSAGES_TOTAL, WORKER_TYPE_LABEL => worker_type).increment(1);
    counter!(
        ETL_EVENTS_RECEIVED_TOTAL,
        WORKER_TYPE_LABEL => worker_type,
        REPLICATION_PATH_LABEL => CDC_REPLICATION_PATH,
    )
    .increment(1);

    let streaming_payload_metadata = StreamingPayloadMetadata::insert(insert_bytes);
    streaming_payload_metadata.record_received();
    streaming_payload_metadata.record_row_size();
}
