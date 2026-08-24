//! Test helpers for invoking destination trait methods.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    data::TableRow,
    destination::{
        Destination, DestinationWriteStatus, TableCopyAttemptId, TableCopyBatch, TableCopyBatchId,
        TableCopyWrite, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::EtlResult,
    event::Event,
    schema::ReplicatedTableSchema,
};

/// Monotonic ID source for independent table-copy writes in tests.
static NEXT_TABLE_COPY_BATCH_ID: AtomicU64 = AtomicU64::new(0);
/// Shared attempt ID for independent table-copy writes in tests.
const TEST_TABLE_COPY_ATTEMPT_ID: TableCopyAttemptId = TableCopyAttemptId::from_u128(1);

/// Invokes [`Destination::write_events`] and waits for its completion.
///
/// This mirrors ETL's streaming-write call boundary without exposing the
/// private pending-result implementation.
pub async fn write_events<D: Destination>(
    destination: &D,
    durability: WriteEventsDurability,
    events: Vec<Event>,
) -> EtlResult<DestinationWriteStatus> {
    let (async_result, pending_result) = WriteEventsResult::new(());
    Destination::write_events(destination, events, durability, async_result).await?;

    pending_result.await.into_result()
}

/// Invokes [`Destination::write_table_rows`] and waits for its completion.
///
/// This mirrors ETL's table-copy call boundary without exposing the private
/// pending-result implementation.
pub async fn write_table_rows<D: Destination>(
    destination: &D,
    schema: &ReplicatedTableSchema,
    rows: Vec<TableRow>,
) -> EtlResult<DestinationWriteStatus> {
    let table_copy = if rows.is_empty() {
        TableCopyWrite::Finish
    } else {
        let ordinal = NEXT_TABLE_COPY_BATCH_ID.fetch_add(1, Ordering::Relaxed);
        let id = TableCopyBatchId::new(TEST_TABLE_COPY_ATTEMPT_ID, ordinal);
        TableCopyWrite::Batch(TableCopyBatch::new(id, rows))
    };

    write_table_copy(destination, schema, table_copy).await
}

/// Invokes [`Destination::write_table_rows`] with an explicit copy write.
///
/// This permits tests to redeliver a batch with the same idempotency key.
pub async fn write_table_copy<D: Destination>(
    destination: &D,
    schema: &ReplicatedTableSchema,
    table_copy: TableCopyWrite,
) -> EtlResult<DestinationWriteStatus> {
    let (async_result, pending_result) = WriteTableRowsResult::new(());
    Destination::write_table_rows(destination, schema, table_copy, async_result).await?;

    pending_result.await.into_result()
}
