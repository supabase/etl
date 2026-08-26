//! Test helpers for invoking destination trait methods.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    data::TableRow,
    destination::{
        Destination, DestinationWriteStatus, DropTableForCopyResult, TableCopyAttemptId,
        TableCopyBatchId, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::EtlResult,
    event::Event,
    schema::ReplicatedTableSchema,
};

/// Monotonic ID source for independent table-copy writes in tests.
static NEXT_TABLE_COPY_BATCH_ID: AtomicU64 = AtomicU64::new(0);
/// Shared attempt ID for independent table-copy writes in tests.
const TEST_TABLE_COPY_ATTEMPT_ID: TableCopyAttemptId = TableCopyAttemptId::from_u128(1);

/// Invokes [`Destination::drop_table_for_copy`] and waits for its completion.
pub async fn drop_table_for_copy<D: Destination>(
    destination: &D,
    schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    let (async_result, pending_result) = DropTableForCopyResult::new(());
    Destination::drop_table_for_copy(destination, schema, async_result).await?;

    pending_result.await.into_result()
}

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
    let batch_id = if rows.is_empty() {
        None
    } else {
        let ordinal = NEXT_TABLE_COPY_BATCH_ID.fetch_add(1, Ordering::Relaxed);
        Some(TableCopyBatchId::new(TEST_TABLE_COPY_ATTEMPT_ID, ordinal))
    };

    write_table_rows_inner(destination, schema, batch_id, rows).await
}

/// Invokes [`Destination::write_table_rows`] with an explicit batch ID.
///
/// This permits tests to redeliver a batch with the same idempotency key.
pub async fn write_table_rows_with_batch_id<D: Destination>(
    destination: &D,
    schema: &ReplicatedTableSchema,
    batch_id: TableCopyBatchId,
    rows: Vec<TableRow>,
) -> EtlResult<DestinationWriteStatus> {
    write_table_rows_inner(destination, schema, Some(batch_id), rows).await
}

/// Invokes [`Destination::write_table_rows`] with explicit copy metadata.
async fn write_table_rows_inner<D: Destination>(
    destination: &D,
    schema: &ReplicatedTableSchema,
    batch_id: Option<TableCopyBatchId>,
    rows: Vec<TableRow>,
) -> EtlResult<DestinationWriteStatus> {
    let (async_result, pending_result) = WriteTableRowsResult::new(());
    Destination::write_table_rows(destination, schema, batch_id, rows, async_result).await?;

    pending_result.await.into_result()
}
