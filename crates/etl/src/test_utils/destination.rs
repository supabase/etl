//! Test helpers for invoking destination trait methods.

use crate::{
    data::TableRow,
    destination::{
        Destination, DestinationWriteStatus, WriteEventsDurability, WriteEventsResult,
        WriteTableRowsResult,
    },
    error::EtlResult,
    event::Event,
    schema::ReplicatedTableSchema,
};

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
    let (async_result, pending_result) = WriteTableRowsResult::new(());
    Destination::write_table_rows(destination, schema, rows, async_result).await?;

    pending_result.await.into_result()
}
