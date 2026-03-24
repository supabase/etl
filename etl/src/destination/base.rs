use etl_postgres::types::TableId;
use std::future::Future;

use crate::destination::async_result::{
    TruncateTableResult, WriteEventsResult, WriteTableRowsResult,
};
use crate::error::EtlResult;
use crate::types::{Event, TableRow};

/// Trait for systems that can receive replicated data from ETL pipelines.
///
/// [`Destination`] implementations define how replicated data is written to target systems.
/// The trait supports both bulk operations for initial table synchronization and streaming
/// operations for real-time replication events.
///
/// The interface is intentionally small and generic. ETL provides ordered data plus minimal
/// coordination hooks, and each destination is free to choose its own execution model, such as
/// inline writes, actors, queues, or spawned tasks.
///
/// ETL is at-least-once, so destinations must tolerate duplicate writes. ETL may also call
/// destination methods in parallel under some circumstances, so implementations must be safe for
/// concurrent use.
pub trait Destination {
    /// Returns the name of the destination.
    fn name() -> &'static str;

    /// Propagates the shutdown signal to the destination.
    ///
    /// Override this method if the destination needs cleanup or bookkeeping during shutdown.
    /// Background streaming destinations should use it to stop writer loops and drain or drop
    /// outstanding work. ETL calls this method at most once for a destination instance, after it
    /// has stopped submitting new work. The default implementation is a no-op.
    fn shutdown(&self) -> impl Future<Output = EtlResult<()>> + Send {
        async { Ok(()) }
    }

    /// Truncates all data in the specified table.
    ///
    /// This operation is called during initial table synchronization to ensure the
    /// destination table starts from a clean state before bulk loading. Truncation is
    /// best-effort and may be skipped if table metadata is missing, and callers may continue
    /// after a truncation error.
    ///
    /// Implementations complete `async_result` when truncation is actually done. ETL still waits
    /// for that result immediately before continuing. The asynchronous result exists mainly to
    /// keep the destination interface uniform across methods, not to let ETL overlap more work
    /// with truncation.
    fn truncate_table(
        &self,
        table_id: TableId,
        async_result: TruncateTableResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes a batch of table rows to the destination.
    ///
    /// This method is used during initial table synchronization to bulk load existing data.
    /// Rows are provided as [`TableRow`] instances with typed cell values. ETL may call this
    /// method multiple times with different batches, including in parallel with other destination
    /// work.
    ///
    /// This method is called even if the source table has no data, so the destination can prepare
    /// its initial state before streaming begins. ETL does not impose a meaningful ordering
    /// requirement on these row batches; it just provides the data that should be written for the
    /// initial snapshot.
    ///
    /// Implementations report asynchronous completion through `async_result`. The method return
    /// value is reserved for immediate dispatch/setup failures before the work has been accepted.
    ///
    /// ETL still waits for each table-copy batch to finish before reading the next batch for the
    /// same copy partition. For non-parallel table copy, that means a new batch is requested only
    /// after the previous result completes. For parallel table copy, ETL already invokes this
    /// method concurrently across partitions, so the asynchronous result is mostly an API
    /// consistency tool rather than a way to queue all copy batches and wait at the end.
    ///
    /// This immediate waiting is intentional: it preserves backpressure and avoids accumulating too
    /// many in-flight row batches in memory.
    fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes streaming replication events to the destination.
    ///
    /// This method handles real-time changes from the Postgres replication stream. Events include
    /// inserts, updates, deletes, and transaction boundaries. ETL may call this method multiple
    /// times with different streaming batches.
    ///
    /// The main ordering guarantee is per table: ETL preserves the required order for streaming
    /// operations on the same table.
    ///
    /// Implementations report asynchronous completion through `async_result`. The method return
    /// value is reserved for immediate dispatch/setup failures before the work has been accepted.
    ///
    /// This lets ETL distinguish synchronous dispatch errors from asynchronous flush completion.
    /// This is also the path where ETL gains real overlap: once dispatch succeeds, the apply loop
    /// may continue processing while the destination finishes the current batch. ETL still will not
    /// hand the destination the next streaming batch until the previous `async_result` has been
    /// completed.
    ///
    /// Async implementations that offload work should coordinate `async_result` with
    /// [`Destination::shutdown`]. ETL calls [`Destination::shutdown`] at most once and only after
    /// it has stopped submitting new work. If the apply loop has already gone away, sending the
    /// result will fail and may be treated as an implicit cancellation.
    ///
    /// During the initial copy phase, transaction boundaries are not a stable global invariant
    /// across all tables. A source transaction may be split across multiple streaming deliveries as
    /// some tables are already ready for streaming and others are still being copied. In practice,
    /// destinations should rely on per-table event ordering and not assume that `begin`/`commit`
    /// boundaries always describe a complete all-tables transaction until initial copy has fully
    /// finished.
    fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
