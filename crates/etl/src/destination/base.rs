use std::future::Future;

use crate::{
    data::TableRow,
    destination::{
        DropTableForCopyResult, FinishTableCopyResult, StreamingDurabilityMode,
        TableCopyDurabilityMode, TrackedEventsBatch, TrackedTableRowsBatch, WriteEventsResult,
        WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::Event,
    schema::ReplicatedTableSchema,
};

/// Trait for systems that can receive replicated data from ETL pipelines.
///
/// [`Destination`] implementations define how replicated data is written to
/// target systems. The trait supports both bulk operations for initial table
/// synchronization and streaming operations for real-time replication events.
///
/// The interface is intentionally small and generic. ETL provides ordered data
/// plus minimal coordination hooks, and each destination is free to choose its
/// own execution model, such as inline writes, actors, queues, or spawned
/// tasks.
///
/// ETL provides at-least-once delivery from the last durable checkpoint. After
/// failures, it may replay streaming events or table-copy rows that were
/// already submitted to the destination.
///
/// By default, a successful destination write is also the durable-completion
/// signal: when [`Destination::write_events`] or
/// [`Destination::write_table_rows`] completes, ETL may checkpoint or advance
/// the submitted work.
///
/// Destinations that opt into deferred durability split acceptance from durable
/// completion. For streaming, [`Destination::write_tracked_events`] may
/// complete when the destination has accepted ownership of the tracked batch,
/// but the destination must later report durable completion through the batch's
/// durability reporter. For table copy,
/// [`Destination::write_tracked_table_rows`] may complete on acceptance, but
/// [`Destination::finish_table_copy`] must not complete until every accepted
/// batch for the copy attempt is durable.
///
/// Implementations must make write replays safe through idempotent writes,
/// durable offset or batch filtering, or by resetting destination state before
/// retrying a table-copy attempt.
///
/// A destination must not report buffered but uncommitted data as durable. ETL
/// may also call destination methods in parallel under some circumstances, so
/// implementations must be safe for concurrent use.
pub trait Destination {
    /// Returns the name of the destination.
    fn name() -> &'static str;

    /// Propagates the shutdown signal to the destination.
    ///
    /// Override this method if the destination needs cleanup or bookkeeping
    /// during shutdown. Background streaming destinations should use it to
    /// stop writer loops and drain or drop outstanding work. ETL calls this
    /// method at most once for a destination instance, after it has stopped
    /// submitting new work. The default implementation is a no-op.
    fn shutdown(&self) -> impl Future<Output = EtlResult<()>> + Send {
        async { Ok(()) }
    }

    /// Initializes destination state after pipeline startup state is prepared.
    ///
    /// ETL calls this hook during pipeline startup, after destination table
    /// metadata, table schemas, and table states have been loaded and tables
    /// removed from the publication have been purged from ETL-owned state. It
    /// runs before workers begin submitting table-specific writes.
    /// Destinations can use it to reconcile durable destination state with
    /// their physical objects after a process restart. The default
    /// implementation is a no-op.
    fn startup(&self) -> impl Future<Output = EtlResult<()>> + Send {
        async { Ok(()) }
    }

    /// Returns the destination's streaming durability mode.
    ///
    /// The default is immediate durability, which preserves the original ETL
    /// contract: the async result from [`Destination::write_events`] is the
    /// point where upstream may treat the batch as durable.
    ///
    /// Deferred destinations split write acceptance from durable completion and
    /// must implement [`Destination::write_tracked_events`].
    fn streaming_durability_mode(&self) -> StreamingDurabilityMode {
        StreamingDurabilityMode::Immediate
    }

    /// Returns the destination's table-copy durability mode.
    ///
    /// The default is immediate durability, which preserves the original ETL
    /// contract: each table-copy async result is both accepted and durable.
    ///
    /// Deferred destinations split batch acceptance from table-copy completion
    /// and must implement [`Destination::write_tracked_table_rows`] plus
    /// [`Destination::finish_table_copy`].
    fn table_copy_durability_mode(&self) -> TableCopyDurabilityMode {
        TableCopyDurabilityMode::Immediate
    }

    /// Drops destination objects before restarting a table copy.
    ///
    /// This operation is called when table synchronization intentionally
    /// restarts from scratch. Implementations should remove the destination
    /// object and any destination-private replay markers for the table so the
    /// next copy can recreate it from the fresh source schema.
    ///
    /// The supplied schema describes the previously known destination table and
    /// exists only so the destination can locate what should be removed. ETL
    /// clears its own destination metadata and stored schemas only after this
    /// result completes successfully.
    fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes a batch of table rows to the destination.
    ///
    /// This method is used during initial table synchronization to bulk load
    /// existing data. Rows are provided as [`TableRow`] instances with
    /// typed cell values. ETL may call this method multiple times with
    /// different batches, including in parallel with other destination
    /// work.
    ///
    /// This method is called even if the source table has no data, so the
    /// destination can prepare its initial state before streaming begins.
    /// ETL does not impose a meaningful ordering requirement on these row
    /// batches; it just provides the data that should be written for the
    /// initial snapshot.
    ///
    /// Implementations report asynchronous completion through `async_result`.
    /// The method return value is reserved for immediate dispatch/setup
    /// failures before the work has been accepted.
    ///
    /// ETL waits for each table-copy batch to finish before reading the next
    /// batch for the same copy partition. When multiple copy workers are
    /// configured, this method can still run concurrently across different
    /// partitions, so the asynchronous result is mostly an API consistency tool
    /// rather than a way to queue all copy batches and wait at the end.
    ///
    /// This immediate waiting is intentional: it preserves backpressure and
    /// avoids accumulating too many in-flight row batches in memory.
    fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes table-copy rows with ETL-assigned tracking metadata.
    ///
    /// Immediate destinations do not need to override this method. The default
    /// implementation delegates to [`Destination::write_table_rows`]. Deferred
    /// table-copy destinations can override this method and treat the async
    /// result as acceptance for the current copy attempt rather than durable
    /// completion.
    fn write_tracked_table_rows(
        &self,
        batch: TrackedTableRowsBatch,
        async_result: WriteTableRowsResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send
    where
        Self: Sync,
    {
        async move {
            self.write_table_rows(&batch.replicated_table_schema, batch.table_rows, async_result)
                .await
        }
    }

    /// Finishes a deferred table-copy attempt.
    ///
    /// ETL calls this method only when
    /// [`Destination::table_copy_durability_mode`] returns
    /// [`TableCopyDurabilityMode::Deferred`]. The async result must be
    /// completed only after every accepted row for the current copy attempt is
    /// durable. The default fails closed so a destination cannot opt into
    /// deferred copy semantics without a completion barrier.
    fn finish_table_copy(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        _async_result: FinishTableCopyResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        async {
            Err(etl_error!(
                ErrorKind::DestinationError,
                "Deferred table-copy durability requested but no finish barrier was implemented"
            ))
        }
    }

    /// Writes streaming replication events to the destination.
    ///
    /// This method handles real-time changes from the Postgres replication
    /// stream. Events include relation notifications, inserts, updates,
    /// deletes, truncates, and transaction boundaries. ETL may call this
    /// method multiple times with different streaming batches.
    ///
    /// Streaming batches are built from size and time limits, not schema
    /// change boundaries. A single call may contain zero, one, or many
    /// [`Event::Relation`] events, including multiple schema changes for the
    /// same table. Implementations that apply destination DDL should process
    /// events in order and update their active table schema each time a
    /// relation event appears.
    ///
    /// The main ordering guarantee is per table: ETL preserves the required
    /// order for streaming operations on the same table.
    ///
    /// Implementations report asynchronous completion through `async_result`.
    /// The method return value is reserved for immediate dispatch/setup
    /// failures before the work has been accepted.
    ///
    /// This lets ETL distinguish synchronous dispatch errors from asynchronous
    /// flush completion. This is also the path where ETL gains real
    /// overlap: once dispatch succeeds, the apply loop may continue
    /// processing while the destination finishes the current batch. ETL still
    /// will not hand the destination the next streaming batch until the
    /// previous `async_result` has been completed.
    ///
    /// Async implementations that offload work should coordinate `async_result`
    /// with [`Destination::shutdown`]. ETL calls [`Destination::shutdown`]
    /// at most once and only after it has stopped submitting new work. If
    /// the apply loop has already gone away, sending the result will fail
    /// and may be treated as an implicit cancellation.
    ///
    /// During the initial copy stage, transaction boundaries are not a stable
    /// global invariant across all tables. A source transaction may be
    /// split across multiple streaming deliveries as some tables are
    /// already ready for streaming and others are still being copied. In
    /// practice, destinations should rely on per-table event ordering and
    /// not assume that `begin`/`commit` boundaries always describe a
    /// complete all-tables transaction until initial copy has fully
    /// finished.
    ///
    /// Each data-bearing [`Event`] also carries its own
    /// [`ReplicatedTableSchema`], so destinations can react to the correct
    /// schema version for that specific change.
    fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes streaming events with ETL-assigned tracking metadata.
    ///
    /// Immediate destinations do not need to override this method. The default
    /// implementation delegates to [`Destination::write_events`]. Deferred
    /// streaming destinations override this method so they can later report the
    /// batch id through the reporter carried by [`TrackedEventsBatch`].
    fn write_tracked_events(
        &self,
        batch: TrackedEventsBatch,
        async_result: WriteEventsResult<()>,
    ) -> impl Future<Output = EtlResult<()>> + Send
    where
        Self: Sync,
    {
        async move { self.write_events(batch.events, async_result).await }
    }
}
