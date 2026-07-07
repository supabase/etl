use std::future::Future;

use crate::{
    data::TableRow,
    destination::{
        DropTableForCopyResult, DurabilityConfig, WriteEventsResult, WriteTableRowsResult,
    },
    error::EtlResult,
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
/// ETL is at-least-once, so destinations must tolerate duplicate writes. ETL
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

    /// Returns destination-provided durability configuration.
    ///
    /// The default configuration preserves the original immediate-durability
    /// behavior: ETL does not dispatch a second streaming batch until the
    /// previous write result completes.
    ///
    /// Destinations that return
    /// [`crate::destination::DestinationWriteStatus::Accepted`] should set
    /// [`crate::destination::DurabilityConfig::streaming_write_limits`] to
    /// allow more than one in-flight streaming write and must preserve the
    /// cumulative durability contract documented on
    /// [`Destination::write_events`].
    fn durability_config(&self) -> DurabilityConfig {
        DurabilityConfig::default()
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
    /// Implementations report asynchronous write status through
    /// `async_result`. The method return value is reserved for immediate
    /// dispatch/setup failures before the work has been accepted.
    ///
    /// [`crate::destination::DestinationWriteStatus::Durable`] means this write
    /// and all earlier accepted writes in the same ordered apply-loop stream
    /// are durable according to the destination contract. ETL may advance
    /// durable replication progress only after observing this status.
    ///
    /// [`crate::destination::DestinationWriteStatus::Accepted`] means the
    /// destination accepted ownership of the write, but ETL must not advance
    /// durable progress for it yet.
    ///
    /// Deferred destinations commonly maintain a moving durability tail. The
    /// first non-durable write is kept pending. After the destination accepts a
    /// successor write, it may complete the previous tail as
    /// [`crate::destination::DestinationWriteStatus::Accepted`] and retain the
    /// successor result as the new pending tail. Once the destination has made
    /// the current tail write and all earlier `Accepted` writes durable, it
    /// completes the current tail as
    /// [`crate::destination::DestinationWriteStatus::Durable`].
    ///
    /// The apply loop processes write results in the order the writes were
    /// dispatched. A deferred destination may return `Accepted` for earlier
    /// writes and `Durable` for a later write. That later `Durable` result is
    /// cumulative: it must mean that later write and all earlier `Accepted`
    /// writes are durable.
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
        async_result: WriteEventsResult,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
