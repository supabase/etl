use std::future::Future;

use crate::{
    data::TableRow,
    destination::{
        DropTableForCopyResult, TableCopyBatchId, WriteEventsDurability, WriteEventsResult,
        WriteTableRowsResult,
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

    /// Finishes destination-owned work and releases resources during shutdown.
    ///
    /// Called after workers complete successfully; stop writer loops and finish
    /// owned work here. Worker failures skip this hook, so resources also need
    /// drop-based cancellation or explicit owner cleanup. Accept requested task
    /// cancellations; propagate panics and task errors without awaiting
    /// remaining tasks. The default implementation is a no-op.
    fn shutdown(&self) -> impl Future<Output = EtlResult<()>> + Send {
        async { Ok(()) }
    }

    /// Initializes destination state after pipeline startup state is prepared.
    ///
    /// ETL calls this hook during pipeline startup, after destination table
    /// metadata, table schemas, and table states have been loaded and tables
    /// removed from the publication have been purged from ETL-owned state. It
    /// runs before workers begin submitting table-specific writes. Destinations
    /// can use it to reconcile durable destination state with their physical
    /// objects after a process restart. The default implementation is a no-op.
    fn startup(&self) -> impl Future<Output = EtlResult<()>> + Send {
        async { Ok(()) }
    }

    /// Drops destination objects before restarting a table copy.
    ///
    /// This operation is called when table synchronization intentionally
    /// restarts from scratch. Implementations should remove the destination
    /// object and any destination-private replay markers for the table so the
    /// next copy can recreate it from the fresh source schema.
    ///
    /// Before reporting success, the destination must ensure that writes
    /// accepted during an earlier copy attempt can no longer modify the
    /// destination table. It may do this by waiting for them, cancelling them,
    /// or rejecting them as stale.
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
    /// existing data. Nonempty row batches carry a [`TableCopyBatchId`] as an
    /// opaque idempotency key. ETL may call this method multiple times with
    /// different batches, including in parallel with other destination work.
    ///
    /// This method is called with an empty row vector and no batch ID even if
    /// the source table has no data, so the destination can prepare its initial
    /// state before streaming begins. A nonempty row vector always has a batch
    /// ID, and an empty row vector never has one. ETL does not impose a
    /// meaningful ordering requirement on these row batches; it just provides
    /// the data that should be written for the initial snapshot.
    ///
    /// Implementations report asynchronous write status through `async_result`.
    /// The method return value is reserved for immediate dispatch/setup
    /// failures before the work has been accepted.
    ///
    /// Copy partitions run in separate tasks, so writes may run inline or be
    /// offloaded. Each partition awaits this method and its `async_result`
    /// before reading another batch, unless cancelled. Parallelism comes from
    /// other copy workers; `Accepted` permits progress without proving
    /// durability.
    ///
    /// [`crate::destination::DestinationWriteStatus::Durable`] means the batch
    /// and all earlier accepted writes it covers are durable.
    /// [`crate::destination::DestinationWriteStatus::Accepted`] transfers
    /// ownership of the batch to the destination and permits ETL to continue
    /// copying, but does not permit ETL to complete the table copy.
    ///
    /// If any row batch returns
    /// [`crate::destination::DestinationWriteStatus::Accepted`], ETL calls this
    /// method once more with an empty row vector and no batch ID after every
    /// copy worker has finished and the source copy transaction has committed.
    /// That call is a table-wide durability barrier: it must return
    /// [`crate::destination::DestinationWriteStatus::Durable`] and cumulatively
    /// cover every accepted write for the table copy. Returning
    /// [`crate::destination::DestinationWriteStatus::Accepted`] from the
    /// barrier fails the copy without advancing its durable state. Empty and
    /// skipped tables also receive a finish write so the destination can
    /// prepare their initial state.
    ///
    /// Initial copy may be interrupted at any await point in this method,
    /// including the final empty call. The incomplete copy restarts from
    /// scratch, so cancellation need not finish the current batch. Dropping
    /// the method future does not stop offloaded or native work; the
    /// destination remains responsible for that work and the reset
    /// guarantees of [`Destination::drop_table_for_copy`].
    ///
    /// Awaiting each result before requesting the next batch bounds ETL-owned
    /// row batches per copy partition. A deferred destination must separately
    /// bound its accepted-but-not-durable work and delay reporting
    /// [`crate::destination::DestinationWriteStatus::Accepted`] until it has
    /// reserved ownership and capacity for the batch.
    fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes streaming replication events to the destination.
    ///
    /// This method handles real-time changes from the Postgres replication
    /// stream. Events include relation notifications, inserts, updates,
    /// deletes, truncates, and transaction boundaries. ETL may call this method
    /// multiple times with different streaming batches.
    ///
    /// Streaming batches are built from size and time limits, not schema change
    /// boundaries. A single call may contain zero, one, or many
    /// [`Event::Relation`] events, including multiple schema changes for the
    /// same table or repeated notifications for an unchanged schema. Process
    /// them in order: compare the snapshot and replication mask with applied
    /// metadata before deciding whether destination DDL is needed. See
    /// [`crate::event::RelationEvent`] for snapshot and replay semantics.
    ///
    /// The main ordering guarantee is per table: ETL preserves the required
    /// order for streaming operations on the same table.
    ///
    /// The apply loop awaits this method directly, so implementations should
    /// dispatch long-running writes to owned tasks or queues and return
    /// promptly. The loop can then continue processing WAL and observing
    /// shutdown while it polls `async_result` separately. Performing the write
    /// inline stalls that loop until the method returns, even if the write
    /// yields to the async runtime. Shutdown does not cancel the method call;
    /// it waits for dispatch and pending write results to finish.
    ///
    /// Implementations report asynchronous write status through `async_result`.
    /// The method return value is reserved for immediate dispatch/setup
    /// failures before the work has been accepted.
    ///
    /// [`crate::destination::DestinationWriteStatus::Durable`] means this write
    /// and all earlier accepted writes in the same ordered apply-loop stream
    /// are durable according to the destination contract. ETL may persist a
    /// checkpoint covering the write only after observing this status.
    ///
    /// [`crate::destination::DestinationWriteStatus::Accepted`] means the
    /// destination accepted ownership of the write, but ETL must not advance
    /// its flushed progress or persisted checkpoint for it yet.
    ///
    /// [`WriteEventsDurability::MayDefer`] permits either status, while
    /// [`WriteEventsDurability::RequireDurable`] requires `Durable` and its
    /// cumulative guarantee for all earlier accepted writes in the same
    /// apply-loop stream.
    ///
    /// ETL may call this method with an empty event vector and
    /// [`WriteEventsDurability::RequireDurable`]. The empty vector carries no
    /// new replication events, but the call may flush or wait for earlier
    /// accepted work and must not complete until all writes covered by its
    /// ordering state are durable. It may return `Durable` immediately if no
    /// such work remains, and it may prove durability over a stronger scope
    /// than the originating apply-loop stream. ETL does not issue empty
    /// [`WriteEventsDurability::MayDefer`] writes.
    ///
    /// ETL keeps at most one streaming write result pending while it continues
    /// building the next batch. If a result completes as
    /// [`crate::destination::DestinationWriteStatus::Accepted`], ETL carries
    /// that write's commit end LSN into the next streaming write instead of
    /// advancing the last flush LSN or persisted checkpoint. A later
    /// [`crate::destination::DestinationWriteStatus::Durable`] result is
    /// cumulative: it must mean that later write and all earlier `Accepted`
    /// writes in the same apply-loop stream are durable.
    ///
    /// When a keepalive observes an accepted commit with no open transaction,
    /// buffered events, or pending write result, ETL may issue an empty
    /// required-durability write. It may buffer subsequent events while that
    /// barrier is pending, but cannot dispatch them until the barrier
    /// completes.
    ///
    /// If no later streaming write is dispatched before shutdown, ETL normally
    /// exits without checkpointing accepted-but-not-durable work. Restart then
    /// replays from the last persisted checkpoint. A terminal table-sync
    /// catchup may issue the empty required-durability barrier described above
    /// instead.
    ///
    /// Async implementations that offload work should coordinate `async_result`
    /// with [`Destination::shutdown`]. ETL calls [`Destination::shutdown`] at
    /// most once and only after it has stopped submitting new work. If the
    /// apply loop has already gone away, sending the result will fail and may
    /// be treated as an implicit cancellation.
    ///
    /// During the initial copy stage, transaction boundaries are not a stable
    /// global invariant across all tables. A source transaction may be split
    /// across multiple streaming deliveries as some tables are already ready
    /// for streaming and others are still being copied. In practice,
    /// destinations should rely on per-table event ordering and not assume that
    /// `begin`/`commit` boundaries always describe a complete all-tables
    /// transaction until initial copy has fully finished.
    ///
    /// Each data-bearing [`Event`] also carries its own
    /// [`ReplicatedTableSchema`], so destinations can react to the correct
    /// schema version for that specific change.
    fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
