use etl_postgres::types::TableId;
use std::future::Future;
use tokio_postgres::types::PgLsn;

use crate::error::EtlResult;
use crate::types::{Event, TableRow};

/// Trait for systems that can receive replicated data from ETL pipelines.
///
/// [`Destination`] implementations define how replicated data is written to target systems.
/// The trait supports both bulk operations for initial table synchronization and streaming
/// operations for real-time replication events.
///
/// # Idempotency
///
/// Implementations should ensure idempotent operations where possible, as the ETL system
/// may retry failed operations. The destination should handle concurrent writes safely
/// when multiple table sync workers are active.
///
/// The trait also provides an optional [`Destination::shutdown`] method with a default no-op
/// implementation. Override this method if your destination requires cleanup or bookkeeping
/// when the pipeline shuts down.
pub trait Destination {
    /// Returns the name of the destination.
    fn name() -> &'static str;

    /// Propagates the shutdown signal to the destination.
    ///
    /// Override this method if the destination needs to perform cleanup or bookkeeping
    /// when the pipeline shuts down. The default implementation is a no-op.
    fn shutdown(&self) -> impl Future<Output = EtlResult<()>> + Send {
        async { Ok(()) }
    }

    /// Truncates all data in the specified table.
    ///
    /// This operation is called during initial table synchronization to ensure the
    /// destination table starts from a clean state before bulk loading. Truncation is
    /// best-effort and may be skipped if table metadata is missing, and callers may continue
    /// after a truncation error.
    fn truncate_table(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes a batch of table rows to the destination.
    ///
    /// This method is used during initial table synchronization to bulk load existing
    /// data. Rows are provided as [`TableRow`] instances with typed cell values.
    /// Implementations should optimize batch insertion performance while maintaining
    /// data consistency.
    ///
    /// Note that this method will be called even if the source table has no data. In that case it
    /// will supply an empty list of rows. This is done by design so that the destination can already
    /// prepare the initial tables before starting streaming.
    fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes streaming replication events to the destination.
    ///
    /// This method handles real-time changes from the Postgres replication stream.
    /// Events include inserts, updates, deletes, and transaction boundaries. The
    /// destination should process events in order, this is required to maintain data consistency.
    ///
    /// Event ordering within a transaction is guaranteed, and transactions are ordered according to
    /// their commit time.
    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = EtlResult<()>> + Send;

    /// Returns the confirmed flush position and in-flight write status for
    /// asynchronous destinations.
    ///
    /// The returned tuple contains:
    /// - [`PgLsn`]: The END LSN of the last fully committed transaction that has been
    ///   durably processed by the destination. This MUST be a `CommitEvent.end_lsn`
    ///   value that was previously delivered via [`Destination::write_events`].
    ///   Returning any other value (mid-transaction LSN, fabricated value) will cause
    ///   incorrect WAL retention and potential data loss on restart.
    /// - [`bool`]: Whether the destination currently has in-flight (unconfirmed) writes.
    ///   When `false` and no new data is flowing, the system can safely advance the
    ///   flush LSN to prevent WAL buildup.
    ///
    /// Returns [`None`] by default, which preserves the existing auto-advance behavior
    /// where `flush_lsn` is advanced immediately after [`Destination::write_events`] completes.
    ///
    /// # Contract
    ///
    /// - The LSN MUST only increase monotonically across calls.
    /// - The LSN MUST be a `CommitEvent.end_lsn` previously delivered via `write_events()`.
    /// - When `has_inflight_writes` is `false`, the destination guarantees no data
    ///   is pending durable processing.
    /// - Violations are caught by `debug_assert!` in development and capped in production.
    fn confirmed_flush_lsn(&self) -> Option<(PgLsn, bool)> {
        None
    }
}
