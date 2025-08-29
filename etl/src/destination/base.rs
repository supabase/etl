use etl_postgres::types::TableId;
use std::future::Future;

use crate::error::EtlResult;
use crate::types::{Event, TableRow};

/// Trait for systems that can receive replicated data from ETL pipelines.
///
/// [`Destination`] implementations define how replicated data is written to target systems.
/// The trait supports both bulk operations for initial table synchronization and streaming
/// operations for real-time replication events.
///
/// Implementations should ensure idempotent operations where possible, as the ETL system
/// may retry failed operations. The destination should handle concurrent writes safely
/// when multiple table sync workers are active.
pub trait Destination {
    /// Truncates all data in the specified table.
    ///
    /// This operation is called during initial table synchronization to ensure the
    /// destination table starts from a clean state before bulk loading. The operation
    /// should be atomic and handle cases where the table may not exist.
    ///
    /// The implementation should assume that when truncation is called, the table might not be
    /// present since truncation could be called after a failure that happened before the table copy
    /// was started.
    fn truncate_table(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes a batch of table rows to the destination.
    ///
    /// This method is used during initial table synchronization to bulk load existing
    /// data. Rows are provided as [`TableRow`] instances with typed cell values.
    /// Implementations should optimize for batch insertion performance while maintaining
    /// data consistency.
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
}
