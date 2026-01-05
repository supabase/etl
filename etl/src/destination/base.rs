use etl_postgres::types::ReplicatedTableSchema;
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
    /// destination table starts from a clean state before bulk loading. The operation
    /// should be atomic and handle cases where the table and its states may not exist, since
    /// truncation is unconditionally called before a table is copied.
    fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> impl Future<Output = EtlResult<()>> + Send;

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
        replicated_table_schema: &ReplicatedTableSchema,
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
    ///
    /// Each [`Event`] that involves data changes (Insert/Update/Delete) contains its own
    /// [`ReplicatedTableSchema`] which destinations can use to understand the schema for that event.
    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = EtlResult<()>> + Send;
}
