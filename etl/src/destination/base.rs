use etl_config::shared::SchemaCreationMode;
use etl_postgres::types::{TableId, TableSchema};
use std::future::Future;
use std::sync::Arc;

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
    /// Returns the name of the destination.
    fn name() -> &'static str;

    /// Creates the destination schema for a table.
    ///
    /// Implementations should ensure the table (and any related views or namespaces)
    /// exists and matches the provided schema. This method is invoked during initial
    /// synchronization before any data is written.
    fn create_table_schema(
        &self,
        table_schema: Arc<TableSchema>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Truncates all data in the specified table.
    ///
    /// This operation is called during initial table synchronization to ensure the
    /// destination table starts from a clean state before bulk loading. The operation
    /// should be atomic and handle cases where the table may not exist.
    ///
    /// The implementation should assume that when truncation is called, the table might not be
    /// present since truncation could be called after a failure that happened before the table copy
    /// was started.
    fn truncate_table(
        &self,
        table_id: TableId,
        schema_creation_mode: SchemaCreationMode,
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
        table_id: TableId,
        table_rows: Vec<TableRow>,
        schema_creation_mode: SchemaCreationMode,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Writes streaming replication events to the destination.
    ///
    /// This method handles real-time changes from the Postgres replication stream.
    /// Events include inserts, updates, deletes, and transaction boundaries. The
    /// destination should process events in order, this is required to maintain data consistency.
    ///
    /// Event ordering within a transaction is guaranteed, and transactions are ordered according to
    /// their commit time.
    fn write_events(
        &self,
        events: Vec<Event>,
        schema_creation_mode: SchemaCreationMode,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
