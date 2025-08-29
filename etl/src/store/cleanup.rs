use etl_postgres::types::TableId;
use std::future::Future;

use crate::error::EtlResult;

/// Combined maintenance operations across state and schema stores.
///
/// Provides atomic cleanup primitives that affect both replication state
/// and schema-related data for a specific table. Implementations should
/// ensure consistency across in-memory caches and the persistent store.
pub trait CleanupStore {
    /// Deletes all stored state for `table_id` for the current pipeline.
    ///
    /// Removes replication state (including history), table schemas, and
    /// table mappings. This must NOT drop or modify the actual destination table.
    ///
    /// Intended for use when a table is removed from the publication.
    fn cleanup_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;
}
