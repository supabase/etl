use std::future::Future;

use crate::{error::EtlResult, types::TableId};

/// Combined maintenance operations across state and schema stores.
///
/// Provides atomic table-scoped primitives that affect both replication state
/// and schema-related data. Implementations should ensure consistency across
/// in-memory caches and the persistent store.
pub trait CleanupStore {
    /// Clears stored table-copy state for `table_id`.
    ///
    /// Removes destination table metadata, all stored table schemas, and
    /// durable table-sync progress while preserving the table replication
    /// phase. This is used after the destination object has been dropped and
    /// before a fresh `0/0` table-copy schema is stored.
    fn clear_table_copy_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    /// Deletes all stored state for `table_id` for the current pipeline.
    ///
    /// Removes replication state (including history), table schemas, and
    /// destination table metadata. This must NOT drop or modify the actual
    /// destination table.
    ///
    /// Intended for use when a table is removed from the publication.
    fn delete_table_pipeline_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
