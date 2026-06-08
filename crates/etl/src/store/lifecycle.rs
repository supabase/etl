//! Lifecycle cleanup store capability.
//!
//! Lifecycle cleanup operations are coordinated mutations over table state,
//! durable progress, versioned schemas, destination metadata, and in-memory
//! caches. Each operation represents a complete lifecycle intent so callers do
//! not compose ad hoc deletion steps and accidentally create an unsupported
//! combination.

use std::future::Future;

use crate::{error::EtlResult, types::TableId};

/// Cleanup operation for ETL lifecycle state.
///
/// Each variant describes one supported cleanup intent:
/// - [`TableLifecycleCleanup::TableCopyRestart`] prepares a single table for a
///   fresh copy after its destination object has already been dropped.
/// - [`TableLifecycleCleanup::PipelineResync`] prepares the whole pipeline for
///   resync after the apply worker slot lineage changes.
/// - [`TableLifecycleCleanup::TableRemoval`] removes all ETL-owned state for a
///   table that is no longer part of the publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLifecycleCleanup {
    /// Clear state that belongs to a previous table copy attempt.
    ///
    /// Deletes destination table metadata, stored table schemas, and durable
    /// table-sync progress for `table_id`. Preserves the table state entry so
    /// the table sync worker can continue driving the copy lifecycle.
    ///
    /// This operation must run only after the destination object has been
    /// dropped successfully, because the destination metadata is what lets the
    /// destination locate that object.
    TableCopyRestart {
        /// The table whose copy state should be cleared.
        table_id: TableId,
    },
    /// Reset pipeline table state after the apply slot lineage changes.
    ///
    /// Resets every current table state to
    /// [`crate::state::TableState::Init`] and deletes durable apply-worker
    /// progress. Preserves table schemas, destination table metadata, and
    /// durable table-sync progress so each restarted table sync can drop any
    /// existing destination object before clearing its own copy state.
    PipelineResync,
    /// Delete all ETL state for a table removed from the publication.
    ///
    /// Deletes table state history, stored table schemas, destination table
    /// metadata, and durable table-sync progress for `table_id`. Does not drop
    /// or otherwise modify the destination object, since publication removal is
    /// an ETL state cleanup rather than a destination data deletion.
    TableRemoval {
        /// The removed table whose ETL state should be deleted.
        table_id: TableId,
    },
}

/// Lifecycle cleanup operations across state and schema stores.
///
/// Provides cleanup primitives for copy restarts, reset-to-resync recovery, and
/// publication removal. Implementations must keep persistent state and
/// in-memory caches consistent for each cleanup operation.
pub trait TableLifecycleStore: Sync {
    /// Cleans up ETL lifecycle state according to `cleanup`.
    ///
    /// This is the single implementation point for lifecycle cleanup semantics.
    /// Prefer the focused convenience methods below at call sites unless the
    /// caller needs to choose the cleanup operation dynamically. Returns the
    /// number of table state entries affected by the cleanup:
    /// - `0` for table-copy cleanup because table state is preserved.
    /// - The number of reset table states for pipeline resync.
    /// - `1` when a table removal deletes an existing table state, otherwise
    ///   `0`.
    fn cleanup_lifecycle_state(
        &self,
        cleanup: TableLifecycleCleanup,
    ) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Clears stored table-copy state for `table_id`.
    ///
    /// Removes destination table metadata, all stored table schemas, and
    /// durable table-sync progress while preserving the table state. This is
    /// used after the destination object has been dropped and
    /// before a fresh `0/0` table-copy schema is stored. This is a convenience
    /// wrapper around [`TableLifecycleCleanup::TableCopyRestart`].
    fn clear_table_copy_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        async move {
            self.cleanup_lifecycle_state(TableLifecycleCleanup::TableCopyRestart { table_id })
                .await?;

            Ok(())
        }
    }

    /// Resets all current table states to [`crate::state::TableState::Init`]
    /// for resync.
    ///
    /// Deletes durable apply-worker progress because the apply slot lineage is
    /// being replaced. Preserves table schemas, destination table metadata, and
    /// durable table-sync progress so each table-sync worker can drop any
    /// existing destination object before clearing its own copy state.
    ///
    /// Returns the number of table states reset. This is a convenience wrapper
    /// around [`TableLifecycleCleanup::PipelineResync`].
    fn reset_table_states_for_resync(&self) -> impl Future<Output = EtlResult<usize>> + Send {
        async move { self.cleanup_lifecycle_state(TableLifecycleCleanup::PipelineResync).await }
    }

    /// Deletes all stored state for `table_id` for the current pipeline.
    ///
    /// Removes table state (including history), table schemas, destination
    /// table metadata, and durable table-sync progress. This must NOT drop or
    /// modify the actual destination table.
    ///
    /// Intended for use when a table is removed from the publication. This is a
    /// convenience wrapper around [`TableLifecycleCleanup::TableRemoval`].
    fn delete_table_pipeline_state(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        async move {
            self.cleanup_lifecycle_state(TableLifecycleCleanup::TableRemoval { table_id }).await?;

            Ok(())
        }
    }
}
