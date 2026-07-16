//! Table state lifecycle store capability.
//!
//! Table state lifecycle operations are coordinated mutations over table state,
//! durable progress, versioned schemas, destination metadata, and store caches.
//! Each operation represents a supported lifecycle intent so callers do not
//! need to compose ad hoc deletion steps.

use std::future::Future;

use crate::{error::EtlResult, schema::TableId};

/// Lifecycle operation for ETL table state.
///
/// Each variant describes one supported lifecycle intent:
/// - [`TableStateOperation::PrepareForCopy`] prepares a single table for a
///   fresh copy after its destination table has been dropped.
/// - [`TableStateOperation::ResetForResync`] resets the whole pipeline for a
///   fresh synchronization pass.
/// - [`TableStateOperation::Delete`] removes all ETL-owned state for a table
///   that is no longer part of the publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableStateOperation {
    /// Prepare a table state entry for a fresh table copy.
    ///
    /// Deletes destination table metadata, stored table schemas, and durable
    /// table-sync progress for `table_id`. Preserves the table state entry so
    /// the table sync worker can continue driving the copy lifecycle.
    ///
    /// This operation must run only after the destination object has been
    /// dropped successfully, because the destination metadata is what lets the
    /// destination locate that object.
    PrepareForCopy {
        /// The table whose copy state should be prepared.
        table_id: TableId,
    },
    /// Reset current table states for a fresh synchronization pass.
    ///
    /// Resets every current table state to [`crate::store::TableState::Init`]
    /// and deletes durable apply-worker progress. Preserves table schemas,
    /// destination table metadata, and durable table-sync progress so each
    /// restarted table sync can drop any existing destination object before
    /// clearing its own copy state.
    ResetForResync,
    /// Delete all ETL-owned state for a table removed from the publication.
    ///
    /// Deletes table state history, stored table schemas, destination table
    /// metadata, and durable table-sync progress for `table_id`. Does not drop
    /// or otherwise modify the destination object, since publication removal is
    /// an ETL state deletion rather than a destination data deletion. A later
    /// re-add starts a new copy without clean-replacement semantics for that
    /// retained destination object.
    Delete {
        /// The removed table whose ETL state should be deleted.
        table_id: TableId,
    },
}

/// Lifecycle operations across state and schema stores.
///
/// Provides primitives for table-copy preparation, reset-to-resync recovery,
/// and publication removal. Implementations must keep persistent state and
/// in-memory caches consistent for each operation.
pub trait TableStateLifecycleStore: Sync {
    /// Applies a table state lifecycle `operation`.
    ///
    /// This is the single implementation point for lifecycle semantics.
    /// Prefer the focused convenience methods below at call sites unless the
    /// caller needs to choose the operation dynamically. Returns the number of
    /// table state entries affected by the operation:
    /// - `0` for copy preparation because table state is preserved.
    /// - The number of reset table states for pipeline resync.
    /// - `1` when a table removal deletes an existing table state, otherwise
    ///   `0`.
    fn apply_table_state_operation(
        &self,
        operation: TableStateOperation,
    ) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Prepares `table_id` for a fresh table copy.
    ///
    /// Removes destination table metadata, all stored table schemas, and
    /// durable table-sync progress while preserving the table state. This is
    /// used after the destination object has been dropped and
    /// before a fresh `0/0` table-copy schema is stored. This is a convenience
    /// wrapper around [`TableStateOperation::PrepareForCopy`].
    fn prepare_table_state_for_copy(
        &self,
        table_id: TableId,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        async move {
            self.apply_table_state_operation(TableStateOperation::PrepareForCopy { table_id })
                .await?;

            Ok(())
        }
    }

    /// Resets all current table states to
    /// [`crate::store::TableState::Init`] for resync.
    ///
    /// Deletes durable apply-worker progress because the apply slot lineage is
    /// being replaced. Preserves table schemas, destination table metadata, and
    /// durable table-sync progress so each table-sync worker can drop any
    /// existing destination object before clearing its own copy state.
    ///
    /// Returns the number of table states reset. This is a convenience wrapper
    /// around [`TableStateOperation::ResetForResync`].
    fn reset_table_states_for_resync(&self) -> impl Future<Output = EtlResult<usize>> + Send {
        async move { self.apply_table_state_operation(TableStateOperation::ResetForResync).await }
    }

    /// Deletes all stored ETL state for `table_id`.
    ///
    /// Removes table state (including history), table schemas, destination
    /// table metadata, and durable table-sync progress. This must NOT drop or
    /// modify the actual destination table.
    ///
    /// Intended for use when a table is removed from the publication. This is a
    /// convenience wrapper around [`TableStateOperation::Delete`].
    fn delete_table_state(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send {
        async move {
            self.apply_table_state_operation(TableStateOperation::Delete { table_id }).await?;

            Ok(())
        }
    }
}
