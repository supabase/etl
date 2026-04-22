//! Shared per-table protocol state for logical replication workers.
//!
//! PostgreSQL 15+ supports column-level publication filtering, where only
//! specific columns are replicated rather than all columns. The worker that
//! currently owns a table therefore needs two pieces of protocol state to
//! decode row changes: the schema snapshot to decode against, and the
//! replication mask built from the latest `RELATION` message for that snapshot.
//!
//! The apply worker and table sync workers share this state because ownership
//! of a table can move between them over time, but at any point exactly one
//! worker owns protocol interpretation for that table. Non-owning workers skip
//! DDL, RELATION, and DML for that table and rely on the owning worker to
//! advance the shared state.
//!
//! The cache is kept in-memory because PostgreSQL guarantees that `RELATION`
//! messages are sent at the start of each connection and after schema changes
//! before any dependent DML. Persisted schemas remain the durable source of
//! truth; this cache only tracks the latest per-table decoding state needed by
//! active workers.

use std::{collections::HashMap, sync::Arc};

use etl_postgres::types::{ReplicationMask, SnapshotId, TableId};
use tokio::sync::RwLock;
use tracing::warn;

/// Shared per-table protocol state used to decode logical replication messages.
#[derive(Debug, Clone)]
pub(crate) struct SharedTableState {
    /// The latest schema snapshot known for the table.
    pub(crate) snapshot_id: SnapshotId,
    /// The replication mask for [`SharedTableState::snapshot_id`], if a
    /// `RELATION` message has already been processed for that snapshot.
    pub(crate) replication_mask: Option<ReplicationMask>,
}

/// Thread-safe container for shared per-table protocol state.
#[derive(Debug, Clone, Default)]
pub(crate) struct SharedTableCache {
    inner: Arc<RwLock<HashMap<TableId, SharedTableState>>>,
}

impl SharedTableCache {
    /// Creates a new empty [`SharedTableCache`] container.
    pub(crate) fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Returns the shared state for a table, if present.
    pub(crate) async fn get(&self, table_id: &TableId) -> Option<SharedTableState> {
        let guard = self.inner.read().await;
        guard.get(table_id).cloned()
    }

    /// Inserts or updates shared table state when the incoming snapshot is not
    /// stale.
    ///
    /// Pass `Some(mask)` to refresh the replication mask for that snapshot, or
    /// `None` to clear it when a newer schema snapshot invalidates the old
    /// `RELATION` state.
    pub(crate) async fn upsert(
        &self,
        table_id: TableId,
        snapshot_id: SnapshotId,
        replication_mask: Option<ReplicationMask>,
    ) {
        let mut guard = self.inner.write().await;
        match guard.get_mut(&table_id) {
            Some(state) if state.snapshot_id > snapshot_id => {
                warn!(
                    table_id = %table_id,
                    current_snapshot_id = %state.snapshot_id,
                    requested_snapshot_id = %snapshot_id,
                    "shared table cache received a stale snapshot update; this may indicate out-of-order protocol state"
                );

                debug_assert!(
                    state.snapshot_id <= snapshot_id,
                    "shared table cache received stale snapshot update for table {table_id}: \
                     current={}, requested={}",
                    state.snapshot_id,
                    snapshot_id
                );
            }
            Some(state) => {
                state.snapshot_id = snapshot_id;
                state.replication_mask = replication_mask;
            }
            None => {
                guard.insert(table_id, SharedTableState { snapshot_id, replication_mask });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use etl_postgres::types::{ColumnSchema, TableName, TableSchema};
    use tokio_postgres::types::Type;

    use super::*;

    fn create_test_mask() -> ReplicationMask {
        let schema = TableSchema::new(
            TableId::new(123),
            TableName::new("public".to_string(), "test_table".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
                ColumnSchema::new("age".to_string(), Type::INT4, -1, 3, None, true),
            ],
        );

        let replicated_columns: HashSet<String> =
            ["id".to_string(), "age".to_string()].into_iter().collect();
        ReplicationMask::build(&schema, &replicated_columns)
    }

    #[tokio::test]
    async fn note_replication_mask_and_get() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let snapshot_id = SnapshotId::new(10.into());
        let replication_mask = create_test_mask();

        cache.upsert(table_id, snapshot_id, Some(replication_mask.clone())).await;

        let state = cache.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id, snapshot_id);
        assert_eq!(
            state.replication_mask.expect("replication mask should exist").as_slice(),
            replication_mask.as_slice()
        );
    }

    #[tokio::test]
    async fn note_schema_snapshot_invalidates_replication_mask() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let replication_mask = create_test_mask();

        cache.upsert(table_id, SnapshotId::new(10.into()), Some(replication_mask)).await;
        cache.upsert(table_id, SnapshotId::new(11.into()), None).await;

        let state = cache.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id, SnapshotId::new(11.into()));
        assert!(state.replication_mask.is_none());
    }

    #[should_panic(expected = "shared table cache received stale snapshot update")]
    #[tokio::test]
    async fn older_snapshot_panics() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let replication_mask = create_test_mask();

        cache.upsert(table_id, SnapshotId::new(11.into()), Some(replication_mask.clone())).await;
        cache.upsert(table_id, SnapshotId::new(10.into()), None).await;
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let cache1 = SharedTableCache::new();
        let cache2 = cache1.clone();
        let table_id = TableId::new(123);
        let snapshot_id = SnapshotId::new(10.into());
        let replication_mask = create_test_mask();

        cache1.upsert(table_id, snapshot_id, Some(replication_mask.clone())).await;

        let state = cache2.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id, snapshot_id);
        assert_eq!(
            state.replication_mask.expect("replication mask should exist").as_slice(),
            replication_mask.as_slice()
        );
    }
}
