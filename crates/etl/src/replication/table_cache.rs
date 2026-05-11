//! Shared per-table protocol state for logical replication workers.
//!
//! PostgreSQL 15+ supports column-level publication filtering, where only
//! specific columns are replicated rather than all columns. The worker that
//! currently owns a table therefore needs three pieces of protocol state to
//! decode row changes: the schema snapshot to decode against, the publication
//! column filter for that snapshot, and the replica-identity semantics for
//! that same snapshot.
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
//!
//! Each table moves between two explicit states:
//! - [`SharedTableState::WaitingForRelation`], where we know which schema
//!   snapshot future relation decoding must target but do not yet have runtime
//!   relation state for that snapshot.
//! - [`SharedTableState::Ready`], where the full [`ReplicatedTableSchema`]
//!   needed for row decoding is already materialized in memory.

use std::{collections::HashMap, sync::Arc};

use etl_postgres::types::{ReplicatedTableSchema, SnapshotId, TableId};
use tokio::sync::RwLock;

/// Shared per-table protocol state used to decode logical replication messages.
#[derive(Debug, Clone)]
pub(crate) enum SharedTableState {
    /// A newer schema snapshot exists, but the runtime relation state for that
    /// snapshot has not been materialized yet.
    WaitingForRelation {
        /// The latest schema snapshot known for the table.
        snapshot_id: SnapshotId,
    },
    /// The runtime replicated schema for the current snapshot is ready to use.
    Ready {
        /// The replicated table schema used to decode row events.
        replicated_table_schema: ReplicatedTableSchema,
    },
}

impl SharedTableState {
    /// Returns the snapshot that this shared state targets.
    pub(crate) fn snapshot_id(&self) -> SnapshotId {
        match self {
            Self::WaitingForRelation { snapshot_id } => *snapshot_id,
            Self::Ready { replicated_table_schema } => replicated_table_schema.inner().snapshot_id,
        }
    }

    /// Returns the runtime replicated schema when it is ready.
    pub(crate) fn replicated_table_schema(&self) -> Option<&ReplicatedTableSchema> {
        match self {
            Self::WaitingForRelation { .. } => None,
            Self::Ready { replicated_table_schema } => Some(replicated_table_schema),
        }
    }
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

    /// Returns the current active table ids in the cache, which represent the
    /// active tables being replicated.
    pub(crate) async fn active_table_ids(&self) -> Vec<TableId> {
        let guard = self.inner.read().await;
        guard.keys().copied().collect()
    }

    /// Records that a table is waiting for a new relation-state refresh for
    /// the supplied snapshot.
    pub(crate) async fn note_waiting_for_relation(
        &self,
        table_id: TableId,
        snapshot_id: SnapshotId,
    ) {
        self.upsert(table_id, SharedTableState::WaitingForRelation { snapshot_id }).await;
    }

    /// Records that a table has a ready runtime replicated schema.
    pub(crate) async fn note_ready(
        &self,
        table_id: TableId,
        replicated_table_schema: ReplicatedTableSchema,
    ) {
        self.upsert(table_id, SharedTableState::Ready { replicated_table_schema }).await;
    }

    /// Inserts or updates shared table state.
    ///
    /// This cache is deliberately oblivious to ordering and ownership. It
    /// assumes workers call it only after the apply loop has established table
    /// ownership, and it follows the order of those calls even when a reconnect
    /// replays older WAL.
    async fn upsert(&self, table_id: TableId, new_state: SharedTableState) {
        let mut guard = self.inner.write().await;
        if let Some(state) = guard.get_mut(&table_id) {
            *state = new_state;
        } else {
            guard.insert(table_id, new_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use etl_postgres::types::{ColumnSchema, TableName, TableSchema};
    use tokio_postgres::types::Type;

    use super::*;

    fn create_test_schema() -> ReplicatedTableSchema {
        let schema = TableSchema::with_snapshot_id(
            TableId::new(123),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, None, true),
                ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, None, true),
            ],
            SnapshotId::new(10.into()),
        );

        let replicated_columns: HashSet<String> =
            ["id".to_owned(), "age".to_owned()].into_iter().collect();
        let replication_mask =
            etl_postgres::types::ReplicationMask::build(&schema, &replicated_columns);
        let identity_mask = etl_postgres::types::IdentityMask::from_bytes(vec![1, 0, 1]);
        ReplicatedTableSchema::from_masks(Arc::new(schema), replication_mask, identity_mask)
    }

    #[tokio::test]
    async fn note_ready_and_get() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let snapshot_id = SnapshotId::new(10.into());
        let replicated_table_schema = create_test_schema();

        cache.note_ready(table_id, replicated_table_schema.clone()).await;

        let state = cache.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id(), snapshot_id);
        assert_eq!(
            state
                .replicated_table_schema()
                .expect("replicated table schema should exist")
                .replication_mask()
                .as_slice(),
            replicated_table_schema.replication_mask().as_slice()
        );
    }

    #[tokio::test]
    async fn note_waiting_for_relation_invalidates_ready_schema() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let replicated_table_schema = create_test_schema();

        cache.note_ready(table_id, replicated_table_schema).await;
        cache.note_waiting_for_relation(table_id, SnapshotId::new(11.into())).await;

        let state = cache.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id(), SnapshotId::new(11.into()));
        assert!(matches!(state, SharedTableState::WaitingForRelation { .. }));
    }

    #[tokio::test]
    async fn older_snapshot_rewinds_to_waiting_relation() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let replicated_table_schema = create_test_schema();

        cache.note_ready(table_id, replicated_table_schema).await;
        cache.note_waiting_for_relation(table_id, SnapshotId::new(9.into())).await;

        let state = cache.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id(), SnapshotId::new(9.into()));
        assert!(matches!(state, SharedTableState::WaitingForRelation { .. }));
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let cache1 = SharedTableCache::new();
        let cache2 = cache1.clone();
        let table_id = TableId::new(123);
        let snapshot_id = SnapshotId::new(10.into());
        let replicated_table_schema = create_test_schema();

        cache1.note_ready(table_id, replicated_table_schema.clone()).await;

        let state = cache2.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id(), snapshot_id);
        assert_eq!(
            state
                .replicated_table_schema()
                .expect("replicated table schema should exist")
                .identity_mask()
                .as_slice(),
            replicated_table_schema.identity_mask().as_slice()
        );
    }

    #[tokio::test]
    async fn waiting_state_exposes_snapshot_without_schema() {
        let cache = SharedTableCache::new();
        let table_id = TableId::new(123);
        let snapshot_id = SnapshotId::new(10.into());

        cache.note_waiting_for_relation(table_id, snapshot_id).await;

        let state = cache.get(&table_id).await.expect("table state should exist");
        assert_eq!(state.snapshot_id(), snapshot_id);
        assert!(state.replicated_table_schema().is_none());
    }

    #[tokio::test]
    async fn active_table_ids_returns_current_tables() {
        let cache = SharedTableCache::new();
        let ready_table_id = TableId::new(123);
        let waiting_table_id = TableId::new(456);
        let waiting_snapshot_id = SnapshotId::new(20.into());

        cache.note_ready(ready_table_id, create_test_schema()).await;
        cache.note_waiting_for_relation(waiting_table_id, waiting_snapshot_id).await;

        let table_ids = cache.active_table_ids().await;

        assert_eq!(table_ids.len(), 2);
        assert!(table_ids.contains(&ready_table_id));
        assert!(table_ids.contains(&waiting_table_id));
    }
}
