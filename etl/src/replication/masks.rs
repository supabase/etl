//! Shared replication mask storage for tracking replicated columns across workers.
//!
//! PostgreSQL 15+ supports column-level publication filtering, where only specific columns
//! are replicated rather than all columns. A [`ReplicationMask`] is a bitmask indicating
//! which columns in a table are being replicated (1 = replicated, 0 = not replicated).
//!
//! This mask is needed to correctly decode replication events, since the stream only
//! contains values for replicated columns. Both the apply worker (for CDC events) and
//! table sync workers (for initial copy) need access to these masks, so they are stored
//! in a shared container passed to all workers.
//!
//! The replication mask is kept in-memory only because PostgreSQL guarantees that RELATION
//! messages are sent at the start of each connection and whenever the schema changes. This
//! ensures we always receive schema information before any data events that depend on it,
//! allowing us to compute the mask on-demand without persistence.
//!
//! **Limitation**: Adding or removing columns from a publication while the pipeline is
//! running will cause schema mismatches. Downstream tables that rely on a fixed schema
//! will break because the replicated column set changes but the destination schema does
//! not automatically update.

use etl_postgres::types::{ReplicationMask, TableId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe container for replication masks shared across workers.
#[derive(Debug, Clone, Default)]
pub struct ReplicationMasksCache {
    inner: Arc<RwLock<HashMap<TableId, ReplicationMask>>>,
}

impl ReplicationMasksCache {
    /// Creates a new empty [`ReplicationMasksCache`] container.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Stores a replication mask for a table.
    ///
    /// This is typically called by the table sync worker after it has determined
    /// which columns are being replicated for a table.
    pub async fn set(&self, table_id: TableId, mask: ReplicationMask) {
        let mut guard = self.inner.write().await;
        guard.insert(table_id, mask);
    }

    /// Retrieves the replication mask for a table.
    ///
    /// Returns `None` if no mask has been set for the given table.
    pub async fn get(&self, table_id: &TableId) -> Option<ReplicationMask> {
        let guard = self.inner.read().await;
        guard.get(table_id).cloned()
    }

    /// Removes the replication mask for a table.
    ///
    /// This is called after processing a DDL schema change message to invalidate the cached
    /// mask. While PostgreSQL guarantees that a RELATION message will be sent before any DML
    /// events after a schema change, we proactively invalidate the mask to ensure consistency.
    /// The next RELATION message will rebuild the mask with the updated schema.
    pub async fn remove(&self, table_id: &TableId) {
        let mut guard = self.inner.write().await;
        guard.remove(table_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_postgres::types::{ColumnSchema, TableName, TableSchema};
    use std::collections::HashSet;
    use tokio_postgres::types::Type;

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
    async fn test_set_and_get() {
        let masks = ReplicationMasksCache::new();
        let table_id = TableId::new(123);
        let mask = create_test_mask();

        masks.set(table_id, mask.clone()).await;

        let retrieved = masks.get(&table_id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), mask.as_slice());
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let masks = ReplicationMasksCache::new();
        let table_id = TableId::new(123);

        let retrieved = masks.get(&table_id).await;
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let masks1 = ReplicationMasksCache::new();
        let masks2 = masks1.clone();
        let table_id = TableId::new(123);
        let mask = create_test_mask();

        masks1.set(table_id, mask.clone()).await;

        // masks2 should see the same data
        let retrieved = masks2.get(&table_id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), mask.as_slice());
    }
}
