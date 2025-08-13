use etl_postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

/// Internal storage for schema cache data.
///
/// This private struct holds the actual schema data and provides methods
/// for accessing cached table schemas. It is wrapped by [`SchemaCache`]
/// for thread-safe access.
#[derive(Debug)]
pub struct Inner {
    table_schemas: HashMap<TableId, TableSchema>,
}

impl Inner {
    /// Returns a reference to the table schema for the given table ID.
    ///
    /// This private method provides direct access to cached schemas without
    /// cloning, used internally by [`SchemaCache`] methods.
    pub fn get_table_schema_ref(&self, table_id: &TableId) -> Option<&TableSchema> {
        self.table_schemas.get(table_id)
    }
}

/// Thread-safe cache for PostgreSQL table schemas.
///
/// [`SchemaCache`] provides efficient in-memory storage of table schema information
/// with thread-safe access patterns. It is designed to minimize database roundtrips
/// by caching frequently accessed schema data.
///
/// The cache currently uses unbounded storage but may implement eviction policies
/// in future versions to manage memory usage in long-running ETL processes.
// TODO: implement eviction of the entries if they go over a certain threshold.
#[derive(Debug, Clone)]
pub struct SchemaCache {
    inner: Arc<Mutex<Inner>>,
}

impl SchemaCache {
    /// Creates a new empty schema cache.
    ///
    /// The cache starts with no entries and will be populated as table schemas
    /// are added during ETL operations.
    pub fn new() -> Self {
        let inner = Inner {
            table_schemas: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Adds a single table schema to the cache.
    ///
    /// If a schema with the same table ID already exists, it will be replaced
    /// with the new schema. This operation is atomic and thread-safe.
    pub async fn add_table_schema(&self, table_schema: TableSchema) {
        let mut inner = self.inner.lock().await;
        inner.table_schemas.insert(table_schema.id, table_schema);
    }

    /// Adds multiple table schemas to the cache in a single operation.
    ///
    /// This method is more efficient than calling [`SchemaCache::add_table_schema`]
    /// multiple times as it acquires the lock only once. Any existing schemas
    /// with matching table IDs will be replaced.
    pub async fn add_table_schemas(&self, table_schemas: Vec<TableSchema>) {
        let mut inner = self.inner.lock().await;
        for table_schema in table_schemas {
            inner.table_schemas.insert(table_schema.id, table_schema);
        }
    }

    /// Retrieves a copy of the table schema for the given table ID.
    ///
    /// Returns [`None`] if no schema is cached for the specified table ID.
    /// The returned schema is cloned to avoid holding locks longer than necessary.
    pub async fn get_table_schema(&self, table_id: &TableId) -> Option<TableSchema> {
        let inner = self.inner.lock().await;
        inner.table_schemas.get(table_id).cloned()
    }

    /// Provides direct access to the internal cache storage.
    ///
    /// This method returns a mutex guard that allows direct manipulation of the
    /// cache contents. It should be used sparingly and only when batch operations
    /// require extended access to the cache.
    pub async fn lock_inner(&'_ self) -> MutexGuard<'_, Inner> {
        self.inner.lock().await
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}
