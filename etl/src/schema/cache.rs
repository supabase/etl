use etl_postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Debug)]
pub struct Inner {
    table_schemas: HashMap<TableId, TableSchema>,
}

impl Inner {
    pub fn get_table_schema_ref(&self, table_id: &TableId) -> Option<&TableSchema> {
        self.table_schemas.get(table_id)
    }
}

// TODO: implement eviction of the entries if they go over a certain threshold.
#[derive(Debug, Clone)]
pub struct SchemaCache {
    inner: Arc<Mutex<Inner>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        let inner = Inner {
            table_schemas: HashMap::new(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn add_table_schema(&self, table_schema: TableSchema) {
        let mut inner = self.inner.lock().await;
        inner.table_schemas.insert(table_schema.id, table_schema);
    }

    pub async fn add_table_schemas(&self, table_schemas: Vec<TableSchema>) {
        let mut inner = self.inner.lock().await;
        for table_schema in table_schemas {
            inner.table_schemas.insert(table_schema.id, table_schema);
        }
    }

    pub async fn get_table_schema(&self, table_id: &TableId) -> Option<TableSchema> {
        let inner = self.inner.lock().await;
        inner.table_schemas.get(table_id).cloned()
    }

    pub async fn lock_inner(&'_ self) -> MutexGuard<'_, Inner> {
        self.inner.lock().await
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}
