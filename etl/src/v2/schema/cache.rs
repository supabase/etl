use postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug)]
struct Inner {
    table_schemas: HashMap<TableId, TableSchema>,
}

// TODO: implement eviction of the entries if they go over a certain threshold.
#[derive(Debug, Clone)]
pub struct SchemaCache {
    inner: Arc<RwLock<Inner>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        let inner = Inner {
            table_schemas: HashMap::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn add_table_schema(&self, table_schema: TableSchema) {
        let mut inner = self.inner.write().await;
        inner.table_schemas.insert(table_schema.id, table_schema);
    }

    pub async fn add_table_schemas(&self, table_schemas: Vec<TableSchema>) {
        let mut inner = self.inner.write().await;
        for table_schema in table_schemas {
            inner.table_schemas.insert(table_schema.id, table_schema);
        }
    }

    pub async fn get_table_schema(&self, table_id: &TableId) -> Option<TableSchema> {
        let inner = self.inner.read().await;
        inner.table_schemas.get(table_id).cloned()
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}
