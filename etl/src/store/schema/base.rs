use etl_postgres::schema::{TableId, TableSchema};
use std::sync::Arc;

use crate::error::EtlResult;

pub trait SchemaStore {
    /// Returns table schema for table with id `table_id` from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_table_schema(
        &self,
        table_id: &TableId,
    ) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;

    /// Returns all table schemas from the cache.
    ///
    /// Does not read from the persistent store.
    fn get_table_schemas(&self) -> impl Future<Output = EtlResult<Vec<Arc<TableSchema>>>> + Send;

    /// Loads table schemas from the persistent state into the cache.
    ///
    /// This should be called once at program start to load the schemas into the cache.
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores a table schema in both the cache and the persistent store.
    fn store_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
