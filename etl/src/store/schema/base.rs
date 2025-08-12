use etl_postgres::schema::{TableId, TableSchema};
use std::collections::HashMap;
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

    /// Returns table mapping for a specific source table ID from the cache.
    ///
    /// Does not load any new data into the cache.
    fn get_table_mapping(
        &self,
        source_table_id: &TableId,
    ) -> impl Future<Output = EtlResult<Option<String>>> + Send;

    /// Returns all table mappings from the cache.
    ///
    /// Does not read from the persistent store.
    fn get_table_mappings(
        &self,
    ) -> impl Future<Output = EtlResult<HashMap<TableId, String>>> + Send;

    /// Loads all table mappings from the persistent state into the cache.
    ///
    /// This can be called lazily when table mappings are needed by the destination.
    fn load_table_mappings(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores a table mapping in both the cache and the persistent store.
    fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
