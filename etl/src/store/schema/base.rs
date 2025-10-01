use etl_postgres::types::{SchemaVersion, TableId, TableSchema, TableSchemaDraft};
use std::sync::Arc;

use crate::error::EtlResult;

/// Trait for storing and retrieving database table schema information.
///
/// [`SchemaStore`] implementations are responsible for defining how the schema information
/// is stored and retrieved.
///
/// Implementations should ensure thread-safety and handle concurrent access to the data.
pub trait SchemaStore {
    /// Returns table schema for table with a specific schema version.
    ///
    /// Does not load any new data into the cache.
    fn get_table_schema(
        &self,
        table_id: &TableId,
        version: SchemaVersion,
    ) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;

    /// Returns the latest table schema version for table with id `table_id` from the cache.
    fn get_latest_table_schema(
        &self,
        table_id: &TableId,
    ) -> impl Future<Output = EtlResult<Option<Arc<TableSchema>>>> + Send;

    /// Loads table schemas from the persistent state into the cache.
    ///
    /// This should be called once the program starts to load the schemas into the cache.
    fn load_table_schemas(&self) -> impl Future<Output = EtlResult<usize>> + Send;

    /// Stores a table schema in both the cache and the persistent store.
    fn store_table_schema(
        &self,
        table_schema: TableSchemaDraft,
    ) -> impl Future<Output = EtlResult<Arc<TableSchema>>> + Send;
}
