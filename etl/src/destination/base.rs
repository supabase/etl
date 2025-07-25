use postgres::schema::{TableId, TableSchema};
use std::future::Future;

use crate::conversions::event::Event;
use crate::conversions::table_row::TableRow;
use crate::error::ETLResult;
use crate::schema::cache::SchemaCache;

pub trait Destination {
    fn inject(&self, _schema_cache: SchemaCache) -> impl Future<Output = ETLResult<()>> + Send {
        // By default, the injection code is a noop, since not all destinations need dependencies
        // to be injected.
        async move { Ok(()) }
    }

    fn write_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> impl Future<Output = ETLResult<()>> + Send;

    fn load_table_schemas(&self) -> impl Future<Output = ETLResult<Vec<TableSchema>>> + Send;

    fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> impl Future<Output = ETLResult<()>> + Send;

    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = ETLResult<()>> + Send;
}
