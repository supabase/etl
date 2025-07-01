use postgres::schema::{TableId, TableSchema};
use std::future::Future;

use crate::conversions::table_row::TableRow;
use crate::error::Result;
use crate::v2::conversions::event::Event;

pub trait Destination {
    fn write_table_schema(&self, schema: TableSchema) -> impl Future<Output = Result<()>> + Send;

    fn load_table_schemas(&self) -> impl Future<Output = Result<Vec<TableSchema>>> + Send;

    fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = Result<()>> + Send;
}
