use etl_postgres::schema::TableId;
use std::future::Future;

use crate::conversions::event::Event;
use crate::conversions::table_row::TableRow;
use crate::error::EtlResult;

pub trait Destination {
    fn truncate_table(&self, table_id: TableId) -> impl Future<Output = EtlResult<()>> + Send;

    fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> impl Future<Output = EtlResult<()>> + Send;

    fn write_events(&self, events: Vec<Event>) -> impl Future<Output = EtlResult<()>> + Send;
}
