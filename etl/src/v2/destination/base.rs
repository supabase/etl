use postgres::schema::{TableId, TableSchema};
use std::future::Future;
use thiserror::Error;

use crate::conversions::table_row::TableRow;
use crate::v2::conversions::event::Event;

#[derive(Debug, Error)]
pub enum DestinationError {}

pub trait Destination {
    fn write_table_schema(
        &self,
        schema: TableSchema,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn load_table_schemas(
        &self,
    ) -> impl Future<Output = Result<Vec<TableSchema>, DestinationError>> + Send;

    fn write_table_rows(
        &self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn write_events(
        &self,
        events: Vec<Event>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;
}
