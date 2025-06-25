use postgres::schema::{Oid, TableSchema};
use std::future::Future;
use thiserror::Error;

use crate::conversions::table_row::TableRow;
use crate::v2::conversions::event::Event;
use crate::v2::destination::bigquery::BigQueryDestinationError;

#[derive(Debug, Error)]
pub enum DestinationError {
    #[error(transparent)]
    BigQuery(#[from] BigQueryDestinationError),
}

pub trait Destination {
    fn write_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn load_table_schemas(
        &self,
    ) -> impl Future<Output = Result<Vec<TableSchema>, DestinationError>> + Send;

    fn write_table_rows(
        &self,
        table_id: Oid,
        table_rows: Vec<TableRow>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn write_events(
        &self,
        events: Vec<Event>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;
}
