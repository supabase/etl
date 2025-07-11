use postgres::schema::{TableId, TableSchema};
use std::future::Future;
use thiserror::Error;

use crate::conversions::event::Event;
use crate::conversions::table_row::TableRow;
#[cfg(feature = "bigquery")]
use crate::destination::bigquery::BigQueryDestinationError;
use crate::schema::cache::SchemaCache;

#[derive(Debug, Error)]
pub enum DestinationError {
    #[cfg(feature = "bigquery")]
    #[error(transparent)]
    BigQuery(#[from] BigQueryDestinationError),
}

pub trait Destination {
    fn inject(
        &self,
        _schema_cache: SchemaCache,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send {
        // By default, the injection code is a noop, since not all destinations need dependencies
        // to be injected.
        async move { Ok(()) }
    }

    fn write_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn load_table_schemas(
        &self,
    ) -> impl Future<Output = Result<Vec<TableSchema>, DestinationError>> + Send;

    fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;

    fn write_events(
        &self,
        events: Vec<Event>,
    ) -> impl Future<Output = Result<(), DestinationError>> + Send;
}
