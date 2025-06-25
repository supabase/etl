use crate::clients::bigquery::table_schema_to_descriptor;
use crate::conversions::table_row::TableRow;
use crate::conversions::Cell;
use crate::v2::clients::bigquery::BigQueryClient;
use crate::v2::conversions::event::Event;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::schema::cache::SchemaCache;
use gcp_bigquery_client::error::BQError;
use postgres::schema::{Oid, TableSchema};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum BigQueryDestinationError {
    #[error("An error occurred with BigQuery: {0}")]
    BigQuery(#[from] BQError),

    #[error("The table schema for table id {0} was not found in the schema cache")]
    MissingTableSchema(Oid),
}

#[derive(Debug)]
struct Inner {
    client: BigQueryClient,
    dataset_id: String,
    max_staleness_mins: u16,
    schema_cache: SchemaCache,
}

#[derive(Debug, Clone)]
pub struct BigQueryDestination {
    inner: Arc<RwLock<Inner>>,
}

impl BigQueryDestination {
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: String,
        gcp_sa_key_path: &str,
        max_staleness_mins: u16,
        schema_cache: SchemaCache,
    ) -> Result<Self, BigQueryDestinationError> {
        let client = BigQueryClient::new_with_key_path(project_id, gcp_sa_key_path).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn new_with_key(
        project_id: String,
        dataset_id: String,
        gcp_sa_key: &str,
        max_staleness_mins: u16,
        schema_cache: SchemaCache,
    ) -> Result<Self, BigQueryDestinationError> {
        let client = BigQueryClient::new_with_key(project_id, gcp_sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    async fn write_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> Result<(), BigQueryDestinationError> {
        let inner = self.inner.read().await;

        inner
            .client
            .create_table_if_missing(
                &inner.dataset_id,
                &table_schema.name.as_bigquery_table_name(),
                &table_schema.column_schemas,
                inner.max_staleness_mins,
            )
            .await?;

        Ok(())
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, BigQueryDestinationError> {
        // TODO: implement loading of table schemas from big query.
        Ok(vec![])
    }

    async fn write_table_rows(
        &self,
        table_id: Oid,
        mut table_rows: Vec<TableRow>,
    ) -> Result<(), BigQueryDestinationError> {
        let mut inner = self.inner.write().await;

        // TODO: figure out how we can avoid a clone.
        let dataset_id = inner.dataset_id.clone();
        let (table_name, table_descriptor) = {
            let schema_cache = inner.schema_cache.read_inner().await;
            let table_schema = schema_cache
                .get_table_schema_ref(&table_id)
                .ok_or(BigQueryDestinationError::MissingTableSchema(table_id))?;

            let table_name = table_schema.name.as_bigquery_table_name();
            let table_descriptor = table_schema_to_descriptor(table_schema);

            (table_name, table_descriptor)
        };

        for table_row in &mut table_rows {
            table_row.values.push(Cell::String("UPSERT".to_string()));
        }

        inner
            .client
            .stream_rows(&dataset_id, table_name, &table_descriptor, &table_rows)
            .await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), BigQueryDestinationError> {
        todo!()
    }
}

impl Destination for BigQueryDestination {
    async fn write_table_schema(&self, table_schema: TableSchema) -> Result<(), DestinationError> {
        self.write_table_schema(table_schema).await?;

        Ok(())
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, DestinationError> {
        let table_schemas = self.load_table_schemas().await?;

        Ok(table_schemas)
    }

    async fn write_table_rows(
        &self,
        table_id: Oid,
        table_rows: Vec<TableRow>,
    ) -> Result<(), DestinationError> {
        self.write_table_rows(table_id, table_rows).await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), DestinationError> {
        self.write_events(events).await?;

        Ok(())
    }
}
