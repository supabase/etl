use gcp_bigquery_client::error::BQError;
use postgres::schema::{Oid, TableSchema};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::conversions::table_row::TableRow;
use crate::v2::clients::bigquery::BigQueryClient;
use crate::v2::conversions::event::Event;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::schema::cache::SchemaCache;

#[derive(Debug, Error)]
pub enum BigQueryDestinationError {
    #[error("An error occurred with BigQuery: {0}")]
    BigQuery(#[from] BQError),
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
        let mut inner = self.inner.read().await;

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
        table_rows: Vec<TableRow>,
    ) -> Result<(), BigQueryDestinationError> {
        todo!()
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
