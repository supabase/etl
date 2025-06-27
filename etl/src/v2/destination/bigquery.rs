use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::storage::TableDescriptor;
use postgres::schema::{Oid, TableSchema};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::conversions::table_row::TableRow;
use crate::conversions::Cell;
use crate::v2::clients::bigquery::BigQueryClient;
use crate::v2::conversions::event::Event;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::schema::cache::SchemaCache;

#[derive(Debug, Error)]
pub enum BigQueryDestinationError {
    #[error("An error occurred with BigQuery: {0}")]
    BigQuery(#[from] BQError),

    #[error("The table schema for table id {0} was not found in the schema cache")]
    MissingTableSchema(Oid),

    #[error("The schema cache was not set on the destination")]
    MissingSchemaCache,
}

#[derive(Debug)]
struct Inner {
    client: BigQueryClient,
    dataset_id: String,
    max_staleness_mins: u16,
    schema_cache: Option<SchemaCache>,
}

#[derive(Debug, Clone)]
pub struct BigQueryDestination {
    inner: Arc<RwLock<Inner>>,
}

impl BigQueryDestination {
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: String,
        sa_key: &str,
        max_staleness_mins: u16,
    ) -> Result<Self, BigQueryDestinationError> {
        let client = BigQueryClient::new_with_key_path(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache: None,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn new_with_key(
        project_id: String,
        dataset_id: String,
        sa_key: &str,
        max_staleness_mins: u16,
    ) -> Result<Self, BigQueryDestinationError> {
        let client = BigQueryClient::new_with_key(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache: None,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn new_with_urls(
        project_id: String,
        dataset_id: String,
        auth_base_url: String,
        v2_base_url: String,
        sa_key: &str,
        max_staleness_mins: u16,
    ) -> Result<Self, BigQueryDestinationError> {
        let client =
            BigQueryClient::new_with_custom_urls(project_id, auth_base_url, v2_base_url, sa_key)
                .await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache: None,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    async fn load_table_id_and_descriptor<I: Deref<Target = Inner>>(
        inner: &I,
        table_id: &Oid,
    ) -> Result<(String, TableDescriptor), BigQueryDestinationError> {
        let schema_cache = inner
            .schema_cache
            .as_ref()
            .ok_or(BigQueryDestinationError::MissingSchemaCache)?
            .read_inner()
            .await;
        let table_schema = schema_cache
            .get_table_schema_ref(table_id)
            .ok_or(BigQueryDestinationError::MissingTableSchema(*table_id))?;

        let table_id = table_schema.name.as_bigquery_table_id();
        let table_descriptor = BigQueryClient::table_schema_to_descriptor(table_schema);

        Ok((table_id, table_descriptor))
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
                &table_schema.name.as_bigquery_table_id(),
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

        let (table_id, table_descriptor) =
            Self::load_table_id_and_descriptor(&inner, &table_id).await?;

        for table_row in &mut table_rows {
            table_row.values.push(Cell::String("UPSERT".to_string()));
        }

        let dataset_id = inner.dataset_id.clone();
        inner
            .client
            .stream_rows(&dataset_id, table_id, &table_descriptor, table_rows)
            .await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), BigQueryDestinationError> {
        let mut table_id_to_table_rows = HashMap::new();

        for event in events {
            match event {
                Event::Insert(mut insert) => {
                    insert
                        .table_row
                        .values
                        .push(Cell::String("UPSERT".to_owned()));
                    let table_rows: &mut Vec<TableRow> =
                        table_id_to_table_rows.entry(insert.table_id).or_default();
                    table_rows.push(insert.table_row);
                }
                Event::Update(mut update) => {
                    update
                        .table_row
                        .values
                        .push(Cell::String("UPSERT".to_owned()));
                    let table_rows: &mut Vec<TableRow> =
                        table_id_to_table_rows.entry(update.table_id).or_default();
                    table_rows.push(update.table_row);
                }
                Event::Delete(delete) => {
                    let Some((is_key, mut old_table_row)) = delete.old_table_row else {
                        continue;
                    };

                    // Only if the old table row is not a key, meaning it has all the columns, we
                    // want to insert it, otherwise we might miss columns.
                    if !is_key {
                        old_table_row.values.push(Cell::String("DELETE".to_owned()));
                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(delete.table_id).or_default();
                        table_rows.push(old_table_row);
                    }
                }
                Event::Truncate(_) => {
                    // BigQuery doesn't support `TRUNCATE` DML statement when using the storage write API.
                    // If you try to truncate a table that has a streaming buffer, you will get the following error:
                    //  TRUNCATE DML statement over table <tablename> would affect rows in the streaming buffer, which is not supported
                }
                _ => {
                    // Every other event type is currently not supported.
                }
            }
        }

        let mut inner = self.inner.write().await;

        for (table_id, table_rows) in table_id_to_table_rows {
            let (table_id, table_descriptor) =
                Self::load_table_id_and_descriptor(&inner, &table_id).await?;

            let dataset_id = inner.dataset_id.clone();
            inner
                .client
                .stream_rows(&dataset_id, table_id, &table_descriptor, table_rows)
                .await?;
        }

        Ok(())
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
