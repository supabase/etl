use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::storage::TableDescriptor;
use postgres::schema::{ColumnSchema, Oid, TableName, TableSchema};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_postgres::types::Type;
use tracing::info;

use crate::conversions::table_row::TableRow;
use crate::conversions::Cell;
use crate::v2::clients::bigquery::{BigQueryCdcMode, BigQueryClient, BigQueryClientError};
use crate::v2::conversions::event::Event;
use crate::v2::destination::base::{Destination, DestinationError};
use crate::v2::schema::cache::SchemaCache;

const ETL_TABLE_SCHEMAS_NAME: &str = "etl_table_schemas";

static ETL_TABLE_SCHEMAS_COLUMNS: LazyLock<Vec<ColumnSchema>> = LazyLock::new(|| {
    vec![
        ColumnSchema::new("table_id".to_string(), Type::INT8, -1, false, true),
        ColumnSchema::new("schema_name".to_string(), Type::TEXT, -1, false, false),
        ColumnSchema::new("table_name".to_string(), Type::TEXT, -1, false, false),
    ]
});

const ETL_TABLE_COLUMNS_NAME: &str = "etl_table_columns";
static ETL_TABLE_COLUMNS_COLUMNS: LazyLock<Vec<ColumnSchema>> = LazyLock::new(|| {
    vec![
        ColumnSchema::new("table_id".to_string(), Type::INT8, -1, false, true),
        ColumnSchema::new("column_name".to_string(), Type::TEXT, -1, false, true),
        ColumnSchema::new("column_type".to_string(), Type::TEXT, -1, false, false),
        ColumnSchema::new("type_modifier".to_string(), Type::INT4, -1, false, false),
        ColumnSchema::new("nullable".to_string(), Type::BOOL, -1, false, false),
        ColumnSchema::new("primary_key".to_string(), Type::BOOL, -1, false, false),
        ColumnSchema::new("column_order".to_string(), Type::INT4, -1, false, false),
    ]
});

#[derive(Debug, Error)]
pub enum BigQueryDestinationError {
    #[error("An error occurred with the BigQuery client: {0}")]
    BigQueryClient(#[from] BigQueryClientError),

    #[error("The table schema for table id {0} was not found in the schema cache")]
    MissingTableSchema(Oid),

    #[error("The schema cache was not set on the destination")]
    MissingSchemaCache,

    #[error("Failed to serialize table schema: {0}")]
    SerializationError(#[from] serde_json::Error),
}

#[derive(Debug)]
struct Inner {
    client: BigQueryClient,
    dataset_id: String,
    max_staleness_mins: u16,
    schema_cache: Option<SchemaCache>,
}

impl Inner {
    async fn ensure_schema_tables_exist(&self) -> Result<(), BigQueryDestinationError> {
        // Create etl_table_schemas table - use ColumnSchema for compatibility
        self.client
            .create_table_if_missing(
                &self.dataset_id,
                ETL_TABLE_SCHEMAS_NAME,
                &ETL_TABLE_SCHEMAS_COLUMNS,
                self.max_staleness_mins,
            )
            .await?;

        // Create etl_table_columns table
        self.client
            .create_table_if_missing(
                &self.dataset_id,
                ETL_TABLE_COLUMNS_NAME,
                &ETL_TABLE_COLUMNS_COLUMNS,
                self.max_staleness_mins,
            )
            .await?;

        Ok(())
    }
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
        let table_descriptor =
            BigQueryClient::column_schemas_to_table_descriptor(&table_schema.column_schemas);

        Ok((table_id, table_descriptor))
    }

    async fn write_table_schema(
        &self,
        table_schema: TableSchema,
    ) -> Result<(), BigQueryDestinationError> {
        let mut inner = self.inner.write().await;

        let dataset_id = inner.dataset_id.clone();

        // Create the actual data table
        inner
            .client
            .create_table_if_missing(
                &dataset_id,
                &table_schema.name.as_bigquery_table_id(),
                &table_schema.column_schemas,
                inner.max_staleness_mins,
            )
            .await?;

        // Ensure schema storage tables exist
        inner.ensure_schema_tables_exist().await?;

        // Store table schema metadata
        let mut table_schema_row = table_schema_to_table_row(&table_schema);
        table_schema_row
            .values
            .push(BigQueryCdcMode::UPSERT.into_cell());
        let table_schema_descriptor =
            BigQueryClient::column_schemas_to_table_descriptor(&ETL_TABLE_SCHEMAS_COLUMNS);
        inner
            .client
            .stream_rows(
                &dataset_id,
                ETL_TABLE_SCHEMAS_NAME.to_string(),
                &table_schema_descriptor,
                vec![table_schema_row],
            )
            .await?;

        // Store column schemas metadata
        let mut column_rows = column_schemas_to_table_rows(&table_schema);
        for column_row in column_rows.iter_mut() {
            column_row.values.push(BigQueryCdcMode::UPSERT.into_cell());
        }
        let column_descriptors =
            BigQueryClient::column_schemas_to_table_descriptor(&ETL_TABLE_COLUMNS_COLUMNS);
        if !column_rows.is_empty() {
            inner
                .client
                .stream_rows(
                    &dataset_id,
                    ETL_TABLE_COLUMNS_NAME.to_string(),
                    &column_descriptors,
                    column_rows,
                )
                .await?;
        }

        Ok(())
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, BigQueryDestinationError> {
        let inner = self.inner.read().await;

        // First check if schema tables exist
        let tables_exist = inner
            .client
            .table_exists(&inner.dataset_id, "etl_table_schemas")
            .await?;
        if !tables_exist {
            return Ok(vec![]);
        }

        // Query to get table schemas with their columns
        let query = format!(
            r#"
            select
                ts.table_id,
                ts.schema_name,
                ts.table_name,
                tc.column_name,
                tc.column_type,
                tc.type_modifier,
                tc.nullable,
                tc.primary_key,
                tc.column_order
            from `{}.{}.{ETL_TABLE_SCHEMAS_NAME}` ts
            join `{}.{}.{ETL_TABLE_COLUMNS_NAME}` tc on ts.table_id = tc.table_id
            order by ts.table_id, tc.column_order
            "#,
            inner.client.project_id(),
            inner.dataset_id,
            inner.client.project_id(),
            inner.dataset_id
        );

        let mut query_results = inner.client.query(QueryRequest::new(query)).await?;

        let mut table_schemas = HashMap::new();

        while query_results.next_row() {
            let table_id: u32 = query_results
                .get_i64_by_name("table_id")
                .map(|opt| opt.unwrap_or(0) as u32)
                .unwrap_or(0);

            let schema_name: String = query_results
                .get_string_by_name("schema_name")
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let table_name: String = query_results
                .get_string_by_name("table_name")
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let column_name: String = query_results
                .get_string_by_name("column_name")
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let column_type_str: String = query_results
                .get_string_by_name("column_type")
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let type_modifier: i32 = query_results
                .get_i64_by_name("type_modifier")
                .map(|opt| opt.unwrap_or(-1) as i32)
                .unwrap_or(-1);

            let nullable: bool = query_results
                .get_bool_by_name("nullable")
                .map(|opt| opt.unwrap_or(true))
                .unwrap_or(true);

            let primary_key: bool = query_results
                .get_bool_by_name("primary_key")
                .map(|opt| opt.unwrap_or(false))
                .unwrap_or(false);

            let column_type = string_to_postgres_type(&column_type_str)?;
            let column_schema = ColumnSchema::new(
                column_name,
                column_type,
                type_modifier,
                nullable,
                primary_key,
            );

            let table_name_obj = TableName::new(schema_name, table_name);

            let entry = table_schemas
                .entry(table_id)
                .or_insert_with(|| (table_name_obj, Vec::new()));

            entry.1.push(column_schema);
        }

        let mut result = Vec::new();
        for (table_id, (table_name, column_schemas)) in table_schemas {
            let table_schema = TableSchema::new(table_id, table_name, column_schemas);
            result.push(table_schema);
        }

        Ok(result)
    }

    async fn write_table_rows(
        &self,
        table_id: Oid,
        mut table_rows: Vec<TableRow>,
    ) -> Result<(), BigQueryDestinationError> {
        let mut inner = self.inner.write().await;

        let (table_id, table_descriptor) =
            Self::load_table_id_and_descriptor(&inner, &table_id).await?;

        let dataset_id = inner.dataset_id.clone();
        for table_row in table_rows.iter_mut() {
            table_row.values.push(BigQueryCdcMode::UPSERT.into_cell());
        }
        inner
            .client
            .stream_rows(&dataset_id, table_id, &table_descriptor, table_rows)
            .await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> Result<(), BigQueryDestinationError> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_table_rows = HashMap::new();
            let mut truncate_events = Vec::new();

            // Process events until we hit a truncate or run out of events
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(mut insert) => {
                        insert
                            .table_row
                            .values
                            .push(BigQueryCdcMode::UPSERT.into_cell());
                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(insert.table_id).or_default();
                        table_rows.push(insert.table_row);
                    }
                    Event::Update(mut update) => {
                        update
                            .table_row
                            .values
                            .push(BigQueryCdcMode::UPSERT.into_cell());
                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(update.table_id).or_default();
                        table_rows.push(update.table_row);
                    }
                    Event::Delete(delete) => {
                        let Some((_, mut old_table_row)) = delete.old_table_row else {
                            info!("The `DELETE` event has no row, so it was skipped");
                            continue;
                        };

                        old_table_row
                            .values
                            .push(BigQueryCdcMode::DELETE.into_cell());
                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(delete.table_id).or_default();
                        table_rows.push(old_table_row);
                    }
                    _ => {
                        // Every other event type is currently not supported.
                    }
                }
            }

            // Process accumulated streaming operations
            if !table_id_to_table_rows.is_empty() {
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
            }

            // Collect all consecutive truncate events
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                truncate_events.push(event_iter.next().unwrap());
            }

            // Process truncate events
            if !truncate_events.is_empty() {
                self.process_truncate_events(truncate_events).await?;
            }
        }

        Ok(())
    }

    async fn process_truncate_events(
        &self,
        events: Vec<Event>,
    ) -> Result<(), BigQueryDestinationError> {
        let inner = self.inner.read().await;

        for event in events {
            if let Event::Truncate(truncate) = event {
                for rel_id in truncate.rel_ids {
                    // Convert rel_id to table_id (Oid)
                    let table_id = rel_id;

                    // Get table information from schema cache
                    let schema_cache = inner
                        .schema_cache
                        .as_ref()
                        .ok_or(BigQueryDestinationError::MissingSchemaCache)?
                        .read_inner()
                        .await;

                    if let Some(table_schema) = schema_cache.get_table_schema_ref(&table_id) {
                        let bigquery_table_id = table_schema.name.as_bigquery_table_id();

                        // Use DELETE statement to clear the table
                        // This is more efficient than TRUNCATE for BigQuery streaming tables
                        let delete_query = format!(
                            "truncate table `{}.{}.{}`",
                            inner.client.project_id(),
                            inner.dataset_id,
                            bigquery_table_id
                        );

                        let query_request = QueryRequest::new(delete_query);
                        inner.client.query(query_request).await?;

                        info!("Truncated table: {}", bigquery_table_id);
                    } else {
                        info!(
                            "Table schema not found for table_id: {}, skipping truncate",
                            table_id
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

impl Destination for BigQueryDestination {
    async fn inject(&self, schema_cache: SchemaCache) -> Result<(), DestinationError> {
        let mut inner = self.inner.write().await;
        inner.schema_cache = Some(schema_cache);

        Ok(())
    }

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

fn table_schema_to_table_row(table_schema: &TableSchema) -> TableRow {
    let columns = vec![
        Cell::U32(table_schema.id),
        Cell::String(table_schema.name.schema.clone()),
        Cell::String(table_schema.name.name.clone()),
    ];

    TableRow::new(columns)
}

fn column_schemas_to_table_rows(table_schema: &TableSchema) -> Vec<TableRow> {
    let mut table_rows = Vec::with_capacity(table_schema.column_schemas.len());

    for (column_order, column_schema) in table_schema.column_schemas.iter().enumerate() {
        let columns = vec![
            Cell::U32(table_schema.id),
            Cell::String(column_schema.name.clone()),
            Cell::String(postgres_type_to_string(&column_schema.typ)),
            Cell::I32(column_schema.modifier),
            Cell::Bool(column_schema.nullable),
            Cell::Bool(column_schema.primary),
            // We store the index of the column since the order of columns is important while generating the
            // vector of `ColumnSchema`.
            Cell::U32(column_order as u32),
        ];

        table_rows.push(TableRow::new(columns));
    }

    table_rows
}

fn postgres_type_to_string(pg_type: &Type) -> String {
    match *pg_type {
        Type::BOOL => "BOOL".to_string(),
        Type::CHAR => "CHAR".to_string(),
        Type::INT2 => "INT2".to_string(),
        Type::INT4 => "INT4".to_string(),
        Type::INT8 => "INT8".to_string(),
        Type::FLOAT4 => "FLOAT4".to_string(),
        Type::FLOAT8 => "FLOAT8".to_string(),
        Type::TEXT => "TEXT".to_string(),
        Type::VARCHAR => "VARCHAR".to_string(),
        Type::TIMESTAMP => "TIMESTAMP".to_string(),
        Type::TIMESTAMPTZ => "TIMESTAMPTZ".to_string(),
        Type::DATE => "DATE".to_string(),
        Type::TIME => "TIME".to_string(),
        Type::TIMETZ => "TIMETZ".to_string(),
        Type::BYTEA => "BYTEA".to_string(),
        Type::UUID => "UUID".to_string(),
        Type::JSON => "JSON".to_string(),
        Type::JSONB => "JSONB".to_string(),
        _ => format!("UNKNOWN({})", pg_type.name()),
    }
}

#[allow(clippy::result_large_err)]
fn string_to_postgres_type(type_str: &str) -> Result<Type, BigQueryDestinationError> {
    match type_str {
        "BOOL" => Ok(Type::BOOL),
        "CHAR" => Ok(Type::CHAR),
        "INT2" => Ok(Type::INT2),
        "INT4" => Ok(Type::INT4),
        "INT8" => Ok(Type::INT8),
        "FLOAT4" => Ok(Type::FLOAT4),
        "FLOAT8" => Ok(Type::FLOAT8),
        "TEXT" => Ok(Type::TEXT),
        "VARCHAR" => Ok(Type::VARCHAR),
        "TIMESTAMP" => Ok(Type::TIMESTAMP),
        "TIMESTAMPTZ" => Ok(Type::TIMESTAMPTZ),
        "DATE" => Ok(Type::DATE),
        "TIME" => Ok(Type::TIME),
        "TIMETZ" => Ok(Type::TIMETZ),
        "BYTEA" => Ok(Type::BYTEA),
        "UUID" => Ok(Type::UUID),
        "JSON" => Ok(Type::JSON),
        "JSONB" => Ok(Type::JSONB),
        _ => {
            // For unknown types, we'll use TEXT as a fallback
            Ok(Type::TEXT)
        }
    }
}
