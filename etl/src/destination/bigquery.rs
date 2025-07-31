use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::storage::TableDescriptor;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use tokio_postgres::types::{PgLsn, Type};
use tracing::{debug, info, warn};

use crate::clients::bigquery::{BigQueryClient, BigQueryOperationType};
use crate::conversions::Cell;
use crate::conversions::event::Event;
use crate::conversions::table_row::TableRow;
use crate::destination::base::Destination;
use crate::error::{ErrorKind, EtlError, EtlResult};
use crate::schema::cache::SchemaCache;

/// Table name for storing ETL table schema metadata in BigQuery.
const ETL_TABLE_SCHEMAS_NAME: &str = "etl_table_schemas";

/// Column name constants for the ETL table schemas metadata table.
const TABLE_ID_COLUMN: &str = "table_id";
const SCHEMA_NAME_COLUMN: &str = "schema_name";
const TABLE_NAME_COLUMN: &str = "table_name";

/// Column definitions for the ETL table schemas metadata table.
/// Defines the structure used to store table-level schema information in BigQuery.
static ETL_TABLE_SCHEMAS_COLUMNS: LazyLock<Vec<ColumnSchema>> = LazyLock::new(|| {
    vec![
        ColumnSchema::new(TABLE_ID_COLUMN.to_string(), Type::INT8, -1, false, true),
        ColumnSchema::new(SCHEMA_NAME_COLUMN.to_string(), Type::TEXT, -1, false, false),
        ColumnSchema::new(TABLE_NAME_COLUMN.to_string(), Type::TEXT, -1, false, false),
    ]
});

/// Table name for storing ETL column schema metadata in BigQuery.
const ETL_TABLE_COLUMNS_NAME: &str = "etl_table_columns";

/// Column name constants for the ETL table columns metadata table.
const COLUMN_NAME_COLUMN: &str = "column_name";
const COLUMN_TYPE_COLUMN: &str = "column_type";
const TYPE_MODIFIER_COLUMN: &str = "type_modifier";
const NULLABLE_COLUMN: &str = "nullable";
const PRIMARY_KEY_COLUMN: &str = "primary_key";
const COLUMN_ORDER_COLUMN: &str = "column_order";

/// Column definitions for the ETL table columns metadata table.
/// Defines the structure used to store column-level schema information in BigQuery.
static ETL_TABLE_COLUMNS_COLUMNS: LazyLock<Vec<ColumnSchema>> = LazyLock::new(|| {
    vec![
        ColumnSchema::new(TABLE_ID_COLUMN.to_string(), Type::INT8, -1, false, true),
        ColumnSchema::new(COLUMN_NAME_COLUMN.to_string(), Type::TEXT, -1, false, true),
        ColumnSchema::new(COLUMN_TYPE_COLUMN.to_string(), Type::TEXT, -1, false, false),
        ColumnSchema::new(
            TYPE_MODIFIER_COLUMN.to_string(),
            Type::INT4,
            -1,
            false,
            false,
        ),
        ColumnSchema::new(NULLABLE_COLUMN.to_string(), Type::BOOL, -1, false, false),
        ColumnSchema::new(PRIMARY_KEY_COLUMN.to_string(), Type::BOOL, -1, false, false),
        ColumnSchema::new(
            COLUMN_ORDER_COLUMN.to_string(),
            Type::INT4,
            -1,
            false,
            false,
        ),
    ]
});

/// Internal state for [`BigQueryDestination`] wrapped in `Arc<Mutex<>>`.
///
/// Contains the BigQuery client, dataset configuration, and injected schema cache.
#[derive(Debug)]
struct Inner {
    client: BigQueryClient,
    dataset_id: String,
    max_staleness_mins: Option<u16>,
    schema_cache: Option<SchemaCache>,
}

impl Inner {
    /// Ensures the ETL metadata tables exist in BigQuery.
    ///
    /// Creates `etl_table_schemas` and `etl_table_columns` tables if they don't exist.
    async fn ensure_schema_tables_exist(&self) -> EtlResult<()> {
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

/// A BigQuery destination that implements the ETL [`Destination`] trait.
///
/// Provides PostgreSQL-to-BigQuery data pipeline functionality including schema mapping,
/// table creation, streaming inserts, and CDC operation handling. Maintains schema metadata
/// in dedicated BigQuery tables for persistence across pipeline runs.
///
/// The destination creates and manages two metadata tables:
/// - `etl_table_schemas`: Stores table-level schema information
/// - `etl_table_columns`: Stores column-level schema details
#[derive(Debug, Clone)]
pub struct BigQueryDestination {
    inner: Arc<Mutex<Inner>>,
}

impl BigQueryDestination {
    /// Creates a new [`BigQueryDestination`] using a service account key file path.
    ///
    /// Initializes the BigQuery client with the provided credentials and project settings.
    /// The `max_staleness_mins` parameter controls table metadata cache freshness.
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: String,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_key_path(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache: None,
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Creates a new [`BigQueryDestination`] using a service account key JSON string.
    ///
    /// Similar to [`BigQueryDestination::new_with_key_path`] but accepts the key content directly
    /// rather than a file path. Useful when credentials are stored in environment variables.
    pub async fn new_with_key(
        project_id: String,
        dataset_id: String,
        sa_key: &str,
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<Self> {
        let client = BigQueryClient::new_with_key(project_id, sa_key).await?;
        let inner = Inner {
            client,
            dataset_id,
            max_staleness_mins,
            schema_cache: None,
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Loads BigQuery table ID and descriptor that are used for streaming operations.
    ///
    /// Returns the BigQuery-formatted table name and its column descriptor for streaming operations.
    async fn load_table_id_and_descriptor<I: Deref<Target = Inner>>(
        inner: &I,
        table_id: &TableId,
        use_cdc_sequence_column: bool,
    ) -> EtlResult<(String, TableDescriptor)> {
        let schema_cache = inner
            .schema_cache
            .as_ref()
            .ok_or_else(|| {
                EtlError::from((
                    ErrorKind::ConfigError,
                    "The schema cache was not set on the destination",
                ))
            })?
            .lock_inner()
            .await;
        let table_schema = schema_cache.get_table_schema_ref(table_id).ok_or_else(|| {
            EtlError::from((
                ErrorKind::DestinationSchemaError,
                "Table schema not found in schema cache",
                format!("table_id: {table_id}"),
            ))
        })?;

        let table_id = table_schema.name.as_bigquery_table_id();
        let table_descriptor = BigQueryClient::column_schemas_to_table_descriptor(
            &table_schema.column_schemas,
            use_cdc_sequence_column,
        );

        Ok((table_id, table_descriptor))
    }

    /// Writes a table schema to BigQuery, creating the data table and storing metadata.
    ///
    /// This method creates the actual data table and inserts schema information into the ETL metadata tables.
    async fn write_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

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
        let mut table_schema_row = Self::table_schema_to_table_row(&table_schema);
        table_schema_row
            .values
            .push(BigQueryOperationType::UPSERT.into_cell());
        let table_schema_descriptor =
            BigQueryClient::column_schemas_to_table_descriptor(&ETL_TABLE_SCHEMAS_COLUMNS, false);
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
        let mut column_rows = Self::column_schemas_to_table_rows(&table_schema);
        for column_row in column_rows.iter_mut() {
            column_row
                .values
                .push(BigQueryOperationType::UPSERT.into_cell());
        }
        let column_descriptors =
            BigQueryClient::column_schemas_to_table_descriptor(&ETL_TABLE_COLUMNS_COLUMNS, false);
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

        debug!("wrote table schema for table '{}'", table_schema.name);

        Ok(())
    }

    /// Loads all table schemas from BigQuery ETL metadata tables.
    ///
    /// Reconstructs [`TableSchema`] objects by joining data from schema and column metadata tables.
    async fn load_table_schemas(&self) -> EtlResult<Vec<TableSchema>> {
        let inner = self.inner.lock().await;

        // First check if schema tables exist
        let tables_exist = inner
            .client
            .table_exists(&inner.dataset_id, "etl_table_schemas")
            .await?;
        if !tables_exist {
            return Ok(vec![]);
        }

        // Query to get table schemas with their columns
        let table_schemas_full_table_name = inner
            .client
            .full_table_name(&inner.dataset_id, ETL_TABLE_SCHEMAS_NAME);
        let table_columns_full_table_name = inner
            .client
            .full_table_name(&inner.dataset_id, ETL_TABLE_COLUMNS_NAME);
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
            from {table_schemas_full_table_name} ts
            join {table_columns_full_table_name} tc on ts.table_id = tc.table_id
            order by ts.table_id, tc.column_order
            "#
        );

        let mut query_results = inner.client.query(QueryRequest::new(query)).await?;

        let mut table_schemas = HashMap::new();

        while query_results.next_row() {
            let table_id: u32 = query_results
                .get_i64_by_name(TABLE_ID_COLUMN)
                .map(|opt| opt.unwrap_or(0) as u32)
                .unwrap_or(0);

            let schema_name: String = query_results
                .get_string_by_name(SCHEMA_NAME_COLUMN)
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let table_name: String = query_results
                .get_string_by_name(TABLE_NAME_COLUMN)
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let column_name: String = query_results
                .get_string_by_name(COLUMN_NAME_COLUMN)
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let column_type_str: String = query_results
                .get_string_by_name(COLUMN_TYPE_COLUMN)
                .map(|opt| opt.unwrap_or_default())
                .unwrap_or_default();

            let type_modifier: i32 = query_results
                .get_i64_by_name(TYPE_MODIFIER_COLUMN)
                .map(|opt| opt.unwrap_or(-1) as i32)
                .unwrap_or(-1);

            let nullable: bool = query_results
                .get_bool_by_name(NULLABLE_COLUMN)
                .map(|opt| opt.unwrap_or(true))
                .unwrap_or(true);

            let primary_key: bool = query_results
                .get_bool_by_name(PRIMARY_KEY_COLUMN)
                .map(|opt| opt.unwrap_or(false))
                .unwrap_or(false);

            let column_type = Self::string_to_postgres_type(&column_type_str)?;
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
            let table_schema = TableSchema::new(TableId::new(table_id), table_name, column_schemas);
            result.push(table_schema);
        }

        Ok(result)
    }

    /// Writes data rows to a BigQuery table, adding CDC mode metadata.
    ///
    /// Each row gets a CDC mode column and sequence number appended before streaming to BigQuery.
    async fn write_table_rows(
        &self,
        table_id: TableId,
        mut table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        // We do not use the sequence column for table rows, since we assume that table rows are always
        // unique by primary key, thus there is no need to order them.
        let (table_id, table_descriptor) =
            Self::load_table_id_and_descriptor(&inner, &table_id, false).await?;

        let dataset_id = inner.dataset_id.clone();
        for table_row in table_rows.iter_mut() {
            table_row
                .values
                .push(BigQueryOperationType::UPSERT.into_cell());
        }
        inner
            .client
            .stream_rows(&dataset_id, table_id, &table_descriptor, table_rows)
            .await?;

        Ok(())
    }

    /// Processes and writes a batch of CDC events to BigQuery.
    ///
    /// Groups events by type, handles inserts/updates/deletes via streaming, and processes truncates separately.
    /// Adds sequence numbers to ensure proper ordering of events with the same system time.
    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
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
                        let sequence_number =
                            Self::generate_sequence_number(insert.start_lsn, insert.commit_lsn);
                        insert
                            .table_row
                            .values
                            .push(BigQueryOperationType::UPSERT.into_cell());
                        insert.table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(insert.table_id).or_default();
                        table_rows.push(insert.table_row);
                    }
                    Event::Update(mut update) => {
                        let sequence_number =
                            Self::generate_sequence_number(update.start_lsn, update.commit_lsn);
                        update
                            .table_row
                            .values
                            .push(BigQueryOperationType::UPSERT.into_cell());
                        update.table_row.values.push(Cell::String(sequence_number));

                        let table_rows: &mut Vec<TableRow> =
                            table_id_to_table_rows.entry(update.table_id).or_default();
                        table_rows.push(update.table_row);
                    }
                    Event::Delete(delete) => {
                        let Some((_, mut old_table_row)) = delete.old_table_row else {
                            info!("the `DELETE` event has no row, so it was skipped");
                            continue;
                        };

                        let sequence_number =
                            Self::generate_sequence_number(delete.start_lsn, delete.commit_lsn);
                        old_table_row
                            .values
                            .push(BigQueryOperationType::DELETE.into_cell());
                        old_table_row.values.push(Cell::String(sequence_number));

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
                let mut inner = self.inner.lock().await;

                for (table_id, table_rows) in table_id_to_table_rows {
                    let (table_id, table_descriptor) =
                        Self::load_table_id_and_descriptor(&inner, &table_id, true).await?;

                    let dataset_id = inner.dataset_id.clone();
                    inner
                        .client
                        .stream_rows(&dataset_id, table_id, &table_descriptor, table_rows)
                        .await?;
                }
            }

            // Collect all consecutive truncate events
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    truncate_events.push(truncate_event);
                }
            }

            // Process truncate events
            if !truncate_events.is_empty() {
                // Right now we are not processing truncate messages, but we do the streaming split
                // just to try out if splitting the streaming affects performance so that we might
                // need it down the line once we figure out a solution for truncation.
                warn!(
                    "'TRUNCATE' events are not supported, skipping apply of {} 'TRUNCATE' events",
                    truncate_events.len()
                );
            }
        }

        Ok(())
    }

    /// Converts a [`TableSchema`] to a [`TableRow`] for insertion into the ETL metadata table.
    ///
    /// Extracts table ID, schema name, and table name for storage in `etl_table_schemas`.
    fn table_schema_to_table_row(table_schema: &TableSchema) -> TableRow {
        let columns = vec![
            Cell::U32(table_schema.id.into()),
            Cell::String(table_schema.name.schema.clone()),
            Cell::String(table_schema.name.name.clone()),
        ];

        TableRow::new(columns)
    }

    /// Converts column schemas from a [`TableSchema`] to [`TableRow`] objects for metadata storage.
    ///
    /// Creates one row per column for insertion into the `etl_table_columns` table.
    fn column_schemas_to_table_rows(table_schema: &TableSchema) -> Vec<TableRow> {
        let mut table_rows = Vec::with_capacity(table_schema.column_schemas.len());

        for (column_order, column_schema) in table_schema.column_schemas.iter().enumerate() {
            let columns = vec![
                Cell::U32(table_schema.id.into()),
                Cell::String(column_schema.name.clone()),
                Cell::String(Self::postgres_type_to_string(&column_schema.typ)),
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

    /// Converts a PostgreSQL [`Type`] to its string representation for metadata storage.
    ///
    /// Maps common PostgreSQL types to their string equivalents, with fallback for unknown types.
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

    /// Converts a string representation back to a PostgreSQL [`Type`].
    ///
    /// Used when reconstructing schemas from BigQuery metadata tables. Falls back to `TEXT` for unknown types.
    fn string_to_postgres_type(type_str: &str) -> EtlResult<Type> {
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

    /// Generates a sequence number from the LSNs of an event.
    ///
    /// Creates a hex-encoded sequence number that ensures events are processed in the correct order
    /// even when they have the same system time. The format is compatible with BigQuery's
    /// `_CHANGE_SEQUENCE_NUMBER` column requirements.
    ///
    /// The rationale for using the LSN is that BigQuery will preserve the highest sequence number
    /// in case of equal primary key, which is what we want since in case of updates, we want the
    /// latest update in Postgres order to be the winner. We have first the `commit_lsn` in the key
    /// so that BigQuery can first order operations based on the LSN at which the transaction committed
    /// and if two operations belong to the same transaction (meaning they have the same LSN), the
    /// `start_lsn` will be used. We first order by `commit_lsn` to preserve the order in which operations
    /// are received by the pipeline since transactions are ordered by commit time and not interleaved.
    fn generate_sequence_number(start_lsn: PgLsn, commit_lsn: PgLsn) -> String {
        let start_lsn = u64::from(start_lsn);
        let commit_lsn = u64::from(commit_lsn);

        format!("{commit_lsn:016x}/{start_lsn:016x}")
    }
}

impl Destination for BigQueryDestination {
    async fn inject(&self, schema_cache: SchemaCache) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner.schema_cache = Some(schema_cache);

        Ok(())
    }

    async fn write_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        self.write_table_schema(table_schema).await?;

        Ok(())
    }

    async fn load_table_schemas(&self) -> EtlResult<Vec<TableSchema>> {
        let table_schemas = self.load_table_schemas().await?;

        Ok(table_schemas)
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows(table_id, table_rows).await?;

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events(events).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use postgres::schema::{ColumnSchema, TableName, TableSchema};
    use tokio_postgres::types::Type;

    use crate::conversions::Cell;

    use super::*;

    #[test]
    fn test_postgres_type_to_string() {
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::BOOL),
            "BOOL"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::INT4),
            "INT4"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::INT8),
            "INT8"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::TEXT),
            "TEXT"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::VARCHAR),
            "VARCHAR"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::TIMESTAMP),
            "TIMESTAMP"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::TIMESTAMPTZ),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::DATE),
            "DATE"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::TIME),
            "TIME"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::BYTEA),
            "BYTEA"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::UUID),
            "UUID"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::JSON),
            "JSON"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::JSONB),
            "JSONB"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::FLOAT4),
            "FLOAT4"
        );
        assert_eq!(
            BigQueryDestination::postgres_type_to_string(&Type::FLOAT8),
            "FLOAT8"
        );
    }

    #[test]
    fn test_string_to_postgres_type() {
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("BOOL").unwrap(),
            Type::BOOL
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("INT4").unwrap(),
            Type::INT4
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("INT8").unwrap(),
            Type::INT8
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("TEXT").unwrap(),
            Type::TEXT
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("VARCHAR").unwrap(),
            Type::VARCHAR
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("TIMESTAMP").unwrap(),
            Type::TIMESTAMP
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("TIMESTAMPTZ").unwrap(),
            Type::TIMESTAMPTZ
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("DATE").unwrap(),
            Type::DATE
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("TIME").unwrap(),
            Type::TIME
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("BYTEA").unwrap(),
            Type::BYTEA
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("UUID").unwrap(),
            Type::UUID
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("JSON").unwrap(),
            Type::JSON
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("JSONB").unwrap(),
            Type::JSONB
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("FLOAT4").unwrap(),
            Type::FLOAT4
        );
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("FLOAT8").unwrap(),
            Type::FLOAT8
        );

        // Test unknown type fallback
        assert_eq!(
            BigQueryDestination::string_to_postgres_type("UNKNOWN_TYPE").unwrap(),
            Type::TEXT
        );
    }

    #[test]
    fn test_column_type_roundtrip() {
        let types = vec![
            Type::BOOL,
            Type::CHAR,
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
            Type::TEXT,
            Type::VARCHAR,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::DATE,
            Type::TIME,
            Type::TIMETZ,
            Type::BYTEA,
            Type::UUID,
            Type::JSON,
            Type::JSONB,
        ];

        for pg_type in types {
            let type_string = BigQueryDestination::postgres_type_to_string(&pg_type);
            let converted_back =
                BigQueryDestination::string_to_postgres_type(&type_string).unwrap();
            assert_eq!(
                pg_type, converted_back,
                "Type conversion failed for: {pg_type:?}"
            );
        }
    }

    #[test]
    fn test_table_schema_with_mixed_column_types() {
        let table_name = TableName::new("test_schema".to_string(), "mixed_table".to_string());
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT8, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::VARCHAR, 255, true, false),
            ColumnSchema::new(
                "created_at".to_string(),
                Type::TIMESTAMPTZ,
                -1,
                false,
                false,
            ),
            ColumnSchema::new("data".to_string(), Type::JSONB, -1, true, false),
            ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
        ];
        let table_schema = TableSchema::new(TableId::new(456), table_name, columns);

        let schema_row = BigQueryDestination::table_schema_to_table_row(&table_schema);
        assert_eq!(schema_row.values[0], Cell::U32(456));
        assert_eq!(
            schema_row.values[1],
            Cell::String("test_schema".to_string())
        );
        assert_eq!(
            schema_row.values[2],
            Cell::String("mixed_table".to_string())
        );

        let column_rows = BigQueryDestination::column_schemas_to_table_rows(&table_schema);
        assert_eq!(column_rows.len(), 5);

        // Verify column order is preserved
        assert_eq!(column_rows[0].values[1], Cell::String("id".to_string()));
        assert_eq!(column_rows[1].values[1], Cell::String("name".to_string()));
        assert_eq!(
            column_rows[2].values[1],
            Cell::String("created_at".to_string())
        );
        assert_eq!(column_rows[3].values[1], Cell::String("data".to_string()));
        assert_eq!(column_rows[4].values[1], Cell::String("active".to_string()));

        // Verify column order values
        assert_eq!(column_rows[0].values[6], Cell::U32(0));
        assert_eq!(column_rows[1].values[6], Cell::U32(1));
        assert_eq!(column_rows[2].values[6], Cell::U32(2));
        assert_eq!(column_rows[3].values[6], Cell::U32(3));
        assert_eq!(column_rows[4].values[6], Cell::U32(4));
    }

    #[test]
    fn test_generate_sequence_number() {
        assert_eq!(
            BigQueryDestination::generate_sequence_number(PgLsn::from(0), PgLsn::from(0)),
            "0000000000000000/0000000000000000"
        );
        assert_eq!(
            BigQueryDestination::generate_sequence_number(PgLsn::from(1), PgLsn::from(0)),
            "0000000000000000/0000000000000001"
        );
        assert_eq!(
            BigQueryDestination::generate_sequence_number(PgLsn::from(255), PgLsn::from(0)),
            "0000000000000000/00000000000000ff"
        );
        assert_eq!(
            BigQueryDestination::generate_sequence_number(PgLsn::from(65535), PgLsn::from(0)),
            "0000000000000000/000000000000ffff"
        );
        assert_eq!(
            BigQueryDestination::generate_sequence_number(PgLsn::from(u64::MAX), PgLsn::from(0)),
            "0000000000000000/ffffffffffffffff"
        );
    }
}
