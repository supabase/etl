use futures::StreamExt;
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::storage::{ColumnMode, StorageApi};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    error::BQError,
    model::{
        query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
        query_parameter_value::QueryParameterValue, query_request::QueryRequest,
        query_response::ResultSet,
    },
    storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor},
    Client,
};
use postgres::schema::{ColumnSchema, TableSchema};
use std::{fmt, fs};
use tokio_postgres::types::Type;
use tracing::info;

use crate::conversions::table_row::TableRow;

/// The maximum number of byte that can be sent by stream.
const MAX_SIZE_BYTES: usize = 9 * 1024 * 1024;

/// The trace id of the client.
const ETL_TRACE_ID: &str = "ETL BigQueryClient";

/// A client for interacting with Google BigQuery.
///
/// This client provides methods for managing tables, inserting data,
/// and executing queries against a BigQuery project.
pub struct BigQueryClient {
    project_id: String,
    client: Client,
}

impl BigQueryClient {
    /// Creates a new [`BigQueryClient`] from a Google Cloud service account key file.
    ///
    /// Reads the service account key from the specified file path and uses it to
    /// authenticate with the BigQuery API.
    pub async fn new_with_key_path(
        project_id: String,
        sa_key_path: &str,
    ) -> Result<BigQueryClient, BQError> {
        let gcp_sa_key = fs::read_to_string(sa_key_path)?;
        let key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] from a Google Cloud service account key string.
    ///
    /// Parses the provided service account key string to authenticate with the
    /// BigQuery API.
    pub async fn new_with_key(project_id: String, sa_key: &str) -> Result<BigQueryClient, BQError> {
        let sa_key = parse_service_account_key(sa_key)?;
        let client = Client::from_service_account_key(sa_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] from a service-account JSON key and allows overriding
    /// the BigQuery endpoint URL—primarily useful for testing against emulators or mock servers.
    ///
    /// This override is intended only for integration tests and local development against
    /// non-Google BigQuery implementations or emulators.
    pub async fn new_with_custom_urls(
        project_id: String,
        auth_base_url: String,
        v2_base_url: String,
        sa_key: &str,
    ) -> Result<BigQueryClient, BQError> {
        let sa_key = parse_service_account_key(sa_key)?;
        let client = ClientBuilder::new()
            .with_auth_base_url(auth_base_url)
            .with_v2_base_url(v2_base_url)
            .build_from_service_account_key(sa_key, false)
            .await?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new table in the specified dataset if it does not already exist.
    ///
    /// Returns `true` if the table was created, and `false` if the table
    /// already existed.
    pub async fn create_table_if_missing(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<bool, BQError> {
        if self.table_exists(dataset_id, table_name).await? {
            return Ok(false);
        }

        self.create_table(dataset_id, table_name, column_schemas, max_staleness_mins)
            .await?;

        Ok(true)
    }

    /// Creates a table in a BigQuery dataset.
    pub async fn create_table(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<(), BQError> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let max_staleness_option = Self::max_staleness_option(max_staleness_mins);
        let project_id = self.project_id.as_str();

        info!("creating table {project_id}.{dataset_id}.{table_name} in bigquery");

        let query = format!(
            "create table `{}.{}.{}` {} {}",
            project_id, dataset_id, table_name, columns_spec, max_staleness_option
        );

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Checks if a table exists in the specified dataset.
    ///
    /// # Panics
    ///
    /// Panics if the query result does not contain the expected `table_exists` column.
    pub async fn table_exists(&self, dataset_id: &str, table_name: &str) -> Result<bool, BQError> {
        let query = format!(
            "select exists (select 1 from `{}.{}.INFORMATION_SCHEMA.TABLES` where table_name = @table_name) as table_exists",
            self.project_id, dataset_id
        );

        let mut request = QueryRequest::new(query);
        let parameter = QueryParameter {
            name: Some("table_name".to_string()),
            parameter_type: Some(QueryParameterType {
                r#type: "string".to_string(),
                array_type: None,
                struct_types: None,
            }),
            parameter_value: Some(QueryParameterValue {
                value: Some(table_name.to_string()),
                array_values: None,
                struct_values: None,
            }),
        };
        request.query_parameters = Some(vec![parameter]);

        let mut result_set = self.query(request).await?;

        let mut exists = false;
        if result_set.next_row() {
            exists = result_set
                .get_bool_by_name("table_exists")?
                .expect("no column named `table_exists` found in query result");
        }

        Ok(exists)
    }

    /// Streams rows to a BigQuery table using the Storage Write API.
    ///
    /// This method is efficient for high-throughput ingestion. It batches rows
    /// to respect the maximum request size.
    pub async fn stream_rows(
        &mut self,
        dataset_id: &str,
        table_name: String,
        table_descriptor: &TableDescriptor,
        table_rows: Vec<TableRow>,
    ) -> Result<(), BQError> {
        let mut table_rows = table_rows.as_slice();

        let default_stream = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );

        loop {
            let (rows, num_processed_rows) =
                StorageApi::create_rows(table_descriptor, table_rows, MAX_SIZE_BYTES);

            let mut response_stream = self
                .client
                .storage_mut()
                .append_rows(&default_stream, rows, ETL_TRACE_ID.to_owned())
                .await?;

            if let Some(r) = response_stream.next().await {
                let _ = r?;
            }

            table_rows = &table_rows[num_processed_rows..];
            if table_rows.is_empty() {
                break;
            }
        }

        Ok(())
    }

    /// Executes an SQL query and returns the result set.
    async fn query(&self, request: QueryRequest) -> Result<ResultSet, BQError> {
        let query_response = self.client.job().query(&self.project_id, request).await?;

        Ok(ResultSet::new_from_query_response(query_response))
    }

    /// Generates an SQL column specification for a `CREATE TABLE` statement.
    fn column_spec(column_schema: &ColumnSchema) -> String {
        let mut column_spec = format!(
            "`{}` {}",
            column_schema.name,
            Self::postgres_to_bigquery_type(&column_schema.typ)
        );

        if !column_schema.nullable && !Self::is_array_type(&column_schema.typ) {
            column_spec.push_str(" not null");
        };

        column_spec
    }

    /// Appends a `PRIMARY KEY` clause to a `CREATE TABLE` statement string.
    fn add_primary_key_clause(column_schemas: &[ColumnSchema]) -> String {
        let identity_columns: Vec<String> = column_schemas
            .iter()
            .filter(|s| s.primary)
            .map(|c| format!("`{}`", c.name))
            .collect();

        if identity_columns.is_empty() {
            return "".to_string();
        }

        format!(", primary key ({})", identity_columns.join(","))
    }

    /// Creates the full column specification clause for a `CREATE TABLE` statement.
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = column_schemas
            .iter()
            .map(Self::column_spec)
            .collect::<Vec<_>>()
            .join(",");

        s.push_str(&Self::add_primary_key_clause(column_schemas));

        format!("({})", s)
    }

    /// Creates the `OPTIONS` clause for specifying max staleness in a `CREATE TABLE` statement.
    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!(
            "options (max_staleness = interval {} minute)",
            max_staleness_mins
        )
    }

    /// Maps a PostgreSQL [`Type`] to a BigQuery data type name.
    fn postgres_to_bigquery_type(typ: &Type) -> String {
        if Self::is_array_type(typ) {
            let element_type = match typ {
                &Type::BOOL_ARRAY => "bool",
                &Type::CHAR_ARRAY
                | &Type::BPCHAR_ARRAY
                | &Type::VARCHAR_ARRAY
                | &Type::NAME_ARRAY
                | &Type::TEXT_ARRAY => "string",
                &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "int64",
                &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => "float64",
                &Type::NUMERIC_ARRAY => "bignumeric",
                &Type::DATE_ARRAY => "date",
                &Type::TIME_ARRAY => "time",
                &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "timestamp",
                &Type::UUID_ARRAY => "string",
                &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "json",
                &Type::OID_ARRAY => "int64",
                &Type::BYTEA_ARRAY => "bytes",
                _ => "string",
            };

            return format!("array<{}>", element_type);
        }

        match typ {
            &Type::BOOL => "bool",
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "string",
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "int64",
            &Type::FLOAT4 | &Type::FLOAT8 => "float64",
            &Type::NUMERIC => "bignumeric",
            &Type::DATE => "date",
            &Type::TIME => "time",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "timestamp",
            &Type::UUID => "string",
            &Type::JSON | &Type::JSONB => "json",
            &Type::OID => "int64",
            &Type::BYTEA => "bytes",
            _ => "string",
        }
        .to_string()
    }

    /// Checks if a PostgreSQL [`Type`] is an array type.
    fn is_array_type(typ: &Type) -> bool {
        matches!(
            typ,
            &Type::BOOL_ARRAY
                | &Type::CHAR_ARRAY
                | &Type::BPCHAR_ARRAY
                | &Type::VARCHAR_ARRAY
                | &Type::NAME_ARRAY
                | &Type::TEXT_ARRAY
                | &Type::INT2_ARRAY
                | &Type::INT4_ARRAY
                | &Type::INT8_ARRAY
                | &Type::FLOAT4_ARRAY
                | &Type::FLOAT8_ARRAY
                | &Type::NUMERIC_ARRAY
                | &Type::DATE_ARRAY
                | &Type::TIME_ARRAY
                | &Type::TIMESTAMP_ARRAY
                | &Type::TIMESTAMPTZ_ARRAY
                | &Type::UUID_ARRAY
                | &Type::JSON_ARRAY
                | &Type::JSONB_ARRAY
                | &Type::OID_ARRAY
                | &Type::BYTEA_ARRAY
        )
    }

    /// Converts a PostgreSQL [`TableSchema`] to a BigQuery [`TableDescriptor`].
    ///
    /// This conversion is necessary for using the BigQuery Storage Write API.
    /// It maps PostgreSQL data types to their corresponding BigQuery counterparts
    /// and sets the appropriate mode (e.g., `NULLABLE`, `REQUIRED`, `REPEATED`).
    ///
    /// This function is defined here and doesn't use the [`From`] trait because
    /// [`TableSchema`] is in another crate, and we don't want to pollute the
    /// `postgres` crate with destination-specific internals.
    pub fn table_schema_to_descriptor(table_schema: &TableSchema) -> TableDescriptor {
        let mut field_descriptors = Vec::with_capacity(table_schema.column_schemas.len());
        let mut number = 1;
        for column_schema in &table_schema.column_schemas {
            let typ = match column_schema.typ {
                Type::BOOL => ColumnType::Bool,
                Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                    ColumnType::String
                }
                Type::INT2 => ColumnType::Int32,
                Type::INT4 => ColumnType::Int32,
                Type::INT8 => ColumnType::Int64,
                Type::FLOAT4 => ColumnType::Float,
                Type::FLOAT8 => ColumnType::Double,
                Type::NUMERIC => ColumnType::String,
                Type::DATE => ColumnType::String,
                Type::TIME => ColumnType::String,
                Type::TIMESTAMP => ColumnType::String,
                Type::TIMESTAMPTZ => ColumnType::String,
                Type::UUID => ColumnType::String,
                Type::JSON => ColumnType::String,
                Type::JSONB => ColumnType::String,
                Type::OID => ColumnType::Int32,
                Type::BYTEA => ColumnType::Bytes,
                Type::BOOL_ARRAY => ColumnType::Bool,
                Type::CHAR_ARRAY
                | Type::BPCHAR_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::NAME_ARRAY
                | Type::TEXT_ARRAY => ColumnType::String,
                Type::INT2_ARRAY => ColumnType::Int32,
                Type::INT4_ARRAY => ColumnType::Int32,
                Type::INT8_ARRAY => ColumnType::Int64,
                Type::FLOAT4_ARRAY => ColumnType::Float,
                Type::FLOAT8_ARRAY => ColumnType::Double,
                Type::NUMERIC_ARRAY => ColumnType::String,
                Type::DATE_ARRAY => ColumnType::String,
                Type::TIME_ARRAY => ColumnType::String,
                Type::TIMESTAMP_ARRAY => ColumnType::String,
                Type::TIMESTAMPTZ_ARRAY => ColumnType::String,
                Type::UUID_ARRAY => ColumnType::String,
                Type::JSON_ARRAY => ColumnType::String,
                Type::JSONB_ARRAY => ColumnType::String,
                Type::OID_ARRAY => ColumnType::Int32,
                Type::BYTEA_ARRAY => ColumnType::Bytes,
                _ => ColumnType::String,
            };

            let mode = if Self::is_array_type(&column_schema.typ) {
                ColumnMode::Repeated
            } else if column_schema.nullable {
                ColumnMode::Nullable
            } else {
                ColumnMode::Required
            };

            field_descriptors.push(FieldDescriptor {
                number,
                name: column_schema.name.clone(),
                typ,
                mode,
            });
            number += 1;
        }

        field_descriptors.push(FieldDescriptor {
            number,
            name: "_CHANGE_TYPE".to_string(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        });

        TableDescriptor { field_descriptors }
    }
}

impl fmt::Debug for BigQueryClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigQueryClient")
            .field("project_id", &self.project_id)
            .finish()
    }
}
