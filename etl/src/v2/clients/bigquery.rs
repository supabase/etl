use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::StreamExt;
use gcp_bigquery_client::storage::{ColumnMode, StorageApi};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    error::BQError,
    google::cloud::bigquery::storage::v1::{WriteStream, WriteStreamView},
    model::{
        query_request::QueryRequest, query_response::ResultSet,
        table_data_insert_all_request::TableDataInsertAllRequest,
    },
    storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor},
    Client,
};
use postgres::schema::{ColumnSchema, TableId, TableSchema};
use std::{collections::HashSet, fmt, fs};
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::conversions::table_row::TableRow;
use crate::conversions::Cell;

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

//TODO: fix all SQL injections
impl BigQueryClient {
    /// Creates a new [`BigQueryClient`] from a Google Cloud service account key file.
    ///
    /// Reads the service account key from the specified file path and uses it to
    /// authenticate with the BigQuery API.
    pub async fn new_with_key_path(
        project_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQueryClient, BQError> {
        let gcp_sa_key = fs::read_to_string(gcp_sa_key_path)?;
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] from a Google Cloud service account key string.
    ///
    /// Parses the provided service account key string to authenticate with the
    /// BigQuery API.
    pub async fn new_with_key(
        project_id: String,
        gcp_sa_key: &str,
    ) -> Result<BigQueryClient, BQError> {
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

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

    /// Maps a PostgreSQL [`Type`] to a BigQuery data type name.
    fn postgres_to_bigquery_type(typ: &Type) -> &'static str {
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
            &Type::BOOL_ARRAY => "array<bool>",
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY => "array<string>",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "array<int64>",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => "array<float64>",
            &Type::NUMERIC_ARRAY => "array<bignumeric>",
            &Type::DATE_ARRAY => "array<date>",
            &Type::TIME_ARRAY => "array<time>",
            &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "array<timestamp>",
            &Type::UUID_ARRAY => "array<string>",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "array<json>",
            &Type::OID_ARRAY => "array<int64>",
            &Type::BYTEA_ARRAY => "array<bytes>",
            _ => "string",
        }
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

    /// Generates an SQL column specification for a `CREATE TABLE` statement.
    fn column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push('`');
        s.push_str(&column_schema.name);
        s.push('`');
        s.push(' ');
        let typ = Self::postgres_to_bigquery_type(&column_schema.typ);
        s.push_str(typ);
        if !column_schema.nullable && !Self::is_array_type(&column_schema.typ) {
            s.push_str(" not null");
        };
    }

    /// Appends a `PRIMARY KEY` clause to a `CREATE TABLE` statement string.
    fn add_primary_key_clause(column_schemas: &[ColumnSchema], s: &mut String) {
        let identity_columns = column_schemas.iter().filter(|s| s.primary);

        s.push_str("primary key (");

        for column in identity_columns {
            s.push('`');
            s.push_str(&column.name);
            s.push('`');
            s.push(',');
        }

        s.pop(); //','
        s.push_str(") not enforced");
    }

    /// Creates the full column specification clause for a `CREATE TABLE` statement.
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();
        s.push('(');

        for column_schema in column_schemas.iter() {
            Self::column_spec(column_schema, &mut s);
            s.push(',');
        }

        let has_identity_cols = column_schemas.iter().any(|s| s.primary);
        if has_identity_cols {
            Self::add_primary_key_clause(column_schemas, &mut s);
        } else {
            s.pop(); //','
        }

        s.push(')');

        s
    }

    /// Creates the `OPTIONS` clause for specifying max staleness in a `CREATE TABLE` statement.
    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!("options (max_staleness = interval {max_staleness_mins} minute)")
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

        let query =
            format!("create table `{project_id}.{dataset_id}.{table_name}` {columns_spec} {max_staleness_option}",);

        let _ = self.query(query).await?;

        Ok(())
    }

    /// Checks if a table exists in the specified dataset.
    ///
    /// # Panics
    ///
    /// Panics if the query result does not contain the expected `table_exists` column.
    pub async fn table_exists(&self, dataset_id: &str, table_name: &str) -> Result<bool, BQError> {
        let query = format!(
            "select exists
                (
                    select * from
                    {dataset_id}.INFORMATION_SCHEMA.TABLES
                    where table_name = '{table_name}'
                ) as table_exists;",
        );

        let mut result_set = self.query(query).await?;

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

    /// Converts a [`Cell`] value into its SQL literal representation for a query.
    fn cell_to_query_value(cell: &Cell, s: &mut String) {
        match cell {
            Cell::Null => s.push_str("null"),
            Cell::Bool(b) => s.push_str(&format!("{b}")),
            Cell::String(str) => s.push_str(&format!("'{str}'")),
            Cell::I16(i) => s.push_str(&format!("{i}")),
            Cell::I32(i) => s.push_str(&format!("{i}")),
            Cell::I64(i) => s.push_str(&format!("{i}")),
            Cell::F32(i) => s.push_str(&format!("{i}")),
            Cell::F64(i) => s.push_str(&format!("{i}")),
            Cell::Numeric(n) => s.push_str(&format!("{n}")),
            Cell::Date(t) => s.push_str(&format!("'{t}'")),
            Cell::Time(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStamp(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStampTz(t) => s.push_str(&format!("'{t}'")),
            Cell::Uuid(t) => s.push_str(&format!("'{t}'")),
            Cell::Json(j) => s.push_str(&format!("'{j}'")),
            Cell::U32(u) => s.push_str(&format!("{u}")),
            Cell::Bytes(b) => {
                let bytes: String = b.iter().map(|b| *b as char).collect();
                s.push_str(&format!("b'{bytes}'"))
            }
            Cell::Array(_) => unreachable!(),
        }
    }

    /// Executes an SQL query and returns the result set.
    async fn query(&self, query: String) -> Result<ResultSet, BQError> {
        let query_response = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        Ok(ResultSet::new_from_query_response(query_response))
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

            let mode = match column_schema.typ {
                Type::BOOL_ARRAY
                | Type::CHAR_ARRAY
                | Type::BPCHAR_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::NAME_ARRAY
                | Type::TEXT_ARRAY
                | Type::INT2_ARRAY
                | Type::INT4_ARRAY
                | Type::INT8_ARRAY
                | Type::FLOAT4_ARRAY
                | Type::FLOAT8_ARRAY
                | Type::NUMERIC_ARRAY
                | Type::DATE_ARRAY
                | Type::TIME_ARRAY
                | Type::TIMESTAMP_ARRAY
                | Type::TIMESTAMPTZ_ARRAY
                | Type::UUID_ARRAY
                | Type::JSON_ARRAY
                | Type::JSONB_ARRAY
                | Type::OID_ARRAY
                | Type::BYTEA_ARRAY => ColumnMode::Repeated,
                _ => {
                    if column_schema.nullable {
                        ColumnMode::Nullable
                    } else {
                        ColumnMode::Required
                    }
                }
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
