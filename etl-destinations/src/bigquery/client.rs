use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{Cell, ColumnSchema, TableRow, Type};
use futures::StreamExt;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::RowError;
use gcp_bigquery_client::storage::{ColumnMode, StorageApi};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    Client,
    error::BQError,
    model::{query_request::QueryRequest, query_response::ResultSet},
    storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor},
};
use std::fmt;
use tracing::info;

use crate::bigquery::encoding::BigQueryTableRow;

/// Maximum byte size for streaming data to BigQuery.
const MAX_SIZE_BYTES: usize = 9 * 1024 * 1024;

/// Trace identifier for ETL operations in BigQuery client.
const ETL_TRACE_ID: &str = "ETL BigQueryClient";

/// Special column name for Change Data Capture operations in BigQuery.
const BIGQUERY_CDC_SPECIAL_COLUMN: &str = "_CHANGE_TYPE";

/// Special column name for Change Data Capture sequence ordering in BigQuery.
const BIGQUERY_CDC_SEQUENCE_COLUMN: &str = "_CHANGE_SEQUENCE_NUMBER";

pub type BigQueryProjectId = String;
pub type BigQueryDatasetId = String;
pub type BigQueryTableId = String;

/// Change Data Capture operation types for BigQuery streaming.
#[derive(Debug)]
pub enum BigQueryOperationType {
    Upsert,
    Delete,
}

impl BigQueryOperationType {
    /// Converts the [`BigQueryOperationType`] into a [`Cell`] value.
    pub fn into_cell(self) -> Cell {
        Cell::String(self.to_string())
    }
}

impl fmt::Display for BigQueryOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BigQueryOperationType::Upsert => write!(f, "UPSERT"),
            BigQueryOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// A client for interacting with Google BigQuery.
///
/// This client provides methods for managing tables, inserting data,
/// and executing queries against a BigQuery project.
pub struct BigQueryClient {
    project_id: BigQueryProjectId,
    client: Client,
}

impl BigQueryClient {
    /// Creates a new [`BigQueryClient`] from a Google Cloud service account key file.
    ///
    /// Reads the service account key from the specified file path and uses it to
    /// authenticate with the BigQuery API.
    pub async fn new_with_key_path(
        project_id: BigQueryProjectId,
        sa_key_path: &str,
    ) -> EtlResult<BigQueryClient> {
        let client = Client::from_service_account_key_file(sa_key_path)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] from a Google Cloud service account key string.
    ///
    /// Parses the provided service account key string to authenticate with the
    /// BigQuery API.
    pub async fn new_with_key(
        project_id: BigQueryProjectId,
        sa_key: &str,
    ) -> EtlResult<BigQueryClient> {
        let sa_key = parse_service_account_key(sa_key)
            .map_err(BQError::from)
            .map_err(bq_error_to_etl_error)?;
        let client = Client::from_service_account_key(sa_key, false)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Returns the full BigQuery table name in the form `project_id.dataset_id.table_id`.
    pub fn full_table_name(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> String {
        format!("`{}.{}.{}`", self.project_id, dataset_id, table_id)
    }

    /// Creates a new table in the specified dataset if it does not already exist.
    ///
    /// Returns `true` if the table was created, and `false` if the table
    /// already existed.
    pub async fn create_table_if_missing(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<bool> {
        if self.table_exists(dataset_id, table_id).await? {
            return Ok(false);
        }

        self.create_table(dataset_id, table_id, column_schemas, max_staleness_mins)
            .await?;

        Ok(true)
    }

    /// Creates a table in a BigQuery dataset.
    pub async fn create_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id);

        let columns_spec = Self::create_columns_spec(column_schemas);
        let max_staleness_option = if let Some(max_staleness_mins) = max_staleness_mins {
            Self::max_staleness_option(max_staleness_mins)
        } else {
            "".to_string()
        };

        info!("creating table {full_table_name} in BigQuery");

        let query = format!("create table {full_table_name} {columns_spec} {max_staleness_option}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Truncates a table in a BigQuery dataset.
    #[allow(dead_code)]
    pub async fn truncate_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id);

        info!("Truncating table {full_table_name} in BigQuery");

        let delete_query = format!("truncate table {full_table_name}",);

        let _ = self.query(QueryRequest::new(delete_query)).await?;

        Ok(())
    }

    /// Checks if a table exists in the specified dataset.
    ///
    /// # Panics
    ///
    /// Panics if the query result does not contain the expected `table_exists` column.
    pub async fn table_exists(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<bool> {
        let table = self
            .client
            .table()
            .get(&self.project_id, dataset_id, table_id, None)
            .await;

        let exists =
            !matches!(table, Err(BQError::ResponseError { error }) if error.error.code == 404);

        Ok(exists)
    }

    /// Streams rows to a BigQuery table using the Storage Write API.
    ///
    /// This method is efficient for high-throughput ingestion. It batches rows
    /// to respect the maximum request size.
    pub async fn stream_rows(
        &mut self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: &TableDescriptor,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // We have to map table rows into the new type due to the limitations of how Rust works.
        let table_rows = table_rows
            .into_iter()
            .map(BigQueryTableRow)
            .collect::<Vec<_>>();

        // We create a slice on table rows, which will be updated while the streaming progresses.
        //
        // Using a slice allows us to deallocate the vector only at the end of streaming, which leads
        // to fewer operations being performed.
        let mut table_rows = table_rows.as_slice();

        let default_stream = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_id.to_string(),
        );

        loop {
            let (rows, num_processed_rows) =
                StorageApi::create_rows(table_descriptor, table_rows, MAX_SIZE_BYTES);

            let mut append_rows_stream = self
                .client
                .storage_mut()
                .append_rows(&default_stream, rows, ETL_TRACE_ID.to_owned())
                .await
                .map_err(bq_error_to_etl_error)?;

            if let Some(append_rows_response) = append_rows_stream.next().await {
                let append_rows_response = append_rows_response
                    .map_err(BQError::from)
                    .map_err(bq_error_to_etl_error)?;
                if !append_rows_response.row_errors.is_empty() {
                    // We convert the error into an `ETLError`.
                    let row_errors = append_rows_response
                        .row_errors
                        .into_iter()
                        .map(row_error_to_etl_error)
                        .collect::<Vec<_>>();

                    return Err(row_errors.into());
                }
            }

            table_rows = &table_rows[num_processed_rows..];
            if table_rows.is_empty() {
                break;
            }
        }

        Ok(())
    }

    /// Executes an SQL query and returns the result set.
    pub async fn query(&self, request: QueryRequest) -> EtlResult<ResultSet> {
        let query_response = self
            .client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(bq_error_to_etl_error)?;

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

    /// Creates a primary key clause string for table creation.
    fn add_primary_key_clause(column_schemas: &[ColumnSchema]) -> String {
        let identity_columns: Vec<String> = column_schemas
            .iter()
            .filter(|s| s.primary)
            .map(|c| format!("`{}`", c.name))
            .collect();

        if identity_columns.is_empty() {
            return "".to_string();
        }

        format!(
            ", primary key ({}) not enforced",
            identity_columns.join(",")
        )
    }

    /// Builds the complete column specification clause for table creation.
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = column_schemas
            .iter()
            .map(Self::column_spec)
            .collect::<Vec<_>>()
            .join(",");

        s.push_str(&Self::add_primary_key_clause(column_schemas));

        format!("({s})")
    }

    /// Creates the max staleness option clause for table creation.
    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!("options (max_staleness = interval {max_staleness_mins} minute)")
    }

    /// Converts a PostgreSQL [`Type`] to its BigQuery equivalent data type string.
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

            return format!("array<{element_type}>");
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

    /// Returns true if the PostgreSQL [`Type`] represents an array type.
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

    /// Converts PostgreSQL column schemas to a BigQuery [`TableDescriptor`] for Storage Write API.
    ///
    /// Maps PostgreSQL data types to BigQuery column types and sets the appropriate
    /// column mode (`NULLABLE`, `REQUIRED`, or `REPEATED`). Automatically adds the
    /// Change Data Capture special column.
    pub fn column_schemas_to_table_descriptor(
        column_schemas: &[ColumnSchema],
        use_cdc_sequence_column: bool,
    ) -> TableDescriptor {
        let mut field_descriptors = Vec::with_capacity(column_schemas.len());
        let mut number = 1;

        for column_schema in column_schemas {
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
            name: BIGQUERY_CDC_SPECIAL_COLUMN.to_string(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        });
        number += 1;

        if use_cdc_sequence_column {
            field_descriptors.push(FieldDescriptor {
                number,
                name: BIGQUERY_CDC_SEQUENCE_COLUMN.to_string(),
                typ: ColumnType::String,
                mode: ColumnMode::Required,
            });
        }

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

/// Converts [`BQError`] to [`EtlError`] with appropriate error kind.
///
/// Maps errors based on their specific type for better error classification and handling.
fn bq_error_to_etl_error(err: BQError) -> EtlError {
    use BQError;

    let (kind, description) = match &err {
        // Authentication related errors
        BQError::InvalidServiceAccountKey(_) => (
            ErrorKind::AuthenticationError,
            "Invalid BigQuery service account key",
        ),
        BQError::InvalidServiceAccountAuthenticator(_) => (
            ErrorKind::AuthenticationError,
            "Invalid BigQuery service account authenticator",
        ),
        BQError::InvalidInstalledFlowAuthenticator(_) => (
            ErrorKind::AuthenticationError,
            "Invalid BigQuery installed flow authenticator",
        ),
        BQError::InvalidApplicationDefaultCredentialsAuthenticator(_) => (
            ErrorKind::AuthenticationError,
            "Invalid BigQuery application default credentials",
        ),
        BQError::InvalidAuthorizedUserAuthenticator(_) => (
            ErrorKind::AuthenticationError,
            "Invalid BigQuery authorized user authenticator",
        ),
        BQError::AuthError(_) => (
            ErrorKind::AuthenticationError,
            "BigQuery authentication error",
        ),
        BQError::YupAuthError(_) => (
            ErrorKind::AuthenticationError,
            "BigQuery OAuth authentication error",
        ),
        BQError::NoToken => (
            ErrorKind::AuthenticationError,
            "BigQuery authentication token missing",
        ),

        // Network and transport errors
        BQError::RequestError(_) => (ErrorKind::IoError, "BigQuery request failed"),
        BQError::TonicTransportError(_) => (ErrorKind::IoError, "BigQuery transport error"),

        // Query and data errors
        BQError::ResponseError { .. } => (ErrorKind::QueryFailed, "BigQuery response error"),
        BQError::NoDataAvailable => (
            ErrorKind::InvalidState,
            "BigQuery result set positioning error",
        ),
        BQError::InvalidColumnIndex { .. } => {
            (ErrorKind::InvalidData, "BigQuery invalid column index")
        }
        BQError::InvalidColumnName { .. } => {
            (ErrorKind::InvalidData, "BigQuery invalid column name")
        }
        BQError::InvalidColumnType { .. } => {
            (ErrorKind::ConversionError, "BigQuery column type mismatch")
        }

        // Serialization errors
        BQError::SerializationError(_) => (
            ErrorKind::SerializationError,
            "BigQuery JSON serialization error",
        ),

        // gRPC errors
        BQError::TonicInvalidMetadataValueError(_) => {
            (ErrorKind::ConfigError, "BigQuery invalid metadata value")
        }
        BQError::TonicStatusError(status) => {
            // Since we do not have access to the `Code` type from `tonic`, we just match on the description
            // statically.
            if status.code().description()
                == "The caller does not have permission to execute the specified operation"
            {
                (ErrorKind::PermissionDenied, "BigQuery permission denied")
            } else {
                (ErrorKind::DestinationError, "BigQuery gRPC status error")
            }
        }
    };

    etl_error!(kind, description, err.to_string())
}

/// Converts BigQuery row errors to [`EtlError`] with [`ErrorKind::DestinationError`].
fn row_error_to_etl_error(err: RowError) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "BigQuery row error",
        format!("{err:?}")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_to_bigquery_type_basic_types() {
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::BOOL),
            "bool"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::TEXT),
            "string"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::INT4),
            "int64"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::FLOAT8),
            "float64"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::TIMESTAMP),
            "timestamp"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::JSON),
            "json"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::BYTEA),
            "bytes"
        );
    }

    #[test]
    fn test_postgres_to_bigquery_type_array_types() {
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::BOOL_ARRAY),
            "array<bool>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::TEXT_ARRAY),
            "array<string>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::INT4_ARRAY),
            "array<int64>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::FLOAT8_ARRAY),
            "array<float64>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::TIMESTAMP_ARRAY),
            "array<timestamp>"
        );
    }

    #[test]
    fn test_is_array_type() {
        assert!(BigQueryClient::is_array_type(&Type::BOOL_ARRAY));
        assert!(BigQueryClient::is_array_type(&Type::TEXT_ARRAY));
        assert!(BigQueryClient::is_array_type(&Type::INT4_ARRAY));
        assert!(BigQueryClient::is_array_type(&Type::FLOAT8_ARRAY));
        assert!(BigQueryClient::is_array_type(&Type::TIMESTAMP_ARRAY));

        assert!(!BigQueryClient::is_array_type(&Type::BOOL));
        assert!(!BigQueryClient::is_array_type(&Type::TEXT));
        assert!(!BigQueryClient::is_array_type(&Type::INT4));
        assert!(!BigQueryClient::is_array_type(&Type::FLOAT8));
        assert!(!BigQueryClient::is_array_type(&Type::TIMESTAMP));
    }

    #[test]
    fn test_column_spec() {
        let column_schema = ColumnSchema::new("test_col".to_string(), Type::TEXT, -1, true, false);
        let spec = BigQueryClient::column_spec(&column_schema);
        assert_eq!(spec, "`test_col` string");

        let not_null_column = ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true);
        let not_null_spec = BigQueryClient::column_spec(&not_null_column);
        assert_eq!(not_null_spec, "`id` int64 not null");

        let array_column =
            ColumnSchema::new("tags".to_string(), Type::TEXT_ARRAY, -1, false, false);
        let array_spec = BigQueryClient::column_spec(&array_column);
        assert_eq!(array_spec, "`tags` array<string>");
    }

    #[test]
    fn test_add_primary_key_clause() {
        let columns_with_pk = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ];
        let pk_clause = BigQueryClient::add_primary_key_clause(&columns_with_pk);
        assert_eq!(pk_clause, ", primary key (`id`) not enforced");

        let columns_with_composite_pk = vec![
            ColumnSchema::new("tenant_id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ];
        let composite_pk_clause =
            BigQueryClient::add_primary_key_clause(&columns_with_composite_pk);
        assert_eq!(
            composite_pk_clause,
            ", primary key (`tenant_id`,`id`) not enforced"
        );

        let columns_no_pk = vec![
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("age".to_string(), Type::INT4, -1, true, false),
        ];
        let no_pk_clause = BigQueryClient::add_primary_key_clause(&columns_no_pk);
        assert_eq!(no_pk_clause, "");
    }

    #[test]
    fn test_create_columns_spec() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
        ];
        let spec = BigQueryClient::create_columns_spec(&columns);
        assert_eq!(
            spec,
            "(`id` int64 not null,`name` string,`active` bool not null, primary key (`id`) not enforced)"
        );
    }

    #[test]
    fn test_max_staleness_option() {
        let option = BigQueryClient::max_staleness_option(15);
        assert_eq!(option, "options (max_staleness = interval 15 minute)");
    }

    #[test]
    fn test_column_schemas_to_table_descriptor() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
            ColumnSchema::new("tags".to_string(), Type::TEXT_ARRAY, -1, false, false),
        ];

        let descriptor = BigQueryClient::column_schemas_to_table_descriptor(&columns, true);

        assert_eq!(descriptor.field_descriptors.len(), 6); // 4 columns + CDC columns

        // Check regular columns
        assert_eq!(descriptor.field_descriptors[0].name, "id");
        assert!(matches!(
            descriptor.field_descriptors[0].typ,
            ColumnType::Int32
        ));
        assert!(matches!(
            descriptor.field_descriptors[0].mode,
            ColumnMode::Required
        ));

        assert_eq!(descriptor.field_descriptors[1].name, "name");
        assert!(matches!(
            descriptor.field_descriptors[1].typ,
            ColumnType::String
        ));
        assert!(matches!(
            descriptor.field_descriptors[1].mode,
            ColumnMode::Nullable
        ));

        assert_eq!(descriptor.field_descriptors[2].name, "active");
        assert!(matches!(
            descriptor.field_descriptors[2].typ,
            ColumnType::Bool
        ));
        assert!(matches!(
            descriptor.field_descriptors[2].mode,
            ColumnMode::Required
        ));

        // Check array column
        assert_eq!(descriptor.field_descriptors[3].name, "tags");
        assert!(matches!(
            descriptor.field_descriptors[3].typ,
            ColumnType::String
        ));
        assert!(matches!(
            descriptor.field_descriptors[3].mode,
            ColumnMode::Repeated
        ));

        // Check CDC columns
        assert_eq!(
            descriptor.field_descriptors[4].name,
            BIGQUERY_CDC_SPECIAL_COLUMN
        );
        assert!(matches!(
            descriptor.field_descriptors[4].typ,
            ColumnType::String
        ));
        assert!(matches!(
            descriptor.field_descriptors[4].mode,
            ColumnMode::Required
        ));

        assert_eq!(
            descriptor.field_descriptors[5].name,
            BIGQUERY_CDC_SEQUENCE_COLUMN
        );
        assert!(matches!(
            descriptor.field_descriptors[5].typ,
            ColumnType::String
        ));
        assert!(matches!(
            descriptor.field_descriptors[5].mode,
            ColumnMode::Required
        ));
    }

    #[test]
    fn test_column_schemas_to_table_descriptor_complex_types() {
        let columns = vec![
            ColumnSchema::new("uuid_col".to_string(), Type::UUID, -1, true, false),
            ColumnSchema::new("json_col".to_string(), Type::JSON, -1, true, false),
            ColumnSchema::new("bytea_col".to_string(), Type::BYTEA, -1, true, false),
            ColumnSchema::new("numeric_col".to_string(), Type::NUMERIC, -1, true, false),
            ColumnSchema::new("date_col".to_string(), Type::DATE, -1, true, false),
            ColumnSchema::new("time_col".to_string(), Type::TIME, -1, true, false),
        ];

        let descriptor = BigQueryClient::column_schemas_to_table_descriptor(&columns, true);

        assert_eq!(descriptor.field_descriptors.len(), 8); // 6 columns + CDC columns

        // Check that UUID, JSON, DATE, TIME are all mapped to String in storage
        assert!(matches!(
            descriptor.field_descriptors[0].typ,
            ColumnType::String
        )); // UUID
        assert!(matches!(
            descriptor.field_descriptors[1].typ,
            ColumnType::String
        )); // JSON
        assert!(matches!(
            descriptor.field_descriptors[2].typ,
            ColumnType::Bytes
        )); // BYTEA
        assert!(matches!(
            descriptor.field_descriptors[3].typ,
            ColumnType::String
        )); // NUMERIC
        assert!(matches!(
            descriptor.field_descriptors[4].typ,
            ColumnType::String
        )); // DATE
        assert!(matches!(
            descriptor.field_descriptors[5].typ,
            ColumnType::String
        )); // TIME
    }

    #[test]
    fn test_full_table_name_formatting() {
        let project_id = "test-project";
        let dataset_id = "test_dataset";
        let table_id = "test_table";

        // Simulate the full_table_name method logic without creating a client
        let full_name = format!("`{project_id}.{dataset_id}.{table_id}`");
        assert_eq!(full_name, "`test-project.test_dataset.test_table`");
    }
}
