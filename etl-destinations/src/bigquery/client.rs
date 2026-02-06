use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{Cell, ColumnSchema, PipelineId, TableRow, Type, is_array_type};
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::RowError;
use gcp_bigquery_client::storage::{BatchAppendResult, ColumnMode, StorageApiConfig};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    Client,
    error::BQError,
    model::{query_request::QueryRequest, query_response::ResultSet},
    storage::{ColumnType, FieldDescriptor, StreamName, TableBatch, TableDescriptor},
};
use prost::Message;
use rand::Rng;
use std::fmt;
use std::time::Duration;
use tokio::time::sleep;
use tonic::Code;
use tracing::{debug, error, info, warn};

use crate::bigquery::encoding::BigQueryTableRow;
use crate::bigquery::metrics::{
    ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL, ETL_BQ_APPEND_BATCHES_BATCH_RETRIES_TOTAL,
    ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL, ETL_BQ_APPEND_BATCHES_BATCH_SIZE,
};
use metrics::{counter, histogram};

/// Trace identifier for ETL operations in BigQuery client.
const ETL_TRACE_ID: &str = "ETL BigQueryClient";

/// Multiplier for calculating max inflight requests from pool size.
///
/// The maximum number of inflight requests is `connection_pool_size * MAX_INFLIGHT_REQUESTS_PER_CONNECTION`.
const MAX_INFLIGHT_REQUESTS_PER_CONNECTION: usize = 100;

/// Maximum safe value for inflight requests to prevent resource exhaustion.
///
/// This upper bound ensures reasonable memory usage and prevents overflow when computing
/// max inflight requests from connection pool size.
const MAX_SAFE_INFLIGHT_REQUESTS: usize = 100_000;

/// Special column name for Change Data Capture operations in BigQuery.
const BIGQUERY_CDC_SPECIAL_COLUMN: &str = "_CHANGE_TYPE";

/// Special column name for Change Data Capture sequence ordering in BigQuery.
const BIGQUERY_CDC_SEQUENCE_COLUMN: &str = "_CHANGE_SEQUENCE_NUMBER";

/// Maximum number of retry attempts for transient BigQuery errors.
const MAX_RETRY_ATTEMPTS: u32 = 10;
/// Initial backoff delay in milliseconds for exponential backoff.
const INITIAL_BACKOFF_MS: u64 = 500;
/// Maximum backoff delay in milliseconds to cap exponential growth.
const MAX_BACKOFF_MS: u64 = 60_000;

/// BigQuery project identifier.
pub type BigQueryProjectId = String;
/// BigQuery dataset identifier.
pub type BigQueryDatasetId = String;
/// BigQuery table identifier.
pub type BigQueryTableId = String;

/// Computes the maximum number of inflight requests for the BigQuery Storage Write API.
///
/// Uses checked arithmetic to safely multiply the connection pool size by the per-connection
/// limit, clamping the result to [`MAX_SAFE_INFLIGHT_REQUESTS`] to prevent overflow and
/// resource exhaustion.
fn compute_max_inflight_requests(connection_pool_size: usize) -> usize {
    connection_pool_size
        .checked_mul(MAX_INFLIGHT_REQUESTS_PER_CONNECTION)
        .unwrap_or(MAX_SAFE_INFLIGHT_REQUESTS)
        .min(MAX_SAFE_INFLIGHT_REQUESTS)
}

/// Processes a single batch result and determines success or failure mode.
///
/// Row errors are permanent failures (bad data, schema mismatch) and fail immediately.
/// Request errors are surfaced for retry decision by the caller.
fn process_single_batch_append_result(
    batch_append_result: BatchAppendResult,
) -> BatchProcessResult {
    let bytes_sent = batch_append_result.bytes_sent;
    let mut total_bytes_received = 0;
    let mut row_errors = Vec::new();

    for response in batch_append_result.responses {
        match response {
            Ok(response) => {
                total_bytes_received += response.encoded_len();

                // Row-level errors are permanent failures (bad data, schema mismatch, etc).
                if !response.row_errors.is_empty() {
                    row_errors.extend(response.row_errors);
                }
            }
            Err(status) => {
                // Request-level error.
                return BatchProcessResult::RequestError {
                    error: BQError::from(status),
                };
            }
        }
    }

    if !row_errors.is_empty() {
        BatchProcessResult::RowErrors { errors: row_errors }
    } else {
        BatchProcessResult::Success {
            bytes_sent,
            bytes_received: total_bytes_received,
        }
    }
}

/// Calculates exponential backoff delay with full jitter.
///
/// Uses the "full jitter" approach: random value between 0 and min(max_backoff, base * 2^attempt).
/// This provides better spread than additive jitter, especially at higher attempts, helping
/// prevent thundering herd when many clients retry simultaneously.
fn calculate_backoff(attempt: u32) -> Duration {
    let exponential = INITIAL_BACKOFF_MS
        .saturating_mul(1u64 << attempt.min(10))
        .min(MAX_BACKOFF_MS);
    let jitter = rand::rng().random_range(0..=exponential);

    Duration::from_millis(jitter)
}

/// Checks if a [`BQError`] represents a transient condition that should be retried.
///
/// Implements retry logic based on Google's official BigQuery Storage Write API guidance
/// and production experience with the API. This deviates from the general AIP-194 guidance
/// to match BigQuery-specific behavior.
fn is_retryable_bq_error(error: &BQError) -> bool {
    match error {
        // Transport-level errors are always retriable (network failures, connection drops, etc.)
        BQError::TonicTransportError(_) => true,

        BQError::TonicStatusError(status) => match status.code() {
            // Immediately retriable: canonical "service unavailable" code
            Code::Unavailable => true,

            // Immediately retriable: transient internal server errors (GOAWAY, backend issues)
            Code::Internal => true,

            // Immediately retriable: concurrency conflicts and server-initiated aborts
            Code::Aborted => true,

            // Immediately retriable: server-cancelled operations (not client-cancelled)
            Code::Cancelled => true,

            // Immediately retriable: deadline exceeded (safe with offset-based deduplication)
            Code::DeadlineExceeded => true,

            // Retriable with backoff: rate limits and quota exhaustion
            Code::ResourceExhausted => true,

            // Transport error that never reached the server
            Code::Unknown => {
                let message = status.message().to_lowercase();
                message.contains("transport") || message.contains("connection")
            }

            // Non-retriable errors
            Code::InvalidArgument => false,
            Code::NotFound => false,
            Code::AlreadyExists => false,
            Code::PermissionDenied => false,
            Code::FailedPrecondition => false,
            Code::Unimplemented => false,
            Code::Unauthenticated => false,
            Code::DataLoss => false,
            Code::OutOfRange => false,
            Code::Ok => false,
        },
        // Other BQError variants are not retriable
        _ => false,
    }
}

/// Extracts the gRPC status code as a string from a [`BQError`] for metrics labeling.
fn error_code_label(error: &BQError) -> &'static str {
    match error {
        BQError::TonicStatusError(status) => match status.code() {
            Code::Ok => "ok",
            Code::Cancelled => "cancelled",
            Code::Unknown => "unknown",
            Code::InvalidArgument => "invalid_argument",
            Code::DeadlineExceeded => "deadline_exceeded",
            Code::NotFound => "not_found",
            Code::AlreadyExists => "already_exists",
            Code::PermissionDenied => "permission_denied",
            Code::ResourceExhausted => "resource_exhausted",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            Code::OutOfRange => "out_of_range",
            Code::Unimplemented => "unimplemented",
            Code::Internal => "internal",
            Code::Unavailable => "unavailable",
            Code::DataLoss => "data_loss",
            Code::Unauthenticated => "unauthenticated",
        },
        _ => "other",
    }
}

/// Converts BigQuery errors to ETL errors with appropriate classification.
///
/// Maps BigQuery error types to ETL error kinds for consistent error handling.
fn bq_error_to_etl_error(err: BQError) -> EtlError {
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
        BQError::RequestError(_) => (ErrorKind::DestinationIoError, "BigQuery request failed"),
        BQError::TonicTransportError(_) => {
            (ErrorKind::DestinationIoError, "BigQuery transport error")
        }

        // Query and data errors
        BQError::ResponseError { .. } => {
            (ErrorKind::DestinationQueryFailed, "BigQuery response error")
        }
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
            (ErrorKind::InvalidData, "BigQuery invalid metadata value")
        }
        BQError::TonicStatusError(status) => match status.code() {
            // Code::Unavailable (14) - Canonical "service unavailable" code.
            // Indicates transient conditions like network issues, server overload, or intentional
            // throttling. BigQuery returns this with messages like "Task is overloaded".
            // Retriable per Google's Storage Write API guidance.
            Code::Unavailable => (ErrorKind::DestinationError, "BigQuery unavailable"),

            // Code::Internal (13) - Internal server errors.
            // In BigQuery context, often manifests as transient backend issues, GOAWAY frames,
            // or temporary processing failures. Retriable per BigQuery backend team guidance.
            Code::Internal => (ErrorKind::DestinationError, "BigQuery internal error"),

            // Code::Aborted (10) - Concurrency conflicts or server-initiated aborts.
            // For Storage Write API, includes sequencer failures and stream aborts due to
            // transient conditions. Retriable per BigQuery backend team guidance.
            Code::Aborted => (ErrorKind::DestinationError, "BigQuery operation aborted"),

            // Code::Cancelled (1) - Server-cancelled operations.
            // In streaming context, server may cancel in-flight appends due to internal
            // reshuffling. Retriable per BigQuery backend team guidance.
            Code::Cancelled => (ErrorKind::DestinationError, "BigQuery operation cancelled"),

            // Code::DeadlineExceeded (4) - Operation timeout.
            // Request may or may not have completed server-side. Safe to retry with offset-based
            // deduplication in Storage Write API. Retriable per Google's guidance.
            Code::DeadlineExceeded => (ErrorKind::DestinationError, "BigQuery deadline exceeded"),

            // Code::ResourceExhausted (8) - Quota or rate limit exhaustion.
            // Requires exponential backoff to allow server capacity recovery. Retriable per
            // Google's Storage Write API guidance, though may need longer backoff periods.
            Code::ResourceExhausted => (ErrorKind::DestinationError, "BigQuery resource exhausted"),

            // Code::FailedPrecondition (9) - Precondition failures.
            // Indicates issues like STREAM_FINALIZED, INVALID_STREAM_STATE, or SCHEMA_MISMATCH.
            // Requires fixing the underlying issue before retrying. Never retry automatically.
            Code::FailedPrecondition => {
                (ErrorKind::DestinationError, "BigQuery precondition failed")
            }

            // Code::Unknown (2) - Transport-level errors.
            // When message contains "transport" or "connection", indicates errors that never
            // reached the server (TCP resets, HTTP/2 GOAWAY). Retriable as per transport error
            // guidance.
            Code::Unknown => (ErrorKind::DestinationError, "BigQuery unknown error"),

            // Code::PermissionDenied (7) - Authorization failure.
            // Requires IAM permission changes. Never retry.
            Code::PermissionDenied => (ErrorKind::DestinationError, "BigQuery permission denied"),

            // Code::Unauthenticated (16) - Authentication failure.
            // Requires credential refresh or configuration fix. Never retry.
            Code::Unauthenticated => (
                ErrorKind::DestinationError,
                "BigQuery authentication failed",
            ),

            // Code::InvalidArgument (3) - Malformed request or invalid data.
            // Client bug that requires code changes. Never retry.
            Code::InvalidArgument => (ErrorKind::DestinationError, "BigQuery invalid argument"),

            // Code::NotFound (5) - Resource doesn't exist.
            // Requires creating the resource (table, dataset, stream) first. Never retry.
            Code::NotFound => (ErrorKind::DestinationError, "BigQuery entity not found"),

            // Code::AlreadyExists (6) - Entity conflict during creation.
            // For streaming with offsets, may indicate row was already written. Never retry.
            Code::AlreadyExists => (
                ErrorKind::DestinationError,
                "BigQuery entity already exists",
            ),

            // Code::OutOfRange (11) - Invalid offset for streaming.
            // Offset beyond current stream end. Requires application-level recovery. Never retry.
            Code::OutOfRange => (ErrorKind::DestinationError, "BigQuery offset out of range"),

            // Code::Unimplemented (12) - Operation not available.
            // Feature not supported by BigQuery. Never retry.
            Code::Unimplemented => (
                ErrorKind::DestinationError,
                "BigQuery operation not supported",
            ),

            // Code::DataLoss (15) - Unrecoverable data corruption.
            // Severe error requiring manual intervention. Never retry.
            Code::DataLoss => (ErrorKind::DestinationError, "BigQuery data loss"),

            // Code::Ok (0) - Should never be an error
            Code::Ok => (ErrorKind::DestinationError, "BigQuery unexpected ok status"),
        },

        // Concurrency and task errors
        BQError::SemaphorePermitError(_) => (
            ErrorKind::DestinationError,
            "BigQuery semaphore permit error",
        ),
        BQError::TokioTaskError(_) => {
            (ErrorKind::DestinationError, "BigQuery task execution error")
        }
        BQError::ConnectionPoolError(_) => (
            ErrorKind::DestinationError,
            "BigQuery connection pool error",
        ),
    };

    etl_error!(kind, description, err.to_string())
}

/// Converts BigQuery row errors to ETL destination errors.
fn row_error_to_etl_error(err: RowError) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "BigQuery row error",
        format!("{err:?}")
    )
}

/// Change Data Capture operation types for BigQuery streaming.
#[derive(Debug)]
pub enum BigQueryOperationType {
    Upsert,
    Delete,
}

impl BigQueryOperationType {
    /// Converts the operation type into a [`Cell`] for streaming.
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

/// Result of processing a single batch, used to determine retry strategy.
enum BatchProcessResult {
    /// Batch succeeded with byte metrics.
    Success {
        bytes_sent: usize,
        bytes_received: usize,
    },
    /// Batch had row-level errors.
    RowErrors { errors: Vec<RowError> },
    /// Batch had a request-level error.
    RequestError { error: BQError },
}

/// Client for interacting with Google BigQuery.
///
/// Provides methods for table management, data insertion, and query execution
/// against BigQuery datasets with authentication and error handling.
#[derive(Clone)]
pub struct BigQueryClient {
    project_id: BigQueryProjectId,
    client: Client,
}

impl BigQueryClient {
    /// Creates a new [`BigQueryClient`] from a service account key file.
    ///
    /// Authenticates with BigQuery using the service account key at the specified file path.
    /// Configures the Storage Write API with the given pool size.
    pub async fn new_with_key_path(
        project_id: BigQueryProjectId,
        sa_key_file: &str,
        connection_pool_size: usize,
    ) -> EtlResult<BigQueryClient> {
        let max_inflight_requests = compute_max_inflight_requests(connection_pool_size);
        let storage_config = StorageApiConfig {
            connection_pool_size,
            max_inflight_requests,
        };

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_service_account_key_file(sa_key_file)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] from a service account key JSON string.
    ///
    /// Parses and uses the provided service account key to authenticate with BigQuery.
    /// Configures the Storage Write API with the given pool size.
    pub async fn new_with_key(
        project_id: BigQueryProjectId,
        sa_key: &str,
        connection_pool_size: usize,
    ) -> EtlResult<BigQueryClient> {
        let max_inflight_requests = compute_max_inflight_requests(connection_pool_size);
        let storage_config = StorageApiConfig {
            connection_pool_size,
            max_inflight_requests,
        };

        let sa_key = parse_service_account_key(sa_key)
            .map_err(BQError::from)
            .map_err(bq_error_to_etl_error)?;
        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_service_account_key(sa_key, false)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] using Application Default Credentials.
    ///
    /// Authenticates with BigQuery using the environment's default credentials.
    /// Configures the Storage Write API with the given pool size.
    /// Returns an error if credentials are missing or invalid.
    pub async fn new_with_adc(
        project_id: BigQueryProjectId,
        connection_pool_size: usize,
    ) -> EtlResult<BigQueryClient> {
        let max_inflight_requests = compute_max_inflight_requests(connection_pool_size);
        let storage_config = StorageApiConfig {
            connection_pool_size,
            max_inflight_requests,
        };

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_application_default_credentials()
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] using OAuth2 installed flow authentication.
    ///
    /// Authenticates with BigQuery using the OAuth2 installed flow.
    /// Configures the Storage Write API with the given pool size.
    pub async fn new_with_flow_authenticator<S: AsRef<[u8]>, P: Into<std::path::PathBuf>>(
        project_id: BigQueryProjectId,
        secret: S,
        persistent_file_path: P,
        connection_pool_size: usize,
    ) -> EtlResult<BigQueryClient> {
        let max_inflight_requests = compute_max_inflight_requests(connection_pool_size);
        let storage_config = StorageApiConfig {
            connection_pool_size,
            max_inflight_requests,
        };

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_installed_flow_authenticator(secret, persistent_file_path)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Returns the fully qualified BigQuery table name.
    ///
    /// Formats the table name as `project_id.dataset_id.table_id` with proper quoting.
    pub fn full_table_name(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<String> {
        let project_id = Self::sanitize_identifier(&self.project_id, "BigQuery project id")?;
        let dataset_id = Self::sanitize_identifier(dataset_id, "BigQuery dataset id")?;
        let table_id = Self::sanitize_identifier(table_id, "BigQuery table id")?;

        Ok(format!("`{project_id}.{dataset_id}.{table_id}`"))
    }

    /// Creates a table in BigQuery if it doesn't already exist, otherwise efficiently truncates
    /// and recreates the table with the same schema.
    ///
    /// This method uses BigQuery's CREATE OR REPLACE TABLE statement which is more efficient
    /// than dropping and recreating as it preserves table metadata and permissions.
    ///
    /// Returns `true` if the table was created fresh, `false` if it already existed and was replaced.
    pub async fn create_or_replace_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<bool> {
        let table_exists = self.table_exists(dataset_id, table_id).await?;

        let full_table_name = self.full_table_name(dataset_id, table_id)?;

        let columns_spec = Self::create_columns_spec(column_schemas)?;
        let max_staleness_option = if let Some(max_staleness_mins) = max_staleness_mins {
            Self::max_staleness_option(max_staleness_mins)
        } else {
            "".to_string()
        };

        info!(
            %full_table_name,
            %table_exists,
            "creating or replacing table in bigquery"
        );

        let query = format!(
            "create or replace table {full_table_name} {columns_spec} {max_staleness_option}"
        );

        let _ = self.query(QueryRequest::new(query)).await?;

        // Return true if it was a fresh creation, false if it was a replacement
        Ok(!table_exists)
    }

    /// Creates a table in BigQuery if it doesn't already exist.
    ///
    /// Returns `true` if the table was created, `false` if it already existed.
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

    /// Creates a new table in the BigQuery dataset.
    ///
    /// Builds and executes a CREATE TABLE statement with the provided column schemas
    /// and optional staleness configuration for CDC operations.
    pub async fn create_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;

        let columns_spec = Self::create_columns_spec(column_schemas)?;
        let max_staleness_option = if let Some(max_staleness_mins) = max_staleness_mins {
            Self::max_staleness_option(max_staleness_mins)
        } else {
            "".to_string()
        };

        info!(%full_table_name, "creating table in bigquery");

        let query = format!("create table {full_table_name} {columns_spec} {max_staleness_option}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Truncates all data from a BigQuery table.
    ///
    /// Executes a TRUNCATE TABLE statement to remove all rows while preserving the table structure.
    #[allow(dead_code)]
    pub async fn truncate_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;

        info!(%full_table_name, "truncating table in bigquery");

        let delete_query = format!("truncate table {full_table_name}",);

        let _ = self.query(QueryRequest::new(delete_query)).await?;

        Ok(())
    }

    /// Creates or replaces a view that points to the specified versioned table.
    ///
    /// This is used during truncation operations to redirect the view to a new table version.
    pub async fn create_or_replace_view(
        &self,
        dataset_id: &BigQueryDatasetId,
        view_name: &BigQueryTableId,
        target_table_id: &BigQueryTableId,
    ) -> EtlResult<()> {
        let full_view_name = self.full_table_name(dataset_id, view_name)?;
        let full_target_table_name = self.full_table_name(dataset_id, target_table_id)?;

        info!(%full_view_name, %full_target_table_name, "creating/replacing view");

        let query = format!(
            "create or replace view {full_view_name} as select * from {full_target_table_name}"
        );

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Drops a table from BigQuery.
    ///
    /// Executes a DROP TABLE statement to remove the table and all its data.
    pub async fn drop_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;

        info!(%full_table_name, "dropping table from bigquery");

        let query = format!("drop table if exists {full_table_name}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Checks whether a table exists in the BigQuery dataset.
    ///
    /// Returns `true` if the table exists, `false` otherwise.
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

    /// Checks whether a dataset exists and is accessible.
    ///
    /// Returns `true` if the dataset exists and the client has access, `false` if the
    /// dataset does not exist. Returns an error for authentication or connectivity failures.
    pub async fn dataset_exists(&self, dataset_id: &BigQueryDatasetId) -> EtlResult<bool> {
        let result = self
            .client
            .dataset()
            .get(&self.project_id, dataset_id)
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(BQError::ResponseError { error }) if error.error.code == 404 => Ok(false),
            Err(e) => Err(bq_error_to_etl_error(e)),
        }
    }

    /// Appends table batches to BigQuery using the concurrent Storage Write API with retry logic.
    ///
    /// Accepts pre-constructed TableBatch objects and processes them concurrently
    /// with controlled parallelism. This allows streaming to multiple different tables efficiently
    /// in a single call.
    ///
    /// If ordering is not required, you may split a table's data into multiple batches,
    /// which can be processed concurrently.
    /// If ordering guarantees are needed, all data for a given table must be included
    /// in a single batch, and it will be processed in order.
    ///
    /// Implements fine-grained retry logic that only retries batches that failed with
    /// transient errors, while preserving successful batch results. Non-retryable errors
    /// (permission denied, invalid data, row errors) fail immediately.
    pub async fn append_table_batches(
        &self,
        pipeline_id: PipelineId,
        table_batches: Vec<TableBatch<BigQueryTableRow>>,
    ) -> EtlResult<(usize, usize)> {
        if table_batches.is_empty() {
            return Ok((0, 0));
        }

        // Record batch sizes
        for batch in &table_batches {
            histogram!(ETL_BQ_APPEND_BATCHES_BATCH_SIZE, "pipeline_id" => pipeline_id.to_string())
                .record(batch.rows().len() as f64);
        }

        debug!(
            batch_count = table_batches.len(),
            "streaming table batches concurrently"
        );

        // Two vectors for efficient batch retry tracking.
        // We swap them each iteration to reuse allocations.
        let mut current_batches = table_batches;
        // Track batches with their last error for better error reporting on retry exhaustion.
        let mut retry_batches: Vec<(TableBatch<BigQueryTableRow>, BQError)> =
            Vec::with_capacity(current_batches.len());

        let mut total_bytes_sent = 0;
        let mut total_bytes_received = 0;

        // Reusable error accumulator across iterations.
        let mut non_retryable_errors = Vec::new();

        // Retry loop for transient errors.
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            if current_batches.is_empty() {
                break;
            }

            debug!(
                attempt = attempt + 1,
                batch_count = current_batches.len(),
                "attempting to append table batches"
            );

            // Attempt to send batches concurrently.
            let batch_append_results = match self
                .client
                .storage()
                .append_table_batches_concurrent(current_batches.clone(), ETL_TRACE_ID)
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    let error_code = error_code_label(&err);
                    let is_retryable = is_retryable_bq_error(&err);

                    counter!(ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
                        "pipeline_id" => pipeline_id.to_string(),
                        "error_code" => error_code,
                        "retryable" => is_retryable.to_string()
                    )
                    .increment(1);

                    // Connection-level error before any batch processing.
                    if is_retryable && attempt < MAX_RETRY_ATTEMPTS - 1 {
                        counter!(ETL_BQ_APPEND_BATCHES_BATCH_RETRIES_TOTAL,
                            "pipeline_id" => pipeline_id.to_string(),
                            "error_code" => error_code,
                            "attempt" => (attempt + 1).to_string()
                        )
                        .increment(1);

                        let backoff = calculate_backoff(attempt);
                        warn!(
                            attempt = attempt + 1,
                            max_attempts = MAX_RETRY_ATTEMPTS,
                            backoff_ms = backoff.as_millis(),
                            error = %err,
                            "bigquery connection error, backing off before retry"
                        );
                        sleep(backoff).await;

                        continue;
                    }

                    // Non-retryable or final attempt - convert and fail.
                    return Err(bq_error_to_etl_error(err));
                }
            };

            // Clear vectors and reuse allocations.
            retry_batches.clear();
            non_retryable_errors.clear();

            for batch_append_result in batch_append_results {
                let batch_index = batch_append_result.batch_index;

                match process_single_batch_append_result(batch_append_result) {
                    BatchProcessResult::Success {
                        bytes_sent,
                        bytes_received,
                    } => {
                        debug!(
                            batch_index,
                            bytes_sent, bytes_received, "batch processed successfully"
                        );

                        total_bytes_sent += bytes_sent;
                        total_bytes_received += bytes_received;
                    }
                    BatchProcessResult::RowErrors { errors } => {
                        // Row errors are permanent (bad data, schema mismatch, etc).
                        let error_count = errors.len();
                        error!(
                            batch_index,
                            error_count, "batch has row errors, failing immediately"
                        );

                        counter!(ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL, "pipeline_id" => pipeline_id.to_string())
                            .increment(error_count as u64);

                        // Convert all row errors to EtlErrors.
                        for row_error in errors {
                            non_retryable_errors.push(row_error_to_etl_error(row_error));
                        }
                    }
                    BatchProcessResult::RequestError { error } => {
                        let error_code = error_code_label(&error);
                        let is_retryable = is_retryable_bq_error(&error);

                        counter!(ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
                            "pipeline_id" => pipeline_id.to_string(),
                            "error_code" => error_code,
                            "retryable" => is_retryable.to_string()
                        )
                        .increment(1);

                        if is_retryable {
                            // Retryable request error, queue batch for retry.
                            warn!(
                                batch_index,
                                error = %error,
                                "batch has retryable request error, will retry"
                            );

                            counter!(ETL_BQ_APPEND_BATCHES_BATCH_RETRIES_TOTAL,
                                "pipeline_id" => pipeline_id.to_string(),
                                "error_code" => error_code,
                                "attempt" => (attempt + 1).to_string()
                            )
                            .increment(1);

                            retry_batches.push((current_batches[batch_index].clone(), error));
                        } else {
                            // Non-retryable request error, convert and accumulate.
                            non_retryable_errors.push(bq_error_to_etl_error(error));
                        }
                    }
                }
            }

            // If we have non-retryable errors, fail immediately. The reason for this is that we are
            // guaranteeing that all batches are flushed if we return success, thus if there is at
            // least one non-retryable error, we just stop and error.
            if !non_retryable_errors.is_empty() {
                return Err(non_retryable_errors.into());
            }

            // If no batches need retry, we're done.
            if retry_batches.is_empty() {
                break;
            }

            // On final attempt, convert batch errors and fail since we can't do anything about it
            // anymore.
            if attempt == MAX_RETRY_ATTEMPTS - 1 {
                let errors: Vec<EtlError> = retry_batches
                    .into_iter()
                    .map(|(_, bq_error)| bq_error_to_etl_error(bq_error))
                    .collect();

                return Err(errors.into());
            }

            // Backoff before retry.
            let backoff = calculate_backoff(attempt);
            warn!(
                attempt = attempt + 1,
                max_attempts = MAX_RETRY_ATTEMPTS,
                backoff_ms = backoff.as_millis(),
                retry_batch_count = retry_batches.len(),
                "bigquery batches encountered transient errors, backing off before retry"
            );
            sleep(backoff).await;

            // Move failed batches to current_batches for next iteration.
            // Extract batches from retry_batches (dropping the error tracking).
            current_batches.clear();
            current_batches.extend(retry_batches.drain(..).map(|(batch, _)| batch));
        }

        Ok((total_bytes_sent, total_bytes_received))
    }

    /// Creates a TableBatch for a specific table with validated rows.
    ///
    /// Converts TableRow instances to BigQueryTableRow and creates a properly configured
    /// TableBatch wrapped in Arc for efficient sharing and retry operations.
    pub fn create_table_batch(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: TableDescriptor,
        rows: Vec<TableRow>,
    ) -> EtlResult<TableBatch<BigQueryTableRow>> {
        let validated_rows = rows
            .into_iter()
            .map(BigQueryTableRow::try_from)
            .collect::<EtlResult<Vec<_>>>()?;

        // We want to use the default stream from BigQuery since it allows multiple connections to
        // send data to it. In addition, it's available by default for every table, so it also reduces
        // complexity.
        let stream_name = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_id.to_string(),
        );

        Ok(TableBatch::new(
            stream_name,
            table_descriptor,
            validated_rows,
        ))
    }

    /// Executes a BigQuery SQL query and returns the result set.
    pub async fn query(&self, request: QueryRequest) -> EtlResult<ResultSet> {
        let query_response = self
            .client
            .job()
            .query(&self.project_id, request)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(ResultSet::new_from_query_response(query_response))
    }

    /// Sanitizes a BigQuery identifier for safe backtick quoting.
    ///
    /// Rejects empty identifiers and identifiers containing control characters. Internal backticks
    /// are escaped by doubling them so the resulting value can be wrapped in backticks without
    /// altering the identifier or allowing statement breaks.
    fn sanitize_identifier(identifier: &str, context: &str) -> EtlResult<String> {
        if identifier.is_empty() {
            return Err(etl_error!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid BigQuery identifier",
                format!("{context} cannot be empty")
            ));
        }

        if identifier.chars().any(char::is_control) {
            return Err(etl_error!(
                ErrorKind::DestinationTableNameInvalid,
                "Invalid BigQuery identifier",
                format!("{context} contains control characters")
            ));
        }

        let mut escaped = String::with_capacity(identifier.len());

        for ch in identifier.chars() {
            match ch {
                // Backticks delimit identifiers in BigQuery. Escape with a backslash
                // per GoogleSQL lexical rules to keep the identifier intact.
                '`' => escaped.push_str("\\`"),
                // Backslash is the escape character; escape it to avoid altering
                // the meaning of the identifier when parsed.
                '\\' => escaped.push_str("\\\\"),
                _ => escaped.push(ch),
            }
        }

        Ok(escaped)
    }

    /// Generates SQL column specification for CREATE TABLE statements.
    fn column_spec(column_schema: &ColumnSchema) -> EtlResult<String> {
        let column_name = Self::sanitize_identifier(&column_schema.name, "BigQuery column name")?;

        let mut column_spec = format!(
            "`{}` {}",
            column_name,
            Self::postgres_to_bigquery_type(&column_schema.typ)
        );

        if !column_schema.nullable && !is_array_type(&column_schema.typ) {
            column_spec.push_str(" not null");
        };

        Ok(column_spec)
    }

    /// Creates a primary key clause for table creation.
    ///
    /// Generates a primary key constraint clause from columns marked as primary key.
    fn add_primary_key_clause(column_schemas: &[ColumnSchema]) -> EtlResult<String> {
        let identity_columns: Vec<String> = column_schemas
            .iter()
            .filter(|s| s.primary)
            .map(|c| {
                Self::sanitize_identifier(&c.name, "BigQuery primary key column")
                    .map(|name| format!("`{name}`"))
            })
            .collect::<EtlResult<Vec<_>>>()?;

        if identity_columns.is_empty() {
            return Ok("".to_string());
        }

        Ok(format!(
            ", primary key ({}) not enforced",
            identity_columns.join(",")
        ))
    }

    /// Builds complete column specifications for CREATE TABLE statements.
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> EtlResult<String> {
        let mut s = column_schemas
            .iter()
            .map(Self::column_spec)
            .collect::<EtlResult<Vec<_>>>()?
            .join(",");

        s.push_str(&Self::add_primary_key_clause(column_schemas)?);

        Ok(format!("({s})"))
    }

    /// Creates max staleness option clause for CDC table creation.
    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!("options (max_staleness = interval {max_staleness_mins} minute)")
    }

    /// Converts Postgres data types to BigQuery equivalent types.
    fn postgres_to_bigquery_type(typ: &Type) -> String {
        if is_array_type(typ) {
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

    /// Converts Postgres column schemas to a BigQuery [`TableDescriptor`].
    ///
    /// Maps data types and nullability to BigQuery column specifications, setting
    /// appropriate column modes and automatically adding CDC special columns.
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

            let mode = if is_array_type(&column_schema.typ) {
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
    /// Formats the client for debugging, excluding sensitive client details.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BigQueryClient")
            .field("project_id", &self.project_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

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
    fn test_column_spec() {
        let column_schema = ColumnSchema::new("test_col".to_string(), Type::TEXT, -1, true, false);
        let spec = BigQueryClient::column_spec(&column_schema).expect("column spec generation");
        assert_eq!(spec, "`test_col` string");

        let not_null_column = ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true);
        let not_null_spec =
            BigQueryClient::column_spec(&not_null_column).expect("not null column spec");
        assert_eq!(not_null_spec, "`id` int64 not null");

        let array_column =
            ColumnSchema::new("tags".to_string(), Type::TEXT_ARRAY, -1, false, false);
        let array_spec = BigQueryClient::column_spec(&array_column).expect("array column spec");
        assert_eq!(array_spec, "`tags` array<string>");
    }

    #[test]
    fn test_column_spec_escapes_backticks() {
        let column_schema = ColumnSchema::new("pwn`name".to_string(), Type::TEXT, -1, true, false);

        let spec = BigQueryClient::column_spec(&column_schema).expect("escaped column spec");

        assert_eq!(spec, "`pwn\\`name` string");
    }

    #[test]
    fn test_sanitize_identifier_rejects_control_chars() {
        let result = BigQueryClient::sanitize_identifier("bad\nname", "column");

        assert!(matches!(
            result,
            Err(err) if err.kind() == ErrorKind::DestinationTableNameInvalid
        ));
    }

    #[test]
    fn test_add_primary_key_clause() {
        let columns_with_pk = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ];
        let pk_clause =
            BigQueryClient::add_primary_key_clause(&columns_with_pk).expect("pk clause");
        assert_eq!(pk_clause, ", primary key (`id`) not enforced");

        let columns_with_composite_pk = vec![
            ColumnSchema::new("tenant_id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ];
        let composite_pk_clause =
            BigQueryClient::add_primary_key_clause(&columns_with_composite_pk)
                .expect("composite pk clause");
        assert_eq!(
            composite_pk_clause,
            ", primary key (`tenant_id`,`id`) not enforced"
        );

        let columns_no_pk = vec![
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("age".to_string(), Type::INT4, -1, true, false),
        ];
        let no_pk_clause =
            BigQueryClient::add_primary_key_clause(&columns_no_pk).expect("no pk clause");
        assert_eq!(no_pk_clause, "");
    }

    #[test]
    fn test_create_columns_spec() {
        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
            ColumnSchema::new("active".to_string(), Type::BOOL, -1, false, false),
        ];
        let spec = BigQueryClient::create_columns_spec(&columns).expect("columns spec");
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
        let full_name = format!(
            "`{project}.{dataset}.{table}`",
            project = BigQueryClient::sanitize_identifier(project_id, "project").unwrap(),
            dataset = BigQueryClient::sanitize_identifier(dataset_id, "dataset").unwrap(),
            table = BigQueryClient::sanitize_identifier(table_id, "table").unwrap()
        );
        assert_eq!(full_name, "`test-project.test_dataset.test_table`");
    }

    #[test]
    fn test_create_or_replace_table_query_generation() {
        let project_id = "test-project";
        let dataset_id = "test_dataset";
        let table_id = "test_table";

        let columns = vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, true, false),
        ];

        // Simulate the query generation logic
        let full_table_name = format!(
            "`{project}.{dataset}.{table}`",
            project = BigQueryClient::sanitize_identifier(project_id, "project").unwrap(),
            dataset = BigQueryClient::sanitize_identifier(dataset_id, "dataset").unwrap(),
            table = BigQueryClient::sanitize_identifier(table_id, "table").unwrap()
        );
        let columns_spec = BigQueryClient::create_columns_spec(&columns).unwrap();
        let query = format!("create or replace table {full_table_name} {columns_spec}");

        let expected_query = "create or replace table `test-project.test_dataset.test_table` (`id` int64 not null,`name` string, primary key (`id`) not enforced)";
        assert_eq!(query, expected_query);
    }

    #[test]
    fn test_create_or_replace_table_query_with_staleness() {
        let project_id = "test-project";
        let dataset_id = "test_dataset";
        let table_id = "test_table";
        let max_staleness_mins = 15;

        let columns = vec![ColumnSchema::new(
            "id".to_string(),
            Type::INT4,
            -1,
            false,
            true,
        )];

        // Simulate the query generation logic with staleness
        let full_table_name = format!(
            "`{project}.{dataset}.{table}`",
            project = BigQueryClient::sanitize_identifier(project_id, "project").unwrap(),
            dataset = BigQueryClient::sanitize_identifier(dataset_id, "dataset").unwrap(),
            table = BigQueryClient::sanitize_identifier(table_id, "table").unwrap()
        );
        let columns_spec = BigQueryClient::create_columns_spec(&columns).unwrap();
        let max_staleness_option = BigQueryClient::max_staleness_option(max_staleness_mins);
        let query = format!(
            "create or replace table {full_table_name} {columns_spec} {max_staleness_option}"
        );

        let expected_query = "create or replace table `test-project.test_dataset.test_table` (`id` int64 not null, primary key (`id`) not enforced) options (max_staleness = interval 15 minute)";
        assert_eq!(query, expected_query);
    }

    #[test]
    fn test_is_retryable_bq_error_immediately_retriable_codes() {
        // Test immediately retriable codes per BigQuery Storage Write API guidance
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unavailable("service unavailable")
        )));
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::internal("internal error")
        )));
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::aborted("operation aborted")
        )));
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::cancelled("operation cancelled")
        )));
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::deadline_exceeded("deadline exceeded")
        )));
    }

    #[test]
    fn test_is_retryable_bq_error_resource_exhausted() {
        // Test retriable with exponential backoff
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::resource_exhausted("quota exceeded")
        )));
    }

    #[test]
    fn test_is_retryable_bq_error_transport_errors() {
        // Test transport-level errors via Code::Unknown with transport-related messages
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unknown("transport error")
        )));
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unknown("connection reset")
        )));
        assert!(is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unknown("Connection refused")
        )));

        // Note: BQError::TonicTransportError is also retriable but we cannot easily construct
        // a test instance since tonic::transport::Error::from_source is private.
        // The actual retry logic handles this case in production via the match arm:
        // BQError::TonicTransportError(_) => true
    }

    #[test]
    fn test_is_retryable_bq_error_non_retriable_codes() {
        // Test non-retriable codes
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::invalid_argument("bad request")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::not_found("table not found")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::already_exists("table exists")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::permission_denied("access denied")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::failed_precondition("stream finalized")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unauthenticated("invalid credentials")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unimplemented("not supported")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::data_loss("data corruption")
        )));
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::out_of_range("invalid offset")
        )));
    }

    #[test]
    fn test_is_retryable_bq_error_unknown_without_transport() {
        // Test that Code::Unknown without transport-related message is not retriable
        assert!(!is_retryable_bq_error(&BQError::TonicStatusError(
            Status::unknown("some other error")
        )));
    }
}
