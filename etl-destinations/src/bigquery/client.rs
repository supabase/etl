use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{Cell, ColumnSchema, PipelineId, ReplicatedTableSchema, Type, is_array_type};
use gcp_bigquery_client::client_builder::ClientBuilder;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::RowError;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::StorageError;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::storage_error::StorageErrorCode;
use gcp_bigquery_client::google::rpc::Status as GoogleRpcStatus;
use gcp_bigquery_client::storage::{
    BatchAppendRequest, BatchAppendResult, ColumnMode, StorageApiConfig,
};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    Client,
    error::BQError,
    model::{query_request::QueryRequest, query_response::ResultSet},
    storage::{ColumnType, FieldDescriptor, StreamName, TableBatch, TableDescriptor},
};
use prost::Message;
use rand::random;
use std::fmt;
use tokio::time::{Duration, Instant, sleep};
use tonic::Code;
use tracing::{debug, error, info, warn};

use crate::bigquery::encoding::BigQueryTableRow;
use crate::bigquery::metrics::{
    ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL, ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL,
};
use metrics::counter;

/// Multiplier for calculating max inflight requests from pool size.
///
/// The maximum number of inflight requests is `connection_pool_size * MAX_INFLIGHT_REQUESTS_PER_CONNECTION`.
const MAX_INFLIGHT_REQUESTS_PER_CONNECTION: usize = 100;

/// Maximum safe value for inflight requests to prevent resource exhaustion.
///
/// This upper bound ensures reasonable memory usage and prevents overflow when computing
/// max inflight requests from connection pool size.
const MAX_SAFE_INFLIGHT_REQUESTS: usize = 100_000;
/// Maximum time to retry appends while BigQuery propagates a schema change.
///
/// Google documents schema update detection as happening on the order of minutes.
const SCHEMA_PROPAGATION_RETRY_TIMEOUT: Duration = Duration::from_secs(180);
/// Initial backoff when retrying appends during schema propagation.
const SCHEMA_PROPAGATION_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Maximum backoff when retrying appends during schema propagation.
const SCHEMA_PROPAGATION_MAX_RETRY_DELAY: Duration = Duration::from_secs(15);
/// Protobuf type name for BigQuery storage errors embedded in gRPC status details.
const BIGQUERY_STORAGE_ERROR_TYPE_NAME: &str = "google.cloud.bigquery.storage.v1.StorageError";

/// Special column name for Change Data Capture operations in BigQuery.
const BIGQUERY_CDC_SPECIAL_COLUMN: &str = "_CHANGE_TYPE";

/// Special column name for Change Data Capture sequence ordering in BigQuery.
const BIGQUERY_CDC_SEQUENCE_COLUMN: &str = "_CHANGE_SEQUENCE_NUMBER";

/// BigQuery project identifier.
pub type BigQueryProjectId = String;
/// BigQuery dataset identifier.
pub type BigQueryDatasetId = String;
/// BigQuery table identifier.
pub type BigQueryTableId = String;

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
#[derive(Debug)]
enum BatchProcessResult {
    /// Batch succeeded with byte metrics.
    Success {
        bytes_sent: usize,
        bytes_received: usize,
    },
    /// Batch hit schema propagation after DDL and should be retried.
    RetryableSchemaPropagation { detail: String },
    /// Batch had row-level errors.
    RowErrors { errors: Vec<RowError> },
    /// Batch had a request-level error.
    RequestError { error: BQError },
}

/// Aggregated result of processing a set of append batches.
#[derive(Debug)]
enum AppendProcessingResult {
    Success {
        bytes_sent: usize,
        bytes_received: usize,
    },
    Retry {
        bytes_sent: usize,
        bytes_received: usize,
        pending_requests: Vec<RetryableAppendRequest>,
    },
    Error(EtlError),
}

/// A batch append request that should be retried after schema propagation finishes.
#[derive(Debug)]
struct RetryableAppendRequest {
    request: BatchAppendRequest<BigQueryTableRow>,
    detail: String,
}

/// Builds a concise description for a set of schema-propagation retries.
fn format_retryable_append_requests(requests: &[RetryableAppendRequest]) -> String {
    match requests.split_first() {
        None => "schema propagation error".to_string(),
        Some((first, [])) => first.detail.clone(),
        Some((first, rest)) => {
            let distinct_other_details = rest
                .iter()
                .filter(|request| request.detail != first.detail)
                .count();

            if distinct_other_details == 0 {
                format!("{} ({} batches)", first.detail, requests.len())
            } else {
                format!(
                    "{} ({} batches, {} additional retry details)",
                    first.detail,
                    requests.len(),
                    distinct_other_details
                )
            }
        }
    }
}

/// Creates a per-batch BigQuery trace identifier for Storage Write requests.
fn create_append_trace_id(pipeline_id: PipelineId, table_id: &str, batch_index: usize) -> String {
    format!(
        "supabase_etl_{pipeline_id}_{table_id}_{batch_index}_{}",
        random::<u32>()
    )
}

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
                return batch_process_result_from_request_error(BQError::from(status));
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

/// Converts request-level append errors to retryable or terminal outcomes.
fn append_processing_result_from_request_error(
    error: BQError,
    append_requests: Vec<BatchAppendRequest<BigQueryTableRow>>,
) -> AppendProcessingResult {
    if is_retryable_schema_propagation_error(&error) {
        let detail = bq_error_to_etl_error(error)
            .detail()
            .unwrap_or("schema propagation error")
            .to_string();
        AppendProcessingResult::Retry {
            pending_requests: append_requests
                .into_iter()
                .map(|request| RetryableAppendRequest {
                    request,
                    detail: detail.clone(),
                })
                .collect(),
            bytes_sent: 0,
            bytes_received: 0,
        }
    } else {
        AppendProcessingResult::Error(bq_error_to_etl_error(error))
    }
}

/// Builds the error returned when local schema-propagation retries are exhausted.
///
/// The destination absorbs the common short propagation delay locally. If BigQuery still has not
/// accepted the schema once that bounded window expires, the worker-level timed retry policy should
/// take over.
fn schema_propagation_timeout_error(detail: &str) -> EtlError {
    etl_error!(
        ErrorKind::DestinationAtomicBatchRetryable,
        "BigQuery schema propagation timed out",
        format!(
            "BigQuery did not accept the updated schema within {} seconds after DDL: {}",
            SCHEMA_PROPAGATION_RETRY_TIMEOUT.as_secs(),
            detail
        )
    )
}

/// Converts BigQuery row errors to ETL destination errors.
fn row_error_to_etl_error(err: RowError) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "BigQuery row error",
        format!("{err:?}")
    )
}

/// Converts a request-level append error into a [`BatchProcessResult`].
fn batch_process_result_from_request_error(error: BQError) -> BatchProcessResult {
    if is_retryable_schema_propagation_error(&error) {
        BatchProcessResult::RetryableSchemaPropagation {
            detail: error.to_string(),
        }
    } else {
        BatchProcessResult::RequestError { error }
    }
}

/// Converts BigQuery errors to ETL errors with appropriate classification.
///
/// Maps BigQuery error types to ETL error kinds for consistent error handling.
fn bq_error_to_etl_error(err: BQError) -> EtlError {
    let (kind, description) = match &err {
        // Authentication related errors
        BQError::InvalidServiceAccountKey(_) => (
            ErrorKind::DestinationAuthenticationError,
            "Invalid BigQuery service account key",
        ),
        BQError::InvalidServiceAccountAuthenticator(_) => (
            ErrorKind::DestinationAuthenticationError,
            "Invalid BigQuery service account authenticator",
        ),
        BQError::InvalidInstalledFlowAuthenticator(_) => (
            ErrorKind::DestinationAuthenticationError,
            "Invalid BigQuery installed flow authenticator",
        ),
        BQError::InvalidApplicationDefaultCredentialsAuthenticator(_) => (
            ErrorKind::DestinationAuthenticationError,
            "Invalid BigQuery application default credentials",
        ),
        BQError::InvalidAuthorizedUserAuthenticator(_) => (
            ErrorKind::DestinationAuthenticationError,
            "Invalid BigQuery authorized user authenticator",
        ),
        BQError::AuthError(_) => (
            ErrorKind::DestinationAuthenticationError,
            "BigQuery authentication error",
        ),
        BQError::YupAuthError(_) => (
            ErrorKind::DestinationAuthenticationError,
            "BigQuery OAuth authentication error",
        ),
        BQError::NoToken => (
            ErrorKind::DestinationAuthenticationError,
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
            Code::NotFound => (
                ErrorKind::DestinationTableMissing,
                "BigQuery entity not found",
            ),

            // Code::AlreadyExists (6) - Entity conflict during creation.
            // For streaming with offsets, may indicate row was already written. Never retry.
            Code::AlreadyExists => (
                ErrorKind::DestinationTableAlreadyExists,
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

    let mut detail = err.to_string();
    if let BQError::TonicStatusError(status) = &err {
        let storage_error_codes = decode_storage_error_codes(status);
        if !storage_error_codes.is_empty() {
            detail.push_str(&format!(
                " [storage_error_codes={}]",
                storage_error_codes.join(",")
            ));
        }
    }

    etl_error!(kind, description, detail)
}

/// Decodes BigQuery Storage error codes from gRPC status details when present.
fn decode_storage_error_codes(status: &tonic::Status) -> Vec<&'static str> {
    let Ok(rpc_status) = GoogleRpcStatus::decode(status.details()) else {
        return Vec::new();
    };

    rpc_status
        .details
        .iter()
        .filter(|detail| {
            detail.type_url.rsplit('/').next() == Some(BIGQUERY_STORAGE_ERROR_TYPE_NAME)
        })
        .filter_map(|detail| StorageError::decode(detail.value.as_slice()).ok())
        .filter_map(|storage_error| StorageErrorCode::try_from(storage_error.code).ok())
        .map(|code| code.as_str_name())
        .collect()
}

/// Returns true when the request-level BigQuery error matches the documented schema propagation case.
///
/// BigQuery documents `StorageErrorCode::SCHEMA_MISMATCH_EXTRA_FIELDS` as the structured signal
/// for schema mismatch during appends. We fall back to the observed rename-path message only when
/// BigQuery does not provide a structured storage error code in the gRPC details.
fn is_retryable_schema_propagation_error(error: &BQError) -> bool {
    let BQError::TonicStatusError(status) = error else {
        return false;
    };

    if status.code() != Code::InvalidArgument {
        return false;
    }

    let storage_error_codes = decode_storage_error_codes(status);
    if storage_error_codes
        .iter()
        .any(|code| *code == StorageErrorCode::SchemaMismatchExtraFields.as_str_name())
    {
        return true;
    }

    let message = status.message().to_ascii_lowercase();
    message.contains("missing in the proto message")
        || message.contains("schema_mismatch_extra_field")
        || message.contains("schema_mismatch_extra_fields")
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
        replicated_table_schema: &ReplicatedTableSchema,
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<bool> {
        let table_exists = self.table_exists(dataset_id, table_id).await?;

        let full_table_name = self.full_table_name(dataset_id, table_id)?;

        let columns_spec = Self::create_columns_spec(replicated_table_schema)?;
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
        replicated_table_schema: &ReplicatedTableSchema,
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<bool> {
        if self.table_exists(dataset_id, table_id).await? {
            return Ok(false);
        }

        self.create_table(
            dataset_id,
            table_id,
            replicated_table_schema,
            max_staleness_mins,
        )
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
        replicated_table_schema: &ReplicatedTableSchema,
        max_staleness_mins: Option<u16>,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;

        let columns_spec = Self::create_columns_spec(replicated_table_schema)?;
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

    /// Adds a column to an existing BigQuery table.
    ///
    /// Executes an ALTER TABLE ADD COLUMN statement to add a new column with the
    /// specified schema. New columns must be nullable in BigQuery.
    pub async fn add_column(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_schema: &ColumnSchema,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = Self::sanitize_identifier(&column_schema.name, "BigQuery column name")?;
        let column_type = Self::postgres_to_bigquery_type(&column_schema.typ);

        info!(
            "adding column `{column_name}` ({column_type}) to table {full_table_name} in BigQuery"
        );

        // BigQuery requires new columns to be nullable (no NOT NULL constraint allowed). Also, we wouldn't
        // be able to add it nonetheless since we don't have a way to set a default value for past columns.
        let query =
            format!("alter table {full_table_name} add column `{column_name}` {column_type}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Drops a column from an existing BigQuery table.
    ///
    /// Executes an ALTER TABLE DROP COLUMN statement to remove the specified column.
    pub async fn drop_column(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_name: &str,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = Self::sanitize_identifier(column_name, "BigQuery column name")?;

        info!("dropping column `{column_name}` from table {full_table_name} in BigQuery");

        let query = format!("alter table {full_table_name} drop column `{column_name}`");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Renames a column in an existing BigQuery table.
    ///
    /// Executes an ALTER TABLE RENAME COLUMN statement to rename the specified column.
    pub async fn rename_column(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        old_name: &str,
        new_name: &str,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let old_name = Self::sanitize_identifier(old_name, "BigQuery column name")?;
        let new_name = Self::sanitize_identifier(new_name, "BigQuery column name")?;

        info!(
            "renaming column `{old_name}` to `{new_name}` in table {full_table_name} in BigQuery"
        );

        let query =
            format!("alter table {full_table_name} rename column `{old_name}` to `{new_name}`");

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

    /// Appends table batches to BigQuery using the concurrent Storage Write API.
    ///
    /// Accepts pre-constructed append requests and processes them concurrently.
    ///
    /// Retries for transient request and transport failures are handled inside the
    /// underlying Storage Write API library. This method also retries the narrow class of
    /// schema propagation failures that can happen after DDL, then converts final failures
    /// into ETL errors.
    pub async fn append_table_batches(
        &self,
        append_requests: Vec<BatchAppendRequest<BigQueryTableRow>>,
    ) -> EtlResult<(usize, usize)> {
        if append_requests.is_empty() {
            return Ok((0, 0));
        }

        let mut pending_requests = append_requests;
        let mut total_bytes_sent = 0;
        let mut total_bytes_received = 0;

        let started_at = Instant::now();
        let mut attempt = 1;
        let mut retry_delay = SCHEMA_PROPAGATION_RETRY_DELAY;

        loop {
            match self.append_table_batches_once(pending_requests).await? {
                AppendProcessingResult::Success {
                    bytes_sent,
                    bytes_received,
                } => {
                    total_bytes_sent += bytes_sent;
                    total_bytes_received += bytes_received;

                    return Ok((total_bytes_sent, total_bytes_received));
                }
                AppendProcessingResult::Retry {
                    pending_requests: next_pending_requests,
                    bytes_sent,
                    bytes_received,
                } => {
                    total_bytes_sent += bytes_sent;
                    total_bytes_received += bytes_received;

                    let retry_summary = format_retryable_append_requests(&next_pending_requests);
                    pending_requests = next_pending_requests
                        .into_iter()
                        .map(|request| request.request)
                        .collect();

                    let pending_batch_count = pending_requests.len();
                    if pending_batch_count == 0 {
                        return Ok((total_bytes_sent, total_bytes_received));
                    }

                    let elapsed = started_at.elapsed();
                    let remaining_timeout =
                        SCHEMA_PROPAGATION_RETRY_TIMEOUT.saturating_sub(elapsed);

                    if remaining_timeout.is_zero() {
                        return Err(schema_propagation_timeout_error(&retry_summary));
                    }

                    let sleep_delay = retry_delay.min(remaining_timeout);

                    if sleep_delay.is_zero() {
                        return Err(schema_propagation_timeout_error(&retry_summary));
                    }

                    warn!(
                        attempt,
                        pending_batch_count,
                        retry_delay_ms = sleep_delay.as_millis() as u64,
                        error_detail = %retry_summary,
                        "bigquery schema change still propagating, retrying append"
                    );

                    sleep(sleep_delay).await;

                    retry_delay = (retry_delay * 2).min(SCHEMA_PROPAGATION_MAX_RETRY_DELAY);
                    attempt += 1;
                }
                AppendProcessingResult::Error(error) => return Err(error),
            }
        }
    }

    /// Executes a single append attempt and classifies the result.
    async fn append_table_batches_once(
        &self,
        append_requests: Vec<BatchAppendRequest<BigQueryTableRow>>,
    ) -> EtlResult<AppendProcessingResult> {
        if append_requests.is_empty() {
            return Ok(AppendProcessingResult::Success {
                bytes_sent: 0,
                bytes_received: 0,
            });
        }

        debug!(
            batch_count = append_requests.len(),
            "streaming table batches concurrently"
        );

        let batch_append_results = self
            .client
            .storage()
            .append_table_batches(append_requests.clone())
            .await
            .inspect_err(|err| {
                let error_code = error_code_label(err);

                counter!(
                    ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
                    "error_code" => error_code
                )
                .increment(1);
            });

        let batch_append_results = match batch_append_results {
            Ok(results) => results,
            Err(error) => {
                return Ok(append_processing_result_from_request_error(
                    error,
                    append_requests,
                ));
            }
        };

        let mut total_bytes_sent = 0;
        let mut total_bytes_received = 0;
        let mut errors = Vec::new();
        let mut retryable_batch_details = vec![None; append_requests.len()];

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
                BatchProcessResult::RetryableSchemaPropagation { detail } => {
                    retryable_batch_details[batch_index] = Some(detail);
                }
                BatchProcessResult::RowErrors { errors: row_errors } => {
                    let error_count = row_errors.len();
                    if error_count > 0 {
                        error!(
                            batch_index,
                            error_count, "batch has row errors, failing append operation"
                        );

                        counter!(ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL)
                            .increment(error_count as u64);
                    }

                    for row_error in row_errors {
                        errors.push(row_error_to_etl_error(row_error));
                    }
                }
                BatchProcessResult::RequestError {
                    error: request_error,
                } => {
                    let error_code = error_code_label(&request_error);
                    warn!(
                        batch_index,
                        error = %request_error,
                        "batch failed with request error after library retries"
                    );

                    counter!(
                        ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
                        "error_code" => error_code
                    )
                    .increment(1);

                    errors.push(bq_error_to_etl_error(request_error));
                }
            }
        }

        if !errors.is_empty() {
            return Ok(AppendProcessingResult::Error(errors.into()));
        }

        if retryable_batch_details.iter().any(Option::is_some) {
            let pending_requests = append_requests
                .into_iter()
                .zip(retryable_batch_details)
                .filter_map(|(request, detail)| {
                    detail.map(|detail| RetryableAppendRequest { request, detail })
                })
                .collect();

            return Ok(AppendProcessingResult::Retry {
                bytes_sent: total_bytes_sent,
                bytes_received: total_bytes_received,
                pending_requests,
            });
        }

        Ok(AppendProcessingResult::Success {
            bytes_sent: total_bytes_sent,
            bytes_received: total_bytes_received,
        })
    }

    /// Invalidates all connections used by the storage write api.
    pub async fn invalidate_all_connections(&self) {
        self.client.storage().invalidate_all_connections().await;
    }

    /// Creates a batch append request for a specific table with validated rows.
    ///
    /// Converts TableRow instances to BigQueryTableRow and creates a properly configured
    /// [`BatchAppendRequest`] for efficient append retries.
    pub fn create_batch_append_request(
        &self,
        pipeline_id: PipelineId,
        batch_index: usize,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: TableDescriptor,
        validated_rows: Vec<BigQueryTableRow>,
    ) -> EtlResult<BatchAppendRequest<BigQueryTableRow>> {
        let stream_name = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_id.to_string(),
        );

        let table_batch = TableBatch::new(stream_name, table_descriptor, validated_rows);
        let trace_id =
            create_append_trace_id(pipeline_id, table_batch.stream_name().table(), batch_index);

        Ok(BatchAppendRequest::new(table_batch, trace_id))
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
    /// Generates a primary key constraint clause from columns marked as primary key,
    /// sorted by their ordinal position to ensure correct composite key ordering.
    fn add_primary_key_clause(
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<Option<String>> {
        let mut primary_key_columns: Vec<_> = replicated_table_schema
            .column_schemas()
            .filter(|s| s.primary_key())
            .collect();

        // If no primary key columns are marked, return early.
        if primary_key_columns.is_empty() {
            return Ok(None);
        }

        // Sort by primary_key_ordinal_position to ensure correct composite key ordering.
        primary_key_columns.sort_by_key(|c| c.primary_key_ordinal_position);

        let primary_key_columns: Vec<String> = primary_key_columns
            .into_iter()
            .map(|c| {
                Self::sanitize_identifier(&c.name, "BigQuery primary key column")
                    .map(|name| format!("`{name}`"))
            })
            .collect::<EtlResult<Vec<_>>>()?;

        let primary_key_clause = format!(
            ", primary key ({}) not enforced",
            primary_key_columns.join(",")
        );

        Ok(Some(primary_key_clause))
    }

    /// Builds complete column specifications for CREATE TABLE statements.
    fn create_columns_spec(replicated_table_schema: &ReplicatedTableSchema) -> EtlResult<String> {
        let mut column_spec = replicated_table_schema
            .column_schemas()
            .map(Self::column_spec)
            .collect::<EtlResult<Vec<_>>>()?
            .join(",");

        if let Some(primary_key_clause) = Self::add_primary_key_clause(replicated_table_schema)? {
            column_spec.push_str(&primary_key_clause);
        }

        Ok(format!("({column_spec})"))
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
        replicated_table_schema: &ReplicatedTableSchema,
        use_cdc_sequence_column: bool,
    ) -> TableDescriptor {
        let mut field_descriptors = vec![];
        let mut number = 1;

        for column_schema in replicated_table_schema.column_schemas() {
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
    use gcp_bigquery_client::google::cloud::bigquery::storage::v1::{
        AppendRowsResponse, append_rows_response,
    };
    use std::sync::Arc;

    use etl::types::{ReplicationMask, TableId, TableName, TableSchema};
    use std::collections::HashSet;

    fn successful_append_response() -> AppendRowsResponse {
        AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_string(),
            response: Some(append_rows_response::Response::AppendResult(
                append_rows_response::AppendResult { offset: None },
            )),
        }
    }

    /// Creates a test column schema with common defaults.
    ///
    /// This helper simplifies column schema creation in tests by providing sensible
    /// defaults for fields that are typically not relevant to the test logic.
    fn test_column(
        name: &str,
        typ: Type,
        ordinal_position: i32,
        nullable: bool,
        primary_key_ordinal: Option<i32>,
    ) -> ColumnSchema {
        ColumnSchema::new(
            name.to_string(),
            typ,
            -1,
            ordinal_position,
            primary_key_ordinal,
            nullable,
        )
    }

    /// Creates a [`ReplicatedTableSchema`] from test columns with all columns replicated.
    fn test_replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let column_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId(1), // Dummy table ID
            TableName::new("public".to_string(), "test_table".to_string()),
            columns,
        ));
        let replication_mask = ReplicationMask::build_or_all(&table_schema, &column_names);

        ReplicatedTableSchema::from_mask(table_schema, replication_mask)
    }

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
        let column_schema = test_column("test_col", Type::TEXT, 1, true, None);
        let spec = BigQueryClient::column_spec(&column_schema).expect("column spec generation");
        assert_eq!(spec, "`test_col` string");

        let not_null_column = test_column("id", Type::INT4, 1, false, Some(1));
        let not_null_spec =
            BigQueryClient::column_spec(&not_null_column).expect("not null column spec");
        assert_eq!(not_null_spec, "`id` int64 not null");

        let array_column = test_column("tags", Type::TEXT_ARRAY, 1, false, None);
        let array_spec = BigQueryClient::column_spec(&array_column).expect("array column spec");
        assert_eq!(array_spec, "`tags` array<string>");
    }

    #[test]
    fn test_column_spec_escapes_backticks() {
        let column_schema = test_column("pwn`name", Type::TEXT, 1, true, None);

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
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema_with_pk = test_replicated_schema(columns_with_pk);
        let pk_clause = BigQueryClient::add_primary_key_clause(&schema_with_pk)
            .expect("pk clause")
            .unwrap();
        assert_eq!(pk_clause, ", primary key (`id`) not enforced");

        // Composite primary key with correct ordinal positions.
        let columns_with_composite_pk = vec![
            test_column("tenant_id", Type::INT4, 1, false, Some(1)),
            test_column("id", Type::INT4, 2, false, Some(2)),
            test_column("name", Type::TEXT, 3, true, None),
        ];
        let schema_with_composite_pk = test_replicated_schema(columns_with_composite_pk);
        let composite_pk_clause = BigQueryClient::add_primary_key_clause(&schema_with_composite_pk)
            .unwrap()
            .expect("composite pk clause");
        assert_eq!(
            composite_pk_clause,
            ", primary key (`tenant_id`,`id`) not enforced"
        );

        // Composite primary key with reversed column order but correct ordinal positions.
        // The primary key clause should still be ordered by ordinal position.
        let columns_with_reversed_pk = vec![
            test_column("id", Type::INT4, 1, false, Some(2)),
            test_column("tenant_id", Type::INT4, 2, false, Some(1)),
            test_column("name", Type::TEXT, 3, true, None),
        ];
        let schema_with_reversed_pk = test_replicated_schema(columns_with_reversed_pk);
        let reversed_pk_clause = BigQueryClient::add_primary_key_clause(&schema_with_reversed_pk)
            .unwrap()
            .expect("reversed pk clause");
        assert_eq!(
            reversed_pk_clause,
            ", primary key (`tenant_id`,`id`) not enforced"
        );

        let columns_no_pk = vec![
            test_column("name", Type::TEXT, 1, true, None),
            test_column("age", Type::INT4, 2, true, None),
        ];
        let schema_no_pk = test_replicated_schema(columns_no_pk);
        let no_pk_clause =
            BigQueryClient::add_primary_key_clause(&schema_no_pk).expect("no pk clause");
        assert!(no_pk_clause.is_none());
    }

    #[test]
    fn test_create_columns_spec() {
        let columns = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
            test_column("active", Type::BOOL, 3, false, None),
        ];
        let schema = test_replicated_schema(columns);
        let spec = BigQueryClient::create_columns_spec(&schema).expect("columns spec");
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
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
            test_column("active", Type::BOOL, 3, false, None),
            test_column("tags", Type::TEXT_ARRAY, 4, false, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = BigQueryClient::column_schemas_to_table_descriptor(&schema, true);

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
            test_column("uuid_col", Type::UUID, 1, true, None),
            test_column("json_col", Type::JSON, 2, true, None),
            test_column("bytea_col", Type::BYTEA, 3, true, None),
            test_column("numeric_col", Type::NUMERIC, 4, true, None),
            test_column("date_col", Type::DATE, 5, true, None),
            test_column("time_col", Type::TIME, 6, true, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = BigQueryClient::column_schemas_to_table_descriptor(&schema, true);

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
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema = test_replicated_schema(columns);

        // Simulate the query generation logic
        let full_table_name = format!(
            "`{project}.{dataset}.{table}`",
            project = BigQueryClient::sanitize_identifier(project_id, "project").unwrap(),
            dataset = BigQueryClient::sanitize_identifier(dataset_id, "dataset").unwrap(),
            table = BigQueryClient::sanitize_identifier(table_id, "table").unwrap()
        );
        let columns_spec = BigQueryClient::create_columns_spec(&schema).unwrap();
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

        let columns = vec![test_column("id", Type::INT4, 1, false, Some(1))];
        let schema = test_replicated_schema(columns);

        // Simulate the query generation logic with staleness
        let full_table_name = format!(
            "`{project}.{dataset}.{table}`",
            project = BigQueryClient::sanitize_identifier(project_id, "project").unwrap(),
            dataset = BigQueryClient::sanitize_identifier(dataset_id, "dataset").unwrap(),
            table = BigQueryClient::sanitize_identifier(table_id, "table").unwrap()
        );
        let columns_spec = BigQueryClient::create_columns_spec(&schema).unwrap();
        let max_staleness_option = BigQueryClient::max_staleness_option(max_staleness_mins);
        let query = format!(
            "create or replace table {full_table_name} {columns_spec} {max_staleness_option}"
        );

        let expected_query = "create or replace table `test-project.test_dataset.test_table` (`id` int64 not null, primary key (`id`) not enforced) options (max_staleness = interval 15 minute)";
        assert_eq!(query, expected_query);
    }

    #[test]
    fn test_process_single_batch_append_result_retries_pure_schema_propagation_errors() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![Err(tonic::Status::invalid_argument(
                "schema_mismatch_extra_fields",
            ))],
            bytes_sent: 128,
        });

        assert!(matches!(
            result,
            BatchProcessResult::RetryableSchemaPropagation { .. }
        ));
    }

    #[test]
    fn test_process_single_batch_append_result_retries_partial_success_schema_propagation() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![
                Ok(successful_append_response()),
                Err(tonic::Status::invalid_argument(
                    "schema_mismatch_extra_fields",
                )),
            ],
            bytes_sent: 128,
        });

        assert!(matches!(
            result,
            BatchProcessResult::RetryableSchemaPropagation { .. }
        ));
    }

    #[test]
    fn test_schema_propagation_timeout_error_is_worker_retryable() {
        let error = schema_propagation_timeout_error("schema lag");

        assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);
        assert_eq!(
            error.description(),
            Some("BigQuery schema propagation timed out")
        );
    }
}
