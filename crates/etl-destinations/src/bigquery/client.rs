use std::fmt;

use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    types::{
        Cell, ColumnSchema, DefaultExpression, PipelineId, ReplicatedTableSchema, Type,
        is_array_type, parse_default_expression,
    },
};
use gcp_bigquery_client::{
    Client,
    client_builder::ClientBuilder,
    error::BQError,
    google::{
        cloud::bigquery::storage::v1::{
            RowError, StorageError, row_error::RowErrorCode, storage_error::StorageErrorCode,
        },
        rpc::Status as GoogleRpcStatus,
    },
    model::{
        query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
        query_parameter_value::QueryParameterValue, query_request::QueryRequest,
        query_response::ResultSet,
    },
    storage::{
        BatchAppendRequest, BatchAppendResult, ColumnMode, ColumnType, FieldDescriptor,
        StorageApiConfig, StreamName, TableBatch, TableDescriptor,
    },
    yup_oauth2::parse_service_account_key,
};
use metrics::counter;
use prost::Message;
use rand::random;
use tokio::time::{Duration, Instant, sleep};
use tonic::Code;
use tracing::{debug, error, info, warn};

use crate::{
    bigquery::{
        encoding::BigQueryTableRow,
        metrics::{
            ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL, ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL,
        },
        sql::{quote_identifier, quote_information_schema_tables_path, quote_table_path},
    },
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
};

/// Multiplier for calculating max inflight requests from pool size.
///
/// The maximum number of inflight requests is `connection_pool_size *
/// MAX_INFLIGHT_REQUESTS_PER_CONNECTION`.
const MAX_INFLIGHT_REQUESTS_PER_CONNECTION: usize = 100;

/// Maximum safe value for inflight requests to prevent resource exhaustion.
///
/// This upper bound ensures reasonable memory usage and prevents overflow when
/// computing max inflight requests from connection pool size.
const MAX_SAFE_INFLIGHT_REQUESTS: usize = 100_000;
/// Maximum time to retry writes while the BigQuery Storage Write API is still
/// using stale table metadata.
///
/// Google documents schema update detection as happening on the order of
/// minutes.
const STORAGE_WRITE_METADATA_LAG_RETRY_TIMEOUT: Duration = Duration::from_secs(180);
/// Initial backoff when retrying writes during storage write metadata lag.
const STORAGE_WRITE_METADATA_LAG_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Maximum backoff when retrying writes during storage write metadata lag.
const STORAGE_WRITE_METADATA_LAG_MAX_RETRY_DELAY: Duration = Duration::from_secs(15);
/// Retry policy for transient BigQuery query job failures.
const QUERY_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 4,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(8),
};
/// BigQuery response reasons that are transient even when surfaced with a 4xx
/// status code.
const TRANSIENT_BIGQUERY_QUERY_REASONS: &[&str] =
    &["backendError", "jobBackendError", "jobRateLimitExceeded", "rateLimitExceeded"];
/// Protobuf type name for BigQuery storage errors embedded in gRPC status
/// details.
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
pub(super) enum BigQueryOperationType {
    Upsert,
    Delete,
}

impl BigQueryOperationType {
    /// Returns the BigQuery CDC operation string.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Upsert => "UPSERT",
            Self::Delete => "DELETE",
        }
    }

    /// Converts the operation type into a [`Cell`] for streaming.
    pub(super) fn into_cell(self) -> Cell {
        Cell::String(self.as_str().to_owned())
    }
}

impl fmt::Display for BigQueryOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Result of processing a single batch, used to determine retry strategy.
#[derive(Debug)]
enum BatchProcessResult {
    /// Batch succeeded with byte metrics.
    Success { bytes_sent: usize, bytes_received: usize },
    /// Batch hit storage write metadata lag after DDL and should be retried.
    RetryableStorageWriteMetadataLag { detail: String },
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

/// A batch append request that should be retried after Storage Write metadata
/// lag clears.
#[derive(Debug)]
struct RetryableAppendRequest {
    request: BatchAppendRequest<BigQueryTableRow>,
    detail: String,
}

/// Builds a concise description for a set of Storage Write metadata lag
/// retries.
fn format_retryable_storage_write_metadata_lag_requests(
    requests: &[RetryableAppendRequest],
) -> String {
    match requests.split_first() {
        None => "storage write metadata lag error".to_owned(),
        Some((first, [])) => first.detail.clone(),
        Some((first, rest)) => {
            let distinct_other_details =
                rest.iter().filter(|request| request.detail != first.detail).count();

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
    format!("supabase_etl_{pipeline_id}_{table_id}_{batch_index}_{}", random::<u32>())
}

/// Computes the maximum number of inflight requests for the BigQuery Storage
/// Write API.
///
/// Uses checked arithmetic to safely multiply the connection pool size by the
/// per-connection limit, clamping the result to [`MAX_SAFE_INFLIGHT_REQUESTS`]
/// to prevent overflow and resource exhaustion.
fn compute_max_inflight_requests(connection_pool_size: usize) -> usize {
    connection_pool_size
        .checked_mul(MAX_INFLIGHT_REQUESTS_PER_CONNECTION)
        .unwrap_or(MAX_SAFE_INFLIGHT_REQUESTS)
        .min(MAX_SAFE_INFLIGHT_REQUESTS)
}

/// Processes a single batch result and determines success or failure mode.
///
/// Row errors are permanent failures (bad data, schema mismatch) and fail
/// immediately. Request errors are surfaced for retry decision by the caller.
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
        BatchProcessResult::Success { bytes_sent, bytes_received: total_bytes_received }
    }
}

/// Extracts the gRPC status code as a string from a [`BQError`] for metrics
/// labeling.
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
    if let Some(detail) = retryable_storage_write_metadata_lag_detail(&error) {
        AppendProcessingResult::Retry {
            pending_requests: append_requests
                .into_iter()
                .map(|request| RetryableAppendRequest { request, detail: detail.clone() })
                .collect(),
            bytes_sent: 0,
            bytes_received: 0,
        }
    } else {
        AppendProcessingResult::Error(bq_error_to_etl_error(error))
    }
}

/// Returns whether a BigQuery query error is transient.
fn is_transient_query_error(error: &BQError) -> RetryDecision {
    match error {
        BQError::RequestError(_) => RetryDecision::Retry,
        BQError::ResponseError { error } if error.error.code >= 500 => RetryDecision::Retry,
        BQError::ResponseError { error }
            if error.error.errors.iter().any(|nested_error| {
                nested_error.get("reason").is_some_and(|reason| {
                    TRANSIENT_BIGQUERY_QUERY_REASONS.contains(&reason.as_str())
                })
            }) =>
        {
            RetryDecision::Retry
        }
        _ => RetryDecision::Stop,
    }
}

/// Logs a transient BigQuery query retry.
fn log_query_retry(attempt: crate::retry::RetryAttempt<'_, BQError>) {
    warn!(
        retry_index = attempt.retry_index,
        max_retries = attempt.max_retries,
        sleep_ms = attempt.sleep_delay.as_millis(),
        error = %attempt.error,
        "retrying transient BigQuery query error"
    );
}

/// Builds the error returned when local Storage Write metadata lag retries are
/// exhausted.
///
/// The destination absorbs the common short lag window locally. If
/// BigQuery still has not accepted the storage write metadata once that bounded
/// window expires, the worker-level timed retry policy should take over.
fn storage_write_metadata_lag_timeout_error(detail: &str) -> EtlError {
    etl_error!(
        ErrorKind::DestinationAtomicBatchRetryable,
        "BigQuery storage write metadata lag timed out",
        format!(
            "BigQuery did not accept the storage write metadata within {} seconds after DDL: {}",
            STORAGE_WRITE_METADATA_LAG_RETRY_TIMEOUT.as_secs(),
            detail
        )
    )
}

/// Converts BigQuery row errors to ETL destination errors.
fn row_error_to_etl_error(err: RowError) -> EtlError {
    let code = RowErrorCode::try_from(err.code)
        .map(|code| code.as_str_name())
        .unwrap_or("UNKNOWN_ROW_ERROR_CODE");

    etl_error!(
        ErrorKind::DestinationError,
        "BigQuery rejected a row in the append request",
        format!(
            "BigQuery rejected row {} with code {}. The detailed BigQuery message was omitted to \
             avoid exposing source row values.",
            err.index, code
        )
    )
}

/// Converts a request-level append error into a [`BatchProcessResult`].
fn batch_process_result_from_request_error(error: BQError) -> BatchProcessResult {
    if retryable_storage_write_metadata_lag_detail(&error).is_some() {
        BatchProcessResult::RetryableStorageWriteMetadataLag { detail: error.to_string() }
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
        BQError::InvalidServiceAccountKey(_) => {
            (ErrorKind::DestinationAuthenticationError, "Invalid BigQuery service account key")
        }
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
        BQError::AuthError(_) => {
            (ErrorKind::DestinationAuthenticationError, "BigQuery authentication error")
        }
        BQError::YupAuthError(_) => {
            (ErrorKind::DestinationAuthenticationError, "BigQuery OAuth authentication error")
        }
        BQError::NoToken => {
            (ErrorKind::DestinationAuthenticationError, "BigQuery authentication token missing")
        }

        // Network and transport errors
        BQError::RequestError(_) => (ErrorKind::DestinationIoError, "BigQuery request failed"),
        BQError::TonicTransportError(_) => {
            (ErrorKind::DestinationIoError, "BigQuery transport error")
        }

        // Query and data errors
        BQError::ResponseError { .. } => {
            (ErrorKind::DestinationQueryFailed, "BigQuery response error")
        }
        BQError::NoDataAvailable => {
            (ErrorKind::InvalidState, "BigQuery result set positioning error")
        }
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
        BQError::SerializationError(_) => {
            (ErrorKind::SerializationError, "BigQuery JSON serialization error")
        }

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
            Code::Unauthenticated => {
                (ErrorKind::DestinationError, "BigQuery authentication failed")
            }

            // Code::InvalidArgument (3) - Malformed request or invalid data.
            // Client bug that requires code changes. Never retry.
            Code::InvalidArgument => (ErrorKind::DestinationError, "BigQuery invalid argument"),

            // Code::NotFound (5) - Resource doesn't exist.
            // Requires creating the resource (table, dataset, stream) first. Never retry.
            Code::NotFound => (ErrorKind::DestinationTableMissing, "BigQuery entity not found"),

            // Code::AlreadyExists (6) - Entity conflict during creation.
            // For streaming with offsets, may indicate row was already written. Never retry.
            Code::AlreadyExists => {
                (ErrorKind::DestinationTableAlreadyExists, "BigQuery entity already exists")
            }

            // Code::OutOfRange (11) - Invalid offset for streaming.
            // Offset beyond current stream end. Requires application-level recovery. Never retry.
            Code::OutOfRange => (ErrorKind::DestinationError, "BigQuery offset out of range"),

            // Code::Unimplemented (12) - Operation not available.
            // Feature not supported by BigQuery. Never retry.
            Code::Unimplemented => {
                (ErrorKind::DestinationError, "BigQuery operation not supported")
            }

            // Code::DataLoss (15) - Unrecoverable data corruption.
            // Severe error requiring manual intervention. Never retry.
            Code::DataLoss => (ErrorKind::DestinationError, "BigQuery data loss"),

            // Code::Ok (0) - Should never be an error
            Code::Ok => (ErrorKind::DestinationError, "BigQuery unexpected ok status"),
        },

        // Concurrency and task errors
        BQError::SemaphorePermitError(_) => {
            (ErrorKind::DestinationError, "BigQuery semaphore permit error")
        }
        BQError::TokioTaskError(_) => {
            (ErrorKind::DestinationError, "BigQuery task execution error")
        }
        BQError::ConnectionPoolError(_) => {
            (ErrorKind::DestinationError, "BigQuery connection pool error")
        }
    };

    let detail = if let BQError::TonicStatusError(status) = &err {
        let storage_error_codes = decode_storage_error_codes(status);
        (!storage_error_codes.is_empty())
            .then(|| format!("storage_error_codes={}", storage_error_codes.join(",")))
    } else {
        None
    };

    if let Some(detail) = detail {
        etl_error!(kind, description, detail, source: err)
    } else {
        etl_error!(kind, description, source: err)
    }
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

/// Returns true when the request-level BigQuery error matches the documented
/// storage write metadata lag case.
///
/// BigQuery documents `StorageErrorCode::SCHEMA_MISMATCH_EXTRA_FIELDS` as the
/// structured signal for schema mismatch during appends. We fall back to the
/// observed rename-path message only when BigQuery does not provide a
/// structured storage error code in the gRPC details.
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
        || message.contains("extra proto fields")
        || message.contains("schema_mismatch_extra_field")
        || message.contains("schema_mismatch_extra_fields")
}

/// Returns true for BigQuery's transient default-stream error after a table is
/// dropped and recreated with the same name.
fn is_retryable_table_recreation_error(error: &BQError) -> bool {
    let BQError::TonicStatusError(status) = error else {
        return false;
    };

    status.code() == Code::NotFound
        && status.message().to_ascii_lowercase().contains("is re-created")
}

/// Returns retry detail when a Storage Write append failed due to BigQuery
/// metadata propagation after DDL.
fn retryable_storage_write_metadata_lag_detail(error: &BQError) -> Option<String> {
    if is_retryable_schema_propagation_error(error) {
        return Some(error.to_string());
    }

    if is_retryable_table_recreation_error(error) {
        return Some(error.to_string());
    }

    None
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
    /// Authenticates with BigQuery using the service account key at the
    /// specified file path. Configures the Storage Write API with the given
    /// pool size.
    pub async fn new_with_key_path(
        project_id: BigQueryProjectId,
        sa_key_file: &str,
        connection_pool_size: usize,
    ) -> EtlResult<BigQueryClient> {
        let max_inflight_requests = compute_max_inflight_requests(connection_pool_size);
        let storage_config = StorageApiConfig { connection_pool_size, max_inflight_requests };

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_service_account_key_file(sa_key_file)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] from a service account key JSON string.
    ///
    /// Parses and uses the provided service account key to authenticate with
    /// BigQuery. Configures the Storage Write API with the given pool size.
    pub async fn new_with_key(
        project_id: BigQueryProjectId,
        sa_key: &str,
        connection_pool_size: usize,
    ) -> EtlResult<BigQueryClient> {
        let max_inflight_requests = compute_max_inflight_requests(connection_pool_size);
        let storage_config = StorageApiConfig { connection_pool_size, max_inflight_requests };

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
        let storage_config = StorageApiConfig { connection_pool_size, max_inflight_requests };

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_application_default_credentials()
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Creates a new [`BigQueryClient`] using OAuth2 installed flow
    /// authentication.
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
        let storage_config = StorageApiConfig { connection_pool_size, max_inflight_requests };

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_installed_flow_authenticator(secret, persistent_file_path)
            .await
            .map_err(bq_error_to_etl_error)?;

        Ok(BigQueryClient { project_id, client })
    }

    /// Returns the fully qualified BigQuery table name.
    ///
    /// Formats the table name as `project_id.dataset_id.table_id` with proper
    /// quoting.
    pub fn full_table_name(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<String> {
        quote_table_path(&self.project_id, dataset_id, table_id)
    }

    /// Creates a table in BigQuery if it doesn't already exist, otherwise
    /// efficiently truncates and recreates the table with the same schema.
    ///
    /// This method uses BigQuery's CREATE OR REPLACE TABLE statement which is
    /// more efficient than dropping and recreating as it preserves table
    /// metadata and permissions.
    ///
    /// Returns `true` if the table was created fresh, `false` if it already
    /// existed and was replaced.
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
            "".to_owned()
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

        self.create_table(dataset_id, table_id, replicated_table_schema, max_staleness_mins)
            .await?;

        Ok(true)
    }

    /// Creates a new table in the BigQuery dataset.
    ///
    /// Builds and executes a CREATE TABLE statement with the provided column
    /// schemas and optional staleness configuration for CDC operations.
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
            "".to_owned()
        };

        info!(%full_table_name, "creating table in bigquery");

        let query = format!("create table {full_table_name} {columns_spec} {max_staleness_option}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Truncates all data from a BigQuery table.
    ///
    /// Executes a TRUNCATE TABLE statement to remove all rows while preserving
    /// the table structure.
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
    /// This is used during truncation operations to redirect the view to a new
    /// table version.
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

    /// Drops a view from BigQuery.
    ///
    /// Executes a DROP VIEW statement to remove the logical view if it exists.
    pub async fn drop_view_if_exists(
        &self,
        dataset_id: &BigQueryDatasetId,
        view_name: &BigQueryTableId,
    ) -> EtlResult<()> {
        let full_view_name = self.full_table_name(dataset_id, view_name)?;

        info!(%full_view_name, "dropping view from bigquery");

        let query = format!("drop view if exists {full_view_name}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Drops a table from BigQuery.
    ///
    /// Executes a DROP TABLE statement to remove the table and all its data.
    pub async fn drop_table_if_exists(
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

    /// Lists physical sequenced table ids for a base table.
    ///
    /// Queries `INFORMATION_SCHEMA.TABLES` instead of using the destination's
    /// local cache so reset cleanup can remove versions left behind by earlier
    /// processes.
    pub async fn list_sequenced_table_ids(
        &self,
        dataset_id: &BigQueryDatasetId,
        base_table_id: &BigQueryTableId,
    ) -> EtlResult<Vec<BigQueryTableId>> {
        info!(%dataset_id, %base_table_id, "listing sequenced tables from bigquery");

        let information_schema_tables =
            quote_information_schema_tables_path(&self.project_id, dataset_id)?;
        let query = format!(
            "select table_name from {information_schema_tables} where table_type = 'BASE TABLE' \
             and starts_with(table_name, @table_name_prefix) order by table_name"
        );
        let mut request = QueryRequest::new(query);
        request.parameter_mode = Some("NAMED".to_owned());
        request.query_parameters = Some(vec![QueryParameter {
            name: Some("table_name_prefix".to_owned()),
            parameter_type: Some(QueryParameterType {
                r#type: "STRING".to_owned(),
                ..Default::default()
            }),
            parameter_value: Some(QueryParameterValue {
                value: Some(format!("{base_table_id}_")),
                ..Default::default()
            }),
        }]);

        let mut result_set = self.query(request).await?;
        let mut table_ids = Vec::new();

        while result_set.next_row() {
            if let Some(table_id) =
                result_set.get_string_by_name("table_name").map_err(bq_error_to_etl_error)?
            {
                table_ids.push(table_id);
            }
        }

        Ok(table_ids)
    }

    /// Adds a column to an existing BigQuery table.
    ///
    /// Executes an ALTER TABLE ADD COLUMN statement to add a new column with
    /// the specified schema. New columns must be nullable in BigQuery.
    pub async fn add_column(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_schema: &ColumnSchema,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(&column_schema.name, "BigQuery column name")?;
        let column_type = Self::postgres_to_bigquery_type(&column_schema.typ);

        info!("adding column {column_name} ({column_type}) to table {full_table_name} in BigQuery");

        // BigQuery requires new columns to be nullable (no NOT NULL constraint
        // allowed). Defaults must be applied through a separate ALTER COLUMN
        // statement because BigQuery rejects ADD COLUMN with a default value.
        let query = format!("alter table {full_table_name} add column {column_name} {column_type}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Drops a column from an existing BigQuery table.
    ///
    /// Executes an ALTER TABLE DROP COLUMN statement to remove the specified
    /// column.
    pub async fn drop_column(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_name: &str,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(column_name, "BigQuery column name")?;

        info!("dropping column {column_name} from table {full_table_name} in BigQuery");

        let query = format!("alter table {full_table_name} drop column {column_name}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Renames a column in an existing BigQuery table.
    ///
    /// Executes an ALTER TABLE RENAME COLUMN statement to rename the specified
    /// column.
    pub async fn rename_column(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        old_name: &str,
        new_name: &str,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let old_name = quote_identifier(old_name, "BigQuery column name")?;
        let new_name = quote_identifier(new_name, "BigQuery column name")?;

        info!("renaming column {old_name} to {new_name} in table {full_table_name} in BigQuery");

        let query = format!("alter table {full_table_name} rename column {old_name} to {new_name}");

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Sets a supported default expression on a BigQuery column.
    pub async fn set_column_default(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_name: &str,
        typ: &Type,
        default_expression: &str,
    ) -> EtlResult<()> {
        let Some(rendered_default_expression) = Self::default_expression(default_expression, typ)
        else {
            warn!(
                dataset_id = %dataset_id,
                table_id = %table_id,
                column_name = %column_name,
                "skipping unsupported source column default for BigQuery"
            );
            return Ok(());
        };

        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(column_name, "BigQuery column name")?;

        info!("setting default for column {column_name} in table {full_table_name} in BigQuery");

        let query = format!(
            "alter table {full_table_name} alter column {column_name} set default \
             {rendered_default_expression}"
        );

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Returns whether a column default can be represented in BigQuery SQL.
    pub(crate) fn supports_column_default(default_expression: &str, typ: &Type) -> bool {
        Self::default_expression(default_expression, typ).is_some()
    }

    /// Drops a default expression from a BigQuery column.
    pub async fn drop_column_default(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_name: &str,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(column_name, "BigQuery column name")?;

        info!("dropping default for column {column_name} in table {full_table_name} in BigQuery");

        let query =
            format!("alter table {full_table_name} alter column {column_name} drop default");

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
        let table = self.client.table().get(&self.project_id, dataset_id, table_id, None).await;

        let exists =
            !matches!(table, Err(BQError::ResponseError { error }) if error.error.code == 404);

        Ok(exists)
    }

    /// Checks whether a dataset exists and is accessible.
    ///
    /// Returns `true` if the dataset exists and the client has access, `false`
    /// if the dataset does not exist. Returns an error for authentication
    /// or connectivity failures.
    pub async fn dataset_exists(&self, dataset_id: &BigQueryDatasetId) -> EtlResult<bool> {
        let result = self.client.dataset().get(&self.project_id, dataset_id).await;

        match result {
            Ok(_) => Ok(true),
            Err(BQError::ResponseError { error }) if error.error.code == 404 => Ok(false),
            Err(e) => Err(bq_error_to_etl_error(e)),
        }
    }

    /// Appends table batches to BigQuery using the concurrent Storage Write
    /// API.
    ///
    /// Accepts pre-constructed append requests and processes them concurrently.
    ///
    /// Retries for transient request and transport failures are handled inside
    /// the underlying Storage Write API library. This method also retries
    /// the narrow class of storage write metadata lag failures that can happen
    /// after DDL, then converts final failures into ETL errors.
    pub(super) async fn append_table_batches(
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
        let mut retry_delay = STORAGE_WRITE_METADATA_LAG_RETRY_DELAY;

        loop {
            match self.append_table_batches_once(pending_requests).await? {
                AppendProcessingResult::Success { bytes_sent, bytes_received } => {
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

                    let retry_summary = format_retryable_storage_write_metadata_lag_requests(
                        &next_pending_requests,
                    );
                    pending_requests =
                        next_pending_requests.into_iter().map(|request| request.request).collect();

                    let pending_batch_count = pending_requests.len();
                    if pending_batch_count == 0 {
                        return Ok((total_bytes_sent, total_bytes_received));
                    }

                    let elapsed = started_at.elapsed();
                    let remaining_timeout =
                        STORAGE_WRITE_METADATA_LAG_RETRY_TIMEOUT.saturating_sub(elapsed);

                    if remaining_timeout.is_zero() {
                        return Err(storage_write_metadata_lag_timeout_error(&retry_summary));
                    }

                    let sleep_delay = retry_delay.min(remaining_timeout);

                    if sleep_delay.is_zero() {
                        return Err(storage_write_metadata_lag_timeout_error(&retry_summary));
                    }

                    warn!(
                        attempt,
                        pending_batch_count,
                        retry_delay_ms = sleep_delay.as_millis() as u64,
                        error_detail = %retry_summary,
                        "bigquery storage write metadata still lagging, retrying append"
                    );

                    sleep(sleep_delay).await;

                    retry_delay = (retry_delay * 2).min(STORAGE_WRITE_METADATA_LAG_MAX_RETRY_DELAY);
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
            return Ok(AppendProcessingResult::Success { bytes_sent: 0, bytes_received: 0 });
        }

        debug!(batch_count = append_requests.len(), "streaming table batches concurrently");

        let batch_append_results =
            self.client.storage().append_table_batches(append_requests.clone()).await.inspect_err(
                |err| {
                    let error_code = error_code_label(err);

                    counter!(
                        ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
                        "error_code" => error_code
                    )
                    .increment(1);
                },
            );

        let batch_append_results = match batch_append_results {
            Ok(results) => results,
            Err(error) => {
                return Ok(append_processing_result_from_request_error(error, append_requests));
            }
        };

        let mut total_bytes_sent = 0;
        let mut total_bytes_received = 0;
        let mut errors = Vec::new();
        let mut retryable_batch_details = vec![None; append_requests.len()];

        for batch_append_result in batch_append_results {
            let batch_index = batch_append_result.batch_index;

            match process_single_batch_append_result(batch_append_result) {
                BatchProcessResult::Success { bytes_sent, bytes_received } => {
                    debug!(batch_index, bytes_sent, bytes_received, "batch processed successfully");

                    total_bytes_sent += bytes_sent;
                    total_bytes_received += bytes_received;
                }
                BatchProcessResult::RetryableStorageWriteMetadataLag { detail } => {
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
                BatchProcessResult::RequestError { error: request_error } => {
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
    /// Converts TableRow instances to BigQueryTableRow and creates a properly
    /// configured [`BatchAppendRequest`] for efficient append retries.
    pub(super) fn create_batch_append_request(
        &self,
        pipeline_id: PipelineId,
        batch_index: usize,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: TableDescriptor,
        validated_rows: Vec<BigQueryTableRow>,
    ) -> EtlResult<BatchAppendRequest<BigQueryTableRow>> {
        let stream_name =
            StreamName::new_default(self.project_id.clone(), dataset_id.clone(), table_id.clone());

        let table_batch = TableBatch::new(stream_name, table_descriptor, validated_rows);
        let trace_id =
            create_append_trace_id(pipeline_id, table_batch.stream_name().table(), batch_index);

        Ok(BatchAppendRequest::new(table_batch, trace_id))
    }

    /// Executes a BigQuery SQL query and returns the result set.
    pub async fn query(&self, request: QueryRequest) -> EtlResult<ResultSet> {
        let query_response = retry_with_backoff(
            QUERY_RETRY_POLICY,
            is_transient_query_error,
            |delay| delay,
            log_query_retry,
            || {
                let request = request.clone();
                async move { self.client.job().query(&self.project_id, request).await }
            },
        )
        .await
        .map_err(|failure| bq_error_to_etl_error(failure.last_error))?;

        Ok(ResultSet::new_from_query_response(query_response))
    }

    /// Generates SQL column specification for CREATE TABLE statements.
    fn column_spec(column_schema: &ColumnSchema) -> EtlResult<String> {
        let column_name = quote_identifier(&column_schema.name, "BigQuery column name")?;

        let mut column_spec =
            format!("{} {}", column_name, Self::postgres_to_bigquery_type(&column_schema.typ));

        if let Some(rendered_default_expression) =
            column_schema.default_expression.as_deref().and_then(|default_expression| {
                Self::default_expression(default_expression, &column_schema.typ)
            })
        {
            column_spec.push_str(&format!(" default {rendered_default_expression}"));
        } else if column_schema.default_expression.is_some() {
            warn!(
                column_name = %column_schema.name,
                "skipping unsupported source column default for BigQuery table creation"
            );
        }

        if !column_schema.nullable && !is_array_type(&column_schema.typ) {
            column_spec.push_str(" not null");
        };

        Ok(column_spec)
    }

    /// Returns a rendered default expression for BigQuery, if supported.
    fn default_expression(default_expression: &str, typ: &Type) -> Option<String> {
        parse_default_expression(default_expression, typ)
            .and_then(|expression| Self::render_default_expression(&expression, typ))
    }

    /// Renders a parsed default expression as BigQuery SQL.
    fn render_default_expression(expression: &DefaultExpression, typ: &Type) -> Option<String> {
        match expression {
            DefaultExpression::StringLiteral(expression) => {
                Self::is_bigquery_string_default_type(typ).then(|| expression.clone())
            }
            DefaultExpression::NumericLiteral(expression) => {
                if Self::is_bigquery_numeric_default_type(typ) {
                    Some(expression.clone())
                } else if Self::is_bigquery_numeric_string_default_type(typ) {
                    Some(Self::quote_numeric_literal_as_string(expression))
                } else {
                    None
                }
            }
            DefaultExpression::BooleanLiteral(expression) => {
                matches!(typ, &Type::BOOL).then(|| expression.clone())
            }
            DefaultExpression::DateLiteral(expression) => {
                matches!(typ, &Type::DATE).then(|| format!("DATE {expression}"))
            }
            DefaultExpression::TimeLiteral(expression) => {
                matches!(typ, &Type::TIME).then(|| format!("TIME {expression}"))
            }
            DefaultExpression::TimeTzLiteral(expression) => {
                matches!(typ, &Type::TIMETZ).then(|| expression.clone())
            }
            DefaultExpression::TimestampLiteral(expression) => {
                matches!(typ, &Type::TIMESTAMP).then(|| format!("TIMESTAMP {expression}"))
            }
            DefaultExpression::TimestampTzLiteral(expression) => {
                matches!(typ, &Type::TIMESTAMPTZ).then(|| format!("TIMESTAMP {expression}"))
            }
            DefaultExpression::IntervalLiteral(expression) => {
                matches!(typ, &Type::INTERVAL).then(|| expression.clone())
            }
            DefaultExpression::JsonLiteral(expression) => {
                Self::is_json_type(typ).then(|| format!("JSON {expression}"))
            }
        }
    }

    /// Returns whether this Postgres type is created as a BigQuery string
    /// column and can safely receive string-producing defaults.
    fn is_bigquery_string_default_type(typ: &Type) -> bool {
        matches!(
            typ,
            &Type::CHAR
                | &Type::BPCHAR
                | &Type::VARCHAR
                | &Type::NAME
                | &Type::TEXT
                | &Type::MONEY
                | &Type::TIMETZ
                | &Type::INTERVAL
                | &Type::UUID
        )
    }

    /// Returns whether this Postgres type is created as a BigQuery numeric
    /// column and can safely receive numeric defaults.
    fn is_bigquery_numeric_default_type(typ: &Type) -> bool {
        matches!(
            typ,
            &Type::INT2
                | &Type::INT4
                | &Type::INT8
                | &Type::FLOAT4
                | &Type::FLOAT8
                | &Type::NUMERIC
                | &Type::OID
        )
    }

    /// Returns whether this Postgres numeric-like type is created as a
    /// BigQuery string column.
    fn is_bigquery_numeric_string_default_type(typ: &Type) -> bool {
        matches!(typ, &Type::MONEY)
    }

    /// Returns whether this Postgres type is created as a BigQuery JSON
    /// column and can safely receive JSON defaults.
    fn is_json_type(typ: &Type) -> bool {
        matches!(typ, &Type::JSON | &Type::JSONB)
    }

    /// Quotes a parser-validated numeric literal as a SQL string literal.
    fn quote_numeric_literal_as_string(expression: &str) -> String {
        format!("'{expression}'")
    }

    /// Creates a primary key clause for table creation.
    ///
    /// BigQuery tables are keyed by the source primary key, which is distinct
    /// from PostgreSQL replica-identity metadata when `REPLICA IDENTITY FULL`
    /// is in use.
    fn add_primary_key_clause(
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<Option<String>> {
        let mut primary_key_columns: Vec<_> =
            replicated_table_schema.primary_key_column_schemas().collect();

        // If no primary key columns are marked, return early.
        if primary_key_columns.is_empty() {
            return Ok(None);
        }

        // Sort by primary_key_ordinal_position to ensure correct composite key
        // ordering. This is needed since the order of column schema returned above, is
        // ordered by the columns ordering, not the primary key ordering.
        primary_key_columns.sort_by_key(|c| c.primary_key_ordinal_position);

        let primary_key_columns: Vec<String> = primary_key_columns
            .into_iter()
            .map(|c| quote_identifier(&c.name, "BigQuery primary key column"))
            .collect::<EtlResult<Vec<_>>>()?;

        let primary_key_clause =
            format!(", primary key ({}) not enforced", primary_key_columns.join(","));

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
                &Type::MONEY_ARRAY => "string",
                &Type::DATE_ARRAY => "date",
                &Type::TIME_ARRAY => "time",
                &Type::TIMETZ_ARRAY => "string",
                &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "timestamp",
                &Type::INTERVAL_ARRAY => "string",
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
            &Type::MONEY => "string",
            &Type::DATE => "date",
            &Type::TIME => "time",
            &Type::TIMETZ => "string",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "timestamp",
            &Type::INTERVAL => "string",
            &Type::UUID => "string",
            &Type::JSON | &Type::JSONB => "json",
            &Type::OID => "int64",
            &Type::BYTEA => "bytes",
            _ => "string",
        }
        .to_owned()
    }

    /// Converts Postgres column schemas to a BigQuery [`TableDescriptor`].
    ///
    /// Maps data types and nullability to BigQuery column specifications,
    /// setting appropriate column modes and automatically adding CDC
    /// special columns.
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
                Type::MONEY => ColumnType::String,
                Type::DATE => ColumnType::String,
                Type::TIME => ColumnType::String,
                Type::TIMETZ => ColumnType::String,
                Type::TIMESTAMP => ColumnType::String,
                Type::TIMESTAMPTZ => ColumnType::String,
                Type::INTERVAL => ColumnType::String,
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
                Type::MONEY_ARRAY => ColumnType::String,
                Type::DATE_ARRAY => ColumnType::String,
                Type::TIME_ARRAY => ColumnType::String,
                Type::TIMETZ_ARRAY => ColumnType::String,
                Type::TIMESTAMP_ARRAY => ColumnType::String,
                Type::TIMESTAMPTZ_ARRAY => ColumnType::String,
                Type::INTERVAL_ARRAY => ColumnType::String,
                Type::UUID_ARRAY => ColumnType::String,
                Type::JSON_ARRAY => ColumnType::String,
                Type::JSONB_ARRAY => ColumnType::String,
                Type::OID_ARRAY => ColumnType::Int32,
                Type::BYTEA_ARRAY => ColumnType::Bytes,
                _ => ColumnType::String,
            };

            let mode = if is_array_type(&column_schema.typ) {
                ColumnMode::Repeated
            } else if use_cdc_sequence_column {
                // CDC delete rows can omit non-key columns, so the writer
                // schema must accept missing scalar fields even when the
                // destination table column is defined as NOT NULL.
                ColumnMode::Nullable
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
            name: BIGQUERY_CDC_SPECIAL_COLUMN.to_owned(),
            typ: ColumnType::String,
            mode: ColumnMode::Required,
        });
        number += 1;

        if use_cdc_sequence_column {
            field_descriptors.push(FieldDescriptor {
                number,
                name: BIGQUERY_CDC_SEQUENCE_COLUMN.to_owned(),
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
        f.debug_struct("BigQueryClient").field("project_id", &self.project_id).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use etl::types::{IdentityMask, ReplicationMask, TableId, TableName, TableSchema};
    use gcp_bigquery_client::{
        error::{NestedResponseError, ResponseError},
        google::cloud::bigquery::storage::v1::{AppendRowsResponse, append_rows_response},
    };

    use super::*;

    fn successful_append_response() -> AppendRowsResponse {
        AppendRowsResponse {
            updated_schema: None,
            row_errors: Vec::new(),
            write_stream: "projects/test/datasets/test/tables/test/streams/_default".to_owned(),
            response: Some(append_rows_response::Response::AppendResult(
                append_rows_response::AppendResult { offset: None },
            )),
        }
    }

    #[test]
    fn row_error_omits_provider_message() {
        let error = row_error_to_etl_error(RowError {
            index: 7,
            code: RowErrorCode::FieldsError as i32,
            message: "invalid value: customer@example.com".to_owned(),
        });

        assert_eq!(error.description(), Some("BigQuery rejected a row in the append request"));
        assert_eq!(
            error.detail(),
            Some(
                "BigQuery rejected row 7 with code FIELDS_ERROR. The detailed BigQuery message \
                 was omitted to avoid exposing source row values."
            )
        );
        assert!(!error.to_string().contains("customer@example.com"));
    }

    /// Creates a test column schema with common defaults.
    ///
    /// This helper simplifies column schema creation in tests by providing
    /// sensible defaults for fields that are typically not relevant to the
    /// test logic.
    fn test_column(
        name: &str,
        typ: Type,
        ordinal_position: i32,
        nullable: bool,
        primary_key_ordinal: Option<i32>,
    ) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), typ, -1, ordinal_position, nullable)
            .with_primary_key_ordinal_position(primary_key_ordinal)
    }

    /// Creates a [`ReplicatedTableSchema`] from test columns with all columns
    /// replicated.
    fn test_replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let column_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId(1), // Dummy table ID
            TableName::new("public".to_owned(), "test_table".to_owned()),
            columns,
        ));
        let replication_mask = ReplicationMask::build_or_all(&table_schema, &column_names);

        ReplicatedTableSchema::from_mask(table_schema, replication_mask)
    }

    /// Creates a [`ReplicatedTableSchema`] from test columns with all columns
    /// replicated and an explicit runtime identity.
    fn test_replicated_schema_with_identity(
        columns: Vec<ColumnSchema>,
        identity_mask: IdentityMask,
    ) -> ReplicatedTableSchema {
        let column_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId(1),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            columns,
        ));
        let replication_mask = ReplicationMask::build_or_all(&table_schema, &column_names);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn postgres_to_bigquery_type_basic_types() {
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::BOOL), "bool");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TEXT), "string");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INT2), "int64");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INT4), "int64");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INT8), "int64");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::FLOAT4), "float64");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::FLOAT8), "float64");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::NUMERIC), "bignumeric");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::MONEY), "string");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::OID), "int64");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::DATE), "date");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TIME), "time");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TIMETZ), "string");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TIMESTAMP), "timestamp");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TIMESTAMPTZ), "timestamp");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INTERVAL), "string");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::JSON), "json");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::BYTEA), "bytes");
    }

    #[test]
    fn postgres_to_bigquery_type_array_types() {
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::BOOL_ARRAY), "array<bool>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TEXT_ARRAY), "array<string>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INT2_ARRAY), "array<int64>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INT4_ARRAY), "array<int64>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INT8_ARRAY), "array<int64>");
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::FLOAT4_ARRAY),
            "array<float64>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::FLOAT8_ARRAY),
            "array<float64>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::NUMERIC_ARRAY),
            "array<bignumeric>"
        );
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::MONEY_ARRAY), "array<string>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::OID_ARRAY), "array<int64>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::DATE_ARRAY), "array<date>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TIME_ARRAY), "array<time>");
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::TIMETZ_ARRAY), "array<string>");
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::TIMESTAMP_ARRAY),
            "array<timestamp>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::TIMESTAMPTZ_ARRAY),
            "array<timestamp>"
        );
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::INTERVAL_ARRAY),
            "array<string>"
        );
        assert_eq!(BigQueryClient::postgres_to_bigquery_type(&Type::INET_ARRAY), "array<string>");
        assert_eq!(
            BigQueryClient::postgres_to_bigquery_type(&Type::INT4_RANGE_ARRAY),
            "array<string>"
        );
    }

    #[test]
    fn column_spec() {
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
    fn column_spec_includes_supported_default() {
        let column_schema = ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 1, true)
            .with_default_expression("'pending'::text".to_owned());

        let spec = BigQueryClient::column_spec(&column_schema).expect("column spec generation");

        assert_eq!(spec, "`status` string default 'pending'");
    }

    #[test]
    fn query_retry_classifies_bigquery_table_update_rate_limit_as_transient() {
        let error = BQError::ResponseError {
            error: ResponseError {
                error: NestedResponseError {
                    code: 400,
                    errors: vec![
                        [("reason".to_owned(), "jobRateLimitExceeded".to_owned())]
                            .into_iter()
                            .collect(),
                    ],
                    message: "Job exceeded rate limits".to_owned(),
                    status: "INVALID_ARGUMENT".to_owned(),
                },
            },
        };

        assert_eq!(is_transient_query_error(&error), RetryDecision::Retry);
    }

    #[test]
    fn default_expression_renders_portable_expressions() {
        let cases = [
            (Type::TEXT, "true", "'true'"),
            (Type::BOOL, "'true'::text", "true"),
            (Type::MONEY, "42", "'42'"),
            (Type::DATE, "'2026-01-01'::date", "DATE '2026-01-01'"),
            (Type::TIME, "'12:30:00'::time", "TIME '12:30:00'"),
            (Type::TIMETZ, "'12:30:00+02'::timetz", "'12:30:00+02'"),
            (Type::INTERVAL, "'30 days'::interval", "'30 days'"),
            (
                Type::TIMESTAMP,
                "'2026-01-01 12:30:00'::timestamp",
                "TIMESTAMP '2026-01-01 12:30:00'",
            ),
            (Type::JSONB, "'{}'::jsonb", "JSON '{}'"),
            (
                Type::UUID,
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            ),
        ];

        for (typ, expression, expected) in cases {
            assert_eq!(
                BigQueryClient::default_expression(expression, &typ).as_deref(),
                Some(expected)
            );
        }
    }

    #[test]
    fn default_expression_rejects_bigquery_unsupported_expressions() {
        let cases = [
            (Type::INT4, "10 + 5"),
            (Type::INT4, "'abc'::text"),
            (Type::TIMESTAMPTZ, "now() + interval '30 days'"),
            (Type::UUID, "gen_random_uuid()"),
            (Type::TIMESTAMPTZ, "now()"),
            (Type::TIMESTAMP, "localtimestamp"),
            (Type::TEXT, "current_date"),
            (Type::DATE, "current_time"),
            (Type::TEXT, "lower('USER'::text)"),
        ];

        for (typ, expression) in cases {
            assert_eq!(BigQueryClient::default_expression(expression, &typ), None);
            assert!(!BigQueryClient::supports_column_default(expression, &typ));
        }
    }

    #[test]
    fn column_spec_escapes_backticks() {
        let column_schema = test_column("pwn`name", Type::TEXT, 1, true, None);

        let spec = BigQueryClient::column_spec(&column_schema).expect("escaped column spec");

        assert_eq!(spec, "`pwn\\`name` string");
    }

    #[test]
    fn add_primary_key_clause() {
        let columns_with_pk = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema_with_pk = test_replicated_schema(columns_with_pk);
        let pk_clause =
            BigQueryClient::add_primary_key_clause(&schema_with_pk).expect("pk clause").unwrap();
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
        assert_eq!(composite_pk_clause, ", primary key (`tenant_id`,`id`) not enforced");

        // BigQuery declares composite primary keys in primary-key ordinal
        // order, even when the table's physical column order differs.
        let columns_with_reversed_pk = vec![
            test_column("id", Type::INT4, 1, false, Some(2)),
            test_column("tenant_id", Type::INT4, 2, false, Some(1)),
            test_column("name", Type::TEXT, 3, true, None),
        ];
        let schema_with_reversed_pk = test_replicated_schema(columns_with_reversed_pk);
        let reversed_pk_clause = BigQueryClient::add_primary_key_clause(&schema_with_reversed_pk)
            .unwrap()
            .expect("reversed pk clause");
        assert_eq!(reversed_pk_clause, ", primary key (`tenant_id`,`id`) not enforced");

        let columns_with_full_identity = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema_with_full_identity = test_replicated_schema_with_identity(
            columns_with_full_identity,
            IdentityMask::from_bytes(vec![1, 1]),
        );
        let full_identity_pk_clause =
            BigQueryClient::add_primary_key_clause(&schema_with_full_identity)
                .unwrap()
                .expect("full identity pk clause");
        assert_eq!(full_identity_pk_clause, ", primary key (`id`) not enforced");

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
    fn column_schemas_to_table_descriptor() {
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
        assert!(matches!(descriptor.field_descriptors[0].typ, ColumnType::Int32));
        assert!(matches!(descriptor.field_descriptors[0].mode, ColumnMode::Nullable));

        assert_eq!(descriptor.field_descriptors[1].name, "name");
        assert!(matches!(descriptor.field_descriptors[1].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[1].mode, ColumnMode::Nullable));

        assert_eq!(descriptor.field_descriptors[2].name, "active");
        assert!(matches!(descriptor.field_descriptors[2].typ, ColumnType::Bool));
        assert!(matches!(descriptor.field_descriptors[2].mode, ColumnMode::Nullable));

        // Check array column
        assert_eq!(descriptor.field_descriptors[3].name, "tags");
        assert!(matches!(descriptor.field_descriptors[3].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[3].mode, ColumnMode::Repeated));

        // Check CDC columns
        assert_eq!(descriptor.field_descriptors[4].name, BIGQUERY_CDC_SPECIAL_COLUMN);
        assert!(matches!(descriptor.field_descriptors[4].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[4].mode, ColumnMode::Required));

        assert_eq!(descriptor.field_descriptors[5].name, BIGQUERY_CDC_SEQUENCE_COLUMN);
        assert!(matches!(descriptor.field_descriptors[5].typ, ColumnType::String));
        assert!(matches!(descriptor.field_descriptors[5].mode, ColumnMode::Required));
        let field_numbers: Vec<u32> =
            descriptor.field_descriptors.iter().map(|field| field.number).collect();
        assert_eq!(field_numbers, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn column_schemas_to_table_descriptor_preserves_required_mode_for_table_copy() {
        let columns = vec![
            test_column("id", Type::INT4, 1, false, Some(1)),
            test_column("name", Type::TEXT, 2, true, None),
        ];
        let schema = test_replicated_schema(columns);

        let descriptor = BigQueryClient::column_schemas_to_table_descriptor(&schema, false);

        assert!(matches!(descriptor.field_descriptors[0].mode, ColumnMode::Required));
        assert!(matches!(descriptor.field_descriptors[1].mode, ColumnMode::Nullable));
        assert_eq!(descriptor.field_descriptors.len(), 3);
    }

    #[test]
    fn column_schemas_to_table_descriptor_complex_types() {
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
        assert!(matches!(descriptor.field_descriptors[0].typ, ColumnType::String)); // UUID
        assert!(matches!(descriptor.field_descriptors[1].typ, ColumnType::String)); // JSON
        assert!(matches!(descriptor.field_descriptors[2].typ, ColumnType::Bytes)); // BYTEA
        assert!(matches!(descriptor.field_descriptors[3].typ, ColumnType::String)); // NUMERIC
        assert!(matches!(descriptor.field_descriptors[4].typ, ColumnType::String)); // DATE
        assert!(matches!(descriptor.field_descriptors[5].typ, ColumnType::String)); // TIME
    }

    #[test]
    fn process_single_batch_append_result_retries_pure_schema_propagation_errors() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![Err(tonic::Status::invalid_argument("schema_mismatch_extra_fields"))],
            bytes_sent: 128,
        });

        assert!(matches!(result, BatchProcessResult::RetryableStorageWriteMetadataLag { .. }));
    }

    #[test]
    fn process_single_batch_append_result_retries_extra_proto_fields_schema_lag() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![Err(tonic::Status::invalid_argument(
                "Found incompatible fields: 'id' and/or mismatch fields, extra proto fields: \
                 'ddl_col_1_0' extra bq fields: ''",
            ))],
            bytes_sent: 128,
        });

        assert!(matches!(result, BatchProcessResult::RetryableStorageWriteMetadataLag { .. }));
    }

    #[test]
    fn process_single_batch_append_result_retries_partial_success_schema_propagation() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![
                Ok(successful_append_response()),
                Err(tonic::Status::invalid_argument("schema_mismatch_extra_fields")),
            ],
            bytes_sent: 128,
        });

        assert!(matches!(result, BatchProcessResult::RetryableStorageWriteMetadataLag { .. }));
    }

    #[test]
    fn process_single_batch_append_result_retries_table_recreation_propagation() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![Err(tonic::Status::not_found(
                "Table 123:dataset.test_users_0 is re-created. Entity: \
                 projects/project/datasets/dataset/tables/test_users_0/streams/_default",
            ))],
            bytes_sent: 128,
        });

        assert!(matches!(result, BatchProcessResult::RetryableStorageWriteMetadataLag { .. }));
    }

    #[test]
    fn process_single_batch_append_result_does_not_retry_generic_not_found() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![Err(tonic::Status::not_found(
                "Table 123:dataset.test_users_0 was not found.",
            ))],
            bytes_sent: 128,
        });

        assert!(matches!(result, BatchProcessResult::RequestError { .. }));
    }

    #[test]
    fn storage_write_metadata_lag_timeout_error_is_worker_retryable() {
        let error = storage_write_metadata_lag_timeout_error("storage write metadata lag");

        assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);
        assert_eq!(error.description(), Some("BigQuery storage write metadata lag timed out"));
    }
}
