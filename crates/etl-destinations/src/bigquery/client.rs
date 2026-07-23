use std::fmt;

use etl::{
    data::Cell,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    pipeline::PipelineId,
    schema::{ColumnSchema, ReplicatedTableSchema, Type, is_array_type},
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
    storage::{AppendRequest, AppendResult, StorageApiConfig, StreamName, TableDescriptor},
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
        schema::{create_columns_spec, default_expression_sql, postgres_to_bigquery_type},
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
/// Maximum time to retry locally retryable BigQuery Storage Write append
/// errors.
///
/// The current retryable cases are Storage Write schema propagation and
/// `NOT_FOUND` responses when the BigQuery table API confirms that the target
/// table exists. Google documents schema update detection as happening on the
/// order of minutes.
const STORAGE_WRITE_RETRY_TIMEOUT: Duration = Duration::from_secs(600);
/// Initial backoff when retrying locally retryable Storage Write errors.
const STORAGE_WRITE_RETRY_DELAY: Duration = Duration::from_secs(1);
/// Maximum backoff when retrying locally retryable Storage Write errors.
const STORAGE_WRITE_MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
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

/// Result of processing one append request, used to determine retry strategy.
#[derive(Debug)]
enum AppendRequestProcessResult {
    /// Request succeeded.
    Success,
    /// Request had row-level errors.
    RowErrors { errors: Vec<RowError> },
    /// Request had a request-level error.
    RequestError { error: BQError },
}

/// Aggregated result of processing a set of append requests.
#[derive(Debug)]
enum AppendProcessingResult {
    Success,
    Retry { pending_requests: Vec<RetryableAppendRequest> },
    Error(EtlError),
}

/// An append request that should be retried after a local Storage Write delay.
#[derive(Debug)]
struct RetryableAppendRequest {
    request: BigQueryAppendRequest,
    detail: String,
}

/// A BigQuery append request with local retry context.
#[derive(Debug, Clone)]
pub(super) struct BigQueryAppendRequest {
    /// Request sent to the Storage Write API client.
    request: AppendRequest<BigQueryTableRow>,
    /// BigQuery dataset id targeted by the request.
    dataset_id: BigQueryDatasetId,
    /// BigQuery table id targeted by the request.
    table_id: BigQueryTableId,
}

/// Builds a concise description for a set of retryable Storage Write requests.
fn format_retryable_storage_write_requests(requests: &[RetryableAppendRequest]) -> String {
    match requests.split_first() {
        None => "retryable storage write error".to_owned(),
        Some((first, [])) => first.detail.clone(),
        Some((first, rest)) => {
            let distinct_other_details =
                rest.iter().filter(|request| request.detail != first.detail).count();

            if distinct_other_details == 0 {
                format!("{} ({} requests)", first.detail, requests.len())
            } else {
                format!(
                    "{} ({} requests, {} additional retry details)",
                    first.detail,
                    requests.len(),
                    distinct_other_details
                )
            }
        }
    }
}

/// Creates a per-request BigQuery trace identifier for Storage Write requests.
fn create_append_trace_id(pipeline_id: PipelineId, table_id: &str, request_index: usize) -> String {
    format!("supabase_etl_{pipeline_id}_{table_id}_{request_index}_{}", random::<u32>())
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

/// Adds equal jitter to a retry delay.
fn storage_write_retry_delay_with_jitter(delay: Duration) -> Duration {
    if delay.is_zero() {
        return delay;
    }

    let half_delay = delay / 2;
    let jitter_range = delay.saturating_sub(half_delay);
    let jitter_range_nanos = jitter_range.as_nanos();

    if jitter_range_nanos == 0 {
        return delay;
    }

    let jitter_nanos = u128::from(random::<u64>()) % (jitter_range_nanos + 1);
    let jitter = Duration::from_nanos(jitter_nanos as u64);

    half_delay + jitter
}

/// Processes one append result and determines its success or failure mode.
///
/// Row errors are permanent failures (bad data, schema mismatch) and fail
/// immediately. Request errors are surfaced for retry decision by the caller.
fn process_append_result(append_result: AppendResult) -> AppendRequestProcessResult {
    let mut row_errors = Vec::new();

    for response in append_result.responses {
        match response {
            Ok(response) => {
                // Row-level errors are permanent failures (bad data, schema mismatch, etc).
                if !response.row_errors.is_empty() {
                    row_errors.extend(response.row_errors);
                }
            }
            Err(status) => {
                return AppendRequestProcessResult::RequestError { error: BQError::from(status) };
            }
        }
    }

    if !row_errors.is_empty() {
        AppendRequestProcessResult::RowErrors { errors: row_errors }
    } else {
        AppendRequestProcessResult::Success
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

/// Returns whether BigQuery rejected `DROP DEFAULT` because no default exists.
fn is_missing_column_default_error(error: &BQError) -> bool {
    let BQError::ResponseError { error } = error else {
        return false;
    };

    error.error.code == 400
        && error.error.errors.iter().any(|nested_error| {
            nested_error.get("reason").is_some_and(|reason| reason == "invalid")
                && nested_error.get("message").is_some_and(|message| {
                    message.starts_with("Cannot DROP DEFAULT value from column ")
                        && message.ends_with(" which does not have a DEFAULT value.")
                })
        })
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

/// Builds the error returned when local Storage Write retries are exhausted.
///
/// The destination absorbs common short Storage Write retry windows locally.
/// If BigQuery still has not accepted the append once that bounded window
/// expires, the worker-level timed retry policy should take over.
fn storage_write_retry_timeout_error(detail: &str) -> EtlError {
    etl_error!(
        ErrorKind::DestinationAtomicBatchRetryable,
        "BigQuery storage write retry timed out",
        format!(
            "BigQuery did not accept the storage write request within {} seconds after the \
             destination table changed: {}",
            STORAGE_WRITE_RETRY_TIMEOUT.as_secs(),
            detail
        )
    )
}

/// Converts BigQuery row errors to ETL destination errors.
fn row_error_to_etl_error(err: RowError) -> EtlError {
    let code = RowErrorCode::try_from(err.code)
        .map_or("UNKNOWN_ROW_ERROR_CODE", |code| code.as_str_name());

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
/// Storage Write schema-propagation cases.
///
/// BigQuery documents `StorageErrorCode::SCHEMA_MISMATCH_EXTRA_FIELDS` as the
/// structured signal for schema mismatch during appends. We fall back to the
/// observed schema-mismatch message forms only when BigQuery does not provide a
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

/// Returns true for Storage Write `NOT_FOUND` responses that need table
/// existence confirmation before retrying.
fn is_storage_write_not_found(error: &BQError) -> bool {
    let BQError::TonicStatusError(status) = error else {
        return false;
    };

    status.code() == Code::NotFound
}

/// Returns retry detail for Storage Write schema update propagation errors.
fn retryable_storage_write_schema_update_detail(error: &BQError) -> Option<String> {
    if is_retryable_schema_propagation_error(error) {
        return Some(error.to_string());
    }

    None
}

/// Returns retry detail for Storage Write `NOT_FOUND` errors when the table
/// exists according to the BigQuery table API.
async fn retryable_storage_write_not_found_detail(
    client: &BigQueryClient,
    request: &BigQueryAppendRequest,
    error: &BQError,
) -> EtlResult<Option<String>> {
    if !is_storage_write_not_found(error) {
        return Ok(None);
    }

    if client.table_exists(&request.dataset_id, &request.table_id).await? {
        return Ok(Some(error.to_string()));
    }

    Ok(None)
}

/// Returns retry detail when a Storage Write append failed with a locally
/// retryable error.
async fn retryable_storage_write_error_detail(
    client: &BigQueryClient,
    request: &BigQueryAppendRequest,
    error: &BQError,
) -> EtlResult<Option<String>> {
    // BigQuery table metadata can reflect a schema update before Storage Write
    // append streams accept rows encoded with that updated schema.
    if let Some(detail) = retryable_storage_write_schema_update_detail(error) {
        return Ok(Some(detail));
    }

    // Storage Write `NOT_FOUND` can be stale default-stream routing when the
    // table still exists after a delete/recreate. This intentionally covers
    // both BigQuery's explicit "is re-created" message and generic
    // "Requested entity was not found" responses.
    if let Some(detail) = retryable_storage_write_not_found_detail(client, request, error).await? {
        return Ok(Some(detail));
    }

    Ok(None)
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
        // Gzip is CPU-bound on every append with no compression-level control, and rows
        // can carry large text/JSON payloads.
        let storage_config =
            StorageApiConfig { connection_pool_size, max_inflight_requests, compression: false };

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
        // Gzip is CPU-bound on every append with no compression-level control, and rows
        // can carry large text/JSON payloads.
        let storage_config =
            StorageApiConfig { connection_pool_size, max_inflight_requests, compression: false };

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
        // Gzip is CPU-bound on every append with no compression-level control, and rows
        // can carry large text/JSON payloads.
        let storage_config =
            StorageApiConfig { connection_pool_size, max_inflight_requests, compression: false };

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
        // Gzip is CPU-bound on every append with no compression-level control, and rows
        // can carry large text/JSON payloads.
        let storage_config =
            StorageApiConfig { connection_pool_size, max_inflight_requests, compression: false };

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

        let columns_spec = create_columns_spec(replicated_table_schema)?;
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

        let columns_spec = create_columns_spec(replicated_table_schema)?;
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
        let column_type = postgres_to_bigquery_type(&column_schema.typ);

        info!("adding column {column_name} ({column_type}) to table {full_table_name} in bigquery");

        if !column_schema.nullable && !is_array_type(&column_schema.typ) {
            warn!(
                dataset_id = %dataset_id,
                table_id = %table_id,
                column_name = %column_schema.name,
                "bigquery does not support adding a top-level not null column; adding it as \
                 nullable"
            );
        }

        // BigQuery cannot add a top-level REQUIRED column or a column with a
        // default to an existing table. Add it as nullable here and apply any
        // supported default through a separate ALTER COLUMN statement.
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

        info!("dropping column {column_name} from table {full_table_name} in bigquery");

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

        info!("renaming column {old_name} to {new_name} in table {full_table_name} in bigquery");

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
        let Some(rendered_default_expression) = default_expression_sql(default_expression, typ)
        else {
            warn!(
                dataset_id = %dataset_id,
                table_id = %table_id,
                column_name = %column_name,
                "skipping unsupported source column default for bigquery"
            );
            return Ok(());
        };

        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(column_name, "BigQuery column name")?;

        info!("setting default for column {column_name} in table {full_table_name} in bigquery");

        let query = format!(
            "alter table {full_table_name} alter column {column_name} set default \
             {rendered_default_expression}"
        );

        let _ = self.query(QueryRequest::new(query)).await?;

        Ok(())
    }

    /// Clears a default expression from a BigQuery column when present.
    pub async fn clear_column_default(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_name: &str,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(column_name, "BigQuery column name")?;

        info!("clearing default for column {column_name} in table {full_table_name} in bigquery");

        let query =
            format!("alter table {full_table_name} alter column {column_name} drop default");

        match self.query_with_bigquery_error(QueryRequest::new(query)).await {
            Ok(_) => {}
            Err(error) if is_missing_column_default_error(&error) => {
                debug!(
                    table_id = %table_id,
                    column_name = %column_name,
                    "column has no default in bigquery; skipping drop default"
                );
            }
            Err(error) => return Err(bq_error_to_etl_error(error)),
        }

        Ok(())
    }

    /// Relaxes a BigQuery column from `REQUIRED` to `NULLABLE` when needed.
    pub(super) async fn drop_column_not_null(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        column_name: &str,
    ) -> EtlResult<()> {
        let table = self
            .client
            .table()
            .get(&self.project_id, dataset_id, table_id, None)
            .await
            .map_err(bq_error_to_etl_error)?;
        let column_mode = table
            .schema
            .fields
            .unwrap_or_default()
            .into_iter()
            .find(|field| field.name == column_name)
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "BigQuery column not found",
                    format!("Column {column_name} was not found in table {table_id}")
                )
            })?
            .mode;

        if column_mode.as_deref() != Some("REQUIRED") {
            debug!(
                table_id = %table_id,
                column_name,
                column_mode = column_mode.as_deref().unwrap_or("NULLABLE"),
                "column is already nullable in bigquery; skipping drop not null"
            );
            return Ok(());
        }

        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let column_name = quote_identifier(column_name, "BigQuery column name")?;

        info!("dropping not null for column {column_name} in table {full_table_name} in bigquery");

        let query =
            format!("alter table {full_table_name} alter column {column_name} drop not null");

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
        match self.client.table().get(&self.project_id, dataset_id, table_id, None).await {
            Ok(_) => Ok(true),
            Err(BQError::ResponseError { error }) if error.error.code == 404 => Ok(false),
            Err(error) => Err(bq_error_to_etl_error(error)),
        }
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

    /// Appends requests to BigQuery using the concurrent Storage Write API.
    ///
    /// Accepts pre-constructed append requests and processes them concurrently.
    ///
    /// Retries for transient request and transport failures are handled inside
    /// the underlying Storage Write API library. This method also retries
    /// locally retryable Storage Write failures that can happen after BigQuery
    /// table metadata changes, then converts final failures into ETL errors.
    pub(super) async fn append(
        &self,
        append_requests: Vec<BigQueryAppendRequest>,
    ) -> EtlResult<()> {
        if append_requests.is_empty() {
            return Ok(());
        }

        let mut pending_requests = append_requests;

        let started_at = Instant::now();
        let mut attempt = 1;
        let mut retry_delay = STORAGE_WRITE_RETRY_DELAY;

        loop {
            match self.append_once(pending_requests).await? {
                AppendProcessingResult::Success => return Ok(()),
                AppendProcessingResult::Retry { pending_requests: next_pending_requests } => {
                    let retry_summary =
                        format_retryable_storage_write_requests(&next_pending_requests);
                    pending_requests =
                        next_pending_requests.into_iter().map(|request| request.request).collect();

                    let pending_request_count = pending_requests.len();
                    if pending_request_count == 0 {
                        return Ok(());
                    }

                    let elapsed = started_at.elapsed();
                    let remaining_timeout = STORAGE_WRITE_RETRY_TIMEOUT.saturating_sub(elapsed);

                    if remaining_timeout.is_zero() {
                        return Err(storage_write_retry_timeout_error(&retry_summary));
                    }

                    let sleep_delay =
                        storage_write_retry_delay_with_jitter(retry_delay.min(remaining_timeout));

                    if sleep_delay.is_zero() {
                        return Err(storage_write_retry_timeout_error(&retry_summary));
                    }

                    warn!(
                        attempt,
                        pending_request_count,
                        retry_delay_ms = sleep_delay.as_millis() as u64,
                        error_detail = %retry_summary,
                        "retrying retryable bigquery storage write append error"
                    );

                    sleep(sleep_delay).await;

                    retry_delay = (retry_delay * 2).min(STORAGE_WRITE_MAX_RETRY_DELAY);
                    attempt += 1;
                }
                AppendProcessingResult::Error(error) => return Err(error),
            }
        }
    }

    /// Executes a single append attempt and classifies the result.
    async fn append_once(
        &self,
        append_requests: Vec<BigQueryAppendRequest>,
    ) -> EtlResult<AppendProcessingResult> {
        if append_requests.is_empty() {
            return Ok(AppendProcessingResult::Success);
        }

        debug!(request_count = append_requests.len(), "streaming append requests concurrently");

        let raw_append_requests =
            append_requests.iter().map(|request| request.request.clone()).collect::<Vec<_>>();
        let append_results =
            self.client.storage().append(raw_append_requests).await.inspect_err(|err| {
                let error_code = error_code_label(err);

                counter!(
                    ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL,
                    "error_code" => error_code
                )
                .increment(1);
            });

        let append_results = match append_results {
            Ok(results) => results,
            Err(error) => {
                let mut retryable_requests = Vec::new();
                let mut has_non_retryable_request = false;

                // A call-level Storage Write error is not tied to a single request index. Retry
                // only if the error is locally retryable for every request in the call.
                for request in append_requests {
                    if let Some(detail) =
                        retryable_storage_write_error_detail(self, &request, &error).await?
                    {
                        retryable_requests.push(RetryableAppendRequest { request, detail });
                    } else {
                        has_non_retryable_request = true;
                        break;
                    }
                }

                if !has_non_retryable_request && !retryable_requests.is_empty() {
                    return Ok(AppendProcessingResult::Retry {
                        pending_requests: retryable_requests,
                    });
                }

                return Ok(AppendProcessingResult::Error(bq_error_to_etl_error(error)));
            }
        };

        let mut errors = Vec::new();
        let mut retryable_request_details = vec![None; append_requests.len()];

        for append_result in append_results {
            let request_index = append_result.request_index;

            match process_append_result(append_result) {
                AppendRequestProcessResult::Success => {
                    debug!(request_index, "append request processed successfully");
                }
                AppendRequestProcessResult::RowErrors { errors: row_errors } => {
                    let error_count = row_errors.len();
                    if error_count > 0 {
                        error!(
                            request_index,
                            error_count, "append request has row errors, failing append operation"
                        );

                        counter!(ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL)
                            .increment(error_count as u64);
                    }

                    for row_error in row_errors {
                        errors.push(row_error_to_etl_error(row_error));
                    }
                }
                AppendRequestProcessResult::RequestError { error: request_error } => {
                    // Request-level errors retain their request index, so only the affected
                    // request needs to be classified and retried.
                    if let Some(detail) = retryable_storage_write_error_detail(
                        self,
                        &append_requests[request_index],
                        &request_error,
                    )
                    .await?
                    {
                        retryable_request_details[request_index] = Some(detail);
                        continue;
                    }

                    let error_code = error_code_label(&request_error);
                    warn!(
                        request_index,
                        error = %request_error,
                        "append request failed after library retries"
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

        if retryable_request_details.iter().any(Option::is_some) {
            let pending_requests = append_requests
                .into_iter()
                .zip(retryable_request_details)
                .filter_map(|(request, detail)| {
                    detail.map(|detail| RetryableAppendRequest { request, detail })
                })
                .collect();

            return Ok(AppendProcessingResult::Retry { pending_requests });
        }

        Ok(AppendProcessingResult::Success)
    }

    /// Invalidates all connections used by the storage write api.
    pub async fn invalidate_all_connections(&self) {
        self.client.storage().invalidate_all_connections().await;
    }

    /// Creates an append request for a specific table with validated rows.
    ///
    /// Converts TableRow instances to BigQueryTableRow and creates a properly
    /// configured [`BigQueryAppendRequest`] for efficient append retries.
    pub(super) fn create_append_request(
        &self,
        pipeline_id: PipelineId,
        request_index: usize,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: TableDescriptor,
        validated_rows: Vec<BigQueryTableRow>,
    ) -> EtlResult<BigQueryAppendRequest> {
        let stream_name =
            StreamName::new_default(self.project_id.clone(), dataset_id.clone(), table_id.clone());

        let trace_id = create_append_trace_id(pipeline_id, stream_name.table(), request_index);

        Ok(BigQueryAppendRequest {
            request: AppendRequest::new(stream_name, table_descriptor, validated_rows, trace_id),
            dataset_id: dataset_id.clone(),
            table_id: table_id.clone(),
        })
    }

    /// Executes a BigQuery SQL query and returns the result set.
    pub async fn query(&self, request: QueryRequest) -> EtlResult<ResultSet> {
        self.query_with_bigquery_error(request).await.map_err(bq_error_to_etl_error)
    }

    /// Executes a BigQuery SQL query while preserving the provider error.
    async fn query_with_bigquery_error(&self, request: QueryRequest) -> Result<ResultSet, BQError> {
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
        .map_err(|failure| failure.last_error)?;

        Ok(ResultSet::new_from_query_response(query_response))
    }

    /// Creates max staleness option clause for CDC table creation.
    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!("options (max_staleness = interval {max_staleness_mins} minute)")
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
    fn missing_column_default_error_matches_only_absent_default_response() {
        let error = BQError::ResponseError {
            error: ResponseError {
                error: NestedResponseError {
                    code: 400,
                    errors: vec![
                        [
                            ("reason".to_owned(), "invalid".to_owned()),
                            (
                                "message".to_owned(),
                                "Cannot DROP DEFAULT value from column status which does not have \
                                 a DEFAULT value."
                                    .to_owned(),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ],
                    message: "Invalid schema change".to_owned(),
                    status: "INVALID_ARGUMENT".to_owned(),
                },
            },
        };

        assert!(is_missing_column_default_error(&error));
        assert!(!is_missing_column_default_error(&BQError::NoDataAvailable));
    }

    #[test]
    fn schema_update_detail_classifies_pure_schema_propagation_errors() {
        let error = BQError::from(tonic::Status::invalid_argument("schema_mismatch_extra_fields"));

        assert!(retryable_storage_write_schema_update_detail(&error).is_some());
    }

    #[test]
    fn schema_update_detail_classifies_extra_proto_fields_schema_lag() {
        let error = BQError::from(tonic::Status::invalid_argument(
            "Found incompatible fields: 'id' and/or mismatch fields, extra proto fields: \
             'ddl_col_1_0' extra bq fields: ''",
        ));

        assert!(retryable_storage_write_schema_update_detail(&error).is_some());
    }

    #[test]
    fn process_append_result_reports_schema_propagation_error() {
        let result = process_append_result(AppendResult {
            request_index: 0,
            responses: vec![
                Ok(successful_append_response()),
                Err(tonic::Status::invalid_argument("schema_mismatch_extra_fields")),
            ],
            total_bytes_sent: 128,
            successful_bytes_sent: 0,
        });

        assert!(matches!(result, AppendRequestProcessResult::RequestError { .. }));
    }

    #[test]
    fn process_append_result_reports_success_without_row_errors() {
        let response = successful_append_response();
        let result = process_append_result(AppendResult {
            request_index: 0,
            responses: vec![Ok(response)],
            total_bytes_sent: 128,
            successful_bytes_sent: 64,
        });

        assert!(matches!(result, AppendRequestProcessResult::Success));
    }

    #[test]
    fn storage_write_not_found_includes_table_recreation_message() {
        let error = BQError::from(tonic::Status::not_found(
            "Table 123:dataset.test_users_0 is re-created. Entity: \
             projects/project/datasets/dataset/tables/test_users_0/streams/_default",
        ));

        assert!(is_storage_write_not_found(&error));
        assert!(retryable_storage_write_schema_update_detail(&error).is_none());
    }

    #[test]
    fn generic_not_found_requires_table_exists_probe() {
        let error = BQError::from(tonic::Status::not_found("Requested entity was not found"));

        assert!(is_storage_write_not_found(&error));
        assert!(retryable_storage_write_schema_update_detail(&error).is_none());
    }

    #[test]
    fn storage_write_retry_jitter_stays_within_delay_bounds() {
        let delay = Duration::from_secs(10);

        for _ in 0..100 {
            let jittered_delay = storage_write_retry_delay_with_jitter(delay);

            assert!(jittered_delay >= Duration::from_secs(5));
            assert!(jittered_delay <= delay);
        }
    }

    #[test]
    fn process_append_result_does_not_retry_generic_not_found() {
        let result = process_append_result(AppendResult {
            request_index: 0,
            responses: vec![Err(tonic::Status::not_found(
                "Table 123:dataset.test_users_0 was not found.",
            ))],
            total_bytes_sent: 128,
            successful_bytes_sent: 0,
        });

        assert!(matches!(result, AppendRequestProcessResult::RequestError { .. }));
    }

    #[test]
    fn storage_write_retry_timeout_error_is_worker_retryable() {
        let error = storage_write_retry_timeout_error("retryable storage write error");

        assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);
        assert_eq!(error.description(), Some("BigQuery storage write retry timed out"));
    }
}
