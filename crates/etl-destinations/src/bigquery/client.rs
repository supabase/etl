use std::{collections::HashMap, fmt, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use etl::{
    data::Cell,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    pipeline::PipelineId,
    schema::{ColumnSchema, ReplicatedTableSchema, Type},
};
use gcp_bigquery_client::{
    Client,
    auth::Authenticator,
    client_builder::ClientBuilder,
    error::BQError,
    google::{
        cloud::bigquery::storage::v1::{
            AppendRowsRequest, CreateWriteStreamRequest, RowError, StorageError, WriteStream,
            append_rows_request, append_rows_response, big_query_write_client::BigQueryWriteClient,
            row_error::RowErrorCode, storage_error::StorageErrorCode, write_stream,
        },
        rpc::Status as GoogleRpcStatus,
    },
    model::{
        query_parameter::QueryParameter, query_parameter_type::QueryParameterType,
        query_parameter_value::QueryParameterValue, query_request::QueryRequest,
        query_response::ResultSet,
    },
    storage::{
        BatchAppendRequest, BatchAppendResult, MAX_BATCH_SIZE_BYTES, StorageApi, StorageApiConfig,
        StreamName, TableBatch, TableDescriptor,
    },
    yup_oauth2::{
        ApplicationDefaultCredentialsAuthenticator as YupApplicationDefaultCredentialsAuthenticator,
        ApplicationDefaultCredentialsFlowOpts, ApplicationSecret,
        AuthorizedUserAuthenticator as YupAuthorizedUserAuthenticator,
        InstalledFlowAuthenticator as YupInstalledFlowAuthenticator, InstalledFlowReturnMethod,
        ServiceAccountKey,
        authenticator::{ApplicationDefaultCredentialsTypes, DefaultAuthenticator},
        authorized_user::AuthorizedUserSecret,
        parse_application_secret, parse_service_account_key,
    },
};
use metrics::counter;
use prost::Message;
use rand::random;
use serde::Deserialize;
use tokio::{
    sync::Semaphore,
    time::{Duration, Instant, sleep},
};
use tonic::{
    Code, Request, Status,
    codec::CompressionEncoding,
    transport::{Channel, ClientTlsConfig},
};
use tracing::{debug, error, info, warn};

use crate::{
    bigquery::{
        encoding::BigQueryTableRow,
        metrics::{
            ETL_BQ_APPEND_BATCHES_BATCH_ERRORS_TOTAL, ETL_BQ_APPEND_BATCHES_BATCH_ROW_ERRORS_TOTAL,
        },
        schema::{
            create_append_only_columns_spec, create_columns_spec, default_expression_sql,
            postgres_to_bigquery_type,
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
/// BigQuery OAuth scope used by the upstream BigQuery client.
const BIG_QUERY_AUTH_URL: &str = "https://www.googleapis.com/auth/bigquery";
/// Base URL for the BigQuery Storage Write API endpoint.
const BIG_QUERY_STORAGE_API_URL: &str = "https://bigquerystorage.googleapis.com";
/// Domain name for BigQuery Storage API TLS configuration.
const BIGQUERY_STORAGE_API_DOMAIN: &str = "bigquerystorage.googleapis.com";
/// Maximum message size for Storage Write gRPC requests and responses.
const STORAGE_WRITE_MAX_MESSAGE_SIZE_BYTES: usize = 20 * 1024 * 1024;
/// HTTP/2 keepalive interval for committed-stream Storage Write connections.
const STORAGE_WRITE_HTTP2_KEEPALIVE_INTERVAL_SECS: u64 = 30;
/// HTTP/2 keepalive timeout for committed-stream Storage Write connections.
const STORAGE_WRITE_HTTP2_KEEPALIVE_TIMEOUT_SECS: u64 = 10;
/// Retry policy for committed-stream append RPC failures.
const COMMITTED_STREAM_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 8,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(30),
};

/// Result of one committed-stream append request.
pub(super) struct CommittedStreamAppendResult {
    pub(super) row_count: usize,
    pub(super) bytes_sent: usize,
    pub(super) bytes_received: usize,
}
/// BigQuery project identifier.
pub type BigQueryProjectId = String;
/// BigQuery dataset identifier.
pub type BigQueryDatasetId = String;
/// BigQuery table identifier.
pub type BigQueryTableId = String;

/// Local authenticator used for low-level Storage Write RPCs not exposed by
/// the high-level client wrapper.
#[derive(Clone)]
struct BigQueryAuthenticator {
    auth: DefaultAuthenticator,
    scopes: Vec<String>,
}

#[derive(Deserialize)]
struct CredentialType {
    #[serde(rename = "type")]
    cred_type: String,
}

fn default_adc_path() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        std::env::var("APPDATA").ok().map(|appdata| {
            PathBuf::from(appdata).join("gcloud/application_default_credentials.json")
        })
    }
    #[cfg(not(target_os = "windows"))]
    {
        std::env::var("HOME").ok().map(|home| {
            PathBuf::from(home).join(".config/gcloud/application_default_credentials.json")
        })
    }
}

fn adc_credential_path() -> Option<PathBuf> {
    std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .map(PathBuf::from)
        .or_else(default_adc_path)
}

impl BigQueryAuthenticator {
    async fn from_service_account_key(
        service_account_key: ServiceAccountKey,
        scopes: &[&str],
    ) -> Result<Arc<dyn Authenticator>, BQError> {
        let auth = gcp_bigquery_client::yup_oauth2::ServiceAccountAuthenticator::builder(
            service_account_key,
        )
        .build()
        .await
        .map_err(BQError::InvalidServiceAccountAuthenticator)?;

        Ok(Arc::new(Self { auth, scopes: scopes.iter().map(|scope| scope.to_string()).collect() }))
    }

    async fn from_application_default_credentials(
        scopes: &[&str],
    ) -> Result<Arc<dyn Authenticator>, BQError> {
        if let Some(auth) =
            Self::try_authorized_user_application_default_credentials(scopes).await?
        {
            return Ok(auth);
        }

        let opts = ApplicationDefaultCredentialsFlowOpts::default();
        let auth = match YupApplicationDefaultCredentialsAuthenticator::builder(opts).await {
            ApplicationDefaultCredentialsTypes::InstanceMetadata(auth) => auth.build().await,
            ApplicationDefaultCredentialsTypes::ServiceAccount(auth) => auth.build().await,
        }
        .map_err(BQError::InvalidApplicationDefaultCredentialsAuthenticator)?;

        Ok(Arc::new(Self { auth, scopes: scopes.iter().map(|scope| scope.to_string()).collect() }))
    }

    async fn try_authorized_user_application_default_credentials(
        scopes: &[&str],
    ) -> Result<Option<Arc<dyn Authenticator>>, BQError> {
        let Some(credential_path) = adc_credential_path() else {
            return Ok(None);
        };
        let Ok(contents) = tokio::fs::read_to_string(&credential_path).await else {
            return Ok(None);
        };
        let Ok(credential_type) = serde_json::from_str::<CredentialType>(&contents) else {
            return Ok(None);
        };
        if credential_type.cred_type != "authorized_user" {
            return Ok(None);
        }

        let secret: AuthorizedUserSecret = serde_json::from_str(&contents)?;
        let auth = YupAuthorizedUserAuthenticator::builder(secret)
            .build()
            .await
            .map_err(BQError::InvalidAuthorizedUserAuthenticator)?;

        Ok(Some(Arc::new(Self {
            auth,
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
        })))
    }

    async fn from_installed_flow<S: AsRef<[u8]>, P: Into<PathBuf>>(
        secret: S,
        scopes: &[&str],
        persistent_file_path: P,
    ) -> Result<Arc<dyn Authenticator>, BQError> {
        let app_secret: ApplicationSecret = parse_application_secret(secret)?;
        let auth = YupInstalledFlowAuthenticator::builder(
            app_secret,
            InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk(persistent_file_path)
        .build()
        .await
        .map_err(BQError::InvalidInstalledFlowAuthenticator)?;

        auth.token(scopes).await.map_err(BQError::YupAuthError)?;

        Ok(Arc::new(Self { auth, scopes: scopes.iter().map(|scope| scope.to_string()).collect() }))
    }
}

#[async_trait]
impl Authenticator for BigQueryAuthenticator {
    async fn access_token(&self) -> Result<String, BQError> {
        Ok(self
            .auth
            .clone()
            .token(self.scopes.as_ref())
            .await?
            .token()
            .ok_or(BQError::NoToken)?
            .to_string())
    }
}

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

/// A batch append request that should be retried after a local Storage Write
/// retry delay.
#[derive(Debug)]
struct RetryableAppendRequest {
    request: BigQueryAppendRequest,
    detail: String,
}

/// A BigQuery append request with local retry context.
#[derive(Debug, Clone)]
pub(super) struct BigQueryAppendRequest {
    /// Request sent to the Storage Write API client.
    request: BatchAppendRequest<BigQueryTableRow>,
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
pub(super) fn create_append_trace_id(
    pipeline_id: PipelineId,
    table_id: &str,
    batch_index: usize,
) -> String {
    format!("supabase_etl_{pipeline_id}_{table_id}_{batch_index}_{}", random::<u32>())
}

/// Creates an authenticated gRPC request with Bearer token authorization.
async fn new_authorized_storage_request<T>(
    auth: Arc<dyn Authenticator>,
    message: T,
) -> Result<Request<T>, BQError> {
    let bearer_token = format!("Bearer {}", auth.access_token().await?);
    let bearer_value = bearer_token.as_str().try_into()?;

    let mut request = Request::new(message);
    request.metadata_mut().insert("authorization", bearer_value);

    Ok(request)
}

/// Creates a configured gRPC client for BigQuery Storage Write API.
async fn create_storage_grpc_client() -> Result<BigQueryWriteClient<Channel>, BQError> {
    let tls_config =
        ClientTlsConfig::new().domain_name(BIGQUERY_STORAGE_API_DOMAIN).with_enabled_roots();

    let channel = Channel::from_static(BIG_QUERY_STORAGE_API_URL)
        .tls_config(tls_config)?
        .http2_keep_alive_interval(Duration::from_secs(STORAGE_WRITE_HTTP2_KEEPALIVE_INTERVAL_SECS))
        .keep_alive_timeout(Duration::from_secs(STORAGE_WRITE_HTTP2_KEEPALIVE_TIMEOUT_SECS))
        .keep_alive_while_idle(true)
        .connect()
        .await?;

    Ok(BigQueryWriteClient::new(channel)
        .max_encoding_message_size(STORAGE_WRITE_MAX_MESSAGE_SIZE_BYTES)
        .max_decoding_message_size(STORAGE_WRITE_MAX_MESSAGE_SIZE_BYTES)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip))
}

/// Returns true when a committed-stream append status can be retried locally.
fn is_retryable_committed_stream_status(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Aborted | Code::Internal | Code::ResourceExhausted | Code::Unavailable
    )
}

/// Returns true when a committed-stream response error can be retried locally.
fn is_retryable_committed_stream_response_status(status: &GoogleRpcStatus) -> bool {
    matches!(
        Code::from_i32(status.code),
        Code::Aborted | Code::Internal | Code::ResourceExhausted | Code::Unavailable
    )
}

/// Returns true when BigQuery has already accepted the rows at this offset.
fn is_committed_stream_already_exists(status: &GoogleRpcStatus) -> bool {
    Code::from_i32(status.code) == Code::AlreadyExists
}

/// Converts a committed-stream append response error into an ETL error.
fn committed_stream_response_error_to_etl_error(status: &GoogleRpcStatus) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "BigQuery committed stream append failed",
        format!(
            "BigQuery returned gRPC code {} while appending with an explicit offset",
            status.code
        )
    )
}

/// Converts a committed-stream transport status into an ETL error.
fn committed_stream_status_to_etl_error(status: Status) -> EtlError {
    let kind = if is_retryable_committed_stream_status(&status) {
        ErrorKind::DestinationAtomicBatchRetryable
    } else {
        ErrorKind::DestinationError
    };

    etl_error!(
        kind,
        "BigQuery committed stream append failed",
        format!(
            "BigQuery Storage Write API returned {} while appending with an explicit offset",
            status.code()
        )
    )
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

fn compute_committed_stream_max_inflight_requests(connection_pool_size: usize) -> usize {
    connection_pool_size.clamp(1, MAX_SAFE_INFLIGHT_REQUESTS)
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
                return BatchProcessResult::RequestError { error: BQError::from(status) };
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

        BQError::RequestError(_) => (ErrorKind::DestinationIoError, "BigQuery request failed"),
        BQError::TonicTransportError(_) => {
            (ErrorKind::DestinationIoError, "BigQuery transport error")
        }

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

        BQError::SerializationError(_) => {
            (ErrorKind::SerializationError, "BigQuery JSON serialization error")
        }

        BQError::TonicInvalidMetadataValueError(_) => {
            (ErrorKind::InvalidData, "BigQuery invalid metadata value")
        }
        BQError::TonicStatusError(status) => match status.code() {
            Code::Unavailable => (ErrorKind::DestinationError, "BigQuery unavailable"),
            Code::Internal => (ErrorKind::DestinationError, "BigQuery internal error"),
            Code::Aborted => (ErrorKind::DestinationError, "BigQuery operation aborted"),
            Code::Cancelled => (ErrorKind::DestinationError, "BigQuery operation cancelled"),
            Code::DeadlineExceeded => (ErrorKind::DestinationError, "BigQuery deadline exceeded"),
            Code::ResourceExhausted => (ErrorKind::DestinationError, "BigQuery resource exhausted"),
            Code::FailedPrecondition => {
                (ErrorKind::DestinationError, "BigQuery precondition failed")
            }
            Code::Unknown => (ErrorKind::DestinationError, "BigQuery unknown error"),
            Code::PermissionDenied => (ErrorKind::DestinationError, "BigQuery permission denied"),
            Code::Unauthenticated => {
                (ErrorKind::DestinationError, "BigQuery authentication failed")
            }
            Code::InvalidArgument => (ErrorKind::DestinationError, "BigQuery invalid argument"),
            Code::NotFound => (ErrorKind::DestinationTableMissing, "BigQuery entity not found"),
            Code::AlreadyExists => {
                (ErrorKind::DestinationTableAlreadyExists, "BigQuery entity already exists")
            }
            Code::OutOfRange => (ErrorKind::DestinationError, "BigQuery offset out of range"),
            Code::Unimplemented => {
                (ErrorKind::DestinationError, "BigQuery operation not supported")
            }
            Code::DataLoss => (ErrorKind::DestinationError, "BigQuery data loss"),
            Code::Ok => (ErrorKind::DestinationError, "BigQuery unexpected ok status"),
        },

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
    auth: Arc<dyn Authenticator>,
    storage_write_client: BigQueryWriteClient<Channel>,
    committed_stream_append_permits: Arc<Semaphore>,
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
        let scopes = [BIG_QUERY_AUTH_URL];
        let service_account_key =
            gcp_bigquery_client::yup_oauth2::read_service_account_key(sa_key_file)
                .await
                .map_err(BQError::from)
                .map_err(bq_error_to_etl_error)?;
        let auth = BigQueryAuthenticator::from_service_account_key(service_account_key, &scopes)
            .await
            .map_err(bq_error_to_etl_error)?;

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_authenticator(auth.clone())
            .await
            .map_err(bq_error_to_etl_error)?;
        let storage_write_client =
            create_storage_grpc_client().await.map_err(bq_error_to_etl_error)?;
        let committed_stream_append_permits = Arc::new(Semaphore::new(
            compute_committed_stream_max_inflight_requests(connection_pool_size),
        ));

        Ok(BigQueryClient {
            project_id,
            client,
            auth,
            storage_write_client,
            committed_stream_append_permits,
        })
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
        let scopes = [BIG_QUERY_AUTH_URL];

        let sa_key = parse_service_account_key(sa_key)
            .map_err(BQError::from)
            .map_err(bq_error_to_etl_error)?;
        let auth = BigQueryAuthenticator::from_service_account_key(sa_key, &scopes)
            .await
            .map_err(bq_error_to_etl_error)?;
        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_authenticator(auth.clone())
            .await
            .map_err(bq_error_to_etl_error)?;
        let storage_write_client =
            create_storage_grpc_client().await.map_err(bq_error_to_etl_error)?;
        let committed_stream_append_permits = Arc::new(Semaphore::new(
            compute_committed_stream_max_inflight_requests(connection_pool_size),
        ));

        Ok(BigQueryClient {
            project_id,
            client,
            auth,
            storage_write_client,
            committed_stream_append_permits,
        })
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
        let scopes = [BIG_QUERY_AUTH_URL];
        let auth = BigQueryAuthenticator::from_application_default_credentials(&scopes)
            .await
            .map_err(bq_error_to_etl_error)?;

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_authenticator(auth.clone())
            .await
            .map_err(bq_error_to_etl_error)?;
        let storage_write_client =
            create_storage_grpc_client().await.map_err(bq_error_to_etl_error)?;
        let committed_stream_append_permits = Arc::new(Semaphore::new(
            compute_committed_stream_max_inflight_requests(connection_pool_size),
        ));

        Ok(BigQueryClient {
            project_id,
            client,
            auth,
            storage_write_client,
            committed_stream_append_permits,
        })
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
        let scopes = [BIG_QUERY_AUTH_URL];
        let auth =
            BigQueryAuthenticator::from_installed_flow(secret, &scopes, persistent_file_path)
                .await
                .map_err(bq_error_to_etl_error)?;

        let client = ClientBuilder::new()
            .with_storage_config(storage_config)
            .build_from_authenticator(auth.clone())
            .await
            .map_err(bq_error_to_etl_error)?;
        let storage_write_client =
            create_storage_grpc_client().await.map_err(bq_error_to_etl_error)?;
        let committed_stream_append_permits = Arc::new(Semaphore::new(
            compute_committed_stream_max_inflight_requests(connection_pool_size),
        ));

        Ok(BigQueryClient {
            project_id,
            client,
            auth,
            storage_write_client,
            committed_stream_append_permits,
        })
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

    /// Creates an append-only history table if it doesn't exist.
    pub async fn create_append_only_table_if_missing(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<bool> {
        if self.table_exists(dataset_id, table_id).await? {
            return Ok(false);
        }

        self.create_append_only_table(dataset_id, table_id, replicated_table_schema).await?;

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

    /// Creates an append-only history table in BigQuery.
    pub async fn create_append_only_table(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let full_table_name = self.full_table_name(dataset_id, table_id)?;
        let columns_spec = create_append_only_columns_spec(replicated_table_schema)?;

        info!(%full_table_name, "creating append-only table in bigquery");

        let query = format!("create table {full_table_name} {columns_spec}");

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
        let Some(rendered_default_expression) = default_expression_sql(default_expression, typ)
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

    /// Creates an explicitly-created committed Storage Write stream.
    pub(super) async fn create_committed_write_stream(
        &self,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
    ) -> EtlResult<String> {
        let parent =
            format!("projects/{}/datasets/{dataset_id}/tables/{table_id}", self.project_id);
        let request = CreateWriteStreamRequest {
            parent: parent.clone(),
            write_stream: Some(WriteStream {
                name: String::new(),
                r#type: write_stream::Type::Committed as i32,
                create_time: None,
                commit_time: None,
                table_schema: None,
                write_mode: write_stream::WriteMode::Insert as i32,
                location: String::new(),
            }),
        };

        let request = new_authorized_storage_request(self.auth.clone(), request)
            .await
            .map_err(bq_error_to_etl_error)?;
        let mut grpc_client = self.storage_write_client.clone();
        let write_stream = grpc_client
            .create_write_stream(request)
            .await
            .map_err(|status| committed_stream_status_to_etl_error(status))?
            .into_inner();

        Ok(write_stream.name)
    }

    /// Appends rows to a committed stream using explicit offsets.
    pub(super) async fn append_committed_stream_rows(
        &self,
        stream_name: &str,
        table_descriptor: TableDescriptor,
        rows: &[BigQueryTableRow],
        start_offset: i64,
        trace_id: String,
    ) -> EtlResult<CommittedStreamAppendResult> {
        if rows.is_empty() {
            return Ok(CommittedStreamAppendResult {
                row_count: 0,
                bytes_sent: 0,
                bytes_received: 0,
            });
        }

        let (encoded_rows, row_count) =
            StorageApi::create_rows(&table_descriptor, rows, MAX_BATCH_SIZE_BYTES);
        if row_count == 0 {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "BigQuery committed stream row is too large",
                "A single encoded row exceeded the Storage Write API request size limit"
            ));
        }

        let request = AppendRowsRequest {
            write_stream: stream_name.to_owned(),
            offset: Some(start_offset),
            trace_id,
            missing_value_interpretations: HashMap::new(),
            default_missing_value_interpretation:
                append_rows_request::MissingValueInterpretation::Unspecified as i32,
            rows: Some(encoded_rows),
        };
        let bytes_sent = request.encoded_len();

        let bytes_received = retry_with_backoff(
            COMMITTED_STREAM_RETRY_POLICY,
            |err: &EtlError| {
                if err.kind() == ErrorKind::DestinationAtomicBatchRetryable {
                    RetryDecision::Retry
                } else {
                    RetryDecision::Stop
                }
            },
            storage_write_retry_delay_with_jitter,
            |attempt| {
                warn!(
                    retry_index = attempt.retry_index,
                    max_retries = attempt.max_retries,
                    sleep_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying retryable bigquery committed stream append error"
                );
            },
            || {
                let request = request.clone();
                async move {
                    self.append_committed_stream_request(stream_name, request, start_offset).await
                }
            },
        )
        .await
        .map_err(|failure| failure.last_error)?;

        Ok(CommittedStreamAppendResult { row_count, bytes_sent, bytes_received })
    }

    async fn append_committed_stream_request(
        &self,
        stream_name: &str,
        request: AppendRowsRequest,
        expected_offset: i64,
    ) -> EtlResult<usize> {
        let _permit = self
            .committed_stream_append_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                etl_error!(
                    ErrorKind::DestinationError,
                    "BigQuery committed stream append limiter closed",
                    "The committed stream append limiter was closed before a permit could be acquired"
                )
            })?;
        let request =
            new_authorized_storage_request(self.auth.clone(), tokio_stream::iter(vec![request]))
                .await
                .map_err(bq_error_to_etl_error)?;
        let mut grpc_client = self.storage_write_client.clone();
        let mut responses = grpc_client
            .append_rows(request)
            .await
            .map_err(committed_stream_status_to_etl_error)?
            .into_inner();

        let response = responses
            .message()
            .await
            .map_err(committed_stream_status_to_etl_error)?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::DestinationAtomicBatchRetryable,
                    "BigQuery committed stream append returned no response",
                    "Storage Write API closed the append stream before acknowledging the request"
                )
            })?;
        let response_bytes = response.encoded_len();

        if !response.row_errors.is_empty() {
            return Err(row_error_to_etl_error(response.row_errors[0].clone()));
        }

        match response.response {
            Some(append_rows_response::Response::AppendResult(result)) => {
                if let Some(offset) = result.offset
                    && offset != expected_offset
                {
                    return Err(etl_error!(
                        ErrorKind::DestinationError,
                        "BigQuery committed stream append returned an unexpected offset",
                        format!(
                            "Expected offset {expected_offset} for stream {stream_name}, got {offset}"
                        )
                    ));
                }

                Ok(response_bytes)
            }
            Some(append_rows_response::Response::Error(status))
                if is_committed_stream_already_exists(&status) =>
            {
                Err(etl_error!(
                    ErrorKind::DestinationError,
                    "BigQuery committed stream append overlapped an existing offset",
                    format!(
                        "BigQuery reported ALREADY_EXISTS for stream {stream_name} at offset \
                         {expected_offset}; refusing to treat the request as successful because \
                         replay batch boundaries may not match the original append"
                    )
                ))
            }
            Some(append_rows_response::Response::Error(status))
                if is_retryable_committed_stream_response_status(&status) =>
            {
                Err(etl_error!(
                    ErrorKind::DestinationAtomicBatchRetryable,
                    "BigQuery committed stream append returned a retryable error",
                    format!(
                        "BigQuery returned gRPC code {} for stream {stream_name} at offset {expected_offset}",
                        status.code
                    )
                ))
            }
            Some(append_rows_response::Response::Error(status)) => {
                Err(committed_stream_response_error_to_etl_error(&status))
            }
            None => Err(etl_error!(
                ErrorKind::DestinationAtomicBatchRetryable,
                "BigQuery committed stream append returned an empty response",
                "Storage Write API did not include an append result or error"
            )),
        }
    }

    /// Appends table batches to BigQuery using the concurrent Storage Write
    /// API.
    ///
    /// Accepts pre-constructed append requests and processes them concurrently.
    ///
    /// Retries for transient request and transport failures are handled inside
    /// the underlying Storage Write API library. This method also retries
    /// locally retryable Storage Write failures that can happen after BigQuery
    /// table metadata changes, then converts final failures into ETL errors.
    pub(super) async fn append_table_batches(
        &self,
        append_requests: Vec<BigQueryAppendRequest>,
    ) -> EtlResult<(usize, usize)> {
        if append_requests.is_empty() {
            return Ok((0, 0));
        }

        let mut pending_requests = append_requests;
        let mut total_bytes_sent = 0;
        let mut total_bytes_received = 0;

        let started_at = Instant::now();
        let mut attempt = 1;
        let mut retry_delay = STORAGE_WRITE_RETRY_DELAY;

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

                    let retry_summary =
                        format_retryable_storage_write_requests(&next_pending_requests);
                    pending_requests =
                        next_pending_requests.into_iter().map(|request| request.request).collect();

                    let pending_batch_count = pending_requests.len();
                    if pending_batch_count == 0 {
                        return Ok((total_bytes_sent, total_bytes_received));
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
                        pending_batch_count,
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
    async fn append_table_batches_once(
        &self,
        append_requests: Vec<BigQueryAppendRequest>,
    ) -> EtlResult<AppendProcessingResult> {
        if append_requests.is_empty() {
            return Ok(AppendProcessingResult::Success { bytes_sent: 0, bytes_received: 0 });
        }

        debug!(batch_count = append_requests.len(), "streaming table batches concurrently");

        let raw_append_requests =
            append_requests.iter().map(|request| request.request.clone()).collect::<Vec<_>>();
        let batch_append_results =
            self.client.storage().append_table_batches(raw_append_requests).await.inspect_err(
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
                let mut retryable_requests = Vec::new();
                let mut has_non_retryable_request = false;

                // A call-level Storage Write error is not tied to a single batch index. Retry
                // only if the error is locally retryable for every append request in the call.
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
                        bytes_sent: 0,
                        bytes_received: 0,
                    });
                }

                return Ok(AppendProcessingResult::Error(bq_error_to_etl_error(error)));
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
                    // Batch-level request errors keep their batch index, so only the affected
                    // append request needs to be classified and retried.
                    if let Some(detail) = retryable_storage_write_error_detail(
                        self,
                        &append_requests[batch_index],
                        &request_error,
                    )
                    .await?
                    {
                        retryable_batch_details[batch_index] = Some(detail);
                        continue;
                    }

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
    /// Creates a properly configured [`BigQueryAppendRequest`] for efficient
    /// append retries.
    pub(super) fn create_batch_append_request(
        &self,
        pipeline_id: PipelineId,
        batch_index: usize,
        dataset_id: &BigQueryDatasetId,
        table_id: &BigQueryTableId,
        table_descriptor: TableDescriptor,
        validated_rows: Vec<BigQueryTableRow>,
    ) -> EtlResult<BigQueryAppendRequest> {
        let stream_name =
            StreamName::new_default(self.project_id.clone(), dataset_id.clone(), table_id.clone());

        let table_batch = TableBatch::new(stream_name, table_descriptor, validated_rows);
        let trace_id =
            create_append_trace_id(pipeline_id, table_batch.stream_name().table(), batch_index);

        Ok(BigQueryAppendRequest {
            request: BatchAppendRequest::new(table_batch, trace_id),
            dataset_id: dataset_id.clone(),
            table_id: table_id.clone(),
        })
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
    fn committed_stream_max_inflight_uses_connection_pool_size_as_proactive_limit() {
        assert_eq!(compute_committed_stream_max_inflight_requests(0), 1);
        assert_eq!(compute_committed_stream_max_inflight_requests(3), 3);
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
    fn process_single_batch_append_result_reports_schema_propagation_error() {
        let result = process_single_batch_append_result(BatchAppendResult {
            batch_index: 0,
            responses: vec![
                Ok(successful_append_response()),
                Err(tonic::Status::invalid_argument("schema_mismatch_extra_fields")),
            ],
            bytes_sent: 128,
        });

        assert!(matches!(result, BatchProcessResult::RequestError { .. }));
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
    fn storage_write_retry_timeout_error_is_worker_retryable() {
        let error = storage_write_retry_timeout_error("retryable storage write error");

        assert_eq!(error.kind(), ErrorKind::DestinationAtomicBatchRetryable);
        assert_eq!(error.description(), Some("BigQuery storage write retry timed out"));
    }
}
