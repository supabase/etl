//! GCS upload support for BigQuery initial-copy staging files.

use std::{fmt, future::Future, io::Write, path::PathBuf, sync::Arc, time::Duration};

use bytes::Bytes;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use gcp_bigquery_client::yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
    AuthorizedUserAuthenticator, InstalledFlowAuthenticator, InstalledFlowReturnMethod,
    ServiceAccountAuthenticator,
    authenticator::{ApplicationDefaultCredentialsTypes, DefaultAuthenticator},
    authorized_user::AuthorizedUserSecret,
    parse_application_secret, parse_service_account_key, read_service_account_key,
};
use reqwest::{
    Body, Client, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RANGE},
};
use serde::{Deserialize, Serialize};
use tokio::{fs, runtime::Handle, sync::OnceCell};
use tracing::warn;

use crate::{
    bigquery::initial_copy::{
        GcsDeleteRequest, GcsObjectMetadata, GcsStreamingUploadRequest, GcsUploader, gcs_uri,
    },
    retry::{RetryAttempt, RetryDecision, RetryPolicy, retry_with_backoff},
};

/// OAuth scope used to upload staged snapshot files to GCS.
const GCS_READ_WRITE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
/// JSON API endpoint used for resumable uploads.
const GCS_UPLOAD_BASE_URL: &str = "https://storage.googleapis.com/upload/storage/v1/b";
/// JSON API endpoint used for object metadata operations.
const GCS_JSON_BASE_URL: &str = "https://storage.googleapis.com/storage/v1/b";
/// Retry policy for transient GCS JSON API failures.
const GCS_REQUEST_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 5,
    initial_delay: Duration::from_millis(250),
    max_delay: Duration::from_secs(5),
};
/// Chunk size used for resumable GCS uploads.
///
/// GCS requires non-final chunks to be multiples of 256 KiB and recommends at
/// least 8 MiB; this uses exactly that recommended minimum. This buffer is
/// held per open writer slot (one per initial-copy Postgres copy connection),
/// so worst-case resident memory from this buffer alone is approximately
/// `GCS_RESUMABLE_UPLOAD_CHUNK_SIZE * max_table_sync_workers *
/// max_copy_connections_per_table`. At the pipeline's default parallelism
/// (4 * 4 = 16 slots), this costs 16 * 8 MiB = 128 MiB worst case, which is
/// safe on memory-constrained (for example 500 MiB) replicator deployments.
/// The Avro writer's block-size buffer follows the same reasoning.
const GCS_RESUMABLE_UPLOAD_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// Uploads BigQuery initial-copy staging files to GCS.
#[derive(Clone)]
pub(crate) struct GoogleCloudStorageUploader {
    client: Client,
    auth: GcsAuth,
}

impl GoogleCloudStorageUploader {
    /// Creates an uploader from a service-account key file path.
    pub(crate) fn new_with_key_path(sa_key_file: impl Into<String>) -> Self {
        Self::new(GcsAuth::ServiceAccountKeyPath {
            path: Arc::from(sa_key_file.into()),
            authenticator: Arc::new(OnceCell::new()),
        })
    }

    /// Creates an uploader from a service-account key JSON string.
    pub(crate) fn new_with_key(sa_key: impl Into<String>) -> Self {
        Self::new(GcsAuth::ServiceAccountKey {
            key: Arc::from(sa_key.into()),
            authenticator: Arc::new(OnceCell::new()),
        })
    }

    /// Creates an uploader using Application Default Credentials.
    pub(crate) fn new_with_adc() -> Self {
        Self::new(GcsAuth::ApplicationDefaultCredentials {
            authenticator: Arc::new(OnceCell::new()),
        })
    }

    /// Creates an uploader using OAuth2 installed-flow credentials.
    pub(crate) fn new_with_flow_authenticator(
        secret: impl Into<Vec<u8>>,
        persistent_file_path: impl Into<PathBuf>,
    ) -> Self {
        Self::new(GcsAuth::InstalledFlow {
            secret: Arc::from(secret.into()),
            persistent_file_path: Arc::new(persistent_file_path.into()),
            authenticator: Arc::new(OnceCell::new()),
        })
    }

    /// Creates an uploader for the provided auth source.
    fn new(auth: GcsAuth) -> Self {
        Self { client: Client::new(), auth }
    }
}

impl GcsUploader for GoogleCloudStorageUploader {
    fn delete_object(
        &self,
        request: GcsDeleteRequest,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        self.delete_object_inner(request)
    }
}

impl GoogleCloudStorageUploader {
    /// Starts one resumable upload session for an object.
    async fn start_resumable_upload_session(
        &self,
        bucket: &str,
        object_name: &str,
        content_type: &str,
        size_bytes: Option<u64>,
    ) -> EtlResult<String> {
        let url = format!("{GCS_UPLOAD_BASE_URL}/{bucket}/o");
        let metadata = GcsObjectUploadMetadata { name: object_name };

        let response = retry_with_backoff(
            GCS_REQUEST_RETRY_POLICY,
            gcs_request_retry_decision,
            |delay| delay,
            log_gcs_request_retry,
            || {
                let auth = self.auth.clone();
                let metadata = &metadata;
                let url = url.clone();
                let content_type = content_type.to_owned();

                async move {
                    let token = auth.access_token().await.map_err(GcsRequestAttemptError::Auth)?;
                    let mut request = self
                        .client
                        .post(url)
                        .bearer_auth(token)
                        .query(&[("uploadType", "resumable")])
                        .header(CONTENT_TYPE, "application/json; charset=UTF-8")
                        .header("X-Upload-Content-Type", content_type);
                    if let Some(size_bytes) = size_bytes {
                        request = request.header("X-Upload-Content-Length", size_bytes);
                    }

                    let response = request
                        .json(metadata)
                        .send()
                        .await
                        .map_err(GcsRequestAttemptError::Request)?;

                    let status = response.status();
                    if !status.is_success() {
                        return Err(GcsRequestAttemptError::Status(status));
                    }

                    Ok(response)
                }
            },
        )
        .await
        .map_err(|failure| gcs_attempt_error(failure.last_error, bucket, object_name))?;

        response
            .headers()
            .get(LOCATION)
            .and_then(|location| location.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "GCS resumable upload session missing",
                    format!("GCS object: gs://{bucket}/{object_name}")
                )
            })
    }

    /// Starts a streaming upload writer for one GCS object.
    pub(crate) async fn start_streaming_upload(
        &self,
        request: GcsStreamingUploadRequest,
    ) -> EtlResult<GcsStreamingUploadWriter> {
        validate_object_location(&request.bucket, &request.object_name)?;

        if request.content_type.is_empty() {
            return Err(etl_error!(
                ErrorKind::InvalidData,
                "GCS upload content type is invalid",
                "Content type must be non-empty."
            ));
        }

        let session_url = self
            .start_resumable_upload_session(
                &request.bucket,
                &request.object_name,
                &request.content_type,
                None,
            )
            .await?;

        Ok(GcsStreamingUploadWriter::new(
            self.client.clone(),
            Handle::current(),
            self.auth.clone(),
            request.bucket,
            request.object_name,
            session_url,
        ))
    }

    /// Deletes one staged object after request validation.
    async fn delete_object_inner(&self, request: GcsDeleteRequest) -> EtlResult<()> {
        validate_object_location(&request.bucket, &request.object_name)?;

        let encoded_object_name = encode_uri_path_part(&request.object_name);
        let url = format!("{}/{}/o/{}", GCS_JSON_BASE_URL, request.bucket, encoded_object_name);

        let response = retry_with_backoff(
            GCS_REQUEST_RETRY_POLICY,
            gcs_request_retry_decision,
            |delay| delay,
            log_gcs_request_retry,
            || {
                let auth = self.auth.clone();
                let url = url.clone();

                async move {
                    let token = auth.access_token().await.map_err(GcsRequestAttemptError::Auth)?;
                    let response = self
                        .client
                        .delete(url)
                        .bearer_auth(token)
                        .send()
                        .await
                        .map_err(GcsRequestAttemptError::Request)?;

                    let status = response.status();
                    if status != StatusCode::NOT_FOUND && !status.is_success() {
                        return Err(GcsRequestAttemptError::Status(status));
                    }

                    Ok(response)
                }
            },
        )
        .await
        .map_err(|failure| {
            gcs_attempt_error(failure.last_error, &request.bucket, &request.object_name)
        })?;

        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if !status.is_success() {
            return Err(gcs_status_error(status, &request.bucket, &request.object_name));
        }

        Ok(())
    }
}

/// Blocking writer that streams Avro bytes into one GCS resumable upload.
pub(crate) struct GcsStreamingUploadWriter {
    client: Client,
    runtime: Handle,
    auth: GcsAuth,
    bucket: String,
    object_name: String,
    session_url: String,
    buffer: Vec<u8>,
    offset: u64,
}

impl GcsStreamingUploadWriter {
    /// Creates a writer for an already-started resumable upload session.
    fn new(
        client: Client,
        runtime: Handle,
        auth: GcsAuth,
        bucket: String,
        object_name: String,
        session_url: String,
    ) -> Self {
        Self {
            client,
            runtime,
            auth,
            bucket,
            object_name,
            session_url,
            buffer: Vec::with_capacity(GCS_RESUMABLE_UPLOAD_CHUNK_SIZE as usize),
            offset: 0,
        }
    }

    /// Finishes the upload and returns metadata for the committed object.
    pub(crate) fn finish_upload(mut self) -> EtlResult<GcsObjectMetadata> {
        let total_size = self.offset + self.buffer.len() as u64;
        let Some(response) = self.upload_buffered_chunk(Some(total_size)).map_err(|err| {
            etl_error!(
                ErrorKind::DestinationIoError,
                "Failed to finish GCS streaming upload",
                format!("GCS object: gs://{}/{}", self.bucket, self.object_name),
                source: err
            )
        })?
        else {
            return Err(etl_error!(
                ErrorKind::DestinationQueryFailed,
                "GCS streaming upload finished without object metadata",
                format!("GCS object: gs://{}/{}", self.bucket, self.object_name)
            ));
        };

        let bucket = response.bucket.unwrap_or(self.bucket);
        let object_name = response.name.unwrap_or(self.object_name);
        let uri = gcs_uri(&bucket, &object_name);

        Ok(GcsObjectMetadata {
            bucket,
            object_name,
            uri,
            size_bytes: response.size.and_then(|size| size.parse().ok()).or(Some(total_size)),
        })
    }

    /// Uploads full non-final chunks while retaining the possible final chunk.
    fn flush_ready_chunks(&mut self) -> std::io::Result<()> {
        let chunk_size = GCS_RESUMABLE_UPLOAD_CHUNK_SIZE as usize;

        while self.buffer.len() > chunk_size {
            // Keep one full chunk buffered until either more bytes arrive or
            // finish provides the total size. This lets exact chunk-multiple
            // objects commit their last chunk as the final resumable request.
            let tail = self.buffer.split_off(chunk_size);
            let chunk = std::mem::replace(&mut self.buffer, tail);
            self.upload_chunk(Bytes::from(chunk), None)?;
        }

        Ok(())
    }

    /// Uploads the current buffer as one chunk.
    fn upload_buffered_chunk(
        &mut self,
        total_size: Option<u64>,
    ) -> std::io::Result<Option<GcsObjectResponse>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let bytes = Bytes::from(std::mem::take(&mut self.buffer));
        self.upload_chunk(bytes, total_size)
    }

    /// Uploads `bytes`, reconciling partial resumable progress after retries.
    fn upload_chunk(
        &mut self,
        mut bytes: Bytes,
        total_size: Option<u64>,
    ) -> std::io::Result<Option<GcsObjectResponse>> {
        let mut offset = self.offset;

        while !bytes.is_empty() {
            let client = self.client.clone();
            let auth = self.auth.clone();
            let session_url = self.session_url.clone();
            let step = self
                .runtime
                .block_on(retry_with_backoff(
                    GCS_REQUEST_RETRY_POLICY,
                    gcs_request_retry_decision,
                    |delay| delay,
                    log_gcs_request_retry,
                    || {
                        let client = client.clone();
                        let auth = auth.clone();
                        let session_url = session_url.clone();
                        let bytes = bytes.clone();

                        async move {
                            upload_resumable_chunk_with_status_check(
                                &client,
                                &session_url,
                                &auth,
                                offset,
                                total_size,
                                bytes,
                            )
                            .await
                        }
                    },
                ))
                .map_err(|failure| self.io_error(failure.last_error))?;

            match step {
                GcsResumableUploadStep::Done(response) => {
                    self.offset = total_size.unwrap_or(offset + bytes.len() as u64);
                    return Ok(Some(response));
                }
                GcsResumableUploadStep::Incomplete { next_offset } => {
                    if next_offset <= offset {
                        return Err(self.io_error(GcsRequestAttemptError::Protocol(format!(
                            "Resumable upload did not advance from offset {offset} to \
                             {next_offset}."
                        ))));
                    }

                    let advanced = (next_offset - offset) as usize;
                    if advanced > bytes.len() {
                        return Err(self.io_error(GcsRequestAttemptError::Protocol(format!(
                            "Resumable upload advanced beyond the current chunk from offset \
                             {offset} to {next_offset}."
                        ))));
                    }

                    self.offset = next_offset;
                    offset = next_offset;
                    // Drop bytes already acknowledged by GCS so retry memory is
                    // bounded by the current chunk, not by the full object.
                    bytes = bytes.slice(advanced..);
                }
            }
        }

        Ok(None)
    }

    /// Converts a GCS upload failure into an I/O error for Avro's writer API.
    fn io_error(&self, error: GcsRequestAttemptError) -> std::io::Error {
        std::io::Error::other(gcs_attempt_error(error, &self.bucket, &self.object_name))
    }
}

impl Write for GcsStreamingUploadWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let chunk_size = GCS_RESUMABLE_UPLOAD_CHUNK_SIZE as usize;
        let mut remaining = buf;

        while !remaining.is_empty() {
            if self.buffer.len() == chunk_size {
                self.upload_buffered_chunk(None)?;
            }

            let available = chunk_size.saturating_sub(self.buffer.len());
            let write_len = remaining.len().min(available);
            // Apache Avro may call `write` with arbitrary slice sizes; this
            // adapter coalesces them into GCS-sized resumable chunks.
            self.buffer.extend_from_slice(&remaining[..write_len]);
            remaining = &remaining[write_len..];
            self.flush_ready_chunks()?;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_ready_chunks()
    }
}

/// Failure from one retryable GCS request attempt.
#[derive(Debug)]
enum GcsRequestAttemptError {
    /// Authentication failed before a request could be sent.
    Auth(etl::error::EtlError),
    /// HTTP transport failure before a response was received.
    Request(reqwest::Error),
    /// HTTP response decoding failed.
    Response(reqwest::Error),
    /// GCS response headers were inconsistent with the resumable protocol.
    Protocol(String),
    /// HTTP response status returned by GCS.
    Status(StatusCode),
}

impl fmt::Display for GcsRequestAttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auth(error) => write!(f, "{error}"),
            Self::Request(error) => write!(f, "{error}"),
            Self::Response(error) => write!(f, "{error}"),
            Self::Protocol(error) => write!(f, "{error}"),
            Self::Status(status) => write!(f, "GCS returned HTTP status {}", status.as_u16()),
        }
    }
}

impl std::error::Error for GcsRequestAttemptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Auth(error) => Some(error),
            Self::Request(error) => Some(error),
            Self::Response(error) => Some(error),
            Self::Protocol(_) => None,
            Self::Status(_) => None,
        }
    }
}

/// Progress returned by one resumable upload request.
#[derive(Debug)]
enum GcsResumableUploadStep {
    /// The object upload completed and returned object metadata.
    Done(GcsObjectResponse),
    /// GCS accepted bytes through `next_offset - 1`.
    Incomplete {
        /// Next byte offset to upload.
        next_offset: u64,
    },
}

/// Authentication source for GCS upload requests.
#[derive(Clone)]
enum GcsAuth {
    /// Service-account JSON stored on disk.
    ServiceAccountKeyPath {
        /// Service-account key path.
        path: Arc<str>,
        /// Cached yup authenticator.
        authenticator: Arc<OnceCell<DefaultAuthenticator>>,
    },
    /// Service-account JSON provided in memory.
    ServiceAccountKey {
        /// Service-account key JSON.
        key: Arc<str>,
        /// Cached yup authenticator.
        authenticator: Arc<OnceCell<DefaultAuthenticator>>,
    },
    /// Environment Application Default Credentials.
    ApplicationDefaultCredentials {
        /// Cached yup authenticator.
        authenticator: Arc<OnceCell<DefaultAuthenticator>>,
    },
    /// OAuth2 installed-flow credentials.
    InstalledFlow {
        /// Client secret JSON bytes.
        secret: Arc<[u8]>,
        /// Token persistence path.
        persistent_file_path: Arc<PathBuf>,
        /// Cached yup authenticator.
        authenticator: Arc<OnceCell<DefaultAuthenticator>>,
    },
}

impl GcsAuth {
    /// Returns an access token for GCS.
    async fn access_token(&self) -> EtlResult<String> {
        let auth = self.authenticator().await?;
        token_from_auth(auth.token(&[GCS_READ_WRITE_SCOPE]).await.map_err(gcs_auth_error)?)
    }

    /// Returns the cached authenticator for this auth source.
    async fn authenticator(&self) -> EtlResult<&DefaultAuthenticator> {
        match self {
            Self::ServiceAccountKeyPath { path, authenticator } => {
                authenticator
                    .get_or_try_init(|| {
                        let path = Arc::clone(path);

                        async move {
                            let key = read_service_account_key(path.as_ref())
                                .await
                                .map_err(gcs_auth_error)?;
                            ServiceAccountAuthenticator::builder(key)
                                .build()
                                .await
                                .map_err(gcs_auth_error)
                        }
                    })
                    .await
            }
            Self::ServiceAccountKey { key, authenticator } => {
                authenticator
                    .get_or_try_init(|| {
                        let key = Arc::clone(key);

                        async move {
                            let key = parse_service_account_key(key.as_bytes())
                                .map_err(gcs_auth_error)?;
                            ServiceAccountAuthenticator::builder(key)
                                .build()
                                .await
                                .map_err(gcs_auth_error)
                        }
                    })
                    .await
            }
            Self::ApplicationDefaultCredentials { authenticator } => {
                authenticator
                    .get_or_try_init(build_application_default_credentials_authenticator)
                    .await
            }
            Self::InstalledFlow { secret, persistent_file_path, authenticator } => {
                authenticator
                    .get_or_try_init(|| {
                        let secret = Arc::clone(secret);
                        let persistent_file_path = Arc::clone(persistent_file_path);

                        async move {
                            let app_secret = parse_application_secret(secret.as_ref())
                                .map_err(gcs_auth_error)?;
                            InstalledFlowAuthenticator::builder(
                                app_secret,
                                InstalledFlowReturnMethod::HTTPRedirect,
                            )
                            .persist_tokens_to_disk(persistent_file_path.as_ref().clone())
                            .build()
                            .await
                            .map_err(gcs_auth_error)
                        }
                    })
                    .await
            }
        }
    }
}

/// Returns an access token string from a yup-oauth2 access token.
fn token_from_auth(token: gcp_bigquery_client::yup_oauth2::AccessToken) -> EtlResult<String> {
    token
        .token()
        .map(str::to_owned)
        .ok_or_else(|| etl_error!(ErrorKind::DestinationAuthenticationError, "GCS token missing"))
}

/// Builds a GCS authenticator using ADC.
async fn build_application_default_credentials_authenticator() -> EtlResult<DefaultAuthenticator> {
    if let Some(auth) = authorized_user_adc_authenticator().await? {
        return Ok(auth);
    }

    let opts = ApplicationDefaultCredentialsFlowOpts::default();
    match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
        ApplicationDefaultCredentialsTypes::InstanceMetadata(auth) => auth.build().await,
        ApplicationDefaultCredentialsTypes::ServiceAccount(auth) => auth.build().await,
    }
    .map_err(gcs_auth_error)
}

/// Returns an authenticator for authorized-user ADC credentials when present.
async fn authorized_user_adc_authenticator() -> EtlResult<Option<DefaultAuthenticator>> {
    let Some(path) = adc_credential_path() else {
        return Ok(None);
    };

    let Ok(contents) = fs::read_to_string(&path).await else {
        return Ok(None);
    };

    let Ok(credential_type) = serde_json::from_str::<CredentialType>(&contents) else {
        return Ok(None);
    };

    if credential_type.credential_type != "authorized_user" {
        return Ok(None);
    }

    let secret = serde_json::from_str::<AuthorizedUserSecret>(&contents).map_err(gcs_adc_error)?;
    let auth =
        AuthorizedUserAuthenticator::builder(secret).build().await.map_err(gcs_auth_error)?;

    Ok(Some(auth))
}

/// Returns the ADC credential path, matching `gcloud` defaults.
fn adc_credential_path() -> Option<PathBuf> {
    std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .ok()
        .map(PathBuf::from)
        .or_else(default_adc_path)
}

/// Returns the default ADC path for the current operating system.
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

/// Validates a GCS bucket/object pair before sending object requests.
fn validate_object_location(bucket: &str, object_name: &str) -> EtlResult<()> {
    if bucket.is_empty() || bucket.contains('/') {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "GCS upload bucket is invalid",
            "Bucket must be non-empty and must not contain '/'."
        ));
    }

    if object_name.is_empty() || object_name.starts_with('/') {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "GCS upload object name is invalid",
            "Object name must be non-empty and relative."
        ));
    }

    Ok(())
}

/// Percent-encodes an object name for a single URI path part.
fn encode_uri_path_part(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

/// Builds an ETL error for GCS authentication failures.
fn gcs_auth_error(error: impl std::error::Error + Send + Sync + 'static) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::DestinationAuthenticationError,
        "GCS authentication failed",
        source: error
    )
}

/// Builds an ETL error for GCS request failures.
fn gcs_request_error(error: reqwest::Error) -> etl::error::EtlError {
    etl_error!(ErrorKind::DestinationIoError, "GCS request failed", source: error)
}

/// Converts a retry attempt failure into an ETL error.
fn gcs_attempt_error(
    error: GcsRequestAttemptError,
    bucket: &str,
    object_name: &str,
) -> etl::error::EtlError {
    match error {
        GcsRequestAttemptError::Auth(error) => error,
        GcsRequestAttemptError::Request(error) => gcs_request_error(error),
        GcsRequestAttemptError::Response(error) => gcs_response_error(error),
        GcsRequestAttemptError::Protocol(error) => etl_error!(
            ErrorKind::DestinationQueryFailed,
            "GCS resumable upload protocol failed",
            format!("GCS object: gs://{bucket}/{object_name}; {error}")
        ),
        GcsRequestAttemptError::Status(status) => gcs_status_error(status, bucket, object_name),
    }
}

/// Returns whether a failed GCS request attempt should be retried.
fn gcs_request_retry_decision(error: &GcsRequestAttemptError) -> RetryDecision {
    match error {
        GcsRequestAttemptError::Auth(_) => RetryDecision::Stop,
        GcsRequestAttemptError::Request(error)
            if error.is_timeout() || error.is_connect() || error.is_request() =>
        {
            RetryDecision::Retry
        }
        GcsRequestAttemptError::Request(_) => RetryDecision::Stop,
        GcsRequestAttemptError::Response(_) => RetryDecision::Stop,
        GcsRequestAttemptError::Protocol(_) => RetryDecision::Stop,
        GcsRequestAttemptError::Status(status) if is_retryable_gcs_status(*status) => {
            RetryDecision::Retry
        }
        GcsRequestAttemptError::Status(_) => RetryDecision::Stop,
    }
}

/// Returns whether a failed chunk attempt should be reconciled with GCS.
fn should_check_resumable_upload_status(error: &GcsRequestAttemptError) -> bool {
    match error {
        GcsRequestAttemptError::Auth(_) => false,
        GcsRequestAttemptError::Request(error) => {
            error.is_timeout() || error.is_connect() || error.is_request()
        }
        GcsRequestAttemptError::Status(status) => is_retryable_gcs_status(*status),
        GcsRequestAttemptError::Response(_) | GcsRequestAttemptError::Protocol(_) => false,
    }
}

/// Converts one resumable upload response into upload progress.
async fn resumable_upload_step_from_response(
    response: reqwest::Response,
) -> Result<GcsResumableUploadStep, GcsRequestAttemptError> {
    let status = response.status();
    if status.is_success() {
        let response =
            response.json::<GcsObjectResponse>().await.map_err(GcsRequestAttemptError::Response)?;
        return Ok(GcsResumableUploadStep::Done(response));
    }

    if status == StatusCode::PERMANENT_REDIRECT {
        let next_offset = resumable_next_offset(response.headers())?;
        return Ok(GcsResumableUploadStep::Incomplete { next_offset });
    }

    Err(GcsRequestAttemptError::Status(status))
}

/// Uploads one chunk, probing session status after uncertain failures.
async fn upload_resumable_chunk_with_status_check(
    client: &Client,
    session_url: &str,
    auth: &GcsAuth,
    offset: u64,
    size_bytes: Option<u64>,
    bytes: Bytes,
) -> Result<GcsResumableUploadStep, GcsRequestAttemptError> {
    let content_range = resumable_content_range(offset, bytes.len() as u64, size_bytes);
    let token = auth.access_token().await.map_err(GcsRequestAttemptError::Auth)?;

    match upload_resumable_chunk(client, session_url, &token, bytes, &content_range).await {
        Ok(step) => Ok(step),
        Err(error) if should_check_resumable_upload_status(&error) => {
            let token = auth.access_token().await.map_err(GcsRequestAttemptError::Auth)?;
            match check_resumable_upload_status(client, session_url, &token, size_bytes).await {
                Ok(GcsResumableUploadStep::Done(response)) => {
                    Ok(GcsResumableUploadStep::Done(response))
                }
                Ok(GcsResumableUploadStep::Incomplete { next_offset }) if next_offset > offset => {
                    Ok(GcsResumableUploadStep::Incomplete { next_offset })
                }
                Ok(GcsResumableUploadStep::Incomplete { next_offset }) if next_offset < offset => {
                    Err(GcsRequestAttemptError::Protocol(format!(
                        "Resumable upload status moved backwards from offset {offset} to \
                         {next_offset}."
                    )))
                }
                Ok(GcsResumableUploadStep::Incomplete { .. }) => Err(error),
                Err(status_error)
                    if gcs_request_retry_decision(&status_error) == RetryDecision::Stop =>
                {
                    Err(status_error)
                }
                Err(_) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Uploads one chunk to a resumable upload session.
async fn upload_resumable_chunk(
    client: &Client,
    session_url: &str,
    token: &str,
    bytes: Bytes,
    content_range: &str,
) -> Result<GcsResumableUploadStep, GcsRequestAttemptError> {
    let response = client
        .put(session_url)
        .bearer_auth(token)
        .header(CONTENT_LENGTH, bytes.len() as u64)
        .header("Content-Range", content_range)
        .body(Body::from(bytes))
        .send()
        .await
        .map_err(GcsRequestAttemptError::Request)?;

    resumable_upload_step_from_response(response).await
}

/// Reads current persisted progress for a resumable upload session.
async fn check_resumable_upload_status(
    client: &Client,
    session_url: &str,
    token: &str,
    size_bytes: Option<u64>,
) -> Result<GcsResumableUploadStep, GcsRequestAttemptError> {
    let response = client
        .put(session_url)
        .bearer_auth(token)
        .header(CONTENT_LENGTH, 0)
        .header("Content-Range", resumable_status_content_range(size_bytes))
        .send()
        .await
        .map_err(GcsRequestAttemptError::Request)?;

    resumable_upload_step_from_response(response).await
}

/// Builds the `Content-Range` value for one resumable upload request.
fn resumable_content_range(offset: u64, chunk_size: u64, total_size: Option<u64>) -> String {
    if chunk_size == 0 {
        resumable_status_content_range(total_size)
    } else {
        let end = offset + chunk_size - 1;
        match total_size {
            Some(total_size) => format!("bytes {offset}-{end}/{total_size}"),
            None => format!("bytes {offset}-{end}/*"),
        }
    }
}

/// Builds the `Content-Range` value for a resumable-upload status check.
fn resumable_status_content_range(total_size: Option<u64>) -> String {
    match total_size {
        Some(total_size) => format!("bytes */{total_size}"),
        None => "bytes */*".to_owned(),
    }
}

/// Returns the next offset from a GCS resumable-upload `Range` header.
fn resumable_next_offset(
    headers: &reqwest::header::HeaderMap,
) -> Result<u64, GcsRequestAttemptError> {
    let Some(range) = headers.get(RANGE) else {
        return Ok(0);
    };
    let range = range.to_str().map_err(|_| {
        GcsRequestAttemptError::Protocol("Invalid resumable upload range header.".to_owned())
    })?;
    let Some(end) = range.strip_prefix("bytes=0-") else {
        return Err(GcsRequestAttemptError::Protocol(format!(
            "Unexpected resumable upload range header: {range}."
        )));
    };
    let end = end.parse::<u64>().map_err(|_| {
        GcsRequestAttemptError::Protocol(format!(
            "Invalid resumable upload range end offset: {range}."
        ))
    })?;

    Ok(end + 1)
}

/// Returns whether a GCS HTTP status is transient.
fn is_retryable_gcs_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::INTERNAL_SERVER_ERROR
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
}

/// Logs a transient GCS request retry.
fn log_gcs_request_retry(attempt: RetryAttempt<'_, GcsRequestAttemptError>) {
    warn!(
        retry_index = attempt.retry_index,
        max_retries = attempt.max_retries,
        retry_delay_ms = attempt.sleep_delay.as_millis() as u64,
        error = %attempt.error,
        "retrying transient gcs request error"
    );
}

/// Builds an ETL error for GCS response deserialization failures.
fn gcs_response_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::DeserializationError,
        "Failed to deserialize GCS upload response",
        source: error
    )
}

/// Builds an ETL error for ADC credential parsing failures.
fn gcs_adc_error(error: impl std::error::Error + Send + Sync + 'static) -> etl::error::EtlError {
    etl_error!(
        ErrorKind::DestinationAuthenticationError,
        "Failed to parse GCS application default credentials",
        source: error
    )
}

/// Builds an ETL error for unsuccessful GCS upload responses.
fn gcs_status_error(status: StatusCode, bucket: &str, object_name: &str) -> etl::error::EtlError {
    let (kind, description) = match status {
        StatusCode::UNAUTHORIZED => {
            (ErrorKind::DestinationAuthenticationError, "GCS request authentication failed")
        }
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, "GCS request permission denied"),
        _ => (ErrorKind::DestinationQueryFailed, "GCS request failed"),
    };

    etl_error!(
        kind,
        description,
        format!(
            "GCS request for bucket '{bucket}' and object '{object_name}' returned HTTP status {}.",
            status.as_u16()
        )
    )
}

/// Minimal object metadata sent when starting a resumable upload session.
#[derive(Debug, Serialize)]
struct GcsObjectUploadMetadata<'a> {
    /// Object name.
    name: &'a str,
}

/// Minimal GCS object response returned by uploads.
#[derive(Debug, Deserialize)]
struct GcsObjectResponse {
    /// Bucket containing the object.
    bucket: Option<String>,
    /// Object name.
    name: Option<String>,
    /// Object size in bytes, encoded as a decimal string.
    size: Option<String>,
}

/// Minimal credential type discriminator used for ADC authorized-user support.
#[derive(Debug, Deserialize)]
struct CredentialType {
    /// Credential type.
    #[serde(rename = "type")]
    credential_type: String,
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use etl::error::ErrorKind;
    use reqwest::{Client, StatusCode};
    use tokio::{runtime::Handle, sync::OnceCell};

    use super::{
        GCS_RESUMABLE_UPLOAD_CHUNK_SIZE, GcsAuth, GcsRequestAttemptError, GcsStreamingUploadWriter,
        encode_uri_path_part, gcs_request_retry_decision, gcs_status_error,
        is_retryable_gcs_status, resumable_content_range, resumable_next_offset,
        resumable_status_content_range, should_check_resumable_upload_status,
        validate_object_location,
    };
    use crate::retry::RetryDecision;

    #[test]
    fn validate_object_location_rejects_invalid_bucket() {
        let err = validate_object_location("bad/bucket", "object.avro").unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn validate_object_location_rejects_absolute_object_name() {
        let err = validate_object_location("bucket", "/object.avro").unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn gcs_status_error_classifies_auth_and_permission_failures() {
        let auth_error = gcs_status_error(StatusCode::UNAUTHORIZED, "bucket", "object.avro");
        let permission_error = gcs_status_error(StatusCode::FORBIDDEN, "bucket", "object.avro");
        let query_error = gcs_status_error(StatusCode::BAD_REQUEST, "bucket", "object.avro");

        assert_eq!(auth_error.kind(), ErrorKind::DestinationAuthenticationError);
        assert_eq!(permission_error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(query_error.kind(), ErrorKind::DestinationQueryFailed);
    }

    #[test]
    fn retryable_gcs_statuses_include_transient_failures() {
        assert!(is_retryable_gcs_status(StatusCode::REQUEST_TIMEOUT));
        assert!(is_retryable_gcs_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_gcs_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_gcs_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_gcs_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retryable_gcs_status(StatusCode::GATEWAY_TIMEOUT));

        assert!(!is_retryable_gcs_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_gcs_status(StatusCode::FORBIDDEN));
    }

    #[test]
    fn gcs_retry_decision_retries_transient_statuses_only() {
        let retry = GcsRequestAttemptError::Status(StatusCode::SERVICE_UNAVAILABLE);
        let stop = GcsRequestAttemptError::Status(StatusCode::BAD_REQUEST);

        assert_eq!(gcs_request_retry_decision(&retry), RetryDecision::Retry);
        assert_eq!(gcs_request_retry_decision(&stop), RetryDecision::Stop);
    }

    #[test]
    fn resumable_upload_status_check_is_used_for_uncertain_failures() {
        let retryable_status = GcsRequestAttemptError::Status(StatusCode::SERVICE_UNAVAILABLE);
        let permanent_status = GcsRequestAttemptError::Status(StatusCode::BAD_REQUEST);
        let protocol_error = GcsRequestAttemptError::Protocol("bad range".to_owned());

        assert!(should_check_resumable_upload_status(&retryable_status));
        assert!(!should_check_resumable_upload_status(&permanent_status));
        assert!(!should_check_resumable_upload_status(&protocol_error));
    }

    #[tokio::test]
    async fn streaming_writer_keeps_exact_chunk_for_final_upload() {
        let auth =
            GcsAuth::ApplicationDefaultCredentials { authenticator: Arc::new(OnceCell::new()) };
        let mut writer = GcsStreamingUploadWriter::new(
            Client::new(),
            Handle::current(),
            auth,
            "bucket".to_owned(),
            "object.avro".to_owned(),
            "http://127.0.0.1/upload".to_owned(),
        );
        let bytes = vec![0_u8; GCS_RESUMABLE_UPLOAD_CHUNK_SIZE as usize];

        writer.write_all(&bytes).unwrap();

        assert_eq!(writer.offset, 0);
        assert_eq!(writer.buffer.len(), GCS_RESUMABLE_UPLOAD_CHUNK_SIZE as usize);
    }

    #[test]
    fn encode_uri_path_part_encodes_object_path_separators() {
        assert_eq!(
            encode_uri_path_part("etl/bigquery/initial-copy/file 1.avro"),
            "etl%2Fbigquery%2Finitial-copy%2Ffile%201.avro"
        );
    }

    #[test]
    fn resumable_content_range_uses_known_total_size() {
        assert_eq!(resumable_content_range(0, 32, Some(100)), "bytes 0-31/100");
        assert_eq!(resumable_content_range(64, 36, Some(100)), "bytes 64-99/100");
        assert_eq!(resumable_content_range(0, 0, Some(0)), "bytes */0");
    }

    #[test]
    fn resumable_content_range_supports_unknown_total_size() {
        assert_eq!(resumable_content_range(0, 32, None), "bytes 0-31/*");
        assert_eq!(resumable_status_content_range(None), "bytes */*");
    }

    #[test]
    fn resumable_next_offset_parses_range_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RANGE, "bytes=0-33554431".parse().unwrap());

        assert_eq!(resumable_next_offset(&headers).unwrap(), 33_554_432);
    }

    #[test]
    fn resumable_next_offset_rejects_unexpected_range_header() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RANGE, "bytes=10-20".parse().unwrap());

        assert!(matches!(
            resumable_next_offset(&headers),
            Err(GcsRequestAttemptError::Protocol(_))
        ));
    }
}
