//! BigQuery initial-copy load-job primitives.

pub(crate) mod avro;
pub mod gcs;

use std::future::Future;

use etl::{
    data::TableRow,
    error::{ErrorKind, EtlResult},
    etl_error,
    pipeline::PipelineId,
    schema::TableName,
};
use rand::random;

use crate::bigquery::{BigQueryDatasetId, BigQueryTableId};

/// Fixed GCS prefix used until the destination exposes staging configuration.
pub const DEFAULT_GCS_PREFIX: &str = "supabase-etl/initial-copy";
/// Fixed staging table prefix used for snapshot load jobs.
pub const DEFAULT_STAGING_TABLE_PREFIX: &str = "_snapshot_";
/// Fixed Avro logical type behavior for BigQuery load jobs.
pub const DEFAULT_USE_AVRO_LOGICAL_TYPES: bool = true;
/// Fixed decimal target type preference for BigQuery load jobs.
pub const DEFAULT_DECIMAL_TARGET_TYPES: &[&str] = &["NUMERIC", "BIGNUMERIC", "STRING"];
/// Fixed BigQuery load-job create disposition.
pub const DEFAULT_CREATE_DISPOSITION: &str = "CREATE_IF_NEEDED";
/// Fixed BigQuery load-job write disposition for staging tables.
pub const DEFAULT_WRITE_DISPOSITION: &str = "WRITE_TRUNCATE";
/// Avro file extension used in staged object names.
const AVRO_FILE_EXTENSION: &str = "avro";
/// Parquet file extension used in staged object names.
const PARQUET_FILE_EXTENSION: &str = "parquet";
/// Avro upload content type used for staged files.
const AVRO_CONTENT_TYPE: &str = "application/avro";
/// Parquet upload content type used for staged files.
const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";
/// BigQuery load-job source format for Avro.
const BIGQUERY_AVRO_SOURCE_FORMAT: &str = "AVRO";
/// BigQuery load-job source format for Parquet.
const BIGQUERY_PARQUET_SOURCE_FORMAT: &str = "PARQUET";

/// Snapshot file format for BigQuery initial-copy files.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum SnapshotFormat {
    /// Avro object container files.
    #[default]
    Avro,
    /// Parquet files.
    Parquet,
}

impl SnapshotFormat {
    /// Returns the file extension used in object names.
    pub fn file_extension(self) -> &'static str {
        match self {
            Self::Avro => AVRO_FILE_EXTENSION,
            Self::Parquet => PARQUET_FILE_EXTENSION,
        }
    }

    /// Returns the upload content type used for staged files.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Avro => AVRO_CONTENT_TYPE,
            Self::Parquet => PARQUET_CONTENT_TYPE,
        }
    }
}

/// Upload body produced by a BigQuery initial-copy encoder.
#[derive(Clone)]
pub enum UploadBody {
    /// In-memory bytes for small files, tests, and mocks.
    Bytes(Vec<u8>),
    /// Local file path for bounded temp-file based uploads.
    File(std::path::PathBuf),
}

/// One batch of initial-copy rows passed to a file encoder.
pub struct SnapshotBatch {
    /// Rows in source table column order.
    pub rows: Vec<TableRow>,
}

/// Generates an opaque run id for one BigQuery initial-copy attempt.
pub fn generate_random_run_id() -> String {
    format!("{:016x}", random::<u64>())
}

/// Request body for uploading one staged snapshot object to GCS.
#[derive(Clone)]
pub struct GcsUploadRequest {
    /// GCS bucket name.
    pub bucket: String,
    /// Object name within the bucket.
    pub object_name: String,
    /// MIME content type for the upload.
    pub content_type: String,
    /// Encoded snapshot bytes or a local file reference.
    pub body: UploadBody,
}

/// Request body for streaming one staged snapshot object to GCS.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GcsStreamingUploadRequest {
    /// GCS bucket name.
    pub bucket: String,
    /// Object name within the bucket.
    pub object_name: String,
    /// MIME content type for the upload.
    pub content_type: String,
}

/// Metadata returned after uploading a staged snapshot object.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GcsObjectMetadata {
    /// GCS bucket name.
    pub bucket: String,
    /// Object name within the bucket.
    pub object_name: String,
    /// Canonical `gs://` URI.
    pub uri: String,
    /// Number of uploaded bytes when known.
    pub size_bytes: Option<u64>,
}

/// Request body for deleting one staged snapshot object from GCS.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GcsDeleteRequest {
    /// GCS bucket name.
    pub bucket: String,
    /// Object name within the bucket.
    pub object_name: String,
}

/// Uploads staged snapshot files to GCS.
pub trait GcsUploader {
    /// Uploads one object and returns metadata for the stored object.
    fn upload_object(
        &self,
        request: GcsUploadRequest,
    ) -> impl Future<Output = EtlResult<GcsObjectMetadata>> + Send;

    /// Deletes one staged object.
    fn delete_object(
        &self,
        request: GcsDeleteRequest,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}

/// BigQuery load-job request for staged initial-copy files.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BigQueryLoadJobRequest {
    /// Deterministic job id.
    pub job_id: String,
    /// Optional BigQuery job location.
    pub location: Option<String>,
    /// Dataset containing the destination staging table.
    pub dataset_id: BigQueryDatasetId,
    /// Destination staging table id.
    pub destination_table_id: BigQueryTableId,
    /// Source GCS URIs.
    pub source_uris: Vec<String>,
    /// Snapshot file format.
    pub source_format: SnapshotFormat,
    /// BigQuery create disposition.
    pub create_disposition: String,
    /// BigQuery write disposition.
    pub write_disposition: String,
    /// Whether Avro logical types should be interpreted by BigQuery.
    pub use_avro_logical_types: bool,
    /// Decimal target type preference.
    pub decimal_target_types: Vec<String>,
}

impl BigQueryLoadJobRequest {
    /// Creates a load-job request with fixed initial-copy defaults.
    pub fn new(
        job_id: String,
        dataset_id: BigQueryDatasetId,
        destination_table_id: BigQueryTableId,
        source_uris: Vec<String>,
        source_format: SnapshotFormat,
    ) -> EtlResult<Self> {
        if source_uris.is_empty() {
            return Err(etl_error!(
                ErrorKind::InvalidData,
                "BigQuery load job requires at least one source URI"
            ));
        }

        for source_uri in &source_uris {
            validate_gcs_load_uri(source_uri)?;
        }

        Ok(Self {
            job_id,
            location: None,
            dataset_id,
            destination_table_id,
            source_uris,
            source_format,
            create_disposition: DEFAULT_CREATE_DISPOSITION.to_owned(),
            write_disposition: DEFAULT_WRITE_DISPOSITION.to_owned(),
            use_avro_logical_types: DEFAULT_USE_AVRO_LOGICAL_TYPES,
            decimal_target_types: DEFAULT_DECIMAL_TARGET_TYPES
                .iter()
                .map(|typ| (*typ).to_owned())
                .collect(),
        })
    }
}

/// Returns the BigQuery load-job source format for a snapshot file format.
pub fn bigquery_source_format(format: SnapshotFormat) -> &'static str {
    match format {
        SnapshotFormat::Avro => BIGQUERY_AVRO_SOURCE_FORMAT,
        SnapshotFormat::Parquet => BIGQUERY_PARQUET_SOURCE_FORMAT,
    }
}

/// Reference to a submitted BigQuery load job.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BigQueryLoadJobRef {
    /// BigQuery project id.
    pub project_id: String,
    /// BigQuery job id.
    pub job_id: String,
    /// BigQuery job location when returned by BigQuery.
    pub location: Option<String>,
}

/// Status for a BigQuery load job.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BigQueryLoadJobStatus {
    /// BigQuery job state.
    pub state: Option<String>,
    /// Terminal error result, if BigQuery marked the job as failed.
    pub error_result: Option<String>,
    /// Additional BigQuery errors.
    pub errors: Vec<String>,
}

impl BigQueryLoadJobStatus {
    /// Returns whether BigQuery reports the job as done.
    pub fn done(&self) -> bool {
        self.state.as_deref() == Some("DONE")
    }

    /// Converts a terminal status into success or a destination error.
    pub fn ensure_done_success(&self) -> EtlResult<()> {
        if !self.done() {
            return Err(etl_error!(
                ErrorKind::DestinationQueryFailed,
                "BigQuery load job is not done",
                format!("Current load job state: {:?}", self.state)
            ));
        }

        if let Some(error_result) = &self.error_result {
            let detail = if self.errors.is_empty() {
                error_result.clone()
            } else {
                format!("{error_result}; errors={}", self.errors.join("; "))
            };

            return Err(etl_error!(
                ErrorKind::DestinationQueryFailed,
                "BigQuery load job failed",
                detail
            ));
        }

        Ok(())
    }
}

/// Submits and reads BigQuery load jobs.
pub trait BigQueryJobClient {
    /// Inserts a BigQuery load job.
    fn insert_load_job(
        &self,
        request: BigQueryLoadJobRequest,
    ) -> impl Future<Output = EtlResult<BigQueryLoadJobRef>> + Send;

    /// Reads the current BigQuery load-job status.
    fn get_load_job(
        &self,
        job_ref: &BigQueryLoadJobRef,
    ) -> impl Future<Output = EtlResult<BigQueryLoadJobStatus>> + Send;
}

/// Builds a deterministic GCS object name for one staged snapshot file.
pub fn gcs_object_name(
    prefix: &str,
    connection_id: PipelineId,
    table_name: &TableName,
    run_id: &str,
    partition_id: usize,
    file_index: usize,
    format: SnapshotFormat,
) -> String {
    let prefix = prefix.trim_matches('/');
    let table_component =
        sanitize_path_component(&format!("{}.{}", table_name.schema, table_name.name));
    let run_id = sanitize_path_component(run_id);
    let object_name = format!(
        "{connection_id}/{table_component}/{run_id}/part-{partition_id}-{file_index}.{extension}",
        extension = format.file_extension()
    );

    if prefix.is_empty() { object_name } else { format!("{prefix}/{object_name}") }
}

/// Builds a canonical `gs://` URI.
pub fn gcs_uri(bucket: &str, object_name: &str) -> String {
    format!("gs://{}/{}", bucket.trim_matches('/'), object_name.trim_start_matches('/'))
}

/// Builds a deterministic staging table id.
pub fn staging_table_id(base_table_id: &str, run_id: &str) -> BigQueryTableId {
    format!(
        "{}{}_{}",
        DEFAULT_STAGING_TABLE_PREFIX,
        sanitize_bigquery_table_id_component(base_table_id),
        sanitize_bigquery_table_id_component(run_id)
    )
}

/// Builds a deterministic BigQuery load-job id.
pub fn load_job_id(
    connection_id: PipelineId,
    table_name: &TableName,
    run_id: &str,
    attempt: u32,
) -> String {
    let table_component =
        sanitize_bigquery_job_id_component(&format!("{}_{}", table_name.schema, table_name.name));
    let run_id = sanitize_bigquery_job_id_component(run_id);

    format!("etl_load_{connection_id}_{table_component}_{run_id}_{attempt}")
}

/// Replaces path separators and control characters in a GCS path component.
fn sanitize_path_component(component: &str) -> String {
    component
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | '\0'..='\u{1f}' => '_',
            _ => ch,
        })
        .collect()
}

/// Validates a BigQuery load-job GCS URI.
fn validate_gcs_load_uri(source_uri: &str) -> EtlResult<()> {
    let Some(path) = source_uri.strip_prefix("gs://") else {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "BigQuery load job source URI must use the gs:// scheme",
            format!("Source URI '{source_uri}' does not start with 'gs://'.")
        ));
    };

    if path.contains('#') {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "BigQuery load job source URI must not include a GCS generation number",
            format!("Source URI '{source_uri}' contains '#'.")
        ));
    }

    let Some((bucket, object_name)) = path.split_once('/') else {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "BigQuery load job source URI must include a bucket and object name",
            format!("Source URI '{source_uri}' is missing an object name.")
        ));
    };

    if bucket.is_empty() || object_name.is_empty() {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "BigQuery load job source URI must include a bucket and object name",
            format!("Source URI '{source_uri}' is not a complete GCS object URI.")
        ));
    }

    Ok(())
}

/// Replaces characters that are invalid in BigQuery table ids.
fn sanitize_bigquery_table_id_component(identifier: &str) -> String {
    sanitize_bigquery_identifier_component(identifier, |ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// Replaces characters that are invalid in BigQuery job ids.
fn sanitize_bigquery_job_id_component(identifier: &str) -> String {
    sanitize_bigquery_identifier_component(identifier, |ch| {
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'
    })
}

/// Replaces characters that are invalid for a specific BigQuery identifier.
fn sanitize_bigquery_identifier_component(
    identifier: &str,
    allowed: impl Fn(char) -> bool,
) -> String {
    let mut sanitized = String::with_capacity(identifier.len());

    for ch in identifier.chars() {
        if allowed(ch) {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized.is_empty() { "_".to_owned() } else { sanitized }
}

impl BigQueryJobClient for crate::bigquery::BigQueryClient {
    fn insert_load_job(
        &self,
        request: BigQueryLoadJobRequest,
    ) -> impl Future<Output = EtlResult<BigQueryLoadJobRef>> + Send {
        crate::bigquery::BigQueryClient::insert_load_job(self, request)
    }

    fn get_load_job(
        &self,
        job_ref: &BigQueryLoadJobRef,
    ) -> impl Future<Output = EtlResult<BigQueryLoadJobStatus>> + Send {
        crate::bigquery::BigQueryClient::get_load_job(self, job_ref)
    }
}

impl GcsUploader for crate::bigquery::BigQueryClient {
    fn upload_object(
        &self,
        request: GcsUploadRequest,
    ) -> impl Future<Output = EtlResult<GcsObjectMetadata>> + Send {
        self.upload_gcs_object(request)
    }

    fn delete_object(
        &self,
        request: GcsDeleteRequest,
    ) -> impl Future<Output = EtlResult<()>> + Send {
        self.delete_gcs_object(request)
    }
}

#[cfg(test)]
mod tests {
    use etl::{pipeline::PipelineId, schema::TableName};

    use super::{
        BigQueryLoadJobRequest, BigQueryLoadJobStatus, DEFAULT_DECIMAL_TARGET_TYPES,
        DEFAULT_GCS_PREFIX, bigquery_source_format, gcs_object_name, gcs_uri,
        generate_random_run_id, load_job_id, staging_table_id,
    };
    use crate::bigquery::initial_copy::SnapshotFormat;

    #[test]
    fn gcs_object_name_uses_stable_layout() {
        let table_name = TableName::new("public".to_owned(), "users".to_owned());
        let connection_id: PipelineId = 42;

        let object_name = gcs_object_name(
            DEFAULT_GCS_PREFIX,
            connection_id,
            &table_name,
            "run-01",
            3,
            7,
            SnapshotFormat::Avro,
        );

        assert_eq!(object_name, "etl/bigquery/initial-copy/42/public.users/run-01/part-3-7.avro");
        assert_eq!(
            gcs_uri("bucket-name", &object_name),
            "gs://bucket-name/etl/bigquery/initial-copy/42/public.users/run-01/part-3-7.avro"
        );
    }

    #[test]
    fn load_job_id_sanitizes_table_and_run_components() {
        let table_name = TableName::new("tenant/schema".to_owned(), "orders.v2".to_owned());
        let connection_id: PipelineId = 9;

        let job_id = load_job_id(connection_id, &table_name, "run:2026-06-29", 2);

        assert_eq!(job_id, "etl_load_9_tenant_schema_orders_v2_run_2026-06-29_2");
    }

    #[test]
    fn staging_table_id_uses_fixed_prefix() {
        assert_eq!(
            staging_table_id("public_users_0", "run:2026-06-29"),
            "_snapshot_public_users_0_run_2026_06_29"
        );
    }

    #[test]
    fn generate_random_run_id_uses_hex_component() {
        let run_id = generate_random_run_id();

        assert_eq!(run_id.len(), 16);
        assert!(run_id.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn bigquery_source_format_maps_snapshot_formats() {
        assert_eq!(bigquery_source_format(SnapshotFormat::Avro), "AVRO");
        assert_eq!(bigquery_source_format(SnapshotFormat::Parquet), "PARQUET");
    }

    #[test]
    fn load_job_request_uses_fixed_defaults() {
        let request = BigQueryLoadJobRequest::new(
            "job".to_owned(),
            "dataset".to_owned(),
            "table".to_owned(),
            vec!["gs://bucket/object.avro".to_owned()],
            SnapshotFormat::Avro,
        )
        .unwrap();

        assert_eq!(request.location, None);
        assert_eq!(request.create_disposition, "CREATE_IF_NEEDED");
        assert_eq!(request.write_disposition, "WRITE_TRUNCATE");
        assert!(request.use_avro_logical_types);
        assert_eq!(
            request.decimal_target_types,
            DEFAULT_DECIMAL_TARGET_TYPES.iter().map(|typ| (*typ).to_owned()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn load_job_request_rejects_non_gcs_uri() {
        let err = BigQueryLoadJobRequest::new(
            "job".to_owned(),
            "dataset".to_owned(),
            "table".to_owned(),
            vec!["https://example.com/object.avro".to_owned()],
            SnapshotFormat::Avro,
        )
        .unwrap_err();

        assert_eq!(err.kind(), etl::error::ErrorKind::InvalidData);
    }

    #[test]
    fn load_job_request_rejects_incomplete_gcs_uri() {
        let err = BigQueryLoadJobRequest::new(
            "job".to_owned(),
            "dataset".to_owned(),
            "table".to_owned(),
            vec!["gs://bucket".to_owned()],
            SnapshotFormat::Avro,
        )
        .unwrap_err();

        assert_eq!(err.kind(), etl::error::ErrorKind::InvalidData);
    }

    #[test]
    fn load_job_request_rejects_gcs_generation_uri() {
        let err = BigQueryLoadJobRequest::new(
            "job".to_owned(),
            "dataset".to_owned(),
            "table".to_owned(),
            vec!["gs://bucket/object.avro#123".to_owned()],
            SnapshotFormat::Avro,
        )
        .unwrap_err();

        assert_eq!(err.kind(), etl::error::ErrorKind::InvalidData);
    }

    #[test]
    fn done_status_with_error_result_is_failure() {
        let status = BigQueryLoadJobStatus {
            state: Some("DONE".to_owned()),
            error_result: Some("invalid source URI".to_owned()),
            errors: vec!["source URI does not exist".to_owned()],
        };

        let err = status.ensure_done_success().unwrap_err();

        assert_eq!(err.kind(), etl::error::ErrorKind::DestinationQueryFailed);
    }

    #[test]
    fn done_status_without_error_result_is_success() {
        let status = BigQueryLoadJobStatus {
            state: Some("DONE".to_owned()),
            error_result: None,
            errors: vec![],
        };

        status.ensure_done_success().unwrap();
    }
}
