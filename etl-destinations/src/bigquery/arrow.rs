use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
};
use arrow::record_batch::RecordBatch;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::{ChangeArrowBatch, TableArrowBatch, TableSchema, Type, is_array_type};
use gcp_bigquery_client::auth::Authenticator;
use gcp_bigquery_client::error::BQError;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::{
    AppendRowsResponse, RowError, append_rows_response,
};
use prost::Message;
use tokio::time::sleep;
use tonic::codegen::*;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, Request, Status};
use uuid::Uuid;

const BIGQUERY_STORAGE_API_URL: &str = "https://bigquerystorage.googleapis.com";
const BIGQUERY_STORAGE_API_DOMAIN: &str = "bigquerystorage.googleapis.com";
const MAX_MESSAGE_SIZE_BYTES: usize = 20 * 1024 * 1024;
const MAX_APPEND_RETRY_ATTEMPTS: u32 = 10;
const INITIAL_APPEND_RETRY_BACKOFF_MS: u64 = 500;
const MAX_APPEND_RETRY_BACKOFF_MS: u64 = 60_000;

pub(super) const ARROW_FALLBACK_ERROR_MARKER: &str = "bigquery arrow fallback";

#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub(super) struct Int64Value {
    #[prost(int64, tag = "1")]
    pub value: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(super) struct ArrowSchemaPayload {
    #[prost(bytes = "vec", tag = "1")]
    pub serialized_schema: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub(super) struct ArrowRecordBatchPayload {
    #[prost(bytes = "vec", tag = "1")]
    pub serialized_record_batch: Vec<u8>,
    #[deprecated]
    #[prost(int64, tag = "2")]
    pub row_count: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(super) struct ArrowAppendRowsRequest {
    #[prost(string, tag = "1")]
    pub write_stream: String,
    #[prost(message, optional, tag = "2")]
    pub offset: Option<Int64Value>,
    #[prost(string, tag = "6")]
    pub trace_id: String,
    #[prost(map = "string, int32", tag = "7")]
    pub missing_value_interpretations: HashMap<String, i32>,
    #[prost(int32, tag = "8")]
    pub default_missing_value_interpretation: i32,
    #[prost(oneof = "arrow_append_rows_request::Rows", tags = "5")]
    pub rows: Option<arrow_append_rows_request::Rows>,
}

pub(super) mod arrow_append_rows_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ArrowData {
        #[prost(message, optional, tag = "1")]
        pub writer_schema: Option<super::ArrowSchemaPayload>,
        #[prost(message, optional, tag = "2")]
        pub rows: Option<super::ArrowRecordBatchPayload>,
    }

    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Rows {
        #[prost(message, tag = "5")]
        ArrowRows(ArrowData),
    }
}

#[derive(Debug, Clone)]
pub(super) struct PreparedBigQueryArrowBatch {
    pub batch: RecordBatch,
}

pub(super) fn prepare_snapshot_arrow_batch(
    batch: &TableArrowBatch,
) -> EtlResult<Option<PreparedBigQueryArrowBatch>> {
    prepare_bigquery_arrow_batch(
        &batch.batch,
        &batch.table_schema,
        Some(string_array(batch.batch.num_rows(), "UPSERT")),
        None,
    )
}

pub(super) fn prepare_stream_arrow_batch(
    group: &ChangeArrowBatch,
) -> EtlResult<Option<PreparedBigQueryArrowBatch>> {
    let change_type = match group.change {
        etl::types::ChangeKind::Delete => "DELETE",
        etl::types::ChangeKind::Insert | etl::types::ChangeKind::Update => "UPSERT",
    };
    let sequence_keys = group
        .commit_lsns
        .values()
        .iter()
        .zip(group.tx_ordinals.values().iter())
        .map(|(commit_lsn, tx_ordinal)| format!("{commit_lsn:016x}/{tx_ordinal:016x}"))
        .collect::<Vec<_>>();

    prepare_bigquery_arrow_batch(
        &group.rows.batch,
        &group.rows.table_schema,
        Some(string_array(group.rows.batch.num_rows(), change_type)),
        Some(Arc::new(StringArray::from(sequence_keys)) as ArrayRef),
    )
}

fn prepare_bigquery_arrow_batch(
    batch: &RecordBatch,
    table_schema: &TableSchema,
    change_type: Option<ArrayRef>,
    sequence_keys: Option<ArrayRef>,
) -> EtlResult<Option<PreparedBigQueryArrowBatch>> {
    let Some((fields, columns)) = prepare_bigquery_arrow_columns(batch, table_schema)? else {
        return Ok(None);
    };

    let mut fields = fields;
    let mut columns = columns;

    if let Some(change_type) = change_type {
        fields.push(Field::new("_CHANGE_TYPE", DataType::Utf8, false));
        columns.push(change_type);
    }

    if let Some(sequence_keys) = sequence_keys {
        fields.push(Field::new("_CHANGE_SEQUENCE_NUMBER", DataType::Utf8, false));
        columns.push(sequence_keys);
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(arrow_error_to_etl_error)?;

    Ok(Some(PreparedBigQueryArrowBatch { batch }))
}

fn prepare_bigquery_arrow_columns(
    batch: &RecordBatch,
    table_schema: &TableSchema,
) -> EtlResult<Option<(Vec<Field>, Vec<ArrayRef>)>> {
    if table_schema.column_schemas.len() != batch.num_columns() {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "bigquery arrow batch schema mismatch",
            format!(
                "expected {} columns, got {}",
                table_schema.column_schemas.len(),
                batch.num_columns()
            )
        ));
    }

    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());

    for (column_schema, column) in table_schema.column_schemas.iter().zip(batch.columns()) {
        let Some((field, column)) =
            prepare_bigquery_arrow_column(column_schema.name.as_str(), &column_schema.typ, column)?
        else {
            return Ok(None);
        };
        fields.push(field);
        columns.push(column);
    }

    Ok(Some((fields, columns)))
}

fn prepare_bigquery_arrow_column(
    column_name: &str,
    typ: &Type,
    column: &ArrayRef,
) -> EtlResult<Option<(Field, ArrayRef)>> {
    if is_array_type(typ) {
        return Ok(None);
    }

    let (data_type, nullable, column) = match typ {
        &Type::BOOL => (DataType::Boolean, true, Arc::clone(column)),
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
            (DataType::Utf8, true, Arc::clone(column))
        }
        &Type::INT2 | &Type::INT4 => (
            DataType::Int64,
            true,
            cast(column, &DataType::Int64).map_err(arrow_error_to_etl_error)?,
        ),
        &Type::INT8 | &Type::OID => (DataType::Int64, true, Arc::clone(column)),
        &Type::FLOAT4 => (
            DataType::Float64,
            true,
            cast(column, &DataType::Float64).map_err(arrow_error_to_etl_error)?,
        ),
        &Type::FLOAT8 => (DataType::Float64, true, Arc::clone(column)),
        &Type::JSON | &Type::JSONB => (DataType::Utf8, true, Arc::clone(column)),
        &Type::DATE => (DataType::Date32, true, Arc::clone(column)),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => {
            let target_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
            (
                target_type.clone(),
                true,
                cast(column, &target_type).map_err(arrow_error_to_etl_error)?,
            )
        }
        &Type::UUID => (
            DataType::Utf8,
            true,
            convert_uuid_array(column).map_err(arrow_error_to_etl_error)?,
        ),
        &Type::BYTEA => (
            DataType::Binary,
            true,
            cast(column, &DataType::Binary).map_err(arrow_error_to_etl_error)?,
        ),
        &Type::NUMERIC | &Type::TIME => return Ok(None),
        _ => return Ok(None),
    };

    Ok(Some((Field::new(column_name, data_type, nullable), column)))
}

fn convert_uuid_array(column: &ArrayRef) -> Result<ArrayRef, ArrowErrorAdapter> {
    let values = column
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| ArrowErrorAdapter("expected uuid fixed-size binary array".to_string()))?;

    let mut builder = arrow::array::StringBuilder::new();
    for row_index in 0..values.len() {
        if values.is_null(row_index) {
            builder.append_null();
            continue;
        }

        let bytes = values.value(row_index);
        let uuid = Uuid::from_slice(bytes)
            .map_err(|error| ArrowErrorAdapter(format!("invalid uuid bytes: {error}")))?;
        builder.append_value(uuid.to_string());
    }

    Ok(Arc::new(builder.finish()))
}

fn string_array(row_count: usize, value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value; row_count]))
}

pub(super) fn build_arrow_append_requests(
    stream_name: &str,
    trace_id: &str,
    batches: &[PreparedBigQueryArrowBatch],
    max_request_size_bytes: usize,
) -> EtlResult<Vec<ArrowAppendRowsRequest>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = batches[0].batch.schema();
    let schema_bytes = serialize_schema(schema.as_ref())?;
    let mut requests = Vec::new();

    for (batch_index, batch) in batches.iter().enumerate() {
        let include_schema = batch_index == 0;
        requests.extend(split_batch_into_requests(
            stream_name,
            trace_id,
            &batch.batch,
            &schema_bytes,
            include_schema,
            max_request_size_bytes,
        )?);
    }

    Ok(requests)
}

fn split_batch_into_requests(
    stream_name: &str,
    trace_id: &str,
    batch: &RecordBatch,
    schema_bytes: &[u8],
    include_schema: bool,
    max_request_size_bytes: usize,
) -> EtlResult<Vec<ArrowAppendRowsRequest>> {
    let serialized_batch = serialize_record_batch(batch)?;
    let request = make_arrow_append_request(
        stream_name,
        trace_id,
        if include_schema {
            Some(schema_bytes)
        } else {
            None
        },
        serialized_batch,
        batch.num_rows(),
    );

    if request.encoded_len() <= max_request_size_bytes {
        return Ok(vec![request]);
    }

    if batch.num_rows() <= 1 {
        return Err(etl_error!(
            ErrorKind::InvalidData,
            "bigquery arrow request exceeds append size limit",
            format!(
                "a single-row arrow append request for stream {stream_name} exceeded {max_request_size_bytes} bytes"
            )
        ));
    }

    let midpoint = batch.num_rows() / 2;
    let left = batch.slice(0, midpoint);
    let right = batch.slice(midpoint, batch.num_rows() - midpoint);

    let mut requests = split_batch_into_requests(
        stream_name,
        trace_id,
        &left,
        schema_bytes,
        include_schema,
        max_request_size_bytes,
    )?;
    requests.extend(split_batch_into_requests(
        stream_name,
        trace_id,
        &right,
        schema_bytes,
        false,
        max_request_size_bytes,
    )?);

    Ok(requests)
}

fn make_arrow_append_request(
    stream_name: &str,
    trace_id: &str,
    schema_bytes: Option<&[u8]>,
    batch_bytes: Vec<u8>,
    row_count: usize,
) -> ArrowAppendRowsRequest {
    #[allow(deprecated)]
    let rows = ArrowRecordBatchPayload {
        serialized_record_batch: batch_bytes,
        row_count: row_count as i64,
    };

    ArrowAppendRowsRequest {
        write_stream: stream_name.to_string(),
        offset: None,
        trace_id: trace_id.to_string(),
        missing_value_interpretations: HashMap::new(),
        default_missing_value_interpretation: 0,
        rows: Some(arrow_append_rows_request::Rows::ArrowRows(
            arrow_append_rows_request::ArrowData {
                writer_schema: schema_bytes.map(|schema_bytes| ArrowSchemaPayload {
                    serialized_schema: schema_bytes.to_vec(),
                }),
                rows: Some(rows),
            },
        )),
    }
}

fn serialize_schema(schema: &Schema) -> EtlResult<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let encoded = generator.schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut dictionary_tracker,
        &options,
    );

    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, &options).map_err(arrow_error_to_etl_error)?;

    Ok(bytes)
}

fn serialize_record_batch(batch: &RecordBatch) -> EtlResult<Vec<u8>> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut compression_context = CompressionContext::default();
    let (encoded_dictionaries, encoded_batch) = generator
        .encode(
            batch,
            &mut dictionary_tracker,
            &options,
            &mut compression_context,
        )
        .map_err(arrow_error_to_etl_error)?;

    let mut bytes = Vec::new();
    for encoded_dictionary in encoded_dictionaries {
        write_message(&mut bytes, encoded_dictionary, &options)
            .map_err(arrow_error_to_etl_error)?;
    }
    write_message(&mut bytes, encoded_batch, &options).map_err(arrow_error_to_etl_error)?;

    Ok(bytes)
}

pub(super) fn append_rows_response_bytes(response: &AppendRowsResponse) -> usize {
    response.encoded_len()
}

pub(super) async fn append_arrow_requests(
    auth: Arc<dyn Authenticator>,
    requests: Vec<ArrowAppendRowsRequest>,
) -> Result<Vec<Result<AppendRowsResponse, Status>>, BQError> {
    if requests.is_empty() {
        return Ok(vec![]);
    }

    let mut last_error = None;

    for attempt in 0..MAX_APPEND_RETRY_ATTEMPTS {
        match append_arrow_requests_once(Arc::clone(&auth), requests.clone()).await {
            Ok(responses) => {
                let decision = classify_append_responses(&responses);
                if decision.should_retry && attempt + 1 < MAX_APPEND_RETRY_ATTEMPTS {
                    sleep(calculate_append_retry_backoff(attempt)).await;
                    continue;
                }

                return Ok(responses);
            }
            Err(error) => {
                let should_retry = matches!(&error, BQError::TonicStatusError(status) if is_retryable_append_status(status));
                last_error = Some(error);

                if should_retry && attempt + 1 < MAX_APPEND_RETRY_ATTEMPTS {
                    sleep(calculate_append_retry_backoff(attempt)).await;
                    continue;
                }

                break;
            }
        }
    }

    Err(last_error.expect("append attempt should record an error"))
}

async fn append_arrow_requests_once(
    auth: Arc<dyn Authenticator>,
    requests: Vec<ArrowAppendRowsRequest>,
) -> Result<Vec<Result<AppendRowsResponse, Status>>, BQError> {
    let bearer_token = format!("Bearer {}", auth.access_token().await?);
    let request = new_authorized_request(bearer_token, tokio_stream::iter(requests))?;
    let mut client = create_grpc_client().await?;
    let response = client.append_rows(request).await?;
    let mut stream = response.into_inner();
    let mut responses = Vec::new();

    while let Some(response) = stream.message().await? {
        responses.push(normalize_append_response(response));
    }

    Ok(responses)
}

fn normalize_append_response(response: AppendRowsResponse) -> Result<AppendRowsResponse, Status> {
    match response.response.as_ref() {
        Some(append_rows_response::Response::AppendResult(_)) => Ok(response),
        Some(append_rows_response::Response::Error(status)) => Err(Status::new(
            Code::from_i32(status.code),
            status.message.clone(),
        )),
        None => Err(Status::unavailable(
            "append response missing append_result/error outcome",
        )),
    }
}

fn calculate_append_retry_backoff(attempt: u32) -> Duration {
    let exponential = INITIAL_APPEND_RETRY_BACKOFF_MS
        .saturating_mul(1_u64 << attempt.min(10))
        .min(MAX_APPEND_RETRY_BACKOFF_MS);

    Duration::from_millis(exponential)
}

fn is_retryable_append_status(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable
        | Code::Internal
        | Code::Aborted
        | Code::Cancelled
        | Code::DeadlineExceeded
        | Code::ResourceExhausted => true,
        Code::Unknown => {
            let message = status.message().to_lowercase();
            message.contains("transport") || message.contains("connection")
        }
        _ => false,
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct AppendResponseDecision {
    pub should_retry: bool,
}

pub(super) fn classify_append_responses(
    responses: &[Result<AppendRowsResponse, Status>],
) -> AppendResponseDecision {
    let mut saw_retryable_error = false;
    let mut saw_non_retryable_error = false;

    for response in responses {
        let Err(status) = response else {
            continue;
        };

        if is_retryable_append_status(status) {
            saw_retryable_error = true;
        } else {
            saw_non_retryable_error = true;
        }
    }

    AppendResponseDecision {
        should_retry: saw_retryable_error && !saw_non_retryable_error,
    }
}

pub(super) fn should_disable_arrow_write(responses: &[Result<AppendRowsResponse, Status>]) -> bool {
    responses.iter().any(|response| match response {
        Ok(_) => false,
        Err(status) => matches!(
            status.code(),
            Code::InvalidArgument | Code::Unimplemented | Code::FailedPrecondition
        ),
    })
}

pub(super) fn arrow_response_statuses_to_etl_errors(
    responses: Vec<Result<AppendRowsResponse, Status>>,
) -> Vec<EtlError> {
    let mut errors = Vec::new();

    for response in responses {
        match response {
            Ok(response) => {
                if !response.row_errors.is_empty() {
                    for row_error in response.row_errors {
                        errors.push(row_error_to_etl_error(row_error));
                    }
                }
            }
            Err(status) => errors.push(etl_error!(
                ErrorKind::DestinationError,
                "bigquery arrow append failed",
                status.to_string()
            )),
        }
    }

    errors
}

pub(super) fn arrow_fallback_error(detail: impl Into<String>) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "bigquery arrow append failed",
        format!("{ARROW_FALLBACK_ERROR_MARKER}: {}", detail.into())
    )
}

fn row_error_to_etl_error(err: RowError) -> EtlError {
    etl_error!(
        ErrorKind::DestinationError,
        "bigquery row error",
        format!("{err:?}")
    )
}

fn arrow_error_to_etl_error(error: impl ToString) -> EtlError {
    etl_error!(
        ErrorKind::ConversionError,
        "bigquery arrow conversion failed",
        error.to_string()
    )
}

async fn create_grpc_client() -> Result<ArrowBigQueryWriteClient<Channel>, BQError> {
    let tls_config = ClientTlsConfig::new()
        .domain_name(BIGQUERY_STORAGE_API_DOMAIN)
        .with_enabled_roots();

    let channel = Channel::from_static(BIGQUERY_STORAGE_API_URL)
        .tls_config(tls_config)?
        .connect()
        .await?;

    Ok(ArrowBigQueryWriteClient::new(channel)
        .max_encoding_message_size(MAX_MESSAGE_SIZE_BYTES)
        .max_decoding_message_size(MAX_MESSAGE_SIZE_BYTES)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip))
}

fn new_authorized_request<T>(bearer_token: String, message: T) -> Result<Request<T>, BQError> {
    let bearer_value = bearer_token.as_str().try_into()?;
    let mut request = Request::new(message);
    request.metadata_mut().insert("authorization", bearer_value);
    Ok(request)
}

#[derive(Debug, Clone)]
struct ArrowBigQueryWriteClient<T> {
    inner: tonic::client::Grpc<T>,
}

impl<T> ArrowBigQueryWriteClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    fn new(inner: T) -> Self {
        Self {
            inner: tonic::client::Grpc::new(inner),
        }
    }

    fn send_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
        self.inner = self.inner.send_compressed(encoding);
        self
    }

    fn accept_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
        self.inner = self.inner.accept_compressed(encoding);
        self
    }

    fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_decoding_message_size(limit);
        self
    }

    fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_encoding_message_size(limit);
        self
    }

    async fn append_rows(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = ArrowAppendRowsRequest>,
    ) -> Result<tonic::Response<tonic::codec::Streaming<AppendRowsResponse>>, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("service was not ready: {}", error.into()))
        })?;

        let codec = tonic_prost::ProstCodec::default();
        let path = http::uri::PathAndQuery::from_static(
            "/google.cloud.bigquery.storage.v1.BigQueryWrite/AppendRows",
        );
        let mut request = request.into_streaming_request();
        request.extensions_mut().insert(GrpcMethod::new(
            "google.cloud.bigquery.storage.v1.BigQueryWrite",
            "AppendRows",
        ));

        self.inner.streaming(request, path, codec).await
    }
}

#[derive(Debug)]
struct ArrowErrorAdapter(String);

impl std::fmt::Display for ArrowErrorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use arrow::array::{BinaryArray, Float32Array, Int32Array, TimestampMicrosecondArray};
    use arrow::ipc::reader::StreamReader;
    use chrono::NaiveDate;
    use etl::conversions::arrow::table_rows_to_arrow_batch;
    use etl::types::{Cell, ColumnSchema, TableId, TableName, TableRow};

    fn make_schema(columns: Vec<ColumnSchema>) -> Arc<TableSchema> {
        Arc::new(TableSchema {
            id: TableId::new(42),
            name: TableName::new("public".to_string(), "users".to_string()),
            column_schemas: columns,
        })
    }

    #[test]
    fn test_prepare_snapshot_arrow_batch_converts_supported_scalars() {
        let schema = make_schema(vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, false, true),
            ColumnSchema::new("score".to_string(), Type::FLOAT4, -1, true, false),
            ColumnSchema::new("joined_at".to_string(), Type::TIMESTAMP, -1, true, false),
            ColumnSchema::new("avatar".to_string(), Type::BYTEA, -1, true, false),
            ColumnSchema::new("user_id".to_string(), Type::UUID, -1, true, false),
        ]);
        let rows = vec![TableRow::new(vec![
            Cell::I32(7),
            Cell::F32(1.5),
            Cell::Timestamp(
                NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .and_hms_micro_opt(3, 4, 5, 6)
                    .unwrap(),
            ),
            Cell::Bytes(vec![1, 2, 3]),
            Cell::Uuid(Uuid::nil()),
        ])];
        let batch = table_rows_to_arrow_batch(Arc::clone(&schema), &rows).unwrap();

        let prepared = prepare_snapshot_arrow_batch(&batch).unwrap().unwrap();
        let schema = prepared.batch.schema();
        let fields = schema.fields();

        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert_eq!(fields[1].data_type(), &DataType::Float64);
        assert_eq!(
            fields[2].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        assert_eq!(fields[3].data_type(), &DataType::Binary);
        assert_eq!(fields[4].data_type(), &DataType::Utf8);
        assert_eq!(fields[5].name(), "_CHANGE_TYPE");

        assert!(
            prepared
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .is_none()
        );
        assert!(
            prepared
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Float32Array>()
                .is_none()
        );
        assert!(
            prepared
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .is_some()
        );
        assert!(
            prepared
                .batch
                .column(3)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .is_some()
        );
        assert!(
            prepared
                .batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .is_some()
        );
    }

    #[test]
    fn test_prepare_snapshot_arrow_batch_falls_back_for_array_columns() {
        let schema = make_schema(vec![ColumnSchema::new(
            "tags".to_string(),
            Type::TEXT_ARRAY,
            -1,
            true,
            false,
        )]);
        let rows = vec![TableRow::new(vec![Cell::Array(
            etl::types::ArrayCell::String(vec![Some("a".to_string())]),
        )])];
        let batch = table_rows_to_arrow_batch(schema, &rows).unwrap();

        assert!(prepare_snapshot_arrow_batch(&batch).unwrap().is_none());
    }

    #[test]
    fn test_build_arrow_append_requests_round_trips_ipc_payload() {
        let schema = make_schema(vec![ColumnSchema::new(
            "id".to_string(),
            Type::INT4,
            -1,
            false,
            true,
        )]);
        let rows = vec![
            TableRow::new(vec![Cell::I32(1)]),
            TableRow::new(vec![Cell::I32(2)]),
        ];
        let batch = table_rows_to_arrow_batch(schema, &rows).unwrap();
        let prepared = prepare_snapshot_arrow_batch(&batch).unwrap().unwrap();
        let requests = build_arrow_append_requests(
            "projects/p/datasets/d/tables/t/streams/_default",
            "trace",
            &[prepared],
            9 * 1024 * 1024,
        )
        .unwrap();

        assert_eq!(requests.len(), 1);

        let arrow_rows = match requests[0].rows.as_ref().unwrap() {
            arrow_append_rows_request::Rows::ArrowRows(arrow_rows) => arrow_rows,
        };

        let mut stream_bytes = arrow_rows
            .writer_schema
            .as_ref()
            .unwrap()
            .serialized_schema
            .clone();
        stream_bytes.extend_from_slice(&arrow_rows.rows.as_ref().unwrap().serialized_record_batch);

        let batch_reader = StreamReader::try_new(Cursor::new(stream_bytes), None).unwrap();
        let decoded_schema = batch_reader.schema();
        assert_eq!(decoded_schema.fields()[0].name(), "id");

        let decoded_batches = batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(decoded_batches[0].num_rows(), 2);
    }

    #[test]
    fn test_should_disable_arrow_write_on_invalid_argument() {
        let responses = vec![Err(Status::invalid_argument("arrow_rows unsupported"))];

        assert!(should_disable_arrow_write(&responses));
    }

    #[test]
    fn test_should_not_disable_arrow_write_on_retryable_response() {
        let responses = vec![Err(Status::unavailable("backend overloaded"))];

        assert!(!should_disable_arrow_write(&responses));
    }
}
