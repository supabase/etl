use std::{string::String, sync::Arc, time::Duration};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use crate::{
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
    snowflake::{
        Error, Result, SnowpipeError,
        auth::TokenProvider,
        streaming::{
            ChannelStatusResponse, InsertRowsResponse, OffsetToken, OpenChannelResponse, RowBatch,
            StreamClient,
        },
    },
};

const SNOWPIPE_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_retries: 3,
    initial_delay: Duration::from_millis(500),
    max_delay: Duration::from_secs(10),
};

const USER_AGENT: &str = "supabase-etl/0.1.0";

/// [`StreamClient`] backed by the Snowpipe Streaming REST API.
///
/// Discovers the ingest host on first use and caches it for the lifetime of the
/// client.
///
/// All mutating calls (open/drop channel, insert rows, channel status) are
/// retried with exponential backoff.
pub struct RestStreamClient<T> {
    account_url: String,
    auth: Arc<T>,
    http: Client,
    ingest_host: OnceCell<String>,
}

impl<T: TokenProvider> RestStreamClient<T> {
    pub fn new(account_url: String, auth: Arc<T>, http: Client) -> Self {
        Self { account_url, auth, http, ingest_host: OnceCell::new() }
    }

    async fn get_or_discover_host(&self) -> Result<&str> {
        self.ingest_host
            .get_or_try_init(|| async {
                let token = self.auth.get_token().await?;
                let url = format!("{}/v2/streaming/hostname", self.account_url);
                let resp = self
                    .http
                    .get(&url)
                    .bearer_auth(&token)
                    .header("User-Agent", USER_AGENT)
                    .send()
                    .await
                    .map_err(Error::HttpTransport)?;

                let status = resp.status();
                if status != StatusCode::OK {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(Error::HttpStatus { status, body });
                }

                // Actual server returns plain text (even with Accept: application/json).
                // Docs say JSON: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#get-hostname
                let body_text = resp.text().await.unwrap_or_default();
                let hostname = serde_json::from_str::<HostnameResponse>(&body_text)
                    .map_or_else(|_| body_text.trim().to_owned(), |r| r.hostname);

                if hostname.is_empty() {
                    return Err(Error::Channel(
                        "hostname discovery returned empty hostname".into(),
                    ));
                }

                debug!(hostname = %hostname, "discovered ingest host");
                let host = if hostname.starts_with("http://") || hostname.starts_with("https://") {
                    hostname
                } else {
                    format!("https://{hostname}")
                };

                Ok(host)
            })
            .await
            .map(String::as_str)
    }
}

impl<T: TokenProvider + 'static> StreamClient for RestStreamClient<T> {
    async fn discover_ingest_host(&self) -> Result<String> {
        self.get_or_discover_host().await.map(ToOwned::to_owned)
    }

    async fn open_channel(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> Result<OpenChannelResponse> {
        let host = self.get_or_discover_host().await?;
        let url = channel_url(host, database, schema, table, channel);

        let auth = Arc::clone(&self.auth);
        let http = self.http.clone();

        retry_with_backoff(
            SNOWPIPE_RETRY_POLICY,
            should_retry,
            |d| d,
            |attempt| {
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying open_channel"
                );
            },
            || {
                let url = url.clone();
                let auth = Arc::clone(&auth);
                let http = http.clone();

                async move {
                    let token = auth.get_token().await?;
                    let resp = http
                        .put(&url)
                        .bearer_auth(&token)
                        .header("User-Agent", USER_AGENT)
                        .header("Content-Type", "application/json")
                        .json(&ChannelLifecycleRequest::new())
                        .send()
                        .await
                        .map_err(Error::HttpTransport)?;

                    let status = resp.status();
                    if status != StatusCode::OK {
                        let body = resp.text().await.unwrap_or_default();
                        if status == StatusCode::UNAUTHORIZED {
                            warn!("received 401 from Snowpipe Streaming API, invalidating token");
                            auth.invalidate_token().await;
                        }
                        return Err(SnowpipeError::from_response(status, body).into());
                    }

                    let response: OpenChannelApiResponse = resp.json().await.map_err(|e| {
                        Error::Encoding(format!("failed to parse open_channel response: {e}"))
                    })?;

                    let status = response.channel_status.ok_or_else(|| {
                        Error::Encoding("open_channel response missing channel_status".into())
                    })?;

                    if let Some(ref code) = status.channel_status_code {
                        let is_ok = code == "SUCCESS" || code == "ACTIVE" || code == "0";
                        if !is_ok {
                            let msg = format!("open_channel returned unexpected status: {code}");
                            return Err(
                                SnowpipeError::ApiStatus { status_code: 1, message: msg }.into()
                            );
                        }
                    }

                    let status = status.into_channel_status(channel)?;
                    Ok(OpenChannelResponse {
                        continuation_token: response.next_continuation_token,
                        offset_token: status.offset_token.clone(),
                        status,
                    })
                }
            },
        )
        .await
        .map_err(|f| f.last_error)
    }

    async fn insert_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
        batch: &RowBatch,
        continuation_token: &str,
    ) -> Result<InsertRowsResponse> {
        let host = self.get_or_discover_host().await?;
        let base_url = insert_url(host, database, schema, table, channel);

        let compressed = batch.bytes().clone();
        let query_params = insert_query_params(batch, continuation_token);

        let auth = Arc::clone(&self.auth);
        let http = self.http.clone();

        retry_with_backoff(
            SNOWPIPE_RETRY_POLICY,
            should_retry,
            |d| d,
            |attempt| {
                if matches!(attempt.error, Error::Snowpipe(SnowpipeError::AuthenticationExpired)) {
                    debug!("auth error on insert_rows, token will be refreshed on retry");
                }
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying insert_rows"
                );
            },
            || {
                let base_url = base_url.clone();
                let query_params = query_params.clone();
                let auth = Arc::clone(&auth);
                let http = http.clone();
                let compressed = compressed.clone();
                async move {
                    let token = auth.get_token().await?;
                    let resp = http
                        .post(&base_url)
                        .query(&query_params)
                        .bearer_auth(&token)
                        .header("User-Agent", USER_AGENT)
                        .header("Content-Type", "application/x-ndjson")
                        .header("Content-Encoding", "zstd")
                        .body(compressed)
                        .send()
                        .await
                        .map_err(Error::HttpTransport)?;

                    let status = resp.status();
                    if status != StatusCode::OK {
                        let body = resp.text().await.unwrap_or_default();
                        let error = SnowpipeError::from_response(status, body);
                        if status == StatusCode::UNAUTHORIZED {
                            warn!("received 401 from Snowpipe Streaming API, invalidating token");
                            auth.invalidate_token().await;
                        }
                        if error.is_authentication_expired() {
                            auth.invalidate_token().await;
                        }
                        return Err(error.into());
                    }

                    let response: InsertRowsApiResponse = resp.json().await.map_err(|e| {
                        Error::Encoding(format!("failed to parse insert_rows response: {e}"))
                    })?;

                    Ok(InsertRowsResponse { continuation_token: response.next_continuation_token })
                }
            },
        )
        .await
        .map_err(|f| f.last_error)
    }

    async fn drop_channel(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> Result<()> {
        let host = self.get_or_discover_host().await?;
        let url = channel_url(host, database, schema, table, channel);

        let auth = Arc::clone(&self.auth);
        let http = self.http.clone();

        retry_with_backoff(
            SNOWPIPE_RETRY_POLICY,
            should_retry,
            |d| d,
            |attempt| {
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying drop_channel"
                );
            },
            || {
                let url = url.clone();
                let auth = Arc::clone(&auth);
                let http = http.clone();
                async move {
                    let token = auth.get_token().await?;
                    let resp = http
                        .delete(&url)
                        .bearer_auth(&token)
                        .header("User-Agent", USER_AGENT)
                        .json(&ChannelLifecycleRequest::new())
                        .send()
                        .await
                        .map_err(Error::HttpTransport)?;

                    let status = resp.status();
                    if status != StatusCode::OK {
                        let body = resp.text().await.unwrap_or_default();
                        if status == StatusCode::UNAUTHORIZED {
                            warn!("received 401 from Snowpipe Streaming API, invalidating token");
                            auth.invalidate_token().await;
                        }
                        return Err(SnowpipeError::from_response(status, body).into());
                    }
                    Ok(())
                }
            },
        )
        .await
        .map_err(|f| f.last_error)
    }

    async fn channel_status(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> Result<ChannelStatusResponse> {
        let host = self.get_or_discover_host().await?;
        let url = channel_status_url(host, database, schema, table);

        let auth = Arc::clone(&self.auth);
        let http = self.http.clone();
        let channel_names = vec![channel.to_owned()];
        let request_body = BulkStatusRequest { channel_names: &channel_names };

        retry_with_backoff(
            SNOWPIPE_RETRY_POLICY,
            should_retry,
            |d| d,
            |attempt| {
                warn!(
                    retry = attempt.retry_index,
                    max = attempt.max_retries,
                    delay_ms = attempt.sleep_delay.as_millis(),
                    error = %attempt.error,
                    "retrying channel_status"
                );
            },
            || {
                let url = url.clone();
                let auth = Arc::clone(&auth);
                let http = http.clone();
                let body = &request_body;
                async move {
                    let token = auth.get_token().await?;
                    let resp = http
                        .post(&url)
                        .bearer_auth(&token)
                        .header("User-Agent", USER_AGENT)
                        .json(body)
                        .send()
                        .await
                        .map_err(Error::HttpTransport)?;

                    let status = resp.status();
                    if status != StatusCode::OK {
                        let body = resp.text().await.unwrap_or_default();
                        if status == StatusCode::UNAUTHORIZED {
                            warn!("received 401 from Snowpipe Streaming API, invalidating token");
                            auth.invalidate_token().await;
                        }
                        return Err(SnowpipeError::from_response(status, body).into());
                    }

                    let response: BulkStatusApiResponse = resp.json().await.map_err(|e| {
                        Error::Encoding(format!("failed to parse channel_status response: {e}"))
                    })?;

                    response.channel_statuses.into_iter().next().map_or_else(
                        || Err(Error::Channel("channel not found in status response".into())),
                        |(name, ch)| ch.into_channel_status(&name),
                    )
                }
            },
        )
        .await
        .map_err(|f| f.last_error)
    }
}

fn pipe_name(table: &str) -> String {
    format!("{table}-STREAMING")
}

fn channel_url(host: &str, db: &str, schema: &str, table: &str, channel: &str) -> String {
    let pipe = pipe_name(table);
    format!("{host}/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}")
}

fn insert_url(host: &str, db: &str, schema: &str, table: &str, channel: &str) -> String {
    let pipe = pipe_name(table);
    format!(
        "{host}/v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/\
         rows"
    )
}

fn channel_status_url(host: &str, db: &str, schema: &str, table: &str) -> String {
    let pipe = pipe_name(table);
    format!("{host}/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}:bulk-channel-status")
}

fn insert_query_params(batch: &RowBatch, continuation_token: &str) -> [(&'static str, String); 3] {
    [
        ("continuationToken", continuation_token.to_owned()),
        ("startOffsetToken", batch.start_offset().as_ref().to_owned()),
        ("endOffsetToken", batch.end_offset().as_ref().to_owned()),
    ]
}

fn should_retry(error: &Error) -> RetryDecision {
    match error {
        Error::Snowpipe(error) => match error {
            SnowpipeError::AuthenticationExpired => RetryDecision::Retry,
            SnowpipeError::StaleContinuation
            | SnowpipeError::ChannelHasUncommittedRows
            | SnowpipeError::ChannelNotFound => RetryDecision::Stop,
            SnowpipeError::ApiStatus { status_code, .. } => match *status_code {
                0 | 2 | 4 => RetryDecision::Stop,
                1 | 3 | 5 | 6 => RetryDecision::Retry,
                _ => RetryDecision::Retry,
            },
            SnowpipeError::HttpStatus { status, .. } => http_status_retry_decision(*status),
        },
        Error::HttpTransport(_) => RetryDecision::Retry,
        Error::HttpStatus { status, .. } => http_status_retry_decision(*status),
        _ => RetryDecision::Stop,
    }
}

fn http_status_retry_decision(status: StatusCode) -> RetryDecision {
    if status == StatusCode::UNAUTHORIZED
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        RetryDecision::Retry
    } else {
        RetryDecision::Stop
    }
}

#[derive(Deserialize)]
struct HostnameResponse {
    hostname: String,
}

#[derive(Serialize)]
struct ChannelLifecycleRequest {
    fail_on_uncommitted_rows: bool,
}

impl ChannelLifecycleRequest {
    fn new() -> Self {
        Self { fail_on_uncommitted_rows: true }
    }
}

#[derive(Deserialize)]
struct OpenChannelApiResponse {
    next_continuation_token: String,
    #[serde(default)]
    channel_status: Option<ChannelStatusDetail>,
}

#[derive(Deserialize)]
struct ChannelStatusDetail {
    #[serde(default)]
    channel_name: Option<String>,
    #[serde(default)]
    channel_status_code: Option<String>,
    #[serde(default)]
    last_committed_offset_token: Option<String>,
    #[serde(default)]
    rows_inserted: u64,
    #[serde(default)]
    rows_parsed: u64,
    #[serde(default, alias = "rows_errors")]
    rows_error_count: u64,
    #[serde(default)]
    last_error_offset_upper_bound: Option<String>,
    #[serde(default)]
    last_error_message: Option<String>,
}

impl ChannelStatusDetail {
    fn into_channel_status(self, fallback_channel: &str) -> Result<ChannelStatusResponse> {
        Ok(ChannelStatusResponse {
            channel: self.channel_name.unwrap_or_else(|| fallback_channel.to_owned()),
            status_code: self.channel_status_code.unwrap_or_default(),
            offset_token: parse_optional_offset_token(self.last_committed_offset_token)?,
            rows_inserted: self.rows_inserted,
            rows_parsed: self.rows_parsed,
            rows_error_count: self.rows_error_count,
            last_error_offset_upper_bound: parse_optional_offset_token(
                self.last_error_offset_upper_bound,
            )?,
            last_error_message: self.last_error_message,
        })
    }
}

#[derive(Deserialize)]
struct InsertRowsApiResponse {
    next_continuation_token: String,
}

#[derive(Serialize)]
struct BulkStatusRequest<'a> {
    channel_names: &'a [String],
}

#[derive(Deserialize)]
struct BulkStatusApiResponse {
    #[serde(default)]
    channel_statuses: std::collections::HashMap<String, BulkStatusChannel>,
}

#[derive(Deserialize)]
struct BulkStatusChannel {
    #[serde(default)]
    channel_status_code: Option<String>,
    #[serde(default)]
    last_committed_offset_token: Option<String>,
    #[serde(default)]
    channel_name: Option<String>,
    #[serde(default)]
    rows_inserted: u64,
    #[serde(default)]
    rows_parsed: u64,
    #[serde(default, alias = "rows_errors")]
    rows_error_count: u64,
    #[serde(default)]
    last_error_offset_upper_bound: Option<String>,
    #[serde(default)]
    last_error_message: Option<String>,
}

impl BulkStatusChannel {
    fn into_channel_status(self, fallback_channel: &str) -> Result<ChannelStatusResponse> {
        Ok(ChannelStatusResponse {
            channel: self.channel_name.unwrap_or_else(|| fallback_channel.to_owned()),
            status_code: self.channel_status_code.unwrap_or_default(),
            offset_token: parse_optional_offset_token(self.last_committed_offset_token)?,
            rows_inserted: self.rows_inserted,
            rows_parsed: self.rows_parsed,
            rows_error_count: self.rows_error_count,
            last_error_offset_upper_bound: parse_optional_offset_token(
                self.last_error_offset_upper_bound,
            )?,
            last_error_message: self.last_error_message,
        })
    }
}

fn parse_optional_offset_token(token: Option<String>) -> Result<Option<OffsetToken>> {
    token.filter(|token| !token.is_empty()).map(|token| token.parse::<OffsetToken>()).transpose()
}

#[cfg(test)]
mod tests {
    use etl::{
        data::{Cell, TableRow},
        schema::{ColumnSchema, Type},
    };
    use serde_json::json;

    use super::*;
    use crate::snowflake::{
        encoding::{CdcMeta, CdcOperation, serialize_row},
        streaming::{OffsetToken, RowBatchBuilder},
    };

    #[test]
    fn should_retry_decision() {
        let snowpipe = |code| {
            Error::Snowpipe(SnowpipeError::ApiStatus { status_code: code, message: "test".into() })
        };

        assert_eq!(should_retry(&snowpipe(0)), RetryDecision::Stop);
        assert_eq!(should_retry(&snowpipe(1)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(2)), RetryDecision::Stop);
        assert_eq!(should_retry(&snowpipe(3)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(4)), RetryDecision::Stop);
        assert_eq!(should_retry(&snowpipe(5)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(6)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(99)), RetryDecision::Retry);

        let http = |status: StatusCode| Error::HttpStatus { status, body: "test".into() };

        assert_eq!(should_retry(&http(StatusCode::INTERNAL_SERVER_ERROR)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(StatusCode::TOO_MANY_REQUESTS)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(StatusCode::REQUEST_TIMEOUT)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(StatusCode::UNAUTHORIZED)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(StatusCode::BAD_REQUEST)), RetryDecision::Stop);

        assert_eq!(
            should_retry(&Error::Snowpipe(SnowpipeError::AuthenticationExpired)),
            RetryDecision::Retry
        );
        assert_eq!(
            should_retry(&Error::Snowpipe(SnowpipeError::StaleContinuation)),
            RetryDecision::Stop
        );
        assert_eq!(should_retry(&Error::Auth("expired".into())), RetryDecision::Stop);
    }

    #[test]
    fn ndjson_formatting() {
        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, true),
        ];

        let mut buf = Vec::new();
        serialize_row(
            &mut buf,
            &cols,
            &TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]),
            CdcMeta::new(CdcOperation::Insert, "0"),
        )
        .unwrap();
        serialize_row(
            &mut buf,
            &cols,
            &TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
            CdcMeta::new(CdcOperation::Insert, "0"),
        )
        .unwrap();

        let text = std::str::from_utf8(&buf).unwrap();
        let lines: Vec<&str> = text.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let row0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(row0["id"], 1);
        assert_eq!(row0["name"], "Alice");

        let row1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(row1["id"], 2);
        assert_eq!(row1["name"], "Bob");
    }

    #[test]
    fn compressed_roundtrip() {
        let cols = [ColumnSchema::new("id".into(), Type::INT4, -1, 1, true)];
        let mut builder = RowBatchBuilder::new();
        builder
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::I32(42)]),
                CdcMeta::new(CdcOperation::Insert, "0"),
                &OffsetToken::zero(),
            )
            .unwrap();

        let batches = builder.finish().unwrap();
        let batch = batches.first().unwrap();
        assert!(batch.size() > 0);

        let decompressed = zstd::decode_all(batch.bytes().as_ref()).unwrap();
        let text = String::from_utf8(decompressed).unwrap();
        let line = text.trim();
        let val: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(val["id"], 42);
    }

    #[test]
    fn insert_query_params_include_start_and_end_offsets() {
        let cols = [ColumnSchema::new("id".into(), Type::INT4, -1, 1, true)];
        let start: OffsetToken = "000000000000000a/0000000000000001".parse().unwrap();
        let end: OffsetToken = "000000000000000a/0000000000000002".parse().unwrap();
        let mut builder = RowBatchBuilder::new();
        builder
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::I32(1)]),
                CdcMeta::new(CdcOperation::Insert, start.as_ref()),
                &start,
            )
            .unwrap();
        builder
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::I32(2)]),
                CdcMeta::new(CdcOperation::Insert, end.as_ref()),
                &end,
            )
            .unwrap();
        let batch = builder.finish().unwrap().pop().unwrap();

        let params = insert_query_params(&batch, "ct0");

        assert_eq!(
            params,
            [
                ("continuationToken", "ct0".to_owned()),
                ("startOffsetToken", start.as_ref().to_owned()),
                ("endOffsetToken", end.as_ref().to_owned()),
            ]
        );
    }

    #[test]
    fn channel_status_parsers_accept_documented_status_fields() {
        // Open Channel has row errors as `rows_error_count` in Snowflake API docs.
        let detail: ChannelStatusDetail = serde_json::from_value(json!({
            "channel_name": "ch0",
            "channel_status_code": "ACTIVE",
            "last_committed_offset_token": "000000000000000a/0000000000000002",
            "rows_inserted": 12,
            "rows_parsed": 13,
            "rows_error_count": 1,
            "last_error_offset_upper_bound": "000000000000000a/0000000000000003",
            "last_error_message": "row rejected"
        }))
        .unwrap();

        let status = detail.into_channel_status("fallback").unwrap();

        assert_eq!(status.channel, "ch0");
        assert_eq!(status.status_code, "ACTIVE");
        assert_eq!(status.offset_token, Some("000000000000000a/0000000000000002".parse().unwrap()));
        assert_eq!(status.rows_inserted, 12);
        assert_eq!(status.rows_parsed, 13);
        assert_eq!(status.rows_error_count, 1);
        assert_eq!(
            status.last_error_offset_upper_bound,
            Some("000000000000000a/0000000000000003".parse().unwrap())
        );
        assert_eq!(status.last_error_message.as_deref(), Some("row rejected"));

        // Bulk Get Channel Status documents the same counter as `rows_errors`.
        let detail: BulkStatusChannel = serde_json::from_value(json!({
            "last_committed_offset_token": "000000000000000a/0000000000000002",
            "rows_inserted": 12,
            "rows_parsed": 13,
            "rows_errors": 2
        }))
        .unwrap();

        let status = detail.into_channel_status("fallback").unwrap();

        assert_eq!(status.channel, "fallback");
        assert_eq!(status.rows_inserted, 12);
        assert_eq!(status.rows_parsed, 13);
        assert_eq!(status.rows_error_count, 2);
    }
}
