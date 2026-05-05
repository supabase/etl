use std::{sync::Arc, time::Duration};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use crate::{
    retry::{RetryDecision, RetryPolicy, retry_with_backoff},
    snowflake::{
        Error, Result,
        auth::TokenProvider,
        encoding::RowBatch,
        streaming::{
            ChannelStatusResponse, InsertRowsResponse, OffsetToken, OpenChannelResponse,
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
pub struct RestStreamClient<T: TokenProvider> {
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

                let status = resp.status().as_u16();
                let body_text = resp.text().await.unwrap_or_default();

                if status != 200 {
                    return Err(Error::HttpStatus { status, body: body_text });
                }

                // Actual server returns plain text (even with Accept: application/json).
                // Docs say JSON: https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api#get-hostname
                let hostname = serde_json::from_str::<HostnameResponse>(&body_text)
                    .map(|r| r.hostname)
                    .unwrap_or_else(|_| body_text.trim().to_string());

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
            .map(|s| s.as_str())
    }
}

impl<T: TokenProvider + 'static> StreamClient for RestStreamClient<T> {
    async fn discover_ingest_host(&self) -> Result<String> {
        self.get_or_discover_host().await.map(|s| s.to_string())
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
                        .body("{}")
                        .send()
                        .await
                        .map_err(Error::HttpTransport)?;

                    let status = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();

                    if status != 200 {
                        return Err(Error::HttpStatus { status, body });
                    }

                    let response: OpenChannelApiResponse =
                        serde_json::from_str(&body).map_err(|e| {
                            Error::Encoding(format!("failed to parse open_channel response: {e}"))
                        })?;

                    if let Some(ref status) = response.channel_status {
                        if let Some(ref code) = status.channel_status_code {
                            let is_ok = code == "SUCCESS" || code == "ACTIVE" || code == "0";
                            if !is_ok {
                                let msg = format!("open_channel returned status {code}: {body}");
                                return Err(Error::Snowpipe { status_code: 1, message: msg });
                            }
                        }
                    }

                    Ok(OpenChannelResponse {
                        continuation_token: response.next_continuation_token,
                        offset_token: response
                            .channel_status
                            .and_then(|cs| cs.last_committed_offset_token)
                            .map(|s| s.parse::<OffsetToken>())
                            .transpose()?,
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
        batch: RowBatch,
        offset_token: &OffsetToken,
        continuation_token: &str,
    ) -> Result<InsertRowsResponse> {
        let host = self.get_or_discover_host().await?;
        let base_url = insert_url(host, database, schema, table, channel);

        let compressed = batch.into_compressed()?;
        let query_params = [
            ("continuationToken", continuation_token.to_string()),
            ("offsetToken", offset_token.as_ref().to_string()),
        ];

        let auth = Arc::clone(&self.auth);
        let http = self.http.clone();

        retry_with_backoff(
            SNOWPIPE_RETRY_POLICY,
            should_retry,
            |d| d,
            |attempt| {
                if matches!(attempt.error, Error::Snowpipe { status_code: 3, .. }) {
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

                    let status = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();

                    if status != 200 {
                        if let Ok(err_resp) = serde_json::from_str::<SnowpipeErrorResponse>(&body) {
                            if let Some(code) = err_resp.status_code {
                                if code == 3 {
                                    auth.invalidate_token().await;
                                }
                                return Err(Error::Snowpipe { status_code: code, message: body });
                            }
                        }
                        return Err(Error::HttpStatus { status, body });
                    }

                    let response: InsertRowsApiResponse =
                        serde_json::from_str(&body).map_err(|e| {
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
                        .send()
                        .await
                        .map_err(Error::HttpTransport)?;

                    let status = resp.status().as_u16();
                    if status != 200 {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(Error::HttpStatus { status, body });
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
        channels: &[String],
    ) -> Result<Vec<ChannelStatusResponse>> {
        let host = self.get_or_discover_host().await?;
        let url = channel_status_url(host, database, schema, table);

        let auth = Arc::clone(&self.auth);
        let http = self.http.clone();
        let request_body = BulkStatusRequest { channel_names: channels };

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

                    let status = resp.status().as_u16();
                    let body_text = resp.text().await.unwrap_or_default();

                    if status != 200 {
                        return Err(Error::HttpStatus { status, body: body_text });
                    }

                    let response: BulkStatusApiResponse = serde_json::from_str(&body_text)
                        .map_err(|e| {
                            Error::Encoding(format!("failed to parse channel_status response: {e}"))
                        })?;

                    Ok(response
                        .channel_statuses
                        .into_iter()
                        .map(|(name, ch)| {
                            Ok(ChannelStatusResponse {
                                channel: name,
                                status_code: ch.channel_status_code.unwrap_or_default(),
                                offset_token: ch
                                    .last_committed_offset_token
                                    .map(|s| s.parse::<OffsetToken>())
                                    .transpose()?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?)
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

fn should_retry(error: &Error) -> RetryDecision {
    match error {
        Error::Snowpipe { status_code, .. } => match *status_code {
            0 => RetryDecision::Stop,
            1 | 5 | 6 => RetryDecision::Retry,
            3 => RetryDecision::Retry,
            2 | 4 => RetryDecision::Stop,
            _ => RetryDecision::Retry,
        },
        Error::HttpTransport(_) => RetryDecision::Retry,
        Error::HttpStatus { status, .. } => match *status {
            408 | 429 => RetryDecision::Retry,
            s if s >= 500 => RetryDecision::Retry,
            _ => RetryDecision::Stop,
        },
        _ => RetryDecision::Stop,
    }
}

#[derive(Deserialize)]
struct HostnameResponse {
    hostname: String,
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
    channel_status_code: Option<String>,
    #[serde(default)]
    last_committed_offset_token: Option<String>,
}

#[derive(Deserialize)]
struct InsertRowsApiResponse {
    next_continuation_token: String,
}

#[derive(Deserialize)]
struct SnowpipeErrorResponse {
    #[serde(default)]
    status_code: Option<u32>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_retry_decision() {
        let snowpipe = |code| Error::Snowpipe { status_code: code, message: "test".into() };

        assert_eq!(should_retry(&snowpipe(0)), RetryDecision::Stop);
        assert_eq!(should_retry(&snowpipe(1)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(2)), RetryDecision::Stop);
        assert_eq!(should_retry(&snowpipe(3)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(4)), RetryDecision::Stop);
        assert_eq!(should_retry(&snowpipe(5)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(6)), RetryDecision::Retry);
        assert_eq!(should_retry(&snowpipe(99)), RetryDecision::Retry);

        let http = |status| Error::HttpStatus { status, body: "test".into() };

        assert_eq!(should_retry(&http(500)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(429)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(408)), RetryDecision::Retry);
        assert_eq!(should_retry(&http(400)), RetryDecision::Stop);

        assert_eq!(should_retry(&Error::Auth("expired".into())), RetryDecision::Stop);
    }

    #[test]
    fn ndjson_formatting() {
        use etl::types::{Cell, ColumnSchema, TableRow, Type};

        use crate::snowflake::encoding::{CdcMeta, CdcOperation};
        let cols = [
            ColumnSchema::new("id".into(), Type::INT4, -1, 1, None, true),
            ColumnSchema::new("name".into(), Type::TEXT, -1, 2, None, true),
        ];

        let mut batch = RowBatch::with_capacity(1024);
        batch
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]),
                CdcMeta::new(CdcOperation::Insert, "0"),
            )
            .unwrap();
        batch
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
                CdcMeta::new(CdcOperation::Insert, "0"),
            )
            .unwrap();

        let text = std::str::from_utf8(batch.as_bytes()).unwrap();
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
    fn into_compressed_roundtrip() {
        use etl::types::{Cell, ColumnSchema, TableRow, Type};

        use crate::snowflake::encoding::{CdcMeta, CdcOperation};
        let cols = [ColumnSchema::new("id".into(), Type::INT4, -1, 1, None, true)];

        let mut batch = RowBatch::with_capacity(1024);
        batch
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::I32(42)]),
                CdcMeta::new(CdcOperation::Insert, "0"),
            )
            .unwrap();

        let raw = batch.as_bytes().to_vec();

        let compressed = batch.into_compressed().unwrap();
        assert!(!compressed.is_empty());

        let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();
        assert_eq!(decompressed, raw);
    }

    #[test]
    fn into_compressed_rejects_oversized() {
        use etl::types::{Cell, ColumnSchema, TableRow, Type};

        use crate::snowflake::encoding::{CdcMeta, CdcOperation};
        let cols = [ColumnSchema::new("data".into(), Type::TEXT, -1, 1, None, true)];

        let large_value = "x".repeat(4 * 1024 * 1024 + 1);
        let mut batch = RowBatch::with_capacity(4 * 1024 * 1024 + 256);
        batch
            .push_row(
                &cols,
                &TableRow::new(vec![Cell::String(large_value)]),
                CdcMeta::new(CdcOperation::Insert, "0"),
            )
            .unwrap();

        let err = batch.into_compressed().unwrap_err();
        assert!(matches!(err, Error::Encoding(msg) if msg.contains("limit")));
    }
}
