mod offset_token;
mod rest_client;

use std::future::Future;

pub use offset_token::OffsetToken;
pub use rest_client::RestStreamClient;

use crate::snowflake::{Result, encoding::RowBatch};

/// Response from opening or reopening a channel.
#[derive(Debug)]
pub struct OpenChannelResponse {
    /// Server-managed sequencer token.
    ///
    /// Must be passed to subsequent `insert_rows` calls on this channel.
    pub continuation_token: String,

    /// Last committed offset token.
    ///
    /// `None` if the channel has never committed data.
    pub offset_token: Option<OffsetToken>,
}

/// Response from inserting rows into a channel.
#[derive(Debug)]
pub struct InsertRowsResponse {
    /// Updated continuation token for the next `insert_rows` call.
    pub continuation_token: String,
}

/// Per-channel status from a bulk status check.
#[derive(Debug)]
pub struct ChannelStatusResponse {
    /// Channel name.
    pub channel: String,

    /// Snowflake-assigned status code (e.g. `ACTIVE`, `SUCCESS`).
    pub status_code: String,

    /// Last committed offset token, or `None` if no data has been committed.
    pub offset_token: Option<OffsetToken>,
}

/// Abstraction over Snowpipe Streaming ingestion backends.
///
/// Enables swapping between REST API and SDK sidecar (should we decide to
/// implement it to scale ingestion 10x) without changing destination logic.
pub trait StreamClient: Send + Sync + 'static {
    /// Discover the ingest hostname for this account.
    ///
    /// Normally, never changes and is discovered only once per account.
    fn discover_ingest_host(&self) -> impl Future<Output = Result<String>> + Send;

    /// Open or reopen a channel.
    ///
    /// Returns the continuation token and last committed offset token (if any).
    fn open_channel(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> impl Future<Output = Result<OpenChannelResponse>> + Send;

    /// Drop a channel.
    ///
    /// Committed data remains in the table.
    fn drop_channel(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Insert a pre-serialized NDJSON batch into a channel.
    ///
    /// The `continuation_token` is the server sequencer from `open_channel`
    /// or the previous `insert_rows` call.
    fn insert_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channel: &str,
        batch: RowBatch,
        offset_token: &OffsetToken,
        continuation_token: &str,
    ) -> impl Future<Output = Result<InsertRowsResponse>> + Send;

    /// Status check for one or more channels.
    fn channel_status(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        channels: &[String],
    ) -> impl Future<Output = Result<Vec<ChannelStatusResponse>>> + Send;
}
