use std::sync::Arc;

use etl::types::PipelineId;

use crate::snowflake::{
    Error, Result,
    encoding::RowBatch,
    streaming_client::{OffsetToken, StreamClient},
};

/// Manages the state and lifecycle of a single Snowpipe Streaming channel.
///
/// Channel is a conduit via which we push data to Snowflake system.
#[derive(Debug, Clone)]
pub struct ChannelHandle<C> {
    /// Streaming API client.
    client: Arc<C>,

    /// Snowflake database name.
    database: String,

    /// Snowflake schema name.
    schema: String,

    /// Snowflake target table name.
    table: String,

    /// Derived channel name.
    channel: String,

    /// Last offset token returned by Snowflake API (on open or insert).
    last_offset_token: Option<OffsetToken>,

    /// Continuation token for the next API call on this channel.
    continuation_token: Option<String>,
}

impl<C: StreamClient> ChannelHandle<C> {
    /// New handle with no offset or continuation tokens.
    pub fn new(
        client: Arc<C>,
        pipeline: PipelineId,
        database: String,
        schema: String,
        table: String,
    ) -> Self {
        let channel = format!("etl_{pipeline}_{table}_ch0");
        Self {
            client,
            database,
            schema,
            table,
            channel,
            last_offset_token: None,
            continuation_token: None,
        }
    }

    /// Open (or reopen) the channel.
    ///
    /// Idempotent, reopening preserves offset history.
    pub async fn open(&mut self) -> Result<()> {
        let response = self
            .client
            .open_channel(&self.database, &self.schema, &self.table, &self.channel)
            .await?;

        self.last_offset_token = response.offset_token;
        self.continuation_token = Some(response.continuation_token);

        Ok(())
    }

    /// Drop the channel.
    ///
    /// Committed data remains in the table.
    pub async fn drop_channel(&mut self) -> Result<()> {
        self.client.drop_channel(&self.database, &self.schema, &self.table, &self.channel).await?;

        self.last_offset_token = None;
        self.continuation_token = None;

        Ok(())
    }

    /// Insert a batch of rows into the channel.
    ///
    /// Updates the continuation token and last offset token on success.
    pub async fn insert_rows(&mut self, batch: RowBatch, offset: &OffsetToken) -> Result<()> {
        let ct = self.continuation_token.as_deref().ok_or_else(|| {
            Error::Channel("insert_rows called on channel without continuation token".into())
        })?;

        let response = self
            .client
            .insert_rows(
                &self.database,
                &self.schema,
                &self.table,
                &self.channel,
                batch,
                offset,
                ct,
            )
            .await?;

        self.continuation_token = Some(response.continuation_token);
        self.last_offset_token = Some(offset.clone());

        Ok(())
    }

    /// Last offset token committed by Snowflake for this channel.
    pub async fn committed_offset(&self) -> Result<Option<OffsetToken>> {
        let statuses = self
            .client
            .channel_status(&self.database, &self.schema, &self.table, &[self.channel.clone()])
            .await?;

        Ok(statuses.into_iter().next().and_then(|s| s.offset_token))
    }
}
