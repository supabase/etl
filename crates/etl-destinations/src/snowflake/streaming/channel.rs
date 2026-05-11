use std::sync::Arc;

use etl::types::PipelineId;
use metrics::{counter, histogram};
use tracing::warn;

use crate::snowflake::{
    Error, OffsetToken, Result, RowBatch, StreamClient,
    metrics::{
        ETL_SNOWFLAKE_BATCH_BYTES, ETL_SNOWFLAKE_BATCH_SIZE,
        ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL, ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
    },
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

    /// Drop and reopen the channel, resetting offsets.
    pub async fn reset(&mut self) -> Result<()> {
        self.drop_channel().await?;
        self.open().await
    }

    /// Send all batches, recovering from channel GC errors.
    pub async fn process_batches(&mut self, batches: Vec<RowBatch>) -> Result<()> {
        fn is_stale_channel(e: &Error) -> bool {
            matches!(e, Error::Snowpipe { status_code: 4, .. })
                || matches!(e, Error::HttpStatus { status: 404, .. })
        }

        for batch in &batches {
            histogram!(ETL_SNOWFLAKE_BATCH_SIZE).record(batch.row_count() as f64);
            histogram!(ETL_SNOWFLAKE_BATCH_BYTES).record(batch.size() as f64);

            match self.send_batch(batch).await {
                Ok(()) => {}
                // If channel is stale, try to reopen it, once.
                Err(e) if is_stale_channel(&e) => {
                    counter!(ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL).increment(1);
                    warn!(table = %self.table, "channel was closed, reopenning and retrying insert");
                    self.open().await?;
                    self.send_batch(batch).await?;
                }
                Err(e) => {
                    counter!(ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL).increment(1);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn send_batch(&mut self, batch: &RowBatch) -> Result<()> {
        let ct = self.continuation_token.as_deref().ok_or_else(|| {
            Error::Channel("send_batch called on channel without continuation token".into())
        })?;

        let response = self
            .client
            .insert_rows(&self.database, &self.schema, &self.table, &self.channel, batch, ct)
            .await?;

        self.continuation_token = Some(response.continuation_token);
        self.last_offset_token = Some(batch.offset().clone());

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
