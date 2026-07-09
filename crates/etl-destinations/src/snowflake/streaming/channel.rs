use std::{sync::Arc, time::Duration};

use etl::pipeline::PipelineId;
use metrics::{counter, histogram};
use tokio::time::{Instant, sleep};
use tracing::warn;

use crate::snowflake::{
    Error, OffsetToken, Result, RowBatch, SnowpipeError, StreamClient,
    metrics::{
        ETL_SNOWFLAKE_BATCH_BYTES, ETL_SNOWFLAKE_BATCH_SIZE,
        ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL, ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
    },
    streaming::ChannelStatusResponse,
};

/// Interval between Snowflake channel commit-status checks.
///
/// Defines how often do we retry/check.
/// Durability barriers use this between status polls. Safe open/drop uses it
/// between retries after Snowflake reports uncommitted channel data.
pub(crate) const DEFAULT_COMMIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Total wall-clock budget for waiting until accepted rows become committed.
///
/// If this deadline is reached, the destination returns an error instead of
/// reporting durability or reopening/dropping a channel destructively.
pub(crate) const DEFAULT_COMMIT_WAIT_TIMEOUT: Duration = Duration::from_secs(180);

/// Row batch accepted by a streaming channel.
#[derive(Debug, Clone)]
pub(crate) struct AcceptedRowBatch {
    /// Final offset accepted by Snowflake for this row batch.
    pub target_offset: OffsetToken,
    /// Number of rows accepted in this row batch.
    pub rows: u64,
    /// Compressed payload bytes accepted in this row batch.
    pub bytes: usize,
    /// Cumulative inserted rows before the pending range began.
    pub baseline_rows_inserted: u64,
    /// Cumulative row errors before the pending range began.
    pub baseline_rows_error_count: u64,
}

impl AcceptedRowBatch {
    fn from_row_batch(
        batch: &RowBatch,
        baseline_rows_inserted: u64,
        baseline_rows_error_count: u64,
    ) -> Result<Self> {
        Ok(Self {
            target_offset: batch.end_offset().clone(),
            rows: u64::try_from(batch.row_count()).map_err(|_| {
                Error::Channel("Snowflake row batch row count overflowed u64.".into())
            })?,
            bytes: batch.size(),
            baseline_rows_inserted,
            baseline_rows_error_count,
        })
    }
}

/// Last durable status observed for a Snowflake channel.
#[derive(Debug, Clone, Default)]
struct ChannelProgress {
    /// Last committed offset reported by Snowflake.
    committed_offset: Option<OffsetToken>,
    /// Cumulative inserted rows reported by Snowflake.
    rows_inserted: u64,
    /// Cumulative row errors reported by Snowflake.
    rows_error_count: u64,
}

impl ChannelProgress {
    /// Updates progress from a channel status response.
    fn observe(&mut self, status: &ChannelStatusResponse) {
        self.committed_offset = status.offset_token.clone();
        self.rows_inserted = status.rows_inserted;
        self.rows_error_count = status.rows_error_count;
    }

    /// Returns whether `offset` has already committed.
    fn is_committed(&self, offset: &OffsetToken) -> bool {
        self.committed_offset.as_ref().is_some_and(|committed| committed >= offset)
    }
}

/// Result of sending one batch to Snowflake.
#[derive(Debug)]
enum BatchAcceptance {
    /// Snowflake accepted ownership of the batch.
    Accepted(AcceptedRowBatch),
    /// The batch was already committed before the retry path resent it.
    AlreadyCommitted,
}

/// Manages the state and lifecycle of a single Snowpipe Streaming channel.
///
/// Channel is a conduit via which we push data to Snowflake system.
#[derive(Debug)]
pub(crate) struct ChannelHandle<C> {
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

    /// Last durable status observed for this channel.
    progress: ChannelProgress,

    /// Continuation token for the next API call on this channel.
    continuation_token: Option<String>,

    /// Interval between Snowflake channel commit-status checks.
    poll_interval: Duration,

    /// Maximum time to wait for Snowflake commit proof.
    wait_timeout: Duration,
}

impl<C> Clone for ChannelHandle<C> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            database: self.database.clone(),
            schema: self.schema.clone(),
            table: self.table.clone(),
            channel: self.channel.clone(),
            progress: self.progress.clone(),
            continuation_token: self.continuation_token.clone(),
            poll_interval: self.poll_interval,
            wait_timeout: self.wait_timeout,
        }
    }
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
        let channel = format!("supabase_etl_{pipeline}_{schema}_{table}_ch0");
        Self {
            client,
            database,
            schema,
            table,
            channel,
            progress: ChannelProgress::default(),
            continuation_token: None,
            poll_interval: DEFAULT_COMMIT_POLL_INTERVAL,
            wait_timeout: DEFAULT_COMMIT_WAIT_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_wait_policy(mut self, poll_interval: Duration, wait_timeout: Duration) -> Self {
        self.poll_interval = poll_interval;
        self.wait_timeout = wait_timeout;
        self
    }

    /// Open or reopen the channel without discarding uncommitted rows.
    pub async fn open(&mut self) -> Result<ChannelStatusResponse> {
        let deadline = Instant::now() + self.wait_timeout;

        loop {
            match self
                .client
                .open_channel(&self.database, &self.schema, &self.table, &self.channel)
                .await
            {
                Ok(response) => {
                    self.progress.observe(&response.status);
                    self.continuation_token = Some(response.continuation_token);
                    return Ok(response.status);
                }
                Err(error @ Error::Snowpipe(SnowpipeError::ChannelHasUncommittedRows)) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }

                    warn!(
                        table = %self.table,
                        channel = %self.channel,
                        "waiting for Snowflake channel rows to commit before reopening"
                    );
                    sleep(self.poll_interval).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Drop the channel without discarding uncommitted rows.
    pub async fn drop_channel(&mut self) -> Result<()> {
        let deadline = Instant::now() + self.wait_timeout;

        loop {
            match self
                .client
                .drop_channel(&self.database, &self.schema, &self.table, &self.channel)
                .await
            {
                Ok(()) => {
                    self.progress = ChannelProgress::default();
                    self.continuation_token = None;
                    return Ok(());
                }
                Err(error @ Error::Snowpipe(SnowpipeError::ChannelHasUncommittedRows)) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }

                    warn!(
                        table = %self.table,
                        channel = %self.channel,
                        "waiting for Snowflake channel rows to commit before dropping"
                    );
                    sleep(self.poll_interval).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Drop and reopen the channel, resetting offsets.
    pub async fn reset(&mut self) -> Result<()> {
        self.drop_channel().await?;
        self.open().await?;
        Ok(())
    }

    /// Returns whether `offset` has already been committed.
    pub fn is_offset_committed(&self, offset: &OffsetToken) -> bool {
        self.progress.is_committed(offset)
    }

    /// Refreshes channel status and returns the latest status.
    pub async fn refresh_status(&mut self) -> Result<ChannelStatusResponse> {
        let status = self
            .client
            .channel_status(&self.database, &self.schema, &self.table, &self.channel)
            .await?;
        self.progress.observe(&status);
        Ok(status)
    }

    /// Fetches the latest committed offset and updates cached channel progress.
    pub async fn fetch_committed_offset(&mut self) -> Result<Option<OffsetToken>> {
        self.refresh_status().await.map(|status| status.offset_token)
    }

    /// Send all batches with immediate append-ack semantics.
    ///
    /// This is kept for table-copy callers. Copy rows do not carry stable WAL
    /// offsets, so streaming committed-offset filtering and deferred
    /// durability tracking must not be applied here (just yet).
    pub async fn send_table_copy_batches(&mut self, batches: Vec<RowBatch>) -> Result<()> {
        for batch in &batches {
            histogram!(ETL_SNOWFLAKE_BATCH_SIZE).record(batch.row_count() as f64);
            histogram!(ETL_SNOWFLAKE_BATCH_BYTES).record(batch.size() as f64);

            match self.append_batch(batch).await {
                Ok(()) => {}
                Err(Error::Snowpipe(error)) if error.is_reopenable_channel_error() => {
                    counter!(ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL).increment(1);
                    warn!(
                        table = %self.table,
                        channel = %self.channel,
                        "channel was stale, reopening and retrying insert"
                    );
                    self.open().await?;
                    self.append_batch(batch).await?;
                }
                Err(error) => {
                    counter!(ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL).increment(1);
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    /// Accept streaming batches, returning accepted-but-not-durable metadata.
    pub async fn accept_streaming_batches(
        &mut self,
        batches: Vec<RowBatch>,
    ) -> Result<Vec<AcceptedRowBatch>> {
        let mut accepted = Vec::new();
        for batch in &batches {
            match self.accept_streaming_batch(batch).await? {
                BatchAcceptance::Accepted(batch) => accepted.push(batch),
                BatchAcceptance::AlreadyCommitted => {}
            }
        }

        Ok(accepted)
    }

    async fn accept_streaming_batch(&mut self, batch: &RowBatch) -> Result<BatchAcceptance> {
        if self.progress.is_committed(batch.end_offset()) {
            return Ok(BatchAcceptance::AlreadyCommitted);
        }

        if self.progress.is_committed(batch.start_offset()) {
            return Err(Error::Channel(format!(
                "Snowflake batch {}..={} overlaps committed offset {:?}; replay filtering should \
                 remove committed rows before batching.",
                batch.start_offset(),
                batch.end_offset(),
                self.progress.committed_offset
            )));
        }

        let baseline_rows_inserted = self.progress.rows_inserted;
        let baseline_rows_error_count = self.progress.rows_error_count;

        histogram!(ETL_SNOWFLAKE_BATCH_SIZE).record(batch.row_count() as f64);
        histogram!(ETL_SNOWFLAKE_BATCH_BYTES).record(batch.size() as f64);

        match self.append_batch(batch).await {
            Ok(()) => Ok(BatchAcceptance::Accepted(AcceptedRowBatch::from_row_batch(
                batch,
                baseline_rows_inserted,
                baseline_rows_error_count,
            )?)),
            Err(Error::Snowpipe(SnowpipeError::StaleContinuation)) => {
                counter!(ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL).increment(1);
                warn!(
                    table = %self.table,
                    channel = %self.channel,
                    "channel was stale, reopening and retrying insert"
                );
                let status = self.open().await?;

                if status
                    .offset_token
                    .as_ref()
                    .is_some_and(|committed| committed >= batch.end_offset())
                {
                    let accepted = AcceptedRowBatch::from_row_batch(
                        batch,
                        baseline_rows_inserted,
                        baseline_rows_error_count,
                    )?;
                    validate_committed_status(&status, &accepted)?;
                    return Ok(BatchAcceptance::AlreadyCommitted);
                }

                if status
                    .offset_token
                    .as_ref()
                    .is_some_and(|committed| committed >= batch.start_offset())
                {
                    return Err(Error::Channel(format!(
                        "Snowflake stale-channel recovery found committed offset {:?} inside \
                         batch {}..={}; failing closed for upstream replay.",
                        self.progress.committed_offset,
                        batch.start_offset(),
                        batch.end_offset()
                    )));
                }

                let baseline_rows_inserted = self.progress.rows_inserted;
                let baseline_rows_error_count = self.progress.rows_error_count;
                self.append_batch(batch).await?;

                Ok(BatchAcceptance::Accepted(AcceptedRowBatch::from_row_batch(
                    batch,
                    baseline_rows_inserted,
                    baseline_rows_error_count,
                )?))
            }
            Err(error) => {
                counter!(ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL).increment(1);
                Err(error)
            }
        }
    }

    async fn append_batch(&mut self, batch: &RowBatch) -> Result<()> {
        let ct = self.continuation_token.as_deref().ok_or_else(|| {
            Error::Channel("append_batch called on channel without continuation token".into())
        })?;

        let response = self
            .client
            .insert_rows(&self.database, &self.schema, &self.table, &self.channel, batch, ct)
            .await?;

        self.continuation_token = Some(response.continuation_token);

        Ok(())
    }
}

/// Validates that a channel status has not rejected rows for an accepted range.
pub(crate) fn validate_committed_status(
    status: &ChannelStatusResponse,
    accepted: &AcceptedRowBatch,
) -> Result<()> {
    if status.rows_error_count > accepted.baseline_rows_error_count {
        return Err(Error::Channel(format!(
            "Snowflake channel {} rejected rows while committing offset {}.",
            status.channel, accepted.target_offset
        )));
    }

    if status.offset_token.as_ref().is_some_and(|committed| committed >= &accepted.target_offset) {
        let expected_rows_inserted =
            accepted.baseline_rows_inserted.checked_add(accepted.rows).ok_or_else(|| {
                Error::Channel("Snowflake expected inserted row count overflowed.".into())
            })?;
        if status.rows_inserted < expected_rows_inserted {
            return Err(Error::Channel(format!(
                "Snowflake channel {} committed offset {} without inserting all accepted rows: \
                 expected at least {expected_rows_inserted}, got {}.",
                status.channel, accepted.target_offset, status.rows_inserted
            )));
        }
    }

    Ok(())
}
