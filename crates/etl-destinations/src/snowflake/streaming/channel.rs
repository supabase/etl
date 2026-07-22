use std::{sync::Arc, time::Duration};

use etl::{pipeline::PipelineId, schema::PgLsn};
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

/// Maximum pending table-copy row batches before a durability wait.
///
/// This matches streaming's 64-batch bound and forces periodic waits when
/// small batches do not reach the byte bound.
const COPY_PENDING_MAX_ROW_BATCHES: usize = 64;

/// Maximum pending compressed table-copy bytes before a durability wait.
///
/// This matches streaming's 256 MiB bound and limits unconfirmed data for
/// large batches. It is not a Snowflake service limit.
const COPY_PENDING_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Row batch accepted by a Snowpipe channel.
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

/// Collapsed durability target for one Snowpipe channel.
///
/// Snowflake committed offsets are cumulative, so multiple accepted row
/// batches can be represented by the latest target offset plus aggregate row
/// and byte counts.
#[derive(Debug, Clone)]
pub(crate) struct PendingDurabilityTarget {
    /// Latest accepted offset for this channel.
    pub target_offset: OffsetToken,
    /// Accepted rows since the baseline status.
    pub rows: u64,
    /// Compressed bytes accepted since the baseline status.
    pub bytes: usize,
    /// Snowflake row batches accepted since the baseline status.
    pub row_batches: usize,
    /// Cumulative inserted rows before the pending range began.
    pub baseline_rows_inserted: u64,
    /// Cumulative row errors before the pending range began.
    pub baseline_rows_error_count: u64,
}

impl PendingDurabilityTarget {
    /// Starts a pending durability target with its first accepted row batch.
    pub(crate) fn new(batch: AcceptedRowBatch) -> Self {
        Self {
            target_offset: batch.target_offset,
            rows: batch.rows,
            bytes: batch.bytes,
            row_batches: 1,
            baseline_rows_inserted: batch.baseline_rows_inserted,
            baseline_rows_error_count: batch.baseline_rows_error_count,
        }
    }

    /// Extends this target with a later batch from the same channel while
    /// preserving the original channel-status baseline.
    pub(crate) fn record(&mut self, batch: AcceptedRowBatch) -> Result<()> {
        let rows = self
            .rows
            .checked_add(batch.rows)
            .ok_or_else(|| Error::Channel("Snowflake pending row count overflowed.".into()))?;
        let bytes = self
            .bytes
            .checked_add(batch.bytes)
            .ok_or_else(|| Error::Channel("Snowflake pending byte count overflowed.".into()))?;
        let row_batches = self
            .row_batches
            .checked_add(1)
            .ok_or_else(|| Error::Channel("Snowflake pending batch count overflowed.".into()))?;

        self.target_offset = batch.target_offset;
        self.rows = rows;
        self.bytes = bytes;
        self.row_batches = row_batches;

        Ok(())
    }

    /// Returns whether accepting another batch of `batch_bytes` bytes requires
    /// waiting for this target to become durable first.
    fn would_exceed_limits(&self, batch_bytes: usize) -> bool {
        self.row_batches >= COPY_PENDING_MAX_ROW_BATCHES
            || self.bytes >= COPY_PENDING_MAX_BYTES
            || self.row_batches.saturating_add(1) > COPY_PENDING_MAX_ROW_BATCHES
            || self.bytes.saturating_add(batch_bytes) > COPY_PENDING_MAX_BYTES
    }

    /// Returns aggregate batch metadata for validating this target against
    /// channel status.
    pub(crate) fn as_accepted_batch(&self) -> AcceptedRowBatch {
        AcceptedRowBatch {
            target_offset: self.target_offset.clone(),
            rows: self.rows,
            bytes: self.bytes,
            baseline_rows_inserted: self.baseline_rows_inserted,
            baseline_rows_error_count: self.baseline_rows_error_count,
        }
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

    /// Whether this handle has entered copy writes without completing the
    /// terminal copy durability barrier.
    ///
    /// This remains set across intermediate durability waits. While set,
    /// streaming writes are rejected; a failed copy must reset the channel.
    copy_barrier_pending: bool,

    /// Last synthetic table-copy offset reserved for the current live sequence.
    ///
    /// `None` means no live copy sequence is retained. This cursor is
    /// process-local and must never be used to resume a failed table copy.
    copy_offset_ordinal: Option<u64>,

    /// Cumulative durability target for copy batches accepted by Snowflake but
    /// not yet confirmed committed.
    ///
    /// `None` means there is no outstanding copy durability debt. The copy
    /// attempt may still be active.
    copy_durability_target: Option<PendingDurabilityTarget>,

    /// Continuation token for the next API call on this channel.
    continuation_token: Option<String>,

    /// Interval between Snowflake channel commit-status checks.
    poll_interval: Duration,

    /// Maximum time to wait for Snowflake commit proof.
    wait_timeout: Duration,
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
            copy_offset_ordinal: None,
            copy_barrier_pending: false,
            copy_durability_target: None,
            continuation_token: None,
            poll_interval: DEFAULT_COMMIT_POLL_INTERVAL,
            wait_timeout: DEFAULT_COMMIT_WAIT_TIMEOUT,
        }
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
                    self.copy_offset_ordinal = None;
                    self.copy_barrier_pending = false;
                    self.copy_durability_target = None;
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

    /// Accepts table-copy batches into a bounded deferred-durability window.
    ///
    /// Each encoded batch retains its zero CDC sequence and receives the next
    /// attempt-local `0/N` request offset. Before a batch would exceed the
    /// pending batch-count or byte limit, this method waits for the current
    /// cumulative target to become durable.
    pub async fn accept_table_copy_batches(&mut self, batches: Vec<RowBatch>) -> Result<()> {
        for batch in batches {
            if self
                .copy_durability_target
                .as_ref()
                .is_some_and(|target| target.would_exceed_limits(batch.size()))
            {
                self.wait_for_pending_copy_durability().await?;
            }

            let offset = self.reserve_copy_offset()?;
            let batch = batch.with_request_offset(offset);

            if let BatchAcceptance::Accepted(accepted) = self.accept_batch(&batch).await? {
                match &mut self.copy_durability_target {
                    Some(target) => target.record(accepted)?,
                    None => {
                        self.copy_durability_target = Some(PendingDurabilityTarget::new(accepted));
                    }
                }
            }
        }

        Ok(())
    }

    /// Waits until every table-copy row accepted by this handle is durable.
    ///
    /// Success completes the terminal copy barrier and permits streaming. The
    /// last synthetic offset remains cached until streaming begins, allowing
    /// repeated barrier calls to validate observed channel progress. Failed
    /// copies must reset the table and channel rather than resume from that
    /// offset.
    pub async fn wait_for_table_copy_durability(&mut self) -> Result<()> {
        match self.copy_offset_ordinal {
            Some(_) => {
                if let Some(committed) = self.progress.committed_offset.as_ref() {
                    self.validate_copy_committed_offset(committed)?;
                }
            }
            None if self.progress.committed_offset.is_some() => {
                return Err(Error::Channel(
                    "Snowflake table copy must start from a reset channel.".into(),
                ));
            }
            None => {}
        }

        self.wait_for_pending_copy_durability().await?;
        self.copy_barrier_pending = false;
        Ok(())
    }

    /// Accepts streaming batches when no copy durability barrier is pending.
    ///
    /// Returns metadata for newly accepted batches that are not yet durable;
    /// already committed batches are omitted. Starting streaming retires the
    /// completed copy offset sequence.
    pub async fn accept_streaming_batches(
        &mut self,
        batches: Vec<RowBatch>,
    ) -> Result<Vec<AcceptedRowBatch>> {
        if self.copy_barrier_pending || self.copy_durability_target.is_some() {
            return Err(Error::Channel(
                "Snowflake streaming cannot start before the table-copy durability barrier.".into(),
            ));
        }
        self.copy_offset_ordinal = None;

        let mut accepted = Vec::new();
        for batch in &batches {
            match self.accept_batch(batch).await? {
                BatchAcceptance::Accepted(batch) => accepted.push(batch),
                BatchAcceptance::AlreadyCommitted => {}
            }
        }

        Ok(accepted)
    }

    /// Reserves the next attempt-local `0/N` copy offset and marks the terminal
    /// copy barrier pending.
    fn reserve_copy_offset(&mut self) -> Result<OffsetToken> {
        let ordinal = match self.copy_offset_ordinal {
            Some(ordinal) => {
                if let Some(committed) = self.progress.committed_offset.as_ref() {
                    self.validate_copy_committed_offset(committed)?;
                }
                ordinal.checked_add(1).ok_or_else(|| {
                    Error::Channel("Snowflake table-copy offset ordinal overflowed.".into())
                })?
            }
            None => {
                if self.progress.committed_offset.is_some() {
                    return Err(Error::Channel(
                        "Snowflake table copy must start from a reset channel.".into(),
                    ));
                }
                1
            }
        };

        self.copy_offset_ordinal = Some(ordinal);
        self.copy_barrier_pending = true;
        Ok(OffsetToken::new(PgLsn::from(0_u64), ordinal))
    }

    /// Validates a committed offset against the synthetic range reserved
    /// locally for the current copy sequence.
    fn validate_copy_committed_offset(&self, offset: &OffsetToken) -> Result<()> {
        let last_reserved = self.copy_offset_ordinal.ok_or_else(|| {
            Error::Channel("Snowflake table copy has no live offset sequence.".into())
        })?;
        let (lsn, ordinal) = offset.decode()?;

        if u64::from(lsn) != 0 || ordinal == 0 || ordinal > last_reserved {
            return Err(Error::Channel(format!(
                "Snowflake committed offset {offset} does not belong to the current table-copy \
                 attempt."
            )));
        }

        Ok(())
    }

    /// Waits for valid commit proof for the pending copy target, then clears
    /// it.
    async fn wait_for_pending_copy_durability(&mut self) -> Result<()> {
        let Some(target) = self.copy_durability_target.clone() else {
            return Ok(());
        };
        let deadline = Instant::now() + self.wait_timeout;
        let accepted = target.as_accepted_batch();

        loop {
            let status = self.refresh_status().await?;
            if let Some(committed) = status.offset_token.as_ref() {
                self.validate_copy_committed_offset(committed)?;
            }
            validate_committed_status(&status, &accepted)?;

            if status.offset_token.as_ref().is_some_and(|offset| offset >= &target.target_offset) {
                self.copy_durability_target = None;
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(Error::Channel(
                    "Timed out waiting for Snowflake table-copy rows to commit.".into(),
                ));
            }

            sleep(self.poll_interval).await;
        }
    }

    /// Sends one batch unless cached or refreshed channel progress covers it.
    ///
    /// A stale continuation token reopens the channel before deciding whether
    /// to retry the append.
    async fn accept_batch(&mut self, batch: &RowBatch) -> Result<BatchAcceptance> {
        if self.copy_barrier_pending
            && let Some(committed) = self.progress.committed_offset.as_ref()
        {
            self.validate_copy_committed_offset(committed)?;
        }

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

                if self.copy_barrier_pending
                    && let Some(committed) = status.offset_token.as_ref()
                {
                    self.validate_copy_committed_offset(committed)?;
                }

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
