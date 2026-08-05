use std::{sync::Arc, time::Duration};

use etl::{pipeline::PipelineId, schema::PgLsn};
use metrics::{counter, histogram};
use tokio::time::{Instant, sleep};
use tracing::warn;

use crate::snowflake::{
    Error, OffsetToken, Result, RowBatch, SnowpipeError, StreamClient,
    metrics::{
        ETL_SNOWFLAKE_ACCEPTED_BATCHES_TOTAL, ETL_SNOWFLAKE_ACCEPTED_ROWS_TOTAL,
        ETL_SNOWFLAKE_APPEND_DURATION_SECONDS, ETL_SNOWFLAKE_BATCH_BYTES, ETL_SNOWFLAKE_BATCH_SIZE,
        ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL, ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
        ETL_SNOWFLAKE_REJECTED_ROWS_TOTAL, FAILURE_TYPE_LABEL,
    },
    streaming::ChannelStatusResponse,
};

/// Interval between Snowflake channel commit-status checks.
///
/// Defines how often do we retry/check.
/// Durability barriers use this between status polls. Safe open/drop uses it
/// between retries after Snowflake reports uncommitted channel data.
pub(crate) const DEFAULT_COMMIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Local polling and recovery budget for waiting until accepted rows commit.
///
/// Once this deadline is reached, the destination starts no new channel
/// operation. An operation already in progress may finish its internal retries
/// so an Open response and its continuation token are not abandoned.
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

/// Maximum channel reopens attempted for one logical append.
///
/// Reopening refreshes transiently stale continuation state, but a competing
/// writer can invalidate every retry. Bound recovery so a persistent ownership
/// conflict fails closed instead of stalling replication indefinitely.
const MAX_CHANNEL_RECOVERY_ATTEMPTS: usize = 3;

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
    /// Snowflake's millisecond channel-creation timestamp captured when this
    /// row batch was accepted.
    pub channel_created_on_ms: u64,
}

impl AcceptedRowBatch {
    fn from_row_batch(
        batch: &RowBatch,
        baseline_rows_inserted: u64,
        channel_created_on_ms: u64,
    ) -> Result<Self> {
        Ok(Self {
            target_offset: batch.end_offset().clone(),
            rows: u64::try_from(batch.row_count()).map_err(|_| {
                Error::Channel("Snowflake row batch row count overflowed u64.".into())
            })?,
            bytes: batch.size(),
            baseline_rows_inserted,
            channel_created_on_ms,
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
    /// Channel-creation timestamp shared by all accepted batches in this
    /// target.
    pub channel_created_on_ms: u64,
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
            channel_created_on_ms: batch.channel_created_on_ms,
        }
    }

    /// Extends this target with a later batch from the same channel while
    /// preserving the original channel-status baseline.
    pub(crate) fn record(&mut self, batch: AcceptedRowBatch) -> Result<()> {
        if self.channel_created_on_ms != batch.channel_created_on_ms {
            return Err(Error::Channel(
                "Snowflake channel lineage changed while durability was pending.".into(),
            ));
        }

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
}

/// Validation policy and evidence for one channel-progress observation.
#[derive(Clone, Copy)]
enum ProgressObservation<'a> {
    /// Ordinary open or status refresh, which must advance monotonically.
    Monotonic,
    /// Open at an explicit caller-owned offset, which may reposition progress.
    ExplicitOffset(&'a OffsetToken),
}

/// Latest validated progress observed for a Snowflake channel.
#[derive(Debug, Clone, Default)]
struct ChannelProgress {
    /// Last committed offset reported by Snowflake.
    committed_offset: Option<OffsetToken>,
    /// Cumulative inserted rows reported by Snowflake.
    rows_inserted: u64,
    /// Cumulative parsed rows reported by Snowflake.
    rows_parsed: u64,
    /// Last cumulative row-error count observed in this process.
    rows_error_count: Option<u64>,
    /// Last known channel creation timestamp, in milliseconds.
    ///
    /// A different timestamp indicates that the named channel was recreated.
    /// Missing timestamps do not replace the last known value.
    created_on_ms: Option<u64>,
}

impl ChannelProgress {
    /// Validates and records a channel-status observation.
    ///
    /// Monotonic observations must remain in the same lineage and advance.
    ///
    /// An explicit-offset observation may reposition progress and replace the
    /// lineage. Counters may reset only when the lineage is known to have
    /// changed.
    ///
    /// Rejected-row observations update only the cumulative error baseline
    /// before returning an error so repeated polls do not recount the same
    /// rows.
    fn observe(
        &mut self,
        status: &ChannelStatusResponse,
        observation: ProgressObservation<'_>,
    ) -> Result<()> {
        if status.rows_error_count > 0 {
            if let Some(previous) = self.rows_error_count.replace(status.rows_error_count)
                && let Some(rejected_rows) = status.rows_error_count.checked_sub(previous)
                && rejected_rows > 0
            {
                counter!(ETL_SNOWFLAKE_REJECTED_ROWS_TOTAL).increment(rejected_rows);
            }
            let boundary = status
                .last_error_offset_upper_bound
                .as_ref()
                .map(|offset| format!(" at or before offset {offset}"))
                .unwrap_or_default();
            return Err(Error::Channel(format!(
                "Snowflake channel {} reports {} rejected rows{boundary}; reset or resync is \
                 required.",
                status.channel, status.rows_error_count
            )));
        }

        // Snowflake does not document creation timestamps as unique.
        // A changed value proves replacement, equal values still require the offset and
        // counter checks below.
        let lineage_changed = matches!(
            (self.created_on_ms, status.created_on_ms),
            (Some(previous), Some(current)) if previous != current
        );

        match observation {
            ProgressObservation::Monotonic => {
                if lineage_changed {
                    return Err(Error::Channel(format!(
                        "Snowflake channel {} changed lineage while writes may require replay.",
                        status.channel
                    )));
                }
                if let Some(previous) = self.committed_offset.as_ref()
                    && status.offset_token.as_ref().is_none_or(|current| current < previous)
                {
                    return Err(Error::Channel(format!(
                        "Snowflake channel {} committed offset moved backward during normal \
                         recovery.",
                        status.channel
                    )));
                }
                if status.rows_inserted < self.rows_inserted {
                    return Err(Error::Channel(format!(
                        "Snowflake channel {} inserted-row counter moved backward.",
                        status.channel
                    )));
                }
                if status.rows_parsed < self.rows_parsed {
                    return Err(Error::Channel(format!(
                        "Snowflake channel {} parsed-row counter moved backward.",
                        status.channel
                    )));
                }
            }
            ProgressObservation::ExplicitOffset(expected_offset) => {
                if status.offset_token.as_ref() != Some(expected_offset) {
                    return Err(Error::Channel(format!(
                        "Snowflake reopened channel {} at offset {:?}, expected {expected_offset}.",
                        status.channel, status.offset_token
                    )));
                }
                if lineage_changed {
                    if status.rows_inserted != 0 || status.rows_parsed != 0 {
                        return Err(Error::Channel(format!(
                            "Snowflake channel {} replacement lineage is not empty after opening \
                             with an explicit offset.",
                            status.channel
                        )));
                    }
                } else if status.rows_inserted != self.rows_inserted
                    || status.rows_parsed != self.rows_parsed
                {
                    return Err(Error::Channel(format!(
                        "Snowflake channel {} row counters changed while opening with an explicit \
                         offset.",
                        status.channel
                    )));
                }
            }
        }

        self.committed_offset = status.offset_token.clone();
        self.rows_inserted = status.rows_inserted;
        self.rows_parsed = status.rows_parsed;
        self.rows_error_count = Some(status.rows_error_count);
        if status.created_on_ms.is_some() {
            self.created_on_ms = status.created_on_ms;
        }

        Ok(())
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
    /// Channel recovery found the batch's end offset committed after an
    /// ambiguous append response.
    ///
    /// The batch must still participate in cumulative durability accounting:
    /// another pending batch can otherwise make its row counter appear
    /// satisfied before Snowflake has exposed all of this batch's rows.
    Recovered(AcceptedRowBatch),
    /// The batch was already committed before the retry path resent it.
    AlreadyCommitted,
}

impl BatchAcceptance {
    /// Returns metadata that must remain in pending durability accounting.
    fn into_pending_batch(self) -> Option<AcceptedRowBatch> {
        match self {
            Self::Accepted(batch) | Self::Recovered(batch) => Some(batch),
            Self::AlreadyCommitted => None,
        }
    }
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

    /// Latest validated progress observed for this channel.
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

    /// Opens or reopens the channel while preserving Snowflake's stored offset.
    ///
    /// Opening fences older continuation tokens. If rows are still in flight,
    /// the request is retried rather than allowing Snowflake to discard them.
    pub async fn open(&mut self) -> Result<ChannelStatusResponse> {
        self.open_with_deadline(None, Instant::now() + self.wait_timeout).await
    }

    /// Opens or reopens the channel at an explicit caller-owned offset.
    ///
    /// Snowflake replaces its stored token with the `offset`, it does not
    /// compare token ordering, alter table rows, or prevent duplicate
    /// ingestion. Callers must therefore coordinate this operation with their
    /// source position and destination state. The returned offset is verified
    /// against the requested value.
    ///
    /// Opening fences older continuation tokens. If rows are still in flight,
    /// the request is retried rather than allowing Snowflake to discard them.
    pub async fn open_at(&mut self, offset: &OffsetToken) -> Result<ChannelStatusResponse> {
        self.open_with_deadline(Some(offset), Instant::now() + self.wait_timeout).await
    }

    /// Opens the channel within an existing operation deadline.
    async fn open_with_deadline(
        &mut self,
        offset: Option<&OffsetToken>,
        deadline: Instant,
    ) -> Result<ChannelStatusResponse> {
        let observation = match offset {
            Some(expected) => ProgressObservation::ExplicitOffset(expected),
            None => ProgressObservation::Monotonic,
        };

        loop {
            if Instant::now() >= deadline {
                return Err(Error::Channel(format!(
                    "Timed out waiting to open Snowflake channel {}.",
                    self.channel
                )));
            }

            match self
                .client
                .open_channel(&self.database, &self.schema, &self.table, &self.channel, offset)
                .await
            {
                Ok(response) => {
                    if response.status.created_on_ms.is_none() {
                        return Err(Error::Channel(format!(
                            "Snowflake open response for channel {} omitted its creation \
                             timestamp.",
                            self.channel
                        )));
                    }
                    if response.status.rows_error_count == 0
                        && response.status.rows_parsed != response.status.rows_inserted
                    {
                        return Err(Error::Channel(format!(
                            "Snowflake channel {} returned inconsistent row counters after \
                             opening.",
                            self.channel
                        )));
                    }
                    self.progress.observe(&response.status, observation)?;
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
                        "waiting for snowflake channel rows to commit before reopening"
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
                        "waiting for snowflake channel rows to commit before dropping"
                    );
                    sleep(self.poll_interval).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Returns whether `offset` has already been committed.
    pub fn is_offset_committed(&self, offset: &OffsetToken) -> bool {
        self.progress.is_committed(offset)
    }

    /// Refreshes channel status within `deadline`.
    async fn refresh_status(&mut self, deadline: Instant) -> Result<ChannelStatusResponse> {
        if Instant::now() >= deadline {
            return Err(Error::Channel(format!(
                "Timed out waiting for Snowflake channel {} status.",
                self.channel
            )));
        }

        let status = match self
            .client
            .channel_status(&self.database, &self.schema, &self.table, &self.channel)
            .await
        {
            Ok(status) => status,
            Err(Error::Snowpipe(error)) if error.is_reopenable_channel_error() => {
                counter!(ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL).increment(1);
                warn!(
                    table = %self.table,
                    channel = %self.channel,
                    error = %error,
                    "channel status requires reopen"
                );
                return self.open_with_deadline(None, deadline).await;
            }
            Err(error) => return Err(error),
        };
        self.progress.observe(&status, ProgressObservation::Monotonic)?;
        Ok(status)
    }

    /// Checks whether the latest channel status proves that `target` is
    /// durable.
    ///
    /// An invalid channel is reopened for recovery before its status is
    /// checked.
    pub(crate) async fn check_durability(
        &mut self,
        target: &PendingDurabilityTarget,
        deadline: Instant,
    ) -> Result<bool> {
        let status = self.refresh_status(deadline).await?;
        if self.copy_barrier_pending
            && let Some(committed) = status.offset_token.as_ref()
        {
            self.validate_copy_committed_offset(committed)?;
        }

        status_proves_durability(&status, target)
    }

    /// Fetches the latest committed offset and updates cached channel progress.
    pub async fn fetch_committed_offset(&mut self) -> Result<Option<OffsetToken>> {
        let deadline = Instant::now() + self.wait_timeout;
        self.refresh_status(deadline).await.map(|status| status.offset_token)
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

            if let Some(accepted) = self.accept_batch(&batch).await?.into_pending_batch() {
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
            if let Some(batch) = self.accept_batch(batch).await?.into_pending_batch() {
                accepted.push(batch);
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

        loop {
            if self.check_durability(&target, deadline).await? {
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

        let started_at = Instant::now();
        let result = self.accept_batch_inner(batch).await;

        histogram!(ETL_SNOWFLAKE_APPEND_DURATION_SECONDS)
            .record(started_at.elapsed().as_secs_f64());

        match &result {
            Ok(BatchAcceptance::Accepted(accepted)) => {
                counter!(ETL_SNOWFLAKE_ACCEPTED_BATCHES_TOTAL).increment(1);
                counter!(ETL_SNOWFLAKE_ACCEPTED_ROWS_TOTAL).increment(accepted.rows);
            }
            Ok(BatchAcceptance::Recovered(_) | BatchAcceptance::AlreadyCommitted) => {}
            Err(error) => {
                counter!(
                    ETL_SNOWFLAKE_INSERT_ERRORS_TOTAL,
                    FAILURE_TYPE_LABEL => error.append_failure_type().as_str(),
                )
                .increment(1);
            }
        }

        result
    }

    /// Performs one logical append after cached replay filtering.
    async fn accept_batch_inner(&mut self, batch: &RowBatch) -> Result<BatchAcceptance> {
        if self.progress.is_committed(batch.start_offset()) {
            return Err(Error::Channel(format!(
                "Snowflake batch {}..={} overlaps committed offset {:?}; replay filtering should \
                 remove committed rows before batching.",
                batch.start_offset(),
                batch.end_offset(),
                self.progress.committed_offset
            )));
        }

        let original_accepted = self.accepted_batch_from_progress(batch)?;
        let mut recovery_attempts = 0;

        histogram!(ETL_SNOWFLAKE_BATCH_SIZE).record(batch.row_count() as f64);
        histogram!(ETL_SNOWFLAKE_BATCH_BYTES).record(batch.size() as f64);

        loop {
            let accepted = self.accepted_batch_from_progress(batch)?;
            match self.append_batch(batch).await {
                Ok(()) => return Ok(BatchAcceptance::Accepted(accepted)),
                Err(Error::Snowpipe(error)) if error.is_reopenable_channel_error() => {
                    if recovery_attempts >= MAX_CHANNEL_RECOVERY_ATTEMPTS {
                        return Err(Error::Channel(format!(
                            "Snowflake channel {} remained invalid after {} recovery attempts; \
                             another writer may own the channel.",
                            self.channel, MAX_CHANNEL_RECOVERY_ATTEMPTS
                        )));
                    }
                    recovery_attempts += 1;
                    counter!(ETL_SNOWFLAKE_CHANNEL_RECOVERIES_TOTAL).increment(1);
                    warn!(
                        table = %self.table,
                        channel = %self.channel,
                        recovery_attempt = recovery_attempts,
                        error = %error,
                        "channel requires reopen before retrying insert"
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
                        return Ok(BatchAcceptance::Recovered(original_accepted));
                    }

                    if status
                        .offset_token
                        .as_ref()
                        .is_some_and(|committed| committed >= batch.start_offset())
                    {
                        return Err(Error::Channel(format!(
                            "Snowflake channel recovery found committed offset {:?} inside batch \
                             {}..={}; failing closed for upstream replay.",
                            self.progress.committed_offset,
                            batch.start_offset(),
                            batch.end_offset()
                        )));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Builds durability metadata against the currently observed channel state.
    fn accepted_batch_from_progress(&self, batch: &RowBatch) -> Result<AcceptedRowBatch> {
        let channel_created_on_ms = self.progress.created_on_ms.ok_or_else(|| {
            Error::Channel(format!(
                "Snowflake channel {} has no observed creation timestamp.",
                self.channel
            ))
        })?;

        AcceptedRowBatch::from_row_batch(batch, self.progress.rows_inserted, channel_created_on_ms)
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

/// Returns whether channel status proves an accepted range is fully durable.
///
/// Snowflake may expose the committed offset before cumulative row counters
/// converge. That state remains pending until a later status observation.
fn status_proves_durability(
    status: &ChannelStatusResponse,
    target: &PendingDurabilityTarget,
) -> Result<bool> {
    if status.created_on_ms.is_some_and(|observed| target.channel_created_on_ms != observed) {
        return Err(Error::Channel(format!(
            "Snowflake channel {} changed lineage while rows were awaiting durability.",
            status.channel
        )));
    }

    if status.rows_error_count > 0 {
        return Err(Error::Channel(format!(
            "Snowflake channel {} rejected rows while committing offset {}.",
            status.channel, target.target_offset
        )));
    }

    if status.offset_token.as_ref().is_none_or(|committed| committed < &target.target_offset) {
        return Ok(false);
    }

    let expected_rows_inserted =
        target.baseline_rows_inserted.checked_add(target.rows).ok_or_else(|| {
            Error::Channel("Snowflake expected inserted row count overflowed.".into())
        })?;

    Ok(status.rows_inserted >= expected_rows_inserted)
}
