use etl_postgres::types::ColumnSchema;
use etl_postgres::types::POSTGRES_EPOCH;
use futures::{Stream, ready};
use pin_project_lite::pin_project;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio_postgres::CopyOutStream;
use tokio_postgres::types::PgLsn;
use tracing::debug;

use crate::conversions::table_row::parse_table_row_from_postgres_copy_bytes;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;
use crate::metrics::{
    ETL_BYTES_PROCESSED_TOTAL, ETL_STATUS_UPDATES_SKIPPED_TOTAL, ETL_STATUS_UPDATES_TOTAL,
    EVENT_TYPE_LABEL, FORCED_LABEL, PIPELINE_ID_LABEL,
};
use crate::types::{PipelineId, TableRow};
use metrics::counter;

/// The amount of milliseconds between two consecutive status updates in case no forced update
/// is requested.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

pin_project! {
    /// A stream that yields rows from a Postgres COPY operation.
    ///
    /// This stream wraps a [`CopyOutStream`] and converts each row into a [`TableRow`]
    /// using the provided column schemas. The conversion process handles both text and
    /// binary format data.
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream<'a> {
        #[pin]
        stream: CopyOutStream,
        column_schemas: &'a [ColumnSchema],
        pipeline_id: PipelineId,
    }
}

impl<'a> TableCopyStream<'a> {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column schemas.
    ///
    /// The column schemas are used to convert the raw Postgres data into [`TableRow`]s.
    pub fn wrap(
        stream: CopyOutStream,
        column_schemas: &'a [ColumnSchema],
        pipeline_id: PipelineId,
    ) -> Self {
        Self {
            stream,
            column_schemas,
            pipeline_id,
        }
    }
}

impl<'a> Stream for TableCopyStream<'a> {
    type Item = EtlResult<TableRow>;

    /// Polls the stream for the next converted table row with comprehensive error handling.
    ///
    /// This method handles the complex process of converting raw Postgres COPY data into
    /// structured [`TableRow`] objects, with detailed error reporting for various failure modes.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            // TODO: allow pluggable table row conversion based on if the data is in text or binary format.
            Some(Ok(row)) => {
                counter!(
                    ETL_BYTES_PROCESSED_TOTAL,
                    PIPELINE_ID_LABEL => this.pipeline_id.to_string(),
                    EVENT_TYPE_LABEL => "copy"
                )
                .increment(row.len() as u64);

                // CONVERSION PHASE: Transform raw bytes into structured TableRow
                // This is where most errors occur due to data format or type issues
                match parse_table_row_from_postgres_copy_bytes(&row, this.column_schemas) {
                    Ok(row) => Poll::Ready(Some(Ok(row))),
                    Err(err) => {
                        // CONVERSION ERROR: Preserve full error context for debugging
                        // These errors typically indicate schema mismatches or data corruption
                        Poll::Ready(Some(Err(err)))
                    }
                }
            }
            Some(Err(err)) => {
                // PROTOCOL ERROR: Postgres connection or protocol-level failure
                // Convert tokio-postgres errors to ETL errors with additional context
                Poll::Ready(Some(Err(err.into())))
            }
            None => {
                // STREAM END: Normal completion - no more rows available
                // This is the success termination condition for table copying
                Poll::Ready(None)
            }
        }
    }
}

/// The status update type when sending a status update message back to Postgres.
#[derive(Debug)]
pub enum StatusUpdateType {
    /// Represents an update requested by Postgres.
    KeepAlive,
    /// Represents an update sent by ETL on its own.
    Timeout,
}

impl Display for StatusUpdateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeepAlive => write!(f, "keep_alive"),
            Self::Timeout => write!(f, "timeout"),
        }
    }
}

pin_project! {
    /// A stream that yields replication events from a Postgres logical replication stream and keeps
    /// track of last sent status updates.
    pub struct EventsStream {
        #[pin]
        stream: LogicalReplicationStream,
        last_update: Option<Instant>,
        last_write_lsn: Option<PgLsn>,
        last_flush_lsn: Option<PgLsn>,
        pipeline_id: PipelineId,
    }
}

impl EventsStream {
    /// Creates a new [`EventsStream`] from a [`LogicalReplicationStream`].
    pub fn wrap(stream: LogicalReplicationStream, pipeline_id: PipelineId) -> Self {
        Self {
            stream,
            last_update: None,
            last_write_lsn: None,
            last_flush_lsn: None,
            pipeline_id,
        }
    }

    /// Sends a status update to the Postgres server.
    ///
    /// This method implements a status update logic that balances Postgres's need for
    /// progress information with network efficiency and system performance. It handles multiple
    /// error scenarios and edge cases related to time synchronization and network communication.
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        mut write_lsn: PgLsn,
        mut flush_lsn: PgLsn,
        force: bool,
        status_update_type: StatusUpdateType,
    ) -> EtlResult<()> {
        let this = self.project();
        let pipeline_id = *this.pipeline_id;

        // If the new write lsn is less than the last one, we can safely ignore it, since we only want
        // to report monotonically increasing values.
        if let Some(last_write_lsn) = this.last_write_lsn
            && write_lsn < *last_write_lsn
        {
            write_lsn = *last_write_lsn;
        }

        // If the new flush lsn is less than the last one, we can safely ignore it, since we only want
        // to report monotonically increasing values.
        if let Some(last_flush_lsn) = this.last_flush_lsn
            && flush_lsn < *last_flush_lsn
        {
            flush_lsn = *last_flush_lsn;
        }

        // If we are not forced to send an update, we can willingly do so based on a set of conditions.
        if !force
            && let (Some(last_update), Some(last_flush)) =
                (this.last_update.as_mut(), this.last_flush_lsn.as_mut())
        {
            // The reason for only checking `flush_lsn` and `apply_lsn` is that if we are not
            // forced to send a status update to Postgres (when reply is requested), we want to just
            // notify it in case we actually durably flushed and persisted events, which is signaled via
            // the two aforementioned fields. Postgres mostly uses the 'write_lsn' field for
            // tracking what was received by the replication client but not what the client actually
            // safely stored.
            //
            // If we were to check `write_lsn` too, we would end up sending updates more frequently
            // when they are not requested, simply because the `write_lsn` is updated for every
            // incoming message in the apply loop.
            if flush_lsn == *last_flush && last_update.elapsed() < STATUS_UPDATE_INTERVAL {
                counter!(
                    ETL_STATUS_UPDATES_SKIPPED_TOTAL,
                    PIPELINE_ID_LABEL => pipeline_id.to_string(),
                )
                .increment(1);

                debug!(
                    last_flush = %flush_lsn,
                    last_update_elapsed_secs = last_update.elapsed().as_secs(),
                    status_update_type = %status_update_type,
                    "skipping status update due to unforced reply and recent update"
                );

                return Ok(());
            }
        }

        // The client's system clock at the time of transmission, as microseconds since midnight
        // on 2000-01-01.
        let ts = POSTGRES_EPOCH
            .elapsed()
            .map_err(|e| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Invalid PostgreSQL epoch",
                    e.to_string()
                )
            })?
            .as_micros() as i64;

        // We will send the `flush_lsn` as `apply_lsn` since in our case, we don't distinguish between
        // them as Postgres does. The reason is that `apply_lsn` is used to mark when an LSN is both
        // durable and visible, but from ETL's perspective we are fine with just it being durable, which
        // is marked via the `flush_lsn`.
        this.stream
            .standby_status_update(write_lsn, flush_lsn, flush_lsn, ts, 0)
            .await?;

        counter!(
            ETL_STATUS_UPDATES_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string(),
            FORCED_LABEL => force.to_string(),
        )
        .increment(1);

        debug!(
            write_lsn = %write_lsn,
            flush_lsn = %flush_lsn,
            apply_lsn = %flush_lsn,
            force = force,
            status_update_type = %status_update_type,
            "status update successfully sent"
        );

        // Update the state after successful send.
        *this.last_update = Some(Instant::now());
        *this.last_write_lsn = Some(write_lsn);
        *this.last_flush_lsn = Some(flush_lsn);

        Ok(())
    }
}

impl Stream for EventsStream {
    type Item = EtlResult<ReplicationMessage<LogicalReplicationMessage>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
