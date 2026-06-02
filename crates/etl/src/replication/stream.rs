use std::{
    fmt::{Display, Formatter},
    mem::size_of,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::Bytes;
use etl_postgres::types::POSTGRES_EPOCH;
use futures::{Stream, ready};
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};
use tokio_postgres::{CopyOutStream, types::PgLsn};
use tracing::debug;

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    metrics::{
        ETL_BYTES_PROCESSED_TOTAL, ETL_ROW_SIZE_BYTES, ETL_STATUS_UPDATES_SKIPPED_TOTAL,
        ETL_STATUS_UPDATES_TOTAL, EVENT_TYPE_LABEL, FORCED_LABEL, PIPELINE_ID_LABEL,
        STATUS_UPDATE_TYPE_LABEL,
    },
    types::{PipelineId, SizeHint},
};

/// The amount of milliseconds between two consecutive status updates in case no
/// forced update is requested.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

/// Raw row bytes from a Postgres COPY operation.
#[derive(Debug)]
pub(crate) struct TableCopyRowBytes {
    bytes: Bytes,
}

impl TableCopyRowBytes {
    /// Creates raw COPY row bytes.
    fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

impl AsRef<[u8]> for TableCopyRowBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl SizeHint for TableCopyRowBytes {
    fn size_hint(&self) -> usize {
        size_of::<Self>() + self.bytes.len()
    }
}

pin_project! {
    /// A stream that yields raw rows from a Postgres COPY operation.
    #[must_use = "streams do nothing unless polled"]
    pub(crate) struct RawTableCopyStream {
        #[pin]
        stream: CopyOutStream,
        pipeline_id: PipelineId,
    }
}

impl RawTableCopyStream {
    /// Creates a new [`RawTableCopyStream`] from a [`CopyOutStream`].
    pub(crate) fn wrap(stream: CopyOutStream, pipeline_id: PipelineId) -> Self {
        Self { stream, pipeline_id }
    }
}

impl Stream for RawTableCopyStream {
    type Item = EtlResult<TableCopyRowBytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(row)) => {
                let row_size_bytes = row.len() as u64;

                counter!(
                    ETL_BYTES_PROCESSED_TOTAL,
                    PIPELINE_ID_LABEL => this.pipeline_id.to_string(),
                    EVENT_TYPE_LABEL => "copy"
                )
                .increment(row_size_bytes);

                histogram!(
                    ETL_ROW_SIZE_BYTES,
                    PIPELINE_ID_LABEL => this.pipeline_id.to_string(),
                    EVENT_TYPE_LABEL => "copy"
                )
                .record(row_size_bytes as f64);

                Poll::Ready(Some(Ok(TableCopyRowBytes::new(row))))
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

/// The status update type when sending a status update message back to
/// Postgres.
#[derive(Debug)]
pub(super) enum StatusUpdateType {
    /// Represents an update in response to a keep alive from Postgres.
    KeepAlive,
    /// Represents a periodic heartbeat sent while the apply loop is otherwise
    /// idle.
    PeriodicKeepAlive,
    /// Represents an update before shutdown that requires acknowledgement from
    /// Postgres.
    ShutdownFlush,
}

impl StatusUpdateType {
    /// Returns `true` whether this status update type requires a reply from
    /// Postgres, `false` otherwise.
    fn request_reply(&self) -> bool {
        match self {
            Self::KeepAlive => false,
            Self::PeriodicKeepAlive => true,
            Self::ShutdownFlush => true,
        }
    }
}

impl Display for StatusUpdateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeepAlive => write!(f, "keep_alive"),
            Self::PeriodicKeepAlive => write!(f, "periodic_keep_alive"),
            Self::ShutdownFlush => write!(f, "shutdown_flush"),
        }
    }
}

pin_project! {
    /// A stream that yields replication events from a Postgres logical replication stream and keeps
    /// track of last sent status updates.
    pub(super) struct EventsStream {
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
    pub(super) fn wrap(stream: LogicalReplicationStream, pipeline_id: PipelineId) -> Self {
        Self { stream, last_update: None, last_write_lsn: None, last_flush_lsn: None, pipeline_id }
    }

    /// Sends a status update to the Postgres server.
    ///
    /// This method implements a status update logic that balances Postgres's
    /// need for progress information with network efficiency and system
    /// performance. It handles multiple error scenarios and edge cases
    /// related to time synchronization and network communication.
    pub(super) async fn send_status_update(
        self: Pin<&mut Self>,
        mut write_lsn: PgLsn,
        mut flush_lsn: PgLsn,
        force: bool,
        status_update_type: StatusUpdateType,
    ) -> EtlResult<()> {
        let this = self.project();
        let pipeline_id = *this.pipeline_id;

        // If the new write lsn is less than the last one, we can safely ignore it,
        // since we only want to report monotonically increasing values.
        if let Some(last_write_lsn) = this.last_write_lsn
            && write_lsn < *last_write_lsn
        {
            write_lsn = *last_write_lsn;
        }

        // If the new flush lsn is less than the last one, we can safely ignore it,
        // since we only want to report monotonically increasing values.
        if let Some(last_flush_lsn) = this.last_flush_lsn
            && flush_lsn < *last_flush_lsn
        {
            flush_lsn = *last_flush_lsn;
        }

        // This invariant is important since if `flush_lsn` becomes bigger, it means
        // that there was a problem during replication.
        debug_assert!(write_lsn >= flush_lsn);

        // If we are not forced to send an update, we can willingly do so based on a set
        // of conditions.
        if !force
            && let (Some(last_update), Some(last_flush)) =
                (this.last_update.as_mut(), this.last_flush_lsn.as_mut())
        {
            // The reason for only checking `flush_lsn` and `apply_lsn` is that if we are
            // not forced to send a status update to Postgres (when reply is
            // requested), we want to just notify it in case we actually durably
            // flushed and persisted events, which is signaled via
            // the two aforementioned fields. Postgres mostly uses the 'write_lsn' field for
            // tracking what was received by the replication client but not what the client
            // actually safely stored.
            //
            // If we were to check `write_lsn` too, we would end up sending updates more
            // frequently when they are not requested, simply because the
            // `write_lsn` is updated for every incoming message in the apply
            // loop.
            if flush_lsn == *last_flush && last_update.elapsed() < STATUS_UPDATE_INTERVAL {
                counter!(
                    ETL_STATUS_UPDATES_SKIPPED_TOTAL,
                    PIPELINE_ID_LABEL => pipeline_id.to_string(),
                    STATUS_UPDATE_TYPE_LABEL => status_update_type.to_string(),
                )
                .increment(1);

                debug!(
                    %flush_lsn,
                    last_update_elapsed_secs = last_update.elapsed().as_secs(),
                    %status_update_type,
                    "skipping status update"
                );

                return Ok(());
            }
        }

        // The client's system clock at the time of transmission, as microseconds since
        // midnight on 2000-01-01.
        let ts = POSTGRES_EPOCH
            .elapsed()
            .map_err(|err| {
                etl_error!(ErrorKind::InvalidState, "Invalid PostgreSQL epoch", err.to_string())
            })?
            .as_micros() as i64;

        // We will send the `flush_lsn` as `apply_lsn` since in our case, we don't
        // distinguish between them as Postgres does. The reason is that
        // `apply_lsn` is used to mark when an LSN is both durable and visible,
        // but from ETL's perspective we are fine with just it being durable, which
        // is marked via the `flush_lsn`.
        let request_reply: u8 = status_update_type.request_reply().into();
        this.stream
            .standby_status_update(write_lsn, flush_lsn, flush_lsn, ts, request_reply)
            .await?;

        counter!(
            ETL_STATUS_UPDATES_TOTAL,
            PIPELINE_ID_LABEL => pipeline_id.to_string(),
            FORCED_LABEL => force.to_string(),
            STATUS_UPDATE_TYPE_LABEL => status_update_type.to_string(),
        )
        .increment(1);

        debug!(
            %write_lsn,
            %flush_lsn,
            apply_lsn = %flush_lsn,
            force,
            %status_update_type,
            "status update sent"
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
            // A successful message.
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            // An error occurred on the server side.
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            // The connection had an error and/or was dropped.
            Poll::Ready(None) => Poll::Ready(None),
            // No message available.
            Poll::Pending => Poll::Pending,
        }
    }
}
