use etl_postgres::schema::ColumnSchema;
use etl_postgres::time::POSTGRES_EPOCH;
use futures::{Stream, ready};
use pin_project_lite::pin_project;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio_postgres::CopyOutStream;
use tokio_postgres::types::PgLsn;
use tracing::debug;

use crate::conversions::table_row::{TableRow, TableRowConverter};
use crate::error::EtlError;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;

/// The amount of milliseconds between two consecutive status updates in case no forced update
/// is requested.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

pin_project! {
    /// A stream that yields rows from a PostgreSQL COPY operation.
    ///
    /// This stream wraps a [`CopyOutStream`] and converts each row into a [`TableRow`]
    /// using the provided column schemas. The conversion process handles both text and
    /// binary format data.
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream<'a> {
        #[pin]
        stream: CopyOutStream,
        column_schemas: &'a [ColumnSchema],
    }
}

impl<'a> TableCopyStream<'a> {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column schemas.
    ///
    /// The column schemas are used to convert the raw PostgreSQL data into [`TableRow`]s.
    pub fn wrap(stream: CopyOutStream, column_schemas: &'a [ColumnSchema]) -> Self {
        Self {
            stream,
            column_schemas,
        }
    }
}

impl<'a> Stream for TableCopyStream<'a> {
    type Item = EtlResult<TableRow>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            // TODO: allow pluggable table row conversion based on if the data is in text or binary format.
            Some(Ok(row)) => match TableRowConverter::try_from(&row, this.column_schemas) {
                Ok(row) => Poll::Ready(Some(Ok(row))),
                Err(err) => Poll::Ready(Some(Err(err))),
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

pin_project! {
    pub struct EventsStream {
        #[pin]
        stream: LogicalReplicationStream,
        last_update: Option<Instant>,
        last_flush_lsn: Option<PgLsn>,
        last_apply_lsn: Option<PgLsn>,
    }
}

impl EventsStream {
    /// Creates a new [`EventsStream`] from a [`LogicalReplicationStream`].
    pub fn wrap(stream: LogicalReplicationStream) -> Self {
        Self {
            stream,
            last_update: None,
            last_flush_lsn: None,
            last_apply_lsn: None,
        }
    }

    /// Sends a status update to the PostgreSQL server with the current replication position.
    ///
    /// The timestamp is calculated relative to the PostgreSQL epoch (2000-01-01 00:00:00 UTC).
    /// This is used to inform the server about the client's progress in processing replication events.
    ///
    /// The update will be sent if:
    /// - force is true, OR
    /// - The write_lsn or apply_lsn has changed from the last update, OR
    /// - It has been more than 100ms since the last update
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        force: bool,
    ) -> EtlResult<()> {
        let this = self.project();

        // If we are not forced to send an update, we can willingly do so based on a set of conditions.
        if !force {
            if let (Some(last_update), Some(last_flush), Some(last_apply)) = (
                this.last_update.as_mut(),
                this.last_flush_lsn.as_mut(),
                this.last_apply_lsn.as_mut(),
            ) {
                // The reason for only checking `flush_lsn` and `apply_lsn` is that if we are not
                // forced to send a status update to Postgres (when reply is requested), we want to just
                // notify it in case we actually durably flushed and persisted events, which is signalled via
                // the two aforementioned fields. The `write_lsn` field is mostly used by Postgres for
                // tracking what was received by the replication client but not what the client actually
                // safely stored.
                //
                // If we were to check `write_lsn` too, we would end up sending updates more frequently
                // when they are not requested, simply because the `write_lsn` is updated for every
                // incoming message in the apply loop.
                if flush_lsn == *last_flush
                    && apply_lsn == *last_apply
                    && last_update.elapsed() < STATUS_UPDATE_INTERVAL
                {
                    return Ok(());
                }
            }
        }

        // The client's system clock at the time of transmission, as microseconds since midnight
        // on 2000-01-01.
        let ts = POSTGRES_EPOCH
            .elapsed()
            .map_err(|e| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Invalid Postgres epoch",
                    e.to_string()
                )
            })?
            .as_micros() as i64;

        this.stream
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, 0)
            .await?;

        debug!(
            "status update successfully sent (write_lsn = {}, flush_lsn = {}, apply_lsn = {})",
            write_lsn, flush_lsn, apply_lsn
        );

        // Update the state after successful send.
        *this.last_update = Some(Instant::now());
        *this.last_flush_lsn = Some(flush_lsn);
        *this.last_apply_lsn = Some(apply_lsn);

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
