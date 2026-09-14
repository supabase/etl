//! Logical replication intake with optional background feedback.

use std::{
    fmt::{self, Debug, Formatter},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt, ready, stream::SplitStream};
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use tokio_postgres::{CopyBothDuplex, types::PgLsn};

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    postgres::stream::feedback::FeedbackHandle,
};

/// Decodes replication messages using the existing pgoutput protocol parser.
fn decode_message(bytes: Bytes) -> EtlResult<ReplicationMessage<LogicalReplicationMessage>> {
    let message = ReplicationMessage::parse(&bytes).map_err(|error| {
        etl_error!(
            ErrorKind::SourceConnectionFailed, "Failed to decode replication message", source: error
        )
    })?;

    match message {
        ReplicationMessage::XLogData(body) => Ok(ReplicationMessage::XLogData(
            body.map_data(|bytes| LogicalReplicationMessage::parse(&bytes))
                .map_err(|error| etl_error!(ErrorKind::SourceConnectionFailed, "Failed to decode logical replication message", source: error))?,
        )),
        ReplicationMessage::PrimaryKeepAlive(body) => Ok(ReplicationMessage::PrimaryKeepAlive(body)),
        _ => Err(etl_error!(ErrorKind::SourceConnectionFailed, "Unsupported replication message")),
    }
}

/// Logical WAL reader with optional background status feedback.
pub struct ReplicationMessageStream {
    /// The read half never holds the transport lock across a pending poll.
    stream: SplitStream<CopyBothDuplex<Bytes>>,
    /// Submits status updates to the background sender when feedback is
    /// enabled.
    feedback_handle: Option<FeedbackHandle>,
}

impl ReplicationMessageStream {
    /// Creates WAL intake and an optional feedback sender future on one
    /// connection.
    ///
    /// Supplying an interval enables feedback; the caller must spawn the
    /// returned future and stop its task when this stream is dropped. With
    /// `None`, no feedback is sent, allowing tests to replay the slot without
    /// acknowledging it. Neither mode opens another PostgreSQL connection.
    pub(crate) fn create(
        stream: CopyBothDuplex<Bytes>,
        keep_alive_deadline_duration: Option<Duration>,
    ) -> (Self, Option<impl Future<Output = EtlResult<()>> + Send + 'static>) {
        let (sink, stream) = stream.split();
        let (feedback_handle, feedback_sender_future) = match keep_alive_deadline_duration {
            Some(keep_alive_deadline_duration) => {
                let (feedback_handle, feedback_sender_future) =
                    FeedbackHandle::create(sink, keep_alive_deadline_duration);
                (Some(feedback_handle), Some(feedback_sender_future))
            }
            None => (None, None),
        };

        (Self { stream, feedback_handle }, feedback_sender_future)
    }

    /// Enqueues safe positions without waiting for the sender to transmit them.
    ///
    /// Returns an error if this stream was created without feedback.
    pub(crate) async fn enqueue_status_update(
        &self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        force: bool,
    ) -> EtlResult<()> {
        let feedback_handle = self.feedback_handle.as_ref().ok_or_else(|| {
            etl_error!(ErrorKind::InvalidState, "Replication feedback is disabled for this stream")
        })?;
        feedback_handle.enqueue_status_update(write_lsn, flush_lsn, force).await
    }
}

impl Debug for ReplicationMessageStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // CopyBothDuplex does not implement Debug, so omit the opaque transport.
        f.debug_struct("ReplicationMessageStream")
            .field("feedback_handle", &self.feedback_handle)
            .finish_non_exhaustive()
    }
}

impl Stream for ReplicationMessageStream {
    type Item = EtlResult<ReplicationMessage<LogicalReplicationMessage>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(
            ready!(Pin::new(&mut self.stream).poll_next(cx))
                .map(|result| result.map_err(Into::into).and_then(decode_message)),
        )
    }
}
