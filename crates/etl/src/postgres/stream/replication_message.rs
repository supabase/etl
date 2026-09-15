//! Logical replication intake and construction of independent feedback.

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
use tokio_postgres::CopyBothDuplex;

use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    postgres::FeedbackHandle,
};

/// Decodes replication messages using the existing pgoutput protocol parser.
fn decode_message(bytes: Bytes) -> EtlResult<ReplicationMessage<LogicalReplicationMessage>> {
    let message = ReplicationMessage::parse(&bytes).map_err(|error| {
        etl_error!(
            ErrorKind::DeserializationError, "Failed to decode replication message", source: error
        )
    })?;

    match message {
        ReplicationMessage::XLogData(body) => {
            let body = body.map_data(|bytes| LogicalReplicationMessage::parse(&bytes)).map_err(
                |error| {
                    etl_error!(
                        ErrorKind::DeserializationError,
                        "Failed to decode logical replication message",
                        source: error
                    )
                },
            )?;

            Ok(ReplicationMessage::XLogData(body))
        }
        ReplicationMessage::PrimaryKeepAlive(body) => {
            Ok(ReplicationMessage::PrimaryKeepAlive(body))
        }
        _ => Err(etl_error!(ErrorKind::DeserializationError, "Unsupported replication message")),
    }
}

/// Read-only logical WAL stream.
pub struct ReplicationMessageStream {
    /// The read half never holds the transport lock across a pending poll.
    stream: SplitStream<CopyBothDuplex<Bytes>>,
}

impl ReplicationMessageStream {
    /// Creates WAL intake and optional independent feedback on one connection.
    ///
    /// Supplying an interval returns a feedback handle and its sender future.
    /// The caller must own the handle, spawn the future, and stop its task when
    /// replication ends. With `None`, no feedback is sent, allowing tests to
    /// replay the slot without acknowledging it. Neither mode opens another
    /// PostgreSQL connection.
    pub(crate) fn create(
        stream: CopyBothDuplex<Bytes>,
        keep_alive_deadline_duration: Option<Duration>,
    ) -> (Self, Option<(FeedbackHandle, impl Future<Output = EtlResult<()>>)>) {
        let (sink, stream) = stream.split();
        let feedback =
            keep_alive_deadline_duration.map(|duration| FeedbackHandle::create(sink, duration));

        (Self { stream }, feedback)
    }
}

impl Debug for ReplicationMessageStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // CopyBothDuplex does not implement Debug, so omit the opaque transport.
        f.debug_struct("ReplicationMessageStream").finish_non_exhaustive()
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bytes::Bytes;

    use crate::{error::ErrorKind, postgres::stream::replication_message::decode_message};

    /// Invalid transport frames and logical payloads retain their parse errors
    /// instead of being classified as retryable connection failures.
    #[test]
    fn malformed_messages_preserve_deserialization_errors() {
        let invalid_transport_frame = Bytes::from_static(b"w");
        // A valid XLogData header reaches the logical parser with a truncated Begin.
        let mut invalid_logical_frame = vec![b'w'];
        invalid_logical_frame.extend_from_slice(&[0; 24]);
        invalid_logical_frame.push(b'B');

        for frame in [invalid_transport_frame, Bytes::from(invalid_logical_frame)] {
            let error = decode_message(frame).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DeserializationError);
            assert!(error.source().is_some());
        }
    }
}
