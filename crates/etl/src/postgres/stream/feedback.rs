//! Serialized replication feedback and its independent heartbeat deadline.
//!
//! Feedback reports safe replication progress through standby status updates.
//! The keep-alive deadline triggers a resend of that feedback when otherwise
//! silent; it does not compute or advance progress.

use std::{
    fmt::{Display, Formatter},
    future::Future,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use etl_postgres::time::POSTGRES_EPOCH;
use futures::{Sink, SinkExt};
use metrics::counter;
use tokio::{sync::mpsc, time::Instant};
use tokio_postgres::types::PgLsn;
use tracing::debug;
#[cfg(feature = "failpoints")]
use tracing::warn;

#[cfg(feature = "failpoints")]
use crate::failpoints::{SEND_STATUS_UPDATE_FP, etl_fail_point_active};
use crate::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    observability::{
        ETL_STATUS_UPDATES_SKIPPED_TOTAL, ETL_STATUS_UPDATES_TOTAL, FORCED_LABEL,
        STATUS_UPDATE_TYPE_LABEL,
    },
};

/// PostgreSQL wire tag for a standby status update.
const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';

/// Minimum interval between non-forced status updates when the flush frontier
/// has not advanced.
///
/// PostgreSQL can emit a primary keepalive for every transaction that pgoutput
/// skips because it contains no published changes when synchronous replication
/// is active. The apply loop attempts a status response for each keepalive,
/// including those that do not request an immediate reply. This interval
/// prevents such bursts from producing an equally large burst of redundant
/// responses.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

/// The status update type when sending a status update message back to
/// Postgres.
#[derive(Debug, Clone, Copy)]
enum StatusUpdateType {
    /// Represents an update in response to a keep alive from Postgres.
    KeepAlive,
    /// Represents a periodic heartbeat sent while the apply loop is idle or
    /// waiting for work to complete.
    ///
    /// Unlike the WAL receiver's ordinary periodic status reports, this
    /// fallback requests a reply. PostgreSQL's response can drive idle
    /// processing once the apply loop is able to read it.
    PeriodicKeepAlive,
}

impl StatusUpdateType {
    /// Returns whether this status update asks PostgreSQL for an immediate
    /// reply.
    fn request_reply(&self) -> bool {
        match self {
            Self::KeepAlive => false,
            Self::PeriodicKeepAlive => true,
        }
    }

    /// Returns the metric label for this status update type.
    fn as_str(&self) -> &'static str {
        match self {
            Self::KeepAlive => "keep_alive",
            Self::PeriodicKeepAlive => "periodic_keep_alive",
        }
    }
}

impl Display for StatusUpdateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Safe progress and reply urgency supplied to the feedback sender.
#[derive(Debug)]
struct StatusUpdateRequest {
    /// Safely consumed WAL position supplied by the apply loop.
    write_lsn: PgLsn,
    /// Safe checkpoint supplied by the apply loop.
    flush_lsn: PgLsn,
    /// Whether this update must bypass debouncing.
    force: bool,
}

/// Independent channel handle for submitting replication status updates.
///
/// The caller owns the sender future returned alongside this handle and must
/// spawn and stop it for the lifetime of replication.
#[derive(Debug)]
pub struct FeedbackHandle {
    /// A bounded queue that applies backpressure if the transport cannot send.
    requests_tx: mpsc::Sender<StatusUpdateRequest>,
}

impl FeedbackHandle {
    /// Creates a channel handle and the [`FeedbackSender`] future.
    ///
    /// The caller must spawn and own the returned future; the handle only
    /// submits requests and never writes to the transport itself.
    pub(crate) fn create<S>(
        sink: S,
        keep_alive_deadline_duration: Duration,
    ) -> (Self, impl Future<Output = EtlResult<()>>)
    where
        S: Sink<Bytes> + Unpin,
        S::Error: Into<EtlError>,
    {
        let (requests_tx, requests_rx) = mpsc::channel(1);
        let feedback_sender = FeedbackSender {
            sink,
            requests_rx,
            last_update: None,
            write_lsn: 0.into(),
            flush_lsn: 0.into(),
            last_sent_flush_lsn: None,
        };

        (Self { requests_tx }, feedback_sender.run(keep_alive_deadline_duration))
    }

    /// Enqueues safe progress, failing when the feedback sender has stopped.
    ///
    /// Waits only for queue capacity, not transmission. Reports unavailable
    /// feedback so the worker can retry replication when the sender stops.
    pub async fn enqueue_status_update(
        &self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        force: bool,
    ) -> EtlResult<()> {
        self.requests_tx.send(StatusUpdateRequest { write_lsn, flush_lsn, force }).await.map_err(
            |error| {
                etl_error!(
                    ErrorKind::ReplicationFeedbackUnavailable,
                    "Replication feedback sender is unavailable",
                    source: error
                )
            },
        )
    }
}

/// Background sender that owns the transport, safe positions, and keep-alive
/// deadline.
///
/// Processes requests from [`FeedbackHandle`] and repeats the latest supplied
/// positions when the deadline expires.
#[derive(Debug)]
struct FeedbackSender<S> {
    /// Write half of the same CopyBoth connection used for WAL intake.
    sink: S,
    /// Explicit requests from the apply loop.
    requests_rx: mpsc::Receiver<StatusUpdateRequest>,
    /// Time of the last successful send, never an incoming primary keepalive.
    last_update: Option<Instant>,
    /// Latest safely consumed position supplied through the request channel.
    write_lsn: PgLsn,
    /// Latest safe checkpoint supplied through the request channel.
    flush_lsn: PgLsn,
    /// Last successfully sent flush position, used for optional-send
    /// debouncing.
    last_sent_flush_lsn: Option<PgLsn>,
}

impl<S> FeedbackSender<S>
where
    S: Sink<Bytes> + Unpin,
    S::Error: Into<EtlError>,
{
    /// Serializes requests and fallback heartbeats until the owner drops its
    /// handle.
    ///
    /// Only successful sends postpone the deadline. Receiving a primary
    /// keepalive cannot do so: PostgreSQL measures timeout from our replies.
    /// Fallbacks repeat the latest supplied safe positions, including debounced
    /// updates. Only the apply loop determines progress; this task never infers
    /// it from WAL intake, a catchup target, or destination work in flight.
    /// This is an outbound inactivity timer, not the WAL receiver's separate
    /// timeout for missing primary messages: paused intake must not stop these
    /// heartbeats or be mistaken here for an unresponsive primary.
    async fn run(mut self, keep_alive_deadline_duration: Duration) -> EtlResult<()> {
        let mut deadline = Instant::now() + keep_alive_deadline_duration;

        loop {
            tokio::select! {
                biased;

                // An expired deadline cannot be starved by optional requests.
                _ = tokio::time::sleep_until(deadline) => {
                    // Keep the connection alive even while intake is backpressured.
                    // This only repeats supplied safe positions; it does not wake
                    // the apply loop to publish fresh progress or settle durability.
                    self.send_status_update(0.into(), 0.into(), true, StatusUpdateType::PeriodicKeepAlive).await?;

                    // Fault injection may suppress a send. Still space retries
                    // rather than spinning on an already-expired deadline.
                    deadline = Instant::now() + keep_alive_deadline_duration;
                }

                request = self.requests_rx.recv() => {
                    let Some(request) = request else { return Ok(()); };

                    if self.send_status_update(request.write_lsn, request.flush_lsn, request.force, StatusUpdateType::KeepAlive).await? {
                        deadline = Instant::now() + keep_alive_deadline_duration;
                    }
                }
            }
        }
    }

    /// Encodes a status update and flushes it into the connection driver's
    /// queue.
    ///
    /// The driver writes to the socket independently. Completion here does not
    /// confirm a TCP flush, receipt by PostgreSQL, or slot persistence.
    ///
    /// Write and flush positions are clamped locally to the latest safe values
    /// supplied by the apply loop, even when an optional send is debounced.
    /// Passing zeros repeats those values, using wire zeros before any progress
    /// is supplied. PostgreSQL skips slot confirmation for a zero flush LSN but
    /// directly replaces its WAL sender's write, flush, and apply positions.
    /// The local clamp prevents those positions from regressing or becoming
    /// unknown in `pg_stat_replication`.
    ///
    /// Primary keepalives do not all request an immediate response. In
    /// particular, a synchronous logical walsender may send one after pgoutput
    /// skips a transaction with no published changes. The apply loop still
    /// enqueues a status update for that keepalive so new flush progress can be
    /// reported promptly, but passes `force = false` when PostgreSQL did not
    /// request a reply.
    ///
    /// PostgreSQL normally requests an immediate reply after roughly half
    /// of `wal_sender_timeout` has elapsed without hearing from ETL. Once it
    /// sends that requested heartbeat, the walsender waits for the response
    /// instead of issuing more requested heartbeats. Orderly walsender shutdown
    /// can also request a reply while waiting for confirmation of its final WAL
    /// position. Keepalives emitted for skipped transactions or in response to
    /// our own reply requests do not request a reply.
    ///
    /// A non-forced call with an unchanged flush frontier is therefore
    /// debounced. A skipped update means ETL suppressed that redundant response
    /// locally; it does not mean a status message was sent and rejected by
    /// PostgreSQL. Outside test fault injection, forced heartbeat
    /// responses always bypass this interval.
    async fn send_status_update(
        &mut self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        force: bool,
        status_update_type: StatusUpdateType,
    ) -> EtlResult<bool> {
        // If the failpoint is active, we do not send any status update. This is useful
        // for testing the system when we want to check what happens when no
        // status updates are sent.
        #[cfg(feature = "failpoints")]
        if etl_fail_point_active(SEND_STATUS_UPDATE_FP) {
            warn!("not sending status update due to active failpoint");

            return Ok(false);
        }

        // Retain safe state independently of sending. Otherwise a debounced
        // update would be forgotten when a later fallback heartbeat fires.
        self.write_lsn = self.write_lsn.max(write_lsn);
        self.flush_lsn = self.flush_lsn.max(flush_lsn);
        let write_lsn = self.write_lsn;
        let flush_lsn = self.flush_lsn;

        // This invariant is important since if `flush_lsn` becomes bigger, it means
        // that there was a problem during replication.
        debug_assert!(write_lsn >= flush_lsn);

        // Debounce only optional replies. PostgreSQL may generate many primary
        // keepalives for consecutive transactions that pgoutput filtered to
        // empty, but replying again before either the flush frontier or this
        // interval advances would provide no new durability information.
        if !force
            && let (Some(last_update), Some(last_flush_lsn)) =
                (self.last_update.as_ref(), self.last_sent_flush_lsn.as_ref())
        {
            // Only a changed flush frontier bypasses the interval because it
            // provides PostgreSQL with new confirmed progress. `write_lsn`
            // tracks receipt and can advance for every incoming message,
            // including keepalives, without advancing that safe frontier while
            // ETL still has unresolved work.
            //
            // Checking `write_lsn` here would defeat the debounce: each new
            // keepalive could trigger another optional response even though the
            // reported flush and apply positions were unchanged.
            if flush_lsn == *last_flush_lsn && last_update.elapsed() < STATUS_UPDATE_INTERVAL {
                counter!(
                    ETL_STATUS_UPDATES_SKIPPED_TOTAL,
                    STATUS_UPDATE_TYPE_LABEL => status_update_type.as_str(),
                )
                .increment(1);

                debug!(
                    %flush_lsn,
                    last_update_elapsed_secs = last_update.elapsed().as_secs(),
                    %status_update_type,
                    "skipping status update"
                );

                return Ok(false);
            }
        }

        // The client's system clock at the time of transmission, as microseconds since
        // midnight on 2000-01-01.
        let ts = POSTGRES_EPOCH
            .elapsed()
            .map_err(
                |err| etl_error!(ErrorKind::InvalidState, "Invalid PostgreSQL epoch", source: err),
            )?
            .as_micros() as i64;

        // We will send the `flush_lsn` as `apply_lsn` since in our case, we don't
        // distinguish between them as Postgres does. The reason is that
        // `apply_lsn` is used to mark when an LSN is both durable and visible,
        // but from ETL's perspective we are fine with just it being durable, which
        // is marked via the `flush_lsn`.
        //
        // This outgoing request flag is separate from `force`. For a primary
        // keepalive, `force` mirrors PostgreSQL's incoming reply-request flag.
        // Background periodic updates are forced and request a
        // reply. PostgreSQL answers with a primary keepalive carrying
        // reply_requested = false. The apply loop may still enqueue an optional
        // KeepAlive response, subject to debouncing, but that response also has
        // reply_requested = false. Neither response requests another answer,
        // so this exchange cannot sustain an infinite keepalive request loop.
        let request_reply: u8 = status_update_type.request_reply().into();
        // CopyBoth supplies the framing; this is the standby-status payload.
        let mut message = BytesMut::with_capacity(34);
        message.put_u8(STANDBY_STATUS_UPDATE_TAG);
        message.put_u64(write_lsn.into());
        message.put_u64(flush_lsn.into());
        message.put_u64(flush_lsn.into());
        message.put_i64(ts);
        message.put_u8(request_reply);
        self.sink.send(message.freeze()).await.map_err(Into::into)?;

        counter!(
            ETL_STATUS_UPDATES_TOTAL,
            FORCED_LABEL => if force { "true" } else { "false" },
            STATUS_UPDATE_TYPE_LABEL => status_update_type.as_str(),
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
        self.last_update = Some(Instant::now());
        self.last_sent_flush_lsn = Some(flush_lsn);

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};
    use futures::FutureExt;
    use tokio::{
        sync::mpsc::UnboundedReceiver,
        task::JoinHandle,
        time::{Duration, advance},
    };

    use crate::{
        error::{ErrorKind, EtlResult},
        etl_error,
        postgres::stream::feedback::FeedbackHandle,
    };

    /// Spawns the feedback sender with an observable in-memory sink.
    fn spawn_feedback_sender()
    -> (FeedbackHandle, UnboundedReceiver<Bytes>, JoinHandle<EtlResult<()>>) {
        let (tx, messages) = tokio::sync::mpsc::unbounded_channel();
        let sink =
            Box::pin(futures::sink::unfold(tx, |tx, message| async move {
                tx.send(message).map_err(|error| etl_error!(
                ErrorKind::SourceConnectionFailed, "Test feedback sink closed", source: error
            ))?;
                Ok::<_, crate::error::EtlError>(tx)
            }));
        let (feedback_handle, feedback_sender_future) =
            FeedbackHandle::create(sink, Duration::from_secs(10));
        (feedback_handle, messages, tokio::spawn(feedback_sender_future))
    }

    /// Checks the actual standby-status payload, including its reply-request
    /// byte.
    fn assert_feedback(mut message: Bytes, write: u64, flush: u64, reply_requested: bool) {
        assert_eq!(message.len(), 34);
        assert_eq!(message.get_u8(), b'r');
        assert_eq!(message.get_u64(), write);
        assert_eq!(message.get_u64(), flush);
        assert_eq!(message.get_u64(), flush);
        assert!(message.get_i64() > 0);
        assert_eq!(message.get_u8(), u8::from(reply_requested));
        assert!(!message.has_remaining());
    }

    /// Advances only the virtual clock and lets the feedback sender task handle
    /// its timer.
    async fn advance_feedback_sender(duration: Duration) {
        advance(duration).await;
        tokio::task::yield_now().await;
    }

    /// Successful normal feedback postpones fallback heartbeats, which repeat
    /// it.
    #[tokio::test(start_paused = true)]
    async fn normal_feedback_resets_deadline_and_periodic_feedback_repeats_positions() {
        let (feedback_handle, mut messages, feedback_sender_task) = spawn_feedback_sender();
        feedback_handle.enqueue_status_update(100.into(), 80.into(), false).await.unwrap();
        tokio::task::yield_now().await;
        assert_feedback(messages.try_recv().unwrap(), 100, 80, false);

        advance_feedback_sender(Duration::from_secs(9)).await;
        assert!(messages.try_recv().is_err());
        feedback_handle.enqueue_status_update(200.into(), 90.into(), false).await.unwrap();
        tokio::task::yield_now().await;
        assert_feedback(messages.try_recv().unwrap(), 200, 90, false);

        advance_feedback_sender(Duration::from_secs(1)).await;
        assert!(messages.try_recv().is_err());
        advance_feedback_sender(Duration::from_secs(9)).await;
        assert_feedback(messages.try_recv().unwrap(), 200, 90, true);
        assert!(messages.try_recv().is_err());
        advance_feedback_sender(Duration::from_secs(10)).await;
        assert_feedback(messages.try_recv().unwrap(), 200, 90, true);

        // A delayed wakeup emits one heartbeat, without replaying missed ticks.
        advance_feedback_sender(Duration::from_secs(100)).await;
        assert_feedback(messages.try_recv().unwrap(), 200, 90, true);
        assert!(messages.try_recv().is_err());

        drop(feedback_handle);
        feedback_sender_task.await.unwrap().unwrap();
    }

    /// Debounced received progress is retained without delaying a heartbeat.
    #[tokio::test(start_paused = true)]
    async fn debounced_feedback_preserves_state_without_postponing_deadline() {
        let (feedback_handle, mut messages, feedback_sender_task) = spawn_feedback_sender();
        feedback_handle.enqueue_status_update(100.into(), 80.into(), false).await.unwrap();
        tokio::task::yield_now().await;
        assert_feedback(messages.try_recv().unwrap(), 100, 80, false);
        advance_feedback_sender(Duration::from_millis(50)).await;
        feedback_handle.enqueue_status_update(200.into(), 80.into(), false).await.unwrap();
        tokio::task::yield_now().await;
        assert!(messages.try_recv().is_err());

        advance_feedback_sender(Duration::from_millis(9950)).await;
        assert_feedback(messages.try_recv().unwrap(), 200, 80, true);
        drop(feedback_handle);
        feedback_sender_task.await.unwrap().unwrap();
    }

    /// Flush advancement and requested replies bypass optional debouncing.
    #[tokio::test(start_paused = true)]
    async fn requested_feedback_is_immediate_and_positions_remain_monotonic() {
        let (feedback_handle, mut messages, feedback_sender_task) = spawn_feedback_sender();
        feedback_handle.enqueue_status_update(100.into(), 80.into(), false).await.unwrap();
        tokio::task::yield_now().await;
        assert_feedback(messages.try_recv().unwrap(), 100, 80, false);
        feedback_handle.enqueue_status_update(200.into(), 90.into(), false).await.unwrap();
        tokio::task::yield_now().await;
        assert_feedback(messages.try_recv().unwrap(), 200, 90, false);

        // An incoming request forces a response but does not request another reply.
        feedback_handle.enqueue_status_update(50.into(), 40.into(), true).await.unwrap();
        tokio::task::yield_now().await;
        assert_feedback(messages.try_recv().unwrap(), 200, 90, false);
        assert!(messages.try_recv().is_err());
        drop(feedback_handle);
        feedback_sender_task.await.unwrap().unwrap();
    }

    /// Before any explicit progress, fallback feedback keeps the connection
    /// alive with zeros.
    #[tokio::test(start_paused = true)]
    async fn initial_heartbeat_reports_no_progress() {
        let (feedback_handle, mut messages, feedback_sender_task) = spawn_feedback_sender();
        tokio::task::yield_now().await;
        assert!(messages.try_recv().is_err());
        advance_feedback_sender(Duration::from_secs(10)).await;
        assert_feedback(messages.try_recv().unwrap(), 0, 0, true);
        drop(feedback_handle);
        feedback_sender_task.await.unwrap().unwrap();
    }

    /// An explicit send failure closes the channel for subsequent requests.
    #[tokio::test(start_paused = true)]
    async fn requested_feedback_failure_rejects_next_request() {
        let (feedback_handle, messages, feedback_sender_task) = spawn_feedback_sender();
        drop(messages);
        feedback_handle.enqueue_status_update(100.into(), 80.into(), true).await.unwrap();
        assert_eq!(
            feedback_sender_task.await.unwrap().unwrap_err().kind(),
            ErrorKind::SourceConnectionFailed
        );
        assert_eq!(
            feedback_handle
                .enqueue_status_update(100.into(), 80.into(), true)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::ReplicationFeedbackUnavailable
        );
    }

    /// A heartbeat failure closes the channel, failing the next apply request.
    #[tokio::test(start_paused = true)]
    async fn periodic_feedback_failure_rejects_next_request() {
        let (feedback_handle, messages, feedback_sender_task) = spawn_feedback_sender();
        drop(messages);
        tokio::task::yield_now().await;
        advance_feedback_sender(Duration::from_secs(10)).await;
        assert!(feedback_sender_task.is_finished());
        assert_eq!(
            feedback_sender_task.await.unwrap().unwrap_err().kind(),
            ErrorKind::SourceConnectionFailed
        );
        assert_eq!(
            feedback_handle
                .enqueue_status_update(100.into(), 80.into(), true)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::ReplicationFeedbackUnavailable
        );
        drop(feedback_handle);
    }

    /// Enqueueing waits only for queue capacity, then wakes with a retryable
    /// error if the sender fails in the transport.
    #[tokio::test]
    async fn enqueue_waits_only_for_capacity_and_wakes_on_sender_failure() {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let sink = Box::pin(futures::sink::unfold(
            (entered_tx, release_rx),
            |(entered_tx, release_rx), _| async move {
                entered_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Err(etl_error!(ErrorKind::InvalidState, "Test feedback failure"))
            },
        ));
        let (handle, sender) = FeedbackHandle::create(sink, Duration::from_secs(10));
        let task = tokio::spawn(sender);
        handle.enqueue_status_update(100.into(), 80.into(), true).now_or_never().unwrap().unwrap();
        entered_rx.await.unwrap();

        // Fill the queue while its consumer is suspended in the transport.
        handle.enqueue_status_update(200.into(), 90.into(), true).now_or_never().unwrap().unwrap();
        let mut blocked = Box::pin(handle.enqueue_status_update(300.into(), 100.into(), true));
        assert!(blocked.as_mut().now_or_never().is_none());

        release_tx.send(()).unwrap();
        assert_eq!(blocked.await.unwrap_err().kind(), ErrorKind::ReplicationFeedbackUnavailable);
        assert_eq!(task.await.unwrap().unwrap_err().kind(), ErrorKind::InvalidState);
    }
}
