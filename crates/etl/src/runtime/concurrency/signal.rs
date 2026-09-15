use tokio::sync::watch;

/// Transmitter side of a coordination signal channel.
///
/// [`SignalTx`] abstracts a watch channel transmitter for sending coordination
/// signals between workers. The signal carries no data payload - it's purely
/// for notification that some event or state change has occurred.
pub(crate) type SignalTx = watch::Sender<()>;

/// Receiver side of a coordination signal channel.
///
/// [`SignalRx`] abstracts a watch channel receiver for detecting coordination
/// signals. Workers can use this to wait for events from other parts of the
/// system without polling or complex synchronization.
#[derive(Debug, Clone)]
pub(crate) struct SignalRx(watch::Receiver<()>);

impl SignalRx {
    /// Wraps a receiver without changing its watch version or subscriptions.
    pub(crate) fn new(rx: watch::Receiver<()>) -> Self {
        Self(rx)
    }

    /// Waits for a coordination update, including ordinary idle time.
    #[hotpath::measure(label = "shutdown_signal_wait")]
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.0.changed().await
    }

    /// Checks for an unseen signal without marking it as observed.
    pub(crate) fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.0.has_changed()
    }
}

/// Creates a new coordination signal channel.
///
/// This function creates a watch-based signaling channel optimized for
/// coordination scenarios where multiple receivers need to be notified of the
/// same event. Unlike mpsc channels, all receivers see the same signal
/// simultaneously.
pub(crate) fn create_signal() -> (SignalTx, SignalRx) {
    let (tx, rx) = watch::channel(());
    (tx, SignalRx::new(rx))
}

#[cfg(test)]
mod tests {
    use std::{future::poll_fn, task::Poll};

    use crate::runtime::concurrency::signal::{SignalRx, create_signal};

    /// Cancelling a wait must not consume the next update for any subscriber.
    #[tokio::test]
    async fn cancelled_signal_wait_preserves_broadcast_delivery() {
        let (tx, mut rx) = create_signal();
        let mut cloned_rx = rx.clone();
        {
            let waiting = rx.changed();
            tokio::pin!(waiting);
            assert!(poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await.is_pending());
        }

        tx.send(()).unwrap();
        assert!(rx.has_changed().unwrap());
        rx.changed().await.unwrap();
        cloned_rx.changed().await.unwrap();
        assert!(!rx.has_changed().unwrap());

        let mut subscribed_rx = SignalRx::new(tx.subscribe());
        assert!(!subscribed_rx.has_changed().unwrap());
        tx.send(()).unwrap();
        subscribed_rx.changed().await.unwrap();
    }

    /// Sender closure must continue to wake a receiver with the original error.
    #[tokio::test]
    async fn signal_wait_reports_sender_closure() {
        let (tx, mut rx) = create_signal();
        drop(tx);
        assert!(rx.changed().await.is_err());
        assert!(rx.has_changed().is_err());
    }
}
