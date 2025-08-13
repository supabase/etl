use tokio::sync::watch;

/// Transmitter side of a coordination signal channel.
///
/// [`SignalTx`] abstracts a watch channel transmitter for sending coordination signals
/// between workers. The signal carries no data payload - it's purely for notification
/// that some event or state change has occurred.
pub type SignalTx = watch::Sender<()>;

/// Receiver side of a coordination signal channel.
///
/// [`SignalRx`] abstracts a watch channel receiver for detecting coordination signals.
/// Workers can use this to wait for events from other parts of the system without
/// polling or complex synchronization.
pub type SignalRx = watch::Receiver<()>;

/// Creates a new coordination signal channel.
///
/// This function creates a watch-based signaling channel optimized for coordination
/// scenarios where multiple receivers need to be notified of the same event. Unlike
/// mpsc channels, all receivers see the same signal simultaneously.
pub fn create_signal() -> (SignalTx, SignalRx) {
    let (tx, rx) = watch::channel(());
    (tx, rx)
}
