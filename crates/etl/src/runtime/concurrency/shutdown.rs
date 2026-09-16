use tokio::sync::watch;

use crate::runtime::concurrency::signal::{SignalRx, SignalTx, create_signal};

/// Transmitter side of the shutdown coordination channel.
///
/// [`ShutdownTx`] enables sending shutdown signals to multiple workers
/// simultaneously. It wraps a signal transmitter with shutdown-specific
/// semantics and provides methods for triggering shutdown and creating receiver
/// subscriptions.
#[derive(Debug, Clone)]
pub struct ShutdownTx(SignalTx);

impl ShutdownTx {
    /// Wraps a signal transmitter with shutdown semantics.
    fn wrap(tx: SignalTx) -> Self {
        Self(tx)
    }

    /// Triggers shutdown for all subscribed workers.
    ///
    /// This method broadcasts a shutdown signal to all workers that have
    /// subscribed to this shutdown channel. Workers should respond by
    /// completing their current operations gracefully and terminating.
    pub fn shutdown(&self) -> Result<(), watch::error::SendError<()>> {
        self.0.send(())
    }

    /// Creates a new shutdown handle for worker subscription.
    ///
    /// Each worker should call this method to get its own handle that can be
    /// used to detect when shutdown has been requested. Multiple handles can be
    /// created from the same transmitter.
    pub(crate) fn subscribe(&self) -> Shutdown {
        Shutdown { tx: self.clone(), rx: self.0.subscribe() }
    }
}

/// Requests shutdown when the owner of the channel goes away.
///
/// A [`Shutdown`] handle owns a transmitter so its worker can request shutdown,
/// which means the channel no longer loses its last transmitter when the
/// pipeline is dropped. Workers that only wait for the signal would then wait
/// for one nobody can send any more, so the pipeline holds this guard and its
/// drop takes over what closing the channel used to do.
#[derive(Debug)]
pub(crate) struct ShutdownOnDrop(ShutdownTx);

impl ShutdownOnDrop {
    /// Guards the channel of the given transmitter.
    pub(crate) fn new(tx: ShutdownTx) -> Self {
        Self(tx)
    }
}

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        // A failed send means no worker is left to notify, which needs no handling.
        let _ = self.0.shutdown();
    }
}

/// Worker-side handle to the shutdown coordination channel.
///
/// [`Shutdown`] is used by workers to detect when shutdown has been requested,
/// and to request it themselves. A worker that learns on its own that the
/// pipeline must stop, such as one whose destination reports that it is
/// shutting down, calls [`Shutdown::request`] and observes the result through
/// the same signal an external [`ShutdownTx::shutdown`] sends. Both directions
/// therefore converge on one shutdown path, whichever comes first.
#[derive(Debug, Clone)]
pub(crate) struct Shutdown {
    tx: ShutdownTx,
    rx: SignalRx,
}

impl Shutdown {
    /// Requests shutdown for every worker of the pipeline.
    pub(crate) fn request(&self) {
        // This handle owns a transmitter, so a send can only fail once every
        // receiver is gone, which leaves nothing to notify.
        let _ = self.tx.shutdown();
    }

    /// Returns true once shutdown has been requested.
    ///
    /// A channel with no transmitter left also counts as requested.
    pub(crate) fn is_requested(&self) -> bool {
        self.rx.has_changed().unwrap_or(true)
    }

    /// Waits until shutdown is requested.
    pub(crate) async fn changed(&mut self) {
        // A closed channel means no further signal can arrive, which callers
        // treat like a shutdown request.
        let _ = self.rx.changed().await;
    }
}

/// Result type that distinguishes between normal operation and shutdown
/// scenarios.
///
/// [`ShutdownResult`] is used by operations that can be interrupted by shutdown
/// signals. It preserves both successful results and any partial data that was
/// being processed when shutdown was requested.
pub(crate) enum ShutdownResult<T, I> {
    /// Normal successful completion with result data.
    Ok(T),
    /// Operation was interrupted by shutdown, with any partial data preserved.
    Shutdown(I),
}

impl<T, I> ShutdownResult<T, I> {
    /// Returns true if this result represents a shutdown scenario.
    pub(crate) fn should_shutdown(&self) -> bool {
        matches!(self, ShutdownResult::Shutdown(_))
    }
}

/// Creates a new shutdown coordination channel.
///
/// This function creates a broadcast channel for coordinating shutdown across
/// multiple workers. The transmitter can be used to trigger shutdown, while
/// handles can be distributed to workers that need to respond to shutdown
/// signals.
pub(crate) fn create_shutdown_channel() -> (ShutdownTx, Shutdown) {
    let (tx, _) = create_signal();
    let shutdown_tx = ShutdownTx::wrap(tx);
    let shutdown = shutdown_tx.subscribe();

    (shutdown_tx, shutdown)
}

#[cfg(test)]
mod tests {
    use crate::runtime::concurrency::{ShutdownOnDrop, create_shutdown_channel};

    #[test]
    fn dropping_the_guard_requests_shutdown() {
        let (shutdown_tx, shutdown) = create_shutdown_channel();
        let guard = ShutdownOnDrop::new(shutdown_tx);

        assert!(!shutdown.is_requested());

        drop(guard);

        assert!(shutdown.is_requested());
    }

    #[test]
    fn a_worker_handle_requests_shutdown_for_every_other_handle() {
        let (shutdown_tx, shutdown) = create_shutdown_channel();
        let worker_shutdown = shutdown_tx.subscribe();

        assert!(!shutdown.is_requested());

        worker_shutdown.request();

        assert!(shutdown.is_requested());
    }
}
