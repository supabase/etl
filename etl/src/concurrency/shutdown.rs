use tokio::sync::watch;

use crate::concurrency::signal::{SignalRx, SignalTx, create_signal};

/// Transmitter side of the shutdown coordination channel.
///
/// [`ShutdownTx`] enables sending shutdown signals to multiple workers simultaneously.
/// It wraps a signal transmitter with shutdown-specific semantics and provides methods
/// for triggering shutdown and creating receiver subscriptions.
#[derive(Debug, Clone)]
pub struct ShutdownTx(SignalTx);

impl ShutdownTx {
    /// Wraps a signal transmitter with shutdown semantics.
    pub fn wrap(tx: SignalTx) -> Self {
        Self(tx)
    }

    /// Triggers shutdown for all subscribed workers.
    ///
    /// This method broadcasts a shutdown signal to all workers that have subscribed
    /// to this shutdown channel. Workers should respond by completing their current
    /// operations gracefully and terminating.
    pub fn shutdown(&self) -> Result<(), watch::error::SendError<()>> {
        self.0.send(())
    }

    /// Creates a new shutdown receiver for worker subscription.
    ///
    /// Each worker should call this method to get its own receiver that can be used
    /// to detect when shutdown has been requested. Multiple receivers can be created
    /// from the same transmitter.
    pub fn subscribe(&self) -> ShutdownRx {
        self.0.subscribe()
    }
}

/// Receiver side of the shutdown coordination channel.
///
/// [`ShutdownRx`] is used by workers to detect when shutdown has been requested.
/// It's a type alias for [`SignalRx`] with shutdown-specific semantics.
pub type ShutdownRx = SignalRx;

/// Result type that distinguishes between normal operation and shutdown scenarios.
///
/// [`ShutdownResult`] is used by operations that can be interrupted by shutdown signals.
/// It preserves both successful results and any partial data that was being processed
/// when shutdown was requested.
pub enum ShutdownResult<T, I> {
    /// Normal successful completion with result data.
    Ok(T),
    /// Operation was interrupted by shutdown, with any partial data preserved.
    Shutdown(I),
}

impl<T, I> ShutdownResult<T, I> {
    /// Returns true if this result represents a shutdown scenario.
    pub fn should_shutdown(&self) -> bool {
        matches!(self, ShutdownResult::Shutdown(_))
    }
}

/// Creates a new shutdown coordination channel.
///
/// This function creates a broadcast channel for coordinating shutdown across multiple
/// workers. The transmitter can be used to trigger shutdown, while receivers can be
/// distributed to workers that need to respond to shutdown signals.
pub fn create_shutdown_channel() -> (ShutdownTx, ShutdownRx) {
    let (tx, rx) = create_signal();
    (ShutdownTx::wrap(tx), rx)
}
