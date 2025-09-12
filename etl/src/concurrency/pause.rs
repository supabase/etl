use tokio::sync::watch;

/// Transmitter side of the pause/resume coordination channel.
///
/// [`PauseTx`] allows pausing and resuming the replication apply loop
/// without advancing LSNs. While paused, the loop continues to send
/// periodic status updates to keep the connection alive.
#[derive(Debug, Clone)]
pub struct PauseTx(watch::Sender<bool>);

impl PauseTx {
    /// Wraps a watch sender into a [`PauseTx`].
    pub fn new(tx: watch::Sender<bool>) -> Self {
        Self(tx)
    }

    /// Requests the apply loop to pause.
    pub fn pause(&self) -> Result<(), watch::error::SendError<bool>> {
        // Use infallible send to support pausing before any receivers subscribe.
        self.0.send_replace(true);
        Ok(())
    }

    /// Requests the apply loop to resume.
    pub fn resume(&self) -> Result<(), watch::error::SendError<bool>> {
        // Use infallible send to support resuming even if no receivers are present.
        self.0.send_replace(false);
        Ok(())
    }

    /// Creates a new pause receiver subscription.
    pub fn subscribe(&self) -> PauseRx {
        self.0.subscribe()
    }
}

/// Receiver side of the pause/resume coordination channel.
pub type PauseRx = watch::Receiver<bool>;

/// Creates a new pause coordination channel.
pub fn create_pause_channel() -> (PauseTx, PauseRx) {
    let (tx, rx) = watch::channel(false);
    (PauseTx::new(tx), rx)
}
