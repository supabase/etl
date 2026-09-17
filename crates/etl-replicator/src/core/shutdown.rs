//! Process termination signals shared across startup and running phases.

use tokio::signal::unix::{Signal, SignalKind, signal};
use tracing::info;

/// Owns signal listeners from before asynchronous initialization until exit.
pub(super) struct ShutdownSignal {
    /// Termination requests from the process supervisor.
    sigterm: Signal,
    /// Interrupts from the controlling terminal.
    sigint: Signal,
}

impl ShutdownSignal {
    /// Registers both listeners, reporting failures before starting work.
    pub(super) fn new() -> std::io::Result<Self> {
        Ok(Self {
            sigterm: signal(SignalKind::terminate())?,
            sigint: signal(SignalKind::interrupt())?,
        })
    }

    /// Waits for SIGINT or SIGTERM without spawning a listener task.
    pub(super) async fn wait(&mut self) {
        tokio::select! {
            _ = self.sigint.recv() => info!("sigint (ctrl+c) received, shutting down replicator"),

            _ = self.sigterm.recv() => info!("sigterm received, shutting down replicator"),
        }
    }
}
