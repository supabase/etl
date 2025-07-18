use tokio::sync::watch;

use crate::concurrency::signal::{SignalRx, SignalTx, create_signal};

#[derive(Debug, Clone)]
pub struct ShutdownTx(SignalTx);

impl ShutdownTx {
    pub fn wrap(tx: SignalTx) -> Self {
        Self(tx)
    }

    pub fn shutdown(&self) -> Result<(), watch::error::SendError<()>> {
        self.0.send(())
    }

    pub fn subscribe(&self) -> ShutdownRx {
        self.0.subscribe()
    }
}

pub type ShutdownRx = SignalRx;

pub enum ShutdownResult<T, I> {
    Ok(T),
    Shutdown(I),
}

impl<T, I> ShutdownResult<T, I> {
    pub fn should_shutdown(&self) -> bool {
        matches!(self, ShutdownResult::Shutdown(_))
    }
}

pub fn create_shutdown_channel() -> (ShutdownTx, ShutdownRx) {
    let (tx, rx) = create_signal();
    (ShutdownTx::wrap(tx), rx)
}
