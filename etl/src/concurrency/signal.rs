use tokio::sync::watch;

/// Type alias to abstract a watch channel of `()`.
pub type SignalTx = watch::Sender<()>;

/// Type alias to abstract a watch channel of `()`.
pub type SignalRx = watch::Receiver<()>;

/// Creates a new pair of [`SignalTx`] and [`SignalRx`].
pub fn create_signal() -> (SignalTx, SignalRx) {
    let (tx, rx) = watch::channel(());
    (tx, rx)
}
