use std::time::Duration;
use std::{fmt, sync::Arc};

use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::warn;

/// Default timeout duration for notifications.
///
/// This duration was chosen empirically since most waiting should not take more than a few seconds.
pub const DEFAULT_NOTIFY_TIMEOUT: Duration = Duration::from_secs(30);

/// A wrapper around [`Arc<Notify>`] that provides automatic timeout functionality for tests.
///
/// This prevents tests from hanging indefinitely when waiting for state changes
/// that may never occur. The timeout ensures tests fail quickly with a clear
/// timeout error instead of hanging forever.
#[derive(Clone)]
pub struct TimedNotify {
    notify: Arc<Notify>,
    timeout_duration: Duration,
}

impl TimedNotify {
    /// Creates a new [`TimedNotify`] with the default timeout.
    pub fn new(notify: Arc<Notify>) -> Self {
        Self::with_timeout(notify, DEFAULT_NOTIFY_TIMEOUT)
    }

    /// Creates a new [`TimedNotify`] with a custom timeout duration.
    pub fn with_timeout(notify: Arc<Notify>, timeout_duration: Duration) -> Self {
        Self {
            notify,
            timeout_duration,
        }
    }

    /// Waits for a notification with timeout.
    ///
    /// # Panics
    ///
    /// Panics if the timeout duration elapses before the notification is received.
    /// This is intentional behavior for tests to fail fast rather than hang.
    pub async fn notified(&self) {
        match timeout(self.timeout_duration, self.notify.notified()).await {
            Ok(()) => {}
            Err(_) => {
                panic!(
                    "Test notification timed out after {:?}. \
                     This likely indicates the expected state was never reached. \
                     Check if the pipeline is running correctly or if the condition is reachable.",
                    self.timeout_duration
                );
            }
        }
    }

    pub async fn try_notified(&self) {
        match timeout(self.timeout_duration, self.notify.notified()).await {
            Ok(()) => {}
            Err(_) => {
                warn!(
                    "Test notification timed out after {:?}. \
                     This likely indicates the expected state was never reached. \
                     Check if the pipeline is running correctly or if the condition is reachable.",
                    self.timeout_duration
                );
            }
        }
    }

    /// Returns the underlying [`Arc<Notify>`] for direct access if needed.
    pub fn inner(&self) -> &Arc<Notify> {
        &self.notify
    }
}

impl fmt::Debug for TimedNotify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimedNotify")
            .field("timeout_duration", &self.timeout_duration)
            .finish()
    }
}
