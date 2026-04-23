use std::{fmt, panic::Location, sync::Arc, time::Duration};

use tokio::{sync::Notify, time::timeout};
use tracing::{error, info};

/// Default timeout duration for notifications.
///
/// This duration was chosen empirically since most waiting should not take more
/// than a few seconds.
pub const DEFAULT_NOTIFY_TIMEOUT: Duration = Duration::from_secs(240);

/// A wrapper around [`Arc<Notify>`] that provides automatic timeout
/// functionality for tests.
///
/// This prevents tests from hanging indefinitely when waiting for state changes
/// that may never occur. The timeout ensures tests fail quickly with a clear
/// timeout error instead of hanging forever.
#[derive(Clone)]
pub struct TimedNotify {
    notify: Arc<Notify>,
    timeout_duration: Duration,
    description: Arc<str>,
}

impl TimedNotify {
    /// Creates a new [`TimedNotify`] with the default timeout.
    pub fn new(notify: Arc<Notify>) -> Self {
        Self::with_description(notify, DEFAULT_NOTIFY_TIMEOUT, "test notification")
    }

    /// Creates a new [`TimedNotify`] with a custom timeout duration.
    pub fn with_timeout(notify: Arc<Notify>, timeout_duration: Duration) -> Self {
        Self::with_description(notify, timeout_duration, "test notification")
    }

    /// Creates a new [`TimedNotify`] with a custom timeout duration and
    /// description.
    pub fn with_description(
        notify: Arc<Notify>,
        timeout_duration: Duration,
        description: impl Into<Arc<str>>,
    ) -> Self {
        Self { notify, timeout_duration, description: description.into() }
    }

    /// Waits for a notification with timeout.
    ///
    /// # Panics
    ///
    /// Panics if the timeout duration elapses before the notification is
    /// received. This is intentional behavior for tests to fail fast rather
    /// than hang.
    #[track_caller]
    pub fn notified(&self) -> impl Future<Output = ()> + '_ {
        let caller = Location::caller();
        let description = Arc::clone(&self.description);

        async move {
            info!(
                description = %description,
                timeout_ms = self.timeout_duration.as_millis(),
                file = caller.file(),
                line = caller.line(),
                column = caller.column(),
                "waiting on timed test notification",
            );

            match timeout(self.timeout_duration, self.notify.notified()).await {
                Ok(()) => {
                    info!(
                        description = %description,
                        file = caller.file(),
                        line = caller.line(),
                        column = caller.column(),
                        "timed test notification received",
                    );
                }
                Err(_) => {
                    error!(
                        description = %description,
                        timeout_ms = self.timeout_duration.as_millis(),
                        file = caller.file(),
                        line = caller.line(),
                        column = caller.column(),
                        "timed test notification timed out",
                    );

                    panic!(
                        "Test notification timed out after {:?} while waiting for {} at {}:{}:{}. \
                         This likely indicates the expected state was never reached. Check if the \
                         condition is reachable.",
                        self.timeout_duration,
                        self.description,
                        caller.file(),
                        caller.line(),
                        caller.column(),
                    );
                }
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
            .field("description", &self.description)
            .finish()
    }
}
