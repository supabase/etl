use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;
use tokio::time::{Sleep, sleep};

/// A simple future that resolves after a configured duration.
///
/// [`DeferredTimer`] starts in an inactive state. Call [`DeferredTimer::start`]
/// to arm it. Once started, polling waits until the inner [`Sleep`] completes,
/// then resolves to `()`.
///
/// The timer remains pending while inactive, which is convenient for optional
/// branches in `tokio::select!` where the timer should not fire unless armed.
#[derive(Debug)]
pub struct DeferredTimer {
    /// The active deadline if armed, or `None` when inactive.
    deadline: Option<Pin<Box<Sleep>>>,
    /// Duration used when (re)arming the timer.
    duration: Duration,
}

impl DeferredTimer {
    /// Creates a new, inactive timer for the given `duration`.
    ///
    /// The returned [`DeferredTimer`] does not progress until
    /// [`DeferredTimer::start`] is called.
    pub fn new(duration: Duration) -> Self {
        Self {
            deadline: None,
            duration,
        }
    }

    /// Arms the timer so that it will resolve after the configured duration.
    ///
    /// Calling this method replaces any previously armed deadline with a new
    /// one based on the stored duration. It is safe to call multiple times.
    pub fn start(&mut self) {
        self.deadline = Some(Box::pin(sleep(self.duration)));
    }
}

impl Future for DeferredTimer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let Some(deadline) = this.deadline.as_mut() else {
            return Poll::Pending;
        };

        ready!(deadline.as_mut().poll(cx));

        Poll::Ready(())
    }
}
