use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;
use tokio::time::{Sleep, sleep};

/// A simple timer future that resolves after a configured duration.
///
/// `Timer` starts inactive; call [`Timer::start`] to arm it. Once started,
/// polling waits until the inner `Sleep` completes, then resolves to `()`.
///
/// Design note: the inner `Sleep` is stored as `Pin<Box<Sleep>>` so that
/// `Timer` itself is `Unpin` and can be used directly in `tokio::select!`.
#[derive(Debug)]
pub struct Timer {
    deadline: Option<Pin<Box<Sleep>>>,
    duration: Duration,
}

impl Timer {
    /// Creates a new, inactive timer for the given `duration`.
    pub fn new(duration: Duration) -> Self {
        Self {
            deadline: None,
            duration,
        }
    }

    /// Arms the timer so that it will resolve after the configured duration.
    pub fn start(&mut self) {
        self.deadline = Some(Box::pin(sleep(self.duration)));
    }
}

impl Future for Timer {
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
