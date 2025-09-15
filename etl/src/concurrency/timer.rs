//! Lightweight timer utilities for cooperative shutdown and backoff.
//!
//! This module provides a minimal deferred timer implementation used in places
//! where a timer must be armed conditionally and awaited inside a `tokio::select!`.
//! The timer is inert until explicitly started via [`DeferredTimer::start`],
//! making it a good fit for "arm-on-demand" flows such as delayed or forced
//! shutdown paths.
//!
//! Design notes:
//! - [`DeferredTimer`] is `Unpin`, so it can be moved freely and used directly
//!   in `select!` statements without additional pinning.
//! - The inner [`Sleep`] future is boxed to keep the outer type small and stable.
//! - Re-arming is supported by calling [`DeferredTimer::start`] again after it
//!   resolves, which replaces the inner sleep with a new one.
//!
//! No panics are expected from this module.

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
