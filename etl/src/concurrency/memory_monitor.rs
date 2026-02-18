use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use etl_config::shared::MemoryBackpressureConfig;
use futures::Stream;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, info};

use crate::concurrency::shutdown::ShutdownRx;

/// Memory refresh interval in milliseconds.
const MEMORY_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

/// Represents a memory snapshot.
#[derive(Debug, Clone, Copy)]
struct MemorySnapshot {
    used: u64,
    total: u64,
}

impl MemorySnapshot {
    /// Refreshes memory readings from the operating system.
    fn from_system(system: &mut sysinfo::System) -> Self {
        system.refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());

        match system.cgroup_limits() {
            Some(cgroup) => MemorySnapshot {
                used: cgroup.rss,
                total: cgroup.total_memory,
            },
            None => MemorySnapshot {
                used: system.used_memory(),
                total: system.total_memory(),
            },
        }
    }

    /// Returns the memory usage percentage in the range `[0.0, 1.0]`.
    fn used_percent(&self) -> f32 {
        let used_percent = self.used as f32 / self.total as f32;
        if used_percent.is_nan() {
            return 1.0;
        }

        used_percent.clamp(0.0, 1.0)
    }
}

/// Internal shared state for memory backpressure.
#[derive(Debug)]
struct MemoryMonitorInner {
    backpressure_active_tx: watch::Sender<bool>,
    config: Option<MemoryBackpressureConfig>,
}

/// Shared memory backpressure controller.
///
/// This component owns a periodic task that samples memory usage and updates a boolean backpressure
/// signal. Consumers can subscribe and pause polling when backpressure is active.
#[derive(Debug, Clone)]
pub struct MemoryMonitor {
    inner: Arc<MemoryMonitorInner>,
}

impl MemoryMonitor {
    /// Creates a new memory backpressure controller and starts the refresh task.
    pub fn new(mut shutdown_rx: ShutdownRx, config: Option<MemoryBackpressureConfig>) -> Self {
        // sysinfo docs suggest to use a single instance of `System` across the program.
        let mut system = sysinfo::System::new();

        // Initialize from a real memory snapshot so startup state reflects current pressure.
        let startup_snapshot = MemorySnapshot::from_system(&mut system);
        let startup_used_percent = startup_snapshot.used_percent();
        let startup_backpressure_active = match &config {
            Some(config) => compute_next_backpressure_active(
                false,
                startup_used_percent,
                config.activate_threshold,
                config.resume_threshold,
            ),
            None => false,
        };

        let this = Self {
            inner: Arc::new(MemoryMonitorInner {
                backpressure_active_tx: watch::channel(startup_backpressure_active).0,
                config,
            }),
        };

        let this_clone = this.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(MEMORY_REFRESH_INTERVAL);
            let mut currently_backpressure_active = startup_backpressure_active;

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.changed() => {
                        info!("memory monitor stopped due to shutdown");

                        return;
                    }

                    _ = ticker.tick() => {
                        let snapshot = MemorySnapshot::from_system(&mut system);
                        let used_percent = snapshot.used_percent();
                        match this_clone.inner.config.as_ref() {
                            Some(config) => {
                                let next_backpressure_active = compute_next_backpressure_active(
                                    currently_backpressure_active,
                                    used_percent,
                                    config.activate_threshold,
                                    config.resume_threshold,
                                );

                                debug!(
                                    used_memory_bytes = snapshot.used,
                                    total_memory_bytes = snapshot.total,
                                    used_percent,
                                    backpressure_active = currently_backpressure_active,
                                    next_backpressure_active,
                                    "memory monitor refreshed memory snapshot"
                                );

                                if next_backpressure_active != currently_backpressure_active {
                                    debug!(
                                        backpressure_active = currently_backpressure_active,
                                        next_backpressure_active,
                                        used_percent,
                                        "memory monitor state changed"
                                    );
                                }

                                currently_backpressure_active = next_backpressure_active;
                                this_clone.set_backpressure_active(next_backpressure_active);
                            }
                            None => {
                                debug!(
                                    used_memory_bytes = snapshot.used,
                                    total_memory_bytes = snapshot.total,
                                    used_percent,
                                    backpressure_active = currently_backpressure_active,
                                    "memory monitor refreshed memory snapshot with backpressure disabled"
                                );
                            }
                        }
                    }
                }
            }
        });

        this
    }

    /// Returns `true` when memory pressure currently activates backpressure.
    pub fn is_backpressure_active(&self) -> bool {
        *self.inner.backpressure_active_tx.borrow()
    }

    /// Creates a new subscription for polling backpressure updates.
    pub fn subscribe(&self) -> MemoryMonitorSubscription {
        // We snapshot the current state of the watch channel and create a stream out of it. The
        // stream will return the new values from this point onward, independently of when it will be
        // polled.
        let rx = self.inner.backpressure_active_tx.subscribe();
        let updates = WatchStream::from_changes(rx.clone());

        MemoryMonitorSubscription {
            current_rx: rx,
            updates,
        }
    }

    /// Updates the backpressure active state and notifies subscribers when it changes.
    fn set_backpressure_active(&self, backpressure_active: bool) {
        let _ = self
            .inner
            .backpressure_active_tx
            .send_if_modified(|current| {
                if *current == backpressure_active {
                    return false;
                }

                *current = backpressure_active;

                true
            });
    }
}

/// Computes the next backpressure active state given the current state and memory usage.
fn compute_next_backpressure_active(
    currently_backpressure_active: bool,
    used_percent: f32,
    activate_threshold: f32,
    resume_threshold: f32,
) -> bool {
    if currently_backpressure_active {
        return used_percent >= resume_threshold;
    }

    used_percent >= activate_threshold
}

#[cfg(test)]
impl MemoryMonitor {
    /// Creates a new memory backpressure controller without spawning a refresh task.
    pub fn new_for_test() -> Self {
        Self {
            inner: Arc::new(MemoryMonitorInner {
                backpressure_active_tx: watch::channel(false).0,
                config: None,
            }),
        }
    }

    /// Updates the backpressure active state in tests.
    pub fn set_backpressure_active_for_test(&self, backpressure_active: bool) {
        self.set_backpressure_active(backpressure_active);
    }
}

/// Subscription to memory backpressure updates.
///
/// This type provides wake-safe polling semantics so streams can return `Pending` while memory is
/// active without risking missed wakeups.
#[derive(Debug)]
pub struct MemoryMonitorSubscription {
    current_rx: watch::Receiver<bool>,
    updates: WatchStream<bool>,
}

impl MemoryMonitorSubscription {
    /// Returns the current backpressure active flag.
    pub fn current_backpressure_active(&self) -> bool {
        *self.current_rx.borrow()
    }

    /// Polls for a new backpressure update.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(backpressure_active))` when there is an unseen update.
    /// - `Poll::Ready(None)` when the underlying signal channel is closed.
    /// - `Poll::Pending` when no update is available yet.
    pub fn poll_update(&mut self, cx: &mut Context<'_>) -> Poll<Option<bool>> {
        match std::pin::Pin::new(&mut self.updates).poll_next(cx) {
            Poll::Ready(Some(backpressure_active)) => Poll::Ready(Some(backpressure_active)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_used_percent_handles_zero_total() {
        let snapshot = MemorySnapshot {
            used: 100,
            total: 0,
        };
        assert_eq!(snapshot.used_percent(), 1.0);
    }

    #[test]
    fn memory_used_percent_half() {
        let snapshot = MemorySnapshot {
            used: 50,
            total: 100,
        };
        assert_eq!(snapshot.used_percent(), 0.5);
    }

    #[test]
    fn threshold_hysteresis_blocks_and_releases() {
        let activate_threshold = 0.85;
        let resume_threshold = 0.75;
        assert!(!compute_next_backpressure_active(
            false,
            activate_threshold - 0.01,
            activate_threshold,
            resume_threshold
        ));
        assert!(compute_next_backpressure_active(
            false,
            activate_threshold + 0.01,
            activate_threshold,
            resume_threshold
        ));
        assert!(compute_next_backpressure_active(
            true,
            resume_threshold + 0.01,
            activate_threshold,
            resume_threshold
        ));
        assert!(!compute_next_backpressure_active(
            true,
            resume_threshold - 0.01,
            activate_threshold,
            resume_threshold
        ));
    }

    #[tokio::test]
    async fn subscription_receives_backpressure_transitions() {
        let signal = MemoryMonitor::new_for_test();
        let mut sub = signal.subscribe();

        signal.set_backpressure_active_for_test(true);
        let backpressure_active = futures::future::poll_fn(|cx| sub.poll_update(cx)).await;
        assert_eq!(backpressure_active, Some(true));

        signal.set_backpressure_active_for_test(false);
        let backpressure_active = futures::future::poll_fn(|cx| sub.poll_update(cx)).await;
        assert_eq!(backpressure_active, Some(false));
    }
}
