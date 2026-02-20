use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use std::time::Instant;

use etl_config::shared::MemoryBackpressureConfig;
use futures::Stream;
use metrics::{counter, gauge, histogram};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, info};

use crate::concurrency::shutdown::ShutdownRx;
use crate::metrics::{
    DIRECTION_LABEL, ETL_MEMORY_BACKPRESSURE_ACTIVATION_DURATION_SECONDS,
    ETL_MEMORY_BACKPRESSURE_ACTIVE, ETL_MEMORY_BACKPRESSURE_TRANSITIONS_TOTAL, PIPELINE_ID_LABEL,
};
use crate::types::PipelineId;

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
    backpressure: Option<BackpressureMonitorInner>,
    total_memory_bytes: AtomicU64,
    memory_refresh_interval_ms: u64,
}

/// Shared backpressure state that exists only when backpressure is configured.
#[derive(Debug)]
struct BackpressureMonitorInner {
    active_tx: watch::Sender<bool>,
    config: MemoryBackpressureConfig,
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
    pub fn new(
        pipeline_id: PipelineId,
        mut shutdown_rx: ShutdownRx,
        memory_backpressure_config: Option<MemoryBackpressureConfig>,
        memory_refresh_interval_ms: u64,
    ) -> Self {
        // sysinfo docs suggest to use a single instance of `System` across the program.
        let mut system = sysinfo::System::new();

        // Initialize from a real memory snapshot so startup state reflects current pressure.
        let startup_snapshot = MemorySnapshot::from_system(&mut system);
        let backpressure = memory_backpressure_config.map(|config| {
            let startup_backpressure_active = compute_next_backpressure_active(
                false,
                startup_snapshot.used_percent(),
                config.activate_threshold,
                config.resume_threshold,
            );
            emit_backpressure_active_metric(pipeline_id, startup_backpressure_active);

            BackpressureMonitorInner {
                active_tx: watch::channel(startup_backpressure_active).0,
                config,
            }
        });

        let this = Self {
            inner: Arc::new(MemoryMonitorInner {
                backpressure,
                total_memory_bytes: AtomicU64::new(startup_snapshot.total),
                memory_refresh_interval_ms,
            }),
        };

        let this_clone = this.clone();
        tokio::spawn(async move {
            let refresh_interval =
                Duration::from_millis(this_clone.inner.memory_refresh_interval_ms);

            let mut ticker = tokio::time::interval(refresh_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let mut currently_backpressure_active = this_clone.is_backpressure_active();
            let mut activation_started_at = currently_backpressure_active.then(Instant::now);

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.changed() => {
                        info!("memory monitor stopped due to shutdown");

                        return;
                    }

                    _ = ticker.tick() => {
                        let snapshot = MemorySnapshot::from_system(&mut system);
                        this_clone
                            .inner
                            .total_memory_bytes
                            .store(snapshot.total, Ordering::Relaxed);

                        if let Some(backpressure) = this_clone.inner.backpressure.as_ref() {
                            let used_percent = snapshot.used_percent();
                            let next_backpressure_active = compute_next_backpressure_active(
                                currently_backpressure_active,
                                used_percent,
                                backpressure.config.activate_threshold,
                                backpressure.config.resume_threshold,
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

                                emit_backpressure_active_metric(
                                    pipeline_id,
                                    next_backpressure_active,
                                );
                                emit_transition_metric(
                                    pipeline_id,
                                    next_backpressure_active,
                                );

                                if next_backpressure_active {
                                    activation_started_at = Some(Instant::now());
                                } else if let Some(started_at) = activation_started_at.take() {
                                    emit_activation_duration_metric(
                                        pipeline_id,
                                        started_at.elapsed(),
                                    );
                                }
                            }

                            currently_backpressure_active = next_backpressure_active;
                            this_clone.set_backpressure_active(next_backpressure_active);
                        } else {
                            debug!(
                                used_memory_bytes = snapshot.used,
                                total_memory_bytes = snapshot.total,
                                "memory monitor refreshed memory snapshot without backpressure"
                            );
                        }
                    }
                }
            }
        });

        this
    }

    /// Returns `true` when memory pressure currently activates backpressure.
    pub fn is_backpressure_active(&self) -> bool {
        self.inner
            .backpressure
            .as_ref()
            .map(|backpressure| *backpressure.active_tx.borrow())
            .unwrap_or(false)
    }

    /// Creates a new subscription for polling backpressure updates.
    ///
    /// Returns [`None`] when memory backpressure is not configured.
    pub fn subscribe(&self) -> Option<MemoryMonitorSubscription> {
        let backpressure = self.inner.backpressure.as_ref()?;

        // We snapshot the current state of the watch channel and create a stream out of it. The
        // stream will return the new values from this point onward, independently of when it will be
        // polled.
        let rx = backpressure.active_tx.subscribe();
        let updates = WatchStream::from_changes(rx.clone());

        Some(MemoryMonitorSubscription {
            current_rx: rx,
            updates,
        })
    }

    /// Returns the shared atomic that stores total memory in bytes.
    pub fn total_memory_bytes(&self) -> u64 {
        self.inner.total_memory_bytes.load(Ordering::Relaxed)
    }

    /// Updates the backpressure active state and notifies subscribers when it changes.
    fn set_backpressure_active(&self, backpressure_active: bool) {
        let Some(backpressure) = self.inner.backpressure.as_ref() else {
            return;
        };

        let _ = backpressure.active_tx.send_if_modified(|current| {
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

fn emit_backpressure_active_metric(pipeline_id: PipelineId, backpressure_active: bool) {
    gauge!(
        ETL_MEMORY_BACKPRESSURE_ACTIVE,
        PIPELINE_ID_LABEL => pipeline_id.to_string()
    )
    .set(if backpressure_active { 1.0 } else { 0.0 });
}

fn emit_transition_metric(pipeline_id: PipelineId, backpressure_active: bool) {
    counter!(
        ETL_MEMORY_BACKPRESSURE_TRANSITIONS_TOTAL,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        DIRECTION_LABEL => if backpressure_active { "activate" } else { "resume" }
    )
    .increment(1);
}

fn emit_activation_duration_metric(pipeline_id: PipelineId, duration: Duration) {
    histogram!(
        ETL_MEMORY_BACKPRESSURE_ACTIVATION_DURATION_SECONDS,
        PIPELINE_ID_LABEL => pipeline_id.to_string()
    )
    .record(duration.as_secs_f64());
}

#[cfg(test)]
impl MemoryMonitor {
    /// Creates a new memory backpressure controller without spawning a refresh task.
    pub fn new_for_test() -> Self {
        Self {
            inner: Arc::new(MemoryMonitorInner {
                backpressure: Some(BackpressureMonitorInner {
                    active_tx: watch::channel(false).0,
                    config: MemoryBackpressureConfig::default(),
                }),
                total_memory_bytes: AtomicU64::new(0),
                memory_refresh_interval_ms: 100,
            }),
        }
    }

    /// Updates the backpressure active state in tests.
    pub fn set_backpressure_active_for_test(&self, backpressure_active: bool) {
        self.set_backpressure_active(backpressure_active);
    }

    /// Updates the total memory snapshot in bytes for tests.
    pub fn set_total_memory_bytes_for_test(&self, total_memory_bytes: u64) {
        self.inner
            .total_memory_bytes
            .store(total_memory_bytes, Ordering::Relaxed);
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
}

impl Stream for MemoryMonitorSubscription {
    type Item = bool;

    /// Polls for a new backpressure update.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(backpressure_active))` when there is an unseen update.
    /// - `Poll::Ready(None)` when the underlying signal channel is closed.
    /// - `Poll::Pending` when no update is available yet.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.updates).poll_next(cx)
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
        let mut sub = signal
            .subscribe()
            .expect("backpressure subscription should exist in tests");

        signal.set_backpressure_active_for_test(true);
        let backpressure_active =
            futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sub).poll_next(cx)).await;
        assert_eq!(backpressure_active, Some(true));

        signal.set_backpressure_active_for_test(false);
        let backpressure_active =
            futures::future::poll_fn(|cx| std::pin::Pin::new(&mut sub).poll_next(cx)).await;
        assert_eq!(backpressure_active, Some(false));
    }
}
