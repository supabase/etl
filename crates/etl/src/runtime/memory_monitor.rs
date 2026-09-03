//! Coherent system and cgroup memory sampling with emergency backpressure.
//!
//! On Linux, the monitor starts at the current process's cgroup, found through
//! `/proc/<pid>/cgroup`, when that hierarchy imposes a tighter capacity than
//! the host. [`sysinfo::Process::cgroup_limits`] reads the process leaf and
//! walks the ancestors visible in the process's cgroup namespace, independently
//! retaining the smallest finite capacity and the smallest remaining headroom
//! (`memory.max - memory.current`). A charge must fit at both the leaf and
//! every ancestor, so the smallest visible headroom represents the earliest
//! boundary this process can observe that could reject its next charge.
//!
//! Kubernetes normally applies each container's memory limit to an independent
//! leaf cgroup, so a sibling container's usage does not consume the process
//! leaf's allowance. A tighter ancestor affects the snapshot only when that
//! ancestor is visible in the process's cgroup namespace. Container runtimes
//! commonly use a private cgroup namespace, where `/proc/<pid>/cgroup` reports
//! `/` and `/sys/fs/cgroup` is rooted at the container leaf. Host-side pod and
//! QoS ancestors still enforce their limits but are not observable from that
//! namespace, so this monitor cannot include them. The monitor therefore
//! controls against the container leaf in that arrangement and accounts for
//! ancestors only when the runtime exposes them. An unconstrained Linux process
//! instead uses host-wide memory, avoiding a misleading ratio based only on the
//! process's leaf-cgroup charge divided by all host RAM.
//! The full cgroup charge is used instead of resident set size because the
//! kernel's memory controller also accounts for page cache, socket buffers,
//! and kernel memory that contribute to the enforced limit. If a previously
//! available cgroup read fails transiently, the last cgroup snapshot is kept
//! rather than falling back to a potentially larger host limit.
//!
//! Platforms without Linux memory cgroups, including macOS, use sysinfo's
//! system-wide used and total memory readings. One shared sampler performs
//! these reads; batch hot paths consume the published snapshot without reading
//! operating-system files themselves.
//!
//! Emergency backpressure pauses new source polling while destination results
//! and already-owned batches continue to drain. Resume is deliberately based
//! on the same full-domain measurement. If measured usage keeps that domain
//! above the resume threshold, the pipeline stays paused rather than probing
//! with more source data and risking an OOM kill.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use etl_config::shared::MemoryBackpressureConfig;
use futures::Stream;
use metrics::{counter, gauge, histogram};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
    time::MissedTickBehavior,
};
use tokio_stream::wrappers::WatchStream;
use tracing::{info, trace};

use crate::{
    observability::{
        DIRECTION_LABEL, ETL_MEMORY_BACKPRESSURE_ACTIVATION_DURATION_SECONDS,
        ETL_MEMORY_BACKPRESSURE_ACTIVE, ETL_MEMORY_BACKPRESSURE_TRANSITIONS_TOTAL,
        ETL_MEMORY_TOTAL_BYTES, ETL_MEMORY_USED_BYTES, MEMORY_SOURCE_LABEL,
    },
    runtime::concurrency::ShutdownRx,
};

/// Identifies the memory domain represented by a [`MemorySnapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemorySnapshotSource {
    /// Current process leaf cgroup, with visible ancestor constraints applied.
    ProcessCgroup,
    /// Root cgroup visible to the process.
    RootCgroup,
    /// Host-wide operating-system memory.
    System,
}

impl MemorySnapshotSource {
    /// Returns the stable diagnostic name for the memory source.
    const fn as_str(self) -> &'static str {
        match self {
            Self::ProcessCgroup => "process_cgroup",
            Self::RootCgroup => "root_cgroup",
            Self::System => "system",
        }
    }
}

/// Represents one coherent memory-domain snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemorySnapshot {
    /// Effective memory usage within the selected domain.
    used: u64,
    /// Effective system capacity or cgroup memory limit.
    total: u64,
    /// Domain from which the snapshot was read.
    source: MemorySnapshotSource,
}

/// Result of attempting to refresh the selected memory domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryRefresh {
    /// Snapshot to use for backpressure decisions.
    snapshot: MemorySnapshot,
    /// Whether the snapshot was read during this refresh attempt.
    fresh: bool,
}

impl MemoryRefresh {
    /// Creates a refresh result from a newly read snapshot.
    const fn fresh(snapshot: MemorySnapshot) -> Self {
        Self { snapshot, fresh: true }
    }

    /// Creates a refresh result that retains the last successful snapshot.
    const fn retained(snapshot: MemorySnapshot) -> Self {
        Self { snapshot, fresh: false }
    }
}

impl MemorySnapshot {
    /// Refreshes the most specific enforceable memory domain available.
    ///
    /// The returned freshness flag is false when a transient cgroup read
    /// failure requires retaining the previous snapshot.
    fn refresh(
        system: &mut sysinfo::System,
        current_pid: Option<sysinfo::Pid>,
        previous: Option<Self>,
    ) -> MemoryRefresh {
        // Host memory distinguishes a genuinely constrained cgroup from the
        // effectively unlimited cgroups used by ordinary Linux processes. It is
        // also the fallback on platforms without memory cgroups.
        system.refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());
        let system_snapshot = Self {
            used: system.used_memory(),
            total: system.total_memory(),
            source: MemorySnapshotSource::System,
        };

        if let Some(cgroup) = current_pid
            .and_then(|pid| system.process(pid))
            .and_then(sysinfo::Process::cgroup_limits)
        {
            let snapshot = Self::constrained_cgroup_snapshot(
                &cgroup,
                MemorySnapshotSource::ProcessCgroup,
                system_snapshot,
            )
            .unwrap_or(system_snapshot);

            return MemoryRefresh::fresh(snapshot);
        }

        // Once the process cgroup has been observed, a missing read is treated as
        // transient. Falling back to host memory could momentarily expand the batch
        // budget far beyond the pod's actual limit.
        if let Some(previous @ Self { source: MemorySnapshotSource::ProcessCgroup, .. }) = previous
        {
            trace!(
                memory_source = previous.source.as_str(),
                "failed to refresh process cgroup memory; retaining previous snapshot"
            );

            return MemoryRefresh::retained(previous);
        }

        if let Some(cgroup) = system.cgroup_limits() {
            let snapshot = Self::constrained_cgroup_snapshot(
                &cgroup,
                MemorySnapshotSource::RootCgroup,
                system_snapshot,
            )
            .unwrap_or(system_snapshot);

            return MemoryRefresh::fresh(snapshot);
        }

        if let Some(previous @ Self { source: MemorySnapshotSource::RootCgroup, .. }) = previous {
            trace!(
                memory_source = previous.source.as_str(),
                "failed to refresh root cgroup memory; retaining previous snapshot"
            );

            return MemoryRefresh::retained(previous);
        }

        MemoryRefresh::fresh(system_snapshot)
    }

    /// Builds a snapshot from effective hierarchical cgroup capacity and
    /// headroom.
    ///
    /// `sysinfo` minimizes capacity and headroom independently across the
    /// process leaf and the ancestors visible in its cgroup namespace.
    /// Consequently, `total - free` is a conservative pressure value for the
    /// visible hierarchy and is not necessarily the literal `memory.current` of
    /// any one cgroup.
    fn from_cgroup_limits(cgroup: &sysinfo::CGroupLimits, source: MemorySnapshotSource) -> Self {
        // `rss` only contains anonymous resident memory and misses other charges
        // enforced by the memory controller.
        Self {
            used: cgroup.total_memory.saturating_sub(cgroup.free_memory),
            total: cgroup.total_memory,
            source,
        }
    }

    /// Returns a cgroup snapshot only when it constrains host capacity.
    fn constrained_cgroup_snapshot(
        cgroup: &sysinfo::CGroupLimits,
        source: MemorySnapshotSource,
        system_snapshot: Self,
    ) -> Option<Self> {
        let snapshot = Self::from_cgroup_limits(cgroup, source);

        (snapshot.total < system_snapshot.total).then_some(snapshot)
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
    /// Handle for the task that refreshes memory snapshots.
    refresh_task: Mutex<Option<JoinHandle<()>>>,
    /// Optional backpressure state derived from memory snapshots.
    backpressure: Option<BackpressureMonitor>,
    /// Latest coherent used and total memory snapshot.
    snapshot: RwLock<MemorySnapshot>,
    /// Revision incremented after each complete snapshot update.
    snapshot_revision: AtomicU64,
    /// Interval between memory refreshes in milliseconds.
    memory_refresh_interval_ms: u64,
}

/// Shared backpressure state that exists only when backpressure is configured.
#[derive(Debug)]
struct BackpressureMonitor {
    /// Latest emergency backpressure state and notification channel.
    active_tx: watch::Sender<bool>,
    /// Validated activation and resume thresholds.
    config: MemoryBackpressureConfig,
}

/// Shared memory monitor and emergency backpressure controller.
///
/// This component owns a periodic task that samples memory usage and updates a
/// boolean backpressure signal. Consumers can subscribe and pause polling when
/// backpressure is active.
#[derive(Debug, Clone)]
pub(crate) struct MemoryMonitor {
    /// Shared sampler state and optional emergency backpressure controller.
    inner: Arc<MemoryMonitorInner>,
}

impl MemoryMonitor {
    /// Creates a new memory monitor and starts its refresh task.
    pub(crate) fn new(
        mut shutdown_rx: ShutdownRx,
        memory_backpressure_config: Option<MemoryBackpressureConfig>,
        memory_refresh_interval_ms: u64,
    ) -> Self {
        // sysinfo docs suggest using a single `System` instance across the program.
        let mut system = sysinfo::System::new();
        let current_pid = sysinfo::get_current_pid().ok();
        if let Some(current_pid) = current_pid {
            system.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[current_pid]),
                false,
                sysinfo::ProcessRefreshKind::nothing(),
            );
        }

        // Initialize from a real memory snapshot so startup state reflects current
        // pressure.
        let startup_snapshot = MemorySnapshot::refresh(&mut system, current_pid, None).snapshot;
        emit_memory_snapshot_metrics(startup_snapshot, None);
        let backpressure = memory_backpressure_config.map(|config| {
            let startup_backpressure_active = compute_next_backpressure_active(
                false,
                startup_snapshot.used_percent(),
                config.activate_threshold,
                config.resume_threshold,
            );
            emit_backpressure_active_metric(startup_backpressure_active);

            BackpressureMonitor { active_tx: watch::channel(startup_backpressure_active).0, config }
        });

        let this = Self {
            inner: Arc::new(MemoryMonitorInner {
                refresh_task: Mutex::new(None),
                backpressure,
                snapshot: RwLock::new(startup_snapshot),
                snapshot_revision: AtomicU64::new(0),
                memory_refresh_interval_ms,
            }),
        };

        let this_clone = this.clone();
        let refresh_task = tokio::spawn(async move {
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
                        let previous_snapshot = this_clone.current_snapshot();
                        let refresh = MemorySnapshot::refresh(
                            &mut system,
                            current_pid,
                            Some(previous_snapshot),
                        );
                        let snapshot = refresh.snapshot;
                        this_clone.publish_refresh(refresh);

                        if let Some(backpressure) = this_clone.inner.backpressure.as_ref() {
                            let used_percent = snapshot.used_percent();
                            let next_backpressure_active = compute_next_backpressure_active(
                                currently_backpressure_active,
                                used_percent,
                                backpressure.config.activate_threshold,
                                backpressure.config.resume_threshold,
                            );

                            trace!(
                                used_memory_bytes = snapshot.used,
                                total_memory_bytes = snapshot.total,
                                memory_source = snapshot.source.as_str(),
                                used_percent,
                                backpressure_active = currently_backpressure_active,
                                next_backpressure_active,
                                "memory monitor refreshed memory snapshot"
                            );

                            if next_backpressure_active != currently_backpressure_active {
                                trace!(
                                    backpressure_active = currently_backpressure_active,
                                    next_backpressure_active,
                                    used_percent,
                                    "memory monitor state changed"
                                );

                                emit_backpressure_active_metric(next_backpressure_active);
                                emit_transition_metric(next_backpressure_active);

                                if next_backpressure_active {
                                    activation_started_at = Some(Instant::now());
                                } else if let Some(started_at) = activation_started_at.take() {
                                    emit_activation_duration_metric(started_at.elapsed());
                                }
                            }

                            currently_backpressure_active = next_backpressure_active;
                            this_clone.set_backpressure_active(next_backpressure_active);
                        } else {
                            trace!(
                                used_memory_bytes = snapshot.used,
                                total_memory_bytes = snapshot.total,
                                memory_source = snapshot.source.as_str(),
                                "memory monitor refreshed memory snapshot without backpressure"
                            );
                        }
                    }
                }
            }
        });
        *this.inner.refresh_task.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(refresh_task);

        this
    }

    /// Returns `true` when memory pressure currently activates backpressure.
    pub(crate) fn is_backpressure_active(&self) -> bool {
        self.inner
            .backpressure
            .as_ref()
            .is_some_and(|backpressure| *backpressure.active_tx.borrow())
    }

    /// Creates a new subscription for polling backpressure updates.
    ///
    /// Returns [`None`] when memory backpressure is not configured.
    pub(crate) fn subscribe(&self) -> Option<MemoryMonitorSubscription> {
        let backpressure = self.inner.backpressure.as_ref()?;

        // Retain a receiver for current-state reads while the stream yields only
        // changes that occur after subscription.
        let rx = backpressure.active_tx.subscribe();
        let updates = WatchStream::from_changes(rx.clone());

        Some(MemoryMonitorSubscription { current_rx: rx, updates })
    }

    /// Returns the memory capacity and revision used for batch governance.
    pub(crate) fn capacity_snapshot(&self) -> MemoryCapacitySnapshot {
        let snapshot = self.inner.snapshot.read().unwrap_or_else(PoisonError::into_inner);

        MemoryCapacitySnapshot {
            revision: self.inner.snapshot_revision.load(Ordering::Relaxed),
            total_memory_bytes: snapshot.total,
        }
    }

    /// Returns the revision of the latest complete memory snapshot.
    ///
    /// The revision is only a change-detection hint. Snapshot contents are
    /// synchronized independently by their [`RwLock`], so this load does not
    /// publish any associated data.
    pub(crate) fn snapshot_revision(&self) -> u64 {
        self.inner.snapshot_revision.load(Ordering::Relaxed)
    }

    /// Waits for the refresh task to finish after shutdown.
    pub(crate) async fn wait_for_refresh_task(&self) -> Result<(), JoinError> {
        let refresh_task =
            self.inner.refresh_task.lock().unwrap_or_else(PoisonError::into_inner).take();

        if let Some(refresh_task) = refresh_task {
            refresh_task.await?;
        }

        Ok(())
    }

    /// Updates the backpressure active state and notifies subscribers when it
    /// changes.
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

    /// Returns the latest complete memory snapshot.
    fn current_snapshot(&self) -> MemorySnapshot {
        *self.inner.snapshot.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Publishes a memory refresh only when it contains a new reading.
    fn publish_refresh(&self, refresh: MemoryRefresh) {
        if refresh.fresh {
            self.publish_snapshot(refresh.snapshot);
        }
    }

    /// Publishes one coherent memory snapshot and advances its wrapping
    /// revision.
    fn publish_snapshot(&self, snapshot: MemorySnapshot) {
        let previous_source = {
            let mut current = self.inner.snapshot.write().unwrap_or_else(PoisonError::into_inner);
            let previous_source = current.source;
            *current = snapshot;

            // Wrapping is intentional. Governor readers compare revisions for
            // inequality, so `u64::MAX -> 0` still denotes a new snapshot. Update
            // it while the snapshot is write-locked so readers cannot pair this
            // snapshot with the preceding revision.
            self.inner.snapshot_revision.fetch_add(1, Ordering::Relaxed);

            previous_source
        };

        emit_memory_snapshot_metrics(snapshot, Some(previous_source));
    }
}

/// Computes the next backpressure active state given the current state and
/// memory usage.
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

/// Records whether emergency memory backpressure is currently active.
fn emit_backpressure_active_metric(backpressure_active: bool) {
    gauge!(ETL_MEMORY_BACKPRESSURE_ACTIVE).set(if backpressure_active { 1.0 } else { 0.0 });
}

/// Emits one coherent used/capacity pair and clears stale source series.
fn emit_memory_snapshot_metrics(
    snapshot: MemorySnapshot,
    previous_source: Option<MemorySnapshotSource>,
) {
    if let Some(previous_source) = previous_source.filter(|source| *source != snapshot.source) {
        gauge!(
            ETL_MEMORY_USED_BYTES,
            MEMORY_SOURCE_LABEL => previous_source.as_str()
        )
        .set(0.0);
        gauge!(
            ETL_MEMORY_TOTAL_BYTES,
            MEMORY_SOURCE_LABEL => previous_source.as_str()
        )
        .set(0.0);
    }

    gauge!(
        ETL_MEMORY_USED_BYTES,
        MEMORY_SOURCE_LABEL => snapshot.source.as_str()
    )
    .set(snapshot.used as f64);
    gauge!(
        ETL_MEMORY_TOTAL_BYTES,
        MEMORY_SOURCE_LABEL => snapshot.source.as_str()
    )
    .set(snapshot.total as f64);
}

/// Counts one emergency backpressure activation or resume transition.
fn emit_transition_metric(backpressure_active: bool) {
    counter!(
        ETL_MEMORY_BACKPRESSURE_TRANSITIONS_TOTAL,
        DIRECTION_LABEL => if backpressure_active { "activate" } else { "resume" }
    )
    .increment(1);
}

/// Records the duration of one completed emergency backpressure interval.
fn emit_activation_duration_metric(duration: Duration) {
    histogram!(ETL_MEMORY_BACKPRESSURE_ACTIVATION_DURATION_SECONDS).record(duration.as_secs_f64());
}

#[cfg(test)]
impl MemoryMonitor {
    /// Creates a new memory monitor without spawning a refresh task.
    pub(crate) fn new_for_test() -> Self {
        Self::new_for_test_with_backpressure(Some(MemoryBackpressureConfig::default()))
    }

    /// Creates a memory monitor with configurable backpressure for tests.
    pub(crate) fn new_for_test_with_backpressure(config: Option<MemoryBackpressureConfig>) -> Self {
        Self {
            inner: Arc::new(MemoryMonitorInner {
                refresh_task: Mutex::new(None),
                backpressure: config.map(|config| BackpressureMonitor {
                    active_tx: watch::channel(false).0,
                    config,
                }),
                snapshot: RwLock::new(MemorySnapshot {
                    used: 0,
                    total: 0,
                    source: MemorySnapshotSource::System,
                }),
                snapshot_revision: AtomicU64::new(0),
                memory_refresh_interval_ms: 100,
            }),
        }
    }

    /// Updates the backpressure active state in tests.
    pub(crate) fn set_backpressure_active_for_test(&self, backpressure_active: bool) {
        self.set_backpressure_active(backpressure_active);
    }

    /// Updates the total memory snapshot in bytes for tests.
    pub(crate) fn set_total_memory_bytes_for_test(&self, total_memory_bytes: u64) {
        let mut snapshot = *self.inner.snapshot.read().unwrap_or_else(PoisonError::into_inner);
        snapshot.total = total_memory_bytes;
        self.publish_snapshot(snapshot);
    }

    /// Replaces the coherent memory snapshot in tests.
    pub(crate) fn set_memory_snapshot_for_test(&self, used: u64, total: u64) {
        self.publish_snapshot(MemorySnapshot { used, total, source: MemorySnapshotSource::System });
    }

    /// Sets the snapshot revision directly for wrapping tests.
    pub(crate) fn set_snapshot_revision_for_test(&self, revision: u64) {
        self.inner.snapshot_revision.store(revision, Ordering::Relaxed);
    }
}

/// System or cgroup memory capacity used for batch governance.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MemoryCapacitySnapshot {
    /// Revision identifying this exact coherent snapshot.
    pub(crate) revision: u64,
    /// Latest system or cgroup memory capacity in bytes.
    pub(crate) total_memory_bytes: u64,
}

/// Subscription to memory backpressure updates.
///
/// This type provides wake-safe polling semantics so streams can return
/// `Pending` while memory is active without risking missed wakeups.
#[derive(Debug)]
pub(crate) struct MemoryMonitorSubscription {
    /// Receiver used for race-free current-state reads.
    current_rx: watch::Receiver<bool>,
    /// Stream of changes that occur after subscription.
    updates: WatchStream<bool>,
}

impl MemoryMonitorSubscription {
    /// Returns the current backpressure active flag.
    pub(crate) fn current_backpressure_active(&self) -> bool {
        *self.current_rx.borrow()
    }
}

impl Stream for MemoryMonitorSubscription {
    type Item = bool;

    /// Polls for a new backpressure update.
    ///
    /// Returns:
    /// - `Poll::Ready(Some(backpressure_active))` when there is an unseen
    ///   update.
    /// - `Poll::Ready(None)` when the underlying signal channel is closed.
    /// - `Poll::Pending` when no update is available yet.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.updates).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use metrics::{
        Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
        with_local_recorder,
    };

    use super::*;
    use crate::runtime::BatchMemoryGovernor;

    /// One captured gauge assignment.
    #[derive(Debug, PartialEq)]
    struct GaugeAssignment {
        /// Metric name.
        metric: String,
        /// Memory source label.
        source: String,
        /// Assigned gauge value.
        value: f64,
    }

    impl GaugeAssignment {
        /// Creates an expected gauge assignment.
        fn new(metric: &str, source: &str, value: f64) -> Self {
            Self { metric: metric.to_owned(), source: source.to_owned(), value }
        }
    }

    /// Gauge handle that captures assignments for assertions.
    struct CapturingGauge {
        /// Metric name.
        metric: String,
        /// Memory source label.
        source: String,
        /// Shared captured assignments.
        assignments: Arc<Mutex<Vec<GaugeAssignment>>>,
    }

    impl GaugeFn for CapturingGauge {
        fn increment(&self, _value: f64) {}

        fn decrement(&self, _value: f64) {}

        fn set(&self, value: f64) {
            self.assignments.lock().unwrap().push(GaugeAssignment {
                metric: self.metric.clone(),
                source: self.source.clone(),
                value,
            });
        }
    }

    /// Recorder that captures gauge assignments.
    #[derive(Default)]
    struct CapturingRecorder {
        /// Shared captured assignments.
        assignments: Arc<Mutex<Vec<GaugeAssignment>>>,
    }

    impl Recorder for CapturingRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            let source = key
                .labels()
                .find(|label| label.key() == MEMORY_SOURCE_LABEL)
                .map(|label| label.value().to_owned())
                .unwrap_or_default();
            Gauge::from_arc(Arc::new(CapturingGauge {
                metric: key.name().to_owned(),
                source,
                assignments: Arc::clone(&self.assignments),
            }))
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    #[test]
    fn memory_metrics_label_coherent_readings_and_clear_the_previous_source() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            emit_memory_snapshot_metrics(
                MemorySnapshot { used: 600, total: 1_000, source: MemorySnapshotSource::System },
                None,
            );
            emit_memory_snapshot_metrics(
                MemorySnapshot {
                    used: 300,
                    total: 500,
                    source: MemorySnapshotSource::ProcessCgroup,
                },
                Some(MemorySnapshotSource::System),
            );
            emit_memory_snapshot_metrics(
                MemorySnapshot {
                    used: 350,
                    total: 500,
                    source: MemorySnapshotSource::ProcessCgroup,
                },
                Some(MemorySnapshotSource::ProcessCgroup),
            );
        });

        assert_eq!(
            *recorder.assignments.lock().unwrap(),
            [
                GaugeAssignment::new(ETL_MEMORY_USED_BYTES, "system", 600.0),
                GaugeAssignment::new(ETL_MEMORY_TOTAL_BYTES, "system", 1_000.0),
                GaugeAssignment::new(ETL_MEMORY_USED_BYTES, "system", 0.0),
                GaugeAssignment::new(ETL_MEMORY_TOTAL_BYTES, "system", 0.0),
                GaugeAssignment::new(ETL_MEMORY_USED_BYTES, "process_cgroup", 300.0),
                GaugeAssignment::new(ETL_MEMORY_TOTAL_BYTES, "process_cgroup", 500.0),
                GaugeAssignment::new(ETL_MEMORY_USED_BYTES, "process_cgroup", 350.0),
                GaugeAssignment::new(ETL_MEMORY_TOTAL_BYTES, "process_cgroup", 500.0),
            ]
        );
    }

    #[test]
    fn live_memory_snapshot_is_nonzero_and_bounded() {
        let mut system = sysinfo::System::new();
        let current_pid = sysinfo::get_current_pid().unwrap();
        system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[current_pid]),
            false,
            sysinfo::ProcessRefreshKind::nothing(),
        );

        let snapshot = MemorySnapshot::refresh(&mut system, Some(current_pid), None).snapshot;

        assert!(snapshot.total > 0);
        assert!(snapshot.used <= snapshot.total);

        #[cfg(not(target_os = "linux"))]
        assert_eq!(snapshot.source, MemorySnapshotSource::System);
    }

    #[test]
    fn memory_used_percent_handles_regular_and_zero_totals() {
        assert_eq!(
            MemorySnapshot { used: 50, total: 100, source: MemorySnapshotSource::System }
                .used_percent(),
            0.5
        );
        assert_eq!(
            MemorySnapshot { used: 100, total: 0, source: MemorySnapshotSource::System }
                .used_percent(),
            1.0
        );
    }

    #[test]
    fn cgroup_snapshot_uses_the_full_memory_charge_instead_of_rss() {
        let cgroup =
            sysinfo::CGroupLimits { total_memory: 100, free_memory: 25, free_swap: 0, rss: 10 };

        let snapshot =
            MemorySnapshot::from_cgroup_limits(&cgroup, MemorySnapshotSource::ProcessCgroup);

        assert_eq!(snapshot.used, 75);
        assert_eq!(snapshot.total, 100);

        let inconsistent_cgroup = sysinfo::CGroupLimits { free_memory: 125, ..cgroup };
        assert_eq!(
            MemorySnapshot::from_cgroup_limits(
                &inconsistent_cgroup,
                MemorySnapshotSource::ProcessCgroup,
            )
            .used,
            0
        );
    }

    #[test]
    fn cgroup_snapshot_is_selected_only_for_a_tighter_capacity() {
        let system_snapshot =
            MemorySnapshot { used: 500, total: 1_000, source: MemorySnapshotSource::System };
        let constrained =
            sysinfo::CGroupLimits { total_memory: 600, free_memory: 200, free_swap: 0, rss: 100 };
        let unconstrained =
            sysinfo::CGroupLimits { total_memory: 1_000, free_memory: 900, ..constrained };

        assert_eq!(
            MemorySnapshot::constrained_cgroup_snapshot(
                &constrained,
                MemorySnapshotSource::ProcessCgroup,
                system_snapshot,
            ),
            Some(MemorySnapshot {
                used: 400,
                total: 600,
                source: MemorySnapshotSource::ProcessCgroup,
            })
        );
        assert_eq!(
            MemorySnapshot::constrained_cgroup_snapshot(
                &unconstrained,
                MemorySnapshotSource::ProcessCgroup,
                system_snapshot,
            ),
            None
        );
    }

    #[test]
    fn threshold_hysteresis_uses_inclusive_activation_and_exclusive_resume() {
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
            activate_threshold,
            activate_threshold,
            resume_threshold
        ));
        assert!(compute_next_backpressure_active(
            true,
            resume_threshold,
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

    #[test]
    fn capacity_snapshot_reflects_vertical_memory_limit_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(4_000, 6_000);

        let snapshot = memory_monitor.capacity_snapshot();

        assert_eq!(snapshot.revision, memory_monitor.snapshot_revision());
        assert_eq!(snapshot.total_memory_bytes, 6_000);
    }

    #[test]
    fn retained_snapshot_does_not_advance_the_batch_target_revision() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(790, 1_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 1.0, 1_000);
        let revision = memory_monitor.snapshot_revision();
        let snapshot = memory_monitor.current_snapshot();

        memory_monitor.publish_refresh(MemoryRefresh::retained(snapshot));

        assert_eq!(memory_monitor.snapshot_revision(), revision);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        // A genuinely new sample advances the revision even when its values and
        // resulting target are unchanged.
        memory_monitor.publish_refresh(MemoryRefresh::fresh(snapshot));

        assert_eq!(memory_monitor.snapshot_revision(), revision.wrapping_add(1));
        assert_eq!(governor.batch_size_target_bytes(), 1_000);
    }

    #[test]
    fn snapshot_revision_wraps_without_losing_the_update() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_snapshot_revision_for_test(u64::MAX);

        memory_monitor.set_memory_snapshot_for_test(4_000, 6_000);

        let snapshot = memory_monitor.capacity_snapshot();

        assert_eq!(snapshot.revision, 0);
        assert_eq!(snapshot.total_memory_bytes, 6_000);
    }

    #[tokio::test]
    async fn subscription_receives_backpressure_transitions() {
        let signal = MemoryMonitor::new_for_test();
        let mut sub = signal.subscribe().unwrap();

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
