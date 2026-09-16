//! Process-wide observations of active replication loops and table copies.
//!
//! Registrations own the lifetime of monitored work; handles record
//! observations without locking the registry. Consumers choose their inactivity
//! policy using [`snapshot`]. Slot acquisition and intentional catchup waits
//! are exempt.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, LazyLock, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Shared activity registrations for this process.
static ACTIVITY_REGISTRY: LazyLock<Mutex<ActivityRegistry>> =
    LazyLock::new(|| Mutex::new(ActivityRegistry::default()));

/// Work whose activity can be observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    /// WAL processing by the main apply worker or a table catchup loop.
    ///
    /// Completed loop iterations establish activity, including keepalive
    /// processing on an idle source. Pending writes do not make an otherwise
    /// progressing loop inactive; independent feedback does not count.
    WalApply {
        /// PostgreSQL's `wal_sender_timeout`, or the fallback when disabled or
        /// unavailable. Health allows at least this long for idle source work;
        /// the proactive keepalive interval is calculated separately.
        wal_sender_timeout: Duration,
    },
    /// An initial snapshot copy completing destination batches.
    ///
    /// Parallel partitions share the table observation. Source commit and the
    /// final destination durability barrier remain part of the copy window.
    InitialTableCopy,
}

/// Immutable observation of one active operation.
#[derive(Debug, Clone, Copy)]
pub struct ActivitySnapshot {
    /// The monitored operation.
    kind: ActivityKind,
    /// Last observation, including the start of a new observation window.
    last_observed_at: Instant,
}

impl ActivitySnapshot {
    /// Returns the kind of work being observed.
    pub fn kind(&self) -> ActivityKind {
        self.kind
    }

    /// Returns when work was last observed or its observation window began.
    ///
    /// Registration and resumption start a fresh window. Completed apply-loop
    /// iterations and copy work refresh it while the operation runs.
    /// This is an activity observation, not a last successful commit timestamp.
    pub fn last_observed_at(&self) -> Instant {
        self.last_observed_at
    }

    /// Returns the time since this operation was last observed.
    pub fn inactive_for(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.last_observed_at)
    }
}

/// Per-operation observation shared without taking the registry lock.
#[derive(Debug)]
struct ActivityState {
    /// The monitored operation.
    kind: ActivityKind,
    /// Fixed clock origin for encoding an [`Instant`] as an atomic integer.
    origin: Instant,
    /// Microseconds since the origin plus one; zero suspends observation.
    last_observed: AtomicU64,
}

impl ActivityState {
    /// Copies the current observation, excluding intentional waits.
    fn snapshot(&self) -> Option<ActivitySnapshot> {
        let micros = self.last_observed.load(Ordering::Relaxed).checked_sub(1)?;

        Some(ActivitySnapshot {
            kind: self.kind,
            last_observed_at: self.origin + Duration::from_micros(micros),
        })
    }
}

/// Lightweight writer for work owned by an [`ActivityRegistration`].
#[derive(Debug, Clone)]
pub(crate) struct ActivityHandle {
    /// Shared observation independent of registry membership.
    state: Arc<ActivityState>,
}

impl ActivityHandle {
    /// Records activity at the current time without locking.
    ///
    /// Concurrent pings may publish out of order, so the latest observation is
    /// preserved. No replication state or memory ownership depends on it.
    pub(crate) fn ping(&self) {
        let now = Instant::now();
        let micros = u64::try_from(now.saturating_duration_since(self.state.origin).as_micros())
            .unwrap_or(u64::MAX);

        self.state.last_observed.fetch_max(micros.saturating_add(1), Ordering::Relaxed);
    }

    /// Exempts an intentional catchup wait until the returned guard is dropped.
    pub(crate) fn suspend(&self) -> ActivitySuspension<'_> {
        self.state.last_observed.store(0, Ordering::Relaxed);

        ActivitySuspension { handle: self }
    }
}

/// Restores a fresh observation window after an intentional catchup wait.
pub(crate) struct ActivitySuspension<'a> {
    /// Activity to resume when the wait ends.
    handle: &'a ActivityHandle,
}

impl Drop for ActivitySuspension<'_> {
    fn drop(&mut self) {
        self.handle.ping();
    }
}

/// Owns uniquely identified observations without retaining completed workers.
#[derive(Debug, Default)]
struct ActivityRegistry {
    /// Next process-unique registration identifier.
    next_id: u64,
    /// Live registrations owned by operations.
    entries: BTreeMap<u64, Arc<ActivityState>>,
}

impl ActivityRegistry {
    /// Registers work with a fresh observation window.
    fn register(&mut self, kind: ActivityKind, now: Instant) -> (u64, ActivityHandle) {
        let id = self.next_id;
        self.next_id =
            self.next_id.checked_add(1).expect("activity registration IDs must not exhaust u64");

        let state = Arc::new(ActivityState { kind, origin: now, last_observed: AtomicU64::new(1) });
        self.entries.insert(id, Arc::clone(&state));

        (id, ActivityHandle { state })
    }

    /// Copies observations while holding the registry lock briefly.
    fn snapshot(&self) -> Vec<ActivitySnapshot> {
        self.entries.values().filter_map(|entry| entry.snapshot()).collect()
    }
}

/// Owns a registration until work completes, is cancelled, or unwinds.
#[derive(Debug)]
pub(crate) struct ActivityRegistration {
    /// Registry entry removed when this registration leaves scope.
    id: u64,
    /// Handle shared with the operation and its child tasks.
    handle: ActivityHandle,
}

impl ActivityRegistration {
    /// Registers an operation after any unbounded slot acquisition completes.
    ///
    /// # Panics
    ///
    /// Panics if this process exhausts the `u64` registration ID space.
    pub(crate) fn register(kind: ActivityKind) -> Self {
        let (id, handle) = ACTIVITY_REGISTRY
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .register(kind, Instant::now());

        Self { id, handle }
    }

    /// Returns a handle that cannot extend the registration's lifetime.
    pub(crate) fn handle(&self) -> ActivityHandle {
        self.handle.clone()
    }
}

impl Drop for ActivityRegistration {
    fn drop(&mut self) {
        ACTIVITY_REGISTRY.lock().unwrap_or_else(PoisonError::into_inner).entries.remove(&self.id);
    }
}

/// Returns the monitored activities across all pipelines in this process.
///
/// Completed and intentionally suspended activities are omitted. An empty
/// snapshot is possible during startup, slot acquisition, and worker retries.
pub fn snapshot() -> Vec<ActivitySnapshot> {
    ACTIVITY_REGISTRY.lock().unwrap_or_else(PoisonError::into_inner).snapshot()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::activity::{ActivityKind, ActivityRegistration, ActivityRegistry, snapshot};

    #[test]
    fn observations_start_at_registration_and_track_each_worker_independently() {
        let now = Instant::now();
        let first_started_at = now - Duration::from_secs(10);
        let second_started_at = now - Duration::from_secs(5);
        let mut registry = ActivityRegistry::default();
        let (_, handle) = registry.register(ActivityKind::InitialTableCopy, first_started_at);
        registry.register(ActivityKind::InitialTableCopy, second_started_at);
        let entries = registry.snapshot();
        assert_eq!(entries[0].last_observed_at(), first_started_at);
        assert_eq!(entries[0].inactive_for(now), Duration::from_secs(10));
        assert_eq!(entries[1].inactive_for(now), Duration::from_secs(5));

        let before_ping = Instant::now();
        handle.ping();
        let after_ping = Instant::now();
        let entries = registry.snapshot();
        // Atomic timestamps truncate to microseconds when encoding the instant.
        assert!(entries[0].last_observed_at() >= before_ping - Duration::from_micros(1));
        assert!(entries[0].last_observed_at() <= after_ping);
        assert_eq!(entries[1].last_observed_at(), second_started_at);
    }

    #[test]
    fn catchup_wait_is_exempt_and_resumes_with_a_fresh_window() {
        let now = Instant::now();
        let mut registry = ActivityRegistry::default();
        let (_, handle) =
            registry.register(ActivityKind::InitialTableCopy, now - Duration::from_secs(600));
        let suspension = handle.suspend();
        assert!(registry.snapshot().is_empty());
        drop(suspension);
        let entry = registry.snapshot()[0];
        assert!(entry.inactive_for(now) < Duration::from_micros(1));
        assert!(entry.last_observed_at() <= Instant::now());
    }

    #[test]
    fn long_main_loop_wait_does_not_hide_catchup_inactivity() {
        let now = Instant::now();
        let kind = ActivityKind::WalApply { wal_sender_timeout: Duration::from_secs(60) };
        let mut registry = ActivityRegistry::default();
        let (_, main) = registry.register(kind, now);
        registry.register(kind, now - Duration::from_secs(600));
        let _waiting = main.suspend();
        let entries = registry.snapshot();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].inactive_for(now), Duration::from_secs(600));
    }

    #[test]
    fn handles_do_not_keep_completed_work_registered() {
        let registration = ActivityRegistration::register(ActivityKind::InitialTableCopy);
        let handle = registration.handle();
        drop(registration);
        handle.ping();
        assert!(snapshot().is_empty());
    }

    #[test]
    fn unwinding_removes_registration() {
        let result = std::panic::catch_unwind(|| {
            let _registration = ActivityRegistration::register(ActivityKind::InitialTableCopy);
            panic!("test unwind");
        });
        assert!(result.is_err());
        assert!(snapshot().is_empty());
    }

    #[tokio::test]
    async fn cancellation_removes_registration() {
        let registration = ActivityRegistration::register(ActivityKind::InitialTableCopy);
        let task = tokio::spawn(async move {
            let _registration = registration;
            std::future::pending::<()>().await;
        });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(snapshot().is_empty());
    }
}
