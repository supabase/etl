//! Dynamic decoded-batch memory governance.
//!
//! This module derives advisory batch-size targets from sampled memory-domain
//! usage, tracked decoded bytes, and the number of batches that may coexist.
//! It is not a memory allocator and does not grant or enforce byte
//! reservations. Size estimates can differ from actual allocations, and an
//! indivisible decoded item can take a batch past its target before ETL can
//! flush it. When the governor first observes each memory-snapshot revision, it
//! captures the then-current tracked-byte estimate and freezes one global
//! target. Changes in active batch slots only repartition that target; the next
//! memory snapshot recalibrates it.
//!
//! The system-memory sample and tracked-byte read are not simultaneous. Bytes
//! may grow or drain between the monitor publishing a revision and the governor
//! first observing it, so the initial target for a revision remains an
//! approximation. Freezing prevents that sampling skew from being repeatedly
//! reinterpreted during slot churn or producing different targets for different
//! readers; it does not make the two measurements atomic.
//!
//! Emergency source pausing remains in [`MemoryMonitor`] as the final safety
//! boundary.
//!
//! An accounting handle follows decoded rows or events while ETL is
//! accumulating or dispatching them. ETL releases it when the destination async
//! result completes (including `Accepted`, `Durable`, or an error). That
//! completion is the accounting handoff point, not proof that physical memory
//! was freed. Any input or derived allocation retained by a destination remains
//! visible in the next selected system or cgroup sample and is then treated as
//! non-batch memory. Normal completion handlers drop accounting handles
//! explicitly at that boundary; [`Drop`] remains the cancellation and
//! error-path safety net.

use std::sync::{
    Arc, Mutex, PoisonError, TryLockError,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use metrics::gauge;
use tracing::debug;

use crate::{
    observability::ETL_BATCH_SIZE_TARGET_BYTES, pipeline::PipelineId, runtime::MemoryMonitor,
};

/// Converts a memory ratio into a byte count, saturating at [`u64::MAX`].
fn ratio_of_bytes(bytes: u64, ratio: f64) -> u64 {
    (bytes as f64 * ratio) as u64
}

/// Converts a [`u64`] byte count into [`usize`] without wrapping on narrower
/// platforms.
fn bytes_to_usize(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

/// Calculates the global decoded-batch target for one memory snapshot.
///
/// The calculation uses these quantities:
///
/// - `L`: selected system capacity or cgroup limit.
/// - `U`: used memory from the same coherent system or cgroup snapshot.
/// - `R`: decoded batch bytes tracked when the governor observes the snapshot.
/// - `T`: normal selected-memory target derived from the backpressure
///   thresholds.
/// - `q`: maximum fraction of `L` targeted for decoded batches.
///
/// First, `L * q` bounds the advisory target independently of other allocations
/// in the selected domain. Next, `U - R` estimates the non-batch baseline
/// because sampled usage already includes retained decoded batches. Subtracting
/// that baseline from `T` gives the total decoded-batch target that can coexist
/// below the normal memory target:
///
/// `target = min(L * q, max(T - max(U - R, 0), 0))`.
///
/// The target includes `R`; it is not additional headroom.
fn calculate_batch_memory_target(
    total_memory_bytes: u64,
    used_memory_bytes: u64,
    normal_memory_target_bytes: Option<u64>,
    tracked_batch_bytes: usize,
    memory_budget_ratio: f64,
) -> u64 {
    let configured_batch_target_bytes = ratio_of_bytes(total_memory_bytes, memory_budget_ratio);
    let tracked_batch_bytes = u64::try_from(tracked_batch_bytes).unwrap_or(u64::MAX);
    let estimated_non_batch_bytes = used_memory_bytes.saturating_sub(tracked_batch_bytes);

    normal_memory_target_bytes.map_or(configured_batch_target_bytes, |normal_target| {
        let pressure_adjusted_target = normal_target.saturating_sub(estimated_non_batch_bytes);

        configured_batch_target_bytes.min(pressure_adjusted_target)
    })
}

/// Divides one snapshot-scoped target across the current batch slots.
///
/// Each potential accumulating or in-flight batch receives an equal target,
/// then the configured preferred maximum caps one batch. A one-byte floor makes
/// the next indivisible item flush immediately when no calculated capacity
/// remains.
fn calculate_per_slot_batch_size_target(
    snapshot_batch_target_bytes: u64,
    active_batch_slots: usize,
    max_batch_bytes: usize,
) -> usize {
    let active_batch_slots = active_batch_slots.max(1);
    let per_slot_batch_size_bytes =
        (bytes_to_usize(snapshot_batch_target_bytes) / active_batch_slots).max(1);

    per_slot_batch_size_bytes.min(max_batch_bytes.max(1))
}

/// Mutable batch-governor state shared by producers and in-flight batches.
#[derive(Debug)]
struct BatchMemoryState {
    /// Serializes snapshot refreshes and slot-count changes.
    update_lock: Mutex<()>,
    /// Potential batches that may exist concurrently.
    active_batch_slots: AtomicUsize,
    /// Decoded bytes currently tracked by accumulating or in-flight batches.
    tracked_batch_bytes: AtomicUsize,
    /// Sticky overflow marker that keeps the governor fail-closed.
    saturated: AtomicBool,
    /// Memory snapshot revision used by the frozen global target.
    memory_snapshot_revision: AtomicU64,
    /// Global advisory target frozen for the current memory snapshot.
    snapshot_batch_target_bytes: AtomicU64,
    /// Current advisory target for one active batch slot.
    batch_size_target_bytes: AtomicUsize,
    /// Preferred ceiling for one batch.
    max_batch_bytes: usize,
}

impl BatchMemoryState {
    /// Creates shared state from the initial memory snapshot target.
    fn new(
        memory_snapshot_revision: u64,
        snapshot_batch_target_bytes: u64,
        max_batch_bytes: usize,
    ) -> Self {
        let max_batch_bytes = max_batch_bytes.max(1);
        let batch_size_target_bytes =
            calculate_per_slot_batch_size_target(snapshot_batch_target_bytes, 1, max_batch_bytes);

        Self {
            update_lock: Mutex::new(()),
            active_batch_slots: AtomicUsize::new(0),
            tracked_batch_bytes: AtomicUsize::new(0),
            saturated: AtomicBool::new(false),
            memory_snapshot_revision: AtomicU64::new(memory_snapshot_revision),
            snapshot_batch_target_bytes: AtomicU64::new(snapshot_batch_target_bytes),
            batch_size_target_bytes: AtomicUsize::new(batch_size_target_bytes),
            max_batch_bytes,
        }
    }

    /// Returns the nonzero slot divisor used for target calculation.
    fn batch_slot_divisor(&self) -> usize {
        self.active_batch_slots.load(Ordering::Relaxed).max(1)
    }

    /// Recomputes the per-slot target from the frozen global target.
    fn recalculate_batch_size_target(&self) {
        let batch_size_target_bytes = if self.saturated.load(Ordering::Relaxed) {
            1
        } else {
            calculate_per_slot_batch_size_target(
                self.snapshot_batch_target_bytes.load(Ordering::Relaxed),
                self.batch_slot_divisor(),
                self.max_batch_bytes,
            )
        };

        self.batch_size_target_bytes.store(batch_size_target_bytes, Ordering::Release);
        gauge!(ETL_BATCH_SIZE_TARGET_BYTES).set(batch_size_target_bytes as f64);
    }

    /// Adds active batch slots and updates their shared target.
    fn register_batch_slots(&self, slots: usize) {
        let _guard = self.update_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.active_batch_slots
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(slots))
            .expect("active batch slot count should fit in usize");
        self.recalculate_batch_size_target();
    }

    /// Removes active batch slots and updates the remaining shared target.
    fn unregister_batch_slots(&self, slots: usize) {
        let _guard = self.update_lock.lock().unwrap_or_else(PoisonError::into_inner);
        self.active_batch_slots
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(slots))
            .expect("registered batch slot count should not underflow");
        self.recalculate_batch_size_target();
    }

    /// Adds tracked bytes and returns the amount represented by one tracker.
    fn add_tracked_bytes(&self, bytes: usize) -> usize {
        if bytes == 0 {
            return 0;
        }

        let previous = self
            .tracked_batch_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            })
            .unwrap();
        let added_bytes = bytes.min(usize::MAX - previous);

        if added_bytes < bytes {
            // Once some live memory cannot be represented by the counter, later
            // releases cannot prove that all unrepresented bytes are gone. Keep the
            // one-byte target for the lifetime of this governor.
            self.saturated.store(true, Ordering::Relaxed);
            self.batch_size_target_bytes.store(1, Ordering::Release);
            gauge!(ETL_BATCH_SIZE_TARGET_BYTES).set(1.0);
        }

        added_bytes
    }

    /// Releases tracked bytes represented by one tracker without wrapping.
    fn release_tracked_bytes(&self, bytes: usize) {
        self.tracked_batch_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(bytes))
            .expect("tracked batch byte count should not underflow");
    }
}

/// Governs decoded batch memory across active batch slots.
#[derive(Debug, Clone)]
pub(crate) struct BatchMemoryGovernor {
    /// Pipeline identifier included in diagnostic logs.
    pipeline_id: PipelineId,
    /// Shared source of coherent memory snapshots.
    memory_monitor: MemoryMonitor,
    /// Maximum fraction of memory targeted for decoded batches.
    memory_budget_ratio: f64,
    /// Shared target, slot, and tracked-byte state.
    state: Arc<BatchMemoryState>,
}

impl BatchMemoryGovernor {
    /// Creates a new [`BatchMemoryGovernor`] instance.
    pub(crate) fn new(
        pipeline_id: PipelineId,
        memory_monitor: MemoryMonitor,
        memory_budget_ratio: f32,
        max_batch_bytes: usize,
    ) -> Self {
        let memory = memory_monitor.capacity_snapshot();
        let initial_batch_target_bytes = calculate_batch_memory_target(
            memory.total_memory_bytes,
            memory.used_memory_bytes,
            memory.normal_memory_target_bytes,
            0,
            f64::from(memory_budget_ratio),
        );

        Self {
            pipeline_id,
            memory_monitor,
            memory_budget_ratio: f64::from(memory_budget_ratio),
            state: Arc::new(BatchMemoryState::new(
                memory.revision,
                initial_batch_target_bytes,
                max_batch_bytes,
            )),
        }
    }

    /// Registers potential concurrent retained batches and returns a guard that
    /// unregisters them on drop.
    pub(crate) fn register_batch_slots(&self, slots: usize) -> BatchSlotsGuard {
        let slots = slots.max(1);
        self.state.register_batch_slots(slots);

        BatchSlotsGuard { state: Arc::clone(&self.state), slots }
    }

    /// Returns the current advisory batch-size target in bytes.
    ///
    /// The common path compares two atomic revisions and loads the atomic
    /// target. The first caller to observe a new memory-snapshot revision
    /// refreshes the shared target while holding the same lock used for slot
    /// changes. This keeps one frozen global target per revision and one
    /// consistent per-slot target for all callers.
    pub(crate) fn batch_size_target_bytes(&self) -> usize {
        let memory_snapshot_revision = self.memory_monitor.snapshot_revision();
        if memory_snapshot_revision != self.state.memory_snapshot_revision.load(Ordering::Acquire) {
            self.try_refresh_batch_size_target();
        }

        self.state.batch_size_target_bytes.load(Ordering::Acquire)
    }

    /// Creates an empty accounting handle for decoded batch memory.
    pub(crate) fn batch_memory_tracker(&self) -> BatchMemoryTracker {
        BatchMemoryTracker { state: Arc::clone(&self.state), tracked_bytes: 0 }
    }

    /// Refreshes the shared target from the newest complete memory snapshot.
    ///
    /// [`MemoryMonitor`] does not include tracked batch bytes in its snapshot.
    /// This method pairs that earlier system-memory sample with the
    /// tracked-byte estimate read when the revision is first observed. The
    /// pairing can contain sampling skew, but it is frozen until a newer
    /// revision arrives.
    fn try_refresh_batch_size_target(&self) {
        let _guard = match self.state.update_lock.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            // Another caller is already refreshing the target or changing the
            // slot count. Since this is advisory, use the current target for
            // this item instead of making concurrent batch producers queue.
            Err(TryLockError::WouldBlock) => return,
        };
        let memory = self.memory_monitor.capacity_snapshot();

        if self.state.memory_snapshot_revision.load(Ordering::Relaxed) == memory.revision {
            return;
        }

        let tracked_batch_bytes = self.state.tracked_batch_bytes.load(Ordering::Relaxed);
        let snapshot_batch_target_bytes = calculate_batch_memory_target(
            memory.total_memory_bytes,
            memory.used_memory_bytes,
            memory.normal_memory_target_bytes,
            tracked_batch_bytes,
            self.memory_budget_ratio,
        );

        self.state
            .snapshot_batch_target_bytes
            .store(snapshot_batch_target_bytes, Ordering::Relaxed);
        self.state.recalculate_batch_size_target();
        self.state.memory_snapshot_revision.store(memory.revision, Ordering::Release);

        debug!(
            pipeline_id = self.pipeline_id,
            memory_snapshot_revision = memory.revision,
            total_memory_bytes = memory.total_memory_bytes,
            used_memory_bytes = memory.used_memory_bytes,
            tracked_batch_bytes,
            memory_budget_ratio = self.memory_budget_ratio,
            normal_memory_target_bytes = ?memory.normal_memory_target_bytes,
            snapshot_batch_target_bytes,
            active_batch_slots = self.state.active_batch_slots.load(Ordering::Relaxed),
            batch_size_target_bytes = self.state.batch_size_target_bytes.load(Ordering::Relaxed),
            "computed snapshot batch memory target"
        );
    }
}

/// Tracks estimated decoded batch bytes until their owner releases them.
///
/// This is only a bookkeeping handle. It does not reserve allocator or
/// operating-system memory and cannot prevent other allocations. It identifies
/// the portion of sampled memory that the batch governor can reason about
/// directly, while the operating-system reading continues to cover driver,
/// allocator, destination, and other allocations.
///
/// Tracking ends when the destination result completes. If the destination
/// keeps memory after completion, the sampled usage still includes it, but the
/// governor conservatively treats it as non-batch memory from the next sample.
#[derive(Debug)]
pub(crate) struct BatchMemoryTracker {
    /// Shared governor state updated by this tracker.
    state: Arc<BatchMemoryState>,
    /// Bytes currently contributed to the shared total.
    tracked_bytes: usize,
}

impl BatchMemoryTracker {
    /// Returns the decoded size used to decide when this batch should flush.
    ///
    /// Once shared accounting saturates, every tracker reports the maximum
    /// size so all batch owners continue to flush immediately rather than
    /// relying on a potentially incomplete local contribution.
    pub(crate) fn size_hint_bytes(&self) -> usize {
        if self.state.saturated.load(Ordering::Relaxed) { usize::MAX } else { self.tracked_bytes }
    }

    /// Moves these accounted bytes into a new tracker and leaves this one
    /// empty while retaining its connection to the same governor.
    ///
    /// This transfer does not change the shared retained-byte total.
    pub(crate) fn take(&mut self) -> Self {
        let empty = Self { state: Arc::clone(&self.state), tracked_bytes: 0 };

        std::mem::replace(self, empty)
    }

    /// Adds decoded bytes retained by this tracker's batch.
    pub(crate) fn grow(&mut self, bytes: usize) {
        let added_bytes = self.state.add_tracked_bytes(bytes);

        // The shared counter includes this tracker, so any bytes that fit there
        // also fit in this tracker's local contribution.
        self.tracked_bytes += added_bytes;
    }

    /// Releases all decoded bytes currently held by this tracker.
    fn clear(&mut self) {
        let released_bytes = std::mem::take(&mut self.tracked_bytes);
        self.state.release_tracked_bytes(released_bytes);
    }
}

impl Drop for BatchMemoryTracker {
    fn drop(&mut self) {
        self.clear();
    }
}

/// RAII guard that decrements active batch slots on drop.
#[derive(Debug)]
pub(crate) struct BatchSlotsGuard {
    /// Shared governor state updated when this guard is dropped.
    state: Arc<BatchMemoryState>,
    /// Number of slots successfully registered by this guard.
    slots: usize,
}

impl Drop for BatchSlotsGuard {
    fn drop(&mut self) {
        self.state.unregister_batch_slots(self.slots);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MemoryMonitor;

    #[test]
    fn batch_size_target_divides_global_target_by_active_batch_slots() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _guard = governor.register_batch_slots(4);

        assert_eq!(governor.batch_size_target_bytes(), 500);
    }

    #[test]
    fn batch_size_target_uses_the_smaller_global_target() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(3_500);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _guard = governor.register_batch_slots(4);
        let mut tracker = governor.batch_memory_tracker();
        tracker.grow(1_000);

        // The configured 20% target is 2,000 bytes. At the 80% normal target,
        // subtracting 2,500 bytes of estimated non-batch memory leaves 5,500
        // bytes available for batches, so the configured target wins.
        assert_eq!(governor.batch_size_target_bytes(), 500);
    }

    #[test]
    fn batch_size_target_reacts_to_untracked_memory_and_recovers_after_release() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(2_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let mut tracker = governor.batch_memory_tracker();
        tracker.grow(1_000);

        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        memory_monitor.set_used_memory_bytes_for_test(7_000);
        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        drop(tracker);

        // Releasing tracked memory does not reinterpret the existing system
        // sample. The next snapshot observes the released state and reduces the
        // batch target because the same usage is now entirely non-batch memory.
        assert_eq!(governor.batch_size_target_bytes(), 2_000);
        memory_monitor.set_used_memory_bytes_for_test(7_000);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        memory_monitor.set_used_memory_bytes_for_test(5_000);
        assert_eq!(governor.batch_size_target_bytes(), 2_000);
    }

    #[test]
    fn batch_size_target_tracks_vertical_memory_limit_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(4_000, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let _guard = governor.register_batch_slots(2);
        let mut tracker = governor.batch_memory_tracker();
        tracker.grow(1_000);

        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        memory_monitor.set_memory_snapshot_for_test(4_000, 6_000);
        assert_eq!(governor.batch_size_target_bytes(), 600);

        memory_monitor.set_memory_snapshot_for_test(4_000, 4_000);
        assert_eq!(governor.batch_size_target_bytes(), 100);

        memory_monitor.set_memory_snapshot_for_test(4_000, 12_000);
        assert_eq!(governor.batch_size_target_bytes(), 1_200);
    }

    #[test]
    fn target_contracts_as_non_batch_memory_approaches_the_normal_target() {
        let tracked_batch_bytes = 1_000;
        let mut previous_target = usize::MAX;

        for used_memory_bytes in (1_000..=9_000).step_by(250) {
            let batch_target_bytes = calculate_batch_memory_target(
                10_000,
                used_memory_bytes,
                Some(8_000),
                tracked_batch_bytes,
                0.2,
            );
            let batch_size_target_bytes =
                calculate_per_slot_batch_size_target(batch_target_bytes, 2, 10_000);

            assert!(batch_size_target_bytes <= previous_target);
            assert!(batch_target_bytes <= 2_000);
            assert!(
                used_memory_bytes
                    .saturating_sub(u64::try_from(tracked_batch_bytes).unwrap())
                    .saturating_add(batch_target_bytes)
                    <= 8_000
            );
            previous_target = batch_size_target_bytes;
        }

        assert_eq!(previous_target, 1);
    }

    #[test]
    fn deterministic_memory_simulation_preserves_target_invariants() {
        const MIB: u64 = 1024 * 1024;

        let mut random_state = 0x7ad1_32e5_91c4_6bf0_u64;
        let mut total_memory_bytes = 512 * MIB;
        let mut non_batch_bytes = 256 * MIB;
        let mut tracked_batch_bytes = 32 * MIB;
        let mut observed_configured_target_bound = false;
        let mut observed_normal_memory_capacity_bound = false;
        let mut observed_empty_capacity = false;

        for step in 0..20_000 {
            // This fixed linear-congruential sequence makes the stress trajectory
            // reproducible while exercising growth, release, concurrency, and
            // vertical memory-limit changes.
            random_state = random_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);

            if step % 97 == 0 {
                total_memory_bytes = (64 + random_state % 8_129) * MIB;
            }

            let non_batch_delta = (random_state.rotate_left(17) % (64 * MIB)).saturating_add(1);
            if random_state & 1 == 0 {
                non_batch_bytes = non_batch_bytes.saturating_add(non_batch_delta);
            } else {
                non_batch_bytes = non_batch_bytes.saturating_sub(non_batch_delta);
            }

            let batch_delta = (random_state.rotate_left(41) % (32 * MIB)).saturating_add(1);
            if random_state & 2 == 0 {
                tracked_batch_bytes = tracked_batch_bytes.saturating_add(batch_delta);
            } else {
                tracked_batch_bytes = tracked_batch_bytes.saturating_sub(batch_delta);
            }

            let used_memory_bytes = non_batch_bytes.saturating_add(tracked_batch_bytes);
            let normal_memory_target_bytes = ratio_of_bytes(total_memory_bytes, 0.8);
            let active_batch_slots = usize::try_from(random_state % 33 + 1).unwrap();
            let max_batch_bytes = usize::try_from(random_state % (16 * MIB) + 1).unwrap();
            let tracked_batch_bytes = usize::try_from(tracked_batch_bytes).unwrap_or(usize::MAX);
            let batch_target_bytes = calculate_batch_memory_target(
                total_memory_bytes,
                used_memory_bytes,
                Some(normal_memory_target_bytes),
                tracked_batch_bytes,
                0.2,
            );
            let batch_size_target_bytes = calculate_per_slot_batch_size_target(
                batch_target_bytes,
                active_batch_slots,
                max_batch_bytes,
            );
            let configured_batch_target_bytes = ratio_of_bytes(total_memory_bytes, 0.2);
            let pressure_adjusted_target_bytes =
                normal_memory_target_bytes.saturating_sub(non_batch_bytes);

            assert!(batch_target_bytes <= configured_batch_target_bytes);
            assert!(batch_target_bytes <= pressure_adjusted_target_bytes);
            assert!(batch_size_target_bytes >= 1);
            assert!(batch_size_target_bytes <= max_batch_bytes.max(1));

            if batch_target_bytes == configured_batch_target_bytes {
                observed_configured_target_bound = true;
            }
            if batch_target_bytes == pressure_adjusted_target_bytes {
                observed_normal_memory_capacity_bound = true;
            }
            if batch_target_bytes == 0 {
                observed_empty_capacity = true;
                assert_eq!(batch_size_target_bytes, 1);
            } else {
                let expected_per_slot =
                    (bytes_to_usize(batch_target_bytes) / active_batch_slots).max(1);
                assert_eq!(batch_size_target_bytes, expected_per_slot.min(max_batch_bytes));
            }
        }

        assert!(observed_configured_target_bound);
        assert!(observed_normal_memory_capacity_bound);
        assert!(observed_empty_capacity);
    }

    #[test]
    fn shared_target_refreshes_when_batch_slot_count_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _first_slot = governor.register_batch_slots(1);
        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        let second_slot = governor.register_batch_slots(1);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        drop(second_slot);
        assert_eq!(governor.batch_size_target_bytes(), 2_000);
    }

    #[test]
    fn slot_changes_redivide_the_frozen_snapshot_target() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(700, 1_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 1.0, 1_000);
        let _first_slot = governor.register_batch_slots(1);

        // The normal target is 800 bytes, so the snapshot establishes a
        // 100-byte global batch target while no tracked memory exists.
        assert_eq!(governor.batch_size_target_bytes(), 100);

        let mut tracker = governor.batch_memory_tracker();
        tracker.grow(100);
        let _second_slot = governor.register_batch_slots(1);

        // The tracked bytes grew after the snapshot. Registering another slot
        // only divides the existing 100-byte target; it must not subtract the new
        // tracked bytes from the older 700-byte usage sample and invent a
        // 200-byte target.
        assert_eq!(governor.batch_size_target_bytes(), 50);

        // Every governor clone shares the same target for this memory revision,
        // even when it is cloned after tracked memory has grown.
        let second_governor = governor.clone();
        assert_eq!(second_governor.batch_size_target_bytes(), 50);

        // The next coherent sample includes the tracked bytes and preserves
        // the same non-batch baseline and global target.
        memory_monitor.set_memory_snapshot_for_test(800, 1_000);
        assert_eq!(governor.batch_size_target_bytes(), 50);
        assert_eq!(second_governor.batch_size_target_bytes(), 50);
    }

    #[test]
    fn shared_target_stays_fixed_until_the_next_memory_snapshot() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let _slot = governor.register_batch_slots(1);
        let mut tracker = governor.batch_memory_tracker();

        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        tracker.grow(1_000);

        // A batch cannot increase its own flush threshold while it grows inside
        // one sampling interval.
        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        // The next memory sample includes the retained allocation. It does not
        // change the estimated non-batch footprint, so the global target remains
        // fixed.
        memory_monitor.set_memory_snapshot_for_test(1_000, 10_000);
        assert_eq!(governor.batch_size_target_bytes(), 2_000);
    }

    #[test]
    fn shared_target_refreshes_when_snapshot_revision_wraps() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        memory_monitor.set_snapshot_revision_for_test(u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);

        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        memory_monitor.set_memory_snapshot_for_test(8_000, 10_000);

        assert_eq!(memory_monitor.snapshot_revision(), 0);
        assert_eq!(governor.batch_size_target_bytes(), 1);
    }

    #[test]
    fn target_read_does_not_wait_for_an_update_in_progress() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);

        let update_guard =
            governor.state.update_lock.lock().unwrap_or_else(PoisonError::into_inner);
        memory_monitor.set_memory_snapshot_for_test(8_000, 10_000);

        // A competing producer keeps using the previous advisory target while
        // another caller owns the short update lock.
        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        drop(update_guard);
        assert_eq!(governor.batch_size_target_bytes(), 1);
    }

    #[test]
    fn batch_size_target_reaches_minimum_without_headroom_or_tracked_batches() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(8_500);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);

        assert_eq!(governor.batch_size_target_bytes(), 1);
    }

    #[test]
    fn batch_size_target_is_capped_by_configured_max_bytes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10 * 1024 * 1024 * 1024);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 8 * 1024 * 1024);

        assert_eq!(governor.batch_size_target_bytes(), 8 * 1024 * 1024);
    }

    #[test]
    fn batch_size_target_uses_global_target_when_lower_than_configured_max_bytes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);

        assert_eq!(governor.batch_size_target_bytes(), 2_000);
    }

    #[test]
    fn batch_size_target_uses_total_memory_when_backpressure_is_disabled() {
        let memory_monitor = MemoryMonitor::new_for_test_with_backpressure(None);
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(9_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _guard = governor.register_batch_slots(4);

        assert_eq!(governor.batch_size_target_bytes(), 500);
    }

    #[test]
    fn tracker_clear_and_drop_release_retained_bytes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(7_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 1.0, 10_000);
        let mut tracker = governor.batch_memory_tracker();

        tracker.grow(600);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 600);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        tracker.clear();
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        tracker.grow(400);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 400);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        drop(tracker);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);
    }

    #[test]
    fn saturated_tracker_stays_fail_closed_without_wrapping() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 1.0, usize::MAX);
        let mut tracker = governor.batch_memory_tracker();

        tracker.grow(usize::MAX);
        tracker.grow(1);

        assert_eq!(tracker.size_hint_bytes(), usize::MAX);
        assert_eq!(governor.batch_size_target_bytes(), 1);

        drop(tracker);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 0);
        assert!(governor.state.saturated.load(Ordering::Relaxed));
        assert_eq!(governor.batch_size_target_bytes(), 1);
    }

    #[test]
    fn saturation_across_trackers_cannot_under_count_live_memory() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 1.0, usize::MAX);
        let mut first = governor.batch_memory_tracker();
        let mut second = governor.batch_memory_tracker();

        first.grow(usize::MAX - 10);
        second.grow(20);

        assert_eq!(first.size_hint_bytes(), usize::MAX);
        assert_eq!(second.size_hint_bytes(), usize::MAX);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), usize::MAX);
        assert_eq!(governor.batch_size_target_bytes(), 1);

        drop(first);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(governor.batch_size_target_bytes(), 1);

        drop(second);
        assert_eq!(governor.state.tracked_batch_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(governor.batch_size_target_bytes(), 1);
    }

    #[test]
    fn batch_slot_guard_releases_registered_slots() {
        let memory_monitor = MemoryMonitor::new_for_test();
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);

        let slots = governor.register_batch_slots(2);
        assert_eq!(governor.state.active_batch_slots.load(Ordering::Relaxed), 2);

        drop(slots);
        assert_eq!(governor.state.active_batch_slots.load(Ordering::Relaxed), 0);
    }
}
