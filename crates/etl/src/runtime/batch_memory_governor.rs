//! Dynamic decoded-batch memory governance.
//!
//! This module derives advisory batch-size targets from detected memory
//! capacity and the number of batches that may coexist. It is not a memory
//! allocator and does not grant or enforce byte reservations. Size estimates
//! can differ from actual allocations, and an indivisible decoded item can take
//! a batch past its target before ETL can flush it.
//!
//! Emergency source pausing remains in [`MemoryMonitor`] as the final safety
//! boundary.

use std::sync::{
    Arc, Mutex, PoisonError, TryLockError,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use metrics::gauge;
use tracing::debug;

use crate::{
    observability::{ETL_BATCH_ACTIVE_SLOTS, ETL_BATCH_SIZE_TARGET_BYTES},
    pipeline::PipelineId,
    runtime::MemoryMonitor,
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

/// Calculates the global decoded-batch target from detected memory capacity.
fn calculate_batch_memory_target(total_memory_bytes: u64, memory_budget_ratio: f64) -> u64 {
    ratio_of_bytes(total_memory_bytes, memory_budget_ratio)
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

/// Snapshot-derived values changed under one short update lock.
#[derive(Debug)]
struct BatchMemoryUpdateState {
    /// Potential batches that may exist concurrently.
    active_batch_slots: usize,
    /// Global advisory target frozen for the current memory snapshot.
    snapshot_batch_target_bytes: u64,
}

/// Mutable batch-governor state shared by batch producers.
#[derive(Debug)]
struct BatchMemoryState {
    /// Snapshot target and slot count protected by one update lock.
    update_state: Mutex<BatchMemoryUpdateState>,
    /// Memory snapshot revision used by the frozen global target.
    memory_snapshot_revision: AtomicU64,
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
            update_state: Mutex::new(BatchMemoryUpdateState {
                active_batch_slots: 0,
                snapshot_batch_target_bytes,
            }),
            memory_snapshot_revision: AtomicU64::new(memory_snapshot_revision),
            batch_size_target_bytes: AtomicUsize::new(batch_size_target_bytes),
            max_batch_bytes,
        }
    }

    /// Recomputes the per-slot target from the frozen global target.
    fn recalculate_batch_size_target(&self, update: &BatchMemoryUpdateState) {
        let batch_size_target_bytes = calculate_per_slot_batch_size_target(
            update.snapshot_batch_target_bytes,
            update.active_batch_slots,
            self.max_batch_bytes,
        );

        self.batch_size_target_bytes.store(batch_size_target_bytes, Ordering::Release);
        gauge!(ETL_BATCH_SIZE_TARGET_BYTES).set(batch_size_target_bytes as f64);
    }

    /// Adds active batch slots and updates their shared target.
    fn register_batch_slots(&self, slots: usize) {
        let mut update = self.update_state.lock().unwrap_or_else(PoisonError::into_inner);
        update.active_batch_slots = update
            .active_batch_slots
            .checked_add(slots)
            .expect("active batch slot count should fit in usize");
        gauge!(ETL_BATCH_ACTIVE_SLOTS).set(update.active_batch_slots as f64);
        self.recalculate_batch_size_target(&update);
    }

    /// Removes active batch slots and updates the remaining shared target.
    fn unregister_batch_slots(&self, slots: usize) {
        let mut update = self.update_state.lock().unwrap_or_else(PoisonError::into_inner);
        update.active_batch_slots = update
            .active_batch_slots
            .checked_sub(slots)
            .expect("registered batch slot count should not underflow");
        gauge!(ETL_BATCH_ACTIVE_SLOTS).set(update.active_batch_slots as f64);
        self.recalculate_batch_size_target(&update);
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
    /// Shared target and slot state.
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
            f64::from(memory_budget_ratio),
        );
        gauge!(ETL_BATCH_ACTIVE_SLOTS).set(0.0);

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
        // We do a first quick check on snapshot versions without locking, just to see if we don't
        // need to recompute the target.
        let memory_snapshot_revision = self.memory_monitor.snapshot_revision();
        if memory_snapshot_revision != self.state.memory_snapshot_revision.load(Ordering::Acquire) {
            self.try_refresh_batch_size_target();
        }

        // The target is advisory and independently atomic. Recalculation is
        // serialized separately, so observing the preceding value briefly is safe.
        self.state.batch_size_target_bytes.load(Ordering::Acquire)
    }

    /// Refreshes the shared target from the newest complete memory snapshot.
    fn try_refresh_batch_size_target(&self) {
        let mut update = match self.state.update_state.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            // Another caller is already refreshing the target or changing the
            // slot count. Since this is advisory, use the current target for
            // this item instead of making concurrent batch producers queue.
            Err(TryLockError::WouldBlock) => return,
        };

        let memory = self.memory_monitor.capacity_snapshot();

        // We check if the revision is the same while under the lock, since we can't modify the current
        // revision in any other paths than this one, so we have consistency while checking this.
        if self.state.memory_snapshot_revision.load(Ordering::Relaxed) == memory.revision {
            return;
        }

        // We calcculate the memory target for batches in bytes.
        let snapshot_batch_target_bytes =
            calculate_batch_memory_target(memory.total_memory_bytes, self.memory_budget_ratio);

        // We update the target and compute the individual batch size given the amount of batch slots
        // currently active in the system.
        update.snapshot_batch_target_bytes = snapshot_batch_target_bytes;
        self.state.recalculate_batch_size_target(&update);

        // Publish the refreshed target before readers accept this revision.
        self.state.memory_snapshot_revision.store(memory.revision, Ordering::Release);

        debug!(
            pipeline_id = self.pipeline_id,
            memory_snapshot_revision = memory.revision,
            total_memory_bytes = memory.total_memory_bytes,
            memory_budget_ratio = self.memory_budget_ratio,
            snapshot_batch_target_bytes,
            active_batch_slots = update.active_batch_slots,
            batch_size_target_bytes = self.state.batch_size_target_bytes.load(Ordering::Relaxed),
            "computed snapshot batch memory target"
        );
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
    fn batch_size_target_is_stable_across_used_memory_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(2_000, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);

        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        memory_monitor.set_memory_snapshot_for_test(9_000, 10_000);
        assert_eq!(governor.batch_size_target_bytes(), 2_000);
    }

    #[test]
    fn batch_size_target_tracks_vertical_memory_limit_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(4_000, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let _guard = governor.register_batch_slots(2);

        assert_eq!(governor.batch_size_target_bytes(), 1_000);

        memory_monitor.set_memory_snapshot_for_test(4_000, 6_000);
        assert_eq!(governor.batch_size_target_bytes(), 600);

        memory_monitor.set_memory_snapshot_for_test(4_000, 4_000);
        assert_eq!(governor.batch_size_target_bytes(), 400);

        memory_monitor.set_memory_snapshot_for_test(4_000, 12_000);
        assert_eq!(governor.batch_size_target_bytes(), 1_200);
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
    fn shared_target_refreshes_when_snapshot_revision_wraps() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        memory_monitor.set_snapshot_revision_for_test(u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);

        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        memory_monitor.set_memory_snapshot_for_test(4_000, 5_000);

        assert_eq!(memory_monitor.snapshot_revision(), 0);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);
    }

    #[test]
    fn target_read_does_not_wait_for_an_update_in_progress() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);

        let update_guard =
            governor.state.update_state.lock().unwrap_or_else(PoisonError::into_inner);
        memory_monitor.set_memory_snapshot_for_test(4_000, 5_000);

        // A competing producer keeps using the previous advisory target while
        // another caller owns the short update lock.
        assert_eq!(governor.batch_size_target_bytes(), 2_000);

        drop(update_guard);
        assert_eq!(governor.batch_size_target_bytes(), 1_000);
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
}
