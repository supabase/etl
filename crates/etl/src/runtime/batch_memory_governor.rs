//! Dynamic decoded-batch memory governance.
//!
//! This module proactively sizes batches from one global ownership quota and
//! observed non-batch memory. Emergency source pausing remains in
//! [`MemoryMonitor`] as a separate final safety boundary.
//!
//! A reservation follows decoded rows or events while ETL is accumulating or
//! dispatching them. ETL releases it when the destination async result
//! completes (including `Accepted`, `Durable`, or an error). That completion is
//! the accounting handoff point, not proof that physical memory was freed. Any
//! input or derived allocation retained by a destination remains visible in the
//! next whole-process or cgroup sample and is then treated as non-batch memory.
//! Normal completion handlers drop reservations explicitly at that boundary;
//! [`Drop`] remains the cancellation and error-path safety net.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use metrics::gauge;
use tracing::debug;

use crate::{
    observability::ETL_BATCH_SIZE_LIMIT_BYTES, pipeline::PipelineId, runtime::MemoryMonitor,
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

/// Intermediate values and final result of one batch-limit calculation.
#[derive(Debug, Clone, Copy)]
struct BatchMemoryLimitCalculation {
    /// Configured maximum global ownership for decoded batches.
    total_batch_quota_bytes: u64,
    /// Sampled memory not attributed to retained decoded batches.
    estimated_non_batch_bytes: u64,
    /// Batch capacity that fits below the normal memory target.
    normal_memory_batch_capacity_bytes: Option<u64>,
    /// Global pool after applying both independent capacity bounds.
    effective_batch_pool_bytes: u64,
    /// Equal share of the global pool before the configured batch ceiling.
    per_slot_batch_size_bytes: usize,
    /// Final limit used to decide when one batch flushes.
    batch_size_limit_bytes: usize,
}

/// Calculates a per-batch limit from independent ownership and process-memory
/// bounds.
///
/// The calculation uses these quantities:
///
/// - `L`: selected system capacity or cgroup limit.
/// - `U`: used memory from the same coherent system or cgroup snapshot.
/// - `R`: decoded batch bytes currently retained by ETL.
/// - `T`: normal process-memory target derived from the backpressure
///   thresholds.
/// - `q`: maximum fraction of `L` assigned to decoded batches.
/// - `S`: potential decoded batches that may coexist.
/// - `M`: configured ceiling for one batch.
///
/// First, `L * q` bounds decoded batches independently of all other process
/// allocations. Next, `U - R` estimates the current non-batch baseline because
/// sampled usage already includes the retained decoded batches. Subtracting
/// that baseline from `T` gives the total amount of decoded batch memory that
/// can coexist below the normal target:
///
/// `pool = min(L * q, max(T - max(U - R, 0), 0))`.
///
/// `pool` includes `R`; it is not additional headroom. Dividing it by `S`
/// reserves an equal share for every potential accumulating or in-flight batch,
/// preventing concurrent owners from each targeting the whole pool. Finally,
/// `M` caps a single batch. A one-byte floor makes the next indivisible row
/// flush immediately when no calculated capacity remains.
fn calculate_batch_memory_limit(
    total_memory_bytes: u64,
    used_memory_bytes: u64,
    normal_memory_target_bytes: Option<u64>,
    retained_batch_bytes: usize,
    memory_budget_ratio: f64,
    active_batch_slots: usize,
    max_batch_bytes: usize,
) -> BatchMemoryLimitCalculation {
    // Ownership bound: decoded batches never target more than the configured
    // fraction of the selected memory capacity.
    let total_batch_quota_bytes = ratio_of_bytes(total_memory_bytes, memory_budget_ratio);

    // Process-pressure bound: remove already-accounted decoded batches from the
    // sampled usage to estimate everything else, then determine how much total
    // batch memory can coexist with that baseline below the normal target.
    let retained_batch_bytes = u64::try_from(retained_batch_bytes).unwrap_or(u64::MAX);
    let estimated_non_batch_bytes = used_memory_bytes.saturating_sub(retained_batch_bytes);
    let normal_memory_batch_capacity_bytes =
        normal_memory_target_bytes.map(|target| target.saturating_sub(estimated_non_batch_bytes));

    // Global pool: honor the tighter independent bound. Without memory
    // backpressure, only the configured ownership quota applies.
    let effective_batch_pool_bytes = normal_memory_batch_capacity_bytes
        .map_or(total_batch_quota_bytes, |capacity| total_batch_quota_bytes.min(capacity));

    // Per-owner limit: reserve an equal share for every batch that may coexist,
    // then apply the configured single-batch ceiling. The one-byte floors keep
    // empty capacity fail-closed without introducing zero-sized thresholds.
    let active_batch_slots = active_batch_slots.max(1);
    let per_slot_batch_size_bytes =
        (bytes_to_usize(effective_batch_pool_bytes) / active_batch_slots).max(1);
    let batch_size_limit_bytes = per_slot_batch_size_bytes.min(max_batch_bytes.max(1));

    BatchMemoryLimitCalculation {
        total_batch_quota_bytes,
        estimated_non_batch_bytes,
        normal_memory_batch_capacity_bytes,
        effective_batch_pool_bytes,
        per_slot_batch_size_bytes,
        batch_size_limit_bytes,
    }
}

/// Shared accounting for decoded bytes retained by live batches.
#[derive(Debug)]
struct RetainedBatchMemory {
    /// Accounted decoded bytes.
    bytes: AtomicUsize,
    /// Sticky overflow marker that keeps the governor fail-closed.
    saturated: AtomicBool,
}

impl RetainedBatchMemory {
    /// Creates empty retained-batch accounting.
    const fn new() -> Self {
        Self { bytes: AtomicUsize::new(0), saturated: AtomicBool::new(false) }
    }

    /// Returns the currently accounted bytes.
    fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Returns whether accounting has ever saturated.
    fn is_saturated(&self) -> bool {
        self.saturated.load(Ordering::Relaxed)
    }

    /// Adds bytes and returns the amount represented by this reservation.
    fn add(&self, bytes: usize) -> usize {
        if bytes == 0 {
            return 0;
        }

        let previous = self
            .bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(bytes))
            })
            .unwrap();
        let added_bytes = bytes.min(usize::MAX - previous);

        if added_bytes < bytes {
            // Once some live memory cannot be represented by the counter, later
            // releases cannot prove that all unrepresented bytes are gone. Keep the
            // one-byte limit for the lifetime of this governor.
            self.saturated.store(true, Ordering::Relaxed);
        }

        added_bytes
    }

    /// Releases bytes represented by one reservation without wrapping.
    fn release(&self, bytes: usize) {
        let _ = self
            .bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(bytes));
    }
}

/// Governs decoded batch memory across active retained-batch slots.
#[derive(Debug, Clone)]
pub(crate) struct BatchMemoryGovernor {
    /// Pipeline identifier included in diagnostic logs.
    pipeline_id: PipelineId,
    /// Shared source of coherent memory snapshots.
    memory_monitor: MemoryMonitor,
    /// Maximum fraction of memory assigned to decoded batches.
    memory_budget_ratio: f64,
    /// Absolute ceiling for one batch.
    max_batch_bytes: usize,
    /// Potential batches that may exist concurrently.
    active_batch_slots: Arc<AtomicUsize>,
    /// Decoded bytes owned by accumulating and in-flight batches.
    retained_batch_memory: Arc<RetainedBatchMemory>,
}

impl BatchMemoryGovernor {
    /// Creates a new [`BatchMemoryGovernor`] instance.
    pub(crate) fn new(
        pipeline_id: PipelineId,
        memory_monitor: MemoryMonitor,
        memory_budget_ratio: f32,
        max_batch_bytes: usize,
    ) -> Self {
        Self {
            pipeline_id,
            memory_monitor,
            memory_budget_ratio: f64::from(memory_budget_ratio),
            max_batch_bytes: max_batch_bytes.max(1),
            active_batch_slots: Arc::new(AtomicUsize::new(0)),
            retained_batch_memory: Arc::new(RetainedBatchMemory::new()),
        }
    }

    /// Registers potential concurrent retained batches and returns a guard that
    /// unregisters them on drop.
    pub(crate) fn register_batch_slots(&self, slots: usize) -> BatchSlotsGuard {
        let slots = slots.max(1);
        self.active_batch_slots
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(slots))
            .expect("active batch slot count should fit in usize");

        BatchSlotsGuard { active_batch_slots: Arc::clone(&self.active_batch_slots), slots }
    }

    /// Returns a cached limit reader that refreshes with memory snapshots and
    /// active batch slots.
    pub(crate) fn cached_limit(&self) -> CachedBatchMemoryLimit {
        CachedBatchMemoryLimit::new(self.clone())
    }

    /// Creates an empty reservation for decoded batch memory.
    pub(crate) fn reservation(&self) -> BatchMemoryReservation {
        BatchMemoryReservation {
            retained_batch_memory: Arc::clone(&self.retained_batch_memory),
            bytes: 0,
        }
    }

    /// Returns the revision of the latest complete memory snapshot.
    fn memory_snapshot_revision(&self) -> u64 {
        self.memory_monitor.snapshot_revision()
    }

    /// Returns the number of potential concurrently retained batches.
    fn active_batch_slots(&self) -> usize {
        self.active_batch_slots.load(Ordering::Relaxed).max(1)
    }

    /// Returns the current per-slot batch memory limit in bytes.
    ///
    /// The configured ratio first establishes a maximum global decoded-batch
    /// quota. When memory backpressure is enabled, that quota is reduced to the
    /// batch capacity that can coexist with estimated non-batch memory below
    /// the normal memory target:
    ///
    /// `min(total_memory * batch_ratio, normal_target - (used - retained))`.
    ///
    /// Subtracting retained bytes from sampled usage estimates the memory owned
    /// by everything else in the workload. Retained bytes remain part of the
    /// resulting global batch pool; they are not treated as fresh headroom.
    /// Dividing that pool across active batch slots prevents concurrent
    /// producers and pipelined destination writes from each assuming they own
    /// all available memory. The configured maximum remains a per-batch
    /// ceiling.
    ///
    /// When the calculated pool is empty, the limit is one byte so an already
    /// decoded, indivisible row is flushed immediately. The emergency memory
    /// signal is responsible for pausing source polling entirely.
    pub(crate) fn batch_size_limit_bytes(&self) -> usize {
        let memory = self.memory_monitor.capacity_snapshot();
        let active_batch_slots = self.active_batch_slots();
        let retained_batch_bytes = self.retained_batch_memory.bytes();
        if self.retained_batch_memory.is_saturated() {
            gauge!(ETL_BATCH_SIZE_LIMIT_BYTES).set(1.0);

            return 1;
        }

        let calculation = calculate_batch_memory_limit(
            memory.total_memory_bytes,
            memory.used_memory_bytes,
            memory.normal_memory_target_bytes,
            retained_batch_bytes,
            self.memory_budget_ratio,
            active_batch_slots,
            self.max_batch_bytes,
        );

        debug!(
            pipeline_id = self.pipeline_id,
            total_memory_bytes = memory.total_memory_bytes,
            used_memory_bytes = memory.used_memory_bytes,
            retained_batch_bytes,
            memory_budget_ratio = self.memory_budget_ratio,
            active_batch_slots,
            normal_memory_target_bytes = ?memory.normal_memory_target_bytes,
            total_batch_quota_bytes = calculation.total_batch_quota_bytes,
            estimated_non_batch_bytes = calculation.estimated_non_batch_bytes,
            normal_memory_batch_capacity_bytes = ?calculation.normal_memory_batch_capacity_bytes,
            effective_batch_pool_bytes = calculation.effective_batch_pool_bytes,
            per_slot_batch_size_bytes = calculation.per_slot_batch_size_bytes,
            max_batch_bytes = self.max_batch_bytes,
            batch_size_limit_bytes = calculation.batch_size_limit_bytes,
            "computed batch memory limit"
        );

        gauge!(ETL_BATCH_SIZE_LIMIT_BYTES).set(calculation.batch_size_limit_bytes as f64);

        calculation.batch_size_limit_bytes
    }
}

/// Cached view over [`BatchMemoryGovernor`] batch size calculations.
///
/// The limit is recomputed only when the monitor publishes a new coherent
/// memory snapshot or the number of active batch slots changes. Keeping the
/// limit fixed between memory samples prevents a growing reservation from
/// continuously moving its own flush threshold forward.
#[derive(Debug, Clone)]
pub(crate) struct CachedBatchMemoryLimit {
    /// Shared governor used when the cached inputs change.
    governor: BatchMemoryGovernor,
    /// Memory snapshot represented by the cached limit.
    last_memory_snapshot_revision: u64,
    /// Batch-slot count represented by the cached limit.
    last_active_batch_slots: usize,
    /// Last complete limit calculation.
    last_known_batch_size_bytes: usize,
}

impl CachedBatchMemoryLimit {
    /// Creates a new cached limit initialized from the current governor
    /// value.
    pub(crate) fn new(governor: BatchMemoryGovernor) -> Self {
        let last_memory_snapshot_revision = governor.memory_snapshot_revision();
        let last_active_batch_slots = governor.active_batch_slots();

        Self {
            last_known_batch_size_bytes: governor.batch_size_limit_bytes(),
            governor,
            last_memory_snapshot_revision,
            last_active_batch_slots,
        }
    }

    /// Returns the current batch memory limit in bytes.
    pub(crate) fn current_batch_size_limit_bytes(&mut self) -> usize {
        let memory_snapshot_revision = self.governor.memory_snapshot_revision();
        let active_batch_slots = self.governor.active_batch_slots();
        let should_refresh = memory_snapshot_revision != self.last_memory_snapshot_revision
            || active_batch_slots != self.last_active_batch_slots;

        if should_refresh {
            self.last_known_batch_size_bytes = self.governor.batch_size_limit_bytes();
            self.last_memory_snapshot_revision = memory_snapshot_revision;
            self.last_active_batch_slots = active_batch_slots;
        }

        self.last_known_batch_size_bytes
    }

    /// Creates an empty reservation linked to this limit's governor.
    pub(crate) fn reservation(&self) -> BatchMemoryReservation {
        self.governor.reservation()
    }
}

/// Tracks decoded batch bytes until their owner releases them.
///
/// Reservations are intentionally independent from sampled system or cgroup
/// usage. They identify the portion of sampled memory that the batch governor
/// can reason about directly, while the operating-system reading continues to
/// cover driver, allocator, destination, and other allocations.
///
/// A reservation ends at destination acknowledgement. If the destination keeps
/// memory after acknowledging, the sampled usage still includes it, but the
/// governor conservatively treats it as non-batch memory from the next sample.
#[derive(Debug)]
pub(crate) struct BatchMemoryReservation {
    /// Shared total updated by this reservation.
    retained_batch_memory: Arc<RetainedBatchMemory>,
    /// Bytes currently contributed to the shared total.
    bytes: usize,
}

impl BatchMemoryReservation {
    /// Returns the decoded size used to decide when this batch should flush.
    ///
    /// Once shared accounting saturates, every reservation reports the maximum
    /// size so all batch owners continue to flush immediately rather than
    /// relying on a potentially incomplete local contribution.
    pub(crate) fn size_hint_bytes(&self) -> usize {
        if self.retained_batch_memory.is_saturated() { usize::MAX } else { self.bytes }
    }

    /// Moves these accounted bytes into a new reservation and leaves this one
    /// empty while retaining its connection to the same governor.
    ///
    /// This transfer does not change the shared retained-byte total.
    pub(crate) fn take(&mut self) -> Self {
        let empty =
            Self { retained_batch_memory: Arc::clone(&self.retained_batch_memory), bytes: 0 };

        std::mem::replace(self, empty)
    }

    /// Adds decoded bytes retained by the reservation's batch.
    pub(crate) fn grow(&mut self, bytes: usize) {
        let added_bytes = self.retained_batch_memory.add(bytes);

        // The shared counter includes this reservation, so any bytes that fit
        // there also fit in this reservation's local contribution.
        self.bytes += added_bytes;
    }

    /// Releases all decoded bytes currently held by this reservation.
    fn clear(&mut self) {
        let released_bytes = std::mem::take(&mut self.bytes);
        self.retained_batch_memory.release(released_bytes);
    }
}

impl Drop for BatchMemoryReservation {
    fn drop(&mut self) {
        self.clear();
    }
}

/// RAII guard that decrements active batch slots on drop.
#[derive(Debug)]
pub(crate) struct BatchSlotsGuard {
    /// Shared slot total decremented when this guard is dropped.
    active_batch_slots: Arc<AtomicUsize>,
    /// Number of slots successfully registered by this guard.
    slots: usize,
}

impl Drop for BatchSlotsGuard {
    fn drop(&mut self) {
        let _ =
            self.active_batch_slots.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(self.slots)
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MemoryMonitor;

    #[test]
    fn batch_size_limit_divides_global_quota_by_active_batch_slots() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _guard = governor.register_batch_slots(4);

        assert_eq!(governor.batch_size_limit_bytes(), 500);
    }

    #[test]
    fn batch_size_limit_uses_the_smaller_of_batch_quota_and_normal_memory_capacity() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(3_500);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _guard = governor.register_batch_slots(4);
        let mut reservation = governor.reservation();
        reservation.grow(1_000);

        // The 20% batch quota is 2,000 bytes. At the 80% normal target,
        // subtracting 2,500 bytes of estimated non-batch memory leaves 5,500
        // bytes of normal-memory capacity for batches, so the smaller quota wins.
        assert_eq!(governor.batch_size_limit_bytes(), 500);
    }

    #[test]
    fn batch_size_limit_reacts_to_untracked_memory_and_recovers_after_release() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(2_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let mut reservation = governor.reservation();
        reservation.grow(1_000);

        assert_eq!(governor.batch_size_limit_bytes(), 2_000);

        memory_monitor.set_used_memory_bytes_for_test(7_000);
        assert_eq!(governor.batch_size_limit_bytes(), 2_000);

        drop(reservation);
        assert_eq!(governor.batch_size_limit_bytes(), 1_000);

        memory_monitor.set_used_memory_bytes_for_test(5_000);
        assert_eq!(governor.batch_size_limit_bytes(), 2_000);
    }

    #[test]
    fn batch_size_limit_tracks_vertical_memory_limit_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(4_000, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let _guard = governor.register_batch_slots(2);
        let mut reservation = governor.reservation();
        reservation.grow(1_000);

        assert_eq!(governor.batch_size_limit_bytes(), 1_000);

        memory_monitor.set_memory_snapshot_for_test(4_000, 6_000);
        assert_eq!(governor.batch_size_limit_bytes(), 600);

        memory_monitor.set_memory_snapshot_for_test(4_000, 4_000);
        assert_eq!(governor.batch_size_limit_bytes(), 100);

        memory_monitor.set_memory_snapshot_for_test(4_000, 12_000);
        assert_eq!(governor.batch_size_limit_bytes(), 1_200);
    }

    #[test]
    fn limit_contracts_as_non_batch_memory_approaches_the_normal_target() {
        let retained_batch_bytes = 1_000;
        let mut previous_limit = usize::MAX;

        for used_memory_bytes in (1_000..=9_000).step_by(250) {
            let calculation = calculate_batch_memory_limit(
                10_000,
                used_memory_bytes,
                Some(8_000),
                retained_batch_bytes,
                0.2,
                2,
                10_000,
            );

            assert!(calculation.batch_size_limit_bytes <= previous_limit);
            assert!(calculation.effective_batch_pool_bytes <= 2_000);
            assert!(
                calculation
                    .estimated_non_batch_bytes
                    .saturating_add(calculation.effective_batch_pool_bytes)
                    <= 8_000
            );
            previous_limit = calculation.batch_size_limit_bytes;
        }

        assert_eq!(previous_limit, 1);
    }

    #[test]
    fn deterministic_memory_simulation_preserves_pool_invariants() {
        const MIB: u64 = 1024 * 1024;

        let mut random_state = 0x7ad1_32e5_91c4_6bf0_u64;
        let mut total_memory_bytes = 512 * MIB;
        let mut non_batch_bytes = 256 * MIB;
        let mut retained_batch_bytes = 32 * MIB;
        let mut observed_batch_quota_bound = false;
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
                retained_batch_bytes = retained_batch_bytes.saturating_add(batch_delta);
            } else {
                retained_batch_bytes = retained_batch_bytes.saturating_sub(batch_delta);
            }

            let used_memory_bytes = non_batch_bytes.saturating_add(retained_batch_bytes);
            let normal_memory_target_bytes = ratio_of_bytes(total_memory_bytes, 0.8);
            let active_batch_slots = usize::try_from(random_state % 33 + 1).unwrap();
            let max_batch_bytes = usize::try_from(random_state % (16 * MIB) + 1).unwrap();
            let retained_batch_bytes = usize::try_from(retained_batch_bytes).unwrap_or(usize::MAX);
            let calculation = calculate_batch_memory_limit(
                total_memory_bytes,
                used_memory_bytes,
                Some(normal_memory_target_bytes),
                retained_batch_bytes,
                0.2,
                active_batch_slots,
                max_batch_bytes,
            );

            assert!(calculation.effective_batch_pool_bytes <= calculation.total_batch_quota_bytes);
            assert!(
                calculation.effective_batch_pool_bytes
                    <= calculation.normal_memory_batch_capacity_bytes.unwrap()
            );
            assert!(calculation.batch_size_limit_bytes >= 1);
            assert!(calculation.batch_size_limit_bytes <= max_batch_bytes.max(1));

            if calculation.effective_batch_pool_bytes == calculation.total_batch_quota_bytes {
                observed_batch_quota_bound = true;
            }
            if calculation.effective_batch_pool_bytes
                == calculation.normal_memory_batch_capacity_bytes.unwrap()
            {
                observed_normal_memory_capacity_bound = true;
            }
            if calculation.effective_batch_pool_bytes == 0 {
                observed_empty_capacity = true;
                assert_eq!(calculation.batch_size_limit_bytes, 1);
            } else {
                let expected_per_slot = (bytes_to_usize(calculation.effective_batch_pool_bytes)
                    / active_batch_slots)
                    .max(1);
                assert_eq!(calculation.per_slot_batch_size_bytes, expected_per_slot);
                assert_eq!(
                    calculation.batch_size_limit_bytes,
                    expected_per_slot.min(max_batch_bytes)
                );
            }
        }

        assert!(observed_batch_quota_bound);
        assert!(observed_normal_memory_capacity_bound);
        assert!(observed_empty_capacity);
    }

    #[test]
    fn cached_limit_refreshes_when_batch_slot_count_changes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _first_slot = governor.register_batch_slots(1);
        let mut cached_limit = governor.cached_limit();

        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 2_000);

        let second_slot = governor.register_batch_slots(1);
        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 1_000);

        drop(second_slot);
        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 2_000);
    }

    #[test]
    fn cached_limit_stays_fixed_until_the_next_memory_snapshot() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let _slot = governor.register_batch_slots(1);
        let mut cached_limit = governor.cached_limit();
        let mut reservation = governor.reservation();

        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 2_000);

        reservation.grow(1_000);

        // A batch cannot increase its own flush threshold while it grows inside
        // one sampling interval.
        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 2_000);

        // The next memory sample includes the retained allocation. It does not
        // change the estimated non-batch footprint, so the global quota remains
        // fixed.
        memory_monitor.set_memory_snapshot_for_test(1_000, 10_000);
        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 2_000);
    }

    #[test]
    fn cached_limit_refreshes_when_snapshot_revision_wraps() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, 10_000);
        memory_monitor.set_snapshot_revision_for_test(u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor.clone(), 0.2, 10_000);
        let mut cached_limit = governor.cached_limit();

        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 2_000);

        memory_monitor.set_memory_snapshot_for_test(8_000, 10_000);

        assert_eq!(memory_monitor.snapshot_revision(), 0);
        assert_eq!(cached_limit.current_batch_size_limit_bytes(), 1);
    }

    #[test]
    fn batch_size_limit_reaches_minimum_without_headroom_or_retained_batches() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(8_500);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);

        assert_eq!(governor.batch_size_limit_bytes(), 1);
    }

    #[test]
    fn batch_size_limit_is_capped_by_configured_max_bytes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10 * 1024 * 1024 * 1024);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 8 * 1024 * 1024);

        assert_eq!(governor.batch_size_limit_bytes(), 8 * 1024 * 1024);
    }

    #[test]
    fn batch_size_limit_uses_global_quota_when_lower_than_configured_max_bytes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);

        assert_eq!(governor.batch_size_limit_bytes(), 2_000);
    }

    #[test]
    fn batch_size_limit_uses_total_memory_when_backpressure_is_disabled() {
        let memory_monitor = MemoryMonitor::new_for_test_with_backpressure(None);
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(9_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);
        let _guard = governor.register_batch_slots(4);

        assert_eq!(governor.batch_size_limit_bytes(), 500);
    }

    #[test]
    fn reservation_clear_and_drop_release_retained_bytes() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_total_memory_bytes_for_test(10_000);
        memory_monitor.set_used_memory_bytes_for_test(7_000);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 1.0, 10_000);
        let mut reservation = governor.reservation();

        reservation.grow(600);
        assert_eq!(governor.batch_size_limit_bytes(), 1_600);

        reservation.clear();
        assert_eq!(governor.batch_size_limit_bytes(), 1_000);

        reservation.grow(400);
        assert_eq!(governor.batch_size_limit_bytes(), 1_400);

        drop(reservation);
        assert_eq!(governor.batch_size_limit_bytes(), 1_000);
    }

    #[test]
    fn saturated_reservation_stays_fail_closed_without_wrapping() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 1.0, usize::MAX);
        let mut reservation = governor.reservation();

        reservation.grow(usize::MAX);
        reservation.grow(1);

        assert_eq!(reservation.size_hint_bytes(), usize::MAX);
        assert_eq!(governor.batch_size_limit_bytes(), 1);

        drop(reservation);
        assert_eq!(governor.retained_batch_memory.bytes(), 0);
        assert!(governor.retained_batch_memory.is_saturated());
        assert_eq!(governor.batch_size_limit_bytes(), 1);
    }

    #[test]
    fn saturation_across_reservations_cannot_under_count_live_memory() {
        let memory_monitor = MemoryMonitor::new_for_test();
        memory_monitor.set_memory_snapshot_for_test(0, u64::MAX);
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 1.0, usize::MAX);
        let mut first = governor.reservation();
        let mut second = governor.reservation();

        first.grow(usize::MAX - 10);
        second.grow(20);

        assert_eq!(first.size_hint_bytes(), usize::MAX);
        assert_eq!(second.size_hint_bytes(), usize::MAX);
        assert_eq!(governor.retained_batch_memory.bytes(), usize::MAX);
        assert_eq!(governor.batch_size_limit_bytes(), 1);

        drop(first);
        assert_eq!(governor.retained_batch_memory.bytes(), 10);
        assert_eq!(governor.batch_size_limit_bytes(), 1);

        drop(second);
        assert_eq!(governor.retained_batch_memory.bytes(), 0);
        assert_eq!(governor.batch_size_limit_bytes(), 1);
    }

    #[test]
    fn batch_slot_guard_releases_registered_slots() {
        let memory_monitor = MemoryMonitor::new_for_test();
        let governor = BatchMemoryGovernor::new(1, memory_monitor, 0.2, 10_000);

        let slots = governor.register_batch_slots(2);
        assert_eq!(governor.active_batch_slots(), 2);

        drop(slots);
        assert_eq!(governor.active_batch_slots(), 1);
        assert_eq!(governor.active_batch_slots.load(Ordering::Relaxed), 0);
    }
}
