use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use metrics::gauge;

use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::metrics::{ETL_IDEAL_BATCH_SIZE_BYTES, PIPELINE_ID_LABEL};
use crate::types::PipelineId;

// This value is purposefully fixed for now.
/// Safety factor applied to the configured memory threshold for batch filling.
const BATCH_FILL_TARGET_SAFETY_FACTOR: f64 = 0.8;
/// Fallback target memory usage ratio when no backpressure threshold is configured.
const BATCH_FILL_TARGET_FALLBACK_RATIO: f64 = 0.8;
/// Refresh interval for cached batch budget reads.
const CACHED_BATCH_BUDGET_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

/// Computes per-stream batch byte budgets from current memory and active stream load.
#[derive(Debug, Clone)]
pub struct BatchBudgetController {
    pipeline_id: PipelineId,
    memory_monitor: MemoryMonitor,
    active_streams: Arc<AtomicUsize>,
}

impl BatchBudgetController {
    /// Creates a new [`BatchBudgetController`] instance.
    pub fn new(pipeline_id: PipelineId, memory_monitor: MemoryMonitor) -> Self {
        Self {
            pipeline_id,
            memory_monitor,
            active_streams: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Registers active stream load units and returns a guard that unregisters on drop.
    pub fn register_stream_load(&self, units: usize) -> ActiveStreamsGuard {
        let units = units.max(1);
        self.active_streams.fetch_add(units, Ordering::Relaxed);

        ActiveStreamsGuard {
            active_streams: self.active_streams.clone(),
            units,
        }
    }

    /// Returns a cached budget reader that refreshes from the controller every 100ms.
    pub fn cached(&self) -> CachedBatchBudget {
        CachedBatchBudget::new(self.clone())
    }

    /// Returns the target memory usage ratio for batch filling.
    ///
    /// When memory backpressure is configured, this is `activate_threshold * 0.8`.
    /// Otherwise this falls back to `0.8`.
    fn batch_fill_target_ratio(&self) -> f64 {
        self.memory_monitor
            .memory_backpressure_activate_threshold()
            .map(|threshold| {
                (f64::from(threshold) * BATCH_FILL_TARGET_SAFETY_FACTOR).clamp(0.0, 1.0)
            })
            .unwrap_or(BATCH_FILL_TARGET_FALLBACK_RATIO)
    }

    /// Returns the ideal per-batch size in bytes.
    ///
    /// The budget is computed as:
    /// (total_memory_bytes * target_ratio) / active_streams
    pub fn ideal_batch_size_bytes(&self) -> usize {
        let total_memory_bytes = self.memory_monitor.total_memory_bytes() as f64;
        let target_ratio = self.batch_fill_target_ratio();
        let target_bytes = (total_memory_bytes * target_ratio).max(1.0) as usize;
        let active_streams = self.active_streams.load(Ordering::Relaxed).max(1);
        let denominator = active_streams;
        let ideal_batch_size_bytes = (target_bytes / denominator).max(1);

        gauge!(
            ETL_IDEAL_BATCH_SIZE_BYTES,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .set(ideal_batch_size_bytes as f64);

        ideal_batch_size_bytes
    }
}

/// Cached view over [`BatchBudgetController`] ideal batch size calculations.
///
/// This avoids querying atomics and recomputing budgets on every item while still adapting quickly
/// to changing memory pressure and worker load.
#[derive(Debug, Clone)]
pub struct CachedBatchBudget {
    controller: BatchBudgetController,
    last_checked_at: Option<Instant>,
    last_known_batch_size_bytes: usize,
}

impl CachedBatchBudget {
    /// Creates a new cached budget initialized from the current controller value.
    pub fn new(controller: BatchBudgetController) -> Self {
        Self {
            last_known_batch_size_bytes: controller.ideal_batch_size_bytes(),
            controller,
            last_checked_at: None,
        }
    }

    /// Returns the current ideal batch size in bytes, refreshing at most every 100ms.
    pub fn current_batch_size_bytes(&mut self) -> usize {
        let now = Instant::now();
        let should_refresh = match self.last_checked_at {
            Some(last_checked_at) => {
                now.duration_since(last_checked_at) >= CACHED_BATCH_BUDGET_REFRESH_INTERVAL
            }
            None => true,
        };

        if should_refresh {
            self.last_known_batch_size_bytes = self.controller.ideal_batch_size_bytes();
            self.last_checked_at = Some(now);
        }

        self.last_known_batch_size_bytes
    }
}

/// RAII guard that decrements active stream load count on drop.
#[derive(Debug)]
pub struct ActiveStreamsGuard {
    active_streams: Arc<AtomicUsize>,
    units: usize,
}

impl Drop for ActiveStreamsGuard {
    fn drop(&mut self) {
        self.active_streams.fetch_sub(self.units, Ordering::Relaxed);
    }
}
