use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use metrics::gauge;
use tracing::debug;

use crate::concurrency::memory_monitor::MemoryMonitor;
use crate::metrics::{ETL_IDEAL_BATCH_SIZE_BYTES, PIPELINE_ID_LABEL};
use crate::types::PipelineId;

/// Refresh interval for cached batch budget reads.
const CACHED_BATCH_BUDGET_REFRESH_INTERVAL: Duration = Duration::from_millis(100);

/// Computes per-stream batch byte budgets from current memory and active stream load.
#[derive(Debug, Clone)]
pub struct BatchBudgetController {
    pipeline_id: PipelineId,
    memory_monitor: MemoryMonitor,
    memory_budget_ratio: f64,
    active_streams: Arc<AtomicUsize>,
}

impl BatchBudgetController {
    /// Creates a new [`BatchBudgetController`] instance.
    pub fn new(
        pipeline_id: PipelineId,
        memory_monitor: MemoryMonitor,
        memory_budget_ratio: f32,
    ) -> Self {
        Self {
            pipeline_id,
            memory_monitor,
            memory_budget_ratio: f64::from(memory_budget_ratio),
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

    /// Returns the ideal per-batch size in bytes.
    ///
    /// The budget is computed as:
    /// `(total_memory_bytes * target_ratio) / active_streams`.
    ///
    /// This intentionally subdivides the configured memory percentage across concurrently active
    /// streams, leaving headroom for other allocations in the process, such as destination batch
    /// construction and serialization buffers.
    pub fn ideal_batch_size_bytes(&self) -> usize {
        let total_memory_bytes = self.memory_monitor.total_memory_bytes() as f64;
        let target_ratio = self.memory_budget_ratio;
        let target_bytes = (total_memory_bytes * target_ratio).max(1.0) as usize;
        let active_streams = self.active_streams.load(Ordering::Relaxed).max(1);
        let ideal_batch_size_bytes = (target_bytes / active_streams).max(1);

        debug!(
            pipeline_id = self.pipeline_id,
            total_memory_bytes = total_memory_bytes as u64,
            target_ratio,
            target_bytes,
            active_streams,
            ideal_batch_size_bytes,
            "computed ideal batch budget size"
        );

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
