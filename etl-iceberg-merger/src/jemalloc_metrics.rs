//! Jemalloc memory metrics collection.
//!
//! Spawns a background task that periodically collects and reports jemalloc memory statistics
//! as Prometheus metrics. Provides visibility into allocator behavior for monitoring and
//! capacity planning.

use metrics::gauge;
use std::time::Duration;
use tracing::{debug, warn};

/// Interval for collecting jemalloc metrics.
const METRICS_INTERVAL: Duration = Duration::from_secs(60);

/// Spawns a background task that periodically collects jemalloc metrics.
///
/// The task runs every 60 seconds and exports the following metrics:
/// - `jemalloc.allocated`: Currently allocated bytes
/// - `jemalloc.resident`: Resident memory (RSS)
/// - `jemalloc.metadata`: Metadata overhead
/// - `jemalloc.retained`: Retained but unused memory
pub fn spawn_jemalloc_metrics_task() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(METRICS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            collect_jemalloc_metrics();
        }
    });
}

/// Collects and reports current jemalloc statistics.
fn collect_jemalloc_metrics() {
    match tikv_jemalloc_ctl::epoch::mib() {
        Ok(epoch_mib) => {
            if let Err(err) = epoch_mib.advance() {
                warn!(?err, "failed to advance jemalloc epoch");
                return;
            }
        }
        Err(err) => {
            warn!(?err, "failed to get jemalloc epoch mib");
            return;
        }
    }

    if let Ok(allocated_mib) = tikv_jemalloc_ctl::stats::allocated::mib()
        && let Ok(allocated) = allocated_mib.read()
    {
        gauge!("jemalloc.allocated").set(allocated as f64);
        debug!(allocated, "jemalloc allocated bytes");
    }

    if let Ok(resident_mib) = tikv_jemalloc_ctl::stats::resident::mib()
        && let Ok(resident) = resident_mib.read()
    {
        gauge!("jemalloc.resident").set(resident as f64);
        debug!(resident, "jemalloc resident bytes");
    }

    if let Ok(metadata_mib) = tikv_jemalloc_ctl::stats::metadata::mib()
        && let Ok(metadata) = metadata_mib.read()
    {
        gauge!("jemalloc.metadata").set(metadata as f64);
        debug!(metadata, "jemalloc metadata bytes");
    }

    if let Ok(retained_mib) = tikv_jemalloc_ctl::stats::retained::mib()
        && let Ok(retained) = retained_mib.read()
    {
        gauge!("jemalloc.retained").set(retained as f64);
        debug!(retained, "jemalloc retained bytes");
    }
}
