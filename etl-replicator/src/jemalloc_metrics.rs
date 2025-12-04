//! Jemalloc allocator metrics for Prometheus monitoring.
//!
//! Exposes jemalloc statistics as Prometheus gauges, enabling monitoring of memory
//! allocation patterns, fragmentation, and overall allocator health. Uses MIB-based
//! access for efficient repeated polling.
//!
//! # Interpreting the Metrics
//!
//! - **Healthy state**: `allocated` close to `active`, `active` close to `resident`.
//! - **Fragmentation**: A large gap between `allocated` and `resident` indicates
//!   overhead from page alignment, dirty pages, or metadata.
//! - **Memory pressure**: If `resident` approaches container limits while `allocated`
//!   is much lower, consider tuning decay settings or investigating allocation patterns.
//! - **Retained memory**: High `retained` is normal on 64-bit Linux. It represents
//!   virtual address space only - no physical memory cost.

use std::time::Duration;

use metrics::{Unit, describe_gauge, gauge};
use tikv_jemalloc_ctl::{epoch, opt, raw, stats};
use tracing::{debug, info, warn};

/// Total bytes allocated by the application and currently in use.
///
/// This is the sum of all active allocations made via `malloc`, `Box::new`, `Vec`, etc.
/// It represents the actual memory your application has requested and is using.
///
/// This is the most accurate measure of your application's memory footprint from the
/// application's perspective. Compare with `resident` to understand overhead.
const JEMALLOC_ALLOCATED_BYTES: &str = "jemalloc_allocated_bytes";

/// Total bytes in active pages allocated by the application.
///
/// Active pages are memory pages that jemalloc has dedicated to serving application
/// allocations. This is a multiple of the page size and includes both currently
/// allocated bytes and freed-but-not-returned bytes within those pages.
///
/// `active >= allocated` always (per jemalloc docs). The gap represents:
/// - Page alignment overhead (allocations rounded up to page boundaries).
/// - Internal fragmentation within pages still in use.
///
/// A large gap suggests many small allocations or size patterns that don't fit
/// jemalloc's size classes efficiently.
const JEMALLOC_ACTIVE_BYTES: &str = "jemalloc_active_bytes";

/// Maximum bytes in physically resident data pages mapped by the allocator.
///
/// Per jemalloc docs, this comprises all pages dedicated to:
/// - Allocator metadata.
/// - Pages backing active allocations.
/// - Unused dirty pages (freed but not yet returned to OS).
///
/// This is the physical RAM footprint that counts against container memory limits.
/// Note: This is a "maximum" because pages may not actually be resident if they
/// correspond to demand-zeroed virtual memory not yet touched.
///
/// **Important**: There is no guaranteed ordering between `resident` and `mapped`.
/// Resident can exceed mapped (due to dirty pages) or be less (unmaterialized pages).
///
/// Monitor this metric against your container memory limits to prevent OOMKilled.
const JEMALLOC_RESIDENT_BYTES: &str = "jemalloc_resident_bytes";

/// Total bytes in active extents mapped by the allocator.
///
/// This is virtual memory mapped via `mmap()` for active extents. Per jemalloc docs,
/// `mapped > active` always, but there is **no strict ordering with `resident`**.
///
/// Why no ordering with resident:
/// - `mapped` excludes inactive extents (even those with dirty pages).
/// - `resident` includes dirty pages but only counts physically-backed memory.
///
/// This metric helps understand virtual memory usage patterns. For container memory
/// limits, focus on `resident` instead since only physical memory is enforced.
const JEMALLOC_MAPPED_BYTES: &str = "jemalloc_mapped_bytes";

/// Total bytes in virtual memory mappings retained for future reuse.
///
/// When jemalloc returns memory to the OS, it can retain the virtual address mapping
/// (without physical pages) for faster reallocation later. This is controlled by
/// `opt.retain` (enabled by default on 64-bit Linux).
///
/// High `retained` values are normal and don't consume physical memory. This metric
/// helps understand jemalloc's virtual memory management but rarely requires action.
const JEMALLOC_RETAINED_BYTES: &str = "jemalloc_retained_bytes";

/// Total bytes dedicated to jemalloc metadata.
///
/// jemalloc maintains internal data structures for tracking allocations, arenas,
/// thread caches, extent maps, etc. This overhead scales with allocation count
/// and arena count, not allocation size.
///
/// Typical overhead is 1-3% of allocated memory. Higher ratios may indicate too
/// many small allocations or too many arenas (`narenas` setting).
const JEMALLOC_METADATA_BYTES: &str = "jemalloc_metadata_bytes";

/// Memory fragmentation ratio: `(resident - allocated) / resident`.
///
/// Measures how efficiently physical memory is being used:
/// - **0.0**: Perfect efficiency (all resident memory is allocated). Rare in practice.
/// - **0.1-0.3**: Healthy range for most workloads.
/// - **0.3-0.5**: Moderate fragmentation. Consider investigating if memory-constrained.
/// - **>0.5**: Significant fragmentation. May indicate allocation pattern issues or
///   need for decay tuning.
///
/// High fragmentation can occur from:
/// - Bursty allocation patterns (memory freed but decay hasn't run).
/// - Many small allocations with varying lifetimes.
/// - Size class mismatches (allocations don't fit jemalloc's size buckets well).
///
/// To reduce fragmentation: lower decay times, reduce `tcache_max`, or investigate
/// allocation patterns with jemalloc's heap profiling.
const JEMALLOC_FRAGMENTATION_RATIO: &str = "jemalloc_fragmentation_ratio";

/// Polling interval for jemalloc statistics.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Label key for pipeline identifier.
const PIPELINE_ID_LABEL: &str = "pipeline_id";

/// Label key for application type.
const APP_TYPE_LABEL: &str = "app_type";

/// Application type value for the replicator.
const APP_TYPE_VALUE: &str = "etl-replicator-app";

/// Logs the current jemalloc configuration for validation.
///
/// Reads and logs opt.* values to verify the malloc_conf settings were applied.
/// Uses raw mallctl for decay settings not exposed by the typed API.
fn log_jemalloc_config() {
    // Read typed opt values.
    let background_thread = opt::background_thread::read().ok();
    let narenas = opt::narenas::read().ok();
    let tcache = opt::tcache::read().ok();
    let tcache_max = opt::tcache_max::read().ok();
    let abort = opt::abort::read().ok();

    // Read decay settings via raw mallctl (not exposed in typed API).
    // SAFETY: These are read-only queries to jemalloc's opt.* configuration values.
    // The keys are valid null-terminated strings and the return type matches jemalloc's ssize_t.
    let dirty_decay_ms: Option<isize> = unsafe { raw::read(b"opt.dirty_decay_ms\0") }.ok();
    let muzzy_decay_ms: Option<isize> = unsafe { raw::read(b"opt.muzzy_decay_ms\0") }.ok();

    info!(
        background_thread = ?background_thread,
        narenas = ?narenas,
        tcache = ?tcache,
        tcache_max = ?tcache_max,
        dirty_decay_ms = ?dirty_decay_ms,
        muzzy_decay_ms = ?muzzy_decay_ms,
        abort_conf = ?abort,
        "jemalloc configuration"
    );
}

/// Registers jemalloc metric descriptions with the global metrics recorder.
fn register_metrics() {
    describe_gauge!(
        JEMALLOC_ALLOCATED_BYTES,
        Unit::Bytes,
        "Total bytes allocated by the application"
    );
    describe_gauge!(
        JEMALLOC_ACTIVE_BYTES,
        Unit::Bytes,
        "Total bytes in active pages allocated by the application"
    );
    describe_gauge!(
        JEMALLOC_RESIDENT_BYTES,
        Unit::Bytes,
        "Total bytes in physically resident data pages mapped by the allocator"
    );
    describe_gauge!(
        JEMALLOC_MAPPED_BYTES,
        Unit::Bytes,
        "Total bytes in active extents mapped by the allocator"
    );
    describe_gauge!(
        JEMALLOC_RETAINED_BYTES,
        Unit::Bytes,
        "Total bytes in virtual memory mappings retained for future reuse"
    );
    describe_gauge!(
        JEMALLOC_METADATA_BYTES,
        Unit::Bytes,
        "Total bytes dedicated to jemalloc metadata"
    );
    describe_gauge!(
        JEMALLOC_FRAGMENTATION_RATIO,
        Unit::Count,
        "Memory fragmentation ratio: (resident - allocated) / resident. Lower is better, >0.5 indicates significant fragmentation"
    );
}

/// Spawns a background task that periodically polls jemalloc statistics.
///
/// The task runs every 10 seconds and updates Prometheus gauges with current
/// allocator statistics. Uses MIB-based access for efficient repeated polling.
///
/// This function should be called after [`etl_telemetry::metrics::init_metrics`]
/// to ensure the metrics recorder is installed.
pub fn spawn_jemalloc_metrics_task(pipeline_id: u64) {
    register_metrics();
    log_jemalloc_config();

    let pipeline_id_str = pipeline_id.to_string();

    tokio::spawn(async move {
        // Initialize MIBs once for efficient repeated lookups.
        // MIBs translate string keys to numeric indices, avoiding string parsing on each read.
        let epoch_mib = match epoch::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc epoch MIB: {err}");
                return;
            }
        };
        let allocated_mib = match stats::allocated::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc allocated MIB: {err}");
                return;
            }
        };
        let active_mib = match stats::active::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc active MIB: {err}");
                return;
            }
        };
        let resident_mib = match stats::resident::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc resident MIB: {err}");
                return;
            }
        };
        let mapped_mib = match stats::mapped::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc mapped MIB: {err}");
                return;
            }
        };
        let retained_mib = match stats::retained::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc retained MIB: {err}");
                return;
            }
        };
        let metadata_mib = match stats::metadata::mib() {
            Ok(mib) => mib,
            Err(err) => {
                warn!("failed to initialize jemalloc metadata MIB: {err}");
                return;
            }
        };

        loop {
            // Advance epoch to refresh cached statistics.
            if let Err(err) = epoch_mib.advance() {
                warn!("failed to advance jemalloc epoch: {err}");
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }

            // Read all statistics.
            let allocated = allocated_mib.read().unwrap_or(0) as f64;
            let active = active_mib.read().unwrap_or(0) as f64;
            let resident = resident_mib.read().unwrap_or(0) as f64;
            let mapped = mapped_mib.read().unwrap_or(0) as f64;
            let retained = retained_mib.read().unwrap_or(0) as f64;
            let metadata = metadata_mib.read().unwrap_or(0) as f64;

            // Update gauges with pipeline_id and app_type labels.
            gauge!(
                JEMALLOC_ALLOCATED_BYTES,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(allocated);
            gauge!(
                JEMALLOC_ACTIVE_BYTES,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(active);
            gauge!(
                JEMALLOC_RESIDENT_BYTES,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(resident);
            gauge!(
                JEMALLOC_MAPPED_BYTES,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(mapped);
            gauge!(
                JEMALLOC_RETAINED_BYTES,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(retained);
            gauge!(
                JEMALLOC_METADATA_BYTES,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(metadata);

            // Calculate fragmentation ratio: (resident - allocated) / resident.
            // A ratio of 0 means no fragmentation, >0.5 indicates significant fragmentation.
            let fragmentation = if resident > 0.0 {
                (resident - allocated) / resident
            } else {
                0.0
            };
            gauge!(
                JEMALLOC_FRAGMENTATION_RATIO,
                PIPELINE_ID_LABEL => pipeline_id_str.clone(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
            )
            .set(fragmentation);

            debug!(
                allocated_mb = allocated / 1_048_576.0,
                resident_mb = resident / 1_048_576.0,
                fragmentation_ratio = fragmentation,
                "jemalloc stats updated"
            );

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
}
