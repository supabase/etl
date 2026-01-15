//! ETL Iceberg merger service binary.
//!
//! Background merge runner that reads raw CDC events from Iceberg changelog tables,
//! uses a Puffin-file-backed secondary index to deduplicate and merge events,
//! and produces clean mirror tables.

/// Jemalloc allocator for better memory management in high-throughput async workloads.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Jemalloc configuration optimized for merge workloads.
///
/// - `narenas:8`: Fixed arena count for predictable memory behavior in containers.
/// - `background_thread:true`: Offloads memory purging to background threads (Linux only).
/// - `metadata_thp:auto`: Enables transparent huge pages for jemalloc metadata, reducing TLB misses.
/// - `dirty_decay_ms:10000`: Returns unused dirty pages to the OS after 10 seconds.
/// - `muzzy_decay_ms:10000`: Returns unused muzzy pages to the OS after 10 seconds.
/// - `tcache_max:8192`: Reduces thread-local cache size for better container memory efficiency.
/// - `abort_conf:true`: Aborts on invalid configuration for fail-fast behavior.
///
/// On Linux, this can be overridden via `MALLOC_CONF` env var.
/// On macOS, use `_RJEM_MALLOC_CONF` (unprefixed symbols not supported).
#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

/// Jemalloc configuration for macOS (uses prefixed symbol since unprefixed not supported).
#[cfg(all(target_os = "macos", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

use etl_iceberg_merger::config::load_merger_config;
use etl_iceberg_merger::scheduler::run_scheduler;
use etl_telemetry::metrics::init_metrics;
use etl_telemetry::tracing::init_tracing_with_top_level_fields;
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
mod jemalloc_metrics;

/// The name of the environment variable which contains version information for this merger.
const APP_VERSION_ENV_NAME: &str = "APP_VERSION";

/// Entry point for the merger service.
///
/// Loads configuration, initializes tracing and Sentry, starts the async runtime,
/// and launches the merge scheduler. Handles all errors and ensures proper
/// service initialization sequence.
fn main() -> anyhow::Result<()> {
    // Load merger config
    let merger_config = load_merger_config()?;

    // Initialize tracing with service name
    let _log_flusher = init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        Some(&merger_config.project_ref),
        None,
    )?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = init_sentry()?;

    // Initialize metrics collection
    init_metrics(Some(&merger_config.project_ref))?;

    // Start the runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(merger_config))?;

    Ok(())
}

/// Main async entry point that starts the merge scheduler.
///
/// Launches the scheduler with the provided configuration and captures any errors
/// to Sentry.
async fn async_main(merger_config: etl_iceberg_merger::config::MergerConfig) -> anyhow::Result<()> {
    // Start the jemalloc metrics collection background task
    #[cfg(not(target_env = "msvc"))]
    jemalloc_metrics::spawn_jemalloc_metrics_task();

    // Run the scheduler
    if let Err(err) = run_scheduler(merger_config).await {
        sentry::capture_error(&*err);
        error!(%err, "an error occurred in the merger scheduler");
        return Err(err);
    }

    Ok(())
}

/// Initializes Sentry with merger-specific configuration.
///
/// Loads configuration and sets up Sentry if a DSN is provided in the config.
/// Tags all errors with the "iceberg-merger" service identifier and configures
/// panic handling to automatically capture and send panics to Sentry.
fn init_sentry() -> anyhow::Result<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_merger_config()
        && let Some(sentry_config) = &config.sentry
    {
        info!("initializing sentry with supplied dsn");

        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(sentry_config.dsn.parse()?),
            environment: Some(config.environment.to_string().into()),
            integrations: vec![std::sync::Arc::new(
                sentry::integrations::panic::PanicIntegration::new(),
            )],
            ..Default::default()
        });

        // Load the version of the merger which is specified via environment variable
        let version = std::env::var(APP_VERSION_ENV_NAME);
        if let Ok(version) = version {
            sentry::configure_scope(|scope| {
                scope.set_tag("service", "iceberg-merger");
                scope.set_tag("version", version);
            });
        } else {
            sentry::configure_scope(|scope| {
                scope.set_tag("service", "iceberg-merger");
            });
        }

        Ok(Some(guard))
    } else {
        info!("sentry not configured, skipping initialization");
        Ok(None)
    }
}
