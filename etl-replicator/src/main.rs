//! ETL replicator service binary.
//!
//! Initializes and runs the replicator pipeline that handles Postgres logical replication
//! and routes data to configured destinations. Includes telemetry, error handling, and
//! graceful shutdown capabilities.

/// Jemalloc allocator for better memory management in high-throughput async workloads.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Jemalloc configuration optimized for high-throughput async CDC workloads.
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

use crate::core::start_replicator_with_config;
use crate::error::{ReplicatorError, ReplicatorResult};
use crate::error_notification::ErrorNotificationClient;
use ::tracing::{debug, error};
use etl_config::shared::ReplicatorConfig;
use std::process::ExitCode;
use tracing::info;

mod core;
mod error;
mod error_notification;
mod error_reporting;
mod init;
mod metrics;
mod sentry;

/// The name of the environment variable which contains version information for this replicator.
const APP_VERSION_ENV_NAME: &str = "APP_VERSION";

/// Entry point for the replicator service.
///
/// Loads configuration, initializes tracing and Sentry, starts the async runtime,
/// and launches the replicator pipeline. Handles all errors and ensures proper
/// service initialization sequence.
fn main() -> ExitCode {
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{}", err.render_report());
            ExitCode::FAILURE
        }
    }
}

/// Runs the replicator service and propagates typed errors.
fn try_main() -> ReplicatorResult<()> {
    // Phase 1: synchronous bootstrap before starting Tokio.
    //
    // Keep all fallible synchronous setup here so we fail fast without paying
    // the cost of building the async runtime unless startup can actually proceed.

    // Install rustls crypto provider before any TLS operations.
    init::crypto::init();

    // Load the replicator config.
    let replicator_config = init::config::init()?;

    // Keep the tracing and sentry guards alive until process shutdown.
    let _log_flusher = init::tracing::init(&replicator_config)?;
    let _sentry_guard = init::sentry::init(&replicator_config)?;

    info!("replicator bootstrap initialized");

    // We prepare the notification client used to send errors.
    let notification_client = init::error_notification::init(&replicator_config);

    // We initialize the Prometheus recorder.
    init::metrics::init(&replicator_config)?;

    debug!("starting tokio runtime");

    // Phase 2: start Tokio only once synchronous bootstrap has succeeded.
    run_async_runtime(replicator_config, notification_client)
}

/// Builds the Tokio runtime and runs the async replicator entry point.
fn run_async_runtime(
    replicator_config: ReplicatorConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(replicator_config, notification_client))
}

/// Main async entry point that starts the replicator pipeline.
///
/// Launches the replicator with the provided configuration and captures any errors
/// to Sentry and optionally sends notifications to the Supabase API.
async fn async_main(
    replicator_config: ReplicatorConfig,
    notification_client: Option<ErrorNotificationClient>,
) -> ReplicatorResult<()> {
    // Keep the feature flags client alive for the full async runtime lifetime.
    let _feature_flags_client = init::feature_flags::init(&replicator_config)?;

    info!("replicator bootstrap completed");

    if let Err(err) =
        start_replicator_with_config(replicator_config, notification_client.clone()).await
    {
        // We send the error to Sentry.
        sentry::capture_error(&err);

        // We log the error.
        error!("{err}");

        // We send an error notification if a client is available.
        if let Some(client) = notification_client {
            let error_message = err.to_string();
            match &err {
                ReplicatorError::Etl(etl_err) => {
                    client.notify_error(error_message.clone(), etl_err).await;
                }
                _ => {
                    client
                        .notify_error(error_message.clone(), error_message)
                        .await;
                }
            }
        }

        return Err(err);
    }

    Ok(())
}
