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

use crate::config::load_replicator_config;
use crate::core::start_replicator_with_config;
use crate::error::{ReplicatorError, ReplicatorResult};
use crate::notification::ErrorNotificationClient;

use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;
use etl_telemetry::tracing::init_tracing_with_top_level_fields;
use secrecy::ExposeSecret;
use tracing::{error, info, warn};

mod config;
mod core;
mod error;
mod feature_flags;
#[cfg(not(target_env = "msvc"))]
mod jemalloc_metrics;
mod migrations;
mod notification;
mod sentry;

/// The name of the environment variable which contains version information for this replicator.
const APP_VERSION_ENV_NAME: &str = "APP_VERSION";

/// Entry point for the replicator service.
///
/// Loads configuration, initializes tracing and Sentry, starts the async runtime,
/// and launches the replicator pipeline. Handles all errors and ensures a proper
/// service initialization sequence.
fn main() -> ReplicatorResult<()> {
    // Load replicator config
    let replicator_config = load_replicator_config()?;

    // Initialize tracing with project reference
    let _log_flusher = init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        replicator_config.project_ref(),
        Some(replicator_config.pipeline.id),
    )
    .map_err(ReplicatorError::config)?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = sentry::init()?;

    // Initialize metrics collection
    init_metrics(replicator_config.project_ref()).map_err(ReplicatorError::config)?;

    // We start the runtime.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(replicator_config))?;

    Ok(())
}

/// Main async entry point that starts the replicator pipeline.
///
/// Launches the replicator with the provided configuration and captures any errors
/// to Sentry and optionally sends notifications to the Supabase API.
async fn async_main(replicator_config: ReplicatorConfig) -> ReplicatorResult<()> {
    // Start the jemalloc metrics collection background task.
    #[cfg(not(target_env = "msvc"))]
    jemalloc_metrics::spawn_jemalloc_metrics_task(replicator_config.pipeline.id);

    let notification_client = replicator_config.supabase.as_ref().and_then(
        |supabase_config| match (&supabase_config.api_url, &supabase_config.api_key) {
            (Some(api_url), Some(api_key)) => Some(ErrorNotificationClient::new(
                api_url.clone(),
                api_key.expose_secret().to_owned(),
                supabase_config.project_ref.clone(),
                replicator_config.pipeline.id.to_string(),
            )),
            _ => {
                warn!(
                    "missing supabase api url and/or key, failure notifications will not be sent"
                );
                None
            }
        },
    );

    // Initialize ConfigCat feature flags if supplied.
    let configcat_sdk_key = replicator_config
        .supabase
        .as_ref()
        .and_then(|s| s.configcat_sdk_key.as_deref());
    let _feature_flags_client = if let Some(configcat_sdk_key) = configcat_sdk_key {
        Some(feature_flags::init_feature_flags(
            configcat_sdk_key,
            replicator_config.project_ref(),
        )?)
    } else {
        info!("configcat not configured for replicator, skipping initialization");
        None
    };

    // We start the replicator and catch any errors.
    if let Err(err) = start_replicator_with_config(replicator_config).await {
        // Capture to Sentry with proper backtrace handling.
        sentry::capture_error(&err);
        error!("{err}");

        // Send an error notification if a client is available.
        if let Some(client) = notification_client {
            let error_message = format!("{err}");
            match err.as_etl_error() {
                Some(etl_err) => {
                    client.notify_error(error_message.clone(), etl_err).await;
                }
                None => {
                    client
                        .notify_error(error_message.clone(), error_message)
                        .await;
                }
            };
        }

        return Err(err);
    }

    Ok(())
}
