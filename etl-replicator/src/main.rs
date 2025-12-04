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
/// - `background_thread:true`: Offloads memory purging to background threads (Linux only).
/// - `metadata_thp:auto`: Enables transparent huge pages for jemalloc metadata, reducing TLB misses.
/// - `dirty_decay_ms:10000`: Returns unused dirty pages to the OS after 10 seconds.
/// - `muzzy_decay_ms:10000`: Returns unused muzzy pages to the OS after 10 seconds.
/// - `tcache_max:8192`: Reduces thread-local cache size for better container memory efficiency.
/// - `abort_conf:true`: Aborts on invalid configuration for fail-fast behavior.
///
/// Note: tikv-jemallocator uses the `_rjem_` prefix for symbols since `unprefixed_malloc_on_supported_platforms`
/// is not enabled. This config can be overridden via `_RJEM_MALLOC_CONF` env var.
/// `narenas` should be set via env var to match container CPU limits per environment.
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

use crate::config::load_replicator_config;
use crate::core::start_replicator_with_config;
use crate::notification::ErrorNotificationClient;
use etl::error::EtlError;
use etl_config::Environment;
use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;
use etl_telemetry::tracing::init_tracing_with_top_level_fields;
use secrecy::ExposeSecret;
use std::sync::Arc;
use tracing::{error, info, warn};

mod config;
mod core;
mod feature_flags;
#[cfg(not(target_env = "msvc"))]
mod jemalloc_metrics;
mod migrations;
mod notification;

/// The name of the environment variable which contains version information for this replicator.
const APP_VERSION_ENV_NAME: &str = "APP_VERSION";

/// Entry point for the replicator service.
///
/// Loads configuration, initializes tracing and Sentry, starts the async runtime,
/// and launches the replicator pipeline. Handles all errors and ensures proper
/// service initialization sequence.
fn main() -> anyhow::Result<()> {
    // Load replicator config
    let replicator_config = load_replicator_config()?;

    // Initialize tracing with project reference
    let _log_flusher = init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        replicator_config.project_ref(),
        Some(replicator_config.pipeline.id),
    )?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = init_sentry()?;

    // Initialize metrics collection
    init_metrics(replicator_config.project_ref())?;

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
async fn async_main(replicator_config: ReplicatorConfig) -> anyhow::Result<()> {
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
        sentry::capture_error(&*err);
        error!("an error occurred in the replicator: {err}");

        // Send an error notification if a client is available.
        if let Some(client) = notification_client {
            let error_message = format!("{err}");
            match err.downcast_ref::<EtlError>() {
                Some(err) => {
                    client.notify_error(error_message.clone(), err).await;
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

/// Initializes Sentry with replicator-specific configuration.
///
/// Loads configuration and sets up Sentry if a DSN is provided in the config.
/// Tags all errors with the "replicator" service identifier and configures
/// panic handling to automatically capture and send panics to Sentry.
fn init_sentry() -> anyhow::Result<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_replicator_config()
        && let Some(sentry_config) = &config.sentry
    {
        info!("initializing sentry with supplied dsn");

        let environment = Environment::load()?;
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(sentry_config.dsn.expose_secret().parse()?),
            environment: Some(environment.to_string().into()),
            integrations: vec![Arc::new(
                sentry::integrations::panic::PanicIntegration::new(),
            )],
            ..Default::default()
        });

        // We load the version of the replicator which is specified via environment variable.
        let version = std::env::var(APP_VERSION_ENV_NAME);

        // Set service tag to differentiate replicator from other services
        sentry::configure_scope(|scope| {
            scope.set_tag("service", "replicator");
            if let Ok(version) = version {
                scope.set_tag("version", version);
            }
        });

        return Ok(Some(guard));
    }

    info!("sentry not configured for replicator, skipping initialization");

    Ok(None)
}
