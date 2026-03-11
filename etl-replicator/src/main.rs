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
use crate::error_notification::ErrorNotificationClient;
use ::sentry::ClientInitGuard;
use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;
use etl_telemetry::tracing::{LogFlusher, init_tracing_with_top_level_fields};
use secrecy::ExposeSecret;
use std::process::ExitCode;
use std::sync::Once;
use tracing::{error, info, warn};

mod config;
mod core;
mod error;
mod error_notification;
mod feature_flags;
mod metrics;
mod sentry;

/// The name of the environment variable which contains version information for this replicator.
const APP_VERSION_ENV_NAME: &str = "APP_VERSION";

/// Ensures crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

/// Long-lived process resources that must remain in scope until shutdown.
struct LongLivedProcessResources {
    _log_flusher: LogFlusher,
    _feature_flags_client: Option<configcat::Client>,
    _sentry_guard: Option<ClientInitGuard>,
}

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
    install_crypto_provider();

    // Load the replicator config.
    let replicator_config = load_replicator_config()?;

    // We prepare the notification client used to send errors.
    let notification_client = build_notification_client(&replicator_config);

    // We initialize the Prometheus recorder.
    init_prometheus(&replicator_config)?;

    // We initialize all long-lived resources that have to stay in scope in order for them to work.
    let _long_lived_process_resources = init_long_lived_process_resources(&replicator_config)?;

    // Phase 2: start Tokio only once synchronous bootstrap has succeeded.
    run_async_runtime(replicator_config, notification_client)
}

/// Initializes long-lived process resources that must survive for the program duration.
fn init_long_lived_process_resources(
    replicator_config: &ReplicatorConfig,
) -> ReplicatorResult<LongLivedProcessResources> {
    let log_flusher = init_tracing(replicator_config)?;
    let feature_flags_client = init_optional_feature_flags(replicator_config)?;
    let sentry_guard = sentry::init()?;

    Ok(LongLivedProcessResources {
        _log_flusher: log_flusher,
        _feature_flags_client: feature_flags_client,
        _sentry_guard: sentry_guard,
    })
}

/// Installs the default cryptographic provider for rustls.
///
/// Uses AWS LC cryptographic provider and ensures it's only installed once
/// across the application lifetime to avoid conflicts. This is needed because
/// Cargo's feature unification causes rustls to have both ring and aws-lc-rs
/// features enabled, requiring explicit selection.
fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

/// Builds the optional error notification client from replicator config.
fn build_notification_client(
    replicator_config: &ReplicatorConfig,
) -> Option<ErrorNotificationClient> {
    replicator_config.supabase.as_ref().and_then(
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
    )
}

/// Initializes the optional ConfigCat client if configured.
fn init_optional_feature_flags(
    replicator_config: &ReplicatorConfig,
) -> ReplicatorResult<Option<configcat::Client>> {
    let configcat_sdk_key = replicator_config
        .supabase
        .as_ref()
        .and_then(|supabase_config| supabase_config.configcat_sdk_key.as_deref());

    if let Some(configcat_sdk_key) = configcat_sdk_key {
        Ok(Some(feature_flags::init_feature_flags(
            configcat_sdk_key,
            replicator_config.project_ref(),
        )?))
    } else {
        info!("configcat not configured for replicator, skipping initialization");
        Ok(None)
    }
}

/// Initializes tracing with top-level fields from replicator config.
fn init_tracing(replicator_config: &ReplicatorConfig) -> ReplicatorResult<LogFlusher> {
    init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        replicator_config.project_ref(),
        Some(replicator_config.pipeline.id),
    )
    .map_err(ReplicatorError::config)
}

/// Initializes the Prometheus recorder and HTTP listener.
fn init_prometheus(replicator_config: &ReplicatorConfig) -> ReplicatorResult<()> {
    init_metrics(replicator_config.project_ref()).map_err(ReplicatorError::config)
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
