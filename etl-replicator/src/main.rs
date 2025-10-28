//! ETL replicator service binary.
//!
//! Initializes and runs the replicator pipeline that handles Postgres logical replication
//! and routes data to configured destinations. Includes telemetry, error handling, and
//! graceful shutdown capabilities.

use etl_config::Environment;
use etl_config::shared::ReplicatorConfig;
use etl_telemetry::metrics::init_metrics;
use etl_telemetry::tracing::init_tracing_with_top_level_fields;
use secrecy::ExposeSecret;
use std::sync::Arc;
use tracing::{error, info};

use crate::config::load_replicator_config;
use crate::core::start_replicator_with_config;
use crate::notification::ErrorNotificationClient;

mod config;
mod core;
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

    // Extract project reference to use in logs
    let project_ref = replicator_config
        .supabase
        .as_ref()
        .map(|s| s.project_ref.clone());

    // Initialize tracing with project reference
    let _log_flusher = init_tracing_with_top_level_fields(
        env!("CARGO_BIN_NAME"),
        project_ref.clone(),
        Some(replicator_config.pipeline.id),
    )?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = init_sentry()?;

    // Initialize metrics collection
    init_metrics(project_ref)?;

    // We start the runtime.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(replicator_config))?;

    Ok(())
}

/// Searches through an error chain to find an [`etl::error::EtlError`].
///
/// Walks through the error's source chain using [`std::error::Error::source`]
/// and attempts to downcast each error to [`etl::error::EtlError`]. Returns
/// the first [`EtlError`] found, or [`None`] if none exists in the chain.
fn find_etl_error(err: &anyhow::Error) -> Option<&etl::error::EtlError> {
    // First, try direct downcast (most common case).
    if let Some(etl_err) = err.downcast_ref::<etl::error::EtlError>() {
        return Some(etl_err);
    }

    // Walk through the error source chain.
    let mut source = err.source();
    while let Some(err) = source {
        if let Some(etl_err) = err.downcast_ref::<etl::error::EtlError>() {
            return Some(etl_err);
        }
        source = err.source();
    }

    None
}

/// Main async entry point that starts the replicator pipeline.
///
/// Launches the replicator with the provided configuration and captures any errors
/// to Sentry and optionally sends notifications to the Supabase API.
async fn async_main(replicator_config: ReplicatorConfig) -> anyhow::Result<()> {
    // Create an error notification client if Supabase config is provided with both API URL and API key.
    let notification_client =
        replicator_config
            .supabase
            .as_ref()
            .and_then(|supabase_config| {
                match (&supabase_config.api_url, &supabase_config.api_key) {
                    (Some(api_url), Some(api_key)) => Some(ErrorNotificationClient::new(
                        api_url.clone(),
                        api_key.expose_secret().to_string(),
                        supabase_config.project_ref.clone(),
                        replicator_config.pipeline.id.to_string(),
                    )),
                    _ => None,
                }
            });

    // We start the replicator and catch any errors.
    if let Err(err) = start_replicator_with_config(replicator_config).await {
        sentry::capture_error(&*err);
        error!("an error occurred in the replicator: {err}");

        // Send an error notification if a client is available.
        if let Some(client) = notification_client {
            // Try to extract EtlError from the error chain.
            // anyhow preserves the original error type, so downcast_ref should work
            // for the top-level error or we can search through the chain.
            if let Some(etl_err) = find_etl_error(&err) {
                client.notify_error(etl_err).await;
            }
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
            dsn: Some(sentry_config.dsn.parse()?),
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
