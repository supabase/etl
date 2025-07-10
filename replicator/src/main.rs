use ::config::Environment;
use telemetry::init_tracing;
use tracing::info;

use crate::config::load_replicator_config;
use crate::core::start_replicator;

mod config;
mod core;
mod migrations;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");

    // Initialize Sentry with replicator-specific tag
    let _sentry_guard = init_sentry()?;

    // We pass `emit_on_span_close = false` to avoid emitting logs on span close
    // for replicator because it is not a web server, and we don't need to emit logs
    // for every closing span.
    let _log_flusher = init_tracing(app_name, false)?;

    // We start the replicator.
    start_replicator().await?;

    Ok(())
}

/// Initializes Sentry with replicator-specific configuration.
///
/// Loads the configuration and initializes Sentry if a DSN is provided.
/// Tags all errors and transactions with the "replicator" service identifier.
fn init_sentry() -> anyhow::Result<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_replicator_config() {
        if let Some(sentry_config) = &config.sentry {
            info!("Initializing Sentry for replicator with DSN");

            let environment = Environment::load()?;
            let guard = sentry::init(sentry::ClientOptions {
                dsn: Some(sentry_config.dsn.parse()?),
                environment: Some(environment.to_string().into()),
                ..Default::default()
            });

            // Set service tag to differentiate replicator from other services
            sentry::configure_scope(|scope| {
                scope.set_tag("service", "replicator");
            });

            return Ok(Some(guard));
        }
    }

    info!("Sentry not configured for replicator, skipping initialization");

    Ok(None)
}
