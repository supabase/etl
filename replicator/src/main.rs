use ::config::Environment;
use std::backtrace::Backtrace;
use std::panic;
use std::sync::Arc;
use telemetry::init_tracing;
use thiserror::__private::AsDynError;
use tracing::{error, info};

use crate::config::load_replicator_config;
use crate::core::start_replicator;

mod config;
mod core;
mod migrations;

fn main() -> anyhow::Result<()> {
    // We want to crash on any panic happening in the code. It's important to have this at the beginning
    // so that any other panic hooks are chained to be executed before this.
    panic::set_hook(Box::new(|info| {
        let stacktrace = Backtrace::force_capture();
        println!("Got panic. @info:{info}\n@stackTrace:{stacktrace}");
        std::process::abort();
    }));

    // Initialize tracing from the binary name
    let _log_flusher = init_tracing(env!("CARGO_BIN_NAME"))?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = init_sentry()?;

    // We start the runtime.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main())?;

    Ok(())
}

async fn async_main() -> anyhow::Result<()> {
    // We start the replicator and catch any errors.
    if let Err(err) = start_replicator().await {
        sentry::capture_error(err.as_dyn_error());
        error!("an error occurred in the replicator: {err}");

        return Err(err);
    }

    Ok(())
}

/// Initializes Sentry with replicator-specific configuration.
///
/// Loads the configuration and initializes Sentry if a DSN is provided.
/// Tags all errors and transactions with the "replicator" service identifier.
/// Configures panic handling to automatically capture panics and send them to Sentry.
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

        // Set service tag to differentiate replicator from other services
        sentry::configure_scope(|scope| {
            scope.set_tag("service", "replicator");
        });

        return Ok(Some(guard));
    }

    info!("sentry not configured for replicator, skipping initialization");

    Ok(None)
}
