use anyhow::{Context, anyhow};
use etl_api::{config::ApiConfig, startup::Application};
use etl_config::{load_config, shared::PgConnectionConfig};
use etl_telemetry::tracing::init_tracing;
use std::env;
use std::sync::Once;
use tracing::{error, info};

/// Ensures crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

mod sentry;

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

/// Entry point for the ETL API service.
///
/// Initializes tracing, Sentry, and starts the Actix web server with command-line
/// argument handling for both server mode and database migration.
fn main() -> anyhow::Result<()> {
    // Install rustls crypto provider before any TLS operations.
    // This is needed because Cargo's feature unification causes rustls to have both
    // ring and aws-lc-rs features enabled, and we must explicitly choose which to use.
    install_crypto_provider();

    // Initialize tracing from the binary name
    let _log_flusher = init_tracing(env!("CARGO_BIN_NAME"))?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = sentry::init()?;

    // We start the runtime.
    actix_web::rt::System::new().block_on(async_main())?;

    Ok(())
}

/// Main async function that handles command-line arguments and starts the service.
///
/// Supports two modes: server mode (no arguments) and migration mode ("migrate" argument).
async fn async_main() -> anyhow::Result<()> {
    let mut args = env::args();
    match args.len() {
        // Run the application server
        1 => {
            let config = load_config::<ApiConfig>()
                .context("loading API configuration for server startup")?;
            log_pg_connection_config(&config.database);
            let application = Application::build(config.clone()).await?;
            application.run_until_stopped().await?;
        }
        // Handle single command commands
        2 => {
            let command = args.nth(1).unwrap();
            match command.as_str() {
                "migrate" => {
                    let config = load_config::<PgConnectionConfig>()
                        .context("loading database configuration for migrations")?;
                    log_pg_connection_config(&config);
                    Application::migrate_database(config).await?;
                    info!("database migrated successfully");
                }
                _ => {
                    error!(%command, "invalid command");
                    return Err(anyhow!("invalid command: {command}"));
                }
            }
        }
        _ => {
            error!("invalid number of command line arguments");
            return Err(anyhow!("invalid number of command line arguments"));
        }
    }

    Ok(())
}

fn log_pg_connection_config(config: &PgConnectionConfig) {
    info!(
        host = config.host,
        port = config.port,
        dbname = config.name,
        username = config.username,
        tls_enabled = config.tls.enabled,
        "pg database options",
    );
}
