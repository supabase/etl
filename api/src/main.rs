use std::env;

use anyhow::anyhow;
use api::{config::ApiConfig, startup::Application};
use config::{Environment, load_config, shared::PgConnectionConfig};
use telemetry::init_tracing;
use tracing::{error, info};

fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");

    // We pass emit_on_span_close = true to emit logs on span close
    // for the api because it is a web server, and we need to emit logs
    // for every closing request. This is a bit of a hack, but it works
    // for now. Ideally the tracing middleware should emit a log on
    // request end, but it doesn't do that yet.
    let _log_flusher = init_tracing(app_name, true)?;

    // Initialize Sentry before the async runtime starts
    let _sentry_guard = init_sentry()?;

    // We start the runtime.
    actix_web::rt::System::new().block_on(async_main())?;

    Ok(())
}

async fn async_main() -> anyhow::Result<()> {
    let mut args = env::args();
    match args.len() {
        // Run the application server
        1 => {
            let config = load_config::<ApiConfig>()?;
            log_pg_connection_config(&config.database);
            let application = Application::build(config.clone()).await?;
            application.run_until_stopped().await?;
        }
        // Handle single command commands
        2 => {
            let command = args.nth(1).unwrap();
            match command.as_str() {
                "migrate" => {
                    let config = load_config::<PgConnectionConfig>()?;
                    log_pg_connection_config(&config);
                    Application::migrate_database(config).await?;
                    info!("database migrated successfully");
                }
                _ => {
                    let message = format!("invalid command: {command}");
                    error!("{message}");
                    return Err(anyhow!(message));
                }
            }
        }
        _ => {
            let message = "invalid number of command line arguments";
            error!("{message}");
            return Err(anyhow!(message));
        }
    }

    Ok(())
}

fn init_sentry() -> anyhow::Result<Option<sentry::ClientInitGuard>> {
    if let Ok(config) = load_config::<ApiConfig>() {
        if let Some(sentry_config) = &config.sentry {
            info!("Initializing Sentry with DSN");

            let environment = Environment::load()?;
            let guard = sentry::init(sentry::ClientOptions {
                dsn: Some(sentry_config.dsn.parse()?),
                environment: Some(environment.to_string().into()),
                traces_sample_rate: 1.0,
                send_default_pii: false,
                max_request_body_size: sentry::MaxRequestBodySize::Always,
                ..Default::default()
            });

            return Ok(Some(guard));
        }
    }

    info!("Sentry not configured, skipping initialization");
    Ok(None)
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
