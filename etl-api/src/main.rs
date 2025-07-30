use anyhow::anyhow;
use etl_api::{config::ApiConfig, startup::Application};
use etl_config::{Environment, load_config, shared::PgConnectionConfig};
use etl_telemetry::init_tracing;
use std::env;
use std::sync::Arc;
use tracing::{error, info};

fn main() -> anyhow::Result<()> {
    // Initialize tracing from the binary name
    let _log_flusher = init_tracing(env!("CARGO_BIN_NAME"))?;

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
    if let Ok(config) = load_config::<ApiConfig>()
        && let Some(sentry_config) = &config.sentry
    {
        info!("initializing sentry with supplied dsn");

        let environment = Environment::load()?;
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(sentry_config.dsn.parse()?),
            environment: Some(environment.to_string().into()),
            traces_sample_rate: 1.0,
            max_request_body_size: sentry::MaxRequestBodySize::Always,
            integrations: vec![Arc::new(
                sentry::integrations::panic::PanicIntegration::new(),
            )],
            ..Default::default()
        });

        // Set service tag to differentiate API from other services
        sentry::configure_scope(|scope| {
            scope.set_tag("service", "api");
        });

        return Ok(Some(guard));
    }

    info!("sentry not configured for api, skipping initialization");

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
