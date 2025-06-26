use std::env;

use anyhow::anyhow;
use api::{config::ApiConfig, startup::Application};
use config::load_config;
use postgres::sqlx::config::PgConnectionConfig;
use telemetry::init_tracing;
use tracing::{error, info};

#[actix_web::main]
pub async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");

    // We pass emit_on_span_close = true to emit logs on span close
    // for the api because it is a web server, and we need to emit logs
    // for every closing request. This is a bit of a hack, but it works
    // for now. Ideally the tracing middleware should emit a log on
    // request end, but it doesn't do that yet.
    let _log_flusher = init_tracing(app_name, true)?;

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

fn log_pg_connection_config(config: &PgConnectionConfig) {
    info!(
        host = config.host,
        port = config.port,
        dbname = config.name,
        username = config.username,
        ssl_mode = config.tls_config.ssl_mode.to_string(),
        "pg database options",
    );
}
