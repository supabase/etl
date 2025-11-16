use clap::{Args, Parser};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use etl_destinations::redis::{RedisConfig, RedisDestination};
use std::error::Error;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Main application arguments combining database and Redis configurations
#[derive(Debug, Parser)]
#[command(name = "redis", version, about, arg_required_else_help = true)]
struct AppArgs {
    // Postgres connection parameters
    #[clap(flatten)]
    db_args: DbArgs,
    // Redis destination parameters
    #[clap(flatten)]
    redis_args: RedisArgs,
    /// Postgres publication name (must be created beforehand with CREATE PUBLICATION)
    #[arg(long)]
    publication: String,
}

// Postgres database connection configuration
#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running (e.g., localhost or IP address) (default: 127.0.0.1)
    #[arg(long, default_value = "127.0.0.1")]
    db_host: String,
    /// Port on which Postgres is running (default: 5432)
    #[arg(long, default_value = "5432")]
    db_port: u16,
    /// Postgres database name to connect to
    #[arg(long)]
    db_name: String,
    /// Postgres database user name (must have REPLICATION privileges)
    #[arg(long)]
    db_username: String,
    /// Postgres database user password (optional if using trust authentication)
    #[arg(long)]
    db_password: Option<String>,
}

// Redis destination configuration
#[derive(Debug, Args)]
struct RedisArgs {
    /// Host on which Redis is running (e.g., localhost or IP address) (default: 127.0.0.1)
    #[arg(long, default_value = "127.0.0.1")]
    redis_host: String,
    /// Port on which Redis is running (default: 6379)
    #[arg(long, default_value = "6379")]
    redis_port: u16,
    /// Redis database user name
    #[arg(long)]
    redis_username: Option<String>,
    /// Redis database user password (optional if using trust authentication)
    #[arg(long)]
    redis_password: Option<String>,
    /// Set a TTL (in seconds) for data replicated in Redis (optional)
    #[arg(long)]
    redis_ttl: Option<i64>,
}

impl From<RedisArgs> for RedisConfig {
    fn from(args: RedisArgs) -> Self {
        Self {
            host: args.redis_host,
            port: args.redis_port,
            username: args.redis_username,
            password: args.redis_password,
            ttl: args.redis_ttl,
        }
    }
}

// Entry point - handles error reporting and process exit
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
        std::process::exit(1);
    }

    Ok(())
}

// Initialize structured logging with configurable log levels via RUST_LOG environment variable
fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "redis=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

// Set default log level if RUST_LOG environment variable is not set
fn set_log_level() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }
}

// Main implementation function containing all the pipeline setup and execution logic
async fn main_impl() -> Result<(), Box<dyn Error>> {
    // Set up logging and tracing
    set_log_level();
    init_tracing();

    // Parse command line arguments
    let args = AppArgs::parse();

    // Configure Postgres connection settings
    // Note: TLS is disabled in this example - enable for production use
    let pg_connection_config = PgConnectionConfig {
        host: args.db_args.db_host,
        port: args.db_args.db_port,
        name: args.db_args.db_name,
        username: args.db_args.db_username,
        password: args.db_args.db_password.map(Into::into),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false, // Set to true and provide certs for production
        },
    };

    // Create in-memory store for tracking table replication states and table schemas
    // In production, you might want to use a persistent store like PostgresStore
    let store = MemoryStore::new();

    // Create pipeline configuration with batching and retry settings
    let pipeline_config = PipelineConfig {
        id: 1, // Using a simple ID for the example
        publication_name: "my_publication_accounts".to_string(),
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 5000,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
    };

    info!("Connection to Redis");
    let redis = RedisDestination::new(args.redis_args.into(), store.clone()).await?;
    // .filter(|_table_row, table_schema| table_schema.name.name == "accounts")
    // If you want to edit data before inserts/updates
    // .map_insert(|mut cell, table_schema, column_schema| {
    //     if table_schema.name.name == "accounts" && column_schema.name == "password" {
    //         // Do not save password in cache
    //         cell.clear();
    //     }

    //     cell
    // })
    // .map_update(|mut cell, table_schema, column_schema| {
    //     if table_schema.name.name == "accounts" && column_schema.name == "password" {
    //         // Do not save password in cache
    //         cell.clear();
    //     }

    //     cell
    // });
    info!("Connected to Redis");
    // Create the pipeline instance with all components
    let mut pipeline = Pipeline::new(pipeline_config, store, redis);

    info!("Starting Redis CDC pipeline - connecting to Postgres and initializing replication...");

    // Start the pipeline - this will:
    // 1. Connect to Postgres
    // 2. Initialize table states based on the publication
    // 3. Start apply and table sync workers
    // 4. Begin streaming replication data
    pipeline.start().await?;

    info!("Pipeline started successfully! Data replication is now active. Press Ctrl+C to stop.");

    // Set up signal handler for graceful shutdown on Ctrl+C
    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal, initiating graceful shutdown...");
    };

    // Wait for either the pipeline to complete naturally or receive a shutdown signal
    // The pipeline will run indefinitely unless an error occurs or it's manually stopped
    tokio::select! {
        result = pipeline.wait() => {
            info!("Pipeline completed normally (this usually indicates an error condition)");
            result?;
        }
        _ = shutdown_signal => {
            info!("Gracefully shutting down pipeline and cleaning up resources...");
        }
    }

    info!("Pipeline stopped successfully. All resources cleaned up.");

    Ok(())
}
