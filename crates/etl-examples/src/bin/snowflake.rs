/// Snowflake Example
///
/// Streams Postgres CDC to Snowflake using Snowpipe Streaming.
///
/// Prerequisites:
/// 1. Postgres with logical replication enabled (wal_level = logical)
/// 2. A publication (e.g. `cargo x seed` to create test tables + publication)
/// 3. Snowflake account with key-pair authentication configured
/// 4. TESTS_SNOWFLAKE_CONNECTION set with Snowflake key-pair credentials
///
/// Usage:
///     source .env
///     cargo run --bin snowflake -p etl-examples --features snowflake -- \
///         --db-host localhost \
///         --db-port 5430 \
///         --db-name etl_testdata \
///         --db-username postgres \
///         --db-password postgres \
///         --publication seed_pub
use std::{error::Error, sync::Arc, sync::Once};

use clap::{Args, Parser};
use etl::{
    config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    },
    pipeline::Pipeline,
    store::PostgresStore,
};
use etl_destinations::snowflake::{AuthManager, Client, Config, Destination};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Ensures crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

/// Installs the default cryptographic provider for rustls.
fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

// Main application arguments combining database and Snowflake configurations
#[derive(Debug, Parser)]
#[command(name = "snowflake", version, about, arg_required_else_help = true)]
struct AppArgs {
    // Postgres connection parameters
    #[clap(flatten)]
    db_args: DbArgs,
    // Snowflake destination parameters
    #[clap(flatten)]
    sf_args: SnowflakeArgs,
    /// Postgres publication name (must be created beforehand with CREATE
    /// PUBLICATION)
    #[arg(long)]
    publication: String,
}

// Postgres database connection configuration
#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running (e.g., localhost or IP address)
    #[arg(long)]
    db_host: String,
    /// Port on which Postgres is running (default: 5432)
    #[arg(long)]
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

// Snowflake pipeline tuning.
#[derive(Debug, Args)]
struct SnowflakeArgs {
    /// Maximum time to wait for a batch to fill in milliseconds (lower values =
    /// lower latency, less throughput)
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    /// Maximum number of concurrent table sync workers (higher values = faster
    /// initial sync, more resource usage)
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
}

// Entry point - handles error reporting and process exit
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(err) = main_impl().await {
        error!(error = %err, "fatal error");
        std::process::exit(1);
    }

    Ok(())
}

// Initialize structured logging with configurable log levels via RUST_LOG
// environment variable
fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "snowflake=info".into()),
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

// Main implementation function containing all the pipeline setup and execution
// logic
async fn main_impl() -> Result<(), Box<dyn Error>> {
    // Set up logging and tracing
    set_log_level();
    init_tracing();

    // Install required crypto provider for authentication
    install_crypto_provider();

    // Parse command line arguments
    let args = AppArgs::parse();

    // Configure Postgres connection settings
    // Note: TLS is disabled in this example - enable for production use
    let pg_connection_config = PgConnectionConfig {
        host: args.db_args.db_host,
        hostaddr: None,
        port: args.db_args.db_port,
        name: args.db_args.db_name,
        username: args.db_args.db_username,
        password: args.db_args.db_password.map(Into::into),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false, // Set to true and provide certs for production
        },
        keepalive: TcpKeepaliveConfig::default(),
    };

    // Create a persistent store for tracking table states and
    // schemas. This runs the Postgres store migrations; Pipeline::start()
    // runs the source migrations required by replication.
    let pipeline_id = 1;
    let store = PostgresStore::new(pipeline_id, pg_connection_config.clone()).await?;

    // Create pipeline configuration with batching and retry settings
    let pipeline_config = PipelineConfig {
        id: pipeline_id, // Using a simple ID for the example
        publication_name: args.publication,
        pg_connection: pg_connection_config,
        store_pg_connection: None,
        batch: BatchConfig {
            max_fill_ms: args.sf_args.max_batch_fill_duration_ms,
            memory_budget_ratio: 0.2,
            max_bytes: 8 * 1024 * 1024,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: args.sf_args.max_table_sync_workers,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    let sf_config = Config::require_tests_env()?;

    // Build the auth manager using key-pair authentication
    let auth = Arc::new(AuthManager::new(sf_config)?);

    // Initialize Snowflake destination -- tables will be automatically created
    // to match the Postgres schema
    let client = Client::new(Arc::clone(&auth), pipeline_id);
    let destination = Destination::new(client, store.clone());

    // Create the pipeline instance with all components
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);

    info!(
        "Starting Snowflake CDC pipeline - connecting to Postgres and initializing replication..."
    );

    // Start the pipeline - this will:
    // 1. Connect to Postgres
    // 2. Initialize table states based on the publication
    // 3. Start apply and table sync workers
    // 4. Begin streaming replication data
    pipeline.start().await?;

    info!("pipeline started, data replication is now active, press ctrl+c to stop");

    // Set up signal handler for graceful shutdown on Ctrl+C
    let shutdown_signal = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
        info!("received ctrl+c signal, initiating graceful shutdown");
    };

    // Wait for either the pipeline to complete naturally or receive a shutdown
    // signal. The pipeline will run indefinitely unless an error occurs or it's
    // manually stopped.
    tokio::select! {
        result = pipeline.wait() => {
            info!("pipeline completed normally (this usually indicates an error condition)");
            result?;
        }
        _ = shutdown_signal => {
            info!("gracefully shutting down pipeline and cleaning up resources");
        }
    }

    info!("pipeline stopped, all resources cleaned up");

    Ok(())
}
