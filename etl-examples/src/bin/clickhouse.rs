/*

ClickHouse Example

This example demonstrates how to use the pipeline to stream
data from Postgres to ClickHouse using change data capture (CDC).

Each Postgres table is replicated as an append-only MergeTree table.
Two CDC metadata columns are appended to every row:
  - `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`
  - `cdc_lsn`: the Postgres LSN at the time of the change

Table names are derived from the Postgres schema and table name using
double-underscore escaping (e.g. `public.orders` → `public__orders`).

Prerequisites:
1. Postgres server with logical replication enabled (wal_level = logical)
2. A publication created in Postgres (CREATE PUBLICATION my_pub FOR ALL TABLES;)
3. A running ClickHouse instance accessible over HTTP(S)

Usage:
    cargo run -p etl-examples --bin clickhouse -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name postgres \
        --db-username postgres \
        --db-password password \
        --ch-url http://localhost:8123 \
        --ch-user default \
        --ch-database default \
        --publication my_pub

For HTTPS connections, provide an `https://` URL — TLS is handled automatically
using webpki root certificates. Use `--ch-password` if your ClickHouse instance
requires authentication.

*/

use clap::{Args, Parser};
use etl::concurrency::memory_monitor::MemorySnapshot;
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use etl_destinations::clickhouse::{ClickHouseDestination, ClickHouseInserterConfig};
use std::error::Error;
use std::sync::Once;
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

/// Main application arguments combining database and ClickHouse configurations.
#[derive(Debug, Parser)]
#[command(name = "clickhouse", version, about, arg_required_else_help = true)]
struct AppArgs {
    /// Postgres connection parameters
    #[clap(flatten)]
    db_args: DbArgs,
    /// ClickHouse destination parameters
    #[clap(flatten)]
    ch_args: ChArgs,
    /// Postgres publication name (must be created beforehand with CREATE PUBLICATION)
    #[arg(long)]
    publication: String,
}

/// Postgres database connection configuration.
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

/// ClickHouse destination configuration.
#[derive(Debug, Args)]
struct ChArgs {
    /// ClickHouse HTTP(S) endpoint (e.g. http://localhost:8123 or https://host:8443)
    #[arg(long)]
    ch_url: String,
    /// ClickHouse user name
    #[arg(long)]
    ch_user: String,
    /// ClickHouse user password (optional)
    #[arg(long)]
    ch_password: Option<String>,
    /// ClickHouse target database
    #[arg(long)]
    ch_database: String,
    /// Maximum time to wait for a batch to fill in milliseconds (lower values = lower latency, less throughput)
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    /// Maximum number of concurrent table sync workers (higher values = faster initial sync, more resource usage)
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
}

/// Entry point — handles error reporting and process exit.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
        std::process::exit(1);
    }

    Ok(())
}

/// Initialize structured logging with configurable log levels via RUST_LOG environment variable.
fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "clickhouse=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Set default log level if RUST_LOG environment variable is not set.
fn set_log_level() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }
}

/// Main implementation containing all pipeline setup and execution logic.
async fn main_impl() -> Result<(), Box<dyn Error>> {
    set_log_level();
    init_tracing();

    // Install required crypto provider for TLS (used when ch_url is https://)
    install_crypto_provider();

    let args = AppArgs::parse();

    // Configure Postgres connection settings
    // Note: TLS is disabled in this example — enable for production use
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
        keepalive: TcpKeepaliveConfig::default(),
    };

    // Create in-memory store for tracking table replication states and schemas.
    // In production, you might want to use a persistent store like PostgresStore.
    let store = MemoryStore::new();

    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: args.publication,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_fill_ms: args.ch_args.max_batch_fill_duration_ms,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: args.ch_args.max_table_sync_workers,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    // Compute max_bytes_per_insert using the same formula as BatchBudget::ideal_batch_size_bytes:
    //   total_memory * memory_budget_ratio / max_table_sync_workers
    let max_bytes_per_insert = {
        let total_memory = MemorySnapshot::from_system(&mut sysinfo::System::new()).total();
        let budget = (total_memory as f64 * f64::from(BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO))
            as u64;
        (budget / u64::from(args.ch_args.max_table_sync_workers)).max(1)
    };

    // Initialize the ClickHouse destination.
    // Tables are created automatically as append-only MergeTree tables.
    let clickhouse_destination = ClickHouseDestination::new(
        args.ch_args.ch_url,
        args.ch_args.ch_user,
        args.ch_args.ch_password,
        args.ch_args.ch_database,
        ClickHouseInserterConfig { max_bytes_per_insert },
        store.clone(),
    )?;

    let mut pipeline = Pipeline::new(pipeline_config, store, clickhouse_destination);

    info!(
        "Starting ClickHouse CDC pipeline - connecting to Postgres and initializing replication..."
    );

    // Start the pipeline — this will:
    // 1. Connect to Postgres
    // 2. Initialize table states based on the publication
    // 3. Start apply and table sync workers
    // 4. Begin streaming replication data
    pipeline.start().await?;

    info!("pipeline started, data replication is now active, press ctrl+c to stop");

    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("received ctrl+c signal, initiating graceful shutdown");
    };

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
