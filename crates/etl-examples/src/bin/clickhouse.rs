/*

ClickHouse Example

This example demonstrates how to use the pipeline to stream
data from Postgres to ClickHouse using change data capture (CDC).

Two table-engine layouts are supported, selected via `--clickhouse-engine`:

- `replacing_merge_tree` (default): each replicated table becomes a
  `ReplacingMergeTree` keyed on the source primary key, with trailing
  `_etl_version` (UInt128 packed `(commit_lsn, tx_ordinal)`) and
  `_etl_deleted` (tombstone) columns. A companion `<table>__current` view
  reads current state via `FINAL` and filters tombstones. Requires
  ClickHouse >= 23.5 and a primary key on the source.
- `merge_tree`: append-only event-log layout with `cdc_operation` and
  `cdc_lsn` columns appended to every row. Works for PK-less source tables.

Table names are derived from the Postgres schema and table name using
double-underscore escaping (e.g. `public.orders` -> `public_orders`).

Prerequisites:
1. Postgres server with logical replication enabled (wal_level = logical).
2. The `seed_pub` publication created by `cargo x seed`.
3. A running ClickHouse instance accessible over HTTP(S).
   `ReplacingMergeTree` also requires ClickHouse 23.5 or newer.

Usage after `source .env`, `cargo x init`, and `cargo x seed`:
    cargo run -p etl-examples --bin clickhouse --features clickhouse -- \
        --db-host "$TESTS_DATABASE_HOST" \
        --db-port "$TESTS_DATABASE_PORT" \
        --db-name etl_testdata \
        --db-username "$TESTS_DATABASE_USERNAME" \
        --publication seed_pub

For HTTPS connections, provide an `https://` URL. TLS uses webpki root
certificates automatically. Set `TESTS_CLICKHOUSE_PASSWORD` when ClickHouse
requires authentication.

*/

use std::{error::Error, sync::Once};

use clap::{Args, Parser};
use etl::{
    config::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    },
    pipeline::Pipeline,
    store::PostgresStore,
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::{
    ClickHouseClientConfig, ClickHouseDestination, ClickHouseInserterConfig,
};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

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
    clickhouse_args: ClickHouseArgs,
    /// Postgres publication name (must be created beforehand with CREATE
    /// PUBLICATION)
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
    /// Postgres database user password (optional with trust authentication).
    #[arg(long, env = "TESTS_DATABASE_PASSWORD", hide_env_values = true)]
    db_password: Option<String>,
}

/// ClickHouse destination configuration.
#[derive(Debug, Args)]
struct ClickHouseArgs {
    /// ClickHouse HTTP(S) endpoint.
    #[arg(long, env = "TESTS_CLICKHOUSE_URL")]
    clickhouse_url: String,
    /// ClickHouse user name.
    #[arg(long, env = "TESTS_CLICKHOUSE_USER")]
    clickhouse_user: String,
    /// ClickHouse user password (optional).
    #[arg(long, env = "TESTS_CLICKHOUSE_PASSWORD", hide_env_values = true)]
    clickhouse_password: Option<String>,
    /// ClickHouse target database.
    #[arg(long, env = "TESTS_CLICKHOUSE_DATABASE", default_value = "default")]
    clickhouse_database: String,
    /// Table engine used for replicated tables. `replacing_merge_tree` is the
    /// default and requires a source primary key and CH >= 23.5; `merge_tree`
    /// gives the append-only event-log layout.
    #[arg(long, value_enum, default_value_t = ClickHouseEngineArg::ReplacingMergeTree)]
    clickhouse_engine: ClickHouseEngineArg,
    /// Maximum time to wait for a batch to fill in milliseconds (lower values =
    /// lower latency, less throughput)
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    /// Maximum number of concurrent table sync workers (higher values = faster
    /// initial sync, more resource usage)
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
}

/// CLI-facing engine choice. Converts to `ClickHouseEngine` via `From`.
#[derive(Debug, Copy, Clone, clap::ValueEnum)]
#[clap(rename_all = "snake_case")]
enum ClickHouseEngineArg {
    MergeTree,
    ReplacingMergeTree,
}

impl From<ClickHouseEngineArg> for ClickHouseEngine {
    fn from(arg: ClickHouseEngineArg) -> Self {
        match arg {
            ClickHouseEngineArg::MergeTree => ClickHouseEngine::MergeTree,
            ClickHouseEngineArg::ReplacingMergeTree => ClickHouseEngine::ReplacingMergeTree,
        }
    }
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

/// Initialize structured logging with configurable log levels via RUST_LOG
/// environment variable.
fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "clickhouse=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Main implementation containing all pipeline setup and execution logic.
async fn main_impl() -> Result<(), Box<dyn Error>> {
    init_tracing();

    // Install required crypto provider for TLS (used when clickhouse_url is https://)
    install_crypto_provider();

    let args = AppArgs::parse();

    // Configure Postgres connection settings
    // Note: TLS is disabled in this example — enable for production use
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

    let pipeline_config = PipelineConfig {
        id: pipeline_id,
        publication_name: args.publication,
        run_source_migrations: true,
        pg_connection: pg_connection_config,
        store_pg_connection: None,
        batch: BatchConfig {
            max_fill_ms: args.clickhouse_args.max_batch_fill_duration_ms,
            memory_budget_ratio: 0.2,
            max_bytes: 8 * 1024 * 1024,
        },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: args.clickhouse_args.max_table_sync_workers,
        memory_refresh_interval_ms: 100,
        replication_lag_refresh_interval_ms: 10_000,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: 2,
    };

    let clickhouse_destination = ClickHouseDestination::new(
        Url::parse(&args.clickhouse_args.clickhouse_url)?,
        args.clickhouse_args.clickhouse_user,
        args.clickhouse_args.clickhouse_password,
        args.clickhouse_args.clickhouse_database,
        ClickHouseInserterConfig {
            engine: args.clickhouse_args.clickhouse_engine.into(),
            ..Default::default()
        },
        ClickHouseClientConfig::default(),
        store.clone(),
    )?;
    clickhouse_destination.validate_engine_support().await?;

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
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
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
