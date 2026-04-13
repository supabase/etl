/*

DuckLake Example

This example demonstrates how to use the pipeline to stream
data from Postgres to DuckLake using change data capture (CDC).

DuckLake separates storage into two components:
- Catalog: metadata stored in a PostgreSQL database
- Data: row data written as Parquet files (local directory or S3 / S3-compatible object storage)

Prerequisites:
1. Postgres server with logical replication enabled (wal_level = logical)
2. A publication created in Postgres (CREATE PUBLICATION my_pub FOR ALL TABLES;)
3. A PostgreSQL database for the DuckLake catalog
4. A local directory or S3 / S3-compatible bucket for Parquet files

Usage:
    cargo run --bin ducklake -p etl-examples -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name mydb \
        --db-username postgres \
        --db-password mypassword \
        --catalog-url postgres://user:pass@localhost:5432/ducklake_catalog \
        --data-path file:///absolute/path/to/lake_data \
        --publication my_pub

Plain local paths such as `./lake_data/` are also accepted and normalized to
absolute `file://` URLs before the destination is created.

*/

use clap::{Args, Parser};
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use etl_config::parse_ducklake_url;
use etl_destinations::ducklake::{DuckLakeDestination, S3Config};
use std::error::Error;
use std::sync::Once;
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

#[derive(Debug, Parser)]
#[command(name = "ducklake", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,
    #[clap(flatten)]
    ducklake_args: DuckLakeArgs,
    /// Postgres publication name (must be created beforehand with CREATE PUBLICATION)
    #[arg(long)]
    publication: String,
}

#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running (e.g., localhost or IP address)
    #[arg(long)]
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

#[derive(Debug, Args)]
struct DuckLakeArgs {
    /// DuckLake catalog URL (e.g., postgres://user:pass@host/db or file:///tmp/catalog.ducklake)
    ///
    /// Plain local paths are accepted and converted to absolute `file://` URLs.
    #[arg(long, value_parser = parse_ducklake_url)]
    catalog_url: Url,
    /// Local directory or S3 / S3-compatible URI for Parquet files (e.g., file:///tmp/lake_data or s3://bucket/)
    ///
    /// Plain local paths are accepted and converted to absolute `file://` URLs.
    #[arg(long, value_parser = parse_ducklake_url)]
    data_path: Url,
    /// DuckDB connection pool size
    #[arg(long, default_value = "4")]
    pool_size: u32,
    /// Maximum time to wait for a batch to fill in milliseconds
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    /// Maximum number of concurrent table sync workers during initial copy
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,

    // S3 / S3-compatible storage credentials (required when --data-path is an s3:// URI)
    /// S3 access key ID
    #[arg(long, requires = "s3_secret_access_key")]
    s3_access_key_id: Option<String>,
    /// S3 secret access key
    #[arg(long, requires = "s3_access_key_id")]
    s3_secret_access_key: Option<String>,
    /// S3 region (default: us-east-1)
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
    /// S3-compatible endpoint, e.g. `127.0.0.1:5000/s3` for Supabase Storage
    #[arg(long)]
    s3_endpoint: Option<String>,
    /// S3 URL style: `path` (for MinIO / Supabase Storage) or `vhost` (AWS default)
    #[arg(long, default_value = "path")]
    s3_url_style: String,
    /// Use SSL/TLS for the S3 connection (disable for local S3-compatible services)
    #[arg(long, default_value = "false")]
    s3_use_ssl: bool,

    /// Postgres schema used for DuckLake metadata tables (e.g. `ducklake`)
    #[arg(long)]
    metadata_schema: Option<String>,

    /// Shared DuckDB log storage path used by `CALL enable_logging(storage_path = ...)`.
    #[arg(long, requires = "duckdb_log_dump_path")]
    duckdb_log_storage_path: Option<String>,
    /// CSV file written from `duckdb_logs` during graceful shutdown.
    #[arg(long, requires = "duckdb_log_storage_path")]
    duckdb_log_dump_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
        std::process::exit(1);
    }

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ducklake=info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_line_number(true)
                .with_file(true),
        )
        .init();
}

fn set_log_level() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    set_log_level();
    init_tracing();
    etl_telemetry::metrics::init_metrics(None, None)?;
    install_crypto_provider();

    let args = AppArgs::parse();
    let pipeline_id = 1;

    let pg_connection_config = PgConnectionConfig {
        host: args.db_args.db_host,
        port: args.db_args.db_port,
        name: args.db_args.db_name,
        username: args.db_args.db_username,
        password: args.db_args.db_password.map(Into::into),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
        keepalive: TcpKeepaliveConfig::default(),
    };

    let store = PostgresStore::new(pipeline_id, pg_connection_config.clone()).await?;

    let pipeline_config = PipelineConfig {
        id: pipeline_id,
        publication_name: args.publication,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_fill_ms: args.ducklake_args.max_batch_fill_duration_ms,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: args.ducklake_args.max_table_sync_workers,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    let s3_config = args.ducklake_args.s3_access_key_id.map(|key_id| S3Config {
        access_key_id: key_id,
        secret_access_key: args.ducklake_args.s3_secret_access_key.unwrap(),
        region: args.ducklake_args.s3_region,
        endpoint: args.ducklake_args.s3_endpoint,
        url_style: args.ducklake_args.s3_url_style,
        use_ssl: args.ducklake_args.s3_use_ssl,
    });

    let ducklake_destination = DuckLakeDestination::new(
        args.ducklake_args.catalog_url,
        args.ducklake_args.data_path,
        args.ducklake_args.pool_size,
        s3_config,
        args.ducklake_args.metadata_schema,
        store.clone(),
    )
    .await?;

    let mut pipeline = Pipeline::new(pipeline_config, store, ducklake_destination);

    info!(
        "Starting DuckLake CDC pipeline - connecting to Postgres and initializing replication..."
    );

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
