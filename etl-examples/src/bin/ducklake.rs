/*

DuckLake Example

This example demonstrates how to use the pipeline to replicate data from
Postgres to a DuckLake data lake using change data capture (CDC).

DuckLake separates storage into two components:
  - Catalog: metadata (tables, snapshots, stats) stored in a PostgreSQL database
  - Data:    row data written as Parquet files to a local directory or cloud storage

Each batch of rows is committed as a single Parquet snapshot so the lake stays
consistent and queryable at all times.

Prerequisites:
1. Postgres server with logical replication enabled (wal_level = logical)
2. A publication created in Postgres:
     CREATE PUBLICATION my_pub FOR ALL TABLES;
3. A separate PostgreSQL database to act as the DuckLake catalog:
     CREATE DATABASE ducklake_catalog;
4. A local data directory or an S3/GCS/Azure bucket for Parquet files

Usage (local data):
    cargo run --bin ducklake -p etl-examples -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name mydb \
        --db-username postgres \
        --db-password mypassword \
        --catalog-url postgres://user:pass@localhost:5432/ducklake_catalog \
        --data-path ./lake_data/ \
        --publication my_pub

Usage (S3-compatible storage):
    cargo run --bin ducklake -p etl-examples -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name mydb \
        --db-username postgres \
        --db-password mypassword \
        --catalog-url postgres://user:pass@localhost:5432/ducklake_catalog \
        --data-path s3://my-bucket/lake/ \
        --publication my_pub \
        --s3-access-key-id AKIAIOSFODNN7EXAMPLE \
        --s3-secret-access-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
        --s3-region us-east-1

The pipeline will automatically:
- Create DuckLake tables matching your Postgres schema
- Perform an initial bulk copy of every table in the publication
- Stream real-time INSERT / UPDATE / DELETE changes via logical replication
- Name tables by combining schema and table: public.orders → public_orders

*/

use clap::{Args, Parser};
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;
use etl_destinations::ducklake::{DuckLakeDestination, S3Config};
use std::error::Error;
use std::sync::Once;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Ensures the crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

/// Installs the default cryptographic provider for rustls.
fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

/// Replicates a Postgres publication into a DuckLake data lake.
#[derive(Debug, Parser)]
#[command(name = "ducklake", version, about, arg_required_else_help = true)]
struct AppArgs {
    /// Postgres connection parameters
    #[clap(flatten)]
    db_args: DbArgs,
    /// DuckLake destination parameters
    #[clap(flatten)]
    ducklake_args: DuckLakeArgs,
    /// Postgres publication name (must be created beforehand with CREATE PUBLICATION)
    #[arg(long)]
    publication: String,
}

/// Postgres database connection configuration
#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running (e.g., localhost or IP address)
    #[arg(long)]
    db_host: String,
    /// Port on which Postgres is running
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

/// DuckLake destination configuration
#[derive(Debug, Args)]
struct DuckLakeArgs {
    /// PostgreSQL connection string for the DuckLake catalog database
    /// (e.g., postgres://user:pass@localhost:5432/ducklake_catalog)
    #[arg(long)]
    catalog_url: String,
    /// Where to store Parquet files: a local directory (./lake_data/) or a
    /// cloud URI (s3://bucket/prefix/, gs://bucket/prefix/, az://container/prefix/)
    #[arg(long)]
    data_path: String,
    /// DuckDB connection pool size
    #[arg(long, default_value = "4")]
    pool_size: u32,
    /// Maximum time to wait for a batch to fill in milliseconds
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    /// Maximum number of concurrent table sync workers during initial copy
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
    /// Postgres schema for DuckLake metadata tables (e.g., ducklake).
    /// Uses the catalog's default schema when not set.
    #[arg(long)]
    metadata_schema: Option<String>,
    /// S3 access key ID (required for private S3-compatible buckets)
    #[arg(long)]
    s3_access_key_id: Option<String>,
    /// S3 secret access key
    #[arg(long)]
    s3_secret_access_key: Option<String>,
    /// S3 region (e.g., us-east-1)
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
    /// Custom S3 endpoint, e.g. 127.0.0.1:5000/s3 for Supabase Storage
    #[arg(long)]
    s3_endpoint: Option<String>,
    /// S3 URL style: path (MinIO/Supabase) or vhost (AWS S3)
    #[arg(long, default_value = "path")]
    s3_url_style: String,
    /// Enable TLS for the S3 connection
    #[arg(long, default_value = "false")]
    s3_use_ssl: bool,
}

impl DuckLakeArgs {
    /// Builds an [`S3Config`] if S3 credentials were provided.
    fn s3_config(&self) -> Option<S3Config> {
        let access_key_id = self.s3_access_key_id.clone()?;
        let secret_access_key = self.s3_secret_access_key.clone().unwrap_or_default();
        Some(S3Config {
            access_key_id,
            secret_access_key,
            region: self.s3_region.clone(),
            endpoint: self.s3_endpoint.clone(),
            url_style: self.s3_url_style.clone(),
            use_ssl: self.s3_use_ssl,
        })
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

/// Initialize structured logging with configurable log levels via RUST_LOG.
fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ducklake=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn set_log_level() {
    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }
}

/// Main implementation — sets up and runs the ETL pipeline.
async fn main_impl() -> Result<(), Box<dyn Error>> {
    set_log_level();
    init_tracing();
    install_crypto_provider();

    let args = AppArgs::parse();

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

    // Use an in-memory store for tracking replication state.
    // For persistent state across restarts, swap this for a PostgresStore.
    let store = MemoryStore::new();

    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: args.publication,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_fill_ms: args.ducklake_args.max_batch_fill_duration_ms,
            memory_budget_ratio: BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO,
        },
        table_error_retry_delay_ms: 10_000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: args.ducklake_args.max_table_sync_workers,
        memory_refresh_interval_ms: 100,
        memory_backpressure: Some(MemoryBackpressureConfig::default()),
        table_sync_copy: TableSyncCopyConfig::default(),
        invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
    };

    let s3_config = args.ducklake_args.s3_config();
    let destination = DuckLakeDestination::new(
        args.ducklake_args.catalog_url,
        args.ducklake_args.data_path,
        args.ducklake_args.pool_size,
        s3_config,
        args.ducklake_args.metadata_schema,
        store.clone(),
    )?;

    let mut pipeline = Pipeline::new(pipeline_config, store, destination);

    info!("Starting DuckLake CDC pipeline — connecting to Postgres and initializing replication...");

    pipeline.start().await?;

    info!("Pipeline started, data replication is now active. Press Ctrl+C to stop.");

    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal, initiating graceful shutdown...");
    };

    tokio::select! {
        result = pipeline.wait() => {
            info!("Pipeline completed (this usually indicates an error condition).");
            result?;
        }
        _ = shutdown_signal => {
            info!("Gracefully shutting down pipeline and cleaning up resources.");
        }
    }

    info!("Pipeline stopped, all resources cleaned up.");

    Ok(())
}
