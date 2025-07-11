/*
BigQuery Example

This example demonstrates how to use the pipeline to stream
data from PostgreSQL to BigQuery using change data capture (CDC).

Usage:
    cargo run --example bigquery --features bigquery -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name mydb \
        --db-username postgres \
        --db-password mypassword \
        --bq-sa-key-file /path/to/service-account-key.json \
        --bq-project-id my-gcp-project \
        --bq-dataset-id my_dataset \
        --publication my_publication
*/

use std::error::Error;

use clap::{Args, Parser};
use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig, TlsConfig};
use etl::{
    destination::bigquery::BigQueryDestination, pipeline::Pipeline,
    state::store::memory::MemoryStateStore,
};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "bigquery", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,

    #[clap(flatten)]
    bq_args: BqArgs,

    /// PostgreSQL publication name
    #[arg(long)]
    publication: String,
}

#[derive(Debug, Args)]
struct DbArgs {
    /// Host on which Postgres is running
    #[arg(long)]
    db_host: String,

    /// Port on which Postgres is running
    #[arg(long)]
    db_port: u16,

    /// Postgres database name
    #[arg(long)]
    db_name: String,

    /// Postgres database user name
    #[arg(long)]
    db_username: String,

    /// Postgres database user password
    #[arg(long)]
    db_password: Option<String>,
}

#[derive(Debug, Args)]
struct BqArgs {
    /// Path to GCP's service account key to access BigQuery
    #[arg(long)]
    bq_sa_key_file: String,

    /// BigQuery project id
    #[arg(long)]
    bq_project_id: String,

    /// BigQuery dataset id
    #[arg(long)]
    bq_dataset_id: String,

    /// Maximum batch size for processing events
    #[arg(long, default_value = "1000")]
    max_batch_size: usize,

    /// Maximum time to wait for a batch to fill (in milliseconds)
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,

    /// Maximum number of table sync workers
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
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
                .unwrap_or_else(|_| "bigquery=info".into()),
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

async fn main_impl() -> Result<(), Box<dyn Error>> {
    set_log_level();
    init_tracing();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

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
    };

    let bigquery_destination = BigQueryDestination::new_with_key_path(
        args.bq_args.bq_project_id,
        args.bq_args.bq_dataset_id,
        &args.bq_args.bq_sa_key_file,
        None, // Use default max_staleness_mins
    )
    .await?;

    // Create in-memory state store for tracking table replication states
    // In production, you might want to use a persistent state store like PostgresStateStore
    let state_store = MemoryStateStore::new();

    // Create pipeline configuration with all necessary settings
    let pipeline_config = PipelineConfig {
        id: 1, // Using a simple ID for the example
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: args.bq_args.max_batch_size,
            max_fill_ms: args.bq_args.max_batch_fill_duration_ms,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_factor: 2.0,
        },
        publication_name: args.publication,
        max_table_sync_workers: args.bq_args.max_table_sync_workers,
    };

    // Create the pipeline with state store and destination
    let mut pipeline = Pipeline::new(1, pipeline_config, state_store, bigquery_destination);

    info!("starting BigQuery CDC pipeline...");

    // Start the pipeline - this will:
    // 1. Connect to PostgreSQL
    // 2. Initialize table states based on the publication
    // 3. Start apply and table sync workers
    // 4. Begin streaming replication data
    pipeline.start().await?;

    info!("pipeline started successfully, press Ctrl+C to stop");

    // Set up signal handler for graceful shutdown
    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("received Ctrl+C signal, initiating graceful shutdown...");
    };

    // Wait for either the pipeline to complete or a shutdown signal
    tokio::select! {
        result = pipeline.wait() => {
            info!("pipeline completed normally");
            result?;
        }
        _ = shutdown_signal => {
            info!("shutting down pipeline...");
        }
    }

    info!("pipeline stopped.");

    Ok(())
}
