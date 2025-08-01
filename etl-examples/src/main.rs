/*

BigQuery Example

This example demonstrates how to use the pipeline to stream
data from PostgreSQL to BigQuery using change data capture (CDC).

Prerequisites:
1. PostgreSQL server with logical replication enabled (wal_level = logical)
2. A publication created in PostgreSQL (CREATE PUBLICATION my_publication FOR ALL TABLES;)
3. GCP service account with BigQuery Data Editor and Job User permissions
4. Service account key file downloaded from GCP console

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

The pipeline will automatically:
- Create tables in BigQuery matching your PostgreSQL schema
- Perform initial data sync for existing tables
- Stream real-time changes using logical replication
- Handle schema changes and DDL operations

*/

use clap::{Args, Parser};
use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::pipeline::Pipeline;
use etl::state::store::memory::MemoryStateStore;
use etl_destinations::bigquery::{BigQueryDestination, install_crypto_provider_for_bigquery};
use std::error::Error;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Main application arguments combining database and BigQuery configurations
#[derive(Debug, Parser)]
#[command(name = "bigquery", version, about, arg_required_else_help = true)]
struct AppArgs {
    // PostgreSQL connection parameters
    #[clap(flatten)]
    db_args: DbArgs,
    // BigQuery destination parameters
    #[clap(flatten)]
    bq_args: BqArgs,
    /// PostgreSQL publication name (must be created beforehand with CREATE PUBLICATION)
    #[arg(long)]
    publication: String,
}

// PostgreSQL database connection configuration
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

// BigQuery destination configuration
#[derive(Debug, Args)]
struct BqArgs {
    /// Path to GCP service account key JSON file (download from GCP Console > IAM & Admin > Service Accounts)
    #[arg(long)]
    bq_sa_key_file: String,
    /// BigQuery project ID (found in GCP Console project selector)
    #[arg(long)]
    bq_project_id: String,
    /// BigQuery dataset ID (must exist in the specified project)
    #[arg(long)]
    bq_dataset_id: String,
    /// Maximum batch size for processing events (higher values = better throughput, more memory usage)
    #[arg(long, default_value = "1000")]
    max_batch_size: usize,
    /// Maximum time to wait for a batch to fill in milliseconds (lower values = lower latency, less throughput)
    #[arg(long, default_value = "5000")]
    max_batch_fill_duration_ms: u64,
    /// Maximum number of concurrent table sync workers (higher values = faster initial sync, more resource usage)
    #[arg(long, default_value = "4")]
    max_table_sync_workers: u16,
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
                .unwrap_or_else(|_| "bigquery=info".into()),
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

    // Install required crypto provider for BigQuery authentication
    install_crypto_provider_for_bigquery();

    // Parse command line arguments
    let args = AppArgs::parse();

    // Configure PostgreSQL connection settings
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

    // Initialize BigQuery destination with service account authentication
    // Tables will be automatically created to match PostgreSQL schema
    let bigquery_destination = BigQueryDestination::new_with_key_path(
        args.bq_args.bq_project_id,
        args.bq_args.bq_dataset_id,
        &args.bq_args.bq_sa_key_file,
        None, // Use default max_staleness_mins (5 minutes)
    )
    .await?;

    // Create in-memory state store for tracking table replication states
    // In production, you might want to use a persistent state store like PostgresStateStore
    let state_store = MemoryStateStore::new();

    // Create pipeline configuration with batching and retry settings
    let pipeline_config = PipelineConfig {
        id: 1, // Using a simple ID for the example
        publication_name: args.publication,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: args.bq_args.max_batch_size,
            max_fill_ms: args.bq_args.max_batch_fill_duration_ms,
        },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: args.bq_args.max_table_sync_workers,
    };

    // Create the pipeline instance with all components
    // Pipeline ID (1) should match the config ID for consistency
    let mut pipeline = Pipeline::new(1, pipeline_config, state_store, bigquery_destination);

    info!(
        "Starting BigQuery CDC pipeline - connecting to PostgreSQL and initializing replication..."
    );

    // Start the pipeline - this will:
    // 1. Connect to PostgreSQL
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
