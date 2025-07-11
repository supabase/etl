/*
BigQuery Example

This example demonstrates how to use the new v2 pipeline architecture to stream
data from PostgreSQL to BigQuery using change data capture (CDC).

Key differences from the original bigquery.rs example:
1. Uses the v2::pipeline::Pipeline instead of BatchDataPipeline
2. Uses v2::destination::BigQueryDestination instead of BigQueryBatchDestination
3. Uses v2::state::store::MemoryStateStore for tracking replication state
4. Provides more granular configuration of pipeline behavior

Usage:
    cargo run --example bigquery --features bigquery \
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
use etl::v2::{
    destination::bigquery::BigQueryDestination, pipeline::Pipeline,
    state::store::memory::MemoryStateStore,
};
use tracing::error;
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

    /// Optional replication slot name (will be generated if not provided)
    #[arg(long)]
    #[allow(dead_code)] // TODO: Use this field for custom slot names
    slot_name: Option<String>,
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
    let db_args = args.db_args;
    let bq_args = args.bq_args;

    let pg_connection_config = PgConnectionConfig {
        host: db_args.db_host,
        port: db_args.db_port,
        name: db_args.db_name,
        username: db_args.db_username,
        password: db_args.db_password.map(Into::into),
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

    // Create BigQuery destination using the v2 architecture
    // This destination handles table schema creation and data insertion
    let bigquery_destination = BigQueryDestination::new_with_key_path(
        bq_args.bq_project_id,
        bq_args.bq_dataset_id,
        &bq_args.bq_sa_key_file,
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
            max_size: bq_args.max_batch_size,
            max_fill_ms: bq_args.max_batch_fill_duration_ms,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_factor: 2.0,
        },
        publication_name: args.publication,
        max_table_sync_workers: bq_args.max_table_sync_workers,
    };

    // Create the v2 pipeline with state store and destination
    let mut pipeline = Pipeline::new(1, pipeline_config, state_store, bigquery_destination);

    println!("Starting BigQuery CDC pipeline...");

    // Start the pipeline - this will:
    // 1. Connect to PostgreSQL
    // 2. Initialize table states based on the publication
    // 3. Start apply and table sync workers
    // 4. Begin streaming replication data
    pipeline.start().await?;

    println!("Pipeline started successfully. Running indefinitely...");

    // Wait for the pipeline to complete (it runs indefinitely unless shutdown)
    let result = pipeline.wait().await;

    println!("Pipeline stopped.");

    result?;

    Ok(())
}
