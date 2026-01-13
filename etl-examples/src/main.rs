//! ETL Example: BigQuery Destination
//!
//! This example demonstrates how to set up an ETL pipeline that replicates
//! data from PostgreSQL to Google BigQuery.

use clap::Parser;
use etl::destination::Destination;
use etl::error::EtlResult;
use etl::pipeline::Pipeline;
use etl::types::{Event, TableRow};
use etl_config::Environment;
use etl_config::shared::{
    BatchConfig, PgConnectionConfig, PipelineConfig, TableSyncCopyConfig, TlsConfig,
};
use etl_destinations::bigquery::{BigQueryArgs, BigQueryDestination};
use etl_postgres::types::TableId;
use etl_telemetry::tracing::init_tracing;
use std::error::Error;
use std::sync::Once;
use tracing::info;

mod store;

use store::ExampleStore;

static INIT_CRYPTO: Once = Once::new();

fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Postgres host.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Postgres port.
    #[arg(long, default_value = "5432")]
    port: u16,

    /// Database name.
    #[arg(long, default_value = "postgres")]
    database: String,

    /// Postgres username.
    #[arg(long, default_value = "postgres")]
    username: String,

    /// Postgres password.
    #[arg(long)]
    password: Option<String>,

    /// Publication name.
    #[arg(long, default_value = "example_pub")]
    publication: String,

    /// BigQuery arguments.
    #[command(flatten)]
    bq_args: BigQueryArgs,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Set development environment for pretty logging.
    Environment::Dev.set();

    // Initialize tracing.
    let _log_flusher = init_tracing("etl-example")?;

    main_impl().await
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    install_crypto_provider();

    let args = Args::parse();

    info!("starting ETL example with BigQuery destination");
    info!(
        host = %args.host,
        port = %args.port,
        database = %args.database,
        username = %args.username,
        publication = %args.publication,
        "postgres connection config"
    );

    let pg_connection_config = PgConnectionConfig {
        host: args.host,
        port: args.port,
        name: args.database,
        username: args.username,
        password: args.password.map(|p| p.into()),
        tls: TlsConfig::disabled(),
        keepalive: None,
    };

    let store = ExampleStore::new();

    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: args.publication,
        pg_connection: pg_connection_config,
        primary_connection: None,
        heartbeat: None,
        batch: BatchConfig {
            max_size: args.bq_args.max_batch_size,
            max_fill_ms: args.bq_args.max_batch_fill_duration_ms,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        table_sync_copy: TableSyncCopyConfig::default(),
    };

    let destination = BigQueryDestination::new_with_key_path(
        args.bq_args.project_id,
        args.bq_args.dataset_id,
        &args.bq_args.sa_key_file,
        args.bq_args.max_staleness_mins,
        args.bq_args.max_concurrent_streams,
        store.clone(),
    )
    .await?;

    let mut pipeline = Pipeline::new(pipeline_config, store, destination);

    info!("starting pipeline");
    pipeline.start().await?;

    info!("pipeline started, press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    info!("shutting down pipeline");
    pipeline.shutdown_and_wait().await?;

    info!("pipeline shutdown complete");
    Ok(())
}
