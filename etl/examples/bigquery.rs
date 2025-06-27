use std::{error::Error, time::Duration};

use clap::{Args, Parser, Subcommand};
use etl::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::bigquery::BigQueryBatchDestination,
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use postgres::tokio::config::PgConnectionConfig;
use postgres::{schema::TableName, tokio::config::PgTlsConfig};
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "bigquery", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,

    #[clap(flatten)]
    bq_args: BqArgs,

    #[clap(subcommand)]
    command: Command,
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

    #[arg(long)]
    max_batch_size: usize,

    #[arg(long)]
    max_batch_fill_duration_secs: u64,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy a table
    CopyTable { schema: String, name: String },

    /// Start a change data capture
    Cdc {
        publication: String,
        slot_name: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
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
        std::env::set_var("RUST_LOG", "info");
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

    let options = PgConnectionConfig {
        host: db_args.db_host,
        port: db_args.db_port,
        name: db_args.db_name,
        username: db_args.db_username,
        password: db_args.db_password.map(Into::into),
        tls_config: PgTlsConfig {
            ssl_mode: SslMode::Disable,
            trusted_root_certs: vec![],
        },
    };

    let (postgres_source, action) = match args.command {
        Command::CopyTable { schema, name } => {
            let table_names = vec![TableName { schema, name }];

            let postgres_source =
                PostgresSource::new(options, vec![], None, TableNamesFrom::Vec(table_names))
                    .await?;
            (postgres_source, PipelineAction::TableCopiesOnly)
        }
        Command::Cdc {
            publication,
            slot_name,
        } => {
            let postgres_source = PostgresSource::new(
                options,
                vec![],
                Some(slot_name),
                TableNamesFrom::Publication(publication),
            )
            .await?;

            (postgres_source, PipelineAction::Both)
        }
    };

    let bigquery_destination = BigQueryBatchDestination::new_with_key_path(
        bq_args.bq_project_id,
        bq_args.bq_dataset_id,
        &bq_args.bq_sa_key_file,
        5,
    )
    .await?;

    let batch_config = BatchConfig::new(
        bq_args.max_batch_size,
        Duration::from_secs(bq_args.max_batch_fill_duration_secs),
    );
    let mut pipeline =
        BatchDataPipeline::new(postgres_source, bigquery_destination, action, batch_config);

    pipeline.start().await?;

    Ok(())
}
