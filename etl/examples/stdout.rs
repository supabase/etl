use std::{error::Error, time::Duration};

use clap::{Args, Parser, Subcommand};
use etl::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::stdout::StdoutDestination,
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use postgres::schema::TableName;
use postgres::tokio::config::PgConnectionConfig;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "stdout", version, about, arg_required_else_help = true)]
struct AppArgs {
    #[clap(flatten)]
    db_args: DbArgs,

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
                .unwrap_or_else(|_| "stdout=info".into()),
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
    let args = AppArgs::parse();
    let db_args = args.db_args;

    let options = PgConnectionConfig {
        host: db_args.db_host,
        port: db_args.db_port,
        name: db_args.db_name,
        username: db_args.db_username,
        password: db_args.db_password.map(Into::into),
        ssl_mode: SslMode::Disable,
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

    let stdout_destination = StdoutDestination;

    let batch_config = BatchConfig::new(1000, Duration::from_secs(10));
    let mut pipeline =
        BatchDataPipeline::new(postgres_source, stdout_destination, action, batch_config);

    pipeline.start().await?;

    Ok(())
}
