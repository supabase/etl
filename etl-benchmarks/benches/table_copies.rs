use clap::{Parser, Subcommand, ValueEnum};
use etl::destination::Destination;
use etl::error::EtlResult;
use etl::pipeline::Pipeline;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::notify::NotifyingStore;
use etl::types::{Event, TableRow};
use etl_config::Environment;
use etl_config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl_destinations::bigquery::{BigQueryDestination, install_crypto_provider_for_bigquery};
use etl_postgres::schema::TableId;
use etl_telemetry::init_tracing;
use sqlx::postgres::PgPool;
use std::error::Error;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Where to send log output
    #[arg(
        long = "log-target",
        value_enum,
        default_value = "terminal",
        global = true
    )]
    log_target: LogTarget,

    #[command(subcommand)]
    command: Commands,
}

#[derive(ValueEnum, Debug, Clone)]
enum LogTarget {
    /// Send logs to terminal with colors and pretty formatting
    Terminal,
    /// Send logs to files in 'logs/' directory
    File,
}

impl From<LogTarget> for Environment {
    fn from(log_target: LogTarget) -> Self {
        match log_target {
            LogTarget::Terminal => Environment::Dev,
            LogTarget::File => Environment::Prod,
        }
    }
}

#[derive(ValueEnum, Debug, Clone)]
enum DestinationType {
    /// Use a null destination that discards all data (fastest)
    Null,
    /// Use BigQuery as the destination
    BigQuery,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the table copies benchmark
    Run {
        /// PostgreSQL host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// PostgreSQL port
        #[arg(long, default_value = "5432")]
        port: u16,

        /// Database name
        #[arg(long, default_value = "bench")]
        database: String,

        /// PostgreSQL username
        #[arg(long, default_value = "postgres")]
        username: String,

        /// PostgreSQL password (optional)
        #[arg(long)]
        password: Option<String>,

        /// Enable TLS
        #[arg(long, default_value = "false")]
        tls_enabled: bool,

        /// TLS trusted root certificates
        #[arg(long, default_value = "")]
        tls_certs: String,

        /// Publication name
        #[arg(long, default_value = "bench_pub")]
        publication_name: String,

        /// Maximum batch size
        #[arg(long, default_value = "100000")]
        batch_max_size: usize,

        /// Maximum batch fill time in milliseconds
        #[arg(long, default_value = "10000")]
        batch_max_fill_ms: u64,

        /// Maximum number of table sync workers
        #[arg(long, default_value = "8")]
        max_table_sync_workers: u16,

        /// Table IDs to replicate (comma-separated)
        #[arg(long, value_delimiter = ',')]
        table_ids: Vec<u32>,

        /// Destination type to use
        #[arg(long, value_enum, default_value = "null")]
        destination: DestinationType,

        /// BigQuery project ID (required when using BigQuery destination)

        #[arg(long)]
        bq_project_id: Option<String>,

        /// BigQuery dataset ID (required when using BigQuery destination)

        #[arg(long)]
        bq_dataset_id: Option<String>,

        /// BigQuery service account key file path (required when using BigQuery destination)

        #[arg(long)]
        bq_sa_key_file: Option<String>,

        /// BigQuery maximum staleness in minutes (optional)

        #[arg(long)]
        bq_max_staleness_mins: Option<u16>,
    },
    /// Prepare the benchmark environment by cleaning up replication slots
    Prepare {
        /// PostgreSQL host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// PostgreSQL port
        #[arg(long, default_value = "5432")]
        port: u16,

        /// Database name
        #[arg(long, default_value = "bench")]
        database: String,

        /// PostgreSQL username
        #[arg(long, default_value = "postgres")]
        username: String,

        /// PostgreSQL password (optional)
        #[arg(long)]
        password: Option<String>,

        /// Enable TLS
        #[arg(long, default_value = "false")]
        tls_enabled: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Filter out the --bench argument that cargo might add
    let args: Vec<String> = std::env::args().filter(|arg| arg != "--bench").collect();

    let args = Args::parse_from(args);

    // Set the environment based on the log target argument
    let environment: Environment = args.log_target.into();
    environment.set();

    // Initialize tracing with the selected environment
    let _log_flusher = init_tracing("table_copies")?;

    match args.command {
        Commands::Run {
            host,
            port,
            database,
            username,
            password,
            tls_enabled,
            tls_certs,
            publication_name,
            batch_max_size,
            batch_max_fill_ms,
            max_table_sync_workers,
            table_ids,
            destination,

            bq_project_id,

            bq_dataset_id,

            bq_sa_key_file,

            bq_max_staleness_mins,
        } => {
            start_pipeline(RunArgs {
                host,
                port,
                database,
                username,
                password,
                tls_enabled,
                tls_certs,
                publication_name,
                batch_max_size,
                batch_max_fill_ms,
                max_table_sync_workers,
                table_ids,
                destination,

                bq_project_id,

                bq_dataset_id,

                bq_sa_key_file,

                bq_max_staleness_mins,
            })
            .await
        }
        Commands::Prepare {
            host,
            port,
            database,
            username,
            password,
            tls_enabled,
        } => {
            prepare_benchmark(PrepareArgs {
                host,
                port,
                database,
                username,
                password,
                tls_enabled,
            })
            .await
        }
    }
}

#[derive(Debug)]
struct RunArgs {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    tls_enabled: bool,
    tls_certs: String,
    publication_name: String,
    batch_max_size: usize,
    batch_max_fill_ms: u64,
    max_table_sync_workers: u16,
    table_ids: Vec<u32>,
    destination: DestinationType,

    bq_project_id: Option<String>,

    bq_dataset_id: Option<String>,

    bq_sa_key_file: Option<String>,

    bq_max_staleness_mins: Option<u16>,
}

#[derive(Debug)]
struct PrepareArgs {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    tls_enabled: bool,
}

async fn prepare_benchmark(args: PrepareArgs) -> Result<(), Box<dyn Error>> {
    info!("Preparing benchmark environment...");

    // Build connection string
    let mut connection_string = format!(
        "postgres://{}@{}:{}/{}",
        args.username, args.host, args.port, args.database
    );

    if let Some(password) = &args.password {
        connection_string = format!(
            "postgres://{}:{}@{}:{}/{}",
            args.username, password, args.host, args.port, args.database
        );
    }

    // Add SSL mode based on TLS settings
    if args.tls_enabled {
        connection_string.push_str("?sslmode=require");
    } else {
        connection_string.push_str("?sslmode=disable");
    }

    info!("Connecting to database at {}:{}", args.host, args.port);

    // Connect to the database
    let pool = PgPool::connect(&connection_string).await?;

    info!("Cleaning up existing replication slots...");

    // Execute the cleanup SQL
    let cleanup_sql = r#"
        do $$
        declare
            slot record;
        begin
            for slot in (select slot_name from pg_replication_slots where slot_name like 'supabase_etl_%')
            loop
                execute 'select pg_drop_replication_slot(' || quote_literal(slot.slot_name) || ')';
            end loop;
        end $$;
    "#;

    sqlx::query(cleanup_sql).execute(&pool).await?;

    info!("Replication slots cleanup completed successfully!");

    // Close the connection
    pool.close().await;

    Ok(())
}

async fn start_pipeline(args: RunArgs) -> Result<(), Box<dyn Error>> {
    info!("Starting ETL pipeline benchmark");
    info!(
        "Database: {}@{}:{}/{}",
        args.username, args.host, args.port, args.database
    );
    info!("Table IDs: {:?}", args.table_ids);
    info!("Destination: {:?}", args.destination);

    let pg_connection_config = PgConnectionConfig {
        host: args.host,
        port: args.port,
        name: args.database,
        username: args.username,
        password: args.password.map(|p| p.into()),
        tls: TlsConfig {
            trusted_root_certs: args.tls_certs,
            enabled: args.tls_enabled,
        },
    };

    let store = NotifyingStore::new();

    // Create the appropriate destination based on the argument
    let destination = match args.destination {
        DestinationType::Null => BenchDestination::Null(NullDestination),

        DestinationType::BigQuery => {
            install_crypto_provider_for_bigquery();

            let project_id = args
                .bq_project_id
                .ok_or("BigQuery project ID is required when using BigQuery destination")?;
            let dataset_id = args
                .bq_dataset_id
                .ok_or("BigQuery dataset ID is required when using BigQuery destination")?;
            let sa_key_file = args.bq_sa_key_file.ok_or(
                "BigQuery service account key file is required when using BigQuery destination",
            )?;

            let bigquery_dest = BigQueryDestination::new_with_key_path(
                project_id,
                dataset_id,
                &sa_key_file,
                args.bq_max_staleness_mins,
                store.clone(),
            )
            .await?;

            BenchDestination::BigQuery(bigquery_dest)
        }
    };

    let mut table_copied_notifications = vec![];
    for table_id in &args.table_ids {
        let table_copied = store
            .notify_on_table_state(
                TableId::new(*table_id),
                TableReplicationPhaseType::FinishedCopy,
            )
            .await;
        table_copied_notifications.push(table_copied);
    }

    let pipeline_config = PipelineConfig {
        id: 1,
        publication_name: args.publication_name,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: args.batch_max_size,
            max_fill_ms: args.batch_max_fill_ms,
        },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: args.max_table_sync_workers,
    };

    let mut pipeline = Pipeline::new(1, pipeline_config, store, destination);
    info!("Starting pipeline...");
    pipeline.start().await?;

    info!(
        "Waiting for all {} tables to complete copy phase...",
        args.table_ids.len()
    );
    for notification in table_copied_notifications {
        notification.notified().await;
    }
    info!("All tables completed copy phase");

    info!("Shutting down pipeline...");
    pipeline.shutdown_and_wait().await?;
    info!("ETL pipeline benchmark completed successfully");

    Ok(())
}

#[derive(Clone)]
struct NullDestination;

#[derive(Clone)]
enum BenchDestination {
    Null(NullDestination),
    BigQuery(BigQueryDestination<NotifyingStore>),
}

impl Destination for BenchDestination {
    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        match self {
            BenchDestination::Null(dest) => dest.write_table_rows(table_id, table_rows).await,

            BenchDestination::BigQuery(dest) => dest.write_table_rows(table_id, table_rows).await,
        }
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        match self {
            BenchDestination::Null(dest) => dest.write_events(events).await,

            BenchDestination::BigQuery(dest) => dest.write_events(events).await,
        }
    }
}

impl Destination for NullDestination {
    async fn write_table_rows(
        &self,
        _table_id: TableId,
        _table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        Ok(())
    }

    async fn write_events(&self, _events: Vec<Event>) -> EtlResult<()> {
        Ok(())
    }
}
