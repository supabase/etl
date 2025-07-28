/*
Table Copies Benchmark

This benchmark allows testing ETL pipeline performance with different destinations.

Usage Examples:

1. Run with null destination (fastest, data is discarded):
   cargo bench --bench table_copies -- run \
     --host localhost --port 5432 --database bench \
     --username postgres --password mypass \
     --publication-name bench_pub \
     --table-ids 1,2,3 \
     --destination null

2. Run with BigQuery destination (requires BigQuery feature):
   cargo bench --bench table_copies --features bigquery -- run \
     --host localhost --port 5432 --database bench \
     --username postgres --password mypass \
     --publication-name bench_pub \
     --table-ids 1,2,3 \
     --destination big-query \
     --bq-project-id my-gcp-project \
     --bq-dataset-id my_dataset \
     --bq-sa-key-file /path/to/service-account-key.json

3. Prepare benchmark environment (clean up replication slots):
   cargo bench --bench table_copies -- prepare \
     --host localhost --port 5432 --database bench \
     --username postgres --password mypass
*/

use std::error::Error;

use clap::{Parser, Subcommand, ValueEnum};
use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig, TlsConfig};
use etl::{
    conversions::{event::Event, table_row::TableRow},
    destination::base::Destination,
    pipeline::Pipeline,
    state::{store::notify::NotifyingStateStore, table::TableReplicationPhaseType},
};
use postgres::schema::{TableId, TableSchema};
use sqlx::postgres::PgPool;

#[cfg(feature = "bigquery")]
use etl::destination::bigquery::BigQueryDestination;
use etl::error::EtlResult;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(ValueEnum, Debug, Clone)]
enum DestinationType {
    /// Use a null destination that discards all data (fastest)
    Null,
    /// Use BigQuery as the destination
    #[cfg(feature = "bigquery")]
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
        #[cfg(feature = "bigquery")]
        #[arg(long)]
        bq_project_id: Option<String>,

        /// BigQuery dataset ID (required when using BigQuery destination)
        #[cfg(feature = "bigquery")]
        #[arg(long)]
        bq_dataset_id: Option<String>,

        /// BigQuery service account key file path (required when using BigQuery destination)
        #[cfg(feature = "bigquery")]
        #[arg(long)]
        bq_sa_key_file: Option<String>,

        /// BigQuery maximum staleness in minutes (optional)
        #[cfg(feature = "bigquery")]
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

        /// TLS trusted root certificates
        #[arg(long, default_value = "")]
        tls_certs: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Filter out the --bench argument that cargo might add
    let args: Vec<String> = std::env::args().filter(|arg| arg != "--bench").collect();

    let args = Args::parse_from(args);

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
            #[cfg(feature = "bigquery")]
            bq_project_id,
            #[cfg(feature = "bigquery")]
            bq_dataset_id,
            #[cfg(feature = "bigquery")]
            bq_sa_key_file,
            #[cfg(feature = "bigquery")]
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
                #[cfg(feature = "bigquery")]
                bq_project_id,
                #[cfg(feature = "bigquery")]
                bq_dataset_id,
                #[cfg(feature = "bigquery")]
                bq_sa_key_file,
                #[cfg(feature = "bigquery")]
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
            tls_certs,
        } => {
            prepare_benchmark(PrepareArgs {
                host,
                port,
                database,
                username,
                password,
                tls_enabled,
                tls_certs,
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
    #[cfg(feature = "bigquery")]
    bq_project_id: Option<String>,
    #[cfg(feature = "bigquery")]
    bq_dataset_id: Option<String>,
    #[cfg(feature = "bigquery")]
    bq_sa_key_file: Option<String>,
    #[cfg(feature = "bigquery")]
    bq_max_staleness_mins: Option<u16>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct PrepareArgs {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: Option<String>,
    tls_enabled: bool,
    tls_certs: String,
}

async fn prepare_benchmark(args: PrepareArgs) -> Result<(), Box<dyn Error>> {
    println!("Preparing benchmark environment...");

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

    println!("Connecting to database at {}:{}", args.host, args.port);

    // Connect to the database
    let pool = PgPool::connect(&connection_string).await?;

    println!("Cleaning up existing replication slots...");

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

    println!("Replication slots cleanup completed successfully!");

    // Close the connection
    pool.close().await;

    Ok(())
}

async fn start_pipeline(args: RunArgs) -> Result<(), Box<dyn Error>> {
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

    // Create the appropriate destination based on the argument
    let destination = match args.destination {
        DestinationType::Null => BenchDestination::Null(NullDestination),
        #[cfg(feature = "bigquery")]
        DestinationType::BigQuery => {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("failed to install default crypto provider");
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
            )
            .await?;

            BenchDestination::BigQuery(bigquery_dest)
        }
    };

    let state_store = NotifyingStateStore::new();

    let mut table_copied_notifications = vec![];
    for table_id in &args.table_ids {
        let table_copied = state_store
            .notify_on_table_state(
                TableId::new(*table_id),
                TableReplicationPhaseType::FinishedCopy,
            )
            .await;
        table_copied_notifications.push(table_copied);
    }

    let pipeline_config = PipelineConfig {
        id: 1,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: args.batch_max_size,
            max_fill_ms: args.batch_max_fill_ms,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_factor: 2.0,
        },
        publication_name: args.publication_name,
        max_table_sync_workers: args.max_table_sync_workers,
    };

    let mut pipeline = Pipeline::new(1, pipeline_config, state_store, destination);
    pipeline.start().await?;

    for notification in table_copied_notifications {
        notification.notified().await;
    }

    pipeline.shutdown_and_wait().await?;

    Ok(())
}

#[derive(Clone)]
struct NullDestination;

#[derive(Clone)]
enum BenchDestination {
    Null(NullDestination),
    #[cfg(feature = "bigquery")]
    BigQuery(BigQueryDestination),
}

impl Destination for BenchDestination {
    async fn inject(&self, schema_cache: etl::schema::cache::SchemaCache) -> EtlResult<()> {
        match self {
            BenchDestination::Null(dest) => dest.inject(schema_cache).await,
            #[cfg(feature = "bigquery")]
            BenchDestination::BigQuery(dest) => dest.inject(schema_cache).await,
        }
    }

    async fn write_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        match self {
            BenchDestination::Null(dest) => dest.write_table_schema(table_schema).await,
            #[cfg(feature = "bigquery")]
            BenchDestination::BigQuery(dest) => dest.write_table_schema(table_schema).await,
        }
    }

    async fn load_table_schemas(&self) -> EtlResult<Vec<TableSchema>> {
        match self {
            BenchDestination::Null(dest) => dest.load_table_schemas().await,
            #[cfg(feature = "bigquery")]
            BenchDestination::BigQuery(dest) => dest.load_table_schemas().await,
        }
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        match self {
            BenchDestination::Null(dest) => dest.write_table_rows(table_id, table_rows).await,
            #[cfg(feature = "bigquery")]
            BenchDestination::BigQuery(dest) => dest.write_table_rows(table_id, table_rows).await,
        }
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        match self {
            BenchDestination::Null(dest) => dest.write_events(events).await,
            #[cfg(feature = "bigquery")]
            BenchDestination::BigQuery(dest) => dest.write_events(events).await,
        }
    }
}

impl Destination for NullDestination {
    async fn inject(&self, _schema_cache: etl::schema::cache::SchemaCache) -> EtlResult<()> {
        Ok(())
    }

    async fn write_table_schema(&self, _table_schema: TableSchema) -> EtlResult<()> {
        Ok(())
    }

    async fn load_table_schemas(&self) -> EtlResult<Vec<TableSchema>> {
        Ok(vec![])
    }

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
