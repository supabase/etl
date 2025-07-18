use std::error::Error;

use clap::{Parser, Subcommand};
use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig, TlsConfig};
use etl::{
    conversions::{event::Event, table_row::TableRow},
    destination::base::{Destination, DestinationError},
    pipeline::Pipeline,
    state::{store::notify::NotifyingStateStore, table::TableReplicationPhaseType},
};
use postgres::schema::{TableId, TableSchema};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
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
    },
    /// Prepare the benchmark environment (placeholder)
    Prepare,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

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
            })
            .await
        }
        Commands::Prepare => {
            println!("Prepare command - placeholder for future implementation");
            Ok(())
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

    let destination = NullDestination;

    let state_store = NotifyingStateStore::new();

    let table_ids = [
        50504, 50509, 50514, 50522, 50527, 50532, 50538, 50543, 50548,
    ];

    let mut table_copied_notifications = vec![];
    for table_id in table_ids {
        let table_copied = state_store
            .notify_on_table_state(table_id, TableReplicationPhaseType::FinishedCopy)
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

impl Destination for NullDestination {
    async fn write_table_schema(&self, table_schema: TableSchema) -> Result<(), DestinationError> {
        println!("Got table {} schema", table_schema.name);
        Ok(())
    }

    async fn load_table_schemas(&self) -> Result<Vec<TableSchema>, DestinationError> {
        Ok(vec![])
    }

    async fn write_table_rows(
        &self,
        _table_id: TableId,
        _table_rows: Vec<TableRow>,
    ) -> Result<(), DestinationError> {
        Ok(())
    }

    async fn write_events(&self, _events: Vec<Event>) -> Result<(), DestinationError> {
        Ok(())
    }
}
