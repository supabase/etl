use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use clap::{Parser, Subcommand};
use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig, TlsConfig};
use etl::{
    conversions::{event::Event, table_row::TableRow},
    destination::base::{Destination, DestinationError},
    pipeline::Pipeline,
    state::{store::notify::NotifyingStateStore, table::TableReplicationPhaseType},
};
use postgres::schema::{TableId, TableSchema};
use sqlx::postgres::PgPool;

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

        /// Table IDs to replicate (comma-separated)
        #[arg(long, value_delimiter = ',')]
        table_ids: Vec<u32>,
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

    let destination = NullDestination {
        rows_received: Arc::new(Mutex::new(0)),
    };

    let state_store = NotifyingStateStore::new();

    let mut table_copied_notifications = vec![];
    for table_id in &args.table_ids {
        let table_copied = state_store
            .notify_on_table_state(*table_id, TableReplicationPhaseType::FinishedCopy)
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
struct NullDestination {
    rows_received: Arc<Mutex<usize>>,
}

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
        table_rows: Vec<TableRow>,
    ) -> Result<(), DestinationError> {
        // println!("Got {} rows", table_rows.len());
        let mut rows_received = self.rows_received.lock().unwrap();
        *rows_received += table_rows.len();
        if *rows_received % 1000 == 0 {
            // println!("Got 100,000 rows");
        }
        println!("Total rows: {}", *rows_received);
        Ok(())
    }

    async fn write_events(&self, _events: Vec<Event>) -> Result<(), DestinationError> {
        Ok(())
    }
}
