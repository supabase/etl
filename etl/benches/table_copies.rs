use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use config::shared::{BatchConfig, PgConnectionConfig, PipelineConfig, RetryConfig, TlsConfig};
use etl::{
    conversions::{event::Event, table_row::TableRow},
    destination::base::{Destination, DestinationError},
    pipeline::Pipeline,
    state::store::memory::MemoryStateStore,
};
use postgres::schema::{TableId, TableSchema};
use tokio::sync::Notify;

async fn start_pipeline() -> Result<(), Box<dyn Error>> {
    let pg_connection_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5431,
        name: "bench".to_string(),
        username: "raminder.singh".to_string(),
        password: None,
        tls: TlsConfig {
            trusted_root_certs: String::new(),
            enabled: false,
        },
    };

    let done = Arc::new(Notify::new());
    let destination = RowCountingDestination {
        stop_after_rows: 49_947_997,
        done: done.clone(),
        inner: Arc::new(Mutex::new(Inner { received_rows: 0 })),
    };

    let state_store = MemoryStateStore::new();

    let pipeline_config = PipelineConfig {
        id: 1,
        pg_connection: pg_connection_config,
        batch: BatchConfig {
            max_size: 100_000,
            max_fill_ms: 10_000,
        },
        apply_worker_init_retry: RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_factor: 2.0,
        },
        publication_name: "bench_pub".to_string(),
        max_table_sync_workers: 2,
    };

    let mut pipeline = Pipeline::new(1, pipeline_config, state_store, destination);
    pipeline.start().await?;

    done.notified().await;
    pipeline.shutdown_and_wait().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    start_pipeline().await
}

struct Inner {
    received_rows: usize,
}

#[derive(Clone)]
struct RowCountingDestination {
    stop_after_rows: usize,
    inner: Arc<Mutex<Inner>>,
    done: Arc<Notify>,
}

impl Destination for RowCountingDestination {
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
        let mut inner = self.inner.lock().unwrap();
        inner.received_rows += 1;
        if inner.received_rows >= self.stop_after_rows {
            self.done.notify_one();
        }
        Ok(())
    }

    async fn write_events(&self, _events: Vec<Event>) -> Result<(), DestinationError> {
        Ok(())
    }
}
