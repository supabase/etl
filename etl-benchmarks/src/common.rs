#[cfg(feature = "bigquery")]
use std::sync::Once;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use etl::{
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
    },
    error::EtlResult,
    test_utils::notifying_store::NotifyingStore,
    types::{Event, ReplicatedTableSchema, SizeHint, TableRow},
};
use etl_config::{
    Environment,
    shared::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
        PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    },
};
#[cfg(feature = "bigquery")]
use etl_destinations::bigquery::BigQueryDestination;
use etl_telemetry::tracing::{LogFlusher, init_tracing};
use serde::Serialize;
use sqlx::{Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use tokio::sync::Notify;
use tracing::info;

/// Prefix used for machine-readable benchmark result lines.
pub const BENCHMARK_RESULT_PREFIX: &str = "BENCHMARK_RESULT ";

/// Default batch fill time for benchmark runs.
pub const BENCHMARK_DEFAULT_BATCH_MAX_FILL_MS: u64 = 1_000;

/// Ensures crypto provider is only initialized once.
#[cfg(feature = "bigquery")]
static INIT_CRYPTO: Once = Once::new();

/// Where benchmark logs should be written.
#[derive(ValueEnum, Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogTarget {
    /// Send logs to terminal with colors and pretty formatting.
    Terminal,
    /// Send logs to files in the logs directory.
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

/// Destination used by benchmark runs.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationType {
    /// Use a null destination that acknowledges all writes.
    Null,
    /// Use BigQuery as the destination.
    #[value(name = "bigquery")]
    BigQuery,
}

#[cfg(test)]
mod tests {
    use clap::ValueEnum;

    use crate::common::DestinationType;

    #[test]
    fn bigquery_destination_uses_bigquery_for_cli() {
        assert_eq!(DestinationType::from_str("bigquery", false), Ok(DestinationType::BigQuery));
    }

    #[test]
    fn bigquery_destination_uses_snake_case_for_serde() {
        let value = serde_json::to_value(DestinationType::BigQuery).unwrap();
        assert_eq!(value, "big_query");
    }
}

/// Postgres connection arguments shared by benchmark commands.
#[derive(Args, Debug, Clone)]
pub struct PgConnectionArgs {
    /// Postgres host.
    #[arg(long, default_value = "localhost")]
    pub host: String,
    /// Postgres port.
    #[arg(long, default_value_t = 5432)]
    pub port: u16,
    /// Database name.
    #[arg(long, default_value = "bench")]
    pub database: String,
    /// Postgres username.
    #[arg(long, default_value = "postgres")]
    pub username: String,
    /// Postgres password.
    #[arg(long)]
    pub password: Option<String>,
    /// Enable TLS.
    #[arg(long, default_value_t = false)]
    pub tls_enabled: bool,
    /// TLS trusted root certificates.
    #[arg(long, default_value = "")]
    pub tls_certs: String,
}

/// Pipeline tuning arguments shared by benchmark commands.
#[derive(Args, Debug, Clone)]
pub struct PipelineTuningArgs {
    /// Maximum batch fill time in milliseconds.
    #[arg(long, default_value_t = BENCHMARK_DEFAULT_BATCH_MAX_FILL_MS)]
    pub batch_max_fill_ms: u64,
    /// Ratio of process memory reserved for stream batch bytes.
    #[arg(long, default_value_t = BatchConfig::DEFAULT_MEMORY_BUDGET_RATIO)]
    pub memory_budget_ratio: f32,
    /// Maximum number of table sync workers.
    #[arg(long, default_value_t = PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS)]
    pub max_table_sync_workers: u16,
    /// Maximum number of parallel copy connections per table.
    #[arg(long, default_value_t = PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE)]
    pub max_copy_connections_per_table: u16,
    /// Disable memory backpressure.
    #[arg(long, default_value_t = false)]
    pub disable_memory_backpressure: bool,
}

/// Destination arguments shared by benchmark commands.
#[derive(Args, Debug, Clone)]
pub struct DestinationArgs {
    /// Destination type to use.
    #[arg(long, value_enum, default_value = "null")]
    pub destination: DestinationType,
    /// BigQuery project ID.
    #[arg(long)]
    pub bq_project_id: Option<String>,
    /// BigQuery dataset ID.
    #[arg(long)]
    pub bq_dataset_id: Option<String>,
    /// BigQuery service account key file path.
    #[arg(long)]
    pub bq_sa_key_file: Option<String>,
    /// BigQuery maximum staleness in minutes.
    #[arg(long)]
    pub bq_max_staleness_mins: Option<u16>,
    /// BigQuery connection pool size.
    #[arg(long, default_value_t = 32)]
    pub bq_connection_pool_size: usize,
}

/// Snapshot of destination-side benchmark counters.
#[derive(Debug, Clone, Serialize)]
pub struct DestinationStatsSnapshot {
    /// Rows written through the table-copy path.
    pub table_rows: u64,
    /// Estimated bytes written through the table-copy path.
    pub table_row_bytes: u64,
    /// Number of table-row batches written.
    pub table_row_batches: u64,
    /// Row-level CDC mutation events observed.
    pub cdc_data_events: u64,
    /// Estimated bytes for row-level CDC mutation events.
    pub cdc_data_event_bytes: u64,
    /// Insert events observed.
    pub inserts: u64,
    /// Update events observed.
    pub updates: u64,
    /// Delete events observed.
    pub deletes: u64,
    /// Truncate events observed.
    pub truncates: u64,
    /// Relation events observed.
    pub relations: u64,
    /// Begin and commit events observed.
    pub transaction_events: u64,
    /// Total logical replication events observed.
    pub total_events: u64,
    /// Estimated bytes for all observed logical replication events.
    pub total_event_bytes: u64,
    /// Number of event batches written.
    pub event_batches: u64,
    /// Largest event batch observed.
    pub max_event_batch_size: u64,
}

#[derive(Debug, Default)]
struct DestinationStats {
    table_rows: AtomicU64,
    table_row_bytes: AtomicU64,
    table_row_batches: AtomicU64,
    cdc_data_events: AtomicU64,
    cdc_data_event_bytes: AtomicU64,
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    truncates: AtomicU64,
    relations: AtomicU64,
    transaction_events: AtomicU64,
    total_events: AtomicU64,
    total_event_bytes: AtomicU64,
    event_batches: AtomicU64,
    max_event_batch_size: AtomicU64,
    cdc_target: AtomicU64,
}

impl DestinationStats {
    fn snapshot(&self) -> DestinationStatsSnapshot {
        DestinationStatsSnapshot {
            table_rows: self.table_rows.load(Ordering::Relaxed),
            table_row_bytes: self.table_row_bytes.load(Ordering::Relaxed),
            table_row_batches: self.table_row_batches.load(Ordering::Relaxed),
            cdc_data_events: self.cdc_data_events.load(Ordering::Relaxed),
            cdc_data_event_bytes: self.cdc_data_event_bytes.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            truncates: self.truncates.load(Ordering::Relaxed),
            relations: self.relations.load(Ordering::Relaxed),
            transaction_events: self.transaction_events.load(Ordering::Relaxed),
            total_events: self.total_events.load(Ordering::Relaxed),
            total_event_bytes: self.total_event_bytes.load(Ordering::Relaxed),
            event_batches: self.event_batches.load(Ordering::Relaxed),
            max_event_batch_size: self.max_event_batch_size.load(Ordering::Relaxed),
        }
    }

    fn reset_cdc(&self) {
        self.cdc_data_events.store(0, Ordering::Relaxed);
        self.cdc_data_event_bytes.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.updates.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.truncates.store(0, Ordering::Relaxed);
        self.relations.store(0, Ordering::Relaxed);
        self.transaction_events.store(0, Ordering::Relaxed);
        self.total_events.store(0, Ordering::Relaxed);
        self.total_event_bytes.store(0, Ordering::Relaxed);
        self.event_batches.store(0, Ordering::Relaxed);
        self.max_event_batch_size.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct EventBatchCounts {
    data_events: u64,
    data_event_bytes: u64,
    inserts: u64,
    updates: u64,
    deletes: u64,
    truncates: u64,
    relations: u64,
    transaction_events: u64,
    total_events: u64,
    total_event_bytes: u64,
}

/// Destination that forwards writes while keeping lightweight counters.
#[derive(Clone)]
pub struct CountingDestination<D> {
    inner: D,
    stats: Arc<DestinationStats>,
    notify: Arc<Notify>,
}

impl<D> CountingDestination<D> {
    /// Wraps a destination in lightweight benchmark counters.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            stats: Arc::new(DestinationStats::default()),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Returns the current destination counter snapshot.
    pub fn stats(&self) -> DestinationStatsSnapshot {
        self.stats.snapshot()
    }

    /// Resets CDC counters without touching table-copy counters.
    pub fn reset_cdc_stats(&self) {
        self.stats.reset_cdc();
    }

    /// Sets the CDC row-event target used by [`Self::wait_for_cdc_target`].
    pub fn set_cdc_target(&self, target: u64) {
        self.stats.cdc_target.store(target, Ordering::Release);
        if self.stats.cdc_data_events.load(Ordering::Acquire) >= target {
            self.notify.notify_waiters();
        }
    }

    /// Waits until the configured CDC target has been observed.
    pub async fn wait_for_cdc_target(&self) {
        loop {
            let target = self.stats.cdc_target.load(Ordering::Acquire);
            if self.stats.cdc_data_events.load(Ordering::Acquire) >= target {
                return;
            }

            self.notify.notified().await;
        }
    }

    fn count_events(stats: &DestinationStats, counts: &EventBatchCounts) {
        if counts.total_events > 0 {
            stats.event_batches.fetch_add(1, Ordering::Relaxed);
        }
        stats.total_events.fetch_add(counts.total_events, Ordering::Relaxed);
        stats.total_event_bytes.fetch_add(counts.total_event_bytes, Ordering::Relaxed);
        stats.cdc_data_events.fetch_add(counts.data_events, Ordering::Release);
        stats.cdc_data_event_bytes.fetch_add(counts.data_event_bytes, Ordering::Relaxed);
        stats.inserts.fetch_add(counts.inserts, Ordering::Relaxed);
        stats.updates.fetch_add(counts.updates, Ordering::Relaxed);
        stats.deletes.fetch_add(counts.deletes, Ordering::Relaxed);
        stats.truncates.fetch_add(counts.truncates, Ordering::Relaxed);
        stats.relations.fetch_add(counts.relations, Ordering::Relaxed);
        stats.transaction_events.fetch_add(counts.transaction_events, Ordering::Relaxed);
        update_atomic_max(&stats.max_event_batch_size, counts.total_events);
    }
}

impl<D> Destination for CountingDestination<D>
where
    D: Destination + Clone + Send + Sync + 'static,
{
    fn name() -> &'static str {
        D::name()
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.inner.shutdown().await
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        self.inner.truncate_table(replicated_table_schema, async_result).await
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let row_count = table_rows.len() as u64;
        let row_bytes = table_rows.iter().map(SizeHint::size_hint).sum::<usize>() as u64;
        self.inner.write_table_rows(replicated_table_schema, table_rows, async_result).await?;

        self.stats.table_rows.fetch_add(row_count, Ordering::Relaxed);
        self.stats.table_row_bytes.fetch_add(row_bytes, Ordering::Relaxed);
        if row_count > 0 {
            self.stats.table_row_batches.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let counts = classify_events(&events);
        self.inner.write_events(events, async_result).await?;

        Self::count_events(&self.stats, &counts);
        let target = self.stats.cdc_target.load(Ordering::Acquire);
        if self.stats.cdc_data_events.load(Ordering::Acquire) >= target {
            self.notify.notify_waiters();
        }

        Ok(())
    }
}

/// Destination that acknowledges and discards all writes.
#[derive(Clone)]
pub struct NullDestination;

impl Destination for NullDestination {
    fn name() -> &'static str {
        "null"
    }

    async fn truncate_table(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        _table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_events(
        &self,
        _events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        async_result.send(Ok(()));
        Ok(())
    }
}

/// Benchmark destination variants.
#[expect(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum BenchDestination {
    /// Null destination variant.
    Null(CountingDestination<NullDestination>),
    /// BigQuery destination variant.
    #[cfg(feature = "bigquery")]
    BigQuery(CountingDestination<BigQueryDestination<NotifyingStore>>),
}

impl BenchDestination {
    /// Creates a benchmark destination from CLI arguments.
    pub async fn new(
        destination_args: &DestinationArgs,
        pipeline_id: u64,
        store: NotifyingStore,
    ) -> Result<Self> {
        #[cfg(not(feature = "bigquery"))]
        let _ = (pipeline_id, &store);

        match destination_args.destination {
            DestinationType::Null => Ok(Self::Null(CountingDestination::new(NullDestination))),
            #[cfg(feature = "bigquery")]
            DestinationType::BigQuery => {
                install_crypto_provider();

                let project_id = destination_args
                    .bq_project_id
                    .clone()
                    .filter(|project_id| !project_id.trim().is_empty())
                    .context("BigQuery project ID is required for BigQuery benchmarks")?;
                let dataset_id = destination_args
                    .bq_dataset_id
                    .clone()
                    .filter(|dataset_id| !dataset_id.trim().is_empty())
                    .context("BigQuery dataset ID is required for BigQuery benchmarks")?;
                let sa_key_file = destination_args
                    .bq_sa_key_file
                    .clone()
                    .filter(|sa_key_file| !sa_key_file.trim().is_empty())
                    .context("BigQuery service account key file is required")?;

                let destination = BigQueryDestination::new_with_key_path(
                    project_id,
                    dataset_id,
                    &sa_key_file,
                    destination_args.bq_max_staleness_mins,
                    destination_args.bq_connection_pool_size,
                    pipeline_id,
                    store,
                )
                .await?;

                Ok(Self::BigQuery(CountingDestination::new(destination)))
            }
            #[cfg(not(feature = "bigquery"))]
            DestinationType::BigQuery => {
                bail!("BigQuery benchmarks require the etl-benchmarks bigquery feature")
            }
        }
    }

    /// Returns destination counter snapshots.
    pub fn stats(&self) -> DestinationStatsSnapshot {
        match self {
            Self::Null(destination) => destination.stats(),
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => destination.stats(),
        }
    }

    /// Resets CDC counters.
    pub fn reset_cdc_stats(&self) {
        match self {
            Self::Null(destination) => destination.reset_cdc_stats(),
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => destination.reset_cdc_stats(),
        }
    }

    /// Sets the target number of CDC mutation events.
    pub fn set_cdc_target(&self, target: u64) {
        match self {
            Self::Null(destination) => destination.set_cdc_target(target),
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => destination.set_cdc_target(target),
        }
    }

    /// Waits for the configured CDC target.
    pub async fn wait_for_cdc_target(&self) {
        match self {
            Self::Null(destination) => destination.wait_for_cdc_target().await,
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => destination.wait_for_cdc_target().await,
        }
    }
}

impl Destination for BenchDestination {
    fn name() -> &'static str {
        "bench_destination"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        match self {
            Self::Null(destination) => destination.shutdown().await,
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => destination.shutdown().await,
        }
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        match self {
            Self::Null(destination) => {
                destination.truncate_table(replicated_table_schema, async_result).await
            }
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => {
                destination.truncate_table(replicated_table_schema, async_result).await
            }
        }
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        match self {
            Self::Null(destination) => {
                destination
                    .write_table_rows(replicated_table_schema, table_rows, async_result)
                    .await
            }
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => {
                destination
                    .write_table_rows(replicated_table_schema, table_rows, async_result)
                    .await
            }
        }
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        match self {
            Self::Null(destination) => destination.write_events(events, async_result).await,
            #[cfg(feature = "bigquery")]
            Self::BigQuery(destination) => destination.write_events(events, async_result).await,
        }
    }
}

/// Initializes benchmark logging.
pub fn init_benchmark_tracing(log_target: LogTarget, name: &str) -> Result<LogFlusher> {
    let environment: Environment = log_target.into();
    environment.set();
    Ok(init_tracing(name)?)
}

/// Builds a Postgres connection string for benchmark helpers.
pub fn connection_string(args: &PgConnectionArgs) -> String {
    let mut connection_string = match &args.password {
        Some(password) => {
            format!(
                "postgres://{}:{}@{}:{}/{}",
                args.username, password, args.host, args.port, args.database
            )
        }
        None => {
            format!("postgres://{}@{}:{}/{}", args.username, args.host, args.port, args.database)
        }
    };

    if args.tls_enabled {
        connection_string.push_str("?sslmode=require");
    } else {
        connection_string.push_str("?sslmode=disable");
    }

    connection_string
}

/// Opens a Postgres pool for benchmark helpers.
pub async fn pg_pool(args: &PgConnectionArgs) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(16)
        .connect(&connection_string(args))
        .await
        .context("failed to connect to Postgres")
}

/// Applies ETL source-side migrations required by replication triggers/state.
pub async fn run_etl_migrations(args: &PgConnectionArgs) -> Result<()> {
    let mut connection = PgConnection::connect(&connection_string(args))
        .await
        .context("failed to connect to Postgres for ETL migrations")?;

    connection
        .execute("set client_min_messages = warning;")
        .await
        .context("failed to set migration client message level")?;
    connection
        .execute("create schema if not exists etl;")
        .await
        .context("failed to create etl schema")?;
    connection
        .execute("set search_path = 'etl';")
        .await
        .context("failed to set migration search path")?;

    sqlx::migrate!("../etl/migrations")
        .run_direct(&mut connection)
        .await
        .context("failed to run ETL migrations")?;

    Ok(())
}

/// Builds an ETL pipeline config for a benchmark.
pub fn pipeline_config(
    pipeline_id: u64,
    publication_name: String,
    pg: &PgConnectionArgs,
    tuning: &PipelineTuningArgs,
    table_sync_copy: TableSyncCopyConfig,
) -> PipelineConfig {
    PipelineConfig {
        id: pipeline_id,
        publication_name,
        pg_connection: PgConnectionConfig {
            host: pg.host.clone(),
            port: pg.port,
            name: pg.database.clone(),
            username: pg.username.clone(),
            password: pg.password.clone().map(Into::into),
            tls: TlsConfig { trusted_root_certs: pg.tls_certs.clone(), enabled: pg.tls_enabled },
            keepalive: TcpKeepaliveConfig::default(),
        },
        batch: BatchConfig {
            max_fill_ms: tuning.batch_max_fill_ms,
            memory_budget_ratio: tuning.memory_budget_ratio,
        },
        table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
        table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
        max_table_sync_workers: tuning.max_table_sync_workers,
        memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
        memory_backpressure: if tuning.disable_memory_backpressure {
            None
        } else {
            Some(MemoryBackpressureConfig::default())
        },
        table_sync_copy,
        invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
        max_copy_connections_per_table: tuning.max_copy_connections_per_table,
    }
}

/// Drops replication slots created by a benchmark pipeline.
pub async fn cleanup_replication_slots(
    pg: &PgConnectionArgs,
    pipeline_id: u64,
    table_ids: &[u32],
) -> Result<()> {
    let pool = pg_pool(pg).await?;

    drop_replication_slot(&pool, &format!("supabase_etl_apply_{pipeline_id}")).await;
    for table_id in table_ids {
        drop_replication_slot(&pool, &format!("supabase_etl_table_sync_{pipeline_id}_{table_id}"))
            .await;
    }

    pool.close().await;
    Ok(())
}

/// Prints a machine-readable benchmark report.
pub fn print_report<T>(report: &T) -> Result<()>
where
    T: Serialize,
{
    let json = serde_json::to_string(report)?;
    println!("{BENCHMARK_RESULT_PREFIX}{json}");
    Ok(())
}

/// Returns duration as milliseconds.
pub fn duration_millis(duration: Duration) -> u128 {
    duration.as_millis()
}

/// Calculates per-second throughput.
pub fn per_second(count: u64, duration: Duration) -> f64 {
    let seconds = duration.as_secs_f64();
    if seconds == 0.0 {
        return 0.0;
    }

    count as f64 / seconds
}

/// Converts bytes to mebibytes.
pub fn bytes_to_mib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

/// Calculates estimated mebibytes per second.
pub fn mib_per_second(bytes: u64, duration: Duration) -> f64 {
    let seconds = duration.as_secs_f64();
    if seconds == 0.0 {
        return 0.0;
    }

    bytes_to_mib(bytes) / seconds
}

/// Quotes a simple SQL identifier.
pub fn quote_identifier(identifier: &str) -> Result<String> {
    if identifier.is_empty() {
        bail!("identifier cannot be empty");
    }

    if !identifier.chars().all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        bail!("identifier '{identifier}' contains unsupported characters");
    }

    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

/// Quotes a schema-qualified table name.
pub fn quote_qualified_table_name(table_name: &str) -> Result<String> {
    table_name
        .split('.')
        .map(quote_identifier)
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("."))
}

/// Splits a schema-qualified table name into schema and table components.
pub fn split_table_name(table_name: &str) -> Result<(String, String)> {
    let parts = table_name.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => Ok(("public".to_owned(), (*table).to_owned())),
        [schema, table] => Ok(((*schema).to_owned(), (*table).to_owned())),
        _ => bail!("table name must be either table or schema.table"),
    }
}

fn classify_events(events: &[Event]) -> EventBatchCounts {
    let mut counts = EventBatchCounts { total_events: events.len() as u64, ..Default::default() };
    for event in events {
        let event_bytes = event.size_hint() as u64;
        counts.total_event_bytes += event_bytes;
        match event {
            Event::Insert(_) => {
                counts.inserts += 1;
                counts.data_events += 1;
                counts.data_event_bytes += event_bytes;
            }
            Event::Update(_) => {
                counts.updates += 1;
                counts.data_events += 1;
                counts.data_event_bytes += event_bytes;
            }
            Event::Delete(_) => {
                counts.deletes += 1;
                counts.data_events += 1;
                counts.data_event_bytes += event_bytes;
            }
            Event::Truncate(_) => {
                counts.truncates += 1;
            }
            Event::Relation(_) => {
                counts.relations += 1;
            }
            Event::Begin(_) | Event::Commit(_) => {
                counts.transaction_events += 1;
            }
            Event::Unsupported => {}
        }
    }

    counts
}

fn update_atomic_max(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

async fn drop_replication_slot(pool: &PgPool, slot_name: &str) {
    info!(slot_name, "dropping replication slot");
    let _ = sqlx::query(
        "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = $1",
    )
    .bind(slot_name)
    .execute(pool)
    .await;
}

#[cfg(feature = "bigquery")]
fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}
