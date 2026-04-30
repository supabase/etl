use std::{
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use etl::{
    pipeline::Pipeline, state::table::TableReplicationPhaseType,
    test_utils::notifying_store::NotifyingStore,
};
use etl_config::shared::TableSyncCopyConfig;
use etl_postgres::types::TableId;
use serde::Serialize;
use sqlx::PgPool;
use tokio::task::JoinSet;
use tracing::info;

use crate::common::{
    BenchDestination, DestinationArgs, DestinationStatsSnapshot, DestinationType, LogTarget,
    PgConnectionArgs, PipelineTuningArgs, bytes_to_mib, cleanup_replication_slots, duration_millis,
    format_decimal, format_duration_ms, format_integer, mib_per_second, per_second, pg_pool,
    pipeline_config, quote_identifier, quote_qualified_table_name, run_etl_migrations,
    split_table_name, write_report,
};

/// Command-line arguments for the table-streaming benchmark.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Where to send log output.
    #[arg(long = "log-target", value_enum, default_value = "terminal", global = true)]
    log_target: LogTarget,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the table-streaming benchmark.
    Run(RunArgs),
}

/// Table-streaming benchmark options.
#[derive(clap::Args, Debug)]
pub struct RunArgs {
    /// Postgres connection options.
    #[command(flatten)]
    pg: PgConnectionArgs,
    /// Pipeline tuning options.
    #[command(flatten)]
    tuning: PipelineTuningArgs,
    /// Destination options.
    #[command(flatten)]
    destination: DestinationArgs,
    /// Pipeline id to use for this benchmark.
    #[arg(long, default_value_t = 2)]
    pipeline_id: u64,
    /// Publication name.
    #[arg(long, default_value = "bench_streaming_pub")]
    publication_name: String,
    /// Source table to stream.
    #[arg(long, default_value = "etl_streaming_benchmark")]
    table_name: String,
    /// Source table IDs to stream.
    #[arg(long, value_delimiter = ',')]
    table_ids: Vec<u32>,
    /// Streaming workload to run.
    #[arg(long, value_enum, default_value = "synthetic")]
    workload: StreamingWorkload,
    /// Create the source table before running.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    create_table: bool,
    /// Truncate the source table before running.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    reset_table: bool,
    /// Recreate the benchmark publication for the source table.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    create_publication: bool,
    /// Number of insert events to produce in count mode.
    #[arg(long, default_value_t = 1_000_000)]
    event_count: u64,
    /// Run producers for a fixed duration instead of a fixed event count.
    #[arg(long)]
    duration_seconds: Option<u64>,
    /// Rows inserted per producer transaction.
    #[arg(long, default_value_t = 5_000)]
    insert_batch_size: u64,
    /// Number of concurrent insert producers.
    #[arg(long, default_value_t = 1)]
    producer_concurrency: u16,
    /// Number of TPC-C warehouses used by the workload.
    #[arg(long, default_value_t = 1)]
    tpcc_warehouses: u16,
    /// TPC-C workload thread concurrency.
    #[arg(long, default_value_t = 1)]
    tpcc_threads: u16,
    /// Time with no new CDC events before the TPC-C stream is considered
    /// drained.
    #[arg(long, default_value_t = 2_000)]
    drain_quiet_ms: u64,
    /// Poll interval used while waiting for TPC-C CDC events to drain.
    #[arg(long, default_value_t = 250)]
    drain_poll_ms: u64,
    /// Write a machine-readable JSON report to this path.
    #[arg(long)]
    report_path: Option<PathBuf>,
}

/// Streaming workload mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum StreamingWorkload {
    /// Run the TPC-C transaction workload against prepared TPC-C tables.
    Tpcc,
    /// Insert rows into a single synthetic benchmark table.
    Synthetic,
}

/// Source used for the produced events field.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProducedEventsSource {
    /// Events are counted by the source-side synthetic producer.
    SourceProducer,
    /// Events are inferred from destination-observed row-level CDC events.
    DestinationObserved,
}

/// Machine-readable table-streaming benchmark report.
#[derive(Debug, Serialize)]
struct TableStreamingReport {
    benchmark: &'static str,
    workload: StreamingWorkload,
    destination: DestinationType,
    pipeline_id: u64,
    publication_name: String,
    table_name: String,
    table_id: Option<u32>,
    table_ids: Vec<u32>,
    table_count: usize,
    requested_event_count: u64,
    duration_seconds: Option<u64>,
    produced_events: u64,
    produced_events_source: ProducedEventsSource,
    throughput_events: u64,
    observed_cdc_events: u64,
    estimated_cdc_payload_bytes: u64,
    estimated_cdc_payload_mib: f64,
    estimated_total_event_bytes: u64,
    estimated_total_event_mib: f64,
    insert_batch_size: u64,
    producer_concurrency: u16,
    tpcc_warehouses: u16,
    tpcc_threads: u16,
    drain_quiet_ms: u64,
    drain_poll_ms: u64,
    pipeline_start_ms: u128,
    ready_wait_ms: u128,
    producer_ms: u128,
    drain_ms: u128,
    end_to_end_ms: u128,
    shutdown_ms: u128,
    end_to_end_with_shutdown_ms: u128,
    total_ms: u128,
    producer_events_per_second: f64,
    end_to_end_events_per_second: f64,
    end_to_end_with_shutdown_events_per_second: f64,
    drain_events_per_second: f64,
    end_to_end_estimated_mib_per_second: f64,
    end_to_end_with_shutdown_estimated_mib_per_second: f64,
    drain_estimated_mib_per_second: f64,
    max_table_sync_workers: u16,
    max_copy_connections_per_table: u16,
    batch_max_fill_ms: u64,
    memory_budget_ratio: f32,
    destination_stats: DestinationStatsSnapshot,
}

/// Runs the table-streaming benchmark binary.
pub async fn main() -> Result<()> {
    let args = Args::parse();
    let _log_flusher = crate::common::init_benchmark_tracing(args.log_target, "table_streaming")?;

    match args.command {
        Commands::Run(args) => run(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    validate_args(&args)?;
    info!("starting table-streaming benchmark");

    let total_started = Instant::now();
    run_etl_migrations(&args.pg).await?;
    let pool = pg_pool(&args.pg).await?;
    let table_name = quote_qualified_table_name(&args.table_name)?;

    if args.workload == StreamingWorkload::Synthetic && args.create_table {
        create_streaming_table(&pool, &args.table_name, &table_name).await?;
    }

    if args.workload == StreamingWorkload::Synthetic && args.reset_table {
        truncate_table(&pool, &table_name).await?;
    }

    if args.workload == StreamingWorkload::Synthetic && args.create_publication {
        create_publication(&pool, &args.publication_name, &table_name).await?;
    }

    let table_ids = match args.workload {
        StreamingWorkload::Tpcc => args.table_ids.clone(),
        StreamingWorkload::Synthetic => vec![get_table_id(&pool, &args.table_name).await?],
    };
    let store = NotifyingStore::new();
    let destination =
        BenchDestination::new(&args.destination, args.pipeline_id, store.clone()).await?;

    let notifications = register_table_ready_notifications(&store, &table_ids).await?;

    let config = pipeline_config(
        args.pipeline_id,
        args.publication_name.clone(),
        &args.pg,
        &args.tuning,
        TableSyncCopyConfig::SkipAllTables,
    );
    config.validate().context("invalid pipeline config")?;

    let mut pipeline = Pipeline::new(config, store, destination.clone());

    let start_started = Instant::now();
    pipeline.start().await.context("failed to start pipeline")?;
    let pipeline_start_ms = duration_millis(start_started.elapsed());

    let ready_started = Instant::now();
    wait_for_tables_ready(notifications).await?;
    let ready_wait_ms = duration_millis(ready_started.elapsed());

    destination.reset_cdc_stats();

    let end_to_end_started = Instant::now();
    let producer_started = Instant::now();
    let produced_events = match args.workload {
        StreamingWorkload::Tpcc => run_tpcc_workload(&args).await?,
        StreamingWorkload::Synthetic => {
            let start_id = next_insert_id(&pool, &table_name).await?;
            if args.duration_seconds.is_none() {
                destination.set_cdc_target(args.event_count);
            }
            produce_inserts(&pool, &table_name, start_id, &args).await?
        }
    };
    let producer_duration = producer_started.elapsed();

    let drain_started = Instant::now();
    match args.workload {
        StreamingWorkload::Tpcc => {
            wait_for_cdc_quiescence(
                &destination,
                Duration::from_millis(args.drain_quiet_ms),
                Duration::from_millis(args.drain_poll_ms),
            )
            .await;
        }
        StreamingWorkload::Synthetic => {
            if args.duration_seconds.is_some() {
                destination.set_cdc_target(produced_events);
            }
            destination.wait_for_cdc_target().await;
        }
    }
    let drain_duration = drain_started.elapsed();
    let end_to_end_duration = end_to_end_started.elapsed();

    let shutdown_started = Instant::now();
    pipeline.shutdown_and_wait().await.context("failed to shut down pipeline")?;
    let shutdown_duration = shutdown_started.elapsed();
    let end_to_end_with_shutdown_duration = end_to_end_started.elapsed();

    cleanup_replication_slots(&args.pg, args.pipeline_id, &table_ids).await?;
    pool.close().await;

    let destination_stats = destination.stats();
    if args.workload == StreamingWorkload::Synthetic
        && destination_stats.cdc_data_events < produced_events
    {
        bail!(
            "CDC validation failed: produced {produced_events}, observed {}",
            destination_stats.cdc_data_events
        );
    }
    if args.workload == StreamingWorkload::Tpcc && destination_stats.cdc_data_events == 0 {
        bail!("TPC-C streaming workload completed without observed CDC row events");
    }

    let produced_events_source = match args.workload {
        StreamingWorkload::Tpcc => ProducedEventsSource::DestinationObserved,
        StreamingWorkload::Synthetic => ProducedEventsSource::SourceProducer,
    };
    let produced_events = match args.workload {
        StreamingWorkload::Tpcc => destination_stats.cdc_data_events,
        StreamingWorkload::Synthetic => produced_events,
    };
    let throughput_events = produced_events;

    let report = TableStreamingReport {
        benchmark: "table_streaming",
        workload: args.workload,
        destination: args.destination.destination,
        pipeline_id: args.pipeline_id,
        publication_name: args.publication_name,
        table_name: args.table_name,
        table_id: table_ids.first().copied(),
        table_count: table_ids.len(),
        table_ids,
        requested_event_count: args.event_count,
        duration_seconds: args.duration_seconds,
        produced_events,
        produced_events_source,
        throughput_events,
        observed_cdc_events: destination_stats.cdc_data_events,
        estimated_cdc_payload_bytes: destination_stats.cdc_data_event_bytes,
        estimated_cdc_payload_mib: bytes_to_mib(destination_stats.cdc_data_event_bytes),
        estimated_total_event_bytes: destination_stats.total_event_bytes,
        estimated_total_event_mib: bytes_to_mib(destination_stats.total_event_bytes),
        insert_batch_size: args.insert_batch_size,
        producer_concurrency: args.producer_concurrency,
        tpcc_warehouses: args.tpcc_warehouses,
        tpcc_threads: args.tpcc_threads,
        drain_quiet_ms: args.drain_quiet_ms,
        drain_poll_ms: args.drain_poll_ms,
        pipeline_start_ms,
        ready_wait_ms,
        producer_ms: duration_millis(producer_duration),
        drain_ms: duration_millis(drain_duration),
        end_to_end_ms: duration_millis(end_to_end_duration),
        shutdown_ms: duration_millis(shutdown_duration),
        end_to_end_with_shutdown_ms: duration_millis(end_to_end_with_shutdown_duration),
        total_ms: duration_millis(total_started.elapsed()),
        producer_events_per_second: per_second(throughput_events, producer_duration),
        end_to_end_events_per_second: per_second(throughput_events, end_to_end_duration),
        end_to_end_with_shutdown_events_per_second: per_second(
            throughput_events,
            end_to_end_with_shutdown_duration,
        ),
        drain_events_per_second: per_second(throughput_events, drain_duration),
        end_to_end_estimated_mib_per_second: mib_per_second(
            destination_stats.cdc_data_event_bytes,
            end_to_end_duration,
        ),
        end_to_end_with_shutdown_estimated_mib_per_second: mib_per_second(
            destination_stats.cdc_data_event_bytes,
            end_to_end_with_shutdown_duration,
        ),
        drain_estimated_mib_per_second: mib_per_second(
            destination_stats.cdc_data_event_bytes,
            drain_duration,
        ),
        max_table_sync_workers: args.tuning.max_table_sync_workers,
        max_copy_connections_per_table: args.tuning.max_copy_connections_per_table,
        batch_max_fill_ms: args.tuning.batch_max_fill_ms,
        memory_budget_ratio: args.tuning.memory_budget_ratio,
        destination_stats,
    };

    print_summary(&report);
    if let Some(report_path) = &args.report_path {
        write_report(&report, report_path)?;
        println!("Report written to {}", report_path.display());
    }
    Ok(())
}

fn print_summary(report: &TableStreamingReport) {
    println!();
    println!("Table streaming benchmark");
    println!("  Destination   {}", destination_label(report.destination));
    println!("  Workload      {}", workload_label(report.workload));
    println!("  Publication   {}", report.publication_name);
    match report.workload {
        StreamingWorkload::Tpcc => {
            println!(
                "  Source tables  {} TPC-C tables",
                format_integer(report.table_count as u128)
            );
        }
        StreamingWorkload::Synthetic => {
            let table_id =
                report.table_id.map_or_else(|| "unknown".to_owned(), |id| id.to_string());
            println!("  Source table   {} ({table_id})", report.table_name);
        }
    }
    println!();
    println!("  CDC");
    match report.produced_events_source {
        ProducedEventsSource::SourceProducer => {
            println!(
                "    Produced       {} events",
                format_integer(u128::from(report.produced_events))
            );
        }
        ProducedEventsSource::DestinationObserved => {
            println!("    Produced       inferred from observed CDC");
        }
    }
    println!(
        "    Observed       {} events",
        format_integer(u128::from(report.observed_cdc_events))
    );
    println!("    Decoded estimate  {} MiB", format_decimal(report.estimated_cdc_payload_mib, 2));
    println!(
        "    Total events   {}",
        format_integer(u128::from(report.destination_stats.total_events))
    );
    println!(
        "    Mix            {} inserts, {} updates, {} deletes, {} relations, {} tx",
        format_integer(u128::from(report.destination_stats.inserts)),
        format_integer(u128::from(report.destination_stats.updates)),
        format_integer(u128::from(report.destination_stats.deletes)),
        format_integer(u128::from(report.destination_stats.relations)),
        format_integer(u128::from(report.destination_stats.transaction_events))
    );
    println!();
    println!("  Throughput");
    println!(
        "    Events/s             {}",
        format_decimal(report.end_to_end_with_shutdown_events_per_second, 2)
    );
    println!(
        "    Est. decoded MiB/s   {}",
        format_decimal(report.end_to_end_with_shutdown_estimated_mib_per_second, 2)
    );
    println!("    Elapsed              {}", format_duration_ms(report.end_to_end_with_shutdown_ms));
    println!();
    println!("  Batches");
    println!(
        "    Event batches  {}",
        format_integer(u128::from(report.destination_stats.event_batches))
    );
    println!(
        "    Max batch      {} events",
        format_integer(u128::from(report.destination_stats.max_event_batch_size))
    );
    println!();
}

fn destination_label(destination: DestinationType) -> &'static str {
    match destination {
        DestinationType::Null => "null",
        DestinationType::BigQuery => "bigquery",
    }
}

fn workload_label(workload: StreamingWorkload) -> &'static str {
    match workload {
        StreamingWorkload::Tpcc => "tpcc",
        StreamingWorkload::Synthetic => "synthetic",
    }
}

fn validate_args(args: &RunArgs) -> Result<()> {
    if args.insert_batch_size == 0 {
        bail!("--insert-batch-size must be greater than 0");
    }

    if args.insert_batch_size > i64::MAX as u64 {
        bail!("--insert-batch-size must fit in a bigint source ID range");
    }

    if args.producer_concurrency == 0 {
        bail!("--producer-concurrency must be greater than 0");
    }

    if args.tpcc_warehouses == 0 {
        bail!("--tpcc-warehouses must be greater than 0");
    }

    if args.tpcc_threads == 0 {
        bail!("--tpcc-threads must be greater than 0");
    }

    if args.drain_quiet_ms == 0 {
        bail!("--drain-quiet-ms must be greater than 0");
    }

    if args.drain_poll_ms == 0 {
        bail!("--drain-poll-ms must be greater than 0");
    }

    if args.workload == StreamingWorkload::Synthetic
        && args.duration_seconds.is_none()
        && args.event_count == 0
    {
        bail!("--event-count must be greater than 0 in count mode");
    }

    if args.workload == StreamingWorkload::Synthetic
        && args.duration_seconds.is_none()
        && args.event_count > i64::MAX as u64
    {
        bail!("--event-count must fit in a bigint source ID range");
    }

    if let Some(duration_seconds) = args.duration_seconds
        && duration_seconds == 0
    {
        bail!("--duration-seconds must be greater than 0");
    }

    if args.workload == StreamingWorkload::Tpcc {
        if args.duration_seconds.is_none() {
            bail!("--duration-seconds is required for --workload tpcc");
        }

        if args.table_ids.is_empty() {
            bail!("--table-ids is required for --workload tpcc");
        }
    }

    Ok(())
}

struct TableReadyNotifications {
    table_id: u32,
    ready: etl::test_utils::notify::TimedNotify,
    errored: etl::test_utils::notify::TimedNotify,
}

async fn register_table_ready_notifications(
    store: &NotifyingStore,
    table_ids: &[u32],
) -> Result<Vec<TableReadyNotifications>> {
    let mut notifications = Vec::with_capacity(table_ids.len());
    for table_id in table_ids {
        let table_id = *table_id;
        let ready = store
            .notify_on_table_state_type(TableId::new(table_id), TableReplicationPhaseType::Ready)
            .await;
        let errored = store
            .notify_on_table_state_type(TableId::new(table_id), TableReplicationPhaseType::Errored)
            .await;
        notifications.push(TableReadyNotifications { table_id, ready, errored });
    }

    Ok(notifications)
}

async fn wait_for_tables_ready(notifications: Vec<TableReadyNotifications>) -> Result<()> {
    let mut tasks = JoinSet::new();
    for notification in notifications {
        tasks.spawn(async move {
            let table_id = notification.table_id;
            tokio::select! {
                () = notification.ready.inner().notified() => Ok(()),
                () = notification.errored.inner().notified() => {
                    bail!("table {table_id} entered errored state before streaming benchmark could start")
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("table ready wait task panicked")??;
    }

    Ok(())
}

async fn run_tpcc_workload(args: &RunArgs) -> Result<u64> {
    let duration_seconds = args.duration_seconds.context("TPC-C duration was not configured")?;
    let password = args.pg.password.clone().unwrap_or_default();
    let warehouses = args.tpcc_warehouses.to_string();
    let port = args.pg.port.to_string();
    let threads = args.tpcc_threads.to_string();
    let duration = format!("{duration_seconds}s");
    let username = args.pg.username.clone();
    let database = args.pg.database.clone();
    let host = args.pg.host.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new("go-tpc")
            .args(["tpcc", "--warehouses", &warehouses, "run"])
            .args(["-d", "postgres"])
            .args(["-U", &username])
            .args(["-p", &password])
            .args(["-D", &database])
            .args(["-H", &host])
            .args(["-P", &port])
            .args(["--conn-params", "sslmode=disable"])
            .args(["-T", &threads])
            .args(["--time", &duration])
            .args(["--output", "plain"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
    })
    .await
    .context("go-tpc workload task panicked")?
    .context("failed to run go-tpc TPC-C workload")?;

    if !output.status.success() {
        bail!("go-tpc TPC-C workload failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }

    Ok(0)
}

async fn wait_for_cdc_quiescence(
    destination: &BenchDestination,
    quiet_duration: Duration,
    poll_interval: Duration,
) {
    let mut last_events = destination.stats().cdc_data_events;
    let mut quiet_started = Instant::now();
    loop {
        tokio::time::sleep(poll_interval).await;
        let current_events = destination.stats().cdc_data_events;
        if current_events == last_events {
            if quiet_started.elapsed() >= quiet_duration {
                return;
            }
        } else {
            last_events = current_events;
            quiet_started = Instant::now();
        }
    }
}

async fn create_streaming_table(
    pool: &PgPool,
    raw_table_name: &str,
    table_name: &str,
) -> Result<()> {
    let (schema, _) = split_table_name(raw_table_name)?;
    let schema_name = quote_identifier(&schema)?;
    sqlx::query(&format!("create schema if not exists {schema_name}"))
        .execute(pool)
        .await
        .context("failed to create benchmark schema")?;

    sqlx::query(&format!(
        "create table if not exists {table_name} (id bigint primary key, payload text not null, \
         produced_at timestamptz not null default clock_timestamp())"
    ))
    .execute(pool)
    .await
    .context("failed to create streaming benchmark table")?;

    Ok(())
}

async fn truncate_table(pool: &PgPool, table_name: &str) -> Result<()> {
    sqlx::query(&format!("truncate table {table_name}"))
        .execute(pool)
        .await
        .context("failed to truncate streaming benchmark table")?;

    Ok(())
}

async fn create_publication(pool: &PgPool, publication_name: &str, table_name: &str) -> Result<()> {
    let publication_name = quote_identifier(publication_name)?;
    sqlx::query(&format!("drop publication if exists {publication_name}"))
        .execute(pool)
        .await
        .context("failed to drop streaming benchmark publication")?;
    sqlx::query(&format!("create publication {publication_name} for table {table_name}"))
        .execute(pool)
        .await
        .context("failed to create streaming benchmark publication")?;

    Ok(())
}

async fn get_table_id(pool: &PgPool, raw_table_name: &str) -> Result<u32> {
    let (schema, table) = split_table_name(raw_table_name)?;
    let oid: i64 = sqlx::query_scalar(
        "select c.oid::bigint from pg_class c join pg_namespace n on n.oid = c.relnamespace where \
         n.nspname = $1 and c.relname = $2 and c.relkind = 'r'",
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await?
    .context("streaming benchmark table was not found")?;

    u32::try_from(oid).context("table OID does not fit into u32")
}

async fn next_insert_id(pool: &PgPool, table_name: &str) -> Result<i64> {
    let sql = format!("select coalesce(max(id), 0) + 1 from {table_name}");
    let id: i64 = sqlx::query_scalar(&sql).fetch_one(pool).await?;
    Ok(id)
}

async fn produce_inserts(
    pool: &PgPool,
    table_name: &str,
    start_id: i64,
    args: &RunArgs,
) -> Result<u64> {
    match args.duration_seconds {
        Some(duration_seconds) => {
            produce_for_duration(
                pool,
                table_name,
                start_id,
                Duration::from_secs(duration_seconds),
                args.insert_batch_size,
                args.producer_concurrency,
            )
            .await
        }
        None => {
            produce_event_count(
                pool,
                table_name,
                start_id,
                args.event_count,
                args.insert_batch_size,
                args.producer_concurrency,
            )
            .await
        }
    }
}

async fn produce_event_count(
    pool: &PgPool,
    table_name: &str,
    start_id: i64,
    event_count: u64,
    insert_batch_size: u64,
    producer_concurrency: u16,
) -> Result<u64> {
    let next_id = Arc::new(AtomicI64::new(start_id));
    let end_id = start_id
        .checked_add(i64::try_from(event_count)?)
        .and_then(|value| value.checked_sub(1))
        .context("event count is too large for bigint source IDs")?;
    let mut tasks = JoinSet::new();

    for _ in 0..producer_concurrency {
        let pool = pool.clone();
        let table_name = table_name.to_owned();
        let next_id = Arc::clone(&next_id);
        tasks.spawn(async move {
            let mut produced = 0_u64;
            loop {
                let start = next_id.fetch_add(insert_batch_size as i64, Ordering::Relaxed);
                if start > end_id {
                    break;
                }

                let end = end_id.min(start + insert_batch_size as i64 - 1);
                insert_range(&pool, &table_name, start, end).await?;
                produced += u64::try_from(end - start + 1)?;
            }

            Result::<u64>::Ok(produced)
        });
    }

    collect_producer_tasks(tasks).await
}

async fn produce_for_duration(
    pool: &PgPool,
    table_name: &str,
    start_id: i64,
    duration: Duration,
    insert_batch_size: u64,
    producer_concurrency: u16,
) -> Result<u64> {
    let next_id = Arc::new(AtomicI64::new(start_id));
    let produced = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + duration;
    let mut tasks = JoinSet::new();

    for _ in 0..producer_concurrency {
        let pool = pool.clone();
        let table_name = table_name.to_owned();
        let next_id = Arc::clone(&next_id);
        let produced = Arc::clone(&produced);
        tasks.spawn(async move {
            loop {
                if Instant::now() >= deadline {
                    break;
                }

                let start = next_id.fetch_add(insert_batch_size as i64, Ordering::Relaxed);
                let end = start + insert_batch_size as i64 - 1;
                insert_range(&pool, &table_name, start, end).await?;
                produced.fetch_add(insert_batch_size, Ordering::Relaxed);
            }

            Result::<u64>::Ok(0)
        });
    }

    collect_producer_tasks(tasks).await?;
    Ok(produced.load(Ordering::Relaxed))
}

async fn collect_producer_tasks(mut tasks: JoinSet<Result<u64>>) -> Result<u64> {
    let mut produced = 0_u64;
    while let Some(result) = tasks.join_next().await {
        produced += result.context("producer task panicked")??;
    }

    Ok(produced)
}

async fn insert_range(pool: &PgPool, table_name: &str, start: i64, end: i64) -> Result<()> {
    let sql = format!(
        "insert into {table_name} (id, payload) select gs, md5(gs::text) from \
         generate_series($1::bigint, $2::bigint) as gs"
    );
    sqlx::query(&sql)
        .bind(start)
        .bind(end)
        .execute(pool)
        .await
        .with_context(|| format!("failed to insert streaming rows {start}..={end}"))?;

    Ok(())
}
