use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
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
    mib_per_second, per_second, pg_pool, pipeline_config, print_report, quote_identifier,
    quote_qualified_table_name, run_etl_migrations, split_table_name,
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
    /// Create the source table before running.
    #[arg(long, default_value_t = true)]
    create_table: bool,
    /// Truncate the source table before running.
    #[arg(long, default_value_t = true)]
    reset_table: bool,
    /// Recreate the benchmark publication for the source table.
    #[arg(long, default_value_t = true)]
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
    /// Compatibility flag appended by `cargo bench`.
    #[arg(long, hide = true)]
    bench: bool,
}

/// Machine-readable table-streaming benchmark report.
#[derive(Debug, Serialize)]
struct TableStreamingReport {
    benchmark: &'static str,
    destination: DestinationType,
    pipeline_id: u64,
    publication_name: String,
    table_name: String,
    table_id: u32,
    requested_event_count: u64,
    duration_seconds: Option<u64>,
    produced_events: u64,
    observed_cdc_events: u64,
    estimated_cdc_payload_bytes: u64,
    estimated_cdc_payload_mib: f64,
    estimated_total_event_bytes: u64,
    estimated_total_event_mib: f64,
    insert_batch_size: u64,
    producer_concurrency: u16,
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
    let _ = args.bench;

    validate_args(&args)?;
    info!("starting table-streaming benchmark");

    let total_started = Instant::now();
    run_etl_migrations(&args.pg).await?;
    let pool = pg_pool(&args.pg).await?;
    let table_name = quote_qualified_table_name(&args.table_name)?;

    if args.create_table {
        create_streaming_table(&pool, &args.table_name, &table_name).await?;
    }

    if args.reset_table {
        truncate_table(&pool, &table_name).await?;
    }

    if args.create_publication {
        create_publication(&pool, &args.publication_name, &table_name).await?;
    }

    let table_id = get_table_id(&pool, &args.table_name).await?;
    let store = NotifyingStore::new();
    let destination =
        BenchDestination::new(&args.destination, args.pipeline_id, store.clone()).await?;

    let ready = store
        .notify_on_table_state_type(TableId::new(table_id), TableReplicationPhaseType::Ready)
        .await;
    let errored = store
        .notify_on_table_state_type(TableId::new(table_id), TableReplicationPhaseType::Errored)
        .await;

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
    tokio::select! {
        () = ready.notified() => {}
        () = errored.notified() => {
            bail!("table entered errored state before streaming benchmark could start");
        }
    }
    let ready_wait_ms = duration_millis(ready_started.elapsed());

    destination.reset_cdc_stats();
    let start_id = next_insert_id(&pool, &table_name).await?;
    if args.duration_seconds.is_none() {
        destination.set_cdc_target(args.event_count);
    }

    let end_to_end_started = Instant::now();
    let producer_started = Instant::now();
    let produced_events = produce_inserts(&pool, &table_name, start_id, &args).await?;
    let producer_duration = producer_started.elapsed();

    if args.duration_seconds.is_some() {
        destination.set_cdc_target(produced_events);
    }

    let drain_started = Instant::now();
    destination.wait_for_cdc_target().await;
    let drain_duration = drain_started.elapsed();
    let end_to_end_duration = end_to_end_started.elapsed();

    let shutdown_started = Instant::now();
    pipeline.shutdown_and_wait().await.context("failed to shut down pipeline")?;
    let shutdown_duration = shutdown_started.elapsed();
    let end_to_end_with_shutdown_duration = end_to_end_started.elapsed();

    cleanup_replication_slots(&args.pg, args.pipeline_id, &[table_id]).await?;
    pool.close().await;

    let destination_stats = destination.stats();
    if destination_stats.cdc_data_events < produced_events {
        bail!(
            "CDC validation failed: produced {produced_events}, observed {}",
            destination_stats.cdc_data_events
        );
    }

    let report = TableStreamingReport {
        benchmark: "table_streaming",
        destination: args.destination.destination,
        pipeline_id: args.pipeline_id,
        publication_name: args.publication_name,
        table_name: args.table_name,
        table_id,
        requested_event_count: args.event_count,
        duration_seconds: args.duration_seconds,
        produced_events,
        observed_cdc_events: destination_stats.cdc_data_events,
        estimated_cdc_payload_bytes: destination_stats.cdc_data_event_bytes,
        estimated_cdc_payload_mib: bytes_to_mib(destination_stats.cdc_data_event_bytes),
        estimated_total_event_bytes: destination_stats.total_event_bytes,
        estimated_total_event_mib: bytes_to_mib(destination_stats.total_event_bytes),
        insert_batch_size: args.insert_batch_size,
        producer_concurrency: args.producer_concurrency,
        pipeline_start_ms,
        ready_wait_ms,
        producer_ms: duration_millis(producer_duration),
        drain_ms: duration_millis(drain_duration),
        end_to_end_ms: duration_millis(end_to_end_duration),
        shutdown_ms: duration_millis(shutdown_duration),
        end_to_end_with_shutdown_ms: duration_millis(end_to_end_with_shutdown_duration),
        total_ms: duration_millis(total_started.elapsed()),
        producer_events_per_second: per_second(produced_events, producer_duration),
        end_to_end_events_per_second: per_second(produced_events, end_to_end_duration),
        end_to_end_with_shutdown_events_per_second: per_second(
            produced_events,
            end_to_end_with_shutdown_duration,
        ),
        drain_events_per_second: per_second(produced_events, drain_duration),
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
    print_report(&report)?;
    Ok(())
}

fn print_summary(report: &TableStreamingReport) {
    println!();
    println!("Table streaming benchmark");
    println!("  destination: {}", destination_label(report.destination));
    println!("  publication: {}", report.publication_name);
    println!("  table: {} ({})", report.table_name, report.table_id);
    println!(
        "  CDC events: produced={}, observed={}, estimated payload={:.2} MiB",
        report.produced_events, report.observed_cdc_events, report.estimated_cdc_payload_mib
    );
    println!(
        "  event mix: inserts={}, updates={}, deletes={}, relations={}, tx={}, total={}",
        report.destination_stats.inserts,
        report.destination_stats.updates,
        report.destination_stats.deletes,
        report.destination_stats.relations,
        report.destination_stats.transaction_events,
        report.destination_stats.total_events
    );
    println!(
        "  producer: {:.2} events/s over {}ms",
        report.producer_events_per_second, report.producer_ms
    );
    println!(
        "  dispatch: {:.2} events/s, {:.2} estimated MiB/s over {}ms",
        report.end_to_end_events_per_second,
        report.end_to_end_estimated_mib_per_second,
        report.end_to_end_ms
    );
    println!(
        "  destination-drained: {:.2} events/s, {:.2} estimated MiB/s over {}ms",
        report.end_to_end_with_shutdown_events_per_second,
        report.end_to_end_with_shutdown_estimated_mib_per_second,
        report.end_to_end_with_shutdown_ms
    );
    println!(
        "  catch-up drain: {:.2} events/s, {:.2} estimated MiB/s over {}ms",
        report.drain_events_per_second, report.drain_estimated_mib_per_second, report.drain_ms
    );
    println!(
        "  batches: {} event batch(es), max batch {} event(s)",
        report.destination_stats.event_batches, report.destination_stats.max_event_batch_size
    );
    println!();
}

fn destination_label(destination: DestinationType) -> &'static str {
    match destination {
        DestinationType::Null => "null",
        DestinationType::BigQuery => "big-query",
    }
}

fn validate_args(args: &RunArgs) -> Result<()> {
    if args.insert_batch_size == 0 {
        bail!("--insert-batch-size must be greater than 0");
    }

    if args.producer_concurrency == 0 {
        bail!("--producer-concurrency must be greater than 0");
    }

    if args.duration_seconds.is_none() && args.event_count == 0 {
        bail!("--event-count must be greater than 0 in count mode");
    }

    if let Some(duration_seconds) = args.duration_seconds
        && duration_seconds == 0
    {
        bail!("--duration-seconds must be greater than 0");
    }

    Ok(())
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
