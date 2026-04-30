use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use etl::{
    pipeline::Pipeline, state::table::TableReplicationPhaseType,
    test_utils::notifying_store::NotifyingStore,
};
use etl_config::shared::TableSyncCopyConfig;
use etl_postgres::types::TableId;
use serde::Serialize;
use tracing::info;

use crate::common::{
    BenchDestination, DestinationArgs, DestinationStatsSnapshot, DestinationType, LogTarget,
    PgConnectionArgs, PipelineTuningArgs, bytes_to_mib, cleanup_replication_slots, duration_millis,
    init_benchmark_tracing, mib_per_second, per_second, pipeline_config, print_report,
    run_etl_migrations,
};

/// Command-line arguments for the table-copy benchmark.
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
    /// Run the table-copy benchmark.
    Run(RunArgs),
}

/// Table-copy benchmark options.
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
    #[arg(long, default_value_t = 1)]
    pipeline_id: u64,
    /// Publication name.
    #[arg(long, default_value = "bench_pub")]
    publication_name: String,
    /// Table IDs to copy.
    #[arg(long, value_delimiter = ',')]
    table_ids: Vec<u32>,
    /// Expected total row count for validation.
    #[arg(long)]
    expected_row_count: Option<u64>,
    /// Compatibility flag appended by `cargo bench`.
    #[arg(long, hide = true)]
    bench: bool,
}

/// Machine-readable table-copy benchmark report.
#[derive(Debug, Serialize)]
struct TableCopyReport {
    benchmark: &'static str,
    destination: DestinationType,
    pipeline_id: u64,
    publication_name: String,
    table_ids: Vec<u32>,
    expected_row_count: Option<u64>,
    copied_rows: u64,
    estimated_copied_bytes: u64,
    estimated_copied_mib: f64,
    table_count: usize,
    pipeline_start_ms: u128,
    copy_wait_ms: u128,
    shutdown_ms: u128,
    total_ms: u128,
    rows_per_second: f64,
    estimated_mib_per_second: f64,
    max_table_sync_workers: u16,
    max_copy_connections_per_table: u16,
    batch_max_fill_ms: u64,
    memory_budget_ratio: f32,
    destination_stats: DestinationStatsSnapshot,
}

/// Runs the table-copy benchmark binary.
pub async fn main() -> Result<()> {
    let args = Args::parse();
    let _log_flusher = init_benchmark_tracing(args.log_target, "table_copy")?;

    match args.command {
        Commands::Run(args) => run(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    let _ = args.bench;

    if args.table_ids.is_empty() {
        bail!("--table-ids must include at least one table");
    }

    info!("starting table-copy benchmark");
    let total_started = Instant::now();
    run_etl_migrations(&args.pg).await?;

    let store = NotifyingStore::new();
    let destination =
        BenchDestination::new(&args.destination, args.pipeline_id, store.clone()).await?;
    let config = pipeline_config(
        args.pipeline_id,
        args.publication_name.clone(),
        &args.pg,
        &args.tuning,
        TableSyncCopyConfig::IncludeTables { table_ids: args.table_ids.clone() },
    );

    config.validate().context("invalid pipeline config")?;

    let table_copied_notifications = register_finished_copy_notifications(&store, &args.table_ids)
        .await
        .context("failed to register table-copy notifications")?;

    let mut pipeline = Pipeline::new(config, store, destination.clone());

    let start_started = Instant::now();
    pipeline.start().await.context("failed to start pipeline")?;
    let pipeline_start_ms = duration_millis(start_started.elapsed());

    let copy_started = Instant::now();
    for notification in table_copied_notifications {
        notification.inner().notified().await;
    }
    let copy_wait_duration = copy_started.elapsed();

    let shutdown_started = Instant::now();
    pipeline.shutdown_and_wait().await.context("failed to shut down pipeline")?;
    let shutdown_ms = duration_millis(shutdown_started.elapsed());

    cleanup_replication_slots(&args.pg, args.pipeline_id, &args.table_ids).await?;

    let destination_stats = destination.stats();
    let copied_rows = destination_stats.table_rows;
    let copied_bytes = destination_stats.table_row_bytes;
    if let Some(expected) = args.expected_row_count
        && copied_rows != expected
    {
        bail!("row count validation failed: expected {expected}, copied {copied_rows}");
    }

    let table_count = args.table_ids.len();
    let report = TableCopyReport {
        benchmark: "table_copy",
        destination: args.destination.destination,
        pipeline_id: args.pipeline_id,
        publication_name: args.publication_name,
        table_ids: args.table_ids,
        expected_row_count: args.expected_row_count,
        copied_rows,
        estimated_copied_bytes: copied_bytes,
        estimated_copied_mib: bytes_to_mib(copied_bytes),
        table_count,
        pipeline_start_ms,
        copy_wait_ms: duration_millis(copy_wait_duration),
        shutdown_ms,
        total_ms: duration_millis(total_started.elapsed()),
        rows_per_second: per_second(copied_rows, copy_wait_duration),
        estimated_mib_per_second: mib_per_second(copied_bytes, copy_wait_duration),
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

fn print_summary(report: &TableCopyReport) {
    println!();
    println!("Table copy benchmark");
    println!("  destination: {}", destination_label(report.destination));
    println!("  publication: {}", report.publication_name);
    println!(
        "  copied: {} rows across {} table(s), {:.2} MiB estimated",
        report.copied_rows, report.table_count, report.estimated_copied_mib
    );
    println!(
        "  throughput: {:.2} rows/s, {:.2} estimated MiB/s",
        report.rows_per_second, report.estimated_mib_per_second
    );
    println!(
        "  timing: start={}ms, copy_wait={}ms, shutdown={}ms, total={}ms",
        report.pipeline_start_ms, report.copy_wait_ms, report.shutdown_ms, report.total_ms
    );
    println!("  batches: {} table-row batch(es)", report.destination_stats.table_row_batches);
    println!();
}

fn destination_label(destination: DestinationType) -> &'static str {
    match destination {
        DestinationType::Null => "null",
        DestinationType::BigQuery => "big-query",
    }
}

async fn register_finished_copy_notifications(
    store: &NotifyingStore,
    table_ids: &[u32],
) -> Result<Vec<etl::test_utils::notify::TimedNotify>> {
    let mut notifications = Vec::with_capacity(table_ids.len());
    for table_id in table_ids {
        notifications.push(
            store
                .notify_on_table_state_type(
                    TableId::new(*table_id),
                    TableReplicationPhaseType::FinishedCopy,
                )
                .await,
        );
    }

    Ok(notifications)
}
