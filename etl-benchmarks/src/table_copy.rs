use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use etl::{
    pipeline::Pipeline,
    state::table::TableReplicationPhaseType,
    test_utils::{notify::TimedNotify, notifying_store::NotifyingStore},
};
use etl_config::shared::TableSyncCopyConfig;
use etl_postgres::types::TableId;
use serde::Serialize;
use tokio::task::JoinSet;
use tracing::info;

use crate::common::{
    BenchDestination, DestinationArgs, DestinationStatsSnapshot, DestinationType, LogTarget,
    PgConnectionArgs, PipelineTuningArgs, bytes_to_mib, cleanup_replication_slots, duration_millis,
    format_decimal, format_duration_ms, format_integer, init_benchmark_tracing, mib_per_second,
    per_second, pipeline_config, run_etl_migrations, write_report,
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
    /// Write a machine-readable JSON report to this path.
    #[arg(long)]
    report_path: Option<PathBuf>,
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

    let table_copy_notifications = register_table_copy_notifications(&store, &args.table_ids)
        .await
        .context("failed to register table-copy notifications")?;

    let mut pipeline = Pipeline::new(config, store, destination.clone());

    let start_started = Instant::now();
    pipeline.start().await.context("failed to start pipeline")?;
    let pipeline_start_ms = duration_millis(start_started.elapsed());

    let copy_started = Instant::now();
    let copy_result =
        wait_for_table_copies(&destination, args.expected_row_count, table_copy_notifications)
            .await;
    let copy_wait_duration = copy_started.elapsed();

    let shutdown_started = Instant::now();
    let shutdown_result = pipeline.shutdown_and_wait().await;
    let shutdown_ms = duration_millis(shutdown_started.elapsed());

    let cleanup_result =
        cleanup_replication_slots(&args.pg, args.pipeline_id, &args.table_ids).await;
    shutdown_result.context("failed to shut down pipeline")?;
    cleanup_result?;
    copy_result?;

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
    if let Some(report_path) = &args.report_path {
        write_report(&report, report_path)?;
        println!("Report written to {}", report_path.display());
    }
    Ok(())
}

fn print_summary(report: &TableCopyReport) {
    println!();
    println!("Table copy benchmark");
    println!("  Destination   {}", destination_label(report.destination));
    println!("  Publication   {}", report.publication_name);
    println!("  Tables        {}", format_integer(report.table_count as u128));
    println!();
    println!("  Data");
    println!("    Rows copied     {}", format_integer(u128::from(report.copied_rows)));
    if let Some(expected) = report.expected_row_count {
        println!("    Rows expected   {}", format_integer(u128::from(expected)));
    }
    println!("    Decoded estimate  {} MiB", format_decimal(report.estimated_copied_mib, 2));
    println!(
        "    Row batches     {}",
        format_integer(u128::from(report.destination_stats.table_row_batches))
    );
    println!();
    println!("  Throughput");
    println!("    Rows/s            {}", format_decimal(report.rows_per_second, 2));
    println!("    Est. decoded MiB/s {}", format_decimal(report.estimated_mib_per_second, 2));
    println!();
    println!("  Timing");
    println!("    Pipeline start  {}", format_duration_ms(report.pipeline_start_ms));
    println!("    Copy wait       {}", format_duration_ms(report.copy_wait_ms));
    println!("    Shutdown        {}", format_duration_ms(report.shutdown_ms));
    println!("    Total           {}", format_duration_ms(report.total_ms));
    println!();
}

fn destination_label(destination: DestinationType) -> &'static str {
    match destination {
        DestinationType::Null => "null",
        DestinationType::BigQuery => "bigquery",
    }
}

struct TableCopyNotifications {
    table_id: u32,
    finished: TimedNotify,
    errored: TimedNotify,
}

async fn register_table_copy_notifications(
    store: &NotifyingStore,
    table_ids: &[u32],
) -> Result<Vec<TableCopyNotifications>> {
    let mut notifications = Vec::with_capacity(table_ids.len());
    for table_id in table_ids {
        let table_id = *table_id;
        let finished = store
            .notify_on_table_state_type(
                TableId::new(table_id),
                TableReplicationPhaseType::FinishedCopy,
            )
            .await;
        let errored = store
            .notify_on_table_state_type(TableId::new(table_id), TableReplicationPhaseType::Errored)
            .await;
        notifications.push(TableCopyNotifications { table_id, finished, errored });
    }

    Ok(notifications)
}

async fn wait_for_table_copies(
    destination: &BenchDestination,
    expected_row_count: Option<u64>,
    notifications: Vec<TableCopyNotifications>,
) -> Result<()> {
    if let Some(expected_row_count) = expected_row_count {
        return wait_for_expected_rows(destination, expected_row_count, notifications).await;
    }

    let mut tasks = JoinSet::new();
    for notification in notifications {
        tasks.spawn(async move {
            let table_id = notification.table_id;
            tokio::select! {
                () = notification.finished.inner().notified() => Ok(()),
                () = notification.errored.inner().notified() => {
                    bail!("table {table_id} entered errored state during table-copy benchmark")
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("table-copy wait task panicked")??;
    }

    Ok(())
}

async fn wait_for_expected_rows(
    destination: &BenchDestination,
    expected_row_count: u64,
    notifications: Vec<TableCopyNotifications>,
) -> Result<()> {
    let mut errors = JoinSet::new();
    for notification in notifications {
        errors.spawn(async move {
            let table_id = notification.table_id;
            notification.errored.inner().notified().await;
            bail!("table {table_id} entered errored state during table-copy benchmark")
        });
    }

    loop {
        if destination.stats().table_rows >= expected_row_count {
            return Ok(());
        }

        tokio::select! {
            result = errors.join_next() => {
                if let Some(result) = result {
                    result.context("table-copy error wait task panicked")??;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}
