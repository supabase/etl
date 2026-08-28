use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use etl::{
    pipeline::Pipeline,
    schema::TableId,
    store::TableStateType,
    test_utils::{notify::TimedNotify, notifying_store::NotifyingStore},
};
#[cfg(feature = "ducklake")]
use etl_config::shared::DuckLakeWriterConfig;
use etl_config::shared::{TableSyncCopyConfig, Validate};
#[cfg(feature = "ducklake")]
use etl_destinations::ducklake::{
    CleanupOldFilesMaintenanceConfig, DuckLakeMaintenanceConfig, ExpireSnapshotsMaintenanceConfig,
    InlineFlushMaintenanceConfig, MergeAdjacentFilesMaintenanceConfig,
    RewriteDataFilesMaintenanceConfig, run_maintenance_once,
};
use serde::Serialize;
#[cfg(feature = "ducklake")]
use sqlx::{AssertSqlSafe, postgres::PgPoolOptions};
use tokio::task::JoinSet;
use tracing::info;
#[cfg(feature = "ducklake")]
use url::Url;

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
    /// Run one local DuckLake adjacent-file compaction after the copy.
    #[arg(long, default_value_t = false)]
    ducklake_compact_after_copy: bool,
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
    memory_backpressure_enabled: bool,
    ducklake_copy_buffer: Option<DuckLakeCopyBufferReport>,
    ducklake_snapshot_count: Option<u64>,
    ducklake_compaction: Option<DuckLakeCompactionReport>,
    ducklake_files: Option<DuckLakeFileStats>,
    destination_stats: DestinationStatsSnapshot,
}

/// DuckLake buffering settings used for one benchmark run.
#[derive(Debug, Serialize)]
struct DuckLakeCopyBufferReport {
    enabled: bool,
    target_bytes: u64,
    max_total_bytes: u64,
    peak_staged_bytes: u64,
}

/// Optional post-copy DuckLake compaction result.
#[derive(Debug, Serialize)]
struct DuckLakeCompactionReport {
    duration_ms: u128,
    merge_adjacent_files_tables: u32,
    merge_adjacent_files_created: u64,
    snapshot_count_after: Option<u64>,
}

/// Local DuckLake Parquet file distribution after a benchmark run.
#[derive(Debug, Serialize)]
struct DuckLakeFileStats {
    file_count: usize,
    total_bytes: u64,
    min_bytes: u64,
    median_bytes: u64,
    p95_bytes: u64,
    max_bytes: u64,
    mean_bytes: f64,
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

    config.validate().context("Invalid pipeline config")?;

    let table_copy_notifications = register_table_copy_notifications(&store, &args.table_ids)
        .await
        .context("Failed to register table-copy notifications")?;

    let mut pipeline = Pipeline::new(config, store, destination.clone());

    let start_started = Instant::now();
    pipeline.start().await.context("Failed to start pipeline")?;
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
    shutdown_result.context("Failed to shut down pipeline")?;
    cleanup_result?;
    copy_result?;

    let destination_stats = destination.stats();
    let copied_rows = destination_stats.table_rows;
    let copied_bytes = destination_stats.table_row_bytes;
    if let Some(expected) = args.expected_row_count
        && copied_rows != expected
    {
        bail!("Row count validation failed: expected {expected}, copied {copied_rows}");
    }

    let table_count = args.table_ids.len();
    let peak_staged_bytes = destination.ducklake_copy_buffer_peak_staged_bytes().unwrap_or(0);
    let ducklake_copy_buffer = (args.destination.destination == DestinationType::DuckLake)
        .then_some(DuckLakeCopyBufferReport {
            enabled: args.destination.ducklake_copy_buffer_enabled,
            target_bytes: args.destination.ducklake_copy_buffer_target_bytes,
            max_total_bytes: args.destination.ducklake_copy_buffer_max_total_bytes,
            peak_staged_bytes,
        });
    let ducklake_files = collect_ducklake_file_stats(&args.destination)?;
    let ducklake_snapshot_count = collect_ducklake_snapshot_count(&args.destination).await?;
    let total_ms = duration_millis(total_started.elapsed());
    let ducklake_compaction = run_ducklake_compaction(&args).await?;
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
        total_ms,
        rows_per_second: per_second(copied_rows, copy_wait_duration),
        estimated_mib_per_second: mib_per_second(copied_bytes, copy_wait_duration),
        max_table_sync_workers: args.tuning.max_table_sync_workers,
        max_copy_connections_per_table: args.tuning.max_copy_connections_per_table,
        batch_max_fill_ms: args.tuning.batch_max_fill_ms,
        memory_budget_ratio: args.tuning.memory_budget_ratio,
        memory_backpressure_enabled: !args.tuning.disable_memory_backpressure,
        ducklake_copy_buffer,
        ducklake_snapshot_count,
        ducklake_compaction,
        ducklake_files,
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
    println!(
        "  Tables        {}",
        format_integer(u128::try_from(report.table_count).unwrap_or(u128::MAX))
    );
    if let Some(copy_buffer) = &report.ducklake_copy_buffer {
        println!("  Copy buffer   {}", if copy_buffer.enabled { "enabled" } else { "disabled" });
        println!(
            "    Target      {} MiB",
            format_decimal(bytes_to_mib(copy_buffer.target_bytes), 2)
        );
        println!(
            "    Max total   {} MiB",
            format_decimal(bytes_to_mib(copy_buffer.max_total_bytes), 2)
        );
        println!(
            "    Peak staged {} MiB",
            format_decimal(bytes_to_mib(copy_buffer.peak_staged_bytes), 2)
        );
    }
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
    if let Some(files) = &report.ducklake_files {
        println!(
            "    Parquet files   {}",
            format_integer(u128::try_from(files.file_count).unwrap_or(u128::MAX))
        );
        println!("    Parquet total   {} MiB", format_decimal(bytes_to_mib(files.total_bytes), 2));
        println!("    Parquet median  {} MiB", format_decimal(bytes_to_mib(files.median_bytes), 2));
        println!("    Parquet p95     {} MiB", format_decimal(bytes_to_mib(files.p95_bytes), 2));
    }
    if let Some(snapshot_count) = report.ducklake_snapshot_count {
        println!("    Snapshots       {}", format_integer(u128::from(snapshot_count)));
    }
    if let Some(compaction) = &report.ducklake_compaction {
        println!("    Compaction      {}", format_duration_ms(compaction.duration_ms));
        println!(
            "      Tables        {}",
            format_integer(u128::from(compaction.merge_adjacent_files_tables))
        );
        println!(
            "      Files created {}",
            format_integer(u128::from(compaction.merge_adjacent_files_created))
        );
        if let Some(snapshot_count) = compaction.snapshot_count_after {
            println!("      Snapshots     {}", format_integer(u128::from(snapshot_count)));
        }
    }
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
        DestinationType::ClickHouse => "clickhouse",
        DestinationType::DuckLake => "ducklake",
        DestinationType::Snowflake => "snowflake",
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
            .notify_on_table_state_type(TableId::new(table_id), TableStateType::FinishedCopy)
            .await;
        let errored =
            store.notify_on_table_state_type(TableId::new(table_id), TableStateType::Errored).await;
        notifications.push(TableCopyNotifications { table_id, finished, errored });
    }

    Ok(notifications)
}

async fn wait_for_table_copies(
    _destination: &BenchDestination,
    _expected_row_count: Option<u64>,
    notifications: Vec<TableCopyNotifications>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    for notification in notifications {
        tasks.spawn(async move {
            let table_id = notification.table_id;
            tokio::select! {
                () = notification.finished.inner().notified() => Ok(()),
                () = notification.errored.inner().notified() => {
                    bail!("Table {table_id} entered errored state during table-copy benchmark")
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("Table-copy wait task panicked")??;
    }

    Ok(())
}

/// Counts snapshots in the PostgreSQL-backed DuckLake metadata catalog.
async fn collect_ducklake_snapshot_count(destination: &DestinationArgs) -> Result<Option<u64>> {
    if destination.destination != DestinationType::DuckLake {
        return Ok(None);
    }

    #[cfg(not(feature = "ducklake"))]
    bail!("DuckLake snapshot collection requires the etl-benchmarks ducklake feature");

    #[cfg(feature = "ducklake")]
    {
        let catalog_url = destination
            .ducklake_catalog_url
            .as_deref()
            .context("DuckLake catalog URL is required for snapshot statistics")?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(catalog_url)
            .await
            .context("Failed to connect to the DuckLake metadata catalog")?;
        let metadata_schema: Option<String> = sqlx::query_scalar(
            r#"select table_schema::text
               from information_schema.tables
               where table_name = 'ducklake_snapshot'
               order by case
                   when table_schema = 'main' then 0
                   when table_schema = 'ducklake' then 1
                   else 2
               end,
               table_schema
               limit 1"#,
        )
        .fetch_optional(&pool)
        .await
        .context("Failed to resolve the DuckLake metadata schema")?;
        let metadata_schema = metadata_schema.context("DuckLake snapshot metadata is missing")?;
        let metadata_schema = format!("\"{}\"", metadata_schema.replace('"', "\"\""));
        let sql = format!("select count(*)::bigint from {metadata_schema}.\"ducklake_snapshot\"");
        let snapshot_count: i64 = sqlx::query_scalar(AssertSqlSafe(sql))
            .fetch_one(&pool)
            .await
            .context("Failed to count DuckLake snapshots")?;
        pool.close().await;

        Ok(Some(u64::try_from(snapshot_count).context("DuckLake snapshot count was negative")?))
    }
}

/// Optionally measures one adjacent-file compaction after the copy completes.
async fn run_ducklake_compaction(args: &RunArgs) -> Result<Option<DuckLakeCompactionReport>> {
    if !args.ducklake_compact_after_copy {
        return Ok(None);
    }
    if args.destination.destination != DestinationType::DuckLake {
        bail!("--ducklake-compact-after-copy requires a DuckLake destination");
    }

    #[cfg(not(feature = "ducklake"))]
    bail!("DuckLake compaction requires the etl-benchmarks ducklake feature");

    #[cfg(feature = "ducklake")]
    {
        let catalog_url = args
            .destination
            .ducklake_catalog_url
            .as_deref()
            .context("DuckLake catalog URL is required for compaction")?;
        let data_path = args
            .destination
            .ducklake_data_path
            .as_deref()
            .context("DuckLake data path is required for compaction")?;
        let catalog_url = Url::parse(catalog_url).context("Invalid DuckLake catalog URL")?;
        let data_path = Url::parse(data_path).context("Invalid DuckLake data path")?;
        if data_path.scheme() != "file" {
            bail!("Post-copy compaction currently supports local DuckLake file data paths only");
        }

        let writer_config = DuckLakeWriterConfig::default();
        let target_file_size = writer_config.target_file_size().to_owned();
        let max_tables_per_run = u32::try_from(args.table_ids.len()).unwrap_or(u32::MAX).max(1);
        let maintenance_config = DuckLakeMaintenanceConfig {
            catalog_url,
            data_path,
            s3: None,
            metadata_schema: None,
            writer_config,
            inline_flush: InlineFlushMaintenanceConfig { enabled: false, min_inlined_bytes: 0 },
            merge_adjacent_files: MergeAdjacentFilesMaintenanceConfig {
                enabled: true,
                max_compacted_files: 40,
                max_tables_per_run,
                target_file_size,
            },
            rewrite_data_files: RewriteDataFilesMaintenanceConfig {
                enabled: false,
                min_active_data_files: 0,
                max_tables_per_run,
            },
            expire_snapshots: ExpireSnapshotsMaintenanceConfig {
                enabled: false,
                older_than: "7 days".to_owned(),
            },
            cleanup_old_files: CleanupOldFilesMaintenanceConfig { enabled: false },
        };
        let started = Instant::now();
        let outcome = run_maintenance_once(maintenance_config)
            .await
            .context("DuckLake post-copy compaction failed")?;
        let duration_ms = duration_millis(started.elapsed());
        let snapshot_count_after = collect_ducklake_snapshot_count(&args.destination).await?;

        Ok(Some(DuckLakeCompactionReport {
            duration_ms,
            merge_adjacent_files_tables: outcome.merge_adjacent_files_tables,
            merge_adjacent_files_created: outcome.merge_adjacent_files_created,
            snapshot_count_after,
        }))
    }
}

/// Collects Parquet file metrics for a local DuckLake benchmark data path.
fn collect_ducklake_file_stats(destination: &DestinationArgs) -> Result<Option<DuckLakeFileStats>> {
    if destination.destination != DestinationType::DuckLake {
        return Ok(None);
    }

    let data_path = destination
        .ducklake_data_path
        .as_deref()
        .context("DuckLake data path is required for file statistics")?;
    let url = url::Url::parse(data_path).context("Invalid DuckLake data path")?;
    if url.scheme() != "file" {
        return Ok(None);
    }
    let root = url
        .to_file_path()
        .map_err(|_| anyhow::anyhow!("DuckLake file data path is not a valid local path"))?;
    let mut file_bytes = Vec::new();
    collect_parquet_file_bytes(&root, &mut file_bytes)?;
    file_bytes.sort_unstable();
    if file_bytes.is_empty() {
        return Ok(Some(DuckLakeFileStats {
            file_count: 0,
            total_bytes: 0,
            min_bytes: 0,
            median_bytes: 0,
            p95_bytes: 0,
            max_bytes: 0,
            mean_bytes: 0.0,
        }));
    }

    let total_bytes = file_bytes.iter().sum();
    Ok(Some(DuckLakeFileStats {
        file_count: file_bytes.len(),
        total_bytes,
        min_bytes: file_bytes[0],
        median_bytes: percentile(&file_bytes, 50),
        p95_bytes: percentile(&file_bytes, 95),
        max_bytes: file_bytes[file_bytes.len() - 1],
        mean_bytes: total_bytes as f64 / file_bytes.len() as f64,
    }))
}

/// Recursively collects Parquet file sizes below one local directory.
fn collect_parquet_file_bytes(path: &Path, file_bytes: &mut Vec<u64>) -> Result<()> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("Failed to read DuckLake data directory {}", path.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_parquet_file_bytes(&entry.path(), file_bytes)?;
        } else if entry.path().extension().is_some_and(|extension| extension == "parquet") {
            file_bytes.push(entry.metadata()?.len());
        }
    }
    Ok(())
}

/// Returns the nearest-rank percentile from sorted unsigned values.
fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100).max(1);
    sorted[rank - 1]
}
