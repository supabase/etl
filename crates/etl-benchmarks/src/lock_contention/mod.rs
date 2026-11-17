//! Lock-contention profile of a pipeline that copies tables while CDC streams.
//!
//! The run uses [`PostgresStore`], so the store's cache lock is on the path,
//! and keeps writers busy during the initial copy, so table-sync workers and
//! the apply worker run at the same time. [`profiler`] records how long tasks
//! wait for each tokio lock.

mod profiler;

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use etl::{
    pipeline::Pipeline,
    store::{PostgresStore, StateStore, TableStateType},
};
use etl_config::shared::{TableSyncCopyConfig, Validate};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::task::JoinSet;

use crate::{
    common::{
        CountingDestination, NullDestination, PgConnectionArgs, PipelineTuningArgs,
        cleanup_replication_slots, finish_run, format_integer, pg_pool, pipeline_config,
    },
    lock_contention::profiler::{LockKind, LockReport, LockReportRow},
};

/// Source schema recreated by every run.
const SCHEMA: &str = "lock_contention";
/// Publication recreated by every run.
const PUBLICATION: &str = "lock_contention_pub";
/// Statements per writer transaction.
const STATEMENTS_PER_TRANSACTION: u64 = 10;
/// Every fourth writer statement inserts; the others update.
const INSERT_EVERY: u64 = 4;
/// Interval between table-state checks while waiting for the copy.
const READY_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Quiet period with no new CDC events before the stream counts as drained.
const DRAIN_QUIET: Duration = Duration::from_secs(2);
/// Interval between CDC event-count checks while draining.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Connections the benchmark pool keeps for setup next to the writers.
const SETUP_CONNECTIONS: u16 = 1;
/// Maximum connections of [`pg_pool`].
const MAX_POOL_CONNECTIONS: u16 = 16;

/// Command-line arguments for the lock-contention profile.
#[derive(Parser)]
#[command(author, version, about = "Profile tokio lock waits in a copy-plus-CDC pipeline run")]
pub struct Args {
    /// Source Postgres connection.
    #[command(flatten)]
    pg: PgConnectionArgs,
    /// Pipeline tuning.
    #[command(flatten)]
    tuning: PipelineTuningArgs,
    /// Source tables to create and copy.
    #[arg(long, default_value_t = 8)]
    tables: u16,
    /// Rows loaded into each table before the copy.
    #[arg(long, default_value_t = 1_000_000)]
    rows_per_table: u32,
    /// Concurrent writers producing CDC during and after the copy.
    #[arg(long, default_value_t = 4)]
    writers: u16,
    /// How long the writers run, starting with the pipeline.
    #[arg(long, default_value_t = 30)]
    duration_seconds: u64,
    /// Longest time to wait for every table to finish copying.
    #[arg(long, default_value_t = 600)]
    ready_timeout_seconds: u64,
    /// Lock operations shown per section.
    #[arg(long, default_value_t = 15)]
    top: usize,
}

impl Args {
    /// Rejects values that cannot produce a meaningful run.
    fn validate(&self) -> Result<()> {
        ensure!(self.tables > 0, "--tables must be greater than 0");
        ensure!(self.rows_per_table > 0, "--rows-per-table must be greater than 0");
        ensure!(
            (1..=MAX_POOL_CONNECTIONS - SETUP_CONNECTIONS).contains(&self.writers),
            "--writers must be between 1 and {}",
            MAX_POOL_CONNECTIONS - SETUP_CONNECTIONS
        );
        ensure!(self.duration_seconds > 0, "--duration-seconds must be greater than 0");
        ensure!(self.ready_timeout_seconds > 0, "--ready-timeout-seconds must be greater than 0");
        Ok(())
    }
}

/// Measurements of one profiled run.
struct RunSummary {
    /// Pipeline start until every table was ready.
    ready_after: Duration,
    /// Writer statements committed.
    statements: u64,
    /// Rows written by the initial copy.
    copied_rows: u64,
    /// Row-level CDC events delivered to the destination.
    cdc_events: u64,
    /// Pipeline start until shutdown finished.
    profiled_for: Duration,
}

/// Runs the lock-contention profile.
pub async fn main() -> Result<()> {
    let args = Args::parse();
    args.validate()?;
    if !cfg!(tokio_unstable) {
        bail!(
            "tokio lock instrumentation needs RUSTFLAGS=\"--cfg tokio_unstable\"; run it through \
             `nix run .#lock-contention` or scripts/lock-contention.sh"
        );
    }
    profiler::install()?;

    let pool = pg_pool(&args.pg).await?;
    let table_ids = prepare_source(&pool, &args).await?;
    let pipeline_id = unique_pipeline_id()?;

    let run_result = run_pipeline(&args, &pool, &pipeline_id).await;
    let cleanup_result = cleanup_replication_slots(&args.pg, &pipeline_id, &table_ids).await;
    let (summary, report) = finish_run(run_result, [cleanup_result])?;

    ensure!(
        report.acquires() > 0,
        "tokio reported no lock acquires; the binary was not built with --cfg tokio_unstable and \
         the `lock-contention` feature"
    );
    print_report(&args, &summary, &report);

    Ok(())
}

/// Returns a pipeline ID that no earlier run's store state or slots use.
fn unique_pipeline_id() -> Result<String> {
    let seconds = SystemTime::now().duration_since(UNIX_EPOCH).context("clock before 1970")?;
    Ok(format!("lc{}", seconds.as_secs()))
}

/// Recreates the source tables and publication, returning the table IDs.
async fn prepare_source(pool: &PgPool, args: &Args) -> Result<Vec<u32>> {
    let rows = i64::from(args.rows_per_table);
    execute(pool, format!("drop schema if exists {SCHEMA} cascade")).await?;
    execute(pool, format!("drop publication if exists {PUBLICATION}")).await?;
    execute(pool, format!("create schema {SCHEMA}")).await?;

    let mut table_names = Vec::with_capacity(usize::from(args.tables));
    for index in 0..args.tables {
        let table_name = format!("{SCHEMA}.t_{index}");
        execute(
            pool,
            format!(
                "create table {table_name} (id bigint generated by default as identity primary \
                 key, k bigint not null, payload text not null)"
            ),
        )
        .await?;
        sqlx::query(AssertSqlSafe(format!(
            "insert into {table_name} (k, payload) select 0, md5(g::text) from generate_series(1, \
             $1) as g"
        )))
        .bind(rows)
        .execute(pool)
        .await
        .with_context(|| format!("failed to load {table_name}"))?;
        table_names.push(table_name);
    }
    execute(pool, format!("create publication {PUBLICATION} for table {}", table_names.join(", ")))
        .await?;

    let oids: Vec<i64> = sqlx::query_scalar(
        "select c.oid::int8 from pg_class c join pg_namespace n on n.oid = c.relnamespace where \
         n.nspname = $1 and c.relkind = 'r' order by c.oid",
    )
    .bind(SCHEMA)
    .fetch_all(pool)
    .await
    .context("failed to read source table IDs")?;
    oids.into_iter()
        .map(|oid| u32::try_from(oid).context("table OID does not fit in u32"))
        .collect()
}

/// Executes one setup statement.
async fn execute(pool: &PgPool, sql: String) -> Result<()> {
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .execute(pool)
        .await
        .with_context(|| format!("failed to run `{sql}`"))?;
    Ok(())
}

/// Runs the pipeline and writers, then returns the run summary and report.
async fn run_pipeline(
    args: &Args,
    pool: &PgPool,
    pipeline_id: &str,
) -> Result<(RunSummary, LockReport)> {
    let config = pipeline_config(
        pipeline_id,
        PUBLICATION.to_owned(),
        &args.pg,
        &args.tuning,
        TableSyncCopyConfig::IncludeAllTables,
    );
    config.validate().context("invalid pipeline config")?;
    let store = PostgresStore::new(pipeline_id, config.pg_connection.clone())
        .await
        .context("failed to open the Postgres store")?;
    let destination = CountingDestination::new(NullDestination);
    let mut pipeline = Pipeline::new(config, store.clone(), destination.clone());

    // Profile from pipeline start; setup locks are not part of the question.
    profiler::reset();
    let started = Instant::now();
    let mut writers =
        spawn_writers(pool, args, started + Duration::from_secs(args.duration_seconds));

    let start_result = pipeline.start().await.context("failed to start pipeline");
    let phase_result: Result<(Duration, u64)> = async {
        start_result?;
        let ready_after = wait_for_ready(&store, args).await?;

        let mut statements = 0;
        while let Some(result) = writers.join_next().await {
            statements += result.context("writer task panicked")??;
        }
        wait_for_cdc_quiescence(&destination).await;

        Ok((ready_after, statements))
    }
    .await;

    let shutdown_result =
        pipeline.shutdown_and_wait().await.context("failed to shut down pipeline");
    let profiled_for = started.elapsed();
    let report = profiler::snapshot();
    let (ready_after, statements) = finish_run(phase_result, [shutdown_result])?;

    let stats = destination.stats();
    let summary = RunSummary {
        ready_after,
        statements,
        copied_rows: stats.table_rows,
        cdc_events: stats.cdc_data_events,
        profiled_for,
    };

    Ok((summary, report))
}

/// Starts writers that update and insert rows until `deadline`.
fn spawn_writers(pool: &PgPool, args: &Args, deadline: Instant) -> JoinSet<Result<u64>> {
    let rows = i64::from(args.rows_per_table);
    let statements: Arc<[(Arc<str>, Arc<str>)]> = (0..args.tables)
        .map(|index| {
            let table_name = format!("{SCHEMA}.t_{index}");
            // The scalar subquery runs once, so the update uses the primary
            // key. A bare `random()` in the filter is volatile and
            // evaluated per row, which turns every update into a
            // sequential scan.
            let update = format!(
                "update {table_name} set k = k + 1 where id = (select 1 + floor(random() * \
                 $1)::bigint)"
            );
            let insert =
                format!("insert into {table_name} (k, payload) values (0, md5(random()::text))");
            (Arc::from(update), Arc::from(insert))
        })
        .collect();

    let mut writers = JoinSet::new();
    for writer in 0..u64::from(args.writers) {
        let pool = pool.clone();
        let statements = Arc::clone(&statements);
        writers.spawn(async move {
            let mut executed = 0_u64;
            while Instant::now() < deadline {
                let mut transaction =
                    pool.begin().await.context("failed to begin writer transaction")?;
                for _ in 0..STATEMENTS_PER_TRANSACTION {
                    let table = usize::try_from((writer + executed) % statements.len() as u64)
                        .context("table index does not fit in usize")?;
                    let (update, insert) = &statements[table];
                    let query = if executed % INSERT_EVERY == INSERT_EVERY - 1 {
                        sqlx::query(AssertSqlSafe(Arc::clone(insert)))
                    } else {
                        sqlx::query(AssertSqlSafe(Arc::clone(update))).bind(rows)
                    };
                    query.execute(&mut *transaction).await.context("writer statement failed")?;
                    executed += 1;
                }
                transaction.commit().await.context("failed to commit writer transaction")?;
            }
            Ok(executed)
        });
    }

    writers
}

/// Waits until every table is ready, failing if one errors or the copy times
/// out.
///
/// Polling reads the store through the same cache lock the pipeline uses, so it
/// adds one acquire per [`READY_POLL_INTERVAL`].
async fn wait_for_ready(store: &PostgresStore, args: &Args) -> Result<Duration> {
    let started = Instant::now();
    let timeout = Duration::from_secs(args.ready_timeout_seconds);
    loop {
        let states = store.get_table_states().await.context("failed to read table states")?;
        if let Some((table_id, _)) =
            states.iter().find(|(_, state)| state.as_type() == TableStateType::Errored)
        {
            bail!("table {table_id} entered the errored state during the copy");
        }
        let ready =
            states.values().filter(|state| state.as_type() == TableStateType::Ready).count();
        if ready == usize::from(args.tables) {
            return Ok(started.elapsed());
        }
        if started.elapsed() > timeout {
            bail!("only {ready} of {} tables were ready after {timeout:?}", args.tables);
        }
        tokio::time::sleep(READY_POLL_INTERVAL).await;
    }
}

/// Waits until no new CDC events arrive for [`DRAIN_QUIET`].
async fn wait_for_cdc_quiescence(destination: &CountingDestination<NullDestination>) {
    let mut last_events = destination.stats().cdc_data_events;
    let mut quiet_since = Instant::now();
    while quiet_since.elapsed() < DRAIN_QUIET {
        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        let events = destination.stats().cdc_data_events;
        if events != last_events {
            last_events = events;
            quiet_since = Instant::now();
        }
    }
}

/// Formats a wait with a unit suited to its size.
fn format_wait(wait: Duration) -> String {
    let nanos = wait.as_nanos();
    if nanos < 1_000 {
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        format!("{:.1} µs", wait.as_secs_f64() * 1e6)
    } else if nanos < 1_000_000_000 {
        format!("{:.1} ms", wait.as_secs_f64() * 1e3)
    } else {
        format!("{:.2} s", wait.as_secs_f64())
    }
}

/// Prints one section of report rows.
fn print_section(title: &str, rows: &[&LockReportRow], top: usize) {
    println!("  {title}");
    if rows.is_empty() {
        println!("    none acquired");
        return;
    }
    for (rank, row) in rows.iter().take(top).enumerate() {
        let wait_percent =
            if row.acquires == 0 { 0.0 } else { row.waits as f64 * 100.0 / row.acquires as f64 };
        println!("  {:>3}. {} {}  {}", rank + 1, row.kind, row.created_at, row.op);
        print!(
            "       {} acquires, {} waited ({wait_percent:.2}%)",
            format_integer(u128::from(row.acquires)),
            format_integer(u128::from(row.waits)),
        );
        if row.waits > 0 {
            print!(
                ", total {}, p50 <= {}, p99 <= {}, max {}",
                format_wait(row.wait_total),
                format_wait(row.wait_p50),
                format_wait(row.wait_p99),
                format_wait(row.wait_max),
            );
        }
        println!();
        if !row.call_sites.is_empty() {
            println!("       waiting call sites ({} sampled):", row.sampled_waits);
            for (site, count) in &row.call_sites {
                println!("         {count:>2}x {site}");
            }
        }
    }
    println!();
}

/// Prints the run summary and lock report.
fn print_report(args: &Args, summary: &RunSummary, report: &LockReport) {
    let (semaphores, locks): (Vec<_>, Vec<_>) =
        report.rows.iter().partition(|row| row.kind == LockKind::Semaphore);

    println!();
    println!("Lock contention profile");
    println!(
        "  Workload   {} tables x {} rows copied while {} writers ran for {} s",
        args.tables,
        format_integer(u128::from(args.rows_per_table)),
        args.writers,
        args.duration_seconds
    );
    println!(
        "  Pipeline   {} table-sync workers, {} copy connections per table",
        args.tuning.max_table_sync_workers, args.tuning.max_copy_connections_per_table
    );
    println!(
        "  Observed   {} rows copied, {} writer statements, {} CDC row events",
        format_integer(u128::from(summary.copied_rows)),
        format_integer(u128::from(summary.statements)),
        format_integer(u128::from(summary.cdc_events))
    );
    println!(
        "  Timings    tables ready after {}, profiled {} (pipeline start to shutdown)",
        format_wait(summary.ready_after),
        format_wait(summary.profiled_for)
    );
    println!(
        "  Acquires   {} tokio lock acquires profiled",
        format_integer(u128::from(report.acquires()))
    );
    println!("  Note       waits include tracing overhead; compare runs, not absolute numbers");
    println!();
    print_section("Mutex and RwLock, by total wait", &locks, args.top);
    print_section(
        "Semaphore, by total wait (waits are often intended limits)",
        &semaphores,
        args.top,
    );
}
