//! Merge scheduler that periodically processes changelog tables.
//!
//! Runs on a fixed time interval, iterating through configured tables sequentially,
//! executing merge operations, and handling errors with retry logic.

use crate::config::{MergerConfig, TableConfig};
use crate::merge::MergeExecutor;
use metrics::{counter, histogram};
use std::time::Instant;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info};

/// Runs the merge scheduler loop.
///
/// Executes merge operations for all configured tables at regular intervals
/// until a shutdown signal is received. Handles errors gracefully and continues
/// processing remaining tables even if one fails.
pub async fn run_scheduler(config: MergerConfig) -> anyhow::Result<()> {
    info!(
        interval_secs = config.merge_interval.as_secs(),
        table_count = config.tables.len(),
        "starting merge scheduler"
    );

    // Set up signal handlers for graceful shutdown
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    // Create merge executor
    let executor = MergeExecutor::new(config.iceberg.clone(), config.batch_size).await?;

    let mut interval = tokio::time::interval(config.merge_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let start = Instant::now();
                info!("starting merge cycle");

                let mut success_count = 0;
                let mut failure_count = 0;

                for table_config in &config.tables {
                    match process_table(&executor, table_config).await {
                        Ok(()) => {
                            success_count += 1;
                            counter!("merger.table.success", "table" => table_config.mirror_table.clone()).increment(1);
                        }
                        Err(err) => {
                            failure_count += 1;
                            error!(
                                namespace = %table_config.namespace,
                                changelog_table = %table_config.changelog_table,
                                mirror_table = %table_config.mirror_table,
                                %err,
                                "failed to merge table"
                            );
                            counter!("merger.table.failure", "table" => table_config.mirror_table.clone()).increment(1);
                        }
                    }
                }

                let duration = start.elapsed();
                histogram!("merger.cycle.duration_seconds").record(duration.as_secs_f64());
                counter!("merger.cycle.completed").increment(1);

                info!(
                    duration_secs = duration.as_secs(),
                    success_count,
                    failure_count,
                    "merge cycle completed"
                );
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down gracefully");
                break;
            }
            _ = sigint.recv() => {
                info!("received SIGINT, shutting down gracefully");
                break;
            }
        }
    }

    info!("merge scheduler shutdown complete");
    Ok(())
}

/// Processes a single table by executing merge operation.
///
/// Creates the mirror table if it doesn't exist, then executes the merge
/// to deduplicate and compact changelog records.
async fn process_table(executor: &MergeExecutor, table_config: &TableConfig) -> anyhow::Result<()> {
    let start = Instant::now();

    info!(
        namespace = %table_config.namespace,
        changelog_table = %table_config.changelog_table,
        mirror_table = %table_config.mirror_table,
        "processing table"
    );

    executor.execute_merge(table_config).await?;

    let duration = start.elapsed();
    histogram!("merger.table.duration_seconds", "table" => table_config.mirror_table.clone())
        .record(duration.as_secs_f64());

    info!(
        namespace = %table_config.namespace,
        mirror_table = %table_config.mirror_table,
        duration_secs = duration.as_secs(),
        "table processing completed"
    );

    Ok(())
}
