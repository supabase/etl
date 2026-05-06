//! One-shot DuckLake maintenance execution for Kubernetes maintenance jobs.

use std::sync::Arc;

use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use pg_escape::{quote_identifier, quote_literal};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::Semaphore;
use tracing::info;
use url::Url;

use crate::ducklake::{
    LAKE_CATALOG, S3Config,
    client::{
        DuckDbBlockingOperationKind, DuckLakeConnectionManager, DuckLakeInterruptRegistry,
        build_warm_ducklake_pool, format_query_error_detail, run_duckdb_blocking,
    },
    config::{
        MAINTENANCE_TARGET_FILE_SIZE, build_setup_plan, current_duckdb_extension_strategy,
        maintenance_target_file_size_sql,
    },
    inline_size::DuckLakePendingInlineSizeSampler,
    maintenance::flush_table_inlined_data,
    metrics::{
        DuckLakeTableStorageMetrics, query_table_storage_metrics,
        resolve_ducklake_metadata_schema_blocking,
    },
};

#[derive(Clone)]
struct DuckDbMaintenanceExecutor {
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
}

impl DuckDbMaintenanceExecutor {
    async fn run<R, F>(&self, operation: F) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            DuckDbBlockingOperationKind::Maintenance,
            operation,
        )
        .await
    }
}

/// Configuration for one external DuckLake maintenance run.
#[derive(Clone, Debug)]
pub struct DuckLakeMaintenanceConfig {
    /// DuckLake PostgreSQL catalog URL.
    pub catalog_url: Url,
    /// DuckLake data path.
    pub data_path: Url,
    /// DuckDB connection pool size for the one-shot runner.
    pub pool_size: u32,
    /// Optional S3-compatible storage config.
    pub s3: Option<S3Config>,
    /// Optional DuckLake metadata schema.
    pub metadata_schema: Option<String>,
    /// Optional DuckDB memory cache limit.
    pub duckdb_memory_cache_limit: Option<String>,
    /// DuckLake `target_file_size` used by compaction.
    pub maintenance_target_file_size: Option<String>,
    /// Inline flush operation config.
    pub inline_flush: InlineFlushMaintenanceConfig,
    /// Merge-adjacent-files operation config.
    pub merge_adjacent_files: MergeAdjacentFilesMaintenanceConfig,
    /// Rewrite-data-files operation config.
    pub rewrite_data_files: RewriteDataFilesMaintenanceConfig,
}

/// Inline flush operation config.
#[derive(Clone, Copy, Debug)]
pub struct InlineFlushMaintenanceConfig {
    /// Whether inline flush is enabled.
    pub enabled: bool,
    /// Minimum pending inlined bytes before flushing a table.
    pub min_inlined_bytes: u64,
}

/// Merge-adjacent-files operation config.
#[derive(Clone, Debug)]
pub struct MergeAdjacentFilesMaintenanceConfig {
    /// Whether merge-adjacent-files is enabled.
    pub enabled: bool,
    /// Maximum compacted output files per table.
    pub max_compacted_files: u32,
    /// Maximum tables selected in one run.
    pub max_tables_per_run: u32,
    /// Target file size used during compaction.
    pub target_file_size: String,
}

/// Rewrite-data-files operation config.
#[derive(Clone, Copy, Debug)]
pub struct RewriteDataFilesMaintenanceConfig {
    /// Whether rewrite-data-files is enabled.
    pub enabled: bool,
    /// Minimum active data-file count before rewrite is attempted.
    pub min_active_data_files: i64,
    /// Maximum tables selected in one run.
    pub max_tables_per_run: u32,
}

/// Structured outcome for one external maintenance run.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DuckLakeMaintenanceOutcome {
    /// Tables whose inline data was flushed.
    pub inline_flush_tables: u32,
    /// Rows flushed from inlined storage.
    pub inline_flush_rows: u64,
    /// Tables passed to merge-adjacent-files.
    pub merge_adjacent_files_tables: u32,
    /// Files created by merge-adjacent-files.
    pub merge_adjacent_files_created: u64,
    /// Tables passed to rewrite-data-files.
    pub rewrite_data_files_tables: u32,
    /// Files created by rewrite-data-files.
    pub rewrite_data_files_created: u64,
}

impl DuckLakeMaintenanceOutcome {
    /// Returns whether any operation did work.
    pub fn applied(&self) -> bool {
        self.inline_flush_rows > 0
            || self.merge_adjacent_files_created > 0
            || self.rewrite_data_files_tables > 0
            || self.rewrite_data_files_created > 0
    }
}

/// Runs one external DuckLake maintenance attempt.
pub async fn run_maintenance_once(
    config: DuckLakeMaintenanceConfig,
) -> EtlResult<DuckLakeMaintenanceOutcome> {
    validate_config(&config)?;

    info!(
        pool_size = config.pool_size,
        metadata_schema = config.metadata_schema.as_deref(),
        inline_flush_enabled = config.inline_flush.enabled,
        inline_flush_min_inlined_bytes = config.inline_flush.min_inlined_bytes,
        merge_adjacent_files_enabled = config.merge_adjacent_files.enabled,
        merge_adjacent_files_max_compacted_files = config.merge_adjacent_files.max_compacted_files,
        merge_adjacent_files_max_tables_per_run = config.merge_adjacent_files.max_tables_per_run,
        merge_adjacent_files_target_file_size = %config.merge_adjacent_files.target_file_size,
        rewrite_data_files_enabled = config.rewrite_data_files.enabled,
        rewrite_data_files_min_active_data_files = config.rewrite_data_files.min_active_data_files,
        rewrite_data_files_max_tables_per_run = config.rewrite_data_files.max_tables_per_run,
        "ducklake external maintenance runner configured"
    );

    let duckdb = open_maintenance_executor(&config).await?;
    let metadata_schema = match config.metadata_schema.clone() {
        Some(metadata_schema) => metadata_schema,
        None => resolve_metadata_schema(&duckdb).await?,
    };
    info!(
        metadata_schema = %metadata_schema,
        "ducklake external maintenance metadata schema resolved"
    );
    let metadata_pg_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(config.catalog_url.as_str())
        .map_err(|source| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "DuckLake catalog metadata pool configuration failed",
                source: source
            )
        })?;
    let table_names = list_ducklake_tables(&metadata_pg_pool, &metadata_schema).await?;
    info!(
        table_count = table_names.len(),
        tables = ?table_names,
        "ducklake external maintenance discovered active tables"
    );
    let mut outcome = DuckLakeMaintenanceOutcome::default();

    if config.inline_flush.enabled {
        run_inline_flush(
            &duckdb,
            &metadata_pg_pool,
            &metadata_schema,
            &table_names,
            config.inline_flush,
            &mut outcome,
        )
        .await?;
    }

    if config.merge_adjacent_files.enabled {
        run_merge_adjacent_files(
            &duckdb,
            &metadata_pg_pool,
            &metadata_schema,
            &table_names,
            &config.merge_adjacent_files,
            &mut outcome,
        )
        .await?;
    }

    if config.rewrite_data_files.enabled {
        merge_adjacent_files_for_rewrite(&duckdb).await?;
        run_rewrite_data_files(
            &duckdb,
            &metadata_pg_pool,
            &metadata_schema,
            &table_names,
            config.rewrite_data_files,
            &mut outcome,
        )
        .await?;
    }

    info!(outcome = ?outcome, applied = outcome.applied(), "ducklake external maintenance completed");
    Ok(outcome)
}

/// Validates one maintenance runner config.
fn validate_config(config: &DuckLakeMaintenanceConfig) -> EtlResult<()> {
    if !matches!(config.catalog_url.scheme(), "postgres" | "postgresql") {
        return Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake external maintenance requires a PostgreSQL catalog",
            format!("unsupported catalog URL scheme `{}`", config.catalog_url.scheme())
        ));
    }
    if config.pool_size == 0 {
        return Err(etl_error!(
            ErrorKind::ConfigError,
            "DuckLake external maintenance pool size must be greater than zero"
        ));
    }
    Ok(())
}

/// Opens initialized DuckDB connections for maintenance.
async fn open_maintenance_executor(
    config: &DuckLakeMaintenanceConfig,
) -> EtlResult<DuckDbMaintenanceExecutor> {
    let extension_strategy = current_duckdb_extension_strategy()?;
    let target_file_size = config
        .maintenance_target_file_size
        .as_deref()
        .or(Some(config.merge_adjacent_files.target_file_size.as_str()))
        .unwrap_or(MAINTENANCE_TARGET_FILE_SIZE);
    info!(target_file_size, "opening ducklake external maintenance connection");
    let setup_plan = Arc::new(build_setup_plan(
        &config.catalog_url,
        &config.data_path,
        config.s3.as_ref(),
        config.metadata_schema.as_deref(),
        config.duckdb_memory_cache_limit.as_deref(),
    )?);
    let manager = DuckLakeConnectionManager {
        setup_plan,
        disable_extension_autoload: extension_strategy.disables_autoload(),
        interrupt_registry: Arc::new(DuckLakeInterruptRegistry::default()),
        #[cfg(feature = "test-utils")]
        open_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let pool = Arc::new(
        build_warm_ducklake_pool(manager, config.pool_size, "external-maintenance").await?,
    );
    let blocking_slots = Arc::new(Semaphore::new(config.pool_size as usize));
    let executor = DuckDbMaintenanceExecutor { pool, blocking_slots };
    let sql = maintenance_target_file_size_sql(Some(target_file_size));
    executor
        .run(move |conn| {
            conn.execute_batch(&sql).map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake target_file_size configuration failed",
                    format_query_error_detail(&sql, &source),
                    source: source
                )
            })?;
            Ok(())
        })
        .await?;
    info!(target_file_size, "ducklake external maintenance connection ready");
    Ok(executor)
}

/// Resolves the hidden DuckLake metadata schema.
async fn resolve_metadata_schema(duckdb: &DuckDbMaintenanceExecutor) -> EtlResult<String> {
    duckdb.run(resolve_ducklake_metadata_schema_blocking).await
}

/// Lists active DuckLake table names from the metadata catalog.
async fn list_ducklake_tables(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
) -> EtlResult<Vec<String>> {
    let sql = format!(
        "SELECT table_name FROM {}.{} WHERE end_snapshot IS NULL ORDER BY table_name",
        quote_identifier(metadata_schema),
        quote_identifier("ducklake_table")
    );
    let rows: Vec<(String,)> =
        sqlx::query_as(&sql).fetch_all(metadata_pg_pool).await.map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake table list query failed",
                format!("metadata_schema={metadata_schema}"),
                source: source
            )
        })?;
    Ok(rows.into_iter().map(|(table_name,)| table_name).collect())
}

/// Runs inline flush for tables that crossed the pending-inline threshold.
async fn run_inline_flush(
    duckdb: &DuckDbMaintenanceExecutor,
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    config: InlineFlushMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        min_inlined_bytes = config.min_inlined_bytes,
        table_count = table_names.len(),
        "ducklake inline flush evaluation started"
    );
    let sampler =
        DuckLakePendingInlineSizeSampler::new(metadata_schema.to_owned(), metadata_pg_pool.clone());
    for table_name in table_names {
        let sizes = sampler.sample_table(table_name).await?;
        if sizes.inlined_bytes < config.min_inlined_bytes {
            info!(
                table = %table_name,
                inlined_bytes = sizes.inlined_bytes,
                min_inlined_bytes = config.min_inlined_bytes,
                "ducklake inline flush skipped below threshold"
            );
            continue;
        }
        info!(
            table = %table_name,
            inlined_bytes = sizes.inlined_bytes,
            min_inlined_bytes = config.min_inlined_bytes,
            "ducklake inline flush executing"
        );
        let table_name_for_query = table_name.clone();
        let rows =
            duckdb.run(move |conn| flush_table_inlined_data(conn, &table_name_for_query)).await?;
        outcome.inline_flush_tables = outcome.inline_flush_tables.saturating_add(1);
        outcome.inline_flush_rows = outcome.inline_flush_rows.saturating_add(rows);
        info!(
            table = %table_name,
            rows,
            "ducklake inline flush completed"
        );
    }
    info!(
        inline_flush_tables = outcome.inline_flush_tables,
        inline_flush_rows = outcome.inline_flush_rows,
        "ducklake inline flush evaluation finished"
    );
    Ok(())
}

/// Runs bounded merge-adjacent-files on selected tables.
async fn run_merge_adjacent_files(
    duckdb: &DuckDbMaintenanceExecutor,
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    config: &MergeAdjacentFilesMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        max_compacted_files = config.max_compacted_files,
        max_tables_per_run = config.max_tables_per_run,
        table_count = table_names.len(),
        "ducklake merge-adjacent-files evaluation started"
    );
    let selected = select_merge_tables(
        metadata_pg_pool,
        metadata_schema,
        table_names,
        config.max_tables_per_run,
    )
    .await?;
    info!(
        selected_tables = ?selected,
        selected_count = selected.len(),
        "ducklake merge-adjacent-files selected tables"
    );
    for table_name in selected {
        info!(
            table = %table_name,
            max_compacted_files = config.max_compacted_files,
            "ducklake merge-adjacent-files executing"
        );
        let table_name_for_query = table_name.clone();
        let max_compacted_files = config.max_compacted_files;
        let files_created = duckdb
            .run(move |conn| merge_adjacent_files(conn, &table_name_for_query, max_compacted_files))
            .await?;
        outcome.merge_adjacent_files_tables = outcome.merge_adjacent_files_tables.saturating_add(1);
        outcome.merge_adjacent_files_created =
            outcome.merge_adjacent_files_created.saturating_add(files_created);
        info!(
            table = %table_name,
            files_created,
            "ducklake merge-adjacent-files completed"
        );
    }
    Ok(())
}

/// Runs DuckLake's whole-lake adjacent-file merge before rewrite-data-files.
async fn merge_adjacent_files_for_rewrite(duckdb: &DuckDbMaintenanceExecutor) -> EtlResult<()> {
    let sql = format!("CALL ducklake_merge_adjacent_files({});", quote_literal(LAKE_CATALOG));
    info!(
        sql = %sql,
        "ducklake rewrite-triggered merge-adjacent-files executing"
    );
    duckdb
        .run(move |conn| {
            conn.execute_batch(&sql).map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake rewrite-triggered merge adjacent files failed",
                    format_query_error_detail(&sql, &source),
                    source: source
                )
            })?;
            Ok(())
        })
        .await?;
    info!("ducklake rewrite-triggered merge-adjacent-files completed");
    Ok(())
}

/// Runs bounded rewrite-data-files on selected tables.
async fn run_rewrite_data_files(
    duckdb: &DuckDbMaintenanceExecutor,
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    config: RewriteDataFilesMaintenanceConfig,
    outcome: &mut DuckLakeMaintenanceOutcome,
) -> EtlResult<()> {
    info!(
        min_active_data_files = config.min_active_data_files,
        max_tables_per_run = config.max_tables_per_run,
        table_count = table_names.len(),
        "ducklake rewrite-data-files evaluation started"
    );
    let selected = select_rewrite_tables(
        metadata_pg_pool,
        metadata_schema,
        table_names,
        config.min_active_data_files,
        config.max_tables_per_run,
    )
    .await?;
    info!(
        selected_tables = ?selected,
        selected_count = selected.len(),
        "ducklake rewrite-data-files selected tables"
    );
    for table_name in selected {
        info!(
            table = %table_name,
            "ducklake rewrite-data-files executing"
        );
        let table_name_for_query = table_name.clone();
        let files_created =
            duckdb.run(move |conn| rewrite_data_files(conn, &table_name_for_query)).await?;
        outcome.rewrite_data_files_tables = outcome.rewrite_data_files_tables.saturating_add(1);
        outcome.rewrite_data_files_created =
            outcome.rewrite_data_files_created.saturating_add(files_created);
        info!(
            table = %table_name,
            files_created,
            "ducklake rewrite-data-files completed"
        );
    }
    Ok(())
}

/// Selects tables with small-file pressure for merge-adjacent-files.
async fn select_merge_tables(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    max_tables_per_run: u32,
) -> EtlResult<Vec<String>> {
    let mut selected = Vec::new();
    for table_name in table_names {
        if is_etl_internal_table(table_name) {
            info!(
                table = %table_name,
                "ducklake rewrite-data-files table skipped because it is internal ETL metadata"
            );
            continue;
        }

        let metrics =
            query_table_storage_metrics(metadata_pg_pool, metadata_schema, table_name).await?;
        if metrics.active_data_files > 1 && metrics.small_file_ratio() > 0.0 {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                small_file_ratio = metrics.small_file_ratio(),
                "ducklake merge-adjacent-files table selected"
            );
            selected.push(table_name.clone());
        } else {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                small_file_ratio = metrics.small_file_ratio(),
                "ducklake merge-adjacent-files table skipped"
            );
        }
        if selected.len() >= max_tables_per_run as usize {
            break;
        }
    }
    Ok(selected)
}

/// Selects tables with delete pressure for rewrite-data-files.
async fn select_rewrite_tables(
    metadata_pg_pool: &PgPool,
    metadata_schema: &str,
    table_names: &[String],
    min_active_data_files: i64,
    max_tables_per_run: u32,
) -> EtlResult<Vec<String>> {
    let mut selected = Vec::new();
    for table_name in table_names {
        if is_etl_internal_table(table_name) {
            info!(
                table = %table_name,
                "ducklake rewrite-data-files table skipped because it is internal ETL metadata"
            );
            continue;
        }

        let metrics =
            query_table_storage_metrics(metadata_pg_pool, metadata_schema, table_name).await?;
        if should_rewrite(&metrics, min_active_data_files) {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                active_delete_files = metrics.active_delete_files,
                deleted_row_ratio = metrics.deleted_row_ratio(),
                min_active_data_files,
                "ducklake rewrite-data-files table selected"
            );
            selected.push(table_name.clone());
        } else {
            info!(
                table = %table_name,
                active_data_files = metrics.active_data_files,
                active_delete_files = metrics.active_delete_files,
                deleted_row_ratio = metrics.deleted_row_ratio(),
                min_active_data_files,
                "ducklake rewrite-data-files table skipped"
            );
        }
        if selected.len() >= max_tables_per_run as usize {
            break;
        }
    }
    Ok(selected)
}

fn is_etl_internal_table(table_name: &str) -> bool {
    table_name.starts_with("__etl_")
}

/// Returns whether a table should be rewritten.
fn should_rewrite(metrics: &DuckLakeTableStorageMetrics, min_active_data_files: i64) -> bool {
    metrics.active_data_files > min_active_data_files
}

/// Calls DuckLake merge-adjacent-files for one table.
fn merge_adjacent_files(
    conn: &duckdb::Connection,
    table_name: &str,
    max_compacted_files: u32,
) -> EtlResult<u64> {
    let sql = format!(
        "SELECT COALESCE(SUM(files_created), 0) FROM ducklake_merge_adjacent_files({}, {}, \
         max_compacted_files => {});",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
        max_compacted_files
    );
    count_maintenance_files(conn, &sql, "DuckLake merge adjacent files failed")
}

/// Calls DuckLake rewrite-data-files for one table.
fn rewrite_data_files(conn: &duckdb::Connection, table_name: &str) -> EtlResult<u64> {
    let sql = format!(
        "CALL ducklake_rewrite_data_files({}, {});",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name)
    );
    conn.execute_batch(&sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake rewrite data files failed",
            format_query_error_detail(&sql, &source),
            source: source
        )
    })?;
    Ok(0)
}

/// Counts files returned by one DuckLake maintenance function.
fn count_maintenance_files(
    conn: &duckdb::Connection,
    sql: &str,
    description: &'static str,
) -> EtlResult<u64> {
    let files_created: i64 = conn.query_row(sql, [], |row| row.get(0)).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            description,
            format_query_error_detail(sql, &source),
            source: source
        )
    })?;
    Ok(files_created.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics(
        active_data_files: i64,
        active_delete_files: i64,
        deleted_rows: i64,
    ) -> DuckLakeTableStorageMetrics {
        DuckLakeTableStorageMetrics {
            active_data_files,
            active_data_bytes: 100,
            small_data_files: 0,
            active_data_rows: 100,
            active_delete_files,
            active_delete_bytes: 10,
            deleted_rows,
        }
    }

    #[test]
    fn should_rewrite_requires_only_file_count() {
        assert!(!should_rewrite(&metrics(39, 1, 50), 40));
        assert!(!should_rewrite(&metrics(40, 1, 50), 40));
        assert!(should_rewrite(&metrics(41, 0, 0), 40));
    }

    #[test]
    fn outcome_reports_applied_work() {
        assert!(!DuckLakeMaintenanceOutcome::default().applied());
        assert!(
            DuckLakeMaintenanceOutcome {
                inline_flush_rows: 1,
                ..DuckLakeMaintenanceOutcome::default()
            }
            .applied()
        );
        assert!(
            DuckLakeMaintenanceOutcome {
                rewrite_data_files_tables: 1,
                ..DuckLakeMaintenanceOutcome::default()
            }
            .applied()
        );
    }
}
