use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, Event, SizeHint, TableId, TableName, TableRow, TableSchema};
use metrics::{counter, gauge, histogram};
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
use rand::Rng;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_postgres::types::PgLsn;

use tracing::{Level, debug, info, warn};
use url::Url;

use crate::ducklake::client::{
    DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
    format_query_error_detail, run_duckdb_blocking,
};
use crate::ducklake::config::{
    DuckDbLogConfig, build_setup_sql, current_duckdb_extension_strategy,
};
use crate::ducklake::encoding::{
    PreparedRows, cell_to_sql_literal_ref, prepare_rows, table_row_to_sql_literal_ref,
};
use crate::ducklake::maintenance::{
    DuckLakeMaintenanceWorker, NOTIFICATION_SEND_TIMEOUT, TableMaintenanceNotification,
    spawn_ducklake_maintenance_worker, table_write_slot,
};
use crate::ducklake::metrics::{
    BATCH_KIND_LABEL, DELETE_ORIGIN_LABEL, DuckLakeMetricsSampler,
    ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS, ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
    ETL_DUCKLAKE_DELETE_PREDICATES, ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
    ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS, ETL_DUCKLAKE_INLINE_FLUSH_ROWS,
    ETL_DUCKLAKE_POOL_SIZE, ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL, ETL_DUCKLAKE_RETRIES_TOTAL,
    ETL_DUCKLAKE_UPSERT_ROWS, PREPARED_ROWS_KIND_LABEL, RESULT_LABEL, RETRY_SCOPE_LABEL,
    SUB_BATCH_KIND_LABEL, register_metrics, spawn_ducklake_metrics_sampler,
};
use crate::ducklake::schema::build_create_table_sql_ducklake;
use crate::ducklake::{DuckLakeTableName, LAKE_CATALOG, S3Config};
use crate::table_name::try_stringify_table_name;

/// Maximum number of rows per SQL `INSERT ... VALUES` batch when nested values
/// force the staging path to bypass DuckDB's appender API.
const SQL_INSERT_BATCH_SIZE: usize = 256;
/// Maximum number of primary-key predicates per SQL `DELETE` batch.
///
/// Keep this small so each delete statement remains cheap while still avoiding
/// one round-trip per deleted row.
const SQL_DELETE_BATCH_SIZE: usize = 16;
/// Maximum number of ordered CDC mutations grouped into one atomic DuckLake
/// transaction.
///
/// Keeping mixed insert/delete/update streams in the same batch improves
/// insert throughput on interleaved workloads while still capping transaction
/// lifetime for DuckLake conflict handling.
const CDC_MUTATION_BATCH_SIZE: usize = 128;
/// ETL-managed marker table storing per-table applied CDC batches.
const APPLIED_BATCHES_TABLE: &str = "__etl_applied_table_batches";
/// Inline small marker-table writes in the DuckLake metadata catalog instead of
/// creating Parquet files for this metadata-like table.
const APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// Enables the expensive post-commit row-count diagnostics when set.
const BATCH_DIAGNOSTICS_ENV_VAR: &str = "ETL_DUCKLAKE_BATCH_DIAGNOSTICS";

/// Event-level table mutations that must be applied in order.
enum TableMutation {
    Insert(TableRow),
    Delete(TableRow),
    Update {
        delete_row: TableRow,
        upsert_row: TableRow,
    },
    Replace(TableRow),
}

/// Prepared table mutations ready for execution and retries.
enum PreparedTableMutation {
    Upsert(PreparedRows),
    Delete {
        // For WHERE clause predicates used in DELETE statements.
        predicates: Vec<String>,
        // To know if it's coming from an update or delete operation.
        origin: &'static str,
    },
}

/// Event-level table mutation annotated with source LSNs for idempotent replay.
struct TrackedTableMutation {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    mutation: TableMutation,
}

/// Truncate event metadata preserved for idempotent replay.
#[derive(Clone, Copy)]
struct TrackedTruncateEvent {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    options: i8,
}

/// Stable hash used to derive per-table batch identifiers.
struct BatchIdHasher(u64);

impl BatchIdHasher {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Default for BatchIdHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for BatchIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

/// Atomic DuckLake batch kinds persisted in the replay marker table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckLakeTableBatchKind {
    Copy,
    Mutation,
    Truncate,
}

impl DuckLakeTableBatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Mutation => "mutation",
            Self::Truncate => "truncate",
        }
    }
}

/// Deterministic identity for one table batch.
struct DuckLakeBatchIdentity {
    batch_id: String,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
}

/// Prepared per-table work executed atomically in one DuckLake transaction.
enum PreparedDuckLakeTableBatchAction {
    Mutation(Vec<PreparedTableMutation>),
    Truncate,
}

/// Prepared atomic DuckLake table batch with replay metadata.
struct PreparedDuckLakeTableBatch {
    table_name: DuckLakeTableName,
    batch_id: String,
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    action: PreparedDuckLakeTableBatchAction,
}
/// Returns whether a DuckLake DDL error indicates another transaction already
/// created the requested table.
fn is_create_table_conflict(error: &duckdb::Error, table_name: &str) -> bool {
    let message = error.to_string();
    message.contains("has been created by another transaction already")
        && message.contains(&format!(r#"attempting to create table "{table_name}""#))
}

// ── destination ───────────────────────────────────────────────────────────────

/// A DuckLake destination that implements the ETL [`Destination`] trait.
///
/// Writes data to a DuckLake data lake. DuckDB connections are pre-initialized,
/// pooled, and bounded by a semaphore so operations can reuse attached lake
/// catalogs without oversubscribing Tokio's blocking threads. Data is persisted
/// as Parquet files at `data_path`; metadata is tracked in a PostgreSQL catalog
/// database.
///
/// All writes are wrapped in explicit transactions so that each batch of rows
/// is committed atomically in DuckLake while file materialization can be
/// deferred to background maintenance.
#[derive(Clone)]
pub struct DuckLakeDestination<S> {
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    maintenance_worker: Arc<Option<DuckLakeMaintenanceWorker>>,
    metrics_sampler: Arc<Option<DuckLakeMetricsSampler>>,
    table_creation_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    store: S,
    duckdb_log: Option<Arc<DuckDbLogConfig>>,
    /// Cache of table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
    /// Cache tracking whether the ETL batch marker table already exists. If it's set then the table has already been created
    applied_batches_table_created: Arc<AtomicBool>,
}

impl<S> Destination for DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "ducklake"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.shutdown_maintenance_worker().await?;
        self.shutdown_metrics_sampler().await?;
        self.dump_duckdb_logs_inner().await
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.truncate_table_inner(table_id).await
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_inner(table_id, table_rows).await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events_inner(events).await
    }
}

impl<S> DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new DuckLake destination.
    ///
    /// - `catalog_url`: DuckLake catalog location. Use a PostgreSQL URL
    ///   (`postgres://user:pass@localhost:5432/mydb`) or a local file URL
    ///   (`file:///tmp/catalog.ducklake`).
    /// - `data_path`: Where Parquet files are stored. Use a local file URL
    ///   (`file:///tmp/lake_data`) or a cloud URL (`s3://bucket/prefix/`,
    ///   `gs://bucket/prefix/`).
    /// - `pool_size`: Number of warm DuckDB connections maintained in the pool.
    ///   `4` is a reasonable default; higher values allow more tables to be
    ///   written in parallel.
    /// - `s3`: Optional S3 credentials. Required when `data_path` is an S3 URI
    ///   and the bucket is not publicly accessible.
    /// - `metadata_schema`: Optional Postgres schema for DuckLake metadata tables
    ///   (e.g. `"ducklake"`). Uses the catalog default schema when not set.
    /// - `duckdb_log`: Optional DuckDB log storage and shutdown dump paths.
    /// - On Linux, DuckDB extensions are loaded from vendored local files when
    ///   a vendored directory is available. The root directory can be forced
    ///   with `ETL_DUCKDB_EXTENSION_ROOT`. Otherwise, DuckDB uses the legacy
    ///   online `INSTALL` flow. On macOS and Windows, DuckDB always uses the
    ///   legacy online `INSTALL` flow.
    ///
    /// Pool initialization is blocking because DuckDB extensions are loaded and
    /// the lake catalog is attached synchronously. This constructor offloads
    /// that warm-up work to Tokio's blocking pool.
    pub async fn new(
        catalog_url: Url,
        data_path: Url,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        duckdb_log: Option<DuckDbLogConfig>,
        store: S,
    ) -> EtlResult<Self> {
        register_metrics();

        if pool_size == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "DuckLake pool size must be greater than zero",
                "pool_size must be at least 1"
            ));
        }

        let extension_strategy = current_duckdb_extension_strategy()?;
        let disable_extension_autoload = extension_strategy.disables_autoload();
        if let crate::ducklake::config::DuckDbExtensionStrategy::VendoredLinux { platform_dir } =
            extension_strategy
        {
            info!(platform = platform_dir, "using vendored duckdb extensions");
        }
        ensure_duckdb_log_paths(duckdb_log.as_ref())?;
        let setup_sql = Arc::new(build_setup_sql(
            &catalog_url,
            &data_path,
            s3.as_ref(),
            metadata_schema.as_deref(),
            duckdb_log.as_ref(),
        )?);

        let manager = Arc::new(DuckLakeConnectionManager {
            setup_sql: Arc::clone(&setup_sql),
            disable_extension_autoload,
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        });

        let pool = build_warm_ducklake_pool(manager.as_ref().clone(), pool_size, "write").await?;
        let created_tables = Arc::default();
        let mut destination = Self {
            manager,
            pool: Arc::new(pool),
            blocking_slots: Arc::new(Semaphore::new(pool_size as usize)),
            maintenance_worker: Arc::new(None),
            metrics_sampler: Arc::new(None),
            table_creation_slots: Arc::new(Semaphore::new(1)),
            table_write_slots: Arc::default(),
            store,
            duckdb_log: duckdb_log.map(Arc::new),
            created_tables: Arc::clone(&created_tables),
            applied_batches_table_created: Arc::default(),
        };
        gauge!(ETL_DUCKLAKE_POOL_SIZE).set(pool_size as f64);
        destination.ensure_applied_batches_table_exists().await?;
        destination.maintenance_worker = Arc::new(
            spawn_ducklake_maintenance_worker(
                DuckLakeConnectionManager {
                    setup_sql: Arc::clone(&setup_sql),
                    disable_extension_autoload,
                    #[cfg(feature = "test-utils")]
                    open_count: Arc::new(AtomicUsize::new(0)),
                },
                Arc::clone(&destination.table_write_slots),
            )
            .await?
            .into(),
        );
        destination.metrics_sampler = Arc::new(
            spawn_ducklake_metrics_sampler(
                DuckLakeConnectionManager {
                    setup_sql: Arc::clone(&setup_sql),
                    disable_extension_autoload,
                    #[cfg(feature = "test-utils")]
                    open_count: Arc::new(AtomicUsize::new(0)),
                },
                Arc::clone(&created_tables),
            )
            .await?
            .into(),
        );

        Ok(destination)
    }

    /// Deletes all rows from the destination table without dropping it.
    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        self.ensure_applied_batches_table_exists().await?;
        self.run_duckdb_blocking(DuckDbBlockingOperationKind::Foreground, move |conn| -> EtlResult<()> {
            conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake BEGIN TRANSACTION failed",
                    source: e
                )
            })?;

            let result = (|| -> EtlResult<()> {
                let delete_table_sql = format!(r#"DELETE FROM {LAKE_CATALOG}."{table_name}";"#);
                conn.execute_batch(&delete_table_sql).map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake DELETE failed",
                        format_query_error_detail(&delete_table_sql, &e),
                        source: e
                    )
                })?;

                clear_applied_batch_markers_for_kind(
                    conn,
                    &table_name,
                    DuckLakeTableBatchKind::Copy,
                )?;
                Ok(())
            })();

            match result {
                Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
                    etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", source: e)
                }),
                Err(error) => {
                    let err = conn.execute_batch("ROLLBACK");
                    if let Err(err) = err {
                        tracing::error!(?err, "error rollback");
                    }
                    Err(error)
                }
            }
        })
        .await
    }

    /// Bulk-inserts rows into the destination table inside a single transaction.
    ///
    /// Wrapping all inserts in one `BEGIN` / `COMMIT` ensures they are written
    /// as one atomic DuckLake change rather than one file per row.
    ///
    /// Copy batches are recorded in the replay marker table so a retry after an
    /// ambiguous post-commit failure can detect already applied rows.
    ///
    /// Small copy batches may stay inlined until the background maintenance
    /// worker materializes them after we emit the maintenance notification.
    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        let approx_bytes = table_rows
            .iter()
            .map(|row| row.size_hint() as u64)
            .sum::<u64>();
        let inserted_rows = table_rows.len() as u64;

        // Here we can have concurrent table writers because it's INSERTs only and CDC (write_events) won't start before the copy phase is complete
        self.ensure_applied_batches_table_exists().await?;
        let table_schema = self.get_table_schema(table_id).await?;
        let prepared_batch = prepare_copy_table_batch(&table_schema, table_name, table_rows)?;
        let table_name = prepared_batch.table_name.clone();
        apply_table_batch_with_retry(
            Arc::clone(&self.manager),
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            prepared_batch,
        )
        .await?;

        self.notify_background_maintenance(TableMaintenanceNotification {
            table_name,
            approx_bytes,
            inserted_rows,
        })
        .await;

        Ok(())
    }

    /// Writes streaming CDC events to the destination.
    ///
    /// Insert, Update, and Delete events are grouped by table and written in
    /// parallel, each table in its own async task. Each DuckDB attempt acquires
    /// one blocking slot before entering `spawn_blocking`. Each table's ordered
    /// CDC stream is split into atomic sub-batches, applied on a reused DuckDB
    /// connection per retry attempt, and recorded in an ETL marker table so
    /// retries can safely detect already committed work.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_mutations: HashMap<TableId, Vec<TrackedTableMutation>> =
                HashMap::new();
            let mut table_id_to_stats: HashMap<TableId, TableMaintenanceNotification> =
                HashMap::new();

            // Accumulate non-truncate events, stopping at the first Truncate.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    // Handled later
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(insert) => {
                        let approx_bytes = insert.table_row.size_hint() as u64;
                        table_id_to_mutations
                            .entry(insert.table_id)
                            .or_default()
                            .push(TrackedTableMutation {
                                start_lsn: insert.start_lsn,
                                commit_lsn: insert.commit_lsn,
                                mutation: TableMutation::Insert(insert.table_row),
                            });
                        let stats = table_id_to_stats.entry(insert.table_id).or_default();
                        stats.approx_bytes = stats.approx_bytes.saturating_add(approx_bytes);
                        stats.inserted_rows = stats.inserted_rows.saturating_add(1);
                    }
                    Event::Update(update) => {
                        let table_id = update.table_id;
                        let table_row = update.table_row;
                        let old_table_row = update.old_table_row;
                        let upsert_bytes = table_row.size_hint() as u64;
                        let mutations = table_id_to_mutations.entry(table_id).or_default();
                        if let Some((_, old_row)) = old_table_row {
                            mutations.push(TrackedTableMutation {
                                start_lsn: update.start_lsn,
                                commit_lsn: update.commit_lsn,
                                mutation: TableMutation::Update {
                                    delete_row: old_row,
                                    upsert_row: table_row,
                                },
                            });
                        } else {
                            debug!(
                                "update event has no old row, deleting by primary key from new row"
                            );
                            mutations.push(TrackedTableMutation {
                                start_lsn: update.start_lsn,
                                commit_lsn: update.commit_lsn,
                                mutation: TableMutation::Replace(table_row),
                            });
                        }
                        let stats = table_id_to_stats.entry(table_id).or_default();
                        stats.approx_bytes = stats.approx_bytes.saturating_add(upsert_bytes);
                        stats.inserted_rows = stats.inserted_rows.saturating_add(1);
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            info!("delete event has no old row, skipping");
                            continue;
                        };
                        table_id_to_mutations
                            .entry(delete.table_id)
                            .or_default()
                            .push(TrackedTableMutation {
                                start_lsn: delete.start_lsn,
                                commit_lsn: delete.commit_lsn,
                                mutation: TableMutation::Delete(old_row),
                            });
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            if !table_id_to_mutations.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (table_id, mutations) in table_id_to_mutations {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let table_schema = self.get_table_schema(table_id).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let manager = Arc::clone(&self.manager);
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let prepared_batches =
                        prepare_mutation_table_batches(&table_schema, table_name, mutations)?;
                    let maintenance_notification =
                        table_id_to_stats.remove(&table_id).map(|mut stats| {
                            stats.table_name = prepared_batches[0].table_name.clone();
                            stats
                        });
                    let maintenance_worker = Arc::clone(&self.maintenance_worker);

                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
                        apply_table_batches_with_retry(
                            manager,
                            pool,
                            blocking_slots,
                            prepared_batches,
                        )
                        .await?;
                        if let (Some(worker), Some(notification)) =
                            (maintenance_worker.as_ref(), maintenance_notification)
                        {
                            let _ = worker
                                .notification_tx
                                .send_timeout(notification, NOTIFICATION_SEND_TIMEOUT)
                                .await;
                        }
                        Ok::<(), etl::error::EtlError>(())
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    result.map_err(|_| {
                        etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake write task panicked")
                    })??;
                }
            }

            // Collect contiguous truncate events while preserving table-local order.
            let mut truncate_table_ids: HashMap<TableId, Vec<TrackedTruncateEvent>> =
                HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for rel_id in truncate.rel_ids {
                        truncate_table_ids
                            .entry(TableId::new(rel_id))
                            .or_default()
                            .push(TrackedTruncateEvent {
                                start_lsn: truncate.start_lsn,
                                commit_lsn: truncate.commit_lsn,
                                options: truncate.options,
                            });
                    }
                }
            }

            if !truncate_table_ids.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (table_id, truncates) in truncate_table_ids {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let manager = Arc::clone(&self.manager);
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let prepared_batch = prepare_truncate_table_batch(table_name, truncates);
                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
                        apply_table_batch_with_retry(manager, pool, blocking_slots, prepared_batch)
                            .await
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    result.map_err(|_| {
                        etl_error!(
                            ErrorKind::ApplyWorkerPanic,
                            "DuckLake truncate task panicked"
                        )
                    })??;
                }
            }
        }

        Ok(())
    }

    /// Ensures the destination table exists, creating it (DDL) if necessary.
    async fn ensure_table_exists(&self, table_id: TableId) -> EtlResult<DuckLakeTableName> {
        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })?;

        let table_name = self
            .get_or_create_table_mapping(table_id, &table_schema.name)
            .await?;

        // Fast path: already created.
        {
            let cache = self.created_tables.lock();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        let _table_creation_permit = self
            .table_creation_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake table creation semaphore closed"
                )
            })?;

        {
            let cache = self.created_tables.lock();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        // `build_create_table_sql_ducklake` generates `CREATE TABLE IF NOT EXISTS "name" (...)`.
        // Prefix the table name with the catalog alias so DuckLake knows which
        // catalog to create the table in.
        let ddl = build_create_table_sql_ducklake(&table_name, &table_schema.column_schemas);
        let quoted_table_name = quote_identifier(&table_name).into_owned();
        let qualified_ddl = ddl.replace(
            &quoted_table_name,
            &format!("{LAKE_CATALOG}.{quoted_table_name}"),
        );

        let created_tables = Arc::clone(&self.created_tables);
        let table_name_clone = table_name.clone();

        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            move |conn| -> EtlResult<()> {
                match conn.execute_batch(&qualified_ddl) {
                    Ok(()) => {
                        created_tables.lock().insert(table_name_clone);
                    }
                    Err(e) if is_create_table_conflict(&e, &table_name_clone) => {
                        created_tables.lock().insert(table_name_clone);
                    }
                    Err(e) => {
                        return Err(etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake CREATE TABLE failed",
                            format_query_error_detail(&qualified_ddl, &e),
                            source: e
                        ));
                    }
                }
                Ok(())
            },
        )
        .await?;

        Ok(table_name)
    }

    /// Ensures the ETL-managed replay marker table exists.
    async fn ensure_applied_batches_table_exists(&self) -> EtlResult<()> {
        if self.applied_batches_table_created.load(Ordering::Relaxed) {
            return Ok(());
        }

        let _table_creation_permit = self
            .table_creation_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake table creation semaphore closed"
                )
            })?;

        if self.applied_batches_table_created.load(Ordering::Relaxed) {
            return Ok(());
        }

        let ddl = format!(
            r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}" (
             table_name VARCHAR NOT NULL,
             batch_id VARCHAR NOT NULL,
             batch_kind VARCHAR NOT NULL,
             first_start_lsn UBIGINT,
             last_commit_lsn UBIGINT,
             applied_at TIMESTAMPTZ NOT NULL
             );"#
        );
        let created = Arc::clone(&self.applied_batches_table_created);
        let table_name = APPLIED_BATCHES_TABLE.to_string();

        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            move |conn| -> EtlResult<()> {
                match conn.execute_batch(&ddl) {
                    Ok(()) => {}
                    Err(e) if is_create_table_conflict(&e, &table_name) => {}
                    Err(e) => {
                        return Err(etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake CREATE TABLE failed",
                            format_query_error_detail(&ddl, &e),
                            source: e
                        ));
                    }
                }

                let set_option_sql = format!(
                    "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
                    APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT,
                    quote_literal(APPLIED_BATCHES_TABLE),
                );
                conn.execute_batch(&set_option_sql).map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake set_option failed",
                        format_query_error_detail(&set_option_sql, &e),
                        source: e
                    )
                })?;

                created.store(true, Ordering::Relaxed);
                Ok(())
            },
        )
        .await?;

        Ok(())
    }

    /// Returns the current source schema for `table_id`.
    async fn get_table_schema(&self, table_id: TableId) -> EtlResult<Arc<TableSchema>> {
        self.store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })
    }

    /// Returns the stored destination table name for `table_id`, creating and
    /// persisting a new mapping if none exists yet.
    async fn get_or_create_table_mapping(
        &self,
        table_id: TableId,
        table_name: &TableName,
    ) -> EtlResult<DuckLakeTableName> {
        if let Some(existing) = self.store.get_table_mapping(&table_id).await? {
            return Ok(existing);
        }

        let ducklake_table_name = table_name_to_ducklake_table_name(table_name)?;
        self.store
            .store_table_mapping(table_id, ducklake_table_name.clone())
            .await?;
        Ok(ducklake_table_name)
    }

    /// Serializes table-local truncate and CDC mutation writes.
    async fn acquire_table_write_slot(&self, table_name: &str) -> EtlResult<OwnedSemaphorePermit> {
        let table_slot = table_write_slot(&self.table_write_slots, table_name);

        table_slot.acquire_owned().await.map_err(|_| {
            etl_error!(
                ErrorKind::InvalidState,
                "DuckLake table write semaphore closed"
            )
        })
    }

    /// Runs one DuckDB operation on Tokio's blocking pool after acquiring a
    /// permit that matches the configured DuckDB concurrency limit.
    async fn run_duckdb_blocking<R, F>(
        &self,
        operation_kind: DuckDbBlockingOperationKind,
        operation: F,
    ) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            operation_kind,
            operation,
        )
        .await
    }

    /// Dumps file-backed DuckDB logs on destination shutdown when configured.
    async fn dump_duckdb_logs_inner(&self) -> EtlResult<()> {
        let Some(duckdb_log) = self.duckdb_log.as_ref().map(Arc::clone) else {
            return Ok(());
        };

        self.run_duckdb_blocking(DuckDbBlockingOperationKind::Foreground, move |conn| {
            dump_duckdb_logs(conn, duckdb_log.as_ref())
        })
        .await
    }

    /// Stops the background DuckLake maintenance worker.
    async fn shutdown_maintenance_worker(&self) -> EtlResult<()> {
        if let Some(maintenance_worker) = &*self.maintenance_worker {
            let _ = maintenance_worker.shutdown_tx.send(());
            let handle = maintenance_worker.handle.lock().take();
            if let Some(handle) = handle {
                handle.await.map_err(|_| {
                    etl_error!(
                        ErrorKind::ApplyWorkerPanic,
                        "DuckLake maintenance worker task panicked"
                    )
                })?;
            }
        }

        Ok(())
    }

    /// Stops the background DuckLake metrics sampler.
    async fn shutdown_metrics_sampler(&self) -> EtlResult<()> {
        if let Some(metrics_sampler) = &*self.metrics_sampler {
            let _ = metrics_sampler.shutdown_tx.send(());
            let handle = metrics_sampler.handle.lock().take();
            if let Some(handle) = handle {
                handle.await.map_err(|_| {
                    etl_error!(
                        ErrorKind::ApplyWorkerPanic,
                        "DuckLake metrics sampler task panicked"
                    )
                })?;
            }
        }

        Ok(())
    }

    /// Sends one table write notification to the maintenance worker.
    async fn notify_background_maintenance(&self, notification: TableMaintenanceNotification) {
        if let Some(maintenance_worker) = &*self.maintenance_worker
            && let Err(error) = maintenance_worker
                .notification_tx
                .send_timeout(notification, NOTIFICATION_SEND_TIMEOUT)
                .await
        {
            match error {
                mpsc::error::SendTimeoutError::Timeout(t) => {
                    warn!(
                        table = %t.table_name,
                        "ducklake maintenance notification timed out"
                    );
                }
                mpsc::error::SendTimeoutError::Closed(t) => {
                    warn!(
                        table = %t.table_name,
                        "ducklake maintenance notification dropped"
                    );
                }
            }
        }
    }

    /// Returns how many DuckDB connections have been initialized for tests.
    #[cfg(feature = "test-utils")]
    pub fn connection_open_count_for_tests(&self) -> usize {
        self.manager.open_count_for_tests()
    }
}

/// Converts a Postgres [`TableName`] to a DuckLake table name string.
///
/// Escapes underscores in schema and table name components by doubling them,
/// then joins the two parts with a single `_`. This matches the convention
/// used by other destinations in this crate.
///
/// # Example
/// - `public.my_table` → `public_my__table`
/// - `my_schema.orders` → `my__schema_orders`
///
/// Returns an error if the table name cannot be converted.
pub fn table_name_to_ducklake_table_name(table_name: &TableName) -> EtlResult<DuckLakeTableName> {
    try_stringify_table_name(table_name)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Creates parent directories for configured DuckDB log storage paths.
fn ensure_duckdb_log_paths(duckdb_log: Option<&DuckDbLogConfig>) -> EtlResult<()> {
    let Some(duckdb_log) = duckdb_log else {
        return Ok(());
    };

    ensure_duckdb_log_storage_path(Path::new(&duckdb_log.storage_path))?;
    ensure_parent_directory(
        Path::new(&duckdb_log.dump_path),
        "DuckDB log dump path parent directory creation failed",
    )?;

    Ok(())
}

/// Prepares the file-backed storage path used by `CALL enable_logging(...)`.
fn ensure_duckdb_log_storage_path(storage_path: &Path) -> EtlResult<()> {
    let is_csv_file = storage_path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("csv"));

    if is_csv_file {
        ensure_parent_directory(
            storage_path,
            "DuckDB log storage parent directory creation failed",
        )
    } else {
        fs::create_dir_all(storage_path).map_err(|error| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckDB log storage directory creation failed",
                storage_path.display().to_string(),
                source: error
            )
        })
    }
}

/// Creates the parent directory of `path` when one is present.
fn ensure_parent_directory(path: &Path, description: &'static str) -> EtlResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }

    fs::create_dir_all(parent).map_err(|error| {
        etl_error!(
            ErrorKind::ConfigError,
            description,
            parent.display().to_string(),
            source: error
        )
    })
}

/// Dumps the current `duckdb_logs` relation to the configured CSV file.
fn dump_duckdb_logs(conn: &duckdb::Connection, duckdb_log: &DuckDbLogConfig) -> EtlResult<()> {
    let select_sql = "SELECT * FROM duckdb_logs";
    let mut statement = conn.prepare(select_sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckDB log dump query preparation failed",
            format_query_error_detail(select_sql, &error),
            source: error
        )
    })?;
    let mut rows = statement.query([]).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckDB log dump query failed",
            format_query_error_detail(select_sql, &error),
            source: error
        )
    })?;
    let mut row_count = 0usize;

    while rows
        .next()
        .map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckDB log dump row fetch failed",
                format_query_error_detail(select_sql, &error),
                source: error
            )
        })?
        .is_some()
    {
        row_count += 1;
    }
    drop(rows);
    drop(statement);

    let copy_sql = format!(
        "COPY (SELECT * FROM duckdb_logs) TO {} (FORMAT CSV, HEADER);",
        quote_literal(&duckdb_log.dump_path)
    );
    conn.execute_batch(&copy_sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckDB log dump copy failed",
            format_query_error_detail(&copy_sql, &error),
            source: error
        )
    })?;

    info!(
        row_count,
        dump_path = %duckdb_log.dump_path,
        "duckdb logs dumped"
    );

    Ok(())
}

/// Maximum number of times a failed write attempt is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 10;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 2_000;
/// Minimum retry delay for transient delete-file visibility failures.
const TRANSIENT_DELETE_FILE_RETRY_DELAY_MS: u64 = 5_000;

/// Applies all prepared atomic batches for one table, reusing one DuckDB
/// connection per attempt and skipping already committed segments by marker.
async fn apply_table_batches_with_retry(
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batches: Vec<PreparedDuckLakeTableBatch>,
) -> EtlResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let batch_count = batches.len();
    let batches = Arc::new(batches);
    let table_name = &batches[0].table_name;
    let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    // This retry mechanism is safe because
    for attempt in 0..=MAX_COMMIT_RETRIES {
        let attempt_batches = Arc::clone(&batches);
        tracing::info!(
            "attempt_batches size -----------__> {}",
            attempt_batches.len()
        );
        match run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            {
                let manager = Arc::clone(&manager);
                move |conn| {
                    let operation_started = std::time::Instant::now();
                    apply_table_batches(manager.as_ref(), conn, attempt_batches.as_ref())?;
                    info!(
                        duration_ms = operation_started.elapsed().as_millis() as u64,
                        "ducklake batch ---- "
                    );
                    Ok(())
                }
            },
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
                counter!(
                    ETL_DUCKLAKE_RETRIES_TOTAL,
                    BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
                    RETRY_SCOPE_LABEL => "table_sequence",
                )
                .increment(1);
                let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
                let jittered = delay.mul_f64(jitter_ratio);
                warn!(
                    attempt = attempt + 1,
                    max = MAX_COMMIT_RETRIES,
                    table = %table_name,
                    batch_count,
                    error = ?e,
                    "ducklake table batch sequence failed, retrying"
                );
                tokio::time::sleep(jittered).await;
                delay = std::cmp::min(delay * 2, Duration::from_millis(MAX_RETRY_DELAY_MS));
            }
            Err(e) => {
                counter!(
                    ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
                    BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
                    RETRY_SCOPE_LABEL => "table_sequence",
                )
                .increment(1);
                return Err(etl_error!(
                    ErrorKind::DuckLakeAtomicBatchRetryable,
                    "DuckLake atomic table batch sequence failed after retries",
                    format!("table={table_name}, batch_count={batch_count}"),
                    source: e
                ));
            }
        }
    }

    Ok(())
}

/// Applies all prepared atomic batches for one table on the same connection.
fn apply_table_batches(
    manager: &DuckLakeConnectionManager,
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
) -> EtlResult<()> {
    for batch in batches {
        // This is useful in case it fails in the middle of the process.
        // As we have a batch id we can check if it was already committed or not and only replay if not.
        if applied_batch_marker_exists(conn, batch)? {
            counter!(
                ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
            )
            .increment(1);
            debug!(
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                "ducklake table batch already committed, skipping replay"
            );
            continue;
        }

        apply_table_batch(manager, conn, batch).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake atomic table batch failed",
                format!(
                    "table={}, batch_id={}, batch_kind={}",
                    batch.table_name,
                    batch.batch_id,
                    batch.batch_kind.as_str()
                ),
                source: error
            )
        })?;
    }

    Ok(())
}

/// Applies one atomic per-table batch and retries on failure.
async fn apply_table_batch_with_retry(
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let table_name = batch.table_name.clone();
    let batch_id = batch.batch_id.clone();
    let batch_kind = batch.batch_kind;
    let batch = Arc::new(batch);
    let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    for attempt in 0..=MAX_COMMIT_RETRIES {
        let attempt_batch = Arc::clone(&batch);
        match run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            {
                let manager = Arc::clone(&manager);
                move |conn| {
                    if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                        counter!(
                            ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
                            BATCH_KIND_LABEL => batch_kind.as_str(),
                        )
                        .increment(1);
                        debug!(
                            table = %attempt_batch.table_name,
                            batch_id = %attempt_batch.batch_id,
                            batch_kind = batch_kind.as_str(),
                            "ducklake table batch already committed, skipping replay"
                        );

                        return Ok(());
                    }

                    apply_table_batch(manager.as_ref(), conn, attempt_batch.as_ref())?;

                    Ok(())
                }
            },
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
                counter!(
                    ETL_DUCKLAKE_RETRIES_TOTAL,
                    BATCH_KIND_LABEL => batch_kind.as_str(),
                    RETRY_SCOPE_LABEL => "single_batch",
                )
                .increment(1);
                let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
                let jittered = delay.mul_f64(jitter_ratio);
                warn!(
                    attempt = attempt + 1,
                    max = MAX_COMMIT_RETRIES,
                    table = %table_name,
                    batch_id = %batch_id,
                    error = ?e,
                    "ducklake table mutation attempt failed, retrying"
                );
                tokio::time::sleep(jittered).await;
                delay = std::cmp::min(
                    delay * 2,
                    Duration::from_millis(
                        MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
                    ),
                );
            }
            Err(e) => {
                counter!(
                    ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
                    BATCH_KIND_LABEL => batch_kind.as_str(),
                    RETRY_SCOPE_LABEL => "single_batch",
                )
                .increment(1);
                return Err(etl_error!(
                    ErrorKind::DuckLakeAtomicBatchRetryable,
                    "DuckLake atomic table batch failed after retries",
                    format!(
                        "table={table_name}, batch_id={batch_id}, batch_kind={}",
                        batch_kind.as_str()
                    ),
                    source: e
                ));
            }
        }
    }

    Ok(())
}

/// Prepares ordered atomic batches for one table's CDC mutations.
///
/// Mutations stay in source order and are split only at the batch-size cap so
/// mixed CDC streams can commit larger insert groups without breaking atomic
/// ordering.
fn prepare_mutation_table_batches(
    table_schema: &TableSchema,
    table_name: DuckLakeTableName,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<Vec<PreparedDuckLakeTableBatch>> {
    let mut prepared_batches = Vec::new();
    let mut pending_mutations = Vec::new();

    for tracked_mutation in tracked_mutations {
        pending_mutations.push(tracked_mutation);
        if pending_mutations.len() >= CDC_MUTATION_BATCH_SIZE {
            push_prepared_mutation_batch(
                &mut prepared_batches,
                table_schema,
                &table_name,
                std::mem::take(&mut pending_mutations),
            )?;
        }
    }

    push_prepared_mutation_batch(
        &mut prepared_batches,
        table_schema,
        &table_name,
        pending_mutations,
    )?;

    Ok(prepared_batches)
}

/// Builds one prepared atomic batch from an ordered slice of tracked mutations.
fn push_prepared_mutation_batch(
    prepared_batches: &mut Vec<PreparedDuckLakeTableBatch>,
    table_schema: &TableSchema,
    table_name: &str,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<()> {
    if tracked_mutations.is_empty() {
        return Ok(());
    }

    let identity = build_mutation_batch_identity(table_name, table_schema, &tracked_mutations)?;
    let mutations = tracked_mutations
        .into_iter()
        .map(|tracked| tracked.mutation)
        .collect();

    prepared_batches.push(PreparedDuckLakeTableBatch {
        table_name: table_name.to_string(),
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Mutation,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        action: PreparedDuckLakeTableBatchAction::Mutation(prepare_table_mutations(
            table_schema,
            mutations,
        )?),
    });

    Ok(())
}

/// Prepares one retry-safe atomic batch for a table-copy row chunk.
fn prepare_copy_table_batch(
    table_schema: &TableSchema,
    table_name: DuckLakeTableName,
    table_rows: Vec<TableRow>,
) -> EtlResult<PreparedDuckLakeTableBatch> {
    let identity = build_copy_batch_identity(&table_name, table_schema, &table_rows)?;
    Ok(PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Copy,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        action: PreparedDuckLakeTableBatchAction::Mutation(vec![PreparedTableMutation::Upsert(
            prepare_rows(table_rows),
        )]),
    })
}

/// Prepares the ordered atomic batch for one table's truncate events.
fn prepare_truncate_table_batch(
    table_name: DuckLakeTableName,
    tracked_truncates: Vec<TrackedTruncateEvent>,
) -> PreparedDuckLakeTableBatch {
    let identity = build_truncate_batch_identity(&table_name, &tracked_truncates);
    PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Truncate,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        action: PreparedDuckLakeTableBatchAction::Truncate,
    }
}

/// Groups ordered row mutations into retryable DuckDB operations.
fn prepare_table_mutations(
    table_schema: &TableSchema,
    mutations: Vec<TableMutation>,
) -> EtlResult<Vec<PreparedTableMutation>> {
    let mut prepared_mutations = Vec::new();
    let mut upsert_rows = Vec::new();
    let mut delete_predicates = Vec::new();

    for mutation in mutations {
        match mutation {
            TableMutation::Insert(row) => {
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }
                upsert_rows.push(row);
            }
            TableMutation::Delete(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                delete_predicates.push(delete_predicate_from_row(table_schema, &row)?);
            }
            TableMutation::Update {
                delete_row,
                upsert_row,
            } => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }

                prepared_mutations.push(PreparedTableMutation::Delete {
                    predicates: vec![delete_predicate_from_row(table_schema, &delete_row)?],
                    origin: "update",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(vec![
                    upsert_row,
                ])));
            }
            TableMutation::Replace(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }

                prepared_mutations.push(PreparedTableMutation::Delete {
                    predicates: vec![delete_predicate_from_row(table_schema, &row)?],
                    origin: "replace",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(vec![row])));
            }
        }
    }

    if !upsert_rows.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(upsert_rows)));
    }
    if !delete_predicates.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Delete {
            predicates: delete_predicates,
            origin: "delete",
        });
    }

    Ok(prepared_mutations)
}

/// Builds a `WHERE` clause from the primary-key values stored in `row`.
fn delete_predicate_from_row(table_schema: &TableSchema, row: &TableRow) -> EtlResult<String> {
    if !table_schema.has_primary_keys() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake delete requires a primary key",
            format!("Table '{}' has no primary key columns", table_schema.name)
        ));
    }

    if row.values().len() != table_schema.column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake row shape does not match schema",
            format!(
                "Expected {} values for table '{}', got {}",
                table_schema.column_schemas.len(),
                table_schema.name,
                row.values().len()
            )
        ));
    }

    let mut predicates = Vec::new();

    for (column_schema, value) in table_schema
        .column_schemas
        .iter()
        .zip(row.values())
        .filter(|(column_schema, _)| column_schema.primary)
    {
        let quoted_column = quote_identifier(&column_schema.name).into_owned();
        let predicate = match value {
            Cell::Null => format!("{quoted_column} IS NULL"),
            _ => format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)),
        };
        predicates.push(predicate);
    }

    Ok(predicates.join(" AND "))
}

/// Builds a deterministic identity for one ordered mutation batch.
fn build_mutation_batch_identity(
    table_name: &str,
    table_schema: &TableSchema,
    tracked_mutations: &[TrackedTableMutation],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "mutation".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for tracked_mutation in tracked_mutations {
        u64::from(tracked_mutation.start_lsn).hash(&mut hasher);
        u64::from(tracked_mutation.commit_lsn).hash(&mut hasher);

        match &tracked_mutation.mutation {
            TableMutation::Insert(row) => {
                "insert".hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
            TableMutation::Delete(row) => {
                "delete".hash(&mut hasher);
                delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
            }
            TableMutation::Update {
                delete_row,
                upsert_row,
            } => {
                "update".hash(&mut hasher);
                delete_predicate_from_row(table_schema, delete_row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, upsert_row);
            }
            TableMutation::Replace(row) => {
                "replace".hash(&mut hasher);
                delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
        }
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Mutation,
        tracked_mutations
            .first()
            .map(|tracked_mutation| tracked_mutation.start_lsn),
        tracked_mutations
            .last()
            .map(|tracked_mutation| tracked_mutation.commit_lsn),
        hasher.finish(),
    ))
}

/// Builds a deterministic identity for one ordered table-copy batch.
fn build_copy_batch_identity(
    table_name: &str,
    table_schema: &TableSchema,
    table_rows: &[TableRow],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "copy".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for row in table_rows {
        delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
        hash_table_row_ref(&mut hasher, row);
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Copy,
        None,
        None,
        hasher.finish(),
    ))
}

/// Builds a deterministic identity for one ordered truncate batch.
fn build_truncate_batch_identity(
    table_name: &str,
    tracked_truncates: &[TrackedTruncateEvent],
) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "truncate".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for tracked_truncate in tracked_truncates {
        u64::from(tracked_truncate.start_lsn).hash(&mut hasher);
        u64::from(tracked_truncate.commit_lsn).hash(&mut hasher);
        tracked_truncate.options.hash(&mut hasher);
    }

    build_batch_identity(
        DuckLakeTableBatchKind::Truncate,
        tracked_truncates
            .first()
            .map(|tracked_truncate| tracked_truncate.start_lsn),
        tracked_truncates
            .last()
            .map(|tracked_truncate| tracked_truncate.commit_lsn),
        hasher.finish(),
    )
}

/// Builds the final persisted batch identity string.
fn build_batch_identity(
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    fingerprint: u64,
) -> DuckLakeBatchIdentity {
    let first_start_lsn_u64 = first_start_lsn.map(u64::from).unwrap_or_default();
    let last_commit_lsn_u64 = last_commit_lsn.map(u64::from).unwrap_or_default();

    DuckLakeBatchIdentity {
        batch_id: format!(
            "{}:{first_start_lsn_u64:016x}:{last_commit_lsn_u64:016x}:{fingerprint:016x}",
            batch_kind.as_str()
        ),
        first_start_lsn,
        last_commit_lsn,
    }
}

/// Hashes a row using its SQL literal form so retries are independent of appender encoding.
fn hash_table_row_ref(hasher: &mut BatchIdHasher, row: &TableRow) {
    table_row_to_sql_literal_ref(row).hash(hasher);
}

/// Returns whether the atomic batch marker already exists.
fn applied_batch_marker_exists(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<bool> {
    let sql = format!(
        r#"SELECT 1 FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {} AND batch_id = {} LIMIT 1;"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.batch_id)
    );
    let mut statement = conn.prepare(&sql).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query prepare failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;
    let mut rows = statement.query([]).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query row fetch failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })
}

/// Inserts the atomic batch marker inside the open DuckLake transaction.
fn insert_applied_batch_marker(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         (table_name, batch_id, batch_kind, first_start_lsn, last_commit_lsn, applied_at) VALUES ({}, {}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.batch_id),
        quote_literal(batch.batch_kind.as_str()),
        optional_lsn_to_sql_literal(batch.first_start_lsn),
        optional_lsn_to_sql_literal(batch.last_commit_lsn),
    );
    conn.execute_batch(&sql).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker insert failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;
    Ok(())
}

/// Deletes persisted markers for one table and batch kind.
fn clear_applied_batch_markers_for_kind(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
) -> EtlResult<()> {
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {} AND batch_kind = {};"#,
        quote_literal(table_name),
        quote_literal(batch_kind.as_str())
    );
    conn.execute_batch(&sql).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker delete failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;
    Ok(())
}

/// Applies one atomic per-table batch in a single DuckLake transaction.
fn apply_table_batch(
    manager: &DuckLakeConnectionManager,
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let batch_started = Instant::now();
    let diagnostics_conn = if batch_diagnostics_enabled() && tracing::enabled!(Level::INFO) {
        match manager.open_duckdb_connection() {
            Ok(conn) => Some(conn),
            Err(error) => {
                debug!(
                    table = %batch.table_name,
                    batch_id = %batch.batch_id,
                    error = ?error,
                    "ducklake diagnostics connection open failed"
                );
                None
            }
        }
    } else {
        None
    };

    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
        tracing::error!(?e, "error transaction");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: e
        )
    })?;

    let result = (|| -> EtlResult<()> {
        let now = Instant::now();
        match &batch.action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
                tracing::info!(
                    "applied batch action length ==== {}",
                    prepared_mutations.len()
                );
                for prepared_mutation in prepared_mutations {
                    apply_table_mutation(
                        conn,
                        &batch.table_name,
                        batch.batch_kind,
                        prepared_mutation,
                    )?;
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => {
                apply_truncate_batch_action(conn, &batch.table_name)?;
            }
        }
        tracing::info!("applied batch action : {}", now.elapsed().as_millis());

        let now = Instant::now();
        insert_applied_batch_marker(conn, batch)?;
        tracing::info!("applied batch marker : {}", now.elapsed().as_millis());
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|e| {
                tracing::error!(?e, "error commit");
                etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", source: e)
            })?;
            histogram!(
                ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(batch_started.elapsed().as_secs_f64());
            histogram!(
                ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(prepared_mutation_count(batch) as f64);

            if tracing::enabled!(Level::INFO)
                && let Some(diagnostics_conn) = diagnostics_conn.as_ref()
            {
                let row_count_after =
                    query_table_row_count(diagnostics_conn, &batch.table_name).ok();
                info!(
                    table = %batch.table_name,
                    batch_id = %batch.batch_id,
                    batch_kind = batch.batch_kind.as_str(),
                    first_start_lsn = ?batch.first_start_lsn,
                    last_commit_lsn = ?batch.last_commit_lsn,
                    row_count_after,
                    sub_batch_kind = batch_log_kind(batch),
                    insert_sub_batch_rows = insert_sub_batch_rows(batch),
                    "ducklake batch committed"
                );
            }

            #[cfg(feature = "test-utils")]
            maybe_fail_after_committed_batch_for_tests(batch.batch_kind, &batch.table_name)?;

            Ok(())
        }
        Err(error) => {
            let err = conn.execute_batch("ROLLBACK");
            if let Err(err) = err {
                tracing::error!(?err, "error rollback");
            }
            Err(error)
        }
    }
}

/// Flushes inlined user data for one table after the write transaction commits.
pub(super) fn flush_table_inlined_data(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
) -> EtlResult<u64> {
    let flush_started = Instant::now();
    let sql = format!(
        r#"SELECT COALESCE(SUM(rows_flushed), 0)
         FROM ducklake_flush_inlined_data({}, table_name => {});"#,
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
    );
    let rows_flushed: i64 = conn.query_row(&sql, [], |row| row.get(0)).map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake inlined data flush failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;
    let rows_flushed = rows_flushed.max(0) as u64;
    let flush_result = if rows_flushed > 0 { "flushed" } else { "noop" };
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_ROWS,
        BATCH_KIND_LABEL => batch_kind.as_str(),
        RESULT_LABEL => flush_result,
    )
    .record(rows_flushed as f64);
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
        BATCH_KIND_LABEL => batch_kind.as_str(),
        RESULT_LABEL => flush_result,
    )
    .record(flush_started.elapsed().as_secs_f64());

    if rows_flushed > 0 {
        info!(
            table = %table_name,
            batch_kind = batch_kind.as_str(),
            rows_flushed,
            "ducklake inlined data flushed"
        );
    } else {
        debug!(
            table = %table_name,
            batch_kind = batch_kind.as_str(),
            "ducklake inlined data already flushed"
        );
    }
    Ok(rows_flushed)
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(conn: &duckdb::Connection, table_name: &str) -> EtlResult<()> {
    let sql = format!(r#"DELETE FROM {LAKE_CATALOG}."{table_name}";"#);
    conn.execute_batch(&sql).map_err(|e| {
        tracing::error!(?e, "error DELETE");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake DELETE failed",
            format_query_error_detail(&sql, &e),
            source: e
        )
    })?;
    Ok(())
}

/// Formats an optional LSN for marker-table inserts.
fn optional_lsn_to_sql_literal(lsn: Option<PgLsn>) -> String {
    lsn.map(|value| u64::from(value).to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

/// Applies one prepared table mutation inside an open transaction.
fn apply_table_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
    prepared_mutation: &PreparedTableMutation,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Upsert(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => prepared_rows_kind(prepared_rows),
            )
            .record(prepared_rows_count(prepared_rows) as f64);
            apply_upsert_mutation(conn, table_name, prepared_rows)
        }
        PreparedTableMutation::Delete { predicates, origin } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(predicates.len() as f64);
            apply_delete_mutation(conn, table_name, predicates.as_slice())
        }
    }
}

/// Applies one upsert batch inside an open DuckLake transaction.
fn apply_upsert_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    prepared_rows: &PreparedRows,
) -> EtlResult<()> {
    let row_count = match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::SqlLiterals(values) => values.len(),
    };

    if row_count == 0 {
        return Ok(());
    }

    // Staging table lives entirely in-memory (regular DuckDB, not DuckLake).
    // TEMP tables are connection-local so there is no cross-connection conflict
    // even when multiple DuckDB connections run concurrently.
    let staging = format!("__staging_{table_name}");

    // Mirror the DuckLake target schema without copying any data.
    conn.execute_batch(&format!(
        r#"CREATE OR REPLACE TEMP TABLE {staging:?} AS
         SELECT * FROM {LAKE_CATALOG}."{table_name}" LIMIT 0;"#
    ))
    .map_err(|e| {
        tracing::error!(?e, "error CREATE TEMP TABLE");

        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake staging table creation failed",
            source: e
        )
    })?;

    let load_result = match prepared_rows {
        PreparedRows::Appender(all_values) => (|| {
            let mut appender = conn.appender(&staging).map_err(|e| {
                tracing::error!(?e, "error appender");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging appender creation failed",
                    source: e
                )
            })?;
            for values in all_values {
                appender
                    .append_row(duckdb::appender_params_from_iter(values))
                    .map_err(|e| {
                        tracing::error!(?e, "error append row");
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake staging append_row failed",
                            source: e
                        )
                    })?;
            }
            appender.flush().map_err(|e| {
                tracing::error!(?e, "error flush");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging appender flush failed",
                    source: e
                )
            })
        })(),
        PreparedRows::SqlLiterals(row_literals) => {
            insert_rows_into_staging_with_sql(conn, &staging, row_literals.as_slice())
        }
    };

    if let Err(error) = load_result {
        tracing::error!(?error, "error LOAD RESULT");

        let res = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
        if let Err(e) = res {
            tracing::error!(?e, "error drop table staging");
        }

        return Err(error);
    }

    let result = conn
        .execute_batch(&format!(
            r#"INSERT INTO {LAKE_CATALOG}."{table_name}" SELECT * FROM {staging:?};"#
        ))
        .map_err(|e| {
            tracing::error!(?e, "error INSERT INTO");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                source: e
            )
        });

    let res = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
    if let Err(e) = res {
        tracing::error!(?e, "error drop table staging");
    }
    if let Err(e) = &result {
        tracing::error!(?e, "error INSERT result");
    }

    result
}

/// Applies one delete batch inside an open DuckLake transaction.
fn apply_delete_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    predicates: &[String],
) -> EtlResult<()> {
    if predicates.is_empty() {
        return Ok(());
    }

    for chunk in predicates.chunks(SQL_DELETE_BATCH_SIZE) {
        let where_clause = chunk
            .iter()
            .map(|predicate| format!("({predicate})"))
            .collect::<Vec<_>>()
            .join(" OR ");

        let sql_query =
            format!(r#"DELETE FROM {LAKE_CATALOG}."{table_name}" WHERE {where_clause};"#);
        conn.execute_batch(&sql_query).map_err(|e| {
            tracing::error!(?e, "error DELETE FROM");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake DELETE failed",
                source: e
            )
        })?;
    }

    Ok(())
}

/// Returns whether expensive post-commit batch diagnostics are enabled.
fn batch_diagnostics_enabled() -> bool {
    env::var(BATCH_DIAGNOSTICS_ENV_VAR)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Returns the visible row count for one DuckLake table.
fn query_table_row_count(
    conn: &duckdb::Connection,
    table_name: &str,
) -> Result<i64, duckdb::Error> {
    conn.query_row(
        &format!(r#"SELECT COUNT(*) FROM {LAKE_CATALOG}."{table_name}";"#),
        [],
        |row| row.get(0),
    )
}

/// Returns the number of values carried by a prepared row payload.
fn prepared_rows_count(prepared_rows: &PreparedRows) -> usize {
    match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::SqlLiterals(values) => values.len(),
    }
}

/// Returns the encoding strategy used by a prepared row payload.
fn prepared_rows_kind(prepared_rows: &PreparedRows) -> &'static str {
    match prepared_rows {
        PreparedRows::Appender(_) => "appender",
        PreparedRows::SqlLiterals(_) => "sql_literals",
    }
}

/// Returns the number of prepared mutation statements in one atomic batch.
fn prepared_mutation_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 1,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations.len(),
    }
}

/// Returns the insert row count when the batch is a pure insert sub-batch.
fn insert_sub_batch_rows(batch: &PreparedDuckLakeTableBatch) -> Option<usize> {
    let PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) = &batch.action else {
        return None;
    };

    if prepared_mutations.len() != 1 {
        return None;
    }

    match &prepared_mutations[0] {
        PreparedTableMutation::Upsert(prepared_rows) => Some(match prepared_rows {
            PreparedRows::Appender(values) => values.len(),
            PreparedRows::SqlLiterals(values) => values.len(),
        }),
        PreparedTableMutation::Delete { .. } => None,
    }
}

/// Classifies a prepared batch for concise INFO logging.
fn batch_log_kind(batch: &PreparedDuckLakeTableBatch) -> &'static str {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => "truncate",
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
            match prepared_mutations.as_slice() {
                [PreparedTableMutation::Upsert(_)] => "insert",
                [PreparedTableMutation::Delete { origin, .. }] => origin,
                [
                    PreparedTableMutation::Delete { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                _ => "mutation",
            }
        }
    }
}

/// Inserts rows into the local staging table using SQL literals.
fn insert_rows_into_staging_with_sql(
    conn: &duckdb::Connection,
    staging: &str,
    row_literals: &[String],
) -> EtlResult<()> {
    for chunk in row_literals.chunks(SQL_INSERT_BATCH_SIZE) {
        conn.execute_batch(&format!(
            "INSERT INTO {staging:?} VALUES {};",
            chunk.join(", ")
        ))
        .map_err(|e| {
            tracing::error!(?e, "error insert_rows_into_staging_with_sql");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging row insert failed",
                source: e
            )
        })?;
    }

    Ok(())
}

#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));

/// Arms a test hook that injects one post-commit failure for the next atomic batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_atomic_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_string());
}

/// Arms a test hook that injects one post-commit failure for the next copy batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_copy_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_string());
}

/// Clears DuckLake destination test hooks.
#[cfg(feature = "test-utils")]
pub fn reset_ducklake_test_hooks() {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = None;
}

/// Injects a synthetic failure after commit so retries must rely on the correct marker path.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_committed_batch_for_tests(
    batch_kind: DuckLakeTableBatchKind,
    table_name: &str,
) -> EtlResult<()> {
    match batch_kind {
        DuckLakeTableBatchKind::Copy => maybe_fail_after_copy_batch_commit_for_tests(table_name),
        DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate => {
            maybe_fail_after_atomic_batch_commit_for_tests(table_name)
        }
    }
}

/// Injects a synthetic failure after commit so retries must rely on the marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_atomic_batch_commit_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "ducklake test hook injected post-commit failure"
        ));
    }

    Ok(())
}

/// Injects a synthetic failure after commit so copy retries must rely on the marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_copy_batch_commit_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "ducklake test hook injected copy post-commit failure"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use duckdb::{Config, Connection};
    use etl::destination::Destination;
    use etl::store::both::memory::MemoryStore;
    use etl::store::schema::SchemaStore;
    use etl::types::{ColumnSchema, Type as PgType};
    use pg_escape::{quote_identifier, quote_literal};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use url::Url;

    use crate::ducklake::metrics::{
        query_catalog_maintenance_metrics_blocking, query_table_storage_metrics_blocking,
    };

    fn make_schema(table_id: u32, schema: &str, table: &str) -> TableSchema {
        TableSchema::new(
            TableId::new(table_id),
            TableName::new(schema.to_string(), table.to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        )
    }

    fn path_to_file_url(path: &Path) -> Url {
        Url::from_file_path(path).expect("failed to convert path to file url")
    }

    fn open_verification_connection() -> Connection {
        if cfg!(target_os = "linux") {
            return Connection::open_in_memory_with_flags(
                Config::default()
                    .enable_autoload_extension(false)
                    .expect("failed to disable DuckDB extension autoload"),
            )
            .expect("failed to open in-memory DuckDB");
        }

        Connection::open_in_memory().expect("failed to open in-memory DuckDB")
    }

    fn ducklake_load_sql() -> String {
        if cfg!(target_os = "linux") {
            let platform_dir = match std::env::consts::ARCH {
                "x86_64" => "linux_amd64",
                "aarch64" => "linux_arm64",
                arch => panic!("unsupported linux architecture for DuckDB test extensions: {arch}"),
            };
            let env_override = std::env::var_os("ETL_DUCKDB_EXTENSION_ROOT").map(PathBuf::from);
            let candidate_roots = env_override
                .into_iter()
                .chain([
                    PathBuf::from("/app/duckdb_extensions"),
                    Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/duckdb/extensions"),
                ])
                .collect::<Vec<_>>();

            for root in candidate_roots {
                let extension_dir = root.join("1.5.1").join(platform_dir);
                let ducklake_extension = extension_dir.join("ducklake.duckdb_extension");
                let json_extension = extension_dir.join("json.duckdb_extension");
                let parquet_extension = extension_dir.join("parquet.duckdb_extension");

                if ducklake_extension.is_file()
                    && json_extension.is_file()
                    && parquet_extension.is_file()
                {
                    return format!(
                        "LOAD {}; LOAD {}; LOAD {};",
                        quote_literal(&ducklake_extension.display().to_string()),
                        quote_literal(&json_extension.display().to_string()),
                        quote_literal(&parquet_extension.display().to_string()),
                    );
                }
            }
        }

        "INSTALL ducklake; LOAD ducklake; INSTALL json; LOAD json; INSTALL parquet; LOAD parquet;"
            .to_string()
    }

    fn open_lake_conn(catalog: &Url, data: &Url) -> Connection {
        let conn = open_verification_connection();
        conn.execute_batch(&format!(
            "{} ATTACH {} AS {} (DATA_PATH {});",
            ducklake_load_sql(),
            quote_literal(&format!("ducklake:{}", catalog.as_str())),
            quote_identifier(LAKE_CATALOG),
            quote_literal(data.as_str()),
        ))
        .expect("failed to attach DuckLake catalog");
        conn
    }

    #[test]
    fn test_table_name_escaping() {
        assert_eq!(
            table_name_to_ducklake_table_name(&TableName {
                schema: "public".to_string(),
                name: "orders".to_string(),
            })
            .unwrap(),
            "public_orders"
        );
        assert_eq!(
            table_name_to_ducklake_table_name(&TableName {
                schema: "my_schema".to_string(),
                name: "my_table".to_string(),
            })
            .unwrap(),
            "my__schema_my__table"
        );
    }

    #[test]
    fn test_is_create_table_conflict_matches_ducklake_commit_conflict() {
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction. Transaction conflict - attempting to create table \"public_users\" in schema \"main\" - but this table has been created by another transaction already".to_string(),
            ),
        );

        assert!(is_create_table_conflict(&error, "public_users"));
        assert!(!is_create_table_conflict(&error, "public_orders"));
    }

    #[test]
    fn test_delete_predicate_from_row_uses_only_primary_key_columns() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("tenant_id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::I32(42),
            Cell::String("alice".to_string()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&table_schema, &row).unwrap(),
            "tenant_id = 7 AND id = 42"
        );
    }

    #[test]
    fn test_prepare_table_mutations_replace_emits_delete_then_upsert() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]);

        let prepared =
            prepare_table_mutations(&table_schema, vec![TableMutation::Replace(row)]).unwrap();

        assert_eq!(prepared.len(), 2);
        match &prepared[0] {
            PreparedTableMutation::Delete { predicates, origin } => {
                assert_eq!(predicates, &vec!["id = 1".to_string()]);
                assert_eq!(origin, &"replace");
            }
            PreparedTableMutation::Upsert(_) => panic!("expected delete first"),
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Delete { .. } => panic!("expected upsert second"),
        }
    }

    #[test]
    fn test_prepare_table_mutations_update_emits_delete_then_upsert() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let prepared = prepare_table_mutations(
            &table_schema,
            vec![TableMutation::Update {
                delete_row: TableRow::new(vec![Cell::I32(1), Cell::String("before".to_string())]),
                upsert_row: TableRow::new(vec![Cell::I32(1), Cell::String("after".to_string())]),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 2);
        match &prepared[0] {
            PreparedTableMutation::Delete { predicates, origin } => {
                assert_eq!(predicates, &vec!["id = 1".to_string()]);
                assert_eq!(origin, &"update");
            }
            PreparedTableMutation::Upsert(_) => panic!("expected delete first"),
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Delete { .. } => panic!("expected upsert second"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_insert_only_uses_single_upsert_operation() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation {
                    start_lsn: PgLsn::from(10),
                    commit_lsn: PgLsn::from(20),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(10),
                    commit_lsn: PgLsn::from(20),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                },
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch_kind, DuckLakeTableBatchKind::Mutation);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                        assert_eq!(rows.len(), 2);
                    }
                    PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                        panic!("expected appender payload")
                    }
                    PreparedTableMutation::Delete { .. } => panic!("expected upsert"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_split_mixed_cdc_at_delete_boundaries() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(110),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(110),
                    mutation: TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(110),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_string()),
                    ])),
                },
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 3);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(prepared[1], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[2],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_group_contiguous_deletes() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(110),
                    mutation: TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(110),
                    commit_lsn: PgLsn::from(120),
                    mutation: TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                },
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Delete { predicates, origin } => {
                        assert_eq!(origin, &"delete");
                        assert_eq!(
                            predicates,
                            &vec!["id = 1".to_string(), "id = 2".to_string()]
                        );
                    }
                    PreparedTableMutation::Upsert(_) => panic!("expected delete batch"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_group_contiguous_updates() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(110),
                    mutation: TableMutation::Update {
                        delete_row: TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("before-a".to_string()),
                        ]),
                        upsert_row: TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("after-a".to_string()),
                        ]),
                    },
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(110),
                    commit_lsn: PgLsn::from(120),
                    mutation: TableMutation::Update {
                        delete_row: TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("before-b".to_string()),
                        ]),
                        upsert_row: TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("after-b".to_string()),
                        ]),
                    },
                },
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 4);
                assert!(matches!(prepared[0], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[1],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(prepared[2], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[3],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_split_non_inserts_at_cap() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let tracked = (0..=CDC_MUTATION_BATCH_SIZE)
            .map(|idx| TrackedTableMutation {
                start_lsn: PgLsn::from(100 + idx as u64),
                commit_lsn: PgLsn::from(200 + idx as u64),
                mutation: TableMutation::Delete(TableRow::new(vec![
                    Cell::I32(idx as i32),
                    Cell::String(format!("name-{idx}")),
                ])),
            })
            .collect();
        let batches =
            prepare_mutation_table_batches(&table_schema, "public_users".to_string(), tracked)
                .unwrap();

        assert_eq!(batches.len(), 2);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), CDC_MUTATION_BATCH_SIZE);
                }
                PreparedTableMutation::Upsert(_) => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), 1);
                }
                PreparedTableMutation::Upsert(_) => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_isolate_update_in_its_own_atomic_batch() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(110),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(110),
                    commit_lsn: PgLsn::from(120),
                    mutation: TableMutation::Update {
                        delete_row: TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("seed".to_string()),
                        ]),
                        upsert_row: TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("grown".to_string()),
                        ]),
                    },
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(120),
                    commit_lsn: PgLsn::from(130),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_string()),
                    ])),
                },
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 4);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(prepared[1], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[2],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(
                    prepared[3],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_build_mutation_batch_identity_is_deterministic() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let tracked = vec![
            TrackedTableMutation {
                start_lsn: PgLsn::from(100),
                commit_lsn: PgLsn::from(200),
                mutation: TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            },
            TrackedTableMutation {
                start_lsn: PgLsn::from(100),
                commit_lsn: PgLsn::from(200),
                mutation: TableMutation::Delete(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            },
        ];

        let first = build_mutation_batch_identity("public_users", &table_schema, &tracked).unwrap();
        let second =
            build_mutation_batch_identity("public_users", &table_schema, &tracked).unwrap();

        assert_eq!(first.batch_id, second.batch_id);
        assert_eq!(first.first_start_lsn, Some(PgLsn::from(100)));
        assert_eq!(first.last_commit_lsn, Some(PgLsn::from(200)));
    }

    #[test]
    fn test_build_mutation_batch_identity_changes_with_order_and_lsn() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let original = build_mutation_batch_identity(
            "public_users",
            &table_schema,
            &[
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(200),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(200),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                },
            ],
        )
        .unwrap();
        let reordered = build_mutation_batch_identity(
            "public_users",
            &table_schema,
            &[
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(200),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                },
                TrackedTableMutation {
                    start_lsn: PgLsn::from(100),
                    commit_lsn: PgLsn::from(200),
                    mutation: TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                },
            ],
        )
        .unwrap();
        let changed_lsn = build_mutation_batch_identity(
            "public_users",
            &table_schema,
            &[TrackedTableMutation {
                start_lsn: PgLsn::from(101),
                commit_lsn: PgLsn::from(201),
                mutation: TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            }],
        )
        .unwrap();

        assert_ne!(original.batch_id, reordered.batch_id);
        assert_ne!(original.batch_id, changed_lsn.batch_id);
    }

    #[test]
    fn test_build_truncate_batch_identity_changes_with_lsn() {
        let first = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent {
                start_lsn: PgLsn::from(300),
                commit_lsn: PgLsn::from(400),
                options: 0,
            }],
        );
        let second = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent {
                start_lsn: PgLsn::from(301),
                commit_lsn: PgLsn::from(401),
                options: 0,
            }],
        );

        assert_ne!(first.batch_id, second.batch_id);
    }

    #[tokio::test]
    async fn test_query_table_storage_metrics_reads_ducklake_metadata() {
        let dir = TempDir::new().expect("failed to create temp dir");
        let catalog = path_to_file_url(&dir.path().join("catalog.ducklake"));
        let data = path_to_file_url(&dir.path().join("data"));
        let store = MemoryStore::new();
        let schema = make_schema(1, "public", "users");
        let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

        store
            .store_table_schema(schema.clone())
            .await
            .expect("failed to seed schema");

        let destination =
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, None, store)
                .await
                .expect("failed to create destination");

        destination
            .write_table_rows(
                schema.id,
                vec![
                    TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]),
                    TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_string())]),
                ],
            )
            .await
            .expect("failed to write rows");

        let conn = open_lake_conn(&catalog, &data);
        let _rows_flushed =
            flush_table_inlined_data(&conn, &table_name, DuckLakeTableBatchKind::Copy)
                .expect("failed to materialize inlined rows for storage metrics test");
        let deadline = Instant::now() + Duration::from_secs(10);
        let metrics = loop {
            let metrics = query_table_storage_metrics_blocking(&conn, &table_name)
                .expect("failed to query storage metrics");
            if metrics.active_data_files >= 1 {
                break metrics;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for storage metrics after materialization"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        assert!(metrics.active_data_files >= 1);
        assert!(metrics.active_data_bytes > 0);
        assert_eq!(metrics.active_delete_files, 0);
        assert_eq!(metrics.deleted_rows, 0);
    }

    #[tokio::test]
    async fn test_query_catalog_maintenance_metrics_reads_ducklake_metadata() {
        let dir = TempDir::new().expect("failed to create temp dir");
        let catalog = path_to_file_url(&dir.path().join("catalog.ducklake"));
        let data = path_to_file_url(&dir.path().join("data"));
        let store = MemoryStore::new();
        let schema = make_schema(1, "public", "users");

        store
            .store_table_schema(schema.clone())
            .await
            .expect("failed to seed schema");

        let destination =
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, None, store)
                .await
                .expect("failed to create destination");

        destination
            .write_table_rows(
                schema.id,
                vec![TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])],
            )
            .await
            .expect("failed to write rows");
        destination
            .truncate_table(schema.id)
            .await
            .expect("failed to truncate table");

        let conn = open_lake_conn(&catalog, &data);
        let metrics = query_catalog_maintenance_metrics_blocking(&conn)
            .expect("failed to query catalog maintenance metrics");

        assert!(metrics.snapshots_total >= 1);
        assert!(metrics.oldest_snapshot_age_seconds >= 0);
        assert!(metrics.files_scheduled_for_deletion_total >= 0);
        assert!(metrics.files_scheduled_for_deletion_bytes >= 0);
        assert!(metrics.oldest_scheduled_deletion_age_seconds >= 0);
    }
}
