#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use etl::{
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
        task_set::DestinationTaskSet,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    state::destination_metadata::DestinationTableMetadata,
    store::{schema::SchemaStore, state::StateStore},
    types::{
        Event, EventSequenceKey, OldTableRow, PartialTableRow, ReplicatedTableSchema, SizeHint,
        TableId, TableName, TableRow, UpdatedTableRow,
    },
};
use metrics::gauge;
use parking_lot::Mutex;
use pg_escape::quote_identifier;
use sqlx::{PgPool, postgres::PgPoolOptions};
#[cfg(feature = "test-utils")]
use tokio::sync::oneshot;
use tokio::{
    sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore},
    task::JoinSet,
};
use tracing::{debug, info};
use url::Url;

use crate::{
    ducklake::{
        DuckLakeTableName, LAKE_CATALOG, S3Config,
        batches::{
            DuckLakeTableBatchKind, TableMutation, TrackedTableMutation, TrackedTruncateEvent,
            apply_table_batch_with_retry, apply_table_batches_with_retry,
            clear_applied_batch_markers_for_kind, clear_table_streaming_progress,
            ensure_applied_batches_table_exists, ensure_streaming_progress_table_exists,
            prepare_copy_table_batch, prepare_mutation_table_batches, prepare_truncate_table_batch,
            read_table_streaming_progress_sequence_key, retain_mutations_after_sequence_key,
            retain_truncates_after_sequence_key,
        },
        client::{
            DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
            format_query_error_detail, run_duckdb_blocking,
        },
        config::{
            MAINTENANCE_TARGET_FILE_SIZE, build_setup_plan, current_duckdb_extension_strategy,
            maintenance_target_file_size_sql, resolve_expire_snapshots_older_than,
            validate_expire_snapshots_older_than_sql,
        },
        inline_size::DuckLakePendingInlineSizeSampler,
        maintenance::{
            DuckLakeMaintenanceWorker, PendingInlineFlushRequests, TableMaintenanceNotification,
            TableWriteActivity, send_maintenance_notification, spawn_ducklake_maintenance_worker,
            table_write_slot,
        },
        metrics::{
            DuckLakeMetricsSampler, ETL_DUCKLAKE_POOL_SIZE, register_metrics,
            resolve_ducklake_metadata_schema_blocking, spawn_ducklake_metrics_sampler,
        },
        schema::build_create_table_sql_ducklake,
    },
    table_name::try_stringify_table_name,
};

/// Shared Postgres metadata pool size for DuckLake background samplers.
///
/// One connection is enough because inline-size sampling and metrics sampling
/// are both best-effort background reads and can safely serialize.
const DUCKLAKE_METADATA_PG_POOL_SIZE: u32 = 1;

/// Builds the shared Postgres metadata pool used by background samplers.
fn build_ducklake_metadata_pg_pool(catalog_url: &Url) -> EtlResult<PgPool> {
    PgPoolOptions::new()
        .max_connections(DUCKLAKE_METADATA_PG_POOL_SIZE)
        .min_connections(0)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(Some(std::time::Duration::from_secs(30)))
        .connect_lazy(catalog_url.as_str())
        .map_err(|source| {
            etl_error!(
                ErrorKind::ConfigError,
                "DuckLake metadata pool configuration failed",
                source: source
            )
        })
}

/// Returns whether a DuckLake DDL error indicates another transaction already
/// created the requested table.
pub(super) fn is_create_table_conflict(error: &duckdb::Error, table_name: &str) -> bool {
    let message = error.to_string();
    message.contains("has been created by another transaction already")
        && message.contains(&format!(r#"attempting to create table "{table_name}""#))
}

// ── destination
// ───────────────────────────────────────────────────────────────

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
/// deferred to coordinated maintenance.
#[derive(Clone)]
pub struct DuckLakeDestination<S> {
    #[cfg(feature = "test-utils")]
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    /// Shared gate that keeps exclusive background maintenance from overlapping
    /// active foreground or table-scoped mutations.
    checkpoint_gate: Arc<RwLock<()>>,
    streaming_tasks: DestinationTaskSet,
    maintenance_worker: Arc<Option<DuckLakeMaintenanceWorker>>,
    metrics_sampler: Arc<Option<DuckLakeMetricsSampler>>,
    table_creation_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    store: S,
    /// Cache of table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
    /// Cache tracking whether the ETL batch marker table already exists. If
    /// it's set then the table has already been created
    applied_batches_table_created: Arc<AtomicBool>,
    /// Cache tracking whether the ETL streaming progress table already exists.
    streaming_progress_table_created: Arc<AtomicBool>,
    /// Signals that one or more inline flushes should run before the next safe
    /// ingest point.
    inline_flush_requested: Arc<AtomicBool>,
    /// Tracks which tables need a requested inline flush before ingestion
    /// resumes.
    inline_flush_requests: Arc<PendingInlineFlushRequests>,
}

impl<S> Destination for DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    fn name() -> &'static str {
        "ducklake"
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.streaming_tasks.drain().await?;
        self.shutdown_maintenance_worker().await?;
        self.shutdown_metrics_sampler().await?;

        Ok(())
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result = self.truncate_table(replicated_table_schema).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_table_rows(replicated_table_schema, table_rows).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        self.streaming_tasks.try_reap().await?;

        let destination = self.clone();
        self.streaming_tasks
            .spawn(async move {
                let result = destination.write_events(events).await;
                async_result.send(result);
            })
            .await;

        Ok(())
    }
}

/// Validates that a replicated table schema can be applied to one DuckLake
/// row-matching mutation.
///
/// DuckLake can stream inserts without replica identity, but update and delete
/// paths need replicated replica-identity columns so the destination can match
/// existing rows safely.
fn validate_ducklake_replica_identity(
    replicated_table_schema: &ReplicatedTableSchema,
    operation: &'static str,
) -> EtlResult<()> {
    if replicated_table_schema.identity_column_schemas().len() == 0 {
        let description = match operation {
            "update" => "DuckLake update requires a replica identity",
            "delete" => "DuckLake delete requires a replica identity",
            _ => "DuckLake mutation requires a replica identity",
        };
        return Err(etl_error!(
            ErrorKind::InvalidState,
            description,
            format!(
                "Table '{}' has no replicated replica-identity columns",
                replicated_table_schema.name()
            )
        ));
    }

    Ok(())
}

impl<S> DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    /// Builds a key-only row from a partial update row when PostgreSQL omits
    /// the old key image because the replica identity did not change.
    fn key_row_from_updated_partial_row(
        replicated_table_schema: &ReplicatedTableSchema,
        partial_row: &PartialTableRow,
    ) -> EtlResult<TableRow> {
        let column_count = replicated_table_schema.column_schemas().len();
        if partial_row.total_columns() != column_count {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row does not match schema",
                format!(
                    "Expected {} replicated columns for table '{}', got {}",
                    column_count,
                    replicated_table_schema.name(),
                    partial_row.total_columns()
                )
            ));
        }

        if partial_row.values().len() + partial_row.missing_column_indexes().len()
            != partial_row.total_columns()
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row shape is inconsistent",
                format!(
                    "Table '{}' partial row reports {} total columns but has {} present and {} \
                     missing",
                    replicated_table_schema.name(),
                    partial_row.total_columns(),
                    partial_row.values().len(),
                    partial_row.missing_column_indexes().len()
                )
            ));
        }

        validate_ducklake_replica_identity(replicated_table_schema, "update")?;

        let mut missing_indexes = partial_row.missing_column_indexes().iter().copied().peekable();
        let mut present_values = partial_row.values().iter();
        let identity_column_count = replicated_table_schema.identity_column_schemas().len();
        let mut identity_columns = replicated_table_schema.identity_column_schemas().peekable();
        let mut key_values = Vec::with_capacity(identity_column_count);

        for (column_index, column_schema) in replicated_table_schema.column_schemas().enumerate() {
            let is_identity = identity_columns.peek().is_some_and(|identity_column| {
                identity_column.ordinal_position == column_schema.ordinal_position
            });

            if missing_indexes.peek().copied() == Some(column_index) {
                missing_indexes.next();
                if is_identity {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake partial update is missing replica-identity columns",
                        format!(
                            "Table '{}' emitted a partial update without key column '{}'",
                            replicated_table_schema.name(),
                            column_schema.name
                        )
                    ));
                }
                continue;
            }

            let Some(value) = present_values.next() else {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake partial update row ended early",
                    format!(
                        "Table '{}' did not provide enough values for its partial update row",
                        replicated_table_schema.name()
                    )
                ));
            };

            if is_identity {
                identity_columns.next();
                key_values.push(value.clone());
            }
        }

        if missing_indexes.next().is_some() || present_values.next().is_some() {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row shape is inconsistent",
                format!(
                    "Table '{}' partial row has leftover values or missing indexes after decoding",
                    replicated_table_schema.name()
                )
            ));
        }

        Ok(TableRow::new(key_values))
    }

    /// Deletes all rows from the destination table.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the truncate work inline.
    pub async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        self.truncate_table_inner(replicated_table_schema).await
    }

    /// Writes an initial-copy batch directly to the destination table.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the write inline.
    pub async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_inner(replicated_table_schema, table_rows).await
    }

    /// Writes one streaming CDC batch directly to the destination.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the batch inline.
    pub async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        #[cfg(feature = "test-utils")]
        wait_if_streaming_write_paused_for_tests().await;

        self.write_events_inner(events).await
    }

    /// Creates a new DuckLake destination.
    ///
    /// - `catalog_url`: DuckLake catalog location. Use a PostgreSQL URL
    ///   (`postgres://user:pass@localhost:5432/mydb`).
    /// - `data_path`: Where Parquet files are stored. Use a local file URL (`file:///tmp/lake_data`)
    ///   or a cloud URL (`s3://bucket/prefix/`, `gs://bucket/prefix/`).
    /// - `pool_size`: Number of warm DuckDB connections maintained in the pool.
    ///   `4` is a reasonable default; higher values allow more tables to be
    ///   written in parallel.
    /// - `s3`: Optional S3 credentials. Required when `data_path` is an S3 URI
    ///   and the bucket is not publicly accessible.
    /// - `metadata_schema`: Optional Postgres schema for DuckLake metadata
    ///   tables (e.g. `"ducklake"`). Uses the catalog default schema when not
    ///   set.
    /// - `duckdb_memory_cache_limit`: Optional DuckDB `memory_limit` value
    ///   (e.g. `"150MB"`). Defaults to `150MB`.
    /// - `maintenance_target_file_size`: Optional DuckLake maintenance
    ///   `target_file_size` value (e.g. `"10MB"`). Defaults to `10MB`.
    /// - `expire_snapshots_older_than`: Optional DuckLake snapshot-retention
    ///   interval (e.g. `"7 days"`). Defaults to `7 days`.
    /// - `duckdb_log`: Optional DuckDB log storage and shutdown dump paths.
    /// - On Linux and macOS, DuckDB extensions are loaded from vendored local
    ///   files when a vendored directory is available. The root directory can
    ///   be forced with `ETL_DUCKDB_EXTENSION_ROOT`. Otherwise, DuckDB uses the
    ///   legacy online `INSTALL` flow. On Windows, DuckDB always uses the
    ///   legacy online `INSTALL` flow.
    ///
    /// Pool initialization is blocking because DuckDB extensions are loaded and
    /// the lake catalog is attached synchronously. This constructor offloads
    /// that warm-up work to Tokio's blocking pool.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        catalog_url: Url,
        data_path: Url,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        duckdb_memory_cache_limit: Option<String>,
        maintenance_target_file_size: Option<String>,
        expire_snapshots_older_than: Option<String>,
        store: S,
    ) -> EtlResult<Self> {
        register_metrics();

        if !matches!(catalog_url.scheme(), "postgres" | "postgresql") {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "DuckLake destination requires a PostgreSQL catalog URL",
                format!("unsupported catalog URL scheme `{}`", catalog_url.scheme())
            ));
        }

        if pool_size == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "DuckLake pool size must be greater than zero",
                "pool_size must be at least 1"
            ));
        }

        let extension_strategy = current_duckdb_extension_strategy()?;
        let disable_extension_autoload = extension_strategy.disables_autoload();
        let maintenance_target_file_size = Arc::<str>::from(
            maintenance_target_file_size
                .unwrap_or_else(|| MAINTENANCE_TARGET_FILE_SIZE.to_string()),
        );
        let expire_snapshots_older_than = Arc::<str>::from(
            resolve_expire_snapshots_older_than(expire_snapshots_older_than.as_deref()).to_owned(),
        );
        if let crate::ducklake::config::DuckDbExtensionStrategy::VendoredLocal { platform_dir } =
            extension_strategy
        {
            info!(platform = platform_dir, "using vendored duckdb extensions");
        }
        let setup_plan = Arc::new(build_setup_plan(
            &catalog_url,
            &data_path,
            s3.as_ref(),
            metadata_schema.as_deref(),
            duckdb_memory_cache_limit.as_deref(),
        )?);

        let manager = Arc::new(DuckLakeConnectionManager {
            setup_plan: Arc::clone(&setup_plan),
            disable_extension_autoload,
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        });

        let pool =
            Arc::new(build_warm_ducklake_pool(manager.as_ref().clone(), pool_size, "write").await?);
        let blocking_slots = Arc::new(Semaphore::new(pool_size as usize));

        // `target_file_size` is a catalog-wide DuckLake option consumed during
        // compaction. Apply it once on the write pool before the maintenance
        // pool starts warming, so the maintenance worker does not need to
        // mutate the catalog from a separate RW DuckDB instance during its
        // background warm-up. Two RW instances ATTACHing the same catalog file
        // and racing a catalog write against concurrent user writes caused
        // lost commits.
        let target_file_size_sql =
            maintenance_target_file_size_sql(Some(maintenance_target_file_size.as_ref()));
        run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            move |conn| -> EtlResult<()> {
                conn.execute_batch(&target_file_size_sql).map_err(|error| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake target_file_size configuration failed",
                        source: error
                    )
                })?;
                Ok(())
            },
        )
        .await?;
        let expire_snapshots_validation_sql =
            validate_expire_snapshots_older_than_sql(expire_snapshots_older_than.as_ref());
        let expire_snapshots_older_than_for_error = Arc::clone(&expire_snapshots_older_than);
        run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            move |conn| -> EtlResult<()> {
                conn.query_row(&expire_snapshots_validation_sql, [], |_row| Ok(())).map_err(
                    |source| {
                        etl_error!(
                            ErrorKind::ConfigError,
                            "DuckLake expire_snapshots_older_than configuration failed",
                            format!(
                                "invalid expire_snapshots_older_than value `{}`",
                                expire_snapshots_older_than_for_error
                            ),
                            source: source
                        )
                    },
                )?;
                Ok(())
            },
        )
        .await?;
        let metadata_schema = match metadata_schema {
            Some(metadata_schema) => metadata_schema,
            None => {
                run_duckdb_blocking(
                    Arc::clone(&pool),
                    Arc::clone(&blocking_slots),
                    DuckDbBlockingOperationKind::Foreground,
                    resolve_ducklake_metadata_schema_blocking,
                )
                .await?
            }
        };
        let metadata_pg_pool = build_ducklake_metadata_pg_pool(&catalog_url)?;
        let pending_inline_size_sampler = Some(DuckLakePendingInlineSizeSampler::new(
            metadata_schema.clone(),
            metadata_pg_pool.clone(),
        ));
        let created_tables = Arc::default();
        let checkpoint_gate = Arc::new(RwLock::new(()));
        let inline_flush_requested = Arc::new(AtomicBool::new(false));
        let inline_flush_requests = Arc::new(PendingInlineFlushRequests::default());
        let mut destination = Self {
            #[cfg(feature = "test-utils")]
            manager,
            pool: Arc::clone(&pool),
            blocking_slots: Arc::clone(&blocking_slots),
            checkpoint_gate: Arc::clone(&checkpoint_gate),
            streaming_tasks: DestinationTaskSet::new(),
            maintenance_worker: Arc::new(None),
            metrics_sampler: Arc::new(None),
            table_creation_slots: Arc::new(Semaphore::new(1)),
            table_write_slots: Arc::default(),
            store,
            created_tables: Arc::clone(&created_tables),
            applied_batches_table_created: Arc::default(),
            streaming_progress_table_created: Arc::default(),
            inline_flush_requested: Arc::clone(&inline_flush_requested),
            inline_flush_requests: Arc::clone(&inline_flush_requests),
        };
        gauge!(ETL_DUCKLAKE_POOL_SIZE).set(pool_size as f64);
        destination.ensure_applied_batches_table_exists().await?;
        destination.ensure_streaming_progress_table_exists().await?;
        destination.maintenance_worker = Arc::new(
            spawn_ducklake_maintenance_worker(
                DuckLakeConnectionManager {
                    setup_plan: Arc::clone(&setup_plan),
                    disable_extension_autoload,
                    #[cfg(feature = "test-utils")]
                    open_count: Arc::new(AtomicUsize::new(0)),
                },
                Arc::clone(&checkpoint_gate),
                Arc::clone(&destination.table_write_slots),
                Arc::clone(&inline_flush_requested),
                Arc::clone(&inline_flush_requests),
                pending_inline_size_sampler,
                Arc::clone(&expire_snapshots_older_than),
            )?
            .into(),
        );
        destination.metrics_sampler = Arc::new(
            spawn_ducklake_metrics_sampler(
                metadata_schema,
                metadata_pg_pool,
                Arc::clone(&created_tables),
                destination
                    .maintenance_worker
                    .as_ref()
                    .as_ref()
                    .ok_or_else(|| {
                        etl_error!(
                            ErrorKind::DestinationError,
                            "Ducklake initialization failed",
                            "maintenance worker should exist before metrics sampler"
                        )
                    })?
                    .notification_tx
                    .clone(),
            )?
            .into(),
        );

        Ok(destination)
    }

    /// Truncates the destination table while keeping its schema and name.
    async fn truncate_table_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(replicated_table_schema).await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        self.ensure_applied_batches_table_exists().await?;
        self.ensure_streaming_progress_table_exists().await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        self.run_duckdb_blocking(
            DuckDbBlockingOperationKind::Foreground,
            move |conn| -> EtlResult<()> {
                conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake BEGIN TRANSACTION failed",
                        source: e
                    )
                })?;

                let result = (|| -> EtlResult<()> {
                    let truncate_table_sql =
                        format!(r#"TRUNCATE TABLE {LAKE_CATALOG}."{table_name}";"#);
                    conn.execute_batch(&truncate_table_sql).map_err(|e| {
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake TRUNCATE TABLE failed",
                            format_query_error_detail(&truncate_table_sql, &e),
                            source: e
                        )
                    })?;

                    clear_applied_batch_markers_for_kind(
                        conn,
                        &table_name,
                        DuckLakeTableBatchKind::Copy,
                    )?;
                    clear_applied_batch_markers_for_kind(
                        conn,
                        &table_name,
                        DuckLakeTableBatchKind::Mutation,
                    )?;
                    clear_applied_batch_markers_for_kind(
                        conn,
                        &table_name,
                        DuckLakeTableBatchKind::Truncate,
                    )?;
                    clear_table_streaming_progress(conn, &table_name)?;
                    Ok(())
                })();

                match result {
                    Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake COMMIT failed",
                            source: e
                        )
                    }),
                    Err(error) => {
                        let err = conn.execute_batch("ROLLBACK");
                        if let Err(err) = err {
                            tracing::error!(?err, "error rollback");
                        }
                        Err(error)
                    }
                }
            },
        )
        .await
    }

    /// Bulk-inserts rows into the destination table inside a single
    /// transaction.
    ///
    /// Wrapping all inserts in one `BEGIN` / `COMMIT` ensures they are written
    /// as one atomic DuckLake change rather than one file per row.
    ///
    /// Copy batches are recorded in the replay marker table so a retry after an
    /// ambiguous post-commit failure can detect already applied rows.
    ///
    /// Small copy batches may stay inlined until maintenance requests a safe
    /// materialization after we emit the maintenance notification.
    async fn write_table_rows_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(replicated_table_schema).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        self.maybe_run_requested_inline_flush().await?;

        let approx_bytes = table_rows.iter().map(|row| row.size_hint() as u64).sum::<u64>();
        let inserted_rows = table_rows.len() as u64;

        // Copy batches for the same table must still serialize so concurrent
        // callers do not race each other inside DuckDB.
        self.ensure_applied_batches_table_exists().await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        let prepared_batch =
            prepare_copy_table_batch(replicated_table_schema, table_name, table_rows)?;
        let table_name = prepared_batch.table_name().to_owned();
        apply_table_batch_with_retry(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            prepared_batch,
        )
        .await?;
        self.notify_background_maintenance(TableMaintenanceNotification::WriteActivity(
            TableWriteActivity { table_name, approx_bytes, inserted_rows },
        ))
        .await;

        Ok(())
    }

    /// Writes streaming CDC events to the destination.
    ///
    /// Insert, Update, and Delete events are grouped by table and written in
    /// parallel, each table in its own async task. Each DuckDB attempt acquires
    /// one blocking slot before entering `spawn_blocking`. Each table's ordered
    /// CDC stream is split into atomic sub-batches, applied on a reused DuckDB
    /// connection per retry attempt, and acknowledged through one per-table
    /// streaming replay watermark so retries can safely detect already
    /// committed work.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        self.maybe_run_requested_inline_flush().await?;
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_mutations: HashMap<
                TableId,
                (ReplicatedTableSchema, Vec<TrackedTableMutation>),
            > = HashMap::new();

            // Accumulate non-truncate events, stopping at the first Truncate.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    // Handled later
                    break;
                }

                let Some(event) = event_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(insert) => {
                        let table_id = insert.replicated_table_schema.id();
                        let entry = table_id_to_mutations.entry(table_id).or_insert_with(|| {
                            (insert.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.0 = insert.replicated_table_schema;
                        entry.1.push(TrackedTableMutation::new(
                            insert.start_lsn,
                            insert.commit_lsn,
                            insert.tx_ordinal,
                            TableMutation::Insert(insert.table_row),
                        ));
                    }
                    Event::Update(update) => {
                        validate_ducklake_replica_identity(
                            &update.replicated_table_schema,
                            "update",
                        )?;
                        let table_id = update.replicated_table_schema.id();
                        let entry = table_id_to_mutations.entry(table_id).or_insert_with(|| {
                            (update.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.0 = update.replicated_table_schema;
                        let table_row = update.updated_table_row;
                        let old_table_row = update.old_table_row;
                        let mutations = &mut entry.1;
                        if let Some(old_row) = old_table_row {
                            mutations.push(TrackedTableMutation::new(
                                update.start_lsn,
                                update.commit_lsn,
                                update.tx_ordinal,
                                TableMutation::Update { delete_row: old_row, new_row: table_row },
                            ));
                        } else {
                            match table_row {
                                UpdatedTableRow::Full(table_row) => {
                                    debug!(
                                        "update event has no old row, deleting by replica \
                                         identity from new row"
                                    );
                                    mutations.push(TrackedTableMutation::new(
                                        update.start_lsn,
                                        update.commit_lsn,
                                        update.tx_ordinal,
                                        TableMutation::Replace(table_row),
                                    ));
                                }
                                UpdatedTableRow::Partial(partial_row) => {
                                    let key_row = Self::key_row_from_updated_partial_row(
                                        &entry.0,
                                        &partial_row,
                                    )?;
                                    debug!(
                                        "update event has no old row, building key image from \
                                         partial new row"
                                    );
                                    mutations.push(TrackedTableMutation::new(
                                        update.start_lsn,
                                        update.commit_lsn,
                                        update.tx_ordinal,
                                        TableMutation::Update {
                                            delete_row: OldTableRow::Key(key_row),
                                            new_row: UpdatedTableRow::Partial(partial_row),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                    Event::Delete(delete) => {
                        validate_ducklake_replica_identity(
                            &delete.replicated_table_schema,
                            "delete",
                        )?;
                        let Some(old_row) = delete.old_table_row else {
                            return Err(etl_error!(
                                ErrorKind::InvalidState,
                                "DuckLake delete requires an old row image",
                                format!(
                                    "Table '{}' emitted a delete without an old row despite \
                                     exposing replica-identity columns",
                                    delete.replicated_table_schema.name()
                                )
                            ));
                        };
                        let table_id = delete.replicated_table_schema.id();
                        let entry = table_id_to_mutations.entry(table_id).or_insert_with(|| {
                            (delete.replicated_table_schema.clone(), Vec::new())
                        });
                        entry.0 = delete.replicated_table_schema;
                        entry.1.push(TrackedTableMutation::new(
                            delete.start_lsn,
                            delete.commit_lsn,
                            delete.tx_ordinal,
                            TableMutation::Delete(old_row),
                        ));
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            if !table_id_to_mutations.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                self.ensure_streaming_progress_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (_, (replicated_table_schema, mutations)) in table_id_to_mutations {
                    let table_name = self.ensure_table_exists(&replicated_table_schema).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let checkpoint_gate = Arc::clone(&self.checkpoint_gate);
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let destination_table_name = table_name.clone();
                    let maintenance_worker = Arc::clone(&self.maintenance_worker);

                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
                        let _checkpoint_guard = checkpoint_gate.read_owned().await;
                        let last_sequence_key =
                            read_table_streaming_progress_sequence_key_blocking(
                                Arc::clone(&pool),
                                Arc::clone(&blocking_slots),
                                destination_table_name.clone(),
                            )
                            .await?;
                        let pending_mutations =
                            retain_mutations_after_sequence_key(mutations, last_sequence_key);
                        if pending_mutations.is_empty() {
                            debug!(
                                table = %destination_table_name,
                                "ducklake streaming mutation replay skipped, no pending events"
                            );
                            return Ok::<(), etl::error::EtlError>(());
                        }

                        let maintenance_notification =
                            maintenance_worker.as_ref().as_ref().map(|_| {
                                TableMaintenanceNotification::WriteActivity(
                                    table_write_activity_for_mutations(
                                        destination_table_name.clone(),
                                        pending_mutations.as_slice(),
                                    ),
                                )
                            });
                        let prepared_batches = prepare_mutation_table_batches(
                            &replicated_table_schema,
                            destination_table_name.clone(),
                            pending_mutations,
                        )?;
                        apply_table_batches_with_retry(pool, blocking_slots, prepared_batches)
                            .await?;
                        if let (Some(worker), Some(notification)) =
                            (maintenance_worker.as_ref(), maintenance_notification)
                        {
                            send_maintenance_notification(&worker.notification_tx, notification)
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
            let mut truncate_table_ids: HashMap<
                TableId,
                (ReplicatedTableSchema, Vec<TrackedTruncateEvent>),
            > = HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for replicated_table_schema in truncate.truncated_tables {
                        let table_id = replicated_table_schema.id();
                        let entry = truncate_table_ids
                            .entry(table_id)
                            .or_insert_with(|| (replicated_table_schema.clone(), Vec::new()));
                        entry.0 = replicated_table_schema;
                        entry.1.push(TrackedTruncateEvent::new(
                            truncate.start_lsn,
                            truncate.commit_lsn,
                            truncate.tx_ordinal,
                            truncate.options,
                        ));
                    }
                }
            }

            if !truncate_table_ids.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                self.ensure_streaming_progress_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (_, (replicated_table_schema, truncates)) in truncate_table_ids {
                    let table_name = self.ensure_table_exists(&replicated_table_schema).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let checkpoint_gate = Arc::clone(&self.checkpoint_gate);
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
                        let _checkpoint_guard = checkpoint_gate.read_owned().await;
                        let last_sequence_key =
                            read_table_streaming_progress_sequence_key_blocking(
                                Arc::clone(&pool),
                                Arc::clone(&blocking_slots),
                                table_name.clone(),
                            )
                            .await?;
                        let pending_truncates =
                            retain_truncates_after_sequence_key(truncates, last_sequence_key);
                        if pending_truncates.is_empty() {
                            debug!(
                                table = %table_name,
                                "ducklake streaming truncate replay skipped, no pending events"
                            );
                            return Ok(());
                        }

                        let prepared_batch =
                            prepare_truncate_table_batch(table_name, pending_truncates);
                        apply_table_batch_with_retry(pool, blocking_slots, prepared_batch).await
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    result.map_err(|_| {
                        etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake truncate task panicked")
                    })??;
                }
            }
        }

        Ok(())
    }

    /// Ensures the destination table exists, creating it (DDL) if necessary.
    async fn ensure_table_exists(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<DuckLakeTableName> {
        let table_id = replicated_table_schema.id();
        let table_name = self.get_or_create_destination_table_name(replicated_table_schema).await?;

        // Fast path: already created.
        {
            let cache = self.created_tables.lock();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        let _table_creation_permit =
            Arc::clone(&self.table_creation_slots).acquire_owned().await.map_err(|_| {
                etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
            })?;

        {
            let cache = self.created_tables.lock();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        let metadata = DestinationTableMetadata::new_applying(
            table_name.clone(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;

        // `build_create_table_sql_ducklake` generates `CREATE TABLE IF NOT EXISTS
        // "name" (...)`. Prefix the table name with the catalog alias so
        // DuckLake knows which catalog to create the table in.
        let ddl = build_create_table_sql_ducklake(
            &table_name,
            &replicated_table_schema.inner().column_schemas,
        );
        let quoted_table_name = quote_identifier(&table_name).into_owned();
        let qualified_ddl =
            ddl.replace(&quoted_table_name, &format!("{LAKE_CATALOG}.{quoted_table_name}"));

        let created_tables = Arc::clone(&self.created_tables);
        let table_name_clone = table_name.clone();
        let _checkpoint_guard = self.acquire_mutation_guard().await;

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
        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;

        Ok(table_name)
    }

    /// Ensures the ETL-managed replay marker table exists.
    async fn ensure_applied_batches_table_exists(&self) -> EtlResult<()> {
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        ensure_applied_batches_table_exists(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            Arc::clone(&self.table_creation_slots),
            Arc::clone(&self.applied_batches_table_created),
        )
        .await
    }

    /// Ensures the ETL-managed streaming progress table exists.
    async fn ensure_streaming_progress_table_exists(&self) -> EtlResult<()> {
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        ensure_streaming_progress_table_exists(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            Arc::clone(&self.table_creation_slots),
            Arc::clone(&self.streaming_progress_table_created),
        )
        .await
    }

    /// Returns the stored destination table name for `table_id`, creating a
    /// default name if none exists yet.
    async fn get_or_create_destination_table_name(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<DuckLakeTableName> {
        let table_id = replicated_table_schema.id();

        if let Some(existing) = self.store.get_destination_table_metadata(table_id).await? {
            return Ok(existing.destination_table_id);
        }

        table_name_to_ducklake_table_name(replicated_table_schema.name())
    }

    /// Serializes table-local truncate and CDC mutation writes.
    async fn acquire_table_write_slot(&self, table_name: &str) -> EtlResult<OwnedSemaphorePermit> {
        let table_slot = table_write_slot(&self.table_write_slots, table_name);

        table_slot.acquire_owned().await.map_err(|_| {
            etl_error!(ErrorKind::InvalidState, "DuckLake table write semaphore closed")
        })
    }

    /// Acquires shared mutation access so exclusive background maintenance
    /// cannot start in the middle of a foreground write sequence.
    async fn acquire_mutation_guard(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.checkpoint_gate).read_owned().await
    }

    /// Runs requested inline flushes before foreground ingestion begins when
    /// safe.
    async fn maybe_run_requested_inline_flush(&self) -> EtlResult<()> {
        if !self.inline_flush_requested.load(Ordering::Acquire) {
            return Ok(());
        }

        crate::ducklake::maintenance::maybe_run_requested_inline_flush(
            Arc::clone(&self.pool),
            Arc::clone(&self.checkpoint_gate),
            Arc::clone(&self.blocking_slots),
            self.inline_flush_requested.as_ref(),
            self.inline_flush_requests.as_ref(),
            self.maintenance_worker.as_ref().as_ref().map(|worker| worker.notification_tx.clone()),
        )
        .await
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

    /// Sends one background-maintenance notification to the maintenance worker.
    async fn notify_background_maintenance(&self, notification: TableMaintenanceNotification) {
        if let Some(maintenance_worker) = &*self.maintenance_worker {
            send_maintenance_notification(&maintenance_worker.notification_tx, notification).await;
        }
    }

    /// Returns how many DuckDB connections have been initialized for tests.
    #[cfg(feature = "test-utils")]
    pub fn connection_open_count_for_tests(&self) -> usize {
        self.manager.open_count_for_tests()
    }
}

/// Reads the persisted streaming replay watermark for one table on DuckDB's
/// blocking pool.
async fn read_table_streaming_progress_sequence_key_blocking(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_name: DuckLakeTableName,
) -> EtlResult<Option<EventSequenceKey>> {
    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Foreground,
        move |conn| read_table_streaming_progress_sequence_key(conn, &table_name),
    )
    .await
}

/// Recomputes maintenance stats from the suffix of mutations that still needs
/// applying.
fn table_write_activity_for_mutations(
    table_name: DuckLakeTableName,
    tracked_mutations: &[TrackedTableMutation],
) -> TableWriteActivity {
    let mut write_activity = TableWriteActivity { table_name, approx_bytes: 0, inserted_rows: 0 };

    for tracked_mutation in tracked_mutations {
        // This is only a fallback estimate for catalogs where we cannot sample
        // actual inlined-table sizes directly. The optional row-threshold path
        // is disabled today, so we treat each mutation as one unit of fallback
        // activity to keep mutation-only streams visible to maintenance.
        write_activity.approx_bytes =
            write_activity.approx_bytes.saturating_add(tracked_mutation.write_activity_size_hint());
        write_activity.inserted_rows = write_activity.inserted_rows.saturating_add(1);
    }

    write_activity
}

#[cfg(feature = "test-utils")]
struct PausedStreamingWriteHook {
    reached_tx: oneshot::Sender<()>,
    resume_rx: oneshot::Receiver<()>,
}

#[cfg(feature = "test-utils")]
static PAUSED_STREAMING_WRITE_HOOK: std::sync::LazyLock<Mutex<Option<PausedStreamingWriteHook>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static PAUSED_STREAMING_WRITE_RESUME_TX: std::sync::LazyLock<Mutex<Option<oneshot::Sender<()>>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

/// Arms a one-shot hook that pauses the next streaming write before DuckLake
/// starts applying it.
#[cfg(feature = "test-utils")]
pub fn arm_pause_next_streaming_write_for_tests() -> oneshot::Receiver<()> {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    *PAUSED_STREAMING_WRITE_HOOK.lock() = Some(PausedStreamingWriteHook { reached_tx, resume_rx });
    *PAUSED_STREAMING_WRITE_RESUME_TX.lock() = Some(resume_tx);
    reached_rx
}

/// Releases the paused streaming-write test hook, if one is armed.
#[cfg(feature = "test-utils")]
pub fn release_paused_streaming_write_for_tests() {
    if let Some(resume_tx) = PAUSED_STREAMING_WRITE_RESUME_TX.lock().take() {
        let _ = resume_tx.send(());
    }
}

/// Clears the paused streaming-write hook without waiting for it to be
/// released.
#[cfg(feature = "test-utils")]
pub fn reset_paused_streaming_write_for_tests() {
    PAUSED_STREAMING_WRITE_HOOK.lock().take();
    PAUSED_STREAMING_WRITE_RESUME_TX.lock().take();
}

#[cfg(feature = "test-utils")]
async fn wait_if_streaming_write_paused_for_tests() {
    let Some(PausedStreamingWriteHook { reached_tx, resume_rx }) =
        PAUSED_STREAMING_WRITE_HOOK.lock().take()
    else {
        return;
    };

    let _ = reached_tx.send(());
    let _ = resume_rx.await;
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

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::{Path, PathBuf},
        time::Instant,
    };

    use duckdb::{Config, Connection};
    use etl::{
        config::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig},
        store::{both::memory::MemoryStore, schema::SchemaStore},
        types::{
            Cell, ColumnSchema, IdentityMask, OldTableRow, PartialTableRow, PgLsn, ReplicationMask,
            SizeHint, TableRow, TableSchema, Type as PgType, UpdatedTableRow,
        },
    };
    use etl_postgres::tokio::test_utils::PgDatabase;
    use pg_escape::{quote_identifier, quote_literal};
    use tempfile::TempDir;
    use tokio_postgres::Client;
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::ducklake::{
        config::catalog_conninfo_from_url,
        maintenance::flush_table_inlined_data,
        metrics::{query_catalog_maintenance_metrics, query_table_storage_metrics},
    };

    const POSTGRES_SCANNER_EXTENSION_FILE: &str = "postgres_scanner.duckdb_extension";

    fn make_schema(table_id: u32, schema: &str, table: &str) -> TableSchema {
        TableSchema::new(
            TableId::new(table_id),
            TableName::new(schema.to_string(), table.to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, 2, None, true),
            ],
        )
    }

    fn make_alternative_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(2),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("email".to_string(), PgType::TEXT, -1, 2, None, false),
                ColumnSchema::new("payload".to_string(), PgType::TEXT, -1, 3, None, true),
            ],
        ));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0]),
        )
    }

    fn make_missing_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(make_schema(3, "public", "users"));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 0]),
        )
    }

    #[test]
    fn table_write_activity_for_mutations_counts_partial_updates_and_deletes() {
        let delete_row = OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]));
        let update_delete_row = OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]));
        let partial_row = UpdatedTableRow::Partial(PartialTableRow::new(
            2,
            TableRow::new(vec![Cell::I32(1), Cell::String("grown".to_string())]),
            vec![],
        ));
        let expected_bytes = delete_row.size_hint() as u64
            + update_delete_row.size_hint() as u64
            + partial_row.size_hint() as u64;
        let write_activity = table_write_activity_for_mutations(
            "public_users".to_string(),
            &[
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(100),
                    0,
                    TableMutation::Delete(delete_row),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(100),
                    1,
                    TableMutation::Update { delete_row: update_delete_row, new_row: partial_row },
                ),
            ],
        );

        assert_eq!(write_activity.approx_bytes, expected_bytes);
        assert_eq!(write_activity.inserted_rows, 2);
    }

    #[test]
    fn key_row_from_updated_partial_row_uses_alternative_identity_columns() {
        let replicated_table_schema = make_alternative_identity_schema();
        let partial_row = PartialTableRow::new(
            3,
            TableRow::new(vec![Cell::I32(1), Cell::String("alice@example.com".to_string())]),
            vec![2],
        );

        let key_row = DuckLakeDestination::<MemoryStore>::key_row_from_updated_partial_row(
            &replicated_table_schema,
            &partial_row,
        )
        .unwrap();

        assert_eq!(key_row, TableRow::new(vec![Cell::String("alice@example.com".to_string())]));
    }

    #[test]
    fn key_row_from_updated_partial_row_rejects_missing_replica_identity() {
        let replicated_table_schema = make_missing_identity_schema();
        let partial_row = PartialTableRow::new(
            2,
            TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]),
            vec![],
        );

        let error = DuckLakeDestination::<MemoryStore>::key_row_from_updated_partial_row(
            &replicated_table_schema,
            &partial_row,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidState);
        assert_eq!(error.description(), Some("DuckLake update requires a replica identity"));
    }

    fn path_to_file_url(path: &Path) -> Url {
        Url::from_file_path(path).expect("failed to convert path to file url")
    }

    fn local_pg_connection_config(database_name: String) -> PgConnectionConfig {
        PgConnectionConfig {
            host: env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set"),
            port: env::var("TESTS_DATABASE_PORT")
                .expect("TESTS_DATABASE_PORT must be set")
                .parse()
                .expect("TESTS_DATABASE_PORT must be a valid port number"),
            name: database_name,
            username: env::var("TESTS_DATABASE_USERNAME")
                .expect("TESTS_DATABASE_USERNAME must be set"),
            password: env::var("TESTS_DATABASE_PASSWORD").ok().map(Into::into),
            tls: TlsConfig { trusted_root_certs: String::new(), enabled: false },
            keepalive: TcpKeepaliveConfig::default(),
        }
    }

    async fn create_catalog_database() -> (PgDatabase<Client>, Url) {
        let database_name = Uuid::new_v4().to_string();
        let host = env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set");
        let port = env::var("TESTS_DATABASE_PORT")
            .expect("TESTS_DATABASE_PORT must be set")
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number");
        let username =
            env::var("TESTS_DATABASE_USERNAME").expect("TESTS_DATABASE_USERNAME must be set");
        let password = env::var("TESTS_DATABASE_PASSWORD").ok();
        let database = PgDatabase::new(local_pg_connection_config(database_name.clone())).await;

        let mut catalog_url = Url::parse("postgres://localhost").expect("failed to parse base url");
        catalog_url.set_host(Some(&host)).expect("failed to set catalog host");
        catalog_url.set_port(Some(port)).expect("failed to set catalog port");
        catalog_url.set_username(&username).expect("failed to set catalog username");
        catalog_url.set_password(password.as_deref()).expect("failed to set catalog password");
        catalog_url.set_path(&database_name);

        (database, catalog_url)
    }

    fn current_vendored_extension_dir() -> Option<PathBuf> {
        let platform_dir = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64" | "amd64") => "linux_amd64",
            ("linux", "aarch64" | "arm64") => "linux_arm64",
            ("macos", "x86_64" | "amd64") => "osx_amd64",
            ("macos", "aarch64" | "arm64") => "osx_arm64",
            _ => return None,
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
            let extension_dir = root.join("1.5.2").join(platform_dir);
            let ducklake_extension = extension_dir.join("ducklake.duckdb_extension");
            let postgres_scanner_extension = extension_dir.join(POSTGRES_SCANNER_EXTENSION_FILE);

            if ducklake_extension.is_file() && postgres_scanner_extension.is_file() {
                return Some(extension_dir);
            }
        }

        None
    }

    fn open_verification_connection() -> Connection {
        let duckdb_dir = tempfile::Builder::new()
            .prefix("etl_ducklake_verify_")
            .tempdir()
            .expect("failed to create verification duckdb dir")
            .keep();
        let duckdb_path = duckdb_dir.join("verify.duckdb");

        if current_vendored_extension_dir().is_some() {
            return Connection::open_with_flags(
                &duckdb_path,
                Config::default()
                    .enable_autoload_extension(false)
                    .expect("failed to disable DuckDB extension autoload"),
            )
            .expect("failed to open verification DuckDB");
        }

        Connection::open(&duckdb_path).expect("failed to open verification DuckDB")
    }

    fn ducklake_load_sql() -> String {
        if let Some(extension_dir) = current_vendored_extension_dir() {
            let ducklake_extension = extension_dir.join("ducklake.duckdb_extension");
            let postgres_scanner_extension = extension_dir.join(POSTGRES_SCANNER_EXTENSION_FILE);

            return format!(
                "LOAD {}; LOAD {};",
                quote_literal(&ducklake_extension.display().to_string()),
                quote_literal(&postgres_scanner_extension.display().to_string()),
            );
        }

        "INSTALL ducklake; LOAD ducklake; INSTALL postgres_scanner; LOAD postgres_scanner;"
            .to_string()
    }

    fn open_lake_conn(catalog: &Url, data: &Url) -> Connection {
        let conn = open_verification_connection();
        let catalog_attach_target =
            catalog_conninfo_from_url(catalog).expect("invalid catalog url");
        conn.execute_batch(&format!(
            "{} ATTACH {} AS {} (DATA_PATH {});",
            ducklake_load_sql(),
            quote_literal(&format!("ducklake:{catalog_attach_target}")),
            quote_identifier(LAKE_CATALOG),
            quote_literal(data.as_str()),
        ))
        .expect("failed to attach DuckLake catalog");
        conn
    }

    fn lake_table_exists(conn: &Connection, table_name: &str) -> bool {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = {} AND \
                 table_schema = {} AND table_name = {}",
                quote_literal(LAKE_CATALOG),
                quote_literal("main"),
                quote_literal(table_name),
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count > 0)
        .unwrap_or(false)
    }

    async fn open_lake_conn_when_table_visible(
        catalog: &Url,
        data: &Url,
        table_name: &str,
    ) -> Connection {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let conn = open_lake_conn(catalog, data);
            if lake_table_exists(&conn, table_name) {
                return conn;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for DuckLake table `{table_name}` to become visible",
            );
            drop(conn);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[test]
    fn table_name_escaping() {
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
    fn is_create_table_conflict_matches_ducklake_commit_conflict() {
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake \
                 transaction. Transaction conflict - attempting to create table \"public_users\" \
                 in schema \"main\" - but this table has been created by another transaction \
                 already"
                    .to_string(),
            ),
        );

        assert!(is_create_table_conflict(&error, "public_users"));
        assert!(!is_create_table_conflict(&error, "public_orders"));
    }

    mod postgres_backed {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn query_table_storage_metrics_reads_ducklake_metadata() {
            let dir = TempDir::new().expect("failed to create temp dir");
            let data = path_to_file_url(&dir.path().join("data"));
            let (_catalog_database, catalog) = create_catalog_database().await;
            let store = MemoryStore::new();
            let schema = make_schema(1, "public", "users");
            let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
            let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

            store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

            let destination = DuckLakeDestination::new(
                catalog.clone(),
                data.clone(),
                1,
                None,
                None,
                None,
                None,
                None,
                store,
            )
            .await
            .expect("failed to create destination");

            destination
                .write_table_rows(
                    &replicated_table_schema,
                    vec![
                        TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]),
                        TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_string())]),
                    ],
                )
                .await
                .expect("failed to write rows");

            let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
            let metadata_schema = resolve_ducklake_metadata_schema_blocking(&conn)
                .expect("failed to resolve metadata schema");
            let metadata_pg_pool =
                build_ducklake_metadata_pg_pool(&catalog).expect("failed to create metadata pool");
            let _rows_flushed = flush_table_inlined_data(&conn, &table_name)
                .expect("failed to materialize inlined rows for storage metrics test");
            let deadline = Instant::now() + Duration::from_secs(10);
            let metrics = loop {
                let metrics =
                    query_table_storage_metrics(&metadata_pg_pool, &metadata_schema, &table_name)
                        .await
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

        #[tokio::test(flavor = "multi_thread")]
        async fn query_catalog_maintenance_metrics_reports_active_data_files_total() {
            let dir = TempDir::new().expect("failed to create temp dir");
            let data = path_to_file_url(&dir.path().join("data"));
            let (_catalog_database, catalog) = create_catalog_database().await;
            let store = MemoryStore::new();
            let schema = make_schema(1, "public", "users");
            let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
            let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

            store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

            let destination = DuckLakeDestination::new(
                catalog.clone(),
                data.clone(),
                1,
                None,
                None,
                None,
                None,
                None,
                store,
            )
            .await
            .expect("failed to create destination");

            destination
                .write_table_rows(
                    &replicated_table_schema,
                    vec![
                        TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]),
                        TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_string())]),
                    ],
                )
                .await
                .expect("failed to write rows");

            let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
            let metadata_schema = resolve_ducklake_metadata_schema_blocking(&conn)
                .expect("failed to resolve metadata schema");
            let metadata_pg_pool =
                build_ducklake_metadata_pg_pool(&catalog).expect("failed to create metadata pool");
            let _rows_flushed = flush_table_inlined_data(&conn, &table_name)
                .expect("failed to materialize inlined rows for catalog metrics test");
            let deadline = Instant::now() + Duration::from_secs(10);
            let metrics = loop {
                let metrics =
                    query_catalog_maintenance_metrics(&metadata_pg_pool, &metadata_schema)
                        .await
                        .expect("failed to query catalog maintenance metrics");
                if metrics.active_data_files_total >= 1 {
                    break metrics;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for active data files total after materialization"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };

            assert!(metrics.active_data_files_total >= 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn query_catalog_maintenance_metrics_reads_ducklake_metadata() {
            let dir = TempDir::new().expect("failed to create temp dir");
            let data = path_to_file_url(&dir.path().join("data"));
            let (_catalog_database, catalog) = create_catalog_database().await;
            let store = MemoryStore::new();
            let schema = make_schema(1, "public", "users");
            let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
            let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

            store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

            let destination = DuckLakeDestination::new(
                catalog.clone(),
                data.clone(),
                1,
                None,
                None,
                None,
                None,
                None,
                store,
            )
            .await
            .expect("failed to create destination");

            destination
                .write_table_rows(
                    &replicated_table_schema,
                    vec![TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())])],
                )
                .await
                .expect("failed to write rows");
            destination
                .truncate_table(&replicated_table_schema)
                .await
                .expect("failed to truncate table");

            destination.shutdown().await.expect("failed to shutdown destination");
            drop(destination);

            let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
            let metadata_schema = resolve_ducklake_metadata_schema_blocking(&conn)
                .expect("failed to resolve metadata schema");
            let metadata_pg_pool =
                build_ducklake_metadata_pg_pool(&catalog).expect("failed to create metadata pool");
            let metrics = query_catalog_maintenance_metrics(&metadata_pg_pool, &metadata_schema)
                .await
                .expect("failed to query catalog maintenance metrics");

            assert!(metrics.active_data_files_total >= 0);
            assert!(metrics.snapshots_total >= 1);
            assert!(metrics.oldest_snapshot_age_seconds >= 0);
            assert!(metrics.files_scheduled_for_deletion_total >= 0);
            assert!(metrics.files_scheduled_for_deletion_bytes >= 0);
            assert!(metrics.oldest_scheduled_deletion_age_seconds >= 0);
        }
    }
}
