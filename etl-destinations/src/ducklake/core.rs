use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::time::Duration;

use etl::destination::Destination;
use etl::destination::async_result::{
    TruncateTableResult, WriteEventsResult, WriteTableRowsResult,
};
use etl::destination::task_set::DestinationTaskSet;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
#[cfg(test)]
use etl::types::Cell;
use etl::types::{Event, EventSequenceKey, SizeHint, TableId, TableName, TableRow, TableSchema};
use metrics::{gauge, histogram};
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
#[cfg(feature = "test-utils")]
use tokio::sync::oneshot;
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;

use tracing::{debug, info, warn};
use url::Url;

use crate::ducklake::batches::{
    DuckLakeTableBatchKind, TableMutation, TrackedTableMutation, TrackedTruncateEvent,
    apply_table_batch_with_retry, apply_table_batches_with_retry,
    clear_applied_batch_markers_for_kind, clear_table_streaming_progress,
    ensure_applied_batches_table_exists, ensure_streaming_progress_table_exists,
    prepare_copy_table_batch, prepare_mutation_table_batches, prepare_truncate_table_batch,
    read_table_streaming_progress_sequence_key, retain_mutations_after_sequence_key,
    retain_truncates_after_sequence_key,
};
use crate::ducklake::client::{
    DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
    format_query_error_detail, run_duckdb_blocking,
};
use crate::ducklake::config::{
    build_maintenance_setup_plan, build_setup_plan, current_duckdb_extension_strategy,
};
use crate::ducklake::inline_size::DuckLakePendingInlineSizeSampler;
use crate::ducklake::maintenance::{
    DuckLakeMaintenanceWorker, ENABLE_CHECKPOINT_MAINTENANCE, TableMaintenanceNotification,
    TableWriteActivity, maybe_run_requested_checkpoint, maybe_run_requested_merge_adjacent_files,
    send_maintenance_notification, spawn_ducklake_maintenance_worker, table_write_slot,
};
use crate::ducklake::metrics::{
    DuckLakeMetricsSampler, ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
    ETL_DUCKLAKE_INLINE_FLUSH_ROWS, ETL_DUCKLAKE_POOL_SIZE, RESULT_LABEL, register_metrics,
    resolve_ducklake_metadata_schema_blocking, spawn_ducklake_metrics_sampler,
};
use crate::ducklake::schema::build_create_table_sql_ducklake;
use crate::ducklake::{DuckLakeTableName, LAKE_CATALOG, S3Config};
use crate::table_name::try_stringify_table_name;

/// Returns whether a DuckLake DDL error indicates another transaction already
/// created the requested table.
pub(super) fn is_create_table_conflict(error: &duckdb::Error, table_name: &str) -> bool {
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
    #[cfg(feature = "test-utils")]
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    /// Shared gate that keeps the global checkpoint from overlapping active
    /// foreground or table-scoped maintenance mutations.
    checkpoint_gate: Arc<RwLock<()>>,
    streaming_tasks: DestinationTaskSet,
    maintenance_worker: Arc<Option<DuckLakeMaintenanceWorker>>,
    metrics_sampler: Arc<Option<DuckLakeMetricsSampler>>,
    table_creation_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    store: S,
    /// Cache of table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
    /// Cache tracking whether the ETL batch marker table already exists. If it's set then the table has already been created
    applied_batches_table_created: Arc<AtomicBool>,
    /// Cache tracking whether the ETL streaming progress table already exists.
    streaming_progress_table_created: Arc<AtomicBool>,
    /// Signals that tier-0 merge_adjacent_files should run before the next streaming batch when safe.
    merge_adjacent_files_requested: Arc<AtomicBool>,
    /// Tracks whether recent writes may have produced files worth revisiting with tier-0 compaction.
    merge_adjacent_files_dirty: Arc<AtomicBool>,
    /// Signals that a checkpoint should run before the next streaming batch when safe.
    checkpoint_requested: Arc<AtomicBool>,
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
        table_id: TableId,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result = self.truncate_table(table_id).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_table_rows(table_id, table_rows).await;
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

impl<S> DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    /// Deletes all rows from the destination table.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the truncate work inline.
    pub async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.truncate_table_inner(table_id).await
    }

    /// Writes an initial-copy batch directly to the destination table.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the write inline.
    pub async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_inner(table_id, table_rows).await
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
    /// - On Linux and macOS, DuckDB extensions are loaded from vendored local
    ///   files when a vendored directory is available. The root directory can
    ///   be forced with `ETL_DUCKDB_EXTENSION_ROOT`. Otherwise, DuckDB uses
    ///   the legacy online `INSTALL` flow. On Windows, DuckDB always uses the
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
        )?);
        let maintenance_setup_plan = Arc::new(build_maintenance_setup_plan(setup_plan.as_ref()));

        let manager = Arc::new(DuckLakeConnectionManager {
            setup_plan: Arc::clone(&setup_plan),
            disable_extension_autoload,
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        });

        let pool =
            Arc::new(build_warm_ducklake_pool(manager.as_ref().clone(), pool_size, "write").await?);
        let blocking_slots = Arc::new(Semaphore::new(pool_size as usize));
        let pending_inline_size_sampler =
            if matches!(catalog_url.scheme(), "postgres" | "postgresql") {
                match run_duckdb_blocking(
                    Arc::clone(&pool),
                    Arc::clone(&blocking_slots),
                    DuckDbBlockingOperationKind::Foreground,
                    resolve_ducklake_metadata_schema_blocking,
                )
                .await
                {
                    Ok(metadata_schema) => {
                        DuckLakePendingInlineSizeSampler::new(&catalog_url, metadata_schema)?
                    }
                    Err(error) => {
                        warn!(
                            error = ?error,
                            "ducklake inline-size sampler initialization skipped"
                        );
                        None
                    }
                }
            } else {
                None
            };
        let created_tables = Arc::default();
        let checkpoint_gate = Arc::new(RwLock::new(()));
        let merge_adjacent_files_requested = Arc::new(AtomicBool::new(false));
        let merge_adjacent_files_dirty = Arc::new(AtomicBool::new(false));
        let checkpoint_requested = Arc::new(AtomicBool::new(false));
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
            merge_adjacent_files_requested: Arc::clone(&merge_adjacent_files_requested),
            merge_adjacent_files_dirty: Arc::clone(&merge_adjacent_files_dirty),
            checkpoint_requested: Arc::clone(&checkpoint_requested),
        };
        gauge!(ETL_DUCKLAKE_POOL_SIZE).set(pool_size as f64);
        destination.ensure_applied_batches_table_exists().await?;
        destination.ensure_streaming_progress_table_exists().await?;
        destination.maintenance_worker = Arc::new(
            spawn_ducklake_maintenance_worker(
                DuckLakeConnectionManager {
                    setup_plan: Arc::clone(&maintenance_setup_plan),
                    disable_extension_autoload,
                    #[cfg(feature = "test-utils")]
                    open_count: Arc::new(AtomicUsize::new(0)),
                },
                Arc::clone(&checkpoint_gate),
                Arc::clone(&destination.table_write_slots),
                Arc::clone(&checkpoint_requested),
                Arc::clone(&merge_adjacent_files_requested),
                Arc::clone(&merge_adjacent_files_dirty),
                pending_inline_size_sampler,
            )?
            .into(),
        );
        destination.metrics_sampler = Arc::new(
            spawn_ducklake_metrics_sampler(
                DuckLakeConnectionManager {
                    setup_plan: Arc::clone(&setup_plan),
                    disable_extension_autoload,
                    #[cfg(feature = "test-utils")]
                    open_count: Arc::new(AtomicUsize::new(0)),
                },
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
    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;
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
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        let prepared_batch = prepare_copy_table_batch(&table_schema, table_name, table_rows)?;
        let table_name = prepared_batch.table_name().to_owned();
        apply_table_batch_with_retry(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            prepared_batch,
        )
        .await?;
        self.merge_adjacent_files_dirty
            .store(true, Ordering::Release);

        self.notify_background_maintenance(TableMaintenanceNotification::WriteActivity(
            TableWriteActivity {
                table_name,
                approx_bytes,
                inserted_rows,
            },
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
        let merge_adjacent_files_ran = self.maybe_run_requested_merge_adjacent_files().await?;
        if !merge_adjacent_files_ran {
            self.maybe_run_requested_checkpoint().await?;
        }
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_mutations: HashMap<TableId, Vec<TrackedTableMutation>> =
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
                        table_id_to_mutations
                            .entry(insert.table_id)
                            .or_default()
                            .push(TrackedTableMutation::new(
                                insert.start_lsn,
                                insert.commit_lsn,
                                insert.tx_ordinal,
                                TableMutation::Insert(insert.table_row),
                            ));
                    }
                    Event::Update(update) => {
                        let table_id = update.table_id;
                        let table_row = update.table_row;
                        let old_table_row = update.old_table_row;
                        let mutations = table_id_to_mutations.entry(table_id).or_default();
                        if let Some((_, old_row)) = old_table_row {
                            mutations.push(TrackedTableMutation::new(
                                update.start_lsn,
                                update.commit_lsn,
                                update.tx_ordinal,
                                TableMutation::Update {
                                    delete_row: old_row,
                                    upsert_row: table_row,
                                },
                            ));
                        } else {
                            debug!(
                                "update event has no old row, deleting by primary key from new row"
                            );
                            mutations.push(TrackedTableMutation::new(
                                update.start_lsn,
                                update.commit_lsn,
                                update.tx_ordinal,
                                TableMutation::Replace(table_row),
                            ));
                        }
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            debug!("delete event has no old row, skipping");
                            continue;
                        };
                        table_id_to_mutations
                            .entry(delete.table_id)
                            .or_default()
                            .push(TrackedTableMutation::new(
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

                for (table_id, mutations) in table_id_to_mutations {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let table_schema = self.get_table_schema(table_id).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let checkpoint_gate = Arc::clone(&self.checkpoint_gate);
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let destination_table_name = table_name.clone();
                    let maintenance_worker = Arc::clone(&self.maintenance_worker);
                    let merge_adjacent_files_dirty = Arc::clone(&self.merge_adjacent_files_dirty);

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
                            &table_schema,
                            destination_table_name.clone(),
                            pending_mutations,
                        )?;
                        apply_table_batches_with_retry(pool, blocking_slots, prepared_batches)
                            .await?;
                        merge_adjacent_files_dirty.store(true, Ordering::Release);
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
            let mut truncate_table_ids: HashMap<TableId, Vec<TrackedTruncateEvent>> =
                HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for rel_id in truncate.rel_ids {
                        truncate_table_ids
                            .entry(TableId::new(rel_id))
                            .or_default()
                            .push(TrackedTruncateEvent::new(
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

                for (table_id, truncates) in truncate_table_ids {
                    let table_name = self.ensure_table_exists(table_id).await?;
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

    /// Acquires shared mutation access so the global checkpoint cannot start in
    /// the middle of a foreground write sequence.
    async fn acquire_mutation_guard(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.checkpoint_gate).read_owned().await
    }

    /// Runs one requested tier-0 merge before ingesting the next CDC batch when safe.
    ///
    /// Returns whether the merge maintenance actually ran.
    async fn maybe_run_requested_merge_adjacent_files(&self) -> EtlResult<bool> {
        if !self.merge_adjacent_files_requested.load(Ordering::Acquire) {
            return Ok(false);
        }

        maybe_run_requested_merge_adjacent_files(
            Arc::clone(&self.pool),
            Arc::clone(&self.checkpoint_gate),
            Arc::clone(&self.blocking_slots),
            self.merge_adjacent_files_requested.as_ref(),
            self.merge_adjacent_files_dirty.as_ref(),
        )
        .await
    }

    /// Runs one requested checkpoint before ingesting the next CDC batch when safe.
    async fn maybe_run_requested_checkpoint(&self) -> EtlResult<()> {
        if !ENABLE_CHECKPOINT_MAINTENANCE {
            self.checkpoint_requested.store(false, Ordering::Release);
            return Ok(());
        }

        if !self.checkpoint_requested.load(Ordering::Acquire) {
            return Ok(());
        }

        maybe_run_requested_checkpoint(
            Arc::clone(&self.pool),
            Arc::clone(&self.checkpoint_gate),
            Arc::clone(&self.blocking_slots),
            self.checkpoint_requested.as_ref(),
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

/// Reads the persisted streaming replay watermark for one table on DuckDB's blocking pool.
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

/// Recomputes maintenance stats from the suffix of mutations that still needs applying.
fn table_write_activity_for_mutations(
    table_name: DuckLakeTableName,
    tracked_mutations: &[TrackedTableMutation],
) -> TableWriteActivity {
    let mut write_activity = TableWriteActivity {
        table_name,
        approx_bytes: 0,
        inserted_rows: 0,
    };

    for tracked_mutation in tracked_mutations {
        if let Some(row) = tracked_mutation.upsert_row() {
            write_activity.approx_bytes = write_activity
                .approx_bytes
                .saturating_add(row.size_hint() as u64);
            write_activity.inserted_rows = write_activity.inserted_rows.saturating_add(1);
        }
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

/// Arms a one-shot hook that pauses the next streaming write before DuckLake starts applying it.
#[cfg(feature = "test-utils")]
pub fn arm_pause_next_streaming_write_for_tests() -> oneshot::Receiver<()> {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    *PAUSED_STREAMING_WRITE_HOOK.lock() = Some(PausedStreamingWriteHook {
        reached_tx,
        resume_rx,
    });
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

/// Clears the paused streaming-write hook without waiting for it to be released.
#[cfg(feature = "test-utils")]
pub fn reset_paused_streaming_write_for_tests() {
    PAUSED_STREAMING_WRITE_HOOK.lock().take();
    PAUSED_STREAMING_WRITE_RESUME_TX.lock().take();
}

#[cfg(feature = "test-utils")]
async fn wait_if_streaming_write_paused_for_tests() {
    let Some(PausedStreamingWriteHook {
        reached_tx,
        resume_rx,
    }) = PAUSED_STREAMING_WRITE_HOOK.lock().take()
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

/// Flushes inlined user data for one table after the write transaction commits.
pub(super) fn flush_table_inlined_data(
    conn: &duckdb::Connection,
    table_name: &str,
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
        RESULT_LABEL => flush_result,
    )
    .record(rows_flushed as f64);
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
        RESULT_LABEL => flush_result,
    )
    .record(flush_started.elapsed().as_secs_f64());

    if rows_flushed > 0 {
        debug!(
            table = %table_name,
            rows_flushed,
            "ducklake inlined data flushed"
        );
    } else {
        debug!(
            table = %table_name,
            "ducklake inlined data already flushed"
        );
    }
    Ok(rows_flushed)
}

#[cfg(test)]
mod tests {
    use super::*;

    use duckdb::{Config, Connection};
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

            if ducklake_extension.is_file() {
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

            return format!(
                "LOAD {};",
                quote_literal(&ducklake_extension.display().to_string()),
            );
        }

        "INSTALL ducklake; LOAD ducklake;".to_string()
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

    fn lake_table_exists(conn: &Connection, table_name: &str) -> bool {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_catalog = {} AND table_schema = {} AND table_name = {}",
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
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store)
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

        let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
        let _rows_flushed = flush_table_inlined_data(&conn, &table_name)
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
    async fn test_query_catalog_maintenance_metrics_reports_active_data_files_total() {
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
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store)
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

        let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
        let _rows_flushed = flush_table_inlined_data(&conn, &table_name)
            .expect("failed to materialize inlined rows for catalog metrics test");
        let deadline = Instant::now() + Duration::from_secs(10);
        let metrics = loop {
            let metrics = query_catalog_maintenance_metrics_blocking(&conn)
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

    #[tokio::test]
    async fn test_query_catalog_maintenance_metrics_reads_ducklake_metadata() {
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
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store)
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

        destination
            .shutdown()
            .await
            .expect("failed to shutdown destination");
        drop(destination);

        let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
        let metrics = query_catalog_maintenance_metrics_blocking(&conn)
            .expect("failed to query catalog maintenance metrics");

        assert!(metrics.active_data_files_total >= 0);
        assert!(metrics.snapshots_total >= 1);
        assert!(metrics.oldest_snapshot_age_seconds >= 0);
        assert!(metrics.files_scheduled_for_deletion_total >= 0);
        assert!(metrics.files_scheduled_for_deletion_bytes >= 0);
        assert!(metrics.oldest_scheduled_deletion_age_seconds >= 0);
    }
}
