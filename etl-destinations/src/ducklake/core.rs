#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicBool},
};

use etl::{
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    state::destination_metadata::DestinationTableMetadata,
    store::{schema::SchemaStore, state::StateStore},
    types::{Event, ReplicatedTableSchema, SizeHint, TableId, TableName, TableRow},
};
use metrics::{gauge, histogram};
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    ducklake::{
        DuckLakeTableName, LAKE_CATALOG, S3Config,
        batches::{
            DuckLakeTableBatchKind, TableMutation, TrackedTableMutation, TrackedTruncateEvent,
            apply_table_batch_with_retry, apply_table_batches_with_retry,
            clear_applied_batch_markers_for_kind, ensure_applied_batches_table_exists,
            prepare_copy_table_batch, prepare_mutation_table_batches, prepare_truncate_table_batch,
        },
        client::{
            DuckDbBlockingOperationKind, DuckLakeConnectionManager, build_warm_ducklake_pool,
            format_query_error_detail, run_duckdb_blocking,
        },
        config::{build_setup_sql, current_duckdb_extension_strategy},
        maintenance::{
            DuckLakeMaintenanceWorker, TableMaintenanceNotification, TableWriteActivity,
            send_maintenance_notification, spawn_ducklake_maintenance_worker, table_write_slot,
        },
        metrics::{
            BATCH_KIND_LABEL, DuckLakeMetricsSampler, ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
            ETL_DUCKLAKE_INLINE_FLUSH_ROWS, ETL_DUCKLAKE_POOL_SIZE, RESULT_LABEL, register_metrics,
            spawn_ducklake_metrics_sampler,
        },
        schema::build_create_table_sql_ducklake,
    },
    table_name::try_stringify_table_name,
};

/// Label values used only for inline-flush metrics and logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckLakeInlineFlushKind {
    Mutation,
    Shutdown,
}

impl DuckLakeInlineFlushKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mutation => "mutation",
            Self::Shutdown => "shutdown",
        }
    }
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
/// deferred to background maintenance.
#[derive(Clone)]
pub struct DuckLakeDestination<S> {
    #[cfg(feature = "test-utils")]
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
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
        self.flush_known_tables_on_shutdown().await;

        Ok(())
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result = self.truncate_table_inner(replicated_table_schema).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_table_rows_inner(replicated_table_schema, table_rows).await;
        async_result.send(result);

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_events(events).await;
        async_result.send(result);

        Ok(())
    }
}

impl<S> DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
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
        self.write_events_inner(events).await
    }

    /// Creates a new DuckLake destination.
    ///
    /// - `catalog_url`: DuckLake catalog location. Use a PostgreSQL URL (`postgres://user:pass@localhost:5432/mydb`)
    ///   or a local file URL (`file:///tmp/catalog.ducklake`).
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
        let setup_sql = Arc::new(build_setup_sql(
            &catalog_url,
            &data_path,
            s3.as_ref(),
            metadata_schema.as_deref(),
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
            #[cfg(feature = "test-utils")]
            manager,
            pool: Arc::new(pool),
            blocking_slots: Arc::new(Semaphore::new(pool_size as usize)),
            maintenance_worker: Arc::new(None),
            metrics_sampler: Arc::new(None),
            table_creation_slots: Arc::new(Semaphore::new(1)),
            table_write_slots: Arc::default(),
            store,
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
            )
            .await?
            .into(),
        );

        Ok(destination)
    }

    /// Deletes all rows from the destination table without dropping it.
    async fn truncate_table_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(replicated_table_schema).await?;
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

    /// Bulk-inserts rows into the destination table inside a single
    /// transaction.
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
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(replicated_table_schema).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        let approx_bytes = table_rows.iter().map(|row| row.size_hint() as u64).sum::<u64>();
        let inserted_rows = table_rows.len() as u64;

        // Here we can have concurrent table writers because it's INSERTs only and CDC
        // (write_events) won't start before the copy phase is complete
        self.ensure_applied_batches_table_exists().await?;
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
    /// connection per retry attempt, and recorded in an ETL marker table so
    /// retries can safely detect already committed work.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_schemas: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();
            let mut table_id_to_mutations: HashMap<TableId, Vec<TrackedTableMutation>> =
                HashMap::new();
            let mut table_id_to_stats: HashMap<TableId, TableWriteActivity> = HashMap::new();

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
                        let table_id = insert.replicated_table_schema.id();
                        table_schemas.entry(table_id).or_insert(insert.replicated_table_schema);
                        table_id_to_mutations.entry(table_id).or_default().push(
                            TrackedTableMutation::new(
                                insert.start_lsn,
                                insert.commit_lsn,
                                TableMutation::Insert(insert.table_row),
                            ),
                        );
                        let stats = table_id_to_stats.entry(table_id).or_default();
                        stats.approx_bytes = stats.approx_bytes.saturating_add(approx_bytes);
                        stats.inserted_rows = stats.inserted_rows.saturating_add(1);
                    }
                    Event::Update(update) => {
                        let table_id = update.replicated_table_schema.id();
                        let table_row = update.table_row;
                        let old_table_row = update.old_table_row;
                        let upsert_bytes = table_row.size_hint() as u64;
                        table_schemas.entry(table_id).or_insert(update.replicated_table_schema);
                        let mutations = table_id_to_mutations.entry(table_id).or_default();
                        if let Some((_, old_row)) = old_table_row {
                            mutations.push(TrackedTableMutation::new(
                                update.start_lsn,
                                update.commit_lsn,
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
                                TableMutation::Replace(table_row),
                            ));
                        }
                        let stats = table_id_to_stats.entry(table_id).or_default();
                        stats.approx_bytes = stats.approx_bytes.saturating_add(upsert_bytes);
                        stats.inserted_rows = stats.inserted_rows.saturating_add(1);
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            debug!("delete event has no old row, skipping");
                            continue;
                        };
                        let table_id = delete.replicated_table_schema.id();
                        table_schemas.entry(table_id).or_insert(delete.replicated_table_schema);
                        table_id_to_mutations.entry(table_id).or_default().push(
                            TrackedTableMutation::new(
                                delete.start_lsn,
                                delete.commit_lsn,
                                TableMutation::Delete(old_row),
                            ),
                        );
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
                    let replicated_schema = table_schemas
                        .remove(&table_id)
                        .expect("schema must be present for every table_id collected from events");
                    let table_name = self.ensure_table_exists(&replicated_schema).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let prepared_batches =
                        prepare_mutation_table_batches(&replicated_schema, table_name, mutations)?;
                    let maintenance_notification =
                        table_id_to_stats.remove(&table_id).map(|mut stats| {
                            stats.table_name = prepared_batches[0].table_name().to_owned();
                            TableMaintenanceNotification::WriteActivity(stats)
                        });
                    let maintenance_worker = Arc::clone(&self.maintenance_worker);

                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
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
            let mut truncate_tables: HashMap<
                TableId,
                (ReplicatedTableSchema, Vec<TrackedTruncateEvent>),
            > = HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for replicated_table_schema in truncate.truncated_tables {
                        let entry = truncate_tables
                            .entry(replicated_table_schema.id())
                            .or_insert_with(|| (replicated_table_schema, Vec::new()));
                        entry.1.push(TrackedTruncateEvent::new(
                            truncate.start_lsn,
                            truncate.commit_lsn,
                            truncate.options,
                        ));
                    }
                }
            }

            if !truncate_tables.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (_table_id, (replicated_schema, truncates)) in truncate_tables {
                    let table_name = self.ensure_table_exists(&replicated_schema).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let prepared_batch = prepare_truncate_table_batch(table_name, truncates);
                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
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

        let table_name = self.get_or_create_applied_table_mapping(replicated_table_schema).await?;

        // Fast path: already created.
        {
            let cache = self.created_tables.lock();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        let _table_creation_permit =
            self.table_creation_slots.clone().acquire_owned().await.map_err(|_| {
                etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
            })?;

        {
            let cache = self.created_tables.lock();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        // `build_create_table_sql_ducklake` generates `CREATE TABLE IF NOT EXISTS
        // "name" (...)`. Prefix the table name with the catalog alias so
        // DuckLake knows which catalog to create the table in.
        let replicated_column_schemas =
            replicated_table_schema.column_schemas().cloned().collect::<Vec<_>>();
        let ddl = build_create_table_sql_ducklake(&table_name, &replicated_column_schemas);
        let quoted_table_name = quote_identifier(&table_name).into_owned();
        let qualified_ddl =
            ddl.replace(&quoted_table_name, &format!("{LAKE_CATALOG}.{quoted_table_name}"));

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

        // Transition from applying to applied now that DDL has succeeded.
        if let Some(metadata) = self.store.get_destination_table_metadata(table_id).await?
            && metadata.is_applying()
        {
            self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        }

        Ok(table_name)
    }

    /// Ensures the ETL-managed replay marker table exists.
    async fn ensure_applied_batches_table_exists(&self) -> EtlResult<()> {
        ensure_applied_batches_table_exists(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            Arc::clone(&self.table_creation_slots),
            Arc::clone(&self.applied_batches_table_created),
        )
        .await
    }

    /// Returns the destination table name for `table_id`, creating and
    /// persisting a new mapping via [`DestinationTableMetadata`] if none
    /// exists yet.
    ///
    /// Existing metadata must already be in the applied state; otherwise the
    /// state store returns an error and the caller must stop. When a
    /// mapping is created for the first time, it is stored with
    /// [`DestinationTableSchemaStatus::Applying`] status. The caller is
    /// responsible for transitioning it to
    /// [`DestinationTableSchemaStatus::Applied`] once the DDL has been
    /// executed successfully.
    async fn get_or_create_applied_table_mapping(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<DuckLakeTableName> {
        let table_id = replicated_table_schema.id();
        if let Some(existing) = self.store.get_applied_destination_table_metadata(table_id).await? {
            return existing.destination_table_id.parse::<DuckLakeTableName>().map_err(|e| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Invalid DuckLake table name in metadata",
                    e.to_string()
                )
            });
        }

        let ducklake_table_name =
            table_name_to_ducklake_table_name(replicated_table_schema.name())?;
        let metadata = DestinationTableMetadata::new_applying(
            ducklake_table_name.clone(),
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );
        self.store.store_destination_table_metadata(table_id, metadata).await?;

        Ok(ducklake_table_name)
    }

    /// Serializes table-local truncate and CDC mutation writes.
    async fn acquire_table_write_slot(&self, table_name: &str) -> EtlResult<OwnedSemaphorePermit> {
        let table_slot = table_write_slot(&self.table_write_slots, table_name);

        table_slot.acquire_owned().await.map_err(|_| {
            etl_error!(ErrorKind::InvalidState, "DuckLake table write semaphore closed")
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

    /// Flushes any remaining inlined rows for known tables before shutdown
    /// completes.
    async fn flush_known_tables_on_shutdown(&self) {
        let table_names = {
            let cache = self.created_tables.lock();
            cache.iter().cloned().collect::<Vec<_>>()
        };

        if table_names.is_empty() {
            debug!("ducklake shutdown inline flush skipped, no known tables");
            return;
        }

        let known_table_count = table_names.len();
        let mut join_set = JoinSet::new();

        for table_name in table_names {
            let pool = Arc::clone(&self.pool);
            let blocking_slots = Arc::clone(&self.blocking_slots);
            let table_write_slots = Arc::clone(&self.table_write_slots);
            let result_table_name = table_name.clone();

            join_set.spawn(async move {
                let result = async move {
                    let table_write_permit = table_write_slot(&table_write_slots, &table_name)
                        .acquire_owned()
                        .await
                        .map_err(|_| {
                            etl_error!(
                                ErrorKind::InvalidState,
                                "DuckLake table write semaphore closed"
                            )
                        })?;

                    run_duckdb_blocking(
                        pool,
                        blocking_slots,
                        DuckDbBlockingOperationKind::Maintenance,
                        move |conn| {
                            let _table_write_permit = table_write_permit;
                            flush_table_inlined_data(
                                conn,
                                &table_name,
                                DuckLakeInlineFlushKind::Shutdown,
                            )
                        },
                    )
                    .await
                }
                .await;

                (result_table_name, result)
            });
        }

        let mut successful_tables = 0usize;
        let mut failed_tables = 0usize;
        let mut total_rows_flushed = 0u64;

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((table_name, Ok(rows_flushed))) => {
                    successful_tables += 1;
                    total_rows_flushed = total_rows_flushed.saturating_add(rows_flushed);
                    debug!(
                        table = %table_name,
                        rows_flushed,
                        "ducklake shutdown inline flush completed"
                    );
                }
                Ok((table_name, Err(error))) => {
                    failed_tables += 1;
                    warn!(
                        table = %table_name,
                        error = ?error,
                        "ducklake shutdown inline flush failed"
                    );
                }
                Err(error) => {
                    failed_tables += 1;
                    warn!(error = ?error, "ducklake shutdown inline flush task panicked");
                }
            }
        }

        info!(
            known_table_count,
            successful_tables,
            failed_tables,
            total_rows_flushed,
            "ducklake shutdown inline flush sweep completed"
        );
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
    inline_flush_kind: DuckLakeInlineFlushKind,
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
        BATCH_KIND_LABEL => inline_flush_kind.as_str(),
        RESULT_LABEL => flush_result,
    )
    .record(rows_flushed as f64);
    histogram!(
        ETL_DUCKLAKE_INLINE_FLUSH_DURATION_SECONDS,
        BATCH_KIND_LABEL => inline_flush_kind.as_str(),
        RESULT_LABEL => flush_result,
    )
    .record(flush_started.elapsed().as_secs_f64());

    if rows_flushed > 0 {
        debug!(
            table = %table_name,
            batch_kind = inline_flush_kind.as_str(),
            rows_flushed,
            "ducklake inlined data flushed"
        );
    } else {
        debug!(
            table = %table_name,
            batch_kind = inline_flush_kind.as_str(),
            "ducklake inlined data already flushed"
        );
    }
    Ok(rows_flushed)
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        time::Duration,
    };

    use duckdb::{Config, Connection};
    use etl::{
        store::{both::memory::MemoryStore, schema::SchemaStore},
        types::{
            Cell, ColumnSchema, ReplicatedTableSchema, ReplicationMask, TableId, TableName,
            TableRow, TableSchema, Type as PgType,
        },
    };
    use pg_escape::{quote_identifier, quote_literal};
    use tempfile::TempDir;
    use url::Url;

    use super::*;
    use crate::ducklake::metrics::{
        query_catalog_maintenance_metrics_blocking, query_table_storage_metrics_blocking,
    };

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
            let extension_dir = root.join("1.5.1").join(platform_dir);
            let ducklake_extension = extension_dir.join("ducklake.duckdb_extension");
            let json_extension = extension_dir.join("json.duckdb_extension");
            let parquet_extension = extension_dir.join("parquet.duckdb_extension");

            if ducklake_extension.is_file()
                && json_extension.is_file()
                && parquet_extension.is_file()
            {
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
            let json_extension = extension_dir.join("json.duckdb_extension");
            let parquet_extension = extension_dir.join("parquet.duckdb_extension");

            return format!(
                "LOAD {}; LOAD {}; LOAD {};",
                quote_literal(&ducklake_extension.display().to_string()),
                quote_literal(&json_extension.display().to_string()),
                quote_literal(&parquet_extension.display().to_string()),
            );
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

    fn lake_table_column_names(conn: &Connection, table_name: &str) -> Vec<String> {
        let mut statement = conn
            .prepare(&format!(
                "SELECT column_name FROM information_schema.columns WHERE table_catalog = {} AND \
                 table_schema = {} AND table_name = {} ORDER BY ordinal_position",
                quote_literal(LAKE_CATALOG),
                quote_literal("main"),
                quote_literal(table_name),
            ))
            .expect("failed to prepare DuckLake column query");
        let mut rows = statement.query([]).expect("failed to query DuckLake table columns");
        let mut column_names = Vec::new();

        while let Some(row) = rows.next().expect("failed to read DuckLake column row") {
            column_names
                .push(row.get::<_, String>(0).expect("failed to read DuckLake column name"));
        }

        column_names
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

    #[tokio::test]
    async fn test_query_table_storage_metrics_reads_ducklake_metadata() {
        let dir = TempDir::new().expect("failed to create temp dir");
        let catalog = path_to_file_url(&dir.path().join("catalog.ducklake"));
        let data = path_to_file_url(&dir.path().join("data"));
        let store = MemoryStore::new();
        let schema = make_schema(1, "public", "users");
        let replicated_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
        let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

        store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

        let destination =
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store)
                .await
                .expect("failed to create destination");

        destination
            .write_table_rows(
                &replicated_schema,
                vec![
                    TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]),
                    TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_string())]),
                ],
            )
            .await
            .expect("failed to write rows");

        let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
        let _rows_flushed =
            flush_table_inlined_data(&conn, &table_name, DuckLakeInlineFlushKind::Mutation)
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
    async fn test_write_table_rows_creates_only_replicated_columns() {
        let dir = TempDir::new().expect("failed to create temp dir");
        let catalog = path_to_file_url(&dir.path().join("catalog.ducklake"));
        let data = path_to_file_url(&dir.path().join("data"));
        let store = MemoryStore::new();
        let schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, 1, Some(1), false),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, 2, None, true),
                ColumnSchema::new("email".to_string(), PgType::TEXT, -1, 3, None, true),
            ],
        );
        let replicated_schema = ReplicatedTableSchema::from_mask(
            Arc::new(schema.clone()),
            ReplicationMask::from_bytes(vec![1, 1, 0]),
        );
        let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

        store.store_table_schema(schema).await.expect("failed to seed schema");

        let destination =
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store.clone())
                .await
                .expect("failed to create destination");

        destination
            .write_table_rows(
                &replicated_schema,
                vec![TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())])],
            )
            .await
            .expect("failed to write filtered rows");

        let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
        assert_eq!(
            lake_table_column_names(&conn, &table_name),
            vec!["id".to_string(), "name".to_string()]
        );

        let metadata = store
            .get_destination_table_metadata(TableId::new(1))
            .await
            .expect("failed to load destination metadata")
            .expect("expected destination metadata to exist");
        assert_eq!(metadata.replication_mask.as_slice(), &[1, 1, 0]);
    }

    #[tokio::test]
    async fn test_query_catalog_maintenance_metrics_reports_active_data_files_total() {
        let dir = TempDir::new().expect("failed to create temp dir");
        let catalog = path_to_file_url(&dir.path().join("catalog.ducklake"));
        let data = path_to_file_url(&dir.path().join("data"));
        let store = MemoryStore::new();
        let schema = make_schema(1, "public", "users");
        let replicated_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
        let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

        store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

        let destination =
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store)
                .await
                .expect("failed to create destination");

        destination
            .write_table_rows(
                &replicated_schema,
                vec![
                    TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]),
                    TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_string())]),
                ],
            )
            .await
            .expect("failed to write rows");

        let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
        let _rows_flushed =
            flush_table_inlined_data(&conn, &table_name, DuckLakeInlineFlushKind::Mutation)
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
        let replicated_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
        let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

        store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

        let destination =
            DuckLakeDestination::new(catalog.clone(), data.clone(), 1, None, None, store)
                .await
                .expect("failed to create destination");

        destination
            .write_table_rows(
                &replicated_schema,
                vec![TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())])],
            )
            .await
            .expect("failed to write rows");
        destination.truncate_table(&replicated_schema).await.expect("failed to truncate table");

        destination.shutdown().await.expect("failed to shutdown destination");
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
