use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "test-utils")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{NaiveDate, NaiveTime};
use duckdb::Config;
use duckdb::types::{TimeUnit, Value};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{ArrayCell, Cell, Event, TableId, TableName, TableRow, TableSchema};
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
use rand::Rng;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_postgres::types::PgLsn;

use tracing::{Level, debug, info, warn};
use url::Url;

use crate::ducklake::S3Config;
use crate::ducklake::config::{
    DuckDbLogConfig, build_setup_sql, current_duckdb_extension_strategy,
};
use crate::ducklake::schema::build_create_table_sql_ducklake;
use crate::table_name::try_stringify_table_name;

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
pub(super) const LAKE_CATALOG: &str = "lake";
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

/// Alias for DuckLake table names.
type DuckLakeTableName = String;

/// Prepared row payload reused across retry attempts.
enum PreparedRows {
    Appender(Vec<Vec<Value>>),
    SqlLiterals(Vec<String>),
}

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
enum DuckLakeTableBatchKind {
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

// ── connection manager ────────────────────────────────────────────────────────

/// Custom r2d2 connection manager that opens an in-memory DuckDB connection and
/// attaches the DuckLake catalog on every `connect()` call.
///
/// Each opened connection is independent and attaches the same catalog, which
/// is safe: DuckLake (backed by a PostgreSQL catalog) supports concurrent writers.
#[derive(Clone)]
struct DuckLakeConnectionManager {
    /// SQL executed immediately after a new connection is opened.
    /// Loads required extensions and attaches the DuckLake catalog.
    setup_sql: Arc<String>,
    /// Disables DuckDB extension autoload/autoinstall when vendored Linux
    /// extensions are required.
    disable_extension_autoload: bool,
    /// Counts successfully initialized DuckDB connections for tests.
    #[cfg(feature = "test-utils")]
    open_count: Arc<AtomicUsize>,
}

/// DuckDB connection state tracked while a pooled connection is checked out.
struct ManagedDuckLakeConnection {
    conn: duckdb::Connection,
    broken: bool,
}

impl DuckLakeConnectionManager {
    /// Opens one fully initialized DuckDB connection and attaches the lake catalog.
    fn open_duckdb_connection(&self) -> Result<duckdb::Connection, duckdb::Error> {
        let conn = if self.disable_extension_autoload {
            duckdb::Connection::open_in_memory_with_flags(
                Config::default().enable_autoload_extension(false)?,
            )?
        } else {
            duckdb::Connection::open_in_memory()?
        };
        conn.execute_batch(&self.setup_sql)?;
        #[cfg(feature = "test-utils")]
        self.open_count.fetch_add(1, Ordering::Relaxed);
        Ok(conn)
    }

    /// Returns the number of successfully initialized DuckDB connections.
    #[cfg(feature = "test-utils")]
    fn open_count_for_tests(&self) -> usize {
        self.open_count.load(Ordering::Relaxed)
    }
}

impl r2d2::ManageConnection for DuckLakeConnectionManager {
    type Connection = ManagedDuckLakeConnection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<ManagedDuckLakeConnection, duckdb::Error> {
        Ok(ManagedDuckLakeConnection {
            conn: self.open_duckdb_connection()?,
            broken: false,
        })
    }

    fn is_valid(&self, conn: &mut ManagedDuckLakeConnection) -> Result<(), duckdb::Error> {
        conn.conn.execute_batch("SELECT 1")?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut ManagedDuckLakeConnection) -> bool {
        conn.broken
    }
}

/// Formats a DuckDB query failure so the displayed [`EtlError`] includes
/// both the SQL statement and the underlying DuckDB error message.
fn format_query_error_detail(sql: &str, error: &duckdb::Error) -> String {
    let compact_sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("sql: {compact_sql}; source: {error}")
}

/// Returns whether a DuckLake DDL error indicates another transaction already
/// created the requested table.
fn is_create_table_conflict(error: &duckdb::Error, table_name: &str) -> bool {
    let message = error.to_string();
    message.contains("has been created by another transaction already")
        && message.contains(&format!("attempting to create table \"{table_name}\""))
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
/// is committed as a single Parquet snapshot rather than one file per row.
#[derive(Clone)]
pub struct DuckLakeDestination<S> {
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
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
        if pool_size == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "DuckLake pool size must be greater than zero",
                "pool_size must be at least 1"
            ));
        }

        let extension_strategy = current_duckdb_extension_strategy()?;
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
            disable_extension_autoload: extension_strategy.disables_autoload(),
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        });

        let manager_for_pool = Arc::clone(&manager);
        let pool = tokio::task::spawn_blocking(move || -> EtlResult<_> {
            let started = Instant::now();
            let pool = r2d2::Pool::builder()
                .max_size(pool_size)
                .min_idle(Some(pool_size))
                .test_on_check_out(true)
                .build(manager_for_pool.as_ref().clone())
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationConnectionFailed,
                        "Failed to build DuckLake connection pool",
                        source: e
                    )
                })?;

            // Check out each warm connection once so pool startup pays the
            // attach/bootstrap cost before the destination begins serving work.
            let mut warmed_connections = Vec::with_capacity(pool_size as usize);
            for _ in 0..pool_size {
                let conn = pool.get().map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationConnectionFailed,
                        "Failed to warm DuckLake connection pool",
                        source: e
                    )
                })?;
                warmed_connections.push(conn);
            }
            drop(warmed_connections);

            info!(
                pool_size,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "ducklake connection pool warmed"
            );

            Ok(pool)
        })
        .await
        .map_err(|_| {
            etl_error!(
                ErrorKind::ApplyWorkerPanic,
                "DuckLake connection pool initialization task panicked"
            )
        })??;

        let destination = Self {
            manager,
            pool: Arc::new(pool),
            blocking_slots: Arc::new(Semaphore::new(pool_size as usize)),
            table_creation_slots: Arc::new(Semaphore::new(1)),
            table_write_slots: Arc::default(),
            store,
            duckdb_log: duckdb_log.map(Arc::new),
            created_tables: Arc::default(),
            applied_batches_table_created: Arc::default(),
        };
        destination.ensure_applied_batches_table_exists().await?;

        Ok(destination)
    }

    /// Deletes all rows from the destination table without dropping it.
    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        self.ensure_applied_batches_table_exists().await?;
        self.run_duckdb_blocking(move |conn| -> EtlResult<()> {
            conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake BEGIN TRANSACTION failed",
                    source: e
                )
            })?;

            let result = (|| -> EtlResult<()> {
                let delete_table_sql = format!("DELETE FROM {LAKE_CATALOG}.\"{table_name}\";");
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
    /// as a single Parquet snapshot rather than one file per row.
    ///
    /// Copy batches are recorded in the replay marker table so a retry after an
    /// ambiguous post-commit failure can detect already applied rows.
    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        self.ensure_applied_batches_table_exists().await?;
        let table_schema = self.get_table_schema(table_id).await?;
        let prepared_batch = prepare_copy_table_batch(&table_schema, table_name, table_rows)?;
        apply_table_batch_with_retry(
            Arc::clone(&self.manager),
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            prepared_batch,
        )
        .await
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
                            .push(TrackedTableMutation {
                                start_lsn: insert.start_lsn,
                                commit_lsn: insert.commit_lsn,
                                mutation: TableMutation::Insert(insert.table_row),
                            });
                    }
                    Event::Update(update) => {
                        let table_id = update.table_id;
                        let table_row = update.table_row;
                        let old_table_row = update.old_table_row;
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

                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
                        apply_table_batches_with_retry(
                            manager,
                            pool,
                            blocking_slots,
                            prepared_batches,
                        )
                        .await
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
            "CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}.\"{APPLIED_BATCHES_TABLE}\" (\
             table_name VARCHAR NOT NULL, \
             batch_id VARCHAR NOT NULL, \
             batch_kind VARCHAR NOT NULL, \
             first_start_lsn UBIGINT, \
             last_commit_lsn UBIGINT, \
             applied_at TIMESTAMPTZ NOT NULL\
             );"
        );
        let created = Arc::clone(&self.applied_batches_table_created);
        let table_name = APPLIED_BATCHES_TABLE.to_string();

        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
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

    /// Serializes writes that target the same DuckLake table.
    async fn acquire_table_write_slot(&self, table_name: &str) -> EtlResult<OwnedSemaphorePermit> {
        let table_slot = {
            let mut table_write_slots = self.table_write_slots.lock();
            Arc::clone(
                table_write_slots
                    .entry(table_name.to_string())
                    .or_insert_with(|| Arc::new(Semaphore::new(1))),
            )
        };

        table_slot.acquire_owned().await.map_err(|_| {
            etl_error!(
                ErrorKind::InvalidState,
                "DuckLake table write semaphore closed"
            )
        })
    }

    /// Runs one DuckDB operation on Tokio's blocking pool after acquiring a
    /// permit that matches the configured DuckDB concurrency limit.
    async fn run_duckdb_blocking<R, F>(&self, operation: F) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            operation,
        )
        .await
    }

    /// Dumps file-backed DuckDB logs on destination shutdown when configured.
    async fn dump_duckdb_logs_inner(&self) -> EtlResult<()> {
        let Some(duckdb_log) = self.duckdb_log.as_ref().map(Arc::clone) else {
            return Ok(());
        };

        self.run_duckdb_blocking(move |conn| dump_duckdb_logs(conn, duckdb_log.as_ref()))
            .await
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

/// Runs one DuckDB operation on Tokio's blocking pool after acquiring a permit
/// that matches the configured DuckDB concurrency limit and then checking out a
/// warm pooled DuckDB connection.
async fn run_duckdb_blocking<R, F>(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    operation: F,
) -> EtlResult<R>
where
    R: Send + 'static,
    F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
{
    let slot_wait_started = Instant::now();
    let permit = blocking_slots.acquire_owned().await.map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake blocking slot acquisition failed"
        )
    })?;
    info!(
        wait_ms = slot_wait_started.elapsed().as_millis() as u64,
        "wait for ducklake blocking slot"
    );

    tokio::task::spawn_blocking(move || -> EtlResult<R> {
        // Please if you modify the code inside this blocking task do not add any
        // blocking operations that could delay other tasks waiting on this slot.
        let _permit = permit;
        let checkout_started = Instant::now();
        let mut pooled_conn = pool.get().map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to check out DuckLake connection",
                source: e
            )
        })?;
        info!(
            wait_ms = checkout_started.elapsed().as_millis() as u64,
            "wait for ducklake pool checkout"
        );
        let operation_started = Instant::now();
        let res = operation(&pooled_conn.conn);
        info!(
            duration_ms = operation_started.elapsed().as_millis() as u64,
            "ducklake blocking operation finished"
        );
        if res.is_err() {
            pooled_conn.broken = true;
        }

        res
    })
    .await
    .map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake blocking task panicked"
        )
    })?
}

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

    let table_name = batches[0].table_name.clone();
    let batch_count = batches.len();
    let batches = Arc::new(batches);
    let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    // This retry mechanism is safe because
    for attempt in 0..=MAX_COMMIT_RETRIES {
        let attempt_batches = Arc::clone(&batches);
        tracing::info!(
            "attempt_batches size -----------__> {}",
            attempt_batches.len()
        );
        match run_duckdb_blocking(Arc::clone(&pool), Arc::clone(&blocking_slots), {
            let manager = Arc::clone(&manager);
            move |conn| {
                let operation_started = std::time::Instant::now();
                apply_table_batches(manager.as_ref(), conn, attempt_batches.as_ref())?;
                info!(
                    duration_ms = operation_started.elapsed().as_millis() as u64,
                    "ducklake batch ---- "
                );
                let operation_started = std::time::Instant::now();
                let res = flush_table_inlined_data(
                    conn,
                    &attempt_batches[0].table_name,
                    DuckLakeTableBatchKind::Mutation,
                );
                info!(
                    duration_ms = operation_started.elapsed().as_millis() as u64,
                    "ducklake inlining flush >>>>>"
                );
                res
            }
        })
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
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
        match run_duckdb_blocking(Arc::clone(&pool), Arc::clone(&blocking_slots), {
            let manager = Arc::clone(&manager);
            move |conn| {
                if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                    debug!(
                        table = %attempt_batch.table_name,
                        batch_id = %attempt_batch.batch_id,
                        batch_kind = batch_kind.as_str(),
                        "ducklake table batch already committed, skipping replay"
                    );

                    if batch_kind == DuckLakeTableBatchKind::Copy {
                        flush_table_inlined_data(conn, &attempt_batch.table_name, batch_kind)?;
                    }

                    return Ok(());
                }

                apply_table_batch(manager.as_ref(), conn, attempt_batch.as_ref())?;

                if batch_kind == DuckLakeTableBatchKind::Copy {
                    flush_table_inlined_data(conn, &attempt_batch.table_name, batch_kind)?;
                }

                Ok(())
            }
        })
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
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
            _ => format!(
                "{quoted_column} = {}",
                cell_to_sql_literal(cell_to_owned(value))
            ),
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
        "SELECT 1 FROM {LAKE_CATALOG}.\"{APPLIED_BATCHES_TABLE}\" \
         WHERE table_name = {} AND batch_id = {} LIMIT 1;",
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
        "INSERT INTO {LAKE_CATALOG}.\"{APPLIED_BATCHES_TABLE}\" \
         (table_name, batch_id, batch_kind, first_start_lsn, last_commit_lsn, applied_at) VALUES \
         ({}, {}, {}, {}, {}, current_timestamp);",
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
        "DELETE FROM {LAKE_CATALOG}.\"{APPLIED_BATCHES_TABLE}\" \
         WHERE table_name = {} AND batch_kind = {};",
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
                    apply_table_mutation(conn, &batch.table_name, prepared_mutation)?;
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
fn flush_table_inlined_data(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
) -> EtlResult<()> {
    let sql = format!(
        "SELECT COALESCE(SUM(rows_flushed), 0) \
         FROM ducklake_flush_inlined_data({}, table_name => {});",
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

    #[cfg(feature = "test-utils")]
    maybe_fail_after_flushed_batch_for_tests(batch_kind, table_name)?;

    Ok(())
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(conn: &duckdb::Connection, table_name: &str) -> EtlResult<()> {
    let sql = format!("DELETE FROM {LAKE_CATALOG}.\"{table_name}\";");
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
    prepared_mutation: &PreparedTableMutation,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Upsert(prepared_rows) => {
            apply_upsert_mutation(conn, table_name, prepared_rows)
        }
        PreparedTableMutation::Delete { predicates, .. } => {
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
        "CREATE OR REPLACE TEMP TABLE {staging:?} AS \
         SELECT * FROM {LAKE_CATALOG}.\"{table_name}\" LIMIT 0;"
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
            "INSERT INTO {LAKE_CATALOG}.\"{table_name}\" SELECT * FROM {staging:?};"
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
            format!("DELETE FROM {LAKE_CATALOG}.\"{table_name}\" WHERE {where_clause};",);
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
        &format!("SELECT COUNT(*) FROM {LAKE_CATALOG}.\"{table_name}\";"),
        [],
        |row| row.get(0),
    )
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

/// Converts table rows into a retryable payload for DuckDB writes.
fn prepare_rows(table_rows: Vec<TableRow>) -> PreparedRows {
    if table_rows
        .iter()
        .any(|row| row.values().iter().any(cell_requires_sql_literals))
    {
        return PreparedRows::SqlLiterals(
            table_rows
                .into_iter()
                .map(table_row_to_sql_literal)
                .collect(),
        );
    }

    PreparedRows::Appender(
        table_rows
            .into_iter()
            .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
            .collect(),
    )
}

/// Returns whether a cell must bypass the DuckDB appender path.
fn cell_requires_sql_literals(cell: &Cell) -> bool {
    matches!(cell, Cell::Array(_))
}

/// Serializes a row into a SQL `VALUES (...)` tuple.
fn table_row_to_sql_literal(row: TableRow) -> String {
    table_row_to_sql_literal_ref(&row)
}

/// Serializes a borrowed row into a SQL `VALUES (...)` tuple.
fn table_row_to_sql_literal_ref(row: &TableRow) -> String {
    format!(
        "({})",
        row.values()
            .iter()
            .map(|cell| cell_to_sql_literal(cell_to_owned(cell)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Converts a [`Cell`] into a DuckDB SQL literal expression.
fn cell_to_sql_literal(cell: Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Bool(b) => {
            if b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Cell::String(s) => quote_literal(&s),
        Cell::I16(i) => i.to_string(),
        Cell::I32(i) => i.to_string(),
        Cell::U32(u) => u.to_string(),
        Cell::I64(i) => i.to_string(),
        Cell::F32(f) => float_literal(f as f64, false),
        Cell::F64(f) => float_literal(f, true),
        Cell::Numeric(n) => quote_literal(&n.to_string()),
        Cell::Date(d) => format!("DATE '{}'", d.format("%Y-%m-%d")),
        Cell::Time(t) => format!("TIME '{}'", t.format("%H:%M:%S%.6f")),
        Cell::Timestamp(dt) => {
            format!("TIMESTAMP '{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
        }
        Cell::TimestampTz(dt) => {
            format!("TIMESTAMPTZ '{}'", dt.format("%Y-%m-%d %H:%M:%S%.6f%:z"))
        }
        Cell::Uuid(u) => format!("CAST({} AS UUID)", quote_literal(&u.to_string())),
        Cell::Json(j) => format!("CAST({} AS JSON)", quote_literal(&j.to_string())),
        Cell::Bytes(b) => format!("from_hex('{}')", encode_hex(&b)),
        Cell::Array(arr) => array_cell_to_sql_literal(arr),
    }
}

/// Clones a [`Cell`] from a borrowed row reference.
fn cell_to_owned(cell: &Cell) -> Cell {
    match cell {
        Cell::Null => Cell::Null,
        Cell::Bool(value) => Cell::Bool(*value),
        Cell::String(value) => Cell::String(value.clone()),
        Cell::I16(value) => Cell::I16(*value),
        Cell::I32(value) => Cell::I32(*value),
        Cell::U32(value) => Cell::U32(*value),
        Cell::I64(value) => Cell::I64(*value),
        Cell::F32(value) => Cell::F32(*value),
        Cell::F64(value) => Cell::F64(*value),
        Cell::Numeric(value) => Cell::Numeric(value.clone()),
        Cell::Date(value) => Cell::Date(*value),
        Cell::Time(value) => Cell::Time(*value),
        Cell::Timestamp(value) => Cell::Timestamp(*value),
        Cell::TimestampTz(value) => Cell::TimestampTz(*value),
        Cell::Uuid(value) => Cell::Uuid(*value),
        Cell::Json(value) => Cell::Json(value.clone()),
        Cell::Bytes(value) => Cell::Bytes(value.clone()),
        Cell::Array(value) => Cell::Array(array_cell_to_owned(value)),
    }
}

/// Clones an [`ArrayCell`] from a borrowed row reference.
fn array_cell_to_owned(cell: &ArrayCell) -> ArrayCell {
    match cell {
        ArrayCell::Bool(values) => ArrayCell::Bool(values.clone()),
        ArrayCell::String(values) => ArrayCell::String(values.clone()),
        ArrayCell::I16(values) => ArrayCell::I16(values.clone()),
        ArrayCell::I32(values) => ArrayCell::I32(values.clone()),
        ArrayCell::U32(values) => ArrayCell::U32(values.clone()),
        ArrayCell::I64(values) => ArrayCell::I64(values.clone()),
        ArrayCell::F32(values) => ArrayCell::F32(values.clone()),
        ArrayCell::F64(values) => ArrayCell::F64(values.clone()),
        ArrayCell::Numeric(values) => ArrayCell::Numeric(values.clone()),
        ArrayCell::Date(values) => ArrayCell::Date(values.clone()),
        ArrayCell::Time(values) => ArrayCell::Time(values.clone()),
        ArrayCell::Timestamp(values) => ArrayCell::Timestamp(values.clone()),
        ArrayCell::TimestampTz(values) => ArrayCell::TimestampTz(values.clone()),
        ArrayCell::Uuid(values) => ArrayCell::Uuid(values.clone()),
        ArrayCell::Json(values) => ArrayCell::Json(values.clone()),
        ArrayCell::Bytes(values) => ArrayCell::Bytes(values.clone()),
    }
}

/// Converts an [`ArrayCell`] into a DuckDB list literal expression.
fn array_cell_to_sql_literal(arr: ArrayCell) -> String {
    let values: Vec<String> = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| {
                    if value {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                })
                .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| quote_literal(&value))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| value.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| float_literal(value as f64, false))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| float_literal(value, true))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| quote_literal(&value.to_string()))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("DATE '{}'", value.format("%Y-%m-%d")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("TIME '{}'", value.format("%H:%M:%S%.6f")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("TIMESTAMP '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("TIMESTAMPTZ '{}'", value.format("%Y-%m-%d %H:%M:%S%.6f%:z")))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("CAST({} AS UUID)", quote_literal(&value.to_string())))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("CAST({} AS JSON)", quote_literal(&value.to_string())))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map(|value| format!("from_hex('{}')", encode_hex(&value)))
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect(),
    };

    format!("[{}]", values.join(", "))
}

/// Returns a DuckDB SQL literal for a floating-point value.
fn float_literal(value: f64, is_double: bool) -> String {
    if value.is_nan() {
        return if is_double {
            "CAST('NaN' AS DOUBLE)".to_string()
        } else {
            "CAST('NaN' AS FLOAT)".to_string()
        };
    }
    if value == f64::INFINITY {
        return if is_double {
            "CAST('Infinity' AS DOUBLE)".to_string()
        } else {
            "CAST('Infinity' AS FLOAT)".to_string()
        };
    }
    if value == f64::NEG_INFINITY {
        return if is_double {
            "CAST('-Infinity' AS DOUBLE)".to_string()
        } else {
            "CAST('-Infinity' AS FLOAT)".to_string()
        };
    }

    value.to_string()
}

/// Encodes bytes as uppercase hexadecimal for DuckDB's `from_hex`.
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

/// Converts a [`Cell`] to a [`duckdb::types::Value`] for use with parameterized
/// INSERT statements.
fn cell_to_value(cell: Cell) -> Value {
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();

    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => Value::Boolean(b),
        Cell::String(s) => Value::Text(s),
        Cell::I16(i) => Value::SmallInt(i),
        Cell::I32(i) => Value::Int(i),
        Cell::U32(u) => Value::UInt(u),
        Cell::I64(i) => Value::BigInt(i),
        Cell::F32(f) => Value::Float(f),
        Cell::F64(f) => Value::Double(f),
        // NUMERIC stored as VARCHAR to avoid precision loss.
        Cell::Numeric(n) => Value::Text(n.to_string()),
        Cell::Date(d) => Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32),
        Cell::Time(t) => {
            let micros = t
                .signed_duration_since(epoch_time)
                .num_microseconds()
                .unwrap_or(0);
            Value::Time64(TimeUnit::Microsecond, micros)
        }
        Cell::Timestamp(dt) => {
            Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
        }
        Cell::TimestampTz(dt) => Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros()),
        // UUID stored as text; DuckDB casts VARCHAR → UUID automatically.
        Cell::Uuid(u) => Value::Text(u.to_string()),
        // JSON serialised as text.
        Cell::Json(j) => Value::Text(j.to_string()),
        Cell::Bytes(b) => Value::Blob(b),
        Cell::Array(arr) => array_cell_to_value(arr),
    }
}

/// Converts an [`ArrayCell`] (with nullable elements) to a `Value::List`.
fn array_cell_to_value(arr: ArrayCell) -> Value {
    let values = match arr {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| o.map(Value::Boolean).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map(Value::Text).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map(Value::SmallInt).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map(Value::Int).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map(Value::UInt).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map(Value::BigInt).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| o.map(Value::Float).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map(Value::Double).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| o.map(|n| Value::Text(n.to_string())).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Date(v) => {
            let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map(|d| Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32))
                        .unwrap_or(Value::Null)
                })
                .collect()
        }
        ArrayCell::Time(v) => {
            let epoch_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            v.into_iter()
                .map(|o| {
                    o.map(|t| {
                        let micros = t
                            .signed_duration_since(epoch_time)
                            .num_microseconds()
                            .unwrap_or(0);
                        Value::Time64(TimeUnit::Microsecond, micros)
                    })
                    .unwrap_or(Value::Null)
                })
                .collect()
        }
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map(|dt| Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros()))
                    .unwrap_or(Value::Null)
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map(|dt| Value::Timestamp(TimeUnit::Microsecond, dt.timestamp_micros()))
                    .unwrap_or(Value::Null)
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| o.map(|u| Value::Text(u.to_string())).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| o.map(|j| Value::Text(j.to_string())).unwrap_or(Value::Null))
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| o.map(Value::Blob).unwrap_or(Value::Null))
            .collect(),
    };
    Value::List(values)
}

#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_FLUSH_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_FLUSH_TABLE: LazyLock<Mutex<Option<String>>> =
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

/// Arms a test hook that injects one failure after flushing a mutation batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_atomic_batch_flush_once_for_tests(table_name: &str) {
    *FAIL_AFTER_ATOMIC_BATCH_FLUSH_TABLE.lock() = Some(table_name.to_string());
}

/// Arms a test hook that injects one failure after flushing a copy batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_copy_batch_flush_once_for_tests(table_name: &str) {
    *FAIL_AFTER_COPY_BATCH_FLUSH_TABLE.lock() = Some(table_name.to_string());
}

/// Clears DuckLake destination test hooks.
#[cfg(feature = "test-utils")]
pub fn reset_ducklake_test_hooks() {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_ATOMIC_BATCH_FLUSH_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_FLUSH_TABLE.lock() = None;
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

/// Injects a synthetic failure after flush so retries must avoid reapplying rows.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_flushed_batch_for_tests(
    batch_kind: DuckLakeTableBatchKind,
    table_name: &str,
) -> EtlResult<()> {
    match batch_kind {
        DuckLakeTableBatchKind::Copy => maybe_fail_after_copy_batch_flush_for_tests(table_name),
        DuckLakeTableBatchKind::Mutation => {
            maybe_fail_after_atomic_batch_flush_for_tests(table_name)
        }
        DuckLakeTableBatchKind::Truncate => Ok(()),
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

/// Injects a synthetic failure after flush so mutation retries re-enter at flush only.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_atomic_batch_flush_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_ATOMIC_BATCH_FLUSH_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "ducklake test hook injected post-flush failure"
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

/// Injects a synthetic failure after flush so copy retries re-enter at flush only.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_copy_batch_flush_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_COPY_BATCH_FLUSH_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "ducklake test hook injected copy post-flush failure"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::{ColumnSchema, Type as PgType};

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
    fn test_cell_to_value_primitives() {
        assert_eq!(cell_to_value(Cell::Null), Value::Null);
        assert_eq!(cell_to_value(Cell::Bool(true)), Value::Boolean(true));
        assert_eq!(
            cell_to_value(Cell::String("hello".to_string())),
            Value::Text("hello".to_string())
        );
        assert_eq!(cell_to_value(Cell::I32(42)), Value::Int(42));
        assert_eq!(cell_to_value(Cell::I64(-1)), Value::BigInt(-1));
        assert_eq!(cell_to_value(Cell::F64(3.46)), Value::Double(3.46));
    }

    #[test]
    fn test_format_query_error_detail_compacts_sql_and_includes_source() {
        let sql = "CREATE TABLE lake.\"orders\" (\n  \"id\" INTEGER NOT NULL\n)";
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("parser error".to_string()),
        );

        assert_eq!(
            format_query_error_detail(sql, &error),
            "sql: CREATE TABLE lake.\"orders\" ( \"id\" INTEGER NOT NULL ); source: parser error"
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
    fn test_array_cell_to_sql_literal_preserves_nulls() {
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::I32(vec![Some(1), None, Some(3)])),
            "[1, NULL, 3]"
        );
        assert_eq!(
            array_cell_to_sql_literal(ArrayCell::Json(vec![
                Some(serde_json::json!({"a": 1})),
                None,
            ])),
            "[CAST('{\"a\":1}' AS JSON), NULL]"
        );
    }

    #[test]
    fn test_prepare_rows_uses_sql_literals_for_arrays() {
        let prepared = prepare_rows(vec![TableRow::new(vec![
            Cell::I32(1),
            Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
        ])]);

        match prepared {
            PreparedRows::SqlLiterals(rows) => {
                assert_eq!(rows, vec!["(1, [1, NULL, 3])"]);
            }
            PreparedRows::Appender(_) => panic!("expected sql literal fallback"),
        }
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
}
