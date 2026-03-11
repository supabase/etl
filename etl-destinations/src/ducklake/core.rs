use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::sync::Arc;
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
use r2d2::Pool;
use rand::Rng;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use tracing::{debug, info, warn};
use url::Url;

use crate::ducklake::S3Config;
use crate::ducklake::config::{build_setup_sql, current_duckdb_extension_strategy};
use crate::ducklake::schema::build_create_table_sql_ducklake;

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
pub(super) const LAKE_CATALOG: &str = "lake";
/// Delimiter used to join schema and table name in the DuckLake table name.
const TABLE_NAME_DELIMITER: &str = "_";
/// Escape string for underscores within schema/table names to prevent collisions.
const TABLE_NAME_ESCAPE: &str = "__";
/// Maximum number of rows per SQL `INSERT ... VALUES` batch when nested values
/// force the staging path to bypass DuckDB's appender API.
const SQL_INSERT_BATCH_SIZE: usize = 256;
/// Maximum number of primary-key predicates per SQL `DELETE` batch.
///
/// We intentionally keep this at one row per statement for CDC deletes. This
/// avoids relying on DuckLake's inlining heuristics and keeps each delete
/// operation as small and predictable as possible inside the transaction.
const SQL_DELETE_BATCH_SIZE: usize = 1;

/// Alias for DuckLake table names.
type DuckLakeTableName = String;

/// Prepared row payload reused across retry attempts.
enum PreparedRows {
    Appender(Vec<Vec<Value>>),
    SqlLiterals(Vec<String>),
}

/// Event-level table mutations that must be applied in order.
enum TableMutation {
    Upsert(TableRow),
    Delete(TableRow),
    Replace(TableRow),
}

/// Prepared table mutations ready for execution and retries.
enum PreparedTableMutation {
    Upsert(PreparedRows),
    Delete(Vec<String>),
}

// ── connection manager ────────────────────────────────────────────────────────

/// Custom r2d2 connection manager that opens an in-memory DuckDB connection and
/// attaches the DuckLake catalog on every `connect()` call.
///
/// Each pool connection is independent and attaches the same catalog, which is
/// safe: DuckLake (backed by a PostgreSQL catalog) supports concurrent writers.
struct DuckLakeConnectionManager {
    /// SQL executed immediately after a new connection is opened.
    /// Loads required extensions and attaches the DuckLake catalog.
    setup_sql: Arc<String>,
    /// Disables DuckDB extension autoload/autoinstall when vendored Linux
    /// extensions are required.
    disable_extension_autoload: bool,
}

impl r2d2::ManageConnection for DuckLakeConnectionManager {
    type Connection = duckdb::Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<duckdb::Connection, duckdb::Error> {
        let conn = if self.disable_extension_autoload {
            duckdb::Connection::open_in_memory_with_flags(
                Config::default().enable_autoload_extension(false)?,
            )?
        } else {
            duckdb::Connection::open_in_memory()?
        };
        conn.execute_batch(&self.setup_sql)?;
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut duckdb::Connection) -> Result<(), duckdb::Error> {
        conn.execute_batch("SELECT 1")?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut duckdb::Connection) -> bool {
        false
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
/// Writes data to a DuckLake data lake. Each DuckDB connection in the pool
/// attaches the same DuckLake catalog, allowing tables to be written in parallel.
/// Data is persisted as Parquet files at `data_path`; metadata is tracked in a
/// PostgreSQL catalog database.
///
/// All writes are wrapped in explicit transactions so that each batch of rows
/// is committed as a single Parquet snapshot rather than one file per row.
#[derive(Clone)]
pub struct DuckLakeDestination<S> {
    pool: Pool<DuckLakeConnectionManager>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    store: S,
    /// Cache of table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
}

impl<S> DuckLakeDestination<S> {
    /// Creates a new DuckLake destination.
    ///
    /// - `catalog_url`: DuckLake catalog location. Use a PostgreSQL URL
    ///   (`postgres://user:pass@localhost:5432/mydb`) or a local file URL
    ///   (`file:///tmp/catalog.ducklake`).
    /// - `data_path`: Where Parquet files are stored. Use a local file URL
    ///   (`file:///tmp/lake_data`) or a cloud URL (`s3://bucket/prefix/`,
    ///   `gs://bucket/prefix/`).
    /// - `pool_size`: Number of concurrent DuckDB connections. `4` is a
    ///   reasonable default; higher values allow more tables to be written in
    ///   parallel.
    /// - `s3`: Optional S3 credentials. Required when `data_path` is an S3 URI
    ///   and the bucket is not publicly accessible.
    /// - `metadata_schema`: Optional Postgres schema for DuckLake metadata tables
    ///   (e.g. `"ducklake"`). Uses the catalog default schema when not set.
    /// - On Linux, DuckDB extensions are loaded from vendored local files when
    ///   a vendored directory is available. The root directory can be forced
    ///   with `ETL_DUCKDB_EXTENSION_ROOT`. Otherwise, DuckDB uses the legacy
    ///   online `INSTALL` flow. On macOS and Windows, DuckDB always uses the
    ///   legacy online `INSTALL` flow.
    ///
    /// Pool initialization is blocking because `r2d2` eagerly opens and
    /// validates connections. This constructor offloads that work to Tokio's
    /// blocking pool.
    pub async fn new(
        catalog_url: Url,
        data_path: Url,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        store: S,
    ) -> EtlResult<Self> {
        let extension_strategy = current_duckdb_extension_strategy()?;
        if let crate::ducklake::config::DuckDbExtensionStrategy::VendoredLinux { platform_dir } =
            extension_strategy
        {
            info!(platform = platform_dir, "using vendored duckdb extensions");
        }
        let setup_sql = build_setup_sql(
            &catalog_url,
            &data_path,
            s3.as_ref(),
            metadata_schema.as_deref(),
        )?;

        let manager = DuckLakeConnectionManager {
            setup_sql: Arc::new(setup_sql),
            disable_extension_autoload: extension_strategy.disables_autoload(),
        };

        let pool =
            tokio::task::spawn_blocking(move || Pool::builder().max_size(pool_size).build(manager))
                .await
                .map_err(|_| {
                    etl_error!(
                        ErrorKind::ApplyWorkerPanic,
                        "DuckLake pool build task panicked"
                    )
                })?
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationConnectionFailed,
                        "Failed to build DuckLake connection pool",
                        source: e
                    )
                })?;

        Ok(Self {
            pool,
            blocking_slots: Arc::new(Semaphore::new(pool_size as usize)),
            table_creation_slots: Arc::new(Semaphore::new(1)),
            store,
            created_tables: Arc::new(Mutex::new(HashSet::new())),
        })
    }
}

impl<S> Destination for DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "ducklake"
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
    /// Deletes all rows from the destination table without dropping it.
    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;
        self.run_duckdb_blocking(move |conn| -> EtlResult<()> {
            conn.execute_batch(&format!("DELETE FROM {LAKE_CATALOG}.\"{table_name}\""))
                .map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake DELETE failed",
                        source: e
                    )
                })?;
            Ok(())
        })
        .await
    }

    /// Bulk-inserts rows into the destination table inside a single transaction.
    ///
    /// Wrapping all inserts in one `BEGIN` / `COMMIT` ensures they are written
    /// as a single Parquet snapshot rather than one file per row.
    ///
    /// If a write attempt fails, the transaction is retried with exponential
    /// backoff. The current implementation preserves the existing broad retry
    /// behavior rather than matching only DuckLake commit conflicts.
    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        let prepared_rows = prepare_rows(table_rows);
        insert_rows_with_retry(
            self.pool.clone(),
            Arc::clone(&self.blocking_slots),
            table_name,
            prepared_rows,
        )
        .await
    }

    /// Writes streaming CDC events to the destination.
    ///
    /// Insert, Update, and Delete events are grouped by table and written in
    /// parallel, each table in its own async task. Each DuckDB attempt acquires
    /// one blocking slot before entering `spawn_blocking`. Truncate events are
    /// deduplicated and processed after the per-table writes.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_mutations: HashMap<TableId, Vec<TableMutation>> = HashMap::new();

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
                            .push(TableMutation::Upsert(insert.table_row));
                    }
                    Event::Update(update) => {
                        let table_id = update.table_id;
                        let table_row = update.table_row;
                        let old_table_row = update.old_table_row;
                        let mutations = table_id_to_mutations.entry(table_id).or_default();
                        if let Some((_, old_row)) = old_table_row {
                            // TODO not sure about this
                            mutations.push(TableMutation::Delete(old_row));
                            mutations.push(TableMutation::Upsert(table_row));
                        } else {
                            debug!(
                                "update event has no old row, deleting by primary key from new row"
                            );
                            mutations.push(TableMutation::Replace(table_row));
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
                            .push(TableMutation::Delete(old_row));
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // One async task per table. Each write attempt acquires one
            // blocking slot, then uses a separate pool connection and commits
            // an independent Parquet snapshot.
            if !table_id_to_mutations.is_empty() {
                let mut join_set = JoinSet::new();

                for (table_id, mutations) in table_id_to_mutations {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let table_schema = self.get_table_schema(table_id).await?;
                    let pool = self.pool.clone();
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    let prepared_mutations = prepare_table_mutations(&table_schema, mutations)?;

                    join_set.spawn(async move {
                        apply_table_mutations_with_retry(
                            pool,
                            blocking_slots,
                            table_name,
                            prepared_mutations,
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

            // Collect and deduplicate Truncate events.
            let mut truncate_table_ids = HashSet::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for rel_id in truncate.rel_ids {
                        truncate_table_ids.insert(TableId::new(rel_id));
                    }
                }
            }

            for table_id in truncate_table_ids {
                self.truncate_table_inner(table_id).await?;
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

        let pool = self.pool.clone();
        let created_tables = Arc::clone(&self.created_tables);
        let table_name_clone = table_name.clone();

        run_duckdb_blocking(
            pool,
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

        let ducklake_table_name = table_name_to_ducklake_table_name(table_name);
        self.store
            .store_table_mapping(table_id, ducklake_table_name.clone())
            .await?;
        Ok(ducklake_table_name)
    }

    /// Runs one DuckDB operation on Tokio's blocking pool after acquiring a
    /// permit that matches the DuckDB connection pool capacity.
    async fn run_duckdb_blocking<R, F>(&self, operation: F) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        run_duckdb_blocking(
            self.pool.clone(),
            Arc::clone(&self.blocking_slots),
            operation,
        )
        .await
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
pub fn table_name_to_ducklake_table_name(table_name: &TableName) -> DuckLakeTableName {
    let escaped_schema = table_name
        .schema
        .replace(TABLE_NAME_DELIMITER, TABLE_NAME_ESCAPE);
    let escaped_table = table_name
        .name
        .replace(TABLE_NAME_DELIMITER, TABLE_NAME_ESCAPE);
    format!("{escaped_schema}{TABLE_NAME_DELIMITER}{escaped_table}")
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Maximum number of times a failed write attempt is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 10;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 2_000;
/// Minimum retry delay for transient delete-file visibility failures.
const TRANSIENT_DELETE_FILE_RETRY_DELAY_MS: u64 = 5_000;

/// Runs one DuckDB operation on Tokio's blocking pool after acquiring a permit
/// that matches the DuckDB connection pool capacity.
async fn run_duckdb_blocking<R, F>(
    pool: Pool<DuckLakeConnectionManager>,
    blocking_slots: Arc<Semaphore>,
    operation: F,
) -> EtlResult<R>
where
    R: Send + 'static,
    F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
{
    let permit = blocking_slots.acquire_owned().await.map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake blocking slot acquisition failed"
        )
    })?;

    tokio::task::spawn_blocking(move || -> EtlResult<R> {
        // Please if you modify the code inside this blocking task do not add any
        // blocking operations that could delay other tasks waiting on this slot.
        let _permit = permit;
        let conn = pool.get().map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to get DuckLake connection from pool",
                source: e
            )
        })?;
        operation(&conn)
    })
    .await
    .map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake blocking task panicked"
        )
    })?
}

/// Calls [`insert_rows`] and retries with exponential backoff on failure.
///
/// The current implementation preserves the existing broad retry behavior:
/// any failed write attempt is retried until the retry budget is exhausted.
async fn insert_rows_with_retry(
    pool: Pool<DuckLakeConnectionManager>,
    blocking_slots: Arc<Semaphore>,
    table_name: DuckLakeTableName,
    prepared_rows: PreparedRows,
) -> EtlResult<()> {
    let prepared_rows = Arc::new(prepared_rows);
    let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    for attempt in 0..=MAX_COMMIT_RETRIES {
        let attempt_table_name = table_name.clone();
        let attempt_rows = Arc::clone(&prepared_rows);

        match run_duckdb_blocking(pool.clone(), Arc::clone(&blocking_slots), move |conn| {
            insert_rows(conn, &attempt_table_name, attempt_rows.as_ref())
        })
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
                // Apply full-jitter: sleep between 50 % and 150 % of the
                // calculated delay so concurrent retriers spread out instead
                // of staying in lock-step and repeatedly colliding.
                let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
                let jittered = delay.mul_f64(jitter_ratio);
                warn!(
                    attempt = attempt + 1,
                    max = MAX_COMMIT_RETRIES,
                    table = %table_name,
                    error = ?e,
                    "DuckLake write attempt failed, retrying"
                );
                tokio::time::sleep(jittered).await;
                delay = std::cmp::min(delay * 2, Duration::from_millis(MAX_RETRY_DELAY_MS));
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

/// Applies an ordered mutation sequence atomically and retries on failure.
async fn apply_table_mutations_with_retry(
    pool: Pool<DuckLakeConnectionManager>,
    blocking_slots: Arc<Semaphore>,
    table_name: DuckLakeTableName,
    prepared_mutations: Vec<PreparedTableMutation>,
) -> EtlResult<()> {
    if prepared_mutations.is_empty() {
        return Ok(());
    }

    let prepared_mutations = Arc::new(prepared_mutations);
    let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    for attempt in 0..=MAX_COMMIT_RETRIES {
        let attempt_table_name = table_name.clone();
        let attempt_mutations = Arc::clone(&prepared_mutations);

        match run_duckdb_blocking(pool.clone(), Arc::clone(&blocking_slots), move |conn| {
            apply_table_mutations(conn, &attempt_table_name, attempt_mutations.as_slice())
        })
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
                let is_transient_delete_file_404 = is_transient_ducklake_delete_file_not_found(&e);
                let base_delay = if is_transient_delete_file_404 {
                    std::cmp::max(
                        delay,
                        Duration::from_millis(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
                    )
                } else {
                    delay
                };
                let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
                let jittered = base_delay.mul_f64(jitter_ratio);
                if is_transient_delete_file_404 {
                    debug!(
                        attempt = attempt + 1,
                        max = MAX_COMMIT_RETRIES,
                        table = %table_name,
                        delay_ms = jittered.as_millis() as u64,
                        error = ?e,
                        "ducklake delete file not visible yet, retrying"
                    );
                } else {
                    warn!(
                        attempt = attempt + 1,
                        max = MAX_COMMIT_RETRIES,
                        table = %table_name,
                        error = ?e,
                        "ducklake table mutation attempt failed, retrying"
                    );
                }
                tokio::time::sleep(jittered).await;
                delay = std::cmp::min(
                    base_delay * 2,
                    Duration::from_millis(
                        MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
                    ),
                );
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

/// Returns whether the error chain matches DuckLake's transient object-store
/// visibility failure for delete parquet files.
fn is_transient_ducklake_delete_file_not_found(error: &etl::error::EtlError) -> bool {
    let mut current: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(source) = current {
        let message = source.to_string();
        if message.contains("delete.parquet")
            && message.contains("404 (Not Found)")
            && message.contains("HTTP Error")
        {
            return true;
        }
        current = source.source();
    }

    false
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
            TableMutation::Upsert(row) => {
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete(std::mem::take(
                        &mut delete_predicates,
                    )));
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
            TableMutation::Replace(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete(std::mem::take(
                        &mut delete_predicates,
                    )));
                }

                prepared_mutations.push(PreparedTableMutation::Delete(vec![
                    delete_predicate_from_row(table_schema, &row)?,
                ]));
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(vec![row])));
            }
        }
    }

    if !upsert_rows.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(upsert_rows)));
    }
    if !delete_predicates.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Delete(delete_predicates));
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

/// Inserts all prepared rows into `lake."table_name"` as a **single
/// Parquet data file**.
///
/// ## Why staging?
///
/// DuckLake creates one Parquet data file **per DML statement** that touches a
/// table, even inside a transaction.  Calling `stmt.execute()` in a loop
/// therefore creates one file per row — extremely inefficient.
///
/// The solution is to:
/// 1. Load all rows into a local in-memory DuckDB staging table using the
///    fast Appender API (no DuckLake involvement, no files written).
/// 2. Issue a **single** `INSERT INTO lake.t SELECT * FROM staging` inside a
///    transaction.  DuckLake sees one DML statement → one Parquet file for
///    the entire batch, regardless of row count.
///
/// On any failure the transaction is rolled back and the staging table is
/// always dropped before returning.
fn insert_rows(
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

    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: e
        )
    })?;

    match apply_upsert_mutation(conn, table_name, prepared_rows) {
        Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", source: e)
        }),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Applies a per-table CDC mutation sequence in one transaction.
fn apply_table_mutations(
    conn: &duckdb::Connection,
    table_name: &str,
    prepared_mutations: &[PreparedTableMutation],
) -> EtlResult<()> {
    if prepared_mutations.is_empty() {
        return Ok(());
    }

    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: e
        )
    })?;

    let result = prepared_mutations
        .iter()
        .try_for_each(|mutation| apply_table_mutation(conn, table_name, mutation));

    match result {
        Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", source: e)
        }),
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
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
        PreparedTableMutation::Delete(predicates) => {
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
    // even when multiple pool connections run concurrently.
    let staging = format!("__staging_{table_name}");

    // Mirror the DuckLake target schema without copying any data.
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP TABLE {staging:?} AS \
         SELECT * FROM {LAKE_CATALOG}.\"{table_name}\" LIMIT 0;"
    ))
    .map_err(|e| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake staging table creation failed",
            source: e
        )
    })?;

    let load_result = match prepared_rows {
        PreparedRows::Appender(all_values) => (|| {
            let mut appender = conn.appender(&staging).map_err(|e| {
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
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake staging append_row failed",
                            source: e
                        )
                    })?;
            }
            appender.flush().map_err(|e| {
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
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
        return Err(error);
    }

    let result = conn
        .execute_batch(&format!(
            "INSERT INTO {LAKE_CATALOG}.\"{table_name}\" SELECT * FROM {staging:?};"
        ))
        .map_err(|e| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                source: e
            )
        });

    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));

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
        conn.execute_batch(&format!(
            "DELETE FROM {LAKE_CATALOG}.\"{table_name}\" WHERE {};",
            chunk
                .iter()
                .map(|predicate| format!("({predicate})"))
                .collect::<Vec<_>>()
                .join(" OR ")
        ))
        .map_err(|e| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake DELETE failed",
                source: e
            )
        })?;
    }

    Ok(())
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
    format!(
        "({})",
        row.into_values()
            .into_iter()
            .map(cell_to_sql_literal)
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
            }),
            "public_orders"
        );
        assert_eq!(
            table_name_to_ducklake_table_name(&TableName {
                schema: "my_schema".to_string(),
                name: "my_table".to_string(),
            }),
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
            PreparedTableMutation::Delete(predicates) => {
                assert_eq!(predicates, &vec!["id = 1".to_string()]);
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
            PreparedTableMutation::Delete(_) => panic!("expected upsert second"),
        }
    }

    #[test]
    fn test_is_transient_ducklake_delete_file_not_found_matches_delete_parquet_404() {
        let error = etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake DELETE failed",
            source: duckdb::Error::DuckDBFailure(
                duckdb::ffi::Error::new(1),
                Some(
                    "HTTP Error: Unable to connect to URL \"https://example.com/path/ducklake-123-delete.parquet\": 404 (Not Found).".to_string(),
                ),
            )
        );

        assert!(is_transient_ducklake_delete_file_not_found(&error));
    }

    #[test]
    fn test_is_transient_ducklake_delete_file_not_found_ignores_other_errors() {
        let error = etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake DELETE failed",
            source: duckdb::Error::DuckDBFailure(
                duckdb::ffi::Error::new(1),
                Some("Transaction conflict".to_string()),
            )
        );

        assert!(!is_transient_ducklake_delete_file_not_found(&error));
    }
}
