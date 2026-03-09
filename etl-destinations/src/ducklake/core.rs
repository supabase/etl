use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDate, NaiveTime};
use duckdb::types::{TimeUnit, Value};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{ArrayCell, Cell, Event, TableId, TableName, TableRow};
use parking_lot::Mutex;
use r2d2::Pool;
use rand::Rng;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use tracing::{debug, info, warn};
use url::Url;

use crate::ducklake::S3Config;
use crate::ducklake::config::build_setup_sql;
use crate::ducklake::schema::build_create_table_sql_ducklake;

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
pub(super) const LAKE_CATALOG: &str = "lake";
/// Delimiter used to join schema and table name in the DuckLake table name.
const TABLE_NAME_DELIMITER: &str = "_";
/// Escape string for underscores within schema/table names to prevent collisions.
const TABLE_NAME_ESCAPE: &str = "__";

/// Alias for DuckLake table names.
type DuckLakeTableName = String;

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
}

impl r2d2::ManageConnection for DuckLakeConnectionManager {
    type Connection = duckdb::Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<duckdb::Connection, duckdb::Error> {
        let conn = duckdb::Connection::open_in_memory()?;
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
    ///   `gs://bucket/prefix/`, `az://container/prefix/`).
    /// - `pool_size`: Number of concurrent DuckDB connections. `4` is a
    ///   reasonable default; higher values allow more tables to be written in
    ///   parallel.
    /// - `s3`: Optional S3 credentials. Required when `data_path` is an S3 URI
    ///   and the bucket is not publicly accessible.
    /// - `metadata_schema`: Optional Postgres schema for DuckLake metadata tables
    ///   (e.g. `"ducklake"`). Uses the catalog default schema when not set.
    pub fn new(
        catalog_url: Url,
        data_path: Url,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        store: S,
    ) -> EtlResult<Self> {
        let setup_sql = build_setup_sql(
            &catalog_url,
            &data_path,
            s3.as_ref(),
            metadata_schema.as_deref(),
        )?;

        let manager = DuckLakeConnectionManager {
            setup_sql: Arc::new(setup_sql),
        };

        let pool = Pool::builder()
            .max_size(pool_size)
            .build(manager)
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

        // Convert to Values before retrying so each blocking attempt can reuse
        // the same rows without needing Clone on Cell.
        let all_values: Vec<Vec<Value>> = table_rows
            .into_iter()
            .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
            .collect();
        insert_rows_with_retry(
            self.pool.clone(),
            Arc::clone(&self.blocking_slots),
            table_name,
            all_values,
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
            let mut table_id_to_rows: HashMap<TableId, Vec<Vec<Value>>> = HashMap::new();

            // Accumulate non-truncate events, stopping at the first Truncate.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    // Handled later
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(insert) => {
                        let values = insert
                            .table_row
                            .into_values()
                            .into_iter()
                            .map(cell_to_value)
                            .collect();
                        table_id_to_rows
                            .entry(insert.table_id)
                            .or_default()
                            .push(values);
                    }
                    Event::Update(update) => {
                        let values = update
                            .table_row
                            .into_values()
                            .into_iter()
                            .map(cell_to_value)
                            .collect();
                        table_id_to_rows
                            .entry(update.table_id)
                            .or_default()
                            .push(values);
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            info!("delete event has no old row, skipping");
                            continue;
                        };
                        let values = old_row
                            .into_values()
                            .into_iter()
                            .map(cell_to_value)
                            .collect();
                        table_id_to_rows
                            .entry(delete.table_id)
                            .or_default()
                            .push(values);
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // One async task per table. Each write attempt acquires one
            // blocking slot, then uses a separate pool connection and commits
            // an independent Parquet snapshot.
            if !table_id_to_rows.is_empty() {
                let mut join_set = JoinSet::new();

                for (table_id, rows) in table_id_to_rows {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let pool = self.pool.clone();
                    let blocking_slots = Arc::clone(&self.blocking_slots);

                    join_set.spawn(async move {
                        insert_rows_with_retry(pool, blocking_slots, table_name, rows).await
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

        // `build_create_table_sql_ducklake` generates `CREATE TABLE IF NOT EXISTS "name" (...)`.
        // Prefix the table name with the catalog alias so DuckLake knows which
        // catalog to create the table in.
        let ddl = build_create_table_sql_ducklake(&table_name, &table_schema.column_schemas);
        let qualified_ddl = ddl.replace(
            &format!("{table_name:?}"),
            &format!("{LAKE_CATALOG}.\"{table_name}\""),
        );

        let pool = self.pool.clone();
        let created_tables = Arc::clone(&self.created_tables);
        let table_name_clone = table_name.clone();

        run_duckdb_blocking(
            pool,
            Arc::clone(&self.blocking_slots),
            move |conn| -> EtlResult<()> {
                conn.execute_batch(&qualified_ddl).map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake CREATE TABLE failed",
                        source: e
                    )
                })?;
                created_tables.lock().insert(table_name_clone);
                Ok(())
            },
        )
        .await?;

        Ok(table_name)
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
    all_values: Vec<Vec<Value>>,
) -> EtlResult<()> {
    let all_values = Arc::new(all_values);
    let mut delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    for attempt in 0..=MAX_COMMIT_RETRIES {
        let attempt_table_name = table_name.clone();
        let attempt_values = Arc::clone(&all_values);

        match run_duckdb_blocking(pool.clone(), Arc::clone(&blocking_slots), move |conn| {
            insert_rows(
                conn,
                &attempt_table_name,
                attempt_values.as_ref().as_slice(),
            )
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

/// Inserts all rows in `all_values` into `lake."table_name"` as a **single
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
    all_values: &[Vec<Value>],
) -> EtlResult<()> {
    if all_values.is_empty() {
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

    // Bulk-load all rows into staging via the Appender (fast, purely in-memory).
    let load_result = (|| {
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
    })();

    if let Err(e) = load_result {
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
        return Err(e);
    }

    // One INSERT SELECT = one Parquet file in DuckLake.
    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: e
        )
    })?;

    let insert_result = conn
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

    let commit_result = match insert_result {
        Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", source: e)
        }),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    };

    // Always drop the staging table.
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));

    commit_result
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
}
