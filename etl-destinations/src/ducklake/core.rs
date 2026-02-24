use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use chrono::{NaiveDate, NaiveTime};
use duckdb::types::{TimeUnit, Value};
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{ArrayCell, Cell, Event, TableId, TableName, TableRow};
use etl::etl_error;
use r2d2::Pool;
use tracing::{debug, info, warn};

use crate::ducklake::schema::build_create_table_sql_ducklake;

/// The DuckDB catalog alias used in every `lake.<table>` qualified name.
const LAKE_CATALOG: &str = "lake";

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

/// Converts a standard `postgres://` or `postgresql://` URI into the libpq
/// key=value format that DuckLake's PostgreSQL catalog backend expects.
///
/// DuckLake does **not** accept URI-style connection strings — it only
/// understands libpq key=value pairs (e.g. `host=localhost dbname=mydb`).
/// Passing a URI causes DuckDB to treat the string as a file path, resulting
/// in "Cannot open file" errors.
///
/// Non-postgres strings are returned unchanged.
fn normalize_catalog_url(catalog_url: &str) -> String {
    let rest = catalog_url
        .strip_prefix("postgres://")
        .or_else(|| catalog_url.strip_prefix("postgresql://"));

    let Some(rest) = rest else {
        return catalog_url.to_string();
    };

    // Split userinfo from host/path: "user:pass@host:port/dbname"
    let (userinfo, host_path) = rest.split_once('@').unwrap_or(("", rest));
    let (host_port, dbname_and_query) = host_path.split_once('/').unwrap_or((host_path, ""));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));
    let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));

    // Split dbname from query string: "mydb?sslmode=disable&connect_timeout=10"
    let (dbname, query) = dbname_and_query.split_once('?').unwrap_or((dbname_and_query, ""));

    let mut parts = Vec::new();
    if !host.is_empty() {
        parts.push(format!("host={host}"));
    }
    if !port.is_empty() && port != "5432" {
        parts.push(format!("port={port}"));
    }
    if !dbname.is_empty() {
        parts.push(format!("dbname={dbname}"));
    }
    if !user.is_empty() {
        parts.push(format!("user={user}"));
    }
    if !password.is_empty() {
        // Quote the password and escape any single quotes / backslashes inside it,
        // as required by the libpq keyword=value format when values contain
        // characters that could otherwise confuse the parser.
        let escaped = password.replace('\\', "\\\\").replace('\'', "\\'");
        parts.push(format!("password='{escaped}'"));
    }
    // Convert URL query params (key=value&...) to libpq key=value pairs.
    for param in query.split('&').filter(|s| !s.is_empty()) {
        parts.push(param.to_string());
    }
    // DuckLake PostgreSQL catalog prefix is "postgres:" (no slashes)
    format!("postgres:{}", parts.join(" "))
}

/// S3-compatible storage credentials for DuckDB's httpfs extension.
///
/// Used when `data_path` points to an S3, GCS, or Azure URI. The `endpoint`
/// field supports an optional path prefix (e.g. `localhost:5000/s3` for
/// Supabase Storage or other S3-compatible services mounted at a sub-path).
#[derive(Debug, Clone)]
pub struct S3Config {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// AWS region or equivalent (e.g. `"us-east-1"`).
    pub region: String,
    /// Host, host:port, or host:port/path of the S3-compatible endpoint.
    /// Defaults to AWS S3 if not set.
    pub endpoint: Option<String>,
    /// `"path"` (default for MinIO / Supabase Storage) or `"vhost"` (AWS S3 default).
    pub url_style: String,
    /// Whether to use HTTPS. Set to `false` for local S3-compatible services.
    pub use_ssl: bool,
}

/// Builds the one-time setup SQL executed for each new pool connection.
///
/// - Always installs and loads the `ducklake` extension.
/// - Installs the `postgres` extension when the catalog is PostgreSQL-backed
///   (`postgres:` prefix after normalisation).
/// - Installs `httpfs` and configures S3 credentials when the data path uses
///   a cloud URI scheme (`s3://`, `gs://`, `az://`).
fn build_setup_sql(catalog_url: &str, data_path: &str, s3: Option<&S3Config>, metadata_schema: Option<&str>) -> String {
    let normalized = normalize_catalog_url(catalog_url);

    let needs_postgres = normalized.starts_with("postgres:");
    let needs_httpfs = data_path.starts_with("s3://")
        || data_path.starts_with("gs://")
        || data_path.starts_with("az://");

    let mut sql = String::from("INSTALL ducklake; LOAD ducklake;");
    if needs_postgres {
        sql.push_str(" INSTALL postgres; LOAD postgres;");
    }
    if needs_httpfs {
        sql.push_str(" INSTALL httpfs; LOAD httpfs;");
        if let Some(s3) = s3 {
            // Escape values for embedding in SQL string literals.
            let key = s3.access_key_id.replace('\'', "''");
            let secret = s3.secret_access_key.replace('\'', "''");
            let region = s3.region.replace('\'', "''");
            let url_style = s3.url_style.replace('\'', "''");
            let use_ssl = if s3.use_ssl { "true" } else { "false" };

            let endpoint_clause = match &s3.endpoint {
                Some(ep) => format!(", ENDPOINT '{}'", ep.replace('\'', "''")),
                None => String::new(),
            };

            sql.push_str(&format!(
                " CREATE OR REPLACE SECRET ducklake_s3 (\
                    TYPE S3, \
                    KEY_ID '{key}', \
                    SECRET '{secret}', \
                    REGION '{region}'{endpoint_clause}, \
                    URL_STYLE '{url_style}', \
                    USE_SSL {use_ssl}\
                );"
            ));
        }
    }
    // Escape single quotes for embedding in a SQL string literal ('' = literal ').
    let sql_normalized = normalized.replace('\'', "''");
    let sql_data_path = data_path.replace('\'', "''");

    let metadata_schema_clause = match metadata_schema {
        Some(schema) => format!(", METADATA_SCHEMA '{}'", schema.replace('\'', "''")),
        None => String::new(),
    };

    sql.push_str(&format!(
        " ATTACH 'ducklake:{sql_normalized}' AS {LAKE_CATALOG} (DATA_PATH '{sql_data_path}'{metadata_schema_clause});"
    ));
    sql
}

// ── destination ───────────────────────────────────────────────────────────────

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
    let escaped_schema = table_name.schema.replace(TABLE_NAME_DELIMITER, TABLE_NAME_ESCAPE);
    let escaped_table = table_name.name.replace(TABLE_NAME_DELIMITER, TABLE_NAME_ESCAPE);
    format!("{escaped_schema}{TABLE_NAME_DELIMITER}{escaped_table}")
}

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
    store: S,
    /// Cache of table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
}

impl<S> DuckLakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new DuckLake destination.
    ///
    /// - `catalog_url`: PostgreSQL connection string used as the DuckLake
    ///   catalog (e.g. `postgres://user:pass@localhost:5432/mydb`).
    /// - `data_path`: Where Parquet files are stored. Can be a local directory
    ///   (`./lake_data/`) or a cloud URI (`s3://bucket/prefix/`,
    ///   `gs://bucket/prefix/`, `az://container/prefix/`).
    /// - `pool_size`: Number of concurrent DuckDB connections. `4` is a
    ///   reasonable default; higher values allow more tables to be written in
    ///   parallel.
    /// - `s3`: Optional S3 credentials. Required when `data_path` is an S3 URI
    ///   and the bucket is not publicly accessible.
    /// - `metadata_schema`: Optional Postgres schema for DuckLake metadata tables
    ///   (e.g. `"ducklake"`). Uses the catalog default schema when not set.
    pub fn new(
        catalog_url: impl Into<String>,
        data_path: impl Into<String>,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        store: S,
    ) -> EtlResult<Self> {
        let catalog_url = catalog_url.into();
        let data_path = data_path.into();

        let setup_sql = build_setup_sql(&catalog_url, &data_path, s3.as_ref(), metadata_schema.as_deref());
        let manager = DuckLakeConnectionManager {
            setup_sql: Arc::new(setup_sql),
        };

        let pool = Pool::builder().max_size(pool_size).build(manager).map_err(|e| {
            etl_error!(
                ErrorKind::DestinationConnectionFailed,
                "Failed to build DuckLake connection pool",
                source: e
            )
        })?;

        Ok(Self {
            pool,
            store,
            created_tables: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Deletes all rows from the destination table without dropping it.
    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || -> EtlResult<()> {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to get DuckLake connection from pool",
                    source: e
                )
            })?;
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
        .map_err(|_| etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake blocking task panicked"))?
    }

    /// Bulk-inserts rows into the destination table inside a single transaction.
    ///
    /// Wrapping all inserts in one `BEGIN` / `COMMIT` ensures they are written
    /// as a single Parquet snapshot rather than one file per row.
    ///
    /// If the COMMIT is rejected by DuckLake due to a write-write conflict
    /// (which happens when parallel copy partitions commit simultaneously),
    /// the transaction is rolled back and retried with exponential backoff.
    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(table_id).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        // Convert to Values before moving into spawn_blocking so the same data
        // can be reused across retry attempts without needing Clone on Cell.
        let all_values: Vec<Vec<Value>> = table_rows
            .into_iter()
            .map(|row| row.into_values().into_iter().map(cell_to_value).collect())
            .collect();
        let pool = self.pool.clone();

        tokio::task::spawn_blocking(move || -> EtlResult<()> {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to get DuckLake connection from pool",
                    source: e
                )
            })?;
            insert_rows_with_retry(&conn, &table_name, &all_values)
        })
        .await
        .map_err(|_| etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake blocking task panicked"))?
    }

    /// Writes streaming CDC events to the destination.
    ///
    /// Insert, Update, and Delete events are grouped by table and written in
    /// parallel, each table in its own `spawn_blocking` task with its own pool
    /// connection and transaction. Truncate events are deduplicated and
    /// processed after the per-table writes.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_rows: HashMap<TableId, Vec<Vec<Value>>> = HashMap::new();

            // Accumulate non-truncate events, stopping at the first Truncate.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(insert) => {
                        let values =
                            insert.table_row.into_values().into_iter().map(cell_to_value).collect();
                        table_id_to_rows.entry(insert.table_id).or_default().push(values);
                    }
                    Event::Update(update) => {
                        let values =
                            update.table_row.into_values().into_iter().map(cell_to_value).collect();
                        table_id_to_rows.entry(update.table_id).or_default().push(values);
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            info!("delete event has no old row, skipping");
                            continue;
                        };
                        let values =
                            old_row.into_values().into_iter().map(cell_to_value).collect();
                        table_id_to_rows.entry(delete.table_id).or_default().push(values);
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            // One spawn_blocking task per table — different tables use separate
            // pool connections and commit independent Parquet snapshots.
            if !table_id_to_rows.is_empty() {
                let mut join_set = tokio::task::JoinSet::new();

                for (table_id, rows) in table_id_to_rows {
                    let table_name = self.ensure_table_exists(table_id).await?;
                    let pool = self.pool.clone();

                    join_set.spawn(tokio::task::spawn_blocking(move || -> EtlResult<()> {
                        let conn = pool.get().map_err(|e| {
                            etl_error!(
                                ErrorKind::DestinationConnectionFailed,
                                "Failed to get DuckLake connection from pool",
                                source: e
                            )
                        })?;
                        insert_rows_with_retry(&conn, &table_name, &rows)
                    }));
                }

                while let Some(result) = join_set.join_next().await {
                    result
                        .map_err(|_| {
                            etl_error!(
                                ErrorKind::ApplyWorkerPanic,
                                "DuckLake blocking task panicked"
                            )
                        })?
                        .map_err(|_| {
                            etl_error!(
                                ErrorKind::ApplyWorkerPanic,
                                "DuckLake blocking task panicked"
                            )
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
            let cache = self.created_tables.lock().unwrap();
            if cache.contains(&table_name) {
                return Ok(table_name);
            }
        }

        // `build_create_table_sql_ducklake` generates `CREATE TABLE IF NOT EXISTS "name" (...)`.
        // Prefix the table name with the catalog alias so DuckLake knows which
        // catalog to create the table in.
        let ddl = build_create_table_sql_ducklake(&table_name, &table_schema.column_schemas);
        let qualified_ddl = ddl.replace(
            &format!("\"{}\"", table_name),
            &format!("{LAKE_CATALOG}.\"{table_name}\""),
        );

        let pool = self.pool.clone();
        let created_tables = Arc::clone(&self.created_tables);
        let table_name_clone = table_name.clone();

        tokio::task::spawn_blocking(move || -> EtlResult<()> {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to get DuckLake connection from pool",
                    source: e
                )
            })?;
            conn.execute_batch(&qualified_ddl).map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    source: e
                )
            })?;
            created_tables.lock().unwrap().insert(table_name_clone);
            Ok(())
        })
        .await
        .map_err(|_| etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake blocking task panicked"))??;

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

// ── helpers ───────────────────────────────────────────────────────────────────

/// Maximum number of times a conflicting COMMIT is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 10;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 2_000;

/// Calls [`insert_rows`] and retries with exponential backoff on failure.
///
/// DuckLake uses optimistic concurrency: when several transactions (e.g.
/// parallel table-copy partitions) try to COMMIT at the same time, one
/// succeeds and the rest receive a write-write conflict error. The safe
/// recovery is to roll back and retry the entire `BEGIN → INSERT → COMMIT`
/// sequence with a short random-jitter delay, which is exactly what this
/// function does.
fn insert_rows_with_retry(
    conn: &duckdb::Connection,
    table_name: &str,
    all_values: &[Vec<Value>],
) -> EtlResult<()> {
    let mut delay = std::time::Duration::from_millis(INITIAL_RETRY_DELAY_MS);

    for attempt in 0..=MAX_COMMIT_RETRIES {
        match insert_rows(conn, table_name, all_values) {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_COMMIT_RETRIES => {
                warn!(
                    attempt = attempt + 1,
                    max = MAX_COMMIT_RETRIES,
                    table = table_name,
                    error = %e,
                    "DuckLake commit conflict, retrying"
                );
                std::thread::sleep(delay);
                delay = std::cmp::min(delay * 2, std::time::Duration::from_millis(MAX_RETRY_DELAY_MS));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
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
        "CREATE OR REPLACE TEMP TABLE \"{staging}\" AS \
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
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS \"{staging}\""));
        return Err(e);
    }

    // One INSERT SELECT = one Parquet file in DuckLake.
    conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS \"{staging}\""));
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: e
        )
    })?;

    let insert_result = conn
        .execute_batch(&format!(
            "INSERT INTO {LAKE_CATALOG}.\"{table_name}\" SELECT * FROM \"{staging}\";"
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
    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS \"{staging}\""));

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
        Cell::Date(d) => {
            Value::Date32(d.signed_duration_since(epoch_date).num_days() as i32)
        }
        Cell::Time(t) => {
            let micros = t.signed_duration_since(epoch_time).num_microseconds().unwrap_or(0);
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
        ArrayCell::Bool(v) => {
            v.into_iter().map(|o| o.map(Value::Boolean).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::String(v) => {
            v.into_iter().map(|o| o.map(Value::Text).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::I16(v) => {
            v.into_iter().map(|o| o.map(Value::SmallInt).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::I32(v) => {
            v.into_iter().map(|o| o.map(Value::Int).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::U32(v) => {
            v.into_iter().map(|o| o.map(Value::UInt).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::I64(v) => {
            v.into_iter().map(|o| o.map(Value::BigInt).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::F32(v) => {
            v.into_iter().map(|o| o.map(Value::Float).unwrap_or(Value::Null)).collect()
        }
        ArrayCell::F64(v) => {
            v.into_iter().map(|o| o.map(Value::Double).unwrap_or(Value::Null)).collect()
        }
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
                o.map(|dt| {
                    Value::Timestamp(TimeUnit::Microsecond, dt.and_utc().timestamp_micros())
                })
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
        ArrayCell::Bytes(v) => {
            v.into_iter().map(|o| o.map(Value::Blob).unwrap_or(Value::Null)).collect()
        }
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
    fn test_normalize_catalog_url_local_file() {
        // Local file paths are passed through unchanged.
        assert_eq!(normalize_catalog_url("metadata.ducklake"), "metadata.ducklake");
        assert_eq!(normalize_catalog_url("/abs/path/catalog.ducklake"), "/abs/path/catalog.ducklake");
    }

    #[test]
    fn test_normalize_catalog_url_postgres_uri() {
        let result = normalize_catalog_url("postgres://bnj@localhost:5432/ducklake_catalog");
        assert_eq!(result, "postgres:host=localhost dbname=ducklake_catalog user=bnj");
    }

    #[test]
    fn test_normalize_catalog_url_postgres_uri_with_query_params() {
        let result = normalize_catalog_url(
            "postgres://user:pass@localhost:5432/mydb?sslmode=disable&connect_timeout=10",
        );
        assert_eq!(
            result,
            "postgres:host=localhost dbname=mydb user=user password='pass' sslmode=disable connect_timeout=10"
        );
    }

    #[test]
    fn test_normalize_catalog_url_postgres_uri_with_password() {
        let result = normalize_catalog_url("postgres://user:secret@db.example.com:5433/mydb");
        assert_eq!(
            result,
            "postgres:host=db.example.com port=5433 dbname=mydb user=user password='secret'"
        );
    }

    #[test]
    fn test_normalize_catalog_url_already_libpq() {
        // Already in libpq format — returned unchanged.
        let libpq = "postgres:host=localhost dbname=mydb user=bnj";
        assert_eq!(normalize_catalog_url(libpq), libpq);
    }

    #[test]
    fn test_build_setup_sql_local() {
        let sql = build_setup_sql("metadata.ducklake", "./data/", None, None);
        assert!(sql.contains("INSTALL ducklake"));
        assert!(sql.contains("LOAD ducklake"));
        assert!(!sql.contains("postgres"));
        assert!(!sql.contains("httpfs"));
        assert!(sql.contains("ducklake:metadata.ducklake"));
        assert!(sql.contains("DATA_PATH './data/'"));
    }

    #[test]
    fn test_build_setup_sql_postgres_local_data() {
        let sql = build_setup_sql("postgres://bnj@localhost:5432/ducklake_catalog", "./data/", None, None);
        assert!(sql.contains("INSTALL postgres"));
        assert!(sql.contains("LOAD postgres"));
        assert!(!sql.contains("httpfs"));
        // URL must be converted to libpq format, not passed as-is.
        assert!(!sql.contains("postgres://"));
        assert!(sql.contains("ducklake:postgres:host=localhost"));
        assert!(sql.contains("DATA_PATH './data/'"));
    }

    #[test]
    fn test_build_setup_sql_postgres_s3_data() {
        let sql = build_setup_sql("postgres://user:pass@host/db", "s3://my-bucket/lake/", None, None);
        assert!(sql.contains("INSTALL postgres"));
        assert!(sql.contains("INSTALL httpfs"));
        assert!(sql.contains("LOAD httpfs"));
        assert!(!sql.contains("postgres://"));
        assert!(sql.contains("DATA_PATH 's3://my-bucket/lake/'"));
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
        assert_eq!(cell_to_value(Cell::F64(3.14)), Value::Double(3.14));
    }
}
