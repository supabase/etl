use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use clickhouse::Client;
use etl::{
    destination::TableCopyBatchId,
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use futures::TryFutureExt;
use tracing::debug;
use url::Url;

use crate::clickhouse::{
    core::{ClickHouseClientConfig, ClickHouseOperationKind},
    encoding::{ClickHouseValue, ColumnEncoding, encode_to_row_binary, rb_varint},
    metrics::{
        ETL_CLICKHOUSE_CONNECTIVITY_CHECK_DURATION_SECONDS, ETL_CLICKHOUSE_DDL_DURATION_SECONDS,
        ETL_CLICKHOUSE_DDL_ERRORS_TOTAL, ETL_CLICKHOUSE_INSERT_BYTES,
        ETL_CLICKHOUSE_INSERT_DURATION_SECONDS, ETL_CLICKHOUSE_INSERT_ENCODING_ERRORS_TOTAL,
        ETL_CLICKHOUSE_INSERT_ERRORS_TOTAL, ETL_CLICKHOUSE_INSERT_ROWS,
        ETL_CLICKHOUSE_SCHEMA_QUERY_DURATION_SECONDS, ETL_CLICKHOUSE_STATEMENTS_PER_BATCH,
        REPLICATION_PATH_LABEL,
    },
    network::new_public_client,
    schema::{clickhouse_column_type, clickhouse_default_clause},
    sql::quote_identifier,
};

/// Returns the `outcome` label for an error returned by [`timeout_call`]:
/// `"timeout"` when the client-side deadline fired, `"failed"` otherwise.
fn outcome_label(err: &EtlError) -> &'static str {
    if err.kind() == ErrorKind::DestinationTimeout { "timeout" } else { "failed" }
}

/// Records the duration histogram for a DDL operation and, on failure,
/// increments the DDL error counter. Centralized so DDL paths that bypass
/// [`ClickHouseClient::execute_ddl`] (notably `truncate_table`, which carries
/// extra error context) record the same metric shape.
fn record_ddl_metrics(kind: DdlKind, start: Instant, err: Option<&EtlError>) {
    metrics::histogram!(
        ETL_CLICKHOUSE_DDL_DURATION_SECONDS,
        "kind" => kind.as_label(),
    )
    .record(start.elapsed().as_secs_f64());
    if let Some(err) = err {
        metrics::counter!(
            ETL_CLICKHOUSE_DDL_ERRORS_TOTAL,
            "kind" => kind.as_label(),
            "outcome" => outcome_label(err),
        )
        .increment(1);
    }
}

/// Formats a `Duration` as a whole-seconds string for ClickHouse
/// `.with_option(...)` settings, floored at `"1"`. ClickHouse interprets `"0"`
/// as "no timeout" for `http_*_timeout`, `max_execution_time`, and
/// `lock_acquire_timeout`; the floor prevents `Duration::ZERO` or any
/// sub-second value from accidentally disabling the bound.
fn floor_secs(d: Duration) -> String {
    d.as_secs().max(1).to_string()
}

/// Runs `fut` under `tokio::time::timeout` using the client-side timeout for
/// `op` from `config`. Inner ClickHouse errors map onto `op.failed_kind()`,
/// which depends on whether the error [`is_retryable`](ClickHouseErrorExt);
/// client-side deadlines map onto [`ErrorKind::DestinationTimeout`].
/// `context`, when present, is appended to the error detail (e.g.
/// `"table: foo"`) so call-site-specific diagnostic info is preserved.
async fn timeout_call<T, F>(
    op: ClickHouseOperationKind,
    config: &ClickHouseClientConfig,
    context: Option<&str>,
    fut: F,
) -> EtlResult<T>
where
    F: Future<Output = Result<T, clickhouse::error::Error>>,
{
    fn detail(op: ClickHouseOperationKind, what: &str, context: Option<&str>) -> String {
        match context {
            Some(c) => format!("{op} {what}; {c}"),
            None => format!("{op} {what}"),
        }
    }

    let client_timeout = config.client_timeout_for(op);
    match tokio::time::timeout(client_timeout, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(etl_error!(
            op.failed_kind(err.is_retryable()),
            "ClickHouse call failed",
            detail(op, "failed", context),
            source: err
        )),
        Err(_) => Err(etl_error!(
            ErrorKind::DestinationTimeout,
            "ClickHouse call timed out",
            detail(op, &format!("timed out after {client_timeout:?}"), context)
        )),
    }
}

/// ClickHouse error codes for an insert whose column list or header no longer
/// matches the table: a listed column is missing (`NO_SUCH_COLUMN_IN_TABLE`)
/// or a header type differs from the column (`INCORRECT_DATA`).
const INSERT_LAYOUT_REJECTION_CODES: [u32; 2] = [16, 117];

/// Returns whether ClickHouse rejected an insert because its column list or
/// header no longer matches the table.
///
/// Network errors and timeouts return `false`: they say nothing about the
/// table's columns.
fn is_insert_layout_rejection(error: &clickhouse::error::Error) -> bool {
    let clickhouse::error::Error::BadResponse(message) = error else {
        return false;
    };
    clickhouse_error_code(message).is_some_and(|code| INSERT_LAYOUT_REJECTION_CODES.contains(&code))
}

/// Parses the code from a ClickHouse server message.
///
/// The server body reads `Code: 117. DB::Exception: ...`. When the body cannot
/// be read, the `clickhouse` crate falls back to the bare `Code: 117` from the
/// `X-ClickHouse-Exception-Code` header. The digits must end the message or be
/// followed by `.`.
fn clickhouse_error_code(message: &str) -> Option<u32> {
    let rest = message.trim_start().strip_prefix("Code: ")?;
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    let (digits, tail) = rest.split_at(end);
    if !tail.is_empty() && !tail.starts_with('.') {
        return None;
    }
    digits.parse().ok()
}

/// Classifies a failed ClickHouse call.
pub(crate) trait ClickHouseErrorExt {
    /// Returns whether the same call may succeed if it is sent again later.
    ///
    /// True for lost connections and for server errors that mean "busy" or
    /// "temporarily unavailable". False for errors that would repeat, such as
    /// a rejected statement or a local encoding failure.
    fn is_retryable(&self) -> bool;
}

impl ClickHouseErrorExt for clickhouse::error::Error {
    fn is_retryable(&self) -> bool {
        use clickhouse::error::Error;

        match self {
            // The public-address guard rejects private DNS answers at connect
            // time with `PermissionDenied`. That repeats on every attempt.
            Error::Network(source) => !is_permission_denied(source.as_ref()),
            Error::TimedOut => true,
            Error::BadResponse(message) => is_retryable_server_response(message),
            // The other variants are local request, encode, or decode errors.
            // Sending the same request again gives the same error.
            _ => false,
        }
    }
}

/// Returns whether an error chain contains a `PermissionDenied` I/O error.
///
/// Walks `source()` links and also the error inside each `io::Error`, because
/// `io::Error::source()` skips the error it wraps.
fn is_permission_denied(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
            if io_error.kind() == std::io::ErrorKind::PermissionDenied {
                return true;
            }
            if let Some(inner) = io_error.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        current = error.source();
    }
    false
}

/// Returns whether a ClickHouse error response is temporary.
fn is_retryable_server_response(message: &str) -> bool {
    match clickhouse_error_code(message) {
        // TIMEOUT_EXCEEDED, TOO_MANY_SIMULTANEOUS_QUERIES, SOCKET_TIMEOUT,
        // NETWORK_ERROR, MEMORY_LIMIT_EXCEEDED, TABLE_IS_READ_ONLY (replica
        // lost Keeper), TOO_MANY_PARTS, CANNOT_SCHEDULE_TASK, KEEPER_EXCEPTION.
        Some(159 | 202 | 209 | 210 | 241 | 242 | 252 | 439 | 999) => true,
        Some(_) => false,
        // No ClickHouse code: the crate reports the bare HTTP status, for
        // example from a proxy. Retry server-side (5xx) statuses only.
        None => http_status(message).is_some_and(|status| (500..600).contains(&status)),
    }
}

/// Parses a leading three-digit HTTP status, such as `503 Service Unavailable`.
fn http_status(message: &str) -> Option<u16> {
    let status = message.get(..3)?;
    let rest = message.get(3..)?;
    if !status.bytes().all(|byte| byte.is_ascii_digit())
        || !(rest.is_empty() || rest.starts_with(' '))
    {
        return None;
    }
    status.parse().ok()
}

/// Failure of a [`ClickHouseClient::insert_rows`] call.
#[derive(Debug)]
pub(crate) struct InsertRowsError {
    /// Error reported to the caller.
    error: EtlError,
    /// Whether ClickHouse rejected the insert because its column list or
    /// header no longer matches the table.
    layout_rejected: bool,
}

impl InsertRowsError {
    /// Returns whether the insert's layout no longer matches the table, so the
    /// cached layout must be reloaded.
    pub(crate) fn is_layout_rejection(&self) -> bool {
        self.layout_rejected
    }
}

impl From<InsertRowsError> for EtlError {
    fn from(failure: InsertRowsError) -> Self {
        failure.error
    }
}

impl From<EtlError> for InsertRowsError {
    fn from(error: EtlError) -> Self {
        Self { error, layout_rejected: false }
    }
}

/// Capacity of the internal write buffer used per INSERT statement.
///
/// When this many bytes have been written to the buffer it is flushed to the
/// network (but the INSERT statement itself is not closed — that only happens
/// when `end()` is called or the `max_bytes_per_insert` limit is reached).
const BUFFERED_CAPACITY: usize = 256 * 1024;

/// A ClickHouse table column returned from `system.columns`.
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, serde::Deserialize)]
pub(crate) struct ClickHouseTableColumn {
    /// Column name.
    pub(crate) name: String,
    /// ClickHouse type string, for example `Int32` or `Nullable(String)`.
    pub(crate) type_name: String,
}

/// Column layout sent with every `RowBinaryWithNamesAndTypes` insert into one
/// table.
///
/// The explicit column list makes ClickHouse reject unknown column names, and
/// the header makes it reject any column whose type differs from the table.
/// A stale or externally altered table therefore fails the insert instead of
/// reinterpreting positional RowBinary bytes.
#[derive(Debug)]
pub(crate) struct RowBinaryLayout {
    /// Quoted, comma-separated column list for the `INSERT` statement.
    column_list: String,
    /// Encoded header: column count, column names, then column types.
    header: Vec<u8>,
    /// Per-column RowBinary encodings, including the trailing CDC columns.
    column_encodings: Box<[ColumnEncoding]>,
}

impl RowBinaryLayout {
    /// Builds the layout for `columns` in table position order.
    pub(crate) fn new(columns: &[ClickHouseTableColumn]) -> Self {
        let column_list =
            columns.iter().map(|column| quote_identifier(&column.name)).collect::<Vec<_>>();
        let mut header = Vec::new();
        rb_varint(columns.len(), &mut header);
        for value in columns
            .iter()
            .map(|column| &column.name)
            .chain(columns.iter().map(|column| &column.type_name))
        {
            rb_varint(value.len(), &mut header);
            header.extend_from_slice(value.as_bytes());
        }

        Self {
            column_list: column_list.join(", "),
            header,
            column_encodings: columns
                .iter()
                .map(|column| ColumnEncoding::from_type_name(&column.type_name))
                .collect(),
        }
    }

    /// Returns the per-column RowBinary encodings.
    #[cfg(test)]
    pub(crate) fn column_encodings(&self) -> &[ColumnEncoding] {
        &self.column_encodings
    }
}

/// Returns the placement clause for an `ADD COLUMN` statement.
///
/// `None` means the destination table has no user columns to anchor on, so the
/// new column goes at the front via `FIRST` (which still places it before the
/// trailing CDC columns).
fn add_column_placement_clause(after_column: Option<&str>) -> String {
    match after_column {
        Some(anchor) => format!("AFTER {}", quote_identifier(anchor)),
        None => "FIRST".to_owned(),
    }
}

/// Builds the SQL used to add a column to a ClickHouse table.
fn build_add_column_sql(
    table_name: &str,
    column: &etl::schema::ColumnSchema,
    after_column: Option<&str>,
    force_nullable: bool,
) -> String {
    let col_type = clickhouse_column_type(column, force_nullable);
    let default_clause = clickhouse_default_clause(column).unwrap_or_default();
    let table_name = quote_identifier(table_name);
    let column_name = quote_identifier(&column.name);
    let placement = add_column_placement_clause(after_column);

    format!(
        "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} \
         {col_type}{default_clause} {placement}"
    )
}

/// Builds the SQL used to drop a column from a ClickHouse table.
fn build_drop_column_sql(table_name: &str, column_name: &str) -> String {
    let table_name = quote_identifier(table_name);
    let column_name = quote_identifier(column_name);
    format!("ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column_name}")
}

/// Builds the SQL used to rename a column in a ClickHouse table.
fn build_rename_column_sql(table_name: &str, old_name: &str, new_name: &str) -> String {
    let table_name = quote_identifier(table_name);
    let old_name = quote_identifier(old_name);
    let new_name = quote_identifier(new_name);
    format!("ALTER TABLE {table_name} RENAME COLUMN IF EXISTS {old_name} TO {new_name}")
}

/// Builds the SQL used to relax a scalar column to `Nullable`.
///
/// Returns `None` when there is nothing to do: the column is already
/// `Nullable(...)`, or it is an `Array(...)`. ClickHouse cannot wrap an array
/// in `Nullable`, and ETL already writes a NULL source array as an empty array.
fn build_drop_not_null_sql(
    table_name: &str,
    column_name: &str,
    physical_type: &str,
) -> Option<String> {
    if physical_type.starts_with("Nullable(") || physical_type.starts_with("Array(") {
        return None;
    }

    let table_name = quote_identifier(table_name);
    let column_name = quote_identifier(column_name);
    Some(format!(
        "ALTER TABLE {table_name} MODIFY COLUMN {column_name} Nullable({physical_type}) SETTINGS \
         mutations_sync = 2"
    ))
}

/// Builds the SQL used to truncate a ClickHouse table.
fn build_truncate_table_sql(table_name: &str) -> String {
    let table_name = quote_identifier(table_name);
    format!("TRUNCATE TABLE IF EXISTS {table_name}")
}

/// Builds the SQL used to drop a ClickHouse table.
fn build_drop_table_sql(table_name: &str) -> String {
    let table_name = quote_identifier(table_name);
    format!("DROP TABLE IF EXISTS {table_name}")
}

/// Builds the SQL used to insert `RowBinaryWithNamesAndTypes` rows into a
/// ClickHouse table.
fn build_insert_rows_sql(table_name: &str, layout: &RowBinaryLayout) -> String {
    let table_name = quote_identifier(table_name);
    format!("INSERT INTO {table_name} ({}) FORMAT RowBinaryWithNamesAndTypes", layout.column_list)
}

/// Kind of DDL being executed; surfaces as a `kind` label on the
/// `etl_clickhouse_ddl_duration_seconds` histogram so per-operation latencies
/// can be distinguished (one-shot CREATE vs. online ALTER, etc.).
#[derive(Copy, Clone)]
pub(crate) enum DdlKind {
    CreateTable,
    CreateView,
    DropTable,
    DropView,
    TruncateTable,
    AddColumn,
    DropColumn,
    RenameColumn,
    ModifyColumn,
}

impl DdlKind {
    fn as_label(self) -> &'static str {
        match self {
            DdlKind::CreateTable => "create_table",
            DdlKind::CreateView => "create_view",
            DdlKind::DropTable => "drop_table",
            DdlKind::DropView => "drop_view",
            DdlKind::TruncateTable => "truncate_table",
            DdlKind::AddColumn => "add_column",
            DdlKind::DropColumn => "drop_column",
            DdlKind::RenameColumn => "rename_column",
            DdlKind::ModifyColumn => "modify_column",
        }
    }
}

/// High-level ClickHouse client used by [`super::core::ClickHouseDestination`].
///
/// Wraps a [`clickhouse::Client`] and exposes typed methods for DDL,
/// truncation, and RowBinary bulk inserts.
#[derive(Clone)]
pub struct ClickHouseClient {
    inner: Arc<Client>,
    config: ClickHouseClientConfig,
}

impl ClickHouseClient {
    /// Creates a new [`ClickHouseClient`].
    ///
    /// When `url` starts with `https://`, TLS is handled automatically by the
    /// `rustls-tls` feature using webpki root certificates.
    pub fn new(
        url: Url,
        user: impl Into<String>,
        password: Option<String>,
        database: impl Into<String>,
        config: ClickHouseClientConfig,
    ) -> Self {
        Self::build(Client::default(), url, user, Some(database.into()), password, config)
    }

    /// Creates a client that requires HTTPS and connects only to public IP
    /// addresses.
    pub async fn new_public(
        url: Url,
        user: impl Into<String>,
        password: Option<String>,
        database: impl Into<String>,
        config: ClickHouseClientConfig,
    ) -> EtlResult<Self> {
        let client = new_public_client(&url, config.connectivity_check_timeout).await?;
        Ok(Self::build(client, url, user, Some(database.into()), password, config))
    }

    /// Variant of [`Self::new`] that does not pin the client to a target
    /// database. The server falls back to the user's profile default database
    /// for every query, which lets the validator probe connectivity / auth /
    /// database existence before the user-supplied database is known to exist.
    pub fn new_without_database(
        url: Url,
        user: impl Into<String>,
        password: Option<String>,
        config: ClickHouseClientConfig,
    ) -> Self {
        Self::build(Client::default(), url, user, None, password, config)
    }

    /// Creates an unscoped client that connects only to public HTTPS addresses.
    pub async fn new_public_without_database(
        url: Url,
        user: impl Into<String>,
        password: Option<String>,
        config: ClickHouseClientConfig,
    ) -> EtlResult<Self> {
        let client = new_public_client(&url, config.connectivity_check_timeout).await?;
        Ok(Self::build(client, url, user, None, password, config))
    }

    fn build(
        client: Client,
        url: Url,
        user: impl Into<String>,
        database: Option<String>,
        password: Option<String>,
        config: ClickHouseClientConfig,
    ) -> Self {
        Self {
            inner: Arc::new({
                let client = client.with_url(url.to_string()).with_user(user);
                let client = match database {
                    Some(database) => client.with_database(database),
                    None => client,
                };
                let client = match password {
                    Some(password) => client.with_password(password),
                    None => client,
                };
                client
                    .with_option("connect_timeout", floor_secs(config.connectivity_check_timeout))
                    .with_option(
                        "http_connection_timeout",
                        floor_secs(config.connectivity_check_timeout),
                    )
                    .with_option("http_send_timeout", floor_secs(config.insert_timeout))
                    .with_option("http_receive_timeout", floor_secs(config.insert_timeout))
                    // Pin synchronous inserts. Since ClickHouse 26.2 the server queues inserts by
                    // default. When the wait for a queued flush times out, the server reports an
                    // error but keeps the rows queued, so they can land after ETL replays the
                    // batch, even after a replayed `TRUNCATE`. A synchronous
                    // insert writes its rows before it reports success, and a
                    // server-side failure leaves nothing queued.
                    .with_option("async_insert", "0")
            }),
            config,
        }
    }

    /// Verifies that the ClickHouse server is reachable.
    ///
    /// Issues a `SELECT 1` round-trip; cheaper than any DDL or metadata query
    /// and exercises the auth/transport path. Mirrors the Iceberg destination's
    /// `validate_connectivity` so destination validators
    /// can treat the two destinations uniformly.
    pub async fn validate_connectivity(&self) -> EtlResult<()> {
        let query = self
            .inner
            .query("SELECT 1")
            .with_option("max_execution_time", floor_secs(self.config.connectivity_check_timeout));
        let start = Instant::now();
        let result = timeout_call(
            ClickHouseOperationKind::ConnectivityCheck,
            &self.config,
            None,
            query.fetch_one::<u8>(),
        )
        .await;
        metrics::histogram!(ETL_CLICKHOUSE_CONNECTIVITY_CHECK_DURATION_SECONDS)
            .record(start.elapsed().as_secs_f64());
        result?;
        Ok(())
    }

    /// Returns whether `database` exists on the ClickHouse server.
    ///
    /// Queries `system.databases` directly so the result is independent of the
    /// client's configured database. Pair with [`Self::new_without_database`]
    /// to probe existence of a database whose presence is unknown.
    pub async fn database_exists(&self, database: &str) -> EtlResult<bool> {
        let query = self
            .inner
            .query("SELECT count() FROM system.databases WHERE name = ?")
            .bind(database)
            .with_option("max_execution_time", floor_secs(self.config.connectivity_check_timeout));
        let start = Instant::now();
        let result = timeout_call(
            ClickHouseOperationKind::ConnectivityCheck,
            &self.config,
            Some(&format!("database: {database}")),
            query.fetch_one::<u64>(),
        )
        .await;
        metrics::histogram!(ETL_CLICKHOUSE_CONNECTIVITY_CHECK_DURATION_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(result? > 0)
    }

    /// Returns the major/minor version pair from `SELECT version()`.
    ///
    /// Trailing components (patch, build) are ignored. Used at destination
    /// construction to gate engine-specific feature requirements (e.g.
    /// ReplacingMergeTree needs >= 23.5).
    pub(crate) async fn server_version(&self) -> EtlResult<(u32, u32)> {
        let query = self
            .inner
            .query("SELECT version()")
            .with_option("max_execution_time", floor_secs(self.config.connectivity_check_timeout));
        let raw = timeout_call(
            ClickHouseOperationKind::ConnectivityCheck,
            &self.config,
            Some("server version"),
            query.fetch_one::<String>(),
        )
        .await?;

        let mut parts = raw.split('.');
        let major = parts.next().and_then(|s| s.parse::<u32>().ok());
        let minor = parts.next().and_then(|s| s.parse::<u32>().ok());

        match (major, minor) {
            (Some(major), Some(minor)) => Ok((major, minor)),
            _ => Err(etl_error!(
                ErrorKind::Unknown,
                "Unable to parse ClickHouse server version",
                format!("server returned '{raw}'")
            )),
        }
    }

    /// Executes a DDL statement (e.g. `CREATE TABLE IF NOT EXISTS …`) and
    /// records its duration in the `etl_clickhouse_ddl_duration_seconds`
    /// histogram labelled with the DDL `kind` and `table_name`.
    pub(crate) async fn execute_ddl(&self, kind: DdlKind, sql: &str) -> EtlResult<()> {
        let ddl_start = Instant::now();
        let ddl_secs = floor_secs(self.config.ddl_timeout);
        let query = self
            .inner
            .query(sql)
            .with_option("max_execution_time", &ddl_secs)
            .with_option("lock_acquire_timeout", &ddl_secs);
        let result =
            timeout_call(ClickHouseOperationKind::Ddl, &self.config, None, query.execute()).await;
        record_ddl_metrics(kind, ddl_start, result.as_ref().err());
        result
    }

    /// Returns the ClickHouse engine name for a table, or `None` if the table
    /// does not exist in the current database.
    pub(crate) async fn table_engine(&self, table_name: &str) -> EtlResult<Option<String>> {
        let query = self
            .inner
            .query(
                "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = ?",
            )
            .with_option("max_execution_time", floor_secs(self.config.schema_query_timeout))
            .bind(table_name);
        let rows = timeout_call(
            ClickHouseOperationKind::SchemaQuery,
            &self.config,
            Some(&format!("table: {table_name}")),
            query.fetch_all::<String>(),
        )
        .await?;

        Ok(rows.into_iter().next())
    }

    /// Returns the insertable ClickHouse columns for a table in position order.
    ///
    /// `MATERIALIZED`, `ALIAS`, and `EPHEMERAL` columns are excluded because an
    /// insert without a column list skips them. Users may add them to derive
    /// values from ETL columns, and RowBinary inserts never carry them.
    pub(crate) async fn table_columns(
        &self,
        table_name: &str,
    ) -> EtlResult<Vec<ClickHouseTableColumn>> {
        let schema_secs = floor_secs(self.config.schema_query_timeout);
        let query = self
            .inner
            .query(
                "SELECT name, type AS type_name FROM system.columns WHERE database = \
                 currentDatabase() AND table = ? AND default_kind IN ('', 'DEFAULT') ORDER BY \
                 position",
            )
            .with_option("max_execution_time", &schema_secs)
            .bind(table_name);
        let start = Instant::now();
        let result = timeout_call(
            ClickHouseOperationKind::SchemaQuery,
            &self.config,
            Some(&format!("table: {table_name}")),
            query.fetch_all::<ClickHouseTableColumn>(),
        )
        .await;
        metrics::histogram!(ETL_CLICKHOUSE_SCHEMA_QUERY_DURATION_SECONDS)
            .record(start.elapsed().as_secs_f64());
        result
    }

    /// Adds a column to an existing ClickHouse table.
    ///
    /// `after_column` controls placement: `Some(name)` inserts the new column
    /// immediately AFTER `name`, `None` inserts it FIRST (used when the table
    /// has no user columns yet). Either placement keeps it before the trailing
    /// CDC columns, which RowBinary encoding requires.
    pub(crate) async fn add_column(
        &self,
        table_name: &str,
        column: &etl::schema::ColumnSchema,
        after_column: Option<&str>,
        force_nullable: bool,
    ) -> EtlResult<()> {
        let sql = build_add_column_sql(table_name, column, after_column, force_nullable);
        self.execute_ddl(DdlKind::AddColumn, &sql).await
    }

    /// Drops a column from an existing ClickHouse table (idempotent).
    pub(crate) async fn drop_column(&self, table_name: &str, column_name: &str) -> EtlResult<()> {
        let sql = build_drop_column_sql(table_name, column_name);
        self.execute_ddl(DdlKind::DropColumn, &sql).await
    }

    /// Renames a column in an existing ClickHouse table (idempotent).
    ///
    /// `RENAME COLUMN IF EXISTS` makes the ALTER a server-side noop when the
    /// old column is already absent, so the check and the rename happen in one
    /// statement without a racy read-then-write.
    pub(crate) async fn rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> EtlResult<()> {
        let sql = build_rename_column_sql(table_name, old_name, new_name);
        self.execute_ddl(DdlKind::RenameColumn, &sql).await
    }

    /// Relaxes an existing scalar column to nullable when needed.
    pub(crate) async fn drop_column_not_null(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> EtlResult<()> {
        let columns = self.table_columns(table_name).await?;
        let column = columns.iter().find(|column| column.name == column_name).ok_or_else(|| {
            etl_error!(
                ErrorKind::CorruptedTableSchema,
                "ClickHouse destination column for nullability change is missing",
                format!("Table '{table_name}', column '{column_name}'")
            )
        })?;
        let Some(sql) = build_drop_not_null_sql(table_name, column_name, &column.type_name) else {
            debug!(table_name, column_name, "clickhouse column needs no nullability change");
            return Ok(());
        };

        self.execute_ddl(DdlKind::ModifyColumn, &sql).await
    }

    /// Executes `TRUNCATE TABLE IF EXISTS` for the supplied table.
    pub(crate) async fn truncate_table(&self, table_name: &str) -> EtlResult<()> {
        let ddl_start = Instant::now();
        let ddl_secs = floor_secs(self.config.ddl_timeout);
        let query = self
            .inner
            .query(&build_truncate_table_sql(table_name))
            .with_option("max_execution_time", &ddl_secs)
            .with_option("lock_acquire_timeout", &ddl_secs);
        let result = timeout_call(
            ClickHouseOperationKind::Ddl,
            &self.config,
            Some(&format!("table: {table_name}")),
            query.execute(),
        )
        .await;
        record_ddl_metrics(DdlKind::TruncateTable, ddl_start, result.as_ref().err());
        result
    }

    /// Executes `DROP TABLE IF EXISTS` for the supplied table.
    pub(crate) async fn drop_table(&self, table_name: &str) -> EtlResult<()> {
        let sql = build_drop_table_sql(table_name);
        self.execute_ddl(DdlKind::DropTable, &sql).await
    }

    /// Inserts `rows` into `table_name` using the `RowBinaryWithNamesAndTypes`
    /// format.
    ///
    /// Each element of `rows` is a complete, already-encoded row of
    /// [`ClickHouseValue`]s in `layout` column order (user columns + CDC
    /// columns). Every INSERT statement starts with the layout header, so
    /// ClickHouse checks column names and types before reading any row. The
    /// error reports whether ClickHouse rejected that layout.
    ///
    /// When the accumulated uncompressed byte count reaches
    /// `max_bytes_per_insert` the current INSERT statement is committed and a
    /// new one is opened, keeping peak memory usage bounded for large initial
    /// copies.
    ///
    /// With `copy_batch_id`, statement `n` sends
    /// `insert_deduplication_token = "<batch id>-<n>"`. Without a token,
    /// ClickHouse compares block contents, so distinct copy batches with
    /// identical rows look like retries and all but one are dropped. A
    /// redelivered batch splits into the same statements and keeps its tokens,
    /// so it is still dropped as a retry. Without `copy_batch_id`, statements
    /// send no token and ClickHouse deduplicates by content, which only drops
    /// exact replays of change-stream blocks.
    ///
    /// The `replication_path` label (`"copy"` or `"cdc"`) is attached to the
    /// `etl_clickhouse_insert_duration_seconds` histogram recorded after each
    /// committed INSERT statement.
    pub(crate) async fn insert_rows(
        &self,
        table_name: &str,
        rows: Vec<Vec<ClickHouseValue>>,
        layout: &RowBinaryLayout,
        max_bytes_per_insert: u64,
        copy_batch_id: Option<TableCopyBatchId>,
        replication_path: &'static str,
    ) -> Result<(), InsertRowsError> {
        let sql = build_insert_rows_sql(table_name, layout);
        let mut rows = rows.into_iter().peekable();
        let mut row_buf = Vec::new();
        let mut statements = 0u64;

        while rows.peek().is_some() {
            #[cfg(feature = "test-utils")]
            pause_before_insert_statement_for_tests(statements).await;

            let mut insert = self
                .inner
                .insert_formatted_with(sql.clone())
                // A profile can disable the header type check, which would read
                // the row bytes as the table's types again.
                .with_option("input_format_with_types_use_header", "1");
            if let Some(batch_id) = copy_batch_id {
                insert = insert
                    .with_option("insert_deduplication_token", format!("{batch_id}-{statements}"));
            }
            let mut insert = insert.buffered_with_capacity(BUFFERED_CAPACITY);
            insert.write_buffered(&layout.header);
            // Only row bytes count toward the budget, so every statement
            // carries at least one row even when the budget is tiny.
            let mut bytes = 0u64;
            let mut rows_in_statement = 0u64;
            let insert_start = Instant::now();

            while bytes < max_bytes_per_insert {
                let Some(row) = rows.next() else { break };
                row_buf.clear();
                encode_to_row_binary(row, &layout.column_encodings, &mut row_buf).inspect_err(
                    |_| {
                        metrics::counter!(
                            ETL_CLICKHOUSE_INSERT_ENCODING_ERRORS_TOTAL,
                            REPLICATION_PATH_LABEL => replication_path,
                        )
                        .increment(1);
                    },
                )?;
                insert.write_buffered(&row_buf);
                bytes += row_buf.len() as u64;
                rows_in_statement += 1;
            }

            let mut layout_rejected = false;
            let result = timeout_call(
                ClickHouseOperationKind::Insert,
                &self.config,
                Some(&format!("table: {table_name}")),
                insert.end().inspect_err(|error| {
                    layout_rejected = is_insert_layout_rejection(error);
                }),
            )
            .await;
            match result.as_ref() {
                Ok(_) => {
                    metrics::histogram!(
                        ETL_CLICKHOUSE_INSERT_DURATION_SECONDS,
                        REPLICATION_PATH_LABEL => replication_path,
                    )
                    .record(insert_start.elapsed().as_secs_f64());
                    metrics::histogram!(
                        ETL_CLICKHOUSE_INSERT_ROWS,
                        REPLICATION_PATH_LABEL => replication_path,
                    )
                    .record(rows_in_statement as f64);
                    metrics::histogram!(
                        ETL_CLICKHOUSE_INSERT_BYTES,
                        REPLICATION_PATH_LABEL => replication_path,
                    )
                    .record(bytes as f64);
                }
                Err(err) => {
                    metrics::counter!(
                        ETL_CLICKHOUSE_INSERT_ERRORS_TOTAL,
                        REPLICATION_PATH_LABEL => replication_path,
                        "outcome" => outcome_label(err),
                    )
                    .increment(1);
                }
            }
            result.map_err(|error| InsertRowsError { error, layout_rejected })?;
            statements += 1;
        }

        if statements > 0 {
            metrics::histogram!(
                ETL_CLICKHOUSE_STATEMENTS_PER_BATCH,
                REPLICATION_PATH_LABEL => replication_path,
            )
            .record(statements as f64);
        }

        Ok(())
    }
}

/// One-shot pause armed before an INSERT statement inside
/// [`ClickHouseClient::insert_rows`].
#[cfg(feature = "test-utils")]
struct ArmedInsertStatementPause {
    /// Zero-based index of the statement to pause before.
    statement_index: u64,
    /// Signals that the paused call reached the armed statement boundary.
    reached: tokio::sync::oneshot::Sender<()>,
    /// Resumes the paused call when signalled or dropped.
    release: tokio::sync::oneshot::Receiver<()>,
}

/// Currently armed insert-statement pauses; each is consumed once.
#[cfg(feature = "test-utils")]
static INSERT_STATEMENT_PAUSES: parking_lot::Mutex<Vec<ArmedInsertStatementPause>> =
    parking_lot::Mutex::new(Vec::new());

/// Arms a one-shot pause before the zero-based `statement_index` INSERT
/// statement of a `ClickHouseClient::insert_rows` call.
///
/// Several pauses may be armed at once; each call crossing an armed
/// statement boundary consumes the earliest matching pause, so two
/// concurrent single-statement writes can both be parked by arming the same
/// index twice.
///
/// Returns the `reached` receiver, signalled at the armed statement boundary
/// after every earlier statement in the call was acknowledged, and the
/// `release` sender that resumes the paused call. Dropping the sender also
/// resumes it, so tests must hold the sender while the pause must stay in
/// force.
#[cfg(feature = "test-utils")]
pub fn arm_pause_before_insert_statement_for_tests(
    statement_index: u64,
) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    INSERT_STATEMENT_PAUSES.lock().push(ArmedInsertStatementPause {
        statement_index,
        reached: reached_tx,
        release: release_rx,
    });

    (reached_rx, release_tx)
}

/// Pauses at an armed statement boundary; no-op when no armed pause matches
/// the statement index.
#[cfg(feature = "test-utils")]
async fn pause_before_insert_statement_for_tests(statement_index: u64) {
    let armed = {
        let mut armed_pauses = INSERT_STATEMENT_PAUSES.lock();
        armed_pauses
            .iter()
            .position(|armed| armed.statement_index == statement_index)
            .map(|index| armed_pauses.remove(index))
    };
    let Some(armed) = armed else {
        return;
    };
    let _ = armed.reached.send(());
    // A test that aborts the paused task never sends; a dropped sender
    // resumes normally.
    let _ = armed.release.await;
}

#[cfg(test)]
mod tests {
    use etl::schema::{ColumnSchema, Type};

    use super::*;

    fn column_schema(name: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.to_owned(),
            typ: Type::INT4,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: Some(1),
            nullable: false,
            default_expression: None,
        }
    }

    #[test]
    fn add_column_sql_quotes_identifiers() {
        let column = column_schema("new\"column");
        let sql = build_add_column_sql("table\"name", &column, Some("old\"column"), true);

        assert_eq!(
            sql,
            "ALTER TABLE \"table\\\"name\" ADD COLUMN IF NOT EXISTS \"new\\\"column\" \
             Nullable(Int32) AFTER \"old\\\"column\""
        );
    }

    #[test]
    fn add_column_sql_uses_first_when_anchor_is_none() {
        let column = column_schema("only_col");
        let sql = build_add_column_sql("test_table", &column, None, true);

        assert_eq!(
            sql,
            "ALTER TABLE \"test_table\" ADD COLUMN IF NOT EXISTS \"only_col\" Nullable(Int32) \
             FIRST"
        );
    }

    #[test]
    fn add_column_sql_includes_supported_default() {
        let column = ColumnSchema::new("score".to_owned(), Type::INT4, -1, 1, false)
            .with_primary_key(1)
            .with_default_expression("42".to_owned());
        let sql = build_add_column_sql("test_table", &column, Some("id"), true);

        assert_eq!(
            sql,
            "ALTER TABLE \"test_table\" ADD COLUMN IF NOT EXISTS \"score\" Nullable(Int32) \
             DEFAULT 42 AFTER \"id\""
        );
    }

    #[test]
    fn add_column_sql_can_preserve_not_null_with_supported_default() {
        let column = ColumnSchema::new("score".to_owned(), Type::INT4, -1, 1, false)
            .with_default_expression("42".to_owned());
        let sql = build_add_column_sql("test_table", &column, Some("id"), false);

        assert_eq!(
            sql,
            "ALTER TABLE \"test_table\" ADD COLUMN IF NOT EXISTS \"score\" Int32 DEFAULT 42 AFTER \
             \"id\""
        );
    }

    #[test]
    fn drop_not_null_sql_uses_actual_type_and_waits_for_mutation() {
        assert_eq!(
            build_drop_not_null_sql("table\"name", "old\"column", "Int32").as_deref(),
            Some(
                "ALTER TABLE \"table\\\"name\" MODIFY COLUMN \"old\\\"column\" Nullable(Int32) \
                 SETTINGS mutations_sync = 2"
            )
        );
        assert_eq!(build_drop_not_null_sql("test_table", "value", "Nullable(Int32)"), None);
        // ClickHouse rejects Nullable(Array(...)), so arrays are left as they
        // are.
        assert_eq!(build_drop_not_null_sql("test_table", "tags", "Array(Nullable(Int32))"), None);
    }

    #[test]
    fn drop_column_sql_quotes_identifiers() {
        let sql = build_drop_column_sql("table\"name", "old\"column");

        assert_eq!(sql, "ALTER TABLE \"table\\\"name\" DROP COLUMN IF EXISTS \"old\\\"column\"");
    }

    #[test]
    fn rename_column_sql_quotes_identifiers() {
        let sql = build_rename_column_sql("table\"name", "old\"column", "new\"column");

        assert_eq!(
            sql,
            "ALTER TABLE \"table\\\"name\" RENAME COLUMN IF EXISTS \"old\\\"column\" TO \
             \"new\\\"column\""
        );
    }

    #[test]
    fn truncate_table_sql_quotes_identifiers() {
        let sql = build_truncate_table_sql("table\"name");

        assert_eq!(sql, "TRUNCATE TABLE IF EXISTS \"table\\\"name\"");
    }

    #[test]
    fn drop_table_sql_quotes_identifiers() {
        let sql = build_drop_table_sql("table\"name");

        assert_eq!(sql, "DROP TABLE IF EXISTS \"table\\\"name\"");
    }

    /// The insert header lists every column name, then every type, matching
    /// the quoted column list.
    #[test]
    fn row_binary_layout_lists_names_before_types() {
        // GIVEN: an identifier that needs quoting and a nullable column.
        let columns = [
            ClickHouseTableColumn { name: "id".to_owned(), type_name: "Int64".to_owned() },
            ClickHouseTableColumn {
                name: "na\"me".to_owned(),
                type_name: "Nullable(String)".to_owned(),
            },
        ];

        // WHEN: the layout is built.
        let layout = RowBinaryLayout::new(&columns);

        // THEN: the SQL, header bytes, and column encodings follow column
        // order.
        assert_eq!(
            build_insert_rows_sql("table\"name", &layout),
            "INSERT INTO \"table\\\"name\" (\"id\", \"na\\\"me\") FORMAT \
             RowBinaryWithNamesAndTypes"
        );
        assert_eq!(
            layout.header,
            [&[2, 2][..], b"id", &[5], b"na\"me", &[5], b"Int64", &[16], b"Nullable(String)",]
                .concat()
        );
        assert_eq!(layout.column_encodings(), [ColumnEncoding::Required, ColumnEncoding::Nullable]);
    }

    /// The client timeout is the server timeout plus the configured epsilon.
    #[test]
    fn client_timeout_adds_epsilon_to_server_timeout() {
        // GIVEN: a config with a custom server timeout and epsilon.
        let config = ClickHouseClientConfig {
            connectivity_check_timeout: Duration::from_secs(10),
            client_timeout_epsilon: Duration::from_secs(3),
            ..Default::default()
        };

        // WHEN: the client timeout is queried.
        // THEN: it adds the epsilon to the server timeout.
        assert_eq!(
            config.client_timeout_for(ClickHouseOperationKind::ConnectivityCheck),
            Duration::from_secs(13)
        );

        // GIVEN: a zero server timeout.
        let config = ClickHouseClientConfig {
            connectivity_check_timeout: Duration::ZERO,
            client_timeout_epsilon: Duration::from_secs(3),
            ..Default::default()
        };

        // THEN: the client timeout is the epsilon alone.
        assert_eq!(
            config.client_timeout_for(ClickHouseOperationKind::ConnectivityCheck),
            Duration::from_secs(3)
        );
    }

    /// Operation kinds display the names interpolated into error messages by
    /// `timeout_call`.
    #[test]
    fn operation_kind_display_matches_error_messages() {
        // GIVEN: each operation kind.
        // WHEN: it is displayed.
        // THEN: it renders the human-readable operation name.
        assert_eq!(ClickHouseOperationKind::ConnectivityCheck.to_string(), "connectivity check");
        assert_eq!(ClickHouseOperationKind::SchemaQuery.to_string(), "schema query");
        assert_eq!(ClickHouseOperationKind::Ddl.to_string(), "DDL");
        assert_eq!(ClickHouseOperationKind::Insert.to_string(), "insert");
    }

    /// A missed deadline returns `DestinationTimeout` with the operation in the
    /// detail.
    #[tokio::test(start_paused = true)]
    async fn timeout_call_returns_destination_timeout_on_deadline() {
        // GIVEN: a future that never resolves. Tokio's paused clock advances
        // virtual time when all tasks are stalled, so the timeout fires
        // immediately in wall-clock terms.
        let config = ClickHouseClientConfig::default();
        let never = std::future::pending::<Result<(), clickhouse::error::Error>>();

        // WHEN: the call is awaited.
        let err = timeout_call(ClickHouseOperationKind::ConnectivityCheck, &config, None, never)
            .await
            .unwrap_err();

        // THEN: the error is a timeout that names the operation.
        assert_eq!(err.kind(), ErrorKind::DestinationTimeout);
        assert!(
            err.detail()
                .is_some_and(|d| d.contains("connectivity check") && d.contains("timed out")),
            "unexpected detail: {:?}",
            err.detail()
        );
    }

    /// A missed deadline keeps the caller's context in the error detail.
    #[tokio::test(start_paused = true)]
    async fn timeout_call_appends_context_to_detail() {
        // GIVEN: a never-resolving future and a context string.
        let config = ClickHouseClientConfig::default();
        let never = std::future::pending::<Result<(), clickhouse::error::Error>>();

        // WHEN: the deadline fires.
        let err =
            timeout_call(ClickHouseOperationKind::Insert, &config, Some("table: users"), never)
                .await
                .unwrap_err();

        // THEN: the detail contains the context.
        assert!(
            err.detail().is_some_and(|d| d.contains("table: users")),
            "unexpected detail: {:?}",
            err.detail()
        );
    }

    /// An inner ClickHouse error maps to the operation's failed kind.
    #[tokio::test(start_paused = true)]
    async fn timeout_call_propagates_inner_error() {
        // GIVEN: a future that fails before the deadline.
        let config = ClickHouseClientConfig::default();
        let fut = async { Err::<(), _>(clickhouse::error::Error::NotEnoughData) };

        // WHEN: the call is awaited without context.
        let err = timeout_call(ClickHouseOperationKind::SchemaQuery, &config, None, fut)
            .await
            .unwrap_err();

        // THEN: the error has the failed kind and names the operation.
        assert_eq!(err.kind(), ErrorKind::DestinationQueryFailed);
        assert!(
            err.detail().is_some_and(|d| d.contains("schema query") && d.contains("failed")),
            "unexpected detail: {:?}",
            err.detail()
        );
    }

    /// A successful future passes its value through unchanged.
    #[tokio::test(start_paused = true)]
    async fn timeout_call_passes_through_success() {
        // GIVEN: a future that resolves before the deadline.
        let config = ClickHouseClientConfig::default();
        let fut = async { Ok::<u32, clickhouse::error::Error>(42) };

        // WHEN: the call is awaited.
        let value =
            timeout_call(ClickHouseOperationKind::Insert, &config, None, fut).await.unwrap();

        // THEN: the inner value is returned.
        assert_eq!(value, 42);
    }

    /// An inner ClickHouse error keeps the context and its source.
    #[tokio::test(start_paused = true)]
    async fn timeout_call_inner_error_includes_context() {
        use std::error::Error as _;

        // GIVEN: a failing future and a context string.
        let config = ClickHouseClientConfig::default();
        let fut = async { Err::<(), _>(clickhouse::error::Error::NotEnoughData) };

        // WHEN: the call is awaited.
        let err = timeout_call(ClickHouseOperationKind::Insert, &config, Some("table: users"), fut)
            .await
            .unwrap_err();

        // THEN: the error has the failed kind, the context, and the source.
        assert_eq!(err.kind(), ErrorKind::DestinationAtomicBatchRetryable);
        assert!(
            err.detail().is_some_and(|d| d.contains("insert failed") && d.contains("table: users")),
            "unexpected detail: {:?}",
            err.detail()
        );
        assert!(err.source().is_some(), "expected inner clickhouse error to be attached");
    }

    /// Each operation kind reads its own server timeout from the config.
    #[test]
    fn server_timeout_per_operation_kind() {
        // GIVEN: a default config.
        let config = ClickHouseClientConfig::default();

        // WHEN: each operation kind's server timeout is queried.
        // THEN: it returns the matching config field.
        assert_eq!(
            config.server_timeout_for(ClickHouseOperationKind::ConnectivityCheck),
            config.connectivity_check_timeout
        );
        assert_eq!(
            config.server_timeout_for(ClickHouseOperationKind::SchemaQuery),
            config.schema_query_timeout
        );
        assert_eq!(config.server_timeout_for(ClickHouseOperationKind::Ddl), config.ddl_timeout);
        assert_eq!(
            config.server_timeout_for(ClickHouseOperationKind::Insert),
            config.insert_timeout
        );
    }

    /// Each operation kind maps to the error kind that drives its retry policy.
    #[test]
    fn operation_kind_failed_kind_per_bucket() {
        use ClickHouseOperationKind::{ConnectivityCheck, Ddl, Insert, SchemaQuery};

        // GIVEN: each operation kind, with a retryable and a permanent error.
        // WHEN: its failed kind is queried.
        // THEN: DDL and schema queries retry only retryable errors;
        // connectivity checks and inserts keep their timed-retry kinds
        // either way.
        for retryable in [true, false] {
            assert_eq!(
                ConnectivityCheck.failed_kind(retryable),
                ErrorKind::DestinationConnectionFailed
            );
            assert_eq!(Insert.failed_kind(retryable), ErrorKind::DestinationAtomicBatchRetryable);
        }
        for op in [SchemaQuery, Ddl] {
            assert_eq!(op.failed_kind(true), ErrorKind::DestinationConnectionFailed);
            assert_eq!(op.failed_kind(false), ErrorKind::DestinationQueryFailed);
        }
    }

    /// Lost connections and temporary server errors are retryable; rejected
    /// statements and local errors are not.
    #[test]
    fn clickhouse_errors_classify_retryability() {
        use clickhouse::error::Error;

        let bad_response = |message: &str| Error::BadResponse(message.to_owned());

        // GIVEN: lost connections, temporary server errors, and 5xx statuses.
        // THEN: they are retryable.
        for error in [
            Error::Network("connection reset".into()),
            Error::TimedOut,
            bad_response("Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)"),
            bad_response("Code: 210. DB::NetException: Connection refused. (NETWORK_ERROR)"),
            bad_response(
                "Code: 242. DB::Exception: Table is in readonly mode. (TABLE_IS_READ_ONLY)",
            ),
            bad_response("Code: 999"),
            bad_response("503 Service Unavailable"),
        ] {
            assert!(error.is_retryable(), "{error}");
        }

        // GIVEN: rejected statements, client-side statuses, and local errors.
        // THEN: they are not retryable.
        for error in [
            bad_response("Code: 36. DB::Exception: Bad arguments. (BAD_ARGUMENTS)"),
            bad_response("Code: 43. DB::Exception: Nested type ... (ILLEGAL_TYPE_OF_ARGUMENT)"),
            bad_response("Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)"),
            bad_response("400 Bad Request"),
            bad_response("5000 rows"),
            bad_response(""),
            Error::NotEnoughData,
            Error::RowNotFound,
        ] {
            assert!(!error.is_retryable(), "{error}");
        }
    }

    /// A connect-time public-address rejection is not retryable, however deeply
    /// the connector wraps it, while other connection errors still are.
    #[test]
    fn public_address_rejection_is_not_retryable() {
        use std::io;

        use clickhouse::error::Error;

        /// Stand-in for a connector error that exposes its cause as `source()`.
        #[derive(Debug)]
        struct ConnectError(io::Error);

        impl std::fmt::Display for ConnectError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("dns error")
            }
        }

        impl std::error::Error for ConnectError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        let denied = || io::Error::new(io::ErrorKind::PermissionDenied, "non-public IP address");

        // GIVEN: the guard's rejection, bare, behind a `source()` link, and
        // wrapped inside another `io::Error`.
        // THEN: none of them is retryable.
        for error in [
            Error::Network(Box::new(denied())),
            Error::Network(Box::new(ConnectError(denied()))),
            Error::Network(Box::new(io::Error::other(denied()))),
        ] {
            assert!(!error.is_retryable(), "{error:?}");
        }

        // GIVEN: a refused connection behind the same wrapper.
        // THEN: it is still retryable.
        let refused = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        assert!(Error::Network(Box::new(ConnectError(refused))).is_retryable());
    }

    /// A temporary server error on DDL becomes a timed-retry error kind, and a
    /// rejected statement stays a manual one.
    #[tokio::test(start_paused = true)]
    async fn timeout_call_classifies_ddl_errors_by_error() {
        async fn ddl_failure(message: &str) -> EtlError {
            let config = ClickHouseClientConfig::default();
            let error = clickhouse::error::Error::BadResponse(message.to_owned());
            let fut = async move { Err::<(), _>(error) };
            timeout_call(ClickHouseOperationKind::Ddl, &config, None, fut).await.unwrap_err()
        }

        // GIVEN: a server timeout and a rejected statement during DDL.
        // WHEN: each passes through `timeout_call`.
        let timeout = ddl_failure("Code: 159. DB::Exception: Timeout exceeded.").await;
        let rejected = ddl_failure("Code: 43. DB::Exception: Illegal type.").await;

        // THEN: only the timeout gets a timed-retry kind.
        assert_eq!(timeout.kind(), ErrorKind::DestinationConnectionFailed);
        assert_eq!(rejected.kind(), ErrorKind::DestinationQueryFailed);
    }

    /// `floor_secs` renders whole seconds with a floor of `"1"`, so zero and
    /// sub-second durations do not disable server-side timeouts.
    #[test]
    fn floor_secs_floors_at_one_second() {
        // GIVEN: whole, zero, sub-second, and fractional durations.
        // WHEN: each is formatted.
        // THEN: whole seconds at or above 1 pass through unchanged.
        assert_eq!(floor_secs(Duration::from_secs(1)), "1");
        assert_eq!(floor_secs(Duration::from_secs(5)), "5");
        assert_eq!(floor_secs(Duration::from_secs(60)), "60");
        // ZERO is floored to 1 to avoid disabling the server-side timeout.
        assert_eq!(floor_secs(Duration::ZERO), "1");
        // Sub-second values are floored to 1 (would otherwise truncate to 0).
        assert_eq!(floor_secs(Duration::from_nanos(1)), "1");
        assert_eq!(floor_secs(Duration::from_millis(500)), "1");
        assert_eq!(floor_secs(Duration::from_millis(999)), "1");
        // Fractional seconds beyond 1s truncate to whole seconds
        // (Duration::as_secs).
        assert_eq!(floor_secs(Duration::from_millis(1500)), "1");
        assert_eq!(floor_secs(Duration::from_millis(2999)), "2");
    }

    /// Only a column-list or header rejection counts as a stale insert layout.
    #[test]
    fn insert_layout_rejection_matches_only_layout_errors() {
        // GIVEN: server rejections of the column list and header.
        let rejections = [
            "Code: 117. DB::Exception: Type of 'id' must be Int64, not Int32: (while reading \
             header). (INCORRECT_DATA)",
            "Code: 16. DB::Exception: No such column nn in table default.t. \
             (NO_SUCH_COLUMN_IN_TABLE)",
            // The crate's fallback when the error body cannot be read.
            "Code: 117",
        ];
        // GIVEN: transient server failures.
        let transient = [
            "Code: 159. DB::Exception: Timeout exceeded: elapsed 1.2 seconds. (TIMEOUT_EXCEEDED)",
            "Code: 210. DB::NetException: I/O error: Broken pipe. (NETWORK_ERROR)",
        ];

        // WHEN: each failure is classified.
        // THEN: only the rejections count.
        let bad_response = |message: &str| clickhouse::error::Error::BadResponse(message.into());
        for message in rejections {
            assert!(is_insert_layout_rejection(&bad_response(message)), "{message}");
        }
        for message in transient {
            assert!(!is_insert_layout_rejection(&bad_response(message)), "{message}");
        }

        // GIVEN: a client-side timeout, which carries no server response.
        // THEN: it does not count.
        assert!(!is_insert_layout_rejection(&clickhouse::error::Error::TimedOut));
    }

    /// Malformed server messages yield no error code instead of panicking.
    #[test]
    fn clickhouse_error_code_rejects_malformed_messages() {
        // GIVEN: a server body and the crate's bare header fallback.
        // THEN: both codes are parsed.
        assert_eq!(clickhouse_error_code("Code: 117. DB::Exception: x"), Some(117));
        assert_eq!(clickhouse_error_code("Code: 117"), Some(117));

        // GIVEN: empty, truncated, non-numeric, overflowing, trailing-text, and
        // non-ASCII input.
        // THEN: no code is returned.
        for message in [
            "",
            "Code: ",
            "Code: .",
            "Code: 117 x",
            "Code: abc. x",
            "Code: -1. x",
            "Code: 99999999999. x",
            "Code: 1é7. x",
            "DB::Exception: Code: 117. x",
        ] {
            assert_eq!(clickhouse_error_code(message), None, "{message:?}");
        }
    }
}
