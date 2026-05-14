use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use clickhouse::Client;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use url::Url;

use crate::clickhouse::{
    core::{ClickHouseClientConfig, ClickHouseOperationKind},
    encoding::{ClickHouseValue, encode_to_row_binary},
    metrics::{ETL_CLICKHOUSE_DDL_DURATION_SECONDS, ETL_CLICKHOUSE_INSERT_DURATION_SECONDS},
    schema::{clickhouse_column_type, quote_identifier},
};

/// Formats a `Duration` as a whole-seconds string for ClickHouse
/// `.with_option(...)` settings, floored at `"1"`. ClickHouse interprets `"0"`
/// as "no timeout" for `http_*_timeout`, `max_execution_time`, and
/// `lock_acquire_timeout`; flooring at 1 second avoids accidentally disabling a
/// server-side bound when an operator passes `Duration::ZERO` or any sub-second
/// value.
fn secs_string(d: Duration) -> String {
    d.as_secs().max(1).to_string()
}

/// Runs `fut` under `tokio::time::timeout` using the client budget for `op`
/// from `config`. Inner ClickHouse errors map onto `op.failed_kind()`;
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
    let budget = config.client_budget(op);
    match tokio::time::timeout(budget, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(etl_error!(
            op.failed_kind(),
            "ClickHouse call failed",
            detail(op, "failed", context),
            source: err
        )),
        Err(_) => Err(etl_error!(
            ErrorKind::DestinationTimeout,
            "ClickHouse call timed out",
            detail(op, &format!("timed out after {budget:?}"), context)
        )),
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

/// Returns the placement clause for an `ADD COLUMN` statement.
///
/// `None` means the destination table has no user columns to anchor on, so
/// the new column goes at the front via `FIRST` (which still places it
/// before the trailing CDC columns).
fn add_column_placement_clause(after_column: Option<&str>) -> String {
    match after_column {
        Some(anchor) => format!("AFTER {}", quote_identifier(anchor)),
        None => "FIRST".to_owned(),
    }
}

/// Builds the SQL used to add a column to a ClickHouse table.
fn build_add_column_sql(
    table_name: &str,
    column: &etl::types::ColumnSchema,
    after_column: Option<&str>,
) -> String {
    let col_type = clickhouse_column_type(column, true);
    let table_name = quote_identifier(table_name);
    let column_name = quote_identifier(&column.name);
    let placement = add_column_placement_clause(after_column);

    format!(
        "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {col_type} {placement}"
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

/// Builds the SQL used to truncate a ClickHouse table.
fn build_truncate_table_sql(table_name: &str) -> String {
    let table_name = quote_identifier(table_name);
    format!("TRUNCATE TABLE IF EXISTS {table_name}")
}

/// Builds the SQL used to insert RowBinary rows into a ClickHouse table.
fn build_insert_rows_sql(table_name: &str) -> String {
    let table_name = quote_identifier(table_name);
    format!("INSERT INTO {table_name} FORMAT RowBinary")
}

/// Kind of DDL being executed; surfaces as a `kind` label on the
/// `etl_clickhouse_ddl_duration_seconds` histogram so per-operation latencies
/// can be distinguished (one-shot CREATE vs. online ALTER, etc.).
#[derive(Copy, Clone)]
pub(crate) enum DdlKind {
    CreateTable,
    AddColumn,
    DropColumn,
    RenameColumn,
}

impl DdlKind {
    fn as_label(self) -> &'static str {
        match self {
            DdlKind::CreateTable => "create_table",
            DdlKind::AddColumn => "add_column",
            DdlKind::DropColumn => "drop_column",
            DdlKind::RenameColumn => "rename_column",
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
        let mut client =
            Client::default().with_url(url.to_string()).with_user(user).with_database(database);

        if let Some(pw) = password {
            client = client.with_password(pw);
        }

        client = client
            .with_option("connect_timeout", secs_string(config.connectivity_check_timeout))
            .with_option("http_connection_timeout", secs_string(config.connectivity_check_timeout))
            .with_option("http_send_timeout", secs_string(config.insert_timeout))
            .with_option("http_receive_timeout", secs_string(config.insert_timeout));

        Self { inner: Arc::new(client), config }
    }

    /// Verifies that the ClickHouse server is reachable.
    ///
    /// Issues a `SELECT 1` round-trip; cheaper than any DDL or metadata
    /// query and exercises the auth/transport path. Mirrors the Iceberg
    /// destination's `validate_connectivity` so callers (notably the
    /// `etl-api` validators) can treat the two destinations uniformly.
    pub async fn validate_connectivity(&self) -> EtlResult<()> {
        timeout_call(
            ClickHouseOperationKind::ConnectivityCheck,
            &self.config,
            None,
            self.inner.query("SELECT 1").fetch_one::<u8>(),
        )
        .await?;
        Ok(())
    }

    /// Executes a DDL statement (e.g. `CREATE TABLE IF NOT EXISTS …`) and
    /// records its duration in the `etl_clickhouse_ddl_duration_seconds`
    /// histogram labelled with the DDL `kind` and `table_name`.
    pub(crate) async fn execute_ddl(&self, kind: DdlKind, sql: &str) -> EtlResult<()> {
        let ddl_start = Instant::now();
        let ddl_secs = secs_string(self.config.ddl_timeout);
        // `max_execution_time` and `lock_acquire_timeout` are applied per-call
        // so the connectivity check and schema query do not inherit the (much
        // larger) DDL budget.
        let query = self
            .inner
            .query(sql)
            .with_option("max_execution_time", &ddl_secs)
            .with_option("lock_acquire_timeout", &ddl_secs);
        let result =
            timeout_call(ClickHouseOperationKind::Ddl, &self.config, None, query.execute()).await;
        metrics::histogram!(
            ETL_CLICKHOUSE_DDL_DURATION_SECONDS,
            "kind" => kind.as_label(),
        )
        .record(ddl_start.elapsed().as_secs_f64());
        result
    }

    /// Returns ClickHouse columns for a table in position order.
    pub(crate) async fn table_columns(
        &self,
        table_name: &str,
    ) -> EtlResult<Vec<ClickHouseTableColumn>> {
        let schema_secs = secs_string(self.config.schema_query_timeout);
        let query = self
            .inner
            .query(
                "SELECT name, type AS type_name FROM system.columns WHERE database = \
                 currentDatabase() AND table = ? ORDER BY position",
            )
            .with_option("max_execution_time", &schema_secs)
            .bind(table_name);
        timeout_call(
            ClickHouseOperationKind::SchemaQuery,
            &self.config,
            Some(&format!("table: {table_name}")),
            query.fetch_all::<ClickHouseTableColumn>(),
        )
        .await
    }

    /// Adds a column to an existing ClickHouse table.
    ///
    /// New columns are always Nullable since ClickHouse cannot backfill
    /// existing rows with a NOT NULL default.
    ///
    /// `after_column` controls placement: `Some(name)` inserts the new column
    /// immediately AFTER `name`, `None` inserts it FIRST (used when the table
    /// has no user columns yet). Either way the new column lands before the
    /// trailing CDC columns (`cdc_operation`, `cdc_lsn`), which is required
    /// because RowBinary encoding is positional.
    pub(crate) async fn add_column(
        &self,
        table_name: &str,
        column: &etl::types::ColumnSchema,
        after_column: Option<&str>,
    ) -> EtlResult<()> {
        let sql = build_add_column_sql(table_name, column, after_column);
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
    /// old column is already absent, so the check and the rename happen in
    /// one statement without a racy read-then-write.
    pub(crate) async fn rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> EtlResult<()> {
        let sql = build_rename_column_sql(table_name, old_name, new_name);
        self.execute_ddl(DdlKind::RenameColumn, &sql).await
    }

    /// Executes `TRUNCATE TABLE IF EXISTS` for the supplied table.
    pub(crate) async fn truncate_table(&self, table_name: &str) -> EtlResult<()> {
        let ddl_secs = secs_string(self.config.ddl_timeout);
        let query = self
            .inner
            .query(&build_truncate_table_sql(table_name))
            .with_option("max_execution_time", &ddl_secs)
            .with_option("lock_acquire_timeout", &ddl_secs);
        timeout_call(
            ClickHouseOperationKind::Ddl,
            &self.config,
            Some(&format!("table: {table_name}")),
            query.execute(),
        )
        .await
    }

    /// Inserts `rows` into `table_name` using the RowBinary format.
    ///
    /// Each element of `rows` is a complete, already-encoded row of
    /// [`ClickHouseValue`]s in column order (user columns + CDC columns).
    /// `nullable_flags` must have the same length as each row.
    ///
    /// When the accumulated uncompressed byte count reaches
    /// `max_bytes_per_insert` the current INSERT statement is committed and
    /// a new one is opened, keeping peak memory usage bounded for large
    /// initial copies.
    ///
    /// The `source` label (`"copy"` or `"streaming"`) is attached to the
    /// `etl_clickhouse_insert_duration_seconds` histogram recorded after each
    /// committed INSERT statement.
    pub(crate) async fn insert_rows(
        &self,
        table_name: &str,
        rows: Vec<Vec<ClickHouseValue>>,
        nullable_flags: &[bool],
        max_bytes_per_insert: u64,
        source: &'static str,
    ) -> EtlResult<()> {
        let sql = build_insert_rows_sql(table_name);
        let mut rows = rows.into_iter().peekable();
        let mut row_buf = Vec::new();

        while rows.peek().is_some() {
            let mut insert = self
                .inner
                .insert_formatted_with(sql.clone())
                .buffered_with_capacity(BUFFERED_CAPACITY);
            let mut bytes = 0u64;
            let insert_start = Instant::now();

            while bytes < max_bytes_per_insert {
                let Some(row) = rows.next() else { break };
                row_buf.clear();
                encode_to_row_binary(row, nullable_flags, &mut row_buf)?;
                insert.write_buffered(&row_buf);
                bytes += row_buf.len() as u64;
            }

            timeout_call(
                ClickHouseOperationKind::Insert,
                &self.config,
                Some(&format!("table: {table_name}")),
                insert.end(),
            )
            .await?;
            metrics::histogram!(
                ETL_CLICKHOUSE_INSERT_DURATION_SECONDS,
                "source" => source,
            )
            .record(insert_start.elapsed().as_secs_f64());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use etl::types::{ColumnSchema, Type};

    use super::*;

    fn column_schema(name: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.to_owned(),
            typ: Type::INT4,
            modifier: -1,
            ordinal_position: 1,
            primary_key_ordinal_position: Some(1),
            nullable: false,
        }
    }

    #[test]
    fn add_column_sql_quotes_identifiers() {
        let column = column_schema("new\"column");
        let sql = build_add_column_sql("table\"name", &column, Some("old\"column"));

        assert_eq!(
            sql,
            "ALTER TABLE \"table\"\"name\" ADD COLUMN IF NOT EXISTS \"new\"\"column\" \
             Nullable(Int32) AFTER \"old\"\"column\""
        );
    }

    #[test]
    fn add_column_sql_uses_first_when_anchor_is_none() {
        let column = column_schema("only_col");
        let sql = build_add_column_sql("test_table", &column, None);

        assert_eq!(
            sql,
            "ALTER TABLE \"test_table\" ADD COLUMN IF NOT EXISTS \"only_col\" Nullable(Int32) \
             FIRST"
        );
    }

    #[test]
    fn drop_column_sql_quotes_identifiers() {
        let sql = build_drop_column_sql("table\"name", "old\"column");

        assert_eq!(sql, "ALTER TABLE \"table\"\"name\" DROP COLUMN IF EXISTS \"old\"\"column\"");
    }

    #[test]
    fn rename_column_sql_quotes_identifiers() {
        let sql = build_rename_column_sql("table\"name", "old\"column", "new\"column");

        assert_eq!(
            sql,
            "ALTER TABLE \"table\"\"name\" RENAME COLUMN IF EXISTS \"old\"\"column\" TO \
             \"new\"\"column\""
        );
    }

    #[test]
    fn truncate_table_sql_quotes_identifiers() {
        let sql = build_truncate_table_sql("table\"name");

        assert_eq!(sql, "TRUNCATE TABLE IF EXISTS \"table\"\"name\"");
    }

    #[test]
    fn insert_rows_sql_quotes_identifiers() {
        let sql = build_insert_rows_sql("table\"name");

        assert_eq!(sql, "INSERT INTO \"table\"\"name\" FORMAT RowBinary");
    }

    #[test]
    fn client_budget_adds_epsilon_to_server_budget() {
        let config = ClickHouseClientConfig {
            connectivity_check_timeout: Duration::from_secs(10),
            client_timeout_epsilon: Duration::from_secs(3),
            ..Default::default()
        };
        assert_eq!(
            config.client_budget(ClickHouseOperationKind::ConnectivityCheck),
            Duration::from_secs(13)
        );

        let config = ClickHouseClientConfig {
            connectivity_check_timeout: Duration::ZERO,
            client_timeout_epsilon: Duration::from_secs(3),
            ..Default::default()
        };
        assert_eq!(
            config.client_budget(ClickHouseOperationKind::ConnectivityCheck),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn operation_kind_display_matches_error_messages() {
        assert_eq!(ClickHouseOperationKind::ConnectivityCheck.to_string(), "connectivity check");
        assert_eq!(ClickHouseOperationKind::SchemaQuery.to_string(), "schema query");
        assert_eq!(ClickHouseOperationKind::Ddl.to_string(), "DDL");
        assert_eq!(ClickHouseOperationKind::Insert.to_string(), "insert");
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_call_returns_destination_timeout_on_deadline() {
        // A future that never resolves; tokio's paused clock advances virtual
        // time when all tasks are stalled, so the timeout fires immediately
        // in real wall-clock terms.
        let config = ClickHouseClientConfig::default();
        let never = std::future::pending::<Result<(), clickhouse::error::Error>>();
        let err = timeout_call(ClickHouseOperationKind::ConnectivityCheck, &config, None, never)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationTimeout);
        assert!(
            err.detail()
                .is_some_and(|d| d.contains("connectivity check") && d.contains("timed out")),
            "unexpected detail: {:?}",
            err.detail()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_call_appends_context_to_detail() {
        let config = ClickHouseClientConfig::default();
        let never = std::future::pending::<Result<(), clickhouse::error::Error>>();
        let err =
            timeout_call(ClickHouseOperationKind::Insert, &config, Some("table: users"), never)
                .await
                .unwrap_err();
        assert!(
            err.detail().is_some_and(|d| d.contains("table: users")),
            "unexpected detail: {:?}",
            err.detail()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_call_propagates_inner_error() {
        let config = ClickHouseClientConfig::default();
        let fut = async { Err::<(), _>(clickhouse::error::Error::NotEnoughData) };
        let err = timeout_call(ClickHouseOperationKind::SchemaQuery, &config, None, fut)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationQueryFailed);
        assert!(
            err.detail().is_some_and(|d| d.contains("schema query") && d.contains("failed")),
            "unexpected detail: {:?}",
            err.detail()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_call_passes_through_success() {
        let config = ClickHouseClientConfig::default();
        let fut = async { Ok::<u32, clickhouse::error::Error>(42) };
        let value =
            timeout_call(ClickHouseOperationKind::Insert, &config, None, fut).await.unwrap();
        assert_eq!(value, 42);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_call_inner_error_includes_context() {
        use std::error::Error as _;
        let config = ClickHouseClientConfig::default();
        let fut = async { Err::<(), _>(clickhouse::error::Error::NotEnoughData) };
        let err = timeout_call(ClickHouseOperationKind::Insert, &config, Some("table: users"), fut)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationAtomicBatchRetryable);
        assert!(
            err.detail().is_some_and(|d| d.contains("insert failed") && d.contains("table: users")),
            "unexpected detail: {:?}",
            err.detail()
        );
        assert!(err.source().is_some(), "expected inner clickhouse error to be attached");
    }

    #[test]
    fn server_budget_per_operation_kind() {
        let config = ClickHouseClientConfig::default();
        assert_eq!(
            config.server_budget(ClickHouseOperationKind::ConnectivityCheck),
            config.connectivity_check_timeout
        );
        assert_eq!(
            config.server_budget(ClickHouseOperationKind::SchemaQuery),
            config.schema_query_timeout
        );
        assert_eq!(config.server_budget(ClickHouseOperationKind::Ddl), config.ddl_timeout);
        assert_eq!(config.server_budget(ClickHouseOperationKind::Insert), config.insert_timeout);
    }

    #[test]
    fn operation_kind_failed_kind_per_bucket() {
        assert_eq!(
            ClickHouseOperationKind::ConnectivityCheck.failed_kind(),
            ErrorKind::DestinationConnectionFailed
        );
        assert_eq!(
            ClickHouseOperationKind::SchemaQuery.failed_kind(),
            ErrorKind::DestinationQueryFailed
        );
        assert_eq!(ClickHouseOperationKind::Ddl.failed_kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(
            ClickHouseOperationKind::Insert.failed_kind(),
            ErrorKind::DestinationAtomicBatchRetryable
        );
    }

    #[test]
    fn secs_string_floors_at_one_second() {
        // Whole seconds at or above 1 pass through unchanged.
        assert_eq!(secs_string(Duration::from_secs(1)), "1");
        assert_eq!(secs_string(Duration::from_secs(5)), "5");
        assert_eq!(secs_string(Duration::from_secs(60)), "60");
        // ZERO is floored to 1 to avoid disabling the server-side timeout.
        assert_eq!(secs_string(Duration::ZERO), "1");
        // Sub-second values are floored to 1 (would otherwise truncate to 0).
        assert_eq!(secs_string(Duration::from_nanos(1)), "1");
        assert_eq!(secs_string(Duration::from_millis(500)), "1");
        assert_eq!(secs_string(Duration::from_millis(999)), "1");
        // Fractional seconds beyond 1s truncate to whole seconds (Duration::as_secs).
        assert_eq!(secs_string(Duration::from_millis(1500)), "1");
        assert_eq!(secs_string(Duration::from_millis(2999)), "2");
    }
}
