use std::{sync::Arc, time::Instant};

use clickhouse::Client;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use url::Url;

use crate::clickhouse::{
    encoding::{ClickHouseValue, rb_encode_row},
    metrics::ETL_CH_INSERT_DURATION_SECONDS,
    schema::{clickhouse_column_type, quote_identifier},
};

/// Capacity of the internal write buffer used per INSERT statement.
///
/// When this many bytes have been written to the buffer it is flushed to the
/// network (but the INSERT statement itself is not closed — that only happens
/// when `end()` is called or the `max_bytes_per_insert` limit is reached).
const BUFFERED_CAPACITY: usize = 256 * 1024;

/// Builds the SQL used to add a column to a ClickHouse table.
fn build_add_column_sql(
    table_name: &str,
    column: &etl::types::ColumnSchema,
    after_column: &str,
) -> String {
    let col_type = clickhouse_column_type(column, true);
    let table_name = quote_identifier(table_name);
    let column_name = quote_identifier(&column.name);
    let after_column = quote_identifier(after_column);

    format!(
        "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {col_type} AFTER \
         {after_column}"
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

/// High-level ClickHouse client used by [`super::core::ClickHouseDestination`].
///
/// Wraps a [`clickhouse::Client`] and exposes typed methods for DDL,
/// truncation, and RowBinary bulk inserts. Cheaply cloneable — the inner client
/// holds an `Arc` internally, and the outer `Arc` here ensures a single shared
/// instance.
#[derive(Clone)]
pub struct ClickHouseClient {
    inner: Arc<Client>,
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
    ) -> Self {
        let mut client =
            Client::default().with_url(url.to_string()).with_user(user).with_database(database);

        if let Some(pw) = password {
            client = client.with_password(pw);
        }

        Self { inner: Arc::new(client) }
    }

    pub async fn ping(&self) -> EtlResult<()> {
        self.inner
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map(|_| ())
            .map_err(|e| etl_error!(ErrorKind::Unknown, "ClickHouse connectivity check failed", e))
    }

    /// Executes a DDL statement (e.g. `CREATE TABLE IF NOT EXISTS …`).
    pub(crate) async fn execute_ddl(&self, sql: &str) -> EtlResult<()> {
        self.inner.query(sql).execute().await.map_err(|e| {
            etl_error!(
                ErrorKind::Unknown,
                "ClickHouse DDL failed",
                format!("DDL execution failed: {e}")
            )
        })
    }

    /// Adds a column to an existing ClickHouse table.
    ///
    /// New columns are always Nullable since ClickHouse cannot backfill
    /// existing rows with a NOT NULL default.
    ///
    /// `after_column` controls placement: the new column is inserted AFTER
    /// the named column. This is critical because RowBinary encoding is
    /// positional -- new user columns must appear before the CDC columns
    /// (`cdc_operation`, `cdc_lsn`), not appended after them.
    pub(crate) async fn add_column(
        &self,
        table_name: &str,
        column: &etl::types::ColumnSchema,
        after_column: &str,
    ) -> EtlResult<()> {
        let sql = build_add_column_sql(table_name, column, after_column);
        self.execute_ddl(&sql).await
    }

    /// Drops a column from an existing ClickHouse table (idempotent).
    pub(crate) async fn drop_column(&self, table_name: &str, column_name: &str) -> EtlResult<()> {
        let sql = build_drop_column_sql(table_name, column_name);
        self.execute_ddl(&sql).await
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
        self.execute_ddl(&sql).await
    }

    /// Executes `TRUNCATE TABLE IF EXISTS` for the supplied table.
    pub(crate) async fn truncate_table(&self, table_name: &str) -> EtlResult<()> {
        self.inner.query(&build_truncate_table_sql(table_name)).execute().await.map_err(|e| {
            etl_error!(
                ErrorKind::Unknown,
                "ClickHouse truncate failed",
                format!("Failed to truncate table '{table_name}': {e}")
            )
        })
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
    /// `etl_ch_insert_duration_seconds` histogram recorded after each committed
    /// INSERT statement.
    pub(crate) async fn insert_rows(
        &self,
        table_name: &str,
        rows: Vec<Vec<ClickHouseValue>>,
        nullable_flags: &[bool],
        max_bytes_per_insert: u64,
        source: &'static str,
    ) -> EtlResult<()> {
        let sql = build_insert_rows_sql(table_name);

        let mut insert =
            self.inner.insert_formatted_with(sql.clone()).buffered_with_capacity(BUFFERED_CAPACITY);
        let mut bytes = 0u64;
        let mut row_buf = Vec::new();
        let mut insert_start = Instant::now();

        for row in rows {
            row_buf.clear();
            rb_encode_row(row, nullable_flags, &mut row_buf)?;

            insert.write_buffered(&row_buf);
            bytes += row_buf.len() as u64;

            if bytes >= max_bytes_per_insert {
                insert.end().await.map_err(|e| {
                    etl_error!(
                        ErrorKind::Unknown,
                        "ClickHouse insert flush failed",
                        format!("Failed to flush INSERT for '{table_name}': {e}")
                    )
                })?;
                metrics::histogram!(
                    ETL_CH_INSERT_DURATION_SECONDS,
                    "table" => table_name.to_string(),
                    "source" => source
                )
                .record(insert_start.elapsed().as_secs_f64());

                insert = self
                    .inner
                    .insert_formatted_with(sql.clone())
                    .buffered_with_capacity(BUFFERED_CAPACITY);
                insert_start = Instant::now();
                bytes = 0;
            }
        }

        insert.end().await.map_err(|e| {
            etl_error!(
                ErrorKind::Unknown,
                "ClickHouse insert flush failed",
                format!("Failed to flush INSERT for '{table_name}': {e}")
            )
        })?;
        metrics::histogram!(
            ETL_CH_INSERT_DURATION_SECONDS,
            "table" => table_name.to_string(),
            "source" => source
        )
        .record(insert_start.elapsed().as_secs_f64());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use etl::types::{ColumnSchema, Type};

    use super::*;

    fn column_schema(name: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
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
        let sql = build_add_column_sql("table\"name", &column, "old\"column");

        assert_eq!(
            sql,
            "ALTER TABLE \"table\"\"name\" ADD COLUMN IF NOT EXISTS \"new\"\"column\" \
             Nullable(Int32) AFTER \"old\"\"column\""
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
}
