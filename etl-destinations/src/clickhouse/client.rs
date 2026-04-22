use std::{sync::Arc, time::Instant};

use clickhouse::Client;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use tracing::info;
use url::Url;

use crate::clickhouse::{
    encoding::{ClickHouseValue, rb_encode_row},
    metrics::ETL_CH_INSERT_DURATION_SECONDS,
    schema::clickhouse_column_type,
};

/// Capacity of the internal write buffer used per INSERT statement.
///
/// When this many bytes have been written to the buffer it is flushed to the
/// network (but the INSERT statement itself is not closed — that only happens
/// when `end()` is called or the `max_bytes_per_insert` limit is reached).
const BUFFERED_CAPACITY: usize = 256 * 1024;

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
        let col_type = clickhouse_column_type(column, true);
        let sql = format!(
            "ALTER TABLE \"{table_name}\" ADD COLUMN IF NOT EXISTS \"{}\" {col_type} AFTER \
             \"{after_column}\"",
            column.name
        );
        self.execute_ddl(&sql).await
    }

    /// Drops a column from an existing ClickHouse table (idempotent).
    pub(crate) async fn drop_column(&self, table_name: &str, column_name: &str) -> EtlResult<()> {
        let sql = format!("ALTER TABLE \"{table_name}\" DROP COLUMN IF EXISTS \"{column_name}\"");
        self.execute_ddl(&sql).await
    }

    /// Renames a column in an existing ClickHouse table.
    ///
    /// Idempotent: checks system.columns before renaming. If the old column
    /// doesn't exist, the rename is assumed already applied and skipped.
    pub(crate) async fn rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> EtlResult<()> {
        let exists: u64 = self
            .inner
            .query(&format!(
                "SELECT count() FROM system.columns WHERE table = '{table_name}' AND name = \
                 '{old_name}'"
            ))
            .fetch_one()
            .await
            .map_err(|e| etl_error!(ErrorKind::Unknown, "ClickHouse column check failed", e))?;
        if exists == 0 {
            info!("rename {old_name} -> {new_name} already applied, skipping");
            return Ok(());
        }

        let sql =
            format!("ALTER TABLE \"{table_name}\" RENAME COLUMN \"{old_name}\" TO \"{new_name}\"");
        self.execute_ddl(&sql).await
    }

    /// Executes `TRUNCATE TABLE IF EXISTS "<table_name>"`.
    pub(crate) async fn truncate_table(&self, table_name: &str) -> EtlResult<()> {
        self.inner
            .query(&format!("TRUNCATE TABLE IF EXISTS \"{table_name}\""))
            .execute()
            .await
            .map_err(|e| {
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
        let sql = format!("INSERT INTO \"{table_name}\" FORMAT RowBinary");

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
