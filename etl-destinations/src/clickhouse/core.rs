use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, RwLock},
};

use crate::clickhouse::metrics::{
    ETL_CH_DDL_DURATION_SECONDS, ETL_CH_INSERT_DURATION_SECONDS, register_metrics,
};
use crate::clickhouse::schema::{build_create_table_sql, table_name_to_clickhouse_table_name};
use chrono::NaiveDate;
use clickhouse::Client;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{ArrayCell, Cell, Event, TableId, TableRow};
use etl::{bail, etl_error};
use etl::{destination::Destination, types::PgLsn};
use std::time::Instant;
use tokio::task::JoinSet;
use tracing::{debug, info};

// ── CDC operation type ────────────────────────────────────────────────────────

#[derive(Copy, Clone)]
enum CdcOperation {
    Insert,
    Update,
    Delete,
}

impl CdcOperation {
    fn as_str(self) -> &'static str {
        match self {
            CdcOperation::Insert => "INSERT",
            CdcOperation::Update => "UPDATE",
            CdcOperation::Delete => "DELETE",
        }
    }
}

/// A single row pending insertion, carrying the CDC metadata alongside the cell data.
struct PendingRow {
    operation: CdcOperation,
    lsn: PgLsn,
    cells: Vec<Cell>,
}

// ── Unix epoch constant for Date conversion ───────────────────────────────────

fn unix_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date")
}

// ── Inserter configuration ────────────────────────────────────────────────────

/// Controls intermediate flushing inside a single `write_table_rows` / `write_events` call.
///
/// The upstream `BatchConfig::max_fill_ms` controls when `write_events` is called;
/// these limits prevent unbounded memory use for very large batches (e.g. initial copy).
pub struct ClickHouseInserterConfig {
    /// Start a new INSERT after this many rows (default: 100_000).
    pub max_rows_per_insert: u64,
    /// Start a new INSERT after this many uncompressed bytes.
    ///
    /// Derive this from `BatchConfig::memory_budget_ratio × total_memory / max_table_sync_workers`
    /// (the same formula used by `BatchBudget::ideal_batch_size_bytes`).
    pub max_bytes_per_insert: u64,
}

impl Default for ClickHouseInserterConfig {
    fn default() -> Self {
        Self {
            max_rows_per_insert: 100_000,
            max_bytes_per_insert: 256 * 1024 * 1024, // 256 MiB
        }
    }
}

// ── ClickHouseValue ───────────────────────────────────────────────────────────

/// Owned ClickHouse-compatible value, moved (not cloned) from a [`Cell`].
enum ClickHouseValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt32(u32),
    Float32(f32),
    Float64(f64),
    /// TEXT, NUMERIC (string), TIME (string), JSON, BYTEA (hex-encoded)
    String(String),
    /// Days since Unix epoch (ClickHouse `Date` on wire = UInt16 LE)
    Date(u16),
    /// Microseconds since Unix epoch (ClickHouse `DateTime64(6)` on wire = Int64 LE)
    DateTime64(i64),
    /// UUID in standard 16-byte big-endian order (converted to ClickHouse wire format on encode)
    Uuid([u8; 16]),
    Array(Vec<ClickHouseValue>),
}

// ── RowBinary encoding ────────────────────────────────────────────────────────
//
// We bypass the `Row` / `Inserter` API entirely and write RowBinary bytes directly
// via `Client::insert_formatted_with("INSERT INTO \"t\" FORMAT RowBinary")`.
//
// This avoids two fatal issues with the `Inserter<T>` path:
//
// 1. `Insert::new` always calls `join_column_names::<T>().expect(…)`, which panics
//    when `COLUMN_NAMES = &[]` regardless of whether validation is enabled.
//
// 2. The RowBinary serde serializer wraps its `BufMut` writer in a fresh `&mut` at
//    every `serialize_some` call, telescoping the type to `&mut &mut … BytesMut` for
//    nullable array elements and overflowing the compiler's recursion limit.
//
// Direct binary encoding has neither problem: it is a simple recursive function that
// writes bytes to a `Vec<u8>` with no generics and no type-level recursion.

/// Encodes a variable-length integer (LEB128) used by ClickHouse for string/array lengths.
fn rb_varint(mut v: usize, buf: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Encodes a value for a `Nullable(T)` column (1-byte null indicator + value if present).
fn rb_encode_nullable(val: &ClickHouseValue, buf: &mut Vec<u8>) {
    match val {
        ClickHouseValue::Null => buf.push(1),
        v => {
            buf.push(0);
            rb_encode_value(v, buf);
        }
    }
}

/// Encodes a value for a non-nullable column (no null indicator byte).
fn rb_encode_value(val: &ClickHouseValue, buf: &mut Vec<u8>) {
    match val {
        ClickHouseValue::Null => {
            // A non-nullable column unexpectedly received NULL (data quality issue from
            // Postgres). Write a zero-length string as the least-harmful fallback.
            buf.push(0); // varint 0 = empty string
        }
        ClickHouseValue::Bool(b) => buf.push(*b as u8),
        ClickHouseValue::Int16(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Int64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::UInt32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Float32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::Float64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        ClickHouseValue::String(s) => {
            rb_varint(s.len(), buf);
            buf.extend_from_slice(s.as_bytes());
        }
        ClickHouseValue::Date(days) => buf.extend_from_slice(&days.to_le_bytes()),
        ClickHouseValue::DateTime64(micros) => buf.extend_from_slice(&micros.to_le_bytes()),
        ClickHouseValue::Uuid(bytes) => {
            // ClickHouse RowBinary UUID = two little-endian u64 (high bits then low bits).
            // Our bytes are in standard UUID big-endian order, so we split into two u64
            // and write each in little-endian.
            let high = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            let low = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
            buf.extend_from_slice(&high.to_le_bytes());
            buf.extend_from_slice(&low.to_le_bytes());
        }
        // Array elements are always Nullable in ClickHouse: Array(Nullable(T)).
        ClickHouseValue::Array(items) => {
            rb_varint(items.len(), buf);
            for item in items {
                rb_encode_nullable(item, buf);
            }
        }
    }
}

/// Encodes a complete row into `buf`, selecting nullable vs non-nullable encoding per column.
fn rb_encode_row(values: &[ClickHouseValue], nullable_flags: &[bool], buf: &mut Vec<u8>) {
    for (val, &is_nullable) in values.iter().zip(nullable_flags.iter()) {
        if is_nullable {
            rb_encode_nullable(val, buf);
        } else {
            rb_encode_value(val, buf);
        }
    }
}

// ── Cell → ClickHouseValue conversion ────────────────────────────────────────

/// Converts a [`Cell`] to a [`ClickHouseValue`], consuming it (no clone).
fn cell_to_clickhouse_value(cell: Cell) -> ClickHouseValue {
    match cell {
        Cell::Null => ClickHouseValue::Null,
        Cell::Bool(b) => ClickHouseValue::Bool(b),
        Cell::I16(v) => ClickHouseValue::Int16(v),
        Cell::I32(v) => ClickHouseValue::Int32(v),
        Cell::I64(v) => ClickHouseValue::Int64(v),
        Cell::U32(v) => ClickHouseValue::UInt32(v),
        Cell::F32(v) => ClickHouseValue::Float32(v),
        Cell::F64(v) => ClickHouseValue::Float64(v),
        Cell::Numeric(n) => ClickHouseValue::String(n.to_string()),
        Cell::Date(d) => {
            let days = d
                .signed_duration_since(unix_epoch())
                .num_days()
                .clamp(0, i64::from(u16::MAX)) as u16;
            ClickHouseValue::Date(days)
        }
        Cell::Time(t) => ClickHouseValue::String(t.to_string()),
        Cell::Timestamp(dt) => ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros()),
        Cell::TimestampTz(dt) => ClickHouseValue::DateTime64(dt.timestamp_micros()),
        Cell::Uuid(u) => ClickHouseValue::Uuid(*u.as_bytes()),
        Cell::Json(j) => ClickHouseValue::String(j.to_string()),
        Cell::Bytes(b) => ClickHouseValue::String(bytes_to_hex(&b)),
        Cell::String(s) => ClickHouseValue::String(s),
        Cell::Array(array_cell) => {
            ClickHouseValue::Array(array_cell_to_clickhouse_values(array_cell))
        }
    }
}

fn array_cell_to_clickhouse_values(array_cell: ArrayCell) -> Vec<ClickHouseValue> {
    match array_cell {
        ArrayCell::Bool(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Bool))
            .collect(),
        ArrayCell::String(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::String))
            .collect(),
        ArrayCell::I16(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Int16))
            .collect(),
        ArrayCell::I32(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Int32))
            .collect(),
        ArrayCell::I64(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Int64))
            .collect(),
        ArrayCell::U32(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::UInt32))
            .collect(),
        ArrayCell::F32(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Float32))
            .collect(),
        ArrayCell::F64(v) => v
            .into_iter()
            .map(|o| o.map_or(ClickHouseValue::Null, ClickHouseValue::Float64))
            .collect(),
        ArrayCell::Numeric(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |n| {
                    ClickHouseValue::String(n.to_string())
                })
            })
            .collect(),
        ArrayCell::Date(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |d| {
                    let days = d
                        .signed_duration_since(unix_epoch())
                        .num_days()
                        .clamp(0, i64::from(u16::MAX)) as u16;
                    ClickHouseValue::Date(days)
                })
            })
            .collect(),
        ArrayCell::Time(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |t| {
                    ClickHouseValue::String(t.to_string())
                })
            })
            .collect(),
        ArrayCell::Timestamp(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |dt| {
                    ClickHouseValue::DateTime64(dt.and_utc().timestamp_micros())
                })
            })
            .collect(),
        ArrayCell::TimestampTz(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |dt| {
                    ClickHouseValue::DateTime64(dt.timestamp_micros())
                })
            })
            .collect(),
        ArrayCell::Uuid(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |u| {
                    ClickHouseValue::Uuid(*u.as_bytes())
                })
            })
            .collect(),
        ArrayCell::Json(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |j| {
                    ClickHouseValue::String(j.to_string())
                })
            })
            .collect(),
        ArrayCell::Bytes(v) => v
            .into_iter()
            .map(|o| {
                o.map_or(ClickHouseValue::Null, |b| {
                    ClickHouseValue::String(bytes_to_hex(&b))
                })
            })
            .collect(),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

// ── Destination struct ────────────────────────────────────────────────────────

/// CDC-capable ClickHouse destination that replicates Postgres tables.
///
/// Uses append-only MergeTree tables with two CDC columns (`cdc_operation`, `cdc_lsn`)
/// appended to each row. Rows are encoded as RowBinary and sent via
/// `INSERT INTO "table" FORMAT RowBinary` — no column-name header required.
///
/// The struct is cheaply cloneable: `client` has an internal `Arc`, and `table_cache`
/// is wrapped in `Arc<RwLock<…>>`.
#[derive(Clone)]
pub struct ClickHouseDestination<S> {
    client: Client,
    inserter_config: Arc<ClickHouseInserterConfig>,
    store: S,
    /// Cache: ClickHouse table name → `Arc<[bool]>` (nullable flags per column,
    /// including the two trailing CDC columns which are always `false`).
    ///
    /// `std::sync::RwLock` is appropriate here: both reads (hot path) and writes (rare,
    /// only on first encounter of a new table) are brief in-memory operations. The lock
    /// is always released before any `.await` point (DDL is executed with no lock held),
    /// so the async `tokio::sync::RwLock` would be unnecessary overhead.
    table_cache: Arc<RwLock<HashMap<String, Arc<[bool]>>>>,
}

impl<S> ClickHouseDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new `ClickHouseDestination`.
    ///
    /// When using an `https://` URL, TLS is handled automatically by the `rustls-tls`
    /// feature using webpki root certificates.
    pub fn new(
        url: impl Into<String>,
        user: impl Into<String>,
        password: Option<String>,
        database: impl Into<String>,
        inserter_config: ClickHouseInserterConfig,
        store: S,
    ) -> EtlResult<Self> {
        register_metrics();
        let client = build_client(url.into(), user.into(), password, database.into());
        Ok(Self {
            client,
            inserter_config: Arc::new(inserter_config),
            store,
            table_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Creates a new `ClickHouseDestination` with TLS using a custom CA certificate.
    ///
    /// Note: The `clickhouse` crate v0.14 does not expose a public API for configuring
    /// custom CA certificates. For HTTPS with standard TLS (webpki roots), use
    /// [`ClickHouseDestination::new`] with an `https://` URL. Custom CA certificate
    /// support is planned for a future update when the crate exposes the necessary API.
    #[allow(dead_code)]
    pub fn new_with_tls(
        _url: impl Into<String>,
        _user: impl Into<String>,
        _password: Option<String>,
        _database: impl Into<String>,
        _ca_cert_pem: String,
        _inserter_config: ClickHouseInserterConfig,
        _store: S,
    ) -> EtlResult<Self> {
        bail!(
            ErrorKind::Unknown,
            "Custom CA certificates not supported",
            "The clickhouse crate v0.14 does not expose an API for custom CA certificates. \
             Use ClickHouseDestination::new() with an https:// URL for standard TLS \
             (webpki root certificates are used for server verification)."
        )
    }

    /// Ensures the ClickHouse table for `table_id` exists, returning
    /// `(ch_table_name, nullable_flags)`.
    ///
    /// Uses a two-phase locking strategy:
    /// 1. Fast-path read (no await) → return cached entry if present.
    /// 2. Slow-path: compute DDL, run `CREATE TABLE IF NOT EXISTS` (await, no lock held),
    ///    then write-lock to insert (using `or_insert` for the concurrent first-writer race).
    async fn ensure_table_exists(&self, table_id: TableId) -> EtlResult<(String, Arc<[bool]>)> {
        // 1. Get table schema from store.
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

        // 2. Determine / persist ClickHouse table name.
        let ch_table_name = {
            if let Some(name) = self.store.get_table_mapping(&table_id).await? {
                name
            } else {
                let name = table_name_to_clickhouse_table_name(
                    &table_schema.name.schema,
                    &table_schema.name.name,
                );
                self.store
                    .store_table_mapping(table_id, name.clone())
                    .await?;
                name
            }
        };

        // 3. Fast-path cache check (no await).
        {
            let guard = self.table_cache.read().unwrap();
            if let Some(flags) = guard.get(&ch_table_name) {
                return Ok((ch_table_name, Arc::clone(flags)));
            }
        }

        // 4. Compute nullable flags (user columns + 2 CDC columns always non-nullable).
        let column_schemas = &table_schema.column_schemas;
        let mut nullable_flags_vec: Vec<bool> = column_schemas.iter().map(|c| c.nullable).collect();
        nullable_flags_vec.push(false); // cdc_operation
        nullable_flags_vec.push(false); // cdc_lsn
        let nullable_flags: Arc<[bool]> = nullable_flags_vec.into();

        // 5. Build and execute DDL (no lock held during this await).
        let ddl = build_create_table_sql(&ch_table_name, column_schemas);
        let ddl_start = Instant::now();
        self.client.query(&ddl).execute().await.map_err(|e| {
            etl_error!(
                ErrorKind::Unknown,
                "ClickHouse DDL failed",
                format!("Failed to create table '{ch_table_name}': {e}")
            )
        })?;
        metrics::histogram!(ETL_CH_DDL_DURATION_SECONDS, "table" => ch_table_name.clone())
            .record(ddl_start.elapsed().as_secs_f64());

        // 6. Write-lock: insert, using or_insert to handle concurrent first-writer race.
        let stored_flags = {
            let mut guard = self.table_cache.write().unwrap();
            Arc::clone(
                guard
                    .entry(ch_table_name.clone())
                    .or_insert_with(|| Arc::clone(&nullable_flags)),
            )
        };

        Ok((ch_table_name, stored_flags))
    }

    async fn truncate_table_inner(&self, table_id: TableId) -> EtlResult<()> {
        let (ch_table_name, _) = self.ensure_table_exists(table_id).await?;
        self.client
            .query(&format!("TRUNCATE TABLE IF EXISTS \"{ch_table_name}\""))
            .execute()
            .await
            .map_err(|e| {
                etl_error!(
                    ErrorKind::Unknown,
                    "ClickHouse truncate failed",
                    format!("Failed to truncate table '{ch_table_name}': {e}")
                )
            })
    }

    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let (ch_table_name, nullable_flags) = self.ensure_table_exists(table_id).await?;
        let sql = format!("INSERT INTO \"{ch_table_name}\" FORMAT RowBinary");
        let max_rows = self.inserter_config.max_rows_per_insert;
        let max_bytes = self.inserter_config.max_bytes_per_insert;

        let mut insert = self
            .client
            .insert_formatted_with(sql.clone())
            .buffered_with_capacity(256 * 1024);
        let mut rows = 0u64;
        let mut bytes = 0u64;
        let mut row_buf = Vec::new();
        let mut insert_start = Instant::now();

        for table_row in table_rows {
            row_buf.clear();
            let mut values: Vec<ClickHouseValue> = table_row
                .into_values()
                .into_iter()
                .map(cell_to_clickhouse_value)
                .collect();
            values.push(ClickHouseValue::String("INSERT".to_string()));
            values.push(ClickHouseValue::Int64(0));
            rb_encode_row(&values, &nullable_flags, &mut row_buf);

            insert.write_buffered(&row_buf);
            rows += 1;
            bytes += row_buf.len() as u64;

            if rows >= max_rows || bytes >= max_bytes {
                insert.end().await.map_err(|e| {
                    etl_error!(
                        ErrorKind::Unknown,
                        "ClickHouse insert flush failed",
                        format!("Failed to flush INSERT for '{ch_table_name}': {e}")
                    )
                })?;
                metrics::histogram!(
                    ETL_CH_INSERT_DURATION_SECONDS,
                    "table" => ch_table_name.clone(),
                    "source" => "copy"
                )
                .record(insert_start.elapsed().as_secs_f64());
                insert = self
                    .client
                    .insert_formatted_with(sql.clone())
                    .buffered_with_capacity(256 * 1024);
                insert_start = Instant::now();
                rows = 0;
                bytes = 0;
            }
        }

        insert.end().await.map_err(|e| {
            etl_error!(
                ErrorKind::Unknown,
                "ClickHouse insert flush failed",
                format!("Failed to flush INSERT for '{ch_table_name}': {e}")
            )
        })?;
        metrics::histogram!(
            ETL_CH_INSERT_DURATION_SECONDS,
            "table" => ch_table_name.clone(),
            "source" => "copy"
        )
        .record(insert_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Processes events in passes driven by an outer loop that runs until the iterator
    /// is exhausted. Each pass:
    /// 1. Accumulates Insert/Update/Delete rows per table until a Truncate (or end).
    /// 2. Writes those rows concurrently.
    /// 3. Drains consecutive Truncate events (deduplicated) and executes them.
    ///
    /// Breaking at a Truncate never skips events — the outer loop resumes from that
    /// position, so rows accumulated before the Truncate are flushed first, then the
    /// Truncate fires, then subsequent events (including inserts on the same table)
    /// are processed in the next pass.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            // Accumulate non-truncate events grouped by table_id.
            let mut table_id_to_rows: HashMap<TableId, Vec<PendingRow>> = HashMap::new();

            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(insert) => {
                        table_id_to_rows
                            .entry(insert.table_id)
                            .or_default()
                            .push(PendingRow {
                                operation: CdcOperation::Insert,
                                lsn: insert.commit_lsn,
                                cells: insert.table_row.into_values(),
                            });
                    }
                    Event::Update(update) => {
                        table_id_to_rows
                            .entry(update.table_id)
                            .or_default()
                            .push(PendingRow {
                                operation: CdcOperation::Update,
                                lsn: update.commit_lsn,
                                cells: update.table_row.into_values(),
                            });
                    }
                    Event::Delete(delete) => {
                        let Some((_, old_row)) = delete.old_table_row else {
                            info!("delete event has no row data, skipping");
                            continue;
                        };
                        table_id_to_rows
                            .entry(delete.table_id)
                            .or_default()
                            .push(PendingRow {
                                operation: CdcOperation::Delete,
                                lsn: delete.commit_lsn,
                                cells: old_row.into_values(),
                            });
                    }
                    event => {
                        debug!(
                            event_type = %event.event_type(),
                            "skipping unsupported event type"
                        );
                    }
                }
            }

            // Write accumulated rows concurrently, one JoinSet task per table.
            if !table_id_to_rows.is_empty() {
                // Phase 1: ensure all tables exist (must happen outside JoinSet spawns
                // since ensure_table_exists holds &self which is not 'static).
                let mut table_meta: HashMap<TableId, (String, Arc<[bool]>)> = HashMap::new();
                for &table_id in table_id_to_rows.keys() {
                    let (name, flags) = self.ensure_table_exists(table_id).await?;
                    table_meta.insert(table_id, (name, flags));
                }

                // Phase 2: spawn concurrent writers with pre-resolved metadata.
                // Only the ClickHouse Client (cheaply cloneable, 'static) goes into spawn.
                let mut join_set: JoinSet<EtlResult<()>> = JoinSet::new();
                for (table_id, row_data) in table_id_to_rows {
                    let (ch_table_name, nullable_flags) = table_meta.remove(&table_id).unwrap();
                    let client = self.client.clone();
                    let max_rows = self.inserter_config.max_rows_per_insert;
                    let max_bytes = self.inserter_config.max_bytes_per_insert;

                    join_set.spawn(async move {
                        let sql = format!("INSERT INTO \"{ch_table_name}\" FORMAT RowBinary");
                        let mut insert = client
                            .insert_formatted_with(sql.clone())
                            .buffered_with_capacity(256 * 1024);
                        let mut rows = 0u64;
                        let mut bytes = 0u64;
                        let mut row_buf = Vec::new();
                        let mut insert_start = Instant::now();

                        for PendingRow {
                            operation,
                            lsn,
                            cells,
                        } in row_data
                        {
                            row_buf.clear();
                            let mut values: Vec<ClickHouseValue> =
                                cells.into_iter().map(cell_to_clickhouse_value).collect();
                            values.push(ClickHouseValue::String(operation.as_str().to_string()));
                            values.push(ClickHouseValue::Int64(i64::try_from(u64::from(lsn)).inspect_err(|error| {
                                tracing::error!(?error, "cannot convert u64 value to i64 for clickhouse destination, fallback to max i64");
                            }).unwrap_or(i64::MAX)));
                            rb_encode_row(&values, &nullable_flags, &mut row_buf);

                            insert.write_buffered(&row_buf);
                            rows += 1;
                            bytes += row_buf.len() as u64;

                            if rows >= max_rows || bytes >= max_bytes {
                                insert.end().await.map_err(|e| {
                                    etl_error!(
                                        ErrorKind::Unknown,
                                        "ClickHouse insert flush failed",
                                        format!(
                                            "Failed to flush INSERT for '{ch_table_name}': {e}"
                                        )
                                    )
                                })?;
                                metrics::histogram!(
                                    ETL_CH_INSERT_DURATION_SECONDS,
                                    "table" => ch_table_name.clone(),
                                    "source" => "streaming"
                                )
                                .record(insert_start.elapsed().as_secs_f64());
                                insert = client
                                    .insert_formatted_with(sql.clone())
                                    .buffered_with_capacity(256 * 1024);
                                insert_start = Instant::now();
                                rows = 0;
                                bytes = 0;
                            }
                        }

                        insert.end().await.map_err(|e| {
                            etl_error!(
                                ErrorKind::Unknown,
                                "ClickHouse insert flush failed",
                                format!("Failed to flush INSERT for '{ch_table_name}': {e}")
                            )
                        })?;
                        metrics::histogram!(
                            ETL_CH_INSERT_DURATION_SECONDS,
                            "table" => ch_table_name.clone(),
                            "source" => "streaming"
                        )
                        .record(insert_start.elapsed().as_secs_f64());

                        Ok(())
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    result
                        .map_err(|_| etl_error!(ErrorKind::Unknown, "Failed to join future"))??;
                }
            }

            // Collect and deduplicate truncate events.
            let mut truncate_table_ids = HashSet::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for table_id in truncate_event.rel_ids {
                        truncate_table_ids.insert(TableId::new(table_id));
                    }
                }
            }

            for table_id in truncate_table_ids {
                self.truncate_table_inner(table_id).await?;
            }
        }

        Ok(())
    }
}

impl<S> Destination for ClickHouseDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "clickhouse"
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

// ── Client builder ────────────────────────────────────────────────────────────

fn build_client(url: String, user: String, password: Option<String>, database: String) -> Client {
    let mut client = Client::default()
        .with_url(url)
        .with_user(user)
        .with_database(database);

    if let Some(pw) = password {
        client = client.with_password(pw);
    }

    client
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use uuid::Uuid;

    #[test]
    fn test_cell_to_clickhouse_value_null() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Null),
            ClickHouseValue::Null
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_bool() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Bool(true)),
            ClickHouseValue::Bool(true)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_i32() {
        assert!(matches!(
            cell_to_clickhouse_value(Cell::I32(42)),
            ClickHouseValue::Int32(42)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_string() {
        if let ClickHouseValue::String(s) =
            cell_to_clickhouse_value(Cell::String("hello".to_string()))
        {
            assert_eq!(s, "hello");
        } else {
            panic!("expected String variant");
        }
    }

    #[test]
    fn test_cell_to_clickhouse_value_date() {
        // 1970-01-01 = day 0
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(epoch)),
            ClickHouseValue::Date(0)
        ));

        // 1970-01-02 = day 1
        let day1 = NaiveDate::from_ymd_opt(1970, 1, 2).unwrap();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Date(day1)),
            ClickHouseValue::Date(1)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_timestamp() {
        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        assert!(matches!(
            cell_to_clickhouse_value(Cell::Timestamp(epoch)),
            ClickHouseValue::DateTime64(0)
        ));
    }

    #[test]
    fn test_cell_to_clickhouse_value_uuid() {
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let expected_bytes = *u.as_bytes();
        if let ClickHouseValue::Uuid(bytes) = cell_to_clickhouse_value(Cell::Uuid(u)) {
            assert_eq!(bytes, expected_bytes);
        } else {
            panic!("expected Uuid variant");
        }
    }

    #[test]
    fn test_cell_to_clickhouse_value_bytes_hex() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        if let ClickHouseValue::String(s) = cell_to_clickhouse_value(Cell::Bytes(bytes)) {
            assert_eq!(s, "deadbeef");
        } else {
            panic!("expected String variant");
        }
    }

    #[test]
    fn test_rb_encode_value_scalars() {
        let mut buf = Vec::new();

        buf.clear();
        rb_encode_value(&ClickHouseValue::Bool(true), &mut buf);
        assert_eq!(buf, [1u8]);

        buf.clear();
        rb_encode_value(&ClickHouseValue::Int32(-1), &mut buf);
        assert_eq!(buf, (-1i32).to_le_bytes());

        buf.clear();
        rb_encode_value(&ClickHouseValue::String("hi".to_string()), &mut buf);
        assert_eq!(buf, [2, b'h', b'i']); // varint(2) + bytes

        buf.clear();
        rb_encode_value(&ClickHouseValue::Date(1), &mut buf);
        assert_eq!(buf, 1u16.to_le_bytes());
    }

    #[test]
    fn test_rb_encode_uuid_wire_format() {
        // UUID 550e8400-e29b-41d4-a716-446655440000
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let val = ClickHouseValue::Uuid(*u.as_bytes());
        let mut buf = Vec::new();
        rb_encode_value(&val, &mut buf);

        assert_eq!(buf.len(), 16);
        // high u64 from bytes 0-7, written LE
        let bytes = u.as_bytes();
        let high = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let low = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let mut expected = high.to_le_bytes().to_vec();
        expected.extend_from_slice(&low.to_le_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_rb_encode_nullable() {
        let mut buf = Vec::new();

        // Null → just the 1 byte
        rb_encode_nullable(&ClickHouseValue::Null, &mut buf);
        assert_eq!(buf, [1u8]);

        buf.clear();
        rb_encode_nullable(&ClickHouseValue::Int32(42), &mut buf);
        let mut expected = vec![0u8]; // not-null indicator
        expected.extend_from_slice(&42i32.to_le_bytes());
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_rb_varint() {
        let mut buf = Vec::new();
        rb_varint(0, &mut buf);
        assert_eq!(buf, [0x00]);

        buf.clear();
        rb_varint(127, &mut buf);
        assert_eq!(buf, [0x7f]);

        buf.clear();
        rb_varint(128, &mut buf);
        assert_eq!(buf, [0x80, 0x01]);

        buf.clear();
        rb_varint(300, &mut buf);
        assert_eq!(buf, [0xac, 0x02]); // 300 = 0b100101100 → [0x2c | 0x80, 0x02]
    }

    #[test]
    fn test_nullable_flags_includes_cdc() {
        let mut all_flags: Vec<bool> = vec![true, false];
        all_flags.push(false); // cdc_operation
        all_flags.push(false); // cdc_lsn

        assert_eq!(all_flags.len(), 4);
        assert!(all_flags[0]);
        assert!(!all_flags[1]);
        assert!(!all_flags[2]);
        assert!(!all_flags[3]);
    }

    #[test]
    fn test_bytes_to_hex() {
        assert_eq!(bytes_to_hex(&[]), "");
        assert_eq!(bytes_to_hex(&[0x00]), "00");
        assert_eq!(bytes_to_hex(&[0xff]), "ff");
        assert_eq!(bytes_to_hex(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }
}
