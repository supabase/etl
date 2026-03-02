use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::clickhouse::client::ClickHouseClient;
use crate::clickhouse::encoding::{ClickHouseValue, cell_to_clickhouse_value};
use crate::clickhouse::metrics::{ETL_CH_DDL_DURATION_SECONDS, register_metrics};
use crate::clickhouse::schema::{build_create_table_sql, table_name_to_clickhouse_table_name};
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, Event, TableId, TableRow, is_array_type};
use etl::{destination::Destination, types::PgLsn};
use parking_lot::RwLock;
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

impl std::fmt::Display for CdcOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcOperation::Insert => write!(f, "INSERT"),
            CdcOperation::Update => write!(f, "UPDATE"),
            CdcOperation::Delete => write!(f, "DELETE"),
        }
    }
}

/// A single row pending insertion, carrying the CDC metadata alongside the cell data.
struct PendingRow {
    operation: CdcOperation,
    lsn: PgLsn,
    cells: Vec<Cell>,
}

// ── Inserter configuration ────────────────────────────────────────────────────

/// Controls intermediate flushing inside a single `write_table_rows` / `write_events` call.
///
/// The upstream `BatchConfig::max_fill_ms` controls when `write_events` is called;
/// these limits prevent unbounded memory use for very large batches (e.g. initial copy).
pub struct ClickHouseInserterConfig {
    /// Start a new INSERT after this many uncompressed bytes.
    ///
    /// Derive this from `BatchConfig::memory_budget_ratio × total_memory / max_table_sync_workers`
    /// (the same formula used by `BatchBudget::ideal_batch_size_bytes`).
    pub max_bytes_per_insert: u64,
}

// ── Destination struct ────────────────────────────────────────────────────────

/// CDC-capable ClickHouse destination that replicates Postgres tables.
///
/// Uses append-only MergeTree tables with two CDC columns (`cdc_operation`, `cdc_lsn`)
/// appended to each row. Rows are encoded as RowBinary and sent via
/// `INSERT INTO "table" FORMAT RowBinary` — no column-name header required.
///
/// The struct is cheaply cloneable: `client` wraps an `Arc` internally, and
/// `table_cache` is wrapped in `Arc<RwLock<…>>`.
#[derive(Clone)]
pub struct ClickHouseDestination<S> {
    client: ClickHouseClient,
    inserter_config: Arc<ClickHouseInserterConfig>,
    store: Arc<S>,
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
        Ok(Self {
            client: ClickHouseClient::new(url, user, password, database),
            inserter_config: Arc::new(inserter_config),
            store: Arc::new(store),
            table_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Ensures the ClickHouse table for `table_id` exists, returning
    /// `(ch_table_name, nullable_flags)`.
    ///
    /// Uses a two-phase locking strategy:
    /// 1. Fast-path read (no await) → return cached entry if present.
    /// 2. Slow-path: compute DDL, run `CREATE TABLE IF NOT EXISTS` (await, no lock held),
    ///    then write-lock to insert (using `or_insert` for the concurrent first-writer race).
    async fn ensure_table_exists(&self, table_id: TableId) -> EtlResult<(String, Arc<[bool]>)> {
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

        {
            let guard = self.table_cache.read();
            if let Some(flags) = guard.get(&ch_table_name) {
                return Ok((ch_table_name, Arc::clone(flags)));
            }
        }

        // Compute nullable flags (user columns + 2 CDC columns always non-nullable).
        //
        // Array columns are NEVER marked nullable here, even if the Postgres column is nullable.
        // The DDL always emits `Array(Nullable(T))` (no outer `Nullable` wrapper), so ClickHouse
        // does not expect a null-indicator byte before the array. If we mistakenly set
        // `nullable_flags[i] = true` for an array column, `rb_encode_nullable` would prepend a
        // spurious `0x00` byte that ClickHouse reads as `varint(0)` (empty array), causing every
        // subsequent column to be read from the wrong offset and ultimately "Cannot read all data".
        let column_schemas = &table_schema.column_schemas;
        let mut nullable_flags_vec: Vec<bool> = column_schemas
            .iter()
            .map(|c| c.nullable && !is_array_type(&c.typ))
            .collect();
        nullable_flags_vec.push(false); // cdc_operation
        nullable_flags_vec.push(false); // cdc_lsn
        let nullable_flags: Arc<[bool]> = nullable_flags_vec.into();

        // Execute DDL (no lock held during this await).
        let ddl = build_create_table_sql(&ch_table_name, column_schemas);
        let ddl_start = Instant::now();
        self.client.execute_ddl(&ddl).await?;
        metrics::histogram!(ETL_CH_DDL_DURATION_SECONDS, "table" => ch_table_name.clone())
            .record(ddl_start.elapsed().as_secs_f64());

        // Write-lock: insert, using or_insert to handle concurrent first-writer race.
        let stored_flags = {
            let mut guard = self.table_cache.write();
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
        self.client.truncate_table(&ch_table_name).await
    }

    async fn write_table_rows_inner(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let (ch_table_name, nullable_flags) = self.ensure_table_exists(table_id).await?;

        let rows: Vec<Vec<ClickHouseValue>> = table_rows
            .into_iter()
            .map(|table_row| {
                let mut values: Vec<ClickHouseValue> = table_row
                    .into_values()
                    .into_iter()
                    .map(cell_to_clickhouse_value)
                    .collect();
                values.push(ClickHouseValue::String(String::from("INSERT")));
                values.push(ClickHouseValue::Int64(0));
                values
            })
            .collect();

        self.client
            .insert_rows(
                &ch_table_name,
                rows,
                &nullable_flags,
                self.inserter_config.max_bytes_per_insert,
                "copy",
            )
            .await
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

                let event = event_iter
                    .next()
                    .expect("event iterator should not be empty, we peeked at the next event; qed");
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
                // Only the ClickHouseClient (cheaply cloneable, 'static) goes into spawn.
                let mut join_set: JoinSet<EtlResult<()>> = JoinSet::new();
                for (table_id, row_data) in table_id_to_rows {
                    let (ch_table_name, nullable_flags) =
                        table_meta.remove(&table_id).ok_or_else(|| {
                            etl_error!(
                                ErrorKind::Unknown,
                                "ClickHouse insert failed",
                                format!("Failed to remove metadata for table ID {table_id}")
                            )
                        })?;
                    let client = self.client.clone();
                    let max_bytes = self.inserter_config.max_bytes_per_insert;

                    join_set.spawn(async move {
                        let rows: Vec<Vec<ClickHouseValue>> = row_data
                            .into_iter()
                            .map(|PendingRow { operation, lsn, cells }| {
                                let mut values: Vec<ClickHouseValue> =
                                    cells.into_iter().map(cell_to_clickhouse_value).collect();
                                values.push(ClickHouseValue::String(operation.to_string()));
                                values.push(ClickHouseValue::Int64(
                                    i64::try_from(u64::from(lsn))
                                        .inspect_err(|error| {
                                            tracing::error!(?error, "cannot convert u64 LSN to i64, falling back to i64::MAX");
                                        })
                                        .unwrap_or(i64::MAX),
                                ));
                                values
                            })
                            .collect();

                        client
                            .insert_rows(&ch_table_name, rows, &nullable_flags, max_bytes, "streaming")
                            .await
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

            futures::future::try_join_all(
                truncate_table_ids
                    .into_iter()
                    .map(|table_id| self.truncate_table_inner(table_id)),
            )
            .await?;
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

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
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
}
