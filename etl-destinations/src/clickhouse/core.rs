use std::{collections::HashMap, sync::Arc, time::Instant};

use etl::{
    bail,
    destination::{
        Destination,
        async_result::{TruncateTableResult, WriteEventsResult, WriteTableRowsResult},
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    state::destination_metadata::{DestinationTableMetadata, DestinationTableSchemaStatus},
    store::{schema::SchemaStore, state::StateStore},
    types::{
        Cell, Event, OldTableRow, PgLsn, ReplicatedTableSchema, SchemaDiff, TableId, TableRow,
        Type, UpdatedTableRow, is_array_type,
    },
};
use parking_lot::RwLock;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use url::Url;

use crate::clickhouse::{
    client::{ClickHouseClient, ClickHouseTableColumn},
    encoding::{ClickHouseValue, cell_to_clickhouse_value},
    metrics::{ETL_CH_DDL_DURATION_SECONDS, register_metrics},
    schema::{
        CDC_LSN_COLUMN_NAME, CDC_OPERATION_COLUMN_NAME, build_create_table_sql,
        table_name_to_clickhouse_table_name,
    },
};

/// Postgres CDC operation kind, written to the `cdc_operation` column.
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

/// A row pending insertion with its CDC metadata.
struct PendingRow {
    /// CDC op kind, written into the `cdc_operation` column.
    operation: CdcOperation,
    /// Commit LSN of the source transaction, written into the `cdc_lsn`
    /// column.
    lsn: PgLsn,
    /// User column values in source schema order. The two CDC columns
    /// (`cdc_operation`, `cdc_lsn`) are appended at encode time and are
    /// not present here.
    cells: Vec<Cell>,
}

/// Converts a Postgres LSN into the ClickHouse CDC LSN value.
fn cdc_lsn_to_clickhouse_value(lsn: PgLsn) -> ClickHouseValue {
    ClickHouseValue::UInt64(u64::from(lsn))
}

/// Returns true if the ClickHouse type has an outer Nullable wrapper.
fn clickhouse_type_expects_nullable_marker(type_name: &str) -> bool {
    type_name.starts_with("Nullable(")
}

/// Returns expected ClickHouse column names for a replicated schema.
fn expected_clickhouse_column_names(schema: &ReplicatedTableSchema) -> Vec<String> {
    let mut names: Vec<String> =
        schema.column_schemas().map(|column| column.name.clone()).collect();
    names.push(CDC_OPERATION_COLUMN_NAME.to_string());
    names.push(CDC_LSN_COLUMN_NAME.to_string());
    names
}

/// Derives RowBinary nullable flags from the actual ClickHouse table schema.
fn nullable_flags_from_clickhouse_columns(
    ch_table_name: &str,
    expected_column_names: &[String],
    actual_columns: &[ClickHouseTableColumn],
) -> EtlResult<Arc<[bool]>> {
    if actual_columns.len() != expected_column_names.len() {
        return Err(etl_error!(
            ErrorKind::CorruptedTableSchema,
            "ClickHouse table schema does not match replicated schema",
            format!(
                "table '{}' has {} columns, but {} were expected",
                ch_table_name,
                actual_columns.len(),
                expected_column_names.len()
            )
        ));
    }

    let mut nullable_flags = Vec::with_capacity(actual_columns.len());
    for (index, (actual_column, expected_name)) in
        actual_columns.iter().zip(expected_column_names).enumerate()
    {
        if actual_column.name != *expected_name {
            return Err(etl_error!(
                ErrorKind::CorruptedTableSchema,
                "ClickHouse table schema does not match replicated schema",
                format!(
                    "table '{}' column {} is named '{}', but '{}' was expected",
                    ch_table_name,
                    index + 1,
                    actual_column.name,
                    expected_name
                )
            ));
        }

        nullable_flags.push(clickhouse_type_expects_nullable_marker(&actual_column.type_name));
    }

    Ok(nullable_flags.into())
}

/// Controls intermediate flushing inside a single `write_table_rows` /
/// `write_events` call.
///
/// The upstream `BatchConfig::max_fill_ms` controls when `write_events` is
/// called; these limits prevent unbounded memory use for very large batches
/// (e.g. initial copy).
pub struct ClickHouseInserterConfig {
    /// Start a new INSERT after this many uncompressed bytes.
    ///
    /// Derive this from `BatchConfig::memory_budget_ratio * total_memory /
    /// max_table_sync_workers` (the same formula used by
    /// `BatchBudget::ideal_batch_size_bytes`).
    pub max_bytes_per_insert: u64,
}

/// CDC-capable ClickHouse destination that replicates Postgres tables.
///
/// Uses append-only MergeTree tables with two CDC columns (`cdc_operation`,
/// `cdc_lsn`) appended to each row. Rows are encoded as RowBinary and sent via
/// `INSERT INTO "table" FORMAT RowBinary` -- no column-name header required.
#[derive(Clone)]
pub struct ClickHouseDestination<S> {
    /// HTTP client used for all DDL and RowBinary INSERT traffic.
    client: ClickHouseClient,
    /// Per-INSERT byte budget; gates intermediate flushes within a single
    /// `write_table_rows` / `write_events` call.
    inserter_config: Arc<ClickHouseInserterConfig>,
    /// Schema/state store used to persist destination table metadata
    /// (Applying / Applied) and to look up replicated schemas.
    store: Arc<S>,
    /// ClickHouse table name -> per-column nullable flags (in column order,
    /// including the two trailing CDC columns which are always `false`).
    ///
    /// Populated lazily on first encounter of a table and consulted on the
    /// hot insert path. `std::sync::RwLock` is sufficient: every critical
    /// section is a brief in-memory map op with no `.await` inside, so the
    /// async `tokio::sync::RwLock` would be needless overhead.
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
        url: Url,
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

    /// Creates a ClickHouse table for a never-before-seen `table_id`,
    /// bracketing the DDL with `DestinationTableMetadata` writes so the
    /// operation is crash-recoverable.
    ///
    /// Sequence:
    /// 1. Persist `Applying` metadata (so a crash between this write and step 3
    ///    leaves a marker that lets restart logic detect the interrupted
    ///    operation).
    /// 2. Execute `CREATE TABLE IF NOT EXISTS` against ClickHouse.
    /// 3. Persist `Applied` metadata.
    ///
    /// Recovery is handled by `ensure_table_exists`: on restart, an
    /// `Applying` row signals that the previous run died mid-creation, so
    /// it re-runs the idempotent DDL and transitions the metadata to
    /// `Applied` itself.
    async fn create_table_with_metadata(
        &self,
        table_id: TableId,
        ch_table_name: &str,
        schema: &ReplicatedTableSchema,
        snapshot_id: etl::types::SnapshotId,
        replication_mask: etl::types::ReplicationMask,
    ) -> EtlResult<()> {
        let metadata = DestinationTableMetadata::new_applying(
            ch_table_name.to_string(),
            snapshot_id,
            replication_mask,
        );
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;

        let column_schemas: Vec<_> = schema.column_schemas().cloned().collect();
        let ddl = build_create_table_sql(ch_table_name, &column_schemas);
        let ddl_start = Instant::now();
        self.client.execute_ddl(&ddl).await?;
        metrics::histogram!(ETL_CH_DDL_DURATION_SECONDS, "table" => ch_table_name.to_string())
            .record(ddl_start.elapsed().as_secs_f64());

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;

        Ok(())
    }

    /// Ensures the ClickHouse table for the given schema exists, returning
    /// `(ch_table_name, nullable_flags)`.
    ///
    /// On first encounter, executes `CREATE TABLE IF NOT EXISTS` and stores
    /// destination metadata with `Applied` status. Subsequent calls return
    /// the cached result.
    async fn ensure_table_exists(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<(String, Arc<[bool]>)> {
        let table_name = schema.name();
        let ch_table_name =
            table_name_to_clickhouse_table_name(&table_name.schema, &table_name.name);

        if let Some(flags) = self.table_cache.read().get(&ch_table_name).cloned() {
            return Ok((ch_table_name, flags));
        }

        let table_id = schema.id();
        match self.store.get_destination_table_metadata(table_id).await? {
            None => {
                self.create_table_with_metadata(
                    table_id,
                    &ch_table_name,
                    schema,
                    schema.inner().snapshot_id,
                    schema.replication_mask().clone(),
                )
                .await?;
            }
            Some(metadata) if metadata.is_applying() => {
                self.recover_applying_metadata(table_id, &ch_table_name, schema, metadata).await?;
            }
            // Applied metadata, cache miss after `handle_relation_event`
            // invalidated the cache. No DDL needed.
            Some(_applied) => {}
        }

        // Compute nullable flags from the actual ClickHouse schema. This matters after
        // `ALTER TABLE ADD COLUMN`: ClickHouse scalar columns are forced to
        // `Nullable(T)` even when the Postgres column is `NOT NULL`, so RowBinary must
        // include the nullable marker byte ClickHouse expects.
        let actual_columns = self.client.table_columns(&ch_table_name).await?;
        let expected_column_names = expected_clickhouse_column_names(schema);
        let nullable_flags = nullable_flags_from_clickhouse_columns(
            &ch_table_name,
            &expected_column_names,
            &actual_columns,
        )?;

        // `or_insert_with` handles the race where a concurrent caller populated
        // the entry between our read-miss and this write.
        let flags = {
            let mut guard = self.table_cache.write();
            Arc::clone(
                guard.entry(ch_table_name.clone()).or_insert_with(|| Arc::clone(&nullable_flags)),
            )
        };

        Ok((ch_table_name, flags))
    }

    /// Re-runs an interrupted DDL idempotently and transitions metadata to
    /// `Applied`. Distinguishes between an interrupted schema change (replays
    /// the diff against the previous snapshot) and an interrupted initial
    /// creation (re-issues `CREATE TABLE IF NOT EXISTS`).
    async fn recover_applying_metadata(
        &self,
        table_id: TableId,
        ch_table_name: &str,
        schema: &ReplicatedTableSchema,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        warn!("table {} has Applying metadata, recovering interrupted operation", table_id);

        match metadata.previous_snapshot_id {
            Some(prev_snapshot_id) => {
                let old_table_schema =
                    self.store.get_table_schema(&table_id, prev_snapshot_id).await?.ok_or_else(
                        || {
                            etl_error!(
                                ErrorKind::InvalidState,
                                "Old schema not found for recovery",
                                format!(
                                    "Cannot find schema for table {} at snapshot_id {}",
                                    table_id, prev_snapshot_id
                                )
                            )
                        },
                    )?;
                let old_schema = ReplicatedTableSchema::from_mask(
                    old_table_schema,
                    metadata.replication_mask.clone(),
                );
                let diff = old_schema.diff(schema);
                self.apply_schema_diff(ch_table_name, &diff, &old_schema).await?;
            }
            None => {
                let column_schemas: Vec<_> = schema.column_schemas().cloned().collect();
                let ddl = build_create_table_sql(ch_table_name, &column_schemas);
                self.client.execute_ddl(&ddl).await?;
            }
        }

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        Ok(())
    }

    async fn truncate_table_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let (ch_table_name, _) = self.ensure_table_exists(schema).await?;
        self.client.truncate_table(&ch_table_name).await
    }

    async fn write_table_rows_inner(
        &self,
        schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let (ch_table_name, nullable_flags) = self.ensure_table_exists(schema).await?;

        let rows: Vec<Vec<ClickHouseValue>> = table_rows
            .into_iter()
            .map(|table_row| {
                let mut values: Vec<ClickHouseValue> =
                    table_row.into_values().into_iter().map(cell_to_clickhouse_value).collect();
                values.push(ClickHouseValue::String(String::from("INSERT")));
                values.push(ClickHouseValue::UInt64(0));
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

    /// Handles a schema change event (Relation) by computing the diff and
    /// applying ALTER TABLE statements.
    async fn handle_relation_event(&self, new_schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let table_id = new_schema.id();
        let new_snapshot_id = new_schema.inner().snapshot_id;
        let new_replication_mask = new_schema.replication_mask().clone();

        let Some(metadata) = self.store.get_applied_destination_table_metadata(table_id).await?
        else {
            bail!(
                ErrorKind::CorruptedTableSchema,
                "Missing destination table metadata",
                format!(
                    "No destination table metadata found for table {} when processing schema \
                     change. The metadata should have been recorded during initial table \
                     synchronization.",
                    table_id
                )
            );
        };

        let current_snapshot_id = metadata.snapshot_id;
        let current_replication_mask = metadata.replication_mask.clone();

        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            info!("schema for table {} unchanged (snapshot_id: {})", table_id, new_snapshot_id);
            return Ok(());
        }

        info!(
            "schema change detected for table {}: snapshot_id {} -> {}",
            table_id, current_snapshot_id, new_snapshot_id
        );

        // Retrieve the old schema to compute the diff.
        let current_table_schema =
            self.store.get_table_schema(&table_id, current_snapshot_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "Old schema not found",
                        format!(
                            "Could not find schema for table {} at snapshot_id {}",
                            table_id, current_snapshot_id
                        )
                    )
                },
            )?;

        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );

        let ch_table_name = &metadata.destination_table_id;

        // Mark as Applying before DDL changes.
        let updated_metadata = DestinationTableMetadata::new_applied(
            ch_table_name.clone(),
            current_snapshot_id,
            current_replication_mask,
        )
        .with_schema_change(
            new_snapshot_id,
            new_replication_mask,
            DestinationTableSchemaStatus::Applying,
        );
        self.store.store_destination_table_metadata(table_id, updated_metadata.clone()).await?;

        // Compute and apply the diff.
        let diff = current_schema.diff(new_schema);
        if let Err(err) = self.apply_schema_diff(ch_table_name, &diff, &current_schema).await {
            warn!(
                "schema change failed for table {}: {}. Manual intervention may be required.",
                table_id, err
            );
            return Err(err);
        }

        // Mark as Applied.
        self.store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;

        // Invalidate cached nullable flags so the next write recomputes them.
        {
            let mut guard = self.table_cache.write();
            guard.remove(ch_table_name);
        }

        info!(
            "schema change completed for table {}: snapshot_id {} applied",
            table_id, new_snapshot_id
        );

        Ok(())
    }

    /// Applies a schema diff to a ClickHouse table: add columns, rename
    /// columns, then drop columns (in that order for safety).
    ///
    /// New columns are placed AFTER the last existing user column (before the
    /// CDC columns) using ClickHouse's `AFTER` clause. This is critical because
    /// RowBinary encoding is positional -- without explicit placement, ADD
    /// COLUMN appends after `cdc_lsn`, misaligning the encoding.
    ///
    /// Schema changes create an inherently inconsistent window: rows written
    /// before the ALTER were encoded with the old column set, while rows
    /// after use the new one. Specifically:
    ///
    /// - ADD COLUMN: existing rows get NULL/default for the new column.
    /// - DROP COLUMN: data in the dropped column is lost for all rows.
    /// - RENAME COLUMN: existing data is preserved under the new name.
    ///
    /// ClickHouse does not support transactional DDL, so if the replicator is
    /// killed between individual ALTER statements the table may be left in a
    /// partially altered state. The `DestinationTableMetadata` Applying/Applied
    /// status tracks this for diagnostic purposes.
    async fn apply_schema_diff(
        &self,
        ch_table_name: &str,
        diff: &SchemaDiff,
        current_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        if diff.is_empty() {
            return Ok(());
        }

        // Track the last user column name for AFTER placement. New columns are
        // inserted after this column, and each added column becomes the new
        // anchor for the next.
        let mut last_user_column: String =
            current_schema.column_schemas().last().map(|c| c.name.clone()).unwrap_or_default();

        for column in &diff.columns_to_add {
            self.client.add_column(ch_table_name, column, &last_user_column).await?;
            last_user_column = column.name.clone();
        }

        for rename in &diff.columns_to_rename {
            self.client.rename_column(ch_table_name, &rename.old_name, &rename.new_name).await?;
        }

        for column in &diff.columns_to_remove {
            self.client.drop_column(ch_table_name, &column.name).await?;
        }

        Ok(())
    }

    /// Processes events in passes driven by an outer loop that runs until the
    /// iterator is exhausted. Each pass:
    /// 1. Accumulates Insert/Update/Delete rows per table until a Truncate,
    ///    Relation, or end of events.
    /// 2. Writes those rows concurrently.
    /// 3. Processes any Relation events (schema changes) sequentially.
    /// 4. Drains consecutive Truncate events (deduplicated) and executes them.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut pending: HashMap<TableId, (ReplicatedTableSchema, Vec<PendingRow>)> =
                HashMap::new();

            // Accumulate data events until we hit a Truncate or Relation boundary.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                    break;
                }

                let event = event_iter.next().expect("peeked event must be present; qed");
                match event {
                    Event::Insert(insert) => {
                        let table_id = insert.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (insert.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
                            operation: CdcOperation::Insert,
                            lsn: insert.commit_lsn,
                            cells: insert.table_row.into_values(),
                        });
                    }
                    Event::Update(update) => {
                        let UpdatedTableRow::Full(table_row) = update.updated_table_row else {
                            warn!("skipping partial update row for ClickHouse");
                            continue;
                        };
                        let table_id = update.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (update.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
                            operation: CdcOperation::Update,
                            lsn: update.commit_lsn,
                            cells: table_row.into_values(),
                        });
                    }
                    Event::Delete(delete) => {
                        let Some(old_table_row) = delete.old_table_row else {
                            info!("delete event has no row data, skipping");
                            continue;
                        };
                        let old_row = match old_table_row {
                            OldTableRow::Full(row) => row,
                            OldTableRow::Key(key_row) => {
                                expand_key_row(key_row, &delete.replicated_table_schema)
                            }
                        };
                        let table_id = delete.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (delete.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
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

            self.flush_pending_rows(pending).await?;

            // Process Relation events (schema changes) sequentially.
            while let Some(Event::Relation(_)) = event_iter.peek() {
                if let Some(Event::Relation(relation)) = event_iter.next() {
                    self.handle_relation_event(&relation.replicated_table_schema).await?;
                }
            }

            // Collect and deduplicate truncate events.
            let mut truncate_schemas: HashMap<TableId, ReplicatedTableSchema> = HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for schema in truncate_event.truncated_tables {
                        truncate_schemas.entry(schema.id()).or_insert(schema);
                    }
                }
            }

            futures::future::try_join_all(
                truncate_schemas.values().map(|schema| self.truncate_table_inner(schema)),
            )
            .await?;
        }

        Ok(())
    }

    /// Encodes the accumulated `PendingRow` batches and inserts them into
    /// ClickHouse, one `JoinSet` task per table. No-op if `pending` is empty.
    ///
    /// All `ensure_table_exists` calls run sequentially before any insert is
    /// spawned, so a schema-resolution failure aborts the whole pass without
    /// any partial-write side effects.
    async fn flush_pending_rows(
        &self,
        pending: HashMap<TableId, (ReplicatedTableSchema, Vec<PendingRow>)>,
    ) -> EtlResult<()> {
        if pending.is_empty() {
            return Ok(());
        }

        let mut prepared: Vec<(String, Arc<[bool]>, Vec<PendingRow>)> =
            Vec::with_capacity(pending.len());
        for (_, (schema, rows)) in pending {
            let (ch_table_name, nullable_flags) = self.ensure_table_exists(&schema).await?;
            prepared.push((ch_table_name, nullable_flags, rows));
        }

        let mut join_set: JoinSet<EtlResult<()>> = JoinSet::new();
        for (ch_table_name, nullable_flags, rows) in prepared {
            let client = self.client.clone();
            let max_bytes = self.inserter_config.max_bytes_per_insert;

            join_set.spawn(async move {
                let rows: Vec<Vec<ClickHouseValue>> = rows
                    .into_iter()
                    .map(|PendingRow { operation, lsn, cells }| {
                        let mut values: Vec<ClickHouseValue> =
                            cells.into_iter().map(cell_to_clickhouse_value).collect();
                        values.push(ClickHouseValue::String(operation.to_string()));
                        values.push(cdc_lsn_to_clickhouse_value(lsn));
                        values
                    })
                    .collect();

                client
                    .insert_rows(&ch_table_name, rows, &nullable_flags, max_bytes, "streaming")
                    .await
            });
        }

        while let Some(result) = join_set.join_next().await {
            result.map_err(|e| {
                etl_error!(ErrorKind::ApplyWorkerPanic, "insert task failed", e.to_string())
            })??;
        }

        Ok(())
    }
}

/// Expands a key-only delete row to full column width for RowBinary encoding.
///
/// PK columns keep their real values. Non-PK columns get `Cell::Null` if
/// nullable, or a type-appropriate zero value if non-nullable (since RowBinary
/// rejects NULL for non-nullable columns).
fn expand_key_row(key_row: TableRow, schema: &ReplicatedTableSchema) -> TableRow {
    let key_cells = key_row.into_values();
    let mut key_iter = key_cells.into_iter();
    let cells: Vec<Cell> = schema
        .column_schemas()
        .map(|col| {
            if col.primary_key_ordinal_position.is_some() {
                key_iter.next().unwrap_or(Cell::Null)
            } else if col.nullable && !is_array_type(&col.typ) {
                // Nullable scalars -> NULL. Array columns are never nullable
                // in ClickHouse (Array(Nullable(T)) without outer Nullable),
                // so they must use an empty array default instead.
                Cell::Null
            } else {
                default_cell(&col.typ)
            }
        })
        .collect();
    TableRow::new(cells)
}

/// Returns a zero-value Cell for a Postgres type, used to fill non-PK columns
/// in key-only DELETE tombstones. Array types produce empty arrays. All other
/// non-primitive types fall through to an empty String, which is a valid zero
/// value for every ClickHouse String-mapped type (numeric, time, json, bytea).
/// Date, Timestamp, and UUID use typed zero values because their ClickHouse
/// wire format is not String.
fn default_cell(typ: &Type) -> Cell {
    use etl::types::ArrayCell;

    match *typ {
        Type::BOOL => Cell::Bool(false),
        Type::INT2 => Cell::I16(0),
        Type::INT4 => Cell::I32(0),
        Type::INT8 => Cell::I64(0),
        Type::OID => Cell::U32(0),
        Type::FLOAT4 => Cell::F32(0.0),
        Type::FLOAT8 => Cell::F64(0.0),
        Type::DATE => Cell::Date(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        Type::TIMESTAMP => Cell::Timestamp(chrono::DateTime::UNIX_EPOCH.naive_utc()),
        Type::TIMESTAMPTZ => Cell::TimestampTz(chrono::DateTime::UNIX_EPOCH),
        Type::UUID => Cell::Uuid(uuid::Uuid::nil()),
        Type::BOOL_ARRAY => Cell::Array(ArrayCell::Bool(Vec::new())),
        Type::INT2_ARRAY => Cell::Array(ArrayCell::I16(Vec::new())),
        Type::INT4_ARRAY => Cell::Array(ArrayCell::I32(Vec::new())),
        Type::INT8_ARRAY => Cell::Array(ArrayCell::I64(Vec::new())),
        Type::OID_ARRAY => Cell::Array(ArrayCell::U32(Vec::new())),
        Type::FLOAT4_ARRAY => Cell::Array(ArrayCell::F32(Vec::new())),
        Type::FLOAT8_ARRAY => Cell::Array(ArrayCell::F64(Vec::new())),
        Type::TEXT_ARRAY
        | Type::VARCHAR_ARRAY
        | Type::CHAR_ARRAY
        | Type::BPCHAR_ARRAY
        | Type::NAME_ARRAY => Cell::Array(ArrayCell::String(Vec::new())),
        Type::NUMERIC_ARRAY => Cell::Array(ArrayCell::Numeric(Vec::new())),
        Type::DATE_ARRAY => Cell::Array(ArrayCell::Date(Vec::new())),
        Type::TIME_ARRAY => Cell::Array(ArrayCell::Time(Vec::new())),
        Type::TIMESTAMP_ARRAY => Cell::Array(ArrayCell::Timestamp(Vec::new())),
        Type::TIMESTAMPTZ_ARRAY => Cell::Array(ArrayCell::TimestampTz(Vec::new())),
        Type::UUID_ARRAY => Cell::Array(ArrayCell::Uuid(Vec::new())),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => Cell::Array(ArrayCell::Json(Vec::new())),
        Type::BYTEA_ARRAY => Cell::Array(ArrayCell::Bytes(Vec::new())),
        _ => Cell::String(String::new()),
    }
}

impl<S> Destination for ClickHouseDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "clickhouse"
    }

    async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result = self.truncate_table_inner(replicated_table_schema).await;
        async_result.send(result);
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_table_rows_inner(replicated_table_schema, table_rows).await;
        async_result.send(result);
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let result = self.write_events_inner(events).await;
        async_result.send(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clickhouse_column(name: &str, type_name: &str) -> ClickHouseTableColumn {
        ClickHouseTableColumn { name: name.to_string(), type_name: type_name.to_string() }
    }

    #[test]
    fn cdc_lsn_value_preserves_full_u64_range() {
        let value = cdc_lsn_to_clickhouse_value(PgLsn::from(u64::MAX));

        match value {
            ClickHouseValue::UInt64(lsn) => assert_eq!(lsn, u64::MAX),
            _ => panic!("expected UInt64 CDC LSN value"),
        }
    }

    #[test]
    fn nullable_flags_use_clickhouse_destination_nullability() {
        let expected_names = vec![
            "id".to_string(),
            "score".to_string(),
            "tags".to_string(),
            CDC_OPERATION_COLUMN_NAME.to_string(),
            CDC_LSN_COLUMN_NAME.to_string(),
        ];
        let actual_columns = vec![
            clickhouse_column("id", "Int64"),
            clickhouse_column("score", "Nullable(Int32)"),
            clickhouse_column("tags", "Array(Nullable(String))"),
            clickhouse_column(CDC_OPERATION_COLUMN_NAME, "String"),
            clickhouse_column(CDC_LSN_COLUMN_NAME, "UInt64"),
        ];

        let flags =
            nullable_flags_from_clickhouse_columns("test_table", &expected_names, &actual_columns)
                .unwrap();

        assert_eq!(flags.as_ref(), [false, true, false, false, false]);
    }

    #[test]
    fn nullable_flags_reject_clickhouse_column_count_mismatch() {
        let expected_names = vec!["id".to_string(), CDC_OPERATION_COLUMN_NAME.to_string()];
        let actual_columns = vec![clickhouse_column("id", "Int64")];

        let err =
            nullable_flags_from_clickhouse_columns("test_table", &expected_names, &actual_columns)
                .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CorruptedTableSchema);
        assert_eq!(err.detail(), Some("table 'test_table' has 1 columns, but 2 were expected"));
    }

    #[test]
    fn nullable_flags_reject_clickhouse_column_order_mismatch() {
        let expected_names = vec!["id".to_string(), "name".to_string()];
        let actual_columns =
            vec![clickhouse_column("name", "String"), clickhouse_column("id", "Int64")];

        let err =
            nullable_flags_from_clickhouse_columns("test_table", &expected_names, &actual_columns)
                .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CorruptedTableSchema);
        assert_eq!(
            err.detail(),
            Some("table 'test_table' column 1 is named 'name', but 'id' was expected")
        );
    }
}
