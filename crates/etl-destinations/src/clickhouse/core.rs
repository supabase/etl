use std::{collections::HashMap, sync::Arc, time::Duration};

use etl::{
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    destination::{
        Destination, DestinationTableMetadata, DestinationTableSchemaStatus,
        DestinationWriteStatus, DropTableForCopyResult, WriteEventsDurability, WriteEventsResult,
        WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::{Event, EventSequenceKey},
    schema::{
        ColumnModification, IdentityType, PgLsn, ReplicatedTableSchema, SchemaDiff,
        SchemaOperation, TableId, Type, is_array_type,
    },
    store::{SchemaStore, StateStore},
};
use etl_config::shared::ClickHouseEngine;
use parking_lot::{Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    clickhouse::{
        client::{ClickHouseClient, ClickHouseTableColumn, DdlKind},
        encoding::{ClickHouseValue, cell_to_clickhouse_value},
        metrics::register_metrics,
        schema::{
            create_current_view_sql, create_table_sql, drop_current_view_sql,
            supports_column_default, trailing_cdc_column_names,
        },
    },
    table_name::try_stringify_table_name,
};

const MAX_ERROR_COLUMN_NAMES: usize = 12;

/// Postgres CDC operation kind. Written to the `cdc_operation` column as the
/// matching uppercase string (`"INSERT"`, `"UPDATE"`, `"DELETE"`) so downstream
/// consumers (ReplacingMergeTree dedup, materialized views, etc.) can filter
/// or branch on operation type.
#[derive(Copy, Clone)]
enum CdcOperation {
    /// New row inserted on the source.
    Insert,
    /// Existing row updated on the source. Carries the post-update values.
    Update,
    /// Row deleted on the source. Carries pre-delete values for the PK
    /// columns; non-PK columns are filled in by `expand_key_row` (NULL for
    /// nullable columns, type-appropriate zero for non-nullable).
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
    /// CDC op kind. Drives both the MergeTree `cdc_operation` string and the
    /// ReplacingMergeTree `_etl_deleted` tombstone flag.
    operation: CdcOperation,
    /// Transaction commit LSN. Written to the MergeTree `cdc_lsn` column,
    /// and forms the high 64 bits of the ReplacingMergeTree `_etl_version`
    /// column.
    commit_lsn: PgLsn,
    /// Zero-based ordinal of this event within its transaction. Forms the
    /// low 64 bits of the ReplacingMergeTree `_etl_version` column so
    /// multi-event same-commit transactions tie-break correctly under
    /// `FINAL`.
    tx_ordinal: u64,
    /// User column values in source schema order. The trailing CDC columns
    /// are appended at encode time and are not present here.
    cells: Vec<Cell>,
}

/// Converts a Postgres LSN into the ClickHouse CDC LSN value.
fn cdc_lsn_to_clickhouse_value(lsn: PgLsn) -> ClickHouseValue {
    ClickHouseValue::UInt64(u64::from(lsn))
}

/// Appends the trailing engine-specific CDC columns to the row encoding.
///
/// MergeTree: `cdc_operation` (String), `cdc_lsn` (UInt64 commit LSN).
/// ReplacingMergeTree: `_etl_version` (UInt128 packed `EventSequenceKey`),
/// `_etl_deleted` (UInt8 tombstone flag).
fn append_cdc_columns(
    values: &mut Vec<ClickHouseValue>,
    operation: CdcOperation,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    engine: ClickHouseEngine,
) {
    match engine {
        ClickHouseEngine::MergeTree => {
            values.push(ClickHouseValue::String(operation.to_string()));
            values.push(cdc_lsn_to_clickhouse_value(commit_lsn));
        }
        ClickHouseEngine::ReplacingMergeTree => {
            let version = EventSequenceKey::new(commit_lsn, tx_ordinal).as_u128();
            values.push(ClickHouseValue::UInt128(version));
            values.push(ClickHouseValue::UInt8(matches!(operation, CdcOperation::Delete) as u8));
        }
    }
}

/// Returns true if the ClickHouse type has an outer Nullable wrapper.
fn clickhouse_type_expects_nullable_marker(type_name: &str) -> bool {
    type_name.starts_with("Nullable(")
}

/// Returns expected ClickHouse column names for a replicated schema under
/// the given engine: user columns in source order, then the engine's
/// trailing CDC columns.
fn expected_clickhouse_column_names(
    schema: &ReplicatedTableSchema,
    engine: ClickHouseEngine,
) -> Vec<String> {
    schema
        .column_schemas()
        .map(|c| c.name.as_str())
        .chain(trailing_cdc_column_names(engine).iter().copied())
        .map(str::to_owned)
        .collect()
}

/// Formats column names for error details without overwhelming wide tables.
fn summarize_column_names<'a>(column_names: impl IntoIterator<Item = &'a str>) -> String {
    let column_names = column_names.into_iter().collect::<Vec<_>>();
    let shown = column_names.iter().take(MAX_ERROR_COLUMN_NAMES).copied().collect::<Vec<_>>();
    let mut summary = shown.join(", ");

    if column_names.len() > MAX_ERROR_COLUMN_NAMES {
        summary.push_str(&format!(", ... ({} more)", column_names.len() - MAX_ERROR_COLUMN_NAMES));
    }

    summary
}

/// Derives RowBinary nullable flags from the actual ClickHouse table schema.
///
/// RowBinary requires a leading null-marker byte before each `Nullable(T)`
/// column. The actual nullability of a ClickHouse column can drift from the
/// source Postgres column: `ALTER TABLE ADD COLUMN` forces the new column to
/// `Nullable(T)` regardless of the upstream `NOT NULL` constraint, because
/// ClickHouse cannot backfill a non-null default for existing rows. Deriving
/// flags from the destination schema therefore matches what ClickHouse expects
/// on the wire even after schema evolution.
///
/// The column-count and column-order checks are an integrity guard: if the
/// destination has otherwise drifted from `ReplicatedTableSchema`, we surface
/// a `CorruptedTableSchema` error rather than emit misaligned RowBinary bytes.
fn nullable_flags_from_clickhouse_columns(
    clickhouse_table_name: &str,
    expected_column_names: &[String],
    actual_columns: &[ClickHouseTableColumn],
) -> EtlResult<Arc<[bool]>> {
    if actual_columns.len() != expected_column_names.len() {
        return Err(etl_error!(
            ErrorKind::CorruptedTableSchema,
            "ClickHouse destination table columns do not match the stored replication schema",
            format!(
                "Destination table '{}' has {} columns, but the stored replication schema expects \
                 {}. Expected columns: {}. Actual columns: {}.",
                clickhouse_table_name,
                actual_columns.len(),
                expected_column_names.len(),
                summarize_column_names(expected_column_names.iter().map(String::as_str)),
                summarize_column_names(actual_columns.iter().map(|column| column.name.as_str()))
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
                "ClickHouse destination table columns do not match the stored replication schema",
                format!(
                    "Destination table '{}' has column '{}' at position {}, but the stored \
                     replication schema expects '{}'. Expected columns: {}. Actual columns: {}.",
                    clickhouse_table_name,
                    actual_column.name,
                    index + 1,
                    expected_name,
                    summarize_column_names(expected_column_names.iter().map(String::as_str)),
                    summarize_column_names(
                        actual_columns.iter().map(|column| column.name.as_str())
                    )
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
/// called; this limit prevents unbounded memory use for very large batches
/// (e.g. initial copy).
#[derive(Copy, Clone)]
pub struct ClickHouseInserterConfig {
    /// Start a new INSERT after this many uncompressed bytes. Fixed cap
    /// because incoming and outgoing buffers can both be near-full at once;
    /// could be made tunable later if needed.
    pub max_bytes_per_insert: u64,
    /// Table engine used when creating replicated tables on ClickHouse.
    pub engine: ClickHouseEngine,
}

impl ClickHouseInserterConfig {
    /// Default per-INSERT byte cap. 64 MiB lands in the upper end of
    /// ClickHouse's recommended bulk-insert range (10k - 100k rows per
    /// INSERT) for typical CDC payload widths.
    ///
    /// See <https://clickhouse.com/docs/optimize/bulk-inserts>.
    pub const DEFAULT_MAX_BYTES_PER_INSERT: u64 = 64 * 1024 * 1024;
}

impl Default for ClickHouseInserterConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_insert: Self::DEFAULT_MAX_BYTES_PER_INSERT,
            engine: ClickHouseEngine::default(),
        }
    }
}

/// Configuration for the [`ClickHouseClient`].
///
/// Holds the server-side and client-side timeouts applied to each operation
/// bucket. Additional client-level knobs can be added here over time.
#[derive(Copy, Clone)]
pub struct ClickHouseClientConfig {
    /// Server-side budget for the connectivity check (`SELECT 1`).
    pub connectivity_check_timeout: Duration,
    /// Server-side budget for schema lookups (`system.columns`).
    pub schema_query_timeout: Duration,
    /// Server-side budget for DDL (CREATE / ALTER / DROP / RENAME / TRUNCATE).
    pub ddl_timeout: Duration,
    /// Server-side budget per INSERT statement. Wraps `insert.end().await`,
    /// which is the only awaited network step inside `insert_rows`; each
    /// flushed chunk therefore gets its own deadline.
    pub insert_timeout: Duration,
    /// Slack added to the server-side budget to derive the client-side
    /// `tokio::time::timeout`.
    pub client_timeout_epsilon: Duration,
}

impl ClickHouseClientConfig {
    /// Default server-side budget for the connectivity check.
    pub const DEFAULT_CONNECTIVITY_CHECK_TIMEOUT: Duration = Duration::from_secs(8);
    /// Default server-side budget for schema lookups.
    pub const DEFAULT_SCHEMA_QUERY_TIMEOUT: Duration = Duration::from_secs(16);
    /// Default server-side budget for DDL.
    pub const DEFAULT_DDL_TIMEOUT: Duration = Duration::from_secs(128);
    /// Default server-side budget per INSERT statement.
    pub const DEFAULT_INSERT_TIMEOUT: Duration = Duration::from_secs(256);
    /// Default slack between server-side and client-side budgets.
    pub const DEFAULT_CLIENT_TIMEOUT_EPSILON: Duration = Duration::from_secs(4);

    /// Server-side timeout for `op`.
    pub(crate) fn server_timeout_for(&self, op: ClickHouseOperationKind) -> Duration {
        match op {
            ClickHouseOperationKind::ConnectivityCheck => self.connectivity_check_timeout,
            ClickHouseOperationKind::SchemaQuery => self.schema_query_timeout,
            ClickHouseOperationKind::Ddl => self.ddl_timeout,
            ClickHouseOperationKind::Insert => self.insert_timeout,
        }
    }

    /// Client-side `tokio::time::timeout` for `op`:
    /// `server_timeout_for(op) + client_timeout_epsilon`.
    pub(crate) fn client_timeout_for(&self, op: ClickHouseOperationKind) -> Duration {
        self.server_timeout_for(op) + self.client_timeout_epsilon
    }
}

impl Default for ClickHouseClientConfig {
    fn default() -> Self {
        Self {
            connectivity_check_timeout: Self::DEFAULT_CONNECTIVITY_CHECK_TIMEOUT,
            schema_query_timeout: Self::DEFAULT_SCHEMA_QUERY_TIMEOUT,
            ddl_timeout: Self::DEFAULT_DDL_TIMEOUT,
            insert_timeout: Self::DEFAULT_INSERT_TIMEOUT,
            client_timeout_epsilon: Self::DEFAULT_CLIENT_TIMEOUT_EPSILON,
        }
    }
}

/// Categories of ClickHouse client calls.
///
/// Used to:
/// - select the corresponding server-side budget,
/// - map a generic clickhouse error onto the appropriate [`ErrorKind`].
#[derive(Copy, Clone)]
pub(crate) enum ClickHouseOperationKind {
    /// Connectivity check (`SELECT 1`).
    ConnectivityCheck,
    /// Schema lookup against `system.columns`.
    SchemaQuery,
    /// DDL: CREATE / ALTER / DROP / RENAME / TRUNCATE.
    Ddl,
    /// INSERT statement flush.
    Insert,
}

impl ClickHouseOperationKind {
    /// Error kind used when the inner future returns a
    /// `clickhouse::error::Error`.
    pub(crate) fn failed_kind(self) -> ErrorKind {
        match self {
            ClickHouseOperationKind::ConnectivityCheck => ErrorKind::DestinationConnectionFailed,
            ClickHouseOperationKind::SchemaQuery | ClickHouseOperationKind::Ddl => {
                ErrorKind::DestinationQueryFailed
            }
            ClickHouseOperationKind::Insert => ErrorKind::DestinationAtomicBatchRetryable,
        }
    }
}

impl std::fmt::Display for ClickHouseOperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            ClickHouseOperationKind::ConnectivityCheck => "connectivity check",
            ClickHouseOperationKind::SchemaQuery => "schema query",
            ClickHouseOperationKind::Ddl => "DDL",
            ClickHouseOperationKind::Insert => "insert",
        };
        f.write_str(name)
    }
}

/// CDC-capable ClickHouse destination that replicates Postgres tables.
///
/// The table engine is configured via [`ClickHouseInserterConfig::engine`];
/// see [`ClickHouseEngine`] for the engine-specific layouts.
#[derive(Clone)]
pub struct ClickHouseDestination<S> {
    /// HTTP client used for all DDL and RowBinary INSERT traffic.
    client: ClickHouseClient,
    /// Per-INSERT byte budget; gates intermediate flushes within a single
    /// `write_table_rows` / `write_events` call.
    inserter_config: ClickHouseInserterConfig,
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
    /// Per-`table_id` locks serialising first-time table creation.
    ///
    /// The two ctid copy workers spawned when `max_copy_connections > 1` share
    /// this destination (it is `Clone` over `Arc` state), so without a guard
    /// both fall through the cache miss in [`Self::ensure_table_exists`] and
    /// issue racing `CREATE TABLE` / `CREATE VIEW` statements. On ClickHouse
    /// Cloud the replicated `... IF NOT EXISTS` is not atomic across replicas,
    /// so the loser fails with "DDL failed". A `tokio::sync::Mutex` (held
    /// across the DDL `.await`) per table makes the second worker wait,
    /// then fall through the post-lock cache re-check. The outer map is
    /// guarded by a brief, await-free `parking_lot::Mutex` and grows at
    /// most one entry per replicated table.
    create_locks: Arc<Mutex<HashMap<TableId, Arc<tokio::sync::Mutex<()>>>>>,
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
        client_config: ClickHouseClientConfig,
        store: S,
    ) -> EtlResult<Self> {
        register_metrics();
        Ok(Self {
            client: ClickHouseClient::new(url, user, password, database, client_config),
            inserter_config,
            store: Arc::new(store),
            table_cache: Arc::new(RwLock::new(HashMap::new())),
            create_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Probes the server version and rejects unsupported engine/version pairs.
    /// Currently the only gate: ReplacingMergeTree requires CH >= 23.5.
    pub async fn validate_engine_support(&self) -> EtlResult<()> {
        let server_version = self.client.server_version().await?;
        ensure_engine_supported(self.inserter_config.engine, server_version)
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
        clickhouse_table_name: &str,
        schema: &ReplicatedTableSchema,
        snapshot_id: etl::schema::SnapshotId,
        replication_mask: etl::schema::ReplicationMask,
    ) -> EtlResult<()> {
        let metadata = DestinationTableMetadata::new_applying(
            clickhouse_table_name.to_owned(),
            snapshot_id,
            replication_mask,
        );
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;

        self.issue_create_table_stmt(clickhouse_table_name, schema).await?;

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;

        Ok(())
    }

    // ClickHouse Cloud transparently substitutes the MergeTree family with its
    // shared-storage variants (`ReplacingMergeTree` -> `SharedReplacingMergeTree`).
    // These are drop-in equivalents, so `system.tables.engine` reads back the
    // `Shared`-prefixed name even though the pipeline configured the plain one.

    /// Rejects writing to a pre-existing ClickHouse table whose engine does
    /// not match the configured one. No-op if the table doesn't exist yet.
    async fn ensure_engine_matches(&self, clickhouse_table_name: &str) -> EtlResult<()> {
        let Some(existing) = self.client.table_engine(clickhouse_table_name).await? else {
            return Ok(());
        };
        let configured = self.inserter_config.engine.as_clickhouse_str();
        if clickhouse_engine_matches(&existing, configured) {
            return Ok(());
        }

        Err(etl_error!(
            ErrorKind::ConfigError,
            "ClickHouse table engine mismatch",
            format!(
                "Table '{clickhouse_table_name}' was previously created with engine '{existing}', \
                 but the pipeline is configured for engine '{configured}'. Either drop the \
                 destination table and re-sync, or reconfigure the pipeline's `engine` to match \
                 the existing table."
            )
        ))
    }

    /// Issues the engine-correct `CREATE TABLE`, and under ReplacingMergeTree
    /// also the companion `CREATE VIEW "<table>__current"`. Both statements
    /// are `IF NOT EXISTS`, so retries on the recovery path are idempotent.
    async fn issue_create_table_stmt(
        &self,
        clickhouse_table_name: &str,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let engine = self.inserter_config.engine;
        let ddl = create_table_sql(engine, clickhouse_table_name, schema.column_schemas())?;
        self.client.execute_ddl(DdlKind::CreateTable, &ddl).await?;

        if matches!(engine, ClickHouseEngine::ReplacingMergeTree) {
            let view_ddl = create_current_view_sql(clickhouse_table_name, schema.column_schemas());
            self.client.execute_ddl(DdlKind::CreateView, &view_ddl).await?;
        }
        Ok(())
    }

    /// Rebuilds the ReplacingMergeTree current-state view from the current
    /// replicated schema.
    ///
    /// The base table can evolve through `ALTER TABLE`, but ClickHouse views
    /// keep the projection they were created with. Drop and recreate the view
    /// after schema changes so `"<table>__current"` follows ADD, DROP, and
    /// RENAME changes. Both statements are idempotent for recovery retries.
    async fn refresh_current_view(
        &self,
        clickhouse_table_name: &str,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let drop_view = drop_current_view_sql(clickhouse_table_name);
        self.client.execute_ddl(DdlKind::DropView, &drop_view).await?;

        let create_view = create_current_view_sql(clickhouse_table_name, schema.column_schemas());
        self.client.execute_ddl(DdlKind::CreateView, &create_view).await
    }

    /// Ensures the ClickHouse table for the given schema exists, returning
    /// `(clickhouse_table_name, nullable_flags)`.
    ///
    /// On first encounter, executes `CREATE TABLE IF NOT EXISTS` and stores
    /// destination metadata with `Applied` status. Subsequent calls return
    /// the cached result.
    async fn ensure_table_exists(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<(String, Arc<[bool]>)> {
        validate_clickhouse_table_shape(schema, self.inserter_config.engine)?;
        let clickhouse_table_name = try_stringify_table_name(schema.name())?;

        if let Some(flags) = self.table_cache.read().get(&clickhouse_table_name).cloned() {
            return Ok((clickhouse_table_name, flags));
        }

        let table_id = schema.id();

        // Serialise the first-time create/recover path per `table_id`. When
        // `max_copy_connections > 1` the two ctid copy workers share this
        // destination and would otherwise both fall through the cache miss
        // above and issue racing CREATE TABLE / CREATE VIEW statements; on
        // ClickHouse Cloud the replicated `... IF NOT EXISTS` is not atomic
        // across replicas, so the loser fails with "DDL failed". See the
        // `create_locks` field doc.
        let table_lock = {
            let mut guard = self.create_locks.lock();
            Arc::clone(guard.entry(table_id).or_default())
        };
        let _create_guard = table_lock.lock().await;

        // Re-check the cache under the lock: a worker that went ahead of us may
        // have completed creation and populated it while we were blocked.
        if let Some(flags) = self.table_cache.read().get(&clickhouse_table_name).cloned() {
            return Ok((clickhouse_table_name, flags));
        }

        // Engine-mismatch detection runs before any DDL or metadata mutation:
        // a pre-existing table created under a different engine must hard-fail
        // here, not silently get an idempotent CREATE TABLE IF NOT EXISTS that
        // would then mis-align RowBinary on insert.
        self.ensure_engine_matches(&clickhouse_table_name).await?;

        match self.store.get_destination_table_metadata(table_id).await? {
            None => {
                self.create_table_with_metadata(
                    table_id,
                    &clickhouse_table_name,
                    schema,
                    schema.inner().snapshot_id,
                    schema.replication_mask().clone(),
                )
                .await?;
            }
            Some(metadata) => {
                if metadata.is_applying() {
                    self.recover_applying_metadata(
                        table_id,
                        &clickhouse_table_name,
                        schema,
                        metadata,
                    )
                    .await?;
                }
                // Otherwise the metadata is already `Applied`: this branch
                // runs after `handle_relation_event` invalidated the cache,
                // so no DDL is needed and we just fall through to recompute
                // nullable flags below.
            }
        }

        // Compute nullable flags from the actual ClickHouse schema. This matters after
        // `ALTER TABLE ADD COLUMN`: ClickHouse scalar columns are forced to
        // `Nullable(T)` even when the Postgres column is `NOT NULL`, so RowBinary must
        // include the nullable marker byte ClickHouse expects.
        let actual_columns = self.client.table_columns(&clickhouse_table_name).await?;
        let expected_column_names =
            expected_clickhouse_column_names(schema, self.inserter_config.engine);
        let nullable_flags = nullable_flags_from_clickhouse_columns(
            &clickhouse_table_name,
            &expected_column_names,
            &actual_columns,
        )?;

        // `or_insert_with` handles the race where a concurrent caller populated
        // the entry between our read-miss and this write.
        let flags = {
            let mut guard = self.table_cache.write();
            Arc::clone(
                guard
                    .entry(clickhouse_table_name.clone())
                    .or_insert_with(|| Arc::clone(&nullable_flags)),
            )
        };

        Ok((clickhouse_table_name, flags))
    }

    /// Re-runs an interrupted DDL idempotently and transitions metadata to
    /// `Applied`. Distinguishes between an interrupted schema change (replays
    /// the diff against the previous snapshot) and an interrupted initial
    /// creation (re-issues `CREATE TABLE IF NOT EXISTS`).
    async fn recover_applying_metadata(
        &self,
        table_id: TableId,
        clickhouse_table_name: &str,
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
                                "Stored schema snapshot missing for ClickHouse schema recovery",
                                format!(
                                    "Table {} needs stored schema snapshot {} to recover the \
                                     destination table, but it was not found.",
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
                self.apply_schema_diff(clickhouse_table_name, &diff, &old_schema, schema).await?;
            }
            None => {
                self.issue_create_table_stmt(clickhouse_table_name, schema).await?;
            }
        }

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        Ok(())
    }

    async fn truncate_table_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let (clickhouse_table_name, _) = self.ensure_table_exists(schema).await?;
        self.client.truncate_table(&clickhouse_table_name).await
    }

    async fn drop_table_for_copy_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let clickhouse_table_name = try_stringify_table_name(schema.name())?;

        if matches!(self.inserter_config.engine, ClickHouseEngine::ReplacingMergeTree) {
            let drop_view = drop_current_view_sql(&clickhouse_table_name);
            self.client.execute_ddl(DdlKind::DropView, &drop_view).await?;
        }

        self.client.drop_table(&clickhouse_table_name).await?;
        self.table_cache.write().remove(&clickhouse_table_name);

        Ok(())
    }

    async fn write_table_rows_inner(
        &self,
        schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let (clickhouse_table_name, nullable_flags) = self.ensure_table_exists(schema).await?;

        let engine = self.inserter_config.engine;
        let rows: Vec<Vec<ClickHouseValue>> = table_rows
            .into_iter()
            .map(|table_row| {
                let mut values: Vec<ClickHouseValue> = table_row
                    .into_values()
                    .into_iter()
                    .map(cell_to_clickhouse_value)
                    .collect::<EtlResult<_>>()?;
                // Initial-copy rows are tagged as INSERT with LSN 0 / tx_ordinal 0
                // (sentinel meaning "this row pre-dates the streaming cursor"). For
                // ReplacingMergeTree, any streaming event then wins on FINAL because its packed
                // `_etl_version` is non-zero.
                append_cdc_columns(&mut values, CdcOperation::Insert, PgLsn::from(0), 0, engine);
                Ok(values)
            })
            .collect::<EtlResult<_>>()?;

        self.client
            .insert_rows(
                &clickhouse_table_name,
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
        validate_clickhouse_table_shape(new_schema, self.inserter_config.engine)?;

        let table_id = new_schema.id();
        let new_snapshot_id = new_schema.inner().snapshot_id;
        let new_replication_mask = new_schema.replication_mask().clone();

        let metadata =
            self.store.get_applied_destination_table_metadata(table_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::CorruptedTableSchema,
                        "Destination metadata missing for ClickHouse schema change",
                        format!(
                            "Table {} received schema snapshot {}, but destination metadata from \
                             initial synchronization was not found.",
                            table_id, new_snapshot_id
                        )
                    )
                },
            )?;

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
                        "Stored schema snapshot missing for ClickHouse schema change",
                        format!(
                            "Table {} needs stored schema snapshot {} to compare with incoming \
                             snapshot {}, but it was not found.",
                            table_id, current_snapshot_id, new_snapshot_id
                        )
                    )
                },
            )?;

        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );

        let clickhouse_table_name = &metadata.destination_table_id;
        // Mark as Applying before DDL changes.
        let updated_metadata = DestinationTableMetadata::new_applied(
            clickhouse_table_name.clone(),
            current_snapshot_id,
            current_replication_mask,
        )
        .with_schema_change(
            new_snapshot_id,
            new_replication_mask,
            DestinationTableSchemaStatus::Applying,
        );
        self.store.store_destination_table_metadata(table_id, updated_metadata.clone()).await?;

        let diff = current_schema.diff(new_schema);
        if let Err(err) =
            self.apply_schema_diff(clickhouse_table_name, &diff, &current_schema, new_schema).await
        {
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
            guard.remove(clickhouse_table_name);
        }

        info!(
            "schema change completed for table {}: snapshot_id {} applied",
            table_id, new_snapshot_id
        );

        Ok(())
    }

    /// Applies a schema diff to a ClickHouse table: add columns, rename
    /// columns, then drop columns (in that order for safety), and refreshes
    /// the ReplacingMergeTree current-state view when needed.
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
        clickhouse_table_name: &str,
        diff: &SchemaDiff,
        current_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let is_replacing_merge_tree =
            matches!(self.inserter_config.engine, ClickHouseEngine::ReplacingMergeTree);
        if diff.is_empty() {
            if is_replacing_merge_tree {
                self.refresh_current_view(clickhouse_table_name, new_schema).await?;
            }
            return Ok(());
        }

        // The ReplacingMergeTree table's `ORDER BY` clause is the source primary key,
        // and ClickHouse uses that ORDER BY as the dedup key during merges.
        // Dropping or renaming a PK column would invalidate that key in a
        // way `ALTER TABLE` cannot fix, so reject the diff before any ALTER
        // is issued.
        if is_replacing_merge_tree {
            reject_pk_alters_under_replacing_merge_tree(
                clickhouse_table_name,
                diff,
                current_schema,
            )?;
        }

        // Keep the current physical order so additions can remain before the
        // trailing CDC columns even when earlier operations renamed or dropped
        // the previous placement anchor.
        let mut user_column_names: Vec<String> =
            current_schema.column_schemas().map(|column| column.name.clone()).collect();

        for operation in diff.operations() {
            match operation {
                SchemaOperation::DropColumn { column } => {
                    self.client.drop_column(clickhouse_table_name, &column.name).await?;
                    user_column_names.retain(|name| name != &column.name);
                }
                SchemaOperation::RenameColumn { old_name, new_name, .. } => {
                    self.client.rename_column(clickhouse_table_name, old_name, new_name).await?;
                    if let Some(name) = user_column_names.iter_mut().find(|name| *name == old_name)
                    {
                        new_name.clone_into(name);
                    }
                }
                SchemaOperation::AddColumn { column } => {
                    self.client
                        .add_column(
                            clickhouse_table_name,
                            column,
                            user_column_names.last().map(String::as_str),
                        )
                        .await?;
                    user_column_names.push(column.name.clone());
                }
                SchemaOperation::ModifyColumn { change, modification } => match modification {
                    ColumnModification::Rename { .. } => {
                        unreachable!("ordered modify operations exclude renames")
                    }
                    ColumnModification::Nullability { old_nullable, new_nullable } => {
                        warn!(
                            table_name = %clickhouse_table_name,
                            column_name = %change.new_column.name,
                            old_nullable,
                            new_nullable,
                            "skipping source column nullability change for ClickHouse"
                        );
                    }
                    ColumnModification::Default { old_expression, new_expression } => {
                        let old_default_was_supported =
                            old_expression.as_deref().is_some_and(|default_expression| {
                                supports_column_default(default_expression, &change.old_column.typ)
                            });

                        if let Some(new_default_expression) = new_expression.as_deref() {
                            if supports_column_default(
                                new_default_expression,
                                &change.new_column.typ,
                            ) {
                                self.client
                                    .set_column_default(
                                        clickhouse_table_name,
                                        &change.new_column.name,
                                        &change.new_column.typ,
                                        new_default_expression,
                                    )
                                    .await?;
                            } else {
                                warn!(
                                    table_name = %clickhouse_table_name,
                                    column_name = %change.new_column.name,
                                    "skipping unsupported source column default for ClickHouse"
                                );
                                if old_default_was_supported {
                                    self.client
                                        .drop_column_default(
                                            clickhouse_table_name,
                                            &change.new_column.name,
                                        )
                                        .await?;
                                }
                            }
                        } else if old_default_was_supported {
                            self.client
                                .drop_column_default(clickhouse_table_name, &change.new_column.name)
                                .await?;
                        } else if old_expression.is_some() {
                            warn!(
                                table_name = %clickhouse_table_name,
                                column_name = %change.new_column.name,
                                "skipping source column default removal for ClickHouse because no \
                                 supported destination default was set"
                            );
                        }
                    }
                },
            }
        }

        if is_replacing_merge_tree {
            self.refresh_current_view(clickhouse_table_name, new_schema).await?;
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
                            commit_lsn: insert.commit_lsn,
                            tx_ordinal: insert.tx_ordinal,
                            cells: insert.table_row.into_values(),
                        });
                    }
                    Event::Update(update) => {
                        let table_row = clickhouse_update_row(
                            &update.replicated_table_schema,
                            update.updated_table_row,
                            self.inserter_config.engine,
                        )?;
                        let table_id = update.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (update.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
                            operation: CdcOperation::Update,
                            commit_lsn: update.commit_lsn,
                            tx_ordinal: update.tx_ordinal,
                            cells: table_row.into_values(),
                        });
                    }
                    Event::Delete(delete) => {
                        let old_table_row = clickhouse_delete_old_row(
                            &delete.replicated_table_schema,
                            delete.old_table_row,
                        )?;
                        let old_row = match old_table_row {
                            OldTableRow::Full(row) => row,
                            OldTableRow::Key(key_row) => {
                                expand_key_row(key_row, &delete.replicated_table_schema)?
                            }
                        };
                        let table_id = delete.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (delete.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
                            operation: CdcOperation::Delete,
                            commit_lsn: delete.commit_lsn,
                            tx_ordinal: delete.tx_ordinal,
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
            let (clickhouse_table_name, nullable_flags) = self.ensure_table_exists(&schema).await?;
            prepared.push((clickhouse_table_name, nullable_flags, rows));
        }

        let mut join_set: JoinSet<EtlResult<()>> = JoinSet::new();
        let engine = self.inserter_config.engine;
        for (clickhouse_table_name, nullable_flags, rows) in prepared {
            let client = self.client.clone();
            let max_bytes = self.inserter_config.max_bytes_per_insert;

            join_set.spawn(async move {
                let rows: Vec<Vec<ClickHouseValue>> = rows
                    .into_iter()
                    .map(|PendingRow { operation, commit_lsn, tx_ordinal, cells }| {
                        let mut values: Vec<ClickHouseValue> = cells
                            .into_iter()
                            .map(cell_to_clickhouse_value)
                            .collect::<EtlResult<_>>()?;
                        append_cdc_columns(&mut values, operation, commit_lsn, tx_ordinal, engine);
                        Ok(values)
                    })
                    .collect::<EtlResult<_>>()?;

                client
                    .insert_rows(
                        &clickhouse_table_name,
                        rows,
                        &nullable_flags,
                        max_bytes,
                        "streaming",
                    )
                    .await
            });
        }

        while let Some(result) = join_set.join_next().await {
            result.map_err(
                |err| etl_error!(ErrorKind::ApplyWorkerPanic, "Insert task failed", source: err),
            )??;
        }

        Ok(())
    }
}

/// Rejects schema diffs that would drop or rename a primary-key column on
/// an ReplacingMergeTree table.
///
/// The destination emits `CREATE TABLE ... ENGINE = ReplacingMergeTree(...)
/// ORDER BY (<pk cols>)`, so the table's sort and dedup keys are bound to
/// those PK column names. ClickHouse `ALTER TABLE` can change column shapes
/// but cannot rewrite the ORDER BY expression, so a PK drop or rename would
/// leave the ORDER BY referring to a column that no longer exists (or has a
/// different meaning), silently breaking dedup. We error before the ALTER
/// reaches the server.
fn reject_pk_alters_under_replacing_merge_tree(
    clickhouse_table_name: &str,
    diff: &SchemaDiff,
    current_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    for column in &diff.columns_to_remove {
        if column.primary_key_ordinal_position.is_some() {
            return Err(etl_error!(
                ErrorKind::SourceSchemaError,
                "ReplacingMergeTree does not support dropping a primary-key column",
                format!(
                    "Table '{clickhouse_table_name}': DROP COLUMN '{name}' would invalidate the \
                     ReplacingMergeTree ORDER BY / dedup key. Switch this table to `engine: \
                     merge_tree` or restore the column on the source.",
                    name = column.name
                )
            ));
        }
    }

    for change in &diff.columns_to_change {
        for modification in &change.modifications {
            let ColumnModification::Rename { old_name, new_name } = modification else {
                continue;
            };

            let was_pk = current_schema
                .column_schemas()
                .find(|c| c.name == *old_name)
                .is_some_and(|c| c.primary_key_ordinal_position.is_some());
            if was_pk {
                return Err(etl_error!(
                    ErrorKind::SourceSchemaError,
                    "ReplacingMergeTree does not support renaming a primary-key column",
                    format!(
                        "Table '{clickhouse_table_name}': RENAME COLUMN '{old_name}' -> \
                         '{new_name}' would invalidate the ReplacingMergeTree ORDER BY / dedup \
                         key. Switch this table to `engine: merge_tree` or revert the rename on \
                         the source."
                    )
                ));
            }
        }
    }

    Ok(())
}

/// Verifies the engine's `min_server_version()` constraint against the given
/// server version. The per-engine version requirement lives on
/// [`ClickHouseEngine`] itself; this function is just the error-construction
/// shell that surfaces the mismatch as an `EtlResult`.
fn ensure_engine_supported(engine: ClickHouseEngine, server_version: (u32, u32)) -> EtlResult<()> {
    if let Some(min) = engine.min_server_version()
        && server_version < min
    {
        let (min_major, min_minor) = min;
        let (major, minor) = server_version;

        return Err(etl_error!(
            ErrorKind::ConfigError,
            "ClickHouse server version is too old for the configured engine",
            format!(
                "Detected ClickHouse {major}.{minor}; engine `{cfg}` requires \
                 {min_major}.{min_minor} or newer. Upgrade ClickHouse or set `engine: merge_tree`.",
                cfg = engine.as_clickhouse_str()
            )
        ));
    }

    Ok(())
}

/// Rejects source schemas the ClickHouse destination cannot represent for the
/// configured engine.
///
/// ReplacingMergeTree-only: the source table must have a primary key.
/// ReplacingMergeTree uses the PK as `ORDER BY`, which is also the dedup key;
/// without a PK there is nothing to merge on.
fn validate_clickhouse_table_shape(
    replicated_table_schema: &ReplicatedTableSchema,
    engine: ClickHouseEngine,
) -> EtlResult<()> {
    if !replicated_table_schema.all_primary_key_columns_replicated() {
        let omitted_columns = replicated_table_schema
            .unreplicated_primary_key_column_schemas()
            .map(|column_schema| column_schema.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ClickHouse requires all source primary-key columns to be replicated",
            format!(
                "Table '{}' omits source primary-key columns from replication: {}",
                replicated_table_schema.name(),
                omitted_columns
            )
        ));
    }

    if matches!(engine, ClickHouseEngine::ReplacingMergeTree)
        && replicated_table_schema.primary_key_column_schemas().next().is_none()
    {
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ClickHouse ReplacingMergeTree requires a primary key",
            format!(
                "Table '{}' has no primary-key columns; set `engine: merge_tree` or define a PK \
                 on the source table.",
                replicated_table_schema.name()
            )
        ));
    }

    Ok(())
}

/// Returns the full new row required for a ClickHouse update.
///
/// ReplacingMergeTree also uses the source primary key as its dedup key, so
/// update events must be keyed by the primary key or carry full row images.
fn clickhouse_update_row(
    replicated_table_schema: &ReplicatedTableSchema,
    updated_table_row: UpdatedTableRow,
    engine: ClickHouseEngine,
) -> EtlResult<TableRow> {
    let UpdatedTableRow::Full(row) = updated_table_row else {
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "ClickHouse update requires a full new row image",
            format!(
                "Table '{}' emitted a partial update row: some column values could not be \
                 reconstructed. Writing it would record NULL for the missing columns and \
                 misrepresent the source.",
                replicated_table_schema.name()
            )
        ));
    };

    if matches!(engine, ClickHouseEngine::ReplacingMergeTree) {
        ensure_clickhouse_key_identity_is_primary_key(replicated_table_schema)?;
    }

    Ok(row)
}

/// Returns the old row image required for a ClickHouse delete tombstone.
fn clickhouse_delete_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
    old_table_row: Option<OldTableRow>,
) -> EtlResult<OldTableRow> {
    old_table_row.ok_or_else(|| {
        etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "ClickHouse delete requires an old row image",
            format!(
                "Table '{}' emitted a delete without an old row image. ClickHouse deletes need \
                 either a full old row or a key image that can be expanded into a tombstone.",
                replicated_table_schema.name()
            )
        )
    })
}

/// Validates that a key-only old-row image can be interpreted as source PK
/// values.
fn ensure_clickhouse_key_identity_is_primary_key(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(
        replicated_table_schema.identity_type(),
        IdentityType::PrimaryKey | IdentityType::Full
    ) {
        Ok(())
    } else {
        let identity_type = replicated_table_schema.identity_type();
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "ClickHouse requires primary-key or full replica identity",
            format!(
                "Table '{}' uses replica identity {:?}. ClickHouse needs the source row identity \
                 to match the primary key (so DELETE tombstones land in the right PK slots) or to \
                 carry the full row image. Configure REPLICA IDENTITY DEFAULT (when the PK is the \
                 natural identity) or REPLICA IDENTITY FULL.",
                replicated_table_schema.name(),
                identity_type
            )
        ))
    }
}

/// Expands a key-only delete row to full column width for RowBinary encoding.
///
/// PK columns keep their real values. Non-PK columns get `Cell::Null` if
/// nullable, or a type-appropriate zero value if non-nullable (since RowBinary
/// rejects NULL for non-nullable columns).
///
/// Caller only reaches this path for key-only deletes, so this function
/// validates that the key row can be interpreted as source primary-key values.
fn expand_key_row(key_row: TableRow, schema: &ReplicatedTableSchema) -> EtlResult<TableRow> {
    let primary_key_column_count = schema.primary_key_column_schemas().len();
    if key_row.values().len() != primary_key_column_count {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "ClickHouse key image does not match the source primary key",
            format!(
                "Expected {} key values for table '{}', got {}",
                primary_key_column_count,
                schema.name(),
                key_row.values().len()
            )
        ));
    }

    ensure_clickhouse_key_identity_is_primary_key(schema)?;

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
    Ok(TableRow::new(cells))
}

/// Returns a zero-value Cell for a Postgres type, used to fill non-PK columns
/// in key-only DELETE tombstones. Array types produce empty arrays. All other
/// non-primitive types fall through to an empty String, which is a valid zero
/// value for every ClickHouse String-mapped type (numeric, time, timetz,
/// interval, json, bytea).
/// Date, Timestamp, and UUID use typed zero values because their ClickHouse
/// wire format is not String.
fn default_cell(typ: &Type) -> Cell {
    use etl::data::ArrayCell;

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
        Type::NUMERIC_ARRAY => Cell::Array(ArrayCell::Numeric(Vec::new())),
        Type::DATE_ARRAY => Cell::Array(ArrayCell::Date(Vec::new())),
        Type::TIME_ARRAY => Cell::Array(ArrayCell::Time(Vec::new())),
        Type::TIMESTAMP_ARRAY => Cell::Array(ArrayCell::Timestamp(Vec::new())),
        Type::TIMESTAMPTZ_ARRAY => Cell::Array(ArrayCell::TimestampTz(Vec::new())),
        Type::UUID_ARRAY => Cell::Array(ArrayCell::Uuid(Vec::new())),
        Type::JSON_ARRAY | Type::JSONB_ARRAY => Cell::Array(ArrayCell::Json(Vec::new())),
        Type::BYTEA_ARRAY => Cell::Array(ArrayCell::Bytes(Vec::new())),
        _ if is_array_type(typ) => Cell::Array(ArrayCell::String(Vec::new())),
        _ => Cell::String(String::new()),
    }
}

impl<S> Destination for ClickHouseDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        etl_config::shared::DestinationKind::ClickHouse.as_str()
    }

    // The trait methods below intentionally do not use `?` on the inner work.
    // Errors must reach the caller via `async_result.send(result)`, not via the
    // outer `EtlResult<()>`; using `?` would short-circuit before `send` runs
    // and leave the receiver waiting. The outer return value just signals
    // "work accepted, watch the channel for completion". `AsyncResult::send`
    // itself returns `()`, and its `Drop` impl synthesizes a "dropped without
    // sending" error if the path ever skips `send`, so the receiver is never
    // silently abandoned.

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        let result = self.drop_table_for_copy_inner(replicated_table_schema).await;
        async_result.send(result);
        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let result = self.write_table_rows_inner(replicated_table_schema, table_rows).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        let result = self.write_events_inner(events).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));
        Ok(())
    }
}

/// Strips ClickHouse Cloud's `Shared` storage-variant prefix so the shared and
/// non-shared MergeTree-family engines compare equal (see
/// `ensure_engine_matches`).
fn normalize_clickhouse_engine(engine: &str) -> &str {
    engine.strip_prefix("Shared").unwrap_or(engine)
}

/// Whether a table's existing engine satisfies the configured one, treating the
/// `Shared` Cloud variants as equivalent to their plain forms.
fn clickhouse_engine_matches(existing: &str, configured: &str) -> bool {
    normalize_clickhouse_engine(existing) == normalize_clickhouse_engine(configured)
}

#[cfg(test)]
mod tests {
    use etl::{
        data::{ArrayCell, PartialTableRow},
        schema::{ColumnSchema, IdentityMask, ReplicationMask, TableName, TableSchema},
    };

    use super::*;
    use crate::clickhouse::schema::{CDC_LSN_COLUMN_NAME, CDC_OPERATION_COLUMN_NAME};

    fn clickhouse_column(name: &str, type_name: &str) -> ClickHouseTableColumn {
        ClickHouseTableColumn { name: name.to_owned(), type_name: type_name.to_owned() }
    }

    #[test]
    fn clickhouse_engine_matches_accepts_cloud_shared_variants() {
        // Cloud `Shared` variants are equivalent to their plain configured forms.
        assert!(clickhouse_engine_matches("SharedReplacingMergeTree", "ReplacingMergeTree"));
        assert!(clickhouse_engine_matches("SharedMergeTree", "MergeTree"));
        assert!(clickhouse_engine_matches("ReplacingMergeTree", "ReplacingMergeTree"));

        // Genuine engine mismatches still fail.
        assert!(!clickhouse_engine_matches("SharedReplacingMergeTree", "MergeTree"));
        assert!(!clickhouse_engine_matches("MergeTree", "ReplacingMergeTree"));
    }

    fn replicated_schema(identity_type: IdentityType) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = match identity_type {
            IdentityType::Full => IdentityMask::from_bytes(vec![1, 1]),
            IdentityType::PrimaryKey => IdentityMask::from_bytes(vec![1, 0]),
            IdentityType::AlternativeKey => IdentityMask::from_bytes(vec![0, 1]),
            IdentityType::Missing => IdentityMask::from_bytes(vec![0, 0]),
        };
        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    fn replicated_schema_with_partial_primary_key() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let replication_mask = ReplicationMask::from_bytes(vec![0, 1, 1]);
        let identity_mask = IdentityMask::from_bytes(vec![0, 1, 0]);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    #[test]
    fn clickhouse_update_row_accepts_primary_key_identity_under_replacing_merge_tree() {
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let result = clickhouse_update_row(
            &replicated_schema(IdentityType::PrimaryKey),
            UpdatedTableRow::Full(row.clone()),
            ClickHouseEngine::ReplacingMergeTree,
        )
        .unwrap();

        assert_eq!(result, row);
    }

    #[test]
    fn clickhouse_update_row_accepts_full_identity_under_replacing_merge_tree() {
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let result = clickhouse_update_row(
            &replicated_schema(IdentityType::Full),
            UpdatedTableRow::Full(row.clone()),
            ClickHouseEngine::ReplacingMergeTree,
        )
        .unwrap();

        assert_eq!(result, row);
    }

    #[test]
    fn clickhouse_update_row_rejects_alternative_key_under_replacing_merge_tree() {
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let err = clickhouse_update_row(
            &replicated_schema(IdentityType::AlternativeKey),
            UpdatedTableRow::Full(row),
            ClickHouseEngine::ReplacingMergeTree,
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn clickhouse_update_row_rejects_missing_identity_under_replacing_merge_tree() {
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let err = clickhouse_update_row(
            &replicated_schema(IdentityType::Missing),
            UpdatedTableRow::Full(row),
            ClickHouseEngine::ReplacingMergeTree,
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn clickhouse_update_row_rejects_partial_rows_before_identity_checks() {
        let partial_row = PartialTableRow::new(2, TableRow::new(vec![Cell::I32(1)]), vec![1]);

        let err = clickhouse_update_row(
            &replicated_schema(IdentityType::AlternativeKey),
            UpdatedTableRow::Partial(partial_row),
            ClickHouseEngine::ReplacingMergeTree,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
        assert!(err.to_string().contains("partial update row"));
    }

    #[test]
    fn clickhouse_delete_old_row_rejects_missing_old_rows() {
        let err = clickhouse_delete_old_row(&replicated_schema(IdentityType::PrimaryKey), None)
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn expand_key_row_rejects_short_key_payload_before_identity_checks() {
        let err =
            expand_key_row(TableRow::new(vec![]), &replicated_schema(IdentityType::AlternativeKey))
                .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidState);
        assert!(err.to_string().contains("Expected 1 key values"));
    }

    #[test]
    fn validate_clickhouse_table_shape_accepts_alternative_identity() {
        validate_clickhouse_table_shape(
            &replicated_schema(IdentityType::AlternativeKey),
            ClickHouseEngine::MergeTree,
        )
        .unwrap();
    }

    #[test]
    fn validate_clickhouse_table_shape_rejects_partial_primary_key() {
        let err = validate_clickhouse_table_shape(
            &replicated_schema_with_partial_primary_key(),
            ClickHouseEngine::MergeTree,
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
        assert!(err.to_string().contains("tenant_id"));
    }

    #[test]
    fn validate_clickhouse_table_shape_rejects_pkless_schema_under_replacing_merge_tree() {
        // --- GIVEN: a PK-less schema and engine = ReplacingMergeTree ---
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(2),
            TableName::new("public".to_owned(), "events".to_owned()),
            vec![ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 1, true)],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1]);
        let schema =
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);

        // --- WHEN: validating under ReplacingMergeTree ---
        let err = validate_clickhouse_table_shape(&schema, ClickHouseEngine::ReplacingMergeTree)
            .unwrap_err();

        // --- THEN: rejected with SourceSchemaError ---
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
    }

    #[test]
    fn ensure_engine_supported_rejects_replacing_merge_tree_on_old_server() {
        // --- GIVEN: server below the ReplacingMergeTree minimum ---
        let err =
            ensure_engine_supported(ClickHouseEngine::ReplacingMergeTree, (23, 4)).unwrap_err();
        // --- THEN: surfaced as a config error ---
        assert_eq!(err.kind(), ErrorKind::ConfigError);
    }

    #[test]
    fn ensure_engine_supported_accepts_merge_tree_on_any_server() {
        ensure_engine_supported(ClickHouseEngine::MergeTree, (20, 0)).unwrap();
    }

    #[test]
    fn ensure_engine_supported_accepts_replacing_merge_tree_on_supported_server() {
        ensure_engine_supported(ClickHouseEngine::ReplacingMergeTree, (23, 5)).unwrap();
        ensure_engine_supported(ClickHouseEngine::ReplacingMergeTree, (24, 1)).unwrap();
    }

    /// Schema with composite PK `(tenant_id, id)` plus a non-PK `value`
    /// column. Used by the PK-ALTER-guard tests.
    fn replicated_schema_for_pk_alters() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(7),
            TableName::new("public".to_owned(), "replacing_merge_tree_alter".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1, 1, 0]);
        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    fn rename_change(
        old_name: &str,
        new_name: &str,
        ordinal_position: i32,
    ) -> etl::schema::ColumnChange {
        etl::schema::ColumnChange {
            ordinal_position,
            old_column: ColumnSchema::new(
                old_name.to_owned(),
                Type::TEXT,
                -1,
                ordinal_position,
                true,
            ),
            new_column: ColumnSchema::new(
                new_name.to_owned(),
                Type::TEXT,
                -1,
                ordinal_position,
                true,
            ),
            modifications: vec![etl::schema::ColumnModification::Rename {
                old_name: old_name.to_owned(),
                new_name: new_name.to_owned(),
            }],
        }
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_allows_non_pk_drop() {
        // --- GIVEN: a diff that drops the non-PK `value` column ---
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(
            Vec::new(),
            vec![ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 3, true)],
            Vec::new(),
            schema.column_schemas().map(|column| column.name.as_str()),
        );
        // --- WHEN/THEN: guard passes ---
        reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_rejects_pk_drop() {
        // --- GIVEN: a diff that drops the PK column `tenant_id` ---
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(
            Vec::new(),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
            ],
            Vec::new(),
            schema.column_schemas().map(|column| column.name.as_str()),
        );
        // --- WHEN: validating ---
        let err = reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
        )
        .unwrap_err();
        // --- THEN: rejected with SourceSchemaError naming the column ---
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
        assert!(err.to_string().contains("tenant_id"), "error must name the PK column: {err}");
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_allows_non_pk_rename() {
        // --- GIVEN: a rename of the non-PK `value` column ---
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(
            Vec::new(),
            Vec::new(),
            vec![rename_change("value", "payload", 3)],
            schema.column_schemas().map(|column| column.name.as_str()),
        );
        // --- WHEN/THEN: guard passes ---
        reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_rejects_pk_rename() {
        // --- GIVEN: a rename of the PK column `id` ---
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(
            Vec::new(),
            Vec::new(),
            vec![rename_change("id", "row_id", 2)],
            schema.column_schemas().map(|column| column.name.as_str()),
        );
        // --- WHEN: validating ---
        let err = reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
        )
        .unwrap_err();
        // --- THEN: rejected with SourceSchemaError naming the rename ---
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
        assert!(
            err.to_string().contains("'id'") && err.to_string().contains("'row_id'"),
            "error must name old + new names: {err}"
        );
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
    fn default_cell_string_mapped_values_are_strings() {
        assert_eq!(default_cell(&Type::MONEY), Cell::String(String::new()));
        assert_eq!(default_cell(&Type::TIMETZ), Cell::String(String::new()));
        assert_eq!(default_cell(&Type::INTERVAL), Cell::String(String::new()));
        assert_eq!(default_cell(&Type::MONEY_ARRAY), Cell::Array(ArrayCell::String(Vec::new())));
        assert_eq!(default_cell(&Type::TIMETZ_ARRAY), Cell::Array(ArrayCell::String(Vec::new())));
        assert_eq!(default_cell(&Type::INTERVAL_ARRAY), Cell::Array(ArrayCell::String(Vec::new())));
    }

    #[test]
    fn nullable_flags_use_clickhouse_destination_nullability() {
        let expected_names = vec![
            "id".to_owned(),
            "score".to_owned(),
            "tags".to_owned(),
            CDC_OPERATION_COLUMN_NAME.to_owned(),
            CDC_LSN_COLUMN_NAME.to_owned(),
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
        let expected_names = vec!["id".to_owned(), CDC_OPERATION_COLUMN_NAME.to_owned()];
        let actual_columns = vec![clickhouse_column("id", "Int64")];

        let err =
            nullable_flags_from_clickhouse_columns("test_table", &expected_names, &actual_columns)
                .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CorruptedTableSchema);
        assert_eq!(
            err.description(),
            Some("ClickHouse destination table columns do not match the stored replication schema")
        );
        assert_eq!(
            err.detail(),
            Some(
                "Destination table 'test_table' has 1 columns, but the stored replication schema \
                 expects 2. Expected columns: id, cdc_operation. Actual columns: id."
            )
        );
    }

    #[test]
    fn nullable_flags_reject_clickhouse_column_order_mismatch() {
        let expected_names = vec!["id".to_owned(), "name".to_owned()];
        let actual_columns =
            vec![clickhouse_column("name", "String"), clickhouse_column("id", "Int64")];

        let err =
            nullable_flags_from_clickhouse_columns("test_table", &expected_names, &actual_columns)
                .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::CorruptedTableSchema);
        assert_eq!(
            err.description(),
            Some("ClickHouse destination table columns do not match the stored replication schema")
        );
        assert_eq!(
            err.detail(),
            Some(
                "Destination table 'test_table' has column 'name' at position 1, but the stored \
                 replication schema expects 'id'. Expected columns: id, name. Actual columns: \
                 name, id."
            )
        );
    }
}
