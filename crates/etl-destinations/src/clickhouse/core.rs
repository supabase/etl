use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use etl::{
    data::{Cell, Date, OldTableRow, TableRow, Timestamp, UpdatedTableRow},
    destination::{
        Destination, DestinationTableMetadata, DestinationTableSchema, DestinationWriteStatus,
        DropTableForCopyResult, TableCopyBatchId, WriteEventsDurability, WriteEventsResult,
        WriteTableRowsResult,
    },
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
    event::{Event, EventSequenceKey},
    schema::{
        ColumnAlterationKind, ColumnMetadataChange, ColumnPresenceChangeReason, ColumnSchema,
        IdentityType, PgLsn, ReplicatedTableSchema, SchemaDiff, SchemaOperation, SchemaPlan,
        TableId, TableName, Type, is_array_type,
    },
    store::{SchemaStore, StateStore},
    task::{TaskGroup, TaskRegistry},
};
use etl_config::shared::ClickHouseEngine;
use parking_lot::{Mutex, RwLock};
use tokio::sync::OwnedMutexGuard;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    clickhouse::{
        CLICKHOUSE_COLUMN_NAME_MAPPING,
        client::{
            ClickHouseClient, ClickHouseTableColumn, DdlKind, InsertRowsError, RowBinaryLayout,
        },
        encoding::{ClickHouseValue, cell_to_clickhouse_value},
        metrics::{CDC_REPLICATION_PATH, COPY_REPLICATION_PATH, register_metrics},
        schema::{
            CDC_LSN_COLUMN_NAME, CDC_OPERATION_COLUMN_NAME, CDC_TX_ORDINAL_COLUMN_NAME,
            CURRENT_VIEW_SUFFIX, clickhouse_type, create_current_view_sql, create_table_sql,
            drop_current_view_sql, supports_column_default, trailing_cdc_columns,
        },
    },
    recovery::{ensure_destination_schema_matches_metadata, ensure_relation_schema_transition},
    table_name::try_stringify_table_name,
};

const MAX_ERROR_COLUMN_NAMES: usize = 12;

/// Postgres CDC operation kind. Written to the `cdc_operation` column as the
/// matching uppercase string (`"INSERT"`, `"UPDATE"`, `"DELETE"`) so downstream
/// consumers (ReplacingMergeTree dedup, materialized views, etc.) can filter or
/// branch on operation type.
#[derive(Copy, Clone)]
enum CdcOperation {
    /// New row inserted on the source.
    Insert,
    /// Existing row updated on the source. Carries the post-update values.
    Update,
    /// Row deleted on the source. Carries pre-delete values for the PK columns;
    /// non-PK columns are filled in by `expand_key_row` (NULL for nullable
    /// columns, type-appropriate zero for non-nullable).
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
    /// Source ordering for this DML event. MergeTree exposes the LSN and
    /// transaction ordinal separately; ReplacingMergeTree stores the packed
    /// key in `_etl_version`.
    sequence_key: EventSequenceKey,
    /// User column values in source schema order. The trailing CDC columns are
    /// appended at encode time and are not present here.
    cells: Vec<Cell>,
}

/// Destination rows derived from one source update.
///
/// Every update yields [`Self::destination_updated_row`]. A primary-key change
/// also yields [`Self::destination_old_key_tombstone`], which callers must
/// write first.
#[derive(Debug)]
struct ClickHouseRowsForUpdate {
    /// Tombstone for the old key when the update changed the primary key.
    destination_old_key_tombstone: Option<TableRow>,

    /// Full row after the update.
    destination_updated_row: TableRow,
}

/// Converts a Postgres LSN into the ClickHouse CDC LSN value.
fn cdc_lsn_to_clickhouse_value(lsn: PgLsn) -> ClickHouseValue {
    ClickHouseValue::UInt64(u64::from(lsn))
}

/// Appends the trailing engine-specific CDC columns to the row encoding.
///
/// MergeTree: `cdc_operation` (String), `cdc_lsn` (UInt64 commit LSN), and
/// `cdc_tx_ordinal` (UInt64 transaction ordinal). ReplacingMergeTree:
/// `_etl_version` (UInt128 packed `EventSequenceKey`) and `_etl_deleted` (UInt8
/// tombstone flag).
fn append_cdc_columns(
    values: &mut Vec<ClickHouseValue>,
    operation: CdcOperation,
    sequence_key: EventSequenceKey,
    engine: ClickHouseEngine,
) {
    match engine {
        ClickHouseEngine::MergeTree => {
            values.push(ClickHouseValue::String(operation.to_string()));
            values.push(cdc_lsn_to_clickhouse_value(sequence_key.commit_lsn));
            values.push(ClickHouseValue::UInt64(sequence_key.tx_ordinal));
        }
        ClickHouseEngine::ReplacingMergeTree => {
            let version = sequence_key.as_u128();
            values.push(ClickHouseValue::UInt128(version));
            values
                .push(ClickHouseValue::UInt8(u8::from(matches!(operation, CdcOperation::Delete))));
        }
    }
}

/// Returns the ClickHouse columns ETL expects for a replicated schema under the
/// given engine: user columns in source order, then the engine's trailing CDC
/// columns.
///
/// Types are the non-nullable form. A physical user column may additionally be
/// wrapped in `Nullable(...)`, because publication-mask additions and relaxed
/// `NOT NULL` columns are nullable in ClickHouse even when the source column is
/// not.
fn expected_clickhouse_columns(
    schema: &ReplicatedTableSchema,
    engine: ClickHouseEngine,
) -> Vec<ClickHouseTableColumn> {
    schema
        .destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING)
        .map(|column| ClickHouseTableColumn {
            type_name: clickhouse_type(&column.typ, false, false),
            name: column.name,
        })
        .chain(trailing_cdc_columns(engine).iter().map(|(name, type_name)| ClickHouseTableColumn {
            name: (*name).to_owned(),
            type_name: (*type_name).to_owned(),
        }))
        .collect()
}

/// Rejects renames that move a ClickHouse nested subcolumn between parents.
fn ensure_clickhouse_renames_are_supported(table_name: &str, plan: &SchemaPlan) -> EtlResult<()> {
    for operation in plan.ordered_operations() {
        let SchemaOperation::AlterColumn { alteration } = operation else {
            continue;
        };
        if alteration.kind() != ColumnAlterationKind::Rename {
            continue;
        }

        let before = alteration.before_column_schema();
        let after = alteration.after_column_schema();
        let before_parent = before.name.rsplit_once('.').map(|(parent, _)| parent);
        let after_parent = after.name.rsplit_once('.').map(|(parent, _)| parent);
        if before_parent != after_parent {
            return Err(etl_error!(
                ErrorKind::SourceSchemaError,
                "ClickHouse cannot move a nested subcolumn during rename",
                format!(
                    "Table '{table_name}' renames column '{}' to '{}', which changes its nested \
                     parent. Use names under the same parent or resynchronize the table.",
                    before.name, after.name,
                )
            ));
        }
    }

    Ok(())
}

/// Returns the destination definition for one added source column.
///
/// A column newly exposed by the replication mask has no destination values for
/// historical rows. ClickHouse scalars therefore use `Nullable(T)` without the
/// source default. Arrays cannot be `Nullable`, so historical rows read as
/// empty arrays, the same value a top-level `NULL` array is stored as.
fn clickhouse_add_column_definition(
    after_column_schema: &ColumnSchema,
    reason: ColumnPresenceChangeReason,
) -> (ColumnSchema, bool) {
    let mut destination_column_schema = after_column_schema.clone();
    if reason == ColumnPresenceChangeReason::ReplicationMask {
        destination_column_schema.default_expression = None;
        return (destination_column_schema, true);
    }

    let preserve_not_null = !after_column_schema.nullable
        && after_column_schema.default_expression.as_deref().is_some_and(|default_expression| {
            supports_column_default(default_expression, &after_column_schema.typ)
        });
    (destination_column_schema, !preserve_not_null)
}

/// Returns whether `typ` holds wall-clock timestamps without a time zone.
fn is_wall_clock_timestamp(typ: &Type) -> bool {
    matches!(*typ, Type::TIMESTAMP | Type::TIMESTAMP_ARRAY)
}

/// Rejects source type changes that change the mapped ClickHouse type before
/// any metadata or DDL mutation.
///
/// ETL does not alter ClickHouse column types. Postgres rewrites existing rows
/// with its own cast or `USING` expression and emits no row events for that
/// rewrite, so a ClickHouse `CAST` of the stored rows could disagree with the
/// source. Writing new rows into the old column would instead reinterpret
/// RowBinary bytes or fail every insert. Changes that keep the mapped type,
/// such as `varchar(50)` to `varchar(100)`, need no DDL and are accepted.
///
/// `timestamp` and `timestamptz` share a ClickHouse type, but a change between
/// them is still rejected: Postgres converts existing values through the
/// session time zone, so stored ClickHouse rows would no longer match.
fn ensure_clickhouse_type_changes_are_supported(
    table_name: &str,
    plan: &SchemaPlan,
) -> EtlResult<()> {
    for operation in plan.ordered_operations() {
        let SchemaOperation::AlterColumn { alteration } = operation else {
            continue;
        };
        if alteration.kind() != ColumnAlterationKind::Type {
            continue;
        }

        let before = alteration.before_column_schema();
        let after = alteration.after_column_schema();
        // Nullability is a separate alteration kind, so compare the types
        // alone.
        let before_type = clickhouse_type(&before.typ, false, false);
        let after_type = clickhouse_type(&after.typ, false, false);
        if before_type != after_type {
            return Err(etl_error!(
                ErrorKind::SourceSchemaError,
                "ClickHouse cannot apply a source column type change",
                format!(
                    "Table '{table_name}' changes column '{}' from {} to {}, which changes its \
                     ClickHouse type from '{before_type}' to '{after_type}'. ETL does not convert \
                     existing ClickHouse rows. Resynchronize the table.",
                    after.name,
                    before.typ.name(),
                    after.typ.name(),
                )
            ));
        }
        if is_wall_clock_timestamp(&before.typ) != is_wall_clock_timestamp(&after.typ) {
            return Err(etl_error!(
                ErrorKind::SourceSchemaError,
                "ClickHouse cannot apply a source column type change",
                format!(
                    "Table '{table_name}' changes column '{}' from {} to {}. Postgres converts \
                     existing values through the session time zone, and ETL does not convert \
                     existing ClickHouse rows. Resynchronize the table.",
                    after.name,
                    before.typ.name(),
                    after.typ.name(),
                )
            ));
        }
    }

    Ok(())
}

/// Returns physical user columns after validating the trailing ETL columns.
fn clickhouse_user_column_names(
    columns: &[ClickHouseTableColumn],
    engine: ClickHouseEngine,
) -> EtlResult<Vec<String>> {
    let trailing_columns = trailing_cdc_columns(engine);
    if columns.len() < trailing_columns.len()
        || !columns[columns.len() - trailing_columns.len()..]
            .iter()
            .map(|column| column.name.as_str())
            .eq(trailing_columns.iter().map(|(name, _)| *name))
    {
        return Err(etl_error!(
            ErrorKind::CorruptedTableSchema,
            "ClickHouse table is missing trailing ETL columns",
            "The destination table cannot be reconciled safely for schema recovery."
        ));
    }

    Ok(columns[..columns.len() - trailing_columns.len()]
        .iter()
        .map(|column| column.name.clone())
        .collect())
}

/// Builds the idempotent metadata suffix of an already completed structural
/// schema plan.
///
/// Recovery starts from the validated full plan and derives a synthetic plan
/// only because the physical table is already at the structural target.
fn metadata_only_schema_plan(
    completed_plan: &SchemaPlan,
    target_schema: &ReplicatedTableSchema,
) -> EtlResult<SchemaPlan> {
    let altered_columns = completed_plan
        .diff()
        .altered_columns
        .iter()
        .filter_map(|change| {
            // Structural recovery established the target name, so compare the
            // remaining metadata from that physical endpoint.
            let mut before_column_schema = change.before_column_schema().clone();
            before_column_schema.name.clone_from(&change.after_column_schema().name);
            ColumnMetadataChange::between(&before_column_schema, change.after_column_schema())
        })
        .collect();

    let target_column_names: Vec<_> = target_schema
        .destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING)
        .map(|column| column.name)
        .collect();
    SchemaDiff::new(Vec::new(), Vec::new(), altered_columns)
        .plan_for_column_names(
            target_column_names.clone(),
            target_column_names,
            CLICKHOUSE_COLUMN_NAME_MAPPING,
        )
        .map_err(Into::into)
}

/// Recoverable physical endpoint of a nontransactional ClickHouse schema
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClickHouseSchemaRecoveryEndpoint {
    /// No structural operation committed, so the full plan can run.
    Previous,
    /// Every structural operation committed, so only metadata work remains.
    Target,
}

/// Classifies only unambiguous schema endpoints; intermediate DDL prefixes and
/// name-reuse transitions require manual recovery.
fn classify_clickhouse_schema_recovery_endpoint(
    table_id: TableId,
    actual_column_names: &[String],
    previous_column_names: &[String],
    target_column_names: &[String],
    has_ambiguous_name_reuse: bool,
) -> EtlResult<ClickHouseSchemaRecoveryEndpoint> {
    if has_ambiguous_name_reuse {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "ClickHouse schema recovery is ambiguous",
            format!(
                "Table {table_id} reuses a column name for a different logical column, so \
                 Applying metadata cannot distinguish the old and target physical schemas. Manual \
                 recovery is required."
            )
        ));
    }

    if actual_column_names == previous_column_names && previous_column_names != target_column_names
    {
        Ok(ClickHouseSchemaRecoveryEndpoint::Previous)
    } else if actual_column_names == target_column_names {
        Ok(ClickHouseSchemaRecoveryEndpoint::Target)
    } else {
        Err(etl_error!(
            ErrorKind::InvalidState,
            "ClickHouse schema recovery found a partial DDL state",
            format!(
                "Table {table_id} matches neither the recoverable previous endpoint nor the \
                 target endpoint. Manual recovery is required."
            )
        ))
    }
}

/// Returns where an added column belongs among the currently present user
/// columns according to the final replicated order.
fn clickhouse_add_column_insertion_index(
    current_column_names: &[String],
    final_column_index_by_name: &HashMap<String, usize>,
    added_column_name: &str,
) -> Option<usize> {
    let final_column_index = *final_column_index_by_name.get(added_column_name)?;
    Some(
        current_column_names
            .iter()
            .position(|name| {
                final_column_index_by_name
                    .get(name.as_str())
                    .is_some_and(|index| *index > final_column_index)
            })
            .unwrap_or(current_column_names.len()),
    )
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

/// Rejects the previous MergeTree column layout without attempting repair.
///
/// This check is only relevant to tables created during the closed alpha.
///
/// Only the missing transaction ordinal is recognized: other name/order drift,
/// metadata types, and an existing source column with that name are not treated
/// as this upgrade.
fn reject_legacy_merge_tree_layout(
    clickhouse_table_name: &str,
    expected_columns: &[ClickHouseTableColumn],
    actual_columns: &[ClickHouseTableColumn],
) -> EtlResult<()> {
    let Some((last_column, legacy_columns)) = expected_columns.split_last() else {
        return Ok(());
    };
    let [.., operation, lsn] = actual_columns else {
        return Ok(());
    };
    if last_column.name != CDC_TX_ORDINAL_COLUMN_NAME
        || operation.name != CDC_OPERATION_COLUMN_NAME
        || operation.type_name != "String"
        || lsn.name != CDC_LSN_COLUMN_NAME
        || lsn.type_name != "UInt64"
        || actual_columns.iter().any(|column| column.name == CDC_TX_ORDINAL_COLUMN_NAME)
        || !actual_columns
            .iter()
            .map(|column| &column.name)
            .eq(legacy_columns.iter().map(|column| &column.name))
    {
        return Ok(());
    }

    Err(etl_error!(
        ErrorKind::CorruptedTableSchema,
        "ClickHouse MergeTree table requires a transaction ordinal upgrade",
        format!(
            "Table '{}' uses the previous MergeTree layout without '{}'. Stop all writers, verify \
             the table schema, add '{} UInt64 DEFAULT 0' after '{}', then restart only upgraded \
             writers with the existing ETL metadata and checkpoints. Historical event order and \
             stale primary-key rows cannot be repaired by this column addition; reset and recopy \
             the table if a fresh current-state baseline is required. ETL does not migrate the \
             table automatically.",
            clickhouse_table_name,
            CDC_TX_ORDINAL_COLUMN_NAME,
            CDC_TX_ORDINAL_COLUMN_NAME,
            CDC_LSN_COLUMN_NAME,
        )
    ))
}

/// Builds the `RowBinaryWithNamesAndTypes` layout from the actual ClickHouse
/// table schema after checking it against the expected columns.
///
/// Column names, order, and types must match the stored replication schema.
/// The only accepted difference is an outer `Nullable(...)` on the physical
/// column: publication-mask additions use `Nullable(T)` without a source
/// default so historical rows remain unknown, and relaxed `NOT NULL` columns
/// stay nullable. The layout then carries the null-marker byte ClickHouse
/// expects on the wire.
///
/// Any other drift, such as a source type change or an external `ALTER`,
/// surfaces as `CorruptedTableSchema` rather than misaligned or reinterpreted
/// RowBinary bytes.
fn row_binary_layout_from_clickhouse_columns(
    clickhouse_table_name: &str,
    expected_columns: &[ClickHouseTableColumn],
    actual_columns: &[ClickHouseTableColumn],
) -> EtlResult<Arc<RowBinaryLayout>> {
    reject_legacy_merge_tree_layout(clickhouse_table_name, expected_columns, actual_columns)?;
    let expected_names =
        || summarize_column_names(expected_columns.iter().map(|c| c.name.as_str()));
    let actual_names = || summarize_column_names(actual_columns.iter().map(|c| c.name.as_str()));
    if actual_columns.len() != expected_columns.len() {
        return Err(etl_error!(
            ErrorKind::CorruptedTableSchema,
            "ClickHouse destination table columns do not match the stored replication schema",
            format!(
                "Destination table '{}' has {} columns, but the stored replication schema expects \
                 {}. Expected columns: {}. Actual columns: {}.",
                clickhouse_table_name,
                actual_columns.len(),
                expected_columns.len(),
                expected_names(),
                actual_names()
            )
        ));
    }

    for (index, (actual_column, expected_column)) in
        actual_columns.iter().zip(expected_columns).enumerate()
    {
        if actual_column.name != expected_column.name {
            return Err(etl_error!(
                ErrorKind::CorruptedTableSchema,
                "ClickHouse destination table columns do not match the stored replication schema",
                format!(
                    "Destination table '{}' has column '{}' at position {}, but the stored \
                     replication schema expects '{}'. Expected columns: {}. Actual columns: {}.",
                    clickhouse_table_name,
                    actual_column.name,
                    index + 1,
                    expected_column.name,
                    expected_names(),
                    actual_names()
                )
            ));
        }

        let actual_value_type = actual_column
            .type_name
            .strip_prefix("Nullable(")
            .and_then(|inner| inner.strip_suffix(')'))
            .unwrap_or(&actual_column.type_name);
        if actual_value_type != expected_column.type_name {
            return Err(etl_error!(
                ErrorKind::CorruptedTableSchema,
                "ClickHouse destination column type does not match the stored replication schema",
                format!(
                    "Destination table '{}' column '{}' has type '{}', but the stored replication \
                     schema expects '{}'. Resynchronize the table after a source column type \
                     change or an external ALTER.",
                    clickhouse_table_name,
                    actual_column.name,
                    actual_column.type_name,
                    expected_column.type_name
                )
            ));
        }
    }

    Ok(Arc::new(RowBinaryLayout::new(actual_columns)))
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
    /// ClickHouse's recommended bulk-insert range (10k - 100k rows per INSERT)
    /// for typical CDC payload widths.
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

    /// Client-side `tokio::time::timeout` for `op`: `server_timeout_for(op) +
    /// client_timeout_epsilon`.
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
    ///
    /// `retryable` says whether that error may succeed when sent again. DDL and
    /// schema queries then get a timed retry instead of stopping for an
    /// operator. Connectivity checks and inserts already get a timed retry.
    pub(crate) fn failed_kind(self, retryable: bool) -> ErrorKind {
        match (self, retryable) {
            (ClickHouseOperationKind::Insert, _) => ErrorKind::DestinationAtomicBatchRetryable,
            (ClickHouseOperationKind::ConnectivityCheck, _)
            | (ClickHouseOperationKind::SchemaQuery | ClickHouseOperationKind::Ddl, true) => {
                ErrorKind::DestinationConnectionFailed
            }
            (ClickHouseOperationKind::SchemaQuery | ClickHouseOperationKind::Ddl, false) => {
                ErrorKind::DestinationQueryFailed
            }
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
pub struct ClickHouseDestination<S> {
    /// Write-path state and operations shared by all destination entrypoints.
    writer: DestinationWriter<S>,
    /// Lifecycle registry for background event-write tasks.
    ///
    /// [`Destination::write_events`] admits its work here and returns;
    /// destructive table resets drain the registry to fence admitted work.
    tasks: TaskRegistry,
    /// Per-table ordering of event batches; see [`EventBatchFences`].
    fences: Arc<EventBatchFences>,
}

// Manual impl: `S` sits behind `Arc`, so cloning must not require `S: Clone`.
impl<S> Clone for ClickHouseDestination<S> {
    fn clone(&self) -> Self {
        Self {
            writer: self.writer.clone(),
            tasks: self.tasks.clone(),
            fences: Arc::clone(&self.fences),
        }
    }
}

/// Applied ClickHouse table state cached for the insert hot path.
#[derive(Clone)]
struct ClickHouseTableCacheEntry {
    /// Destination table name selected by durable metadata.
    table_name: String,
    /// Exact applied schema endpoint validated before this entry was inserted.
    metadata: DestinationTableMetadata,
    /// Checked `RowBinaryWithNamesAndTypes` layout, including the trailing CDC
    /// columns.
    layout: Arc<RowBinaryLayout>,
}

/// Drops `table_id`'s cached layout when ClickHouse rejected an insert that
/// used `layout` because its column list or header no longer matches the
/// table, then returns the insert's error.
///
/// That happens when the table changed after the layout was loaded, for
/// example through an external `ALTER`. Evicting makes the next write reload
/// and check the table, which reports the drift as `CorruptedTableSchema`
/// instead of retrying the same rejected header. Other failures, such as
/// timeouts and network errors, keep the layout so a retry needs no extra
/// schema query. The entry is removed only while it still holds `layout`, so a
/// failed insert cannot evict a layout that a concurrent writer already
/// reloaded.
fn evict_layout_after_rejected_insert(
    table_cache: &RwLock<HashMap<TableId, Arc<ClickHouseTableCacheEntry>>>,
    table_id: TableId,
    layout: &Arc<RowBinaryLayout>,
    failure: InsertRowsError,
) -> EtlError {
    if failure.is_layout_rejection() {
        let mut guard = table_cache.write();
        if guard.get(&table_id).is_some_and(|entry| Arc::ptr_eq(&entry.layout, layout)) {
            guard.remove(&table_id);
        }
    }
    failure.into()
}

/// Execution context captured by ClickHouse background event tasks.
///
/// Before resetting a table, [`ClickHouseDestination`] retains exclusive
/// access to its [`TaskRegistry`] while waiting for every admitted event task
/// to finish. A task that captured the complete destination could later access
/// that same task registry, causing the reset to wait for the task while the
/// task waits for the reset-held registry.
///
/// This type contains the state needed to execute writes but deliberately
/// omits [`TaskRegistry`], making that recursive registry access unavailable
/// through the task's execution context. It omits [`EventBatchFences`] for the
/// same reason. A task already holds the fences of every table it writes, so
/// it must never wait on them again.
struct DestinationWriter<S> {
    /// HTTP client used for all DDL and RowBinary INSERT traffic.
    client: ClickHouseClient,
    /// Per-INSERT byte budget; gates intermediate flushes within a single
    /// `write_table_rows` / `write_events` call.
    inserter_config: ClickHouseInserterConfig,
    /// Schema/state store used to persist destination table metadata (Creating
    /// / Applying / Applied) and to look up replicated schemas.
    store: Arc<S>,
    /// Source table ID -> validated applied ClickHouse table state.
    ///
    /// Populated lazily on first encounter of a table and consulted on the hot
    /// insert path. `std::sync::RwLock` is sufficient: every critical section
    /// is a brief in-memory map op with no `.await` inside, so the async
    /// `tokio::sync::RwLock` would be needless overhead.
    table_cache: Arc<RwLock<HashMap<TableId, Arc<ClickHouseTableCacheEntry>>>>,
    /// Per-`table_id` locks serialising first-time table creation.
    ///
    /// The two ctid copy workers spawned when `max_copy_connections > 1` share
    /// this destination (it is `Clone` over `Arc` state), so without a guard
    /// both fall through the cache miss in [`Self::prepare_table_for_writes`]
    /// and issue racing `CREATE TABLE` / `CREATE VIEW` statements. On
    /// ClickHouse Cloud the replicated `... IF NOT EXISTS` is not atomic across
    /// replicas, so the loser fails with "DDL failed". A `tokio::sync::Mutex`
    /// (held across the DDL `.await`) per table makes the second worker wait,
    /// then fall through the post-lock cache re-check. The outer map is guarded
    /// by a brief, await-free `parking_lot::Mutex` and grows at most one entry
    /// per replicated table.
    create_locks: Arc<Mutex<HashMap<TableId, Arc<tokio::sync::Mutex<()>>>>>,
}

// Manual impl: `S` sits behind `Arc`, so cloning must not require `S: Clone`.
impl<S> Clone for DestinationWriter<S> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            inserter_config: self.inserter_config,
            store: Arc::clone(&self.store),
            table_cache: Arc::clone(&self.table_cache),
            create_locks: Arc::clone(&self.create_locks),
        }
    }
}

/// Returns the id of every table that `events` write, in ascending order.
///
/// Transaction markers and unsupported events touch no table.
fn batch_table_ids(events: &[Event]) -> BTreeSet<TableId> {
    let mut table_ids = BTreeSet::new();
    for event in events {
        match event {
            Event::Insert(insert) => {
                table_ids.insert(insert.replicated_table_schema.id());
            }
            Event::Update(update) => {
                table_ids.insert(update.replicated_table_schema.id());
            }
            Event::Delete(delete) => {
                table_ids.insert(delete.replicated_table_schema.id());
            }
            Event::Relation(relation) => {
                table_ids.insert(relation.replicated_table_schema.id());
            }
            Event::Truncate(truncate) => {
                table_ids.extend(truncate.truncated_tables.iter().map(ReplicatedTableSchema::id));
            }
            Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
        }
    }
    table_ids
}

/// Per-table fences that keep event batches for one table in dispatch order.
///
/// The apply loop keeps at most one event batch in flight per worker. Any
/// error that exits the apply loop breaks that guarantee. The loop abandons
/// its pending batch, but the batch task keeps running. The retried attempt
/// then replays from the last flushed LSN through this same destination.
/// Without a fence, the replay's `TRUNCATE` can overtake the abandoned
/// `INSERT`, and the insert then restores rows the truncate removed.
///
/// [`Destination::write_events`] acquires the fence of every table the batch
/// touches before it spawns the batch task. The task holds the fences until the
/// batch is acknowledged. Acquiring in the caller is what guarantees the
/// order. The apply loop dispatches batches in source stream order, so fences
/// taken there follow that order too. A lock taken inside the task would
/// follow the scheduler instead.
///
/// A fence is not the same lock as a `create_locks` entry. A fence spans a
/// whole batch. Inside the batch, DDL takes the create lock one statement at a
/// time. A tokio mutex cannot be locked again by the task that already holds
/// it, so one lock cannot play both roles.
struct EventBatchFences {
    /// One fence per table, created on first use. The map lock is a brief,
    /// await-free `parking_lot::Mutex`.
    fences: Mutex<HashMap<TableId, Arc<tokio::sync::Mutex<()>>>>,
}

impl EventBatchFences {
    fn new() -> Self {
        Self { fences: Mutex::new(HashMap::new()) }
    }

    /// Acquires the fence of every table that `events` write and returns the
    /// guards. Dropping the guards releases the fences.
    ///
    /// Fences are acquired in ascending table id order. Two batches that
    /// share tables therefore lock them in the same order, so neither can
    /// hold one fence while waiting for the other's. Guards live inside the
    /// batch task, so aborting the task releases them too.
    async fn acquire(&self, events: &[Event]) -> Vec<OwnedMutexGuard<()>> {
        let table_ids = batch_table_ids(events);
        let fences: Vec<Arc<tokio::sync::Mutex<()>>> = {
            let mut map = self.fences.lock();
            table_ids
                .into_iter()
                .map(|table_id| Arc::clone(map.entry(table_id).or_default()))
                .collect()
        };

        let mut guards = Vec::with_capacity(fences.len());
        for fence in fences {
            #[cfg(feature = "test-utils")]
            if fence.try_lock().is_err() {
                notify_fence_wait_for_tests();
            }
            guards.push(fence.lock_owned().await);
        }
        guards
    }
}

/// Tests waiting to hear that a batch dispatch had to wait for a fence.
#[cfg(feature = "test-utils")]
static FENCE_WAIT_OBSERVERS: Mutex<Vec<tokio::sync::oneshot::Sender<()>>> = Mutex::new(Vec::new());

/// Returns a receiver that fires the next time a batch dispatch has to wait
/// for a fence held by an earlier batch. Each receiver fires once.
#[cfg(feature = "test-utils")]
pub fn notify_on_fence_wait_for_tests() -> tokio::sync::oneshot::Receiver<()> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    FENCE_WAIT_OBSERVERS.lock().push(sender);
    receiver
}

/// Fires every armed fence-wait observer.
#[cfg(feature = "test-utils")]
fn notify_fence_wait_for_tests() {
    for observer in FENCE_WAIT_OBSERVERS.lock().drain(..) {
        let _ = observer.send(());
    }
}

impl<S> ClickHouseDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new `ClickHouseDestination`.
    ///
    /// When using an `https://` URL, TLS is handled automatically by the `rustls-tls`
    /// feature using webpki root certificates.
    ///
    /// This constructor permits trusted local destinations. Use
    /// [`Self::new_public`] for untrusted HTTPS configuration.
    pub fn new(
        url: Url,
        user: impl Into<String>,
        password: Option<String>,
        database: impl Into<String>,
        inserter_config: ClickHouseInserterConfig,
        client_config: ClickHouseClientConfig,
        store: S,
    ) -> EtlResult<Self> {
        let client = ClickHouseClient::new(url, user, password, database, client_config);
        Ok(Self::from_client(client, inserter_config, store))
    }

    /// Creates a destination that connects only to public HTTPS addresses.
    pub async fn new_public(
        url: Url,
        user: impl Into<String>,
        password: Option<String>,
        database: impl Into<String>,
        inserter_config: ClickHouseInserterConfig,
        client_config: ClickHouseClientConfig,
        store: S,
    ) -> EtlResult<Self> {
        let client =
            ClickHouseClient::new_public(url, user, password, database, client_config).await?;
        Ok(Self::from_client(client, inserter_config, store))
    }

    /// Creates a destination from a configured ClickHouse client.
    fn from_client(
        client: ClickHouseClient,
        inserter_config: ClickHouseInserterConfig,
        store: S,
    ) -> Self {
        register_metrics();
        Self {
            writer: DestinationWriter {
                client,
                inserter_config,
                store: Arc::new(store),
                table_cache: Arc::new(RwLock::new(HashMap::new())),
                create_locks: Arc::new(Mutex::new(HashMap::new())),
            },
            tasks: TaskRegistry::new(),
            fences: Arc::new(EventBatchFences::new()),
        }
    }

    /// Probes the server version and rejects unsupported engine/version pairs.
    /// Currently the only gate: ReplacingMergeTree requires CH >= 23.5.
    pub async fn validate_engine_support(&self) -> EtlResult<()> {
        let server_version = self.writer.client.server_version().await?;
        ensure_engine_supported(self.writer.inserter_config.engine, server_version)
    }

    /// Writes an initial-copy batch directly to the destination table,
    /// awaiting the write inline instead of reporting through the trait's
    /// async completion result.
    ///
    /// Test-only entrypoint for exercising the production write path without
    /// pipeline plumbing. It passes no [`TableCopyBatchId`], so its inserts
    /// send no deduplication token.
    #[cfg(feature = "test-utils")]
    pub async fn write_table_rows(
        &self,
        schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.writer.write_table_rows_inner(schema, None, table_rows).await
    }

    /// Dispatches a streaming event batch through the [`Destination`] trait
    /// and awaits its asynchronous completion.
    ///
    /// Test-only entrypoint for exercising the production dispatch path,
    /// including task admission and the async result channel, without
    /// pipeline plumbing.
    #[cfg(feature = "test-utils")]
    pub async fn write_events(&self, events: Vec<Event>) -> EtlResult<()>
    where
        S: 'static,
    {
        etl::test_utils::destination::write_events(self, WriteEventsDurability::MayDefer, events)
            .await
            .map(|_| ())
    }
}

impl<S> DestinationWriter<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a ClickHouse table for a never-before-seen `table_id`,
    /// bracketing the DDL with `DestinationTableMetadata` writes so the
    /// operation is crash-recoverable.
    ///
    /// Sequence:
    /// 1. Persist `Creating` metadata (so a crash between this write and step 3
    ///    leaves a marker that lets restart logic detect the interrupted
    ///    operation).
    /// 2. Execute `CREATE TABLE IF NOT EXISTS` against ClickHouse.
    /// 3. Persist `Applied` metadata.
    ///
    /// Recovery is handled by `prepare_table_for_writes`: on restart, a
    /// `Creating` row signals that the previous run died mid-creation, so it
    /// re-runs the idempotent DDL and transitions the metadata to `Applied`
    /// itself.
    async fn create_table_with_metadata(
        &self,
        table_id: TableId,
        clickhouse_table_name: &str,
        schema: &ReplicatedTableSchema,
        snapshot_id: etl::schema::SnapshotId,
        replication_mask: etl::schema::ReplicationMask,
    ) -> EtlResult<()> {
        let metadata = DestinationTableMetadata::new_creating(
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
    // shared-storage variants (`ReplacingMergeTree` ->
    // `SharedReplacingMergeTree`). These are drop-in equivalents, so
    // `system.tables.engine` reads back the `Shared`-prefixed name even though
    // the pipeline configured the plain one.

    /// Rejects writing to a pre-existing ClickHouse table whose engine does not
    /// match the configured one. No-op if the table doesn't exist yet.
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

    /// Rejects creating a table when ClickHouse already has an object with its
    /// name, or with its `__current` view name under ReplacingMergeTree.
    ///
    /// Without destination metadata ETL cannot prove it owns such an object.
    /// `CREATE ... IF NOT EXISTS` would keep it, and the copy would land on top
    /// of rows that outrank copy rows.
    async fn ensure_table_absent(&self, clickhouse_table_name: &str) -> EtlResult<()> {
        let mut names = vec![clickhouse_table_name.to_owned()];
        if matches!(self.inserter_config.engine, ClickHouseEngine::ReplacingMergeTree) {
            names.push(format!("{clickhouse_table_name}{CURRENT_VIEW_SUFFIX}"));
        }
        for name in names {
            if self.client.table_engine(&name).await?.is_some() {
                return Err(etl_error!(
                    ErrorKind::DestinationTableAlreadyExists,
                    "ClickHouse destination table already exists",
                    format!(
                        "Table '{name}' exists, but this pipeline has no destination metadata \
                         proving ownership. Drop the table or use another database before \
                         retrying."
                    )
                ));
            }
        }
        Ok(())
    }

    /// Issues the engine-correct `CREATE TABLE`, and under ReplacingMergeTree
    /// also the companion `CREATE VIEW "<table>__current"`. Both statements are
    /// `IF NOT EXISTS`, so retries on the recovery path are idempotent.
    async fn issue_create_table_stmt(
        &self,
        clickhouse_table_name: &str,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let engine = self.inserter_config.engine;
        let destination_columns: Vec<_> =
            schema.destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING).collect();
        let ddl = create_table_sql(engine, clickhouse_table_name, &destination_columns)?;
        self.client.execute_ddl(DdlKind::CreateTable, &ddl).await?;

        if matches!(engine, ClickHouseEngine::ReplacingMergeTree) {
            let view_ddl = create_current_view_sql(clickhouse_table_name, &destination_columns);
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

        let destination_columns: Vec<_> =
            schema.destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING).collect();
        let create_view = create_current_view_sql(clickhouse_table_name, &destination_columns);
        self.client.execute_ddl(DdlKind::CreateView, &create_view).await
    }

    /// Returns the lock serializing table preparation and schema transitions.
    fn table_preparation_lock(&self, table_id: TableId) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.create_locks.lock();
        Arc::clone(guard.entry(table_id).or_default())
    }

    /// Prepares the ETL-owned ClickHouse table for writes, returning
    /// `(clickhouse_table_name, layout)`.
    ///
    /// Applied metadata never triggers repair DDL. A cold cache performs the
    /// one read-only schema load required to build the checked RowBinary
    /// layout; missing or externally modified tables fail that load.
    async fn prepare_table_for_writes(
        &self,
        schema: &ReplicatedTableSchema,
    ) -> EtlResult<(String, Arc<RowBinaryLayout>)> {
        let table_id = schema.id();

        if let Some(entry) = self.table_cache.read().get(&table_id).cloned() {
            ensure_destination_schema_matches_metadata(
                "ClickHouse",
                table_id,
                &entry.metadata,
                schema,
            )?;
            return Ok((entry.table_name.clone(), Arc::clone(&entry.layout)));
        }

        // Serialise the first-time create/recover path per `table_id`. When
        // `max_copy_connections > 1` the two ctid copy workers share this
        // destination and would otherwise both fall through the cache miss
        // above and issue racing CREATE TABLE / CREATE VIEW statements; on
        // ClickHouse Cloud the replicated `... IF NOT EXISTS` is not atomic
        // across replicas, so the loser fails with "DDL failed". See the
        // `create_locks` field doc.
        let table_lock = self.table_preparation_lock(table_id);
        let _create_guard = table_lock.lock().await;

        // Another caller may have populated the cache while this caller waited.
        if let Some(entry) = self.table_cache.read().get(&table_id).cloned() {
            ensure_destination_schema_matches_metadata(
                "ClickHouse",
                table_id,
                &entry.metadata,
                schema,
            )?;
            return Ok((entry.table_name.clone(), Arc::clone(&entry.layout)));
        }

        // Load durable metadata once under the lock so table identity and state
        // cannot race a schema transition.
        let metadata = self.store.get_destination_table_metadata(table_id).await?;
        let clickhouse_table_name = metadata.as_ref().map_or_else(
            || try_stringify_table_name(schema.name()),
            |metadata| Ok(metadata.table_id().to_owned()),
        )?;
        if let Some(metadata) = &metadata {
            ensure_destination_schema_matches_metadata("ClickHouse", table_id, metadata, schema)?;
        }

        match metadata {
            None => {
                validate_clickhouse_table_shape(schema, self.inserter_config.engine)?;
                validate_clickhouse_table_name(
                    &clickhouse_table_name,
                    schema.name(),
                    self.inserter_config.engine,
                )?;
                // An existing object without metadata is not ETL's to reuse.
                self.ensure_table_absent(&clickhouse_table_name).await?;
                self.create_table_with_metadata(
                    table_id,
                    &clickhouse_table_name,
                    schema,
                    schema.inner().snapshot_id,
                    schema.replication_mask().clone(),
                )
                .await?;
            }
            Some(metadata) if metadata.is_pending() => {
                validate_clickhouse_table_shape(schema, self.inserter_config.engine)?;
                validate_clickhouse_table_name(
                    &clickhouse_table_name,
                    schema.name(),
                    self.inserter_config.engine,
                )?;
                self.ensure_engine_matches(&clickhouse_table_name).await?;
                self.recover_pending_metadata(table_id, &clickhouse_table_name, schema, metadata)
                    .await?;
            }
            Some(_) => {}
        }

        // Build the layout from the actual ClickHouse schema. This matters
        // after `ALTER TABLE ADD COLUMN`: ClickHouse scalar columns are forced
        // to `Nullable(T)` even when the Postgres column is `NOT NULL`, so
        // RowBinary must include the nullable marker byte ClickHouse expects.
        let actual_columns = self.client.table_columns(&clickhouse_table_name).await?;
        let layout = row_binary_layout_from_clickhouse_columns(
            &clickhouse_table_name,
            &expected_clickhouse_columns(schema, self.inserter_config.engine),
            &actual_columns,
        )?;

        let applied_metadata = DestinationTableMetadata::new_applied(
            clickhouse_table_name.clone(),
            schema.inner().snapshot_id,
            schema.replication_mask().clone(),
        );
        let entry = {
            let mut guard = self.table_cache.write();
            Arc::clone(guard.entry(table_id).or_insert_with(|| {
                Arc::new(ClickHouseTableCacheEntry {
                    table_name: clickhouse_table_name.clone(),
                    metadata: applied_metadata,
                    layout,
                })
            }))
        };

        Ok((entry.table_name.clone(), Arc::clone(&entry.layout)))
    }

    /// Recovers initial creation or an unambiguous schema-change endpoint and
    /// transitions metadata to `Applied`.
    ///
    /// ClickHouse DDL is nontransactional, so intermediate operation prefixes
    /// require manual recovery rather than replaying the plan from the start.
    async fn recover_pending_metadata(
        &self,
        table_id: TableId,
        clickhouse_table_name: &str,
        schema: &ReplicatedTableSchema,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        warn!("table {} has pending metadata, recovering interrupted operation", table_id);

        ensure_destination_schema_matches_metadata("ClickHouse", table_id, &metadata, schema)?;

        match metadata.table_schema().clone() {
            DestinationTableSchema::Applying {
                previous_snapshot_id,
                previous_replication_mask,
                ..
            } => {
                // Recovery replays the interrupted diff from the previous
                // snapshot to the target snapshot recorded in the metadata.
                let prev_snapshot_id = previous_snapshot_id;
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
                let actual_columns = self.client.table_columns(clickhouse_table_name).await?;
                let old_schema = ReplicatedTableSchema::from_mask(
                    Arc::clone(&old_table_schema),
                    previous_replication_mask,
                );
                let plan = old_schema.plan_schema_change(schema, CLICKHOUSE_COLUMN_NAME_MAPPING)?;
                ensure_clickhouse_renames_are_supported(clickhouse_table_name, &plan)?;
                for endpoint_schema in [&old_schema, schema] {
                    reject_legacy_merge_tree_layout(
                        clickhouse_table_name,
                        &expected_clickhouse_columns(endpoint_schema, self.inserter_config.engine),
                        &actual_columns,
                    )?;
                }
                let actual_user_column_names =
                    clickhouse_user_column_names(&actual_columns, self.inserter_config.engine)?;
                let old_column_names: Vec<_> = old_schema
                    .destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING)
                    .map(|column| column.name)
                    .collect();
                let target_column_names: Vec<_> = schema
                    .destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING)
                    .map(|column| column.name)
                    .collect();
                let old_ordinal_by_name: HashMap<_, _> = old_table_schema
                    .column_schemas
                    .iter()
                    .map(|column| {
                        (
                            CLICKHOUSE_COLUMN_NAME_MAPPING.map_name(&column.name),
                            column.ordinal_position,
                        )
                    })
                    .collect();
                let has_reused_endpoint_name = schema.column_schemas().any(|target_column| {
                    let target_name = CLICKHOUSE_COLUMN_NAME_MAPPING.map_name(&target_column.name);
                    old_ordinal_by_name
                        .get(&target_name)
                        .is_some_and(|ordinal| *ordinal != target_column.ordinal_position)
                });
                let has_structural_operations = plan.ordered_operations().iter().any(|operation| {
                    matches!(
                        operation,
                        SchemaOperation::DropColumn { .. } | SchemaOperation::AddColumn { .. }
                    ) || matches!(operation, SchemaOperation::AlterColumn { alteration }
                        if alteration.kind() == ColumnAlterationKind::Rename)
                });

                let recovery_endpoint = classify_clickhouse_schema_recovery_endpoint(
                    table_id,
                    &actual_user_column_names,
                    &old_column_names,
                    &target_column_names,
                    has_reused_endpoint_name && has_structural_operations,
                )?;

                match recovery_endpoint {
                    ClickHouseSchemaRecoveryEndpoint::Previous => {
                        self.apply_schema_plan(clickhouse_table_name, &plan, &old_schema, schema)
                            .await?;
                    }
                    ClickHouseSchemaRecoveryEndpoint::Target => {
                        let metadata_plan = metadata_only_schema_plan(&plan, schema)?;
                        self.apply_schema_plan(
                            clickhouse_table_name,
                            &metadata_plan,
                            schema,
                            schema,
                        )
                        .await?;
                    }
                }
            }
            DestinationTableSchema::Creating { .. } => {
                self.issue_create_table_stmt(clickhouse_table_name, schema).await?;
            }
            DestinationTableSchema::Applied { .. } => {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "ClickHouse recovery received applied destination metadata",
                    format!("Table {table_id} does not have an interrupted destination operation")
                ));
            }
        }

        let actual_columns = self.client.table_columns(clickhouse_table_name).await?;
        row_binary_layout_from_clickhouse_columns(
            clickhouse_table_name,
            &expected_clickhouse_columns(schema, self.inserter_config.engine),
            &actual_columns,
        )?;

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await?;
        self.table_cache.write().remove(&table_id);
        Ok(())
    }

    async fn truncate_table_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let (clickhouse_table_name, _) = self.prepare_table_for_writes(schema).await?;
        self.client.truncate_table(&clickhouse_table_name).await
    }

    async fn drop_table_for_copy_inner(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        #[cfg(feature = "test-utils")]
        if std::mem::take(&mut *DROP_TABLE_FOR_COPY_FAILURE.lock()) {
            return Err(etl_error!(
                ErrorKind::DestinationError,
                "Injected ClickHouse table reset failure",
                "One-shot failure armed by arm_fail_drop_table_for_copy_once_for_tests"
            ));
        }

        // Destination metadata names the table this source table writes to. The
        // current source name differs from it after a rename.
        let metadata = self.store.get_destination_table_metadata(schema.id()).await?;
        let clickhouse_table_name = metadata.as_ref().map_or_else(
            || try_stringify_table_name(schema.name()),
            |metadata| Ok(metadata.table_id().to_owned()),
        )?;

        if matches!(self.inserter_config.engine, ClickHouseEngine::ReplacingMergeTree) {
            let drop_view = drop_current_view_sql(&clickhouse_table_name);
            self.client.execute_ddl(DdlKind::DropView, &drop_view).await?;
        }

        self.client.drop_table(&clickhouse_table_name).await?;
        self.table_cache.write().remove(&schema.id());

        Ok(())
    }

    async fn write_table_rows_inner(
        &self,
        schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let (clickhouse_table_name, layout) = self.prepare_table_for_writes(schema).await?;

        let engine = self.inserter_config.engine;
        let rows: Vec<Vec<ClickHouseValue>> = table_rows
            .into_iter()
            .map(|table_row| {
                let mut values: Vec<ClickHouseValue> = table_row
                    .into_values()
                    .into_iter()
                    .map(cell_to_clickhouse_value)
                    .collect::<EtlResult<Vec<_>>>()?;
                // Initial-copy rows are tagged as INSERT with LSN 0 /
                // tx_ordinal 0 (sentinel meaning "this row pre-dates the
                // streaming cursor"). For ReplacingMergeTree, any streaming
                // event then wins on FINAL because its packed `_etl_version` is
                // non-zero.
                append_cdc_columns(
                    &mut values,
                    CdcOperation::Insert,
                    EventSequenceKey::new(PgLsn::from(0), 0),
                    engine,
                );
                Ok(values)
            })
            .collect::<EtlResult<_>>()?;

        self.client
            .insert_rows(
                &clickhouse_table_name,
                rows,
                &layout,
                self.inserter_config.max_bytes_per_insert,
                batch_id,
                COPY_REPLICATION_PATH,
            )
            .await
            .map_err(|failure| {
                evict_layout_after_rejected_insert(&self.table_cache, schema.id(), &layout, failure)
            })
    }

    /// Handles relation metadata, applying the schema diff for a new snapshot
    /// or recovering an interrupted transition.
    async fn handle_relation_event(&self, new_schema: &ReplicatedTableSchema) -> EtlResult<()> {
        validate_clickhouse_schema_capabilities(new_schema, self.inserter_config.engine)?;

        let table_id = new_schema.id();
        let new_snapshot_id = new_schema.inner().snapshot_id;
        let new_replication_mask = new_schema.replication_mask().clone();

        // Serialize cache reconstruction and schema transitions. This ensures a
        // cold-cache writer cannot repopulate an old RowBinary layout after the
        // relation handler invalidates it.
        let table_lock = self.table_preparation_lock(table_id);
        let _preparation_guard = table_lock.lock().await;

        let metadata =
            self.store.get_destination_table_metadata(table_id).await?.ok_or_else(|| {
                etl_error!(
                    ErrorKind::CorruptedTableSchema,
                    "Destination metadata missing for ClickHouse schema change",
                    format!(
                        "Table {} received schema snapshot {}, but destination metadata from \
                         initial synchronization was not found.",
                        table_id, new_snapshot_id
                    )
                )
            })?;

        // A relation event identifies the exact target schema through its
        // snapshot ID and replication mask. It can therefore resume an
        // interrupted change without inventing a DML event sequence key.
        if metadata.is_pending() {
            let clickhouse_table_name = metadata.table_id().to_owned();
            self.recover_pending_metadata(table_id, &clickhouse_table_name, new_schema, metadata)
                .await?;
            return Ok(());
        }

        let current_snapshot_id = metadata.snapshot_id();
        let current_replication_mask = metadata.replication_mask().clone();

        // At-least-once delivery can replay an older relation or an
        // equal-snapshot mask from a deployment that missed its logical schema
        // message. Neither carries ordering sufficient to drive ClickHouse DDL.
        ensure_relation_schema_transition(
            "ClickHouse",
            table_id,
            current_snapshot_id,
            &current_replication_mask,
            new_snapshot_id,
            &new_replication_mask,
        )?;

        if current_snapshot_id == new_snapshot_id {
            debug!(
                table_id = %table_id,
                snapshot_id = %new_snapshot_id,
                "clickhouse table schema unchanged"
            );
            return Ok(());
        }

        info!(
            table_id = %table_id,
            current_snapshot_id = %current_snapshot_id,
            new_snapshot_id = %new_snapshot_id,
            "clickhouse table schema change detected"
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

        let clickhouse_table_name = metadata.table_id();
        let plan = current_schema.plan_schema_change(new_schema, CLICKHOUSE_COLUMN_NAME_MAPPING)?;
        ensure_clickhouse_renames_are_supported(clickhouse_table_name, &plan)?;
        ensure_clickhouse_type_changes_are_supported(clickhouse_table_name, &plan)?;
        if matches!(self.inserter_config.engine, ClickHouseEngine::ReplacingMergeTree) {
            reject_pk_alters_under_replacing_merge_tree(
                clickhouse_table_name,
                plan.diff(),
                &current_schema,
                new_schema,
            )?;
        }
        // Applied metadata proves the table is at the current layout, so the
        // current endpoint alone identifies a closed-alpha MergeTree table.
        // Reject it before any cache, metadata, or DDL mutation so the operator
        // sees the upgrade instruction with the table and metadata untouched,
        // exactly as the DML and recovery paths behave.
        let actual_columns = self.client.table_columns(clickhouse_table_name).await?;
        reject_legacy_merge_tree_layout(
            clickhouse_table_name,
            &expected_clickhouse_columns(&current_schema, self.inserter_config.engine),
            &actual_columns,
        )?;
        // A cached RowBinary layout is valid only for Applied metadata. Remove
        // it before persisting Applying so cancellation cannot leave pending
        // durable metadata reachable through the old cache. If the metadata
        // write fails, the cache miss is harmless and can be repopulated from
        // the still-Applied metadata.
        self.table_cache.write().remove(&table_id);

        // Mark as Applying before DDL changes.
        let updated_metadata = DestinationTableMetadata::new_applied(
            clickhouse_table_name.to_owned(),
            current_snapshot_id,
            current_replication_mask,
        )
        .with_schema_change(new_snapshot_id, new_replication_mask)?;
        self.store.store_destination_table_metadata(table_id, updated_metadata.clone()).await?;

        if let Err(err) =
            self.apply_schema_plan(clickhouse_table_name, &plan, &current_schema, new_schema).await
        {
            warn!(
                table_id = %table_id,
                error = %err,
                "clickhouse table schema change failed; manual intervention may be required"
            );
            return Err(err);
        }

        // Mark as Applied.
        self.store
            .store_destination_table_metadata(table_id, updated_metadata.to_applied())
            .await?;

        info!(
            table_id = %table_id,
            snapshot_id = %new_snapshot_id,
            "clickhouse table schema change completed"
        );

        Ok(())
    }

    /// Translates a shared ordered schema plan into ClickHouse DDL and
    /// refreshes the ReplacingMergeTree current-state view when needed.
    ///
    /// New columns are placed at their after-schema position before the
    /// trailing CDC columns because RowBinary encoding is positional.
    ///
    /// ClickHouse does not support transactional DDL, so if the replicator is
    /// killed between individual ALTER statements the table may be left in a
    /// partially altered state. The `DestinationTableMetadata` Applying/Applied
    /// status tracks this for diagnostic purposes.
    async fn apply_schema_plan(
        &self,
        clickhouse_table_name: &str,
        plan: &SchemaPlan,
        before_schema: &ReplicatedTableSchema,
        after_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let is_replacing_merge_tree =
            matches!(self.inserter_config.engine, ClickHouseEngine::ReplacingMergeTree);
        ensure_clickhouse_type_changes_are_supported(clickhouse_table_name, plan)?;
        if plan.is_empty() {
            if is_replacing_merge_tree {
                self.refresh_current_view(clickhouse_table_name, after_schema).await?;
            }
            return Ok(());
        }

        // Inspect endpoint facts before applying anything. This validation does
        // not emit DDL; all DDL below still comes from `ordered_operations`.
        // ReplacingMergeTree cannot alter the source-primary-key expression
        // used as its sort and deduplication key.
        if is_replacing_merge_tree {
            reject_pk_alters_under_replacing_merge_tree(
                clickhouse_table_name,
                plan.diff(),
                before_schema,
                after_schema,
            )?;
        }

        // Keep the before physical order so additions can remain before the
        // trailing CDC columns even when earlier operations renamed or dropped
        // the earlier placement anchor.
        let mut user_column_names: Vec<String> = before_schema
            .destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING)
            .map(|column| column.name)
            .collect();
        let after_column_index_by_name: HashMap<_, _> = after_schema
            .destination_column_schemas(CLICKHOUSE_COLUMN_NAME_MAPPING)
            .enumerate()
            .map(|(index, column)| (column.name, index))
            .collect();

        // Translate the shared plan without revalidating names or regrouping
        // operations.
        for operation in plan.ordered_operations() {
            match operation {
                SchemaOperation::DropColumn { before_column_schema, reason: _ } => {
                    self.client
                        .drop_column(clickhouse_table_name, &before_column_schema.name)
                        .await?;
                    user_column_names.retain(|name| name != &before_column_schema.name);
                }
                SchemaOperation::AddColumn { after_column_schema, reason } => {
                    let (destination_column_schema, force_nullable) =
                        clickhouse_add_column_definition(after_column_schema, *reason);
                    if force_nullable
                        && !after_column_schema.nullable
                        && !is_array_type(&after_column_schema.typ)
                    {
                        warn!(
                            table_name = %clickhouse_table_name,
                            column_name = %after_column_schema.name,
                            "adding a source not null column as nullable in clickhouse; the \
                             destination schema will be more permissive"
                        );
                    }

                    let insertion_index = clickhouse_add_column_insertion_index(
                        &user_column_names,
                        &after_column_index_by_name,
                        &after_column_schema.name,
                    )
                    .ok_or_else(|| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "ClickHouse add-column after state is missing from the after schema",
                            format!(
                                "Table '{clickhouse_table_name}', column '{}'",
                                after_column_schema.name
                            )
                        )
                    })?;
                    let preceding_column_name = insertion_index
                        .checked_sub(1)
                        .map(|index| user_column_names[index].clone());
                    if *reason == ColumnPresenceChangeReason::ReplicationMask
                        && after_column_schema.default_expression.is_some()
                    {
                        warn!(
                            table_name = %clickhouse_table_name,
                            column_name = %after_column_schema.name,
                            "not applying the source default to a publication-added clickhouse \
                             column; the destination schema will differ from the logical source \
                             schema"
                        );
                    }
                    self.client
                        .add_column(
                            clickhouse_table_name,
                            &destination_column_schema,
                            preceding_column_name.as_deref(),
                            force_nullable,
                        )
                        .await?;
                    user_column_names.insert(insertion_index, after_column_schema.name.clone());
                }
                SchemaOperation::AlterColumn { alteration } => {
                    let before = alteration.before_column_schema();
                    let after = alteration.after_column_schema();
                    match alteration.kind() {
                        ColumnAlterationKind::Rename => {
                            self.client
                                .rename_column(clickhouse_table_name, &before.name, &after.name)
                                .await?;
                            if let Some(name) =
                                user_column_names.iter_mut().find(|name| **name == before.name)
                            {
                                after.name.clone_into(name);
                            }
                        }
                        ColumnAlterationKind::Type => {
                            // Validation above rejected every change of the
                            // mapped ClickHouse type, so the column already
                            // has the target type.
                        }
                        ColumnAlterationKind::Nullability => {
                            if !before.nullable && after.nullable {
                                self.client
                                    .drop_column_not_null(clickhouse_table_name, &before.name)
                                    .await?;
                            } else {
                                warn!(
                                    table_name = %clickhouse_table_name,
                                    column_name = %before.name,
                                    "clickhouse does not tighten an existing nullable column to \
                                     not null; keeping the destination column nullable"
                                );
                            }
                        }
                        ColumnAlterationKind::Default => {
                            // ETL writes every column on every insert, so a
                            // ClickHouse default only fills rows stored before
                            // the column was added. Changing it would change
                            // those rows, while Postgres keeps their add-time
                            // value.
                            warn!(
                                table_name = %clickhouse_table_name,
                                column_name = %before.name,
                                "skipping source column default change for clickhouse because it \
                                 would change rows stored before the column was added"
                            );
                        }
                    }
                }
            }
        }

        if is_replacing_merge_tree {
            self.refresh_current_view(clickhouse_table_name, after_schema).await?;
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
    ///
    /// Schema changes are applied only after all preceding inserts in the batch
    /// are complete: step 2 awaits every INSERT before step 3 runs any DDL, and
    /// the client pins `async_insert = 0`, so an insert acknowledgement implies
    /// the rows were written into the table and cannot be overtaken by a
    /// following `ALTER TABLE`.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut pending: HashMap<TableId, (ReplicatedTableSchema, Vec<PendingRow>)> =
                HashMap::new();

            // Accumulate data events until we hit a Truncate or Relation
            // boundary.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_) | Event::Relation(_)) {
                    break;
                }

                let Some(event) = event_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(insert) => {
                        let sequence_key = insert.event_sequence_key();
                        let table_id = insert.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (insert.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
                            operation: CdcOperation::Insert,
                            sequence_key,
                            cells: insert.table_row.into_values(),
                        });
                    }
                    Event::Update(update) => {
                        let sequence_key = update.event_sequence_key();
                        let source_updated_row = update.updated_table_row;
                        let source_old_row = update.old_table_row;
                        let rows_for_update = clickhouse_rows_for_update(
                            &update.replicated_table_schema,
                            source_updated_row,
                            source_old_row,
                        )?;
                        let table_id = update.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (update.replicated_table_schema, Vec::new()));

                        // A primary-key change produces two destination rows.
                        // Queue the old-key tombstone before the updated row
                        // under its new key.
                        if let Some(destination_old_key_tombstone) =
                            rows_for_update.destination_old_key_tombstone
                        {
                            entry.1.push(PendingRow {
                                operation: CdcOperation::Delete,
                                sequence_key,
                                cells: destination_old_key_tombstone.into_values(),
                            });
                        }

                        entry.1.push(PendingRow {
                            operation: CdcOperation::Update,
                            sequence_key,
                            cells: rows_for_update.destination_updated_row.into_values(),
                        });
                    }
                    Event::Delete(delete) => {
                        let sequence_key = delete.event_sequence_key();
                        let source_old_row = clickhouse_delete_old_row(
                            &delete.replicated_table_schema,
                            delete.old_table_row,
                        )?;
                        let destination_old_row = match source_old_row {
                            OldTableRow::Full(source_old_row) => source_old_row,
                            OldTableRow::Key(source_old_key_row) => {
                                expand_key_row(source_old_key_row, &delete.replicated_table_schema)?
                            }
                        };
                        let table_id = delete.replicated_table_schema.id();
                        let entry = pending
                            .entry(table_id)
                            .or_insert_with(|| (delete.replicated_table_schema, Vec::new()));
                        entry.1.push(PendingRow {
                            operation: CdcOperation::Delete,
                            sequence_key,
                            cells: destination_old_row.into_values(),
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
    /// ClickHouse, one task per table. No-op if `pending` is empty.
    ///
    /// All `prepare_table_for_writes` calls run sequentially before any insert
    /// is spawned, so a schema-resolution failure aborts the whole pass without
    /// any partial-write side effects.
    async fn flush_pending_rows(
        &self,
        pending: HashMap<TableId, (ReplicatedTableSchema, Vec<PendingRow>)>,
    ) -> EtlResult<()> {
        if pending.is_empty() {
            return Ok(());
        }

        let mut prepared: Vec<(TableId, String, Arc<RowBinaryLayout>, Vec<PendingRow>)> =
            Vec::with_capacity(pending.len());
        for (table_id, (schema, rows)) in pending {
            let (clickhouse_table_name, layout) = self.prepare_table_for_writes(&schema).await?;
            prepared.push((table_id, clickhouse_table_name, layout, rows));
        }

        let mut tasks: TaskGroup<()> = TaskGroup::new();
        let engine = self.inserter_config.engine;
        for (table_id, clickhouse_table_name, layout, rows) in prepared {
            let client = self.client.clone();
            let table_cache = Arc::clone(&self.table_cache);
            let max_bytes = self.inserter_config.max_bytes_per_insert;

            tasks.spawn(async move {
                let rows: Vec<Vec<ClickHouseValue>> = rows
                    .into_iter()
                    .map(|PendingRow { operation, sequence_key, cells }| {
                        let mut values: Vec<ClickHouseValue> = cells
                            .into_iter()
                            .map(cell_to_clickhouse_value)
                            .collect::<EtlResult<Vec<_>>>()?;
                        append_cdc_columns(&mut values, operation, sequence_key, engine);
                        Ok(values)
                    })
                    .collect::<EtlResult<_>>()?;

                client
                    .insert_rows(
                        &clickhouse_table_name,
                        rows,
                        &layout,
                        max_bytes,
                        None,
                        CDC_REPLICATION_PATH,
                    )
                    .await
                    .map_err(|failure| {
                        evict_layout_after_rejected_insert(&table_cache, table_id, &layout, failure)
                    })
            });
        }

        tasks.wait().await?;

        Ok(())
    }
}

/// Rejects primary-key changes that cannot update ReplacingMergeTree's fixed
/// sort and deduplication key.
///
/// The destination emits `CREATE TABLE ... ENGINE = ReplacingMergeTree(...)
/// ORDER BY (<pk cols>)`, so the table's sort and dedup keys are bound to those
/// PK column names. ClickHouse `ALTER TABLE` can change column shapes but
/// cannot rewrite the ORDER BY expression, so a PK drop or rename would leave
/// the ORDER BY referring to a column that no longer exists (or has a different
/// meaning), silently breaking dedup. We error before the ALTER reaches the
/// server.
fn reject_pk_alters_under_replacing_merge_tree(
    clickhouse_table_name: &str,
    diff: &SchemaDiff,
    before_schema: &ReplicatedTableSchema,
    after_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    for change in &diff.dropped_columns {
        let column = &change.before_column_schema;
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

    for change in &diff.altered_columns {
        if change.name_changed() {
            let before_name = &change.before_column_schema().name;
            let after_name = &change.after_column_schema().name;
            let was_pk = before_schema
                .column_schemas()
                .find(|c| c.name == *before_name)
                .is_some_and(|c| c.primary_key_ordinal_position.is_some());
            if was_pk {
                return Err(etl_error!(
                    ErrorKind::SourceSchemaError,
                    "ReplacingMergeTree does not support renaming a primary-key column",
                    format!(
                        "Table '{clickhouse_table_name}': RENAME COLUMN '{before_name}' -> \
                         '{after_name}' would invalidate the ReplacingMergeTree ORDER BY / dedup \
                         key. Switch this table to `engine: merge_tree` or revert the rename on \
                         the source."
                    )
                ));
            }
        }
    }

    let before_primary_key: Vec<_> = before_schema
        .primary_key_column_schemas()
        .map(|column| (column.ordinal_position, column.primary_key_ordinal_position))
        .collect();
    let after_primary_key: Vec<_> = after_schema
        .primary_key_column_schemas()
        .map(|column| (column.ordinal_position, column.primary_key_ordinal_position))
        .collect();
    if before_primary_key != after_primary_key {
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ReplacingMergeTree does not support changing a primary key",
            format!(
                "Table '{clickhouse_table_name}' changed its primary-key columns or order, but \
                 the ReplacingMergeTree ORDER BY / dedup key cannot be altered. Switch this table \
                 to `engine: merge_tree` or restore the source primary key."
            )
        ));
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
/// Source columns must not collide with the engine's trailing ETL columns.
/// ReplacingMergeTree also requires a source primary key because it uses that
/// key for ordering and deduplication.
fn validate_clickhouse_table_shape(
    replicated_table_schema: &ReplicatedTableSchema,
    engine: ClickHouseEngine,
) -> EtlResult<()> {
    replicated_table_schema.validate_destination_column_names(CLICKHOUSE_COLUMN_NAME_MAPPING)?;
    validate_clickhouse_schema_capabilities(replicated_table_schema, engine)
}

/// Rejects destination table names that ReplacingMergeTree reserves for
/// current views.
///
/// The encoder doubles underscores, so a source table ending in `_current`
/// encodes to `<other>__current`, the current view name of the table whose
/// encoding is `<other>`. ClickHouse's `IF NOT EXISTS` keeps whichever object
/// exists first, so the collision would otherwise pass silently.
fn validate_clickhouse_table_name(
    clickhouse_table_name: &str,
    source_table_name: &TableName,
    engine: ClickHouseEngine,
) -> EtlResult<()> {
    if matches!(engine, ClickHouseEngine::ReplacingMergeTree)
        && clickhouse_table_name.ends_with(CURRENT_VIEW_SUFFIX)
    {
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ClickHouse table name collides with a current view name",
            format!(
                "Table '{source_table_name}' maps to '{clickhouse_table_name}', which \
                 ReplacingMergeTree reserves for the current view of another table; rename the \
                 source table or set `engine: merge_tree`."
            )
        ));
    }

    Ok(())
}

/// Validates ClickHouse-specific schema capabilities.
///
/// Shared planning owns destination name-equivalence validation during schema
/// changes. This check owns collisions with ETL-managed columns and engine key
/// requirements.
fn validate_clickhouse_schema_capabilities(
    replicated_table_schema: &ReplicatedTableSchema,
    engine: ClickHouseEngine,
) -> EtlResult<()> {
    let trailing_columns = trailing_cdc_columns(engine);
    if let Some(column) = replicated_table_schema.column_schemas().find(|column| {
        trailing_columns.iter().any(|(trailing_name, _)| {
            CLICKHOUSE_COLUMN_NAME_MAPPING.equivalent(&column.name, trailing_name)
        })
    }) {
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ClickHouse source column collides with an ETL column",
            format!(
                "Table '{}' column '{}' conflicts with a ClickHouse column owned by ETL.",
                replicated_table_schema.name(),
                column.name
            )
        ));
    }

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

    // Both engines derive current state per key: ReplacingMergeTree keeps the
    // highest version and the MergeTree current-state query takes the latest
    // event. A deferrable key lets one statement move a row into a key before
    // the key's previous row moves out, so the later old-key tombstone would
    // hide the moved row.
    if replicated_table_schema.inner().primary_key_deferrable {
        return Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "ClickHouse requires a non-deferrable primary key",
            format!(
                "Table '{}' has a DEFERRABLE primary key. Recreate it as NOT DEFERRABLE, or \
                 remove the table from the publication.",
                replicated_table_schema.name()
            )
        ));
    }

    Ok(())
}

/// Validates a positional full row against the replicated schema width.
///
/// Returns [`ErrorKind::InvalidState`] when the width differs. Continuing could
/// truncate primary-key comparison or misalign RowBinary encoding.
fn validate_clickhouse_full_row_width(
    replicated_table_schema: &ReplicatedTableSchema,
    row: &TableRow,
) -> EtlResult<()> {
    let column_count = replicated_table_schema.column_schemas().len();

    if row.values().len() != column_count {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "ClickHouse full row image does not match the replicated schema",
            format!(
                "Expected {} values for table '{}', got {}",
                column_count,
                replicated_table_schema.name(),
                row.values().len()
            )
        ));
    }

    Ok(())
}

/// Validates a positional primary-key row against the source primary-key width.
///
/// Returns [`ErrorKind::InvalidState`] when the width differs. Continuing could
/// compare or encode values under the wrong primary-key columns.
fn validate_clickhouse_pk_width(
    replicated_table_schema: &ReplicatedTableSchema,
    row: &TableRow,
) -> EtlResult<()> {
    let primary_key_column_count = replicated_table_schema.primary_key_column_schemas().len();

    if row.values().len() != primary_key_column_count {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "ClickHouse key image does not match the source primary key",
            format!(
                "Expected {} key values for table '{}', got {}",
                primary_key_column_count,
                replicated_table_schema.name(),
                row.values().len()
            )
        ));
    }

    Ok(())
}

/// Extracts and validates the complete new row required for an update.
fn clickhouse_full_update_row(
    replicated_table_schema: &ReplicatedTableSchema,
    source_updated_row: UpdatedTableRow,
) -> EtlResult<TableRow> {
    let UpdatedTableRow::Full(destination_updated_row) = source_updated_row else {
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

    validate_clickhouse_full_row_width(replicated_table_schema, &destination_updated_row)?;

    Ok(destination_updated_row)
}

/// Derives the destination rows needed to represent one source update.
///
/// Every update produces an updated row. A primary-key change also produces an
/// old-key tombstone so reconstructed current state no longer contains the
/// former key.
fn clickhouse_rows_for_update(
    replicated_table_schema: &ReplicatedTableSchema,
    source_updated_row: UpdatedTableRow,
    source_old_row: Option<OldTableRow>,
) -> EtlResult<ClickHouseRowsForUpdate> {
    let destination_updated_row =
        clickhouse_full_update_row(replicated_table_schema, source_updated_row)?;
    if replicated_table_schema.primary_key_column_schemas().next().is_none() {
        return Ok(ClickHouseRowsForUpdate {
            destination_old_key_tombstone: None,
            destination_updated_row,
        });
    }

    let primary_key_was_changed = match source_old_row.as_ref() {
        Some(source_old_row) => clickhouse_primary_key_was_changed(
            replicated_table_schema,
            source_old_row,
            &destination_updated_row,
        )?,
        None => {
            validate_clickhouse_update_without_old_row(replicated_table_schema)?;
            false
        }
    };

    let destination_old_key_tombstone = match (primary_key_was_changed, source_old_row) {
        (true, Some(OldTableRow::Full(source_old_row))) => Some(source_old_row),
        (true, Some(OldTableRow::Key(source_old_key_row))) => {
            Some(expand_key_row(source_old_key_row, replicated_table_schema)?)
        }
        (true, None) => {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "ClickHouse primary key change is missing old row",
                format!(
                    "Table '{}' primary key change was detected without an old row image",
                    replicated_table_schema.name()
                )
            ));
        }
        (false, _) => None,
    };

    Ok(ClickHouseRowsForUpdate { destination_old_key_tombstone, destination_updated_row })
}

/// Validates an update that omitted its old row image.
///
/// Only [`IdentityType::PrimaryKey`] is safe because PostgreSQL omits its old
/// key when that key is unchanged. `Full`, `AlternativeKey`, and `Missing`
/// identities are rejected because they cannot prove that no old-key tombstone
/// is required.
///
/// Returns [`ErrorKind::SourceReplicaIdentityError`] for each unsafe identity.
fn validate_clickhouse_update_without_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey) {
        Ok(())
    } else {
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "ClickHouse update requires old primary-key values",
            format!(
                "Table '{}' emitted an update without an old row image for replica identity {:?}. \
                 ClickHouse can only skip the generated delete when the source replica identity \
                 matches the primary key.",
                replicated_table_schema.name(),
                replicated_table_schema.identity_type()
            )
        ))
    }
}

/// IEEE float scalar compared with PostgreSQL key equality semantics.
trait PostgresFloat: PartialEq + Copy {
    /// Returns whether the value is `NaN`.
    fn is_nan(self) -> bool;
}

impl PostgresFloat for f32 {
    fn is_nan(self) -> bool {
        f32::is_nan(self)
    }
}

impl PostgresFloat for f64 {
    fn is_nan(self) -> bool {
        f64::is_nan(self)
    }
}

/// Compares two float key values using PostgreSQL equality semantics, which
/// treat `NaN` values as equal.
fn postgres_float_equal<T: PostgresFloat>(old_value: T, new_value: T) -> bool {
    old_value == new_value || (old_value.is_nan() && new_value.is_nan())
}

/// Compares two float array key values element-wise using
/// [`postgres_float_equal`] for present elements.
fn postgres_float_array_equal<T: PostgresFloat>(
    old_values: &[Option<T>],
    new_values: &[Option<T>],
) -> bool {
    old_values.len() == new_values.len()
        && old_values.iter().zip(new_values).all(|(old_value, new_value)| {
            match (old_value, new_value) {
                (Some(old_value), Some(new_value)) => postgres_float_equal(*old_value, *new_value),
                (None, None) => true,
                _ => false,
            }
        })
}

/// Compares two key values using PostgreSQL equality semantics.
///
/// - Treats floating-point `NaN` values as equal, including values inside
///   arrays.
fn postgres_key_cell_equal(old_value: &Cell, new_value: &Cell) -> bool {
    use etl::data::ArrayCell;

    if old_value == new_value {
        return true;
    }

    match (old_value, new_value) {
        (Cell::F32(old_value), Cell::F32(new_value)) => {
            postgres_float_equal(*old_value, *new_value)
        }
        (Cell::F64(old_value), Cell::F64(new_value)) => {
            postgres_float_equal(*old_value, *new_value)
        }
        (Cell::Array(ArrayCell::F32(old_values)), Cell::Array(ArrayCell::F32(new_values))) => {
            postgres_float_array_equal(old_values, new_values)
        }
        (Cell::Array(ArrayCell::F64(old_values)), Cell::Array(ArrayCell::F64(new_values))) => {
            postgres_float_array_equal(old_values, new_values)
        }
        _ => false,
    }
}

/// Returns whether an update changed the source primary key.
///
/// Returns [`ErrorKind::InvalidState`] for positional row-width mismatches and
/// [`ErrorKind::SourceReplicaIdentityError`] when a key image is not the source
/// primary key.
///
/// Key-image identity is checked before width because an alternative identity
/// can legitimately contain a different number of columns.
fn clickhouse_primary_key_was_changed(
    replicated_table_schema: &ReplicatedTableSchema,
    source_old_row: &OldTableRow,
    destination_updated_row: &TableRow,
) -> EtlResult<bool> {
    match source_old_row {
        OldTableRow::Full(source_old_row) => {
            validate_clickhouse_full_row_width(replicated_table_schema, source_old_row)?;

            Ok(replicated_table_schema
                .column_schemas()
                .zip(source_old_row.values())
                .zip(destination_updated_row.values())
                .any(|((column_schema, old_value), new_value)| {
                    column_schema.primary_key() && !postgres_key_cell_equal(old_value, new_value)
                }))
        }
        OldTableRow::Key(source_old_key_row) => {
            validate_clickhouse_key_image_identity(replicated_table_schema)?;
            validate_clickhouse_pk_width(replicated_table_schema, source_old_key_row)?;

            Ok(source_old_key_row
                .values()
                .iter()
                .zip(
                    replicated_table_schema
                        .column_schemas()
                        .zip(destination_updated_row.values())
                        .filter(|(column_schema, _)| column_schema.primary_key())
                        .map(|(_, value)| value),
                )
                .any(|(old_value, new_value)| !postgres_key_cell_equal(old_value, new_value)))
        }
    }
}

/// Returns the old row image required for a ClickHouse delete tombstone.
fn clickhouse_delete_old_row(
    replicated_table_schema: &ReplicatedTableSchema,
    source_old_row: Option<OldTableRow>,
) -> EtlResult<OldTableRow> {
    source_old_row.ok_or_else(|| {
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

/// Validates that a key-only old row contains source primary-key values.
///
/// Returns [`ErrorKind::SourceReplicaIdentityError`] for every identity other
/// than [`IdentityType::PrimaryKey`]. Alternative-key values cannot safely key
/// a ClickHouse tombstone.
fn validate_clickhouse_key_image_identity(
    replicated_table_schema: &ReplicatedTableSchema,
) -> EtlResult<()> {
    if matches!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey) {
        Ok(())
    } else {
        let identity_type = replicated_table_schema.identity_type();
        Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "ClickHouse key image does not match the source primary key",
            format!(
                "Table '{}' emitted a key image for replica identity {:?}, but ClickHouse rows \
                 are keyed by the source primary key. Configure REPLICA IDENTITY DEFAULT or \
                 REPLICA IDENTITY FULL.",
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
///
/// Key-image identity is checked before width because an alternative identity
/// can legitimately contain a different number of columns.
fn expand_key_row(
    source_old_key_row: TableRow,
    schema: &ReplicatedTableSchema,
) -> EtlResult<TableRow> {
    validate_clickhouse_key_image_identity(schema)?;
    validate_clickhouse_pk_width(schema, &source_old_key_row)?;

    let key_cells = source_old_key_row.into_values();
    let mut key_iter = key_cells.into_iter();
    let cells: Vec<Cell> = schema
        .column_schemas()
        .map(|col| {
            if col.primary_key_ordinal_position.is_some() {
                key_iter.next().unwrap_or(Cell::Null)
            } else if col.nullable && !is_array_type(&col.typ) {
                // Nullable scalars -> NULL. Array columns are never nullable in
                // ClickHouse (Array(Nullable(T)) without outer Nullable), so
                // they must use an empty array default instead.
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
/// interval, json, bytea). Date, Timestamp, and UUID use typed zero values
/// because their ClickHouse wire format is not String.
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
        Type::DATE => Cell::Date(Date::Value(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())),
        Type::TIMESTAMP => {
            Cell::Timestamp(Timestamp::Value(chrono::DateTime::UNIX_EPOCH.naive_utc()))
        }
        Type::TIMESTAMPTZ => Cell::TimestampTz(Timestamp::Value(chrono::DateTime::UNIX_EPOCH)),
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
    S: StateStore + SchemaStore + Send + Sync + 'static,
{
    fn name() -> &'static str {
        etl_config::shared::DestinationKind::ClickHouse.as_str()
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.tasks.shutdown().await
    }

    // The trait methods below use `?` only for lifecycle failures raised
    // before work is admitted (task reaping and registry draining). Errors
    // from admitted work must reach the caller via `async_result.send(result)`;
    // using `?` there would short-circuit before `send` runs and leave the
    // receiver waiting. `AsyncResult::send` itself returns `()`, and its
    // `Drop` impl synthesizes a "dropped without sending" error if a path
    // (including an aborted background task) skips `send`, so the receiver is
    // never silently abandoned.

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        // Acquire the task registry before any client work. Event tasks have
        // no registry access, so they can finish while the reset waits for
        // them; their inserts and DDL all carry client-side timeouts, so the
        // drain cannot wait unboundedly.
        let task_guard = self.tasks.drain().await?;

        let result = self.writer.drop_table_for_copy_inner(replicated_table_schema).await;

        // Publish the remote result before allowing another event task to run.
        async_result.send(result);
        drop(task_guard);

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let result =
            self.writer.write_table_rows_inner(replicated_table_schema, batch_id, table_rows).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        // Surface panics from previously admitted event tasks before
        // admitting more work.
        self.tasks.try_reap().await?;

        // Wait until every earlier batch that touches one of this batch's
        // tables has finished. Batches take their fences in dispatch order.
        // On the normal path no such batch is in flight and this returns at
        // once. See `EventBatchFences`.
        let fence_guards = self.fences.acquire(&events).await;

        // Durability needs no branch: the task completes only after every
        // INSERT in the batch is acknowledged under `async_insert = 0`,
        // so each result is already `Durable` and `RequireDurable` calls are
        // satisfied by construction. `Accepted` is never reported.
        let writer = self.writer.clone();
        self.tasks
            .spawn_with(move || async move {
                let result = writer.write_events_inner(events).await;
                // Release the fences first so the next batch for these tables
                // can start as soon as this one has finished.
                drop(fence_guards);
                async_result.send(result.map(|_| DestinationWriteStatus::Durable));
            })
            .await;

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

/// One-shot failure armed for the next table reset's remote work.
#[cfg(feature = "test-utils")]
static DROP_TABLE_FOR_COPY_FAILURE: Mutex<bool> = Mutex::new(false);

/// Arms the next [`ClickHouseDestination`] table reset to fail once before
/// any remote work, after its task-registry drain.
#[cfg(feature = "test-utils")]
pub fn arm_fail_drop_table_for_copy_once_for_tests() {
    *DROP_TABLE_FOR_COPY_FAILURE.lock() = true;
}

#[cfg(test)]
mod tests {
    use etl::{
        data::{ArrayCell, PartialTableRow},
        event::{
            BeginEvent, CommitEvent, DeleteEvent, InsertEvent, RelationEvent, TruncateEvent,
            UpdateEvent,
        },
        schema::{
            ColumnSchema, IdentityMask, PgLsn, ReplicationMask, SnapshotId, TableName, TableSchema,
        },
    };

    use super::*;
    use crate::clickhouse::{
        encoding::{ColumnEncoding, encode_to_row_binary},
        schema::{
            CDC_LSN_COLUMN_NAME, CDC_OPERATION_COLUMN_NAME, CDC_TX_ORDINAL_COLUMN_NAME,
            ETL_DELETED_COLUMN_NAME, ETL_VERSION_COLUMN_NAME, clickhouse_column_type,
        },
    };

    /// Creates a synthetic composite snapshot ID for tests.
    fn test_snapshot_id(commit_lsn: u64, message_lsn: u64) -> SnapshotId {
        SnapshotId::new(PgLsn::from(commit_lsn), PgLsn::from(message_lsn))
    }

    fn clickhouse_column(name: &str, type_name: &str) -> ClickHouseTableColumn {
        ClickHouseTableColumn { name: name.to_owned(), type_name: type_name.to_owned() }
    }

    /// Builds a minimal replicated schema for `table_id`; only the id matters.
    fn schema_for_table(table_id: u32) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(table_id),
            TableName::new("public".to_owned(), format!("table_{table_id}")),
            vec![ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1)],
        ));
        ReplicatedTableSchema::all(table_schema)
    }

    /// Every event kind that writes a table contributes that table to the
    /// fence set, including each table of a multi-table truncate, while
    /// transaction markers and unsupported events contribute nothing. The
    /// events name tables out of order; the result is ascending regardless.
    #[test]
    fn batch_table_ids_cover_every_written_table() {
        let lsn = PgLsn::from(100);
        let events = vec![
            Event::Begin(BeginEvent { commit_lsn: lsn, tx_ordinal: 0, timestamp: 0, xid: 1 }),
            Event::Insert(InsertEvent {
                commit_lsn: lsn,
                tx_ordinal: 1,
                replicated_table_schema: schema_for_table(4),
                table_row: TableRow::new(vec![Cell::I32(1)]),
            }),
            Event::Update(UpdateEvent {
                commit_lsn: lsn,
                tx_ordinal: 2,
                replicated_table_schema: schema_for_table(2),
                updated_table_row: UpdatedTableRow::Full(TableRow::new(vec![Cell::I32(1)])),
                old_table_row: None,
            }),
            Event::Delete(DeleteEvent {
                commit_lsn: lsn,
                tx_ordinal: 3,
                replicated_table_schema: schema_for_table(6),
                old_table_row: None,
            }),
            Event::Relation(RelationEvent { replicated_table_schema: schema_for_table(1) }),
            Event::Truncate(TruncateEvent {
                commit_lsn: lsn,
                tx_ordinal: 4,
                options: 0,
                truncated_tables: vec![
                    schema_for_table(5),
                    schema_for_table(3),
                    schema_for_table(4),
                ],
            }),
            Event::Commit(CommitEvent {
                commit_lsn: lsn,
                tx_ordinal: 5,
                flags: 0,
                end_lsn: lsn,
                timestamp: 0,
            }),
            Event::Unsupported,
        ];

        let table_ids: Vec<TableId> = batch_table_ids(&events).into_iter().collect();

        assert_eq!(table_ids, (1..=6).map(TableId::new).collect::<Vec<_>>());
    }

    /// Batches with no table writes hold no fences.
    #[test]
    fn batch_table_ids_of_marker_only_batch_are_empty() {
        let lsn = PgLsn::from(100);
        let events = vec![
            Event::Begin(BeginEvent { commit_lsn: lsn, tx_ordinal: 0, timestamp: 0, xid: 1 }),
            Event::Commit(CommitEvent {
                commit_lsn: lsn,
                tx_ordinal: 1,
                flags: 0,
                end_lsn: lsn,
                timestamp: 0,
            }),
        ];

        assert!(batch_table_ids(&events).is_empty());
        assert!(batch_table_ids(&[]).is_empty());
    }

    /// Builds one insert event for `schema`; only the table matters.
    fn insert_for(schema: &ReplicatedTableSchema) -> Event {
        Event::Insert(InsertEvent {
            commit_lsn: PgLsn::from(100),
            tx_ordinal: 0,
            replicated_table_schema: schema.clone(),
            table_row: TableRow::new(vec![Cell::I32(1)]),
        })
    }

    /// Two dispatches that share tables acquire their fences in the same
    /// order, so neither can hold one fence while waiting for the other's.
    ///
    /// This test is here to catch future deadlocks. The fences stay
    /// deadlock-free only because every batch locks them in the same order.
    /// The only thing enforcing that order is the `BTreeSet` in
    /// [`batch_table_ids`]. Swapping it for an unsorted collection would
    /// compile without complaint.
    ///
    /// The scenario is the classic two-lock deadlock. One batch names the
    /// tables left then right, the other right then left. Each takes its
    /// first fence and then waits for its second, which the other holds. With
    /// acquisition in event order the two would wait on each other forever.
    ///
    /// The paused clock makes that deadlock observable at once. When every
    /// task is blocked on a fence, the runtime has nothing to run and jumps
    /// straight to the timeout. On the success path the timeout never fires.
    ///
    /// We checked that this test can catch such a deadlock by introducing
    /// one on purpose (locking fences in event order instead of sorted
    /// order) and observing the test fail within a fraction of a second.
    /// Reversing the sorted order still passes, because that is still one
    /// shared order. The test cares that the order is shared, not which
    /// direction it runs.
    #[tokio::test(start_paused = true)]
    async fn fences_of_overlapping_batches_do_not_deadlock() {
        let fences = Arc::new(EventBatchFences::new());
        let left = schema_for_table(1);
        let right = schema_for_table(2);

        // Two earlier batches hold one fence each, so both dispatches below
        // have to wait. The release order below controls how they wake.
        let left_held = fences.acquire(&[insert_for(&left)]).await;
        let right_held = fences.acquire(&[insert_for(&right)]).await;

        let forward = tokio::spawn({
            let fences = Arc::clone(&fences);
            let events = vec![insert_for(&left), insert_for(&right)];
            async move { fences.acquire(&events).await }
        });
        let backward = tokio::spawn({
            let fences = Arc::clone(&fences);
            let events = vec![insert_for(&right), insert_for(&left)];
            async move { fences.acquire(&events).await }
        });
        // Let both dispatches reach their first wait before anything is
        // released.
        tokio::task::yield_now().await;

        // Release right first. With event-ordered acquisition, the backward
        // dispatch would take right and then wait for left. Releasing left
        // would then hand it to the forward dispatch, which would wait for
        // right. Neither could finish.
        drop(right_held);
        tokio::task::yield_now().await;
        drop(left_held);

        let both = async {
            forward.await.unwrap();
            backward.await.unwrap();
        };
        // The timeout only bounds the failure path; see the test doc.
        tokio::time::timeout(Duration::from_secs(1), both)
            .await
            .expect("fence acquisition deadlocked");
    }

    #[test]
    fn replication_mask_addition_is_nullable_and_omits_source_default() {
        let source_column = ColumnSchema::new("score".to_owned(), Type::INT4, -1, 1, false)
            .with_default_expression("42".to_owned());

        let (destination_column, force_nullable) = clickhouse_add_column_definition(
            &source_column,
            ColumnPresenceChangeReason::ReplicationMask,
        );

        assert!(force_nullable);
        assert_eq!(destination_column.default_expression, None);
        assert_eq!(clickhouse_column_type(&destination_column, force_nullable), "Nullable(Int32)");
    }

    /// A publication-added array has no historical values, so it is added
    /// without the source default and old rows read as empty arrays.
    #[test]
    fn replication_mask_array_addition_is_a_non_nullable_array_without_default() {
        let source_column = ColumnSchema::new("scores".to_owned(), Type::INT4_ARRAY, -1, 1, false)
            .with_default_expression("'{1}'::integer[]".to_owned());

        let (destination_column, force_nullable) = clickhouse_add_column_definition(
            &source_column,
            ColumnPresenceChangeReason::ReplicationMask,
        );

        assert_eq!(destination_column.default_expression, None);
        assert_eq!(
            clickhouse_column_type(&destination_column, force_nullable),
            "Array(Nullable(Int32))"
        );
    }

    #[test]
    fn rename_guard_rejects_moving_nested_subcolumns_between_parents() {
        let before = ColumnSchema::new("old.value".to_owned(), Type::TEXT, -1, 1, true);
        let after = ColumnSchema::new("new.value".to_owned(), Type::TEXT, -1, 1, true);
        let change = ColumnMetadataChange::between(&before, &after).unwrap();
        let plan = SchemaDiff::new(Vec::new(), Vec::new(), vec![change])
            .plan_for_column_names(
                vec!["old.value".to_owned()],
                vec!["new.value".to_owned()],
                CLICKHOUSE_COLUMN_NAME_MAPPING,
            )
            .unwrap();

        let error = ensure_clickhouse_renames_are_supported("events", &plan).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
    }

    #[test]
    fn rename_guard_accepts_nested_subcolumns_under_the_same_parent() {
        let before = ColumnSchema::new("payload.old".to_owned(), Type::TEXT, -1, 1, true);
        let after = ColumnSchema::new("payload.new".to_owned(), Type::TEXT, -1, 1, true);
        let change = ColumnMetadataChange::between(&before, &after).unwrap();
        let plan = SchemaDiff::new(Vec::new(), Vec::new(), vec![change])
            .plan_for_column_names(
                vec!["payload.old".to_owned()],
                vec!["payload.new".to_owned()],
                CLICKHOUSE_COLUMN_NAME_MAPPING,
            )
            .unwrap();

        ensure_clickhouse_renames_are_supported("events", &plan).unwrap();
    }

    #[test]
    fn clickhouse_engine_matches_accepts_cloud_shared_variants() {
        // Cloud `Shared` variants are equivalent to their plain configured
        // forms.
        assert!(clickhouse_engine_matches("SharedReplacingMergeTree", "ReplacingMergeTree"));
        assert!(clickhouse_engine_matches("SharedMergeTree", "MergeTree"));
        assert!(clickhouse_engine_matches("ReplacingMergeTree", "ReplacingMergeTree"));

        // Genuine engine mismatches still fail.
        assert!(!clickhouse_engine_matches("SharedReplacingMergeTree", "MergeTree"));
        assert!(!clickhouse_engine_matches("MergeTree", "ReplacingMergeTree"));
    }

    #[test]
    fn initial_creation_recovery_rejects_a_different_schema_target() {
        let arriving_schema = replicated_schema(IdentityType::PrimaryKey);
        let metadata = DestinationTableMetadata::new_creating(
            "public_users".to_owned(),
            test_snapshot_id(1_u64, 1_u64),
            arriving_schema.replication_mask().clone(),
        );

        let error = ensure_destination_schema_matches_metadata(
            "ClickHouse",
            arriving_schema.id(),
            &metadata,
            &arriving_schema,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DestinationSchemaRewind);
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

    /// Builds a schema with the requested primary-key type and identity.
    fn replicated_schema_with_primary_key_type(
        primary_key_type: Type,
        identity_type: IdentityType,
    ) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), primary_key_type, -1, 1, false)
                    .with_primary_key(1),
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

    /// Builds a primary-key identity schema with two key columns whose physical
    /// order differs from primary-key ordinal order.
    fn replicated_composite_primary_key_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(2),
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 2, false)
                    .with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1, 1, 0]);

        ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
    }

    /// Builds a schema with one PK column and a two-column alternative
    /// identity.
    fn replicated_schema_with_composite_alternative_identity() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 2, false),
                ColumnSchema::new("external_id".to_owned(), Type::TEXT, -1, 3, false),
            ],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![0, 1, 1]);

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

    fn replicated_schema_with_column_name(column_name: &str) -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new(column_name.to_owned(), Type::TEXT, -1, 2, true),
            ],
        ));

        ReplicatedTableSchema::all(table_schema)
    }

    #[test]
    fn clickhouse_rows_for_update_emits_old_key_tombstone_when_primary_key_changes() {
        // GIVEN: An update changes the primary key from one to two.
        let update_row = TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_owned())]);

        // WHEN: Destination rows are prepared from the old key image.
        let rows = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::PrimaryKey),
            UpdatedTableRow::Full(update_row.clone()),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1)]))),
        )
        .unwrap();

        // THEN: The old key is tombstoned and the new row is preserved.
        assert_eq!(
            rows.destination_old_key_tombstone,
            Some(TableRow::new(vec![Cell::I32(1), Cell::Null]))
        );
        assert_eq!(rows.destination_updated_row, update_row);
    }

    #[test]
    fn clickhouse_rows_for_update_projects_composite_old_key_in_schema_order() {
        // GIVEN: Composite key ordinals differ from schema column order.
        let update_row =
            TableRow::new(vec![Cell::I32(2), Cell::I32(10), Cell::String("updated".to_owned())]);

        // WHEN: An update changes one column of the composite key.
        let rows = clickhouse_rows_for_update(
            &replicated_composite_primary_key_schema(),
            UpdatedTableRow::Full(update_row.clone()),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(1), Cell::I32(10)]))),
        )
        .unwrap();

        // THEN: The tombstone uses schema order and the new row is intact.
        assert_eq!(
            rows.destination_old_key_tombstone,
            Some(TableRow::new(vec![Cell::I32(1), Cell::I32(10), Cell::Null]))
        );
        assert_eq!(rows.destination_updated_row, update_row);
    }

    #[test]
    fn clickhouse_rows_for_update_skips_tombstone_when_primary_key_is_unchanged() {
        // GIVEN: A full-identity update changes only a non-key column.
        let update_row = TableRow::new(vec![Cell::I32(1), Cell::String("updated".to_owned())]);

        // WHEN: Destination rows are prepared from the full old row.
        let rows = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::Full),
            UpdatedTableRow::Full(update_row.clone()),
            Some(OldTableRow::Full(TableRow::new(vec![
                Cell::I32(1),
                Cell::String("before".to_owned()),
            ]))),
        )
        .unwrap();

        // THEN: No tombstone is emitted and the updated row is preserved.
        assert_eq!(rows.destination_old_key_tombstone, None);
        assert_eq!(rows.destination_updated_row, update_row);
    }

    #[test]
    fn clickhouse_rows_for_update_accepts_primary_key_identity_without_old_row() {
        // GIVEN: A primary-key identity update supplies a complete new row.
        let update_row = TableRow::new(vec![Cell::I32(1), Cell::String("updated".to_owned())]);

        // WHEN: Destination rows are prepared without an old row image.
        let rows = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::PrimaryKey),
            UpdatedTableRow::Full(update_row.clone()),
            None,
        )
        .unwrap();

        // THEN: The update is accepted without a tombstone.
        assert_eq!(rows.destination_old_key_tombstone, None);
        assert_eq!(rows.destination_updated_row, update_row);
    }

    #[test]
    fn clickhouse_rows_for_update_uses_postgres_nan_equality_for_primary_keys() {
        // GIVEN: Scalar and array primary keys contain NaN values.
        let cases = [
            (Type::FLOAT4, Cell::F32(f32::NAN)),
            (Type::FLOAT8, Cell::F64(f64::NAN)),
            (Type::FLOAT4_ARRAY, Cell::Array(ArrayCell::F32(vec![Some(f32::NAN), None]))),
            (Type::FLOAT8_ARRAY, Cell::Array(ArrayCell::F64(vec![Some(f64::NAN), None]))),
        ];

        // WHEN: A non-key column changes but the NaN key is unchanged.
        for (primary_key_type, primary_key_value) in cases {
            let rows = clickhouse_rows_for_update(
                &replicated_schema_with_primary_key_type(primary_key_type, IdentityType::Full),
                UpdatedTableRow::Full(TableRow::new(vec![
                    primary_key_value.clone(),
                    Cell::String("updated".to_owned()),
                ])),
                Some(OldTableRow::Full(TableRow::new(vec![
                    primary_key_value,
                    Cell::String("before".to_owned()),
                ]))),
            )
            .unwrap();

            // THEN: PostgreSQL NaN equality prevents a spurious tombstone.
            assert!(rows.destination_old_key_tombstone.is_none());
        }

        // WHEN: A non-NaN element changes in an array primary key.
        let rows = clickhouse_rows_for_update(
            &replicated_schema_with_primary_key_type(Type::FLOAT8_ARRAY, IdentityType::Full),
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::Array(ArrayCell::F64(vec![Some(f64::NAN), Some(2.0)])),
                Cell::String("updated".to_owned()),
            ])),
            Some(OldTableRow::Full(TableRow::new(vec![
                Cell::Array(ArrayCell::F64(vec![Some(f64::NAN), Some(1.0)])),
                Cell::String("before".to_owned()),
            ]))),
        )
        .unwrap();

        // THEN: The actual key change still produces a tombstone.
        assert!(rows.destination_old_key_tombstone.is_some());
    }

    #[test]
    fn clickhouse_rows_for_update_uses_postgres_nan_equality_for_key_image() {
        // GIVEN: The old key image and new row both have a NaN primary key.

        // WHEN: Rows are prepared using primary-key replica identity.
        let rows = clickhouse_rows_for_update(
            &replicated_schema_with_primary_key_type(Type::FLOAT8, IdentityType::PrimaryKey),
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::F64(f64::NAN),
                Cell::String("updated".to_owned()),
            ])),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::F64(f64::NAN)]))),
        )
        .unwrap();

        // THEN: PostgreSQL NaN equality prevents a spurious tombstone.
        assert!(rows.destination_old_key_tombstone.is_none());
    }

    #[test]
    fn clickhouse_rows_for_update_accepts_alternative_identity_with_full_old_row() {
        // GIVEN: An alternative-identity update changes the primary key.
        let old_row = TableRow::new(vec![Cell::I32(1), Cell::String("before".to_owned())]);
        let update_row = TableRow::new(vec![Cell::I32(2), Cell::String("updated".to_owned())]);

        // WHEN: Destination rows are prepared from the full old row.
        let rows = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::AlternativeKey),
            UpdatedTableRow::Full(update_row.clone()),
            Some(OldTableRow::Full(old_row.clone())),
        )
        .unwrap();

        // THEN: The old row is tombstoned and the new row is preserved.
        assert_eq!(rows.destination_old_key_tombstone, Some(old_row));
        assert_eq!(rows.destination_updated_row, update_row);
    }

    #[test]
    fn clickhouse_rows_for_update_rejects_alternative_identity_key_image() {
        // GIVEN: An update supplies only an alternative-identity key image.

        // WHEN: Destination rows are prepared from that key image.
        let error = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::AlternativeKey),
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I32(2),
                Cell::String("updated".to_owned()),
            ])),
            Some(OldTableRow::Key(TableRow::new(vec![Cell::String("before".to_owned())]))),
        )
        .unwrap_err();

        // THEN: The unsafe replica identity is rejected.
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn clickhouse_rows_for_update_rejects_composite_alternative_identity_key_image() {
        // GIVEN: A composite alternative identity omits the primary key.

        // WHEN: Rows are prepared from the two-column key image.
        let error = clickhouse_rows_for_update(
            &replicated_schema_with_composite_alternative_identity(),
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I32(2),
                Cell::I32(10),
                Cell::String("after".to_owned()),
            ])),
            Some(OldTableRow::Key(TableRow::new(vec![
                Cell::I32(10),
                Cell::String("before".to_owned()),
            ]))),
        )
        .unwrap_err();

        // THEN: The unsafe replica identity is rejected.
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn clickhouse_rows_for_update_rejects_alternative_identity_without_old_row() {
        // GIVEN: An alternative-identity update has no old row image.

        // WHEN: Destination rows are prepared from the new row alone.
        let error = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::AlternativeKey),
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I32(2),
                Cell::String("updated".to_owned()),
            ])),
            None,
        )
        .unwrap_err();

        // THEN: The unsafe replica identity is rejected.
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn clickhouse_rows_for_update_rejects_malformed_new_row_width_without_old_row() {
        // GIVEN: A primary-key update has an undersized complete new row.

        // WHEN: Destination rows are prepared without an old row image.
        let error = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::PrimaryKey),
            UpdatedTableRow::Full(TableRow::new(vec![Cell::I32(1)])),
            None,
        )
        .unwrap_err();

        // THEN: The malformed row width is rejected as invalid state.
        assert_eq!(error.kind(), ErrorKind::InvalidState);
    }

    #[test]
    fn clickhouse_rows_for_update_rejects_malformed_full_old_row_width() {
        // GIVEN: A full-identity update has an undersized old row.

        // WHEN: Rows are prepared with a correctly sized new row.
        let error = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::Full),
            UpdatedTableRow::Full(TableRow::new(vec![
                Cell::I32(1),
                Cell::String("updated".to_owned()),
            ])),
            Some(OldTableRow::Full(TableRow::new(vec![Cell::I32(1)]))),
        )
        .unwrap_err();

        // THEN: The malformed old row width is rejected as invalid state.
        assert_eq!(error.kind(), ErrorKind::InvalidState);
    }

    #[test]
    fn clickhouse_rows_for_update_rejects_partial_rows_before_identity_checks() {
        // GIVEN: An alternative-identity update contains a partial new row.
        let partial_row = PartialTableRow::new(2, TableRow::new(vec![Cell::I32(1)]), vec![1]);

        // WHEN: Destination rows are prepared without an old row image.
        let error = clickhouse_rows_for_update(
            &replicated_schema(IdentityType::AlternativeKey),
            UpdatedTableRow::Partial(partial_row),
            None,
        )
        .unwrap_err();

        // THEN: The partial-row error precedes replica identity validation.
        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert!(error.to_string().contains("partial update row"));
    }

    /// A full update row carrying a NULL array encodes as an empty array.
    #[test]
    fn clickhouse_full_update_row_encodes_null_array_as_empty_array() {
        // GIVEN: The replicated schema contains a nullable array column.
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![ColumnSchema::new("tags".to_owned(), Type::TEXT_ARRAY, -1, 1, true)],
        ));
        let schema = ReplicatedTableSchema::all(table_schema);

        // WHEN: A complete update row containing a null array is encoded.
        let row = clickhouse_full_update_row(
            &schema,
            UpdatedTableRow::Full(TableRow::new(vec![Cell::Null])),
        )
        .unwrap();
        let values = row
            .into_values()
            .into_iter()
            .map(cell_to_clickhouse_value)
            .collect::<EtlResult<Vec<_>>>()
            .unwrap();
        let mut buf = Vec::new();
        encode_to_row_binary(values, &[ColumnEncoding::Array], &mut buf).unwrap();

        // THEN: RowBinary carries a zero-length array.
        assert_eq!(buf, [0x00]);
    }

    #[test]
    fn clickhouse_delete_old_row_rejects_missing_old_rows() {
        let err = clickhouse_delete_old_row(&replicated_schema(IdentityType::PrimaryKey), None)
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn expand_key_row_rejects_short_primary_key_payload() {
        // GIVEN: An empty key payload must identify a single-column key.

        // WHEN: The key image is expanded to a complete row.
        let err =
            expand_key_row(TableRow::new(vec![]), &replicated_schema(IdentityType::PrimaryKey))
                .unwrap_err();

        // THEN: The missing key value is rejected as invalid state.
        assert_eq!(err.kind(), ErrorKind::InvalidState);
        assert!(err.to_string().contains("Expected 1 key values"));
    }

    #[test]
    fn expand_key_row_rejects_composite_alternative_identity() {
        // GIVEN: The key image contains a composite alternative identity.

        // WHEN: The key image is expanded to a complete row.
        let err = expand_key_row(
            TableRow::new(vec![Cell::I32(10), Cell::String("before".to_owned())]),
            &replicated_schema_with_composite_alternative_identity(),
        )
        .unwrap_err();

        // THEN: The unsafe replica identity is rejected.
        assert_eq!(err.kind(), ErrorKind::SourceReplicaIdentityError);
    }

    #[test]
    fn validate_clickhouse_table_shape_allows_other_engine_metadata_columns() {
        // GIVEN: Source columns use MergeTree metadata names.

        // WHEN: The schema is validated for ReplacingMergeTree.

        // THEN: The other engine's metadata names are accepted.
        for column_name in
            [CDC_OPERATION_COLUMN_NAME, CDC_LSN_COLUMN_NAME, CDC_TX_ORDINAL_COLUMN_NAME]
        {
            validate_clickhouse_table_shape(
                &replicated_schema_with_column_name(column_name),
                ClickHouseEngine::ReplacingMergeTree,
            )
            .unwrap();
        }

        // GIVEN: Source columns use ReplacingMergeTree metadata names.

        // WHEN: The schema is validated for MergeTree.

        // THEN: The other engine's metadata names are accepted.
        for column_name in [ETL_VERSION_COLUMN_NAME, ETL_DELETED_COLUMN_NAME] {
            validate_clickhouse_table_shape(
                &replicated_schema_with_column_name(column_name),
                ClickHouseEngine::MergeTree,
            )
            .unwrap();
        }
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
    fn validate_clickhouse_table_shape_accepts_nullable_arrays() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![ColumnSchema::new("tags".to_owned(), Type::TEXT_ARRAY, -1, 1, true)],
        ));
        let replicated_table_schema = ReplicatedTableSchema::all(table_schema);

        validate_clickhouse_table_shape(&replicated_table_schema, ClickHouseEngine::MergeTree)
            .unwrap();
    }

    #[test]
    fn validate_clickhouse_table_shape_rejects_pkless_schema_under_replacing_merge_tree() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(2),
            TableName::new("public".to_owned(), "events".to_owned()),
            vec![ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 1, true)],
        ));
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1]);
        let schema =
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);

        let err = validate_clickhouse_table_shape(&schema, ClickHouseEngine::ReplacingMergeTree)
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
    }

    #[test]
    fn validate_clickhouse_table_shape_rejects_deferrable_primary_key() {
        // GIVEN: A keyed table whose primary key is DEFERRABLE. PostgreSQL
        // cannot use a deferrable key as the replica identity, so the table
        // publishes updates and deletes with FULL identity.
        let table_schema = Arc::new(
            TableSchema::new(
                TableId::new(3),
                TableName::new("public".to_owned(), "positions".to_owned()),
                vec![
                    ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false)
                        .with_primary_key(1),
                    ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                ],
            )
            .with_primary_key_deferrable(true),
        );
        let replication_mask = ReplicationMask::all(&table_schema);
        let identity_mask = IdentityMask::from_bytes(vec![1, 1]);
        let schema =
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask);

        for engine in [ClickHouseEngine::MergeTree, ClickHouseEngine::ReplacingMergeTree] {
            // WHEN: Either engine validates the table.
            let err = validate_clickhouse_table_shape(&schema, engine).unwrap_err();

            // THEN: The deferrable-key check rejects it.
            assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
            assert_eq!(err.description(), Some("ClickHouse requires a non-deferrable primary key"));
        }
    }

    #[test]
    fn validate_clickhouse_table_name_rejects_current_view_suffix_under_replacing_merge_tree() {
        // GIVEN: an encoded name that equals another table's current view.
        let source_name = TableName::new("public".to_owned(), "foo_current".to_owned());
        let clickhouse_table_name = "public_foo__current";

        // WHEN: validated for both engines.
        let error = validate_clickhouse_table_name(
            clickhouse_table_name,
            &source_name,
            ClickHouseEngine::ReplacingMergeTree,
        )
        .unwrap_err();
        let merge_tree = validate_clickhouse_table_name(
            clickhouse_table_name,
            &source_name,
            ClickHouseEngine::MergeTree,
        );

        // THEN: only the engine with current views rejects it.
        assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        merge_tree.unwrap();
    }

    #[test]
    fn validate_clickhouse_table_shape_rejects_engine_owned_column_names() {
        let cases = [
            (ClickHouseEngine::MergeTree, CDC_OPERATION_COLUMN_NAME),
            (ClickHouseEngine::MergeTree, CDC_LSN_COLUMN_NAME),
            (ClickHouseEngine::MergeTree, CDC_TX_ORDINAL_COLUMN_NAME),
            (ClickHouseEngine::ReplacingMergeTree, ETL_VERSION_COLUMN_NAME),
            (ClickHouseEngine::ReplacingMergeTree, ETL_DELETED_COLUMN_NAME),
        ];

        for (engine, column_name) in cases {
            let error = validate_clickhouse_table_shape(
                &replicated_schema_with_column_name(column_name),
                engine,
            )
            .unwrap_err();

            assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
            assert_eq!(
                error.description(),
                Some("ClickHouse source column collides with an ETL column")
            );
        }

        validate_clickhouse_table_shape(
            &replicated_schema_with_column_name("CDC_OPERATION"),
            ClickHouseEngine::MergeTree,
        )
        .unwrap();
    }

    #[test]
    fn ensure_engine_supported_rejects_replacing_merge_tree_on_old_server() {
        let err =
            ensure_engine_supported(ClickHouseEngine::ReplacingMergeTree, (23, 4)).unwrap_err();
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

    /// Schema with composite PK `(tenant_id, id)` plus a non-PK `value` column.
    /// Used by the PK-ALTER-guard tests.
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
    ) -> etl::schema::ColumnMetadataChange {
        let before_column_schema =
            ColumnSchema::new(old_name.to_owned(), Type::TEXT, -1, ordinal_position, true);
        let after_column_schema =
            ColumnSchema::new(new_name.to_owned(), Type::TEXT, -1, ordinal_position, true);

        etl::schema::ColumnMetadataChange::between(&before_column_schema, &after_column_schema)
            .unwrap()
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_allows_non_pk_drop() {
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(
            Vec::new(),
            vec![ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 3, true)],
            Vec::new(),
        );
        reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_rejects_pk_drop() {
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(
            Vec::new(),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
            ],
            Vec::new(),
        );
        let err = reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
            &schema,
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
        // The error should identify the primary-key column that blocks the
        // operation.
        assert!(err.to_string().contains("tenant_id"));
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_allows_non_pk_rename() {
        let schema = replicated_schema_for_pk_alters();
        let diff =
            SchemaDiff::new(Vec::new(), Vec::new(), vec![rename_change("value", "payload", 3)]);
        reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
            &schema,
        )
        .unwrap();
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_rejects_pk_rename() {
        let schema = replicated_schema_for_pk_alters();
        let diff = SchemaDiff::new(Vec::new(), Vec::new(), vec![rename_change("id", "row_id", 2)]);
        let err = reject_pk_alters_under_replacing_merge_tree(
            "public_replacing_merge_tree__alter",
            &diff,
            &schema,
            &schema,
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::SourceSchemaError);
        // The error should identify both endpoints of the blocked rename.
        assert!(err.to_string().contains("'id'") && err.to_string().contains("'row_id'"));
    }

    #[test]
    fn reject_pk_alters_under_replacing_merge_tree_rejects_pk_membership_and_order_changes() {
        let current_schema = replicated_schema_for_pk_alters();
        for (tenant_key_position, id_key_position, value_key_position) in
            [(Some(2), Some(1), None), (Some(1), Some(2), Some(3)), (Some(1), None, None)]
        {
            let new_table_schema = Arc::new(TableSchema::new(
                TableId::new(7),
                TableName::new("public".to_owned(), "replacing_merge_tree_alter".to_owned()),
                vec![
                    ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                        .with_primary_key_ordinal_position(tenant_key_position),
                    ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, false)
                        .with_primary_key_ordinal_position(id_key_position),
                    ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 3, true)
                        .with_primary_key_ordinal_position(value_key_position),
                ],
            ));
            let identity_mask = IdentityMask::from_bytes(vec![
                u8::from(tenant_key_position.is_some()),
                u8::from(id_key_position.is_some()),
                u8::from(value_key_position.is_some()),
            ]);
            let new_schema = ReplicatedTableSchema::from_masks(
                Arc::clone(&new_table_schema),
                ReplicationMask::all(&new_table_schema),
                identity_mask,
            );
            let diff = current_schema.diff(&new_schema);

            let error = reject_pk_alters_under_replacing_merge_tree(
                "public_replacing_merge_tree__alter",
                &diff,
                &current_schema,
                &new_schema,
            )
            .unwrap_err();

            assert_eq!(error.kind(), ErrorKind::SourceSchemaError);
        }
    }

    #[test]
    fn clickhouse_schema_recovery_accepts_only_unambiguous_endpoints() {
        let table_id = TableId::new(7);
        let previous = vec!["a".to_owned(), "b".to_owned()];
        let target = vec!["b".to_owned(), "c".to_owned()];

        assert_eq!(
            classify_clickhouse_schema_recovery_endpoint(
                table_id, &previous, &previous, &target, false,
            )
            .unwrap(),
            ClickHouseSchemaRecoveryEndpoint::Previous
        );
        assert_eq!(
            classify_clickhouse_schema_recovery_endpoint(
                table_id, &target, &previous, &target, false,
            )
            .unwrap(),
            ClickHouseSchemaRecoveryEndpoint::Target
        );

        for partial in [
            vec!["a".to_owned(), "c".to_owned()],
            vec!["supabase_etl_ddl_tmp_column_1_0".to_owned(), "b".to_owned()],
        ] {
            let error = classify_clickhouse_schema_recovery_endpoint(
                table_id, &partial, &previous, &target, false,
            )
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidState);
        }

        let error =
            classify_clickhouse_schema_recovery_endpoint(table_id, &target, &target, &target, true)
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidState);
    }

    #[test]
    fn clickhouse_add_column_uses_final_replicated_position() {
        let current = vec!["a".to_owned(), "c".to_owned()];
        let final_column_index_by_name = HashMap::from([
            ("a".to_owned(), 0_usize),
            ("b".to_owned(), 1_usize),
            ("c".to_owned(), 2_usize),
        ]);

        let insertion_index =
            clickhouse_add_column_insertion_index(&current, &final_column_index_by_name, "b")
                .unwrap();

        assert_eq!(insertion_index, 1);
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
    fn row_binary_layout_accepts_destination_nullability() {
        // GIVEN: The source columns are NOT NULL, but ClickHouse made `score`
        // nullable when the column was added.
        let expected_columns = vec![
            clickhouse_column("id", "Int64"),
            clickhouse_column("score", "Int32"),
            clickhouse_column("tags", "Array(Nullable(String))"),
            clickhouse_column(CDC_OPERATION_COLUMN_NAME, "String"),
        ];
        let actual_columns = vec![
            clickhouse_column("id", "Int64"),
            clickhouse_column("score", "Nullable(Int32)"),
            clickhouse_column("tags", "Array(Nullable(String))"),
            clickhouse_column(CDC_OPERATION_COLUMN_NAME, "String"),
        ];

        // WHEN: The layout is built from the actual table.
        let layout = row_binary_layout_from_clickhouse_columns(
            "test_table",
            &expected_columns,
            &actual_columns,
        )
        .unwrap();

        // THEN: Only the Nullable wrapper adds a null marker.
        assert_eq!(
            layout.column_encodings(),
            [
                ColumnEncoding::Required,
                ColumnEncoding::Nullable,
                ColumnEncoding::Array,
                ColumnEncoding::Required,
            ]
        );
    }

    /// Any column drift other than an outer `Nullable(...)` rejects the table.
    #[test]
    fn row_binary_layout_rejects_clickhouse_schema_drift() {
        // GIVEN: an expected layout and physical tables that differ from it.
        let expected_columns =
            vec![clickhouse_column("id", "Int64"), clickhouse_column("name", "String")];
        let cases = [
            // Missing column.
            vec![clickhouse_column("id", "Int64")],
            // Reordered columns.
            vec![clickhouse_column("name", "String"), clickhouse_column("id", "Int64")],
            // Source type change that ClickHouse never applied.
            vec![clickhouse_column("id", "Int32"), clickhouse_column("name", "String")],
            // A wrapper other than Nullable, which is a different type.
            vec![
                clickhouse_column("id", "Int64"),
                clickhouse_column("name", "LowCardinality(String)"),
            ],
        ];

        for actual_columns in cases {
            // WHEN: the layout is built from the drifted table.
            let error = row_binary_layout_from_clickhouse_columns(
                "test_table",
                &expected_columns,
                &actual_columns,
            )
            .unwrap_err();

            // THEN: the table is reported as corrupted.
            assert_eq!(error.kind(), ErrorKind::CorruptedTableSchema, "{actual_columns:?}");
        }
    }
}
