#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use etl::{
    data::{OldTableRow, PartialTableRow, TableRow, UpdatedTableRow},
    destination::{
        Destination, DestinationTableMetadata, DestinationTableSchemaStatus,
        DestinationWriteStatus, DropTableForCopyResult, TaskSet, WriteEventsDurability,
        WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::{Event, EventSequenceKey},
    schema::{
        ColumnModification, ColumnSchema, ReplicatedTableSchema, ReplicationMask, SchemaDiff,
        SnapshotId, TableId, TableName, TableSchema,
    },
    store::{DestinationStore, TableStateType},
};
use etl_config::ducklake_catalog_metadata_connect_options;
use metrics::gauge;
use parking_lot::Mutex;
use pg_escape::{quote_identifier as quote_postgres_identifier, quote_literal};
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
#[cfg(feature = "test-utils")]
use tokio::sync::oneshot;
use tokio::{
    sync::{
        OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock, Semaphore,
        TryAcquireError,
    },
    task::JoinSet,
};
use tracing::{debug, info, warn};
use url::Url;

use crate::ducklake::{
    ATTACH_DATA_INLINING_ROW_LIMIT, COPY_DATA_INLINING_ROW_LIMIT, DuckLakeTableName, LAKE_CATALOG,
    S3Config,
    batches::{
        TableMutation, TrackedTableMutation, TrackedTruncateEvent, apply_table_batch_with_retry,
        apply_table_batches_with_retry, ensure_applied_batches_table_exists,
        ensure_streaming_progress_table_exists, prepare_copy_table_batch,
        prepare_mutation_table_batches, prepare_truncate_table_batch,
        read_table_streaming_progress_sequence_key, retain_mutations_after_sequence_key,
        retain_truncates_after_sequence_key,
    },
    client::{
        DuckLakeConnectionManager, DuckLakeInterruptRegistry, build_warm_ducklake_pool,
        format_query_error_detail, run_duckdb_blocking,
    },
    config::{
        MAINTENANCE_TARGET_FILE_SIZE, MIN_EXPIRE_SNAPSHOTS_OLDER_THAN, build_setup_plan,
        current_duckdb_extension_strategy, maintenance_target_file_size_sql,
        resolve_expire_snapshots_older_than, validate_expire_snapshots_older_than_sql,
    },
    external_maintenance::ExternalMaintenanceOperations,
    inline_size::DuckLakePendingInlineSizeSampler,
    metrics::{
        DuckLakeMetricsSampler, ETL_DUCKLAKE_POOL_SIZE, query_catalog_maintenance_metrics,
        query_table_storage_metrics, register_metrics, resolve_ducklake_metadata_schema_blocking,
        spawn_ducklake_metrics_sampler,
    },
    replay_epoch::{
        ensure_replay_epoch_table_exists, read_table_replay_epoch, rotate_table_replay_epoch,
    },
    schema::{
        build_add_column_sql_ducklake, build_create_table_sql_ducklake,
        build_drop_column_sql_ducklake, build_drop_default_sql_ducklake,
        build_rename_column_sql_ducklake, build_set_default_sql_ducklake,
        supports_column_default_ducklake,
    },
    sql::qualified_lake_table_name,
};

/// Shared Postgres metadata pool size for DuckLake background samplers.
///
/// One connection is enough because inline-size sampling and metrics sampling
/// are both best-effort background reads and can safely serialize.
const DUCKLAKE_METADATA_PG_POOL_SIZE: u32 = 1;

/// Prefix for ETL-owned tombstone columns that keep same-name replacement DDL
/// replay-safe.
pub(super) const DUCKLAKE_DROPPED_COLUMN_PREFIX: &str = "__etl_ducklake_dropped_";

/// Builds the shared Postgres metadata pool used by background samplers.
fn build_ducklake_metadata_pg_pool(catalog_url: &Url) -> EtlResult<PgPool> {
    let options = ducklake_catalog_metadata_connect_options(catalog_url).map_err(|source| {
        etl_error!(
            ErrorKind::ConfigError,
            "DuckLake metadata pool configuration failed",
            source: source
        )
    })?;

    Ok(PgPoolOptions::new()
        .max_connections(DUCKLAKE_METADATA_PG_POOL_SIZE)
        .min_connections(0)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(Some(std::time::Duration::from_secs(30)))
        .connect_lazy_with(options))
}

/// Returns whether a DuckLake DDL error indicates another transaction already
/// created the requested table.
pub(super) fn is_create_table_conflict(error: &duckdb::Error, table_name: &str) -> bool {
    let message = error.to_string();
    message.contains("has been created by another transaction already")
        && message.contains(table_name)
}

/// Parses `expire_snapshots_older_than` into seconds for cheap metadata-only
/// trigger sampling.
fn expire_snapshots_retention_seconds(value: &str) -> Option<i64> {
    humantime::parse_duration(value)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
}

/// Returns whether file maintenance should run for a table.
///
/// Initial COPY deliberately creates Parquet files in each batch. Deferring
/// rewrites and compaction until every copy completes keeps maintenance from
/// competing with the writer and avoids repeatedly compacting a growing table.
fn should_request_file_maintenance(
    copy_phase_active: bool,
    active_data_files: i64,
    rewrite_data_files_min_active_data_files: i64,
) -> bool {
    !copy_phase_active && active_data_files > rewrite_data_files_min_active_data_files
}

// ── destination
// ───────────────────────────────────────────────────────────────

/// A DuckLake destination that implements the ETL [`Destination`] trait.
///
/// Writes data to a DuckLake data lake. DuckDB connections are pre-initialized,
/// pooled, and bounded by a semaphore so operations can reuse attached lake
/// catalogs without oversubscribing Tokio's blocking threads. Data is persisted
/// as Parquet files at `data_path`; metadata is tracked in a PostgreSQL catalog
/// database.
///
/// All writes are wrapped in explicit transactions so that each batch of rows
/// is committed atomically in DuckLake while file materialization can be
/// deferred to coordinated maintenance.
#[derive(Clone)]
pub struct DuckLakeDestination<S> {
    manager: Arc<DuckLakeConnectionManager>,
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    /// Shared gate that keeps external maintenance pauses from overlapping
    /// active foreground or table-scoped mutations.
    checkpoint_gate: Arc<RwLock<()>>,
    tasks: TaskSet,
    metrics_sampler: Arc<Option<DuckLakeMetricsSampler>>,
    metadata_schema: Arc<str>,
    expire_snapshots_older_than: Arc<str>,
    metadata_pg_pool: PgPool,
    table_creation_slots: Arc<Semaphore>,
    table_write_slots: Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    /// Cache of table-level inlining limits installed by this process.
    table_data_inlining_limits: Arc<Mutex<HashMap<DuckLakeTableName, u64>>>,
    store: S,
    /// Cache of table names whose DDL has already been executed.
    created_tables: Arc<Mutex<HashSet<DuckLakeTableName>>>,
    /// Cache tracking whether the ETL batch marker table already exists. If
    /// it's set then the table has already been created
    applied_batches_table_created: Arc<AtomicBool>,
    /// Cache tracking whether the ETL streaming progress table already exists.
    streaming_progress_table_created: Arc<AtomicBool>,
}

/// Held by an external DuckLake maintenance coordinator while foreground
/// mutations must be quiesced.
pub struct DuckLakeExternalMaintenancePause {
    _guard: OwnedRwLockWriteGuard<()>,
}

/// Maintenance operations sampled from DuckLake catalog state.
pub(super) struct ExternalMaintenanceOperationSample {
    /// Operations the replicator should request.
    pub operations: ExternalMaintenanceOperations,
    /// Whether any table is currently in initial copy.
    pub copy_phase_active: bool,
}

/// Runtime backend used for DuckLake external maintenance coordination.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DuckLakeMaintenanceMode {
    #[default]
    Disabled,
    Kubernetes,
    Postgres,
}

/// Runtime configuration for DuckLake external maintenance coordination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DuckLakeExternalMaintenanceConfig {
    pub mode: DuckLakeMaintenanceMode,
    pub pipeline_id: u64,
}

impl DuckLakeExternalMaintenanceConfig {
    pub const fn disabled() -> Self {
        Self { mode: DuckLakeMaintenanceMode::Disabled, pipeline_id: 0 }
    }

    pub const fn kubernetes(pipeline_id: u64) -> Self {
        Self { mode: DuckLakeMaintenanceMode::Kubernetes, pipeline_id }
    }

    pub const fn postgres(pipeline_id: u64) -> Self {
        Self { mode: DuckLakeMaintenanceMode::Postgres, pipeline_id }
    }
}

impl Default for DuckLakeExternalMaintenanceConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Returns the table-local semaphore shared by concurrent foreground writes.
fn table_write_slot(
    table_write_slots: &Arc<Mutex<HashMap<DuckLakeTableName, Arc<Semaphore>>>>,
    table_name: &DuckLakeTableName,
) -> Arc<Semaphore> {
    let mut slots = table_write_slots.lock();
    let slot = slots.entry(table_name.clone()).or_insert_with(|| Arc::new(Semaphore::new(1)));
    Arc::clone(slot)
}

/// Builds a table-scoped DuckLake inlining option statement.
fn table_data_inlining_row_limit_sql(table_name: &DuckLakeTableName, row_limit: u64) -> String {
    format!(
        "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {row_limit}, schema => {}, \
         table_name => {});",
        quote_literal(table_name.schema()),
        quote_literal(table_name.table()),
    )
}

/// Waits for process shutdown signals and interrupts active DuckDB calls.
#[cfg(unix)]
async fn interrupt_duckdb_connections_on_process_shutdown(manager: Arc<DuckLakeConnectionManager>) {
    let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
        warn!("ducklake failed to register sigterm interrupt handler");
        return;
    };
    let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
        warn!("ducklake failed to register sigint interrupt handler");
        return;
    };

    let signal_name = tokio::select! {
        _ = sigterm.recv() => "sigterm",
        _ = sigint.recv() => "sigint",
    };

    let interrupted_connections = manager.interrupt_all_connections_for_process_shutdown();
    info!(
        interrupted_connections,
        signal = signal_name,
        "ducklake process shutdown signal received, interrupted active duckdb connections"
    );
}

/// Waits for process shutdown signals and interrupts active DuckDB calls.
#[cfg(not(unix))]
async fn interrupt_duckdb_connections_on_process_shutdown(manager: Arc<DuckLakeConnectionManager>) {
    if tokio::signal::ctrl_c().await.is_err() {
        warn!("ducklake failed to register ctrl-c interrupt handler");
        return;
    }

    let interrupted_connections = manager.interrupt_all_connections_for_process_shutdown();
    info!(
        interrupted_connections,
        signal = "ctrl_c",
        "ducklake process shutdown signal received, interrupted active duckdb connections"
    );
}

impl<S> Destination for DuckLakeDestination<S>
where
    S: DestinationStore,
{
    fn name() -> &'static str {
        etl_config::shared::DestinationKind::Ducklake.as_str()
    }

    async fn shutdown(&self) -> EtlResult<()> {
        let interrupted_connections = self.manager.interrupt_all_connections_for_shutdown();
        info!(
            interrupted_connections,
            "ducklake shutdown requested, interrupted active duckdb connections"
        );
        self.tasks.shutdown().await?;
        self.shutdown_metrics_sampler().await?;

        Ok(())
    }

    async fn startup(&self) -> EtlResult<()> {
        self.reconcile_existing_tables_after_restart().await
    }

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
        let result = self.write_table_rows(replicated_table_schema, table_rows).await;
        async_result.send(result.map(|_| DestinationWriteStatus::Durable));

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let destination = self.clone();
        self.tasks
            .spawn(async move {
                let result = destination.write_events(events).await;
                async_result.send(result.map(|_| DestinationWriteStatus::Durable));
            })
            .await;

        Ok(())
    }
}

/// Validates that a replicated table schema can be applied to one DuckLake
/// row-matching mutation.
///
/// DuckLake can stream inserts without replica identity, but update and delete
/// paths need replicated replica-identity columns so the destination can match
/// existing rows safely.
fn validate_ducklake_replica_identity(
    replicated_table_schema: &ReplicatedTableSchema,
    operation: &'static str,
) -> EtlResult<()> {
    if replicated_table_schema.identity_column_schemas().len() == 0 {
        let description = match operation {
            "update" => "DuckLake update requires a replica identity",
            "delete" => "DuckLake delete requires a replica identity",
            _ => "DuckLake mutation requires a replica identity",
        };
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            description,
            format!(
                "Table '{}' has no replicated replica-identity columns",
                replicated_table_schema.name()
            )
        ));
    }

    Ok(())
}

/// Builds the query used to inspect a DuckLake table shape.
fn ducklake_table_columns_sql(table_name: &DuckLakeTableName) -> String {
    format!(
        "select column_name from information_schema.columns where table_catalog = {} and \
         table_schema = {} and table_name = {} order by ordinal_position",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name.schema()),
        quote_literal(table_name.table())
    )
}

/// Reads DuckLake table column names using blocking DuckDB APIs.
///
/// Call only from a [`run_duckdb_blocking`] closure.
fn read_ducklake_table_column_names_blocking(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
) -> EtlResult<Vec<String>> {
    let sql = ducklake_table_columns_sql(table_name);
    let mut statement = conn.prepare(&sql).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake table schema lookup failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;
    let mut rows = statement.query([]).map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake table schema lookup failed",
            format_query_error_detail(&sql),
            source: source
        )
    })?;
    let mut column_names = Vec::new();
    while let Some(row) = rows.next().map_err(|source| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake table schema lookup failed",
            format_query_error_detail(&sql),
            source: source
        )
    })? {
        column_names.push(row.get(0).map_err(|source| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake table schema lookup failed",
                format_query_error_detail(&sql),
                source: source
            )
        })?);
    }

    Ok(column_names)
}

/// Finds a destination column by name.
fn find_ducklake_column(column_names: &[String], column_name: &str) -> Option<usize> {
    column_names.iter().position(|name| name == column_name)
}

/// One planned DuckLake DDL statement.
#[derive(Debug, PartialEq, Eq)]
struct DuckLakeSchemaDdlStatement {
    /// SQL to execute.
    sql: String,
    /// Error description to attach if execution fails.
    error_description: &'static str,
}

/// Planned DuckLake schema DDL and the expected resulting column names.
#[derive(Debug, PartialEq, Eq)]
struct DuckLakeSchemaDiffPlan {
    /// Statements to execute in order.
    statements: Vec<DuckLakeSchemaDdlStatement>,
    /// Destination column names after applying the plan.
    column_names: Vec<String>,
}

/// Ordered CDC mutations that all use the same replicated table schema.
struct TableMutationSegment {
    /// Replicated schema used to encode every mutation in this segment.
    replicated_table_schema: ReplicatedTableSchema,
    /// Ordered mutations for the schema.
    mutations: Vec<TrackedTableMutation>,
}

/// Returns whether two replicated schemas have the same row shape and identity.
fn replicated_table_schemas_match(
    left: &ReplicatedTableSchema,
    right: &ReplicatedTableSchema,
) -> bool {
    left.id() == right.id()
        && left.inner().snapshot_id == right.inner().snapshot_id
        && left.replication_mask() == right.replication_mask()
        && left.identity_mask() == right.identity_mask()
}

/// Appends a mutation to the latest compatible schema segment.
fn push_table_mutation_segment(
    segments: &mut Vec<TableMutationSegment>,
    replicated_table_schema: ReplicatedTableSchema,
    mutation: TrackedTableMutation,
) {
    if let Some(segment) = segments.last_mut()
        && replicated_table_schemas_match(
            &segment.replicated_table_schema,
            &replicated_table_schema,
        )
    {
        segment.mutations.push(mutation);
        return;
    }

    segments.push(TableMutationSegment { replicated_table_schema, mutations: vec![mutation] });
}

/// Returns a deterministic hash for generated DuckLake identifiers.
fn stable_ducklake_identifier_hash(value: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Builds the tombstone name used when a dropped column name is reused.
fn dropped_column_tombstone_name_ducklake(column: &ColumnSchema) -> String {
    format!(
        "{DUCKLAKE_DROPPED_COLUMN_PREFIX}{}_{:016x}",
        column.ordinal_position,
        stable_ducklake_identifier_hash(&column.name)
    )
}

/// Returns ETL tombstone columns that are not active replicated columns.
fn tombstone_columns_to_cleanup_ducklake(
    column_names: &[String],
    target_schema: &ReplicatedTableSchema,
) -> Vec<String> {
    let active_column_names: HashSet<_> =
        target_schema.column_schemas().map(|column| column.name.as_str()).collect();

    column_names
        .iter()
        .filter(|column_name| {
            column_name.starts_with(DUCKLAKE_DROPPED_COLUMN_PREFIX)
                && !active_column_names.contains(column_name.as_str())
        })
        .cloned()
        .collect()
}

/// Plans idempotent DuckLake schema DDL for the current destination columns.
fn plan_schema_diff_sql_ducklake(
    table_name: &DuckLakeTableName,
    mut column_names: Vec<String>,
    diff: &SchemaDiff,
) -> EtlResult<DuckLakeSchemaDiffPlan> {
    let added_column_names: HashSet<_> =
        diff.columns_to_add.iter().map(|column| column.name.as_str()).collect();
    let rename_target_names: HashSet<_> = diff
        .columns_to_change
        .iter()
        .flat_map(|change| &change.modifications)
        .filter_map(|modification| match modification {
            ColumnModification::Rename { new_name, .. } => Some(new_name.as_str()),
            _ => None,
        })
        .collect();
    let reused_removed_column_names: HashSet<_> = diff
        .columns_to_remove
        .iter()
        .filter(|column| {
            added_column_names.contains(column.name.as_str())
                || rename_target_names.contains(column.name.as_str())
        })
        .map(|column| column.name.as_str())
        .collect();
    let mut statements = Vec::new();

    for column in &diff.columns_to_remove {
        if reused_removed_column_names.contains(column.name.as_str()) {
            let tombstone_name = dropped_column_tombstone_name_ducklake(column);
            let old_index = find_ducklake_column(&column_names, &column.name);
            let tombstone_index = find_ducklake_column(&column_names, &tombstone_name);

            match (old_index, tombstone_index) {
                (Some(index), None) => {
                    statements.push(DuckLakeSchemaDdlStatement {
                        sql: build_rename_column_sql_ducklake(
                            table_name,
                            &column.name,
                            &tombstone_name,
                        ),
                        error_description: "DuckLake alter table rename dropped column failed",
                    });
                    column_names[index] = tombstone_name;
                }
                (Some(_), Some(_)) => {
                    debug!(
                        table = %table_name,
                        column = %column.name,
                        tombstone_column = %tombstone_name,
                        "ducklake drop column skipped because reused column name was already \
                         tombstoned"
                    );
                }
                (None, Some(_)) => {
                    debug!(
                        table = %table_name,
                        column = %column.name,
                        tombstone_column = %tombstone_name,
                        "ducklake drop column skipped because destination column is already \
                         tombstoned"
                    );
                }
                (None, None) => {
                    debug!(
                        table = %table_name,
                        column = %column.name,
                        "ducklake drop column skipped because destination column is already absent"
                    );
                }
            }

            continue;
        }

        let Some(index) = find_ducklake_column(&column_names, &column.name) else {
            debug!(
                table = %table_name,
                column = %column.name,
                "ducklake drop column skipped because destination column is already absent"
            );
            continue;
        };

        statements.push(DuckLakeSchemaDdlStatement {
            sql: build_drop_column_sql_ducklake(table_name, &column.name),
            error_description: "DuckLake alter table drop column failed",
        });
        column_names.remove(index);
    }

    for change in &diff.columns_to_change {
        for modification in &change.modifications {
            let ColumnModification::Rename { old_name, new_name } = modification else {
                continue;
            };

            let old_index = find_ducklake_column(&column_names, old_name);
            let new_index = find_ducklake_column(&column_names, new_name);

            match (old_index, new_index) {
                (Some(index), None) => {
                    statements.push(DuckLakeSchemaDdlStatement {
                        sql: build_rename_column_sql_ducklake(table_name, old_name, new_name),
                        error_description: "DuckLake alter table rename column failed",
                    });
                    column_names[index] = new_name.clone();
                }
                (None, Some(_)) => {
                    debug!(
                        table = %table_name,
                        old_column = %old_name,
                        new_column = %new_name,
                        "ducklake rename column skipped because destination column already has new \
                         name"
                    );
                }
                (None, None) => {
                    return Err(etl_error!(
                        ErrorKind::CorruptedTableSchema,
                        "DuckLake destination column for rename is missing",
                        format!(
                            "Table '{table_name}' has neither old column '{old_name}' nor new \
                             column '{new_name}'"
                        )
                    ));
                }
                (Some(_), Some(_))
                    if added_column_names.contains(old_name.as_str())
                        || rename_target_names.contains(old_name.as_str()) =>
                {
                    debug!(
                        table = %table_name,
                        old_column = %old_name,
                        new_column = %new_name,
                        "ducklake rename column skipped because destination has both names after \
                         replay"
                    );
                }
                (Some(index), Some(_)) => {
                    debug!(
                        table = %table_name,
                        old_column = %old_name,
                        new_column = %new_name,
                        "ducklake dropping stale rename source column because destination already \
                         has target name"
                    );
                    statements.push(DuckLakeSchemaDdlStatement {
                        sql: build_drop_column_sql_ducklake(table_name, old_name),
                        error_description: "DuckLake alter table drop stale rename source column \
                                            failed",
                    });
                    column_names.remove(index);
                }
            }
        }
    }

    for change in &diff.columns_to_change {
        if find_ducklake_column(&column_names, &change.new_column.name).is_none() {
            debug!(
                table = %table_name,
                column = %change.new_column.name,
                "ducklake column update skipped because destination column is absent"
            );
            continue;
        }

        for modification in &change.modifications {
            match modification {
                ColumnModification::Rename { .. } => {}
                ColumnModification::Nullability { old_nullable, new_nullable } => {
                    warn!(
                        table_name = %table_name,
                        column_name = %change.new_column.name,
                        old_nullable,
                        new_nullable,
                        "skipping source column nullability change for DuckLake"
                    );
                }
                ColumnModification::Default { old_expression, new_expression } => {
                    let old_default_was_supported =
                        old_expression.as_deref().is_some_and(|default_expression| {
                            supports_column_default_ducklake(
                                default_expression,
                                &change.old_column.typ,
                            )
                        });

                    if let Some(new_default_expression) = new_expression.as_deref() {
                        let Some(sql) = build_set_default_sql_ducklake(
                            table_name,
                            &change.new_column.name,
                            &change.new_column.typ,
                            new_default_expression,
                        ) else {
                            warn!(
                                table_name = %table_name,
                                column_name = %change.new_column.name,
                                "skipping unsupported source column default for DuckLake"
                            );
                            if old_default_was_supported {
                                statements.push(DuckLakeSchemaDdlStatement {
                                    sql: build_drop_default_sql_ducklake(
                                        table_name,
                                        &change.new_column.name,
                                    ),
                                    error_description: "DuckLake alter table drop default failed",
                                });
                            }
                            continue;
                        };
                        statements.push(DuckLakeSchemaDdlStatement {
                            sql,
                            error_description: "DuckLake alter table set default failed",
                        });
                    } else if old_default_was_supported {
                        statements.push(DuckLakeSchemaDdlStatement {
                            sql: build_drop_default_sql_ducklake(
                                table_name,
                                &change.new_column.name,
                            ),
                            error_description: "DuckLake alter table drop default failed",
                        });
                    } else if old_expression.is_some() {
                        warn!(
                            table_name = %table_name,
                            column_name = %change.new_column.name,
                            "skipping source column default removal for DuckLake because no \
                             supported destination default was set"
                        );
                    }
                }
            }
        }
    }

    for column in &diff.columns_to_add {
        if find_ducklake_column(&column_names, &column.name).is_some() {
            debug!(
                table = %table_name,
                column = %column.name,
                "ducklake add column skipped because destination column already exists"
            );

            if let Some(default_expression) = column.default_expression.as_deref() {
                let Some(sql) = build_set_default_sql_ducklake(
                    table_name,
                    &column.name,
                    &column.typ,
                    default_expression,
                ) else {
                    warn!(
                        table_name = %table_name,
                        column_name = %column.name,
                        "skipping unsupported source column default for DuckLake"
                    );
                    continue;
                };
                statements.push(DuckLakeSchemaDdlStatement {
                    sql,
                    error_description: "DuckLake alter table set default failed",
                });
            }

            continue;
        }

        statements.push(DuckLakeSchemaDdlStatement {
            sql: build_add_column_sql_ducklake(table_name, column),
            error_description: "DuckLake alter table add column failed",
        });
        column_names.push(column.name.clone());
    }

    Ok(DuckLakeSchemaDiffPlan { statements, column_names })
}

/// Builds the best previous-schema replication mask available during recovery.
///
/// [`DestinationTableMetadata`] stores the target mask, not the previous mask.
/// For a source-schema change we project target bits back by ordinal position.
/// Columns that no longer exist in the target schema are treated as previously
/// replicated so the idempotent DDL planner can drop them if they exist.
fn previous_replication_mask_for_recovery(
    previous_schema: &TableSchema,
    target_schema: &TableSchema,
    target_replication_mask: &ReplicationMask,
) -> ReplicationMask {
    let mask = previous_schema
        .column_schemas
        .iter()
        .map(|previous_column| {
            target_schema
                .column_schemas
                .iter()
                .position(|target_column| {
                    target_column.ordinal_position == previous_column.ordinal_position
                })
                .and_then(|index| target_replication_mask.as_slice().get(index).copied())
                .unwrap_or(1)
        })
        .collect();

    ReplicationMask::from_bytes(mask)
}

/// Returns target replicated columns that are missing from DuckLake.
fn missing_replicated_columns_ducklake(
    ducklake_columns: &[String],
    target_schema: &ReplicatedTableSchema,
) -> Vec<ColumnSchema> {
    target_schema
        .column_schemas()
        .filter(|column| find_ducklake_column(ducklake_columns, &column.name).is_none())
        .cloned()
        .collect()
}

impl<S> DuckLakeDestination<S>
where
    S: DestinationStore,
{
    /// Builds a key-only row from a partial update row when PostgreSQL omits
    /// the old key image because the replica identity did not change.
    fn key_row_from_updated_partial_row(
        replicated_table_schema: &ReplicatedTableSchema,
        partial_row: &PartialTableRow,
    ) -> EtlResult<TableRow> {
        let column_count = replicated_table_schema.column_schemas().len();
        if partial_row.total_columns() != column_count {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row does not match schema",
                format!(
                    "Expected {} replicated columns for table '{}', got {}",
                    column_count,
                    replicated_table_schema.name(),
                    partial_row.total_columns()
                )
            ));
        }

        if partial_row.values().len() + partial_row.missing_column_indexes().len()
            != partial_row.total_columns()
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row shape is inconsistent",
                format!(
                    "Table '{}' partial row reports {} total columns but has {} present and {} \
                     missing",
                    replicated_table_schema.name(),
                    partial_row.total_columns(),
                    partial_row.values().len(),
                    partial_row.missing_column_indexes().len()
                )
            ));
        }

        validate_ducklake_replica_identity(replicated_table_schema, "update")?;

        let mut missing_indexes = partial_row.missing_column_indexes().iter().copied().peekable();
        let mut present_values = partial_row.values().iter();
        let identity_column_count = replicated_table_schema.identity_column_schemas().len();
        let mut identity_columns = replicated_table_schema.identity_column_schemas().peekable();
        let mut key_values = Vec::with_capacity(identity_column_count);

        for (column_index, column_schema) in replicated_table_schema.column_schemas().enumerate() {
            let is_identity = identity_columns.peek().is_some_and(|identity_column| {
                identity_column.ordinal_position == column_schema.ordinal_position
            });

            if missing_indexes.peek().copied() == Some(column_index) {
                missing_indexes.next();
                if is_identity {
                    return Err(etl_error!(
                        ErrorKind::SourceReplicaIdentityError,
                        "DuckLake partial update is missing replica-identity columns",
                        format!(
                            "Table '{}' emitted a partial update without key column '{}'",
                            replicated_table_schema.name(),
                            column_schema.name
                        )
                    ));
                }
                continue;
            }

            let Some(value) = present_values.next() else {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake partial update row ended early",
                    format!(
                        "Table '{}' did not provide enough values for its partial update row",
                        replicated_table_schema.name()
                    )
                ));
            };

            if is_identity {
                identity_columns.next();
                key_values.push(value.clone());
            }
        }

        if missing_indexes.next().is_some() || present_values.next().is_some() {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial update row shape is inconsistent",
                format!(
                    "Table '{}' partial row has leftover values or missing indexes after decoding",
                    replicated_table_schema.name()
                )
            ));
        }

        Ok(TableRow::new(key_values))
    }

    /// Deletes all rows from the destination table.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the truncate work inline.
    pub async fn truncate_table(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        self.truncate_table_inner(replicated_table_schema).await
    }

    /// Writes an initial-copy batch directly to the destination table.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the write inline.
    pub async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_inner(replicated_table_schema, table_rows).await
    }

    /// Writes one streaming CDC batch directly to the destination.
    ///
    /// This convenience wrapper preserves the pre-async-result direct-call API
    /// by awaiting the batch inline.
    pub async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        #[cfg(feature = "test-utils")]
        wait_if_streaming_write_paused_for_tests().await;

        self.write_events_inner(events).await
    }

    /// Creates a new DuckLake destination.
    ///
    /// - `catalog_url`: DuckLake catalog location. Use a PostgreSQL URL
    ///   (`postgres://user:pass@localhost:5432/mydb`).
    /// - `data_path`: Where Parquet files are stored. Use a local file URL (`file:///tmp/lake_data`)
    ///   or a cloud URL (`s3://bucket/prefix/`, `gs://bucket/prefix/`).
    /// - `pool_size`: Number of warm DuckDB connections maintained in the pool.
    ///   `4` is a reasonable default; higher values allow more tables to be
    ///   written in parallel.
    /// - `s3`: Optional S3 credentials. Required when `data_path` is an S3 URI
    ///   and the bucket is not publicly accessible.
    /// - `metadata_schema`: Optional Postgres schema for DuckLake metadata
    ///   tables (e.g. `"ducklake"`). Uses the catalog default schema when not
    ///   set.
    /// - `maintenance_target_file_size`: Optional DuckLake maintenance
    ///   `target_file_size` value (e.g. `"500MB"`). Defaults to `500MB`.
    /// - `expire_snapshots_older_than`: Optional DuckLake snapshot-retention
    ///   interval (e.g. `"7 days"`). Defaults to `7 days`.
    /// - `duckdb_log`: Optional DuckDB log storage and shutdown dump paths.
    /// - On Linux and macOS, DuckDB extensions are loaded from vendored local
    ///   files when a vendored directory is available. The root directory can
    ///   be forced with `ETL_DUCKDB_EXTENSION_ROOT`. Otherwise, DuckDB uses the
    ///   legacy online `INSTALL` flow. On Windows, DuckDB always uses the
    ///   legacy online `INSTALL` flow.
    ///
    /// Pool initialization is blocking because DuckDB extensions are loaded and
    /// the lake catalog is attached synchronously. This constructor offloads
    /// that warm-up work to Tokio's blocking pool.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        catalog_url: Url,
        data_path: Url,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        maintenance_target_file_size: Option<String>,
        expire_snapshots_older_than: Option<String>,
        store: S,
    ) -> EtlResult<Self> {
        Self::new_with_external_maintenance(
            catalog_url,
            data_path,
            pool_size,
            s3,
            metadata_schema,
            maintenance_target_file_size,
            expire_snapshots_older_than,
            DuckLakeExternalMaintenanceConfig::default(),
            store,
        )
        .await
    }

    /// Creates a new DuckLake destination with explicit external maintenance
    /// runtime configuration.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_external_maintenance(
        catalog_url: Url,
        data_path: Url,
        pool_size: u32,
        s3: Option<S3Config>,
        metadata_schema: Option<String>,
        maintenance_target_file_size: Option<String>,
        expire_snapshots_older_than: Option<String>,
        external_maintenance: DuckLakeExternalMaintenanceConfig,
        store: S,
    ) -> EtlResult<Self> {
        register_metrics();

        if !matches!(catalog_url.scheme(), "postgres" | "postgresql") {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "DuckLake destination requires a PostgreSQL catalog URL",
                format!("unsupported catalog URL scheme `{}`", catalog_url.scheme())
            ));
        }

        if pool_size == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "DuckLake pool size must be greater than zero",
                "Pool size must be at least 1"
            ));
        }

        let extension_strategy = current_duckdb_extension_strategy()?;
        let disable_extension_autoload = extension_strategy.disables_autoload();
        let maintenance_target_file_size = Arc::<str>::from(
            maintenance_target_file_size.unwrap_or_else(|| MAINTENANCE_TARGET_FILE_SIZE.to_owned()),
        );
        let expire_snapshots_older_than = Arc::<str>::from(
            resolve_expire_snapshots_older_than(expire_snapshots_older_than.as_deref()).to_owned(),
        );
        if let crate::ducklake::config::DuckDbExtensionStrategy::VendoredLocal { platform_dir } =
            extension_strategy
        {
            info!(platform = platform_dir, "using vendored duckdb extensions");
        }
        let setup_plan = Arc::new(build_setup_plan(
            &catalog_url,
            &data_path,
            s3.as_ref(),
            metadata_schema.as_deref(),
        )?);

        let manager = Arc::new(DuckLakeConnectionManager {
            setup_plan: Arc::clone(&setup_plan),
            disable_extension_autoload,
            interrupt_registry: Arc::new(DuckLakeInterruptRegistry::default()),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        });

        let pool =
            Arc::new(build_warm_ducklake_pool(manager.as_ref().clone(), pool_size, "write").await?);
        let blocking_slots = Arc::new(Semaphore::new(pool_size as usize));

        // `target_file_size` is a catalog-wide DuckLake option consumed during
        // compaction. Apply it once on the write pool so foreground writes and
        // external maintenance jobs use the same configured catalog option.
        let target_file_size_sql =
            maintenance_target_file_size_sql(Some(maintenance_target_file_size.as_ref()));
        run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            move |conn| -> EtlResult<()> {
                conn.execute_batch(&target_file_size_sql).map_err(|error| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake target_file_size configuration failed",
                        source: error
                    )
                })?;
                Ok(())
            },
        )
        .await?;
        let expire_snapshots_validation_sql =
            validate_expire_snapshots_older_than_sql(expire_snapshots_older_than.as_ref());
        let expire_snapshots_older_than_for_error = Arc::clone(&expire_snapshots_older_than);
        run_duckdb_blocking(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            move |conn| -> EtlResult<()> {
                let retention_is_safe: bool = conn
                    .query_row(&expire_snapshots_validation_sql, [], |row| row.get(0))
                    .map_err(|source| {
                        etl_error!(
                            ErrorKind::ConfigError,
                            "DuckLake expire_snapshots_older_than configuration failed",
                            format!(
                                "Invalid expire_snapshots_older_than value `{}`",
                                expire_snapshots_older_than_for_error
                            ),
                            source: source
                        )
                    })?;
                if !retention_is_safe {
                    return Err(etl_error!(
                        ErrorKind::ConfigError,
                        "DuckLake expire_snapshots_older_than configuration failed",
                        format!(
                            "Snapshot retention must be at least {}, got `{}`",
                            MIN_EXPIRE_SNAPSHOTS_OLDER_THAN, expire_snapshots_older_than_for_error
                        )
                    ));
                }
                Ok(())
            },
        )
        .await?;
        let metadata_schema = match metadata_schema {
            Some(metadata_schema) => metadata_schema,
            None => {
                run_duckdb_blocking(
                    Arc::clone(&pool),
                    Arc::clone(&blocking_slots),
                    resolve_ducklake_metadata_schema_blocking,
                )
                .await?
            }
        };
        let metadata_schema = Arc::<str>::from(metadata_schema);
        let metadata_pg_pool = build_ducklake_metadata_pg_pool(&catalog_url)?;
        ensure_replay_epoch_table_exists(&metadata_pg_pool, metadata_schema.as_ref()).await?;
        let created_tables = Arc::default();
        let checkpoint_gate = Arc::new(RwLock::new(()));
        let mut destination = Self {
            manager: Arc::clone(&manager),
            pool: Arc::clone(&pool),
            blocking_slots: Arc::clone(&blocking_slots),
            checkpoint_gate: Arc::clone(&checkpoint_gate),
            tasks: TaskSet::new(),
            metrics_sampler: Arc::new(None),
            metadata_schema: Arc::clone(&metadata_schema),
            expire_snapshots_older_than: Arc::clone(&expire_snapshots_older_than),
            metadata_pg_pool: metadata_pg_pool.clone(),
            table_creation_slots: Arc::new(Semaphore::new(1)),
            table_write_slots: Arc::default(),
            table_data_inlining_limits: Arc::default(),
            store,
            created_tables: Arc::clone(&created_tables),
            applied_batches_table_created: Arc::default(),
            streaming_progress_table_created: Arc::default(),
        };
        gauge!(ETL_DUCKLAKE_POOL_SIZE).set(pool_size as f64);
        let shutdown_signal_manager = Arc::clone(&manager);
        destination
            .tasks
            .spawn(async move {
                interrupt_duckdb_connections_on_process_shutdown(shutdown_signal_manager).await;
            })
            .await;
        destination.ensure_applied_batches_table_exists().await?;
        destination.ensure_streaming_progress_table_exists().await?;
        destination.metrics_sampler = Arc::new(
            spawn_ducklake_metrics_sampler(
                metadata_schema.to_string(),
                metadata_pg_pool.clone(),
                Arc::clone(&created_tables),
            )?
            .into(),
        );
        match external_maintenance.mode {
            DuckLakeMaintenanceMode::Disabled => {
                info!("ducklake external maintenance watcher disabled by configuration");
            }
            DuckLakeMaintenanceMode::Kubernetes => {
                use crate::ducklake::external_maintenance::run_kubernetes_external_maintenance_watcher;

                let watcher_destination = destination.clone();
                destination
                    .tasks
                    .spawn(async move {
                        if let Err(error) =
                            run_kubernetes_external_maintenance_watcher(watcher_destination).await
                        {
                            warn!(
                                error = %error,
                                "ducklake external maintenance watcher exited"
                            );
                        }
                    })
                    .await;
            }
            DuckLakeMaintenanceMode::Postgres => {
                use crate::ducklake::external_maintenance::run_postgres_external_maintenance_watcher;

                let watcher_destination = destination.clone();
                let maintenance_pool = metadata_pg_pool.clone();
                let pipeline_id = external_maintenance.pipeline_id as i64;
                destination
                    .tasks
                    .spawn(async move {
                        if let Err(error) = run_postgres_external_maintenance_watcher(
                            watcher_destination,
                            pipeline_id,
                            maintenance_pool,
                        )
                        .await
                        {
                            warn!(
                                error = %error,
                                "ducklake external maintenance watcher exited"
                            );
                        }
                    })
                    .await;
            }
        }

        Ok(destination)
    }

    /// Truncates the destination table while keeping its schema and name.
    async fn truncate_table_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(replicated_table_schema).await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        self.ensure_streaming_data_inlining_limit(&table_name).await?;
        self.ensure_applied_batches_table_exists().await?;
        self.ensure_streaming_progress_table_exists().await?;
        let replay_epoch = self.rotate_table_replay_epoch(&table_name).await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        self.run_duckdb_blocking(move |conn| -> EtlResult<()> {
            conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake BEGIN TRANSACTION failed",
                    source: e
                )
            })?;

            let result = (|| -> EtlResult<()> {
                let target_table = qualified_lake_table_name(&table_name);
                let truncate_table_sql = format!("TRUNCATE TABLE {target_table};");
                conn.execute_batch(&truncate_table_sql).map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake TRUNCATE TABLE failed",
                        format_query_error_detail(&truncate_table_sql),
                        source: e
                    )
                })?;
                debug!(
                    table = %table_name,
                    replay_epoch,
                    "ducklake table replay epoch rotated after truncate"
                );
                Ok(())
            })();

            match result {
                Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake COMMIT failed",
                        source: e
                    )
                }),
                Err(error) => {
                    let err = conn.execute_batch("ROLLBACK");
                    if let Err(err) = err {
                        tracing::error!(error = %err, "error rollback");
                    }
                    Err(error)
                }
            }
        })
        .await
    }

    /// Drops the destination table and rotates replay state before restarting a
    /// copy.
    async fn drop_table_for_copy_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_name = self.resolve_destination_table_name(replicated_table_schema).await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        self.ensure_applied_batches_table_exists().await?;
        self.ensure_streaming_progress_table_exists().await?;
        let replay_epoch = self.rotate_table_replay_epoch(&table_name).await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        let table_name_for_drop = table_name.clone();

        self.run_duckdb_blocking(move |conn| -> EtlResult<()> {
            conn.execute_batch("BEGIN TRANSACTION").map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake BEGIN TRANSACTION failed",
                    source: e
                )
            })?;

            let result = (|| -> EtlResult<()> {
                let table_name = qualified_lake_table_name(&table_name_for_drop);
                let drop_table_sql = format!("DROP TABLE IF EXISTS {table_name};");
                conn.execute_batch(&drop_table_sql).map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake DROP TABLE failed",
                        format_query_error_detail(&drop_table_sql),
                        source: e
                    )
                })?;
                debug!(
                    table = %table_name_for_drop,
                    replay_epoch,
                    "ducklake table replay epoch rotated after drop-for-copy"
                );
                Ok(())
            })();

            match result {
                Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake COMMIT failed",
                        source: e
                    )
                }),
                Err(error) => {
                    let err = conn.execute_batch("ROLLBACK");
                    if let Err(err) = err {
                        tracing::error!(error = %err, "error rollback");
                    }
                    Err(error)
                }
            }
        })
        .await?;

        self.created_tables.lock().remove(&table_name);
        self.table_data_inlining_limits.lock().remove(&table_name);

        Ok(())
    }

    /// Bulk-inserts rows into the destination table inside a single
    /// transaction.
    ///
    /// Wrapping all inserts in one `BEGIN` / `COMMIT` ensures they are written
    /// as one atomic DuckLake change rather than one file per row.
    ///
    /// Copy batches are recorded in the replay marker table so a retry after an
    /// ambiguous post-commit failure can detect already applied rows.
    ///
    /// Initial-copy rows are written directly to Parquet files. This avoids
    /// accumulating large snapshot loads in the catalog when source batches
    /// are smaller than the regular streaming inline threshold.
    async fn write_table_rows_inner(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        let table_name = self.ensure_table_exists(replicated_table_schema).await?;

        if table_rows.is_empty() {
            return Ok(());
        }

        // Copy batches for the same table must still serialize so concurrent
        // callers do not race each other inside DuckDB.
        self.ensure_applied_batches_table_exists().await?;
        let _table_write_permit = self.acquire_table_write_slot(&table_name).await?;
        self.ensure_copy_data_inlining_limit(&table_name).await?;
        let replay_epoch = self.read_table_replay_epoch(&table_name).await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        let prepared_batch = prepare_copy_table_batch(
            replicated_table_schema,
            table_name,
            replay_epoch,
            table_rows,
        )?;
        apply_table_batch_with_retry(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            prepared_batch,
        )
        .await?;

        Ok(())
    }

    /// Handles a schema-change relation event by applying the destination DDL
    /// diff and advancing destination table metadata.
    async fn handle_relation_event(
        &self,
        new_replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_id = new_replicated_table_schema.id();
        let new_snapshot_id = new_replicated_table_schema.inner().snapshot_id;
        let new_replication_mask = new_replicated_table_schema.replication_mask().clone();

        let metadata =
            self.store.get_destination_table_metadata(table_id).await?.ok_or_else(|| {
                etl_error!(
                    ErrorKind::CorruptedTableSchema,
                    "Destination metadata missing for DuckLake schema change",
                    format!(
                        "Table {} received schema snapshot {}, but destination metadata from \
                         initial synchronization was not found.",
                        table_id, new_snapshot_id
                    )
                )
            })?;
        let table_name = DuckLakeTableName::from_metadata_id(&metadata.destination_table_id)?;
        let metadata = if metadata.is_applying() {
            self.recover_applying_metadata(
                table_id,
                &table_name,
                metadata,
                Some(new_replicated_table_schema),
            )
            .await?
        } else {
            metadata
        };

        let current_snapshot_id = metadata.snapshot_id;
        let current_replication_mask = metadata.replication_mask.clone();
        if current_snapshot_id == new_snapshot_id
            && current_replication_mask == new_replication_mask
        {
            self.reconcile_missing_replicated_columns(&table_name, new_replicated_table_schema)
                .await?;
            self.cleanup_tombstone_columns_after_applied(&table_name, new_replicated_table_schema)
                .await;
            info!(
                table_id = %table_id,
                snapshot_id = %new_snapshot_id,
                replication_mask = %new_replication_mask,
                "ducklake schema unchanged"
            );
            return Ok(());
        }

        info!(
            table_id = %table_id,
            current_snapshot_id = %current_snapshot_id,
            new_snapshot_id = %new_snapshot_id,
            current_replication_mask = %current_replication_mask,
            new_replication_mask = %new_replication_mask,
            "ducklake schema change detected"
        );

        let current_table_schema = self
            .load_exact_table_schema(
                table_id,
                current_snapshot_id,
                "Stored schema snapshot missing for DuckLake schema change",
            )
            .await?;
        let current_schema = ReplicatedTableSchema::from_mask(
            current_table_schema,
            current_replication_mask.clone(),
        );
        self.cleanup_tombstone_columns_after_applied(&table_name, &current_schema).await;

        let updated_metadata = DestinationTableMetadata::new_applied(
            table_name.to_metadata_id()?,
            current_snapshot_id,
            current_replication_mask,
        )
        .with_schema_change(
            new_snapshot_id,
            new_replication_mask,
            DestinationTableSchemaStatus::Applying,
        );
        self.store.store_destination_table_metadata(table_id, updated_metadata.clone()).await?;

        let diff = current_schema.diff(new_replicated_table_schema);
        if let Err(error) = self.apply_schema_diff(&table_name, &diff).await {
            warn!(
                error = %error,
                table_id = %table_id,
                table = %table_name,
                "ducklake schema change failed"
            );
            return Err(error);
        }
        self.reconcile_missing_replicated_columns(&table_name, new_replicated_table_schema).await?;

        let applied_metadata = updated_metadata.to_applied();
        self.store.store_destination_table_metadata(table_id, applied_metadata).await?;
        self.cleanup_tombstone_columns_after_applied(&table_name, new_replicated_table_schema)
            .await;
        self.created_tables.lock().insert(table_name.clone());

        info!(
            table_id = %table_id,
            table = %table_name,
            snapshot_id = %new_snapshot_id,
            "ducklake schema change completed"
        );

        Ok(())
    }

    /// Applies a schema diff while serializing with table-local writes and
    /// external maintenance.
    ///
    /// A table that was copied with data inlining disabled must restore its
    /// streaming setting before DuckLake applies a later schema change. This
    /// lets DuckLake update its inlined-data representation with the DDL.
    async fn apply_schema_diff(
        &self,
        table_name: &DuckLakeTableName,
        diff: &SchemaDiff,
    ) -> EtlResult<()> {
        if diff.is_empty() {
            debug!(table = %table_name, "ducklake schema diff is empty");
            return Ok(());
        }

        info!(
            table = %table_name,
            additions = diff.columns_to_add.len(),
            removals = diff.columns_to_remove.len(),
            changes = diff.columns_to_change.len(),
            "ducklake applying schema diff"
        );

        let _table_write_permit = self.acquire_table_write_slot(table_name).await?;
        self.ensure_streaming_data_inlining_limit(table_name).await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        let table_name = table_name.clone();
        let diff = diff.clone();

        run_duckdb_blocking(Arc::clone(&self.pool), Arc::clone(&self.blocking_slots), move |conn| {
            let execute_ddl = |sql: &str, description: &'static str| -> EtlResult<()> {
                conn.execute_batch(sql).map_err(|source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        description,
                        format_query_error_detail(sql),
                        source: source
                    )
                })
            };

            execute_ddl("begin transaction", "DuckLake DDL transaction failed")?;

            let apply_result = (|| -> EtlResult<()> {
                let column_names = read_ducklake_table_column_names_blocking(conn, &table_name)?;
                let DuckLakeSchemaDiffPlan { statements, column_names: _column_names } =
                    plan_schema_diff_sql_ducklake(&table_name, column_names, &diff)?;

                for statement in statements {
                    execute_ddl(&statement.sql, statement.error_description)?;
                }

                Ok(())
            })();

            if let Err(error) = apply_result {
                if let Err(rollback_error) = conn.execute_batch("rollback") {
                    warn!(
                        error = %rollback_error,
                        table = %table_name,
                        "ducklake schema change rollback failed"
                    );
                }
                return Err(error);
            }

            execute_ddl("commit", "DuckLake DDL transaction commit failed")
        })
        .await
    }

    /// Adds target replicated columns missing from the physical DuckLake table.
    async fn reconcile_missing_replicated_columns(
        &self,
        table_name: &DuckLakeTableName,
        target_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let table_name_for_read = table_name.clone();
        let ducklake_columns = run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            move |conn| read_ducklake_table_column_names_blocking(conn, &table_name_for_read),
        )
        .await?;
        let missing_columns = missing_replicated_columns_ducklake(&ducklake_columns, target_schema);

        if missing_columns.is_empty() {
            return Ok(());
        }

        warn!(
            table = %table_name,
            missing_column_count = missing_columns.len(),
            "ducklake destination table is missing replicated columns, reconciling"
        );

        let diff = SchemaDiff {
            columns_to_add: missing_columns,
            columns_to_remove: Vec::new(),
            columns_to_change: Vec::new(),
        };

        self.apply_schema_diff(table_name, &diff).await
    }

    /// Drops ETL tombstone columns after schema metadata is durably `Applied`.
    async fn cleanup_tombstone_columns(
        &self,
        table_name: &DuckLakeTableName,
        target_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let _table_write_permit = self.acquire_table_write_slot(table_name).await?;
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        let table_name = table_name.clone();
        let target_schema = target_schema.clone();

        run_duckdb_blocking(Arc::clone(&self.pool), Arc::clone(&self.blocking_slots), move |conn| {
            let execute_ddl = |sql: &str, description: &'static str| -> EtlResult<()> {
                conn.execute_batch(sql).map_err(|source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        description,
                        format_query_error_detail(sql),
                        source: source
                    )
                })
            };

            let column_names = read_ducklake_table_column_names_blocking(conn, &table_name)?;
            let columns_to_drop =
                tombstone_columns_to_cleanup_ducklake(&column_names, &target_schema);
            if columns_to_drop.is_empty() {
                return Ok(());
            }

            info!(
                table = %table_name,
                tombstone_column_count = columns_to_drop.len(),
                "ducklake cleaning up tombstone columns"
            );

            execute_ddl("begin transaction", "DuckLake tombstone cleanup transaction failed")?;

            let cleanup_result = (|| -> EtlResult<()> {
                for column_name in columns_to_drop {
                    let sql = build_drop_column_sql_ducklake(&table_name, &column_name);
                    execute_ddl(&sql, "DuckLake tombstone column drop failed")?;
                }

                Ok(())
            })();

            if let Err(error) = cleanup_result {
                if let Err(rollback_error) = conn.execute_batch("rollback") {
                    warn!(
                        error = %rollback_error,
                        table = %table_name,
                        "ducklake tombstone cleanup rollback failed"
                    );
                }
                return Err(error);
            }

            execute_ddl("commit", "DuckLake tombstone cleanup transaction commit failed")
        })
        .await
    }

    /// Best-effort wrapper for post-`Applied` tombstone cleanup.
    async fn cleanup_tombstone_columns_after_applied(
        &self,
        table_name: &DuckLakeTableName,
        target_schema: &ReplicatedTableSchema,
    ) {
        if let Err(error) = self.cleanup_tombstone_columns(table_name, target_schema).await {
            warn!(
                error = %error,
                table = %table_name,
                "ducklake tombstone column cleanup failed"
            );
        }
    }

    /// Reconciles persisted destination metadata with physical DuckLake tables.
    async fn reconcile_existing_tables_after_restart(&self) -> EtlResult<()> {
        let table_schemas = self.store.get_table_schemas().await?;
        let table_ids: HashSet<_> = table_schemas.iter().map(|schema| schema.id).collect();

        if table_ids.is_empty() {
            return Ok(());
        }

        info!(
            table_count = table_ids.len(),
            "ducklake reconciling destination table schemas after restart"
        );

        for table_id in table_ids {
            let Some(metadata) = self.store.get_destination_table_metadata(table_id).await? else {
                continue;
            };

            let table_name = DuckLakeTableName::from_metadata_id(&metadata.destination_table_id)?;
            if metadata.is_applying() {
                self.recover_applying_metadata(table_id, &table_name, metadata, None).await?;
                continue;
            }

            let target_table_schema = self
                .load_exact_table_schema(
                    table_id,
                    metadata.snapshot_id,
                    "DuckLake startup schema not found",
                )
                .await?;
            let target_schema =
                ReplicatedTableSchema::from_mask(target_table_schema, metadata.replication_mask);

            self.issue_create_table_stmt(&table_name, &target_schema).await?;
            self.reconcile_missing_replicated_columns(&table_name, &target_schema).await?;
            self.cleanup_tombstone_columns_after_applied(&table_name, &target_schema).await;
            self.created_tables.lock().insert(table_name);
        }

        Ok(())
    }

    /// Writes streaming CDC events to the destination.
    ///
    /// Insert, Update, and Delete events are grouped by table and written in
    /// parallel, each table in its own async task. Each DuckDB attempt acquires
    /// one blocking slot before entering `spawn_blocking`. Each table's ordered
    /// CDC stream is split into atomic sub-batches, applied on a reused DuckDB
    /// connection per retry attempt, and acknowledged through one per-table
    /// streaming replay watermark so retries can safely detect already
    /// committed work.
    async fn write_events_inner(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            let mut table_id_to_mutations: HashMap<TableId, Vec<TableMutationSegment>> =
                HashMap::new();

            // Accumulate row events, stopping at the first DDL or truncate boundary.
            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Relation(_) | Event::Truncate(_)) {
                    break;
                }

                let Some(event) = event_iter.next() else {
                    break;
                };
                match event {
                    Event::Insert(insert) => {
                        let table_id = insert.replicated_table_schema.id();
                        let mutation = TrackedTableMutation::new(
                            insert.start_lsn,
                            insert.commit_lsn,
                            insert.tx_ordinal,
                            TableMutation::Insert(insert.table_row),
                        );
                        push_table_mutation_segment(
                            table_id_to_mutations.entry(table_id).or_default(),
                            insert.replicated_table_schema,
                            mutation,
                        );
                    }
                    Event::Update(update) => {
                        validate_ducklake_replica_identity(
                            &update.replicated_table_schema,
                            "update",
                        )?;
                        let table_id = update.replicated_table_schema.id();
                        let replicated_table_schema = update.replicated_table_schema;
                        let table_row = update.updated_table_row;
                        let old_table_row = update.old_table_row;
                        let segments = table_id_to_mutations.entry(table_id).or_default();
                        if let Some(old_row) = old_table_row {
                            let mutation = TrackedTableMutation::new(
                                update.start_lsn,
                                update.commit_lsn,
                                update.tx_ordinal,
                                TableMutation::Update { delete_row: old_row, new_row: table_row },
                            );
                            push_table_mutation_segment(
                                segments,
                                replicated_table_schema,
                                mutation,
                            );
                        } else {
                            match table_row {
                                UpdatedTableRow::Full(table_row) => {
                                    debug!(
                                        "update event has no old row, deleting by replica \
                                         identity from new row"
                                    );
                                    let mutation = TrackedTableMutation::new(
                                        update.start_lsn,
                                        update.commit_lsn,
                                        update.tx_ordinal,
                                        TableMutation::Replace(table_row),
                                    );
                                    push_table_mutation_segment(
                                        segments,
                                        replicated_table_schema,
                                        mutation,
                                    );
                                }
                                UpdatedTableRow::Partial(partial_row) => {
                                    let key_row = Self::key_row_from_updated_partial_row(
                                        &replicated_table_schema,
                                        &partial_row,
                                    )?;
                                    debug!(
                                        "update event has no old row, building key image from \
                                         partial new row"
                                    );
                                    let mutation = TrackedTableMutation::new(
                                        update.start_lsn,
                                        update.commit_lsn,
                                        update.tx_ordinal,
                                        TableMutation::Update {
                                            delete_row: OldTableRow::Key(key_row),
                                            new_row: UpdatedTableRow::Partial(partial_row),
                                        },
                                    );
                                    push_table_mutation_segment(
                                        segments,
                                        replicated_table_schema,
                                        mutation,
                                    );
                                }
                            }
                        }
                    }
                    Event::Delete(delete) => {
                        validate_ducklake_replica_identity(
                            &delete.replicated_table_schema,
                            "delete",
                        )?;
                        let Some(old_row) = delete.old_table_row else {
                            return Err(etl_error!(
                                ErrorKind::SourceReplicaIdentityError,
                                "DuckLake delete requires an old row image",
                                format!(
                                    "Table '{}' emitted a delete without an old row despite \
                                     exposing replica-identity columns",
                                    delete.replicated_table_schema.name()
                                )
                            ));
                        };
                        let table_id = delete.replicated_table_schema.id();
                        let mutation = TrackedTableMutation::new(
                            delete.start_lsn,
                            delete.commit_lsn,
                            delete.tx_ordinal,
                            TableMutation::Delete(old_row),
                        );
                        push_table_mutation_segment(
                            table_id_to_mutations.entry(table_id).or_default(),
                            delete.replicated_table_schema,
                            mutation,
                        );
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "skipping unsupported event type");
                    }
                }
            }

            if !table_id_to_mutations.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                self.ensure_streaming_progress_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (_, mutation_segments) in table_id_to_mutations {
                    let destination = self.clone();

                    join_set.spawn(async move {
                        for segment in mutation_segments {
                            let destination_table_name = destination
                                .ensure_table_ready_for_streaming_schema(
                                    &segment.replicated_table_schema,
                                )
                                .await?;
                            let _table_write_permit = destination
                                .acquire_table_write_slot(&destination_table_name)
                                .await?;
                            destination
                                .ensure_streaming_data_inlining_limit(&destination_table_name)
                                .await?;
                            let replay_epoch = destination
                                .read_table_replay_epoch(&destination_table_name)
                                .await?;
                            let checkpoint_wait_started = tokio::time::Instant::now();
                            let _checkpoint_guard =
                                Arc::clone(&destination.checkpoint_gate).read_owned().await;
                            let checkpoint_wait = checkpoint_wait_started.elapsed();
                            if checkpoint_wait > Duration::from_secs(1) {
                                info!(
                                    table = %destination_table_name,
                                    checkpoint_wait_ms = checkpoint_wait.as_millis() as u64,
                                    "ducklake waited for checkpoint gate before streaming write"
                                );
                            }
                            let last_sequence_key =
                                read_table_streaming_progress_sequence_key_blocking(
                                    Arc::clone(&destination.pool),
                                    Arc::clone(&destination.blocking_slots),
                                    destination_table_name.clone(),
                                    replay_epoch.clone(),
                                )
                                .await?;
                            let pending_mutations = retain_mutations_after_sequence_key(
                                segment.mutations,
                                last_sequence_key,
                            );
                            if pending_mutations.is_empty() {
                                debug!(
                                    table = %destination_table_name,
                                    "ducklake streaming mutation replay skipped, no pending events"
                                );
                                continue;
                            }
                            let is_first_streaming_batch = last_sequence_key.is_none();
                            info!(
                                table = %destination_table_name,
                                pending_mutation_count = pending_mutations.len(),
                                is_first_streaming_batch,
                                "ducklake applying streaming mutations"
                            );

                            let prepared_batches = prepare_mutation_table_batches(
                                &segment.replicated_table_schema,
                                destination_table_name.clone(),
                                replay_epoch,
                                pending_mutations,
                            )?;
                            apply_table_batches_with_retry(
                                Arc::clone(&destination.pool),
                                Arc::clone(&destination.blocking_slots),
                                prepared_batches,
                            )
                            .await?;
                            info!(
                                table = %destination_table_name,
                                is_first_streaming_batch,
                                "ducklake applied streaming mutations"
                            );
                        }

                        Ok::<(), etl::error::EtlError>(())
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    result.map_err(|_| {
                        etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake write task panicked")
                    })??;
                }
            }

            // Apply schema changes sequentially before any later row events
            // are encoded with the new replicated schema.
            while let Some(Event::Relation(_)) = event_iter.peek() {
                if let Some(Event::Relation(relation)) = event_iter.next() {
                    self.handle_relation_event(&relation.replicated_table_schema).await?;
                }
            }

            // Collect contiguous truncate events while preserving table-local order.
            let mut truncate_table_ids: HashMap<
                TableId,
                (ReplicatedTableSchema, Vec<TrackedTruncateEvent>),
            > = HashMap::new();
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate)) = event_iter.next() {
                    for replicated_table_schema in truncate.truncated_tables {
                        let table_id = replicated_table_schema.id();
                        let entry = truncate_table_ids
                            .entry(table_id)
                            .or_insert_with(|| (replicated_table_schema.clone(), Vec::new()));
                        entry.0 = replicated_table_schema;
                        entry.1.push(TrackedTruncateEvent::new(
                            truncate.start_lsn,
                            truncate.commit_lsn,
                            truncate.tx_ordinal,
                            truncate.options,
                        ));
                    }
                }
            }

            if !truncate_table_ids.is_empty() {
                self.ensure_applied_batches_table_exists().await?;
                self.ensure_streaming_progress_table_exists().await?;
                let mut join_set = JoinSet::new();

                for (_, (replicated_table_schema, truncates)) in truncate_table_ids {
                    let table_name = self.ensure_table_exists(&replicated_table_schema).await?;
                    let table_write_permit = self.acquire_table_write_slot(&table_name).await?;
                    self.ensure_streaming_data_inlining_limit(&table_name).await?;
                    let replay_epoch = self.read_table_replay_epoch(&table_name).await?;
                    let checkpoint_gate = Arc::clone(&self.checkpoint_gate);
                    let pool = Arc::clone(&self.pool);
                    let blocking_slots = Arc::clone(&self.blocking_slots);
                    join_set.spawn(async move {
                        let _table_write_permit = table_write_permit;
                        let _checkpoint_guard = checkpoint_gate.read_owned().await;
                        let last_sequence_key =
                            read_table_streaming_progress_sequence_key_blocking(
                                Arc::clone(&pool),
                                Arc::clone(&blocking_slots),
                                table_name.clone(),
                                replay_epoch.clone(),
                            )
                            .await?;
                        let pending_truncates =
                            retain_truncates_after_sequence_key(truncates, last_sequence_key);
                        if pending_truncates.is_empty() {
                            debug!(
                                table = %table_name,
                                "ducklake streaming truncate replay skipped, no pending events"
                            );
                            return Ok(());
                        }

                        let prepared_batch = prepare_truncate_table_batch(
                            table_name,
                            replay_epoch,
                            pending_truncates,
                        );
                        apply_table_batch_with_retry(pool, blocking_slots, prepared_batch).await
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    result.map_err(|_| {
                        etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake truncate task panicked")
                    })??;
                }
            }
        }

        Ok(())
    }

    /// Creates a DuckLake table and brackets the DDL with destination metadata
    /// state transitions.
    async fn create_table_with_metadata(
        &self,
        table_id: TableId,
        table_name: &DuckLakeTableName,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let metadata = DestinationTableMetadata::new_applying(
            table_name.to_metadata_id()?,
            replicated_table_schema.inner().snapshot_id,
            replicated_table_schema.replication_mask().clone(),
        );
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;

        self.issue_create_table_stmt(table_name, replicated_table_schema).await?;
        self.reconcile_missing_replicated_columns(table_name, replicated_table_schema).await?;

        self.store.store_destination_table_metadata(table_id, metadata.to_applied()).await
    }

    /// Issues DuckLake's idempotent `create table if not exists` statement.
    async fn issue_create_table_stmt(
        &self,
        table_name: &DuckLakeTableName,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<()> {
        let column_schemas: Vec<_> = replicated_table_schema.column_schemas().cloned().collect();
        let ddl = build_create_table_sql_ducklake(table_name, &column_schemas);
        let created_tables = Arc::clone(&self.created_tables);
        let table_name = table_name.clone();
        let _checkpoint_guard = self.acquire_mutation_guard().await;

        run_duckdb_blocking(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            move |conn| -> EtlResult<()> {
                debug!(table = %table_name, "ducklake create table begin");
                match conn.execute_batch(&ddl) {
                    Ok(()) => {
                        created_tables.lock().insert(table_name.clone());
                    }
                    Err(error) if is_create_table_conflict(&error, table_name.table()) => {
                        created_tables.lock().insert(table_name.clone());
                    }
                    Err(error) => {
                        return Err(etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake create table failed",
                            format_query_error_detail(&ddl),
                            source: error
                        ));
                    }
                }
                debug!(table = %table_name, "ducklake create table finished");
                Ok(())
            },
        )
        .await
    }

    /// Loads an exact table schema snapshot from the store.
    async fn load_exact_table_schema(
        &self,
        table_id: TableId,
        snapshot_id: SnapshotId,
        missing_schema_description: &'static str,
    ) -> EtlResult<Arc<TableSchema>> {
        let table_schema =
            self.store.get_table_schema(&table_id, snapshot_id).await?.ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    missing_schema_description,
                    format!(
                        "Table {} needs stored schema snapshot {}, but it was not found.",
                        table_id, snapshot_id
                    )
                )
            })?;

        if table_schema.snapshot_id != snapshot_id {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                missing_schema_description,
                format!(
                    "Table {} needs exact schema snapshot {}, but only snapshot {} was found.",
                    table_id, snapshot_id, table_schema.snapshot_id
                )
            ));
        }

        Ok(table_schema)
    }

    /// Loads the best available previous schema for interrupted DDL recovery.
    async fn load_previous_recovery_table_schema(
        &self,
        table_id: TableId,
        previous_snapshot_id: SnapshotId,
    ) -> EtlResult<Arc<TableSchema>> {
        let table_schema =
            self.store.get_table_schema(&table_id, previous_snapshot_id).await?.ok_or_else(
                || {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake schema recovery previous schema not found",
                        format!(
                            "Table {} needs stored schema snapshot {} to recover the destination \
                             table, but it was not found.",
                            table_id, previous_snapshot_id
                        )
                    )
                },
            )?;

        if table_schema.snapshot_id != previous_snapshot_id {
            warn!(
                table_id = %table_id,
                requested_snapshot_id = %previous_snapshot_id,
                found_snapshot_id = %table_schema.snapshot_id,
                "ducklake schema recovery exact previous schema not found, using nearest \
                 available schema"
            );
        }

        Ok(table_schema)
    }

    /// Resolves the target replicated schema for interrupted DDL recovery.
    async fn target_schema_for_recovery(
        &self,
        table_id: TableId,
        metadata: &DestinationTableMetadata,
        provided_target_schema: Option<&ReplicatedTableSchema>,
    ) -> EtlResult<ReplicatedTableSchema> {
        if let Some(schema) = provided_target_schema
            && schema.inner().snapshot_id == metadata.snapshot_id
            && schema.replication_mask() == &metadata.replication_mask
        {
            return Ok(schema.clone());
        }

        let target_table_schema = self
            .load_exact_table_schema(
                table_id,
                metadata.snapshot_id,
                "DuckLake schema recovery target schema not found",
            )
            .await?;

        Ok(ReplicatedTableSchema::from_mask(target_table_schema, metadata.replication_mask.clone()))
    }

    /// Replays interrupted DuckLake DDL and transitions metadata back to
    /// `Applied`.
    async fn recover_applying_metadata(
        &self,
        table_id: TableId,
        table_name: &DuckLakeTableName,
        metadata: DestinationTableMetadata,
        target_schema: Option<&ReplicatedTableSchema>,
    ) -> EtlResult<DestinationTableMetadata> {
        warn!(
            table_id = %table_id,
            table = %table_name,
            "ducklake table has Applying metadata, recovering interrupted operation"
        );

        let target_schema =
            self.target_schema_for_recovery(table_id, &metadata, target_schema).await?;

        match metadata.previous_snapshot_id {
            Some(previous_snapshot_id) => {
                let previous_table_schema = self
                    .load_previous_recovery_table_schema(table_id, previous_snapshot_id)
                    .await?;
                let previous_replication_mask = previous_replication_mask_for_recovery(
                    &previous_table_schema,
                    target_schema.inner(),
                    &metadata.replication_mask,
                );
                let old_schema = ReplicatedTableSchema::from_mask(
                    previous_table_schema,
                    previous_replication_mask,
                );
                let diff = old_schema.diff(&target_schema);
                self.apply_schema_diff(table_name, &diff).await?;
            }
            None => {
                self.issue_create_table_stmt(table_name, &target_schema).await?;
            }
        }
        self.reconcile_missing_replicated_columns(table_name, &target_schema).await?;

        let metadata = metadata.to_applied();
        self.store.store_destination_table_metadata(table_id, metadata.clone()).await?;
        self.cleanup_tombstone_columns_after_applied(table_name, &target_schema).await;
        self.created_tables.lock().insert(table_name.clone());

        Ok(metadata)
    }

    /// Ensures the destination table exists, creating it (DDL) if necessary.
    async fn ensure_table_exists(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<DuckLakeTableName> {
        let table_id = replicated_table_schema.id();
        let metadata = self.store.get_destination_table_metadata(table_id).await?;
        let table_name = metadata.as_ref().map_or_else(
            || table_name_to_ducklake_table_name(replicated_table_schema.name()),
            |metadata| DuckLakeTableName::from_metadata_id(&metadata.destination_table_id),
        )?;

        if !metadata.as_ref().is_some_and(DestinationTableMetadata::is_applying)
            && self.created_tables.lock().contains(&table_name)
        {
            return Ok(table_name);
        }

        info!(
            table_id = %table_id,
            table = %table_name,
            "ducklake destination table cache miss, ensuring table exists"
        );

        let _table_creation_permit =
            Arc::clone(&self.table_creation_slots).acquire_owned().await.map_err(|_| {
                etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
            })?;

        let metadata = self.store.get_destination_table_metadata(table_id).await?;
        let table_name = metadata.as_ref().map_or_else(
            || table_name_to_ducklake_table_name(replicated_table_schema.name()),
            |metadata| DuckLakeTableName::from_metadata_id(&metadata.destination_table_id),
        )?;

        if !metadata.as_ref().is_some_and(DestinationTableMetadata::is_applying)
            && self.created_tables.lock().contains(&table_name)
        {
            return Ok(table_name);
        }

        match metadata {
            None => {
                self.create_table_with_metadata(table_id, &table_name, replicated_table_schema)
                    .await?;
            }
            Some(metadata) if metadata.is_applying() => {
                self.recover_applying_metadata(
                    table_id,
                    &table_name,
                    metadata,
                    Some(replicated_table_schema),
                )
                .await?;
            }
            Some(_) => {
                self.issue_create_table_stmt(&table_name, replicated_table_schema).await?;
                self.reconcile_missing_replicated_columns(&table_name, replicated_table_schema)
                    .await?;
            }
        }

        Ok(table_name)
    }

    /// Ensures destination metadata and physical columns can accept a row
    /// schema.
    async fn ensure_table_ready_for_streaming_schema(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<DuckLakeTableName> {
        let table_id = replicated_table_schema.id();
        if let Some(metadata) = self.store.get_destination_table_metadata(table_id).await?
            && metadata.is_applied()
            && (metadata.snapshot_id < replicated_table_schema.inner().snapshot_id
                || (metadata.snapshot_id == replicated_table_schema.inner().snapshot_id
                    && &metadata.replication_mask != replicated_table_schema.replication_mask()))
        {
            self.handle_relation_event(replicated_table_schema).await?;
        }

        self.ensure_table_exists(replicated_table_schema).await
    }

    /// Ensures the ETL-managed replay marker table exists.
    async fn ensure_applied_batches_table_exists(&self) -> EtlResult<()> {
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        ensure_applied_batches_table_exists(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            Arc::clone(&self.table_creation_slots),
            Arc::clone(&self.applied_batches_table_created),
        )
        .await
    }

    /// Ensures the ETL-managed streaming progress table exists.
    async fn ensure_streaming_progress_table_exists(&self) -> EtlResult<()> {
        let _checkpoint_guard = self.acquire_mutation_guard().await;
        ensure_streaming_progress_table_exists(
            Arc::clone(&self.pool),
            Arc::clone(&self.blocking_slots),
            Arc::clone(&self.table_creation_slots),
            Arc::clone(&self.streaming_progress_table_created),
        )
        .await
    }

    /// Returns the stored destination table name or the deterministic default.
    async fn resolve_destination_table_name(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
    ) -> EtlResult<DuckLakeTableName> {
        let table_id = replicated_table_schema.id();

        if let Some(existing) = self.store.get_destination_table_metadata(table_id).await? {
            return DuckLakeTableName::from_metadata_id(&existing.destination_table_id);
        }

        table_name_to_ducklake_table_name(replicated_table_schema.name())
    }

    /// Serializes table-local truncate and CDC mutation writes.
    async fn acquire_table_write_slot(
        &self,
        table_name: &DuckLakeTableName,
    ) -> EtlResult<OwnedSemaphorePermit> {
        let table_slot = table_write_slot(&self.table_write_slots, table_name);
        match Arc::clone(&table_slot).try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(TryAcquireError::NoPermits) => {
                info!(
                    table = %table_name,
                    "ducklake waiting for table write slot"
                );
                let permit = table_slot.acquire_owned().await.map_err(|_| {
                    etl_error!(ErrorKind::InvalidState, "DuckLake table write semaphore closed")
                })?;
                info!(
                    table = %table_name,
                    "ducklake acquired table write slot after wait"
                );
                Ok(permit)
            }
            Err(TryAcquireError::Closed) => {
                Err(etl_error!(ErrorKind::InvalidState, "DuckLake table write semaphore closed"))
            }
        }
    }

    /// Disables data inlining for one table while its initial copy is active.
    async fn ensure_copy_data_inlining_limit(
        &self,
        table_name: &DuckLakeTableName,
    ) -> EtlResult<()> {
        self.set_table_data_inlining_row_limit(table_name, COPY_DATA_INLINING_ROW_LIMIT).await
    }

    /// Restores the regular streaming inlining limit for one table.
    async fn ensure_streaming_data_inlining_limit(
        &self,
        table_name: &DuckLakeTableName,
    ) -> EtlResult<()> {
        self.set_table_data_inlining_row_limit(table_name, ATTACH_DATA_INLINING_ROW_LIMIT).await
    }

    /// Sets one table's DuckLake inlining limit once per process and phase.
    ///
    /// Callers hold the table write slot, so there is no concurrent phase
    /// transition for the same destination table.
    async fn set_table_data_inlining_row_limit(
        &self,
        table_name: &DuckLakeTableName,
        row_limit: u64,
    ) -> EtlResult<()> {
        if self.table_data_inlining_limits.lock().get(table_name) == Some(&row_limit) {
            return Ok(());
        }

        let table_name_for_sql = table_name.clone();
        self.run_duckdb_blocking(move |conn| -> EtlResult<()> {
            let sql = table_data_inlining_row_limit_sql(&table_name_for_sql, row_limit);
            conn.execute_batch(&sql).map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake set table data inlining limit failed",
                    format_query_error_detail(&sql),
                    source: source
                )
            })
        })
        .await?;

        self.table_data_inlining_limits.lock().insert(table_name.clone(), row_limit);

        Ok(())
    }

    /// Acquires shared mutation access so exclusive external maintenance cannot
    /// start in the middle of a foreground write sequence.
    async fn acquire_mutation_guard(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.checkpoint_gate).read_owned().await
    }

    async fn read_table_replay_epoch(&self, table_name: &DuckLakeTableName) -> EtlResult<String> {
        read_table_replay_epoch(&self.metadata_pg_pool, self.metadata_schema.as_ref(), table_name)
            .await
    }

    async fn rotate_table_replay_epoch(&self, table_name: &DuckLakeTableName) -> EtlResult<String> {
        rotate_table_replay_epoch(&self.metadata_pg_pool, self.metadata_schema.as_ref(), table_name)
            .await
    }

    /// Acquires exclusive DuckLake mutation access for an external maintenance
    /// run. While this guard is held, new foreground writes and in-process
    /// background maintenance operations wait before mutating the catalog.
    pub async fn acquire_external_maintenance_pause(&self) -> DuckLakeExternalMaintenancePause {
        DuckLakeExternalMaintenancePause {
            _guard: Arc::clone(&self.checkpoint_gate).write_owned().await,
        }
    }

    /// Samples catalog state and returns which externally coordinated
    /// maintenance operations should be requested now.
    pub(super) async fn sample_external_maintenance_operations(
        &self,
        inline_flush_min_inlined_bytes: u64,
        rewrite_data_files_min_active_data_files: i64,
    ) -> EtlResult<ExternalMaintenanceOperationSample> {
        let table_names = self.list_active_ducklake_tables().await?;
        let inline_sampler = DuckLakePendingInlineSizeSampler::new(
            self.metadata_schema.to_string(),
            self.metadata_pg_pool.clone(),
        );
        let copy_phase_active = self.has_active_table_copy().await?;
        let mut operations = ExternalMaintenanceOperations::default();
        let catalog_metrics = query_catalog_maintenance_metrics(
            &self.metadata_pg_pool,
            self.metadata_schema.as_ref(),
        )
        .await?;

        match expire_snapshots_retention_seconds(self.expire_snapshots_older_than.as_ref()) {
            Some(retention_seconds) => {
                operations.expire_snapshots = catalog_metrics.snapshots_total > 1
                    && catalog_metrics.oldest_snapshot_age_seconds >= retention_seconds;
                debug!(
                    metadata_schema = %self.metadata_schema,
                    expire_snapshots_older_than = %self.expire_snapshots_older_than,
                    retention_seconds,
                    snapshots_total = catalog_metrics.snapshots_total,
                    oldest_snapshot_age_seconds = catalog_metrics.oldest_snapshot_age_seconds,
                    expire_snapshots = operations.expire_snapshots,
                    "ducklake sampled expire snapshots trigger: metadata_schema={}, \
                     expire_snapshots_older_than={}, retention_seconds={}, snapshots_total={}, \
                     oldest_snapshot_age_seconds={}, expire_snapshots={}",
                    self.metadata_schema,
                    self.expire_snapshots_older_than,
                    retention_seconds,
                    catalog_metrics.snapshots_total,
                    catalog_metrics.oldest_snapshot_age_seconds,
                    operations.expire_snapshots
                );
            }
            None => {
                warn!(
                    metadata_schema = %self.metadata_schema,
                    expire_snapshots_older_than = %self.expire_snapshots_older_than,
                    "ducklake could not parse expire_snapshots_older_than for external maintenance \
                     trigger sampling: metadata_schema={}, expire_snapshots_older_than={}",
                    self.metadata_schema,
                    self.expire_snapshots_older_than
                );
            }
        }

        for table_name in table_names {
            if table_name.is_internal_helper() {
                continue;
            }

            if !operations.inline_flush {
                let sizes = inline_sampler.sample_table(&table_name).await?;
                operations.inline_flush = sizes.inlined_bytes >= inline_flush_min_inlined_bytes;
            }

            if !operations.rewrite_data_files && !copy_phase_active {
                let metrics = query_table_storage_metrics(
                    &self.metadata_pg_pool,
                    self.metadata_schema.as_ref(),
                    &table_name,
                )
                .await?;
                operations.rewrite_data_files = should_request_file_maintenance(
                    copy_phase_active,
                    metrics.active_data_files,
                    rewrite_data_files_min_active_data_files,
                );
            }

            if operations.inline_flush && operations.rewrite_data_files {
                break;
            }
        }

        if !copy_phase_active && operations.rewrite_data_files {
            operations.merge_adjacent_files = true;
            operations.cleanup_old_files = true;
        }

        Ok(ExternalMaintenanceOperationSample { operations, copy_phase_active })
    }

    /// Returns whether any table is currently in initial copy.
    async fn has_active_table_copy(&self) -> EtlResult<bool> {
        let table_states = self.store.get_table_states().await?;
        Ok(table_states.values().any(|state| state.as_type() == TableStateType::DataSync))
    }

    /// Lists active DuckLake table names from the metadata catalog.
    async fn list_active_ducklake_tables(&self) -> EtlResult<Vec<DuckLakeTableName>> {
        let sql = format!(
            "SELECT s.schema_name, t.table_name FROM {}.{} AS t JOIN {}.{} AS s ON s.schema_id = \
             t.schema_id WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL ORDER BY \
             s.schema_name, t.table_name",
            quote_postgres_identifier(self.metadata_schema.as_ref()),
            quote_postgres_identifier("ducklake_table"),
            quote_postgres_identifier(self.metadata_schema.as_ref()),
            quote_postgres_identifier("ducklake_schema")
        );
        let rows: Vec<(String, String)> = sqlx::query_as(AssertSqlSafe(sql))
            .fetch_all(&self.metadata_pg_pool)
            .await
            .map_err(|source| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake table list query failed",
                    format!("metadata_schema={}", self.metadata_schema.as_ref()),
                    source: source
                )
            })?;
        Ok(rows
            .into_iter()
            .map(|(schema_name, table_name)| DuckLakeTableName::new(schema_name, table_name))
            .collect())
    }

    /// Runs one DuckDB operation on Tokio's blocking pool after acquiring a
    /// permit that matches the configured DuckDB concurrency limit.
    async fn run_duckdb_blocking<R, F>(&self, operation: F) -> EtlResult<R>
    where
        R: Send + 'static,
        F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
    {
        run_duckdb_blocking(Arc::clone(&self.pool), Arc::clone(&self.blocking_slots), operation)
            .await
    }

    /// Stops the background DuckLake metrics sampler.
    async fn shutdown_metrics_sampler(&self) -> EtlResult<()> {
        if let Some(metrics_sampler) = &*self.metrics_sampler {
            let _ = metrics_sampler.shutdown_tx.send(());
            let handle = metrics_sampler.handle.lock().take();
            if let Some(handle) = handle {
                handle.abort();
                if let Err(err) = handle.await
                    && !err.is_cancelled()
                {
                    return Err(etl_error!(
                        ErrorKind::ApplyWorkerPanic,
                        "DuckLake metrics sampler task panicked"
                    ));
                }
            }
        }

        Ok(())
    }
    /// Returns how many DuckDB connections have been initialized for tests.
    #[cfg(feature = "test-utils")]
    pub fn connection_open_count_for_tests(&self) -> usize {
        self.manager.open_count_for_tests()
    }
}

/// Reads the persisted streaming replay watermark for one table on DuckDB's
/// blocking pool.
async fn read_table_streaming_progress_sequence_key_blocking(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_name: DuckLakeTableName,
    replay_epoch: String,
) -> EtlResult<Option<EventSequenceKey>> {
    run_duckdb_blocking(pool, blocking_slots, move |conn| {
        read_table_streaming_progress_sequence_key(conn, &table_name, &replay_epoch)
    })
    .await
}

#[cfg(feature = "test-utils")]
struct PausedStreamingWriteHook {
    reached_tx: oneshot::Sender<()>,
    resume_rx: oneshot::Receiver<()>,
}

#[cfg(feature = "test-utils")]
static PAUSED_STREAMING_WRITE_HOOK: std::sync::LazyLock<Mutex<Option<PausedStreamingWriteHook>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static PAUSED_STREAMING_WRITE_RESUME_TX: std::sync::LazyLock<Mutex<Option<oneshot::Sender<()>>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

/// Arms a one-shot hook that pauses the next streaming write before DuckLake
/// starts applying it.
#[cfg(feature = "test-utils")]
pub fn arm_pause_next_streaming_write_for_tests() -> oneshot::Receiver<()> {
    let (reached_tx, reached_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    *PAUSED_STREAMING_WRITE_HOOK.lock() = Some(PausedStreamingWriteHook { reached_tx, resume_rx });
    *PAUSED_STREAMING_WRITE_RESUME_TX.lock() = Some(resume_tx);
    reached_rx
}

/// Releases the paused streaming-write test hook, if one is armed.
#[cfg(feature = "test-utils")]
pub fn release_paused_streaming_write_for_tests() {
    if let Some(resume_tx) = PAUSED_STREAMING_WRITE_RESUME_TX.lock().take() {
        let _ = resume_tx.send(());
    }
}

/// Clears the paused streaming-write hook without waiting for it to be
/// released.
#[cfg(feature = "test-utils")]
pub fn reset_paused_streaming_write_for_tests() {
    PAUSED_STREAMING_WRITE_HOOK.lock().take();
    PAUSED_STREAMING_WRITE_RESUME_TX.lock().take();
}

#[cfg(feature = "test-utils")]
async fn wait_if_streaming_write_paused_for_tests() {
    let Some(PausedStreamingWriteHook { reached_tx, resume_rx }) =
        PAUSED_STREAMING_WRITE_HOOK.lock().take()
    else {
        return;
    };

    let _ = reached_tx.send(());
    let _ = resume_rx.await;
}

/// Converts a Postgres [`TableName`] to the matching DuckLake schema/table.
///
/// Source schemas are preserved in DuckLake. For example, `public.my_table`
/// becomes the DuckLake table `lake.public.my_table`.
pub fn table_name_to_ducklake_table_name(table_name: &TableName) -> EtlResult<DuckLakeTableName> {
    Ok(DuckLakeTableName::from_source(table_name))
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::{Path, PathBuf},
        time::Instant,
    };

    use duckdb::{Config, Connection};
    use etl::{
        config::{PgConnectionConfig, TcpKeepaliveConfig},
        data::{Cell, PartialTableRow, TableRow},
        schema::{
            ColumnChange, ColumnModification, ColumnSchema, IdentityMask, ReplicationMask,
            SchemaDiff, SnapshotId, TableSchema, Type as PgType,
        },
        store::{MemoryStore, SchemaStore},
    };
    use etl_maintenance::ducklake::flush_table_inlined_data;
    use etl_postgres::{test_utils::local_tls_config_from_env, tokio::test_utils::PgDatabase};
    use pg_escape::{quote_identifier, quote_literal};
    use tempfile::TempDir;
    use tokio_postgres::Client;
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::ducklake::{
        LAKE_CATALOG,
        config::catalog_conninfo_from_url,
        metrics::{query_catalog_maintenance_metrics, query_table_storage_metrics},
    };

    const POSTGRES_SCANNER_EXTENSION_FILE: &str = "postgres_scanner.duckdb_extension";

    /// Keeps compaction from competing with batches that are creating Parquet
    /// files during an initial copy.
    #[test]
    fn file_maintenance_is_deferred_during_copy() {
        assert!(!should_request_file_maintenance(true, 41, 40));
        assert!(!should_request_file_maintenance(false, 40, 40));
        assert!(should_request_file_maintenance(false, 41, 40));
    }

    #[test]
    fn copy_data_inlining_limit_targets_the_destination_table() {
        let sql =
            table_data_inlining_row_limit_sql(&ducklake_table_name(), COPY_DATA_INLINING_ROW_LIMIT);

        assert_eq!(
            sql,
            "CALL lake.set_option('data_inlining_row_limit', 0, schema => 'public', table_name => \
             'users');"
        );
    }

    #[test]
    fn expire_snapshots_retention_seconds_uses_humantime_duration_syntax() {
        assert_eq!(expire_snapshots_retention_seconds("7 days"), Some(604_800));
        assert_eq!(expire_snapshots_retention_seconds("2h 30min"), Some(9_000));
        assert_eq!(expire_snapshots_retention_seconds("bad interval"), None);
    }

    fn make_schema(table_id: u32, schema: &str, table: &str) -> TableSchema {
        TableSchema::new(
            TableId::new(table_id),
            TableName::new(schema.to_owned(), table.to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ],
        )
    }

    fn ducklake_table_name() -> DuckLakeTableName {
        DuckLakeTableName::new("public", "users")
    }

    fn make_alternative_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(2),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        ));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0]),
        )
    }

    fn make_missing_identity_schema() -> ReplicatedTableSchema {
        let table_schema = Arc::new(make_schema(3, "public", "users"));

        ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 0]),
        )
    }

    fn rename_change(old_name: &str, new_name: &str, ordinal_position: i32) -> ColumnChange {
        ColumnChange {
            ordinal_position,
            old_column: ColumnSchema::new(
                old_name.to_owned(),
                PgType::TEXT,
                -1,
                ordinal_position,
                true,
            ),
            new_column: ColumnSchema::new(
                new_name.to_owned(),
                PgType::TEXT,
                -1,
                ordinal_position,
                true,
            ),
            modifications: vec![ColumnModification::Rename {
                old_name: old_name.to_owned(),
                new_name: new_name.to_owned(),
            }],
        }
    }

    fn default_change(
        name: &str,
        ordinal_position: i32,
        old_expression: Option<&str>,
        new_expression: Option<&str>,
    ) -> ColumnChange {
        let old_column =
            ColumnSchema::new(name.to_owned(), PgType::TEXT, -1, ordinal_position, true)
                .with_default_expression_option(old_expression.map(ToOwned::to_owned));
        let new_column =
            ColumnSchema::new(name.to_owned(), PgType::TEXT, -1, ordinal_position, true)
                .with_default_expression_option(new_expression.map(ToOwned::to_owned));

        ColumnChange {
            ordinal_position,
            old_column,
            new_column,
            modifications: vec![ColumnModification::Default {
                old_expression: old_expression.map(ToOwned::to_owned),
                new_expression: new_expression.map(ToOwned::to_owned),
            }],
        }
    }

    #[test]
    fn key_row_from_updated_partial_row_uses_alternative_identity_columns() {
        let replicated_table_schema = make_alternative_identity_schema();
        let partial_row = PartialTableRow::new(
            3,
            TableRow::new(vec![Cell::I32(1), Cell::String("alice@example.com".to_owned())]),
            vec![2],
        );

        let key_row = DuckLakeDestination::<MemoryStore>::key_row_from_updated_partial_row(
            &replicated_table_schema,
            &partial_row,
        )
        .unwrap();

        assert_eq!(key_row, TableRow::new(vec![Cell::String("alice@example.com".to_owned())]));
    }

    #[test]
    fn key_row_from_updated_partial_row_rejects_missing_replica_identity() {
        let replicated_table_schema = make_missing_identity_schema();
        let partial_row = PartialTableRow::new(
            2,
            TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]),
            vec![],
        );

        let error = DuckLakeDestination::<MemoryStore>::key_row_from_updated_partial_row(
            &replicated_table_schema,
            &partial_row,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert_eq!(error.description(), Some("DuckLake update requires a replica identity"));
    }

    #[test]
    fn plan_schema_diff_renames_before_adding_reused_source_name() {
        let diff = SchemaDiff {
            columns_to_add: vec![ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true)],
            columns_to_remove: Vec::new(),
            columns_to_change: vec![rename_change("name", "full_name", 2)],
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "name".to_owned()],
            &diff,
        )
        .expect("schema diff should plan");
        let statement_sql: Vec<_> =
            plan.statements.iter().map(|statement| statement.sql.clone()).collect();

        assert_eq!(
            statement_sql,
            vec![
                r#"alter table "lake"."public"."users" rename column "name" to "full_name""#,
                r#"alter table "lake"."public"."users" add column "name" varchar"#,
            ]
        );
        assert_eq!(plan.column_names, vec!["id", "full_name", "name"]);
    }

    #[test]
    fn plan_schema_diff_adds_column_with_supported_default() {
        let diff = SchemaDiff {
            columns_to_add: vec![
                ColumnSchema::new("status".to_owned(), PgType::TEXT, -1, 3, true)
                    .with_default_expression("'pending'::text".to_owned()),
            ],
            columns_to_remove: Vec::new(),
            columns_to_change: Vec::new(),
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "name".to_owned()],
            &diff,
        )
        .expect("schema diff should plan");
        let statement_sql: Vec<_> =
            plan.statements.iter().map(|statement| statement.sql.clone()).collect();

        assert_eq!(
            statement_sql,
            vec![
                r#"alter table "lake"."public"."users" add column "status" varchar default 'pending'"#
            ]
        );
        assert_eq!(plan.column_names, vec!["id", "name", "status"]);
    }

    #[test]
    fn plan_schema_diff_skips_unsupported_default_drop() {
        let diff = SchemaDiff {
            columns_to_add: Vec::new(),
            columns_to_remove: Vec::new(),
            columns_to_change: vec![default_change(
                "status",
                3,
                Some("array['unsupported']::text[]"),
                None,
            )],
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "name".to_owned(), "status".to_owned()],
            &diff,
        )
        .expect("schema diff should plan");

        assert!(plan.statements.is_empty());
        assert_eq!(plan.column_names, vec!["id", "name", "status"]);
    }

    #[test]
    fn plan_schema_diff_skips_replayed_rename_with_reused_source_name() {
        let diff = SchemaDiff {
            columns_to_add: vec![ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true)],
            columns_to_remove: Vec::new(),
            columns_to_change: vec![rename_change("name", "full_name", 2)],
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "full_name".to_owned(), "name".to_owned()],
            &diff,
        )
        .expect("schema diff replay should plan");

        assert!(plan.statements.is_empty());
        assert_eq!(plan.column_names, vec!["id", "full_name", "name"]);
    }

    #[test]
    fn plan_schema_diff_skips_replayed_rename_with_source_name_as_rename_target() {
        let diff = SchemaDiff {
            columns_to_add: Vec::new(),
            columns_to_remove: Vec::new(),
            columns_to_change: vec![
                rename_change("name", "full_name", 2),
                rename_change("email", "name", 3),
            ],
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "full_name".to_owned(), "name".to_owned()],
            &diff,
        )
        .expect("schema diff replay should plan");

        assert!(plan.statements.is_empty());
        assert_eq!(plan.column_names, vec!["id", "full_name", "name"]);
    }

    #[test]
    fn plan_schema_diff_drops_stale_rename_source_when_target_exists() {
        let diff = SchemaDiff {
            columns_to_add: Vec::new(),
            columns_to_remove: Vec::new(),
            columns_to_change: vec![rename_change("ddl_col_4_1", "ddl_col_4_0", 4)],
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "ddl_col_4_1".to_owned(), "ddl_col_4_0".to_owned()],
            &diff,
        )
        .expect("schema diff with stale rename source should plan");
        let statement_sql: Vec<_> =
            plan.statements.iter().map(|statement| statement.sql.as_str()).collect();

        assert_eq!(
            statement_sql,
            vec![r#"alter table "lake"."public"."users" drop column "ddl_col_4_1""#]
        );
        assert_eq!(plan.column_names, vec!["id", "ddl_col_4_0"]);
    }

    #[test]
    fn plan_schema_diff_tombstones_removed_column_when_name_is_reused_by_rename() {
        let removed_column = ColumnSchema::new("status".to_owned(), PgType::TEXT, -1, 3, true);
        let tombstone_name = dropped_column_tombstone_name_ducklake(&removed_column);
        let diff = SchemaDiff {
            columns_to_add: Vec::new(),
            columns_to_remove: vec![removed_column],
            columns_to_change: vec![rename_change("name", "status", 2)],
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "name".to_owned(), "status".to_owned()],
            &diff,
        )
        .expect("schema diff should tombstone reused removed name");
        let statement_sql: Vec<_> =
            plan.statements.iter().map(|statement| statement.sql.clone()).collect();

        assert_eq!(
            statement_sql,
            vec![
                format!(
                    r#"alter table "lake"."public"."users" rename column "status" to "{tombstone_name}""#
                ),
                r#"alter table "lake"."public"."users" rename column "name" to "status""#
                    .to_owned(),
            ]
        );
        assert_eq!(plan.column_names, vec!["id", "status", tombstone_name.as_str()]);
    }

    #[test]
    fn plan_schema_diff_tombstones_removed_column_when_name_is_reused_by_add() {
        let removed_column = ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true);
        let tombstone_name = dropped_column_tombstone_name_ducklake(&removed_column);
        let diff = SchemaDiff {
            columns_to_add: vec![ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true)],
            columns_to_remove: vec![removed_column],
            columns_to_change: Vec::new(),
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), "name".to_owned()],
            &diff,
        )
        .expect("schema diff should tombstone reused removed name");
        let statement_sql: Vec<_> =
            plan.statements.iter().map(|statement| statement.sql.as_str()).collect();

        assert_eq!(
            statement_sql,
            vec![
                format!(
                    r#"alter table "lake"."public"."users" rename column "name" to "{tombstone_name}""#
                ),
                r#"alter table "lake"."public"."users" add column "name" varchar"#.to_owned(),
            ]
        );
        assert_eq!(plan.column_names, vec!["id", tombstone_name.as_str(), "name"]);
    }

    #[test]
    fn plan_schema_diff_skips_replayed_reused_removed_column_name() {
        let removed_column = ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true);
        let tombstone_name = dropped_column_tombstone_name_ducklake(&removed_column);
        let diff = SchemaDiff {
            columns_to_add: vec![ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true)],
            columns_to_remove: vec![removed_column],
            columns_to_change: Vec::new(),
        };

        let plan = plan_schema_diff_sql_ducklake(
            &ducklake_table_name(),
            vec!["id".to_owned(), tombstone_name.clone(), "name".to_owned()],
            &diff,
        )
        .expect("schema diff replay should plan");

        assert!(plan.statements.is_empty());
        assert_eq!(plan.column_names, vec!["id", tombstone_name.as_str(), "name"]);
    }

    #[test]
    fn tombstone_columns_to_cleanup_keeps_active_prefixed_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new(
                    "__etl_ducklake_dropped_business".to_owned(),
                    PgType::TEXT,
                    -1,
                    2,
                    true,
                ),
            ],
        ));
        let replicated_table_schema =
            ReplicatedTableSchema::from_mask(table_schema, ReplicationMask::from_bytes(vec![1, 1]));
        let column_names = vec![
            "id".to_owned(),
            "__etl_ducklake_dropped_business".to_owned(),
            "__etl_ducklake_dropped_3_abcdef".to_owned(),
        ];

        let columns_to_cleanup =
            tombstone_columns_to_cleanup_ducklake(&column_names, &replicated_table_schema);

        assert_eq!(columns_to_cleanup, vec!["__etl_ducklake_dropped_3_abcdef"]);
    }

    #[test]
    fn missing_replicated_columns_ducklake_returns_only_active_target_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(4),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
                ColumnSchema::new("hidden".to_owned(), PgType::TEXT, -1, 3, true),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 4, true),
            ],
        ));
        let target_schema = ReplicatedTableSchema::from_mask(
            Arc::clone(&table_schema),
            ReplicationMask::from_bytes(vec![1, 1, 0, 1]),
        );

        let missing_columns = missing_replicated_columns_ducklake(
            &["id".to_owned(), "name".to_owned()],
            &target_schema,
        );
        let missing_column_names: Vec<_> =
            missing_columns.iter().map(|column| column.name.as_str()).collect();

        assert_eq!(missing_column_names, vec!["email"]);
    }

    #[test]
    fn previous_replication_mask_for_recovery_matches_previous_schema_width() {
        let previous_schema = TableSchema::new(
            TableId::new(4),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("hidden".to_owned(), PgType::TEXT, -1, 2, true),
            ],
        );
        let target_schema = TableSchema::with_snapshot_id(
            previous_schema.id,
            previous_schema.name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("hidden".to_owned(), PgType::TEXT, -1, 2, true),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 3, true),
            ],
            SnapshotId::from(42_u64),
        );
        let target_mask = ReplicationMask::from_bytes(vec![1, 0, 1]);

        let previous_mask =
            previous_replication_mask_for_recovery(&previous_schema, &target_schema, &target_mask);

        assert_eq!(previous_mask.to_bytes(), vec![1, 0]);
    }

    #[test]
    fn previous_replication_mask_for_recovery_keeps_removed_columns_for_drop_diff() {
        let previous_schema = TableSchema::new(
            TableId::new(5),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
                ColumnSchema::new("old_col".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        );
        let target_schema = TableSchema::with_snapshot_id(
            previous_schema.id,
            previous_schema.name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ],
            SnapshotId::from(43_u64),
        );
        let target_mask = ReplicationMask::from_bytes(vec![1, 1]);

        let previous_mask =
            previous_replication_mask_for_recovery(&previous_schema, &target_schema, &target_mask);

        assert_eq!(previous_mask.to_bytes(), vec![1, 1, 1]);
    }

    fn path_to_file_url(path: &Path) -> Url {
        Url::from_file_path(path).expect("failed to convert path to file url")
    }

    fn local_pg_connection_config(database_name: String) -> PgConnectionConfig {
        PgConnectionConfig {
            host: env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set"),
            hostaddr: None,
            port: env::var("TESTS_DATABASE_PORT")
                .expect("TESTS_DATABASE_PORT must be set")
                .parse()
                .expect("TESTS_DATABASE_PORT must be a valid port number"),
            name: database_name,
            username: env::var("TESTS_DATABASE_USERNAME")
                .expect("TESTS_DATABASE_USERNAME must be set"),
            password: env::var("TESTS_DATABASE_PASSWORD").ok().map(Into::into),
            tls: local_tls_config_from_env(),
            keepalive: TcpKeepaliveConfig::default(),
        }
    }

    async fn create_catalog_database() -> (PgDatabase<Client>, Url) {
        let database_name = Uuid::new_v4().to_string();
        let host = env::var("TESTS_DATABASE_HOST").expect("TESTS_DATABASE_HOST must be set");
        let port = env::var("TESTS_DATABASE_PORT")
            .expect("TESTS_DATABASE_PORT must be set")
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number");
        let username =
            env::var("TESTS_DATABASE_USERNAME").expect("TESTS_DATABASE_USERNAME must be set");
        let password = env::var("TESTS_DATABASE_PASSWORD").ok();
        let database = PgDatabase::new(local_pg_connection_config(database_name.clone())).await;

        let mut catalog_url = Url::parse("postgres://localhost").expect("failed to parse base url");
        catalog_url.set_host(Some(&host)).expect("failed to set catalog host");
        catalog_url.set_port(Some(port)).expect("failed to set catalog port");
        catalog_url.set_username(&username).expect("failed to set catalog username");
        catalog_url.set_password(password.as_deref()).expect("failed to set catalog password");
        catalog_url.set_path(&database_name);

        (database, catalog_url)
    }

    fn current_vendored_extension_dir() -> Option<PathBuf> {
        let platform_dir = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64" | "amd64") => "linux_amd64",
            ("linux", "aarch64" | "arm64") => "linux_arm64",
            ("macos", "x86_64" | "amd64") => "osx_amd64",
            ("macos", "aarch64" | "arm64") => "osx_arm64",
            _ => return None,
        };
        let env_override = std::env::var_os("ETL_DUCKDB_EXTENSION_ROOT").map(PathBuf::from);
        let candidate_roots = env_override
            .into_iter()
            .chain([
                PathBuf::from("/app/duckdb_extensions"),
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/duckdb/extensions"),
            ])
            .collect::<Vec<_>>();

        for root in candidate_roots {
            let extension_dir = root.join("1.5.3").join(platform_dir);
            let ducklake_extension = extension_dir.join("ducklake.duckdb_extension");
            let postgres_scanner_extension = extension_dir.join(POSTGRES_SCANNER_EXTENSION_FILE);

            if ducklake_extension.is_file() && postgres_scanner_extension.is_file() {
                return Some(extension_dir);
            }
        }

        None
    }

    fn open_verification_connection() -> Connection {
        let duckdb_dir = tempfile::Builder::new()
            .prefix("etl_ducklake_verify_")
            .tempdir()
            .expect("failed to create verification duckdb dir")
            .keep();
        let duckdb_path = duckdb_dir.join("verify.duckdb");

        let conn = if current_vendored_extension_dir().is_some() {
            Connection::open_with_flags(
                &duckdb_path,
                Config::default()
                    .enable_autoload_extension(false)
                    .expect("failed to disable DuckDB extension autoload"),
            )
            .expect("failed to open verification DuckDB")
        } else {
            Connection::open(&duckdb_path).expect("failed to open verification DuckDB")
        };

        conn.execute_batch("SET preserve_insertion_order = false;")
            .expect("failed to configure verification DuckDB session");
        conn
    }

    fn ducklake_load_sql() -> String {
        if let Some(extension_dir) = current_vendored_extension_dir() {
            let ducklake_extension = extension_dir.join("ducklake.duckdb_extension");
            let postgres_scanner_extension = extension_dir.join(POSTGRES_SCANNER_EXTENSION_FILE);

            return format!(
                "LOAD {}; LOAD {};",
                quote_literal(&ducklake_extension.display().to_string()),
                quote_literal(&postgres_scanner_extension.display().to_string()),
            );
        }

        "INSTALL ducklake; LOAD ducklake; INSTALL postgres_scanner; LOAD postgres_scanner;"
            .to_owned()
    }

    fn open_lake_conn(catalog: &Url, data: &Url) -> Connection {
        let conn = open_verification_connection();
        let catalog_attach_target =
            catalog_conninfo_from_url(catalog).expect("invalid catalog url");
        conn.execute_batch(&format!(
            "{} ATTACH {} AS {} (DATA_PATH {});",
            ducklake_load_sql(),
            quote_literal(&format!("ducklake:{catalog_attach_target}")),
            quote_identifier(LAKE_CATALOG),
            quote_literal(data.as_str()),
        ))
        .expect("failed to attach DuckLake catalog");
        conn
    }

    fn lake_table_exists(conn: &Connection, table_name: &DuckLakeTableName) -> bool {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = {} AND \
                 table_schema = {} AND table_name = {}",
                quote_literal(LAKE_CATALOG),
                quote_literal(table_name.schema()),
                quote_literal(table_name.table()),
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .is_ok_and(|count| count > 0)
    }

    async fn open_lake_conn_when_table_visible(
        catalog: &Url,
        data: &Url,
        table_name: &DuckLakeTableName,
    ) -> Connection {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let conn = open_lake_conn(catalog, data);
            if lake_table_exists(&conn, table_name) {
                return conn;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for DuckLake table `{table_name}` to become visible",
            );
            drop(conn);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[test]
    fn table_name_to_ducklake_table_name_preserves_schema() {
        assert_eq!(
            table_name_to_ducklake_table_name(&TableName {
                schema: "public".to_owned(),
                name: "orders".to_owned(),
            })
            .unwrap(),
            DuckLakeTableName::new("public", "orders")
        );
        assert_eq!(
            table_name_to_ducklake_table_name(&TableName {
                schema: "my_schema".to_owned(),
                name: "my_table".to_owned(),
            })
            .unwrap(),
            DuckLakeTableName::new("my_schema", "my_table")
        );
    }

    #[test]
    fn is_create_table_conflict_matches_ducklake_commit_conflict() {
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "TransactionContext Error: Failed to commit: Failed to commit DuckLake \
                 transaction. Transaction conflict - attempting to create table \"public_users\" \
                 in schema \"main\" - but this table has been created by another transaction \
                 already"
                    .to_owned(),
            ),
        );

        assert!(is_create_table_conflict(&error, "public_users"));
        assert!(!is_create_table_conflict(&error, "public_orders"));
    }

    mod postgres_backed {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn query_table_storage_metrics_reads_ducklake_metadata() {
            let dir = TempDir::new().expect("failed to create temp dir");
            let data = path_to_file_url(&dir.path().join("data"));
            let (_catalog_database, catalog) = create_catalog_database().await;
            let store = MemoryStore::new();
            let schema = make_schema(1, "public", "users");
            let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
            let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

            store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

            let destination = DuckLakeDestination::new(
                catalog.clone(),
                data.clone(),
                1,
                None,
                None,
                None,
                None,
                store,
            )
            .await
            .expect("failed to create destination");

            destination
                .write_table_rows(
                    &replicated_table_schema,
                    vec![
                        TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]),
                        TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_owned())]),
                    ],
                )
                .await
                .expect("failed to write rows");

            let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
            let metadata_schema = resolve_ducklake_metadata_schema_blocking(&conn)
                .expect("failed to resolve metadata schema");
            let metadata_pg_pool =
                build_ducklake_metadata_pg_pool(&catalog).expect("failed to create metadata pool");
            let _rows_flushed =
                flush_table_inlined_data(&conn, table_name.schema(), table_name.table())
                    .expect("failed to materialize inlined rows for storage metrics test");
            let deadline = Instant::now() + Duration::from_secs(10);
            let metrics = loop {
                let metrics =
                    query_table_storage_metrics(&metadata_pg_pool, &metadata_schema, &table_name)
                        .await
                        .expect("failed to query storage metrics");
                if metrics.active_data_files >= 1 {
                    break metrics;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for storage metrics after materialization"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };

            assert!(metrics.active_data_files >= 1);
            assert!(metrics.active_data_bytes > 0);
            assert_eq!(metrics.active_delete_files, 0);
            assert_eq!(metrics.deleted_rows, 0);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn query_catalog_maintenance_metrics_reports_active_data_files_total() {
            let dir = TempDir::new().expect("failed to create temp dir");
            let data = path_to_file_url(&dir.path().join("data"));
            let (_catalog_database, catalog) = create_catalog_database().await;
            let store = MemoryStore::new();
            let schema = make_schema(1, "public", "users");
            let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
            let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

            store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

            let destination = DuckLakeDestination::new(
                catalog.clone(),
                data.clone(),
                1,
                None,
                None,
                None,
                None,
                store,
            )
            .await
            .expect("failed to create destination");

            destination
                .write_table_rows(
                    &replicated_table_schema,
                    vec![
                        TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]),
                        TableRow::new(vec![Cell::I32(2), Cell::String("bob".to_owned())]),
                    ],
                )
                .await
                .expect("failed to write rows");

            let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
            let metadata_schema = resolve_ducklake_metadata_schema_blocking(&conn)
                .expect("failed to resolve metadata schema");
            let metadata_pg_pool =
                build_ducklake_metadata_pg_pool(&catalog).expect("failed to create metadata pool");
            let _rows_flushed =
                flush_table_inlined_data(&conn, table_name.schema(), table_name.table())
                    .expect("failed to materialize inlined rows for catalog metrics test");
            let deadline = Instant::now() + Duration::from_secs(10);
            let metrics = loop {
                let metrics =
                    query_catalog_maintenance_metrics(&metadata_pg_pool, &metadata_schema)
                        .await
                        .expect("failed to query catalog maintenance metrics");
                if metrics.active_data_files_total >= 1 {
                    break metrics;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for active data files total after materialization"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };

            assert!(metrics.active_data_files_total >= 1);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn query_catalog_maintenance_metrics_reads_ducklake_metadata() {
            let dir = TempDir::new().expect("failed to create temp dir");
            let data = path_to_file_url(&dir.path().join("data"));
            let (_catalog_database, catalog) = create_catalog_database().await;
            let store = MemoryStore::new();
            let schema = make_schema(1, "public", "users");
            let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(schema.clone()));
            let table_name = table_name_to_ducklake_table_name(&schema.name).unwrap();

            store.store_table_schema(schema.clone()).await.expect("failed to seed schema");

            let destination = DuckLakeDestination::new(
                catalog.clone(),
                data.clone(),
                1,
                None,
                None,
                None,
                None,
                store,
            )
            .await
            .expect("failed to create destination");

            destination
                .write_table_rows(
                    &replicated_table_schema,
                    vec![TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())])],
                )
                .await
                .expect("failed to write rows");
            destination
                .truncate_table(&replicated_table_schema)
                .await
                .expect("failed to truncate table");

            destination.shutdown().await.expect("failed to shutdown destination");
            drop(destination);

            let conn = open_lake_conn_when_table_visible(&catalog, &data, &table_name).await;
            let metadata_schema = resolve_ducklake_metadata_schema_blocking(&conn)
                .expect("failed to resolve metadata schema");
            let metadata_pg_pool =
                build_ducklake_metadata_pg_pool(&catalog).expect("failed to create metadata pool");
            let metrics = query_catalog_maintenance_metrics(&metadata_pg_pool, &metadata_schema)
                .await
                .expect("failed to query catalog maintenance metrics");

            assert!(metrics.active_data_files_total >= 0);
            assert!(metrics.snapshots_total >= 1);
            assert!(metrics.oldest_snapshot_age_seconds >= 0);
            assert!(metrics.files_scheduled_for_deletion_total >= 0);
            assert!(metrics.files_scheduled_for_deletion_bytes >= 0);
            assert!(metrics.oldest_scheduled_deletion_age_seconds >= 0);
        }
    }
}
