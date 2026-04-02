//! Table batches are the atomic per-table write units used by DuckLake writes.
//! Copy, mutation, and truncate inputs are normalized into deterministic
//! batches so each attempt can replay the same SQL and marker writes.
//! Batch ids persisted in the applied-marker table make retries idempotent
//! after ambiguous failures, while bounded batch sizes preserve table-local
//! ordering without letting one transaction grow unbounded.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Cell, TableRow, TableSchema};
use metrics::{counter, histogram};
#[cfg(feature = "test-utils")]
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
use rand::Rng;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_postgres::types::PgLsn;
use tracing::{debug, trace, warn};

use crate::ducklake::client::{
    DuckDbBlockingOperationKind, DuckLakeConnectionManager, format_query_error_detail,
    run_duckdb_blocking,
};
use crate::ducklake::core::is_create_table_conflict;
use crate::ducklake::encoding::{
    PreparedRows, cell_to_sql_literal_ref, prepare_rows, table_row_to_sql_literal_ref,
};
use crate::ducklake::metrics::{
    BATCH_KIND_LABEL, DELETE_ORIGIN_LABEL, ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
    ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS, ETL_DUCKLAKE_DELETE_PREDICATES,
    ETL_DUCKLAKE_FAILED_BATCHES_TOTAL, ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
    ETL_DUCKLAKE_RETRIES_TOTAL, ETL_DUCKLAKE_UPSERT_ROWS, PREPARED_ROWS_KIND_LABEL,
    RETRY_SCOPE_LABEL, SUB_BATCH_KIND_LABEL,
};
use crate::ducklake::{DuckLakeTableName, LAKE_CATALOG};
use crate::retry::{RetryAttempt, RetryDecision, RetryPolicy, retry_with_backoff};

/// Maximum number of rows per SQL `INSERT ... VALUES` batch when nested values
/// force the staging path to bypass DuckDB's appender API.
const SQL_INSERT_BATCH_SIZE: usize = 128;
/// Maximum number of primary-key predicates per SQL `DELETE` batch.
///
/// Keep this small so each delete statement remains cheap while still avoiding
/// one round-trip per deleted row.
const SQL_DELETE_BATCH_SIZE: usize = 16;
/// Maximum number of ordered CDC mutations grouped into one atomic DuckLake
/// transaction.
///
/// Keeping mixed insert/delete/update streams in the same batch improves
/// insert throughput on interleaved workloads while still capping transaction
/// lifetime for DuckLake conflict handling.
const CDC_MUTATION_BATCH_SIZE: usize = 8;
/// ETL-managed marker table storing per-table applied CDC batches.
const APPLIED_BATCHES_TABLE: &str = "__etl_applied_table_batches";
/// Inline small marker-table writes in the DuckLake metadata catalog instead of
/// creating Parquet files for this metadata-like table.
const APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// Maximum number of times a failed write attempt is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 10;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 2_000;
/// Minimum retry delay for transient delete-file visibility failures.
const TRANSIENT_DELETE_FILE_RETRY_DELAY_MS: u64 = 5_000;

/// Event-level table mutations that must be applied in order.
pub(super) enum TableMutation {
    Insert(TableRow),
    Delete(TableRow),
    Update {
        delete_row: TableRow,
        upsert_row: TableRow,
    },
    Replace(TableRow),
}

/// Prepared table mutations ready for execution and retries.
enum PreparedTableMutation {
    Upsert(PreparedRows),
    Delete {
        // For WHERE clause predicates used in DELETE statements.
        predicates: Vec<String>,
        // To know if it's coming from an update or delete operation.
        origin: &'static str,
    },
}

/// Event-level table mutation annotated with source LSNs for idempotent replay.
pub(super) struct TrackedTableMutation {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    mutation: TableMutation,
}

impl TrackedTableMutation {
    /// Creates one tracked mutation preserved for retry-safe replay.
    pub(super) fn new(start_lsn: PgLsn, commit_lsn: PgLsn, mutation: TableMutation) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            mutation,
        }
    }
}

/// Truncate event metadata preserved for idempotent replay.
#[derive(Clone, Copy)]
pub(super) struct TrackedTruncateEvent {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    options: i8,
}

impl TrackedTruncateEvent {
    /// Creates one tracked truncate event preserved for retry-safe replay.
    pub(super) fn new(start_lsn: PgLsn, commit_lsn: PgLsn, options: i8) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            options,
        }
    }
}

/// Stable hash used to derive per-table batch identifiers.
struct BatchIdHasher(u64);

impl BatchIdHasher {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Default for BatchIdHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for BatchIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

/// Atomic DuckLake batch kinds persisted in the replay marker table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckLakeTableBatchKind {
    Copy,
    Mutation,
    Truncate,
}

impl DuckLakeTableBatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Mutation => "mutation",
            Self::Truncate => "truncate",
        }
    }
}

/// Deterministic identity for one table batch.
struct DuckLakeBatchIdentity {
    batch_id: String,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
}

/// Prepared per-table work executed atomically in one DuckLake transaction.
enum PreparedDuckLakeTableBatchAction {
    Mutation(Vec<PreparedTableMutation>),
    Truncate,
}

/// Prepared atomic DuckLake table batch with replay metadata.
pub(super) struct PreparedDuckLakeTableBatch {
    table_name: DuckLakeTableName,
    batch_id: String,
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    action: PreparedDuckLakeTableBatchAction,
}

impl PreparedDuckLakeTableBatch {
    /// Returns the destination table this batch targets.
    pub(super) fn table_name(&self) -> &str {
        &self.table_name
    }
}

/// Ensures the ETL-managed replay marker table exists.
pub(super) async fn ensure_applied_batches_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    applied_batches_table_created: Arc<AtomicBool>,
) -> EtlResult<()> {
    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let _table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake table creation semaphore closed"
        )
    })?;

    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}" (
             table_name VARCHAR NOT NULL,
             batch_id VARCHAR NOT NULL,
             batch_kind VARCHAR NOT NULL,
             first_start_lsn UBIGINT,
             last_commit_lsn UBIGINT,
             applied_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&applied_batches_table_created);
    let table_name = APPLIED_BATCHES_TABLE.to_string();

    run_duckdb_blocking(
        pool,
        blocking_slots,
        DuckDbBlockingOperationKind::Foreground,
        move |conn| -> EtlResult<()> {
            match conn.execute_batch(&ddl) {
                Ok(()) => {}
                Err(error) if is_create_table_conflict(&error, &table_name) => {}
                Err(error) => {
                    return Err(etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake CREATE TABLE failed",
                        format_query_error_detail(&ddl, &error),
                        source: error
                    ));
                }
            }

            let set_option_sql = format!(
                "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
                APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT,
                quote_literal(APPLIED_BATCHES_TABLE),
            );
            conn.execute_batch(&set_option_sql).map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake set_option failed",
                    format_query_error_detail(&set_option_sql, &error),
                    source: error
                )
            })?;

            created.store(true, Ordering::Relaxed);
            Ok(())
        },
    )
    .await
}

/// Applies all prepared atomic batches for one table, reusing one DuckDB
/// connection per attempt and skipping already committed segments by marker.
pub(super) async fn apply_table_batches_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batches: Vec<PreparedDuckLakeTableBatch>,
) -> EtlResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let batch_count = batches.len();
    let batches = Arc::new(batches);
    let table_name = batches[0].table_name.clone();

    retry_with_backoff(
        RetryPolicy {
            max_retries: MAX_COMMIT_RETRIES,
            initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_delay: Duration::from_millis(MAX_RETRY_DELAY_MS),
        },
        |_| RetryDecision::Retry,
        jitter_ducklake_retry_delay,
        |attempt: RetryAttempt<'_, etl::error::EtlError>| {
            counter!(
                ETL_DUCKLAKE_RETRIES_TOTAL,
                BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
                RETRY_SCOPE_LABEL => "table_sequence",
            )
            .increment(1);
            warn!(
                attempt = attempt.retry_index,
                max = attempt.max_retries,
                table = %table_name,
                batch_count,
                error = ?attempt.error,
                "ducklake table batch sequence failed, retrying"
            );
        },
        move || {
            let attempt_batches = Arc::clone(&batches);
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            async move {
                run_duckdb_blocking(
                    pool,
                    blocking_slots,
                    DuckDbBlockingOperationKind::Foreground,
                    move |conn| {
                        apply_table_batches(conn, attempt_batches.as_ref())?;
                        Ok(())
                    },
                )
                .await
            }
        },
    )
    .await
    .map_err(|failure| {
        counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            BATCH_KIND_LABEL => DuckLakeTableBatchKind::Mutation.as_str(),
            RETRY_SCOPE_LABEL => "table_sequence",
        )
        .increment(1);
        etl_error!(
            ErrorKind::DestinationAtomicBatchRetryable,
            "DuckLake atomic table batch sequence failed after retries",
            format!("table={table_name}, batch_count={batch_count}"),
            source: failure.last_error
        )
    })
}

/// Applies one atomic per-table batch and retries on failure.
pub(super) async fn apply_table_batch_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let table_name = batch.table_name.clone();
    let batch_id = batch.batch_id.clone();
    let batch_kind = batch.batch_kind;
    let batch = Arc::new(batch);

    retry_with_backoff(
        RetryPolicy {
            max_retries: MAX_COMMIT_RETRIES,
            initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
            max_delay: Duration::from_millis(
                MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
            ),
        },
        |_| RetryDecision::Retry,
        jitter_ducklake_retry_delay,
        |attempt: RetryAttempt<'_, etl::error::EtlError>| {
            counter!(
                ETL_DUCKLAKE_RETRIES_TOTAL,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                RETRY_SCOPE_LABEL => "single_batch",
            )
            .increment(1);
            warn!(
                attempt = attempt.retry_index,
                max = attempt.max_retries,
                table = %table_name,
                batch_id = %batch_id,
                error = ?attempt.error,
                "ducklake table mutation attempt failed, retrying"
            );
        },
        move || {
            let attempt_batch = Arc::clone(&batch);
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            async move {
                run_duckdb_blocking(
                    pool,
                    blocking_slots,
                    DuckDbBlockingOperationKind::Foreground,
                    move |conn| {
                        if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                            counter!(
                                ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
                                BATCH_KIND_LABEL => batch_kind.as_str(),
                            )
                            .increment(1);
                            debug!(
                                table = %attempt_batch.table_name,
                                batch_id = %attempt_batch.batch_id,
                                batch_kind = batch_kind.as_str(),
                                "ducklake table batch already committed, skipping replay"
                            );

                            return Ok(());
                        }

                        apply_table_batch(conn, attempt_batch.as_ref())?;

                        Ok(())
                    },
                )
                .await
            }
        },
    )
    .await
    .map_err(|failure| {
        counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            BATCH_KIND_LABEL => batch_kind.as_str(),
            RETRY_SCOPE_LABEL => "single_batch",
        )
        .increment(1);
        etl_error!(
            ErrorKind::DestinationAtomicBatchRetryable,
            "DuckLake atomic table batch failed after retries",
            format!(
                "table={table_name}, batch_id={batch_id}, batch_kind={}",
                batch_kind.as_str()
            ),
            source: failure.last_error
        )
    })
}

/// Prepares ordered atomic batches for one table's CDC mutations.
///
/// Mutations stay in source order and are split only at the batch-size cap so
/// mixed CDC streams can commit larger insert groups without breaking atomic
/// ordering.
pub(super) fn prepare_mutation_table_batches(
    table_schema: &TableSchema,
    table_name: DuckLakeTableName,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<Vec<PreparedDuckLakeTableBatch>> {
    let mut prepared_batches = Vec::new();
    let mut pending_mutations = Vec::new();

    for tracked_mutation in tracked_mutations {
        pending_mutations.push(tracked_mutation);
        if pending_mutations.len() >= CDC_MUTATION_BATCH_SIZE {
            push_prepared_mutation_batch(
                &mut prepared_batches,
                table_schema,
                &table_name,
                std::mem::take(&mut pending_mutations),
            )?;
        }
    }

    push_prepared_mutation_batch(
        &mut prepared_batches,
        table_schema,
        &table_name,
        pending_mutations,
    )?;

    Ok(prepared_batches)
}

/// Prepares one retry-safe atomic batch for a table-copy row chunk.
pub(super) fn prepare_copy_table_batch(
    table_schema: &TableSchema,
    table_name: DuckLakeTableName,
    table_rows: Vec<TableRow>,
) -> EtlResult<PreparedDuckLakeTableBatch> {
    let identity = build_copy_batch_identity(&table_name, table_schema, &table_rows)?;
    Ok(PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Copy,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        action: PreparedDuckLakeTableBatchAction::Mutation(vec![PreparedTableMutation::Upsert(
            prepare_rows(table_rows),
        )]),
    })
}

/// Prepares the ordered atomic batch for one table's truncate events.
pub(super) fn prepare_truncate_table_batch(
    table_name: DuckLakeTableName,
    tracked_truncates: Vec<TrackedTruncateEvent>,
) -> PreparedDuckLakeTableBatch {
    let identity = build_truncate_batch_identity(&table_name, &tracked_truncates);
    PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Truncate,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        action: PreparedDuckLakeTableBatchAction::Truncate,
    }
}

/// Deletes persisted markers for one table and batch kind.
pub(super) fn clear_applied_batch_markers_for_kind(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
) -> EtlResult<()> {
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {} AND batch_kind = {};"#,
        quote_literal(table_name),
        quote_literal(batch_kind.as_str())
    );
    conn.execute_batch(&sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker delete failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    Ok(())
}

/// Applies jitter to one DuckLake retry delay.
fn jitter_ducklake_retry_delay(base_delay: Duration) -> Duration {
    let jitter_ratio = rand::rng().random_range(0.5..=1.5_f64);
    base_delay.mul_f64(jitter_ratio)
}

/// Applies all prepared atomic batches for one table on the same connection.
fn apply_table_batches(
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
) -> EtlResult<()> {
    for batch in batches {
        // This is useful in case it fails in the middle of the process.
        // As we have a batch id we can check if it was already committed or not and only replay if not.
        if applied_batch_marker_exists(conn, batch)? {
            counter!(
                ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
            )
            .increment(1);
            debug!(
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                "ducklake table batch already committed, skipping replay"
            );
            continue;
        }

        apply_table_batch(conn, batch).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake atomic table batch failed",
                format!(
                    "table={}, batch_id={}, batch_kind={}",
                    batch.table_name,
                    batch.batch_id,
                    batch.batch_kind.as_str()
                ),
                source: error
            )
        })?;
    }

    Ok(())
}

/// Builds one prepared atomic batch from an ordered slice of tracked mutations.
fn push_prepared_mutation_batch(
    prepared_batches: &mut Vec<PreparedDuckLakeTableBatch>,
    table_schema: &TableSchema,
    table_name: &str,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<()> {
    if tracked_mutations.is_empty() {
        return Ok(());
    }

    let identity = build_mutation_batch_identity(table_name, table_schema, &tracked_mutations)?;
    let mutations = tracked_mutations
        .into_iter()
        .map(|tracked| tracked.mutation)
        .collect();

    prepared_batches.push(PreparedDuckLakeTableBatch {
        table_name: table_name.to_string(),
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Mutation,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        action: PreparedDuckLakeTableBatchAction::Mutation(prepare_table_mutations(
            table_schema,
            mutations,
        )?),
    });

    Ok(())
}

/// Groups ordered row mutations into retryable DuckDB operations.
fn prepare_table_mutations(
    table_schema: &TableSchema,
    mutations: Vec<TableMutation>,
) -> EtlResult<Vec<PreparedTableMutation>> {
    let mut prepared_mutations = Vec::new();
    let mut upsert_rows = Vec::new();
    let mut delete_predicates = Vec::new();

    for mutation in mutations {
        match mutation {
            TableMutation::Insert(row) => {
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }
                upsert_rows.push(row);
            }
            TableMutation::Delete(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                delete_predicates.push(delete_predicate_from_row(table_schema, &row)?);
            }
            TableMutation::Update {
                delete_row,
                upsert_row,
            } => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }

                prepared_mutations.push(PreparedTableMutation::Delete {
                    predicates: vec![delete_predicate_from_row(table_schema, &delete_row)?],
                    origin: "update",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(vec![
                    upsert_row,
                ])));
            }
            TableMutation::Replace(row) => {
                if !upsert_rows.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(
                        std::mem::take(&mut upsert_rows),
                    )));
                }
                if !delete_predicates.is_empty() {
                    prepared_mutations.push(PreparedTableMutation::Delete {
                        predicates: std::mem::take(&mut delete_predicates),
                        origin: "delete",
                    });
                }

                prepared_mutations.push(PreparedTableMutation::Delete {
                    predicates: vec![delete_predicate_from_row(table_schema, &row)?],
                    origin: "replace",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(vec![row])));
            }
        }
    }

    if !upsert_rows.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Upsert(prepare_rows(upsert_rows)));
    }
    if !delete_predicates.is_empty() {
        prepared_mutations.push(PreparedTableMutation::Delete {
            predicates: delete_predicates,
            origin: "delete",
        });
    }

    Ok(prepared_mutations)
}

/// Builds a `WHERE` clause from the primary-key values stored in `row`.
fn delete_predicate_from_row(table_schema: &TableSchema, row: &TableRow) -> EtlResult<String> {
    if !table_schema.has_primary_keys() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake delete requires a primary key",
            format!("Table '{}' has no primary key columns", table_schema.name)
        ));
    }

    if row.values().len() != table_schema.column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake row shape does not match schema",
            format!(
                "Expected {} values for table '{}', got {}",
                table_schema.column_schemas.len(),
                table_schema.name,
                row.values().len()
            )
        ));
    }

    let mut predicates = Vec::new();

    for (column_schema, value) in table_schema
        .column_schemas
        .iter()
        .zip(row.values())
        .filter(|(column_schema, _)| column_schema.primary)
    {
        let quoted_column = quote_identifier(&column_schema.name).into_owned();
        let predicate = match value {
            Cell::Null => format!("{quoted_column} IS NULL"),
            _ => format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)),
        };
        predicates.push(predicate);
    }

    Ok(predicates.join(" AND "))
}

/// Builds a deterministic identity for one ordered mutation batch.
fn build_mutation_batch_identity(
    table_name: &str,
    table_schema: &TableSchema,
    tracked_mutations: &[TrackedTableMutation],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "mutation".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for tracked_mutation in tracked_mutations {
        u64::from(tracked_mutation.start_lsn).hash(&mut hasher);
        u64::from(tracked_mutation.commit_lsn).hash(&mut hasher);

        match &tracked_mutation.mutation {
            TableMutation::Insert(row) => {
                "insert".hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
            TableMutation::Delete(row) => {
                "delete".hash(&mut hasher);
                delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
            }
            TableMutation::Update {
                delete_row,
                upsert_row,
            } => {
                "update".hash(&mut hasher);
                delete_predicate_from_row(table_schema, delete_row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, upsert_row);
            }
            TableMutation::Replace(row) => {
                "replace".hash(&mut hasher);
                delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
        }
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Mutation,
        tracked_mutations
            .first()
            .map(|tracked_mutation| tracked_mutation.start_lsn),
        tracked_mutations
            .last()
            .map(|tracked_mutation| tracked_mutation.commit_lsn),
        hasher.finish(),
    ))
}

/// Builds a deterministic identity for one ordered table-copy batch.
fn build_copy_batch_identity(
    table_name: &str,
    table_schema: &TableSchema,
    table_rows: &[TableRow],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "copy".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for row in table_rows {
        delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
        hash_table_row_ref(&mut hasher, row);
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Copy,
        None,
        None,
        hasher.finish(),
    ))
}

/// Builds a deterministic identity for one ordered truncate batch.
fn build_truncate_batch_identity(
    table_name: &str,
    tracked_truncates: &[TrackedTruncateEvent],
) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "truncate".hash(&mut hasher);
    table_name.hash(&mut hasher);

    for tracked_truncate in tracked_truncates {
        u64::from(tracked_truncate.start_lsn).hash(&mut hasher);
        u64::from(tracked_truncate.commit_lsn).hash(&mut hasher);
        tracked_truncate.options.hash(&mut hasher);
    }

    build_batch_identity(
        DuckLakeTableBatchKind::Truncate,
        tracked_truncates
            .first()
            .map(|tracked_truncate| tracked_truncate.start_lsn),
        tracked_truncates
            .last()
            .map(|tracked_truncate| tracked_truncate.commit_lsn),
        hasher.finish(),
    )
}

/// Builds the final persisted batch identity string.
fn build_batch_identity(
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    fingerprint: u64,
) -> DuckLakeBatchIdentity {
    let first_start_lsn_u64 = first_start_lsn.map(u64::from).unwrap_or_default();
    let last_commit_lsn_u64 = last_commit_lsn.map(u64::from).unwrap_or_default();

    DuckLakeBatchIdentity {
        batch_id: format!(
            "{}:{first_start_lsn_u64:016x}:{last_commit_lsn_u64:016x}:{fingerprint:016x}",
            batch_kind.as_str()
        ),
        first_start_lsn,
        last_commit_lsn,
    }
}

/// Hashes a row using its SQL literal form so retries are independent of appender encoding.
fn hash_table_row_ref(hasher: &mut BatchIdHasher, row: &TableRow) {
    table_row_to_sql_literal_ref(row).hash(hasher);
}

/// Returns whether the atomic batch marker already exists.
fn applied_batch_marker_exists(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<bool> {
    let sql = format!(
        r#"SELECT 1 FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {} AND batch_id = {} LIMIT 1;"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.batch_id)
    );
    let mut statement = conn.prepare(&sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query prepare failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    let mut rows = statement.query([]).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query row fetch failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })
}

/// Inserts the atomic batch marker inside the open DuckLake transaction.
fn insert_applied_batch_marker(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         (table_name, batch_id, batch_kind, first_start_lsn, last_commit_lsn, applied_at) VALUES ({}, {}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.batch_id),
        quote_literal(batch.batch_kind.as_str()),
        optional_lsn_to_sql_literal(batch.first_start_lsn),
        optional_lsn_to_sql_literal(batch.last_commit_lsn),
    );
    conn.execute_batch(&sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker insert failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    Ok(())
}

/// Applies one atomic per-table batch in a single DuckLake transaction.
fn apply_table_batch(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let batch_started = Instant::now();

    conn.execute_batch("BEGIN TRANSACTION").map_err(|error| {
        tracing::error!(?error, "error transaction");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: error
        )
    })?;

    let result = (|| -> EtlResult<()> {
        match &batch.action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
                for prepared_mutation in prepared_mutations {
                    apply_table_mutation(
                        conn,
                        &batch.table_name,
                        batch.batch_kind,
                        prepared_mutation,
                    )?;
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => {
                apply_truncate_batch_action(conn, &batch.table_name)?;
            }
        }

        insert_applied_batch_marker(conn, batch)?;
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|error| {
                tracing::error!(?error, "error commit");
                etl_error!(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", source: error)
            })?;
            histogram!(
                ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(batch_started.elapsed().as_secs_f64());
            histogram!(
                ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                SUB_BATCH_KIND_LABEL => batch_log_kind(batch),
            )
            .record(prepared_mutation_count(batch) as f64);
            trace!(
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                first_start_lsn = ?batch.first_start_lsn,
                last_commit_lsn = ?batch.last_commit_lsn,
                sub_batch_kind = batch_log_kind(batch),
                insert_sub_batch_rows = apply_sub_batch_rows(batch),
                "ducklake batch committed"
            );

            #[cfg(feature = "test-utils")]
            maybe_fail_after_committed_batch_for_tests(batch.batch_kind, &batch.table_name)?;

            Ok(())
        }
        Err(error) => {
            let rollback = conn.execute_batch("ROLLBACK");
            if let Err(rollback) = rollback {
                tracing::error!(?rollback, "error rollback");
            }
            Err(error)
        }
    }
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(conn: &duckdb::Connection, table_name: &str) -> EtlResult<()> {
    let sql = format!(r#"DELETE FROM {LAKE_CATALOG}."{table_name}";"#);
    conn.execute_batch(&sql).map_err(|error| {
        tracing::error!(?error, "error DELETE");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake DELETE failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    Ok(())
}

/// Formats an optional LSN for marker-table inserts.
fn optional_lsn_to_sql_literal(lsn: Option<PgLsn>) -> String {
    lsn.map(|value| u64::from(value).to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

/// Applies one prepared table mutation inside an open transaction.
fn apply_table_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    batch_kind: DuckLakeTableBatchKind,
    prepared_mutation: &PreparedTableMutation,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Upsert(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => prepared_rows_kind(prepared_rows),
            )
            .record(prepared_rows_count(prepared_rows) as f64);
            apply_upsert_mutation(conn, table_name, prepared_rows)
        }
        PreparedTableMutation::Delete { predicates, origin } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch_kind.as_str(),
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(predicates.len() as f64);
            apply_delete_mutation(conn, table_name, predicates.as_slice())
        }
    }
}

/// Applies one upsert batch inside an open DuckLake transaction.
fn apply_upsert_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    prepared_rows: &PreparedRows,
) -> EtlResult<()> {
    let row_count = match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::SqlLiterals(values) => values.len(),
    };

    if row_count == 0 {
        return Ok(());
    }

    // Staging table lives entirely in-memory (regular DuckDB, not DuckLake).
    // TEMP tables are connection-local so there is no cross-connection conflict
    // even when multiple DuckDB connections run concurrently.
    let staging = format!("__staging_{table_name}");

    // Mirror the DuckLake target schema without copying any data.
    conn.execute_batch(&format!(
        r#"CREATE OR REPLACE TEMP TABLE {staging:?} AS
         SELECT * FROM {LAKE_CATALOG}."{table_name}" LIMIT 0;"#
    ))
    .map_err(|error| {
        tracing::error!(?error, "error CREATE TEMP TABLE");

        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake staging table creation failed",
            source: error
        )
    })?;

    let load_result = match prepared_rows {
        PreparedRows::Appender(all_values) => (|| {
            let mut appender = conn.appender(&staging).map_err(|error| {
                tracing::error!(?error, "error appender");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging appender creation failed",
                    source: error
                )
            })?;
            for values in all_values {
                appender
                    .append_row(duckdb::appender_params_from_iter(values))
                    .map_err(|error| {
                        tracing::error!(?error, "error append row");
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake staging append_row failed",
                            source: error
                        )
                    })?;
            }
            appender.flush().map_err(|error| {
                tracing::error!(?error, "error flush");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging appender flush failed",
                    source: error
                )
            })
        })(),
        PreparedRows::SqlLiterals(row_literals) => {
            insert_rows_into_staging_with_sql(conn, &staging, row_literals.as_slice())
        }
    };

    if let Err(error) = load_result {
        tracing::error!(?error, "error LOAD RESULT");

        let drop_result = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
        if let Err(error) = drop_result {
            tracing::error!(?error, "error drop table staging");
        }

        return Err(error);
    }

    let result = conn
        .execute_batch(&format!(
            r#"INSERT INTO {LAKE_CATALOG}."{table_name}" SELECT * FROM {staging:?};"#
        ))
        .map_err(|error| {
            tracing::error!(?error, "error INSERT INTO");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                source: error
            )
        });

    let drop_result = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
    if let Err(error) = drop_result {
        tracing::error!(?error, "error drop table staging");
    }
    if let Err(error) = &result {
        tracing::error!(?error, "error INSERT result");
    }

    result
}

/// Applies one delete batch inside an open DuckLake transaction.
fn apply_delete_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    predicates: &[String],
) -> EtlResult<()> {
    if predicates.is_empty() {
        return Ok(());
    }

    for chunk in predicates.chunks(SQL_DELETE_BATCH_SIZE) {
        let where_clause = chunk
            .iter()
            .map(|predicate| format!("({predicate})"))
            .collect::<Vec<_>>()
            .join(" OR ");

        let sql_query =
            format!(r#"DELETE FROM {LAKE_CATALOG}."{table_name}" WHERE {where_clause};"#);
        conn.execute_batch(&sql_query).map_err(|error| {
            tracing::error!(?error, "error DELETE FROM");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake DELETE failed",
                source: error
            )
        })?;
    }

    Ok(())
}

/// Returns the number of values carried by a prepared row payload.
fn prepared_rows_count(prepared_rows: &PreparedRows) -> usize {
    match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::SqlLiterals(values) => values.len(),
    }
}

/// Returns the encoding strategy used by a prepared row payload.
fn prepared_rows_kind(prepared_rows: &PreparedRows) -> &'static str {
    match prepared_rows {
        PreparedRows::Appender(_) => "appender",
        PreparedRows::SqlLiterals(_) => "sql_literals",
    }
}

/// Returns the number of prepared mutation statements in one atomic batch.
fn prepared_mutation_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 1,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations.len(),
    }
}

/// Returns the insert row count when the batch is a pure insert sub-batch.
fn apply_sub_batch_rows(batch: &PreparedDuckLakeTableBatch) -> Option<usize> {
    let PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) = &batch.action else {
        return None;
    };

    if prepared_mutations.len() != 1 {
        return None;
    }

    match &prepared_mutations[0] {
        PreparedTableMutation::Upsert(prepared_rows) => Some(match prepared_rows {
            PreparedRows::Appender(values) => values.len(),
            PreparedRows::SqlLiterals(values) => values.len(),
        }),
        PreparedTableMutation::Delete { .. } => None,
    }
}

/// Classifies a prepared batch for concise INFO logging.
fn batch_log_kind(batch: &PreparedDuckLakeTableBatch) -> &'static str {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => "truncate",
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
            match prepared_mutations.as_slice() {
                [PreparedTableMutation::Upsert(_)] => "insert",
                [PreparedTableMutation::Delete { origin, .. }] => origin,
                [
                    PreparedTableMutation::Delete { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                _ => "mutation",
            }
        }
    }
}

/// Inserts rows into the local staging table using SQL literals.
fn insert_rows_into_staging_with_sql(
    conn: &duckdb::Connection,
    staging: &str,
    row_literals: &[String],
) -> EtlResult<()> {
    for chunk in row_literals.chunks(SQL_INSERT_BATCH_SIZE) {
        conn.execute_batch(&format!(
            "INSERT INTO {staging:?} VALUES {};",
            chunk.join(", ")
        ))
        .map_err(|error| {
            tracing::error!(?error, "error insert_rows_into_staging_with_sql");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging row insert failed",
                source: error
            )
        })?;
    }

    Ok(())
}

#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));

/// Arms a test hook that injects one post-commit failure for the next atomic batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_atomic_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_string());
}

/// Arms a test hook that injects one post-commit failure for the next copy batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_copy_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_string());
}

/// Clears DuckLake destination test hooks.
#[cfg(feature = "test-utils")]
pub fn reset_ducklake_test_hooks() {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = None;
}

/// Injects a synthetic failure after commit so retries must rely on the correct marker path.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_committed_batch_for_tests(
    batch_kind: DuckLakeTableBatchKind,
    table_name: &str,
) -> EtlResult<()> {
    match batch_kind {
        DuckLakeTableBatchKind::Copy => maybe_fail_after_copy_batch_commit_for_tests(table_name),
        DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate => {
            maybe_fail_after_atomic_batch_commit_for_tests(table_name)
        }
    }
}

/// Injects a synthetic failure after commit so retries must rely on the marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_atomic_batch_commit_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "ducklake test hook injected post-commit failure"
        ));
    }

    Ok(())
}

/// Injects a synthetic failure after commit so copy retries must rely on the marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_copy_batch_commit_for_tests(table_name: &str) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "ducklake test hook injected copy post-commit failure"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use etl::types::{ColumnSchema, TableId, TableName, Type as PgType};

    fn make_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        )
    }

    #[test]
    fn test_delete_predicate_from_row_uses_only_primary_key_columns() {
        let table_schema = TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_string(), "users".to_string()),
            vec![
                ColumnSchema::new("tenant_id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
                ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
            ],
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::I32(42),
            Cell::String("alice".to_string()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&table_schema, &row).unwrap(),
            "tenant_id = 7 AND id = 42"
        );
    }

    #[test]
    fn test_prepare_table_mutations_replace_emits_delete_then_upsert() {
        let table_schema = make_schema();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_string())]);

        let prepared =
            prepare_table_mutations(&table_schema, vec![TableMutation::Replace(row)]).unwrap();

        assert_eq!(prepared.len(), 2);
        match &prepared[0] {
            PreparedTableMutation::Delete { predicates, origin } => {
                assert_eq!(predicates, &vec!["id = 1".to_string()]);
                assert_eq!(origin, &"replace");
            }
            PreparedTableMutation::Upsert(_) => panic!("expected delete first"),
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Delete { .. } => panic!("expected upsert second"),
        }
    }

    #[test]
    fn test_prepare_table_mutations_update_emits_delete_then_upsert() {
        let table_schema = make_schema();
        let prepared = prepare_table_mutations(
            &table_schema,
            vec![TableMutation::Update {
                delete_row: TableRow::new(vec![Cell::I32(1), Cell::String("before".to_string())]),
                upsert_row: TableRow::new(vec![Cell::I32(1), Cell::String("after".to_string())]),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 2);
        match &prepared[0] {
            PreparedTableMutation::Delete { predicates, origin } => {
                assert_eq!(predicates, &vec!["id = 1".to_string()]);
                assert_eq!(origin, &"update");
            }
            PreparedTableMutation::Upsert(_) => panic!("expected delete first"),
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Delete { .. } => panic!("expected upsert second"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_insert_only_uses_single_upsert_operation() {
        let table_schema = make_schema();
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(10),
                    PgLsn::from(20),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(10),
                    PgLsn::from(20),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                ),
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].batch_kind, DuckLakeTableBatchKind::Mutation);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                        assert_eq!(rows.len(), 2);
                    }
                    PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                        panic!("expected appender payload")
                    }
                    PreparedTableMutation::Delete { .. } => panic!("expected upsert"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_split_mixed_cdc_at_delete_boundaries() {
        let table_schema = make_schema();
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_string()),
                    ])),
                ),
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 3);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(prepared[1], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[2],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_group_contiguous_deletes() {
        let table_schema = make_schema();
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                ),
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Delete { predicates, origin } => {
                        assert_eq!(origin, &"delete");
                        assert_eq!(
                            predicates,
                            &vec!["id = 1".to_string(), "id = 2".to_string()]
                        );
                    }
                    PreparedTableMutation::Upsert(_) => panic!("expected delete batch"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_group_contiguous_updates() {
        let table_schema = make_schema();
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    TableMutation::Update {
                        delete_row: TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("before-a".to_string()),
                        ]),
                        upsert_row: TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("after-a".to_string()),
                        ]),
                    },
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    TableMutation::Update {
                        delete_row: TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("before-b".to_string()),
                        ]),
                        upsert_row: TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("after-b".to_string()),
                        ]),
                    },
                ),
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 4);
                assert!(matches!(prepared[0], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[1],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(prepared[2], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[3],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_split_non_inserts_at_cap() {
        let table_schema = make_schema();
        let tracked = (0..=CDC_MUTATION_BATCH_SIZE)
            .map(|idx| {
                TrackedTableMutation::new(
                    PgLsn::from(100 + idx as u64),
                    PgLsn::from(200 + idx as u64),
                    TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(idx as i32),
                        Cell::String(format!("name-{idx}")),
                    ])),
                )
            })
            .collect();
        let batches =
            prepare_mutation_table_batches(&table_schema, "public_users".to_string(), tracked)
                .unwrap();

        assert_eq!(batches.len(), 2);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), CDC_MUTATION_BATCH_SIZE);
                }
                PreparedTableMutation::Upsert(_) => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), 1);
                }
                PreparedTableMutation::Upsert(_) => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_prepare_mutation_table_batches_isolate_update_in_its_own_atomic_batch() {
        let table_schema = make_schema();
        let batches = prepare_mutation_table_batches(
            &table_schema,
            "public_users".to_string(),
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    TableMutation::Update {
                        delete_row: TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("seed".to_string()),
                        ]),
                        upsert_row: TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("grown".to_string()),
                        ]),
                    },
                ),
                TrackedTableMutation::new(
                    PgLsn::from(120),
                    PgLsn::from(130),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_string()),
                    ])),
                ),
            ],
        )
        .unwrap();

        assert_eq!(batches.len(), 1);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 4);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(prepared[1], PreparedTableMutation::Delete { .. }));
                assert!(matches!(
                    prepared[2],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
                assert!(matches!(
                    prepared[3],
                    PreparedTableMutation::Upsert(PreparedRows::Appender(_))
                ));
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn test_build_mutation_batch_identity_is_deterministic() {
        let table_schema = make_schema();
        let tracked = vec![
            TrackedTableMutation::new(
                PgLsn::from(100),
                PgLsn::from(200),
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            ),
            TrackedTableMutation::new(
                PgLsn::from(100),
                PgLsn::from(200),
                TableMutation::Delete(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            ),
        ];

        let first = build_mutation_batch_identity("public_users", &table_schema, &tracked).unwrap();
        let second =
            build_mutation_batch_identity("public_users", &table_schema, &tracked).unwrap();

        assert_eq!(first.batch_id, second.batch_id);
        assert_eq!(first.first_start_lsn, Some(PgLsn::from(100)));
        assert_eq!(first.last_commit_lsn, Some(PgLsn::from(200)));
    }

    #[test]
    fn test_build_mutation_batch_identity_changes_with_order_and_lsn() {
        let table_schema = make_schema();
        let original = build_mutation_batch_identity(
            "public_users",
            &table_schema,
            &[
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let reordered = build_mutation_batch_identity(
            "public_users",
            &table_schema,
            &[
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let changed_lsn = build_mutation_batch_identity(
            "public_users",
            &table_schema,
            &[TrackedTableMutation::new(
                PgLsn::from(101),
                PgLsn::from(201),
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            )],
        )
        .unwrap();

        assert_ne!(original.batch_id, reordered.batch_id);
        assert_ne!(original.batch_id, changed_lsn.batch_id);
    }

    #[test]
    fn test_build_truncate_batch_identity_changes_with_lsn() {
        let first = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent::new(
                PgLsn::from(300),
                PgLsn::from(400),
                0,
            )],
        );
        let second = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent::new(
                PgLsn::from(301),
                PgLsn::from(401),
                0,
            )],
        );

        assert_ne!(first.batch_id, second.batch_id);
    }
}
