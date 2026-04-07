//! Table batches are the atomic per-table write units used by DuckLake writes.
//! Copy, mutation, and truncate inputs are normalized into deterministic
//! batches so each attempt can replay the same SQL and replay bookkeeping.
//! Copy batches persist ids in the applied-marker table, while streaming
//! mutation and truncate batches advance a per-table progress watermark.
//! Bounded batch sizes preserve table-local ordering without letting one
//! transaction grow unbounded.

#[cfg(feature = "test-utils")]
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::UInt64Array;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::record_batch_to_table_rows;
use etl::types::{
    Cell, ChangeArrowBatch, ChangeKind, EventSequenceKey, RowImage, TableArrowBatch,
    TableChangeSet, TableRow, TableSchema,
};
use metrics::{counter, histogram};
#[cfg(feature = "test-utils")]
use parking_lot::Mutex;
use pg_escape::{quote_identifier, quote_literal};
use rand::Rng;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, warn};

use crate::ducklake::arrow::{prepare_arrow_rows, project_primary_key_batch};
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
    ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS, ETL_DUCKLAKE_BATCH_SUBSTAGE_DURATION_SECONDS,
    ETL_DUCKLAKE_DELETE_PREDICATES, ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
    ETL_DUCKLAKE_MUTATION_OPERATION_DURATION_SECONDS, ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
    ETL_DUCKLAKE_RETRIES_TOTAL, ETL_DUCKLAKE_UPSERT_ROWS, OPERATION_KIND_LABEL,
    PREPARED_ROWS_KIND_LABEL, RETRY_SCOPE_LABEL, SUB_BATCH_KIND_LABEL, SUBSTAGE_LABEL,
};
use crate::ducklake::{DuckLakeTableName, LAKE_CATALOG};
use crate::retry::{RetryAttempt, RetryDecision, RetryPolicy, retry_with_backoff};

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
const CDC_MUTATION_BATCH_SIZE: usize = 16;
/// ETL-managed marker table storing per-table applied copy batches.
const APPLIED_BATCHES_TABLE: &str = "__etl_applied_table_batches";
/// Inline small marker-table writes in the DuckLake metadata catalog instead of
/// creating Parquet files for this metadata-like table.
const APPLIED_BATCHES_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// ETL-managed per-table streaming replay progress for steady-state CDC retries.
const STREAMING_PROGRESS_TABLE: &str = "__etl_streaming_progress";
/// Inline small progress-table writes in the DuckLake metadata catalog instead
/// of materializing files for this metadata-like table.
const STREAMING_PROGRESS_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// Maximum number of times a failed write attempt is retried before giving up.
const MAX_COMMIT_RETRIES: u32 = 10;
/// Initial backoff duration before the first retry.
const INITIAL_RETRY_DELAY_MS: u64 = 50;
/// Upper bound on backoff duration.
const MAX_RETRY_DELAY_MS: u64 = 2_000;
/// Minimum retry delay for transient delete-file visibility failures.
const TRANSIENT_DELETE_FILE_RETRY_DELAY_MS: u64 = 5_000;
/// Shared label value for mutation samples that do not execute a delete.
const DELETE_ORIGIN_NONE: &str = "none";

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
    Copy(PreparedRows),
    Upsert(PreparedRows),
    Delete {
        // For WHERE clause predicates used in DELETE statements.
        predicates: Vec<String>,
        // To know if it's coming from an update or delete operation.
        origin: &'static str,
    },
    DeleteKeys {
        keys: PreparedRows,
        key_columns: Vec<String>,
        join_predicate: String,
        origin: &'static str,
    },
}

/// Low-cardinality mutation kinds recorded in the latency histogram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DuckLakeMutationOperationKind {
    Insert,
    Delete,
}

impl DuckLakeMutationOperationKind {
    /// Returns the Prometheus label value for this mutation kind.
    fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Delete => "delete",
        }
    }
}

/// Internal substages tracked inside one committed DuckLake atomic batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DuckLakeBatchSubstage {
    StagingPrepare,
    StagingLoad,
    ProgressUpdate,
    CommitOnly,
}

impl DuckLakeBatchSubstage {
    /// Returns the Prometheus label value for this batch substage.
    fn as_str(self) -> &'static str {
        match self {
            Self::StagingPrepare => "staging_prepare",
            Self::StagingLoad => "staging_load",
            Self::ProgressUpdate => "progress_update",
            Self::CommitOnly => "commit_only",
        }
    }
}

/// Event-level table mutation annotated with source LSNs for idempotent replay.
pub(super) struct TrackedTableMutation {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    mutation: TableMutation,
}

impl TrackedTableMutation {
    /// Creates one tracked mutation preserved for retry-safe replay.
    pub(super) fn new(
        start_lsn: PgLsn,
        commit_lsn: PgLsn,
        tx_ordinal: u64,
        mutation: TableMutation,
    ) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            tx_ordinal,
            mutation,
        }
    }

    /// Returns the stable event sequence key for this mutation.
    fn sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
    }

    /// Returns the row that will be upserted for this mutation, if any.
    pub(super) fn upsert_row(&self) -> Option<&TableRow> {
        match &self.mutation {
            TableMutation::Insert(row) | TableMutation::Replace(row) => Some(row),
            TableMutation::Update { upsert_row, .. } => Some(upsert_row),
            TableMutation::Delete(_) => None,
        }
    }
}

/// Truncate event metadata preserved for idempotent replay.
#[derive(Clone, Copy)]
pub(super) struct TrackedTruncateEvent {
    start_lsn: PgLsn,
    commit_lsn: PgLsn,
    tx_ordinal: u64,
    options: i8,
}

impl TrackedTruncateEvent {
    /// Creates one tracked truncate event preserved for retry-safe replay.
    pub(super) fn new(start_lsn: PgLsn, commit_lsn: PgLsn, tx_ordinal: u64, options: i8) -> Self {
        Self {
            start_lsn,
            commit_lsn,
            tx_ordinal,
            options,
        }
    }

    /// Returns the stable event sequence key for this truncate.
    fn sequence_key(&self) -> EventSequenceKey {
        EventSequenceKey::new(self.commit_lsn, self.tx_ordinal)
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

/// Atomic DuckLake batch kinds used by replay bookkeeping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckLakeTableBatchKind {
    Copy,
    Mutation,
    Truncate,
}

impl DuckLakeTableBatchKind {
    pub(super) fn as_str(self) -> &'static str {
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
    first_sequence_key: Option<EventSequenceKey>,
    last_sequence_key: Option<EventSequenceKey>,
    action: PreparedDuckLakeTableBatchAction,
}

impl PreparedDuckLakeTableBatch {
    /// Returns the destination table this batch targets.
    pub(super) fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns whether this batch uses the streaming progress replay path.
    fn uses_streaming_progress(&self) -> bool {
        matches!(
            self.batch_kind,
            DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate
        )
    }
}

/// One table-local streaming replay watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TableStreamingProgress {
    last_sequence_key: EventSequenceKey,
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

/// Ensures the ETL-managed streaming progress table exists.
pub(super) async fn ensure_streaming_progress_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    streaming_progress_table_created: Arc<AtomicBool>,
) -> EtlResult<()> {
    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let _table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake table creation semaphore closed"
        )
    })?;

    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" (
             table_name VARCHAR NOT NULL,
             last_commit_lsn UBIGINT NOT NULL,
             last_tx_ordinal UBIGINT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&streaming_progress_table_created);
    let table_name = STREAMING_PROGRESS_TABLE.to_string();

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
                STREAMING_PROGRESS_TABLE_DATA_INLINING_ROW_LIMIT,
                quote_literal(STREAMING_PROGRESS_TABLE),
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
/// connection per attempt and skipping already committed segments.
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
                batch_kind = batch_kind.as_str(),
                error = ?attempt.error,
                "ducklake table batch attempt failed, retrying"
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
                        if batch_kind == DuckLakeTableBatchKind::Copy {
                            if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                                record_replayed_batch_skip(attempt_batch.as_ref());
                                return Ok(());
                            }

                            apply_table_batch(conn, attempt_batch.as_ref())?;
                            return Ok(());
                        }

                        apply_table_batches(conn, std::slice::from_ref(attempt_batch.as_ref()))?;
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

/// Prepares one retry-safe atomic batch for an Arrow-native table-copy chunk.
pub(super) fn prepare_copy_arrow_batch(
    table_schema: &TableSchema,
    table_name: DuckLakeTableName,
    table_batch: TableArrowBatch,
) -> EtlResult<PreparedDuckLakeTableBatch> {
    let identity =
        build_copy_batch_identity_from_arrow_batch(&table_name, table_schema, &table_batch)?;

    Ok(PreparedDuckLakeTableBatch {
        table_name,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Copy,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key: None,
        last_sequence_key: None,
        action: PreparedDuckLakeTableBatchAction::Mutation(vec![PreparedTableMutation::Copy(
            prepare_arrow_rows(&table_batch.batch)?,
        )]),
    })
}

/// Prepares ordered atomic batches for one table's Arrow-native CDC changes.
///
/// The same [`CDC_MUTATION_BATCH_SIZE`] cap used by the row-oriented path still
/// applies here. Update old/new images are counted together so one logical
/// update never gets split across two atomic DuckLake transactions.
pub(super) fn prepare_change_table_batches(
    table_schema: &TableSchema,
    table_name: DuckLakeTableName,
    change_set: TableChangeSet,
) -> EtlResult<Vec<PreparedDuckLakeTableBatch>> {
    let group_count = change_set.groups.len();
    let mut prepared_batches = Vec::with_capacity(group_count);
    let mut pending_groups = Vec::with_capacity(group_count.min(CDC_MUTATION_BATCH_SIZE));
    let mut pending_rows = 0usize;
    let mut groups = change_set.groups.into_iter().peekable();

    while let Some(group) = groups.next() {
        let group_rows = group.rows.row_count();
        let starts_update_pair =
            group.change == ChangeKind::Update && matches!(group.row_image, RowImage::Old { .. });
        let pair_rows = if starts_update_pair {
            let next_group = groups.peek().ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake update batch is missing a new-row image"
                )
            })?;
            if next_group.change != ChangeKind::Update || next_group.row_image != RowImage::New {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake update batch lost old/new ordering"
                ));
            }
            let next_rows = next_group.rows.row_count();
            if next_rows != group_rows {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake update batch row counts do not match",
                    format!("old_rows={group_rows}, new_rows={next_rows}")
                ));
            }
            next_rows
        } else {
            0
        };
        let mutation_rows = group_rows.saturating_add(pair_rows);
        if !pending_groups.is_empty()
            && pending_rows.saturating_add(mutation_rows) > CDC_MUTATION_BATCH_SIZE
        {
            push_prepared_change_batch(
                &mut prepared_batches,
                table_schema,
                &table_name,
                std::mem::take(&mut pending_groups),
            )?;
            pending_rows = 0;
        }

        pending_rows = pending_rows.saturating_add(mutation_rows);
        pending_groups.push(group);

        if starts_update_pair {
            // Keep the paired new-row image in the same atomic batch as its old image.
            pending_groups.push(
                groups
                    .next()
                    .expect("peeked update pair must exist when consuming groups"),
            );
        }
    }

    push_prepared_change_batch(
        &mut prepared_batches,
        table_schema,
        &table_name,
        pending_groups,
    )?;

    Ok(prepared_batches)
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
        first_sequence_key: tracked_truncates
            .first()
            .map(TrackedTruncateEvent::sequence_key),
        last_sequence_key: tracked_truncates
            .last()
            .map(TrackedTruncateEvent::sequence_key),
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

/// Deletes the persisted streaming replay watermark for one table.
pub(super) fn clear_table_streaming_progress(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<()> {
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {};"#,
        quote_literal(table_name),
    );
    conn.execute_batch(&sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress delete failed",
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

/// Replay decision for one streaming batch after reading the table watermark.
enum StreamingReplayDecision {
    Skip,
    Apply,
}

/// Records that one replay-safe batch was skipped because it was already committed.
fn record_replayed_batch_skip(batch: &PreparedDuckLakeTableBatch) {
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
}

/// Reads the steady-state streaming replay watermark for one table.
fn read_table_streaming_progress(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<Option<TableStreamingProgress>> {
    let sql = format!(
        r#"SELECT last_commit_lsn, last_tx_ordinal
         FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {} LIMIT 1;"#,
        quote_literal(table_name),
    );
    let mut statement = conn.prepare(&sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query prepare failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    let mut rows = statement.query([]).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;

    let Some(row) = rows.next().map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress row fetch failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?
    else {
        return Ok(None);
    };

    let last_commit_lsn: u64 = row.get(0).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress commit lsn read failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    let last_tx_ordinal: u64 = row.get(1).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress tx ordinal read failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;

    Ok(Some(TableStreamingProgress {
        last_sequence_key: EventSequenceKey::new(PgLsn::from(last_commit_lsn), last_tx_ordinal),
    }))
}

/// Reads the last applied streaming sequence key for one table.
pub(super) fn read_table_streaming_progress_sequence_key(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<Option<EventSequenceKey>> {
    Ok(read_table_streaming_progress(conn, table_name)?.map(|progress| progress.last_sequence_key))
}

/// Drops already-applied tracked mutations using the persisted sequence key.
pub(super) fn retain_mutations_after_sequence_key(
    tracked_mutations: Vec<TrackedTableMutation>,
    last_sequence_key: Option<EventSequenceKey>,
) -> Vec<TrackedTableMutation> {
    match last_sequence_key {
        Some(last_sequence_key) => tracked_mutations
            .into_iter()
            .filter(|tracked_mutation| {
                compare_sequence_keys(tracked_mutation.sequence_key(), last_sequence_key)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_mutations,
    }
}

/// Drops already-applied tracked truncates using the persisted sequence key.
pub(super) fn retain_truncates_after_sequence_key(
    tracked_truncates: Vec<TrackedTruncateEvent>,
    last_sequence_key: Option<EventSequenceKey>,
) -> Vec<TrackedTruncateEvent> {
    match last_sequence_key {
        Some(last_sequence_key) => tracked_truncates
            .into_iter()
            .filter(|tracked_truncate| {
                compare_sequence_keys(tracked_truncate.sequence_key(), last_sequence_key)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_truncates,
    }
}

/// Drops already-applied Arrow-native CDC rows using the persisted sequence key.
pub(super) fn retain_change_set_after_sequence_key(
    change_set: TableChangeSet,
    last_sequence_key: Option<EventSequenceKey>,
) -> EtlResult<TableChangeSet> {
    let Some(last_sequence_key) = last_sequence_key else {
        return Ok(change_set);
    };

    let mut retained_groups = Vec::with_capacity(change_set.groups.len());
    let mut groups = change_set.groups.into_iter().peekable();

    while let Some(group) = groups.next() {
        if group.change == ChangeKind::Update && matches!(group.row_image, RowImage::Old { .. }) {
            let next_group = groups.next().ok_or_else(|| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake update batch is missing a new-row image"
                )
            })?;

            if next_group.change != ChangeKind::Update || next_group.row_image != RowImage::New {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake update batch lost old/new ordering"
                ));
            }

            if group.rows.row_count() != next_group.rows.row_count() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake update batch row counts do not match",
                    format!(
                        "old_rows={}, new_rows={}",
                        group.rows.row_count(),
                        next_group.rows.row_count()
                    )
                ));
            }

            let first_sequence_key = change_group_first_sequence_key(&group)
                .or_else(|| change_group_first_sequence_key(&next_group));
            let last_group_sequence_key = change_group_last_sequence_key(&next_group)
                .or_else(|| change_group_last_sequence_key(&group));

            if let (Some(first_sequence_key), Some(last_group_sequence_key)) =
                (first_sequence_key, last_group_sequence_key)
            {
                match compare_sequence_keys(last_sequence_key, first_sequence_key) {
                    std::cmp::Ordering::Less => {
                        retained_groups.push(group);
                        retained_groups.push(next_group);
                    }
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                        if compare_sequence_keys(last_sequence_key, last_group_sequence_key)
                            == std::cmp::Ordering::Less
                        {
                            let first_pending_row_index =
                                first_pending_row_index(&group, last_sequence_key).ok_or_else(|| {
                                    etl_error!(
                                        ErrorKind::InvalidState,
                                        "DuckLake streaming progress landed inside an Arrow update batch without a remaining row"
                                    )
                                })?;
                            retained_groups
                                .push(slice_change_group_from(&group, first_pending_row_index));
                            retained_groups.push(slice_change_group_from(
                                &next_group,
                                first_pending_row_index,
                            ));
                        }
                    }
                }
            }

            continue;
        }

        match (
            change_group_first_sequence_key(&group),
            change_group_last_sequence_key(&group),
        ) {
            (Some(first_sequence_key), Some(last_group_sequence_key)) => {
                match compare_sequence_keys(last_sequence_key, first_sequence_key) {
                    std::cmp::Ordering::Less => retained_groups.push(group),
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                        if compare_sequence_keys(last_sequence_key, last_group_sequence_key)
                            == std::cmp::Ordering::Less
                        {
                            let first_pending_row_index =
                                first_pending_row_index(&group, last_sequence_key).ok_or_else(|| {
                                    etl_error!(
                                        ErrorKind::InvalidState,
                                        "DuckLake streaming progress landed inside an Arrow batch without a remaining row"
                                    )
                                })?;
                            retained_groups
                                .push(slice_change_group_from(&group, first_pending_row_index));
                        }
                    }
                }
            }
            _ => retained_groups.push(group),
        }
    }

    Ok(TableChangeSet {
        table_id: change_set.table_id,
        groups: retained_groups,
    })
}

/// Returns the first row index strictly after the applied sequence key.
fn first_pending_row_index(
    group: &ChangeArrowBatch,
    last_sequence_key: EventSequenceKey,
) -> Option<usize> {
    group
        .commit_lsns
        .values()
        .iter()
        .zip(group.tx_ordinals.values().iter())
        .position(|(commit_lsn, tx_ordinal)| {
            compare_sequence_keys(
                EventSequenceKey::new(PgLsn::from(*commit_lsn), *tx_ordinal),
                last_sequence_key,
            ) == std::cmp::Ordering::Greater
        })
}

/// Returns one sliced change group starting at the first pending row index.
fn slice_change_group_from(
    group: &ChangeArrowBatch,
    first_pending_row_index: usize,
) -> ChangeArrowBatch {
    let row_count = group.rows.row_count() - first_pending_row_index;
    let batch = group.rows.batch.slice(first_pending_row_index, row_count);
    let commit_lsns =
        UInt64Array::from(group.commit_lsns.values()[first_pending_row_index..].to_vec());
    let tx_ordinals =
        UInt64Array::from(group.tx_ordinals.values()[first_pending_row_index..].to_vec());

    ChangeArrowBatch {
        rows: TableArrowBatch::new(
            group.rows.table_id,
            Arc::clone(&group.rows.table_schema),
            batch,
        ),
        change: group.change,
        row_image: group.row_image,
        commit_lsns,
        tx_ordinals,
    }
}

/// Decides whether a streaming batch must be replayed or skipped.
fn streaming_replay_decision(
    progress: TableStreamingProgress,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<StreamingReplayDecision> {
    let first_sequence_key = batch.first_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its first sequence key",
            format!(
                "table={}, batch_kind={}",
                batch.table_name,
                batch.batch_kind.as_str()
            )
        )
    })?;
    let last_sequence_key = batch.last_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last sequence key",
            format!(
                "table={}, batch_kind={}",
                batch.table_name,
                batch.batch_kind.as_str()
            )
        )
    })?;

    if compare_sequence_keys(progress.last_sequence_key, first_sequence_key)
        != std::cmp::Ordering::Less
    {
        if compare_sequence_keys(progress.last_sequence_key, last_sequence_key)
            == std::cmp::Ordering::Less
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake streaming progress landed inside an atomic batch",
                format!(
                    "table={}, progress={}, first={}, last={}",
                    batch.table_name,
                    progress.last_sequence_key,
                    first_sequence_key,
                    last_sequence_key
                )
            ));
        }

        return Ok(StreamingReplayDecision::Skip);
    }

    Ok(StreamingReplayDecision::Apply)
}

/// Compares two ETL event sequence keys using commit LSN then transaction ordinal.
fn compare_sequence_keys(left: EventSequenceKey, right: EventSequenceKey) -> std::cmp::Ordering {
    (u64::from(left.commit_lsn), left.tx_ordinal)
        .cmp(&(u64::from(right.commit_lsn), right.tx_ordinal))
}

/// Applies all prepared atomic batches for one table on the same connection.
fn apply_table_batches(
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
) -> EtlResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let mut streaming_progress = if batches[0].uses_streaming_progress() {
        read_table_streaming_progress(conn, batches[0].table_name())?
    } else {
        None
    };

    for batch in batches {
        if !batch.uses_streaming_progress() {
            // Copy batches keep the marker path because initial-copy retries
            // still depend on per-batch idempotency.
            if applied_batch_marker_exists(conn, batch)? {
                record_replayed_batch_skip(batch);
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
            continue;
        }

        if let Some(progress) = streaming_progress {
            match streaming_replay_decision(progress, batch)? {
                StreamingReplayDecision::Skip => {
                    record_replayed_batch_skip(batch);
                    continue;
                }
                StreamingReplayDecision::Apply => {}
            }
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

        streaming_progress = batch
            .last_sequence_key
            .map(|last_sequence_key| TableStreamingProgress { last_sequence_key });
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
    let first_sequence_key = tracked_mutations
        .first()
        .map(TrackedTableMutation::sequence_key);
    let last_sequence_key = tracked_mutations
        .last()
        .map(TrackedTableMutation::sequence_key);
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
        first_sequence_key,
        last_sequence_key,
        action: PreparedDuckLakeTableBatchAction::Mutation(prepare_table_mutations(
            table_schema,
            mutations,
        )?),
    });

    Ok(())
}

/// Returns the first sequence key present in one Arrow-native change group.
fn change_group_first_sequence_key(group: &ChangeArrowBatch) -> Option<EventSequenceKey> {
    group
        .commit_lsns
        .values()
        .first()
        .zip(group.tx_ordinals.values().first())
        .map(|(commit_lsn, tx_ordinal)| {
            EventSequenceKey::new(PgLsn::from(*commit_lsn), *tx_ordinal)
        })
}

/// Returns the last sequence key present in one Arrow-native change group.
fn change_group_last_sequence_key(group: &ChangeArrowBatch) -> Option<EventSequenceKey> {
    group
        .commit_lsns
        .values()
        .last()
        .zip(group.tx_ordinals.values().last())
        .map(|(commit_lsn, tx_ordinal)| {
            EventSequenceKey::new(PgLsn::from(*commit_lsn), *tx_ordinal)
        })
}

/// Builds one prepared atomic batch from ordered Arrow-native CDC groups.
fn push_prepared_change_batch(
    prepared_batches: &mut Vec<PreparedDuckLakeTableBatch>,
    table_schema: &TableSchema,
    table_name: &str,
    groups: Vec<ChangeArrowBatch>,
) -> EtlResult<()> {
    if groups.is_empty() {
        return Ok(());
    }

    let identity = build_change_batch_identity(table_name, table_schema, &groups)?;
    let first_sequence_key = groups.first().and_then(change_group_first_sequence_key);
    let last_sequence_key = groups.last().and_then(change_group_last_sequence_key);
    prepared_batches.push(PreparedDuckLakeTableBatch {
        table_name: table_name.to_string(),
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Mutation,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_sequence_key,
        last_sequence_key,
        action: PreparedDuckLakeTableBatchAction::Mutation(prepare_change_table_mutations(
            table_schema,
            &groups,
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

/// Groups ordered Arrow-native CDC changes into retryable DuckDB operations.
fn prepare_change_table_mutations(
    table_schema: &TableSchema,
    groups: &[ChangeArrowBatch],
) -> EtlResult<Vec<PreparedTableMutation>> {
    let mut prepared_mutations = Vec::new();
    let mut index = 0;

    while let Some(group) = groups.get(index) {
        match (group.change, group.row_image) {
            (ChangeKind::Insert, RowImage::New) => {
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_arrow_rows(
                    &group.rows.batch,
                )?));
                index += 1;
            }
            (ChangeKind::Delete, RowImage::Old { .. }) => {
                prepared_mutations.push(PreparedTableMutation::DeleteKeys {
                    keys: prepare_arrow_rows(&project_primary_key_batch(
                        table_schema,
                        &group.rows.batch,
                    )?)?,
                    key_columns: primary_key_column_names(table_schema),
                    join_predicate: delete_join_predicate_from_schema(table_schema)?,
                    origin: "delete",
                });
                index += 1;
            }
            (ChangeKind::Update, RowImage::Old { .. }) => {
                let next_group = groups.get(index + 1).ok_or_else(|| {
                    etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake update batch is missing a new-row image"
                    )
                })?;
                if next_group.change != ChangeKind::Update || next_group.row_image != RowImage::New
                {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake update batch lost old/new ordering"
                    ));
                }
                if group.rows.row_count() != next_group.rows.row_count() {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake update batch row counts do not match",
                        format!(
                            "old_rows={}, new_rows={}",
                            group.rows.row_count(),
                            next_group.rows.row_count()
                        )
                    ));
                }

                prepared_mutations.push(PreparedTableMutation::DeleteKeys {
                    keys: prepare_arrow_rows(&project_primary_key_batch(
                        table_schema,
                        &group.rows.batch,
                    )?)?,
                    key_columns: primary_key_column_names(table_schema),
                    join_predicate: delete_join_predicate_from_schema(table_schema)?,
                    origin: "update",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_arrow_rows(
                    &next_group.rows.batch,
                )?));
                index += 2;
            }
            (ChangeKind::Update, RowImage::New) => {
                prepared_mutations.push(PreparedTableMutation::DeleteKeys {
                    keys: prepare_arrow_rows(&project_primary_key_batch(
                        table_schema,
                        &group.rows.batch,
                    )?)?,
                    key_columns: primary_key_column_names(table_schema),
                    join_predicate: delete_join_predicate_from_schema(table_schema)?,
                    origin: "replace",
                });
                prepared_mutations.push(PreparedTableMutation::Upsert(prepare_arrow_rows(
                    &group.rows.batch,
                )?));
                index += 1;
            }
            (ChangeKind::Insert, RowImage::Old { .. }) | (ChangeKind::Delete, RowImage::New) => {
                index += 1;
            }
        }
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

/// Builds a `USING` join predicate from the table's primary-key columns.
fn delete_join_predicate_from_schema(table_schema: &TableSchema) -> EtlResult<String> {
    if !table_schema.has_primary_keys() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake delete requires a primary key",
            format!("Table '{}' has no primary key columns", table_schema.name)
        ));
    }

    Ok(table_schema
        .column_schemas
        .iter()
        .filter(|column_schema| column_schema.primary)
        .map(|column_schema| {
            let quoted_column = quote_identifier(&column_schema.name);
            format!("target.{quoted_column} IS NOT DISTINCT FROM stage.{quoted_column}")
        })
        .collect::<Vec<_>>()
        .join(" AND "))
}

/// Returns the ordered list of primary-key column names for a table schema.
fn primary_key_column_names(table_schema: &TableSchema) -> Vec<String> {
    table_schema
        .column_schemas
        .iter()
        .filter(|column_schema| column_schema.primary)
        .map(|column_schema| column_schema.name.clone())
        .collect()
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

/// Builds a deterministic identity for one ordered Arrow-native mutation batch.
fn build_change_batch_identity(
    table_name: &str,
    table_schema: &TableSchema,
    groups: &[ChangeArrowBatch],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "mutation".hash(&mut hasher);
    table_name.hash(&mut hasher);

    let mut first_commit_lsn = None;
    let mut last_commit_lsn = None;

    for group in groups {
        first_commit_lsn = first_commit_lsn
            .or_else(|| group.commit_lsns.values().first().copied().map(PgLsn::from));
        last_commit_lsn = group
            .commit_lsns
            .values()
            .last()
            .copied()
            .map(PgLsn::from)
            .or(last_commit_lsn);

        match (group.change, group.row_image) {
            (ChangeKind::Insert, RowImage::New) => "insert".hash(&mut hasher),
            (ChangeKind::Update, RowImage::Old { .. }) => "update_old".hash(&mut hasher),
            (ChangeKind::Update, RowImage::New) => "update_new".hash(&mut hasher),
            (ChangeKind::Delete, RowImage::Old { .. }) => "delete".hash(&mut hasher),
            (ChangeKind::Insert, RowImage::Old { .. }) => "insert_old".hash(&mut hasher),
            (ChangeKind::Delete, RowImage::New) => "delete_new".hash(&mut hasher),
        }

        for commit_lsn in group.commit_lsns.values() {
            commit_lsn.hash(&mut hasher);
        }
        for tx_ordinal in group.tx_ordinals.values() {
            tx_ordinal.hash(&mut hasher);
        }

        let rows = record_batch_to_table_rows(&group.rows.batch);
        match (group.change, group.row_image) {
            (ChangeKind::Delete, RowImage::Old { .. })
            | (ChangeKind::Update, RowImage::Old { .. }) => {
                for row in &rows {
                    delete_predicate_from_row(table_schema, row)?.hash(&mut hasher);
                }
            }
            _ => {
                for row in &rows {
                    hash_table_row_ref(&mut hasher, row);
                }
            }
        }
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Mutation,
        first_commit_lsn,
        last_commit_lsn,
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

/// Builds a deterministic identity for one Arrow-native table-copy batch.
fn build_copy_batch_identity_from_arrow_batch(
    table_name: &str,
    table_schema: &TableSchema,
    table_batch: &TableArrowBatch,
) -> EtlResult<DuckLakeBatchIdentity> {
    build_copy_batch_identity(
        table_name,
        table_schema,
        &record_batch_to_table_rows(&table_batch.batch),
    )
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

/// Updates the steady-state streaming replay watermark inside the open transaction.
fn update_table_streaming_progress(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let last_sequence_key = batch.last_sequence_key.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last sequence key",
            format!(
                "table={}, batch_kind={}",
                batch.table_name,
                batch.batch_kind.as_str()
            )
        )
    })?;
    let sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {};
         INSERT INTO {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         (table_name, last_commit_lsn, last_tx_ordinal, updated_at)
         VALUES ({}, {}, {}, current_timestamp);"#,
        quote_literal(&batch.table_name),
        quote_literal(&batch.table_name),
        u64::from(last_sequence_key.commit_lsn),
        last_sequence_key.tx_ordinal,
    );
    conn.execute_batch(&sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress update failed",
            format_query_error_detail(&sql, &error),
            source: error
        )
    })?;
    Ok(())
}

/// Reusable per-batch temp staging table for DuckLake upserts.
struct ReusableStagingTable {
    table_name: DuckLakeTableName,
    staging_name: String,
    created: bool,
}

impl ReusableStagingTable {
    /// Creates a fresh staging-table manager for one destination table.
    fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            staging_name: format!("__staging_{table_name}"),
            created: false,
        }
    }

    /// Loads one prepared row set into staging and applies it to the target table.
    fn stage_and_insert(
        &mut self,
        conn: &duckdb::Connection,
        batch_kind: &'static str,
        sub_batch_kind: &'static str,
        prepared_rows: &PreparedRows,
    ) -> EtlResult<()> {
        let prepare_started = Instant::now();
        self.prepare(conn)?;
        record_batch_substage_duration(
            batch_kind,
            sub_batch_kind,
            DuckLakeBatchSubstage::StagingPrepare,
            prepare_started.elapsed().as_secs_f64(),
        );

        let load_started = Instant::now();
        self.load_rows(conn, prepared_rows)?;
        record_batch_substage_duration(
            batch_kind,
            sub_batch_kind,
            DuckLakeBatchSubstage::StagingLoad,
            load_started.elapsed().as_secs_f64(),
        );

        let sql = format!(
            r#"INSERT INTO {LAKE_CATALOG}."{table_name}" SELECT * FROM {staging:?};"#,
            table_name = &self.table_name,
            staging = &self.staging_name,
        );
        conn.execute_batch(&sql).map_err(|error| {
            tracing::error!(?error, "error INSERT INTO");
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                source: error
            )
        })?;
        record_profiled_mutation_operation_duration(
            conn,
            DuckLakeMutationOperationKind::Insert,
            DELETE_ORIGIN_NONE,
        );
        Ok(())
    }

    /// Drops the temp staging table after the batch finishes.
    fn cleanup(&self, conn: &duckdb::Connection) {
        if !self.created {
            return;
        }

        if let Err(error) = conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS {staging:?}",
            staging = &self.staging_name
        )) {
            tracing::error!(?error, "error drop table staging");
        }
    }

    /// Creates the temp table once, then clears it before each reuse.
    fn prepare(&mut self, conn: &duckdb::Connection) -> EtlResult<()> {
        if self.created {
            let sql = format!("TRUNCATE TABLE {staging:?};", staging = &self.staging_name);
            conn.execute_batch(&sql).map_err(|error| {
                tracing::error!(?error, "error clear staging");
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging table clear failed",
                    source: error
                )
            })?;
            return Ok(());
        }

        #[cfg(feature = "test-utils")]
        {
            let mut counts = STAGING_TABLE_CREATIONS_BY_TABLE.lock();
            *counts.entry(self.table_name.clone()).or_default() += 1;
        }

        conn.execute_batch(&format!(
            r#"CREATE OR REPLACE TEMP TABLE {staging:?} AS
             SELECT * FROM {LAKE_CATALOG}."{table_name}" LIMIT 0;"#,
            staging = &self.staging_name,
            table_name = &self.table_name,
        ))
        .map_err(|error| {
            tracing::error!(?error, "error CREATE TEMP TABLE");

            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging table creation failed",
                source: error
            )
        })?;
        self.created = true;
        Ok(())
    }

    /// Loads one prepared row payload into the temp staging table.
    fn load_rows(&self, conn: &duckdb::Connection, prepared_rows: &PreparedRows) -> EtlResult<()> {
        match prepared_rows {
            PreparedRows::Appender(all_values) => {
                let mut appender = conn.appender(&self.staging_name).map_err(|error| {
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
                })?;
            }
            PreparedRows::Arrow(batches) => {
                let mut appender = conn.appender(&self.staging_name).map_err(|error| {
                    tracing::error!(?error, "error arrow appender");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging Arrow appender creation failed",
                        source: error
                    )
                })?;
                for batch in batches {
                    append_retryable_arrow_batch(&mut appender, batch).map_err(|error| {
                        tracing::error!(?error, "error append record batch");
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake staging append_record_batch failed",
                            source: error
                        )
                    })?;
                }
                appender.flush().map_err(|error| {
                    tracing::error!(?error, "error arrow flush");
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging Arrow appender flush failed",
                        source: error
                    )
                })?;
            }
        }
        Ok(())
    }
}

/// Applies one atomic per-table batch in a single DuckLake transaction.
fn apply_table_batch(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let batch_started = Instant::now();
    let batch_kind = batch.batch_kind.as_str();
    let sub_batch_kind = batch_log_kind(batch);
    let batch_size = batch_item_count(batch);
    let prepared_mutation_count = prepared_mutation_count(batch);
    let upsert_rows = batch_upsert_row_count(batch);
    let delete_predicates = batch_delete_predicate_count(batch);

    let begin_started = Instant::now();
    if let Err(error) = conn.execute_batch("BEGIN TRANSACTION") {
        tracing::error!(?error, "error transaction");
        warn!(
            table = %batch.table_name,
            batch_id = %batch.batch_id,
            batch_kind,
            sub_batch_kind,
            batch_size,
            prepared_mutation_count,
            upsert_rows,
            delete_predicates,
            elapsed_ms = begin_started.elapsed().as_millis() as u64,
            error = ?error,
            "ducklake begin transaction failed"
        );
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            source: error
        ));
    }
    info!(
        table = %batch.table_name,
        batch_id = %batch.batch_id,
        batch_kind,
        sub_batch_kind,
        batch_size,
        prepared_mutation_count,
        upsert_rows,
        delete_predicates,
        elapsed_ms = begin_started.elapsed().as_millis() as u64,
        "ducklake transaction opened"
    );

    let mut reusable_staging_table = ReusableStagingTable::new(&batch.table_name);
    let result = (|| -> EtlResult<()> {
        match &batch.action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
                for prepared_mutation in prepared_mutations {
                    apply_table_mutation(
                        conn,
                        batch_kind,
                        sub_batch_kind,
                        prepared_mutation,
                        &mut reusable_staging_table,
                    )?;
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => {
                apply_truncate_batch_action(conn, &batch.table_name)?;
            }
        }

        if batch.uses_streaming_progress() {
            let progress_update_started = Instant::now();
            update_table_streaming_progress(conn, batch)?;
            record_batch_substage_duration(
                batch_kind,
                sub_batch_kind,
                DuckLakeBatchSubstage::ProgressUpdate,
                progress_update_started.elapsed().as_secs_f64(),
            );
        } else {
            insert_applied_batch_marker(conn, batch)?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            let commit_started = Instant::now();
            if let Err(error) = conn.execute_batch("COMMIT") {
                tracing::error!(?error, "error commit");
                reusable_staging_table.cleanup(conn);
                warn!(
                    table = %batch.table_name,
                    batch_id = %batch.batch_id,
                    batch_kind,
                    sub_batch_kind,
                    batch_size,
                    prepared_mutation_count,
                    upsert_rows,
                    delete_predicates,
                    commit_elapsed_ms = commit_started.elapsed().as_millis() as u64,
                    elapsed_ms = batch_started.elapsed().as_millis() as u64,
                    error = ?error,
                    "ducklake commit failed"
                );
                return Err(etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake COMMIT failed",
                    source: error
                ));
            }
            record_batch_substage_duration(
                batch_kind,
                sub_batch_kind,
                DuckLakeBatchSubstage::CommitOnly,
                commit_started.elapsed().as_secs_f64(),
            );
            reusable_staging_table.cleanup(conn);
            histogram!(
                ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
                BATCH_KIND_LABEL => batch_kind,
                SUB_BATCH_KIND_LABEL => sub_batch_kind,
            )
            .record(batch_started.elapsed().as_secs_f64());
            histogram!(
                ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
                BATCH_KIND_LABEL => batch_kind,
                SUB_BATCH_KIND_LABEL => sub_batch_kind,
            )
            .record(prepared_mutation_count as f64);
            info!(
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind,
                first_start_lsn = ?batch.first_start_lsn,
                last_commit_lsn = ?batch.last_commit_lsn,
                sub_batch_kind,
                batch_size,
                prepared_mutation_count,
                upsert_rows,
                delete_predicates,
                commit_elapsed_ms = commit_started.elapsed().as_millis() as u64,
                elapsed_ms = batch_started.elapsed().as_millis() as u64,
                insert_sub_batch_rows = apply_sub_batch_rows(batch),
                "ducklake batch committed"
            );

            #[cfg(feature = "test-utils")]
            maybe_fail_after_committed_batch_for_tests(batch.batch_kind, &batch.table_name)?;

            Ok(())
        }
        Err(error) => {
            let rollback_started = Instant::now();
            let rollback = conn.execute_batch("ROLLBACK");
            let rollback_elapsed_ms = rollback_started.elapsed().as_millis() as u64;
            reusable_staging_table.cleanup(conn);
            if let Err(rollback) = rollback {
                tracing::error!(?rollback, "error rollback");
                warn!(
                    table = %batch.table_name,
                    batch_id = %batch.batch_id,
                    batch_kind,
                    sub_batch_kind,
                    batch_size,
                    prepared_mutation_count,
                    upsert_rows,
                    delete_predicates,
                    rollback_elapsed_ms,
                    elapsed_ms = batch_started.elapsed().as_millis() as u64,
                    error = ?error,
                    rollback_error = ?rollback,
                    "ducklake transaction rollback failed"
                );
            } else {
                warn!(
                    table = %batch.table_name,
                    batch_id = %batch.batch_id,
                    batch_kind,
                    sub_batch_kind,
                    batch_size,
                    prepared_mutation_count,
                    upsert_rows,
                    delete_predicates,
                    rollback_elapsed_ms,
                    elapsed_ms = batch_started.elapsed().as_millis() as u64,
                    error = ?error,
                    "ducklake transaction rolled back"
                );
            }
            Err(error)
        }
    }
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(conn: &duckdb::Connection, table_name: &str) -> EtlResult<()> {
    let sql = format!(r#"TRUNCATE TABLE {LAKE_CATALOG}."{table_name}";"#);
    conn.execute_batch(&sql).map_err(|error| {
        tracing::error!(?error, "error TRUNCATE TABLE");
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake TRUNCATE TABLE failed",
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

/// Returns the last DuckDB-profiled statement latency in seconds, if available.
fn duckdb_profile_latency_seconds(conn: &duckdb::Connection) -> Option<f64> {
    conn.get_profiling_info()?
        .metrics
        .get("LATENCY")?
        .parse::<f64>()
        .ok()
}

/// Records one prepared DuckLake mutation duration sample from DuckDB profiling.
fn record_mutation_operation_duration(
    operation_kind: DuckLakeMutationOperationKind,
    delete_origin: &'static str,
    duration_seconds: f64,
) {
    histogram!(
        ETL_DUCKLAKE_MUTATION_OPERATION_DURATION_SECONDS,
        OPERATION_KIND_LABEL => operation_kind.as_str(),
        DELETE_ORIGIN_LABEL => delete_origin,
    )
    .record(duration_seconds);
}

/// Records the last DuckDB-profiled statement latency for one mutation kind.
fn record_profiled_mutation_operation_duration(
    conn: &duckdb::Connection,
    operation_kind: DuckLakeMutationOperationKind,
    delete_origin: &'static str,
) {
    if let Some(latency_seconds) = duckdb_profile_latency_seconds(conn) {
        record_mutation_operation_duration(operation_kind, delete_origin, latency_seconds);
    }
}

/// Records one internal DuckLake atomic-batch substage duration.
fn record_batch_substage_duration(
    batch_kind: &'static str,
    sub_batch_kind: &'static str,
    substage: DuckLakeBatchSubstage,
    duration_seconds: f64,
) {
    histogram!(
        ETL_DUCKLAKE_BATCH_SUBSTAGE_DURATION_SECONDS,
        BATCH_KIND_LABEL => batch_kind,
        SUB_BATCH_KIND_LABEL => sub_batch_kind,
        SUBSTAGE_LABEL => substage.as_str(),
    )
    .record(duration_seconds);
}

/// Applies one prepared table mutation inside an open transaction.
fn apply_table_mutation(
    conn: &duckdb::Connection,
    batch_kind: &'static str,
    sub_batch_kind: &'static str,
    prepared_mutation: &PreparedTableMutation,
    reusable_staging_table: &mut ReusableStagingTable,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Copy(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch_kind,
                PREPARED_ROWS_KIND_LABEL => prepared_rows_kind(prepared_rows),
            )
            .record(prepared_rows_count(prepared_rows) as f64);
            apply_copy_mutation(
                conn,
                batch_kind,
                sub_batch_kind,
                &reusable_staging_table.table_name,
                prepared_rows,
            )
        }
        PreparedTableMutation::Upsert(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch_kind,
                PREPARED_ROWS_KIND_LABEL => prepared_rows_kind(prepared_rows),
            )
            .record(prepared_rows_count(prepared_rows) as f64);
            apply_upsert_mutation(
                conn,
                batch_kind,
                sub_batch_kind,
                prepared_rows,
                reusable_staging_table,
            )
        }
        PreparedTableMutation::Delete { predicates, origin } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch_kind,
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(predicates.len() as f64);
            apply_delete_mutation(
                conn,
                &reusable_staging_table.table_name,
                predicates.as_slice(),
                origin,
            )
        }
        PreparedTableMutation::DeleteKeys {
            keys,
            key_columns,
            join_predicate,
            origin,
        } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch_kind,
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(prepared_rows_count(keys) as f64);
            apply_delete_keys_mutation(
                conn,
                (batch_kind, sub_batch_kind),
                &reusable_staging_table.table_name,
                keys,
                key_columns,
                join_predicate,
                origin,
            )
        }
    }
}

/// Appends one retry-safe Arrow record batch to DuckDB.
fn append_retryable_arrow_batch(
    appender: &mut duckdb::Appender<'_>,
    batch: &duckdb::arrow::record_batch::RecordBatch,
) -> duckdb::Result<()> {
    // [`RecordBatch::clone`] is shallow: it only clones the schema/column `Arc`s.
    // The underlying source buffers stay shared while we keep the prepared payload
    // immutable for retry-safe replays.
    appender.append_record_batch(batch.clone())
}

/// Applies one copy batch directly into the destination table.
fn apply_copy_mutation(
    conn: &duckdb::Connection,
    batch_kind: &'static str,
    sub_batch_kind: &'static str,
    table_name: &str,
    prepared_rows: &PreparedRows,
) -> EtlResult<()> {
    let PreparedRows::Arrow(batches) = prepared_rows else {
        let mut reusable_staging_table = ReusableStagingTable::new(table_name);
        return reusable_staging_table.stage_and_insert(
            conn,
            batch_kind,
            sub_batch_kind,
            prepared_rows,
        );
    };

    if batches.is_empty() {
        return Ok(());
    }

    let mut appender = conn
        .appender_to_catalog_and_db(table_name, LAKE_CATALOG, "main")
        .map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake destination appender creation failed",
                source: error
            )
        })?;

    for batch in batches {
        append_retryable_arrow_batch(&mut appender, batch).map_err(|error| {
            etl_error!(
                ErrorKind::DestinationQueryFailed,
                "DuckLake destination append_record_batch failed",
                source: error
            )
        })?;
    }

    appender.flush().map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake destination appender flush failed",
            source: error
        )
    })?;

    Ok(())
}

/// Applies one upsert batch inside an open DuckLake transaction.
fn apply_upsert_mutation(
    conn: &duckdb::Connection,
    batch_kind: &'static str,
    sub_batch_kind: &'static str,
    prepared_rows: &PreparedRows,
    reusable_staging_table: &mut ReusableStagingTable,
) -> EtlResult<()> {
    let row_count = match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::Arrow(batches) => batches.iter().map(|batch| batch.num_rows()).sum(),
    };

    if row_count == 0 {
        return Ok(());
    }

    reusable_staging_table.stage_and_insert(conn, batch_kind, sub_batch_kind, prepared_rows)
}

/// Applies one delete batch inside an open DuckLake transaction.
fn apply_delete_mutation(
    conn: &duckdb::Connection,
    table_name: &str,
    predicates: &[String],
    delete_origin: &'static str,
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
        record_profiled_mutation_operation_duration(
            conn,
            DuckLakeMutationOperationKind::Delete,
            delete_origin,
        );
    }

    Ok(())
}

/// Applies one primary-key staging delete batch inside an open DuckLake transaction.
fn apply_delete_keys_mutation(
    conn: &duckdb::Connection,
    batch_labels: (&'static str, &'static str),
    table_name: &str,
    prepared_rows: &PreparedRows,
    key_columns: &[String],
    join_predicate: &str,
    delete_origin: &'static str,
) -> EtlResult<()> {
    let (batch_kind, sub_batch_kind) = batch_labels;
    let row_count = prepared_rows_count(prepared_rows);
    if row_count == 0 {
        return Ok(());
    }

    let staging = format!("__staging_keys_{table_name}");
    let selected_columns = key_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let create_stage_sql = format!(
        r#"CREATE OR REPLACE TEMP TABLE {staging:?} AS
           SELECT {selected_columns} FROM {LAKE_CATALOG}."{table_name}" WHERE 1 = 0;"#
    );
    let prepare_started = Instant::now();
    conn.execute_batch(&create_stage_sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake key staging table creation failed",
            format_query_error_detail(&create_stage_sql, &error),
            source: error
        )
    })?;
    record_batch_substage_duration(
        batch_kind,
        sub_batch_kind,
        DuckLakeBatchSubstage::StagingPrepare,
        prepare_started.elapsed().as_secs_f64(),
    );

    let load_started = Instant::now();
    let load_result = match prepared_rows {
        PreparedRows::Appender(values) => (|| {
            let mut appender = conn.appender(&staging).map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake key staging appender creation failed",
                    source: error
                )
            })?;
            for value_row in values {
                appender
                    .append_row(duckdb::appender_params_from_iter(value_row))
                    .map_err(|error| {
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake key staging append_row failed",
                            source: error
                        )
                    })?;
            }
            appender.flush().map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake key staging appender flush failed",
                    source: error
                )
            })
        })(),
        PreparedRows::Arrow(batches) => (|| {
            let mut appender = conn.appender(&staging).map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake key staging Arrow appender creation failed",
                    source: error
                )
            })?;
            for batch in batches {
                append_retryable_arrow_batch(&mut appender, batch).map_err(|error| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake key staging append_record_batch failed",
                        source: error
                    )
                })?;
            }
            appender.flush().map_err(|error| {
                etl_error!(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake key staging Arrow appender flush failed",
                    source: error
                )
            })
        })(),
    };

    if let Err(error) = load_result {
        let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
        return Err(error);
    }
    record_batch_substage_duration(
        batch_kind,
        sub_batch_kind,
        DuckLakeBatchSubstage::StagingLoad,
        load_started.elapsed().as_secs_f64(),
    );

    let delete_sql = format!(
        r#"DELETE FROM {LAKE_CATALOG}."{table_name}" AS target
           USING {staging:?} AS stage
           WHERE {join_predicate};"#
    );
    let result = conn.execute_batch(&delete_sql).map_err(|error| {
        etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake staged DELETE failed",
            format_query_error_detail(&delete_sql, &error),
            source: error
        )
    });
    if result.is_ok() {
        record_profiled_mutation_operation_duration(
            conn,
            DuckLakeMutationOperationKind::Delete,
            delete_origin,
        );
    }

    let _ = conn.execute_batch(&format!("DROP TABLE IF EXISTS {staging:?}"));
    result
}

/// Returns the number of values carried by a prepared row payload.
fn prepared_rows_count(prepared_rows: &PreparedRows) -> usize {
    match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::Arrow(batches) => batches.iter().map(|batch| batch.num_rows()).sum(),
    }
}

/// Returns the encoding strategy used by a prepared row payload.
fn prepared_rows_kind(prepared_rows: &PreparedRows) -> &'static str {
    match prepared_rows {
        PreparedRows::Appender(_) => "appender",
        PreparedRows::Arrow(_) => "arrow",
    }
}

/// Returns the number of prepared mutation statements in one atomic batch.
fn prepared_mutation_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 1,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations.len(),
    }
}

/// Returns the total number of row and delete entries carried by one batch.
fn batch_item_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 1,
        PreparedDuckLakeTableBatchAction::Mutation(_) => {
            batch_upsert_row_count(batch) + batch_delete_predicate_count(batch)
        }
    }
}

/// Returns the total number of upsert rows carried by one batch.
fn batch_upsert_row_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 0,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations
            .iter()
            .map(|prepared_mutation| match prepared_mutation {
                PreparedTableMutation::Copy(prepared_rows)
                | PreparedTableMutation::Upsert(prepared_rows) => {
                    prepared_rows_count(prepared_rows)
                }
                PreparedTableMutation::Delete { .. } | PreparedTableMutation::DeleteKeys { .. } => {
                    0
                }
            })
            .sum(),
    }
}

/// Returns the total number of delete predicates carried by one batch.
fn batch_delete_predicate_count(batch: &PreparedDuckLakeTableBatch) -> usize {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => 0,
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => prepared_mutations
            .iter()
            .map(|prepared_mutation| match prepared_mutation {
                PreparedTableMutation::Copy(_) | PreparedTableMutation::Upsert(_) => 0,
                PreparedTableMutation::Delete { predicates, .. } => predicates.len(),
                PreparedTableMutation::DeleteKeys { keys, .. } => prepared_rows_count(keys),
            })
            .sum(),
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
        PreparedTableMutation::Copy(prepared_rows) => Some(prepared_rows_count(prepared_rows)),
        PreparedTableMutation::Upsert(prepared_rows) => Some(match prepared_rows {
            PreparedRows::Appender(values) => values.len(),
            PreparedRows::Arrow(batches) => batches.iter().map(|batch| batch.num_rows()).sum(),
        }),
        PreparedTableMutation::Delete { .. } | PreparedTableMutation::DeleteKeys { .. } => None,
    }
}

/// Classifies a prepared batch for concise INFO logging.
fn batch_log_kind(batch: &PreparedDuckLakeTableBatch) -> &'static str {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => "truncate",
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
            match prepared_mutations.as_slice() {
                [PreparedTableMutation::Copy(_)] => "copy",
                [PreparedTableMutation::Upsert(_)] => "insert",
                [PreparedTableMutation::Delete { origin, .. }] => origin,
                [PreparedTableMutation::DeleteKeys { origin, .. }] => origin,
                [
                    PreparedTableMutation::Delete { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                [
                    PreparedTableMutation::DeleteKeys { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                _ => "mutation",
            }
        }
    }
}

#[cfg(feature = "test-utils")]
static FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static FAIL_AFTER_COPY_BATCH_COMMIT_TABLE: LazyLock<Mutex<Option<String>>> =
    LazyLock::new(|| Mutex::new(None));
#[cfg(feature = "test-utils")]
static STAGING_TABLE_CREATIONS_BY_TABLE: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().clear();
}

/// Returns the number of staging-table creations performed for one table since the last reset.
#[cfg(feature = "test-utils")]
pub fn ducklake_staging_table_creations_for_tests(table_name: &str) -> usize {
    STAGING_TABLE_CREATIONS_BY_TABLE
        .lock()
        .get(table_name)
        .copied()
        .unwrap_or_default()
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

/// Injects a synthetic failure after commit so retries must rely on the progress row.
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

    use std::sync::Arc;

    use arrow::array::UInt64Array;
    use duckdb::Connection;
    use etl::table_rows_to_arrow_batch;
    use etl::types::{ColumnSchema, TableId, TableName, Type as PgType};
    use etl_telemetry::metrics::init_metrics_handle;

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

    fn make_arrow_batch(table_schema: &Arc<TableSchema>, rows: &[TableRow]) -> TableArrowBatch {
        table_rows_to_arrow_batch(Arc::clone(table_schema), rows).unwrap()
    }
    fn open_metric_test_connection() -> Connection {
        let conn = Connection::open_in_memory().expect("failed to open metric test duckdb");
        conn.execute_batch(&format!(
            r#"ATTACH ':memory:' AS {LAKE_CATALOG};
               CREATE TABLE {LAKE_CATALOG}."public_users" (
                   id INTEGER NOT NULL,
                   name TEXT
               );
               CREATE TABLE {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" (
                   table_name TEXT NOT NULL,
                   last_commit_lsn UBIGINT NOT NULL,
                   last_tx_ordinal UBIGINT NOT NULL,
                   updated_at TIMESTAMP NOT NULL
               );"#
        ))
        .expect("failed to initialize metric test schema");
        conn.execute_batch(
            r#"
            SET enable_profiling = 'no_output';
            SET profiling_mode = 'standard';
            SET profiling_coverage = 'ALL';
            SET custom_profiling_settings = '{"LATENCY":"true"}';
            "#,
        )
        .expect("failed to enable duckdb profiling for metrics");
        conn
    }

    fn mutation_operation_duration_count(
        rendered: &str,
        operation_kind: DuckLakeMutationOperationKind,
        delete_origin: &str,
    ) -> f64 {
        let operation_kind_label =
            format!(r#"{OPERATION_KIND_LABEL}="{}""#, operation_kind.as_str());
        let delete_origin_label = format!(r#"{DELETE_ORIGIN_LABEL}="{}""#, delete_origin);

        rendered
            .lines()
            .find_map(|line| {
                if line.starts_with(&format!(
                    "{ETL_DUCKLAKE_MUTATION_OPERATION_DURATION_SECONDS}_count"
                )) && line.contains(&operation_kind_label)
                    && line.contains(&delete_origin_label)
                {
                    line.split_whitespace().last()?.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
    }

    fn batch_substage_duration_count(
        rendered: &str,
        batch_kind: &str,
        sub_batch_kind: &str,
        substage: DuckLakeBatchSubstage,
    ) -> f64 {
        let batch_kind_label = format!(r#"{BATCH_KIND_LABEL}="{}""#, batch_kind);
        let sub_batch_kind_label = format!(r#"{SUB_BATCH_KIND_LABEL}="{}""#, sub_batch_kind);
        let substage_label = format!(r#"{SUBSTAGE_LABEL}="{}""#, substage.as_str());

        rendered
            .lines()
            .find_map(|line| {
                if line.starts_with(&format!(
                    "{ETL_DUCKLAKE_BATCH_SUBSTAGE_DURATION_SECONDS}_count"
                )) && line.contains(&batch_kind_label)
                    && line.contains(&sub_batch_kind_label)
                    && line.contains(&substage_label)
                {
                    line.split_whitespace().last()?.parse::<f64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0.0)
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
            _ => panic!("expected delete first"),
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected upsert second"),
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
            _ => panic!("expected delete first"),
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected upsert second"),
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
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(10),
                    PgLsn::from(20),
                    1,
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
                    _ => panic!("expected upsert"),
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
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    1,
                    TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    2,
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
                    0,
                    TableMutation::Delete(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    0,
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
                    _ => panic!("expected delete batch"),
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
                    0,
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
                    0,
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
                    0,
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
                _ => panic!("expected delete batch"),
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), 1);
                }
                _ => panic!("expected delete batch"),
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
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(110),
                    PgLsn::from(120),
                    1,
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
                    2,
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
    fn test_retain_mutations_after_sequence_key_drops_applied_prefix() {
        let retained = retain_mutations_after_sequence_key(
            vec![
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(110),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("one".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(120),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("two".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(130),
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(3),
                        Cell::String("three".to_string()),
                    ])),
                ),
            ],
            Some(EventSequenceKey::new(PgLsn::from(120), 0)),
        );

        assert_eq!(retained.len(), 1);
        assert_eq!(
            retained[0].sequence_key(),
            EventSequenceKey::new(PgLsn::from(130), 0)
        );
    }

    #[test]
    fn test_retain_truncates_after_sequence_key_drops_applied_prefix() {
        let retained = retain_truncates_after_sequence_key(
            vec![
                TrackedTruncateEvent::new(PgLsn::from(100), PgLsn::from(200), 0, 0),
                TrackedTruncateEvent::new(PgLsn::from(100), PgLsn::from(200), 1, 0),
                TrackedTruncateEvent::new(PgLsn::from(100), PgLsn::from(210), 0, 0),
            ],
            Some(EventSequenceKey::new(PgLsn::from(200), 0)),
        );

        assert_eq!(retained.len(), 2);
        assert_eq!(
            retained[0].sequence_key(),
            EventSequenceKey::new(PgLsn::from(200), 1)
        );
        assert_eq!(
            retained[1].sequence_key(),
            EventSequenceKey::new(PgLsn::from(210), 0)
        );
    }

    #[test]
    fn test_prepare_change_table_batches_keeps_update_pairs_together_at_cap() {
        let table_schema = Arc::new(make_schema());
        let insert_rows = (0..(CDC_MUTATION_BATCH_SIZE - 1))
            .map(|idx| {
                TableRow::new(vec![
                    Cell::I32(idx as i32),
                    Cell::String(format!("i-{idx}")),
                ])
            })
            .collect::<Vec<_>>();
        let old_row = TableRow::new(vec![Cell::I32(999), Cell::String("before".to_string())]);
        let new_row = TableRow::new(vec![Cell::I32(999), Cell::String("after".to_string())]);

        let change_set = TableChangeSet {
            table_id: table_schema.id,
            groups: vec![
                ChangeArrowBatch {
                    rows: make_arrow_batch(&table_schema, &insert_rows),
                    change: ChangeKind::Insert,
                    row_image: RowImage::New,
                    commit_lsns: UInt64Array::from(vec![10_u64; insert_rows.len()]),
                    tx_ordinals: UInt64Array::from(vec![0_u64; insert_rows.len()]),
                },
                ChangeArrowBatch {
                    rows: make_arrow_batch(&table_schema, std::slice::from_ref(&old_row)),
                    change: ChangeKind::Update,
                    row_image: RowImage::Old { key_only: false },
                    commit_lsns: UInt64Array::from(vec![11_u64]),
                    tx_ordinals: UInt64Array::from(vec![1_u64]),
                },
                ChangeArrowBatch {
                    rows: make_arrow_batch(&table_schema, std::slice::from_ref(&new_row)),
                    change: ChangeKind::Update,
                    row_image: RowImage::New,
                    commit_lsns: UInt64Array::from(vec![11_u64]),
                    tx_ordinals: UInt64Array::from(vec![1_u64]),
                },
            ],
        };

        let batches = prepare_change_table_batches(
            table_schema.as_ref(),
            "public_users".to_string(),
            change_set,
        )
        .unwrap();

        assert_eq!(batches.len(), 2);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 1);
                match &prepared[0] {
                    PreparedTableMutation::Upsert(PreparedRows::Arrow(rows)) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].num_rows(), CDC_MUTATION_BATCH_SIZE - 1);
                    }
                    _ => panic!("expected insert upsert batch"),
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => {
                assert_eq!(prepared.len(), 2);
                assert!(matches!(
                    prepared[0],
                    PreparedTableMutation::DeleteKeys {
                        origin: "update",
                        ..
                    }
                ));
                match &prepared[1] {
                    PreparedTableMutation::Upsert(PreparedRows::Arrow(rows)) => {
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].num_rows(), 1);
                    }
                    _ => panic!("expected update upsert batch"),
                }
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
                0,
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_string()),
                ])),
            ),
            TrackedTableMutation::new(
                PgLsn::from(100),
                PgLsn::from(200),
                1,
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
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    1,
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
                    0,
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_string()),
                    ])),
                ),
                TrackedTableMutation::new(
                    PgLsn::from(100),
                    PgLsn::from(200),
                    1,
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
                0,
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
                0,
            )],
        );
        let second = build_truncate_batch_identity(
            "public_users",
            &[TrackedTruncateEvent::new(
                PgLsn::from(301),
                PgLsn::from(401),
                0,
                0,
            )],
        );

        assert_ne!(first.batch_id, second.batch_id);
    }

    #[tokio::test]
    async fn test_apply_table_batch_records_mutation_duration_histogram_by_operation_kind() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        crate::ducklake::metrics::register_metrics();

        let conn = open_metric_test_connection();
        conn.execute_batch(&format!(
            r#"INSERT INTO {LAKE_CATALOG}."public_users" VALUES (1, 'before');"#
        ))
        .expect("failed to seed mutation metric test row");

        let batch = prepare_mutation_table_batches(
            &make_schema(),
            "public_users".to_string(),
            vec![TrackedTableMutation::new(
                PgLsn::from(10),
                PgLsn::from(20),
                0,
                TableMutation::Update {
                    delete_row: TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("before".to_string()),
                    ]),
                    upsert_row: TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("after".to_string()),
                    ]),
                },
            )],
        )
        .expect("failed to prepare mutation batch")
        .into_iter()
        .next()
        .expect("expected prepared mutation batch");

        let rendered_before = handle.render();
        let insert_before = mutation_operation_duration_count(
            &rendered_before,
            DuckLakeMutationOperationKind::Insert,
            DELETE_ORIGIN_NONE,
        );
        let delete_before = mutation_operation_duration_count(
            &rendered_before,
            DuckLakeMutationOperationKind::Delete,
            "update",
        );

        apply_table_batch(&conn, &batch).expect("failed to apply mutation batch");

        let rendered_after = handle.render();
        let insert_after = mutation_operation_duration_count(
            &rendered_after,
            DuckLakeMutationOperationKind::Insert,
            DELETE_ORIGIN_NONE,
        );
        let delete_after = mutation_operation_duration_count(
            &rendered_after,
            DuckLakeMutationOperationKind::Delete,
            "update",
        );

        assert!(
            insert_after > insert_before,
            "insert mutation duration count did not increase"
        );
        assert!(
            delete_after > delete_before,
            "delete mutation duration count did not increase"
        );
        assert_eq!(
            conn.query_row(
                &format!(r#"SELECT name FROM {LAKE_CATALOG}."public_users" WHERE id = 1 LIMIT 1;"#),
                [],
                |row| row.get::<_, String>(0),
            )
            .expect("failed to read updated metric test row"),
            "after".to_string()
        );
    }

    #[tokio::test]
    async fn test_apply_table_batch_records_one_delete_duration_sample_per_delete_statement() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        crate::ducklake::metrics::register_metrics();

        let conn = open_metric_test_connection();
        for id in 0..=SQL_DELETE_BATCH_SIZE {
            conn.execute_batch(&format!(
                r#"INSERT INTO {LAKE_CATALOG}."public_users" VALUES ({id}, 'before');"#
            ))
            .expect("failed to seed delete metric test row");
        }

        let predicates = (0..=SQL_DELETE_BATCH_SIZE)
            .map(|id| format!("id = {id}"))
            .collect::<Vec<_>>();
        let batch = PreparedDuckLakeTableBatch {
            table_name: "public_users".to_string(),
            batch_id: "mutation:test-delete-chunks".to_string(),
            batch_kind: DuckLakeTableBatchKind::Mutation,
            first_start_lsn: Some(PgLsn::from(10)),
            last_commit_lsn: Some(PgLsn::from(20)),
            first_sequence_key: Some(EventSequenceKey::new(PgLsn::from(20), 0)),
            last_sequence_key: Some(EventSequenceKey::new(PgLsn::from(20), 0)),
            action: PreparedDuckLakeTableBatchAction::Mutation(vec![
                PreparedTableMutation::Delete {
                    predicates,
                    origin: "delete",
                },
            ]),
        };

        let rendered_before = handle.render();
        let delete_before = mutation_operation_duration_count(
            &rendered_before,
            DuckLakeMutationOperationKind::Delete,
            "delete",
        );

        apply_table_batch(&conn, &batch).expect("failed to apply delete batch");

        let rendered_after = handle.render();
        let delete_after = mutation_operation_duration_count(
            &rendered_after,
            DuckLakeMutationOperationKind::Delete,
            "delete",
        );

        assert_eq!(
            delete_after - delete_before,
            2.0,
            "delete mutation duration count should increase once per chunked delete statement"
        );
        assert_eq!(
            conn.query_row(
                &format!(r#"SELECT COUNT(*) FROM {LAKE_CATALOG}."public_users";"#),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("failed to count remaining delete metric test rows"),
            0
        );
    }

    #[tokio::test]
    async fn test_apply_table_batch_records_batch_substage_duration_histogram() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        crate::ducklake::metrics::register_metrics();

        let conn = open_metric_test_connection();
        conn.execute_batch(&format!(
            r#"INSERT INTO {LAKE_CATALOG}."public_users" VALUES (1, 'before');"#
        ))
        .expect("failed to seed substage metric test row");

        let batch = prepare_mutation_table_batches(
            &make_schema(),
            "public_users".to_string(),
            vec![TrackedTableMutation::new(
                PgLsn::from(10),
                PgLsn::from(20),
                0,
                TableMutation::Update {
                    delete_row: TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("before".to_string()),
                    ]),
                    upsert_row: TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("after".to_string()),
                    ]),
                },
            )],
        )
        .expect("failed to prepare substage metric batch")
        .into_iter()
        .next()
        .expect("expected prepared substage metric batch");

        let rendered_before = handle.render();
        let staging_prepare_before = batch_substage_duration_count(
            &rendered_before,
            "mutation",
            "update",
            DuckLakeBatchSubstage::StagingPrepare,
        );
        let staging_load_before = batch_substage_duration_count(
            &rendered_before,
            "mutation",
            "update",
            DuckLakeBatchSubstage::StagingLoad,
        );
        let progress_update_before = batch_substage_duration_count(
            &rendered_before,
            "mutation",
            "update",
            DuckLakeBatchSubstage::ProgressUpdate,
        );
        let commit_only_before = batch_substage_duration_count(
            &rendered_before,
            "mutation",
            "update",
            DuckLakeBatchSubstage::CommitOnly,
        );

        apply_table_batch(&conn, &batch).expect("failed to apply substage metric batch");

        let rendered_after = handle.render();
        let staging_prepare_after = batch_substage_duration_count(
            &rendered_after,
            "mutation",
            "update",
            DuckLakeBatchSubstage::StagingPrepare,
        );
        let staging_load_after = batch_substage_duration_count(
            &rendered_after,
            "mutation",
            "update",
            DuckLakeBatchSubstage::StagingLoad,
        );
        let progress_update_after = batch_substage_duration_count(
            &rendered_after,
            "mutation",
            "update",
            DuckLakeBatchSubstage::ProgressUpdate,
        );
        let commit_only_after = batch_substage_duration_count(
            &rendered_after,
            "mutation",
            "update",
            DuckLakeBatchSubstage::CommitOnly,
        );

        assert!(
            staging_prepare_after > staging_prepare_before,
            "staging prepare substage count did not increase"
        );
        assert!(
            staging_load_after > staging_load_before,
            "staging load substage count did not increase"
        );
        assert!(
            progress_update_after > progress_update_before,
            "progress update substage count did not increase"
        );
        assert!(
            commit_only_after > commit_only_before,
            "commit-only substage count did not increase"
        );
    }

    #[tokio::test]
    async fn test_apply_table_batch_records_replace_delete_duration_under_replace_origin() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        crate::ducklake::metrics::register_metrics();

        let conn = open_metric_test_connection();
        conn.execute_batch(&format!(
            r#"INSERT INTO {LAKE_CATALOG}."public_users" VALUES (1, 'before');"#
        ))
        .expect("failed to seed replace metric test row");

        let batch = prepare_mutation_table_batches(
            &make_schema(),
            "public_users".to_string(),
            vec![TrackedTableMutation::new(
                PgLsn::from(10),
                PgLsn::from(20),
                0,
                TableMutation::Replace(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("after".to_string()),
                ])),
            )],
        )
        .expect("failed to prepare replace metric batch")
        .into_iter()
        .next()
        .expect("expected prepared replace metric batch");

        let rendered_before = handle.render();
        let replace_delete_before = mutation_operation_duration_count(
            &rendered_before,
            DuckLakeMutationOperationKind::Delete,
            "replace",
        );

        apply_table_batch(&conn, &batch).expect("failed to apply replace metric batch");

        let rendered_after = handle.render();
        let replace_delete_after = mutation_operation_duration_count(
            &rendered_after,
            DuckLakeMutationOperationKind::Delete,
            "replace",
        );

        assert!(
            replace_delete_after > replace_delete_before,
            "replace delete mutation duration count did not increase"
        );
    }
}
