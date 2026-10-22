//! Table batches are the atomic per-table write units used by DuckLake writes.
//! Copy, mutation, and truncate inputs are normalized into prepared batches so
//! each attempt can replay the same SQL and replay bookkeeping. Copy batches
//! receive opaque upstream ids and retain them across destination-local
//! retries. Streaming mutation and truncate batches advance a per-table
//! progress watermark. Bounded batch sizes preserve table-local ordering
//! without letting one transaction grow unbounded.

#[cfg(feature = "test-utils")]
use std::sync::LazyLock;
use std::{
    collections::HashMap,
    error, fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use etl::{
    data::{Cell, OldTableRow, PartialTableRow, SizeHint, TableRow, UpdatedTableRow},
    destination::TableCopyBatchId,
    error::{ErrorKind, EtlResult, Retryability},
    etl_error,
    event::EventPosition,
    schema::ReplicatedTableSchema,
};
use etl_maintenance::ducklake::RedactedDuckDbError;
use metrics::{counter, histogram};
#[cfg(feature = "test-utils")]
use parking_lot::Mutex;
use pg_escape::quote_literal;
use rand::Rng;
use tokio::{sync::Semaphore, time::Instant};
use tokio_postgres::types::PgLsn;
use tracing::{Instrument, debug, trace, warn};

use crate::{
    ducklake::{
        DUCKLAKE_COLUMN_NAME_MAPPING, DuckLakeTableName, LAKE_CATALOG,
        client::{
            DuckLakeBlockingOperationContext, DuckLakeConnectionManager, FOREGROUND_QUERY_TIMEOUT,
            format_query_error_detail, is_ducklake_shutdown_requested_error, run_duckdb_blocking,
            run_duckdb_blocking_with_timeout_and_context,
        },
        core::{DuckLakeWriteGuard, is_create_table_conflict},
        duckdb_error::{DuckDbErrorType, duckdb_error_retryability, duckdb_error_type},
        encoding::{
            PreparedRows, cell_to_sql_literal_ref, prepare_copy_rows, prepare_rows,
            table_row_to_sql_literal_ref,
        },
        metrics::{
            BATCH_KIND_LABEL, DELETE_ORIGIN_LABEL, ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS, ETL_DUCKLAKE_DELETE_PREDICATES,
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL, ETL_DUCKLAKE_REPLAYED_BATCHES_TOTAL,
            ETL_DUCKLAKE_RETRIES_TOTAL, ETL_DUCKLAKE_UPSERT_ROWS, PREPARED_ROWS_KIND_LABEL,
            RETRY_SCOPE_LABEL, SUB_BATCH_KIND_LABEL,
        },
        replay_epoch::LEGACY_REPLAY_EPOCH,
        sql::{qualified_lake_table_name, quote_identifier},
    },
    retry::{
        ClassifiedError, RetryAttempt, RetryPolicy, classified_error, classified_error_with_detail,
        retry_with_backoff,
    },
};

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
/// Keeping mixed insert/delete/update streams in the same batch improves insert
/// throughput on interleaved workloads while still capping transaction lifetime
/// for DuckLake conflict handling.
const CDC_MUTATION_BATCH_SIZE: usize = 16;
/// ETL-managed marker table storing per-table applied copy batches.
const APPLIED_BATCHES_TABLE: &str = "__etl_applied_table_batches";
/// Data inlining limit for append-only DuckLake helper tables.
///
/// This per-table option intentionally overrides the COPY pool's attach-level
/// limit of zero. Helper markers and progress stay inline, while rows written
/// to the replicated table during COPY still become Parquet files. Maintenance
/// performs helper-table deletions while foreground mutations are paused.
const HELPER_TABLE_DATA_INLINING_ROW_LIMIT: usize = 256;
/// Replay epoch column shared by ETL helper tables.
const REPLAY_EPOCH_COLUMN: &str = "replay_epoch";

/// Formats an optional LSN without using debug output.
fn format_optional_lsn(lsn: Option<PgLsn>) -> String {
    lsn.map_or_else(|| "none".to_owned(), |lsn| lsn.to_string())
}

/// Formats an event position using DuckLake's existing fixed-width hexadecimal
/// representation.
fn format_event_position(event_position: EventPosition) -> String {
    let commit_lsn = u64::from(event_position.commit_lsn);
    format!("{commit_lsn:016x}/{:016x}", event_position.tx_ordinal)
}

/// Formats an optional event position without using debug output.
fn format_optional_event_position(event_position: Option<EventPosition>) -> String {
    event_position.map_or_else(|| "none".to_owned(), format_event_position)
}

/// Sanitized DuckDB query failure for statements that may contain row values.
///
/// Keeps the DuckDB exception type and its classification, which carry no row
/// values, and drops the message.
#[derive(Debug)]
struct DuckDbSensitiveQueryError {
    error_type: Option<DuckDbErrorType>,
    retryability: Retryability,
}

impl DuckDbSensitiveQueryError {
    /// Sanitizes one failed DuckDB call.
    fn new(error: &duckdb::Error) -> Self {
        Self {
            error_type: duckdb_error_type(error),
            retryability: duckdb_error_retryability(error),
        }
    }

    /// Returns whether DuckDB interrupted the query.
    fn is_interrupt(&self) -> bool {
        self.error_type == Some(DuckDbErrorType::Interrupt)
    }
}

impl fmt::Display for DuckDbSensitiveQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.error_type {
            Some(error_type) => write!(
                f,
                "DuckDB query failed with a {error_type:?} error; error message omitted because \
                 it may contain row values"
            ),
            None => write!(
                f,
                "DuckDB query failed; error message omitted because it may contain row values"
            ),
        }
    }
}

impl error::Error for DuckDbSensitiveQueryError {}

/// Classified like the DuckDB error it replaces.
impl ClassifiedError for DuckDbSensitiveQueryError {
    fn retryability(&self) -> Retryability {
        self.retryability
    }
}

/// Formats query context for a delete mutation without row values.
fn format_delete_mutation_error_detail(
    target_table: &str,
    predicate_count: usize,
    chunk_index: usize,
    chunk_count: usize,
    chunk_predicate_count: usize,
) -> String {
    format!(
        "sql: DELETE FROM {target_table} WHERE [redacted predicates]; predicate_count: \
         {predicate_count}; chunk_index: {chunk_index}; chunk_count: {chunk_count}; \
         chunk_predicate_count: {chunk_predicate_count}"
    )
}

/// Formats query context for an update mutation without row values.
fn format_update_mutation_error_detail(
    target_table: &str,
    assignment_count: usize,
    has_predicate: bool,
) -> String {
    format!(
        "sql: UPDATE {target_table} SET [redacted assignments] WHERE [redacted predicate]; \
         assignment_count: {assignment_count}; has_predicate: {has_predicate}"
    )
}

/// ETL-managed per-table streaming replay progress for steady-state CDC
/// retries.
const STREAMING_PROGRESS_TABLE: &str = "__etl_streaming_progress";
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
    Delete(OldTableRow),
    Update { delete_row: OldTableRow, new_row: UpdatedTableRow },
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
    Update {
        // For SET clause assignments used in UPDATE statements. Example: "value=1, id=3"
        assignments: Vec<String>,
        // For the WHERE clause predicate used in the UPDATE statement. Example: "id = 4 AND
        // content = 'hello'"
        predicate: String,
    },
}

/// Borrowed row shape used to build delete predicates.
enum DeletePredicateRowRef<'a> {
    Full(&'a TableRow),
    Key(&'a TableRow),
}

impl<'a> From<&'a TableRow> for DeletePredicateRowRef<'a> {
    fn from(value: &'a TableRow) -> Self {
        Self::Full(value)
    }
}

impl<'a> From<&'a OldTableRow> for DeletePredicateRowRef<'a> {
    fn from(value: &'a OldTableRow) -> Self {
        match value {
            OldTableRow::Full(row) => Self::Full(row),
            OldTableRow::Key(row) => Self::Key(row),
        }
    }
}

/// Event-level table mutation annotated for idempotent replay.
pub(super) struct TrackedTableMutation {
    event_position: EventPosition,
    mutation: TableMutation,
}

impl TrackedTableMutation {
    /// Creates one tracked mutation preserved for retry-safe replay.
    pub(super) fn new(event_position: EventPosition, mutation: TableMutation) -> Self {
        Self { event_position, mutation }
    }

    /// Returns the stable event position for this mutation.
    fn event_position(&self) -> EventPosition {
        self.event_position
    }
}

/// Truncate event metadata preserved for idempotent replay.
#[derive(Clone, Copy)]
pub(super) struct TrackedTruncateEvent {
    event_position: EventPosition,
    options: i8,
}

impl TrackedTruncateEvent {
    /// Creates one tracked truncate event preserved for retry-safe replay.
    pub(super) fn new(event_position: EventPosition, options: i8) -> Self {
        Self { event_position, options }
    }

    /// Returns the stable event position for this truncate.
    fn event_position(&self) -> EventPosition {
        self.event_position
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
    CopyComplete,
    Mutation,
    Truncate,
}

impl DuckLakeTableBatchKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::CopyComplete => "copy_complete",
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

/// Returns destination-visible column names in replicated write order.
fn replicated_column_names(replicated_table_schema: &ReplicatedTableSchema) -> Vec<String> {
    replicated_table_schema
        .destination_column_schemas(DUCKLAKE_COLUMN_NAME_MAPPING)
        .map(|column| column.name)
        .collect()
}

/// Prepared per-table work executed atomically in one DuckLake transaction.
enum PreparedDuckLakeTableBatchAction {
    Mutation(Vec<PreparedTableMutation>),
    Truncate,
}

/// Test-only transaction hook, scoped to one batch rather than global state.
#[cfg(test)]
type BatchTestHook = Arc<
    dyn Fn(&duckdb::Connection, &DuckLakeBlockingOperationContext) -> EtlResult<()> + Send + Sync,
>;

/// Prepared atomic DuckLake table batch with replay metadata.
pub(super) struct PreparedDuckLakeTableBatch {
    /// Pauses or faults this batch's transaction in deterministic unit tests.
    #[cfg(test)]
    test_hook: Option<BatchTestHook>,
    table_name: DuckLakeTableName,
    replay_epoch: String,
    batch_id: String,
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
    first_event_position: Option<EventPosition>,
    last_event_position: Option<EventPosition>,
    insert_column_names: Vec<String>,
    action: PreparedDuckLakeTableBatchAction,
}

/// Compact durable marker retained while COPY rows remain staged.
struct PreparedDuckLakeBatchMarker {
    batch_kind: DuckLakeTableBatchKind,
    first_start_lsn: Option<PgLsn>,
    last_commit_lsn: Option<PgLsn>,
}

/// Prepared initial-copy rows with a lightweight size estimate for buffering.
pub(super) struct PreparedDuckLakeCopyBatch {
    batch: PreparedDuckLakeTableBatch,
    estimated_bytes: u64,
}

impl PreparedDuckLakeCopyBatch {
    /// Returns the approximate decoded bytes owned by this row batch.
    pub(super) fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// Converts this prepared copy payload into the existing atomic-batch path.
    pub(super) fn into_atomic_batch(self) -> PreparedDuckLakeTableBatch {
        self.batch
    }

    /// Separates staged row data from its durable applied-batch marker.
    fn into_buffer_parts(
        mut self,
    ) -> EtlResult<(String, PreparedDuckLakeBatchMarker, PreparedRows)> {
        let PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) = &mut self.batch.action
        else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy batch is not a mutation"
            ));
        };
        if prepared_mutations.len() != 1 {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy batch has an invalid mutation count",
                format!("Expected 1 mutation, got {}", prepared_mutations.len())
            ));
        }
        let prepared_mutation = prepared_mutations.pop().ok_or_else(|| {
            etl_error!(ErrorKind::InvalidState, "DuckLake buffered copy mutation is missing")
        })?;
        let PreparedTableMutation::Upsert(prepared_rows) = prepared_mutation else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy mutation is not an upsert"
            ));
        };

        let marker = PreparedDuckLakeBatchMarker {
            batch_kind: self.batch.batch_kind,
            first_start_lsn: self.batch.first_start_lsn,
            last_commit_lsn: self.batch.last_commit_lsn,
        };

        Ok((self.batch.batch_id, marker, prepared_rows))
    }
}

impl PreparedDuckLakeTableBatch {
    /// Returns the destination table this batch targets.
    pub(super) fn table_name(&self) -> &DuckLakeTableName {
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
    last_event_position: EventPosition,
}

/// Replay progress retained while the sequence's table write guard is held.
#[derive(Clone, Debug, Default)]
struct TableStreamingProgressCache {
    /// `None` requires a catalog read; `Some(None)` records an empty catalog.
    progress: Option<Option<TableStreamingProgress>>,
    /// Counts real catalog reads across successful and failed test attempts.
    #[cfg(test)]
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

/// Ensures the ETL-managed replay marker table exists.
pub(super) async fn ensure_applied_batches_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    applied_batches_table_created: Arc<AtomicBool>,
    guard: impl Send + 'static,
) -> EtlResult<()> {
    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
    })?;

    if applied_batches_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}" (
             table_name VARCHAR NOT NULL,
             replay_epoch VARCHAR,
             batch_id VARCHAR NOT NULL,
             batch_kind VARCHAR NOT NULL,
             first_start_lsn UBIGINT,
             last_commit_lsn UBIGINT,
             applied_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&applied_batches_table_created);
    let table_name = APPLIED_BATCHES_TABLE.to_owned();

    run_duckdb_blocking(pool, blocking_slots, move |conn| -> EtlResult<()> {
        let _guard = guard;
        let _table_creation_permit = table_creation_permit;
        match conn.execute_batch(&ddl) {
            Ok(()) => {}
            Err(error) if is_create_table_conflict(&error, &table_name) => {}
            Err(error) => {
                return Err(classified_error_with_detail(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    format_query_error_detail(&ddl),
                    RedactedDuckDbError::from(error),
                ));
            }
        }
        ensure_helper_table_replay_epoch_column(conn, APPLIED_BATCHES_TABLE)?;

        let set_option_sql = format!(
            "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
            HELPER_TABLE_DATA_INLINING_ROW_LIMIT,
            quote_literal(APPLIED_BATCHES_TABLE),
        );
        conn.execute_batch(&set_option_sql).map_err(|err| {
            classified_error_with_detail(
                ErrorKind::DestinationQueryFailed,
                "DuckLake set_option failed",
                format_query_error_detail(&set_option_sql),
                RedactedDuckDbError::from(err),
            )
        })?;

        created.store(true, Ordering::Relaxed);
        Ok(())
    })
    .await
}

/// Ensures the ETL-managed streaming progress table exists.
pub(super) async fn ensure_streaming_progress_table_exists(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    table_creation_slots: Arc<Semaphore>,
    streaming_progress_table_created: Arc<AtomicBool>,
    guard: impl Send + 'static,
) -> EtlResult<()> {
    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let table_creation_permit = table_creation_slots.acquire_owned().await.map_err(|_| {
        etl_error!(ErrorKind::InvalidState, "DuckLake table creation semaphore closed")
    })?;

    if streaming_progress_table_created.load(Ordering::Relaxed) {
        return Ok(());
    }

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}" (
             table_name VARCHAR NOT NULL,
             replay_epoch VARCHAR,
             last_commit_lsn UBIGINT NOT NULL,
             last_tx_ordinal UBIGINT NOT NULL,
             updated_at TIMESTAMPTZ NOT NULL
             );"#
    );
    let created = Arc::clone(&streaming_progress_table_created);
    let table_name = STREAMING_PROGRESS_TABLE.to_owned();

    run_duckdb_blocking(pool, blocking_slots, move |conn| -> EtlResult<()> {
        let _guard = guard;
        let _table_creation_permit = table_creation_permit;
        match conn.execute_batch(&ddl) {
            Ok(()) => {}
            Err(err) if is_create_table_conflict(&err, &table_name) => {}
            Err(err) => {
                return Err(classified_error_with_detail(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake CREATE TABLE failed",
                    format_query_error_detail(&ddl),
                    RedactedDuckDbError::from(err),
                ));
            }
        }
        ensure_helper_table_replay_epoch_column(conn, STREAMING_PROGRESS_TABLE)?;

        let set_option_sql = format!(
            "CALL {LAKE_CATALOG}.set_option('data_inlining_row_limit', {}, table_name => {});",
            HELPER_TABLE_DATA_INLINING_ROW_LIMIT,
            quote_literal(STREAMING_PROGRESS_TABLE),
        );
        conn.execute_batch(&set_option_sql).map_err(|error| {
            classified_error_with_detail(
                ErrorKind::DestinationQueryFailed,
                "DuckLake set_option failed",
                format_query_error_detail(&set_option_sql),
                RedactedDuckDbError::from(error),
            )
        })?;

        created.store(true, Ordering::Relaxed);
        Ok(())
    })
    .await
}

/// Adds the replay epoch column to helper tables created by older versions.
fn ensure_helper_table_replay_epoch_column(
    conn: &duckdb::Connection,
    table_name: &str,
) -> EtlResult<()> {
    if helper_table_has_column(conn, table_name, REPLAY_EPOCH_COLUMN)? {
        return Ok(());
    }

    let table_name = format!(r#"{LAKE_CATALOG}."{table_name}""#);
    let column_name = quote_identifier(REPLAY_EPOCH_COLUMN);
    let sql = format!("ALTER TABLE {table_name} ADD COLUMN {column_name} VARCHAR;");
    conn.execute_batch(&sql).map_err(|source| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table migration failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(source),
        )
    })?;

    Ok(())
}

fn helper_table_has_column(
    conn: &duckdb::Connection,
    table_name: &str,
    column_name: &str,
) -> EtlResult<bool> {
    let sql = format!(
        "SELECT 1 FROM information_schema.columns WHERE table_catalog = {} AND table_name = {} \
         AND column_name = {} LIMIT 1;",
        quote_literal(LAKE_CATALOG),
        quote_literal(table_name),
        quote_literal(column_name)
    );
    let mut statement = conn.prepare(&sql).map_err(|source| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table schema lookup failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(source),
        )
    })?;
    let mut rows = statement.query([]).map_err(|source| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table schema lookup failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(source),
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|source| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake helper table schema row fetch failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(source),
        )
    })
}

/// Applies ordered atomic batches with independent deadlines and retry budgets.
/// The write guard remains held across the sequence, including retry backoff.
pub(super) async fn apply_table_batches_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batches: Vec<PreparedDuckLakeTableBatch>,
    write_guard: DuckLakeWriteGuard,
) -> EtlResult<()> {
    apply_table_batches_with_policy(
        pool,
        blocking_slots,
        batches,
        write_guard,
        FOREGROUND_QUERY_TIMEOUT,
        batch_retry_policy(),
    )
    .await
    .map(|_| ())
}

/// Returns the retry policy shared by sequence and standalone atomic batches.
fn batch_retry_policy() -> RetryPolicy {
    RetryPolicy {
        max_retries: MAX_COMMIT_RETRIES,
        initial_delay: Duration::from_millis(INITIAL_RETRY_DELAY_MS),
        max_delay: Duration::from_millis(
            MAX_RETRY_DELAY_MS.max(TRANSIENT_DELETE_FILE_RETRY_DELAY_MS),
        ),
    }
}

/// Runs a sequence with explicit per-attempt limits, also used by deadline
/// tests.
async fn apply_table_batches_with_policy(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batches: Vec<PreparedDuckLakeTableBatch>,
    write_guard: DuckLakeWriteGuard,
    timeout: Duration,
    retry_policy: RetryPolicy,
) -> EtlResult<TableStreamingProgressCache> {
    let batch_count = batches.len();
    let sequence_started = Instant::now();
    let mut streaming_progress = TableStreamingProgressCache::default();
    for (batch_index, batch) in batches.into_iter().enumerate() {
        let span = tracing::info_span!(
            "ducklake_table_batch",
            table = %batch.table_name,
            batch_id = %batch.batch_id,
            batch_index = batch_index + 1,
            batch_count,
            completed_batches = batch_index,
        );
        let result = apply_table_batch_with_policy(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            batch,
            write_guard.clone(),
            timeout,
            retry_policy,
            streaming_progress,
        )
        .instrument(span.clone())
        .await;
        let _entered = span.enter();
        if result.is_err() {
            warn!(
                sequence_elapsed_ms = sequence_started.elapsed().as_millis() as u64,
                "ducklake table batch sequence failed"
            );
        } else {
            debug!(
                sequence_elapsed_ms = sequence_started.elapsed().as_millis() as u64,
                completed_batches = batch_index + 1,
                "ducklake table batch sequence advanced"
            );
        }
        streaming_progress = result?;
    }
    Ok(streaming_progress)
}

/// Applies one atomic per-table batch and retries on failure.
pub(super) async fn apply_table_batch_with_retry(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
    write_guard: DuckLakeWriteGuard,
) -> EtlResult<()> {
    apply_table_batch_with_policy(
        pool,
        blocking_slots,
        batch,
        write_guard,
        FOREGROUND_QUERY_TIMEOUT,
        batch_retry_policy(),
        TableStreamingProgressCache::default(),
    )
    .await
    .map(|_| ())
}

/// Retries one atomic batch, giving each attempt a new bounded native
/// operation.
async fn apply_table_batch_with_policy(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    batch: PreparedDuckLakeTableBatch,
    write_guard: DuckLakeWriteGuard,
    timeout: Duration,
    retry_policy: RetryPolicy,
    streaming_progress: TableStreamingProgressCache,
) -> EtlResult<TableStreamingProgressCache> {
    let batch_started = Instant::now();
    let table_name = batch.table_name.clone();
    let batch_id = batch.batch_id.clone();
    let batch_kind = batch.batch_kind;
    let batch = Arc::new(batch);
    let mut first_attempt = true;

    retry_with_backoff(
        retry_policy,
        is_ducklake_shutdown_requested_error,
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
                batch_retry_elapsed_ms = batch_started.elapsed().as_millis() as u64,
                table = %table_name,
                batch_id = %batch_id,
                error = %attempt.error,
                "ducklake table mutation attempt failed, retrying"
            );
        },
        move || {
            let mut streaming_progress = streaming_progress.clone();
            if !first_attempt {
                // An interrupted commit can have reached durable storage.
                // Resolve its outcome from the catalog before replaying it.
                streaming_progress.progress = None;
            }
            first_attempt = false;
            let attempt_batch = Arc::clone(&batch);
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            let write_guard = write_guard.clone();
            let span = tracing::Span::current();
            async move {
                run_duckdb_blocking_with_timeout_and_context(
                    pool,
                    blocking_slots,
                    timeout,
                    move |conn, context| {
                        let _entered = span.enter();
                        let _write_guard = write_guard;
                        if batch_kind == DuckLakeTableBatchKind::Copy {
                            #[cfg(feature = "test-utils")]
                            crate::ducklake::core::wait_if_copy_append_paused_for_tests();
                            if applied_batch_marker_exists(conn, attempt_batch.as_ref())? {
                                record_replayed_batch_skip(attempt_batch.as_ref());
                                return Ok(streaming_progress);
                            }

                            apply_table_batch(conn, attempt_batch.as_ref(), context)?;
                            return Ok(streaming_progress);
                        }

                        apply_table_batches(
                            conn,
                            std::slice::from_ref(attempt_batch.as_ref()),
                            context,
                            &mut streaming_progress,
                        )?;
                        Ok(streaming_progress)
                    },
                )
                .await
            }
        },
    )
    .await
    .map_err(|failure| {
        if is_ducklake_shutdown_requested_error(&failure.last_error) {
            return failure.last_error;
        }

        counter!(
            ETL_DUCKLAKE_FAILED_BATCHES_TOTAL,
            BATCH_KIND_LABEL => batch_kind.as_str(),
            RETRY_SCOPE_LABEL => "single_batch",
        )
        .increment(1);
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake atomic table batch failed after retries",
            format!("table={table_name}, batch_id={batch_id}, batch_kind={}", batch_kind.as_str()),
            failure.last_error,
        )
    })
}

/// Prepares ordered atomic batches for one table's CDC mutations.
///
/// Mutations stay in source order and are split only at the batch-size cap so
/// mixed CDC streams can commit larger insert groups without breaking atomic
/// ordering.
pub(super) fn prepare_mutation_table_batches(
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    replay_epoch: String,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<Vec<PreparedDuckLakeTableBatch>> {
    let mut prepared_batches = Vec::new();
    let mut pending_mutations = Vec::new();

    for tracked_mutation in tracked_mutations {
        pending_mutations.push(tracked_mutation);
        if pending_mutations.len() >= CDC_MUTATION_BATCH_SIZE {
            push_prepared_mutation_batch(
                &mut prepared_batches,
                replicated_table_schema,
                &table_name,
                &replay_epoch,
                std::mem::take(&mut pending_mutations),
            )?;
        }
    }

    push_prepared_mutation_batch(
        &mut prepared_batches,
        replicated_table_schema,
        &table_name,
        &replay_epoch,
        pending_mutations,
    )?;

    Ok(prepared_batches)
}

/// Prepares one retry-safe atomic batch for a table-copy row chunk.
pub(super) fn prepare_copy_table_batch(
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: DuckLakeTableName,
    replay_epoch: String,
    batch_id: TableCopyBatchId,
    table_rows: Vec<TableRow>,
) -> EtlResult<PreparedDuckLakeCopyBatch> {
    let estimated_bytes =
        table_rows.iter().map(SizeHint::size_hint).fold(0usize, usize::saturating_add);
    Ok(PreparedDuckLakeCopyBatch {
        batch: PreparedDuckLakeTableBatch {
            #[cfg(test)]
            test_hook: None,
            table_name,
            replay_epoch,
            batch_id: batch_id.to_string(),
            batch_kind: DuckLakeTableBatchKind::Copy,
            first_start_lsn: None,
            last_commit_lsn: None,
            first_event_position: None,
            last_event_position: None,
            insert_column_names: replicated_column_names(replicated_table_schema),
            action: PreparedDuckLakeTableBatchAction::Mutation(vec![
                PreparedTableMutation::Upsert(prepare_copy_rows(
                    replicated_table_schema,
                    table_rows,
                )?),
            ]),
        },
        estimated_bytes: u64::try_from(estimated_bytes).unwrap_or(u64::MAX),
    })
}

/// Prepares the durable marker written after every copy worker finishes.
pub(super) fn prepare_copy_complete_table_batch(
    table_name: DuckLakeTableName,
    replay_epoch: String,
) -> PreparedDuckLakeTableBatch {
    let identity = build_copy_complete_batch_identity(&table_name);
    PreparedDuckLakeTableBatch {
        #[cfg(test)]
        test_hook: None,
        table_name,
        replay_epoch,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::CopyComplete,
        first_start_lsn: None,
        last_commit_lsn: None,
        first_event_position: None,
        last_event_position: None,
        insert_column_names: Vec::new(),
        action: PreparedDuckLakeTableBatchAction::Mutation(Vec::new()),
    }
}

/// Prepares the ordered atomic batch for one table's truncate events.
pub(super) fn prepare_truncate_table_batch(
    table_name: DuckLakeTableName,
    replay_epoch: String,
    tracked_truncates: Vec<TrackedTruncateEvent>,
) -> PreparedDuckLakeTableBatch {
    let identity = build_truncate_batch_identity(&table_name, &tracked_truncates);
    PreparedDuckLakeTableBatch {
        #[cfg(test)]
        test_hook: None,
        table_name,
        replay_epoch,
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Truncate,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_event_position: tracked_truncates.first().map(TrackedTruncateEvent::event_position),
        last_event_position: tracked_truncates.last().map(TrackedTruncateEvent::event_position),
        insert_column_names: Vec::new(),
        action: PreparedDuckLakeTableBatchAction::Truncate,
    }
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

/// Records that one replay-safe batch was skipped because it was already
/// committed.
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
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
) -> EtlResult<Option<TableStreamingProgress>> {
    let table_id = table_name.id();
    let sql = format!(
        r#"SELECT last_commit_lsn, last_tx_ordinal
         FROM {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         WHERE table_name = {} AND COALESCE({REPLAY_EPOCH_COLUMN}, {}) = {}
         ORDER BY last_commit_lsn DESC, last_tx_ordinal DESC
         LIMIT 1;"#,
        quote_literal(&table_id),
        quote_literal(LEGACY_REPLAY_EPOCH),
        quote_literal(replay_epoch),
    );
    let mut statement = conn.prepare(&sql).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query prepare failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;
    let mut rows = statement.query([]).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress query failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;

    let Some(row) = rows.next().map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress row fetch failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?
    else {
        return Ok(None);
    };

    let last_commit_lsn: u64 = row.get(0).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress commit lsn read failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;
    let last_tx_ordinal: u64 = row.get(1).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress tx ordinal read failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;

    Ok(Some(TableStreamingProgress {
        last_event_position: EventPosition::new(PgLsn::from(last_commit_lsn), last_tx_ordinal),
    }))
}

/// Reads the last applied streaming event position for one table.
pub(super) fn read_table_streaming_progress_event_position(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
) -> EtlResult<Option<EventPosition>> {
    Ok(read_table_streaming_progress(conn, table_name, replay_epoch)?
        .map(|progress| progress.last_event_position))
}

/// Drops already-applied tracked mutations using the persisted event position.
pub(super) fn retain_mutations_after_event_position(
    tracked_mutations: Vec<TrackedTableMutation>,
    last_event_position: Option<EventPosition>,
) -> Vec<TrackedTableMutation> {
    match last_event_position {
        Some(last_event_position) => tracked_mutations
            .into_iter()
            .filter(|tracked_mutation| {
                compare_event_positions(tracked_mutation.event_position(), last_event_position)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_mutations,
    }
}

/// Drops already-applied tracked truncates using the persisted event position.
pub(super) fn retain_truncates_after_event_position(
    tracked_truncates: Vec<TrackedTruncateEvent>,
    last_event_position: Option<EventPosition>,
) -> Vec<TrackedTruncateEvent> {
    match last_event_position {
        Some(last_event_position) => tracked_truncates
            .into_iter()
            .filter(|tracked_truncate| {
                compare_event_positions(tracked_truncate.event_position(), last_event_position)
                    == std::cmp::Ordering::Greater
            })
            .collect(),
        None => tracked_truncates,
    }
}

/// Decides whether a streaming batch must be replayed or skipped.
fn streaming_replay_decision(
    progress: TableStreamingProgress,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<StreamingReplayDecision> {
    let first_event_position = batch.first_event_position.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its first event position",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;
    let last_event_position = batch.last_event_position.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last event position",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;

    if compare_event_positions(progress.last_event_position, first_event_position)
        != std::cmp::Ordering::Less
    {
        if compare_event_positions(progress.last_event_position, last_event_position)
            == std::cmp::Ordering::Less
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake streaming progress landed inside an atomic batch",
                format!(
                    "table={}, progress={}, first={}, last={}",
                    batch.table_name,
                    format_event_position(progress.last_event_position),
                    format_event_position(first_event_position),
                    format_event_position(last_event_position)
                )
            ));
        }

        return Ok(StreamingReplayDecision::Skip);
    }

    Ok(StreamingReplayDecision::Apply)
}

/// Compares two ETL event positions using commit LSN then transaction
/// ordinal.
fn compare_event_positions(left: EventPosition, right: EventPosition) -> std::cmp::Ordering {
    (u64::from(left.commit_lsn), left.tx_ordinal)
        .cmp(&(u64::from(right.commit_lsn), right.tx_ordinal))
}

/// Applies all prepared atomic batches for one table on the same connection.
fn apply_table_batches(
    conn: &duckdb::Connection,
    batches: &[PreparedDuckLakeTableBatch],
    operation_context: &DuckLakeBlockingOperationContext,
    streaming_progress: &mut TableStreamingProgressCache,
) -> EtlResult<()> {
    if batches.is_empty() {
        return Ok(());
    }

    if batches[0].uses_streaming_progress() && streaming_progress.progress.is_none() {
        #[cfg(test)]
        streaming_progress.reads.fetch_add(1, Ordering::Relaxed);
        streaming_progress.progress = Some(read_table_streaming_progress(
            conn,
            batches[0].table_name(),
            &batches[0].replay_epoch,
        )?);
    }

    for batch in batches {
        if !batch.uses_streaming_progress() {
            // Copy batches keep the marker path because initial-copy retries
            // still depend on per-batch idempotency.
            if applied_batch_marker_exists(conn, batch)? {
                record_replayed_batch_skip(batch);
                continue;
            }

            apply_table_batch(conn, batch, operation_context).map_err(|error| {
                classified_error_with_detail(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake atomic table batch failed",
                    format!(
                        "table={}, batch_id={}, batch_kind={}",
                        batch.table_name,
                        batch.batch_id,
                        batch.batch_kind.as_str()
                    ),
                    error,
                )
            })?;
            continue;
        }

        if let Some(Some(progress)) = streaming_progress.progress {
            match streaming_replay_decision(progress, batch)? {
                StreamingReplayDecision::Skip => {
                    record_replayed_batch_skip(batch);
                    continue;
                }
                StreamingReplayDecision::Apply => {}
            }
        }

        apply_table_batch(conn, batch, operation_context).map_err(|error| {
            classified_error_with_detail(
                ErrorKind::DestinationQueryFailed,
                "DuckLake atomic table batch failed",
                format!(
                    "table={}, batch_id={}, batch_kind={}",
                    batch.table_name,
                    batch.batch_id,
                    batch.batch_kind.as_str()
                ),
                error,
            )
        })?;

        streaming_progress.progress = Some(
            batch
                .last_event_position
                .map(|last_event_position| TableStreamingProgress { last_event_position }),
        );
    }

    Ok(())
}

/// Builds one prepared atomic batch from an ordered slice of tracked mutations.
fn push_prepared_mutation_batch(
    prepared_batches: &mut Vec<PreparedDuckLakeTableBatch>,
    replicated_table_schema: &ReplicatedTableSchema,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
    tracked_mutations: Vec<TrackedTableMutation>,
) -> EtlResult<()> {
    if tracked_mutations.is_empty() {
        return Ok(());
    }

    let identity =
        build_mutation_batch_identity(table_name, replicated_table_schema, &tracked_mutations)?;
    let first_event_position = tracked_mutations.first().map(TrackedTableMutation::event_position);
    let last_event_position = tracked_mutations.last().map(TrackedTableMutation::event_position);
    let mutations = tracked_mutations.into_iter().map(|tracked| tracked.mutation).collect();

    prepared_batches.push(PreparedDuckLakeTableBatch {
        #[cfg(test)]
        test_hook: None,
        table_name: table_name.clone(),
        replay_epoch: replay_epoch.to_owned(),
        batch_id: identity.batch_id,
        batch_kind: DuckLakeTableBatchKind::Mutation,
        first_start_lsn: identity.first_start_lsn,
        last_commit_lsn: identity.last_commit_lsn,
        first_event_position,
        last_event_position,
        insert_column_names: replicated_column_names(replicated_table_schema),
        action: PreparedDuckLakeTableBatchAction::Mutation(prepare_table_mutations(
            replicated_table_schema,
            mutations,
        )?),
    });

    Ok(())
}

/// Groups ordered row mutations into retryable DuckDB operations.
fn prepare_table_mutations(
    replicated_table_schema: &ReplicatedTableSchema,
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
                delete_predicates.push(delete_predicate_from_row(replicated_table_schema, &row)?);
            }
            TableMutation::Update { delete_row, new_row } => {
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
                match new_row {
                    UpdatedTableRow::Full(upsert_row) => {
                        prepared_mutations.push(PreparedTableMutation::Delete {
                            predicates: vec![delete_predicate_from_row(
                                replicated_table_schema,
                                &delete_row,
                            )?],
                            origin: "update",
                        });
                        prepared_mutations
                            .push(PreparedTableMutation::Upsert(prepare_rows(vec![upsert_row])));
                    }
                    UpdatedTableRow::Partial(partial_row) => {
                        prepared_mutations.push(PreparedTableMutation::Update {
                            assignments: update_assignments_from_partial_row(
                                replicated_table_schema,
                                &partial_row,
                            )?,
                            predicate: delete_predicate_from_row(
                                replicated_table_schema,
                                &delete_row,
                            )?,
                        });
                    }
                }
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
                    predicates: vec![delete_predicate_from_row(replicated_table_schema, &row)?],
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

/// Builds a `WHERE` clause from the replica-identity values stored in `row`.
fn delete_predicate_from_row<'a>(
    replicated_table_schema: &ReplicatedTableSchema,
    row: impl Into<DeletePredicateRowRef<'a>>,
) -> EtlResult<String> {
    let row = row.into();
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    let identity_column_schemas: Vec<_> =
        replicated_table_schema.identity_column_schemas().collect();
    if identity_column_schemas.is_empty() {
        return Err(etl_error!(
            ErrorKind::SourceReplicaIdentityError,
            "DuckLake delete requires a replica identity",
            format!(
                "Table '{}' has no replicated replica-identity columns",
                replicated_table_schema.name()
            )
        ));
    }

    let key_values: Vec<_> = match row {
        DeletePredicateRowRef::Full(row) => {
            if row.values().len() != replicated_column_schemas.len() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake row shape does not match schema",
                    format!(
                        "Expected {} values for table '{}', got {}",
                        replicated_column_schemas.len(),
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                ));
            }

            let mut identity_columns = identity_column_schemas.iter().copied().peekable();
            let mut key_values = Vec::with_capacity(identity_column_schemas.len());

            for (column_schema, value) in replicated_column_schemas.iter().zip(row.values()) {
                if identity_columns.peek().is_some_and(|identity_column| {
                    identity_column.ordinal_position == column_schema.ordinal_position
                }) {
                    let Some(identity_column) = identity_columns.next() else {
                        return Err(etl_error!(
                            ErrorKind::InvalidState,
                            "DuckLake replica identity schema is inconsistent",
                            format!(
                                "Table '{}' identity columns ended unexpectedly",
                                replicated_table_schema.name()
                            )
                        ));
                    };

                    key_values.push((identity_column, value));
                }
            }

            key_values
        }
        DeletePredicateRowRef::Key(row) => {
            if row.values().len() != identity_column_schemas.len() {
                return Err(etl_error!(
                    ErrorKind::InvalidState,
                    "DuckLake key image does not match replica identity",
                    format!(
                        "Expected {} key values for table '{}', got {}",
                        identity_column_schemas.len(),
                        replicated_table_schema.name(),
                        row.values().len()
                    )
                ));
            }

            identity_column_schemas.iter().copied().zip(row.values()).collect()
        }
    };

    let mut predicates = Vec::new();
    for (column_schema, value) in key_values {
        let quoted_column =
            quote_identifier(&DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column_schema.name));
        let predicate = match value {
            Cell::Null => format!("{quoted_column} IS NULL"),
            _ => format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)),
        };
        predicates.push(predicate);
    }

    Ok(predicates.join(" AND "))
}

/// Builds SQL `SET` assignments from a partial update row.
fn update_assignments_from_partial_row(
    replicated_table_schema: &ReplicatedTableSchema,
    partial_row: &PartialTableRow,
) -> EtlResult<Vec<String>> {
    let replicated_column_schemas: Vec<_> = replicated_table_schema.column_schemas().collect();
    if partial_row.total_columns() != replicated_column_schemas.len() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row does not match schema",
            format!(
                "Expected {} replicated columns for table '{}', got {}",
                replicated_column_schemas.len(),
                replicated_table_schema.name(),
                partial_row.total_columns()
            )
        ));
    }

    if partial_row.values().is_empty() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial update row has no assignments",
            format!(
                "Table '{}' emitted an empty partial update row",
                replicated_table_schema.name()
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
                "Table '{}' partial row reports {} total columns but has {} present and {} missing",
                replicated_table_schema.name(),
                partial_row.total_columns(),
                partial_row.values().len(),
                partial_row.missing_column_indexes().len()
            )
        ));
    }

    let mut assignments = Vec::with_capacity(partial_row.values().len());
    let mut missing_indexes = partial_row.missing_column_indexes().iter().copied().peekable();
    let mut present_values = partial_row.values().iter();
    for (column_index, column_schema) in replicated_column_schemas.iter().enumerate() {
        if missing_indexes.peek().copied() == Some(column_index) {
            missing_indexes.next();
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
        let quoted_column =
            quote_identifier(&DUCKLAKE_COLUMN_NAME_MAPPING.map_name(&column_schema.name));
        assignments.push(format!("{quoted_column} = {}", cell_to_sql_literal_ref(value)));
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

    Ok(assignments)
}

/// Builds a deterministic identity for one ordered mutation batch.
fn build_mutation_batch_identity(
    table_name: &DuckLakeTableName,
    replicated_table_schema: &ReplicatedTableSchema,
    tracked_mutations: &[TrackedTableMutation],
) -> EtlResult<DuckLakeBatchIdentity> {
    let mut hasher = BatchIdHasher::new();
    "mutation".hash(&mut hasher);
    table_name.id().hash(&mut hasher);

    for tracked_mutation in tracked_mutations {
        u64::from(tracked_mutation.event_position.commit_lsn).hash(&mut hasher);
        tracked_mutation.event_position.tx_ordinal.hash(&mut hasher);

        match &tracked_mutation.mutation {
            TableMutation::Insert(row) => {
                "insert".hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
            TableMutation::Delete(row) => {
                "delete".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, row)?.hash(&mut hasher);
            }
            TableMutation::Update { delete_row, new_row } => {
                "update".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, delete_row)?.hash(&mut hasher);
                match new_row {
                    UpdatedTableRow::Full(row) => hash_table_row_ref(&mut hasher, row),
                    UpdatedTableRow::Partial(row) => hash_partial_table_row_ref(&mut hasher, row)?,
                }
            }
            TableMutation::Replace(row) => {
                "replace".hash(&mut hasher);
                delete_predicate_from_row(replicated_table_schema, row)?.hash(&mut hasher);
                hash_table_row_ref(&mut hasher, row);
            }
        }
    }

    Ok(build_batch_identity(
        DuckLakeTableBatchKind::Mutation,
        None,
        tracked_mutations.last().map(|tracked_mutation| tracked_mutation.event_position.commit_lsn),
        hasher.finish(),
    ))
}

/// Builds the deterministic identity shared by retries of a copy barrier.
fn build_copy_complete_batch_identity(table_name: &DuckLakeTableName) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "copy_complete".hash(&mut hasher);
    table_name.id().hash(&mut hasher);

    build_batch_identity(DuckLakeTableBatchKind::CopyComplete, None, None, hasher.finish())
}

/// Builds a deterministic identity for one ordered truncate batch.
fn build_truncate_batch_identity(
    table_name: &DuckLakeTableName,
    tracked_truncates: &[TrackedTruncateEvent],
) -> DuckLakeBatchIdentity {
    let mut hasher = BatchIdHasher::new();
    "truncate".hash(&mut hasher);
    table_name.id().hash(&mut hasher);

    for tracked_truncate in tracked_truncates {
        u64::from(tracked_truncate.event_position.commit_lsn).hash(&mut hasher);
        tracked_truncate.event_position.tx_ordinal.hash(&mut hasher);
        tracked_truncate.options.hash(&mut hasher);
    }

    build_batch_identity(
        DuckLakeTableBatchKind::Truncate,
        None,
        tracked_truncates.last().map(|tracked_truncate| tracked_truncate.event_position.commit_lsn),
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

/// Hashes a row using its SQL literal form so retries are independent of
/// appender encoding.
fn hash_table_row_ref(hasher: &mut BatchIdHasher, row: &TableRow) {
    table_row_to_sql_literal_ref(row).hash(hasher);
}

/// Hashes a partial row using column indexes and SQL literal forms.
fn hash_partial_table_row_ref(hasher: &mut BatchIdHasher, row: &PartialTableRow) -> EtlResult<()> {
    row.total_columns().hash(hasher);
    let mut missing_indexes = row.missing_column_indexes().iter().copied().peekable();
    let mut present_values = row.values().iter();

    for column_index in 0..row.total_columns() {
        if missing_indexes.peek().copied() == Some(column_index) {
            missing_indexes.next();
            continue;
        }

        let Some(value) = present_values.next() else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake partial row shape is inconsistent",
                format!("Partial row ended before replicated column index {}", column_index)
            ));
        };

        column_index.hash(hasher);
        cell_to_sql_literal_ref(value).hash(hasher);
    }

    if present_values.next().is_some() {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "DuckLake partial row shape is inconsistent",
            "Partial row contained more present values than its missing indexes allow"
        ));
    }

    Ok(())
}

/// Returns whether the atomic batch marker already exists.
fn applied_batch_marker_exists(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<bool> {
    applied_batch_marker_id_exists(conn, &batch.table_name, &batch.replay_epoch, &batch.batch_id)
}

/// Returns whether one marker identity already exists.
fn applied_batch_marker_id_exists(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
    batch_id: &str,
) -> EtlResult<bool> {
    let table_id = table_name.id();
    let sql = format!(
        r#"SELECT 1 FROM {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         WHERE table_name = {}
           AND COALESCE({REPLAY_EPOCH_COLUMN}, {}) = {}
           AND batch_id = {}
         LIMIT 1;"#,
        quote_literal(&table_id),
        quote_literal(LEGACY_REPLAY_EPOCH),
        quote_literal(replay_epoch),
        quote_literal(batch_id)
    );
    let mut statement = conn.prepare(&sql).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query prepare failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;
    let mut rows = statement.query([]).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;

    rows.next().map(|row| row.is_some()).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake marker query row fetch failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })
}

/// Inserts the atomic batch marker inside the open DuckLake transaction.
fn insert_applied_batch_marker(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let marker = PreparedDuckLakeBatchMarker {
        batch_kind: batch.batch_kind,
        first_start_lsn: batch.first_start_lsn,
        last_commit_lsn: batch.last_commit_lsn,
    };
    insert_applied_batch_marker_fields(
        conn,
        &batch.table_name,
        &batch.replay_epoch,
        &batch.batch_id,
        &marker,
    )
}

/// Inserts one compact marker inside the open DuckLake transaction.
fn insert_applied_batch_marker_fields(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    replay_epoch: &str,
    batch_id: &str,
    marker: &PreparedDuckLakeBatchMarker,
) -> EtlResult<()> {
    let table_id = table_name.id();
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{APPLIED_BATCHES_TABLE}"
         (table_name, replay_epoch, batch_id, batch_kind, first_start_lsn, last_commit_lsn, applied_at)
         VALUES ({}, {}, {}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&table_id),
        quote_literal(replay_epoch),
        quote_literal(batch_id),
        quote_literal(marker.batch_kind.as_str()),
        optional_lsn_to_sql_literal(marker.first_start_lsn),
        optional_lsn_to_sql_literal(marker.last_commit_lsn),
    );
    conn.execute_batch(&sql).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake batch marker insert failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;
    Ok(())
}

/// Appends the steady-state streaming replay watermark inside the open
/// transaction.
fn update_table_streaming_progress(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
) -> EtlResult<()> {
    let last_event_position = batch.last_event_position.ok_or_else(|| {
        etl_error!(
            ErrorKind::InvalidState,
            "DuckLake streaming batch is missing its last event position",
            format!("table={}, batch_kind={}", batch.table_name, batch.batch_kind.as_str())
        )
    })?;
    let table_id = batch.table_name.id();
    let sql = format!(
        r#"INSERT INTO {LAKE_CATALOG}."{STREAMING_PROGRESS_TABLE}"
         (table_name, replay_epoch, last_commit_lsn, last_tx_ordinal, updated_at)
         VALUES ({}, {}, {}, {}, current_timestamp);"#,
        quote_literal(&table_id),
        quote_literal(&batch.replay_epoch),
        u64::from(last_event_position.commit_lsn),
        last_event_position.tx_ordinal,
    );
    conn.execute_batch(&sql).map_err(|err| {
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake streaming progress update failed",
            format_query_error_detail(&sql),
            RedactedDuckDbError::from(err),
        )
    })?;
    Ok(())
}

/// Joins quoted column identifiers for insert/select lists.
fn quoted_column_list(column_names: &[String]) -> String {
    column_names
        .iter()
        .map(|column_name| quote_identifier(column_name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Reusable per-batch temp staging table for DuckLake upserts.
struct ReusableStagingTable {
    table_name: DuckLakeTableName,
    staging_name: String,
    created: bool,
    insert_column_names: Vec<String>,
}

impl ReusableStagingTable {
    /// Creates a fresh staging-table manager for one destination table.
    fn new(table_name: &DuckLakeTableName, insert_column_names: Vec<String>) -> Self {
        Self {
            table_name: table_name.clone(),
            staging_name: format!("__staging_{}", table_name.id()),
            created: false,
            insert_column_names,
        }
    }

    /// Loads one prepared row set into staging and applies it to the target
    /// table.
    fn stage_and_insert(
        &mut self,
        conn: &duckdb::Connection,
        prepared_rows: &PreparedRows,
    ) -> EtlResult<()> {
        self.prepare(conn)?;
        self.load_rows(conn, prepared_rows)?;
        self.insert_staged_rows(conn)
    }

    /// Drops the temp staging table after the batch finishes.
    fn cleanup(&self, conn: &duckdb::Connection) {
        if !self.created {
            return;
        }

        let staging_table = quote_identifier(&self.staging_name);
        if let Err(error) = conn.execute_batch(&format!("drop table if exists {staging_table}")) {
            let error = RedactedDuckDbError::from(error);
            tracing::error!(error = %error, "error drop table staging");
        }
    }

    /// Creates the temp table once, then clears it before each reuse.
    fn prepare(&mut self, conn: &duckdb::Connection) -> EtlResult<()> {
        if self.created {
            return self.clear(conn);
        }

        self.ensure_created(conn)
    }

    /// Creates the temporary staging table without clearing existing rows.
    fn ensure_created(&mut self, conn: &duckdb::Connection) -> EtlResult<()> {
        if self.created {
            return Ok(());
        }

        #[cfg(feature = "test-utils")]
        {
            let mut counts = STAGING_TABLE_CREATIONS_BY_TABLE.lock();
            *counts.entry(self.table_name.id()).or_default() += 1;
        }

        let staging_table = quote_identifier(&self.staging_name);
        let column_list = quoted_column_list(&self.insert_column_names);
        let target_table = qualified_lake_table_name(&self.table_name);
        conn.execute_batch(&format!(
            "create or replace temp table {staging_table} as
             select {column_list} from {target_table} limit 0;"
        ))
        .map_err(|error| {
            let error = RedactedDuckDbError::from(error);
            tracing::error!(error = %error, "error creating temporary table");

            classified_error(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging table creation failed",
                error,
            )
        })?;
        self.created = true;
        Ok(())
    }

    /// Inserts every currently staged row into the destination table.
    fn insert_staged_rows(&self, conn: &duckdb::Connection) -> EtlResult<()> {
        let column_list = quoted_column_list(&self.insert_column_names);
        let target_table = qualified_lake_table_name(&self.table_name);
        let staging_table = quote_identifier(&self.staging_name);
        let sql = format!(
            "insert into {target_table} ({column_list}) select {column_list} from {staging_table};"
        );
        conn.execute_batch(&sql).map_err(|error| {
            let error = RedactedDuckDbError::from(error);
            tracing::error!(error = %error, "error inserting rows");
            classified_error_with_detail(
                ErrorKind::DestinationQueryFailed,
                "DuckLake INSERT SELECT failed",
                format_query_error_detail(&sql),
                error,
            )
        })?;
        Ok(())
    }

    /// Clears all rows from an existing temporary staging table.
    fn clear(&self, conn: &duckdb::Connection) -> EtlResult<()> {
        if !self.created {
            return Ok(());
        }

        let staging_table = quote_identifier(&self.staging_name);
        let sql = format!("truncate table {staging_table};");
        conn.execute_batch(&sql).map_err(|error| {
            let error = RedactedDuckDbError::from(error);
            tracing::error!(error = %error, "error clear staging");
            classified_error(
                ErrorKind::DestinationQueryFailed,
                "DuckLake staging table clear failed",
                error,
            )
        })?;
        Ok(())
    }

    /// Loads one prepared row payload into the temp staging table.
    fn load_rows(&self, conn: &duckdb::Connection, prepared_rows: &PreparedRows) -> EtlResult<()> {
        match prepared_rows {
            PreparedRows::Appender(all_values) => {
                let mut appender = conn.appender(&self.staging_name).map_err(|error| {
                    let error = RedactedDuckDbError::from(error);
                    tracing::error!(error = %error, "error appender");
                    classified_error(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender creation failed",
                        error,
                    )
                })?;
                for values in all_values {
                    appender.append_row(duckdb::appender_params_from_iter(values)).map_err(
                        |err| {
                            let err = RedactedDuckDbError::from(err);
                            tracing::error!(error = %err, "error append row");
                            classified_error(
                                ErrorKind::DestinationQueryFailed,
                                "DuckLake staging append_row failed",
                                err,
                            )
                        },
                    )?;
                }
                appender.flush().map_err(|err| {
                    let err = RedactedDuckDbError::from(err);
                    tracing::error!(error = %err, "error flush");
                    classified_error(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender flush failed",
                        err,
                    )
                })?;
            }
            PreparedRows::ArrowRecordBatch(record_batch) => {
                let mut appender = conn.appender(&self.staging_name).map_err(|error| {
                    let error = RedactedDuckDbError::from(error);
                    tracing::error!(error = %error, "error appender");
                    classified_error(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender creation failed",
                        error,
                    )
                })?;
                appender.append_record_batch(record_batch.clone()).map_err(|err| {
                    let err = RedactedDuckDbError::from(err);
                    tracing::error!(error = %err, "error append record batch");
                    classified_error(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging append_record_batch failed",
                        err,
                    )
                })?;
                appender.flush().map_err(|err| {
                    let err = RedactedDuckDbError::from(err);
                    tracing::error!(error = %err, "error flush");
                    classified_error(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake staging appender flush failed",
                        err,
                    )
                })?;
            }
            PreparedRows::SqlLiterals(row_literals) => {
                insert_rows_into_staging_with_sql(
                    conn,
                    &self.staging_name,
                    row_literals.as_slice(),
                )?;
            }
        }
        Ok(())
    }
}

/// Connection-local accumulator for one table-copy replay epoch.
pub(super) struct DuckLakeCopyAccumulator {
    table_name: DuckLakeTableName,
    replay_epoch: String,
    staging: ReusableStagingTable,
    pending_markers: HashMap<String, PreparedDuckLakeBatchMarker>,
    staged_bytes: u64,
}

impl DuckLakeCopyAccumulator {
    /// Creates an empty accumulator shaped from its first prepared batch.
    pub(super) fn new(batch: &PreparedDuckLakeCopyBatch) -> Self {
        Self {
            table_name: batch.batch.table_name.clone(),
            replay_epoch: batch.batch.replay_epoch.clone(),
            staging: ReusableStagingTable::new(
                &batch.batch.table_name,
                batch.batch.insert_column_names.clone(),
            ),
            pending_markers: HashMap::new(),
            staged_bytes: 0,
        }
    }

    /// Returns the approximate decoded bytes awaiting a durable flush.
    pub(super) fn staged_bytes(&self) -> u64 {
        self.staged_bytes
    }

    /// Appends one prepared copy batch to the connection-local staging table.
    ///
    /// Returns `false` when the deterministic batch marker proves that this
    /// batch was already committed during the same replay epoch.
    pub(super) fn append(
        &mut self,
        conn: &duckdb::Connection,
        batch: PreparedDuckLakeCopyBatch,
    ) -> EtlResult<bool> {
        if batch.batch.table_name != self.table_name
            || batch.batch.replay_epoch != self.replay_epoch
            || batch.batch.insert_column_names != self.staging.insert_column_names
        {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake buffered copy batch does not match its staging session",
                format!("table={}", self.table_name)
            ));
        }

        if self.pending_markers.contains_key(&batch.batch.batch_id)
            || applied_batch_marker_exists(conn, &batch.batch)?
        {
            record_replayed_batch_skip(&batch.batch);
            return Ok(false);
        }

        let estimated_bytes = batch.estimated_bytes;
        let (batch_id, marker, prepared_rows) = batch.into_buffer_parts()?;
        self.staging.ensure_created(conn)?;
        self.staging.load_rows(conn, &prepared_rows)?;
        self.pending_markers.insert(batch_id, marker);
        self.staged_bytes = self.staged_bytes.saturating_add(estimated_bytes);

        Ok(true)
    }

    /// Commits all staged rows and their markers as one DuckLake change.
    ///
    /// When supplied, `copy_complete` is committed in the same transaction as
    /// the final staged window. A failure before `COMMIT` is rolled back. A
    /// `COMMIT` failure has an ambiguous outcome, so the dedicated connection
    /// and complete table-copy attempt are invalidated instead of retried.
    /// Staging is cleared only after `COMMIT` succeeds.
    pub(super) fn flush(
        &mut self,
        conn: &duckdb::Connection,
        copy_complete: Option<PreparedDuckLakeTableBatch>,
    ) -> EtlResult<()> {
        if self.pending_markers.is_empty() && copy_complete.is_none() {
            return Ok(());
        }

        let started = Instant::now();
        conn.execute_batch("begin transaction").map_err(|error| {
            classified_error(
                ErrorKind::DestinationQueryFailed,
                "DuckLake buffered copy BEGIN TRANSACTION failed",
                RedactedDuckDbError::from(error),
            )
        })?;

        let result = (|| -> EtlResult<()> {
            if !self.pending_markers.is_empty() {
                self.staging.insert_staged_rows(conn)?;
                for (batch_id, marker) in &self.pending_markers {
                    insert_applied_batch_marker_fields(
                        conn,
                        &self.table_name,
                        &self.replay_epoch,
                        batch_id,
                        marker,
                    )?;
                }
            }

            if let Some(copy_complete) = &copy_complete {
                if copy_complete.table_name != self.table_name
                    || copy_complete.replay_epoch != self.replay_epoch
                {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "DuckLake copy-complete marker does not match its buffered session",
                        format!("table={}", self.table_name)
                    ));
                }
                if !applied_batch_marker_exists(conn, copy_complete)? {
                    insert_applied_batch_marker(conn, copy_complete)?;
                }
            }

            Ok(())
        })();

        if let Err(error) = result {
            if let Err(rollback_error) = conn.execute_batch("rollback") {
                let rollback_error = RedactedDuckDbError::from(rollback_error);
                tracing::error!(error = %rollback_error, "error rollback buffered copy");
            }
            return Err(error);
        }

        // A COMMIT error does not prove whether DuckLake committed. Returning
        // the error makes the shared runner mark this connection broken; the
        // owner then fences the buffer until recovery drops and recopies the
        // complete table instead of retrying an ambiguous transaction.
        conn.execute_batch("commit").map_err(|error| {
            classified_error(
                ErrorKind::DestinationQueryFailed,
                "DuckLake buffered copy COMMIT failed",
                RedactedDuckDbError::from(error),
            )
        })?;

        #[cfg(feature = "test-utils")]
        maybe_fail_after_committed_batch_for_tests(DuckLakeTableBatchKind::Copy, &self.table_name)?;

        self.staging.clear(conn)?;
        histogram!(
            ETL_DUCKLAKE_BATCH_COMMIT_DURATION_SECONDS,
            BATCH_KIND_LABEL => DuckLakeTableBatchKind::Copy.as_str(),
            SUB_BATCH_KIND_LABEL => "buffered_insert",
        )
        .record(started.elapsed().as_secs_f64());
        histogram!(
            ETL_DUCKLAKE_BATCH_PREPARED_MUTATIONS,
            BATCH_KIND_LABEL => DuckLakeTableBatchKind::Copy.as_str(),
            SUB_BATCH_KIND_LABEL => "buffered_insert",
        )
        .record(self.pending_markers.len() as f64);
        trace!(
            table = %self.table_name,
            copy_batch_count = self.pending_markers.len(),
            staged_bytes = self.staged_bytes,
            copy_complete = copy_complete.is_some(),
            "ducklake buffered copy committed"
        );
        self.pending_markers.clear();
        self.staged_bytes = 0;

        Ok(())
    }
}

/// Applies one atomic per-table batch in a single DuckLake transaction.
fn apply_table_batch(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    let batch_started = Instant::now();
    let result = apply_table_batch_inner(conn, batch, operation_context);
    if result.is_err() {
        warn!(
            table = %batch.table_name, batch_id = %batch.batch_id,
            batch_elapsed_ms = batch_started.elapsed().as_millis() as u64,
            ducklake_operation_id = operation_context.operation_id(),
            operation_elapsed_ms = operation_context.elapsed_ms(),
            remaining_timeout_ms = operation_context.remaining_timeout_ms(),
            "ducklake atomic batch attempt failed"
        );
    }
    result
}

/// Executes the transaction and its replay watermark on the same connection.
fn apply_table_batch_inner(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    let batch_started = Instant::now();

    conn.execute_batch("BEGIN TRANSACTION").map_err(|error| {
        let error = RedactedDuckDbError::from(error);
        tracing::error!(error = %error, "error transaction");
        classified_error(
            ErrorKind::DestinationQueryFailed,
            "DuckLake BEGIN TRANSACTION failed",
            error,
        )
    })?;

    let mut reusable_staging_table =
        ReusableStagingTable::new(&batch.table_name, batch.insert_column_names.clone());
    let result = (|| -> EtlResult<()> {
        #[cfg(test)]
        if let Some(hook) = &batch.test_hook {
            hook(conn, operation_context)?;
        }
        match &batch.action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
                for prepared_mutation in prepared_mutations {
                    apply_table_mutation(
                        conn,
                        batch,
                        prepared_mutation,
                        &mut reusable_staging_table,
                        operation_context,
                    )?;
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => {
                apply_truncate_batch_action(conn, &batch.table_name)?;
            }
        }

        if batch.uses_streaming_progress() {
            update_table_streaming_progress(conn, batch)?;
        } else {
            insert_applied_batch_marker(conn, batch)?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT").map_err(|error| {
                let error = RedactedDuckDbError::from(error);
                tracing::error!(error = %error, "error commit");
                reusable_staging_table.cleanup(conn);
                classified_error(ErrorKind::DestinationQueryFailed, "DuckLake COMMIT failed", error)
            })?;
            reusable_staging_table.cleanup(conn);
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
                first_start_lsn = %format_optional_lsn(batch.first_start_lsn),
                last_commit_lsn = %format_optional_lsn(batch.last_commit_lsn),
                sub_batch_kind = batch_log_kind(batch),
                insert_sub_batch_rows = apply_sub_batch_rows(batch),
                batch_elapsed_ms = batch_started.elapsed().as_millis() as u64,
                ducklake_operation_id = operation_context.operation_id(),
                operation_elapsed_ms = operation_context.elapsed_ms(),
                remaining_timeout_ms = operation_context.remaining_timeout_ms(),
                "ducklake batch committed"
            );

            #[cfg(feature = "test-utils")]
            maybe_fail_after_committed_batch_for_tests(batch.batch_kind, &batch.table_name)?;

            Ok(())
        }
        Err(err) => {
            let rollback = conn.execute_batch("ROLLBACK");
            reusable_staging_table.cleanup(conn);
            if let Err(err) = rollback {
                let err = RedactedDuckDbError::from(err);
                tracing::error!(error = %err, "error rollback");
            }

            Err(err)
        }
    }
}

/// Applies the truncate action inside an open transaction.
fn apply_truncate_batch_action(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
) -> EtlResult<()> {
    let target_table = qualified_lake_table_name(table_name);
    let sql = format!("TRUNCATE TABLE {target_table};");
    conn.execute_batch(&sql).map_err(|error| {
        let error = RedactedDuckDbError::from(error);
        tracing::error!(error = %error, "error truncating table");
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake TRUNCATE TABLE failed",
            format_query_error_detail(&sql),
            error,
        )
    })?;
    Ok(())
}

/// Formats an optional LSN for marker-table inserts.
fn optional_lsn_to_sql_literal(lsn: Option<PgLsn>) -> String {
    lsn.map_or_else(|| "NULL".to_owned(), |value| u64::from(value).to_string())
}

/// Applies one prepared table mutation inside an open transaction.
fn apply_table_mutation(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    prepared_mutation: &PreparedTableMutation,
    reusable_staging_table: &mut ReusableStagingTable,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    match prepared_mutation {
        PreparedTableMutation::Upsert(prepared_rows) => {
            histogram!(
                ETL_DUCKLAKE_UPSERT_ROWS,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                PREPARED_ROWS_KIND_LABEL => prepared_rows_kind(prepared_rows),
            )
            .record(prepared_rows_count(prepared_rows) as f64);
            apply_upsert_mutation(conn, prepared_rows, reusable_staging_table)
        }
        PreparedTableMutation::Delete { predicates, origin } => {
            histogram!(
                ETL_DUCKLAKE_DELETE_PREDICATES,
                BATCH_KIND_LABEL => batch.batch_kind.as_str(),
                DELETE_ORIGIN_LABEL => *origin,
            )
            .record(predicates.len() as f64);
            apply_delete_mutation(conn, batch, predicates.as_slice(), origin, operation_context)
        }
        PreparedTableMutation::Update { assignments, predicate } => apply_update_mutation(
            conn,
            &reusable_staging_table.table_name,
            assignments.as_slice(),
            predicate,
        ),
    }
}

/// Applies one upsert batch inside an open DuckLake transaction.
fn apply_upsert_mutation(
    conn: &duckdb::Connection,
    prepared_rows: &PreparedRows,
    reusable_staging_table: &mut ReusableStagingTable,
) -> EtlResult<()> {
    let row_count = prepared_rows_count(prepared_rows);

    if row_count == 0 {
        return Ok(());
    }

    reusable_staging_table.stage_and_insert(conn, prepared_rows)
}

/// Applies one delete batch inside an open DuckLake transaction.
fn apply_delete_mutation(
    conn: &duckdb::Connection,
    batch: &PreparedDuckLakeTableBatch,
    predicates: &[String],
    origin: &'static str,
    operation_context: &DuckLakeBlockingOperationContext,
) -> EtlResult<()> {
    if predicates.is_empty() {
        return Ok(());
    }

    let target_table = qualified_lake_table_name(&batch.table_name);
    let chunk_count = predicates.len().div_ceil(SQL_DELETE_BATCH_SIZE);
    for (chunk_index, chunk) in predicates.chunks(SQL_DELETE_BATCH_SIZE).enumerate() {
        let where_clause =
            chunk.iter().map(|predicate| format!("({predicate})")).collect::<Vec<_>>().join(" OR ");

        let sql_query = format!("DELETE FROM {target_table} WHERE {where_clause};");
        let statement_started = Instant::now();
        let statement_budget_ms = operation_context.remaining_timeout_ms();
        conn.execute_batch(&sql_query).map_err(|error| {
            let error = DuckDbSensitiveQueryError::new(&error);
            tracing::error!(
                error = %error,
                table = %batch.table_name,
                batch_id = %batch.batch_id,
                batch_kind = batch.batch_kind.as_str(),
                first_start_lsn = %format_optional_lsn(batch.first_start_lsn),
                last_commit_lsn = %format_optional_lsn(batch.last_commit_lsn),
                first_event_position = %format_optional_event_position(batch.first_event_position),
                last_event_position = %format_optional_event_position(batch.last_event_position),
                delete_origin = origin,
                delete_predicate_count = predicates.len(),
                delete_chunk_index = chunk_index,
                delete_chunk_count = chunk_count,
                delete_chunk_predicate_count = chunk.len(),
                duckdb_interrupted = error.is_interrupt(),
                statement_elapsed_ms = statement_started.elapsed().as_millis() as u64,
                statement_budget_ms,
                operation_elapsed_ms = operation_context.elapsed_ms(),
                remaining_timeout_ms = operation_context.remaining_timeout_ms(),
                ducklake_interrupt_reason = operation_context.interrupt_reason_label(),
                ducklake_operation_id = operation_context.operation_id(),
                ducklake_operation_kind = operation_context.operation_kind(),
                ducklake_operation_timeout_ms = operation_context.timeout_ms(),
                "error deleting rows"
            );
            classified_error_with_detail(
                ErrorKind::DestinationQueryFailed,
                "DuckLake DELETE failed",
                format_delete_mutation_error_detail(
                    &target_table,
                    predicates.len(),
                    chunk_index,
                    chunk_count,
                    chunk.len(),
                ),
                error,
            )
        })?;
    }

    Ok(())
}

/// Applies one update statement inside an open DuckLake transaction.
fn apply_update_mutation(
    conn: &duckdb::Connection,
    table_name: &DuckLakeTableName,
    assignments: &[String],
    predicate: &str,
) -> EtlResult<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    let set_clause = assignments.join(", ");
    let target_table = qualified_lake_table_name(table_name);
    let sql_query = format!("UPDATE {target_table} SET {set_clause} WHERE {predicate};");
    conn.execute_batch(&sql_query).map_err(|error| {
        let error = DuckDbSensitiveQueryError::new(&error);
        tracing::error!(error = %error, "error updating rows");
        classified_error_with_detail(
            ErrorKind::DestinationQueryFailed,
            "DuckLake UPDATE failed",
            format_update_mutation_error_detail(
                &target_table,
                assignments.len(),
                !predicate.is_empty(),
            ),
            error,
        )
    })?;

    Ok(())
}

/// Returns the number of values carried by a prepared row payload.
fn prepared_rows_count(prepared_rows: &PreparedRows) -> usize {
    match prepared_rows {
        PreparedRows::Appender(values) => values.len(),
        PreparedRows::ArrowRecordBatch(record_batch) => record_batch.num_rows(),
        PreparedRows::SqlLiterals(values) => values.len(),
    }
}

/// Returns the encoding strategy used by a prepared row payload.
fn prepared_rows_kind(prepared_rows: &PreparedRows) -> &'static str {
    match prepared_rows {
        PreparedRows::Appender(_) => "appender",
        PreparedRows::ArrowRecordBatch(_) => "arrow_record_batch",
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
        PreparedTableMutation::Upsert(prepared_rows) => Some(prepared_rows_count(prepared_rows)),
        PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => None,
    }
}

/// Classifies a prepared batch for concise INFO logging.
fn batch_log_kind(batch: &PreparedDuckLakeTableBatch) -> &'static str {
    match &batch.action {
        PreparedDuckLakeTableBatchAction::Truncate => "truncate",
        PreparedDuckLakeTableBatchAction::Mutation(prepared_mutations) => {
            match prepared_mutations.as_slice() {
                [PreparedTableMutation::Upsert(_)] => "insert",
                [PreparedTableMutation::Delete { origin, .. }]
                | [
                    PreparedTableMutation::Delete { origin, .. },
                    PreparedTableMutation::Upsert(_),
                ] => origin,
                [PreparedTableMutation::Update { .. }] => "update",
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
    let staging_table = quote_identifier(staging);
    for chunk in row_literals.chunks(SQL_INSERT_BATCH_SIZE) {
        conn.execute_batch(&format!("INSERT INTO {staging_table} VALUES {};", chunk.join(", ")))
            .map_err(|err| {
                let err = RedactedDuckDbError::from(err);
                tracing::error!(error = %err, "error insert_rows_into_staging_with_sql");
                classified_error(
                    ErrorKind::DestinationQueryFailed,
                    "DuckLake staging row insert failed",
                    err,
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
#[cfg(feature = "test-utils")]
static STAGING_TABLE_CREATIONS_BY_TABLE: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Arms a test hook that injects one post-commit failure for the next atomic
/// batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_atomic_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_owned());
}

/// Arms a test hook that injects one post-commit failure for the next copy
/// batch.
#[cfg(feature = "test-utils")]
pub fn arm_fail_after_copy_batch_commit_once_for_tests(table_name: &str) {
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = Some(table_name.to_owned());
}

/// Clears DuckLake destination test hooks.
#[cfg(feature = "test-utils")]
pub fn reset_ducklake_test_hooks() {
    *FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock() = None;
    *FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock() = None;
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().clear();
}

/// Returns the number of staging-table creations performed for one table since
/// the last reset.
#[cfg(feature = "test-utils")]
pub fn ducklake_staging_table_creations_for_tests(table_name: &str) -> usize {
    STAGING_TABLE_CREATIONS_BY_TABLE.lock().get(table_name).copied().unwrap_or_default()
}

/// Injects a synthetic retryable failure after commit, like a connection lost
/// before the commit was acknowledged, so retries must rely on the correct
/// marker path.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_committed_batch_for_tests(
    batch_kind: DuckLakeTableBatchKind,
    table_name: &DuckLakeTableName,
) -> EtlResult<()> {
    match batch_kind {
        DuckLakeTableBatchKind::Copy | DuckLakeTableBatchKind::CopyComplete => {
            maybe_fail_after_copy_batch_commit_for_tests(table_name)
        }
        DuckLakeTableBatchKind::Mutation | DuckLakeTableBatchKind::Truncate => {
            maybe_fail_after_atomic_batch_commit_for_tests(table_name)
        }
    }
}

/// Injects a synthetic failure after commit so retries must rely on the
/// progress row.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_atomic_batch_commit_for_tests(table_name: &DuckLakeTableName) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_ATOMIC_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name.id().as_str()) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake test hook injected post-commit failure"
        )
        .with_retryability(Retryability::Retryable));
    }

    Ok(())
}

/// Injects a synthetic failure after commit so copy retries must rely on the
/// marker table.
#[cfg(feature = "test-utils")]
fn maybe_fail_after_copy_batch_commit_for_tests(table_name: &DuckLakeTableName) -> EtlResult<()> {
    let mut fail_table = FAIL_AFTER_COPY_BATCH_COMMIT_TABLE.lock();
    if fail_table.as_deref() == Some(table_name.id().as_str()) {
        *fail_table = None;
        return Err(etl_error!(
            ErrorKind::DestinationQueryFailed,
            "DuckLake test hook injected copy post-commit failure"
        )
        .with_retryability(Retryability::Retryable));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use etl::{
        data::{OldTableRow, PartialTableRow, Timestamp, UpdatedTableRow},
        destination::TableCopyAttemptId,
        schema::{
            ColumnSchema, IdentityMask, ReplicatedTableSchema, ReplicationMask, TableId, TableName,
            TableSchema, Type as PgType,
        },
    };

    use super::*;

    /// In-memory SQL fixture for the real transaction, watchdog, and replay
    /// paths.
    struct BatchDeadlineFixture {
        pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
        slots: Arc<Semaphore>,
        guard: DuckLakeWriteGuard,
        reader: duckdb::Connection,
    }

    impl BatchDeadlineFixture {
        /// Creates only synthetic, process-local tables; no extensions or
        /// services.
        async fn new() -> Self {
            use crate::ducklake::{
                client::{DuckLakeInterruptRegistry, build_warm_ducklake_pool},
                config::{DuckLakeSetupPlan, DuckLakeSetupStep},
            };
            let setup = DuckLakeSetupPlan::from_steps(vec![DuckLakeSetupStep {
                label: "test_tables",
                sql: "attach ':memory:' as lake;
                    create schema lake.public;
                    create table lake.public.users (id integer, name varchar);
                    create table lake.public.attempts (id integer);
                    create table lake.__etl_streaming_progress (
                        table_name varchar, replay_epoch varchar, last_commit_lsn ubigint,
                        last_tx_ordinal ubigint, updated_at timestamptz
                    );"
                .to_owned(),
            }]);
            let manager = DuckLakeConnectionManager::new(
                Arc::new(setup),
                true,
                Arc::new(DuckLakeInterruptRegistry::default()),
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap();
            let pool =
                Arc::new(build_warm_ducklake_pool(manager, 1, "batch-test", ()).await.unwrap());
            let reader =
                run_duckdb_blocking(Arc::clone(&pool), Arc::new(Semaphore::new(1)), |conn| {
                    Ok(conn.try_clone().unwrap())
                })
                .await
                .unwrap();
            let permit = Arc::new(Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap());
            let checkpoint = Arc::new(Arc::new(tokio::sync::RwLock::new(())).read_owned().await);
            Self {
                pool,
                slots: Arc::new(Semaphore::new(1)),
                guard: DuckLakeWriteGuard::new(&permit, &checkpoint),
                reader,
            }
        }

        /// Verifies that data and replay watermarks committed exactly once.
        fn assert_committed(&self, rows: i64, batches: i64) {
            let count: i64 = self
                .reader
                .query_row("select count(*) from lake.public.users", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, rows);
            let attempts: i64 = self
                .reader
                .query_row("select count(*) from lake.public.attempts", [], |row| row.get(0))
                .unwrap();
            assert_eq!(attempts, batches);
            let progress: i64 = self
                .reader
                .query_row("select count(*) from lake.__etl_streaming_progress", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(progress, batches);
        }
    }

    /// A transaction barrier lets the test advance time without wall-clock
    /// sleeps.
    struct BatchDeadlineEvent {
        batch_index: usize,
        operation_id: u64,
        remaining_timeout_ms: u64,
        release: std::sync::mpsc::Sender<bool>,
    }

    /// Builds two real CDC batches and optional barriers inside their
    /// transactions.
    fn deadline_test_batches(
        events: Option<tokio::sync::mpsc::UnboundedSender<BatchDeadlineEvent>>,
    ) -> Vec<PreparedDuckLakeTableBatch> {
        let mutations = (0..=CDC_MUTATION_BATCH_SIZE)
            .map(|index| {
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(1), u64::try_from(index).unwrap()),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(i32::try_from(index).unwrap()),
                        Cell::String("synthetic".to_owned()),
                    ])),
                )
            })
            .collect();
        let mut batches = prepare_mutation_table_batches(
            &make_replicated_schema(),
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            mutations,
        )
        .unwrap();
        for (batch_index, batch) in batches.iter_mut().enumerate() {
            if let Some(events) = &events {
                let events = events.clone();
                batch.test_hook = Some(Arc::new(move |conn, context| {
                    // A timed-out attempt must roll back this write together
                    // with its watermark.
                    conn.execute(
                        "insert into lake.public.attempts values (?)",
                        [i64::try_from(batch_index).unwrap()],
                    )
                    .unwrap();
                    let (release, wait) = std::sync::mpsc::channel();
                    events
                        .send(BatchDeadlineEvent {
                            batch_index,
                            operation_id: context.operation_id(),
                            remaining_timeout_ms: context.remaining_timeout_ms(),
                            release,
                        })
                        .map_err(|_| etl_error!(ErrorKind::InvalidState, "Test receiver closed"))?;
                    let expect_timeout = wait
                        .recv()
                        .map_err(|_| etl_error!(ErrorKind::InvalidState, "Test barrier closed"))?;
                    if expect_timeout {
                        while context.interrupt_reason_label() == "none" {
                            std::thread::yield_now();
                        }
                        assert_eq!(context.interrupt_reason_label(), "query_timeout");
                        return Err(etl_error!(
                            ErrorKind::DestinationTimeout,
                            "Test transaction interrupted"
                        ));
                    }
                    Ok(())
                }));
            }
        }
        batches
    }

    /// Keeps Tokio from automatically jumping to a watchdog while native SQL
    /// runs.
    fn hold_test_clock() -> tokio_util::task::AbortOnDropHandle<()> {
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }

    /// Uses the production timeout but removes retry sleeps from barrier-driven
    /// tests.
    fn deadline_test_policy(max_retries: u32) -> RetryPolicy {
        RetryPolicy { max_retries, initial_delay: Duration::ZERO, max_delay: Duration::ZERO }
    }

    /// Each batch fits its budget even though the sequence takes more than 180
    /// seconds.
    #[tokio::test]
    async fn table_sequence_renews_timeout_for_each_atomic_batch() {
        let fixture = BatchDeadlineFixture::new().await;
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let batches = deadline_test_batches(Some(events));
        tokio::time::pause();
        let _clock = hold_test_clock();
        let started = Instant::now();
        let task = tokio::spawn(apply_table_batches_with_policy(
            Arc::clone(&fixture.pool),
            Arc::clone(&fixture.slots),
            batches,
            fixture.guard.clone(),
            FOREGROUND_QUERY_TIMEOUT,
            deadline_test_policy(0),
        ));
        let mut operation_ids = Vec::new();
        for batch_index in 0..2 {
            let event = observed.recv().await.unwrap();
            assert_eq!(event.batch_index, batch_index);
            operation_ids.push(event.operation_id);
            tokio::time::advance(Duration::from_secs(100)).await;
            event.release.send(false).unwrap();
        }
        let progress = task.await.unwrap().unwrap();
        assert_eq!(progress.reads.load(Ordering::Relaxed), 1);
        assert_ne!(operation_ids[0], operation_ids[1]);
        assert!(started.elapsed() > FOREGROUND_QUERY_TIMEOUT);
        fixture.assert_committed(17, 2);
        // Replay the complete input through the same async path, without
        // duplicating rows.
        let replay_progress = apply_table_batches_with_policy(
            Arc::clone(&fixture.pool),
            Arc::clone(&fixture.slots),
            deadline_test_batches(None),
            fixture.guard.clone(),
            FOREGROUND_QUERY_TIMEOUT,
            deadline_test_policy(0),
        )
        .await
        .unwrap();
        assert_eq!(replay_progress.reads.load(Ordering::Relaxed), 1);
        fixture.assert_committed(17, 2);
    }

    /// Earlier commits survive a later timeout, whose partial writes are rolled
    /// back.
    #[tokio::test]
    async fn table_sequence_retries_only_timed_out_batch() {
        let fixture = BatchDeadlineFixture::new().await;
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let batches = deadline_test_batches(Some(events));
        tokio::time::pause();
        let _clock = hold_test_clock();
        let task = tokio::spawn(apply_table_batches_with_policy(
            Arc::clone(&fixture.pool),
            Arc::clone(&fixture.slots),
            batches,
            fixture.guard.clone(),
            FOREGROUND_QUERY_TIMEOUT,
            deadline_test_policy(1),
        ));
        let first = observed.recv().await.unwrap();
        assert_eq!(first.batch_index, 0);
        first.release.send(false).unwrap();
        let interrupted = observed.recv().await.unwrap();
        assert_eq!(interrupted.batch_index, 1);
        fixture.assert_committed(16, 1);
        tokio::time::advance(FOREGROUND_QUERY_TIMEOUT + Duration::from_secs(1)).await;
        interrupted.release.send(true).unwrap();
        // Let the ordinary zero-delay retry timer run after the controlled
        // timeout.
        tokio::time::resume();
        let retry = observed.recv().await.unwrap();
        assert_eq!(retry.batch_index, 1);
        assert_ne!(interrupted.operation_id, retry.operation_id);
        assert!(retry.remaining_timeout_ms > 170_000);
        retry.release.send(false).unwrap();
        let progress = task.await.unwrap().unwrap();
        assert_eq!(progress.reads.load(Ordering::Relaxed), 2);
        fixture.assert_committed(17, 2);
    }

    /// A slow atomic transaction still fails, without acknowledging unfinished
    /// data.
    #[tokio::test]
    async fn table_sequence_enforces_single_batch_timeout() {
        let fixture = BatchDeadlineFixture::new().await;
        let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let mut batches = deadline_test_batches(Some(events));
        batches.truncate(1);
        tokio::time::pause();
        let _clock = hold_test_clock();
        let task = tokio::spawn(apply_table_batches_with_policy(
            Arc::clone(&fixture.pool),
            Arc::clone(&fixture.slots),
            batches,
            fixture.guard.clone(),
            FOREGROUND_QUERY_TIMEOUT,
            deadline_test_policy(0),
        ));
        let event = observed.recv().await.unwrap();
        tokio::time::advance(FOREGROUND_QUERY_TIMEOUT + Duration::from_secs(1)).await;
        event.release.send(true).unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("timeout_ms=180000"));
        fixture.assert_committed(0, 0);
    }

    #[test]
    fn event_position_format_preserves_fixed_width_hex_encoding() {
        let event_position = EventPosition::new(PgLsn::from(1), 2);

        assert_eq!(format_event_position(event_position), "0000000000000001/0000000000000002");
    }

    fn make_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ],
        )
    }

    fn make_replicated_schema() -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(make_schema()))
    }

    fn make_replicated_schema_with_columns(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            columns,
        )))
    }

    #[test]
    fn replicated_column_names_use_ducklake_mapping() {
        let schema = make_replicated_schema_with_columns(vec![
            ColumnSchema::new("ID".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("Display_Name".to_owned(), PgType::TEXT, -1, 2, true),
        ]);

        assert_eq!(replicated_column_names(&schema), ["id", "display_name"]);
    }

    fn ducklake_table_name() -> DuckLakeTableName {
        DuckLakeTableName::new("public", "users")
    }

    fn attach_lake_catalog(conn: &duckdb::Connection) {
        conn.execute_batch("attach ':memory:' as lake;").unwrap();
    }

    fn make_prepared_batch(table_name: DuckLakeTableName) -> PreparedDuckLakeTableBatch {
        PreparedDuckLakeTableBatch {
            #[cfg(test)]
            test_hook: None,
            table_name,
            replay_epoch: LEGACY_REPLAY_EPOCH.to_owned(),
            batch_id: "test-batch".to_owned(),
            batch_kind: DuckLakeTableBatchKind::Mutation,
            first_start_lsn: None,
            last_commit_lsn: None,
            first_event_position: None,
            last_event_position: None,
            insert_column_names: vec![],
            action: PreparedDuckLakeTableBatchAction::Mutation(vec![]),
        }
    }

    /// Asserts that a failed query against a missing catalog keeps its DuckDB
    /// type and classification but not the row values in its statement.
    fn assert_query_failure_omits_sensitive_value(
        error: &etl::error::EtlError,
        description: &'static str,
        sensitive_value: &str,
    ) {
        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(error.description(), Some(description));
        assert_eq!(error.retryability(), Retryability::Permanent);
        assert!(!error.to_string().contains(sensitive_value));
        assert!(!error.detail().is_some_and(|detail| detail.contains(sensitive_value)));
        let source = error.source().unwrap();
        assert!(!source.to_string().contains(sensitive_value));
        assert_eq!(
            source.to_string(),
            "DuckDB query failed with a Binder error; error message omitted because it may \
             contain row values"
        );
    }

    #[test]
    fn applied_batch_marker_exists_filters_by_replay_epoch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_applied_table_batches" (
                 table_name varchar not null,
                 replay_epoch varchar,
                 batch_id varchar not null,
                 batch_kind varchar not null,
                 first_start_lsn ubigint,
                 last_commit_lsn ubigint,
                 applied_at timestamptz not null
               );"#,
        )
        .unwrap();
        let mut batch = make_prepared_batch(ducklake_table_name());
        batch.replay_epoch = "current".to_owned();
        let table_id = batch.table_name.id();

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_applied_table_batches"
               (table_name, replay_epoch, batch_id, batch_kind, applied_at)
               values ({}, 'other', 'test-batch', 'mutation', current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();
        assert!(!applied_batch_marker_exists(&conn, &batch).unwrap());

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_applied_table_batches"
               (table_name, replay_epoch, batch_id, batch_kind, applied_at)
               values ({}, 'current', 'test-batch', 'mutation', current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();
        assert!(applied_batch_marker_exists(&conn, &batch).unwrap());
    }

    #[test]
    fn applied_batch_marker_exists_treats_null_replay_epoch_as_legacy() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_applied_table_batches" (
                 table_name varchar not null,
                 replay_epoch varchar,
                 batch_id varchar not null,
                 batch_kind varchar not null,
                 first_start_lsn ubigint,
                 last_commit_lsn ubigint,
                 applied_at timestamptz not null
               );"#,
        )
        .unwrap();
        let legacy_batch = make_prepared_batch(ducklake_table_name());
        let mut current_batch = make_prepared_batch(ducklake_table_name());
        current_batch.replay_epoch = "current".to_owned();
        let table_id = legacy_batch.table_name.id();

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_applied_table_batches"
               (table_name, replay_epoch, batch_id, batch_kind, applied_at)
               values ({}, null, 'test-batch', 'mutation', current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();

        assert!(applied_batch_marker_exists(&conn, &legacy_batch).unwrap());
        assert!(!applied_batch_marker_exists(&conn, &current_batch).unwrap());
    }

    #[test]
    fn ensure_helper_table_replay_epoch_column_adds_missing_column() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_applied_table_batches" (
                 table_name varchar not null,
                 batch_id varchar not null
               );"#,
        )
        .unwrap();

        assert!(
            !helper_table_has_column(&conn, APPLIED_BATCHES_TABLE, REPLAY_EPOCH_COLUMN).unwrap()
        );
        ensure_helper_table_replay_epoch_column(&conn, APPLIED_BATCHES_TABLE).unwrap();
        assert!(
            helper_table_has_column(&conn, APPLIED_BATCHES_TABLE, REPLAY_EPOCH_COLUMN).unwrap()
        );
        ensure_helper_table_replay_epoch_column(&conn, APPLIED_BATCHES_TABLE).unwrap();
    }

    #[test]
    fn read_table_streaming_progress_event_position_filters_by_replay_epoch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        attach_lake_catalog(&conn);
        conn.execute_batch(
            r#"create table lake."__etl_streaming_progress" (
                 table_name varchar not null,
                 replay_epoch varchar,
                 last_commit_lsn ubigint not null,
                 last_tx_ordinal ubigint not null,
                 updated_at timestamptz not null
               );"#,
        )
        .unwrap();
        let table_name = ducklake_table_name();
        let table_id = table_name.id();

        conn.execute_batch(&format!(
            r#"insert into lake."__etl_streaming_progress"
               (table_name, replay_epoch, last_commit_lsn, last_tx_ordinal, updated_at)
               values
                 ({0}, 'current', 20, 1, current_timestamp),
                 ({0}, 'other', 999, 0, current_timestamp),
                 ({0}, 'current', 20, 2, current_timestamp),
                 ({0}, null, 30, 0, current_timestamp);"#,
            quote_literal(&table_id)
        ))
        .unwrap();

        let current_position =
            read_table_streaming_progress_event_position(&conn, &table_name, "current").unwrap();
        assert_eq!(current_position, Some(EventPosition::new(PgLsn::from(20), 2)));

        let legacy_position =
            read_table_streaming_progress_event_position(&conn, &table_name, LEGACY_REPLAY_EPOCH)
                .unwrap();
        assert_eq!(legacy_position, Some(EventPosition::new(PgLsn::from(30), 0)));
    }

    #[test]
    fn staging_load_rows_appends_arrow_record_batch() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "create table staging_arrow_copy (id integer, name varchar, created_at timestamp);",
        )
        .unwrap();
        let replicated_table_schema = make_replicated_schema_with_columns(vec![
            ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ColumnSchema::new("created_at".to_owned(), PgType::TIMESTAMP, -1, 3, true),
        ]);
        let prepared_rows = prepare_copy_rows(
            &replicated_table_schema,
            vec![
                TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                    Cell::Timestamp(Timestamp::Value(
                        chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
                            .unwrap()
                            .and_hms_opt(3, 4, 5)
                            .unwrap(),
                    )),
                ]),
                TableRow::new(vec![Cell::I32(2), Cell::Null, Cell::Null]),
            ],
        )
        .unwrap();
        assert!(matches!(prepared_rows, PreparedRows::ArrowRecordBatch(_)));

        let staging_table = ReusableStagingTable {
            table_name: ducklake_table_name(),
            staging_name: "staging_arrow_copy".to_owned(),
            created: true,
            insert_column_names: vec!["id".to_owned(), "name".to_owned(), "created_at".to_owned()],
        };

        staging_table.load_rows(&conn, &prepared_rows).unwrap();

        let count: i64 = conn
            .query_row("select count(*) from staging_arrow_copy", [], |row| row.get(0))
            .unwrap();
        let id_sum: i64 =
            conn.query_row("select sum(id) from staging_arrow_copy", [], |row| row.get(0)).unwrap();
        assert_eq!(count, 2);
        assert_eq!(id_sum, 3);
    }

    #[test]
    fn apply_delete_mutation_failure_omits_row_values_from_detail() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let batch = make_prepared_batch(ducklake_table_name());
        let operation_context = DuckLakeBlockingOperationContext::for_tests();
        let sensitive_value = "alice@example.com";
        let predicates = vec![format!("\"email\" = '{sensitive_value}'")];

        let error = apply_delete_mutation(
            &conn,
            &batch,
            predicates.as_slice(),
            "delete",
            &operation_context,
        )
        .unwrap_err();

        assert_query_failure_omits_sensitive_value(
            &error,
            "DuckLake DELETE failed",
            sensitive_value,
        );
    }

    #[test]
    fn apply_update_mutation_failure_omits_row_values_from_detail() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let sensitive_value = "secret-token";
        let assignments = vec![format!("\"token\" = '{sensitive_value}'")];
        let predicate = format!("\"token\" = '{sensitive_value}'");

        let error = apply_update_mutation(
            &conn,
            &ducklake_table_name(),
            assignments.as_slice(),
            &predicate,
        )
        .unwrap_err();

        assert_query_failure_omits_sensitive_value(
            &error,
            "DuckLake UPDATE failed",
            sensitive_value,
        );
    }

    #[test]
    fn delete_predicate_from_row_uses_only_replica_identity_columns() {
        let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), PgType::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        )));
        let row =
            TableRow::new(vec![Cell::I32(7), Cell::I32(42), Cell::String("alice".to_owned())]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"tenant_id\" = 7 AND \"id\" = 42"
        );
    }

    #[test]
    fn delete_predicate_from_row_supports_alternative_identity_without_primary_key() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0]),
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("alice@example.com".to_owned()),
            Cell::String("alice".to_owned()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"email\" = 'alice@example.com'"
        );
    }

    #[test]
    fn delete_predicate_from_row_uses_full_replica_identity_columns() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 1, 1]),
        );
        let row = TableRow::new(vec![
            Cell::I32(7),
            Cell::String("alice@example.com".to_owned()),
            Cell::String("alice".to_owned()),
        ]);

        assert_eq!(
            delete_predicate_from_row(&replicated_table_schema, &row).unwrap(),
            "\"id\" = 7 AND \"email\" = 'alice@example.com' AND \"name\" = 'alice'"
        );
    }

    #[test]
    fn delete_predicate_from_row_rejects_missing_replica_identity() {
        let table_schema = Arc::new(make_schema());
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 0]),
        );
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let error = delete_predicate_from_row(&replicated_table_schema, &row).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::SourceReplicaIdentityError);
        assert_eq!(error.description(), Some("DuckLake delete requires a replica identity"));
    }

    #[test]
    fn prepare_table_mutations_replace_emits_delete_then_upsert() {
        let replicated_table_schema = make_replicated_schema();
        let row = TableRow::new(vec![Cell::I32(1), Cell::String("alice".to_owned())]);

        let prepared =
            prepare_table_mutations(&replicated_table_schema, vec![TableMutation::Replace(row)])
                .unwrap();

        assert_eq!(prepared.len(), 2);
        match &prepared[0] {
            PreparedTableMutation::Delete { predicates, origin } => {
                assert_eq!(predicates, &vec!["\"id\" = 1".to_owned()]);
                assert_eq!(origin, &"replace");
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                panic!("expected delete first")
            }
        }
        match &prepared[1] {
            PreparedTableMutation::Upsert(PreparedRows::Appender(rows)) => {
                assert_eq!(rows.len(), 1);
            }
            PreparedTableMutation::Upsert(PreparedRows::SqlLiterals(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Upsert(PreparedRows::ArrowRecordBatch(_)) => {
                panic!("expected appender payload")
            }
            PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => {
                panic!("expected upsert second")
            }
        }
    }

    #[test]
    fn prepare_table_mutations_update_emits_update_statement() {
        let replicated_table_schema = make_replicated_schema();
        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::I32(1)])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    2,
                    TableRow::new(vec![Cell::I32(1), Cell::String("after".to_owned())]),
                    vec![],
                )),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec!["\"id\" = 1".to_owned(), "\"name\" = 'after'".to_owned()]
                );
                assert_eq!(predicate, "\"id\" = 1");
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Delete { .. } => {
                panic!("expected update")
            }
        }
    }

    #[test]
    fn prepare_table_mutations_update_uses_alternative_identity_key_for_changed_key_update() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 4, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![0, 1, 0, 0]),
        );

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Key(TableRow::new(vec![Cell::String(
                    "alice@example.com".to_owned(),
                )])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    4,
                    TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice@new.example.com".to_owned()),
                        Cell::String("ripe".to_owned()),
                    ]),
                    vec![3],
                )),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec![
                        "\"id\" = 1".to_owned(),
                        "\"email\" = 'alice@new.example.com'".to_owned(),
                        "\"name\" = 'ripe'".to_owned(),
                    ]
                );
                assert_eq!(predicate, "\"email\" = 'alice@example.com'");
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Delete { .. } => {
                panic!("expected update")
            }
        }
    }

    #[test]
    fn prepare_table_mutations_update_uses_full_replica_identity_predicate() {
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), "users".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 2, false),
                ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 3, true),
                ColumnSchema::new("payload".to_owned(), PgType::TEXT, -1, 4, true),
            ],
        ));
        let replicated_table_schema = ReplicatedTableSchema::from_masks(
            Arc::clone(&table_schema),
            ReplicationMask::all(&table_schema),
            IdentityMask::from_bytes(vec![1, 1, 1, 1]),
        );

        let prepared = prepare_table_mutations(
            &replicated_table_schema,
            vec![TableMutation::Update {
                delete_row: OldTableRow::Full(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice@example.com".to_owned()),
                    Cell::String("seed".to_owned()),
                    Cell::String("toast".to_owned()),
                ])),
                new_row: UpdatedTableRow::Partial(PartialTableRow::new(
                    4,
                    TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice@example.com".to_owned()),
                        Cell::String("grown".to_owned()),
                    ]),
                    vec![3],
                )),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        match &prepared[0] {
            PreparedTableMutation::Update { assignments, predicate } => {
                assert_eq!(
                    assignments,
                    &vec![
                        "\"id\" = 1".to_owned(),
                        "\"email\" = 'alice@example.com'".to_owned(),
                        "\"name\" = 'grown'".to_owned(),
                    ]
                );
                assert_eq!(
                    predicate,
                    "\"id\" = 1 AND \"email\" = 'alice@example.com' AND \"name\" = 'seed' AND \
                     \"payload\" = 'toast'"
                );
            }
            PreparedTableMutation::Upsert(_) | PreparedTableMutation::Delete { .. } => {
                panic!("expected update")
            }
        }
    }

    #[test]
    fn prepare_mutation_table_batches_insert_only_uses_single_upsert_operation() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(20), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(20), 1),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
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
                    PreparedTableMutation::Upsert(PreparedRows::ArrowRecordBatch(_)) => {
                        panic!("expected appender payload")
                    }
                    PreparedTableMutation::Delete { .. } | PreparedTableMutation::Update { .. } => {
                        panic!("expected upsert")
                    }
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_split_mixed_cdc_at_delete_boundaries() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 1),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ]))),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 2),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_owned()),
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
    fn prepare_mutation_table_batches_group_contiguous_deletes() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 0),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ]))),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(120), 0),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ]))),
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
                            &vec!["\"id\" = 1".to_owned(), "\"id\" = 2".to_owned()]
                        );
                    }
                    PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                        panic!("expected delete batch")
                    }
                }
            }
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_group_contiguous_updates() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 0),
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("before-a".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(1),
                            Cell::String("after-a".to_owned()),
                        ])),
                    },
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(120), 0),
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("before-b".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(2),
                            Cell::String("after-b".to_owned()),
                        ])),
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
    fn prepare_mutation_table_batches_split_non_inserts_at_cap() {
        let replicated_table_schema = make_replicated_schema();
        let tracked = (0..=CDC_MUTATION_BATCH_SIZE)
            .map(|idx| {
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(200 + idx as u64), 0),
                    TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(idx as i32),
                        Cell::String(format!("name-{idx}")),
                    ]))),
                )
            })
            .collect();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            tracked,
        )
        .unwrap();

        assert_eq!(batches.len(), 2);

        match &batches[0].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), CDC_MUTATION_BATCH_SIZE);
                }
                PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                    panic!("expected delete batch")
                }
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }

        match &batches[1].action {
            PreparedDuckLakeTableBatchAction::Mutation(prepared) => match &prepared[0] {
                PreparedTableMutation::Delete { predicates, .. } => {
                    assert_eq!(predicates.len(), 1);
                }
                PreparedTableMutation::Upsert(_) | PreparedTableMutation::Update { .. } => {
                    panic!("expected delete batch")
                }
            },
            PreparedDuckLakeTableBatchAction::Truncate => panic!("expected mutation batch"),
        }
    }

    #[test]
    fn prepare_mutation_table_batches_isolate_update_in_its_own_atomic_batch() {
        let replicated_table_schema = make_replicated_schema();
        let batches = prepare_mutation_table_batches(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            vec![
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(0),
                        Cell::String("seed".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(120), 1),
                    TableMutation::Update {
                        delete_row: OldTableRow::Full(TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("seed".to_owned()),
                        ])),
                        new_row: UpdatedTableRow::Full(TableRow::new(vec![
                            Cell::I32(0),
                            Cell::String("grown".to_owned()),
                        ])),
                    },
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(130), 2),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(999),
                        Cell::String("tail".to_owned()),
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
    fn retain_mutations_after_event_position_drops_applied_prefix() {
        let retained = retain_mutations_after_event_position(
            vec![
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(110), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("one".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(120), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("two".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(130), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(3),
                        Cell::String("three".to_owned()),
                    ])),
                ),
            ],
            Some(EventPosition::new(PgLsn::from(120), 0)),
        );

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].event_position(), EventPosition::new(PgLsn::from(130), 0));
    }

    #[test]
    fn retain_truncates_after_event_position_drops_applied_prefix() {
        let retained = retain_truncates_after_event_position(
            vec![
                TrackedTruncateEvent::new(EventPosition::new(PgLsn::from(200), 0), 0),
                TrackedTruncateEvent::new(EventPosition::new(PgLsn::from(200), 1), 0),
                TrackedTruncateEvent::new(EventPosition::new(PgLsn::from(210), 0), 0),
            ],
            Some(EventPosition::new(PgLsn::from(200), 0)),
        );

        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].event_position(), EventPosition::new(PgLsn::from(200), 1));
        assert_eq!(retained[1].event_position(), EventPosition::new(PgLsn::from(210), 0));
    }

    #[test]
    fn prepare_copy_table_batch_uses_propagated_id() {
        let replicated_table_schema = make_replicated_schema();
        let batch_id = TableCopyBatchId::new(TableCopyAttemptId::from_u128(1), 2);
        let expected_batch_id = batch_id.to_string();
        let prepared = prepare_copy_table_batch(
            &replicated_table_schema,
            ducklake_table_name(),
            LEGACY_REPLAY_EPOCH.to_owned(),
            batch_id,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("identical".to_owned())])],
        )
        .unwrap();

        assert_eq!(prepared.batch.batch_id, expected_batch_id);
    }

    #[test]
    fn build_mutation_batch_identity_is_deterministic() {
        let replicated_table_schema = make_replicated_schema();
        let tracked = vec![
            TrackedTableMutation::new(
                EventPosition::new(PgLsn::from(200), 0),
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ])),
            ),
            TrackedTableMutation::new(
                EventPosition::new(PgLsn::from(200), 1),
                TableMutation::Delete(OldTableRow::Full(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ]))),
            ),
        ];

        let table_name = ducklake_table_name();
        let first =
            build_mutation_batch_identity(&table_name, &replicated_table_schema, &tracked).unwrap();
        let second =
            build_mutation_batch_identity(&table_name, &replicated_table_schema, &tracked).unwrap();

        assert_eq!(first.batch_id, second.batch_id);
        assert_eq!(first.first_start_lsn, None);
        assert_eq!(first.last_commit_lsn, Some(PgLsn::from(200)));
    }

    #[test]
    fn build_mutation_batch_identity_changes_with_order_and_event_position() {
        let replicated_table_schema = make_replicated_schema();
        let table_name = ducklake_table_name();
        let original = build_mutation_batch_identity(
            &table_name,
            &replicated_table_schema,
            &[
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(200), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(200), 1),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let reordered = build_mutation_batch_identity(
            &table_name,
            &replicated_table_schema,
            &[
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(200), 0),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("bob".to_owned()),
                    ])),
                ),
                TrackedTableMutation::new(
                    EventPosition::new(PgLsn::from(200), 1),
                    TableMutation::Insert(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("alice".to_owned()),
                    ])),
                ),
            ],
        )
        .unwrap();
        let changed_lsn = build_mutation_batch_identity(
            &table_name,
            &replicated_table_schema,
            &[TrackedTableMutation::new(
                EventPosition::new(PgLsn::from(201), 0),
                TableMutation::Insert(TableRow::new(vec![
                    Cell::I32(1),
                    Cell::String("alice".to_owned()),
                ])),
            )],
        )
        .unwrap();

        assert_ne!(original.batch_id, reordered.batch_id);
        assert_ne!(original.batch_id, changed_lsn.batch_id);
    }

    #[test]
    fn build_truncate_batch_identity_changes_with_event_position() {
        let table_name = ducklake_table_name();
        let first = build_truncate_batch_identity(
            &table_name,
            &[TrackedTruncateEvent::new(EventPosition::new(PgLsn::from(400), 0), 0)],
        );
        let second = build_truncate_batch_identity(
            &table_name,
            &[TrackedTruncateEvent::new(EventPosition::new(PgLsn::from(401), 0), 0)],
        );

        assert_ne!(first.batch_id, second.batch_id);
    }

    /// Replica-identity predicate and single-row mutation properties.
    ///
    /// One source DELETE or UPDATE changes exactly one Postgres tuple, so the
    /// destination predicate built from its old row must select exactly the
    /// rows whose identity equals it, and applying the mutation must change
    /// exactly one destination row.
    mod identity_properties {
        use std::str::FromStr;

        use duckdb::types::Value;
        use etl::{
            data::{ArrayCell, PgNumeric},
            test_utils::property::{any_f32, any_f64, run_property},
        };
        use proptest::{
            collection::vec,
            option,
            prelude::*,
            sample::{Index, subsequence},
        };

        use super::*;
        use crate::ducklake::{
            encoding::tests::encoder_properties::{
                expected_value, json_value, readback_expression, sql_text, values_match,
            },
            schema::build_create_table_sql_ducklake,
        };

        /// How the generated table identifies rows in DELETE and UPDATE
        /// events.
        #[derive(Clone, Copy, Debug)]
        enum IdentityKind {
            /// `replica identity default` with an `id` primary key.
            PrimaryKey,
            /// `replica identity full` on a table without a primary key.
            Full,
        }

        /// Payload column type in the generated table.
        #[derive(Clone, Copy, Debug, PartialEq)]
        enum PayloadKind {
            Float8,
            Float4,
            Json,
            Decimal,
            VarcharNumeric,
            Int4Array,
            Text,
            Bool,
        }

        /// Every payload kind.
        const ALL_PAYLOAD_KINDS: [PayloadKind; 8] = [
            PayloadKind::Float8,
            PayloadKind::Float4,
            PayloadKind::Json,
            PayloadKind::Decimal,
            PayloadKind::VarcharNumeric,
            PayloadKind::Int4Array,
            PayloadKind::Text,
            PayloadKind::Bool,
        ];

        impl PayloadKind {
            /// Returns the Postgres type and modifier of this column.
            fn pg_type(self) -> (PgType, i32) {
                match self {
                    Self::Float8 => (PgType::FLOAT8, -1),
                    Self::Float4 => (PgType::FLOAT4, -1),
                    Self::Json => (PgType::JSONB, -1),
                    // `numeric(10, 2)` maps to DuckDB `decimal(10, 2)`.
                    Self::Decimal => (PgType::NUMERIC, ((10 << 16) | 2) + 4),
                    Self::VarcharNumeric => (PgType::NUMERIC, -1),
                    Self::Int4Array => (PgType::INT4_ARRAY, -1),
                    Self::Text => (PgType::TEXT, -1),
                    Self::Bool => (PgType::BOOL, -1),
                }
            }

            /// Returns codec-shaped cells for this column, including NULLs.
            fn cell(self) -> BoxedStrategy<Cell> {
                let value = match self {
                    Self::Float8 => any_f64().prop_map(Cell::F64).boxed(),
                    Self::Float4 => any_f32().prop_map(Cell::F32).boxed(),
                    Self::Json => json_value().prop_map(Cell::Json).boxed(),
                    Self::Decimal => (any::<bool>(), 0i64..100_000_000, 0u8..100)
                        .prop_map(|(negative, integer, fraction)| {
                            let sign =
                                if negative && (integer, fraction) != (0, 0) { "-" } else { "" };
                            let text = format!("{sign}{integer}.{fraction:02}");
                            Cell::Numeric(PgNumeric::from_str(&text).unwrap())
                        })
                        .boxed(),
                    Self::VarcharNumeric => prop_oneof![
                        Just("NaN".to_owned()),
                        Just("-Infinity".to_owned()),
                        "-?[0-9]{1,30}(\\.[0-9]{1,10})?",
                    ]
                    .prop_map(|text| Cell::Numeric(PgNumeric::from_str(&text).unwrap()))
                    .boxed(),
                    Self::Int4Array => vec(option::weighted(0.8, -3i32..3), 0..4)
                        .prop_map(|values| Cell::Array(ArrayCell::I32(values)))
                        .boxed(),
                    Self::Text => sql_text().prop_map(Cell::String).boxed(),
                    Self::Bool => any::<bool>().prop_map(Cell::Bool).boxed(),
                };
                prop_oneof![1 => Just(Cell::Null), 5 => value].boxed()
            }
        }

        /// Generated table rows plus one DELETE or full UPDATE of a target row.
        #[derive(Clone, Debug)]
        struct IdentityCase {
            identity: IdentityKind,
            payload: Vec<PayloadKind>,
            rows: Vec<TableRow>,
            target: usize,
            /// Replacement row for an UPDATE, or `None` for a DELETE.
            update: Option<TableRow>,
        }

        impl IdentityCase {
            /// Returns the replicated schema with the case's identity.
            fn schema(&self) -> ReplicatedTableSchema {
                let mut id = ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false);
                if matches!(self.identity, IdentityKind::PrimaryKey) {
                    id = id.with_primary_key(1);
                }
                let mut columns = vec![id];
                for (index, kind) in self.payload.iter().enumerate() {
                    let (typ, modifier) = kind.pg_type();
                    columns.push(ColumnSchema::new(
                        format!("c{index}"),
                        typ,
                        modifier,
                        index as i32 + 2,
                        true,
                    ));
                }
                let identity_mask = columns
                    .iter()
                    .enumerate()
                    .map(|(index, _)| {
                        u8::from(index == 0 || matches!(self.identity, IdentityKind::Full))
                    })
                    .collect();
                let table_schema = Arc::new(TableSchema::new(
                    TableId::new(1),
                    TableName::new("public".to_owned(), "users".to_owned()),
                    columns,
                ));
                ReplicatedTableSchema::from_masks(
                    Arc::clone(&table_schema),
                    ReplicationMask::all(&table_schema),
                    IdentityMask::from_bytes(identity_mask),
                )
            }

            /// Returns the old-row image Postgres sends for `row`.
            fn old_row(&self, row: &TableRow) -> OldTableRow {
                match self.identity {
                    IdentityKind::PrimaryKey => {
                        OldTableRow::Key(TableRow::new(vec![row.values()[0].clone()]))
                    }
                    IdentityKind::Full => OldTableRow::Full(row.clone()),
                }
            }

            /// Returns whether two rows have equal replica-identity values
            /// under Postgres equality.
            fn identity_equal(&self, left: &TableRow, right: &TableRow) -> bool {
                match self.identity {
                    IdentityKind::PrimaryKey => left.values()[0] == right.values()[0],
                    IdentityKind::Full => left
                        .values()
                        .iter()
                        .zip(right.values())
                        .all(|(left, right)| postgres_cells_equal(left, right)),
                }
            }
        }

        /// Compares cells like Postgres does: `-0 = 0` and `NaN = NaN` for
        /// floats, structural equality otherwise.
        fn postgres_cells_equal(left: &Cell, right: &Cell) -> bool {
            match (left, right) {
                (Cell::F64(left), Cell::F64(right)) => {
                    left == right || (left.is_nan() && right.is_nan())
                }
                (Cell::F32(left), Cell::F32(right)) => {
                    left == right || (left.is_nan() && right.is_nan())
                }
                _ => left == right,
            }
        }

        /// Generates tables whose rows are drawn from a small pool, so equal
        /// identities and (for FULL identity) duplicate tuples are common.
        fn identity_case(
            identities: Vec<IdentityKind>,
            kinds: &'static [PayloadKind],
        ) -> impl Strategy<Value = IdentityCase> {
            (proptest::sample::select(identities), subsequence(kinds, 1..=4))
                .prop_flat_map(|(identity, payload)| {
                    let row = (0i32..3, payload.iter().map(|kind| kind.cell()).collect::<Vec<_>>());
                    (
                        Just(identity),
                        Just(payload),
                        vec(row.clone(), 1..4),
                        vec(any::<Index>(), 1..7),
                        any::<Index>(),
                        option::of(row),
                    )
                })
                .prop_map(|(identity, payload, pool, picks, target, update)| {
                    let make_row = |id: i32, values: &[Cell]| {
                        TableRow::new(
                            std::iter::once(Cell::I32(id)).chain(values.iter().cloned()).collect(),
                        )
                    };
                    let rows = picks
                        .iter()
                        .enumerate()
                        .map(|(index, pick)| {
                            let (id, values) = pick.get(&pool);
                            // Primary keys are unique in Postgres;
                            // FULL-identity
                            // tables keep the pooled id so whole tuples repeat.
                            let id = match identity {
                                IdentityKind::PrimaryKey => index as i32,
                                IdentityKind::Full => *id,
                            };
                            make_row(id, values)
                        })
                        .collect::<Vec<_>>();
                    let target = target.index(rows.len());
                    let update = update.map(|(id, values)| {
                        let id = match identity {
                            IdentityKind::PrimaryKey => target as i32,
                            IdentityKind::Full => id,
                        };
                        make_row(id, &values)
                    });
                    IdentityCase { identity, payload, rows, target, update }
                })
        }

        /// Opens an in-memory lake with the streaming progress helper table
        /// and the case's destination table.
        pub(super) fn lake_connection(schema: &ReplicatedTableSchema) -> duckdb::Connection {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            attach_lake_catalog(&conn);
            conn.execute_batch(
                r#"create table lake."__etl_streaming_progress" (
                     table_name varchar not null,
                     replay_epoch varchar,
                     last_commit_lsn ubigint not null,
                     last_tx_ordinal ubigint not null,
                     updated_at timestamptz not null
                   );"#,
            )
            .unwrap();
            let columns =
                schema.destination_column_schemas(DUCKLAKE_COLUMN_NAME_MAPPING).collect::<Vec<_>>();
            conn.execute_batch(&build_create_table_sql_ducklake(&ducklake_table_name(), &columns))
                .unwrap();
            conn
        }

        /// Applies mutations at consecutive LSNs starting at `first_lsn`
        /// through the production batch preparation and apply path.
        fn apply_mutations(
            conn: &duckdb::Connection,
            schema: &ReplicatedTableSchema,
            first_lsn: u64,
            mutations: Vec<TableMutation>,
        ) -> EtlResult<()> {
            let tracked = mutations
                .into_iter()
                .enumerate()
                .map(|(offset, mutation)| {
                    TrackedTableMutation::new(
                        EventPosition::new(PgLsn::from(first_lsn + offset as u64), 0),
                        mutation,
                    )
                })
                .collect();
            let batches = prepare_mutation_table_batches(
                schema,
                ducklake_table_name(),
                LEGACY_REPLAY_EPOCH.to_owned(),
                tracked,
            )?;
            apply_table_batches(
                conn,
                &batches,
                &DuckLakeBlockingOperationContext::for_tests(),
                &mut TableStreamingProgressCache::default(),
            )
        }

        /// Reads the destination table as rows of comparable values.
        fn read_rows(conn: &duckdb::Connection, schema: &ReplicatedTableSchema) -> Vec<Vec<Value>> {
            let columns = schema
                .column_schemas()
                .map(|column| readback_expression(&column.typ, &column.name))
                .collect::<Vec<_>>();
            let mut statement = conn
                .prepare(&format!(
                    "select {} from {}",
                    columns.join(", "),
                    qualified_lake_table_name(&ducklake_table_name())
                ))
                .unwrap();
            statement
                .query_map([], |row| {
                    (0..columns.len()).map(|index| row.get::<_, Value>(index)).collect()
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        }

        /// Returns whether the stored rows equal the model rows as multisets.
        fn rows_match_model(
            schema: &ReplicatedTableSchema,
            stored: &[Vec<Value>],
            model: &[TableRow],
        ) -> bool {
            let json_columns = schema
                .column_schemas()
                .map(|column| matches!(column.typ, PgType::JSON | PgType::JSONB))
                .collect::<Vec<_>>();
            let mut unmatched = stored.iter().collect::<Vec<_>>();
            stored.len() == model.len()
                && model.iter().all(|row| {
                    let expected = row.values().iter().map(expected_value).collect::<Vec<_>>();
                    let position = unmatched.iter().position(|stored| {
                        expected.iter().zip(stored.iter()).zip(&json_columns).all(
                            |((expected, stored), is_json)| {
                                values_match(expected, stored, *is_json)
                            },
                        )
                    });
                    position.map(|position| unmatched.swap_remove(position)).is_some()
                })
        }

        /// Checks the predicate count law and the single-row mutation law.
        ///
        /// With `allow_duplicates` false, the mutation law is skipped when
        /// several rows share the target identity; that case is pinned by
        /// `full_identity_changes_exactly_one_row_with_duplicates`.
        fn check_identity_case(
            case: &IdentityCase,
            allow_duplicates: bool,
        ) -> Result<(), TestCaseError> {
            let schema = case.schema();
            let conn = lake_connection(&schema);
            apply_mutations(
                &conn,
                &schema,
                1,
                case.rows.iter().cloned().map(TableMutation::Insert).collect(),
            )
            .unwrap();
            prop_assert!(
                rows_match_model(&schema, &read_rows(&conn, &schema), &case.rows),
                "inserted rows differ from the model"
            );

            let target_row = &case.rows[case.target];
            let old_row = case.old_row(target_row);
            let matching =
                case.rows.iter().filter(|row| case.identity_equal(row, target_row)).count();
            let predicate = delete_predicate_from_row(&schema, &old_row).unwrap();
            let count: i64 = conn
                .query_row(
                    &format!(
                        "select count(*) from {} where {predicate}",
                        qualified_lake_table_name(&ducklake_table_name())
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            prop_assert_eq!(
                count as usize,
                matching,
                "predicate {} matched a different number of rows",
                predicate
            );
            if matching > 1 && !allow_duplicates {
                return Ok(());
            }

            let mut model = case.rows.clone();
            model.remove(case.target);
            let mutation = match &case.update {
                Some(new_row) => {
                    model.push(new_row.clone());
                    TableMutation::Update {
                        delete_row: old_row,
                        new_row: UpdatedTableRow::Full(new_row.clone()),
                    }
                }
                None => TableMutation::Delete(old_row),
            };
            apply_mutations(&conn, &schema, case.rows.len() as u64 + 1, vec![mutation]).unwrap();
            let stored = read_rows(&conn, &schema);
            prop_assert!(
                rows_match_model(&schema, &stored, &model),
                "after one mutation the table holds {} rows {:?}, expected {} rows",
                stored.len(),
                stored,
                model.len()
            );
            Ok(())
        }

        #[test]
        fn identity_predicates_match_and_mutations_change_exactly_one_row() {
            run_property(
                "ducklake_identity_predicates_change_one_row",
                &identity_case(
                    vec![IdentityKind::PrimaryKey, IdentityKind::Full],
                    &ALL_PAYLOAD_KINDS,
                ),
                |case| check_identity_case(case, false),
            );
        }

        #[test]
        #[ignore = "known defect: replica identity full deletes or updates every duplicate tuple"]
        fn full_identity_changes_exactly_one_row_with_duplicates() {
            run_property(
                "ducklake_full_identity_duplicates_change_one_row",
                &identity_case(vec![IdentityKind::Full], &ALL_PAYLOAD_KINDS),
                |case| check_identity_case(case, true),
            );
        }

        /// Returns a FULL-identity case over `id int4` plus one text column.
        fn duplicate_text_case(update: Option<TableRow>) -> IdentityCase {
            let row = TableRow::new(vec![Cell::I32(1), Cell::String("a".to_owned())]);
            IdentityCase {
                identity: IdentityKind::Full,
                payload: vec![PayloadKind::Text],
                rows: vec![row.clone(), row],
                target: 0,
                update,
            }
        }

        #[test]
        #[ignore = "known defect: replica identity full delete removes every duplicate tuple"]
        fn full_identity_delete_removes_only_one_of_duplicate_rows() {
            check_identity_case(&duplicate_text_case(None), true).unwrap();
        }

        #[test]
        #[ignore = "known defect: replica identity full update rewrites every duplicate tuple"]
        fn full_identity_update_changes_only_one_of_duplicate_rows() {
            let new_row = TableRow::new(vec![Cell::I32(1), Cell::String("b".to_owned())]);
            check_identity_case(&duplicate_text_case(Some(new_row)), true).unwrap();
        }

        #[test]
        fn full_identity_delete_matches_appended_float8_value() {
            let case = IdentityCase {
                identity: IdentityKind::Full,
                payload: vec![PayloadKind::Float8],
                rows: vec![TableRow::new(vec![Cell::I32(1), Cell::F64(1e-30)])],
                target: 0,
                update: None,
            };
            check_identity_case(&case, false).unwrap();
        }
    }

    /// Batching, crash, and redelivery properties for streaming mutations.
    ///
    /// Postgres redelivers from the confirmed flush position after a crash,
    /// and each attempt may split the stream at different `write_events`
    /// boundaries. The streaming progress watermark must make the final table
    /// equal to applying every source event exactly once, in order.
    mod replay_properties {
        use std::{collections::BTreeMap, ops::Range};

        use etl::test_utils::property::run_property;
        use proptest::{collection::vec, prelude::*, sample::Index};

        use super::{identity_properties::lake_connection, *};

        /// Number of events per generated source transaction.
        const EVENTS_PER_TRANSACTION: usize = 3;

        /// One source event on the `(id primary key, v text)` table.
        #[derive(Clone, Debug)]
        enum ModelOp {
            Insert(i32, String),
            Delete(i32),
            Update(i32, String),
            Replace(i32, String),
        }

        impl ModelOp {
            /// Returns the CDC mutation the destination receives for this
            /// event.
            fn mutation(&self) -> TableMutation {
                let row = |id: i32, value: &str| {
                    TableRow::new(vec![Cell::I32(id), Cell::String(value.to_owned())])
                };
                let key = |id: i32| OldTableRow::Key(TableRow::new(vec![Cell::I32(id)]));
                match self {
                    Self::Insert(id, value) => TableMutation::Insert(row(*id, value)),
                    Self::Delete(id) => TableMutation::Delete(key(*id)),
                    Self::Update(id, value) => TableMutation::Update {
                        delete_row: key(*id),
                        new_row: UpdatedTableRow::Full(row(*id, value)),
                    },
                    Self::Replace(id, value) => TableMutation::Replace(row(*id, value)),
                }
            }

            /// Applies this event to the exactly-once model.
            fn apply(&self, model: &mut BTreeMap<i32, String>) {
                match self {
                    Self::Insert(id, value)
                    | Self::Update(id, value)
                    | Self::Replace(id, value) => {
                        model.insert(*id, value.clone());
                    }
                    Self::Delete(id) => {
                        model.remove(id);
                    }
                }
            }
        }

        /// A source event stream with two delivery attempts.
        #[derive(Clone, Debug)]
        struct ReplayCase {
            ops: Vec<ModelOp>,
            /// `write_events` boundaries for the first attempt.
            first_cuts: Vec<Index>,
            /// First-attempt call that crashes.
            crash_call: Index,
            /// Batches of the crashing call that commit before the crash.
            committed_batches: Index,
            /// Whether the crashing call is retried before the redelivery.
            retry_crashed_call: bool,
            /// Redelivery start, at or before the crashed call.
            redelivery_start: Index,
            /// `write_events` boundaries for the redelivery.
            second_cuts: Vec<Index>,
        }

        /// Turns raw choices into events that are valid for a primary-key
        /// table: inserts only for absent keys, other events only for present
        /// keys.
        fn valid_ops(raw: Vec<(u8, i32, String)>) -> Vec<ModelOp> {
            let mut present = BTreeMap::new();
            raw.into_iter()
                .map(|(choice, id, value)| {
                    if present.insert(id, ()).is_none() {
                        return ModelOp::Insert(id, value);
                    }
                    match choice % 3 {
                        0 => {
                            present.remove(&id);
                            ModelOp::Delete(id)
                        }
                        1 => ModelOp::Update(id, value),
                        _ => ModelOp::Replace(id, value),
                    }
                })
                .collect()
        }

        /// Generates long event streams over a few colliding keys, so atomic
        /// batches (capped at `CDC_MUTATION_BATCH_SIZE`) mix every event kind.
        fn replay_case() -> impl Strategy<Value = ReplayCase> {
            (
                vec((any::<u8>(), 0i32..4, "[a-c]{0,2}"), 1..40).prop_map(valid_ops),
                vec(any::<Index>(), 0..5),
                any::<Index>(),
                any::<Index>(),
                any::<bool>(),
                any::<Index>(),
                vec(any::<Index>(), 0..5),
            )
                .prop_map(
                    |(
                        ops,
                        first_cuts,
                        crash_call,
                        committed_batches,
                        retry_crashed_call,
                        redelivery_start,
                        second_cuts,
                    )| ReplayCase {
                        ops,
                        first_cuts,
                        crash_call,
                        committed_batches,
                        retry_crashed_call,
                        redelivery_start,
                        second_cuts,
                    },
                )
        }

        /// Returns the strictly increasing source position of event `index`.
        fn event_position(index: usize) -> EventPosition {
            let transaction = (index / EVENTS_PER_TRANSACTION) as u64;
            EventPosition::new(
                PgLsn::from(100 + transaction * 10),
                (index % EVENTS_PER_TRANSACTION) as u64,
            )
        }

        /// Splits `range` into consecutive `write_events` calls at `cuts`.
        fn split(range: Range<usize>, cuts: &[Index]) -> Vec<Range<usize>> {
            let mut bounds = cuts
                .iter()
                .map(|cut| range.start + cut.index(range.len() + 1))
                .chain([range.start, range.end])
                .collect::<Vec<_>>();
            bounds.sort_unstable();
            bounds.dedup();
            bounds.windows(2).map(|pair| pair[0]..pair[1]).collect()
        }

        /// Delivers one `write_events` call like the destination does: read
        /// the watermark, drop applied events, prepare, and apply.
        ///
        /// With `crash_after`, only that many prepared batches commit before
        /// the simulated crash; `retry` then reapplies the whole call like
        /// `apply_table_batches_with_retry` does.
        fn deliver(
            conn: &duckdb::Connection,
            schema: &ReplicatedTableSchema,
            ops: &[ModelOp],
            range: Range<usize>,
            crash_after: Option<Index>,
            retry: bool,
        ) -> EtlResult<()> {
            let last_event_position = read_table_streaming_progress_event_position(
                conn,
                &ducklake_table_name(),
                LEGACY_REPLAY_EPOCH,
            )?;
            let tracked = range
                .map(|index| {
                    TrackedTableMutation::new(event_position(index), ops[index].mutation())
                })
                .collect();
            let pending = retain_mutations_after_event_position(tracked, last_event_position);
            let batches = prepare_mutation_table_batches(
                schema,
                ducklake_table_name(),
                LEGACY_REPLAY_EPOCH.to_owned(),
                pending,
            )?;
            let context = DuckLakeBlockingOperationContext::for_tests();
            match crash_after {
                Some(committed) => {
                    let committed = committed.index(batches.len() + 1);
                    apply_table_batches(
                        conn,
                        &batches[..committed],
                        &context,
                        &mut TableStreamingProgressCache::default(),
                    )?;
                    if retry {
                        apply_table_batches(
                            conn,
                            &batches,
                            &context,
                            &mut TableStreamingProgressCache::default(),
                        )?;
                    }
                    Ok(())
                }
                None => apply_table_batches(
                    conn,
                    &batches,
                    &context,
                    &mut TableStreamingProgressCache::default(),
                ),
            }
        }

        /// Checks that a crash plus redelivery leaves the exactly-once state.
        fn check_replay(case: &ReplayCase) -> Result<(), TestCaseError> {
            let table_schema = Arc::new(TableSchema::new(
                TableId::new(1),
                TableName::new("public".to_owned(), "users".to_owned()),
                vec![
                    ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false)
                        .with_primary_key(1),
                    ColumnSchema::new("v".to_owned(), PgType::TEXT, -1, 2, true),
                ],
            ));
            let schema = ReplicatedTableSchema::from_masks(
                Arc::clone(&table_schema),
                ReplicationMask::all(&table_schema),
                IdentityMask::from_bytes(vec![1, 0]),
            );
            let conn = lake_connection(&schema);
            let ops = &case.ops;

            let first_calls = split(0..ops.len(), &case.first_cuts);
            let crash_call = case.crash_call.index(first_calls.len());
            for (call, range) in first_calls.iter().enumerate().take(crash_call + 1) {
                let crash = (call == crash_call).then_some(case.committed_batches);
                let result =
                    deliver(&conn, &schema, ops, range.clone(), crash, case.retry_crashed_call);
                prop_assert!(result.is_ok(), "first attempt call {call} failed: {result:?}");
            }

            // Calls before the crash returned, so the slot may confirm up to
            // the crashed call's start; Postgres resumes at a
            // transaction start.
            let confirmed = first_calls[crash_call].start;
            let start = case.redelivery_start.index(confirmed + 1);
            let start = start - start % EVENTS_PER_TRANSACTION;
            for range in split(start..ops.len(), &case.second_cuts) {
                let result = deliver(&conn, &schema, ops, range.clone(), None, false);
                prop_assert!(result.is_ok(), "redelivery of {range:?} failed: {result:?}");
            }

            let mut model = BTreeMap::new();
            for op in ops {
                op.apply(&mut model);
            }
            let mut statement = conn
                .prepare(&format!(
                    "select id, v from {} order by id",
                    qualified_lake_table_name(&ducklake_table_name())
                ))
                .unwrap();
            let stored = statement
                .query_map([], |row| Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?)))
                .unwrap()
                .collect::<Result<BTreeMap<_, _>, _>>()
                .unwrap();
            let row_count: i64 = conn
                .query_row(
                    &format!(
                        "select count(*) from {}",
                        qualified_lake_table_name(&ducklake_table_name())
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            prop_assert_eq!(row_count as usize, stored.len(), "duplicate primary keys stored");
            prop_assert_eq!(stored, model);
            Ok(())
        }

        #[test]
        fn crash_and_redelivery_at_any_boundary_apply_each_event_once() {
            run_property("ducklake_crash_redelivery_exactly_once", &replay_case(), check_replay);
        }
    }
}
