//! Crash-and-replay properties for both ClickHouse engines.
//!
//! CDC inserts carry no deduplication token, so a replayed batch appends
//! identical rows unless ClickHouse drops it as a repeated block. The current
//! state stays correct either way because each key resolves to its newest
//! version: the latest `(cdc_lsn, cdc_tx_ordinal)` for MergeTree and the
//! highest `_etl_version` under `FINAL` for ReplacingMergeTree. Copied rows
//! sit at position zero and must be stored once, because a failed copy drops
//! the table before copying it again. ReplacingMergeTree merges may collapse
//! identical rows in the background, so only MergeTree detects every stored
//! copy. Partial updates are rejected by design, so tables use replica
//! identity `FULL`.
//!
//! Replicated and Shared MergeTree deduplicate repeated insert blocks by
//! default, while local tables do not. Each case turns deduplication on once
//! the initial sync finished, so every streamed write and replay runs under
//! the same block dedup as ClickHouse Cloud. Truncate keeps the table, so the
//! setting lasts for the whole case.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use etl::{
    pipeline::PipelineId,
    schema::TableName,
    store::PostgresStore,
    test_utils::cdc_workload::{WorkloadRecord, WorkloadShape, WorkloadTables},
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::{
    ClickHouseDestination,
    test_utils::{ClickHouseTestDatabase, redact_clickhouse_error, setup_clickhouse_database},
};
use etl_telemetry::tracing::init_test_tracing;
use proptest::test_runner::TestCaseError;

use crate::support::{
    clickhouse::current_state_query,
    crash_replay::{CrashReplayTarget, StoredVersion, VersionOrigin, run_crash_replay_property},
    crypto::install_crypto_provider,
};

/// Recent insert blocks each table remembers for deduplication. It covers
/// every block one case writes, so any repeated block is dropped.
const DEDUPLICATION_WINDOW_BLOCKS: u32 = 1000;

/// Insert blocks ClickHouse dropped as duplicates across this process's cases.
///
/// Nextest runs each test in its own process, so this counts one property.
static DUPLICATE_BLOCKS_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Cases whose dropped-block count could not be read, for example because
/// the user may not flush or read the query log.
static UNMEASURED_CASES: AtomicU64 = AtomicU64::new(0);

/// One current-state row.
#[derive(clickhouse::Row, serde::Deserialize)]
struct CurrentRow {
    /// Primary key.
    id: i64,
    /// Nullable text column.
    note: Option<String>,
    /// Integer column.
    counter: i32,
}

/// One stored MergeTree row with its CDC position.
#[derive(clickhouse::Row, serde::Deserialize)]
struct MergeTreeVersion {
    /// Primary key.
    id: i64,
    /// Nullable text column.
    note: Option<String>,
    /// Integer column.
    counter: i32,
    /// Change kind: `INSERT`, `UPDATE`, or `DELETE`.
    cdc_operation: String,
    /// Commit LSN of the change, zero for copied rows.
    cdc_lsn: u64,
    /// Position of the change within its transaction.
    cdc_tx_ordinal: u64,
}

/// One stored ReplacingMergeTree row with its version.
#[derive(clickhouse::Row, serde::Deserialize)]
struct ReplacingMergeTreeVersion {
    /// Primary key.
    id: i64,
    /// Nullable text column.
    note: Option<String>,
    /// Integer column.
    counter: i32,
    /// Packed source position, zero for copied rows. Merges keep the highest
    /// version of each key.
    #[serde(rename = "_etl_version")]
    version: u128,
    /// `1` when the version deletes the key, otherwise `0`.
    #[serde(rename = "_etl_deleted")]
    deleted: u8,
}

/// Returns where a version at source position `position` came from: copied
/// rows are written at position zero.
fn origin(position: u128) -> VersionOrigin {
    if position == 0 { VersionOrigin::Copy } else { VersionOrigin::Change }
}

/// Returns the ClickHouse table name of a source table whose name components
/// neither start nor end with `_`: `{schema}_{table}` with every `_` doubled.
fn clickhouse_table_name(table: &TableName) -> String {
    format!("{}_{}", table.schema.replace('_', "__"), table.name.replace('_', "__"))
}

/// One ClickHouse database receiving one case.
struct ClickHouseTarget {
    /// Database the case writes, dropped on drop.
    database: ClickHouseTestDatabase,
    /// Table engine under test.
    engine: ClickHouseEngine,
}

impl ClickHouseTarget {
    /// Runs a query and maps failures.
    async fn fetch<T>(&self, sql: &str) -> Result<Vec<T>, TestCaseError>
    where
        T: for<'a> clickhouse::Row<Value<'a> = T> + serde::de::DeserializeOwned + 'static,
    {
        self.database.db_client().query(sql).fetch_all::<T>().await.map_err(|error| {
            TestCaseError::fail(format!(
                "ClickHouse query failed: {}",
                redact_clickhouse_error(error)
            ))
        })
    }

    /// Runs a statement and maps failures.
    async fn execute(&self, sql: &str) -> Result<(), TestCaseError> {
        self.database.db_client().query(sql).execute().await.map_err(|error| {
            TestCaseError::fail(format!(
                "ClickHouse statement failed: {}",
                redact_clickhouse_error(error)
            ))
        })
    }
}

impl CrashReplayTarget for ClickHouseTarget {
    type Config = ClickHouseEngine;
    type Destination = ClickHouseDestination<PostgresStore>;

    const SETTLE_TIMEOUT: Duration = Duration::ZERO;
    const SHAPE: WorkloadShape = WorkloadShape::all().without_partial_updates();

    async fn setup(engine: ClickHouseEngine, _: &WorkloadTables, _: &PipelineId) -> Self {
        Self { database: setup_clickhouse_database().await, engine }
    }

    async fn build_destination(&self, store: PostgresStore) -> Self::Destination {
        self.database.build_destination_with_engine(store, self.engine).await
    }

    async fn after_tables_synced(&self, tables: &WorkloadTables) -> Result<(), TestCaseError> {
        for table in tables.names() {
            self.execute(&format!(
                "alter table \"{}\" modify setting non_replicated_deduplication_window = {}",
                clickhouse_table_name(table),
                DEDUPLICATION_WINDOW_BLOCKS
            ))
            .await?;
        }

        Ok(())
    }

    async fn current_rows(&self, table: &TableName) -> Result<Vec<WorkloadRecord>, TestCaseError> {
        let sql = current_state_query(
            self.engine,
            &clickhouse_table_name(table),
            "id, note, counter",
            &["id"],
            "id",
        );
        let rows = self.fetch::<CurrentRow>(&sql).await?;

        Ok(rows.into_iter().map(|row| WorkloadRecord::new(row.id, row.note, row.counter)).collect())
    }

    async fn stored_versions(
        &self,
        table: &TableName,
    ) -> Result<Option<Vec<StoredVersion>>, TestCaseError> {
        let table = clickhouse_table_name(table);

        let versions = match self.engine {
            ClickHouseEngine::MergeTree => {
                let sql = format!(
                    "select id, note, counter, cdc_operation, cdc_lsn, cdc_tx_ordinal from \
                     \"{table}\""
                );
                self.fetch::<MergeTreeVersion>(&sql)
                    .await?
                    .into_iter()
                    .map(|row| {
                        let record = (row.cdc_operation != "DELETE")
                            .then(|| WorkloadRecord::new(row.id, row.note, row.counter));
                        let position = format!("{:016x}/{:016x}", row.cdc_lsn, row.cdc_tx_ordinal);
                        let packed =
                            (u128::from(row.cdc_lsn) << 64) | u128::from(row.cdc_tx_ordinal);
                        StoredVersion::new(origin(packed), row.id, position, record)
                    })
                    .collect()
            }
            ClickHouseEngine::ReplacingMergeTree => {
                let sql = format!(
                    "select id, note, counter, _etl_version, _etl_deleted from \"{table}\""
                );
                self.fetch::<ReplacingMergeTreeVersion>(&sql)
                    .await?
                    .into_iter()
                    .map(|row| {
                        let record = (row.deleted == 0)
                            .then(|| WorkloadRecord::new(row.id, row.note, row.counter));
                        let position = format!("{:032x}", row.version);
                        StoredVersion::new(origin(row.version), row.id, position, record)
                    })
                    .collect()
            }
        };

        Ok(Some(versions))
    }

    async fn teardown(self) {
        // Each case owns its database, so the query log attributes dropped
        // blocks to this case even while other tests share the server.
        let dropped = async {
            self.execute("system flush logs").await.ok()?;
            self.fetch::<u64>(
                "select sum(ProfileEvents['DuplicatedInsertedBlocks']) from system.query_log \
                 where type = 'QueryFinish' and has(databases, currentDatabase())",
            )
            .await
            .ok()?
            .first()
            .copied()
        }
        .await;
        match dropped {
            Some(dropped) => DUPLICATE_BLOCKS_DROPPED.fetch_add(dropped, Ordering::Relaxed),
            None => UNMEASURED_CASES.fetch_add(1, Ordering::Relaxed),
        };

        self.database.drop_database().await;
    }
}

/// Runs the property for `engine` and prints how many replayed blocks
/// ClickHouse dropped, so a run shows whether deduplication fired.
fn run_clickhouse_crash_replay(property: &str, engine: ClickHouseEngine) {
    init_test_tracing();
    install_crypto_provider();

    run_crash_replay_property::<ClickHouseTarget>(property, engine);

    let dropped = DUPLICATE_BLOCKS_DROPPED.load(Ordering::Relaxed);
    let unmeasured = UNMEASURED_CASES.load(Ordering::Relaxed);
    println!(
        "property '{property}' duplicate blocks dropped by ClickHouse: {dropped} (cases not \
         measured: {unmeasured})"
    );
}

/// ReplacingMergeTree converges to the source after crashes, lost
/// acknowledgements, and rejected writes.
#[tokio::test(flavor = "multi_thread")]
async fn crash_replay_converges_replacing_merge_tree() {
    run_clickhouse_crash_replay(
        "ClickHouse ReplacingMergeTree crash replay",
        ClickHouseEngine::ReplacingMergeTree,
    );
}

/// MergeTree converges to the source after crashes, lost acknowledgements,
/// and rejected writes.
#[tokio::test(flavor = "multi_thread")]
async fn crash_replay_converges_merge_tree() {
    run_clickhouse_crash_replay("ClickHouse MergeTree crash replay", ClickHouseEngine::MergeTree);
}
