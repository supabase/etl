//! Crash-and-replay property for DuckLake.
//!
//! DuckLake commits each streaming batch together with a per-table watermark,
//! so a replayed or retried batch at or below the watermark is skipped. Its
//! tables hold current rows, so any replayed copy shows up as a duplicate key.
//! Every change shape is supported, including partial updates.

use std::time::Duration;

use duckdb::Connection;
use etl::{
    pipeline::PipelineId,
    schema::TableName,
    store::PostgresStore,
    test_utils::cdc_workload::{WorkloadRecord, WorkloadShape, WorkloadTables},
};
use etl_config::shared::DestinationConfig;
use etl_destinations::ducklake::{DuckLakeDestination, DuckLakeTableName};
use etl_maintenance::ducklake::RedactedDuckDbError;
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::quote_literal;
use proptest::test_runner::TestCaseError;

use crate::support::{
    crash_replay::{CrashReplayTarget, run_crash_replay_property},
    ducklake::{
        DuckLakeTestEnv, checkpoint_lake, create_test_lake, open_lake_conn,
        qualified_lake_table_name,
    },
};

/// Maps a DuckDB failure to a test failure without provider text.
fn duckdb_failure(error: duckdb::Error) -> TestCaseError {
    TestCaseError::fail(format!("DuckLake query failed: {}", RedactedDuckDbError::from(error)))
}

/// Returns whether the lake contains `table`.
fn lake_table_exists(conn: &Connection, table: &DuckLakeTableName) -> Result<bool, TestCaseError> {
    let sql = format!(
        "select count(*) from duckdb_tables() where database_name = 'lake' and schema_name = {} \
         and table_name = {}",
        quote_literal(table.schema()),
        quote_literal(table.table())
    );
    let count: i64 = conn.query_row(&sql, [], |row| row.get(0)).map_err(duckdb_failure)?;

    Ok(count > 0)
}

/// One DuckLake lake receiving one case.
struct DuckLakeTarget {
    /// Catalog database and data directory of the lake.
    lake: DuckLakeTestEnv,
}

impl CrashReplayTarget for DuckLakeTarget {
    type Config = ();
    type Destination = DuckLakeDestination<PostgresStore>;

    const SETTLE_TIMEOUT: Duration = Duration::ZERO;
    const SHAPE: WorkloadShape = WorkloadShape::all();

    async fn setup(_: (), _: &WorkloadTables, _: &PipelineId) -> Self {
        Self { lake: create_test_lake("crash_replay").await }
    }

    async fn build_destination(&self, store: PostgresStore) -> Self::Destination {
        // The production default keeps cases realistic and follows it when it
        // changes.
        DuckLakeDestination::new(
            self.lake.catalog_url.clone(),
            self.lake.data_url.clone(),
            DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE,
            None,
            None,
            None,
            None,
            store,
        )
        .await
        .unwrap()
    }

    async fn current_rows(&self, table: &TableName) -> Result<Vec<WorkloadRecord>, TestCaseError> {
        // A fresh connection can see stale catalog metadata until checkpoint.
        checkpoint_lake(&self.lake.catalog_url, &self.lake.data_url);
        let conn = open_lake_conn(&self.lake.catalog_url, &self.lake.data_url);

        // The destination creates a table on its first write, so a table the
        // pipeline never wrote to has no rows.
        let table = DuckLakeTableName::from_source(table);
        if !lake_table_exists(&conn, &table)? {
            return Ok(Vec::new());
        }

        let sql = format!("select id, note, counter from {}", qualified_lake_table_name(&table));
        let mut statement = conn.prepare(&sql).map_err(duckdb_failure)?;
        let rows = statement
            .query_map([], |row| Ok(WorkloadRecord::new(row.get(0)?, row.get(1)?, row.get(2)?)))
            .map_err(duckdb_failure)?;

        rows.collect::<Result<_, _>>().map_err(duckdb_failure)
    }
}

/// DuckLake converges to the source after crashes, lost acknowledgements,
/// and rejected writes.
#[tokio::test(flavor = "multi_thread")]
async fn crash_replay_converges() {
    init_test_tracing();

    run_crash_replay_property::<DuckLakeTarget>("DuckLake crash replay", ());
}
