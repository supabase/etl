//! Crash-and-replay property for BigQuery.
//!
//! Every row is a CDC `UPSERT` or `DELETE` keyed by the primary key and
//! ordered by `_CHANGE_SEQUENCE_NUMBER`, appended to the default stream
//! without a deduplication token. A replayed batch appends again, and the
//! current view stays correct because the newest sequence number of each key
//! wins. BigQuery rejects partial updates by design, so tables use replica
//! identity `FULL`.
//!
//! The property makes paid BigQuery calls, so it is ignored by default and
//! runs nightly.

use std::time::Duration;

use etl::{
    pipeline::PipelineId,
    schema::TableName,
    store::PostgresStore,
    test_utils::cdc_workload::{WorkloadRecord, WorkloadShape, WorkloadTables},
};
use etl_destinations::bigquery::{
    BigQueryDestination,
    test_utils::{
        BigQueryDatabase, parse_table_cell, setup_bigquery_database,
        skip_if_missing_bigquery_env_vars,
    },
};
use etl_telemetry::tracing::init_test_tracing;
use gcp_bigquery_client::model::table_row::TableRow;
use proptest::test_runner::TestCaseError;

use crate::support::{
    crash_replay::{CrashReplayTarget, run_crash_replay_property},
    crypto::install_crypto_provider,
};

/// Parses one `id, note, counter` row of the current view.
fn workload_record(row: TableRow) -> Result<WorkloadRecord, TestCaseError> {
    let Some([id, note, counter]) =
        row.columns.and_then(|columns| <[_; 3]>::try_from(columns).ok())
    else {
        return Err(TestCaseError::fail("unexpected BigQuery row shape"));
    };

    let id = parse_table_cell(id).ok_or_else(|| TestCaseError::fail("BigQuery row has no id"))?;
    let counter = parse_table_cell(counter)
        .ok_or_else(|| TestCaseError::fail("BigQuery row has no counter"))?;

    Ok(WorkloadRecord::new(id, parse_table_cell(note), counter))
}

/// One BigQuery dataset receiving one case.
struct BigQueryTarget {
    /// Dataset the case writes, deleted on drop.
    database: BigQueryDatabase,
    /// Pipeline the destination writes for.
    pipeline_id: PipelineId,
}

impl CrashReplayTarget for BigQueryTarget {
    type Config = ();
    type Destination = BigQueryDestination<PostgresStore>;

    const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);
    const SHAPE: WorkloadShape = WorkloadShape::all().without_partial_updates();

    async fn setup(_: (), _: &WorkloadTables, pipeline_id: &PipelineId) -> Self {
        Self { database: setup_bigquery_database().await, pipeline_id: pipeline_id.clone() }
    }

    async fn build_destination(&self, store: PostgresStore) -> Self::Destination {
        self.database.build_destination(self.pipeline_id.clone(), store).await
    }

    async fn current_rows(&self, table: &TableName) -> Result<Vec<WorkloadRecord>, TestCaseError> {
        let rows = self
            .database
            .query_current_rows(table, "id, note, counter")
            .await
            .map_err(|error| TestCaseError::fail(format!("BigQuery query failed: {error}")))?;

        rows.into_iter().map(workload_record).collect()
    }
}

/// BigQuery converges to the source after crashes, lost acknowledgements, and
/// rejected writes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "nightly: makes paid BigQuery calls"]
async fn crash_replay_converges() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    run_crash_replay_property::<BigQueryTarget>("BigQuery crash replay", ());
}
