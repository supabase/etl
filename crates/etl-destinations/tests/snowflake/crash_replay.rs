//! Crash-and-replay property for Snowflake.
//!
//! Each table streams through one Snowpipe Streaming channel whose committed
//! offset token is the last applied source position. After a restart or a
//! retry, events at or below the committed token are skipped, and the
//! `_CURRENT` view resolves each key to its latest change. The view keeps no
//! delete for the old key of a key-changing update, so updates keep their
//! keys.
//!
//! The property needs Snowflake credentials, so it is ignored by default and
//! runs in the nightly Snowflake workflow.

use std::{sync::Arc, time::Duration};

use etl::{
    pipeline::PipelineId,
    schema::TableName,
    store::PostgresStore,
    test_utils::cdc_workload::{WorkloadRecord, WorkloadShape, WorkloadTables},
};
use etl_destinations::snowflake::{
    AuthManager, Client, Config, Destination, HttpExchanger, SqlClient,
    test_utils::{load_test_config, query_rows},
};
use etl_telemetry::tracing::init_test_tracing;
use proptest::test_runner::TestCaseError;
use serde_json::Value;

use crate::{
    snowflake::common::{
        build_auth, companion_name, drop_table_and_views, quote, snowflake_table_name,
    },
    support::crash_replay::{CrashReplayTarget, run_crash_replay_property},
};

/// Parses an integer the SQL API returned as a string.
fn parse_integer<T: std::str::FromStr>(value: &Value) -> Result<T, TestCaseError> {
    value
        .as_str()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| TestCaseError::fail("Snowflake returned a malformed integer"))
}

/// Parses one `id, note, counter` row of the `_CURRENT` view.
fn workload_record(row: &[Value]) -> Result<WorkloadRecord, TestCaseError> {
    let [id, note, counter] = row else {
        return Err(TestCaseError::fail("unexpected Snowflake row shape"));
    };
    let note = match note {
        Value::Null => None,
        Value::String(note) => Some(note.clone()),
        _ => return Err(TestCaseError::fail("Snowflake returned a malformed note")),
    };

    Ok(WorkloadRecord::new(parse_integer(id)?, note, parse_integer(counter)?))
}

/// Snowflake tables receiving one case in the shared test schema.
struct SnowflakeTarget {
    /// Connection settings without credentials.
    config: Config,
    /// Authentication shared by the SQL and streaming clients.
    auth: Arc<AuthManager<HttpExchanger>>,
    /// SQL client for verification and cleanup.
    sql: SqlClient<AuthManager<HttpExchanger>>,
    /// Pipeline whose channels the destination opens.
    pipeline_id: PipelineId,
    /// Physical tables to drop on teardown.
    tables: Vec<String>,
}

impl CrashReplayTarget for SnowflakeTarget {
    type Config = ();
    type Destination = Destination<PostgresStore>;

    const SETTLE_TIMEOUT: Duration = Duration::from_secs(180);
    const SHAPE: WorkloadShape = WorkloadShape::all().without_key_changes();

    async fn setup(_: (), tables: &WorkloadTables, pipeline_id: &PipelineId) -> Self {
        let config = load_test_config().clone_without_credentials();
        let auth = build_auth();
        let sql = SqlClient::new(
            config.clone_without_credentials(),
            Arc::clone(&auth),
            reqwest::Client::new(),
        );
        let tables = tables.names().iter().map(snowflake_table_name).collect();

        Self { config, auth, sql, pipeline_id: pipeline_id.clone(), tables }
    }

    async fn build_destination(&self, store: PostgresStore) -> Self::Destination {
        Destination::new(Client::new(Arc::clone(&self.auth), self.pipeline_id.clone()), store)
    }

    async fn current_rows(&self, table: &TableName) -> Result<Vec<WorkloadRecord>, TestCaseError> {
        let sql = format!(
            "select \"id\"::varchar, \"note\", \"counter\"::varchar from {}.{}.{}",
            quote(self.config.database()),
            quote(self.config.schema()),
            quote(&companion_name(&snowflake_table_name(table)))
        );
        let rows = query_rows(&self.sql, &sql)
            .await
            .map_err(|error| TestCaseError::fail(format!("Snowflake query failed: {error}")))?;

        rows.iter().map(|row| workload_record(row)).collect()
    }

    async fn teardown(self) {
        for table in &self.tables {
            drop_table_and_views(&self.sql, table).await;
        }
    }
}

/// Snowflake converges to the source after crashes, lost acknowledgements,
/// and rejected writes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Snowflake credentials"]
async fn crash_replay_converges() {
    init_test_tracing();

    run_crash_replay_property::<SnowflakeTarget>("Snowflake crash replay", ());
}
