//! Shared source database pool for out-of-band ETL queries.

use std::{sync::LazyLock, time::Duration};

use sqlx::postgres::PgPoolOptions;

use crate::{
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::OUT_OF_BAND_POOL,
    pipeline::PipelineId,
    postgres::{client::SlotState, pool::InstrumentedPgPool},
};

/// Maximum number of connections in the out-of-band pool.
const MAX_POOL_CONNECTIONS: u32 = 1;
/// Minimum duration after which idle out-of-band connections are closed.
const MIN_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Extra idle time kept beyond the configured lag refresh interval.
const IDLE_TIMEOUT_REFRESH_PADDING: Duration = Duration::from_secs(30);
/// Application name for ETL out-of-band source database connections.
const APP_NAME_REPLICATOR_OUT_OF_BAND: &str = "supabase_etl_replicator_out_of_band";

/// Connection options for out-of-band source database queries.
///
/// Uses the common bounded-query Postgres defaults because lag sampling queries
/// should be quick and should not block source database work.
static OUT_OF_BAND_OPTIONS: LazyLock<PgConnectionOptions> =
    LazyLock::new(|| PgConnectionOptions::builder(APP_NAME_REPLICATOR_OUT_OF_BAND).build());

/// Shared lazy pool for out-of-band source database queries.
#[derive(Debug, Clone)]
pub(crate) struct OutOfBandSourcePool {
    pool: InstrumentedPgPool,
}

impl OutOfBandSourcePool {
    /// Creates a new lazy out-of-band source pool.
    pub(crate) fn new(
        connection_config: &PgConnectionConfig,
        lag_refresh_interval: Duration,
    ) -> Self {
        let connect_options = connection_config.with_db(Some(&OUT_OF_BAND_OPTIONS));
        let idle_timeout =
            lag_refresh_interval.saturating_add(IDLE_TIMEOUT_REFRESH_PADDING).max(MIN_IDLE_TIMEOUT);
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(MAX_POOL_CONNECTIONS)
            .idle_timeout(Some(idle_timeout))
            .connect_lazy_with(connect_options);

        Self { pool: InstrumentedPgPool::new(pool, OUT_OF_BAND_POOL) }
    }

    /// Samples the source WAL heads and all slots of one pipeline.
    pub(crate) async fn sample_pipeline_lag(
        &self,
        pipeline_id: &PipelineId,
    ) -> EtlResult<etl_postgres::lag::PipelineLagMetrics> {
        etl_postgres::lag::get_pipeline_lag_metrics(self.pool.executor(), pipeline_id)
            .await
            .map_err(|error| {
                etl_error!(ErrorKind::SourceConnectionFailed, "Source lag query failed", source: error)
            })
    }

    /// Checks whether a copy slot exists and remains usable.
    pub(crate) async fn get_slot_state(&self, slot_name: &str) -> EtlResult<SlotState> {
        let status: Option<Option<String>> =
            sqlx::query_scalar("select wal_status from pg_replication_slots where slot_name = $1")
                .bind(slot_name)
                .fetch_optional(self.pool.executor())
                .await
                .map_err(|error| {
                    etl_error!(
                        ErrorKind::SourceConnectionFailed,
                        "Replication slot state query failed",
                        source: error
                    )
                })?;

        match status {
            None => Err(etl_error!(
                ErrorKind::ReplicationSlotNotFound,
                "Replication slot not found",
                slot_name.to_owned()
            )),
            Some(Some(status)) if status == "lost" => Ok(SlotState::Invalidated),
            Some(_) => Ok(SlotState::NotInvalidated),
        }
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::time::Duration;

    use etl_postgres::source::connect_to_source_database;
    use uuid::Uuid;

    use crate::{
        error::ErrorKind,
        postgres::{client::SlotState, source_pool::OutOfBandSourcePool},
        test_utils::database::{local_pg_read_replica_connection_config, spawn_source_database},
    };

    /// Monitor cadence used by source-pool tests.
    const TEST_MONITOR_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

    /// Slot validity distinguishes missing slots, including bound hostile
    /// names.
    #[tokio::test(flavor = "multi_thread")]
    async fn exclusive_slot_state_detects_missing_slot() {
        let database = spawn_source_database().await;
        let client = database.client.as_ref().unwrap();
        let pool = OutOfBandSourcePool::new(&database.config, TEST_MONITOR_REFRESH_INTERVAL);
        for reserve_wal in [false, true] {
            let name = format!("test_sample_{}", Uuid::new_v4().simple());
            client
                .query_one(
                    "select * from pg_create_physical_replication_slot($1, $2, true)",
                    &[&name, &reserve_wal],
                )
                .await
                .unwrap();
            let state = pool.get_slot_state(&name).await.unwrap();
            assert!(matches!(state, SlotState::NotInvalidated));
            assert!(
                pool.sample_pipeline_lag(&1.into())
                    .await
                    .unwrap()
                    .source_wal
                    .decoding_lsn()
                    .is_some()
            );
            client.query_one("select pg_drop_replication_slot($1)", &[&name]).await.unwrap();
        }
        for name in ["test_missing_slot", r"slot'; select 1; --\"] {
            let error = pool.get_slot_state(name).await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ReplicationSlotNotFound);
        }
    }

    /// A standby reports slot validity and uses replayed WAL for lag.
    #[tokio::test(flavor = "multi_thread")]
    async fn exclusive_lag_sample_works_on_read_replica() {
        let database = spawn_source_database().await;
        let mut config = local_pg_read_replica_connection_config(&database.config);
        config.name = "postgres".to_owned();
        let raw = connect_to_source_database(&config, 0, 1, None).await.unwrap();
        let mut conn = raw.acquire().await.unwrap();
        let in_recovery: bool =
            sqlx::query_scalar("select pg_is_in_recovery()").fetch_one(&mut *conn).await.unwrap();
        assert!(in_recovery);
        let name = format!("test_sample_{}", Uuid::new_v4().simple());
        sqlx::query("select * from pg_create_physical_replication_slot($1, false, true)")
            .bind(&name)
            .execute(&mut *conn)
            .await
            .unwrap();
        let pool = OutOfBandSourcePool::new(&config, TEST_MONITOR_REFRESH_INTERVAL);
        let state = pool.get_slot_state(&name).await.unwrap();
        assert!(matches!(state, SlotState::NotInvalidated));
        let sample = pool.sample_pipeline_lag(&1.into()).await.unwrap();
        assert!(sample.source_wal.decoding_lsn().is_some());
        assert!(sample.source_wal.write_lsn().is_none());
        sqlx::query("select pg_drop_replication_slot($1)")
            .bind(&name)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
}
