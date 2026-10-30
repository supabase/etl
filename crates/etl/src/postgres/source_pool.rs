//! Shared source database pool for out-of-band ETL queries.

use std::{sync::LazyLock, time::Duration};

use sqlx::postgres::PgPoolOptions;

use crate::{
    config::{IntoConnectOptions, PgConnectionConfig, PgConnectionOptions},
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::OUT_OF_BAND_POOL,
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
        pipeline_id: u64,
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

    /// Confirms both the creating backend and its slot have disappeared.
    ///
    /// Slot absence alone is insufficient: a creation request whose response
    /// was lost may still be running before allocating the slot. PID reuse is
    /// conservative here; an unrelated live backend delays release, never
    /// causes a slot or backend to be deleted.
    pub(crate) async fn table_sync_slot_is_released(
        &self,
        slot_name: &str,
        backend_pid: i32,
    ) -> EtlResult<bool> {
        sqlx::query_scalar(
            "select not exists (select 1 from pg_stat_activity where pid = $1) \
             and not exists (select 1 from pg_replication_slots where slot_name = $2)",
        )
        .bind(backend_pid)
        .bind(slot_name)
        .fetch_one(self.pool.executor())
        .await
        .map_err(|error| {
            etl_error!(ErrorKind::SourceConnectionFailed, "Table sync slot release query failed", source: error)
        })
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::time::Duration;

    use etl_postgres::source::connect_to_source_database;
    use uuid::Uuid;

    use crate::{
        config::{ReplicationSlotConfig, ReplicationSlotPersistence},
        error::ErrorKind,
        postgres::{
            client::{PgReplicationClient, SlotState},
            source_pool::OutOfBandSourcePool,
        },
        test_utils::database::{local_pg_read_replica_connection_config, spawn_source_database},
    };

    /// Monitor cadence used by source-pool tests.
    const TEST_MONITOR_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

    /// A live creator blocks release before allocation, and another slot with
    /// the same name blocks release after that creator exits. Observation must
    /// never drop the other session's resource.
    #[tokio::test(flavor = "multi_thread")]
    async fn exclusive_slot_release_requires_backend_and_slot_absence() {
        let database = spawn_source_database().await;
        let source = database.client.as_ref().unwrap();
        let pool = OutOfBandSourcePool::new(&database.config, TEST_MONITOR_REFRESH_INTERVAL);
        let name = format!("test_release_{}", Uuid::new_v4().simple());
        let mut creator = PgReplicationClient::connect(database.config.clone()).await.unwrap();
        let backend_pid = creator.backend_pid();

        assert!(!pool.table_sync_slot_is_released(&name, backend_pid).await.unwrap());
        creator
            .create_slot(
                &name,
                &ReplicationSlotConfig {
                    persistence: ReplicationSlotPersistence::Temporary,
                    failover: false,
                },
            )
            .await
            .unwrap();
        assert!(!pool.table_sync_slot_is_released(&name, backend_pid).await.unwrap());
        drop(creator);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !pool.table_sync_slot_is_released(&name, backend_pid).await.unwrap() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        source
            .query_one(
                "select * from pg_create_physical_replication_slot($1, false, true)",
                &[&name],
            )
            .await
            .unwrap();
        assert!(!pool.table_sync_slot_is_released(&name, backend_pid).await.unwrap());
        source.query_one("select pg_drop_replication_slot($1)", &[&name]).await.unwrap();
        assert!(pool.table_sync_slot_is_released(&name, backend_pid).await.unwrap());
    }

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
            assert!(pool.sample_pipeline_lag(1).await.unwrap().source_wal.decoding_lsn().is_some());
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
        let sample = pool.sample_pipeline_lag(1).await.unwrap();
        assert!(sample.source_wal.decoding_lsn().is_some());
        assert!(sample.source_wal.write_lsn().is_none());
        sqlx::query("select pg_drop_replication_slot($1)")
            .bind(&name)
            .execute(&mut *conn)
            .await
            .unwrap();
    }
}
