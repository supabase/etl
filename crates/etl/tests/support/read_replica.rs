//! Shared synchronization helpers for integration tests that use the physical
//! read replica.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use etl_config::shared::PgConnectionConfig;
use etl_postgres::tokio::test_utils::{PgDatabase, try_connect_to_pg_database};
use tokio::time::sleep;
use tokio_postgres::{Client, types::PgLsn};

/// Maximum time to wait for a read-replica condition.
const READ_REPLICA_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Delay between read-replica condition checks.
pub(crate) const READ_REPLICA_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Waits until the read replica replays `target_lsn`.
pub(crate) async fn wait_for_read_replica_replay(
    replica_config: &PgConnectionConfig,
    target_lsn: PgLsn,
) {
    let mut monitor_config = replica_config.clone();
    monitor_config.name = "postgres".to_owned();

    wait_until("read replica replay", || async {
        let Ok((client, _)) = try_connect_to_pg_database(&monitor_config).await else {
            return Ok(false);
        };

        let row =
            client.query_one("select pg_is_in_recovery(), pg_last_wal_replay_lsn()", &[]).await?;
        let in_recovery: bool = row.get(0);
        let replay_lsn: Option<PgLsn> = row.get(1);

        Ok(in_recovery && replay_lsn.is_some_and(|replay_lsn| replay_lsn >= target_lsn))
    })
    .await;
}

/// Emits a standby snapshot and waits until the read replica replays it.
pub(crate) async fn wait_for_read_replica_to_catch_up(
    primary: &PgDatabase<Client>,
    replica_config: &PgConnectionConfig,
) -> PgLsn {
    // Emit a standby snapshot WAL record so the returned LSN is a concrete
    // replay barrier and standby logical slot creation has fresh transaction
    // snapshot metadata available.
    let target_lsn = primary.log_standby_snapshot().await.unwrap();
    wait_for_read_replica_replay(replica_config, target_lsn).await;

    target_lsn
}

/// Waits until an asynchronous PostgreSQL condition becomes true.
pub(crate) async fn wait_until<F, Fut>(description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, tokio_postgres::Error>>,
{
    let deadline = Instant::now() + READ_REPLICA_WAIT_TIMEOUT;

    loop {
        if condition().await.unwrap() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description} after {READ_REPLICA_WAIT_TIMEOUT:?}",
        );

        sleep(READ_REPLICA_POLL_INTERVAL).await;
    }
}
