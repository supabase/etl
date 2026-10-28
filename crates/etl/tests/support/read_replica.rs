//! Shared synchronization helpers for integration tests that use the physical
//! read replica.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use etl_config::shared::PgConnectionConfig;
use etl_postgres::tokio::test_utils::{PgDatabase, try_connect_to_pg_database};
use tokio::time::sleep;
use tokio_postgres::{Client, SimpleQueryMessage, types::PgLsn};

/// Maximum time to wait for a read-replica condition.
const READ_REPLICA_WAIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Delay between read-replica condition checks.
pub(crate) const READ_REPLICA_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Replica state reported when a replay wait times out.
///
/// Each query names one reason replay can stall: a paused or disconnected WAL
/// receiver, a startup process waiting on a recovery conflict, or a session
/// that holds a conflicting lock.
const REPLICA_DIAGNOSTIC_QUERIES: [(&str, &str); 5] = [
    (
        "recovery",
        "select pg_is_in_recovery() as in_recovery, pg_last_wal_receive_lsn() as receive_lsn, \
         pg_last_wal_replay_lsn() as replay_lsn, pg_last_xact_replay_timestamp() as \
         last_replay_at, case when pg_is_in_recovery() then pg_is_wal_replay_paused() end as \
         replay_paused",
    ),
    (
        "startup process",
        "select wait_event_type, wait_event from pg_stat_activity where backend_type = 'startup'",
    ),
    (
        "wal receiver",
        "select status, written_lsn, flushed_lsn, last_msg_receipt_time from pg_stat_wal_receiver",
    ),
    (
        "recovery conflicts",
        "select datname, confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, \
         confl_deadlock from pg_stat_database_conflicts where confl_tablespace + confl_lock + \
         confl_snapshot + confl_bufferpin + confl_deadlock > 0",
    ),
    (
        "client sessions",
        "select datname, application_name, state, wait_event_type, wait_event, backend_start from \
         pg_stat_activity where backend_type = 'client backend' and pid <> pg_backend_pid()",
    ),
];

/// Formats the rows of one diagnostic query as `column=value` lines.
fn format_diagnostic_rows(messages: &[SimpleQueryMessage]) -> String {
    let rows = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        format!("{}={}", column.name(), row.get(index).unwrap_or("null"))
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();

    if rows.is_empty() { "(no rows)".to_owned() } else { rows.join("\n    ") }
}

/// Reads the replica state that explains why replay has not advanced.
///
/// Failures are reported inline because this runs only to annotate a timeout.
async fn describe_read_replica(monitor_config: &PgConnectionConfig) -> String {
    let client = match try_connect_to_pg_database(monitor_config).await {
        Ok((client, _)) => client,
        Err(error) => return format!("replica diagnostics unavailable: {error}"),
    };

    let mut report = String::from("replica diagnostics:");
    for (label, sql) in REPLICA_DIAGNOSTIC_QUERIES {
        let section = match client.simple_query(sql).await {
            Ok(messages) => format_diagnostic_rows(&messages),
            Err(error) => format!("query failed: {error}"),
        };
        report.push_str(&format!("\n  {label}:\n    {section}"));
    }

    report
}

/// Waits until the read replica replays `target_lsn`.
///
/// On timeout the panic names the last check's outcome and the replica's
/// recovery state, so a stalled replay can be told apart from an unreachable
/// replica.
pub(crate) async fn wait_for_read_replica_replay(
    replica_config: &PgConnectionConfig,
    target_lsn: PgLsn,
) {
    let mut monitor_config = replica_config.clone();
    monitor_config.name = "postgres".to_owned();
    let deadline = Instant::now() + READ_REPLICA_WAIT_TIMEOUT;

    loop {
        let last_check = match try_connect_to_pg_database(&monitor_config).await {
            Ok((client, _)) => {
                let row = client
                    .query_one("select pg_is_in_recovery(), pg_last_wal_replay_lsn()", &[])
                    .await
                    .unwrap();
                let in_recovery: bool = row.get(0);
                let replay_lsn: Option<PgLsn> = row.get(1);

                if in_recovery && replay_lsn.is_some_and(|replay_lsn| replay_lsn >= target_lsn) {
                    return;
                }

                let replay_lsn =
                    replay_lsn.map_or_else(|| "null".to_owned(), |lsn| lsn.to_string());
                format!("in_recovery={in_recovery}, replay_lsn={replay_lsn}")
            }
            Err(error) => format!("connection failed: {error}"),
        };

        if Instant::now() >= deadline {
            let diagnostics = describe_read_replica(&monitor_config).await;
            panic!(
                "timed out waiting for read replica replay to {target_lsn} after \
                 {READ_REPLICA_WAIT_TIMEOUT:?}; last check: {last_check}\n{diagnostics}"
            );
        }

        sleep(READ_REPLICA_POLL_INTERVAL).await;
    }
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
