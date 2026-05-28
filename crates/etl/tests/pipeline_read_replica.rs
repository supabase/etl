use std::{
    future::Future,
    time::{Duration, Instant},
};

use etl::{
    state::TableStateType,
    test_utils::{
        database::{
            local_pg_read_replica_connection_config, spawn_source_database, test_table_name,
        },
        event::EventCondition,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
    types::{EventType, PipelineId},
};
use etl_config::shared::PgConnectionConfig;
use etl_postgres::{
    below_version,
    replication::slots::EtlReplicationSlot,
    tokio::test_utils::{PgDatabase, try_connect_to_pg_database},
    version::POSTGRES_16,
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio::{
    select,
    task::yield_now,
    time::{interval, sleep},
};
use tokio_postgres::{Client, types::PgLsn};

const READ_REPLICA_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const READ_REPLICA_POLL_INTERVAL: Duration = Duration::from_millis(200);

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_replicates_table_copy_and_cdc_from_read_replica() {
    init_test_tracing();

    let mut primary = spawn_source_database().await;
    if below_version!(primary.server_version(), POSTGRES_16) {
        eprintln!("Skipping test: PostgreSQL 16+ required for logical decoding on standbys");
        return;
    }

    let database_schema = setup_test_database_schema(&primary, TableSelection::UsersOnly).await;
    insert_users_data(&mut primary, &database_schema.users_schema().name, 1..=2).await;

    let replica_config = local_pg_read_replica_connection_config(&primary.config);
    let setup_lsn = log_standby_snapshot(&primary).await;
    wait_for_read_replica_replay(&replica_config, setup_lsn).await;
    assert_read_replica(&replica_config).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &replica_config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();
    wait_with_standby_snapshots(&primary, users_ready.notified()).await;

    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&database_schema.users_schema().id).map_or(0, Vec::len), 2);

    let users_inserted = destination
        .wait_for_all_events(vec![EventCondition::Table(
            EventType::Insert,
            database_schema.users_schema().id,
            3,
        )])
        .await;

    insert_users_data(&mut primary, &database_schema.users_schema().name, 3..=3).await;
    let change_lsn = current_wal_flush_lsn(&primary).await;
    wait_for_read_replica_replay(&replica_config, change_lsn).await;
    users_inserted.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_advances_read_replica_slot_on_idle_keepalive() {
    init_test_tracing();

    let primary = spawn_source_database().await;
    if below_version!(primary.server_version(), POSTGRES_16) {
        eprintln!("Skipping test: PostgreSQL 16+ required for logical decoding on standbys");
        return;
    }

    let database_schema = setup_test_database_schema(&primary, TableSelection::UsersOnly).await;
    let unpublished_table = test_table_name("unpublished_wal");
    primary
        .create_table(unpublished_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    let replica_config = local_pg_read_replica_connection_config(&primary.config);
    let setup_lsn = log_standby_snapshot(&primary).await;
    wait_for_read_replica_replay(&replica_config, setup_lsn).await;
    assert_read_replica(&replica_config).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &replica_config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_ready = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();
    wait_with_standby_snapshots(&primary, users_ready.notified()).await;

    primary.insert_values(unpublished_table, &["value"], &[&1_i32]).await.unwrap();
    let unrelated_change_lsn = current_wal_flush_lsn(&primary).await;
    wait_for_read_replica_replay(&replica_config, unrelated_change_lsn).await;

    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    wait_for_slot_confirmed_flush_lsn(&replica_config, &apply_slot_name, unrelated_change_lsn)
        .await;

    let events = destination.get_events().await;
    assert!(events.iter().all(|event| EventType::from(event) != EventType::Insert));

    pipeline.shutdown_and_wait().await.unwrap();
}

async fn log_standby_snapshot(primary: &PgDatabase<Client>) -> PgLsn {
    let row = primary
        .client
        .as_ref()
        .expect("primary client should be initialized")
        .query_one("select pg_log_standby_snapshot()", &[])
        .await
        .unwrap();

    row.get(0)
}

async fn current_wal_flush_lsn(primary: &PgDatabase<Client>) -> PgLsn {
    let row = primary
        .client
        .as_ref()
        .expect("primary client should be initialized")
        .query_one("select pg_current_wal_flush_lsn()", &[])
        .await
        .unwrap();

    row.get(0)
}

async fn assert_read_replica(replica_config: &PgConnectionConfig) {
    let (client, _) = try_connect_to_pg_database(replica_config)
        .await
        .expect("read replica should accept connections");
    let row = client.query_one("select pg_is_in_recovery()", &[]).await.unwrap();
    let in_recovery: bool = row.get(0);

    assert!(in_recovery, "configured read replica is not in recovery");
}

async fn wait_for_read_replica_replay(replica_config: &PgConnectionConfig, target_lsn: PgLsn) {
    wait_until("read replica replay", || async {
        let Ok((client, _)) = try_connect_to_pg_database(replica_config).await else {
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

async fn wait_with_standby_snapshots<F>(primary: &PgDatabase<Client>, future: F)
where
    F: Future<Output = ()>,
{
    tokio::pin!(future);

    let mut snapshot_interval = interval(READ_REPLICA_POLL_INTERVAL);
    loop {
        select! {
            () = &mut future => return,
            _ = snapshot_interval.tick() => {
                log_standby_snapshot(primary).await;
            }
        }
    }
}

async fn wait_for_slot_confirmed_flush_lsn(
    replica_config: &PgConnectionConfig,
    slot_name: &str,
    target_lsn: PgLsn,
) {
    wait_until("read replica slot confirmed_flush_lsn", || async {
        let Ok((client, _)) = try_connect_to_pg_database(replica_config).await else {
            return Ok(false);
        };
        let row = client
            .query_opt(
                "select confirmed_flush_lsn from pg_replication_slots where slot_name = $1",
                &[&slot_name],
            )
            .await?;

        let Some(row) = row else {
            return Ok(false);
        };
        let confirmed_flush_lsn: Option<PgLsn> = row.get(0);

        Ok(confirmed_flush_lsn.is_some_and(|confirmed_flush_lsn| confirmed_flush_lsn >= target_lsn))
    })
    .await;
}

async fn wait_until<F, Fut>(description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, tokio_postgres::Error>>,
{
    let deadline = Instant::now() + READ_REPLICA_WAIT_TIMEOUT;

    loop {
        if condition().await.unwrap_or(false) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description} after {READ_REPLICA_WAIT_TIMEOUT:?}",
        );

        sleep(READ_REPLICA_POLL_INTERVAL).await;
        yield_now().await;
    }
}
