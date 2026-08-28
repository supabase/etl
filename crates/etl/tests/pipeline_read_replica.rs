use etl::{
    event::EventType,
    pipeline::PipelineId,
    test_utils::{
        database::{
            local_pg_read_replica_connection_config, spawn_source_database, test_table_name,
            wait_for_replication_slot_flush_lsn,
        },
        event::EventCondition,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
};
use etl_config::shared::PgConnectionConfig;
use etl_postgres::{
    below_version,
    slots::EtlReplicationSlot,
    tokio::test_utils::{PgDatabase, try_connect_to_pg_database},
    version::POSTGRES_16,
};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio::{select, time::interval};
use tokio_postgres::Client;

use crate::support::read_replica::{
    READ_REPLICA_POLL_INTERVAL, wait_for_read_replica_replay, wait_for_read_replica_to_catch_up,
};

async fn assert_read_replica(replica_config: &PgConnectionConfig) {
    let (client, _) = try_connect_to_pg_database(replica_config).await.unwrap();
    let row = client.query_one("select pg_is_in_recovery()", &[]).await.unwrap();
    let in_recovery: bool = row.get(0);

    assert!(in_recovery, "configured read replica is not in recovery");
}

async fn assert_database_and_publication_visible_on_read_replica(
    replica_config: &PgConnectionConfig,
    expected_database_name: &str,
    publication_name: &str,
) {
    let (client, _) = try_connect_to_pg_database(replica_config).await.unwrap();
    let row = client
        .query_one(
            "select current_database(), exists(select 1 from pg_publication where pubname = $1)",
            &[&publication_name],
        )
        .await
        .unwrap();
    let database_name: String = row.get(0);
    let publication_exists: bool = row.get(1);

    assert_eq!(database_name, expected_database_name);
    assert!(publication_exists, "Publication {publication_name} should be visible on the replica");
}

async fn assert_replication_slot_absent(primary: &PgDatabase<Client>, slot_name: &str) {
    let row = primary
        .client
        .as_ref()
        .unwrap()
        .query_opt("select 1 from pg_replication_slots where slot_name = $1", &[&slot_name])
        .await
        .unwrap();

    assert!(row.is_none(), "Replication slot {slot_name} should not exist on the primary");
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
                // Logical slot creation on a standby may wait for a running-xacts
                // snapshot from the primary when the primary is otherwise idle,
                // so keep nudging the primary while the pipeline starts.
                primary.log_standby_snapshot().await.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_replica_replays_multiple_test_databases() {
    init_test_tracing();

    let primary_a = spawn_source_database().await;
    if below_version!(primary_a.server_version(), POSTGRES_16) {
        eprintln!("Skipping test: PostgreSQL 16+ required for logical decoding on standbys");
        return;
    }
    let schema_a = setup_test_database_schema(&primary_a, TableSelection::UsersOnly).await;

    let primary_b = spawn_source_database().await;
    let schema_b = setup_test_database_schema(&primary_b, TableSelection::UsersOnly).await;

    assert_ne!(primary_a.config.name, primary_b.config.name);

    let replica_a = local_pg_read_replica_connection_config(&primary_a.config);
    let replica_b = local_pg_read_replica_connection_config(&primary_b.config);

    // Physical replication is cluster-wide, so wait for the standby to replay
    // each database's setup before connecting to those databases on the replica.
    wait_for_read_replica_to_catch_up(&primary_a, &replica_a).await;
    wait_for_read_replica_to_catch_up(&primary_b, &replica_b).await;

    let publication_name_a = schema_a.publication_name();
    let publication_name_b = schema_b.publication_name();
    assert_database_and_publication_visible_on_read_replica(
        &replica_a,
        &primary_a.config.name,
        &publication_name_a,
    )
    .await;
    assert_database_and_publication_visible_on_read_replica(
        &replica_b,
        &primary_b.config.name,
        &publication_name_b,
    )
    .await;
}

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
    // The publication and schema setup happen on the primary, so the pipeline
    // only starts after the standby has replayed that setup WAL.
    wait_for_read_replica_to_catch_up(&primary, &replica_config).await;
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

    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    wait_with_standby_snapshots(&primary, users_sync_complete_notify.notified()).await;

    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    // In read-replica mode ETL logical slots must be created on the replica,
    // while the primary should only own the physical standby slot.
    assert_replication_slot_absent(&primary, &apply_slot_name).await;

    let users_insert_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            3,
        )])
        .await;

    insert_users_data(&mut primary, &database_schema.users_schema().name, 3..=3).await;
    wait_for_read_replica_to_catch_up(&primary, &replica_config).await;

    users_insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&database_schema.users_schema().id).map_or(0, Vec::len), 2);
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
    // The publication and schema setup happen on the primary, so the pipeline
    // only starts after the standby has replayed that setup WAL.
    wait_for_read_replica_to_catch_up(&primary, &replica_config).await;
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

    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    wait_with_standby_snapshots(&primary, users_sync_complete_notify.notified()).await;

    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    // In read-replica mode ETL logical slots must be created on the replica,
    // while the primary should only own the physical standby slot.
    assert_replication_slot_absent(&primary, &apply_slot_name).await;

    primary.insert_values(unpublished_table, &["value"], &[&1_i32]).await.unwrap();
    // Capture the cluster WAL frontier after the unpublished-table write
    // without emitting an extra standby snapshot record.
    let unrelated_change_lsn = primary.current_wal_flush_lsn().await.unwrap();
    // The slot progress assertion below is meaningful only after the standby
    // has replayed the WAL generated by the unpublished table change.
    wait_for_read_replica_replay(&replica_config, unrelated_change_lsn).await;

    // Even though the change is not in the publication and emits no data event,
    // idle keepalive feedback should still advance the replica-side slot.
    let (replica_client, _) = try_connect_to_pg_database(&replica_config).await.unwrap();
    wait_for_replication_slot_flush_lsn(&replica_client, &apply_slot_name, unrelated_change_lsn)
        .await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    assert!(events.iter().all(|event| EventType::from(event) != EventType::Insert));
}
