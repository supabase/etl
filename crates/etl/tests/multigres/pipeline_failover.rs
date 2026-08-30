//! Pipeline failover tests for Multigres.

use etl::{
    error::ErrorKind,
    event::EventType,
    pipeline::PipelineId,
    test_utils::{
        database::TEST_DATABASE_SCHEMA,
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::PipelineBuilder,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, build_expected_users_inserts, insert_users_data,
            setup_test_database_schema,
        },
    },
};
use etl_postgres::slots::EtlReplicationSlot;
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::quote_identifier;
use rand::random;

use crate::support::Multigres;

/// Delay before the apply worker reconnects after a primary switch.
const APPLY_RETRY_DELAY_MS: u64 = 2_000;
/// Number of apply-worker retries allowed during the failover window.
const APPLY_RETRY_MAX_ATTEMPTS: u32 = 60;

/// Verifies that an ETL pipeline resumes across a Multigres primary switch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the isolated Multigres failover cluster"]
async fn pipeline_multigres_failover() {
    init_test_tracing();

    let mut multigres = Multigres::new().await;
    multigres
        .run_sql(&format!("create schema {}", quote_identifier(TEST_DATABASE_SCHEMA)))
        .await
        .unwrap();
    let database_schema = setup_test_database_schema(&multigres, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;
    insert_users_data(&mut multigres, &users_schema.name, 1..=1).await;

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = PipelineBuilder::new(
        multigres.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_replication_slot_failover(true)
    // Multigres tears down the pinned replication stream during a leader
    // change. Retrying in place must resume the same failover slot and pipeline
    // without another initial copy.
    .with_retry_config(APPLY_RETRY_DELAY_MS, APPLY_RETRY_MAX_ATTEMPTS)
    .build();

    let table_sync_complete = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete.notified().await;

    let primary_insert = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    insert_users_data(&mut multigres, &users_schema.name, 2..=2).await;
    let primary_flush_lsn = multigres.current_wal_flush_lsn().await.unwrap();
    primary_insert.notified().await;

    multigres.wait_for_synchronized_failover_slot(&apply_slot_name, primary_flush_lsn).await;

    let held_streaming = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut multigres, &users_schema.name, 3..=3).await;

    held_streaming.wait_reached().await;

    let failover_insert = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    multigres.failover().await;

    // The original attempt was not acknowledged. The promoted standby must
    // retransmit it from the synchronized failover slot.
    held_streaming.release_err(
        ErrorKind::DestinationTimeout,
        "Release the unacknowledged primary write after failover",
    );

    failover_insert.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert_eq!(destination.write_table_rows_called().await, 1);
    assert_eq!(destination.get_table_rows().await.get(&table_id).map_or(0, Vec::len), 1);

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_users_inserts =
        build_expected_users_inserts(2, &users_schema, vec![("user_2", 2), ("user_3", 3)]);
    assert_events_equal(users_inserts, &expected_users_inserts);

    multigres.drop_replication_slot(&apply_slot_name).await;
    multigres
        .run_sql(&format!(
            "drop publication {}",
            quote_identifier(&database_schema.publication_name())
        ))
        .await
        .unwrap();
    multigres
        .run_sql(&format!("drop schema {} cascade", quote_identifier(TEST_DATABASE_SCHEMA)))
        .await
        .unwrap();
}
