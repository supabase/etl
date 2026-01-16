#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::error::ErrorKind;
use etl::state::table::{TableReplicationPhase, TableReplicationPhaseType};
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::event::group_events_by_type_and_table_id;
use etl::test_utils::notifying_store::NotifyingStore;
use etl::test_utils::pipeline::{
    create_pipeline, create_pipeline_with_batch_config, create_pipeline_with_table_sync_copy_config,
};
use etl::test_utils::table::assert_table_schema;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{
    TableSelection, assert_events_equal, build_expected_orders_inserts,
    build_expected_users_inserts, get_n_integers_sum, get_users_age_sum_from_rows,
    insert_mock_data, insert_orders_data, insert_users_data, setup_test_database_schema,
};
use etl::types::{Event, EventType, InsertEvent, PipelineId, Type};
use etl_config::shared::{BatchConfig, TableSyncCopyConfig};
use etl_postgres::below_version;
use etl_postgres::replication::slots::EtlReplicationSlot;
use etl_postgres::tokio::test_utils::{TableModification, id_column_schema};
use etl_postgres::types::{ColumnSchema, TableId};
use etl_postgres::version::POSTGRES_15;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_shutdown_calls_destination_shutdown() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Wait for the table to be ready.
    let table_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // Shutdown should not have been called yet.
    assert!(!destination.shutdown_called().await);

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify that shutdown was called on the destination.
    assert!(destination.shutdown_called().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_fails_when_slot_deleted_with_non_init_tables() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Wait for the table to be ready.
    let table_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify that the replication slot for the apply worker exists.
    let apply_slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
        .try_into()
        .unwrap();
    let slot_exists = database
        .replication_slot_exists(&apply_slot_name)
        .await
        .unwrap();
    assert!(slot_exists, "Apply slot should exist after pipeline start");

    // Wait for Postgres to mark the slot as inactive before dropping it.
    while database
        .replication_slot_is_active(&apply_slot_name)
        .await
        .unwrap()
    {
        sleep(Duration::from_millis(100)).await;
    }

    // Delete the apply worker slot to simulate slot loss.
    database
        .run_sql(&format!(
            "select pg_drop_replication_slot('{apply_slot_name}')"
        ))
        .await
        .unwrap();
    let slot_exists = !database
        .replication_slot_exists(&apply_slot_name)
        .await
        .unwrap();
    assert!(slot_exists, "Apply slot should not exist after deletion");

    // Restart the pipeline - it should fail because tables are not in Init state.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // The pipeline starts successfully (the actual work happens in a spawned task).
    pipeline.start().await.unwrap();

    // The error surfaces when we wait for the pipeline to complete.
    let wait_result = pipeline.wait().await;
    assert!(wait_result.is_err(), "Pipeline wait should fail");

    let err = wait_result.unwrap_err();
    assert!(
        err.kinds().contains(&ErrorKind::InvalidState),
        "Error should be InvalidState, got: {:?}",
        err.kinds()
    );

    // Verify that the slot was cleaned up (deleted) after the validation failure.
    let slot_exists = !database
        .replication_slot_exists(&apply_slot_name)
        .await
        .unwrap();
    assert!(
        slot_exists,
        "Apply slot should be deleted after validation failure"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_copy_survives_pipeline_restarts() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // We wait for both table states to be in sync done.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that the table schemas have been stored.
    let table_schemas = store.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(
        *table_schemas
            .get(&database_schema.users_schema().id)
            .unwrap(),
        database_schema.users_schema()
    );
    assert_eq!(
        *table_schemas
            .get(&database_schema.orders_schema().id)
            .unwrap(),
        database_schema.orders_schema()
    );

    // We recreate a pipeline, assuming the other one was stopped, using the same state and destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We wait for two inserts to be processed, one for `users` and one for `orders`.
    let insert_events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    // Insert a single row for each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        // 1 element.
        0..=0,
        true,
    )
    .await;

    insert_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that both inserts were received, and we know that we can receive them only when the table
    // schemas are available.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .unwrap();
    let orders_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap();

    assert_eq!(users_inserts.len(), 1);
    assert_eq!(orders_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_changes_are_correctly_handled() {
    init_test_tracing();

    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    // Create two tables in the test schema and a publication for that schema.
    let table_1 = test_table_name("table_1");
    let table_1_id = database
        .create_table(table_1.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let table_2 = test_table_name("table_2");
    let table_2_id = database
        .create_table(table_2.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    let publication_name = "test_pub_cleanup";
    database
        .create_publication_for_all(publication_name, Some(&table_1.schema))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_string(),
        store.clone(),
        destination.clone(),
    );

    // Wait for initial copy completion (Ready) for both tables.
    let table_1_ready_notify = store
        .notify_on_table_state_type(table_1_id, TableReplicationPhaseType::Ready)
        .await;
    let table_2_ready_notify = store
        .notify_on_table_state_type(table_2_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    table_1_ready_notify.notified().await;
    table_2_ready_notify.notified().await;

    // Insert one row in each table and wait for two insert events.
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;
    database
        .insert_values(table_1.clone(), &["value"], &[&1])
        .await
        .unwrap();
    database
        .insert_values(table_2.clone(), &["value"], &[&1])
        .await
        .unwrap();
    inserts_notify.notified().await;

    // Drop table_2 so it's no longer part of the publication.
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!("drop table {}", table_2.as_quoted_identifier()),
            &[],
        )
        .await
        .unwrap();

    // Shutdown pipeline after the table was dropped. We do this to show that the dropping of a table
    // doesn't cause issues with the pipeline since the change is picked up on pipeline restart.
    pipeline.shutdown_and_wait().await.unwrap();

    // Create table_3 which is going to be added to the publication.
    let table_3 = test_table_name("table_3");
    let table_3_id = database
        .create_table(table_3.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    // Restart pipeline; it should detect table_2 is gone and purge its state
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_string(),
        store.clone(),
        destination.clone(),
    );

    // Wait for the table_3 to be done.
    let table_3_ready_notify = store
        .notify_on_table_state_type(table_3_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    table_3_ready_notify.notified().await;

    // Insert one row in table_1 and wait for it. (We wait for 4 inserts since it keeps the previous
    // ones).
    let inserts_notify = destination
        .wait_for_events_count_deduped(vec![(EventType::Insert, 4)])
        .await;

    database
        .insert_values(table_1.clone(), &["value"], &[&2])
        .await
        .unwrap();
    database
        .insert_values(table_3.clone(), &["value"], &[&1])
        .await
        .unwrap();

    inserts_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Assert that table_2 state is gone but destination data remains.
    let states = store.get_table_replication_states().await;
    assert!(states.contains_key(&table_1_id));
    assert!(!states.contains_key(&table_2_id));
    assert!(states.contains_key(&table_3_id));

    // The destination should have the 2 events of the first table, the 1 event of the removed table
    // and the 1 event of the new table.
    // Use de-duplicated events for assertions to be robust to potential duplicates
    // on restart where confirmed_flush_lsn may not have been stored.
    let events = destination.get_events_deduped().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let table_1_inserts = grouped
        .get(&(EventType::Insert, table_1_id))
        .cloned()
        .unwrap();
    assert_eq!(table_1_inserts.len(), 2);
    let table_2_inserts = grouped
        .get(&(EventType::Insert, table_2_id))
        .cloned()
        .unwrap();
    assert_eq!(table_2_inserts.len(), 1);
    let table_3_inserts = grouped
        .get(&(EventType::Insert, table_3_id))
        .cloned()
        .unwrap();
    assert_eq!(table_3_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_for_all_tables_in_schema_ignores_new_tables_until_restart() {
    init_test_tracing();

    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    // Create first table.
    let table_1 = test_table_name("table_1");
    let table_1_id = database
        .create_table(table_1.clone(), true, &[("name", "text not null")])
        .await
        .unwrap();

    // Create a publication for all tables in the test schema.
    let publication_name = "test_pub_all_schema";
    database
        .create_publication_for_all(publication_name, Some(&table_1.schema))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_string(),
        store.clone(),
        destination.clone(),
    );

    let table_ready_notify = store
        .notify_on_table_state_type(table_1_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // Create a new table in the same schema and insert a row.
    let table_2 = test_table_name("table_2");
    let table_2_id = database
        .create_table(table_2.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    database
        .insert_values(table_2.clone(), &["value"], &[&1_i32])
        .await
        .unwrap();

    // Wait for the events to come in from the new table.
    sleep(Duration::from_secs(2)).await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that only the schemas of the first table were stored.
    let table_schemas = store.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(!table_schemas.contains_key(&table_2_id));

    // We restart the pipeline and verify that the new table is now processed.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_string(),
        store.clone(),
        destination.clone(),
    );

    let table_ready_notify = store
        .notify_on_table_state_type(table_2_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that both schemas exist.
    let table_schemas = store.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(table_schemas.contains_key(&table_2_id));
}

async fn run_table_sync_copy_case<F>(
    table_sync_copy_fn: F,
    expected_users_copied_rows: usize,
    expected_orders_copied_rows: usize,
) where
    F: FnOnce(TableId, TableId) -> TableSyncCopyConfig,
{
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let users_table_id = database_schema.users_schema().id;
    let orders_table_id = database_schema.orders_schema().id;
    let users_table_name = database_schema.users_schema().name.clone();
    let orders_table_name = database_schema.orders_schema().name.clone();

    // We insert a single user and order.
    insert_users_data(&mut database, &users_table_name, 0..=0).await;
    insert_orders_data(&mut database, &orders_table_name, 0..=0).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let table_sync_copy = table_sync_copy_fn(users_table_id, orders_table_id);
    let mut pipeline = create_pipeline_with_table_sync_copy_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        table_sync_copy,
    );

    // We wait for both tables to be ready for streaming.
    let users_table_ready_notify = store
        .notify_on_table_state_type(users_table_id, TableReplicationPhaseType::Ready)
        .await;
    let orders_table_ready_notify = store
        .notify_on_table_state_type(orders_table_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    users_table_ready_notify.notified().await;
    orders_table_ready_notify.notified().await;

    // We wait for the two inserts.
    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    // We insert additional data.
    insert_users_data(&mut database, &users_table_name, 1..=1).await;
    insert_orders_data(&mut database, &orders_table_name, 1..=1).await;

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We validate that the table rows are correct.
    let table_rows = destination.get_table_rows().await;
    let users_table_copied_rows = table_rows
        .get(&users_table_id)
        .map(|r| r.len())
        .unwrap_or(0);
    let orders_table_copied_rows = table_rows
        .get(&orders_table_id)
        .map(|r| r.len())
        .unwrap_or(0);
    assert_eq!(users_table_copied_rows, expected_users_copied_rows);
    assert_eq!(orders_table_copied_rows, expected_orders_copied_rows);
    // We always expect the method to be called since the downstream table should be created
    // nonetheless.
    assert_eq!(destination.write_table_rows_called().await, 2);

    // We validate that the single insert was received.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    assert_eq!(
        grouped_events
            .get(&(EventType::Insert, users_table_id))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        grouped_events
            .get(&(EventType::Insert, orders_table_id))
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_include_all_tables() {
    run_table_sync_copy_case(|_, _| TableSyncCopyConfig::IncludeAllTables, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_skip_all_tables() {
    run_table_sync_copy_case(|_, _| TableSyncCopyConfig::SkipAllTables, 0, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_include_only_specified_tables() {
    run_table_sync_copy_case(
        |users_table_id, _| TableSyncCopyConfig::IncludeTables {
            table_ids: vec![users_table_id.into_inner()],
        },
        1,
        0,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_skip_only_specified_tables() {
    run_table_sync_copy_case(
        |users_table_id, _| TableSyncCopyConfig::SkipTables {
            table_ids: vec![users_table_id.into_inner()],
        },
        0,
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_replicates_existing_data() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=rows_inserted,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);

    // Check that the replication slots for the two tables have been removed.
    let users_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.users_schema().id)
            .try_into()
            .unwrap();
    let orders_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.orders_schema().id)
            .try_into()
            .unwrap();
    assert!(
        !database
            .replication_slot_exists(&users_replication_slot)
            .await
            .unwrap()
    );
    assert!(
        !database
            .replication_slot_exists(&orders_replication_slot)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_sync_streams_new_data() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=rows_inserted,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for initial table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Insert additional data to test streaming.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (rows_inserted + 1)..=(rows_inserted + 2),
        true,
    )
    .await;

    // Register notifications for ready state.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // We wait for all the inserts to be received.
    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 8)])
        .await;

    // Insert more data to test apply worker processing.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (rows_inserted + 3)..=(rows_inserted + 4),
        true,
    )
    .await;

    users_state_notify.notified().await;
    orders_state_notify.notified().await;
    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify initial table copy data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);

    // Get all the events that were produced to the destination and assert them individually by table
    // since the only thing we are guaranteed is that the order of operations is preserved within the
    // same table but not across tables given the asynchronous nature of the pipeline (e.g., we could
    // start streaming earlier on a table for data which was inserted after another table which was
    // modified before this one)
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .unwrap();
    let orders_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap();

    // Build expected events for verification
    let expected_users_inserts = build_expected_users_inserts(
        11,
        database_schema.users_schema().id,
        vec![
            ("user_11", 11),
            ("user_12", 12),
            ("user_13", 13),
            ("user_14", 14),
        ],
    );
    let expected_orders_inserts = build_expected_orders_inserts(
        11,
        database_schema.orders_schema().id,
        vec![
            "description_11",
            "description_12",
            "description_13",
            "description_14",
        ],
    );
    assert_events_equal(users_inserts, &expected_users_inserts);
    assert_events_equal(orders_inserts, &expected_orders_inserts);

    // Check that the replication slots for the two tables have been removed.
    let users_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.users_schema().id)
            .try_into()
            .unwrap();
    let orders_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.orders_schema().id)
            .try_into()
            .unwrap();
    assert!(
        !database
            .replication_slot_exists(&users_replication_slot)
            .await
            .unwrap()
    );
    assert!(
        !database
            .replication_slot_exists(&orders_replication_slot)
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_streams_new_data_with_batch_timeout_expired() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    // We set a batch of 1000 elements to check if after 1000ms we still get the batch which is <
    // 1000 elements.
    let batch_config = BatchConfig {
        max_size: 1000,
        max_fill_ms: 1000,
    };
    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        batch_config,
    );

    // Register notifications for initial table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Insert additional data to test streaming.
    let rows_inserted = 5;
    insert_users_data(
        &mut database,
        &database_schema.users_schema().name,
        1..=rows_inserted,
    )
    .await;

    // We wait for all the inserts to be received.
    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 5)])
        .await;

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .unwrap();
    // Build expected events for verification
    let expected_users_inserts = build_expected_users_inserts(
        1,
        database_schema.users_schema().id,
        vec![
            ("user_1", 1),
            ("user_2", 2),
            ("user_3", 3),
            ("user_4", 4),
            ("user_5", 5),
        ],
    );
    assert_events_equal(users_inserts, &expected_users_inserts);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_processing_converges_to_apply_loop_with_no_events_coming() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Insert some data to test that the table copy is performed.
    let rows_inserted = 5;
    insert_users_data(
        &mut database,
        &database_schema.users_schema().name,
        1..=rows_inserted,
    )
    .await;

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    // We set a batch of 1000 elements to still check that even with batching we are getting all the
    // data.
    let batch_config = BatchConfig {
        max_size: 1000,
        max_fill_ms: 1000,
    };
    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        batch_config,
    );

    // Register notifications for initial table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify initial table copy data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_processing_with_schema_change_errors_table() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::OrdersOnly).await;

    // Insert data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_1"],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for initial table copy completion.
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    orders_state_notify.notified().await;

    // Register notification for the sync done state.
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_2"],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    // Register notification for the ready state.
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description"],
            &[&"description_3"],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    // Register notification for the errored state.
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Errored,
        )
        .await;

    // Change the schema of orders by adding a new column.
    database
        .alter_table(
            database_schema.orders_schema().name.clone(),
            &[TableModification::AddColumn {
                name: "date",
                data_type: "integer",
            }],
        )
        .await
        .unwrap();

    // Insert new data in the table.
    database
        .insert_values(
            database_schema.orders_schema().name.clone(),
            &["description", "date"],
            &[&"description_with_date", &10],
        )
        .await
        .unwrap();

    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We assert that the schema is the initial one.
    let table_schemas = store.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(
        *table_schemas
            .get(&database_schema.orders_schema().id)
            .unwrap(),
        database_schema.orders_schema()
    );

    // We check that we got the insert events after the first data of the table has been copied.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let orders_inserts = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap();

    let expected_orders_inserts = build_expected_orders_inserts(
        2,
        database_schema.orders_schema().id,
        vec!["description_2", "description_3"],
    );
    assert_events_equal(orders_inserts, &expected_orders_inserts);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_without_primary_key_is_errored() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("no_primary_key_table");
    let table_id = database
        .create_table(table_name.clone(), false, &[("name", "text")])
        .await
        .unwrap();

    let publication_name = "test_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    // Insert a row to later check that this doesn't appear in destination's table rows.
    database
        .insert_values(table_name.clone(), &["name"], &[&"abc"])
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    // We wait for the table to be errored.
    let errored_state = state_store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Errored)
        .await;

    pipeline.start().await.unwrap();

    // Insert a row to later check that it is not processed by the apply worker.
    database
        .insert_values(table_name.clone(), &["name"], &[&"abc1"])
        .await
        .unwrap();

    errored_state.notified().await;

    // Wait for the pipeline expecting an error to be returned.
    let err = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(err.kinds().len(), 1);
    assert_eq!(err.kinds()[0], ErrorKind::SourceSchemaError);

    // We expect the insert events to not be saved.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_id));
    assert!(insert_events.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_respects_column_level_publication() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // Create a table with multiple columns including a sensitive 'email' column.
    let table_name = test_table_name("users");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("name", "text not null"),
                ("age", "integer not null"),
                ("email", "text not null"),
            ],
        )
        .await
        .unwrap();

    // Create publication with only a subset of columns (excluding 'email').
    let publication_name = "test_pub".to_string();
    database
        .run_sql(&format!(
            "create publication {publication_name} for table {} (id, name, age)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("Failed to create publication with column filter");

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    // Wait for the table to be ready.
    let table_ready_notify = state_store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // Wait for two insert events to be processed.
    let insert_events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 2)])
        .await;

    // Insert test data with all columns (including email).
    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Alice', 25, 'alice@example.com'), ('Bob', 30, 'bob@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    insert_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the events and check that only published columns are included.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    assert_eq!(insert_events.len(), 2);

    // Check that each insert event contains only the published columns (id, name, age).
    // Since Cell values don't include column names, we verify by checking the count.
    for event in insert_events {
        if let Event::Insert(InsertEvent { table_row, .. }) = event {
            // Verify exactly 3 columns (id, name, age).
            // If email was included, there would be 4 values.
            assert_eq!(table_row.values.len(), 3);
        }
    }

    // Also verify the stored table schema only includes published columns.
    let table_schemas = state_store.get_table_schemas().await;
    let stored_schema = table_schemas.get(&table_id).unwrap();
    let column_names: Vec<&str> = stored_schema
        .column_schemas
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(column_names.contains(&"id"));
    assert!(column_names.contains(&"name"));
    assert!(column_names.contains(&"age"));
    assert!(!column_names.contains(&"email"));
    assert_eq!(stored_schema.column_schemas.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_tables_are_created_at_destination() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Create an empty table with a primary key.
    let table_name = test_table_name("empty_table");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text"), ("created_at", "timestamp")],
        )
        .await
        .unwrap();

    // Create publication for the table.
    let publication_name = format!("pub_{}", random::<u32>());
    database
        .run_sql(&format!(
            "create publication {} for table {}",
            publication_name,
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start the pipeline.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for the table to be ready.
    let table_ready_notify = state_store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready)
        .await;

    table_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the table schema was stored.
    let table_schemas = state_store.get_table_schemas().await;
    let table_schema = table_schemas.get(&table_id).unwrap();
    assert_table_schema(
        table_schema,
        table_id,
        table_name,
        &[
            id_column_schema(),
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::TEXT,
                modifier: -1,
                nullable: true,
                primary: false,
            },
            ColumnSchema {
                name: "created_at".to_string(),
                typ: Type::TIMESTAMP,
                modifier: -1,
                nullable: true,
                primary: false,
            },
        ],
    );

    // Verify no rows were written (table was empty).
    let all_table_rows = destination.get_table_rows().await;
    let empty_vec = vec![];
    let table_rows = all_table_rows.get(&table_id).unwrap_or(&empty_vec);
    assert!(table_rows.is_empty());

    // Verify that the write table rows method was called nonetheless.
    assert_eq!(destination.write_table_rows_called().await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_processes_concurrent_inserts_during_startup() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Start the pipeline after spawning the insert task.
    pipeline.start().await.unwrap();

    // Spawn a task that inserts data concurrently using a separate connection.
    // This creates a race condition where some rows may be captured during table copy
    // and others during streaming replication.
    let rows_to_insert = 10;
    let users_table_name = database_schema.users_schema().name.clone();
    let orders_table_name = database_schema.orders_schema().name.clone();
    let mut duplicate_database = database.duplicate().await;

    // Wait for both tables to reach Ready state.
    let users_ready_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_ready_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // Wait for all rows to be processed (either as table copy or streaming inserts).
    // This waits for 20 total inserts across both tables (10 users + 10 orders).
    let all_events_notify = destination
        .wait_for_all_events(vec![(EventType::Insert, (rows_to_insert * 2) as u64)])
        .await;

    // Use a JoinHandle to ensure the task completes and the database isn't dropped prematurely.
    let insert_handle = tokio::spawn(async move {
        insert_mock_data(
            &mut duplicate_database,
            &users_table_name,
            &orders_table_name,
            1..=rows_to_insert,
            true,
        )
        .await;

        // Return the database to prevent it from being dropped and destroying the test database.
        duplicate_database
    });

    users_ready_notify.notified().await;
    orders_ready_notify.notified().await;
    all_events_notify.notified().await;

    // Wait for the insert task to complete and retrieve the database connection.
    let duplicate_database = insert_handle.await.unwrap();

    // Validate that the sum of table rows (from copy) + insert events (from streaming) equals expected count.
    let table_rows = destination.get_table_rows().await;
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);

    let users_copied_rows = table_rows
        .get(&database_schema.users_schema().id)
        .map(|r| r.len())
        .unwrap_or(0);
    let users_insert_events = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .map(|e| e.len())
        .unwrap_or(0);
    let total_users = users_copied_rows + users_insert_events;

    let orders_copied_rows = table_rows
        .get(&database_schema.orders_schema().id)
        .map(|r| r.len())
        .unwrap_or(0);
    let orders_insert_events = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .map(|e| e.len())
        .unwrap_or(0);
    let total_orders = orders_copied_rows + orders_insert_events;

    assert_eq!(total_users, rows_to_insert);
    assert_eq!(total_orders, rows_to_insert);

    // Validate that both tables are in Ready state after inserts.
    let states = store.get_table_replication_states().await;
    assert_eq!(
        states.get(&database_schema.users_schema().id),
        Some(&TableReplicationPhase::Ready)
    );
    assert_eq!(
        states.get(&database_schema.orders_schema().id),
        Some(&TableReplicationPhase::Ready)
    );

    // Clear events and table rows to prepare for updates and deletes.
    destination.clear_events().await;
    destination.clear_table_rows().await;

    // Spawn a task to perform updates and deletes.
    let rows_to_update = 5;
    let rows_to_delete = 3;
    let users_table_name = database_schema.users_schema().name.clone();
    let orders_table_name = database_schema.orders_schema().name.clone();

    // Wait for all update and delete events to be processed.
    let updates_deletes_notify = destination
        .wait_for_events_count(vec![
            (EventType::Update, (rows_to_update * 2) as u64),
            (EventType::Delete, (rows_to_delete * 2) as u64),
        ])
        .await;

    let update_delete_handle = tokio::spawn(async move {
        // Update rows 1-5 for both tables.
        for i in 1..=rows_to_update {
            duplicate_database
                .update_with_expressions(
                    users_table_name.clone(),
                    &["age = age + 100"],
                    &["id"],
                    &[&i.to_string()],
                    " and ",
                )
                .await
                .unwrap();

            duplicate_database
                .update_with_expressions(
                    orders_table_name.clone(),
                    &["description = description || '_updated'"],
                    &["id"],
                    &[&i.to_string()],
                    " and ",
                )
                .await
                .unwrap();
        }

        // Delete rows 6-8 for both tables.
        for i in 6..=(6 + rows_to_delete - 1) {
            duplicate_database
                .delete_values(
                    users_table_name.clone(),
                    &["id"],
                    &[&i.to_string()],
                    " and ",
                )
                .await
                .unwrap();

            duplicate_database
                .delete_values(
                    orders_table_name.clone(),
                    &["id"],
                    &[&i.to_string()],
                    " and ",
                )
                .await
                .unwrap();
        }

        duplicate_database
    });

    updates_deletes_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Wait for the update/delete task to complete.
    let _duplicate_database = update_delete_handle.await.unwrap();

    // Validate that both tables are in Ready state.
    let states = store.get_table_replication_states().await;
    assert_eq!(
        states.get(&database_schema.users_schema().id),
        Some(&TableReplicationPhase::Ready)
    );
    assert_eq!(
        states.get(&database_schema.orders_schema().id),
        Some(&TableReplicationPhase::Ready)
    );

    // Validate the update and delete events were received correctly.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);

    let users_updates = grouped_events
        .get(&(EventType::Update, database_schema.users_schema().id))
        .map(|e| e.len())
        .unwrap_or(0);
    let users_deletes = grouped_events
        .get(&(EventType::Delete, database_schema.users_schema().id))
        .map(|e| e.len())
        .unwrap_or(0);

    let orders_updates = grouped_events
        .get(&(EventType::Update, database_schema.orders_schema().id))
        .map(|e| e.len())
        .unwrap_or(0);
    let orders_deletes = grouped_events
        .get(&(EventType::Delete, database_schema.orders_schema().id))
        .map(|e| e.len())
        .unwrap_or(0);

    assert_eq!(users_updates, rows_to_update);
    assert_eq!(users_deletes, rows_to_delete);
    assert_eq!(orders_updates, rows_to_update);
    assert_eq!(orders_deletes, rows_to_delete);
}
