use etl::destination::memory::MemoryDestination;
use etl::error::ErrorKind;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::event::group_events_by_type_and_table_id;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{
    TableSelection, assert_events_equal, build_expected_orders_inserts,
    build_expected_users_inserts, get_n_integers_sum, get_users_age_sum_from_rows,
    insert_mock_data, insert_users_data, setup_test_database_schema,
};
use etl::types::{EventType, PipelineId};
use etl_config::shared::BatchConfig;
use etl_postgres::replication::slots::get_slot_name;
use etl_postgres::replication::worker::WorkerType;
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use std::time::Duration;
use tokio::time::sleep;

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
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that the states are correctly set.
    let table_replication_states = store.get_table_replication_states().await;
    assert_eq!(table_replication_states.len(), 2);
    assert_eq!(
        table_replication_states
            .get(&database_schema.users_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::SyncDone
    );
    assert_eq!(
        table_replication_states
            .get(&database_schema.orders_schema().id)
            .unwrap()
            .as_type(),
        TableReplicationPhaseType::SyncDone
    );

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

    // Wait for initial copy completion (SyncDone) for both tables.
    let table_1_done = store
        .notify_on_table_state(table_1_id, TableReplicationPhaseType::SyncDone)
        .await;
    let table_2_done = store
        .notify_on_table_state(table_2_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_1_done.notified().await;
    table_2_done.notified().await;

    if let Some(server_version) = database.server_version()
        && server_version.get() <= 150000
    {
        println!(
            "Skipping test for PostgreSQL version <= 15, CREATE PUBLICATION FOR TABLES IN SCHEMA is not supported"
        );
        return;
    }

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

    // Shutdown pipeline before altering publication (by dropping one table)
    pipeline.shutdown_and_wait().await.unwrap();

    // Drop table_2 so it's no longer part of the publication
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
    let table_3_done = store
        .notify_on_table_state(table_3_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_3_done.notified().await;

    // Insert one row in table_1 and wait for it. (We wait for 4 inserts since it keeps the previous
    // ones).
    let inserts_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
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

    // Assert that table_2 state is gone but destination data remains
    let states = store.get_table_replication_states().await;
    assert!(states.contains_key(&table_1_id));
    assert!(!states.contains_key(&table_2_id));
    assert!(states.contains_key(&table_3_id));

    // The destination should have the 2 events of the first table, the 1 event of the removed table
    // and the 1 event of the new table.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let table_1_inserts = grouped
        .get(&(EventType::Insert, table_1_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(table_1_inserts.len(), 2);
    let table_2_inserts = grouped
        .get(&(EventType::Insert, table_2_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(table_2_inserts.len(), 1);
    let table_3_inserts = grouped
        .get(&(EventType::Insert, table_3_id))
        .cloned()
        .unwrap_or_default();
    assert_eq!(table_3_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_for_all_tables_in_schema_ignores_new_tables_until_restart() {
    init_test_tracing();

    let database = spawn_source_database().await;

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

    let sync_done = store
        .notify_on_table_state(table_1_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    sync_done.notified().await;

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

    if let Some(server_version) = database.server_version()
        && server_version.get() <= 150000
    {
        println!(
            "Skipping test for PostgreSQL version <= 15, CREATE PUBLICATION FOR TABLES IN SCHEMA is not supported"
        );
        return;
    }

    // We restart the pipeline and verify that the new table is now processed.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_string(),
        store.clone(),
        destination.clone(),
    );

    let sync_done = store
        .notify_on_table_state(table_2_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    sync_done.notified().await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that both schemas exist.
    let table_schemas = store.get_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(table_schemas.contains_key(&table_2_id));
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
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
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
    let users_replication_slot = get_slot_name(
        pipeline_id,
        WorkerType::TableSync {
            table_id: database_schema.users_schema().id,
        },
    )
    .unwrap();
    let orders_replication_slot = get_slot_name(
        pipeline_id,
        WorkerType::TableSync {
            table_id: database_schema.orders_schema().id,
        },
    )
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
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
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
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
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
    let users_replication_slot = get_slot_name(
        pipeline_id,
        WorkerType::TableSync {
            table_id: database_schema.users_schema().id,
        },
    )
    .unwrap();
    let orders_replication_slot = get_slot_name(
        pipeline_id,
        WorkerType::TableSync {
            table_id: database_schema.orders_schema().id,
        },
    )
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
async fn table_sync_streams_new_data_with_batch() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    // We set a batch of 1000 elements to still check that even with batching we are getting all the
    // data.
    let batch_config = BatchConfig {
        max_size: 1000,
        max_fill_ms: 1000,
    };
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        Some(batch_config),
    );

    // Register notifications for initial table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
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

    // Register notifications for ready state.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    // We wait for all the inserts to be received.
    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 5)])
        .await;

    users_state_notify.notified().await;
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
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        Some(batch_config),
    );

    // Register notifications for initial table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
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
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    orders_state_notify.notified().await;

    // Register notification for the sync done state.
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
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
        .notify_on_table_state(
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
        .notify_on_table_state(
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
        .notify_on_table_state(table_id, TableReplicationPhaseType::Errored)
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

    // We expect no events to be saved.
    let events = destination.get_events().await;
    assert!(events.is_empty());
}
