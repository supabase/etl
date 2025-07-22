use config::shared::BatchConfig;
use etl::conversions::event::EventType;
use etl::destination::base::Destination;
use etl::encryption::bigquery::install_crypto_provider_once;
use etl::pipeline::PipelineId;
use etl::state::store::notify::NotifyingStateStore;
use etl::state::table::TableReplicationPhaseType;
use rand::random;
use telemetry::init_test_tracing;

use etl::test_utils::bigquery::setup_bigquery_connection;
use etl::test_utils::database::spawn_database;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::bigquery::{
    BigQueryOrder, BigQueryUser, parse_bigquery_table_rows,
};
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();
    install_crypto_provider_once();

    let mut database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let bigquery_database = setup_bigquery_connection().await;

    // Insert initial test data.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    let state_store = NotifyingStateStore::new();
    let raw_destination = bigquery_database.build_destination().await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We load the table schemas and check that they are correctly fetched.
    let mut table_schemas = destination.load_table_schemas().await.unwrap();
    table_schemas.sort();
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());

    // We query BigQuery directly to get the data which has been inserted by tests.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![
            BigQueryUser::new(1, "user_1", 1),
            BigQueryUser::new(2, "user_2", 2),
        ]
    );
    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await
        .unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![
            BigQueryOrder::new(1, "description_1"),
            BigQueryOrder::new(2, "description_2"),
        ]
    );

    // We restart the pipeline and check that we can process events since we have load the table
    // schema from the destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We expect 2 insert events for each table (4 total).
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    // Insert additional data.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We load the table schemas and check that they are correctly fetched.
    let mut table_schemas = destination.load_table_schemas().await.unwrap();
    table_schemas.sort();
    assert_eq!(table_schemas[0], database_schema.orders_schema());
    assert_eq!(table_schemas[1], database_schema.users_schema());

    // We query BigQuery directly to get the data which has been inserted by tests.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![
            BigQueryUser::new(1, "user_1", 1),
            BigQueryUser::new(2, "user_2", 2),
            BigQueryUser::new(3, "user_3", 3),
            BigQueryUser::new(4, "user_4", 4),
        ]
    );
    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await
        .unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![
            BigQueryOrder::new(1, "description_1"),
            BigQueryOrder::new(2, "description_2"),
            BigQueryOrder::new(3, "description_3"),
            BigQueryOrder::new(4, "description_4"),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();
    install_crypto_provider_once();

    let database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let bigquery_database = setup_bigquery_connection().await;

    let state_store = NotifyingStateStore::new();
    let raw_destination = bigquery_database.build_destination().await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert a row.
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // We query BigQuery to check for the insert.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_1", 1),]);

    // Wait for the update.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Update the row.
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &["'user_10'", "10"],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // We query BigQuery to check for the update.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![BigQueryUser::new(1, "user_10", 10),]
    );

    // Wait for the update.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    // Update the row.
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["name"],
            &["'user_10'"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery to check for deletion.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await;
    assert!(users_rows.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_subsequent_updates() {
    init_test_tracing();
    install_crypto_provider_once();

    let mut database_1 = spawn_database().await;
    let mut database_2 = database_1.duplicate().await;
    let database_schema = setup_test_database_schema(&database_1, TableSelection::UsersOnly).await;

    let bigquery_database = setup_bigquery_connection().await;

    let state_store = NotifyingStateStore::new();
    let raw_destination = bigquery_database.build_destination().await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database_1.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 2)])
        .await;

    // Insert a row.
    database_1
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    // Create two transactions A and B on separate connections to make sure that the updates are
    // ordered correctly.
    let transaction_a = database_1.begin_transaction().await;
    transaction_a
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &["'user_3'", "3"],
        )
        .await
        .unwrap();
    transaction_a.commit_transaction().await;
    let transaction_b = database_2.begin_transaction().await;
    transaction_b
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &["'user_2'", "2"],
        )
        .await
        .unwrap();
    transaction_b.commit_transaction().await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery to check for the final value.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_2", 2),]);
}

// This test is disabled since truncation is currently not supported by BigQuery when doing CDC
// streaming. The test is kept just for future use.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn table_truncate_with_batching() {
    init_test_tracing();
    install_crypto_provider_once();

    let mut database = spawn_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let bigquery_database = setup_bigquery_connection().await;

    let state_store = NotifyingStateStore::new();
    let raw_destination = bigquery_database.build_destination().await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        state_store.clone(),
        destination.clone(),
        // We use a batch size > 1, so that we can make sure that interleaved truncate statements
        // work well with multiple batches of events.
        Some(BatchConfig {
            max_size: 10,
            max_fill_ms: 1000,
        }),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Wait for the 4 inserts (2 per table) and 2 truncates (one per table).
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4), (EventType::Truncate, 2)])
        .await;

    // Insert 1 row per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=1,
        false,
    )
    .await;

    // We truncate both tables.
    database
        .truncate_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    database
        .truncate_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();

    // Insert 1 extra row per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        2..=2,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery directly to get the data which has been inserted by tests expecting that
    // only the rows after truncation are there.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(2, "user_2", 2),]);
    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await
        .unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![BigQueryOrder::new(2, "description_2"),]
    );
}
