use crate::common::bigquery::setup_bigquery_connection;
use crate::common::database::spawn_database;
use crate::common::install_crypto_provider_once;
use crate::common::pipeline_v2::{create_pipeline_identity, spawn_pg_pipeline};
use crate::common::state_store::TestStateStore;
use crate::common::test_schema::bigquery::{
    parse_bigquery_table_rows, BigQueryOrder, BigQueryUser,
};
use crate::common::test_schema::{insert_mock_data, setup_test_database_schema, TableSelection};
use etl::v2::state::table::TableReplicationPhaseType;
use telemetry::init_test_tracing;

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_and_data_are_copied() {
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

    let state_store = TestStateStore::new();
    let destination = bigquery_database.build_destination().await;

    // Start pipeline from scratch.
    let identity = create_pipeline_identity(&database_schema.publication_name());
    let mut pipeline = spawn_pg_pipeline(
        &identity,
        &database.config,
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await;
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows.len(), 2);
    assert_eq!(
        parsed_users_rows,
        vec![
            BigQueryUser::new(1, "user_1", 1),
            BigQueryUser::new(2, "user_2", 2),
        ]
    );

    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await;
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(parsed_orders_rows.len(), 2);
    assert_eq!(
        parsed_orders_rows,
        vec![
            BigQueryOrder::new(1, "description_1"),
            BigQueryOrder::new(2, "description_2"),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_and_events_are_copied() {
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

    let state_store = TestStateStore::new();
    let destination = bigquery_database.build_destination().await;

    // Start pipeline from scratch.
    let identity = create_pipeline_identity(&database_schema.publication_name());
    let mut pipeline = spawn_pg_pipeline(
        &identity,
        &database.config,
        state_store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::FinishedCopy,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Register notifications for ready state.
    let users_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = state_store
        .notify_on_replication_phase(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
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

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await;
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows.len(), 4);
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
        .await;
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(parsed_orders_rows.len(), 4);
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
