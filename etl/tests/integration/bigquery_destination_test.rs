use etl::v2::destination::bigquery::BigQueryDestination;
use etl::v2::state::table::TableReplicationPhaseType;

use crate::common::database::spawn_database;
use crate::common::pipeline_v2::{create_pipeline_identity, spawn_pg_pipeline};
use crate::common::state_store::TestStateStore;
use crate::common::test_schema::{insert_mock_data, setup_test_database_schema, TableSelection};

async fn setup_bigquery_destination() -> BigQueryDestination {
    BigQueryDestination::new_with_key(
        "local-project".to_owned(),
        "local-dataset".to_owned(),
        "",
        1,
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy() {
    let mut database = spawn_database().await;
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

    let state_store = TestStateStore::new();
    let destination = setup_bigquery_destination().await;

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
}
