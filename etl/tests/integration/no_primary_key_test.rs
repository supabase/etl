use etl::{
    destination::memory::MemoryDestination,
    pipeline::PipelineId,
    state::{store::notify::NotifyingStateStore, table::TableReplicationPhaseType},
};
use rand::random;
use telemetry::init_test_tracing;

use crate::common::{
    database::{spawn_database, test_table_name},
    pipeline::create_pipeline,
    test_destination_wrapper::TestDestinationWrapper,
};

#[tokio::test(flavor = "multi_thread")]
async fn tables_without_primary_key_are_skipped_test() {
    init_test_tracing();
    let database = spawn_database().await;

    let table_name = test_table_name("no_primary_key");
    let table_id = database
        .create_table(table_name.clone(), false, &[("name", "text")])
        .await
        .unwrap();

    let publication_name = "test_pub".to_string();
    database
        .create_publication(&publication_name, &[table_name.clone()])
        .await
        .expect("Failed to create publication");

    database
        .insert_values(table_name.clone(), &["name"], &[&"abc"])
        .await
        .unwrap();

    let state_store = NotifyingStateStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    let notification = state_store
        .notify_on_table_state(table_id, TableReplicationPhaseType::Skipped)
        .await;

    pipeline.start().await.unwrap();

    // insert a row to later check that it is not processed by the apply worker
    database
        .insert_values(table_name.clone(), &["name"], &[&"abc1"])
        .await
        .unwrap();

    notification.notified().await;

    // insert another row
    database
        .insert_values(table_name, &["name"], &[&"abc2"])
        .await
        .unwrap();

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    assert_eq!(dbg!(events).len(), 0);
}
