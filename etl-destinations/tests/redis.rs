#![cfg(feature = "redis")]

use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::spawn_source_database;
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, setup_test_database_schema};
use etl::types::{EventType, PipelineId};
use etl_destinations::encryption::install_crypto_provider;
use etl_destinations::redis::{RedisConfig, RedisDestination};
use etl_telemetry::tracing::init_test_tracing;
use fred::prelude::KeysInterface;
use rand::random;
use serde_json::Value;

mod support;

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let redis_cfg = RedisConfig {
        host: String::from("127.0.0.1"),
        port: 6379,
        username: None,
        password: None,
        ttl: None,
    };
    let redis = RedisDestination::new(redis_cfg.clone(), store.clone())
        .await
        .unwrap();
    let destination = TestDestinationWrapper::wrap(redis);

    // Start pipeline from scratch.
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

    let redis_client = crate::support::redis::create_redis_connection(redis_cfg)
        .await
        .unwrap();

    // Test if it has been inserted in redis
    let user_row: String = redis_client
        .get(format!(
            "{}::::id:1",
            database_schema.users_schema().name.name
        ))
        .await
        .unwrap();
    let user_value: Value = serde_json::from_str(&user_row).unwrap();
    assert_eq!(
        serde_json::json!({
            "id": 1,
            "name": "user_1",
            "age": 1
        }),
        user_value
    );

    // Wait for the update.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Update the row.
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_10", &10],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // Test if it has been updated in redis
    let user_row: String = redis_client
        .get(format!(
            "{}::::id:1",
            database_schema.users_schema().name.name
        ))
        .await
        .unwrap();
    let user_value: Value = serde_json::from_str(&user_row).unwrap();
    assert_eq!(
        serde_json::json!({
            "id": 1,
            "name": "user_10",
            "age": 10
        }),
        user_value
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

    // Test if it has been removed in redis
    assert!(
        redis_client
            .get::<String, _>(format!(
                "{}::::id:1",
                database_schema.users_schema().name.name
            ))
            .await
            .is_err()
    );
}
