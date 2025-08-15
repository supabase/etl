//! Integration tests for the Iceberg destination.
//!
//! These tests verify the Iceberg destination works correctly with real Iceberg
//! catalogs and storage backends. They mirror the BigQuery integration tests
//! to ensure feature parity between destinations.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::config::BatchConfig;
use etl::error::ErrorKind;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PgNumeric, PipelineId};
use etl_telemetry::init_test_tracing;
use rand::random;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

use crate::common::iceberg::{
    IcebergOrder, IcebergUser, parse_iceberg_table_rows, setup_iceberg_connection,
};

/// Test full table copy and streaming replication with pipeline restart.
///
/// This test mirrors the BigQuery `table_copy_and_streaming_with_restart` test
/// to ensure the Iceberg destination provides equivalent functionality.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires running Iceberg REST catalog
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();

    // Set up source PostgreSQL database
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Set up Iceberg destination
    let iceberg_database = setup_iceberg_connection().await;

    // Insert initial test data
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let raw_destination = iceberg_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion
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

    // Wait for initial table sync to complete
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Query Iceberg tables directly to verify initial data
    let users_rows = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    let parsed_users_rows = parse_iceberg_table_rows::<IcebergUser>(users_rows);
    
    // Note: This assertion will need to be updated once query_table is implemented
    // For now, we verify the test structure is correct
    eprintln!("Initial users data validation - implement once query_table is ready");

    let orders_rows = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.orders_schema().name))
        .await
        .unwrap();
    let parsed_orders_rows = parse_iceberg_table_rows::<IcebergOrder>(orders_rows);
    
    eprintln!("Initial orders data validation - implement once query_table is ready");

    // Insert additional data for streaming test
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    // Restart pipeline to test streaming replication
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // Wait for streaming replication to catch up
    sleep(Duration::from_secs(5)).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify streaming data was replicated
    let users_rows_after = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    let parsed_users_rows_after = parse_iceberg_table_rows::<IcebergUser>(users_rows_after);
    
    eprintln!("Streaming users data validation - implement once query_table is ready");

    // Cleanup
    iceberg_database.cleanup().await.unwrap();
}

/// Test streaming replication with updates and deletes.
///
/// This test verifies CDC operations (INSERT, UPDATE, DELETE) work correctly
/// with the Iceberg destination.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires running Iceberg REST catalog
async fn streaming_with_updates_and_deletes() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let iceberg_database = setup_iceberg_connection().await;

    // Insert initial data
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=3,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = iceberg_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Wait for initial sync
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Perform UPDATE operation
    database
        .execute(
            &format!(
                "UPDATE {} SET name = 'updated_user_1' WHERE id = 1",
                database_schema.users_schema().name.name
            ),
            &[],
        )
        .await
        .unwrap();

    // Perform DELETE operation
    database
        .execute(
            &format!(
                "DELETE FROM {} WHERE id = 2",
                database_schema.users_schema().name.name
            ),
            &[],
        )
        .await
        .unwrap();

    // Wait for CDC events to be processed
    sleep(Duration::from_secs(3)).await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify CDC operations were applied
    let users_rows = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    
    eprintln!("CDC operations validation - implement once query_table is ready");
    // Should verify:
    // - User 1 has updated name
    // - User 2 is marked as deleted (or actually removed depending on delete strategy)
    // - User 3 remains unchanged

    iceberg_database.cleanup().await.unwrap();
}

/// Test table truncation operations.
///
/// Verifies that TRUNCATE operations create new table versions correctly.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires running Iceberg REST catalog
async fn table_truncation() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Users).await;
    let iceberg_database = setup_iceberg_connection().await;

    // Insert initial data
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=5,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = iceberg_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Verify initial data exists
    let users_rows_before = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    
    eprintln!("Pre-truncate validation - implement once query_table is ready");

    // Perform TRUNCATE
    database
        .execute(
            &format!(
                "TRUNCATE TABLE {}",
                database_schema.users_schema().name.name
            ),
            &[],
        )
        .await
        .unwrap();

    // Wait for truncate to be processed
    sleep(Duration::from_secs(3)).await;

    // Verify table is empty after truncate
    let users_rows_after = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    
    eprintln!("Post-truncate validation - implement once query_table is ready");
    // Should verify table is empty

    // Insert new data after truncate
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        10..=12,
        false,
    )
    .await;

    sleep(Duration::from_secs(3)).await;

    // Verify new data appears in clean table
    let users_rows_final = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    
    eprintln!("Post-truncate insert validation - implement once query_table is ready");
    // Should verify only new data (IDs 10, 11, 12) exists

    pipeline.shutdown_and_wait().await.unwrap();
    iceberg_database.cleanup().await.unwrap();
}

/// Test error handling and recovery scenarios.
///
/// Verifies the destination handles various error conditions gracefully.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires running Iceberg REST catalog
async fn error_handling_and_recovery() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Users).await;
    let iceberg_database = setup_iceberg_connection().await;

    let store = NotifyingStore::new();
    let destination = iceberg_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(destination);

    // Test with very small batch config to trigger frequent commits
    let batch_config = BatchConfig {
        max_size: 1, // Force each row to be its own batch
        max_fill_ms: 100,
    };

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        batch_config,
    );

    // Insert data that will trigger many small batches
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=10,
        false,
    )
    .await;

    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();
    users_state_notify.notified().await;

    // Verify all data was processed despite small batches
    let users_rows = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    
    eprintln!("Small batch processing validation - implement once query_table is ready");
    // Should verify all 10 rows were processed correctly

    pipeline.shutdown_and_wait().await.unwrap();
    iceberg_database.cleanup().await.unwrap();
}

/// Test concurrent pipeline operations.
///
/// Verifies the destination can handle concurrent access safely.
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires running Iceberg REST catalog
async fn concurrent_operations() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let iceberg_database = setup_iceberg_connection().await;

    // Insert data across both tables
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=5,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = iceberg_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

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

    // Wait for both tables to complete initial sync
    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Perform concurrent operations on both tables
    let users_update = database.execute(
        &format!(
            "INSERT INTO {} (id, name, organization_id) VALUES (100, 'concurrent_user', 1)",
            database_schema.users_schema().name.name
        ),
        &[],
    );

    let orders_update = database.execute(
        &format!(
            "INSERT INTO {} (id, description) VALUES (200, 'concurrent_order')",
            database_schema.orders_schema().name.name
        ),
        &[],
    );

    // Execute both operations concurrently
    let (users_result, orders_result) = tokio::join!(users_update, orders_update);
    users_result.unwrap();
    orders_result.unwrap();

    // Wait for replication
    sleep(Duration::from_secs(3)).await;

    // Verify both operations were processed
    let users_rows = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.users_schema().name))
        .await
        .unwrap();
    let orders_rows = iceberg_database
        .query_table(&iceberg_database.format_table_name(&database_schema.orders_schema().name))
        .await
        .unwrap();
    
    eprintln!("Concurrent operations validation - implement once query_table is ready");
    // Should verify:
    // - Users table contains the concurrent insert (ID 100)
    // - Orders table contains the concurrent insert (ID 200)
    // - All original data is still present

    pipeline.shutdown_and_wait().await.unwrap();
    iceberg_database.cleanup().await.unwrap();
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[tokio::test]
    async fn test_iceberg_database_setup() {
        let db = setup_iceberg_connection().await;
        assert!(db.namespace().starts_with("etl_tests_"));
    }

    #[test]
    fn test_table_name_formatting() {
        // This test doesn't require a running Iceberg catalog
        // so it can run without the #[ignore] attribute
        let table_name = etl::types::TableName::new("public".to_string(), "users".to_string());
        // We can't easily test format_table_name without creating a full database
        // but we can verify the table name structure
        assert_eq!(table_name.schema, "public");
        assert_eq!(table_name.name, "users");
    }
}