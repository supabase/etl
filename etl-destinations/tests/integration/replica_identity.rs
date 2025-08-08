use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::types::{EventType, PipelineId};
use etl_destinations::bigquery::install_crypto_provider_for_bigquery;
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::init_test_tracing;
use rand::distr::Alphanumeric;
use rand::{Rng, random};

use crate::common::bigquery::{parse_table_cell, setup_bigquery_connection};

/// Generates a string of random ASCII printable characters of the specified length.
/// This is useful for creating large text values that won't compress well,
/// ensuring they trigger PostgreSQL's TOAST storage mechanism.
fn generate_random_ascii_string(length: usize) -> String {
    let rng = rand::rng();
    rng.sample_iter(Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_default_replica_identity() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text"), // Column that will contain TOAST-able data
                ("small_int", "int4"),  // Column we'll update to test TOAST behavior
            ],
        )
        .await
        .unwrap();

    // Forcing storage to be set to external so that unchanged TOAST columns are not sent
    // in update events. Ideally, large values would automatically trigger the storage
    // to external but couldn't force Postgres in the tests even with very large
    // values, hence this workaround.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "large_text",
                alteration: "set storage external",
            }],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_toast".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert a row with a large text value that will be TOASTed
    // PostgreSQL typically TOASTs values larger than ~2KB (TOAST_TUPLE_THRESHOLD)
    let large_text_size_bytes = 8192;
    let large_text_value = generate_random_ascii_string(large_text_size_bytes);
    let initial_int_value = 100;

    let insert_event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["large_text", "small_int"],
            &[&large_text_value, &initial_int_value],
        )
        .await
        .unwrap();

    insert_event_notify.notified().await;

    // Verify the initial insert worked correctly
    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows.len(), 1);

    let initial_row = &table_rows[0];
    let columns = initial_row.columns.as_ref().unwrap();

    // Verify the large text was inserted correctly
    let inserted_large_text: String = parse_table_cell(columns[1].clone()).unwrap();
    assert_eq!(inserted_large_text, large_text_value);

    let inserted_int: i32 = parse_table_cell(columns[2].clone()).unwrap();
    assert_eq!(inserted_int, initial_int_value);

    // Now update only the small_int column, leaving the large_text unchanged
    // This should trigger TOAST behavior where PostgreSQL sends UnchangedToast
    // for the large_text column
    let updated_int_value = 200;

    let update_event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    database
        .update_values(table_name.clone(), &["small_int"], &[&updated_int_value])
        .await
        .unwrap();

    update_event_notify.notified().await;

    // Verify the update behavior - the large_text should be replaced with
    // default value (empty string) and small_int should be updated
    let table_rows_after_update = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows_after_update.len(), 1);

    let updated_row = &table_rows_after_update[0];
    let updated_columns = updated_row.columns.as_ref().unwrap();

    let large_text_after_update: String = parse_table_cell(updated_columns[1].clone()).unwrap();
    assert_eq!(
        large_text_after_update, "",
        "TOAST value should be reset to default during updates of other columns"
    );

    // The small_int should be updated
    let int_after_update: i32 = parse_table_cell(updated_columns[2].clone()).unwrap();
    assert_eq!(int_after_update, updated_int_value);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_full_replica_identity() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text"), // Column that will contain TOAST-able data
                ("small_int", "int4"),  // Column we'll update to test TOAST behavior
            ],
        )
        .await
        .unwrap();

    // Forcing storage to be set to external so that unchanged TOAST columns are not sent
    // in update events. Ideally, large values would automatically trigger the storage
    // to external but couldn't force Postgres in the tests even with very large
    // values, hence this workaround.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "large_text",
                alteration: "set storage external",
            }],
        )
        .await
        .unwrap();

    // Set replica identity on the table to full to test that TOAST values are sent
    // even if non-TOAST columns are updated
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::ReplicaIdentity { value: "full" }],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_toast".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert a row with a large text value that will be TOASTed
    // PostgreSQL typically TOASTs values larger than ~2KB (TOAST_TUPLE_THRESHOLD)
    let large_text_size_bytes = 8192;
    let large_text_value = generate_random_ascii_string(large_text_size_bytes);
    let initial_int_value = 100;

    let insert_event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["large_text", "small_int"],
            &[&large_text_value, &initial_int_value],
        )
        .await
        .unwrap();

    insert_event_notify.notified().await;

    // Verify the initial insert worked correctly
    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows.len(), 1);

    let initial_row = &table_rows[0];
    let columns = initial_row.columns.as_ref().unwrap();

    // Verify the large text was inserted correctly
    let inserted_large_text: String = parse_table_cell(columns[1].clone()).unwrap();
    assert_eq!(inserted_large_text, large_text_value);

    let inserted_int: i32 = parse_table_cell(columns[2].clone()).unwrap();
    assert_eq!(inserted_int, initial_int_value);

    // Now update only the small_int column, leaving the large_text unchanged
    // This should trigger TOAST behavior where PostgreSQL sends UnchangedToast
    // for the large_text column
    let updated_int_value = 200;

    let update_event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    database
        .update_values(table_name.clone(), &["small_int"], &[&updated_int_value])
        .await
        .unwrap();

    update_event_notify.notified().await;

    // Verify the update behavior - the large_text should not be replaced with
    // a default value and small_int should be updated
    let table_rows_after_update = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows_after_update.len(), 1);

    let updated_row = &table_rows_after_update[0];
    let updated_columns = updated_row.columns.as_ref().unwrap();

    let large_text_after_update: String = parse_table_cell(updated_columns[1].clone()).unwrap();
    assert_eq!(
        large_text_after_update, inserted_large_text,
        "TOAST value should be updated"
    );

    // The small_int should be updated
    let int_after_update: i32 = parse_table_cell(updated_columns[2].clone()).unwrap();
    assert_eq!(int_after_update, updated_int_value);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn update_toast_values_with_default_replica_identity() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text"), // Column that will contain TOAST-able data
                ("small_int", "int4"),  // Column we'll update to test TOAST behavior
            ],
        )
        .await
        .unwrap();

    // Forcing size to be set to external so that unchanged TOAST columns are not sent
    // in update events. Ideally, large values would automatically trigger the storage
    // to external but couldn't force Postgres in the tests even with very large
    // values, hence this workaround.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "large_text",
                alteration: "set storage external",
            }],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_toast".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert a row with a large text value that will be TOASTed
    // PostgreSQL typically TOASTs values larger than ~2KB (TOAST_TUPLE_THRESHOLD)
    let large_text_size_bytes = 8192;
    let large_text_value = generate_random_ascii_string(large_text_size_bytes);
    let initial_int_value = 100;

    let insert_event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["large_text", "small_int"],
            &[&large_text_value, &initial_int_value],
        )
        .await
        .unwrap();

    insert_event_notify.notified().await;

    // Verify the initial insert worked correctly
    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows.len(), 1);

    let initial_row = &table_rows[0];
    let columns = initial_row.columns.as_ref().unwrap();

    // Verify the large text was inserted correctly
    let inserted_large_text: String = parse_table_cell(columns[1].clone()).unwrap();
    assert_eq!(inserted_large_text, large_text_value);

    let inserted_int: i32 = parse_table_cell(columns[2].clone()).unwrap();
    assert_eq!(inserted_int, initial_int_value);

    // Test update to the toast column does set it to that value
    let update_event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    let updated_large_text_value = generate_random_ascii_string(large_text_size_bytes);
    database
        .update_values(
            table_name.clone(),
            &["large_text"],
            &[&updated_large_text_value],
        )
        .await
        .unwrap();

    update_event_notify.notified().await;

    // Verify that the large_text is updated
    let table_rows_after_update = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows_after_update.len(), 1);

    let updated_row = &table_rows_after_update[0];
    let updated_columns = updated_row.columns.as_ref().unwrap();

    // Both Large text and small int should be updated
    let updated_large_text: String = parse_table_cell(updated_columns[1].clone()).unwrap();
    assert_eq!(
        updated_large_text, updated_large_text_value,
        "TOAST value should be updated"
    );

    let updated_int: i32 = parse_table_cell(updated_columns[2].clone()).unwrap();
    assert_eq!(updated_int, initial_int_value);

    pipeline.shutdown_and_wait().await.unwrap();
}

/// Tests that PostgreSQL rejects update operations when replica identity is set to NONE.
/// When a table has replica identity NONE and is part of a publication that publishes updates,
/// PostgreSQL will reject update operations because it cannot identify which row to update
/// without sufficient replica identity information.
#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_none_replica_identity() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text"), // Column that will contain TOAST-able data
                ("small_int", "int4"),  // Column we'll update to test TOAST behavior
            ],
        )
        .await
        .unwrap();

    // Forcing storage to be set to external so that unchanged TOAST columns are not sent
    // in update events. Ideally, large values would automatically trigger the storage
    // to external but couldn't force Postgres in the tests even with very large
    // values, hence this workaround.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "large_text",
                alteration: "set storage external",
            }],
        )
        .await
        .unwrap();

    // Set replica identity on the table to none to test that PostgreSQL rejects
    // update operations when there's insufficient replica identity information
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::ReplicaIdentity { value: "nothing" }],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_toast".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert a row with a large text value that will be TOASTed
    // PostgreSQL typically TOASTs values larger than ~2KB (TOAST_TUPLE_THRESHOLD)
    let large_text_size_bytes = 8192;
    let large_text_value = generate_random_ascii_string(large_text_size_bytes);
    let initial_int_value = 100;

    let insert_event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["large_text", "small_int"],
            &[&large_text_value, &initial_int_value],
        )
        .await
        .unwrap();

    insert_event_notify.notified().await;

    // Verify the initial insert worked correctly
    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    assert_eq!(table_rows.len(), 1);

    let initial_row = &table_rows[0];
    let columns = initial_row.columns.as_ref().unwrap();

    // Verify the large text was inserted correctly
    let inserted_large_text: String = parse_table_cell(columns[1].clone()).unwrap();
    assert_eq!(inserted_large_text, large_text_value);

    let inserted_int: i32 = parse_table_cell(columns[2].clone()).unwrap();
    assert_eq!(inserted_int, initial_int_value);

    // Now attempt to update only the small_int column
    // With replica identity NONE, PostgreSQL should reject the update operation
    // because the table does not have sufficient replica identity information for updates
    let updated_int_value = 200;

    let result = database
        .update_values(table_name.clone(), &["small_int"], &[&updated_int_value])
        .await;

    // Verify that the update operation fails with the expected error
    assert!(
        result.is_err(),
        "Update should fail when replica identity is none"
    );
    let err = result.unwrap_err();
    let db_err = err.as_db_error().unwrap();

    let code = db_err.code().code();
    // 55000 error code is for object_not_in_prerequisite_state
    assert_eq!("55000", code);

    let error_message = db_err.message();
    assert_eq!(
        error_message,
        "cannot update table \"toast_values_test\" because it does not have a replica identity and publishes updates",
        "Expected replica identity error, got: {}",
        error_message
    );

    pipeline.shutdown_and_wait().await.unwrap();
}
