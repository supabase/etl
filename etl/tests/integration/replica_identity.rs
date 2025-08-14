use etl::destination::memory::MemoryDestination;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::materialize::{FromTableRow, materialize_events};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::types::{Cell, EventType, PipelineId};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use rand::distr::Alphanumeric;
use rand::{Rng, random};

/// Simple struct to represent a row in our toast test table
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ToastTable {
    pub id: i64,
    pub large_text: String,
    pub small_int: i32,
}

impl ToastTable {
    pub fn new(id: i64, large_text: &str, small_int: i32) -> Self {
        Self {
            id,
            large_text: large_text.to_owned(),
            small_int,
        }
    }
}

impl FromTableRow for ToastTable {
    type Id = i64;

    fn from_table_row(table_row: &etl::types::TableRow) -> Option<Self> {
        let values = &table_row.values;
        if values.len() == 3 {
            let id = match &values[0] {
                Cell::I64(i) => *i,
                _other => {
                    return None;
                }
            };
            let large_text = match &values[1] {
                Cell::String(s) => s.clone(),
                Cell::Null => String::new(),
                _other => {
                    return None;
                }
            };
            let small_int = match &values[2] {
                Cell::I32(i) => *i,
                _other => {
                    return None;
                }
            };

            Some(ToastTable::new(id, &large_text, small_int))
        } else {
            None
        }
    }

    fn id(&self) -> Self::Id {
        self.id
    }
}

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

const LARGE_TEXT_SIZE_BYTES: usize = 8192;

/// Tests that TOAST values are replaced with default values when updating non-TOAST columns
/// with default replica identity. When a table uses default replica identity (primary key)
/// and non-TOAST columns are updated, PostgreSQL sends UnchangedToast for TOAST values.
/// We send a default to the destination for the missing values .
#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_default_replica_identity() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text not null"), // Column that will contain TOAST-able data
                ("small_int", "int4 not null"),  // Column we'll update to test TOAST behavior
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
    let memory_destination = MemoryDestination::new();
    let destination = TestDestinationWrapper::wrap(memory_destination);

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
    let large_text_value = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
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
    let parsed_table_rows =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows.len(), 1);

    let expected_initial_row = ToastTable::new(1, &large_text_value, initial_int_value);
    assert_eq!(parsed_table_rows, vec![expected_initial_row]);

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
    let parsed_table_rows_after_update =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows_after_update.len(), 1);

    let expected_updated_row = ToastTable::new(1, "", updated_int_value);
    assert_eq!(parsed_table_rows_after_update, vec![expected_updated_row]);

    pipeline.shutdown_and_wait().await.unwrap();
}

/// Tests that TOAST values are preserved when updating non-TOAST columns with full replica identity.
/// When a table uses full replica identity and non-TOAST columns are updated, PostgreSQL includes
/// all column values in the update event. We send these values to the destination.
#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_full_replica_identity() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text not null"), // Column that will contain TOAST-able data
                ("small_int", "int4 not null"),  // Column we'll update to test TOAST behavior
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
    let memory_destination = MemoryDestination::new();
    let destination = TestDestinationWrapper::wrap(memory_destination);

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
    let large_text_value = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
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
    let parsed_table_rows =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows.len(), 1);

    let expected_initial_row = ToastTable::new(1, &large_text_value, initial_int_value);
    assert_eq!(parsed_table_rows, vec![expected_initial_row]);

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
    let parsed_table_rows_after_update =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows_after_update.len(), 1);

    let expected_updated_row = ToastTable::new(1, &large_text_value, updated_int_value);
    assert_eq!(parsed_table_rows_after_update, vec![expected_updated_row]);

    pipeline.shutdown_and_wait().await.unwrap();
}

/// Tests that TOAST values are correctly updated when directly modifying TOAST columns
/// with default replica identity. When updating the TOAST column itself, PostgreSQL
/// sends the new value regardless of replica identity settings, ensuring the destination
/// receives and applies the updated TOAST data correctly.
#[tokio::test(flavor = "multi_thread")]
async fn update_toast_values_with_default_replica_identity() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text not null"), // Column that will contain TOAST-able data
                ("small_int", "int4 not null"),  // Column we'll update to test TOAST behavior
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
    let memory_destination = MemoryDestination::new();
    let destination = TestDestinationWrapper::wrap(memory_destination);

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
    let large_text_value = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
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
    let parsed_table_rows =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows.len(), 1);

    let expected_initial_row = ToastTable::new(1, &large_text_value, initial_int_value);
    assert_eq!(parsed_table_rows, vec![expected_initial_row]);

    // Test update to the toast column does set it to that value
    let update_event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    let updated_large_text_value = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
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
    let parsed_table_rows_after_update =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows_after_update.len(), 1);

    let expected_updated_row = ToastTable::new(1, &updated_large_text_value, initial_int_value);
    assert_eq!(parsed_table_rows_after_update, vec![expected_updated_row]);

    pipeline.shutdown_and_wait().await.unwrap();
}

/// Tests that PostgreSQL rejects update operations when replica identity is set to none.
/// When a table has replica identity none and is part of a publication that publishes updates,
/// PostgreSQL will reject update operations because it cannot identify which row to update
/// without sufficient replica identity information.
#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_none_replica_identity() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text not null"), // Column that will contain TOAST-able data
                ("small_int", "int4 not null"),  // Column we'll update to test TOAST behavior
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
    let memory_destination = MemoryDestination::new();
    let destination = TestDestinationWrapper::wrap(memory_destination);

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
    let large_text_value = generate_random_ascii_string(LARGE_TEXT_SIZE_BYTES);
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
    let parsed_table_rows =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows.len(), 1);

    let expected_initial_row = ToastTable::new(1, &large_text_value, initial_int_value);
    assert_eq!(parsed_table_rows, vec![expected_initial_row]);

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
        "Expected replica identity error, got: {error_message}"
    );

    pipeline.shutdown_and_wait().await.unwrap();
}

/// Tests that TOAST values are replaced with default values when the table uses
/// replica identity with a unique index that includes columns being updated.
/// When updating columns that are part of the replica identity index, PostgreSQL sends
/// UnchangedToast for large TOAST values. We send a default to the destination for the
/// missing values .
#[tokio::test(flavor = "multi_thread")]
async fn update_non_toast_values_with_unique_index_replica_identity() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("toast_values_test");

    // Create a table with a text column that will be large enough to trigger TOAST
    // and a regular integer column for testing updates
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("large_text", "text not null"), // Column that will contain TOAST-able data
                ("small_int", "int4 not null"),  // Column we'll update to test TOAST behavior
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

    // Create a unique index on id and small_int columns
    let index_name = "unique_id_small_int_idx";
    let create_index_query = format!(
        "create unique index {index_name} on {} (id, small_int)",
        table_name.as_quoted_identifier()
    );
    database.run_sql(&create_index_query).await.unwrap();

    // Set replica identity to use the unique index
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::ReplicaIdentity {
                value: &format!("using index {index_name}"),
            }],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new();
    let destination = TestDestinationWrapper::wrap(memory_destination);

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
    let parsed_table_rows =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows.len(), 1);

    let expected_initial_row = ToastTable::new(1, &large_text_value, initial_int_value);
    assert_eq!(parsed_table_rows[0], expected_initial_row);

    // Now update the small_int column, which is part of the unique index used for replica identity
    // This should trigger TOAST behavior where PostgreSQL sends UnchangedToast
    // for the large_text column, since the replica identity includes the column being updated
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
    let parsed_table_rows_after_update =
        materialize_events::<ToastTable>(&destination.get_events().await, None).await;
    assert_eq!(parsed_table_rows_after_update.len(), 1);

    let expected_updated_row = ToastTable::new(1, "", updated_int_value);
    assert_eq!(parsed_table_rows_after_update[0], expected_updated_row);

    pipeline.shutdown_and_wait().await.unwrap();
}
