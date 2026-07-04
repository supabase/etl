#![cfg(feature = "test-utils")]

use std::collections::HashMap;

use etl::{
    destination::{DestinationTableMetadata, DestinationWriteStreamState},
    error::ErrorKind,
    etl_error,
    schema::{ColumnSchema, ReplicationMask, SnapshotId, TableId, TableName, TableSchema},
    store::{
        MemoryStore, PostgresStore, SchemaStore, StateStore, TableRetryPolicy,
        TableSchemaRetention, TableState, TableStateLifecycleStore, WorkerType,
    },
    test_utils::{database::spawn_source_database, notifying_store::NotifyingStore},
};
use etl_postgres::source::connect_to_source_database;
use etl_telemetry::tracing::init_test_tracing;
use sqlx::postgres::types::Oid as SqlxTableId;
use tokio_postgres::types::{PgLsn, Type as PgType};

/// Creates a test column schema with sensible defaults.
fn test_column(
    name: &str,
    typ: PgType,
    modifier: i32,
    ordinal_position: i32,
    nullable: bool,
    primary_key: bool,
) -> ColumnSchema {
    ColumnSchema::new(name.to_owned(), typ, modifier, ordinal_position, nullable)
        .with_primary_key_ordinal_position(if primary_key { Some(1) } else { None })
}

fn create_sample_table_schema() -> TableSchema {
    let table_id = TableId::new(12345);
    let table_name = TableName::new("public".to_owned(), "test_table".to_owned());
    let columns = vec![
        test_column("id", PgType::INT4, -1, 1, false, true),
        test_column("name", PgType::TEXT, -1, 2, true, false),
        test_column("created_at", PgType::TIMESTAMPTZ, -1, 3, false, false),
    ];

    TableSchema::new(table_id, table_name, columns)
}

fn create_another_table_schema() -> TableSchema {
    let table_id = TableId::new(67890);
    let table_name = TableName::new("public".to_owned(), "another_table".to_owned());
    let columns = vec![
        test_column("id", PgType::INT8, -1, 1, false, true),
        test_column("description", PgType::VARCHAR, 255, 2, true, false),
    ];

    TableSchema::new(table_id, table_name, columns)
}

#[tokio::test(flavor = "multi_thread")]
async fn state_store_operations() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Test initial state - should be empty
    let state = store.get_table_state(table_id).await.unwrap();
    assert!(state.is_none());

    let all_states = store.get_table_states().await.unwrap();
    assert!(all_states.is_empty());

    // Test updating state
    let init_state = TableState::Init;
    store.update_table_state(table_id, init_state.clone()).await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(init_state.clone()));

    let all_states = store.get_table_states().await.unwrap();
    assert_eq!(all_states.len(), 1);
    assert_eq!(all_states.get(&table_id), Some(&init_state));

    // Test updating to a different state
    let data_sync_state = TableState::DataSync;
    store.update_table_state(table_id, data_sync_state.clone()).await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(data_sync_state.clone()));

    // Test SyncDone state with LSN
    let lsn = "0/1000000".parse::<PgLsn>().unwrap();
    let sync_done_state = TableState::SyncDone { lsn };
    store.update_table_state(table_id, sync_done_state.clone()).await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(sync_done_state));

    // Test Errored state with retry policy
    let errored_state = TableState::Errored {
        reason: "Test error".to_owned(),
        solution: Some("Test solution".to_owned()),
        retry_policy: TableRetryPolicy::ManualRetry,
        source_err: etl_error!(ErrorKind::Unknown, "Test error"),
    };
    store.update_table_state(table_id, errored_state.clone()).await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(errored_state));
}

#[tokio::test(flavor = "multi_thread")]
async fn state_store_rollback() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Set initial state
    let init_state = TableState::Init;
    store.update_table_state(table_id, init_state.clone()).await.unwrap();

    // Update to a different state
    let data_sync_state = TableState::DataSync;
    store.update_table_state(table_id, data_sync_state.clone()).await.unwrap();

    // Verify two rows exist before rollback (init + data_sync)
    let pool = connect_to_source_database(&database.config, 0, 1, None)
        .await
        .expect("Failed to connect to source database with sqlx");
    let count_before: i64 = sqlx::query_scalar(
        "select count(*) from etl.replication_state where pipeline_id = $1 and table_id = $2",
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count_before, 2);

    // Verify current state
    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(data_sync_state));

    // Rollback to previous state
    let rolled_back_state = store.rollback_table_state(table_id).await.unwrap();
    assert_eq!(rolled_back_state, init_state);

    // Verify state was rolled back
    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(init_state));

    // Verify the rolled-from row was deleted to avoid buildup
    let count_after: i64 = sqlx::query_scalar(
        "select count(*) from etl.replication_state where pipeline_id = $1 and table_id = $2",
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count_after, 1);

    // Test rollback when there's no previous state
    let result = store.rollback_table_state(table_id).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn state_store_load_states() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id1 = TableId::new(12345);
    let table_id2 = TableId::new(67890);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Add some states directly to the database
    let init_state = TableState::Init;
    let data_sync_state = TableState::DataSync;

    store.update_table_state(table_id1, init_state.clone()).await.unwrap();
    store.update_table_state(table_id2, data_sync_state.clone()).await.unwrap();

    // Create a new store instance (simulating restart)
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Initially empty (not loaded yet)
    let states = new_store.get_table_states().await.unwrap();
    assert!(states.is_empty());

    // Load states from database
    let loaded_count = new_store.load_table_states().await.unwrap();
    assert_eq!(loaded_count, 2);

    // Verify loaded states
    let states = new_store.get_table_states().await.unwrap();
    assert_eq!(states.len(), 2);
    assert_eq!(states.get(&table_id1), Some(&init_state));
    assert_eq!(states.get(&table_id2), Some(&data_sync_state));
}

#[tokio::test(flavor = "multi_thread")]
async fn state_store_replication_progress_is_monotonic() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let apply_worker = WorkerType::Apply;
    let table_sync_worker = WorkerType::TableSync { table_id };

    assert_eq!(store.get_replication_progress(apply_worker).await.unwrap(), None);

    let first_lsn = PgLsn::from(100u64);
    let stale_lsn = PgLsn::from(90u64);
    let later_lsn = PgLsn::from(120u64);

    assert_eq!(
        store.upsert_replication_progress(apply_worker, first_lsn).await.unwrap(),
        first_lsn
    );
    assert_eq!(
        store.upsert_replication_progress(apply_worker, stale_lsn).await.unwrap(),
        first_lsn
    );
    assert_eq!(
        store.upsert_replication_progress(apply_worker, later_lsn).await.unwrap(),
        later_lsn
    );
    assert_eq!(store.get_replication_progress(apply_worker).await.unwrap(), Some(later_lsn));

    let table_sync_lsn = PgLsn::from(75u64);
    assert_eq!(
        store.upsert_replication_progress(table_sync_worker, table_sync_lsn).await.unwrap(),
        table_sync_lsn
    );
    assert_eq!(
        store.get_replication_progress(table_sync_worker).await.unwrap(),
        Some(table_sync_lsn)
    );
    assert_eq!(store.get_replication_progress(apply_worker).await.unwrap(), Some(later_lsn));

    store.delete_replication_progress(table_sync_worker).await.unwrap();
    assert_eq!(store.get_replication_progress(table_sync_worker).await.unwrap(), None);
    assert_eq!(store.get_replication_progress(apply_worker).await.unwrap(), Some(later_lsn));
}

#[tokio::test(flavor = "multi_thread")]
async fn state_store_destination_write_stream_state_persists_and_clears() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);
    let other_table_id = TableId::new(67890);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let stream_state = DestinationWriteStreamState::new(
        "dest_table_0001".to_owned(),
        "projects/p/datasets/d/tables/dest_table_0001/streams/s1".to_owned(),
        42,
    );
    let other_stream_state = DestinationWriteStreamState::new(
        "other_dest_table_0001".to_owned(),
        "projects/p/datasets/d/tables/other_dest_table_0001/streams/s2".to_owned(),
        7,
    );

    assert!(
        store
            .get_destination_write_stream_state(table_id, stream_state.destination_table_id.clone())
            .await
            .unwrap()
            .is_none()
    );

    store.store_destination_write_stream_state(table_id, stream_state.clone()).await.unwrap();
    store
        .store_destination_write_stream_state(other_table_id, other_stream_state.clone())
        .await
        .unwrap();

    let loaded = store
        .get_destination_write_stream_state(table_id, stream_state.destination_table_id.clone())
        .await
        .unwrap()
        .expect("stream state should be stored");
    assert_eq!(loaded, stream_state);

    let advanced = stream_state.advanced_to(
        stream_state.next_offset + 3,
        "0000000000000002/0000000000000000/0000000000000000".to_owned(),
    );
    store.store_destination_write_stream_state(table_id, advanced.clone()).await.unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(advanced.clone())
    );

    let stale = DestinationWriteStreamState::new(
        advanced.destination_table_id.clone(),
        "projects/p/datasets/d/tables/dest_table_0001/streams/stale".to_owned(),
        advanced.next_offset - 1,
    );
    store.store_destination_write_stream_state(table_id, stale).await.unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(advanced.clone())
    );

    let same_offset_different_stream = DestinationWriteStreamState::new(
        advanced.destination_table_id.clone(),
        "projects/p/datasets/d/tables/dest_table_0001/streams/same_offset_stale".to_owned(),
        advanced.next_offset,
    );
    store
        .store_destination_write_stream_state(table_id, same_offset_different_stream)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(advanced.clone())
    );

    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    assert_eq!(
        new_store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(advanced)
    );

    new_store.prepare_table_state_for_copy(table_id).await.unwrap();
    assert!(
        new_store
            .get_destination_write_stream_state(table_id, stream_state.destination_table_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        new_store
            .get_destination_write_stream_state(
                other_table_id,
                other_stream_state.destination_table_id.clone()
            )
            .await
            .unwrap(),
        Some(other_stream_state)
    );
}

async fn assert_destination_write_stream_state_is_monotonic<S>(store: S)
where
    S: StateStore,
{
    let table_id = TableId::new(12345);
    let stream_state = DestinationWriteStreamState::new(
        "dest_table_0001".to_owned(),
        "projects/p/datasets/d/tables/dest_table_0001/streams/s1".to_owned(),
        42,
    );
    let advanced = stream_state.advanced_to(
        stream_state.next_offset + 3,
        "0000000000000002/0000000000000000/0000000000000000".to_owned(),
    );

    store.store_destination_write_stream_state(table_id, advanced.clone()).await.unwrap();
    store.store_destination_write_stream_state(table_id, stream_state).await.unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(advanced.clone())
    );

    let same_offset_lower_sequence = DestinationWriteStreamState::new(
        advanced.destination_table_id.clone(),
        advanced.stream_name.clone(),
        advanced.next_offset,
    )
    .with_last_sequence_number(Some(
        "0000000000000001/0000000000000000/0000000000000000".to_owned(),
    ));
    store
        .store_destination_write_stream_state(table_id, same_offset_lower_sequence)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(advanced.clone())
    );

    let same_offset_higher_sequence = DestinationWriteStreamState::new(
        advanced.destination_table_id.clone(),
        advanced.stream_name.clone(),
        advanced.next_offset,
    )
    .with_last_sequence_number(Some(
        "0000000000000003/0000000000000000/0000000000000000".to_owned(),
    ));
    store
        .store_destination_write_stream_state(table_id, same_offset_higher_sequence.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(same_offset_higher_sequence.clone())
    );

    let same_offset_different_stream = DestinationWriteStreamState::new(
        same_offset_higher_sequence.destination_table_id.clone(),
        "projects/p/datasets/d/tables/dest_table_0001/streams/same_offset_stale".to_owned(),
        same_offset_higher_sequence.next_offset,
    );
    store
        .store_destination_write_stream_state(table_id, same_offset_different_stream)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_destination_write_stream_state(table_id, advanced.destination_table_id.clone())
            .await
            .unwrap(),
        Some(same_offset_higher_sequence)
    );
}

#[tokio::test]
async fn memory_store_destination_write_stream_state_is_monotonic() {
    assert_destination_write_stream_state_is_monotonic(MemoryStore::new()).await;
}

#[tokio::test]
async fn notifying_store_destination_write_stream_state_is_monotonic() {
    assert_destination_write_stream_state_is_monotonic(NotifyingStore::new()).await;
}

async fn assert_reset_for_resync_clears_destination_write_stream_state<S>(store: S)
where
    S: StateStore + TableStateLifecycleStore,
{
    let table_id = TableId::new(12345);
    let stream_state = DestinationWriteStreamState::new(
        "dest_table_0001".to_owned(),
        "projects/p/datasets/d/tables/dest_table_0001/streams/s1".to_owned(),
        42,
    );

    store.store_destination_write_stream_state(table_id, stream_state.clone()).await.unwrap();
    assert!(
        store
            .get_destination_write_stream_state(table_id, stream_state.destination_table_id.clone())
            .await
            .unwrap()
            .is_some()
    );

    store.reset_table_states_for_resync().await.unwrap();
    assert!(
        store
            .get_destination_write_stream_state(table_id, stream_state.destination_table_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn memory_store_reset_for_resync_clears_destination_write_stream_state() {
    assert_reset_for_resync_clears_destination_write_stream_state(MemoryStore::new()).await;
}

#[tokio::test]
async fn notifying_store_reset_for_resync_clears_destination_write_stream_state() {
    assert_reset_for_resync_clears_destination_write_stream_state(NotifyingStore::new()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_operations() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let table_schema = create_sample_table_schema();
    let table_id = table_schema.id;

    // Test initial state - should be empty
    let schema = store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap();
    assert!(schema.is_none());

    let all_schemas = store.get_table_schemas().await.unwrap();
    assert!(all_schemas.is_empty());

    // Test storing schema
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let schema = store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.id, table_schema.id);
    assert_eq!(schema.name, table_schema.name);
    assert_eq!(schema.column_schemas.len(), table_schema.column_schemas.len());

    let all_schemas = store.get_table_schemas().await.unwrap();
    assert_eq!(all_schemas.len(), 1);

    // Test storing another schema
    let table_schema2 = create_another_table_schema();
    store.store_table_schema(table_schema2.clone()).await.unwrap();

    let all_schemas = store.get_table_schemas().await.unwrap();
    assert_eq!(all_schemas.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_load_schemas() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let table_schema1 = create_sample_table_schema();
    let table_schema2 = create_another_table_schema();

    // Store schemas
    store.store_table_schema(table_schema1.clone()).await.unwrap();
    store.store_table_schema(table_schema2.clone()).await.unwrap();

    // Create a new store instance (simulating restart)
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Initially empty (not loaded yet)
    let schemas = new_store.get_table_schemas().await.unwrap();
    assert!(schemas.is_empty());

    // Load schemas from database
    let loaded_count = new_store.load_table_schemas().await.unwrap();
    assert_eq!(loaded_count, 2);

    // Verify loaded schemas
    let schemas = new_store.get_table_schemas().await.unwrap();
    assert_eq!(schemas.len(), 2);

    let schema1 = new_store.get_table_schema(&table_schema1.id, SnapshotId::max()).await.unwrap();
    assert!(schema1.is_some());
    let schema1 = schema1.unwrap();
    assert_eq!(schema1.id, table_schema1.id);
    assert_eq!(schema1.name, table_schema1.name);

    let schema2 = new_store.get_table_schema(&table_schema2.id, SnapshotId::max()).await.unwrap();
    assert!(schema2.is_some());
    let schema2 = schema2.unwrap();
    assert_eq!(schema2.id, table_schema2.id);
    assert_eq!(schema2.name, table_schema2.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_versioning() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let mut table_schema = create_sample_table_schema();

    // Store initial schema at snapshot 0
    store.store_table_schema(table_schema.clone()).await.unwrap();

    // Create a new version with a higher snapshot_id
    table_schema.add_column_schema(test_column(
        "updated_at",
        PgType::TIMESTAMPTZ,
        -1,
        4,
        true,
        false,
    ));
    table_schema.snapshot_id = SnapshotId::from(100u64); // New snapshot for the schema change

    // Store updated schema as new version
    store.store_table_schema(table_schema.clone()).await.unwrap();

    // Verify querying at snapshot 100+ returns the updated schema
    let schema = store.get_table_schema(&table_schema.id, SnapshotId::max()).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_schemas.len(), 4); // Original 3 + 1 new column
    assert_eq!(schema.snapshot_id, SnapshotId::from(100u64));

    // Verify querying at snapshot 50 returns the original schema
    let schema = store.get_table_schema(&table_schema.id, SnapshotId::from(50u64)).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_schemas.len(), 3); // Original 3 columns
    assert_eq!(schema.snapshot_id, SnapshotId::initial());

    // Verify the new column was added in the latest version
    let schema =
        store.get_table_schema(&table_schema.id, SnapshotId::max()).await.unwrap().unwrap();
    let updated_at_column = schema.column_schemas.iter().find(|c| c.name == "updated_at");
    assert!(updated_at_column.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_upsert_replaces_columns() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Create initial schema with 3 columns
    let table_id = TableId::new(12345);
    let table_name = TableName::new("public".to_owned(), "test_table".to_owned());
    let initial_columns = vec![
        test_column("id", PgType::INT4, -1, 1, false, true),
        test_column("name", PgType::TEXT, -1, 2, true, false),
        test_column("old_column", PgType::TEXT, -1, 3, true, false),
    ];
    let table_schema = TableSchema::new(table_id, table_name.clone(), initial_columns);

    // Store initial schema
    store.store_table_schema(table_schema.clone()).await.unwrap();

    // Verify initial columns
    let schema = store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();
    assert_eq!(schema.column_schemas.len(), 3);
    assert!(schema.column_schemas.iter().any(|c| c.name == "old_column"));

    // Create updated schema with SAME snapshot_id but different columns
    // (simulating a retry or re-processing scenario)
    let updated_columns = vec![
        test_column("id", PgType::INT4, -1, 1, false, true),
        test_column("name", PgType::TEXT, -1, 2, true, false),
        test_column("new_column", PgType::TEXT, -1, 3, true, false), // replaced old_column
        test_column("extra_column", PgType::INT8, -1, 4, true, false), // added column
    ];
    let updated_schema = TableSchema::new(table_id, table_name, updated_columns);

    // Store updated schema with same snapshot_id (upsert)
    store.store_table_schema(updated_schema.clone()).await.unwrap();

    // Verify columns were replaced, not accumulated
    // Need to clear cache and reload from DB to verify DB state
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let schema = new_store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();

    assert_eq!(schema.column_schemas.len(), 4); // Should be 4, not 3+4=7
    assert!(
        !schema.column_schemas.iter().any(|c| c.name == "old_column"),
        "old_column should have been deleted"
    );
    assert!(
        schema.column_schemas.iter().any(|c| c.name == "new_column"),
        "new_column should exist"
    );
    assert!(
        schema.column_schemas.iter().any(|c| c.name == "extra_column"),
        "extra_column should exist"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_cache_eviction() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Store 3 schema versions for table 1
    let table_id_1 = TableId::new(12345);
    let table_name_1 = TableName::new("public".to_owned(), "test_table".to_owned());
    for snapshot_id in [0u64, 100, 200] {
        let columns = vec![
            test_column("id", PgType::INT4, -1, 1, false, true),
            test_column(&format!("col_at_{snapshot_id}"), PgType::TEXT, -1, 2, true, false),
        ];
        let mut table_schema = TableSchema::new(table_id_1, table_name_1.clone(), columns);
        table_schema.snapshot_id = SnapshotId::from(snapshot_id);
        store.store_table_schema(table_schema.clone()).await.unwrap();
    }

    // Store 3 schemas for table 2 to verify eviction is per-table
    let table_id_2 = TableId::new(67890);
    let table_name_2 = TableName::new("public".to_owned(), "table_2".to_owned());
    for snapshot_id in [0u64, 100, 200] {
        let columns = vec![test_column("id", PgType::INT4, -1, 1, false, true)];
        let mut schema = TableSchema::new(table_id_2, table_name_2.clone(), columns);
        schema.snapshot_id = SnapshotId::from(snapshot_id);
        store.store_table_schema(schema).await.unwrap();
    }

    // Check cache size - should have 2 schemas per table = 4 total
    let cached_schemas = store.get_table_schemas().await.unwrap();
    assert_eq!(cached_schemas.len(), 4, "Should have 2 schemas per table");

    // Verify eviction keeps newest snapshots (100 and 200), evicts oldest (0)
    let table_1_snapshots: Vec<SnapshotId> =
        cached_schemas.iter().filter(|s| s.id == table_id_1).map(|s| s.snapshot_id).collect();
    assert!(
        table_1_snapshots.contains(&SnapshotId::from(100u64))
            && table_1_snapshots.contains(&SnapshotId::from(200u64))
    );
    assert!(!table_1_snapshots.contains(&SnapshotId::initial()), "Snapshot 0 should be evicted");

    let table_2_snapshots: Vec<SnapshotId> =
        cached_schemas.iter().filter(|s| s.id == table_id_2).map(|s| s.snapshot_id).collect();
    assert!(
        !table_2_snapshots.contains(&SnapshotId::initial()),
        "Table 2 snapshot 0 should be evicted"
    );

    // Evicted schemas should still be loadable from DB
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let schema_0 =
        new_store.get_table_schema(&table_id_1, SnapshotId::initial()).await.unwrap().unwrap();
    assert_eq!(schema_0.snapshot_id, SnapshotId::initial());
    assert!(schema_0.column_schemas.iter().any(|c| c.name == "col_at_0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_prunes_obsolete_versions_from_database_and_cache() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let table_id = TableId::new(12345);
    let table_name = TableName::new("public".to_owned(), "test_table".to_owned());

    for snapshot_id in [0u64, 100, 200, 300] {
        let columns = vec![
            test_column("id", PgType::INT4, -1, 1, false, true),
            test_column(&format!("col_at_{snapshot_id}"), PgType::TEXT, -1, 2, true, false),
        ];
        let mut table_schema = TableSchema::new(table_id, table_name.clone(), columns);
        table_schema.snapshot_id = SnapshotId::from(snapshot_id);
        store.store_table_schema(table_schema).await.unwrap();
    }

    let other_table_id = TableId::new(67890);
    let other_table_name = TableName::new("public".to_owned(), "other_table".to_owned());
    for snapshot_id in [0u64, 150] {
        let columns = vec![
            test_column("id", PgType::INT4, -1, 1, false, true),
            test_column(&format!("other_col_at_{snapshot_id}"), PgType::TEXT, -1, 2, true, false),
        ];
        let mut table_schema = TableSchema::new(other_table_id, other_table_name.clone(), columns);
        table_schema.snapshot_id = SnapshotId::from(snapshot_id);
        store.store_table_schema(table_schema).await.unwrap();
    }

    let untouched_table_id = TableId::new(24680);
    let untouched_table_name = TableName::new("public".to_owned(), "untouched_table".to_owned());
    for snapshot_id in [0u64, 50] {
        let columns = vec![
            test_column("id", PgType::INT4, -1, 1, false, true),
            test_column(
                &format!("untouched_col_at_{snapshot_id}"),
                PgType::TEXT,
                -1,
                2,
                true,
                false,
            ),
        ];
        let mut table_schema =
            TableSchema::new(untouched_table_id, untouched_table_name.clone(), columns);
        table_schema.snapshot_id = SnapshotId::from(snapshot_id);
        store.store_table_schema(table_schema).await.unwrap();
    }

    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    let obsolete_schema_ids: Vec<i64> = sqlx::query_scalar(
        r#"
        select id
        from etl.table_schemas
        where pipeline_id = $1
          and (
              (table_id = $2 and snapshot_id < $3::pg_lsn)
              or (table_id = $4 and snapshot_id < $5::pg_lsn)
          )
        "#,
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(SnapshotId::from(200u64).to_pg_lsn_string())
    .bind(SqlxTableId(other_table_id.into_inner()))
    .bind(SnapshotId::from(150u64).to_pg_lsn_string())
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(obsolete_schema_ids.len(), 3);

    let obsolete_column_count_before: i64 = sqlx::query_scalar(
        "select count(*) from etl.table_columns where table_schema_id = any($1)",
    )
    .bind(&obsolete_schema_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(obsolete_column_count_before > 0);

    let deleted = store
        .prune_table_schemas(HashMap::from([
            (table_id, TableSchemaRetention::SnapshotId(SnapshotId::from(200u64))),
            (other_table_id, TableSchemaRetention::SnapshotId(SnapshotId::from(200u64))),
        ]))
        .await
        .unwrap();
    assert_eq!(deleted, 3);

    let cached_schemas = store.get_table_schemas().await.unwrap();
    let table_snapshots: Vec<_> =
        cached_schemas.iter().filter(|schema| schema.id == table_id).collect();
    assert_eq!(table_snapshots.len(), 2);
    assert!(table_snapshots.iter().any(|schema| schema.snapshot_id == SnapshotId::from(200u64)));
    assert!(table_snapshots.iter().any(|schema| schema.snapshot_id == SnapshotId::from(300u64)));

    let other_table_snapshots: Vec<_> =
        cached_schemas.iter().filter(|schema| schema.id == other_table_id).collect();
    assert_eq!(other_table_snapshots.len(), 1);
    assert_eq!(other_table_snapshots[0].snapshot_id, SnapshotId::from(150u64));

    let untouched_table_snapshots: Vec<_> =
        cached_schemas.iter().filter(|schema| schema.id == untouched_table_id).collect();
    assert_eq!(untouched_table_snapshots.len(), 2);

    let schema_count: i64 = sqlx::query_scalar(
        "select count(*) from etl.table_schemas where pipeline_id = $1 and table_id = $2",
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(schema_count, 2);

    let untouched_schema_count: i64 = sqlx::query_scalar(
        "select count(*) from etl.table_schemas where pipeline_id = $1 and table_id = $2",
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(untouched_table_id.into_inner()))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(untouched_schema_count, 2);

    let obsolete_column_count_after: i64 = sqlx::query_scalar(
        "select count(*) from etl.table_columns where table_schema_id = any($1)",
    )
    .bind(&obsolete_schema_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(obsolete_column_count_after, 0);

    let old_schema = store.get_table_schema(&table_id, SnapshotId::from(100u64)).await.unwrap();
    assert!(old_schema.is_none());

    let retained_schema =
        store.get_table_schema(&table_id, SnapshotId::from(250u64)).await.unwrap().unwrap();
    assert_eq!(retained_schema.snapshot_id, SnapshotId::from(200u64));

    let latest_schema =
        store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();
    assert_eq!(latest_schema.snapshot_id, SnapshotId::from(300u64));
}

#[tokio::test(flavor = "multi_thread")]
async fn multiple_pipelines_isolation() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id1 = 1;
    let pipeline_id2 = 2;
    let table_id = TableId::new(12345);

    let store1 = PostgresStore::new(pipeline_id1, database.config.clone()).await.unwrap();
    let store2 = PostgresStore::new(pipeline_id2, database.config.clone()).await.unwrap();

    // Test state isolation
    let init_state = TableState::Init;
    store1.update_table_state(table_id, init_state.clone()).await.unwrap();

    let data_sync_state = TableState::DataSync;
    store2.update_table_state(table_id, data_sync_state.clone()).await.unwrap();

    assert_eq!(store1.get_table_state(table_id).await.unwrap(), Some(init_state));
    assert_eq!(store2.get_table_state(table_id).await.unwrap(), Some(data_sync_state));

    // Test schema isolation
    let table_schema1 = create_sample_table_schema();
    let table_schema2 = create_another_table_schema();

    store1.store_table_schema(table_schema1.clone()).await.unwrap();
    store2.store_table_schema(table_schema2.clone()).await.unwrap();

    let schemas1 = store1.get_table_schemas().await.unwrap();
    assert_eq!(schemas1.len(), 1);
    assert_eq!(schemas1[0].id, table_schema1.id);

    let schemas2 = store2.get_table_schemas().await.unwrap();
    assert_eq!(schemas2.len(), 1);
    assert_eq!(schemas2[0].id, table_schema2.id);

    // Test destination table metadata isolation.
    let metadata1 = DestinationTableMetadata::new_applied(
        "pipeline1_table".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1, 1]),
    );
    let metadata2 = DestinationTableMetadata::new_applied(
        "pipeline2_table".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1, 1]),
    );

    store1.store_destination_table_metadata(table_id, metadata1.clone()).await.unwrap();
    store2.store_destination_table_metadata(table_id, metadata2.clone()).await.unwrap();

    assert_eq!(
        store1
            .get_applied_destination_table_metadata(table_id)
            .await
            .unwrap()
            .map(|m| m.destination_table_id),
        Some("pipeline1_table".to_owned())
    );
    assert_eq!(
        store2
            .get_applied_destination_table_metadata(table_id)
            .await
            .unwrap()
            .map(|m| m.destination_table_id),
        Some("pipeline2_table".to_owned())
    );

    // Verify isolation persists after loading from database
    let new_store1 = PostgresStore::new(pipeline_id1, database.config.clone()).await.unwrap();
    new_store1.load_destination_tables_metadata().await.unwrap();
    assert_eq!(
        new_store1
            .get_applied_destination_table_metadata(table_id)
            .await
            .unwrap()
            .map(|m| m.destination_table_id),
        Some("pipeline1_table".to_owned())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn errored_state_with_different_retry_policies() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Test Errored state with NoRetry policy
    let errored_no_retry = TableState::Errored {
        reason: "Fatal error".to_owned(),
        solution: None,
        retry_policy: TableRetryPolicy::NoRetry,
        source_err: etl_error!(ErrorKind::Unknown, "Test error"),
    };
    store.update_table_state(table_id, errored_no_retry.clone()).await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(errored_no_retry));

    // Test Errored state with TimedRetry policy
    let next_retry = chrono::Utc::now() + chrono::Duration::minutes(5);
    let errored_timed_retry = TableState::Errored {
        reason: "Temporary error".to_owned(),
        solution: Some("Wait and retry".to_owned()),
        retry_policy: TableRetryPolicy::TimedRetry { next_retry },
        source_err: etl_error!(ErrorKind::Unknown, "Test error"),
    };
    store.update_table_state(table_id, errored_timed_retry.clone()).await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(errored_timed_retry));
}

#[tokio::test(flavor = "multi_thread")]
async fn state_transitions_and_history() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Create a series of state transitions
    let init_state = TableState::Init;
    store.update_table_state(table_id, init_state.clone()).await.unwrap();

    let data_sync_state = TableState::DataSync;
    store.update_table_state(table_id, data_sync_state.clone()).await.unwrap();

    let finished_copy_state = TableState::FinishedCopy;
    store.update_table_state(table_id, finished_copy_state.clone()).await.unwrap();

    let lsn = "0/2000000".parse::<PgLsn>().unwrap();
    let sync_done_state = TableState::SyncDone { lsn };
    store.update_table_state(table_id, sync_done_state.clone()).await.unwrap();

    let ready_state = TableState::Ready;
    store.update_table_state(table_id, ready_state.clone()).await.unwrap();

    // Verify final state
    let state = store.get_table_state(table_id).await.unwrap();
    assert_eq!(state, Some(ready_state));

    // Test rollback through the history
    let rolled_back_state = store.rollback_table_state(table_id).await.unwrap();
    assert_eq!(rolled_back_state, sync_done_state);

    let rolled_back_state = store.rollback_table_state(table_id).await.unwrap();
    assert_eq!(rolled_back_state, finished_copy_state);

    let rolled_back_state = store.rollback_table_state(table_id).await.unwrap();
    assert_eq!(rolled_back_state, data_sync_state);

    let rolled_back_state = store.rollback_table_state(table_id).await.unwrap();
    assert_eq!(rolled_back_state, init_state);

    // No more rollbacks possible
    let result = store.rollback_table_state(table_id).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_table_state_deletes_state_schema_metadata_and_progress_for_table() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Test idempotency: deleting state for a non-existent table should succeed.
    let nonexistent_table_id = TableId::new(99999);
    store.delete_table_state(nonexistent_table_id).await.unwrap();

    // Prepare two tables: one we will delete, one we will keep.
    let table_1_schema = create_sample_table_schema();
    let table_1_id = table_1_schema.id;
    let table_2_schema = create_another_table_schema();
    let table_2_id = table_2_schema.id;

    // Populate state, schema, and metadata for both tables.
    store.update_table_state(table_1_id, TableState::Ready).await.unwrap();
    store.update_table_state(table_2_id, TableState::DataSync).await.unwrap();

    store.store_table_schema(table_1_schema.clone()).await.unwrap();
    store.store_table_schema(table_2_schema.clone()).await.unwrap();

    let metadata1 = DestinationTableMetadata::new_applied(
        "dest_table_1".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1, 1]),
    );
    let metadata2 = DestinationTableMetadata::new_applied(
        "dest_table_2".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1, 1]),
    );

    store.store_destination_table_metadata(table_1_id, metadata1).await.unwrap();
    store.store_destination_table_metadata(table_2_id, metadata2).await.unwrap();
    store
        .upsert_replication_progress(
            WorkerType::TableSync { table_id: table_1_id },
            PgLsn::from(200u64),
        )
        .await
        .unwrap();
    store
        .upsert_replication_progress(
            WorkerType::TableSync { table_id: table_2_id },
            PgLsn::from(300u64),
        )
        .await
        .unwrap();

    // Sanity check before deleting state.
    assert!(store.get_table_state(table_1_id).await.unwrap().is_some());
    assert!(store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_applied_destination_table_metadata(table_1_id).await.unwrap().is_some());
    assert!(
        store
            .get_replication_progress(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_some()
    );

    // Delete table state for table 1.
    store.delete_table_state(table_1_id).await.unwrap();

    // Verify in-memory cache for table 1 has been deleted.
    assert!(store.get_table_state(table_1_id).await.unwrap().is_none());
    assert!(store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(store.get_applied_destination_table_metadata(table_1_id).await.unwrap().is_none());
    assert!(
        store
            .get_replication_progress(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_none()
    );

    // Verify other table is unaffected.
    assert!(store.get_table_state(table_2_id).await.unwrap().is_some());
    assert!(store.get_table_schema(&table_2_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_applied_destination_table_metadata(table_2_id).await.unwrap().is_some());
    assert!(
        store
            .get_replication_progress(WorkerType::TableSync { table_id: table_2_id })
            .await
            .unwrap()
            .is_some()
    );

    // Create a new store instance and load from DB to ensure persistence.
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_table_states().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    // Table 1 should not be present after reload.
    assert!(new_store.get_table_state(table_1_id).await.unwrap().is_none());
    assert!(new_store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(new_store.get_applied_destination_table_metadata(table_1_id).await.unwrap().is_none());
    assert!(
        new_store
            .get_replication_progress(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_none()
    );

    // Table 2 should still be present.
    assert!(new_store.get_table_state(table_2_id).await.unwrap().is_some());
    assert!(new_store.get_table_schema(&table_2_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(new_store.get_applied_destination_table_metadata(table_2_id).await.unwrap().is_some());
    assert!(
        new_store
            .get_replication_progress(WorkerType::TableSync { table_id: table_2_id })
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn prepare_table_state_for_copy_preserves_state_and_deletes_copy_data() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Test idempotency: preparing copy state for a non-existent table should
    // succeed.
    let nonexistent_table_id = TableId::new(99999);
    store.prepare_table_state_for_copy(nonexistent_table_id).await.unwrap();

    let mut table_schema = create_sample_table_schema();
    let table_id = table_schema.id;
    let other_table_schema = create_another_table_schema();
    let other_table_id = other_table_schema.id;

    store.update_table_state(table_id, TableState::DataSync).await.unwrap();
    store.update_table_state(other_table_id, TableState::Ready).await.unwrap();

    table_schema.snapshot_id = SnapshotId::initial();
    store.store_table_schema(table_schema.clone()).await.unwrap();
    table_schema.snapshot_id = SnapshotId::from(100u64);
    store.store_table_schema(table_schema).await.unwrap();
    store.store_table_schema(other_table_schema).await.unwrap();

    let metadata = DestinationTableMetadata::new_applied(
        "dest_table".to_owned(),
        SnapshotId::from(100u64),
        ReplicationMask::from_bytes(vec![1, 1, 1]),
    );
    let other_metadata = DestinationTableMetadata::new_applied(
        "other_dest_table".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1]),
    );
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();
    store.store_destination_table_metadata(other_table_id, other_metadata).await.unwrap();
    store
        .upsert_replication_progress(WorkerType::TableSync { table_id }, PgLsn::from(200u64))
        .await
        .unwrap();

    store.prepare_table_state_for_copy(table_id).await.unwrap();

    assert_eq!(store.get_table_state(table_id).await.unwrap(), Some(TableState::DataSync));
    assert!(store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(store.get_applied_destination_table_metadata(table_id).await.unwrap().is_none());
    assert!(
        store.get_replication_progress(WorkerType::TableSync { table_id }).await.unwrap().is_none()
    );

    assert!(store.get_table_schema(&other_table_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_applied_destination_table_metadata(other_table_id).await.unwrap().is_some());

    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_table_states().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    assert_eq!(new_store.get_table_state(table_id).await.unwrap(), Some(TableState::DataSync));
    assert!(new_store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(new_store.get_applied_destination_table_metadata(table_id).await.unwrap().is_none());
    assert!(
        new_store
            .get_replication_progress(WorkerType::TableSync { table_id })
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        new_store.get_table_schema(&other_table_id, SnapshotId::max()).await.unwrap().is_some()
    );
    assert!(
        new_store.get_applied_destination_table_metadata(other_table_id).await.unwrap().is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_table_states_for_resync_resets_states_and_apply_progress_only() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    let table_1_schema = create_sample_table_schema();
    let table_1_id = table_1_schema.id;
    let table_2_schema = create_another_table_schema();
    let table_2_id = table_2_schema.id;

    store.update_table_state(table_1_id, TableState::Ready).await.unwrap();
    store.update_table_state(table_2_id, TableState::DataSync).await.unwrap();
    store.store_table_schema(table_1_schema).await.unwrap();
    store.store_table_schema(table_2_schema).await.unwrap();

    let metadata1 = DestinationTableMetadata::new_applied(
        "dest_table_1".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1, 1]),
    );
    let metadata2 = DestinationTableMetadata::new_applied(
        "dest_table_2".to_owned(),
        SnapshotId::initial(),
        ReplicationMask::from_bytes(vec![1, 1]),
    );
    store.store_destination_table_metadata(table_1_id, metadata1).await.unwrap();
    store.store_destination_table_metadata(table_2_id, metadata2).await.unwrap();
    store.upsert_replication_progress(WorkerType::Apply, PgLsn::from(500u64)).await.unwrap();
    store
        .upsert_replication_progress(
            WorkerType::TableSync { table_id: table_1_id },
            PgLsn::from(200u64),
        )
        .await
        .unwrap();
    store
        .upsert_replication_progress(
            WorkerType::TableSync { table_id: table_2_id },
            PgLsn::from(300u64),
        )
        .await
        .unwrap();

    let reset_count = store.reset_table_states_for_resync().await.unwrap();

    assert_eq!(reset_count, 2);
    assert_eq!(store.get_table_state(table_1_id).await.unwrap(), Some(TableState::Init));
    assert_eq!(store.get_table_state(table_2_id).await.unwrap(), Some(TableState::Init));
    assert!(store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_table_schema(&table_2_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_applied_destination_table_metadata(table_1_id).await.unwrap().is_some());
    assert!(store.get_applied_destination_table_metadata(table_2_id).await.unwrap().is_some());
    assert!(store.get_replication_progress(WorkerType::Apply).await.unwrap().is_none());
    assert!(
        store
            .get_replication_progress(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get_replication_progress(WorkerType::TableSync { table_id: table_2_id })
            .await
            .unwrap()
            .is_some()
    );

    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_table_states().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    assert_eq!(new_store.get_table_state(table_1_id).await.unwrap(), Some(TableState::Init));
    assert_eq!(new_store.get_table_state(table_2_id).await.unwrap(), Some(TableState::Init));
    assert!(new_store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(new_store.get_table_schema(&table_2_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(new_store.get_applied_destination_table_metadata(table_1_id).await.unwrap().is_some());
    assert!(new_store.get_applied_destination_table_metadata(table_2_id).await.unwrap().is_some());
    assert!(new_store.get_replication_progress(WorkerType::Apply).await.unwrap().is_none());
    assert!(
        new_store
            .get_replication_progress(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        new_store
            .get_replication_progress(WorkerType::TableSync { table_id: table_2_id })
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_mask_loads_correctly_from_string_bytea() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);
    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    let pool = connect_to_source_database(&database.config, 0, 1, None)
        .await
        .expect("Failed to connect to source database with sqlx");

    // Manually insert a row with a specific replication mask bytea.
    // The mask [1, 0, 1, 1, 0] represents columns: replicated, not replicated,
    // replicated, replicated, not replicated.
    let expected_mask_bytes: Vec<u8> = vec![1, 0, 1, 1, 0];

    sqlx::query(
        r#"
        INSERT INTO etl.destination_tables_metadata
            (pipeline_id, table_id, destination_table_id, snapshot_id, schema_status, replication_mask)
        VALUES ($1, $2, 'test_dest_table', '0/0'::pg_lsn, 'applied', $3::bytea)
        "#,
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(&expected_mask_bytes)
    .execute(&pool)
    .await
    .unwrap();

    // Load metadata using the store.
    store.load_destination_tables_metadata().await.unwrap();

    // Verify the loaded replication mask matches what was inserted
    let metadata = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("Metadata should exist");

    assert_eq!(
        metadata.replication_mask.as_slice(),
        &expected_mask_bytes,
        "Loaded replication mask should match inserted bytea"
    );
    assert_eq!(metadata.destination_table_id, "test_dest_table");
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_mask_various_patterns() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    let pool = connect_to_source_database(&database.config, 0, 1, None)
        .await
        .expect("Failed to connect to source database with sqlx");

    // Test various mask patterns
    let test_cases: Vec<(TableId, &str, Vec<u8>)> = vec![
        // All columns replicated
        (TableId::new(1001), "all_ones", vec![1, 1, 1, 1, 1]),
        // No columns replicated
        (TableId::new(1002), "all_zeros", vec![0, 0, 0, 0]),
        // Single column replicated
        (TableId::new(1003), "single_one", vec![1]),
        // Alternating pattern
        (TableId::new(1004), "alternating", vec![1, 0, 1, 0, 1, 0]),
        // Large mask (20 columns)
        (
            TableId::new(1005),
            "large",
            vec![1, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1],
        ),
        // Empty mask (table with no columns - edge case)
        (TableId::new(1006), "empty", vec![]),
    ];

    // Insert all test cases
    for (table_id, dest_name, mask_bytes) in &test_cases {
        sqlx::query(
            r#"
            INSERT INTO etl.destination_tables_metadata
                (pipeline_id, table_id, destination_table_id, snapshot_id, schema_status, replication_mask)
            VALUES ($1, $2, $3, '0/0'::pg_lsn, 'applied', $4)
            "#,
        )
        .bind(pipeline_id as i64)
        .bind(SqlxTableId(table_id.into_inner()))
        .bind(*dest_name)
        .bind(mask_bytes)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Load all metadata using the store.
    store.load_destination_tables_metadata().await.unwrap();

    // Verify each test case
    for (table_id, dest_name, expected_mask) in &test_cases {
        let metadata = store
            .get_applied_destination_table_metadata(*table_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("Metadata for {dest_name} should exist"));

        assert_eq!(
            metadata.replication_mask.as_slice(),
            expected_mask.as_slice(),
            "Mask mismatch for {}: expected {:?}, got {:?}",
            dest_name,
            expected_mask,
            metadata.replication_mask.as_slice()
        );
        assert_eq!(metadata.destination_table_id, *dest_name, "Destination table ID mismatch");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_mask_roundtrip() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(54321);

    // Create a store and save metadata with a specific mask
    let original_mask = ReplicationMask::from_bytes(vec![1, 0, 1, 0, 1, 1, 0, 0]);
    let metadata = DestinationTableMetadata::new_applied(
        "roundtrip_table".to_owned(),
        SnapshotId::initial(),
        original_mask.clone(),
    );

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();

    // Create a fresh store and load from database
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    // Verify the loaded mask matches the original
    let loaded_metadata = new_store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("Metadata should exist after loading");

    assert_eq!(
        loaded_metadata.replication_mask.as_slice(),
        original_mask.as_slice(),
        "Roundtrip should preserve replication mask exactly"
    );
}
