#![cfg(feature = "test-utils")]

use std::collections::BTreeMap;

use etl::{
    destination::{DestinationTableMetadata, DestinationTableSchemaStatus},
    error::ErrorKind,
    etl_error,
    schema::{ColumnSchema, ReplicationMask, SnapshotId, TableId, TableName, TableSchema},
    store::{
        PostgresStore, SchemaStore, StateStore, TableRetryPolicy, TableState,
        TableStateLifecycleStore, WorkerType,
    },
    test_utils::database::spawn_source_database,
};
use etl_postgres::source::connect_to_source_database;
use etl_telemetry::tracing::init_test_tracing;
use sqlx::postgres::types::Oid as SqlxTableId;
use tokio_postgres::types::{PgLsn, Type as PgType};

/// Creates a synthetic composite snapshot ID for tests.
fn test_snapshot_id(commit_lsn: u64, message_lsn: u64) -> SnapshotId {
    SnapshotId::new(PgLsn::from(commit_lsn), PgLsn::from(message_lsn))
}

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
    let sync_done_state = TableState::SyncDone { lsn, table_decoding_state: None };
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
async fn state_store_replication_checkpoint_is_monotonic() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let apply_worker = WorkerType::Apply;
    let table_sync_worker = WorkerType::TableSync { table_id };

    assert_eq!(store.get_replication_checkpoint(apply_worker).await.unwrap(), None);

    let first_lsn = PgLsn::from(100u64);
    let stale_lsn = PgLsn::from(90u64);
    let later_lsn = PgLsn::from(120u64);

    assert_eq!(
        store.upsert_replication_checkpoint(apply_worker, first_lsn).await.unwrap(),
        first_lsn
    );
    assert_eq!(
        store.upsert_replication_checkpoint(apply_worker, stale_lsn).await.unwrap(),
        first_lsn
    );
    assert_eq!(
        store.upsert_replication_checkpoint(apply_worker, later_lsn).await.unwrap(),
        later_lsn
    );
    assert_eq!(store.get_replication_checkpoint(apply_worker).await.unwrap(), Some(later_lsn));

    let table_sync_lsn = PgLsn::from(75u64);
    assert_eq!(
        store.upsert_replication_checkpoint(table_sync_worker, table_sync_lsn).await.unwrap(),
        table_sync_lsn
    );
    assert_eq!(
        store.get_replication_checkpoint(table_sync_worker).await.unwrap(),
        Some(table_sync_lsn)
    );
    assert_eq!(store.get_replication_checkpoint(apply_worker).await.unwrap(), Some(later_lsn));

    store.delete_replication_checkpoint(table_sync_worker).await.unwrap();
    assert_eq!(store.get_replication_checkpoint(table_sync_worker).await.unwrap(), None);
    assert_eq!(store.get_replication_checkpoint(apply_worker).await.unwrap(), Some(later_lsn));
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

    // Store the initial schema at snapshot 0:0.
    store.store_table_schema(table_schema.clone()).await.unwrap();

    // Create a new version with a later composite snapshot ID.
    table_schema.add_column_schema(test_column(
        "updated_at",
        PgType::TIMESTAMPTZ,
        -1,
        4,
        true,
        false,
    ));
    table_schema.snapshot_id = test_snapshot_id(100u64, 100u64);

    // Store the updated schema as a new version.
    store.store_table_schema(table_schema.clone()).await.unwrap();

    // The maximum boundary returns the updated schema.
    let schema = store.get_table_schema(&table_schema.id, SnapshotId::max()).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_schemas.len(), 4);
    assert_eq!(schema.snapshot_id, test_snapshot_id(100u64, 100u64));

    // An earlier composite boundary returns the initial schema.
    let schema =
        store.get_table_schema(&table_schema.id, test_snapshot_id(50u64, 50u64)).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_schemas.len(), 3);
    assert_eq!(schema.snapshot_id, SnapshotId::initial());

    // The latest version contains the new column.
    let schema =
        store.get_table_schema(&table_schema.id, SnapshotId::max()).await.unwrap().unwrap();
    let updated_at_column = schema.column_schemas.iter().find(|c| c.name == "updated_at");
    assert!(updated_at_column.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_orders_composite_snapshots_by_commit_then_message_lsn() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let mut table_schema = create_sample_table_schema();
    let table_id = table_schema.id;

    store.store_table_schema(table_schema.clone()).await.unwrap();

    let first_commit_snapshot = SnapshotId::new(PgLsn::from(2), PgLsn::from(30));
    table_schema.add_column_schema(test_column("first", PgType::TEXT, -1, 4, true, false));
    table_schema.snapshot_id = first_commit_snapshot;
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let second_commit_first_snapshot = SnapshotId::new(PgLsn::from(10), PgLsn::from(2));
    table_schema.add_column_schema(test_column("second", PgType::TEXT, -1, 5, true, false));
    table_schema.snapshot_id = second_commit_first_snapshot;
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let second_commit_second_snapshot = SnapshotId::new(PgLsn::from(10), PgLsn::from(10));
    table_schema.add_column_schema(test_column("third", PgType::TEXT, -1, 6, true, false));
    table_schema.snapshot_id = second_commit_second_snapshot;
    store.store_table_schema(table_schema.clone()).await.unwrap();

    table_schema.add_column_schema(test_column("maximum", PgType::TEXT, -1, 7, true, false));
    table_schema.snapshot_id = SnapshotId::max();
    store.store_table_schema(table_schema).await.unwrap();

    let reloaded_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let checkpoint_lsn = PgLsn::from(5);

    let at_maximum =
        reloaded_store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();
    assert_eq!(at_maximum.snapshot_id, SnapshotId::max());

    // This is the original failure relationship: the schema message was
    // written before the checkpoint, but its schema does not activate until a
    // later commit LSN. A message-LSN-only identifier would incorrectly make
    // it eligible at the checkpoint.
    assert!(second_commit_first_snapshot.message_lsn() < checkpoint_lsn);
    assert!(checkpoint_lsn < second_commit_first_snapshot.commit_lsn());
    let legacy_message_lsn = second_commit_first_snapshot.message_lsn();
    let migrated_snapshot_id = SnapshotId::new(legacy_message_lsn, legacy_message_lsn);
    assert!(migrated_snapshot_id <= SnapshotId::at_lsn(checkpoint_lsn));
    assert!(second_commit_first_snapshot > SnapshotId::at_lsn(checkpoint_lsn));

    // Query from newest to oldest so each narrower bound misses newer cached
    // entries and exercises numeric ordering of the persisted text.
    let at_second_commit = reloaded_store
        .get_table_schema(&table_id, SnapshotId::at_lsn(PgLsn::from(10)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(at_second_commit.snapshot_id, second_commit_second_snapshot);

    let between_second_commit_messages = reloaded_store
        .get_table_schema(&table_id, SnapshotId::new(PgLsn::from(10), PgLsn::from(5)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(between_second_commit_messages.snapshot_id, second_commit_first_snapshot);

    let at_checkpoint_before_second_commit = reloaded_store
        .get_table_schema(&table_id, SnapshotId::at_lsn(checkpoint_lsn))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(at_checkpoint_before_second_commit.snapshot_id, first_commit_snapshot);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_upsert_replaces_columns() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();

    // Create the initial schema with three columns.
    let table_id = TableId::new(12345);
    let table_name = TableName::new("public".to_owned(), "test_table".to_owned());
    let initial_columns = vec![
        test_column("id", PgType::INT4, -1, 1, false, true),
        test_column("name", PgType::TEXT, -1, 2, true, false),
        test_column("old_column", PgType::TEXT, -1, 3, true, false),
    ];
    let table_schema = TableSchema::new(table_id, table_name.clone(), initial_columns);

    // Store the initial schema.
    store.store_table_schema(table_schema.clone()).await.unwrap();

    // Verify the initial columns.
    let schema = store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();
    assert_eq!(schema.column_schemas.len(), 3);
    assert!(schema.column_schemas.iter().any(|c| c.name == "old_column"));

    // Create an updated schema with the same snapshot ID but different columns,
    // simulating a retry or reprocessing scenario.
    let updated_columns = vec![
        test_column("id", PgType::INT4, -1, 1, false, true),
        test_column("name", PgType::TEXT, -1, 2, true, false),
        test_column("new_column", PgType::TEXT, -1, 3, true, false), // replaced old_column
        test_column("extra_column", PgType::INT8, -1, 4, true, false), // added column
    ];
    let updated_schema = TableSchema::new(table_id, table_name, updated_columns);

    // Upsert the updated schema at the same snapshot ID.
    store.store_table_schema(updated_schema.clone()).await.unwrap();

    // Reload from the database to verify that columns were replaced rather
    // than accumulated.
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

    // Store three schema versions for table 1.
    let table_id_1 = TableId::new(12345);
    let table_name_1 = TableName::new("public".to_owned(), "test_table".to_owned());
    for snapshot_id in [0u64, 100, 200] {
        let columns = vec![
            test_column("id", PgType::INT4, -1, 1, false, true),
            test_column(&format!("col_at_{snapshot_id}"), PgType::TEXT, -1, 2, true, false),
        ];
        let mut table_schema = TableSchema::new(table_id_1, table_name_1.clone(), columns);
        table_schema.snapshot_id = test_snapshot_id(snapshot_id, snapshot_id);
        store.store_table_schema(table_schema.clone()).await.unwrap();
    }

    // Store three schemas for table 2 to verify that eviction is per-table.
    let table_id_2 = TableId::new(67890);
    let table_name_2 = TableName::new("public".to_owned(), "table_2".to_owned());
    for snapshot_id in [0u64, 100, 200] {
        let columns = vec![test_column("id", PgType::INT4, -1, 1, false, true)];
        let mut schema = TableSchema::new(table_id_2, table_name_2.clone(), columns);
        schema.snapshot_id = test_snapshot_id(snapshot_id, snapshot_id);
        store.store_table_schema(schema).await.unwrap();
    }

    // The cache retains two schemas per table, for four schemas total.
    let cached_schemas = store.get_table_schemas().await.unwrap();
    assert_eq!(cached_schemas.len(), 4, "Should have 2 schemas per table");

    // Eviction keeps the two newest snapshots and removes the initial snapshot.
    let table_1_snapshots: Vec<SnapshotId> =
        cached_schemas.iter().filter(|s| s.id == table_id_1).map(|s| s.snapshot_id).collect();
    assert!(
        table_1_snapshots.contains(&test_snapshot_id(100u64, 100u64))
            && table_1_snapshots.contains(&test_snapshot_id(200u64, 200u64))
    );
    assert!(!table_1_snapshots.contains(&SnapshotId::initial()));

    let table_2_snapshots: Vec<SnapshotId> =
        cached_schemas.iter().filter(|s| s.id == table_id_2).map(|s| s.snapshot_id).collect();
    assert!(!table_2_snapshots.contains(&SnapshotId::initial()));

    // Evicted schemas remain loadable from the database.
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
        table_schema.snapshot_id = test_snapshot_id(snapshot_id, snapshot_id);
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
        table_schema.snapshot_id = test_snapshot_id(snapshot_id, snapshot_id);
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
        table_schema.snapshot_id = test_snapshot_id(snapshot_id, snapshot_id);
        store.store_table_schema(table_schema).await.unwrap();
    }

    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    let obsolete_schema_ids: Vec<i64> = sqlx::query_scalar(
        r#"
        select id
        from etl.table_schemas
        where pipeline_id = $1
          and (
              (
                  table_id = $2
                  and (
                      pg_catalog.split_part(snapshot_id, ':', 1)::pg_catalog.numeric,
                      pg_catalog.split_part(snapshot_id, ':', 2)::pg_catalog.numeric
                  ) < (
                      pg_catalog.split_part($3, ':', 1)::pg_catalog.numeric,
                      pg_catalog.split_part($3, ':', 2)::pg_catalog.numeric
                  )
              )
              or (
                  table_id = $4
                  and (
                      pg_catalog.split_part(snapshot_id, ':', 1)::pg_catalog.numeric,
                      pg_catalog.split_part(snapshot_id, ':', 2)::pg_catalog.numeric
                  ) < (
                      pg_catalog.split_part($5, ':', 1)::pg_catalog.numeric,
                      pg_catalog.split_part($5, ':', 2)::pg_catalog.numeric
                  )
              )
          )
        "#,
    )
    .bind(pipeline_id as i64)
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(test_snapshot_id(200u64, 200u64).to_string())
    .bind(SqlxTableId(other_table_id.into_inner()))
    .bind(test_snapshot_id(150u64, 150u64).to_string())
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
        .prune_table_schemas(BTreeMap::from([
            (table_id, test_snapshot_id(200u64, 200u64)),
            (other_table_id, test_snapshot_id(200u64, 200u64)),
        ]))
        .await
        .unwrap();
    assert_eq!(deleted, 3);

    let cached_schemas = store.get_table_schemas().await.unwrap();
    let table_snapshots: Vec<_> =
        cached_schemas.iter().filter(|schema| schema.id == table_id).collect();
    assert_eq!(table_snapshots.len(), 2);
    assert!(
        table_snapshots.iter().any(|schema| schema.snapshot_id == test_snapshot_id(200u64, 200u64))
    );
    assert!(
        table_snapshots.iter().any(|schema| schema.snapshot_id == test_snapshot_id(300u64, 300u64))
    );

    let other_table_snapshots: Vec<_> =
        cached_schemas.iter().filter(|schema| schema.id == other_table_id).collect();
    assert_eq!(other_table_snapshots.len(), 1);
    assert_eq!(other_table_snapshots[0].snapshot_id, test_snapshot_id(150u64, 150u64));

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

    let old_schema =
        store.get_table_schema(&table_id, test_snapshot_id(100u64, 100u64)).await.unwrap();
    assert!(old_schema.is_none());

    let retained_schema =
        store.get_table_schema(&table_id, test_snapshot_id(250u64, 250u64)).await.unwrap().unwrap();
    assert_eq!(retained_schema.snapshot_id, test_snapshot_id(200u64, 200u64));

    let latest_schema =
        store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();
    assert_eq!(latest_schema.snapshot_id, test_snapshot_id(300u64, 300u64));
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
    let sync_done_state = TableState::SyncDone { lsn, table_decoding_state: None };
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
        .upsert_replication_checkpoint(
            WorkerType::TableSync { table_id: table_1_id },
            PgLsn::from(200u64),
        )
        .await
        .unwrap();
    store
        .upsert_replication_checkpoint(
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
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_1_id })
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
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_1_id })
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
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_2_id })
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
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_1_id })
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
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_2_id })
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
    table_schema.snapshot_id = test_snapshot_id(100u64, 100u64);
    store.store_table_schema(table_schema).await.unwrap();
    store.store_table_schema(other_table_schema).await.unwrap();

    let metadata = DestinationTableMetadata::new_applied(
        "dest_table".to_owned(),
        test_snapshot_id(100u64, 100u64),
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
        .upsert_replication_checkpoint(WorkerType::TableSync { table_id }, PgLsn::from(200u64))
        .await
        .unwrap();

    store.prepare_table_state_for_copy(table_id).await.unwrap();

    assert_eq!(store.get_table_state(table_id).await.unwrap(), Some(TableState::DataSync));
    assert!(store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(store.get_applied_destination_table_metadata(table_id).await.unwrap().is_none());
    assert!(
        store
            .get_replication_checkpoint(WorkerType::TableSync { table_id })
            .await
            .unwrap()
            .is_none()
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
            .get_replication_checkpoint(WorkerType::TableSync { table_id })
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
async fn reset_table_states_for_resync_resets_states_and_apply_checkpoint_only() {
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
    store.upsert_replication_checkpoint(WorkerType::Apply, PgLsn::from(500u64)).await.unwrap();
    store
        .upsert_replication_checkpoint(
            WorkerType::TableSync { table_id: table_1_id },
            PgLsn::from(200u64),
        )
        .await
        .unwrap();
    store
        .upsert_replication_checkpoint(
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
    assert!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap().is_none());
    assert!(
        store
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_2_id })
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
    assert!(new_store.get_replication_checkpoint(WorkerType::Apply).await.unwrap().is_none());
    assert!(
        new_store
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_1_id })
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        new_store
            .get_replication_checkpoint(WorkerType::TableSync { table_id: table_2_id })
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
        VALUES (
            $1,
            $2,
            'test_dest_table',
            '0:0',
            'applied',
            $3::bytea
        )
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

    // Rows written before the previous-mask migration load with no recovery
    // endpoint, then gain one automatically on their next schema transition.
    let legacy_metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert_eq!(legacy_metadata.previous_replication_mask, None);
    let target_mask = ReplicationMask::from_bytes(vec![1, 1, 1, 1, 0]);
    let applying_metadata = legacy_metadata.with_schema_change(
        test_snapshot_id(10, 11),
        target_mask.clone(),
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, applying_metadata).await.unwrap();

    let reloaded_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    reloaded_store.load_destination_tables_metadata().await.unwrap();
    let upgraded_metadata =
        reloaded_store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert_eq!(
        upgraded_metadata.previous_replication_mask,
        Some(ReplicationMask::from_bytes(expected_mask_bytes))
    );
    assert_eq!(upgraded_metadata.replication_mask, target_mask);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_metadata_load_accepts_initial_applying_and_rejects_incomplete_endpoint() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12346);
    // We initialize the store to run the migrations.
    let _store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();

    sqlx::query(
        r#"
        insert into etl.destination_tables_metadata
            (pipeline_id, table_id, destination_table_id, snapshot_id,
             schema_status, replication_mask)
        values ($1, $2, 'test_dest_table', '20:21', 'applying', $3::bytea)
        "#,
    )
    .bind(i64::try_from(pipeline_id).unwrap())
    .bind(SqlxTableId(table_id.into_inner()))
    .bind(vec![1_u8, 1])
    .execute(&pool)
    .await
    .unwrap();

    let reloaded_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    reloaded_store.load_destination_tables_metadata().await.unwrap();
    let metadata = reloaded_store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(metadata.is_applying());
    assert_eq!(metadata.previous_snapshot_id, None);
    assert_eq!(metadata.previous_replication_mask, None);

    sqlx::query(
        r#"
        update etl.destination_tables_metadata
        set previous_snapshot_id = '10:11'
        where pipeline_id = $1 and table_id = $2
        "#,
    )
    .bind(i64::try_from(pipeline_id).unwrap())
    .bind(SqlxTableId(table_id.into_inner()))
    .execute(&pool)
    .await
    .unwrap();

    let incomplete_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let error = incomplete_store.load_destination_tables_metadata().await.unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidState);
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
            VALUES (
                $1,
                $2,
                $3,
                '0:0',
                'applied',
                $4
            )
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
async fn destination_metadata_roundtrip_preserves_composite_snapshot_and_replication_mask() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(54321);

    // Store metadata with a composite snapshot ID and a specific mask.
    let original_mask = ReplicationMask::from_bytes(vec![1, 0, 1, 0, 1, 1, 0, 0]);
    let snapshot_id = SnapshotId::new(PgLsn::from(200), PgLsn::from(100));
    let metadata = DestinationTableMetadata::new_applied(
        "roundtrip_table".to_owned(),
        snapshot_id,
        original_mask.clone(),
    );

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();

    // Load the metadata through a fresh store.
    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    // The loaded metadata preserves both the mask and composite snapshot ID.
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
    assert_eq!(loaded_metadata.snapshot_id, snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_metadata_roundtrip_preserves_previous_logical_endpoint() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(54322);
    let target_snapshot_id = SnapshotId::new(PgLsn::from(300), PgLsn::from(150));
    let target_mask = ReplicationMask::from_bytes(vec![1, 1, 1]);
    let previous_snapshot_id = SnapshotId::new(PgLsn::from(200), PgLsn::from(100));
    let previous_mask = ReplicationMask::from_bytes(vec![1, 0, 1]);
    let metadata = DestinationTableMetadata::new_applied(
        "roundtrip_table".to_owned(),
        previous_snapshot_id,
        previous_mask.clone(),
    )
    .with_schema_change(
        target_snapshot_id,
        target_mask.clone(),
        DestinationTableSchemaStatus::Applying,
    );

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();

    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();
    let loaded_metadata =
        new_store.get_destination_table_metadata(table_id).await.unwrap().unwrap();

    assert_eq!(loaded_metadata.snapshot_id, target_snapshot_id);
    assert_eq!(loaded_metadata.replication_mask, target_mask);
    assert_eq!(loaded_metadata.previous_snapshot_id, Some(previous_snapshot_id));
    assert_eq!(loaded_metadata.previous_replication_mask, Some(previous_mask));
    assert_eq!(loaded_metadata.schema_status, DestinationTableSchemaStatus::Applying);
}
