#![cfg(feature = "test-utils")]

use etl::state::table::{RetryPolicy, TableReplicationPhase};
use etl::store::both::postgres::PostgresStore;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::test_utils::database::spawn_source_database_for_store;
use etl_postgres::replication::connect_to_source_database;
use etl_postgres::types::{ColumnSchema, TableId, TableName, TableSchema};
use etl_telemetry::tracing::init_test_tracing;
use sqlx::postgres::types::Oid as SqlxTableId;
use tokio_postgres::types::{PgLsn, Type as PgType};

fn create_sample_table_schema() -> TableSchema {
    let table_id = TableId::new(12345);
    let table_name = TableName::new("public".to_string(), "test_table".to_string());
    let columns = vec![
        ColumnSchema::new("id".to_string(), PgType::INT4, -1, false, true),
        ColumnSchema::new("name".to_string(), PgType::TEXT, -1, true, false),
        ColumnSchema::new(
            "created_at".to_string(),
            PgType::TIMESTAMPTZ,
            -1,
            false,
            false,
        ),
    ];

    TableSchema::new(table_id, table_name, columns)
}

fn create_another_table_schema() -> TableSchema {
    let table_id = TableId::new(67890);
    let table_name = TableName::new("public".to_string(), "another_table".to_string());
    let columns = vec![
        ColumnSchema::new("id".to_string(), PgType::INT8, -1, false, true),
        ColumnSchema::new("description".to_string(), PgType::VARCHAR, 255, true, false),
    ];

    TableSchema::new(table_id, table_name, columns)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_state_store_operations() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Test initial state - should be empty
    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert!(state.is_none());

    let all_states = store.get_table_replication_states().await.unwrap();
    assert!(all_states.is_empty());

    // Test updating state
    let init_phase = TableReplicationPhase::Init;
    store
        .update_table_replication_state(table_id, init_phase.clone())
        .await
        .unwrap();

    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(init_phase.clone()));

    let all_states = store.get_table_replication_states().await.unwrap();
    assert_eq!(all_states.len(), 1);
    assert_eq!(all_states.get(&table_id), Some(&init_phase));

    // Test updating to a different state
    let data_sync_phase = TableReplicationPhase::DataSync;
    store
        .update_table_replication_state(table_id, data_sync_phase.clone())
        .await
        .unwrap();

    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(data_sync_phase.clone()));

    // Test SyncDone state with LSN
    let lsn = "0/1000000".parse::<PgLsn>().unwrap();
    let sync_done_phase = TableReplicationPhase::SyncDone { lsn };
    store
        .update_table_replication_state(table_id, sync_done_phase.clone())
        .await
        .unwrap();

    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(sync_done_phase));

    // Test Errored state with retry policy
    let errored_phase = TableReplicationPhase::Errored {
        reason: "Test error".to_string(),
        solution: Some("Test solution".to_string()),
        retry_policy: RetryPolicy::ManualRetry,
    };
    store
        .update_table_replication_state(table_id, errored_phase.clone())
        .await
        .unwrap();

    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(errored_phase));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_state_store_rollback() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Set initial state
    let init_phase = TableReplicationPhase::Init;
    store
        .update_table_replication_state(table_id, init_phase.clone())
        .await
        .unwrap();

    // Update to a different state
    let data_sync_phase = TableReplicationPhase::DataSync;
    store
        .update_table_replication_state(table_id, data_sync_phase.clone())
        .await
        .unwrap();

    // Verify two rows exist before rollback (init + data_sync)
    let pool = connect_to_source_database(&database.config, 1, 1)
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
    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(data_sync_phase));

    // Rollback to previous state
    let rolled_back_state = store
        .rollback_table_replication_state(table_id)
        .await
        .unwrap();
    assert_eq!(rolled_back_state, init_phase);

    // Verify state was rolled back
    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(init_phase));

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
    let result = store.rollback_table_replication_state(table_id).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_state_store_load_states() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;
    let table_id1 = TableId::new(12345);
    let table_id2 = TableId::new(67890);

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Add some states directly to the database
    let init_phase = TableReplicationPhase::Init;
    let data_sync_phase = TableReplicationPhase::DataSync;

    store
        .update_table_replication_state(table_id1, init_phase.clone())
        .await
        .unwrap();
    store
        .update_table_replication_state(table_id2, data_sync_phase.clone())
        .await
        .unwrap();

    // Create a new store instance (simulating restart)
    let new_store = PostgresStore::new(pipeline_id, database.config.clone());

    // Initially empty (not loaded yet)
    let states = new_store.get_table_replication_states().await.unwrap();
    assert!(states.is_empty());

    // Load states from database
    let loaded_count = new_store.load_table_replication_states().await.unwrap();
    assert_eq!(loaded_count, 2);

    // Verify loaded states
    let states = new_store.get_table_replication_states().await.unwrap();
    assert_eq!(states.len(), 2);
    assert_eq!(states.get(&table_id1), Some(&init_phase));
    assert_eq!(states.get(&table_id2), Some(&data_sync_phase));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_store_operations() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone());
    let table_schema = create_sample_table_schema();
    let table_id = table_schema.id;

    // Test initial state - should be empty
    let schema = store.get_table_schema(&table_id).await.unwrap();
    assert!(schema.is_none());

    let all_schemas = store.get_table_schemas().await.unwrap();
    assert!(all_schemas.is_empty());

    // Test storing schema
    store
        .store_table_schema(table_schema.clone())
        .await
        .unwrap();

    let schema = store.get_table_schema(&table_id).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.id, table_schema.id);
    assert_eq!(schema.name, table_schema.name);
    assert_eq!(
        schema.column_schemas.len(),
        table_schema.column_schemas.len()
    );

    let all_schemas = store.get_table_schemas().await.unwrap();
    assert_eq!(all_schemas.len(), 1);

    // Test storing another schema
    let table_schema2 = create_another_table_schema();
    store
        .store_table_schema(table_schema2.clone())
        .await
        .unwrap();

    let all_schemas = store.get_table_schemas().await.unwrap();
    assert_eq!(all_schemas.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_store_load_schemas() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone());
    let table_schema1 = create_sample_table_schema();
    let table_schema2 = create_another_table_schema();

    // Store schemas
    store
        .store_table_schema(table_schema1.clone())
        .await
        .unwrap();
    store
        .store_table_schema(table_schema2.clone())
        .await
        .unwrap();

    // Create a new store instance (simulating restart)
    let new_store = PostgresStore::new(pipeline_id, database.config.clone());

    // Initially empty (not loaded yet)
    let schemas = new_store.get_table_schemas().await.unwrap();
    assert!(schemas.is_empty());

    // Load schemas from database
    let loaded_count = new_store.load_table_schemas().await.unwrap();
    assert_eq!(loaded_count, 2);

    // Verify loaded schemas
    let schemas = new_store.get_table_schemas().await.unwrap();
    assert_eq!(schemas.len(), 2);

    let schema1 = new_store.get_table_schema(&table_schema1.id).await.unwrap();
    assert!(schema1.is_some());
    let schema1 = schema1.unwrap();
    assert_eq!(schema1.id, table_schema1.id);
    assert_eq!(schema1.name, table_schema1.name);

    let schema2 = new_store.get_table_schema(&table_schema2.id).await.unwrap();
    assert!(schema2.is_some());
    let schema2 = schema2.unwrap();
    assert_eq!(schema2.id, table_schema2.id);
    assert_eq!(schema2.name, table_schema2.name);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_store_update_existing() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone());
    let mut table_schema = create_sample_table_schema();

    // Store initial schema
    store
        .store_table_schema(table_schema.clone())
        .await
        .unwrap();

    // Update schema by adding a column
    table_schema.add_column_schema(ColumnSchema::new(
        "updated_at".to_string(),
        PgType::TIMESTAMPTZ,
        -1,
        true,
        false,
    ));

    // Store updated schema
    store
        .store_table_schema(table_schema.clone())
        .await
        .unwrap();

    // Verify updated schema
    let schema = store.get_table_schema(&table_schema.id).await.unwrap();
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.column_schemas.len(), 4); // Original 3 + 1 new column

    // Verify the new column was added
    let updated_at_column = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "updated_at");
    assert!(updated_at_column.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_pipelines_isolation() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id1 = 1;
    let pipeline_id2 = 2;
    let table_id = TableId::new(12345);

    let store1 = PostgresStore::new(pipeline_id1, database.config.clone());
    let store2 = PostgresStore::new(pipeline_id2, database.config.clone());

    // Add state to pipeline 1
    let init_phase = TableReplicationPhase::Init;
    store1
        .update_table_replication_state(table_id, init_phase.clone())
        .await
        .unwrap();

    // Add different state to pipeline 2 for the same table
    let data_sync_phase = TableReplicationPhase::DataSync;
    store2
        .update_table_replication_state(table_id, data_sync_phase.clone())
        .await
        .unwrap();

    // Verify isolation - each pipeline sees only its own state
    let state1 = store1.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state1, Some(init_phase));

    let state2 = store2.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state2, Some(data_sync_phase));

    // Test schema isolation
    let table_schema1 = create_sample_table_schema();
    let table_schema2 = create_another_table_schema();

    store1
        .store_table_schema(table_schema1.clone())
        .await
        .unwrap();
    store2
        .store_table_schema(table_schema2.clone())
        .await
        .unwrap();

    // Each pipeline sees only its own schemas
    let schemas1 = store1.get_table_schemas().await.unwrap();
    assert_eq!(schemas1.len(), 1);
    assert_eq!(schemas1[0].id, table_schema1.id);

    let schemas2 = store2.get_table_schemas().await.unwrap();
    assert_eq!(schemas2.len(), 1);
    assert_eq!(schemas2[0].id, table_schema2.id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_errored_state_with_different_retry_policies() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Test Errored state with NoRetry policy
    let errored_no_retry = TableReplicationPhase::Errored {
        reason: "Fatal error".to_string(),
        solution: None,
        retry_policy: RetryPolicy::NoRetry,
    };
    store
        .update_table_replication_state(table_id, errored_no_retry.clone())
        .await
        .unwrap();

    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(errored_no_retry));

    // Test Errored state with TimedRetry policy
    let next_retry = chrono::Utc::now() + chrono::Duration::minutes(5);
    let errored_timed_retry = TableReplicationPhase::Errored {
        reason: "Temporary error".to_string(),
        solution: Some("Wait and retry".to_string()),
        retry_policy: RetryPolicy::TimedRetry { next_retry },
    };
    store
        .update_table_replication_state(table_id, errored_timed_retry.clone())
        .await
        .unwrap();

    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(errored_timed_retry));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_state_transitions_and_history() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12345);

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Create a series of state transitions
    let init_phase = TableReplicationPhase::Init;
    store
        .update_table_replication_state(table_id, init_phase.clone())
        .await
        .unwrap();

    let data_sync_phase = TableReplicationPhase::DataSync;
    store
        .update_table_replication_state(table_id, data_sync_phase.clone())
        .await
        .unwrap();

    let finished_copy_phase = TableReplicationPhase::FinishedCopy;
    store
        .update_table_replication_state(table_id, finished_copy_phase.clone())
        .await
        .unwrap();

    let lsn = "0/2000000".parse::<PgLsn>().unwrap();
    let sync_done_phase = TableReplicationPhase::SyncDone { lsn };
    store
        .update_table_replication_state(table_id, sync_done_phase.clone())
        .await
        .unwrap();

    let ready_phase = TableReplicationPhase::Ready;
    store
        .update_table_replication_state(table_id, ready_phase.clone())
        .await
        .unwrap();

    // Verify final state
    let state = store.get_table_replication_state(table_id).await.unwrap();
    assert_eq!(state, Some(ready_phase));

    // Test rollback through the history
    let rolled_back_state = store
        .rollback_table_replication_state(table_id)
        .await
        .unwrap();
    assert_eq!(rolled_back_state, sync_done_phase);

    let rolled_back_state = store
        .rollback_table_replication_state(table_id)
        .await
        .unwrap();
    assert_eq!(rolled_back_state, finished_copy_phase);

    let rolled_back_state = store
        .rollback_table_replication_state(table_id)
        .await
        .unwrap();
    assert_eq!(rolled_back_state, data_sync_phase);

    let rolled_back_state = store
        .rollback_table_replication_state(table_id)
        .await
        .unwrap();
    assert_eq!(rolled_back_state, init_phase);

    // No more rollbacks possible
    let result = store.rollback_table_replication_state(table_id).await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_mappings_basic_operations() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    let table_id1 = TableId::new(12345);
    let table_id2 = TableId::new(67890);

    // Test initial state - should be empty
    let mapping = store.get_table_mapping(&table_id1).await.unwrap();
    assert!(mapping.is_none());

    let all_mappings = store.get_table_mappings().await.unwrap();
    assert!(all_mappings.is_empty());

    // Test storing and retrieving mappings
    store
        .store_table_mapping(table_id1, "public_users_1".to_string())
        .await
        .unwrap();

    store
        .store_table_mapping(table_id2, "public_orders_2".to_string())
        .await
        .unwrap();

    let all_mappings = store.get_table_mappings().await.unwrap();
    assert_eq!(all_mappings.len(), 2);
    assert_eq!(
        all_mappings.get(&table_id1),
        Some(&"public_users_1".to_string())
    );
    assert_eq!(
        all_mappings.get(&table_id2),
        Some(&"public_orders_2".to_string())
    );

    // Test updating an existing mapping (upsert)
    store
        .store_table_mapping(table_id1, "public_users_1_updated".to_string())
        .await
        .unwrap();

    let mapping = store.get_table_mapping(&table_id1).await.unwrap();
    assert_eq!(mapping, Some("public_users_1_updated".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_mappings_persistence_and_loading() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Store some mappings
    store
        .store_table_mapping(TableId::new(1), "dest_table_1".to_string())
        .await
        .unwrap();
    store
        .store_table_mapping(TableId::new(2), "dest_table_2".to_string())
        .await
        .unwrap();

    // Create a new store instance (simulating restart)
    let new_store = PostgresStore::new(pipeline_id, database.config.clone());

    // Initially empty cache
    let mappings = new_store.get_table_mappings().await.unwrap();
    assert!(mappings.is_empty());

    // Load all mappings from database
    let loaded_count = new_store.load_table_mappings().await.unwrap();
    assert_eq!(loaded_count, 2);

    // Verify loaded mappings
    let mappings = new_store.get_table_mappings().await.unwrap();
    assert_eq!(mappings.len(), 2);
    assert_eq!(
        mappings.get(&TableId::new(1)),
        Some(&"dest_table_1".to_string())
    );
    assert_eq!(
        mappings.get(&TableId::new(2)),
        Some(&"dest_table_2".to_string())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_mappings_pipeline_isolation() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id1 = 1;
    let pipeline_id2 = 2;

    let store1 = PostgresStore::new(pipeline_id1, database.config.clone());
    let store2 = PostgresStore::new(pipeline_id2, database.config.clone());

    let table_id = TableId::new(12345);

    // Store different mappings for the same table ID in different pipelines
    store1
        .store_table_mapping(table_id, "pipeline1_table".to_string())
        .await
        .unwrap();

    store2
        .store_table_mapping(table_id, "pipeline2_table".to_string())
        .await
        .unwrap();

    // Verify isolation - each pipeline sees only its own mapping
    let mapping1 = store1.get_table_mapping(&table_id).await.unwrap();
    assert_eq!(mapping1, Some("pipeline1_table".to_string()));

    let mapping2 = store2.get_table_mapping(&table_id).await.unwrap();
    assert_eq!(mapping2, Some("pipeline2_table".to_string()));

    // Verify isolation persists after loading from database
    let new_store1 = PostgresStore::new(pipeline_id1, database.config.clone());
    new_store1.load_table_mappings().await.unwrap();

    let loaded_mapping1 = new_store1.get_table_mapping(&table_id).await.unwrap();
    assert_eq!(loaded_mapping1, Some("pipeline1_table".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleanup_deletes_state_schema_and_mapping_for_table() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;

    let store = PostgresStore::new(pipeline_id, database.config.clone());

    // Prepare two tables: one we will delete, one we will keep
    let table_1_schema = create_sample_table_schema();
    let table_1_id = table_1_schema.id;
    let table_2_schema = create_another_table_schema();
    let table_2_id = table_2_schema.id;

    // Populate state, schema, and mapping for both tables
    store
        .update_table_replication_state(table_1_id, TableReplicationPhase::Ready)
        .await
        .unwrap();
    store
        .update_table_replication_state(table_2_id, TableReplicationPhase::DataSync)
        .await
        .unwrap();

    store
        .store_table_schema(table_1_schema.clone())
        .await
        .unwrap();
    store
        .store_table_schema(table_2_schema.clone())
        .await
        .unwrap();

    store
        .store_table_mapping(table_1_id, "dest_table_1".to_string())
        .await
        .unwrap();
    store
        .store_table_mapping(table_2_id, "dest_table_2".to_string())
        .await
        .unwrap();

    // Sanity check before cleanup
    assert!(
        store
            .get_table_replication_state(table_1_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(store.get_table_schema(&table_1_id).await.unwrap().is_some());
    assert!(
        store
            .get_table_mapping(&table_1_id)
            .await
            .unwrap()
            .is_some()
    );

    // Execute cleanup for table 1
    store.cleanup_table_state(table_1_id).await.unwrap();

    // Verify in-memory cache for table 1 has been cleaned
    assert!(
        store
            .get_table_replication_state(table_1_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_table_schema(&table_1_id).await.unwrap().is_none());
    assert!(
        store
            .get_table_mapping(&table_1_id)
            .await
            .unwrap()
            .is_none()
    );

    // Verify other table is unaffected
    assert!(
        store
            .get_table_replication_state(table_2_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(store.get_table_schema(&table_2_id).await.unwrap().is_some());
    assert!(
        store
            .get_table_mapping(&table_2_id)
            .await
            .unwrap()
            .is_some()
    );

    // Create a new store instance and load from DB to ensure persistence
    let new_store = PostgresStore::new(pipeline_id, database.config.clone());
    new_store.load_table_replication_states().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_table_mappings().await.unwrap();

    // Table 1 should not be present after reload
    assert!(
        new_store
            .get_table_replication_state(table_1_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        new_store
            .get_table_schema(&table_1_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        new_store
            .get_table_mapping(&table_1_id)
            .await
            .unwrap()
            .is_none()
    );

    // Table 2 should still be present
    assert!(
        new_store
            .get_table_replication_state(table_2_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        new_store
            .get_table_schema(&table_2_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        new_store
            .get_table_mapping(&table_2_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cleanup_idempotent_when_no_state_present() {
    init_test_tracing();

    let database = spawn_source_database_for_store().await;
    let pipeline_id = 1;
    let store = PostgresStore::new(pipeline_id, database.config.clone());

    let table_schema = create_sample_table_schema();
    let table_id = table_schema.id;

    // Ensure no state exists yet
    assert!(
        store
            .get_table_replication_state(table_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_table_schema(&table_id).await.unwrap().is_none());
    assert!(store.get_table_mapping(&table_id).await.unwrap().is_none());

    // Calling cleanup should succeed even if nothing exists
    store.cleanup_table_state(table_id).await.unwrap();

    // Add state and clean up again
    store
        .update_table_replication_state(table_id, TableReplicationPhase::Init)
        .await
        .unwrap();
    store.store_table_schema(table_schema).await.unwrap();
    store
        .store_table_mapping(table_id, "dest_table".to_string())
        .await
        .unwrap();

    store.cleanup_table_state(table_id).await.unwrap();

    // Verify everything is gone
    assert!(
        store
            .get_table_replication_state(table_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.get_table_schema(&table_id).await.unwrap().is_none());
    assert!(store.get_table_mapping(&table_id).await.unwrap().is_none());
}
