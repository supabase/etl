#![cfg(feature = "test-utils")]

use std::collections::BTreeMap;

use etl::{
    destination::{DestinationTableMetadata, DestinationTableSchema},
    error::ErrorKind,
    etl_error,
    schema::{ColumnSchema, ReplicationMask, SnapshotId, TableId, TableName, TableSchema},
    store::{
        PostgresStore, SchemaStore, StateStore, TableRetryPolicy, TableState,
        TableStateLifecycleStore, TableStateOperation, WorkerType,
    },
    test_utils::database::spawn_source_database,
};
use etl_postgres::source::connect_to_source_database;
use etl_telemetry::tracing::init_test_tracing;
use sqlx::postgres::types::Oid as SqlxTableId;
use tokio_postgres::types::{PgLsn, Type as PgType};

/// PostgreSQL's maximum number of columns in a physical table.
const MAX_POSTGRES_TABLE_COLUMNS: i32 = 1600;

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

/// Stores synthetic versions of one table schema.
async fn store_schema_versions(store: &PostgresStore, schema: &TableSchema, versions: &[u64]) {
    for &version in versions {
        let mut schema = schema.clone();
        schema.snapshot_id = test_snapshot_id(version, version);
        store.store_table_schema(schema).await.unwrap();
    }
}

/// Counts actual DELETE statements, including successful zero-delete queries,
/// and allows tests to reject a prune without changing the store.
async fn track_schema_prune_queries(pool: &sqlx::PgPool) {
    sqlx::raw_sql(
        r#"
        create table test.schema_prune_queries (
            calls bigint not null,
            reject_prune boolean not null
        );
        insert into test.schema_prune_queries values (0, false);
        create function test.record_schema_prune_query() returns trigger
        language plpgsql as $$
        begin
            if (select reject_prune from test.schema_prune_queries) then
                raise exception 'injected schema prune failure';
            end if;
            update test.schema_prune_queries set calls = calls + 1;
            return null;
        end;
        $$;
        create trigger record_schema_prune_query
        before delete on etl.table_schemas
        for each statement execute function test.record_schema_prune_query();
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

/// Returns the number of successful schema-prune SQL statements.
async fn schema_prune_query_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("select calls from test.schema_prune_queries").fetch_one(pool).await.unwrap()
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

    store.update_table_states(vec![]).await.unwrap();

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

/// Checkpoints remain monotonic, cached across clones, and isolated by worker.
/// A fresh store reloads persisted values after a restart.
#[tokio::test(flavor = "multi_thread")]
async fn state_store_replication_checkpoint_is_monotonic_and_cached() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    assert_eq!(store.load_replication_checkpoints().await.unwrap(), 0);

    let other_pipeline = PostgresStore::new(2, database.config.clone()).await.unwrap();
    other_pipeline
        .upsert_replication_checkpoint(WorkerType::Apply, PgLsn::from(999))
        .await
        .unwrap();

    let cloned_store = store.clone();
    let table_sync_worker = WorkerType::TableSync { table_id: TableId::new(12345) };
    let checkpoints = [(WorkerType::Apply, 100u64, 120u64), (table_sync_worker, 75, 200)];

    for (worker, first, latest) in checkpoints {
        for reader in [&store, &cloned_store] {
            assert_eq!(reader.get_replication_checkpoint(worker).await.unwrap(), None);
        }

        let first = PgLsn::from(first);
        let latest = PgLsn::from(latest);

        assert_eq!(store.upsert_replication_checkpoint(worker, first).await.unwrap(), first);
        assert_eq!(
            cloned_store.upsert_replication_checkpoint(worker, PgLsn::from(1)).await.unwrap(),
            first
        );
        assert_eq!(store.get_replication_checkpoint(worker).await.unwrap(), Some(first));

        assert_eq!(
            cloned_store.upsert_replication_checkpoint(worker, latest).await.unwrap(),
            latest
        );

        for reader in [&store, &cloned_store] {
            assert_eq!(reader.get_replication_checkpoint(worker).await.unwrap(), Some(latest));
        }
    }

    drop(cloned_store);
    drop(store);

    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    assert_eq!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(), None);
    assert_eq!(store.load_replication_checkpoints().await.unwrap(), 2);

    for (worker, _, latest) in checkpoints {
        assert_eq!(
            store.get_replication_checkpoint(worker).await.unwrap(),
            Some(PgLsn::from(latest))
        );
    }

    store.delete_replication_checkpoint(table_sync_worker).await.unwrap();

    assert_eq!(store.get_replication_checkpoint(table_sync_worker).await.unwrap(), None);
    assert_eq!(
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(),
        Some(PgLsn::from(120))
    );
}

/// Failed writes cannot advance a checkpoint. A failed reset discards its old
/// cached boundary; an explicit startup load restores the confirmed database
/// value.
#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_cache_recovers_after_rejected_mutations() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let saved = PgLsn::from(100u64);
    store.upsert_replication_checkpoint(WorkerType::Apply, saved).await.unwrap();
    let client = database.client.as_ref().unwrap();
    client
        .batch_execute(
            "create function test.reject_checkpoint() returns trigger language plpgsql as $$
                 begin raise exception 'injected checkpoint failure'; end;
             $$;
             create trigger reject_checkpoint before insert or delete on etl.replication_progress
             for each statement execute function test.reject_checkpoint();",
        )
        .await
        .unwrap();
    store.upsert_replication_checkpoint(WorkerType::Apply, PgLsn::from(200u64)).await.unwrap_err();
    assert_eq!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(), Some(saved));

    store.delete_replication_checkpoint(WorkerType::Apply).await.unwrap_err();
    assert_eq!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(), None);
    assert_eq!(store.load_replication_checkpoints().await.unwrap(), 1);
    assert_eq!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(), Some(saved));

    client
        .batch_execute("drop trigger reject_checkpoint on etl.replication_progress")
        .await
        .unwrap();
    let latest = PgLsn::from(300u64);
    assert_eq!(
        store.upsert_replication_checkpoint(WorkerType::Apply, latest).await.unwrap(),
        latest
    );
    assert_eq!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(), Some(latest));

    store.delete_replication_checkpoint(WorkerType::Apply).await.unwrap();
    assert_eq!(store.get_replication_checkpoint(WorkerType::Apply).await.unwrap(), None);
}

/// Schema replacement commits every column batch together and preserves the
/// previously committed database and cache contents if a later batch fails.
#[tokio::test(flavor = "multi_thread")]
async fn schema_store_upsert_replaces_columns_atomically_in_batches() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    let original = create_sample_table_schema();
    store.store_table_schema(original.clone()).await.unwrap();

    let mut wide = original.clone();
    wide.name = TableName::new("schema'\"; --\\".to_owned(), "table'\"; --\\".to_owned());
    wide.column_schemas = (1..=MAX_POSTGRES_TABLE_COLUMNS)
        .map(|ordinal| {
            let mut column = test_column(
                &format!("column {ordinal}'\"; --\\"),
                PgType::TEXT,
                -1,
                ordinal,
                ordinal % 2 == 0,
                ordinal == 1,
            );
            column.default_expression = Some(r"'synthetic''; select 1; --\'::text".to_owned());

            column
        })
        .collect();

    // The duplicate is in the last batch, after earlier inserts succeeded.
    let mut invalid = wide.clone();
    *invalid.column_schemas.last_mut().unwrap() = wide.column_schemas[0].clone();
    store.store_table_schema(invalid).await.unwrap_err();

    assert_eq!(
        *store.get_table_schema(&original.id, SnapshotId::max()).await.unwrap().unwrap(),
        original
    );
    let loaded = etl_postgres::store::schema::load_table_schemas(&pool, 1).await.unwrap();
    assert_eq!(loaded, [original]);

    store.store_table_schema(wide.clone()).await.unwrap();

    assert_eq!(*store.get_table_schema(&wide.id, SnapshotId::max()).await.unwrap().unwrap(), wide);
    let loaded = etl_postgres::store::schema::load_table_schemas(&pool, 1).await.unwrap();
    assert_eq!(loaded.as_slice(), std::slice::from_ref(&wide));

    wide.column_schemas.clear();
    store.store_table_schema(wide.clone()).await.unwrap();

    let loaded = etl_postgres::store::schema::load_table_schemas(&pool, 1).await.unwrap();
    assert_eq!(loaded, [wide]);
    let counts: (i64, i64) = sqlx::query_as(
        "select (select count(*) from etl.table_schemas), (select count(*) from etl.table_columns)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 0));
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
    reloaded_store.load_table_schemas().await.unwrap();
    let checkpoint_lsn = PgLsn::from(5);

    let at_maximum =
        reloaded_store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().unwrap();
    assert_eq!(at_maximum.snapshot_id, SnapshotId::max());

    // This is the original failure relationship: the schema message was written
    // before the checkpoint, but its schema does not activate until a later
    // commit LSN. A message-LSN-only identifier would incorrectly make it
    // eligible at the checkpoint.
    assert!(second_commit_first_snapshot.message_lsn() < checkpoint_lsn);
    assert!(checkpoint_lsn < second_commit_first_snapshot.commit_lsn());
    let legacy_message_lsn = second_commit_first_snapshot.message_lsn();
    let migrated_snapshot_id = SnapshotId::new(legacy_message_lsn, legacy_message_lsn);
    assert!(migrated_snapshot_id <= SnapshotId::at_lsn(checkpoint_lsn));
    assert!(second_commit_first_snapshot > SnapshotId::at_lsn(checkpoint_lsn));

    // Narrower bounds must select the correct version from the loaded index.
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

/// Retained versions are loaded together, so older lookups cannot leave gaps
/// that cause subsequent at-or-before reads to select the wrong schema.
#[tokio::test(flavor = "multi_thread")]
async fn schema_store_loads_retained_schemas_once() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let schemas = [create_sample_table_schema(), create_another_table_schema()];

    for schema in &schemas {
        store_schema_versions(&store, schema, &[0, 100, 200]).await;
    }
    drop(store);

    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    assert!(store.get_table_schema(&schemas[0].id, SnapshotId::max()).await.unwrap().is_none());
    assert!(store.get_table_schemas().await.unwrap().is_empty());

    assert_eq!(store.load_table_schemas().await.unwrap(), 6);

    let cloned_store = store.clone();

    for schema in &schemas {
        for (requested, expected) in [(0, 0), (150, 100), (200, 200), (100, 100)] {
            let loaded = cloned_store
                .get_table_schema(&schema.id, test_snapshot_id(requested, requested))
                .await
                .unwrap()
                .unwrap();
            let mut expected_schema = schema.clone();
            expected_schema.snapshot_id = test_snapshot_id(expected, expected);

            assert_eq!(*loaded, expected_schema);
        }
    }

    assert_eq!(store.get_table_schemas().await.unwrap().len(), 6);
    assert!(store.get_table_schema(&TableId::new(0), SnapshotId::max()).await.unwrap().is_none());
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
async fn schema_store_skips_successful_prune_boundaries_across_clones() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    track_schema_prune_queries(&pool).await;
    let first = create_sample_table_schema();
    let second = create_another_table_schema();
    store_schema_versions(&store, &first, &[0, 100, 200]).await;
    store_schema_versions(&store, &second, &[0, 100, 200]).await;
    let boundaries = BTreeMap::from([
        (first.id, test_snapshot_id(100, 100)),
        (second.id, test_snapshot_id(50, 50)),
    ]);

    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 1);
    assert_eq!(schema_prune_query_count(&pool).await, 1);

    // The second table's zero-delete result is remembered too, including by
    // other handles sharing the same store lifecycle.
    assert_eq!(store.clone().prune_table_schemas(boundaries).await.unwrap(), 0);
    assert_eq!(
        store
            .prune_table_schemas(BTreeMap::from([
                (first.id, test_snapshot_id(50, 50)),
                (second.id, SnapshotId::initial()),
            ]))
            .await
            .unwrap(),
        0
    );
    assert_eq!(schema_prune_query_count(&pool).await, 1);

    // A mixed request still prunes the table whose boundary advances.
    assert_eq!(
        store
            .prune_table_schemas(BTreeMap::from([
                (first.id, test_snapshot_id(200, 200)),
                (second.id, test_snapshot_id(50, 50)),
            ]))
            .await
            .unwrap(),
        1
    );
    assert_eq!(schema_prune_query_count(&pool).await, 2);
    assert_eq!(
        store
            .prune_table_schemas(BTreeMap::from([
                (first.id, test_snapshot_id(150, 150)),
                (second.id, test_snapshot_id(100, 100)),
            ]))
            .await
            .unwrap(),
        1
    );
    assert_eq!(schema_prune_query_count(&pool).await, 3);
    assert!(store.get_table_schema(&first.id, test_snapshot_id(100, 100)).await.unwrap().is_none());
    assert_eq!(
        store
            .get_table_schema(&second.id, test_snapshot_id(150, 150))
            .await
            .unwrap()
            .unwrap()
            .snapshot_id,
        test_snapshot_id(100, 100)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_retries_failed_prune_boundaries() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    track_schema_prune_queries(&pool).await;
    let schema = create_sample_table_schema();
    store_schema_versions(&store, &schema, &[0, 100]).await;
    let boundaries = BTreeMap::from([(schema.id, test_snapshot_id(100, 100))]);

    sqlx::query("update test.schema_prune_queries set reject_prune = true")
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.prune_table_schemas(boundaries.clone()).await.is_err());
    assert!(store.get_table_schema(&schema.id, SnapshotId::initial()).await.unwrap().is_some());

    sqlx::query("update test.schema_prune_queries set reject_prune = false")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 1);
    assert_eq!(store.prune_table_schemas(boundaries).await.unwrap(), 0);
    assert_eq!(schema_prune_query_count(&pool).await, 1);
    assert!(store.get_table_schema(&schema.id, SnapshotId::initial()).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_invalidates_prune_boundaries_for_older_schema_writes() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    track_schema_prune_queries(&pool).await;
    let schema = create_sample_table_schema();
    store_schema_versions(&store, &schema, &[0, 100]).await;
    let boundaries = BTreeMap::from([(schema.id, test_snapshot_id(200, 200))]);
    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 1);

    // Newer insertion and an older cleanup are safe in either lock order.
    let (pruned, ()) = tokio::join!(
        store.prune_table_schemas(boundaries.clone()),
        store_schema_versions(&store, &schema, &[300]),
    );
    assert_eq!(pruned.unwrap(), 0);
    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 0);
    assert_eq!(schema_prune_query_count(&pool).await, 1);

    // Insertion below the boundary changes the retained snapshot, so the
    // previous successful query no longer makes another query redundant.
    store_schema_versions(&store, &schema, &[150]).await;
    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 1);
    assert_eq!(schema_prune_query_count(&pool).await, 2);
    assert_eq!(
        store
            .get_table_schema(&schema.id, test_snapshot_id(200, 200))
            .await
            .unwrap()
            .unwrap()
            .snapshot_id,
        test_snapshot_id(150, 150)
    );

    // An insertion exactly at the boundary must invalidate it as well.
    store_schema_versions(&store, &schema, &[200]).await;
    assert_eq!(store.prune_table_schemas(boundaries).await.unwrap(), 1);
    assert_eq!(schema_prune_query_count(&pool).await, 3);
    assert_eq!(
        store.get_table_schema(&schema.id, SnapshotId::max()).await.unwrap().unwrap().snapshot_id,
        test_snapshot_id(300, 300)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_reloads_and_restarts_retry_pruning() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    track_schema_prune_queries(&pool).await;
    let schema = create_sample_table_schema();
    store_schema_versions(&store, &schema, &[0, 100]).await;
    let boundaries = BTreeMap::from([(schema.id, test_snapshot_id(100, 100))]);
    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 1);

    store.load_table_schemas().await.unwrap();
    assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 0);
    assert_eq!(schema_prune_query_count(&pool).await, 2);

    let restarted = PostgresStore::new(1, database.config.clone()).await.unwrap();
    restarted.load_table_schemas().await.unwrap();
    assert_eq!(restarted.prune_table_schemas(boundaries).await.unwrap(), 0);
    assert_eq!(schema_prune_query_count(&pool).await, 3);
    assert_eq!(
        restarted
            .get_table_schema(&schema.id, SnapshotId::max())
            .await
            .unwrap()
            .unwrap()
            .snapshot_id,
        test_snapshot_id(100, 100)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_store_lifecycle_changes_invalidate_prune_boundaries() {
    let database = spawn_source_database().await;
    let store = PostgresStore::new(1, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
    track_schema_prune_queries(&pool).await;
    let schema = create_sample_table_schema();
    let boundaries = BTreeMap::from([(schema.id, test_snapshot_id(100, 100))]);

    for operation in [
        TableStateOperation::PrepareForCopy { table_id: schema.id },
        TableStateOperation::ResetForResync,
        TableStateOperation::Delete { table_id: schema.id },
    ] {
        store.update_table_state(schema.id, TableState::Ready).await.unwrap();
        store_schema_versions(&store, &schema, &[0, 100]).await;
        assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 1);

        store.apply_table_state_operation(operation).await.unwrap();
        let queries_before_retry = schema_prune_query_count(&pool).await;
        assert_eq!(store.prune_table_schemas(boundaries.clone()).await.unwrap(), 0);
        assert_eq!(schema_prune_query_count(&pool).await, queries_before_retry + 1);
    }
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
            .get_destination_table_metadata(table_id)
            .await
            .unwrap()
            .map(|m| m.table_id().to_owned()),
        Some("pipeline1_table".to_owned())
    );
    assert_eq!(
        store2
            .get_destination_table_metadata(table_id)
            .await
            .unwrap()
            .map(|m| m.table_id().to_owned()),
        Some("pipeline2_table".to_owned())
    );

    // Verify isolation persists after loading from database
    let new_store1 = PostgresStore::new(pipeline_id1, database.config.clone()).await.unwrap();
    new_store1.load_destination_tables_metadata().await.unwrap();
    assert_eq!(
        new_store1
            .get_destination_table_metadata(table_id)
            .await
            .unwrap()
            .map(|m| m.table_id().to_owned()),
        Some("pipeline1_table".to_owned())
    );

    // Deleting one pipeline's state must preserve the other pipeline's state.
    store1.delete_table_state(table_id).await.unwrap();

    let new_store2 = PostgresStore::new(pipeline_id2, database.config.clone()).await.unwrap();
    new_store2.load_table_states().await.unwrap();
    assert_eq!(new_store2.get_table_state(table_id).await.unwrap(), Some(TableState::DataSync));
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
    assert!(store.get_destination_table_metadata(table_1_id).await.unwrap().is_some());
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
    assert!(store.get_destination_table_metadata(table_1_id).await.unwrap().is_none());
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
    assert!(store.get_destination_table_metadata(table_2_id).await.unwrap().is_some());
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
    new_store.load_replication_checkpoints().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    // Table 1 should not be present after reload.
    assert!(new_store.get_table_state(table_1_id).await.unwrap().is_none());
    assert!(new_store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(new_store.get_destination_table_metadata(table_1_id).await.unwrap().is_none());
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
    assert!(new_store.get_destination_table_metadata(table_2_id).await.unwrap().is_some());
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
    assert!(store.get_destination_table_metadata(table_id).await.unwrap().is_none());
    assert!(
        store
            .get_replication_checkpoint(WorkerType::TableSync { table_id })
            .await
            .unwrap()
            .is_none()
    );

    assert!(store.get_table_schema(&other_table_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(store.get_destination_table_metadata(other_table_id).await.unwrap().is_some());

    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_table_states().await.unwrap();
    new_store.load_replication_checkpoints().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    assert_eq!(new_store.get_table_state(table_id).await.unwrap(), Some(TableState::DataSync));
    assert!(new_store.get_table_schema(&table_id, SnapshotId::max()).await.unwrap().is_none());
    assert!(new_store.get_destination_table_metadata(table_id).await.unwrap().is_none());
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
    assert!(new_store.get_destination_table_metadata(other_table_id).await.unwrap().is_some());
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
    assert!(store.get_destination_table_metadata(table_1_id).await.unwrap().is_some());
    assert!(store.get_destination_table_metadata(table_2_id).await.unwrap().is_some());
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
    new_store.load_replication_checkpoints().await.unwrap();
    new_store.load_table_schemas().await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();

    assert_eq!(new_store.get_table_state(table_1_id).await.unwrap(), Some(TableState::Init));
    assert_eq!(new_store.get_table_state(table_2_id).await.unwrap(), Some(TableState::Init));
    assert!(new_store.get_table_schema(&table_1_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(new_store.get_table_schema(&table_2_id, SnapshotId::max()).await.unwrap().is_some());
    assert!(new_store.get_destination_table_metadata(table_1_id).await.unwrap().is_some());
    assert!(new_store.get_destination_table_metadata(table_2_id).await.unwrap().is_some());
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

    // Manually insert a row with a specific replication mask bytea. The mask
    // [1, 0, 1, 1, 0] represents columns: replicated, not replicated,
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
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("Metadata should exist");

    assert_eq!(
        metadata.replication_mask().as_slice(),
        &expected_mask_bytes,
        "Loaded replication mask should match inserted bytea"
    );
    assert_eq!(metadata.table_id(), "test_dest_table");

    // Rows written before the previous-mask migration load with no recovery
    // endpoint, then gain one automatically on their next schema transition.
    let legacy_metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(matches!(legacy_metadata.table_schema(), DestinationTableSchema::Applied { .. }));
    let target_mask = ReplicationMask::from_bytes(vec![1, 1, 1, 1, 0]);
    let applying_metadata =
        legacy_metadata.with_schema_change(test_snapshot_id(10, 11), target_mask.clone()).unwrap();
    store.store_destination_table_metadata(table_id, applying_metadata).await.unwrap();

    let reloaded_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    reloaded_store.load_destination_tables_metadata().await.unwrap();
    let upgraded_metadata =
        reloaded_store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(matches!(
        upgraded_metadata.table_schema(),
        DestinationTableSchema::Applying { previous_replication_mask, .. }
            if previous_replication_mask.as_slice() == expected_mask_bytes
    ));
    assert_eq!(upgraded_metadata.replication_mask(), &target_mask);
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_metadata_loads_creating_and_rejects_incomplete_applying() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let pipeline_id = 1;
    let table_id = TableId::new(12346);
    // Initialize the store to run the migrations.
    PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    let pool = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();

    sqlx::query(
        r#"
        insert into etl.destination_tables_metadata
            (pipeline_id, table_id, destination_table_id, snapshot_id,
             schema_status, replication_mask)
        values ($1, $2, 'test_dest_table', '20:21', 'creating', $3::bytea)
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
    assert!(metadata.is_creating());
    assert!(matches!(metadata.table_schema(), DestinationTableSchema::Creating { .. }));

    sqlx::query(
        r#"
        update etl.destination_tables_metadata
        set schema_status = 'applying', previous_snapshot_id = '10:11'
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
            .get_destination_table_metadata(*table_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("Metadata for {dest_name} should exist"));

        assert_eq!(
            metadata.replication_mask().as_slice(),
            expected_mask.as_slice(),
            "Mask mismatch for {}: expected {:?}, got {:?}",
            dest_name,
            expected_mask,
            metadata.replication_mask().as_slice()
        );
        assert_eq!(metadata.table_id(), *dest_name, "Destination table ID mismatch");
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
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("Metadata should exist after loading");

    assert_eq!(
        loaded_metadata.replication_mask().as_slice(),
        original_mask.as_slice(),
        "Roundtrip should preserve replication mask exactly"
    );
    assert_eq!(loaded_metadata.snapshot_id(), snapshot_id);
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
    .with_schema_change(target_snapshot_id, target_mask.clone())
    .unwrap();

    let store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    store.store_destination_table_metadata(table_id, metadata).await.unwrap();

    let new_store = PostgresStore::new(pipeline_id, database.config.clone()).await.unwrap();
    new_store.load_destination_tables_metadata().await.unwrap();
    let loaded_metadata =
        new_store.get_destination_table_metadata(table_id).await.unwrap().unwrap();

    assert_eq!(loaded_metadata.snapshot_id(), target_snapshot_id);
    assert_eq!(loaded_metadata.replication_mask(), &target_mask);
    assert!(matches!(
        loaded_metadata.table_schema(),
        DestinationTableSchema::Applying {
            previous_snapshot_id: loaded_previous_snapshot_id,
            previous_replication_mask,
            ..
        } if *loaded_previous_snapshot_id == previous_snapshot_id
            && previous_replication_mask == &previous_mask
    ));
}
