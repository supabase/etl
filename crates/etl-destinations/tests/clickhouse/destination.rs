//! Direct destination tests for ClickHouse crash recovery of interrupted
//! schema changes.

use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    destination::{DestinationTableMetadata, DestinationTableSchemaStatus},
    error::ErrorKind,
    schema::{
        ColumnSchema, ReplicatedTableSchema, ReplicationMask, SnapshotId, TableId, TableName,
        TableSchema, Type as PgType,
    },
    store::{MemoryStore, SchemaStore, StateStore},
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::test_utils::setup_clickhouse_database;
use etl_telemetry::tracing::init_test_tracing;

use crate::support::crypto::install_crypto_provider;

/// An `(id, email)` row read back from the recovered users table.
#[derive(clickhouse::Row, serde::Deserialize, Debug, PartialEq)]
struct UserRow {
    id: i32,
    email: Option<String>,
}

/// Builds the shared source schema for the recovery tests: a users table at
/// snapshot 100 with columns `[id, name, email]`. The publication initially
/// replicates `[id, name]` and later switches to `[id, email]`, so the mask
/// changes while the snapshot stays the same.
fn users_table_schema(table_name: &str) -> TableSchema {
    TableSchema::with_snapshot_id(
        TableId::new(71),
        TableName::new("public".to_owned(), table_name.to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), PgType::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), PgType::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), PgType::TEXT, -1, 3, true),
        ],
        SnapshotId::from(100_u64),
    )
}

/// Recovery of a publication-mask-only schema change interrupted by a crash.
///
/// The mask can change without a new schema snapshot when the publication's
/// column list changes, so the previous physical endpoint is identified by
/// `(previous_snapshot_id, previous_replication_mask)`. Recovery must replay
/// the `[id, name] -> [id, email]` diff from the stored previous mask;
/// reconstructing the previous endpoint from the target mask would produce an
/// empty diff, mark the change applied, and leave the physical table diverged.
#[tokio::test(flavor = "multi_thread")]
async fn recovers_interrupted_mask_only_schema_change() {
    init_test_tracing();
    install_crypto_provider();

    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();

    // --- GIVEN: users applied at (snapshot 100, mask [1, 1, 0]) = [id, name] ---
    let table_schema = users_table_schema("maskrecovery");
    let table_id = table_schema.id;
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let table_schema = Arc::new(table_schema);
    let old_replicated = ReplicatedTableSchema::from_mask(
        Arc::clone(&table_schema),
        ReplicationMask::from_bytes(vec![1, 1, 0]),
    );
    let new_replicated = ReplicatedTableSchema::from_mask(
        Arc::clone(&table_schema),
        ReplicationMask::from_bytes(vec![1, 0, 1]),
    );

    let destination =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    destination
        .write_table_rows(
            &old_replicated,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("Alice".to_owned())])],
        )
        .await
        .unwrap();

    // --- GIVEN: the publication switched to [id, email] and the schema change
    // crashed before any DDL ran ---
    let applied_metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    let applying_metadata = applied_metadata.with_schema_change(
        SnapshotId::from(100_u64),
        new_replicated.replication_mask().clone(),
        DestinationTableSchemaStatus::Applying,
    );
    store.store_destination_table_metadata(table_id, applying_metadata).await.unwrap();

    // --- WHEN: a restarted destination writes rows for the target schema ---
    let restarted =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    restarted
        .write_table_rows(
            &new_replicated,
            vec![TableRow::new(vec![Cell::I32(2), Cell::String("bob@example.com".to_owned())])],
        )
        .await
        .unwrap();

    // --- THEN: the physical table converged to the target endpoint [id, email] ---
    assert_eq!(database.column_names("public_maskrecovery").await, vec!["id", "email"]);

    let metadata = store
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination metadata should exist");
    assert!(metadata.is_applied());
    assert_eq!(metadata.snapshot_id, SnapshotId::from(100_u64));
    assert_eq!(metadata.replication_mask, ReplicationMask::from_bytes(vec![1, 0, 1]));
    assert_eq!(metadata.previous_snapshot_id, None);
    assert_eq!(metadata.previous_replication_mask, None);

    // Alice pre-dates the email column, so she keeps NULL; Bob lands with his
    // replicated email.
    let rows: Vec<UserRow> =
        database.query("select id, email from public_maskrecovery order by id").await;
    assert_eq!(
        rows,
        vec![
            UserRow { id: 1, email: None },
            UserRow { id: 2, email: Some("bob@example.com".to_owned()) },
        ]
    );
}

/// Recovery fails closed when an interrupted schema change has no stored
/// previous replication mask (rows persisted before the mask was tracked).
///
/// Guessing the previous endpoint from the target mask could silently mark
/// the change applied while the physical table diverges, so the destination
/// must surface an invalid-state error and keep the metadata in `Applying`.
#[tokio::test(flavor = "multi_thread")]
async fn fails_closed_when_previous_replication_mask_missing() {
    init_test_tracing();
    install_crypto_provider();

    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();

    let table_schema = users_table_schema("maskfailclosed");
    let table_id = table_schema.id;
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let table_schema = Arc::new(table_schema);
    let new_replicated = ReplicatedTableSchema::from_mask(
        Arc::clone(&table_schema),
        ReplicationMask::from_bytes(vec![1, 0, 1]),
    );

    // --- GIVEN: an interrupted schema change without a previous mask ---
    let applying_metadata = DestinationTableMetadata {
        destination_table_id: "public_maskfailclosed".to_owned(),
        snapshot_id: SnapshotId::from(100_u64),
        previous_snapshot_id: Some(SnapshotId::from(100_u64)),
        previous_replication_mask: None,
        schema_status: DestinationTableSchemaStatus::Applying,
        replication_mask: new_replicated.replication_mask().clone(),
    };
    store.store_destination_table_metadata(table_id, applying_metadata).await.unwrap();

    // --- WHEN: a restarted destination attempts to write for the table ---
    let destination =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    let err = destination
        .write_table_rows(
            &new_replicated,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("alice@example.com".to_owned())])],
        )
        .await
        .unwrap_err();

    // --- THEN: the error fails closed and metadata stays in Applying ---
    assert_eq!(err.kind(), ErrorKind::InvalidState);
    assert_eq!(
        err.description(),
        Some("Previous replication mask missing for ClickHouse schema recovery")
    );

    let metadata = store
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination metadata should exist");
    assert!(metadata.is_applying());
}

/// Recovery fails closed when the stored previous replication mask width
/// disagrees with the schema snapshot returned for `previous_snapshot_id`.
///
/// The schema lookup has at-or-before semantics and an older binary can leave
/// a stale mask, so the stored mask can pair with a schema of a different
/// width. `from_mask` only debug-asserts the widths, so recovery must reject
/// the mismatch instead of computing a silently wrong diff, and keep the
/// metadata in `Applying`.
#[tokio::test(flavor = "multi_thread")]
async fn fails_closed_when_previous_replication_mask_width_mismatches() {
    init_test_tracing();
    install_crypto_provider();

    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();

    let table_schema = users_table_schema("maskwidthmismatch");
    let table_id = table_schema.id;
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let table_schema = Arc::new(table_schema);
    let new_replicated = ReplicatedTableSchema::from_mask(
        Arc::clone(&table_schema),
        ReplicationMask::from_bytes(vec![1, 0, 1]),
    );

    // --- GIVEN: an interrupted schema change whose stored previous mask has
    // width 2, while the schema at the previous snapshot has 3 columns ---
    let applying_metadata = DestinationTableMetadata {
        destination_table_id: "public_maskwidthmismatch".to_owned(),
        snapshot_id: SnapshotId::from(100_u64),
        previous_snapshot_id: Some(SnapshotId::from(100_u64)),
        previous_replication_mask: Some(ReplicationMask::from_bytes(vec![1, 1])),
        schema_status: DestinationTableSchemaStatus::Applying,
        replication_mask: new_replicated.replication_mask().clone(),
    };
    store.store_destination_table_metadata(table_id, applying_metadata).await.unwrap();

    // --- WHEN: a restarted destination attempts to write for the table ---
    let destination =
        database.build_destination_with_engine(store.clone(), ClickHouseEngine::MergeTree).await;
    let err = destination
        .write_table_rows(
            &new_replicated,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("alice@example.com".to_owned())])],
        )
        .await
        .unwrap_err();

    // --- THEN: the error fails closed and metadata stays in Applying ---
    assert_eq!(err.kind(), ErrorKind::InvalidState);
    assert_eq!(
        err.description(),
        Some("Previous replication mask width mismatch for ClickHouse schema recovery")
    );

    let metadata = store
        .get_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination metadata should exist");
    assert!(metadata.is_applying());
}
