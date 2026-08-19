use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
    store::{MemoryStore, SchemaStore, StateStore, TableStateLifecycleStore},
};
use etl_destinations::bigquery::test_utils::{
    setup_bigquery_database, skip_if_missing_bigquery_env_vars,
};
use etl_telemetry::tracing::init_test_tracing;

use crate::support::{
    bigquery::{BigQueryUser, parse_bigquery_table_rows},
    crypto::install_crypto_provider,
};

fn make_users_schema(table_name: &str) -> TableSchema {
    TableSchema::new(
        TableId::new(1),
        TableName::new("public".to_owned(), table_name.to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, true),
        ],
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_table_can_be_dropped_and_recreated_repeatedly() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_name = format!("destination_recopy_{}", uuid::Uuid::new_v4().simple());
    let table_schema = make_users_schema(&table_name);
    let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));

    store.store_table_schema(table_schema.clone()).await.unwrap();

    let destination = bigquery_database.build_destination(1_u64, store.clone()).await;

    for iteration in 1..=3 {
        let users_rows = vec![
            TableRow::new(vec![
                Cell::I32(1),
                Cell::String(format!("user_{iteration}_1")),
                Cell::I32(iteration * 10 + 1),
            ]),
            TableRow::new(vec![
                Cell::I32(2),
                Cell::String(format!("user_{iteration}_2")),
                Cell::I32(iteration * 10 + 2),
            ]),
        ];

        destination.write_table_rows_for_tests(&replicated_table_schema, users_rows).await.unwrap();

        let mut rows = parse_bigquery_table_rows::<BigQueryUser>(
            bigquery_database.query_table(table_schema.name.clone()).await.unwrap(),
        );
        rows.sort();

        assert_eq!(
            rows,
            vec![
                BigQueryUser::new(1, &format!("user_{iteration}_1"), iteration * 10 + 1),
                BigQueryUser::new(2, &format!("user_{iteration}_2"), iteration * 10 + 2),
            ]
        );

        destination.drop_table_for_copy_for_tests(&replicated_table_schema).await.unwrap();

        // Match the table-sync lifecycle: ETL clears durable copy metadata only
        // after the destination drop succeeds, then stores the fresh schema.
        store.prepare_table_state_for_copy(table_schema.id).await.unwrap();
        store.store_table_schema(table_schema.clone()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn table_creation_recovers_after_destination_restart() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_name = format!("destination_creation_recovery_{}", uuid::Uuid::new_v4().simple());
    let table_schema = make_users_schema(&table_name);
    let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));
    let table_id = replicated_table_schema.id();

    store.store_table_schema(table_schema).await.unwrap();

    let destination = bigquery_database.build_destination(1_u64, store.clone()).await;
    destination.write_table_rows_for_tests(&replicated_table_schema, Vec::new()).await.unwrap();

    let applied_metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(applied_metadata.is_applied());

    // Simulate a crash after `Creating` was stored and the idempotent table DDL
    // completed, but before the final `Applied` metadata write.
    let creating_metadata = etl::destination::DestinationTableMetadata::new_creating(
        applied_metadata.table_id().to_owned(),
        applied_metadata.snapshot_id(),
        applied_metadata.replication_mask().clone(),
    );
    store.store_destination_table_metadata(table_id, creating_metadata).await.unwrap();

    let restarted_destination = bigquery_database.build_destination(1_u64, store.clone()).await;
    restarted_destination
        .write_table_rows_for_tests(&replicated_table_schema, Vec::new())
        .await
        .unwrap();

    let recovered_metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(recovered_metadata.is_applied());
    assert_eq!(recovered_metadata.table_id(), applied_metadata.table_id());
    assert_eq!(recovered_metadata.snapshot_id(), applied_metadata.snapshot_id());
    assert_eq!(recovered_metadata.replication_mask(), applied_metadata.replication_mask());
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_metadata_does_not_recreate_a_missing_table_after_restart() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_name = format!("destination_missing_applied_{}", uuid::Uuid::new_v4().simple());
    let table_schema = make_users_schema(&table_name);
    let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));
    let table_id = replicated_table_schema.id();

    store.store_table_schema(table_schema).await.unwrap();

    let destination = bigquery_database.build_destination(1_u64, store.clone()).await;
    destination.write_table_rows_for_tests(&replicated_table_schema, Vec::new()).await.unwrap();

    let applied_metadata = store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    assert!(applied_metadata.is_applied());

    // Removing the destination object without clearing its durable metadata
    // simulates out-of-band deletion after successful table preparation.
    destination.drop_table_for_copy_for_tests(&replicated_table_schema).await.unwrap();

    let restarted_destination = bigquery_database.build_destination(1_u64, store.clone()).await;
    restarted_destination
        .write_table_rows_for_tests(
            &replicated_table_schema,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("user_1".to_owned()),
                Cell::I32(42),
            ])],
        )
        .await
        .unwrap_err();

    assert_eq!(
        store.get_destination_table_metadata(table_id).await.unwrap(),
        Some(applied_metadata)
    );
}
