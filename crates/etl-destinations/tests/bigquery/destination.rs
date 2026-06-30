use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
    store::{MemoryStore, SchemaStore},
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

    let destination = bigquery_database.build_destination(1_u64, store).await;

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

        destination
            .write_table_rows_for_tests(&replicated_table_schema, users_rows)
            .await
            .unwrap();

        let mut rows = parse_bigquery_table_rows::<BigQueryUser>(
            bigquery_database
                .query_table(table_schema.name.clone())
                .await
                .unwrap()
        );
        rows.sort();

        assert_eq!(
            rows,
            vec![
                BigQueryUser::new(1, &format!("user_{iteration}_1"), iteration * 10 + 1),
                BigQueryUser::new(2, &format!("user_{iteration}_2"), iteration * 10 + 2),
            ]
        );

        destination
            .drop_table_for_copy_for_tests(&replicated_table_schema)
            .await.unwrap();
    }
}
