use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    schema::{ColumnSchema, ReplicatedTableSchema, TableId, TableName, TableSchema, Type},
    store::{MemoryStore, SchemaStore, StateStore, TableStateLifecycleStore},
};
use etl_config::shared::{
    BigQueryPartitionBy, BigQueryTableOptions, BigQueryTableOptionsConfig,
    BigQueryTimePartitionGranularity,
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
    make_users_schema_with_id(1, table_name)
}

fn make_users_schema_with_id(table_id: u32, table_name: &str) -> TableSchema {
    TableSchema::new(
        TableId::new(table_id),
        TableName::new("public".to_owned(), table_name.to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, true),
        ],
    )
}

fn table_options(
    table_id: TableId,
    partition_by: Option<BigQueryPartitionBy>,
    cluster_by: &[&str],
) -> BigQueryTableOptionsConfig {
    BigQueryTableOptionsConfig {
        tables: vec![BigQueryTableOptions {
            table_id: table_id.into_inner(),
            partition_by,
            cluster_by: cluster_by.iter().map(|column| (*column).to_owned()).collect(),
        }],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn table_options_are_applied_only_when_the_physical_table_is_created() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_name = format!("destination_table_options_{}", uuid::Uuid::new_v4().simple());
    let table_schema = make_users_schema(&table_name);
    let replicated_table_schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));
    store.store_table_schema(table_schema.clone()).await.unwrap();

    let original_options = table_options(
        table_schema.id,
        Some(BigQueryPartitionBy::IntegerRange {
            column: "age".to_owned(),
            start: 0,
            end: 100,
            interval: 10,
        }),
        &["name", "id"],
    );
    let destination = bigquery_database
        .build_destination_with_table_options(1_u64, store.clone(), original_options)
        .await;

    destination
        .write_table_rows_for_tests(
            &replicated_table_schema,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("created".to_owned()),
                Cell::I32(42),
            ])],
        )
        .await
        .unwrap();

    let physical_table_id = store
        .get_destination_table_metadata(table_schema.id)
        .await
        .unwrap()
        .expect("destination metadata should exist")
        .table_id()
        .to_owned();
    let metadata = bigquery_database
        .get_table_metadata_by_id(&physical_table_id)
        .await
        .expect("physical BigQuery table should exist");
    let range_partitioning = metadata.range_partitioning.expect("range partitioning should exist");
    let range = range_partitioning.range.expect("partition range should exist");
    assert_eq!(range_partitioning.field.as_deref(), Some("age"));
    assert_eq!(
        (range.start.as_str(), range.end.as_str(), range.interval.as_str()),
        ("0", "100", "10")
    );
    assert_eq!(
        metadata.clustering.and_then(|clustering| clustering.fields),
        Some(vec!["name".to_owned(), "id".to_owned()])
    );

    // A restarted destination may carry a changed configuration, but an existing
    // physical table must retain the layout selected at its creation.
    let changed_options = table_options(
        table_schema.id,
        Some(BigQueryPartitionBy::TimeColumn {
            column: "not_replicated".to_owned(),
            granularity: BigQueryTimePartitionGranularity::Day,
        }),
        &["also_not_replicated"],
    );
    let restarted_destination = bigquery_database
        .build_destination_with_table_options(1_u64, store.clone(), changed_options)
        .await;
    restarted_destination
        .write_table_rows_for_tests(
            &replicated_table_schema,
            vec![TableRow::new(vec![
                Cell::I32(2),
                Cell::String("restarted".to_owned()),
                Cell::I32(43),
            ])],
        )
        .await
        .unwrap();

    let metadata = bigquery_database
        .get_table_metadata_by_id(&physical_table_id)
        .await
        .expect("physical BigQuery table should still exist");
    let range_partitioning = metadata.range_partitioning.expect("range partitioning should remain");
    assert_eq!(range_partitioning.field.as_deref(), Some("age"));
    assert_eq!(
        metadata.clustering.and_then(|clustering| clustering.fields),
        Some(vec!["name".to_owned(), "id".to_owned()])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_options_support_all_partitioning_and_clustering_combinations() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_suffix = uuid::Uuid::new_v4().simple();
    let schemas = [
        make_users_schema_with_id(10, &format!("destination_partition_only_{table_suffix}")),
        make_users_schema_with_id(11, &format!("destination_cluster_only_{table_suffix}")),
        make_users_schema_with_id(12, &format!("destination_partition_cluster_{table_suffix}")),
        make_users_schema_with_id(13, &format!("destination_default_layout_{table_suffix}")),
    ];
    let options = BigQueryTableOptionsConfig {
        tables: vec![
            BigQueryTableOptions {
                table_id: schemas[0].id.into_inner(),
                partition_by: Some(BigQueryPartitionBy::IntegerRange {
                    column: "age".to_owned(),
                    start: 0,
                    end: 100,
                    interval: 10,
                }),
                cluster_by: vec![],
            },
            BigQueryTableOptions {
                table_id: schemas[1].id.into_inner(),
                partition_by: None,
                cluster_by: vec!["name".to_owned()],
            },
            BigQueryTableOptions {
                table_id: schemas[2].id.into_inner(),
                partition_by: Some(BigQueryPartitionBy::IntegerRange {
                    column: "age".to_owned(),
                    start: 0,
                    end: 100,
                    interval: 10,
                }),
                cluster_by: vec!["name".to_owned()],
            },
        ],
    };
    let destination =
        bigquery_database.build_destination_with_table_options(1_u64, store.clone(), options).await;

    for (index, table_schema) in schemas.iter().enumerate() {
        store.store_table_schema(table_schema.clone()).await.unwrap();
        destination
            .write_table_rows_for_tests(
                &ReplicatedTableSchema::all(Arc::new(table_schema.clone())),
                vec![TableRow::new(vec![
                    Cell::I32(i32::try_from(index).unwrap()),
                    Cell::String(format!("user_{index}")),
                    Cell::I32(42),
                ])],
            )
            .await
            .unwrap();
    }

    for (index, table_schema) in schemas.iter().enumerate() {
        let physical_table_id = store
            .get_destination_table_metadata(table_schema.id)
            .await
            .unwrap()
            .expect("destination metadata should exist")
            .table_id()
            .to_owned();
        let metadata = bigquery_database
            .get_table_metadata_by_id(&physical_table_id)
            .await
            .expect("physical BigQuery table should exist");

        assert_eq!(metadata.range_partitioning.is_some(), matches!(index, 0 | 2));
        assert!(metadata.time_partitioning.is_none());
        assert_eq!(
            metadata.clustering.and_then(|clustering| clustering.fields),
            match index {
                1 | 2 => Some(vec!["name".to_owned()]),
                _ => None,
            }
        );
    }
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
