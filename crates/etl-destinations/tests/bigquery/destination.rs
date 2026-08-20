use std::sync::Arc;

use etl::{
    data::{Cell, TableRow},
    destination::WriteEventsDurability,
    event::{Event, InsertEvent, RelationEvent},
    schema::{
        ColumnSchema, PgLsn, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableSchema,
        Type,
    },
    store::{MemoryStore, SchemaStore, StateStore, TableStateLifecycleStore},
    test_utils::destination::write_events,
};
use etl_destinations::bigquery::test_utils::{
    parse_table_cell, setup_bigquery_database, skip_if_missing_bigquery_env_vars,
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

/// Creates a synthetic schema snapshot identifier for destination tests.
fn test_snapshot_id(value: u64) -> SnapshotId {
    let lsn = PgLsn::from(value);
    SnapshotId::new(lsn, lsn)
}

#[tokio::test(flavor = "multi_thread")]
async fn flexible_column_names_work_for_copy_cdc_and_schema_changes() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_id = TableId::new(1);
    let flexible_column_names = [
        "1st_value",
        "café",
        "metric١",
        "link‿value",
        "dash-value",
        "accent\u{301}mark",
        "value&unit",
        "value%unit",
        "value=unit",
        "value+unit",
        "value:unit",
        "value'unit",
        "value<unit",
        "value>unit",
        "value#unit",
        "value|unit",
        "display label",
    ];
    let table_name = TableName::new(
        "public".to_owned(),
        format!("destination_flexible_names_{}", uuid::Uuid::new_v4().simple()),
    );
    let initial_column_schemas = flexible_column_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let column_schema = ColumnSchema::new(
                (*name).to_owned(),
                if index == 0 { Type::INT4 } else { Type::TEXT },
                -1,
                i32::try_from(index).unwrap() + 1,
                false,
            );

            match index {
                0 => column_schema.with_primary_key(1),
                1 => column_schema.with_default_expression("'copy-default'::text".to_owned()),
                _ => column_schema,
            }
        })
        .collect();
    let initial_table_schema = store
        .store_table_schema(TableSchema::with_snapshot_id(
            table_id,
            table_name.clone(),
            initial_column_schemas,
            test_snapshot_id(100),
        ))
        .await
        .unwrap();
    let initial_schema = ReplicatedTableSchema::all(initial_table_schema);
    let destination = bigquery_database.build_destination(1_u64, store.clone()).await;
    let mut cells = vec![Cell::I32(1)];
    cells.extend(
        flexible_column_names
            .iter()
            .enumerate()
            .skip(1)
            .map(|(index, _)| Cell::String(format!("copied_{index}"))),
    );

    destination
        .write_table_rows_for_tests(&initial_schema, vec![TableRow::new(cells)])
        .await
        .unwrap();

    let mut changed_column_schemas = flexible_column_names
        .iter()
        .enumerate()
        .filter_map(|(index, name)| {
            if index == 2 {
                return None;
            }

            let name = if index == 1 { "renamed label" } else { name };
            let column_schema = ColumnSchema::new(
                name.to_owned(),
                if index == 0 { Type::INT4 } else { Type::TEXT },
                -1,
                i32::try_from(index).unwrap() + 1,
                index == 1,
            );

            Some(if index == 0 { column_schema.with_primary_key(1) } else { column_schema })
        })
        .collect::<Vec<_>>();
    changed_column_schemas.push(
        ColumnSchema::new("service %".to_owned(), Type::TEXT, -1, 18, true)
            .with_default_expression("'standard'::text".to_owned()),
    );
    let changed_table_schema = store
        .store_table_schema(TableSchema::with_snapshot_id(
            table_id,
            table_name.clone(),
            changed_column_schemas,
            test_snapshot_id(200),
        ))
        .await
        .unwrap();
    let changed_schema = ReplicatedTableSchema::all(changed_table_schema);
    let mut streamed_cells = vec![Cell::I32(2), Cell::String("streamed_1".to_owned())];
    streamed_cells.extend(
        flexible_column_names
            .iter()
            .enumerate()
            .skip(3)
            .map(|(index, _)| Cell::String(format!("streamed_{index}"))),
    );
    streamed_cells.push(Cell::String("priority".to_owned()));

    write_events(
        &destination,
        WriteEventsDurability::RequireDurable,
        vec![
            Event::Relation(RelationEvent { replicated_table_schema: changed_schema.clone() }),
            Event::Insert(InsertEvent {
                commit_lsn: PgLsn::from(300_u64),
                tx_ordinal: 0,
                replicated_table_schema: changed_schema,
                table_row: TableRow::new(streamed_cells),
            }),
        ],
    )
    .await
    .unwrap();

    let table_schema = bigquery_database.query_table_schema(table_name.clone()).await.unwrap();
    let expected_names = std::iter::once(flexible_column_names[0])
        .chain(std::iter::once("renamed label"))
        .chain(flexible_column_names.into_iter().skip(3))
        .chain(std::iter::once("service %"))
        .collect::<Vec<_>>();
    table_schema.assert_columns(&expected_names);

    let destination_metadata =
        store.get_destination_table_metadata(table_id).await.unwrap().unwrap();
    let defaults =
        bigquery_database.query_column_defaults_by_id(destination_metadata.table_id()).await;
    assert_eq!(
        defaults
            .iter()
            .find(|column| column.column_name == "renamed label")
            .and_then(|column| column.column_default.as_deref()),
        None
    );
    assert_eq!(
        defaults
            .iter()
            .find(|column| column.column_name == "service %")
            .and_then(|column| column.column_default.as_deref()),
        Some("'standard'")
    );

    let mut rows = bigquery_database
        .query_table(table_name)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            let columns = row.columns.unwrap();
            (
                parse_table_cell::<i64>(columns[0].clone()).unwrap(),
                columns[1..]
                    .iter()
                    .map(|column| parse_table_cell::<String>(column.clone()))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();

    let mut copied_values = vec![Some("copied_1".to_owned())];
    copied_values
        .extend((3..flexible_column_names.len()).map(|index| Some(format!("copied_{index}"))));
    copied_values.push(None);
    let mut streamed_values = vec![Some("streamed_1".to_owned())];
    streamed_values
        .extend((3..flexible_column_names.len()).map(|index| Some(format!("streamed_{index}"))));
    streamed_values.push(Some("priority".to_owned()));

    assert_eq!(rows, [(1, copied_values), (2, streamed_values)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn nullable_array_is_stored_as_empty_array() {
    install_crypto_provider();
    init_test_tracing();

    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    let bigquery_database = setup_bigquery_database().await;
    let store = MemoryStore::new();
    let table_name = TableName::new(
        "public".to_owned(),
        format!("destination_nullable_array_{}", uuid::Uuid::new_v4().simple()),
    );
    let table_schema = store
        .store_table_schema(TableSchema::new(
            TableId::new(1),
            table_name.clone(),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("values".to_owned(), Type::INT4_ARRAY, -1, 2, true),
            ],
        ))
        .await
        .unwrap();
    let replicated_table_schema = ReplicatedTableSchema::all(table_schema);
    let destination = bigquery_database.build_destination(1_u64, store).await;

    destination
        .write_table_rows_for_tests(
            &replicated_table_schema,
            vec![TableRow::new(vec![Cell::I32(1), Cell::Null])],
        )
        .await
        .unwrap();

    let table_rows = bigquery_database.query_table(table_name).await.unwrap();
    assert_eq!(table_rows.len(), 1);
    let columns = table_rows[0].columns.as_ref().unwrap();
    assert!(columns[1].value.as_ref().unwrap().as_array().unwrap().is_empty());
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
