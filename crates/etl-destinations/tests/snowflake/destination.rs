use std::sync::Arc;

use etl::{
    state::destination_metadata::DestinationTableMetadata,
    store::{schema::SchemaStore, state::StateStore},
    test_utils::notifying_store::NotifyingStore,
    types::{
        Cell, ColumnSchema, DeleteEvent, Event, InsertEvent, OldTableRow, PgLsn, PipelineId,
        RelationEvent, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableRow,
        TableSchema, Type, UpdateEvent, UpdatedTableRow,
    },
};
use etl_destinations::snowflake::{
    AuthManager, Config, HttpExchanger, OffsetToken, RestStreamClient, SnowflakeClient,
    SnowflakeDestination, SqlClient,
    test_utils::{load_test_config, query_rows},
};

use super::common::{build_auth, poll_destination_offset, with_table_cleanup};

struct TestHarness {
    destination: SnowflakeDestination<
        NotifyingStore,
        AuthManager<HttpExchanger>,
        RestStreamClient<AuthManager<HttpExchanger>>,
    >,
    sql: SqlClient<AuthManager<HttpExchanger>>,
    config: Config,
    store: NotifyingStore,
}

impl TestHarness {
    fn new() -> Self {
        let config = load_test_config();
        let auth = build_auth();
        let sql = SqlClient::new(config.clone(), Arc::clone(&auth), reqwest::Client::new());
        let store = NotifyingStore::new();
        let pipeline_id: PipelineId = 1;

        let client = SnowflakeClient::new(config.clone(), Arc::clone(&auth), pipeline_id);
        let destination = SnowflakeDestination::new(client, store.clone());

        Self { destination, sql, config, store }
    }
}

fn snowflake_table_name(src_schema: &str, src_table: &str) -> String {
    let escaped_schema = src_schema.replace('_', "__");
    let escaped_table = src_table.replace('_', "__");
    format!("{escaped_schema}_{escaped_table}").to_uppercase()
}

async fn poll_and_query_rows(
    harness: &TestHarness,
    table_id: TableId,
    sf_table: &str,
    expected_offset: &OffsetToken,
) -> Vec<Vec<serde_json::Value>> {
    let committed = poll_destination_offset(
        &harness.destination,
        table_id,
        expected_offset,
        std::time::Duration::from_secs(5),
        18,
    )
    .await;
    assert_eq!(
        committed,
        Some(expected_offset.clone()),
        "expected offset {expected_offset:?} not committed within 90s for table {sf_table}"
    );

    let fqn =
        format!("\"{}\".\"{}\".\"{sf_table}\"", harness.config.database, harness.config.schema);
    query_rows(&harness.sql, &format!("SELECT * FROM {fqn} ORDER BY \"_cdc_sequence_number\""))
        .await
        .expect("query_rows failed")
}

fn make_table_schema(table_id: u32, schema: &str, table: &str) -> TableSchema {
    TableSchema::new(
        TableId::new(table_id),
        TableName::new(schema.to_string(), table.to_string()),
        vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
        ],
    )
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn write_table_rows_basic() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1001);
    let table_schema = make_table_schema(1001, "public", &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));

    harness.store.store_table_schema(table_schema).await.unwrap();

    // Write 2 rows, poll,  verify rows are there `_cdc_operation = "insert"`.
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        harness
            .destination
            .write_table_rows(
                &schema,
                vec![
                    TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]),
                    TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
                ],
            )
            .await
            .expect("write_table_rows failed");

        let zero_offset = OffsetToken::zero();
        let rows = poll_and_query_rows(&harness, table_id, &sf_table, &zero_offset).await;
        assert_eq!(rows.len(), 2, "expected 2 rows");

        let zero_offset = zero_offset.to_string();
        // Column order: id, name, _cdc_operation, _cdc_sequence_number
        assert_eq!(rows[0][0], serde_json::json!("1"));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
        assert_eq!(rows[0][2], serde_json::json!("insert"));
        assert_eq!(rows[0][3], serde_json::json!(zero_offset));

        assert_eq!(rows[1][0], serde_json::json!("2"));
        assert_eq!(rows[1][1], serde_json::json!("Bob"));
        assert_eq!(rows[1][2], serde_json::json!("insert"));
        assert_eq!(rows[1][3], serde_json::json!(zero_offset));
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn write_table_rows_empty() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_schema = make_table_schema(1002, "public", &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));

    harness.store.store_table_schema(table_schema).await.unwrap();

    // Write empty vec. Verify table is created but has no rows.
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        harness
            .destination
            .write_table_rows(&schema, vec![])
            .await
            .expect("write_table_rows with empty rows failed");

        let exists = harness.sql.table_exists(&sf_table).await.expect("table_exists failed");
        assert!(exists, "table should have been created even with empty row set");

        let fqn =
            format!("\"{}\".\"{}\".\"{sf_table}\"", harness.config.database, harness.config.schema);
        let rows = query_rows(&harness.sql, &format!("SELECT * FROM {fqn}"))
            .await
            .expect("query_rows failed");
        assert_eq!(rows.len(), 0, "table should be empty");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn write_events_insert_update_delete() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1003);
    let table_schema = make_table_schema(1003, "public", &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));

    harness.store.store_table_schema(table_schema).await.unwrap();

    // Send Insert, Update (Full), Delete (Full) events.
    // Poll and verify 3 rows with operations "insert", "update", "delete".
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        harness
            .destination
            .process_events(vec![
                Event::Insert(InsertEvent {
                    start_lsn: PgLsn::from(1u64),
                    commit_lsn: PgLsn::from(1u64),
                    tx_ordinal: 0,
                    replicated_table_schema: schema.clone(),
                    table_row: TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]),
                }),
                Event::Update(UpdateEvent {
                    start_lsn: PgLsn::from(2u64),
                    commit_lsn: PgLsn::from(2u64),
                    tx_ordinal: 0,
                    replicated_table_schema: schema.clone(),
                    updated_table_row: UpdatedTableRow::Full(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("Alice Updated".into()),
                    ])),
                    old_table_row: Some(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(1),
                        Cell::String("Alice".into()),
                    ]))),
                }),
                Event::Delete(DeleteEvent {
                    start_lsn: PgLsn::from(3u64),
                    commit_lsn: PgLsn::from(3u64),
                    tx_ordinal: 0,
                    replicated_table_schema: schema.clone(),
                    old_table_row: Some(OldTableRow::Full(TableRow::new(vec![
                        Cell::I32(2),
                        Cell::String("Bob".into()),
                    ]))),
                }),
            ])
            .await
            .expect("process_events failed");

        let expected_offset = OffsetToken::new(PgLsn::from(3u64), 0);
        let rows = poll_and_query_rows(&harness, table_id, &sf_table, &expected_offset).await;
        assert_eq!(rows.len(), 3, "expected 3 rows (insert + update + delete)");

        // Column order: id, name, _cdc_operation, _cdc_sequence_number
        // Rows ordered by _cdc_sequence_number.
        assert_eq!(rows[0][0], serde_json::json!("1"));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
        assert_eq!(rows[0][2], serde_json::json!("insert"));

        assert_eq!(rows[1][0], serde_json::json!("1"));
        assert_eq!(rows[1][1], serde_json::json!("Alice Updated"));
        assert_eq!(rows[1][2], serde_json::json!("update"));

        assert_eq!(rows[2][0], serde_json::json!("2"));
        assert_eq!(rows[2][1], serde_json::json!("Bob"));
        assert_eq!(rows[2][2], serde_json::json!("delete"));
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn write_events_delete_key_only() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1004);
    let table_schema = make_table_schema(1004, "public", &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));

    harness.store.store_table_schema(table_schema).await.unwrap();

    // Delete event with `OldTableRow::Key`.
    // Verify the delete row has PK value present but non-PK columns are NULL.
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        harness
            .destination
            .process_events(vec![Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(1u64),
                commit_lsn: PgLsn::from(1u64),
                tx_ordinal: 0,
                replicated_table_schema: schema.clone(),
                old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(42)]))),
            })])
            .await
            .expect("process_events failed");

        let expected_offset = OffsetToken::new(PgLsn::from(1u64), 0);
        let rows = poll_and_query_rows(&harness, table_id, &sf_table, &expected_offset).await;
        assert_eq!(rows.len(), 1, "expected 1 delete row");

        // Column order: id, name, _cdc_operation, _cdc_sequence_number
        assert_eq!(&rows[0][0], &serde_json::Value::String("42".into()),);
        assert_eq!(&rows[0][1], &serde_json::Value::Null);
        assert_eq!(&rows[0][2], &serde_json::Value::String("delete".into()),);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn schema_evolution_add_column() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1006);

    let initial_schema = TableSchema::new(
        table_id,
        TableName::new("public".to_string(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
        ],
    );
    let initial_replicated = ReplicatedTableSchema::all(Arc::new(initial_schema.clone()));

    let new_snapshot_id = SnapshotId::new(PgLsn::from(100u64));
    let evolved_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_string(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_string(), Type::INT4, -1, 1, Some(1), false),
            ColumnSchema::new("name".to_string(), Type::TEXT, -1, 2, None, true),
            ColumnSchema::new("email".to_string(), Type::TEXT, -1, 3, None, true),
        ],
        new_snapshot_id,
    );
    let evolved_replicated = ReplicatedTableSchema::all(Arc::new(evolved_schema.clone()));

    harness.store.store_table_schema(initial_schema.clone()).await.unwrap();
    harness.store.store_table_schema(evolved_schema.clone()).await.unwrap();

    // Create table (write initial rows).
    // Then add a column, then send a RelationEvent with the new schema.
    // Finally, insert a row with the new column.
    // Verify the new column exists in Snowflake.
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        harness
            .destination
            .write_table_rows(
                &initial_replicated,
                vec![TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
            )
            .await
            .expect("initial write_table_rows failed");

        let initial_metadata = DestinationTableMetadata::new_applied(
            sf_table.clone(),
            SnapshotId::initial(),
            initial_replicated.replication_mask().clone(),
        );
        harness.store.store_destination_table_metadata(table_id, initial_metadata).await.unwrap();

        harness
            .destination
            .process_events(vec![Event::Relation(RelationEvent {
                start_lsn: PgLsn::from(100u64),
                commit_lsn: PgLsn::from(100u64),
                tx_ordinal: 0,
                replicated_table_schema: evolved_replicated.clone(),
            })])
            .await
            .expect("process_events (RelationEvent) failed");

        harness
            .destination
            .process_events(vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(101u64),
                commit_lsn: PgLsn::from(101u64),
                tx_ordinal: 0,
                replicated_table_schema: evolved_replicated.clone(),
                table_row: TableRow::new(vec![
                    Cell::I32(2),
                    Cell::String("Bob".into()),
                    Cell::String("bob@example.com".into()),
                ]),
            })])
            .await
            .expect("process_events (Insert with new column) failed");

        let expected_offset = OffsetToken::new(PgLsn::from(101u64), 0);
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &expected_offset,
            std::time::Duration::from_secs(5),
            18,
        )
        .await;
        assert_eq!(committed, Some(expected_offset), "data should commit within 90s");

        let fqn =
            format!("\"{}\".\"{}\".\"{sf_table}\"", harness.config.database, harness.config.schema);
        let rows =
            query_rows(&harness.sql, &format!("SELECT \"email\" FROM {fqn} WHERE \"id\" = '2'"))
                .await
                .expect("query_rows for email column failed");

        assert_eq!(rows.len(), 1, "expected one row for id=2");
        assert_eq!(
            rows[0][0],
            serde_json::Value::String("bob@example.com".into()),
            "expected email = 'bob@example.com', got: {:?}",
            rows[0][0]
        );
    })
    .await;
}
