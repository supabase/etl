use std::{sync::Arc, time::Duration};

use etl::{
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    destination::{DestinationTableMetadata, DestinationWriteStatus, WriteEventsDurability},
    event::{DeleteEvent, Event, InsertEvent, RelationEvent, UpdateEvent},
    pipeline::PipelineId,
    schema::{
        ColumnSchema, PgLsn, ReplicatedTableSchema, SnapshotId, TableId, TableName, TableSchema,
        Type,
    },
    store::{SchemaStore, StateStore},
    test_utils::{
        destination::{
            write_events as invoke_write_events, write_table_rows as invoke_write_table_rows,
        },
        notifying_store::NotifyingStore,
    },
};
use etl_destinations::snowflake::{
    AuthManager, Client, Config, Destination, HttpExchanger, OffsetToken, RestStreamClient,
    SqlClient,
    test_utils::{load_test_config, query_rows},
};

use super::common::{build_auth, poll_destination_offset, with_table_cleanup};

const DESTINATION_OFFSET_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DESTINATION_OFFSET_MAX_ATTEMPTS: usize = 90;

type SnowflakeTestDestination = Destination<
    NotifyingStore,
    AuthManager<HttpExchanger>,
    RestStreamClient<AuthManager<HttpExchanger>>,
>;

struct TestHarness {
    destination: SnowflakeTestDestination,
    sql: SqlClient<AuthManager<HttpExchanger>>,
    config: Config,
    store: NotifyingStore,
}

impl TestHarness {
    fn new() -> Self {
        let config = load_test_config().clone_without_credentials();
        let auth = build_auth();
        let sql = SqlClient::new(
            config.clone_without_credentials(),
            Arc::clone(&auth),
            reqwest::Client::new(),
        );
        let store = NotifyingStore::new();
        let pipeline_id: PipelineId = 1;

        let client = Client::new(Arc::clone(&auth), pipeline_id);
        let destination = Destination::new(client, store.clone());

        Self { destination, sql, config, store }
    }
}

fn snowflake_table_name(src_schema: &str, src_table: &str) -> String {
    let escaped_schema = src_schema.replace('_', "__");
    let escaped_table = src_table.replace('_', "__");
    format!("{escaped_schema}_{escaped_table}").to_uppercase()
}

fn first_copy_request_offset() -> OffsetToken {
    OffsetToken::new(PgLsn::from(0_u64), 1)
}

/// Writes one nonempty copy batch and completes its terminal durability
/// barrier.
async fn write_table_copy_and_wait(
    destination: &SnowflakeTestDestination,
    schema: &ReplicatedTableSchema,
    rows: Vec<TableRow>,
) {
    assert!(!rows.is_empty(), "table-copy test batch should not be empty");

    let status =
        invoke_write_table_rows(destination, schema, rows).await.expect("table-copy write failed");
    assert_eq!(status, DestinationWriteStatus::Accepted);

    let barrier = invoke_write_table_rows(destination, schema, vec![])
        .await
        .expect("table-copy durability barrier failed");
    assert_eq!(barrier, DestinationWriteStatus::Durable);
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
        DESTINATION_OFFSET_POLL_INTERVAL,
        DESTINATION_OFFSET_MAX_ATTEMPTS,
    )
    .await;
    assert_eq!(
        committed,
        Some(expected_offset.clone()),
        "expected offset {expected_offset:?} not committed within 90s for table {sf_table}"
    );

    let fqn =
        format!("\"{}\".\"{}\".\"{sf_table}\"", harness.config.database(), harness.config.schema());
    query_rows(&harness.sql, &format!("SELECT * FROM {fqn} ORDER BY \"_cdc_sequence_number\""))
        .await
        .expect("query_rows failed")
}

fn make_table_schema(table_id: u32, schema: &str, table: &str) -> TableSchema {
    TableSchema::new(
        TableId::new(table_id),
        TableName::new(schema.to_owned(), table.to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
    )
}

/// Builds the schema used by existing-column default change tests.
fn status_default_schema(
    table_id: TableId,
    table: &str,
    snapshot_lsn: Option<u64>,
    default_expression: Option<&str>,
) -> TableSchema {
    let mut status_column = ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, true);
    if let Some(default_expression) = default_expression {
        status_column = status_column.with_default_expression(default_expression.to_owned());
    }

    let columns = vec![
        ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
        status_column,
    ];
    let table_name = TableName::new("public".to_owned(), table.to_owned());

    match snapshot_lsn {
        Some(snapshot_lsn) => TableSchema::with_snapshot_id(
            table_id,
            table_name,
            columns,
            SnapshotId::new(PgLsn::from(snapshot_lsn)),
        ),
        None => TableSchema::new(table_id, table_name, columns),
    }
}

/// Applies a single Snowflake relation event for schema evolution tests.
async fn apply_relation_event(
    destination: &SnowflakeTestDestination,
    replicated_table_schema: ReplicatedTableSchema,
    context: &str,
) {
    invoke_write_events(
        destination,
        WriteEventsDurability::MayDefer,
        vec![Event::Relation(RelationEvent { replicated_table_schema })],
    )
    .await
    .unwrap_or_else(|error| panic!("write_events ({context}) failed: {error}"));
}

/// Inserts a row while omitting `status`, allowing Snowflake defaults to apply.
async fn insert_row_omitting_status(
    sql: &SqlClient<AuthManager<HttpExchanger>>,
    fqn: &str,
    id: i32,
    sequence_number: &str,
    context: &str,
) {
    sql.execute_ddl(&format!(
        "INSERT INTO {fqn} (\"id\", \"_cdc_operation\", \"_cdc_sequence_number\") VALUES ({id}, \
         'insert', '{sequence_number}')"
    ))
    .await
    .unwrap_or_else(|error| panic!("insert {context} failed: {error}"));
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn create_table_with_simulator_defaults() {
    fn make_simulator_defaults_table_schema(
        table_id: u32,
        schema: &str,
        table: &str,
    ) -> TableSchema {
        TableSchema::new(
            TableId::new(table_id),
            TableName::new(schema.to_owned(), table.to_owned()),
            vec![
                ColumnSchema::new("text_col".to_owned(), Type::TEXT, -1, 1, true)
                    .with_default_expression("'base_text_literal'::text".to_owned()),
                ColumnSchema::new("varchar_col".to_owned(), Type::VARCHAR, 255, 2, true)
                    .with_default_expression(
                        "'base_varchar_literal'::character varying".to_owned(),
                    ),
                ColumnSchema::new("smallint_col".to_owned(), Type::INT2, -1, 3, true)
                    .with_default_expression("7".to_owned()),
                ColumnSchema::new("integer_col".to_owned(), Type::INT4, -1, 4, true)
                    .with_default_expression("42".to_owned()),
                ColumnSchema::new("numeric_col".to_owned(), Type::NUMERIC, -1, 5, true)
                    .with_default_expression("(10 + 5)".to_owned()),
                ColumnSchema::new("boolean_col".to_owned(), Type::BOOL, -1, 6, true)
                    .with_default_expression("false".to_owned()),
                ColumnSchema::new("date_col".to_owned(), Type::DATE, -1, 7, true)
                    .with_default_expression("'2026-01-01'::date".to_owned()),
                ColumnSchema::new("timestamp_col".to_owned(), Type::TIMESTAMP, -1, 8, true)
                    .with_default_expression("'2026-01-01 12:30:00'::timestamp".to_owned()),
                ColumnSchema::new("timestamptz_col".to_owned(), Type::TIMESTAMPTZ, -1, 9, true)
                    .with_default_expression("now()".to_owned()),
                ColumnSchema::new("time_col".to_owned(), Type::TIME, -1, 10, true)
                    .with_default_expression("'12:30:00'::time".to_owned()),
                ColumnSchema::new("jsonb_col".to_owned(), Type::JSONB, -1, 11, true)
                    .with_default_expression(r#"'{"source": "base"}'::jsonb"#.to_owned()),
                ColumnSchema::new("json_col".to_owned(), Type::JSON, -1, 12, true)
                    .with_default_expression(r#"'{"source": "base"}'::json"#.to_owned()),
            ],
        )
    }
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_schema = make_simulator_defaults_table_schema(1012, "public", &src_table);
    let schema = ReplicatedTableSchema::all(Arc::new(table_schema.clone()));

    harness.store.store_table_schema(table_schema).await.unwrap();

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let status = invoke_write_table_rows(&harness.destination, &schema, vec![])
            .await
            .expect("write_table_rows with simulator defaults failed");
        assert_eq!(status, DestinationWriteStatus::Durable);

        let exists = harness.sql.table_exists(&sf_table).await.expect("table_exists failed");
        assert!(exists, "table with simulator defaults should have been created");
    })
    .await;
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

    // The nonempty copy write transfers ownership before Snowflake commits it.
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let status = invoke_write_table_rows(
            &harness.destination,
            &schema,
            vec![
                TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())]),
                TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
            ],
        )
        .await
        .expect("write_table_rows failed");
        assert_eq!(status, DestinationWriteStatus::Accepted);

        let metadata = harness
            .store
            .get_destination_table_metadata(table_id)
            .await
            .unwrap()
            .expect("successful table setup should store destination metadata");
        assert!(metadata.is_applied());
        assert_eq!(metadata.snapshot_id, schema.inner().snapshot_id);
        assert_eq!(&metadata.replication_mask, schema.replication_mask());

        // The empty write is the table-wide durability barrier.
        let status = invoke_write_table_rows(&harness.destination, &schema, vec![])
            .await
            .expect("table-copy durability barrier failed");
        assert_eq!(status, DestinationWriteStatus::Durable);

        let copy_offset = first_copy_request_offset();
        let rows = poll_and_query_rows(&harness, table_id, &sf_table, &copy_offset).await;
        assert_eq!(rows.len(), 2, "expected 2 rows");

        let zero_sequence = OffsetToken::zero().to_string();
        // Column order: id, name, _cdc_operation, _cdc_sequence_number
        assert_eq!(rows[0][0], serde_json::json!("1"));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
        assert_eq!(rows[0][2], serde_json::json!("insert"));
        assert_eq!(rows[0][3], serde_json::json!(zero_sequence));

        assert_eq!(rows[1][0], serde_json::json!("2"));
        assert_eq!(rows[1][1], serde_json::json!("Bob"));
        assert_eq!(rows[1][2], serde_json::json!("insert"));
        assert_eq!(rows[1][3], serde_json::json!(zero_sequence));
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

    // An empty copy initializes the table and is immediately durable.
    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let status = invoke_write_table_rows(&harness.destination, &schema, vec![])
            .await
            .expect("write_table_rows with empty rows failed");
        assert_eq!(status, DestinationWriteStatus::Durable);

        let exists = harness.sql.table_exists(&sf_table).await.expect("table_exists failed");
        assert!(exists, "table should have been created even with empty row set");

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );
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
        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![
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
            ],
        )
        .await
        .expect("write_events failed");

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
        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Delete(DeleteEvent {
                start_lsn: PgLsn::from(1u64),
                commit_lsn: PgLsn::from(1u64),
                tx_ordinal: 0,
                replicated_table_schema: schema.clone(),
                old_table_row: Some(OldTableRow::Key(TableRow::new(vec![Cell::I32(42)]))),
            })],
        )
        .await
        .expect("write_events failed");

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
    let copy_offset = first_copy_request_offset();

    let initial_schema = TableSchema::new(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
    );
    let initial_replicated = ReplicatedTableSchema::all(Arc::new(initial_schema.clone()));

    let new_snapshot_id = SnapshotId::new(PgLsn::from(100u64));
    let evolved_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
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
        write_table_copy_and_wait(
            &harness.destination,
            &initial_replicated,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
        )
        .await;

        // Wait for initial data to commit before DDL, channel refresh loses uncommitted
        // rows.
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert!(committed.is_some(), "initial data should commit before DDL");

        let initial_metadata = DestinationTableMetadata::new_applied(
            sf_table.clone(),
            SnapshotId::initial(),
            initial_replicated.replication_mask().clone(),
        );
        harness.store.store_destination_table_metadata(table_id, initial_metadata).await.unwrap();

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: evolved_replicated.clone(),
            })],
        )
        .await
        .expect("write_events (RelationEvent) failed");

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(101u64),
                commit_lsn: PgLsn::from(101u64),
                tx_ordinal: 0,
                replicated_table_schema: evolved_replicated.clone(),
                table_row: TableRow::new(vec![
                    Cell::I32(2),
                    Cell::String("Bob".into()),
                    Cell::String("bob@example.com".into()),
                ]),
            })],
        )
        .await
        .expect("write_events (Insert with new column) failed");

        let expected_offset = OffsetToken::new(PgLsn::from(101u64), 0);
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &expected_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert_eq!(committed, Some(expected_offset), "data should commit within 90s");

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );
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

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn schema_evolution_add_column_defaults() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1011);
    let copy_offset = first_copy_request_offset();

    let initial_schema = TableSchema::new(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
    );
    let initial_replicated = ReplicatedTableSchema::all(Arc::new(initial_schema.clone()));

    let new_snapshot_id = SnapshotId::new(PgLsn::from(100u64));
    let evolved_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 3, true)
                .with_default_expression("'new'::text".to_owned()),
            ColumnSchema::new("score".to_owned(), Type::INT4, -1, 4, true)
                .with_default_expression("15".to_owned()),
            ColumnSchema::new("active".to_owned(), Type::BOOL, -1, 5, true)
                .with_default_expression("true".to_owned()),
        ],
        new_snapshot_id,
    );
    let evolved_replicated = ReplicatedTableSchema::all(Arc::new(evolved_schema.clone()));

    harness.store.store_table_schema(initial_schema).await.unwrap();
    harness.store.store_table_schema(evolved_schema).await.unwrap();

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        write_table_copy_and_wait(
            &harness.destination,
            &initial_replicated,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
        )
        .await;

        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert!(committed.is_some(), "initial data should commit before DDL");

        let initial_metadata = DestinationTableMetadata::new_applied(
            sf_table.clone(),
            SnapshotId::initial(),
            initial_replicated.replication_mask().clone(),
        );
        harness.store.store_destination_table_metadata(table_id, initial_metadata).await.unwrap();

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: evolved_replicated.clone(),
            })],
        )
        .await
        .expect("write_events (RelationEvent defaults) failed");

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(101u64),
                commit_lsn: PgLsn::from(101u64),
                tx_ordinal: 0,
                replicated_table_schema: evolved_replicated.clone(),
                table_row: TableRow::new(vec![
                    Cell::I32(2),
                    Cell::String("Bob".into()),
                    Cell::String("new".into()),
                    Cell::I32(15),
                    Cell::Bool(true),
                ]),
            })],
        )
        .await
        .expect("write_events (Insert with defaults) failed");

        let expected_offset = OffsetToken::new(PgLsn::from(101u64), 0);
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &expected_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert_eq!(committed, Some(expected_offset), "data should commit within 90s");

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );
        let rows = query_rows(
            &harness.sql,
            &format!(
                "SELECT \"id\"::VARCHAR, \"status\", \"score\"::VARCHAR, \"active\"::VARCHAR FROM \
                 {fqn} ORDER BY \"id\""
            ),
        )
        .await
        .expect("query for defaulted rows failed");
        assert_eq!(
            rows,
            vec![
                vec![
                    serde_json::json!("1"),
                    serde_json::json!("new"),
                    serde_json::json!("15"),
                    serde_json::json!("true"),
                ],
                vec![
                    serde_json::json!("2"),
                    serde_json::json!("new"),
                    serde_json::json!("15"),
                    serde_json::json!("true"),
                ],
            ]
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn schema_evolution_existing_column_default_changes() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1012);

    let initial_schema = status_default_schema(table_id, &src_table, None, None);
    let initial_replicated = ReplicatedTableSchema::all(Arc::new(initial_schema.clone()));

    let set_default_schema =
        status_default_schema(table_id, &src_table, Some(100), Some("'pending'::text"));
    let set_default_replicated = ReplicatedTableSchema::all(Arc::new(set_default_schema.clone()));

    let unsupported_default_schema = status_default_schema(
        table_id,
        &src_table,
        Some(200),
        Some("array['unsupported']::text[]"),
    );
    let unsupported_default_replicated =
        ReplicatedTableSchema::all(Arc::new(unsupported_default_schema.clone()));

    let reset_default_schema =
        status_default_schema(table_id, &src_table, Some(300), Some("'queued'::text"));
    let reset_default_replicated =
        ReplicatedTableSchema::all(Arc::new(reset_default_schema.clone()));

    let drop_default_schema = status_default_schema(table_id, &src_table, Some(400), None);
    let drop_default_replicated = ReplicatedTableSchema::all(Arc::new(drop_default_schema.clone()));

    harness.store.store_table_schema(initial_schema).await.unwrap();
    harness.store.store_table_schema(set_default_schema).await.unwrap();
    harness.store.store_table_schema(unsupported_default_schema).await.unwrap();
    harness.store.store_table_schema(reset_default_schema).await.unwrap();
    harness.store.store_table_schema(drop_default_schema).await.unwrap();

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        let status = invoke_write_table_rows(&harness.destination, &initial_replicated, vec![])
            .await
            .expect("initial table write failed");
        assert_eq!(status, DestinationWriteStatus::Durable);

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );

        apply_relation_event(&harness.destination, set_default_replicated, "set default").await;
        insert_row_omitting_status(&harness.sql, &fqn, 1, "manual-1", "after skipped set default")
            .await;

        apply_relation_event(
            &harness.destination,
            unsupported_default_replicated,
            "unsupported default",
        )
        .await;
        insert_row_omitting_status(
            &harness.sql,
            &fqn,
            2,
            "manual-2",
            "after unsupported default is skipped",
        )
        .await;

        apply_relation_event(&harness.destination, reset_default_replicated, "reset default").await;
        insert_row_omitting_status(
            &harness.sql,
            &fqn,
            3,
            "manual-3",
            "after skipped reset default",
        )
        .await;

        apply_relation_event(&harness.destination, drop_default_replicated, "drop default").await;
        insert_row_omitting_status(&harness.sql, &fqn, 4, "manual-4", "after skipped drop default")
            .await;

        let rows = query_rows(
            &harness.sql,
            &format!("SELECT \"id\"::VARCHAR, \"status\" FROM {fqn} ORDER BY \"id\""),
        )
        .await
        .expect("query for existing-column default rows failed");
        assert_eq!(
            rows,
            vec![
                vec![serde_json::json!("1"), serde_json::Value::Null],
                vec![serde_json::json!("2"), serde_json::Value::Null],
                vec![serde_json::json!("3"), serde_json::Value::Null],
                vec![serde_json::json!("4"), serde_json::Value::Null],
            ]
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn schema_evolution_applies_ordered_name_reuse_plan() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1007);
    let copy_offset = first_copy_request_offset();

    let initial_schema = TableSchema::new(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("first".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("second".to_owned(), Type::TEXT, -1, 3, true),
            ColumnSchema::new("replaced".to_owned(), Type::TEXT, -1, 4, true),
        ],
    );
    let initial_replicated = ReplicatedTableSchema::all(Arc::new(initial_schema.clone()));

    let new_snapshot_id = SnapshotId::new(PgLsn::from(100u64));
    let evolved_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("second".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("first".to_owned(), Type::TEXT, -1, 3, true),
            ColumnSchema::new("replaced".to_owned(), Type::TEXT, -1, 5, true),
        ],
        new_snapshot_id,
    );
    let evolved_replicated = ReplicatedTableSchema::all(Arc::new(evolved_schema.clone()));

    harness.store.store_table_schema(initial_schema).await.unwrap();
    harness.store.store_table_schema(evolved_schema).await.unwrap();

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        write_table_copy_and_wait(
            &harness.destination,
            &initial_replicated,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("first-1".into()),
                Cell::String("second-1".into()),
                Cell::String("old-replaced".into()),
            ])],
        )
        .await;

        // Wait for initial data to commit before DDL -- channel refresh loses
        // uncommitted rows.
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert!(committed.is_some(), "initial data should commit before DDL");

        let initial_metadata = DestinationTableMetadata::new_applied(
            sf_table.clone(),
            SnapshotId::initial(),
            initial_replicated.replication_mask().clone(),
        );
        harness.store.store_destination_table_metadata(table_id, initial_metadata).await.unwrap();

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: evolved_replicated.clone(),
            })],
        )
        .await
        .expect("write_events (RelationEvent ordered plan) failed");

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(101u64),
                commit_lsn: PgLsn::from(101u64),
                tx_ordinal: 0,
                replicated_table_schema: evolved_replicated.clone(),
                table_row: TableRow::new(vec![
                    Cell::I32(2),
                    Cell::String("second-2".into()),
                    Cell::String("first-2".into()),
                    Cell::String("new-replaced".into()),
                ]),
            })],
        )
        .await
        .expect("write_events (Insert after ordered plan) failed");

        let expected_offset = OffsetToken::new(PgLsn::from(101u64), 0);
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &expected_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert_eq!(committed, Some(expected_offset), "data should commit within 90s");

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );

        let rows = query_rows(
            &harness.sql,
            &format!(
                "SELECT \"id\", \"second\", \"first\", \"replaced\" FROM {fqn} ORDER BY \"id\""
            ),
        )
        .await
        .expect("query after ordered schema plan failed");
        assert_eq!(
            rows,
            vec![
                vec![
                    serde_json::json!("1"),
                    serde_json::json!("first-1"),
                    serde_json::json!("second-1"),
                    serde_json::Value::Null,
                ],
                vec![
                    serde_json::json!("2"),
                    serde_json::json!("second-2"),
                    serde_json::json!("first-2"),
                    serde_json::json!("new-replaced"),
                ],
            ]
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn schema_evolution_drop_column() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1008);
    let copy_offset = first_copy_request_offset();

    let initial_schema = TableSchema::new(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
        ],
    );
    let initial_replicated = ReplicatedTableSchema::all(Arc::new(initial_schema.clone()));

    let new_snapshot_id = SnapshotId::new(PgLsn::from(100u64));
    let evolved_schema = TableSchema::with_snapshot_id(
        table_id,
        TableName::new("public".to_owned(), src_table.clone()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
        new_snapshot_id,
    );
    let evolved_replicated = ReplicatedTableSchema::all(Arc::new(evolved_schema.clone()));

    harness.store.store_table_schema(initial_schema).await.unwrap();
    harness.store.store_table_schema(evolved_schema).await.unwrap();

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        write_table_copy_and_wait(
            &harness.destination,
            &initial_replicated,
            vec![TableRow::new(vec![
                Cell::I32(1),
                Cell::String("Alice".into()),
                Cell::String("alice@test.com".into()),
            ])],
        )
        .await;

        // Wait for initial data to commit before DDL -- channel refresh loses
        // uncommitted rows.
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert!(committed.is_some(), "initial data should commit before DDL");

        let initial_metadata = DestinationTableMetadata::new_applied(
            sf_table.clone(),
            SnapshotId::initial(),
            initial_replicated.replication_mask().clone(),
        );
        harness.store.store_destination_table_metadata(table_id, initial_metadata).await.unwrap();

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent {
                replicated_table_schema: evolved_replicated.clone(),
            })],
        )
        .await
        .expect("write_events (RelationEvent drop) failed");

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(101u64),
                commit_lsn: PgLsn::from(101u64),
                tx_ordinal: 0,
                replicated_table_schema: evolved_replicated.clone(),
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
            })],
        )
        .await
        .expect("write_events (Insert after column drop) failed");

        let expected_offset = OffsetToken::new(PgLsn::from(101u64), 0);
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &expected_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert_eq!(committed, Some(expected_offset), "data should commit within 90s");

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );

        // New row landed with remaining columns.
        let rows =
            query_rows(&harness.sql, &format!("SELECT \"name\" FROM {fqn} WHERE \"id\" = '2'"))
                .await
                .expect("query after drop failed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], serde_json::json!("Bob"));

        // Dropped column is gone from the table.
        let result = query_rows(&harness.sql, &format!("SELECT \"email\" FROM {fqn}")).await;
        assert!(result.is_err(), "column 'email' should not exist after DROP COLUMN");
    })
    .await;
}

#[tokio::test]
#[ignore = "requires Snowflake credentials"]
async fn schema_evolution_interleaved_ddl_dml() {
    let harness = TestHarness::new();
    let src_table = format!("ETL_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let sf_table = snowflake_table_name("public", &src_table);

    let table_id = TableId::new(1009);
    let table_name = TableName::new("public".to_owned(), src_table.clone());

    // v1: (id, name)
    let schema_v1 = TableSchema::new(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ],
    );
    let replicated_v1 = ReplicatedTableSchema::all(Arc::new(schema_v1.clone()));

    // v2: ADD COLUMN email (ordinal 3)
    let schema_v2 = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
        ],
        SnapshotId::new(PgLsn::from(100u64)),
    );
    let replicated_v2 = ReplicatedTableSchema::all(Arc::new(schema_v2.clone()));

    // v3: RENAME name -> full_name (ordinal 2 unchanged)
    let schema_v3 = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("full_name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
        ],
        SnapshotId::new(PgLsn::from(200u64)),
    );
    let replicated_v3 = ReplicatedTableSchema::all(Arc::new(schema_v3.clone()));

    // v4: DROP COLUMN email (ordinal 3 removed)
    let schema_v4 = TableSchema::with_snapshot_id(
        table_id,
        table_name.clone(),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("full_name".to_owned(), Type::TEXT, -1, 2, true),
        ],
        SnapshotId::new(PgLsn::from(300u64)),
    );
    let replicated_v4 = ReplicatedTableSchema::all(Arc::new(schema_v4.clone()));

    harness.store.store_table_schema(schema_v1).await.unwrap();
    harness.store.store_table_schema(schema_v2).await.unwrap();
    harness.store.store_table_schema(schema_v3).await.unwrap();
    harness.store.store_table_schema(schema_v4).await.unwrap();

    let poll = |offset_lsn: u64, ord: u64| {
        let expected = OffsetToken::new(PgLsn::from(offset_lsn), ord);
        async {
            let committed = poll_destination_offset(
                &harness.destination,
                table_id,
                &expected,
                DESTINATION_OFFSET_POLL_INTERVAL,
                DESTINATION_OFFSET_MAX_ATTEMPTS,
            )
            .await;
            assert_eq!(committed, Some(expected), "data should commit before next DDL");
        }
    };

    with_table_cleanup(&harness.sql, &[&sf_table], || async {
        // Initial table copy with v1 schema.
        write_table_copy_and_wait(
            &harness.destination,
            &replicated_v1,
            vec![TableRow::new(vec![Cell::I32(1), Cell::String("Alice".into())])],
        )
        .await;

        // Wait for initial data to commit before DDL.
        let copy_offset = first_copy_request_offset();
        let committed = poll_destination_offset(
            &harness.destination,
            table_id,
            &copy_offset,
            DESTINATION_OFFSET_POLL_INTERVAL,
            DESTINATION_OFFSET_MAX_ATTEMPTS,
        )
        .await;
        assert!(committed.is_some(), "initial data should commit before DDL");

        let initial_metadata = DestinationTableMetadata::new_applied(
            sf_table.clone(),
            SnapshotId::initial(),
            replicated_v1.replication_mask().clone(),
        );
        harness.store.store_destination_table_metadata(table_id, initial_metadata).await.unwrap();

        // Phase 1: Insert(v1) + ADD COLUMN
        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(1u64),
                commit_lsn: PgLsn::from(1u64),
                tx_ordinal: 0,
                replicated_table_schema: replicated_v1.clone(),
                table_row: TableRow::new(vec![Cell::I32(2), Cell::String("Bob".into())]),
            })],
        )
        .await
        .expect("write_events (Insert v1) failed");
        poll(1, 0).await;

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent { replicated_table_schema: replicated_v2.clone() })],
        )
        .await
        .expect("write_events (Relation v2) failed");

        // Phase 2: Insert(v2) + RENAME
        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(101u64),
                commit_lsn: PgLsn::from(101u64),
                tx_ordinal: 0,
                replicated_table_schema: replicated_v2.clone(),
                table_row: TableRow::new(vec![
                    Cell::I32(3),
                    Cell::String("Charlie".into()),
                    Cell::String("charlie@test.com".into()),
                ]),
            })],
        )
        .await
        .expect("write_events (Insert v2) failed");
        poll(101, 0).await;

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent { replicated_table_schema: replicated_v3.clone() })],
        )
        .await
        .expect("write_events (Relation v3) failed");

        // Phase 3: Insert(v3) + DROP COLUMN
        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(201u64),
                commit_lsn: PgLsn::from(201u64),
                tx_ordinal: 0,
                replicated_table_schema: replicated_v3.clone(),
                table_row: TableRow::new(vec![
                    Cell::I32(4),
                    Cell::String("Diana".into()),
                    Cell::String("diana@test.com".into()),
                ]),
            })],
        )
        .await
        .expect("write_events (Insert v3) failed");
        poll(201, 0).await;

        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Relation(RelationEvent { replicated_table_schema: replicated_v4.clone() })],
        )
        .await
        .expect("write_events (Relation v4) failed");

        // Phase 4: Final insert after all DDL.
        invoke_write_events(
            &harness.destination,
            WriteEventsDurability::MayDefer,
            vec![Event::Insert(InsertEvent {
                start_lsn: PgLsn::from(301u64),
                commit_lsn: PgLsn::from(301u64),
                tx_ordinal: 0,
                replicated_table_schema: replicated_v4.clone(),
                table_row: TableRow::new(vec![Cell::I32(5), Cell::String("Eve".into())]),
            })],
        )
        .await
        .expect("write_events (Insert v4) failed");
        poll(301, 0).await;

        let fqn = format!(
            "\"{}\".\"{}\".\"{sf_table}\"",
            harness.config.database(),
            harness.config.schema()
        );

        // Final schema should be (id, full_name) -- email was dropped, name was
        // renamed.
        let rows = query_rows(
            &harness.sql,
            &format!("SELECT \"id\", \"full_name\" FROM {fqn} ORDER BY \"id\""),
        )
        .await
        .expect("final query failed");

        // 5 rows: 1 from initial copy + 4 from CDC inserts.
        assert_eq!(rows.len(), 5, "expected 5 rows, got {}", rows.len());
        assert_eq!(rows[0][0], serde_json::json!("1"));
        assert_eq!(rows[0][1], serde_json::json!("Alice"));
        assert_eq!(rows[1][0], serde_json::json!("2"));
        assert_eq!(rows[1][1], serde_json::json!("Bob"));
        assert_eq!(rows[2][0], serde_json::json!("3"));
        assert_eq!(rows[2][1], serde_json::json!("Charlie"));
        assert_eq!(rows[3][0], serde_json::json!("4"));
        assert_eq!(rows[3][1], serde_json::json!("Diana"));
        assert_eq!(rows[4][0], serde_json::json!("5"));
        assert_eq!(rows[4][1], serde_json::json!("Eve"));

        // Dropped column should not exist.
        let result = query_rows(&harness.sql, &format!("SELECT \"email\" FROM {fqn}")).await;
        assert!(result.is_err(), "column 'email' should not exist after DROP COLUMN");

        // Old column name should not exist.
        let result = query_rows(&harness.sql, &format!("SELECT \"name\" FROM {fqn}")).await;
        assert!(result.is_err(), "column 'name' should not exist after RENAME");
    })
    .await;
}
