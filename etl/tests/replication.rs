use std::{collections::HashSet, time::Duration};

use etl::{
    error::ErrorKind,
    replication::client::{
        CtidPartition, PgReplicationChildTransaction, PgReplicationClient, SlotState,
    },
    test_utils::{
        database::{spawn_source_database, test_table_name},
        pipeline::test_slot_name,
        schema::assert_table_schema_columns,
        test_schema::create_partitioned_table,
    },
};
use etl_postgres::{
    below_version,
    tokio::test_utils::{TableModification, id_column_schema},
    types::{ColumnSchema, convert_type_oid_to_type},
    version::POSTGRES_15,
};
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use pg_escape::quote_identifier;
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};
use serde_json::Value as JsonValue;
use tokio::{pin, time::timeout};
use tokio_postgres::{
    CopyOutStream,
    types::{ToSql, Type},
};

/// Creates a test column schema with sensible defaults.
fn test_column(
    name: &str,
    typ: Type,
    ordinal_position: i32,
    nullable: bool,
    primary_key: bool,
) -> ColumnSchema {
    ColumnSchema::new(
        name.to_string(),
        typ,
        -1,
        ordinal_position,
        if primary_key { Some(1) } else { None },
        nullable,
    )
}

fn column_schemas_from_ddl_message(message: &JsonValue) -> Vec<ColumnSchema> {
    let primary_key_positions: std::collections::HashMap<i32, i32> = message["identity"]
        ["primary_key_attnums"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, attnum)| (attnum.as_i64().unwrap() as i32, i32::try_from(index + 1).unwrap()))
        .collect();

    let mut columns = message["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| {
            ColumnSchema::new(
                column["attname"].as_str().unwrap().to_string(),
                convert_type_oid_to_type(column["atttypid"].as_u64().unwrap() as u32),
                column["atttypmod"].as_i64().unwrap() as i32,
                column["attnum"].as_i64().unwrap() as i32,
                primary_key_positions.get(&(column["attnum"].as_i64().unwrap() as i32)).copied(),
                !column["attnotnull"].as_bool().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    columns.sort_by_key(|column| column.ordinal_position);

    columns
}

async fn count_stream_rows(stream: CopyOutStream) -> u64 {
    pin!(stream);

    let mut row_count = 0;
    while let Some(row) = stream.next().await {
        row.unwrap();
        row_count += 1;
    }

    row_count
}

#[derive(Default)]
struct MessageCounts {
    begin_count: u64,
    commit_count: u64,
    origin_count: u64,
    message_count: u64,
    ddl_message_count: u64,
    relation_count: u64,
    type_count: u64,
    insert_count: u64,
    update_count: u64,
    delete_count: u64,
    truncate_count: u64,
    other_count: u64,
}

async fn count_stream_components<F>(
    stream: LogicalReplicationStream,
    mut should_stop: F,
) -> MessageCounts
where
    F: FnMut(&MessageCounts) -> bool,
{
    let mut counts = MessageCounts::default();

    pin!(stream);
    while let Some(event) = stream.next().await {
        if let Ok(event) = event {
            match event {
                ReplicationMessage::XLogData(event) => match event.data() {
                    LogicalReplicationMessage::Begin(_) => counts.begin_count += 1,
                    LogicalReplicationMessage::Commit(_) => counts.commit_count += 1,
                    LogicalReplicationMessage::Origin(_) => counts.origin_count += 1,
                    LogicalReplicationMessage::Message(message) => {
                        counts.message_count += 1;
                        if message.prefix().is_ok_and(|prefix| prefix == "supabase_etl_ddl") {
                            counts.ddl_message_count += 1;
                        }
                    }
                    LogicalReplicationMessage::Relation(_) => counts.relation_count += 1,
                    LogicalReplicationMessage::Type(_) => counts.type_count += 1,
                    LogicalReplicationMessage::Insert(_) => counts.insert_count += 1,
                    LogicalReplicationMessage::Update(_) => counts.update_count += 1,
                    LogicalReplicationMessage::Delete(_) => counts.delete_count += 1,
                    LogicalReplicationMessage::Truncate(_) => counts.truncate_count += 1,
                    _ => counts.other_count += 1,
                },
                _ => counts.other_count += 1,
            }
        }

        if should_stop(&counts) {
            break;
        }
    }

    counts
}

async fn collect_ddl_messages(
    stream: LogicalReplicationStream,
    expected_count: usize,
) -> Vec<JsonValue> {
    let mut messages = Vec::with_capacity(expected_count);

    pin!(stream);
    while messages.len() < expected_count {
        let event = timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("timed out while waiting for logical replication data")
            .expect("logical replication stream ended unexpectedly")
            .expect("failed to decode logical replication data");

        let ReplicationMessage::XLogData(event) = event else {
            continue;
        };

        let LogicalReplicationMessage::Message(message) = event.data() else {
            continue;
        };

        let prefix = message.prefix().expect("message prefix should decode");
        if prefix != "supabase_etl_ddl" {
            continue;
        }

        let content = message.content().expect("message content should decode");
        let json = serde_json::from_str(content).expect("ddl message should be valid json");
        println!("{}", json);
        messages.push(json);
    }

    messages
}

#[derive(Debug, PartialEq, Eq)]
enum StreamMarker {
    Begin,
    DdlMessage(Vec<String>),
    Relation(Vec<String>),
    Insert,
    Commit,
}

async fn collect_stream_markers(
    stream: LogicalReplicationStream,
    expected_count: usize,
) -> Vec<StreamMarker> {
    let mut markers = Vec::with_capacity(expected_count);

    pin!(stream);
    while markers.len() < expected_count {
        let event = timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("timed out while waiting for logical replication data")
            .expect("logical replication stream ended unexpectedly")
            .expect("failed to decode logical replication data");

        let ReplicationMessage::XLogData(event) = event else {
            continue;
        };

        match event.data() {
            LogicalReplicationMessage::Begin(_) => markers.push(StreamMarker::Begin),
            LogicalReplicationMessage::Commit(_) => markers.push(StreamMarker::Commit),
            LogicalReplicationMessage::Message(message) => {
                let prefix = message.prefix().expect("message prefix should decode");
                if prefix != "supabase_etl_ddl" {
                    continue;
                }

                let content = message.content().expect("message content should decode");
                let json: JsonValue =
                    serde_json::from_str(content).expect("ddl message should be valid json");
                let column_names = json["columns"]
                    .as_array()
                    .expect("ddl message columns should be an array")
                    .iter()
                    .map(|column| {
                        column["attname"]
                            .as_str()
                            .expect("ddl message column should have attname")
                            .to_string()
                    })
                    .collect();

                markers.push(StreamMarker::DdlMessage(column_names));
            }
            LogicalReplicationMessage::Relation(relation) => {
                let column_names = relation
                    .columns()
                    .iter()
                    .map(|column| {
                        column.name().expect("relation column name should decode").to_string()
                    })
                    .collect();

                markers.push(StreamMarker::Relation(column_names));
            }
            LogicalReplicationMessage::Insert(_) => markers.push(StreamMarker::Insert),
            _ => {}
        }
    }

    markers
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_creates_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let slot_name = test_slot_name("my_slot");
    let create_slot = client.create_slot(&slot_name).await.unwrap();
    assert!(!create_slot.consistent_point.to_string().is_empty());

    let get_slot = client.get_slot(&slot_name).await.unwrap();
    assert!(!get_slot.confirmed_flush_lsn.to_string().is_empty());

    // Since we did not do anything with the slot, we expect the consistent point to
    // be the same as the confirmed flush lsn.
    assert_eq!(create_slot.consistent_point, get_slot.confirmed_flush_lsn);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_and_delete_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let slot_name = test_slot_name("my_slot");

    // Create the slot and verify it exists
    let create_slot = client.create_slot(&slot_name).await.unwrap();
    assert!(!create_slot.consistent_point.to_string().is_empty());

    let get_slot = client.get_slot(&slot_name).await.unwrap();
    assert!(!get_slot.confirmed_flush_lsn.to_string().is_empty());

    // Delete the slot
    client.delete_slot(&slot_name).await.unwrap();

    // Verify the slot no longer exists
    let result = client.get_slot(&slot_name).await;
    assert!(matches!(result, Err(ref err) if err.kind() == ErrorKind::ReplicationSlotNotFound));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_nonexistent_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let slot_name = test_slot_name("nonexistent_slot");

    // Attempt to delete a slot that doesn't exist
    let result = client.delete_slot(&slot_name).await;
    assert!(matches!(result, Err(ref err) if err.kind() == ErrorKind::ReplicationSlotNotFound));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_slot_if_exists_deletes_existing_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let slot_name = test_slot_name("delete_if_exists_slot");
    client.create_slot(&slot_name).await.unwrap();

    client.delete_slot_if_exists(&slot_name).await.unwrap();

    let result = client.get_slot(&slot_name).await;
    assert!(matches!(result, Err(ref err) if err.kind() == ErrorKind::ReplicationSlotNotFound));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_slot_if_exists_on_nonexistent_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let slot_name = test_slot_name("delete_if_exists_nonexistent_slot");
    client.delete_slot_if_exists(&slot_name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_doesnt_recreate_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let slot_name = test_slot_name("my_slot");
    assert!(client.create_slot(&slot_name).await.is_ok());
    assert!(matches!(
        client.create_slot(&slot_name).await,
        Err(ref err) if err.kind() == ErrorKind::ReplicationSlotAlreadyExists
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_reads_wal_sender_timeout() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let wal_sender_timeout = client.get_wal_sender_timeout().await.unwrap();

    assert_eq!(wal_sender_timeout, Some(Duration::from_secs(10)));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_is_consistent() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let age_schema = test_column("age", Type::INT4, 2, true, false);

    let table_1_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1'.
    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // We use the transaction to consistently read the table schemas.
    let table_1_schema = transaction.get_table_schema(table_1_id).await.unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(table_1_schema.id, table_1_id);
    assert_eq!(table_1_schema.name, test_table_name("table_1"));
    assert_table_schema_columns(&table_1_schema, &[id_column_schema(), age_schema.clone()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_across_multiple_connections() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let first_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let second_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let age_schema = test_column("age", Type::INT4, 2, true, false);
    let year_schema = test_column("year", Type::INT4, 3, true, false);

    let table_1_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1'.
    let (transaction, _) =
        first_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // We use the transaction to consistently read the table schemas.
    let table_1_schema = transaction.get_table_schema(table_1_id).await.unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(table_1_schema.id, table_1_id);
    assert_eq!(table_1_schema.name, test_table_name("table_1"));
    assert_table_schema_columns(&table_1_schema, &[id_column_schema(), age_schema.clone()]);

    // We create a new table in the database and update the schema of the old one.
    let table_2_id = database
        .create_table(test_table_name("table_2"), true, &[("year", "integer")])
        .await
        .unwrap();
    database
        .alter_table(
            test_table_name("table_1"),
            &[TableModification::AddColumn { name: "year", data_type: "integer" }],
        )
        .await
        .unwrap();

    // We create the slot when the database schema contains both 'table_1' and
    // 'table_2'.
    let (transaction, _) =
        second_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // We use the transaction to consistently read the table schemas.
    let table_1_schema = transaction.get_table_schema(table_1_id).await.unwrap();
    let table_2_schema = transaction.get_table_schema(table_2_id).await.unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(table_1_schema.id, table_1_id);
    assert_eq!(table_1_schema.name, test_table_name("table_1"));
    assert_table_schema_columns(
        &table_1_schema,
        &[id_column_schema(), age_schema.clone(), year_schema.clone()],
    );
    assert_eq!(table_2_schema.id, table_2_id);
    assert_eq!(table_2_schema.name, test_table_name("table_2"));
    assert_table_schema_columns(&table_2_schema, &[id_column_schema(), year_schema]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_preserves_primary_key_constraint_order() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("composite_pk_order");
    let quoted_table_name = table_name.as_quoted_identifier();
    database
        .run_sql(&format!(
            "create table {quoted_table_name} (
                id integer not null,
                tenant_id integer not null,
                name text,
                primary key (tenant_id, id)
            )"
        ))
        .await
        .unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("composite_pk_order")).await.unwrap();

    let table_id = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace where \
             n.nspname = $1 and c.relname = $2",
            &[&table_name.schema, &table_name.name],
        )
        .await
        .unwrap()
        .get(0);
    let table_schema = transaction.get_table_schema(table_id).await.unwrap();
    transaction.commit().await.unwrap();

    assert_eq!(table_schema.id, table_id);
    assert_eq!(table_schema.name, table_name);

    let id_column = &table_schema.column_schemas[0];
    assert_eq!(id_column.name, "id");
    assert_eq!(id_column.ordinal_position, 1);
    assert_eq!(id_column.primary_key_ordinal_position, Some(2));

    let tenant_id_column = &table_schema.column_schemas[1];
    assert_eq!(tenant_id_column.name, "tenant_id");
    assert_eq!(tenant_id_column.ordinal_position, 2);
    assert_eq!(tenant_id_column.primary_key_ordinal_position, Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ddl_message_primary_key_order_matches_loaded_table_schema() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("ddl_composite_pk_order");
    let quoted_table_name = table_name.as_quoted_identifier();
    database
        .run_sql(&format!(
            "create table {quoted_table_name} (
                id integer not null,
                tenant_id integer not null,
                name text,
                primary key (tenant_id, id)
            )"
        ))
        .await
        .unwrap();

    let table_id = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace where \
             n.nspname = $1 and c.relname = $2",
            &[&table_name.schema, &table_name.name],
        )
        .await
        .unwrap()
        .get(0);

    let publication_name = "ddl_composite_pk_order_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_composite_pk_order_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .unwrap();

    let messages = collect_ddl_messages(stream, 1).await;
    let message = &messages[0];
    assert_eq!(message["identity"]["primary_key_attnums"], serde_json::json!([2, 1]));

    let columns = message["columns"].as_array().unwrap();
    let id_column = columns.iter().find(|column| column["attname"] == "id").unwrap();
    assert_eq!(id_column["attnum"], 1);

    let tenant_id_column = columns.iter().find(|column| column["attname"] == "tenant_id").unwrap();
    assert_eq!(tenant_id_column["attnum"], 2);

    let introspection_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) = introspection_client
        .create_slot_with_transaction(&test_slot_name("ddl_composite_pk_order_read"))
        .await
        .unwrap();
    let table_schema = transaction.get_table_schema(table_id).await.unwrap();
    transaction.commit().await.unwrap();
    let streamed_column_schemas = column_schemas_from_ddl_message(message);

    assert_eq!(streamed_column_schemas, table_schema.column_schemas);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_stream_is_consistent() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // We create a table and insert one row.
    let table_1_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();

    // An earlier version of this test only inserted one row but was
    // incorrectly committing the transaction before the copy stream was done.
    // The test still passed because the copy messages were buffered
    // and the commit was not yet sent to the server.
    // We now insert a larger number of rows to ensure that the copy stream
    // is not buffered and the commit is sent only after the copy stream is done.
    let expected_rows_count = 1_0000;

    database
        .insert_generate_series(test_table_name("table_1"), &["age"], 1, expected_rows_count, 1)
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1' data.
    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // We create a transaction to copy the table data consistently.
    let column_schemas = [test_column("age", Type::INT4, 2, true, false)];
    let stream =
        transaction.get_table_copy_stream(table_1_id, &column_schemas, None).await.unwrap();

    let rows_count = count_stream_rows(stream).await;

    // Transaction should be committed after the copy stream is exhausted.
    transaction.commit().await.unwrap();

    // We expect to have the inserted number of rows.
    assert_eq!(rows_count, expected_rows_count as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_stream_respects_row_filter() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Row filters in publication are only available from Postgres 15+;
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for row filters");
        return;
    }
    // We create a table and insert one row.
    let test_table_name = test_table_name("table_1");
    let test_table_id =
        database.create_table(test_table_name.clone(), true, &[("age", "integer")]).await.unwrap();

    database
        .run_sql(&format!(
            "alter table {} replica identity full",
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "create publication {} for table {} where (age >= 18)",
            quote_identifier("test_pub"),
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // We apply a row filter (age >= 18), so we expect the number of rows
    // post-synchronization to be the numbers 18..=30 when inserting the range
    // 1..=30 (`insert_generate_series` has an inclusive end) We use (18..30+1)
    // since inclusive ranges don't have a `len` for i32.
    let total_rows_count = 30;
    let expected_rows_count = (18..1 + total_rows_count as i32).len();

    database
        .insert_generate_series(test_table_name, &["age"], 1, total_rows_count, 1)
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1' data.
    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // We create a transaction to copy the table data consistently.
    let column_schemas = [test_column("age", Type::INT4, 2, true, false)];
    let stream = transaction
        .get_table_copy_stream(test_table_id, &column_schemas, Some("test_pub"))
        .await
        .unwrap();

    let rows_count = count_stream_rows(stream).await;

    // Transaction should be committed after the copy stream is exhausted.
    transaction.commit().await.unwrap();

    // We expect to have the inserted number of rows.
    assert_eq!(rows_count, expected_rows_count as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_replicated_column_names_respects_column_filter() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // We create a table with multiple columns.
    let test_table_name = test_table_name("table_1");
    let test_table_id = database
        .create_table(
            test_table_name.clone(),
            true,
            &[("name", "text"), ("age", "integer"), ("email", "text")],
        )
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "alter table {} replica identity full",
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create publication with only a subset of columns (excluding 'email').
    let publication_name = "test_pub";
    database
        .run_sql(&format!(
            "create publication {} for table {} (id, name, age)",
            quote_identifier(publication_name),
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // Insert test data with all columns.
    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Alice', 25, 'alice@example.com')",
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Bob', 30, 'bob@example.com')",
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Create the slot when the database schema contains the test data.
    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // Get table schema without publication filter - should include ALL columns.
    let table_schema = transaction.get_table_schema(test_table_id).await.unwrap();

    // Verify all columns are present in the schema.
    assert_eq!(table_schema.id, test_table_id);
    assert_eq!(table_schema.name, test_table_name);
    assert_table_schema_columns(
        &table_schema,
        &[
            id_column_schema(),
            test_column("name", Type::TEXT, 2, true, false),
            test_column("age", Type::INT4, 3, true, false),
            test_column("email", Type::TEXT, 4, true, false),
        ],
    );

    // Get replicated column names from the publication - should only include
    // published columns.
    let replicated_columns = transaction
        .get_replicated_column_names(test_table_id, &table_schema, publication_name)
        .await
        .unwrap();

    // Transaction should be committed after queries are done.
    transaction.commit().await.unwrap();

    // Verify only the published columns are returned (id, name, age - not email).
    assert_eq!(replicated_columns.len(), 3);
    assert!(replicated_columns.contains("id"));
    assert!(replicated_columns.contains("name"));
    assert!(replicated_columns.contains("age"));
    assert!(!replicated_columns.contains("email"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_replicated_column_names_for_all_tables_publication() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // Create a table with multiple columns.
    let test_table_name = test_table_name("table_1");
    let test_table_id = database
        .create_table(
            test_table_name.clone(),
            true,
            &[("name", "text"), ("age", "integer"), ("email", "text")],
        )
        .await
        .unwrap();

    database
        .run_sql(&format!("alter table {test_table_name} replica identity full"))
        .await
        .unwrap();

    // Create a FOR ALL TABLES publication. Column filtering is NOT supported with
    // this type.
    let publication_name = "test_pub_all_tables";
    database
        .run_sql(&format!("create publication {publication_name} for all tables"))
        .await
        .unwrap();

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // Get table schema.
    let table_schema = transaction.get_table_schema(test_table_id).await.unwrap();

    // Get replicated column names - FOR ALL TABLES doesn't support column
    // filtering, so all columns should be returned.
    let replicated_columns = transaction
        .get_replicated_column_names(test_table_id, &table_schema, publication_name)
        .await
        .unwrap();

    transaction.commit().await.unwrap();

    // All columns should be returned since FOR ALL TABLES doesn't support column
    // filtering.
    assert_eq!(replicated_columns.len(), 4);
    assert!(replicated_columns.contains("id"));
    assert!(replicated_columns.contains("name"));
    assert!(replicated_columns.contains("age"));
    assert!(replicated_columns.contains("email"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_replicated_column_names_for_tables_in_schema_publication() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // Create a table with multiple columns.
    let test_table_name = test_table_name("table_1");
    let test_table_id = database
        .create_table(
            test_table_name.clone(),
            true,
            &[("name", "text"), ("age", "integer"), ("email", "text")],
        )
        .await
        .unwrap();

    database
        .run_sql(&format!("alter table {test_table_name} replica identity full"))
        .await
        .unwrap();

    // Create a FOR TABLES IN SCHEMA publication. Column filtering is NOT supported
    // with this type. Note: Tables are created in the "test" schema by
    // test_table_name().
    let publication_name = "test_pub_schema";
    database
        .run_sql(&format!("create publication {publication_name} for tables in schema test"))
        .await
        .unwrap();

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // Get table schema.
    let table_schema = transaction.get_table_schema(test_table_id).await.unwrap();

    // Get replicated column names - FOR TABLES IN SCHEMA doesn't support column
    // filtering, so all columns should be returned.
    let replicated_columns = transaction
        .get_replicated_column_names(test_table_id, &table_schema, publication_name)
        .await
        .unwrap();

    transaction.commit().await.unwrap();

    // All columns should be returned since FOR TABLES IN SCHEMA doesn't support
    // column filtering.
    assert_eq!(replicated_columns.len(), 4);
    assert!(replicated_columns.contains("id"));
    assert!(replicated_columns.contains("name"));
    assert!(replicated_columns.contains("age"));
    assert!(replicated_columns.contains("email"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_replicated_column_names_errors_when_table_not_in_publication() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // Create a table with multiple columns.
    let table_1_name = test_table_name("table_1");
    let table_1_id = database
        .create_table(table_1_name.clone(), true, &[("name", "text"), ("age", "integer")])
        .await
        .unwrap();

    database.run_sql(&format!("alter table {table_1_name} replica identity full")).await.unwrap();

    // Create a second table that WILL be in the publication.
    let table_2_name = test_table_name("table_2");
    database.create_table(table_2_name.clone(), true, &[("data", "text")]).await.unwrap();

    // Create publication for only the second table, NOT including table_1.
    let publication_name = "test_pub_other";
    database
        .run_sql(&format!("create publication {publication_name} for table {table_2_name}"))
        .await
        .unwrap();

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // Get table schema for the table NOT in the publication.
    let table_schema = transaction.get_table_schema(table_1_id).await.unwrap();

    // Attempting to get replicated column names for a table not in the publication
    // should error.
    let result =
        transaction.get_replicated_column_names(table_1_id, &table_schema, publication_name).await;

    transaction.commit().await.unwrap();

    // Should return a ConfigError since the table is not in the publication.
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConfigError);
    assert!(err.to_string().contains("not included in publication"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_publication_table_ids_errors_when_empty() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Create an empty publication (no tables).
    let publication_name = "test_pub_empty";
    database.run_sql(&format!("create publication {publication_name}")).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // Attempting to get table IDs from an empty publication should error.
    let result = client.get_publication_table_ids(publication_name).await;

    // Should return a ConfigError since the publication has no tables.
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ConfigError);
    assert!(err.to_string().contains("does not contain any tables"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_stream_no_row_filter() {
    init_test_tracing();
    let database = spawn_source_database().await;
    // We create a table and insert one row.
    let test_table_name = test_table_name("table_1");
    let test_table_id =
        database.create_table(test_table_name.clone(), true, &[("age", "integer")]).await.unwrap();

    database
        .run_sql(&format!(
            "alter table {} replica identity full",
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "create publication {} for table {}",
            quote_identifier("test_pub"),
            test_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let expected_rows_count = 30;

    database
        .insert_generate_series(test_table_name, &["age"], 1, expected_rows_count, 1)
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1' data.
    let (transaction, _) =
        parent_client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    // We create a transaction to copy the table data consistently.
    let column_schemas = [test_column("age", Type::INT4, 2, true, false)];
    let stream = transaction
        .get_table_copy_stream(test_table_id, &column_schemas, Some("test_pub"))
        .await
        .unwrap();

    let rows_count = count_stream_rows(stream).await;

    // Transaction should be committed after the copy stream is exhausted.
    transaction.commit().await.unwrap();

    // We expect to have the inserted number of rows.
    assert_eq!(rows_count, expected_rows_count as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publication_creation_and_check() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // We create two tables and a publication on those tables.
    let table_1_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();
    let table_2_id = database
        .create_table(test_table_name("table_2"), true, &[("age", "integer")])
        .await
        .unwrap();
    database
        .create_publication(
            "my_publication",
            &[test_table_name("table_1"), test_table_name("table_2")],
        )
        .await
        .unwrap();

    // We check if the publication exists.
    let publication_exists = parent_client.publication_exists("my_publication").await.unwrap();
    assert!(publication_exists);

    // We check the table names of the tables in the publication.
    let table_names = parent_client.get_publication_table_names("my_publication").await.unwrap();
    assert_eq!(table_names, vec![test_table_name("table_1"), test_table_name("table_2")]);

    // We check the table ids of the tables in the publication.
    let table_ids: HashSet<_> = parent_client
        .get_publication_table_ids("my_publication")
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(table_ids, HashSet::from([table_1_id, table_2_id]));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_publication_table_ids_collapse_partitioned_root() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // We create a partitioned parent with two child partitions.
    let table_name = test_table_name("part_parent");
    let (parent_table_id, _children) = create_partitioned_table(
        &database,
        table_name.clone(),
        &[("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")],
    )
    .await
    .unwrap();

    let publication_name = "pub_part_root";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let id = client.get_publication_table_ids(publication_name).await.unwrap();

    // We expect to get only the parent table id.
    assert_eq!(id, vec![parent_table_id]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_start_logical_replication() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // We create a slot which is going to replicate data before we insert the data.
    let slot_name = test_slot_name("my_slot");
    let slot = parent_client.create_slot(&slot_name).await.unwrap();

    // We create a table with a publication and 10 entries.
    database.create_table(test_table_name("table_1"), true, &[("age", "integer")]).await.unwrap();
    database.create_publication("my_publication", &[test_table_name("table_1")]).await.unwrap();
    for i in 0..10 {
        database
            .insert_values(
                test_table_name("table_1"),
                &["age"],
                &[&i as &(dyn ToSql + Sync + 'static)],
            )
            .await
            .unwrap();
    }

    // We start the cdc of events from the consistent point.
    let stream = parent_client
        .start_logical_replication("my_publication", &slot_name, slot.consistent_point)
        .await
        .unwrap();
    let counts = count_stream_components(stream, |counts| counts.insert_count == 10).await;
    assert_eq!(counts.insert_count, 10);

    // We create a new connection and start another replication instance from the
    // same slot to check if the same data is received.
    let parent_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // We try to stream again from that consistent point and see if we get the same
    // data.
    let stream = parent_client
        .start_logical_replication("my_publication", &slot_name, slot.consistent_point)
        .await
        .unwrap();
    let counts = count_stream_components(stream, |counts| counts.insert_count == 10).await;
    assert_eq!(counts.insert_count, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_change_messages_emit_enriched_payload_for_multiple_alter_table_variants() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("ddl_message_payload");
    let quoted_table_name = table_name.as_quoted_identifier();
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null default 0")],
        )
        .await
        .unwrap();

    let publication_name = "ddl_message_payload_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_message_payload_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "email",
                data_type: "text not null default 'unknown@example.com'",
            }],
        )
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {quoted_table_name} (name, age, email) values ('alice', 30, \
             'alice@example.com')"
        ))
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "name", new_name: "full_name" }],
        )
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {quoted_table_name} (full_name, age, email) values ('alice 2', 31, \
             'alice2@example.com')"
        ))
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn { name: "age", alteration: "type bigint" }],
        )
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {quoted_table_name} (full_name, age, email) values ('alice 3', 32, \
             'alice3@example.com')"
        ))
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::ReplicaIdentity { value: "full" }])
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {quoted_table_name} (full_name, age, email) values ('alice 4', 33, \
             'alice4@example.com')"
        ))
        .await
        .unwrap();

    let messages = collect_ddl_messages(stream, 4).await;
    assert_eq!(messages.len(), 4);

    let add_column_message = &messages[0];
    assert_eq!(add_column_message["command_tag"], "ALTER TABLE");
    assert_eq!(add_column_message["oid"], table_id.into_inner() as u64);
    assert_eq!(add_column_message["nspname"], table_name.schema);
    assert_eq!(add_column_message["relname"], table_name.name);
    assert_eq!(add_column_message["relkind"], "r");
    assert!(
        add_column_message["current_query"]
            .as_str()
            .expect("current_query should be present")
            .contains("add column email"),
        "statement text should mention the executed alter table statement"
    );
    assert_eq!(add_column_message["identity"]["primary_key_attnums"], serde_json::json!([1]));
    assert_eq!(add_column_message["identity"]["relreplident"], "d");
    assert!(
        add_column_message["commands"]
            .as_array()
            .expect("commands should be an array")
            .iter()
            .any(|command| command["command_tag"] == "ALTER TABLE"),
        "commands should include the alter table base command"
    );
    let add_columns = add_column_message["columns"].as_array().expect("columns should be an array");
    let email_column = add_columns
        .iter()
        .find(|column| column["attname"] == "email")
        .expect("email column should be present after add column");
    assert_eq!(email_column["attnum"], 4);
    assert_eq!(email_column["atttypid"], Type::TEXT.oid() as u64);
    assert_eq!(email_column["typname"], "text");
    assert_eq!(email_column["formatted_type"], "text");
    assert_eq!(email_column["attnotnull"], true);
    assert_eq!(email_column["atthasdef"], true);
    assert!(
        email_column["default_expression"]
            .as_str()
            .expect("default expression should exist")
            .contains("unknown@example.com"),
        "default expression should include the added default value"
    );

    let rename_message = &messages[1];
    let rename_columns = rename_message["columns"].as_array().expect("columns should be an array");
    assert!(
        rename_columns
            .iter()
            .any(|column| column["attname"] == "full_name" && column["attnum"] == 2),
        "rename should preserve attnum and expose the new column name"
    );
    assert!(
        rename_message["commands"]
            .as_array()
            .expect("commands should be an array")
            .iter()
            .any(|command| command["object_type"] == "table column"),
        "rename column should emit table column command metadata"
    );

    let alter_type_message = &messages[2];
    let alter_type_columns =
        alter_type_message["columns"].as_array().expect("columns should be an array");
    let age_column = alter_type_columns
        .iter()
        .find(|column| column["attname"] == "age")
        .expect("age column should still be present after type change");
    assert_eq!(age_column["attnum"], 3);
    assert_eq!(age_column["atttypid"], Type::INT8.oid() as u64);
    assert_eq!(age_column["typname"], "int8");
    assert_eq!(age_column["formatted_type"], "bigint");

    let replica_identity_message = &messages[3];
    assert_eq!(replica_identity_message["identity"]["relreplident"], "f");
    let replica_identity_columns =
        replica_identity_message["columns"].as_array().expect("columns should be an array");
    assert_eq!(replica_identity_columns.len(), 4);
    assert!(
        replica_identity_columns.iter().any(|column| column["attname"] == "full_name"),
        "replica identity change should keep the current post-ddl schema snapshot"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_change_messages_skip_unpublished_and_temporary_tables() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let published_table = test_table_name("ddl_filter_published");
    let unpublished_table = test_table_name("ddl_filter_unpublished");

    database
        .create_table(published_table.clone(), true, &[("name", "text not null")])
        .await
        .unwrap();
    database
        .create_table(unpublished_table.clone(), true, &[("name", "text not null")])
        .await
        .unwrap();

    let publication_name = "ddl_filter_pub";
    database
        .create_publication(publication_name, std::slice::from_ref(&published_table))
        .await
        .unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_filter_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    database
        .alter_table(
            unpublished_table,
            &[TableModification::AddColumn { name: "note", data_type: "text" }],
        )
        .await
        .unwrap();

    database.run_sql("create temporary table temp_ddl_filter (id integer)").await.unwrap();
    database.run_sql("alter table temp_ddl_filter add column note text").await.unwrap();

    let result = timeout(Duration::from_secs(2), collect_ddl_messages(stream, 1)).await;
    assert!(
        result.is_err(),
        "ddl on unpublished or temporary tables should not emit supabase_etl_ddl messages"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_change_messages_allow_alter_table_from_table_owner_role() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("ddl_table_owner_role");
    database.create_table(table_name.clone(), true, &[("name", "text not null")]).await.unwrap();

    let role_name = "ddl_table_owner_role_user";
    let quoted_role_name = quote_identifier(role_name);
    database.run_sql(&format!("create role {quoted_role_name}")).await.unwrap();
    database
        .run_sql(&format!(
            "grant usage on schema {} to {quoted_role_name}",
            quote_identifier(&table_name.schema)
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "alter table {} owner to {quoted_role_name}",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "ddl_table_owner_role_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_table_owner_role_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    database.run_sql(&format!("set role {quoted_role_name}")).await.unwrap();
    let alter_result = database
        .alter_table(
            table_name,
            &[TableModification::AddColumn { name: "note", data_type: "text" }],
        )
        .await;
    database.run_sql("reset role").await.unwrap();
    alter_result.unwrap();

    let messages = collect_ddl_messages(stream, 1).await;
    let columns = messages[0]["columns"].as_array().unwrap();
    assert!(
        columns.iter().any(|column| column["attname"] == "note"),
        "ddl emitted by a table owner role should include the new column"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_alter_table_statement_with_multiple_subcommands_emits_one_ddl_message() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("ddl_multi_subcommand");
    let quoted_table_name = table_name.as_quoted_identifier();

    database.create_table(table_name.clone(), true, &[("name", "text not null")]).await.unwrap();

    let publication_name = "ddl_multi_subcommand_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_multi_subcommand_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[
                TableModification::AddColumn {
                    name: "email",
                    data_type: "text not null default 'unknown@example.com'",
                },
                TableModification::AddColumn {
                    name: "status",
                    data_type: "text not null default 'pending'",
                },
            ],
        )
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {quoted_table_name} (name, email, status) values ('alice', \
             'alice@example.com', 'active')"
        ))
        .await
        .unwrap();

    let counts = count_stream_components(stream, |counts| {
        counts.insert_count == 1 && counts.relation_count == 1 && counts.ddl_message_count == 1
    })
    .await;

    assert_eq!(counts.ddl_message_count, 1);
    assert_eq!(counts.message_count, 1);
    assert_eq!(counts.relation_count, 1);
    assert_eq!(counts.insert_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_change_messages_respect_skip_ddl_log_setting() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("ddl_skip_log");
    database.create_table(table_name.clone(), true, &[("name", "text not null")]).await.unwrap();

    let publication_name = "ddl_skip_log_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_skip_log_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    database.run_sql("set supabase_etl.skip_ddl_log = 'true'").await.unwrap();
    database
        .alter_table(
            table_name,
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(2), collect_ddl_messages(stream, 1)).await;
    assert!(
        result.is_err(),
        "ddl messages should be suppressed when supabase_etl.skip_ddl_log is enabled"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_same_transaction_ddl_messages_precede_relation_and_insert_in_pgoutput() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let table_name = test_table_name("ddl_message_ordering");
    database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null default 0")],
        )
        .await
        .unwrap();

    let publication_name = "ddl_message_ordering_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("ddl_message_ordering_slot");
    let slot = client.create_slot(&slot_name).await.unwrap();
    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();

    let transaction = database.begin_transaction().await;

    transaction
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "status",
                data_type: "text not null default 'pending'",
            }],
        )
        .await
        .unwrap();
    transaction
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"alice", &30, &"active"])
        .await
        .unwrap();
    transaction
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();
    transaction
        .insert_values(table_name.clone(), &["name", "status"], &[&"bob", &"pending"])
        .await
        .unwrap();

    transaction.commit_transaction().await;

    let markers = collect_stream_markers(stream, 8).await;
    assert_eq!(
        markers,
        vec![
            StreamMarker::Begin,
            StreamMarker::DdlMessage(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
                "status".to_string(),
            ]),
            StreamMarker::Relation(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
                "status".to_string(),
            ]),
            StreamMarker::Insert,
            StreamMarker::DdlMessage(vec![
                "id".to_string(),
                "name".to_string(),
                "status".to_string(),
            ]),
            StreamMarker::Relation(vec![
                "id".to_string(),
                "name".to_string(),
                "status".to_string(),
            ]),
            StreamMarker::Insert,
            StreamMarker::Commit,
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_slot_state_returns_valid_for_healthy_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // Create a slot
    let slot_name = test_slot_name("healthy_slot");
    client.create_slot(&slot_name).await.unwrap();

    // Check the slot state - it should be valid
    let slot_state = client.get_slot_state(&slot_name).await.unwrap();
    assert_eq!(slot_state, SlotState::Valid);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_slot_state_returns_error_for_nonexistent_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // Try to get state for a slot that doesn't exist
    let slot_name = test_slot_name("nonexistent_slot");
    let result = client.get_slot_state(&slot_name).await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::ReplicationSlotNotFound);
}

// Serialized via nextest test-group "shared-pg" (shares the source PG cluster).
#[tokio::test(flavor = "multi_thread")]
async fn exclusive_get_slot_state_returns_invalidated_for_lost_slot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    // Create a slot
    let slot_name = test_slot_name("invalidated_slot");
    client.create_slot(&slot_name).await.unwrap();

    // Verify the slot is initially valid
    let slot_state = client.get_slot_state(&slot_name).await.unwrap();
    assert_eq!(slot_state, SlotState::Valid, "Slot should be valid initially");

    // Try to invalidate the slot using the database helper
    database.invalidate_slot(&slot_name).await;

    // Verify the slot state is now Invalidated
    let slot_state = client.get_slot_state(&slot_name).await.unwrap();
    assert_eq!(
        slot_state,
        SlotState::Invalidated,
        "Slot should be invalidated after exceeding WAL limit"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plan_ctid_partitions_returns_correct_partitions() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let table_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();

    let expected_rows: i64 = 100;
    database
        .insert_generate_series(test_table_name("table_1"), &["age"], 1, expected_rows, 1)
        .await
        .unwrap();

    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    let partitions = transaction.plan_ctid_partitions(table_id, 4).await.unwrap();

    assert!(!partitions.is_empty(), "expected at least one partition for non-empty table");
    assert!(partitions.len() <= 4, "planner should not exceed requested partitions");

    if partitions.len() == 1 {
        assert!(matches!(partitions.first(), Some(CtidPartition::OpenEnd { .. })));
    } else {
        assert!(matches!(partitions.first(), Some(CtidPartition::OpenStart { .. })));
        assert!(matches!(partitions.last(), Some(CtidPartition::OpenEnd { .. })));
    }

    for partition in &partitions {
        match partition {
            CtidPartition::OpenStart { end_tid } => {
                assert!(
                    end_tid.starts_with('(') && end_tid.ends_with(')'),
                    "end_tid should be a valid tid format: {end_tid}"
                );
            }
            CtidPartition::Closed { start_tid, end_tid } => {
                assert!(
                    start_tid.starts_with('(') && start_tid.ends_with(')'),
                    "start_tid should be a valid tid format: {start_tid}"
                );
                assert!(
                    end_tid.starts_with('(') && end_tid.ends_with(')'),
                    "end_tid should be a valid tid format: {end_tid}"
                );
            }
            CtidPartition::OpenEnd { start_tid } => {
                assert!(
                    start_tid.starts_with('(') && start_tid.ends_with(')'),
                    "start_tid should be a valid tid format: {start_tid}"
                );
            }
        }
    }

    transaction.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plan_ctid_partitions_returns_empty_for_empty_table() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let table_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();

    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    let partitions = transaction.plan_ctid_partitions(table_id, 4).await.unwrap();

    assert!(partitions.is_empty(), "expected no partitions for empty table");

    transaction.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_stream_with_ctid_partition() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();

    let table_id = database
        .create_table(test_table_name("table_1"), true, &[("age", "integer")])
        .await
        .unwrap();

    let expected_rows: i64 = 100;
    database
        .insert_generate_series(test_table_name("table_1"), &["age"], 1, expected_rows, 1)
        .await
        .unwrap();

    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("my_slot")).await.unwrap();

    let column_schemas = &[ColumnSchema::new("age".to_string(), Type::INT4, -1, 1, None, true)];

    let partitions = transaction.plan_ctid_partitions(table_id, 4).await.unwrap();
    assert!(!partitions.is_empty(), "expected at least one partition for non-empty table");
    assert!(partitions.len() <= 4, "planner should not exceed requested partitions");

    // Export the snapshot so child connections can share it.
    let snapshot_id = transaction.export_snapshot().await.unwrap();
    assert!(!snapshot_id.is_empty(), "snapshot id should not be empty");

    let mut total_rows: u64 = 0;
    for partition in &partitions {
        let child = transaction.get_cloned_client().fork_child().await.unwrap();
        let child_tx = PgReplicationChildTransaction::new(child, &snapshot_id).await.unwrap();

        let stream = child_tx
            .get_table_copy_stream_with_ctid_partition(table_id, column_schemas, None, partition)
            .await
            .unwrap();

        total_rows += count_stream_rows(stream).await;
        child_tx.commit().await.unwrap();
    }

    transaction.commit().await.unwrap();

    assert_eq!(
        total_rows, expected_rows as u64,
        "total rows across all partitions should match inserted rows"
    );
}
