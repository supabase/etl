use etl::v2::replication::client::{PgReplicationClient, PgReplicationError};
use futures::StreamExt;
use postgres::schema::ColumnSchema;
use postgres::tokio::test_utils::{id_column_schema, TableModification};
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use postgres_replication::LogicalReplicationStream;
use telemetry::init_test_tracing;
use tokio::pin;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::CopyOutStream;

use crate::common::database::{spawn_database, test_table_name};
use crate::common::pipeline::test_slot_name;
use crate::common::table::assert_table_schema;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_creates_slot() {
    init_test_tracing();
    let database = spawn_database().await;

    let client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    let slot_name = test_slot_name("my_slot");
    let create_slot = client.create_slot(&slot_name).await.unwrap();
    assert!(!create_slot.consistent_point.to_string().is_empty());

    let get_slot = client.get_slot(&slot_name).await.unwrap();
    assert!(!get_slot.confirmed_flush_lsn.to_string().is_empty());

    // Since we did not do anything with the slot, we expect the consistent point to be the same
    // as the confirmed flush lsn.
    assert_eq!(create_slot.consistent_point, get_slot.confirmed_flush_lsn);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_and_delete_slot() {
    init_test_tracing();
    let database = spawn_database().await;

    let client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

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
    assert!(matches!(result, Err(PgReplicationError::SlotNotFound(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_nonexistent_slot() {
    init_test_tracing();
    let database = spawn_database().await;

    let client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    let slot_name = test_slot_name("nonexistent_slot");

    // Attempt to delete a slot that doesn't exist
    let result = client.delete_slot(&slot_name).await;
    assert!(matches!(result, Err(PgReplicationError::SlotNotFound(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replication_client_doesnt_recreate_slot() {
    init_test_tracing();
    let database = spawn_database().await;

    let client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    let slot_name = test_slot_name("my_slot");
    assert!(client.create_slot(&slot_name).await.is_ok());
    assert!(matches!(
        client.create_slot(&slot_name).await,
        Err(PgReplicationError::SlotAlreadyExists(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_is_consistent() {
    init_test_tracing();
    let database = spawn_database().await;

    let client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    let age_schema = ColumnSchema {
        name: "age".to_string(),
        typ: Type::INT4,
        modifier: -1,
        nullable: true,
        primary: false,
    };

    let table_1_id = database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1'.
    let (transaction, _) = client
        .create_slot_with_transaction(&test_slot_name("my_slot"))
        .await
        .unwrap();

    // We use the transaction to consistently read the table schemas.
    let table_1_schema = transaction
        .get_table_schemas(&[table_1_id], None)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_table_schema(
        &table_1_schema,
        table_1_id,
        test_table_name("table_1"),
        &[id_column_schema(), age_schema.clone()],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_schema_copy_across_multiple_connections() {
    init_test_tracing();
    let database = spawn_database().await;

    let first_client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();
    let second_client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    let age_schema = ColumnSchema {
        name: "age".to_string(),
        typ: Type::INT4,
        modifier: -1,
        nullable: true,
        primary: false,
    };
    let year_schema = ColumnSchema {
        name: "year".to_string(),
        typ: Type::INT4,
        modifier: -1,
        nullable: true,
        primary: false,
    };

    let table_1_id = database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1'.
    let (transaction, _) = first_client
        .create_slot_with_transaction(&test_slot_name("my_slot"))
        .await
        .unwrap();

    // We use the transaction to consistently read the table schemas.
    let table_1_schema = transaction
        .get_table_schemas(&[table_1_id], None)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_table_schema(
        &table_1_schema,
        table_1_id,
        test_table_name("table_1"),
        &[id_column_schema(), age_schema.clone()],
    );

    // We create a new table in the database and update the schema of the old one.
    let table_2_id = database
        .create_table(test_table_name("table_2"), &[("year", "integer")])
        .await
        .unwrap();
    database
        .alter_table(
            test_table_name("table_1"),
            &[TableModification::AddColumn {
                name: "year",
                data_type: "integer",
            }],
        )
        .await
        .unwrap();

    // We create the slot when the database schema contains both 'table_1' and 'table_2'.
    let (transaction, _) = second_client
        .create_slot_with_transaction(&test_slot_name("my_slot"))
        .await
        .unwrap();

    // We use the transaction to consistently read the table schemas.
    let table_1_schema = transaction
        .get_table_schemas(&[table_1_id], None)
        .await
        .unwrap();
    let table_2_schema = transaction
        .get_table_schemas(&[table_2_id], None)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_table_schema(
        &table_1_schema,
        table_1_id,
        test_table_name("table_1"),
        &[id_column_schema(), age_schema.clone(), year_schema.clone()],
    );
    assert_table_schema(
        &table_2_schema,
        table_2_id,
        test_table_name("table_2"),
        &[id_column_schema(), year_schema],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_copy_stream_is_consistent() {
    init_test_tracing();
    let database = spawn_database().await;

    let parent_client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    // We create a table and insert one row.
    let table_1_id = database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
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
        .insert_generate_series(
            test_table_name("table_1"),
            &["age"],
            1,
            expected_rows_count,
            1,
        )
        .await
        .unwrap();

    // We create the slot when the database schema contains only 'table_1' data.
    let (transaction, _) = parent_client
        .create_slot_with_transaction(&test_slot_name("my_slot"))
        .await
        .unwrap();

    // We create a transaction to copy the table data consistently.
    let stream = transaction
        .get_table_copy_stream(
            table_1_id,
            &[ColumnSchema {
                name: "age".to_string(),
                typ: Type::INT4,
                modifier: -1,
                nullable: true,
                primary: false,
            }],
        )
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
    let database = spawn_database().await;

    let parent_client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    // We create two tables and a publication on those tables.
    let table_1_id = database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
        .await
        .unwrap();
    let table_2_id = database
        .create_table(test_table_name("table_2"), &[("age", "integer")])
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
    let publication_exists = parent_client
        .publication_exists("my_publication")
        .await
        .unwrap();
    assert!(publication_exists);

    // We check the table names of the tables in the publication.
    let table_names = parent_client
        .get_publication_table_names("my_publication")
        .await
        .unwrap();
    assert_eq!(
        table_names,
        vec![test_table_name("table_1"), test_table_name("table_2")]
    );

    // We check the table ids of the tables in the publication.
    let table_ids = parent_client
        .get_publication_table_ids("my_publication")
        .await
        .unwrap();
    assert_eq!(table_ids, vec![table_1_id, table_2_id]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_start_logical_replication() {
    init_test_tracing();
    let database = spawn_database().await;

    let parent_client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    // We create a slot which is going to replicate data before we insert the data.
    let slot_name = test_slot_name("my_slot");
    let slot = parent_client.create_slot(&slot_name).await.unwrap();

    // We create a table with a publication and 10 entries.
    database
        .create_table(test_table_name("table_1"), &[("age", "integer")])
        .await
        .unwrap();
    database
        .create_publication("my_publication", &[test_table_name("table_1")])
        .await
        .unwrap();
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

    // We create a new connection and start another replication instance from the same slot to check
    // if the same data is received.
    let parent_client = PgReplicationClient::connect(database.config.clone())
        .await
        .unwrap();

    // We try to stream again from that consistent point and see if we get the same data.
    let stream = parent_client
        .start_logical_replication("my_publication", &slot_name, slot.consistent_point)
        .await
        .unwrap();
    let counts = count_stream_components(stream, |counts| counts.insert_count == 10).await;
    assert_eq!(counts.insert_count, 10);
}
