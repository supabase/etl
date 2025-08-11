use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::config::BatchConfig;
use etl::error::ErrorKind;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::{create_pipeline, create_pipeline_with};
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::test_utils::test_schema::{TableSelection, insert_mock_data, setup_test_database_schema};
use etl::types::{EventType, PgNumeric, PipelineId};
use etl_destinations::bigquery::install_crypto_provider_for_bigquery;
use etl_telemetry::init_test_tracing;
use rand::random;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

use crate::common::bigquery::{
    BigQueryOrder, BigQueryUser, NonNullableColsScalar, NullableColsArray, NullableColsScalar,
    parse_bigquery_table_rows, setup_bigquery_connection,
};

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    init_test_tracing();

    install_crypto_provider_for_bigquery();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let bigquery_database = setup_bigquery_connection().await;

    // Insert initial test data.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery directly to get the data which has been inserted by tests.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![
            BigQueryUser::new(1, "user_1", 1),
            BigQueryUser::new(2, "user_2", 2),
        ]
    );
    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await
        .unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![
            BigQueryOrder::new(1, "description_1"),
            BigQueryOrder::new(2, "description_2"),
        ]
    );

    // We restart the pipeline and check that we can process events since we have load the table
    // schema from the destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We expect 2 insert events for each table (4 total).
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 4)])
        .await;

    // Insert additional data.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery directly to get the data which has been inserted by tests.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![
            BigQueryUser::new(1, "user_1", 1),
            BigQueryUser::new(2, "user_2", 2),
            BigQueryUser::new(3, "user_3", 3),
            BigQueryUser::new(4, "user_4", 4),
        ]
    );
    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await
        .unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![
            BigQueryOrder::new(1, "description_1"),
            BigQueryOrder::new(2, "description_2"),
            BigQueryOrder::new(3, "description_3"),
            BigQueryOrder::new(4, "description_4"),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_insert_update_delete() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let bigquery_database = setup_bigquery_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert a row.
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // We query BigQuery to check for the insert.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_1", 1),]);

    // Wait for the update.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Update the row.
    database
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_10", &10],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    // We query BigQuery to check for the update.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![BigQueryUser::new(1, "user_10", 10),]
    );

    // Wait for the update.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    // Update the row.
    database
        .delete_values(
            database_schema.users_schema().name.clone(),
            &["name"],
            &["'user_10'"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery to check for deletion.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await;
    assert!(users_rows.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_subsequent_updates() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let mut database_1 = spawn_source_database().await;
    let mut database_2 = database_1.duplicate().await;
    let database_schema = setup_test_database_schema(&database_1, TableSelection::UsersOnly).await;

    let bigquery_database = setup_bigquery_connection().await;

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database_1.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1), (EventType::Update, 2)])
        .await;

    // Insert a row.
    database_1
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    // Create two transactions A and B on separate connections to make sure that the updates are
    // ordered correctly.
    let transaction_a = database_1.begin_transaction().await;
    transaction_a
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_3", &3],
        )
        .await
        .unwrap();
    transaction_a.commit_transaction().await;
    let transaction_b = database_2.begin_transaction().await;
    transaction_b
        .update_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_2", &2],
        )
        .await
        .unwrap();
    transaction_b.commit_transaction().await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery to check for the final value.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_2", 2),]);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_truncate_with_batching() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let bigquery_database = setup_bigquery_connection().await;

    // We create table `test_users_1` to simulate an error in the system where a table with that name
    // already exists and should be replaced for replication to work correctly.
    bigquery_database
        .create_table("test_users_1", &[("age", "integer")])
        .await;

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline_with(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        // We use a batch size > 1, so that we can make sure that interleaved truncate statements
        // work well with multiple batches of events.
        Some(BatchConfig {
            max_size: 10,
            max_fill_ms: 1000,
        }),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state(
            database_schema.users_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::SyncDone,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Wait for the 8 inserts (4 per table + 4 after truncate) and 2 truncates (1 per table).
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 8), (EventType::Truncate, 2)])
        .await;

    // Insert 2 rows per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=2,
        false,
    )
    .await;

    // We truncate both tables.
    database
        .truncate_table(database_schema.users_schema().name.clone())
        .await
        .unwrap();
    database
        .truncate_table(database_schema.orders_schema().name.clone())
        .await
        .unwrap();

    // Insert 2 extra rows per each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        3..=4,
        false,
    )
    .await;

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery directly to get the data which tests have inserted, expecting that
    // only the rows after truncation are there.
    let users_rows = bigquery_database
        .query_table(database_schema.users_schema().name)
        .await
        .unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![
            BigQueryUser::new(3, "user_3", 3),
            BigQueryUser::new(4, "user_4", 4),
        ]
    );
    let orders_rows = bigquery_database
        .query_table(database_schema.orders_schema().name)
        .await
        .unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![
            BigQueryOrder::new(3, "description_3"),
            BigQueryOrder::new(4, "description_4")
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_nullable_scalar_columns() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("nullable_cols_scalar");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("b", "bool"),
                ("t", "text"),
                ("i2", "int2"),
                ("i4", "int4"),
                ("i8", "int8"),
                ("f4", "float4"),
                ("f8", "float8"),
                ("n", "numeric"),
                ("by", "bytea"),
                ("d", "date"),
                ("ti", "time"),
                ("ts", "timestamp"),
                ("tstz", "timestamptz"),
                ("u", "uuid"),
                ("j", "json"),
                ("jb", "jsonb"),
                ("o", "oid"),
            ],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &[
                "b", "t", "i2", "i4", "i8", "f4", "f8", "n", "by", "d", "ti", "ts", "tstz", "u",
                "j", "jb", "o",
            ],
            &[
                &Option::<bool>::None,
                &Option::<String>::None,
                &Option::<i16>::None,
                &Option::<i32>::None,
                &Option::<i64>::None,
                &Option::<f32>::None,
                &Option::<f64>::None,
                &Option::<PgNumeric>::None,
                &Option::<Vec<u8>>::None,
                &Option::<NaiveDate>::None,
                &Option::<NaiveTime>::None,
                &Option::<NaiveDateTime>::None,
                &Option::<DateTime<Utc>>::None,
                &Option::<uuid::Uuid>::None,
                &Option::<serde_json::Value>::None,
                &Option::<serde_json::Value>::None,
                &Option::<u32>::None,
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsScalar>(table_rows);
    assert_eq!(parsed_table_rows, vec![NullableColsScalar::all_nulls(1),]);

    // update
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Define test values
    let updated_bool = true;
    let updated_text = "updated_text".to_string();
    let updated_i2 = 42i16;
    let updated_i4 = 1000i32;
    let updated_i8 = 123456789i64;
    let updated_f4 = 3.15f32;
    let updated_f8 = 2.717f64;
    let updated_numeric = PgNumeric::from_str("99.99").unwrap();
    let updated_bytes = b"test_bytes".to_vec();
    let updated_date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
    let updated_time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let updated_timestamp = NaiveDateTime::new(updated_date, updated_time);
    let updated_timestamptz = DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp, Utc);
    let updated_uuid = uuid::Uuid::new_v4();
    let updated_json = serde_json::json!({"key": "value"});
    let updated_jsonb = serde_json::json!({"jsonb": "data"});
    let updated_oid = 12345u32;

    database
        .update_values(
            table_name.clone(),
            &[
                "b", "t", "i2", "i4", "i8", "f4", "f8", "n", "by", "d", "ti", "ts", "tstz", "u",
                "j", "jb", "o",
            ],
            &[
                &Some(updated_bool),
                &Some(updated_text.clone()),
                &Some(updated_i2),
                &Some(updated_i4),
                &Some(updated_i8),
                &Some(updated_f4),
                &Some(updated_f8),
                &Some(updated_numeric.clone()),
                &Some(updated_bytes.clone()),
                &Some(updated_date),
                &Some(updated_time),
                &Some(updated_timestamp),
                &Some(updated_timestamptz),
                &Some(updated_uuid),
                &Some(updated_json.clone()),
                &Some(updated_jsonb.clone()),
                &Some(updated_oid),
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsScalar>(table_rows);

    let expected_row = NullableColsScalar::with_non_null_values(
        1,
        updated_bool,
        updated_text,
        updated_i2,
        updated_i4,
        updated_i8,
        updated_f4,
        updated_f8,
        updated_numeric,
        updated_bytes,
        updated_date,
        updated_time,
        updated_timestamp,
        updated_timestamptz,
        updated_uuid,
        updated_json,
        updated_jsonb,
        updated_oid,
    );

    assert_eq!(parsed_table_rows, vec![expected_row]);

    // delete
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    database
        .delete_values(table_name.clone(), &["id"], &["'1'"], "")
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_nullable_array_columns() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("nullable_cols_array");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("b_arr", "bool[]"),
                ("t_arr", "text[]"),
                ("i2_arr", "int2[]"),
                ("i4_arr", "int4[]"),
                ("i8_arr", "int8[]"),
                ("f4_arr", "float4[]"),
                ("f8_arr", "float8[]"),
                ("n_arr", "numeric[]"),
                ("by_arr", "bytea[]"),
                ("d_arr", "date[]"),
                ("ti_arr", "time[]"),
                ("ts_arr", "timestamp[]"),
                ("tstz_arr", "timestamptz[]"),
                ("u_arr", "uuid[]"),
                ("j_arr", "json[]"),
                ("jb_arr", "jsonb[]"),
                ("o_arr", "oid[]"),
            ],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_array".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert with null arrays
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &[
                "b_arr", "t_arr", "i2_arr", "i4_arr", "i8_arr", "f4_arr", "f8_arr", "n_arr",
                "by_arr", "d_arr", "ti_arr", "ts_arr", "tstz_arr", "u_arr", "j_arr", "jb_arr",
                "o_arr",
            ],
            &[
                &Option::<Vec<bool>>::None,
                &Option::<Vec<String>>::None,
                &Option::<Vec<i16>>::None,
                &Option::<Vec<i32>>::None,
                &Option::<Vec<i64>>::None,
                &Option::<Vec<f32>>::None,
                &Option::<Vec<f64>>::None,
                &Option::<Vec<PgNumeric>>::None,
                &Option::<Vec<Vec<u8>>>::None,
                &Option::<Vec<NaiveDate>>::None,
                &Option::<Vec<NaiveTime>>::None,
                &Option::<Vec<NaiveDateTime>>::None,
                &Option::<Vec<DateTime<Utc>>>::None,
                &Option::<Vec<uuid::Uuid>>::None,
                &Option::<Vec<serde_json::Value>>::None,
                &Option::<Vec<serde_json::Value>>::None,
                &Option::<Vec<u32>>::None,
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsArray>(table_rows);
    // null arrays are returned as empty arrays by big query: See this section:
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_nulls
    assert_eq!(parsed_table_rows, vec![NullableColsArray::all_empty(1),]);

    // update with array values
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Define test array values
    let updated_bool_arr = vec![true, false, true];
    let updated_text_arr = vec!["hello".to_string(), "world".to_string()];
    let updated_i2_arr = vec![1i16, 2i16, 3i16];
    let updated_i4_arr = vec![100i32, 200i32];
    let updated_i8_arr = vec![1000i64, 2000i64, 3000i64];
    let updated_f4_arr = vec![1.5f32, 2.5f32];
    let updated_f8_arr = vec![std::f64::consts::PI, std::f64::consts::E];
    let updated_n_arr = vec![
        PgNumeric::from_str("3.141").unwrap(),
        PgNumeric::from_str("2.718").unwrap(),
    ];
    let updated_bytes_arr = vec![b"test_bytes1".to_vec(), b"test_bytes2".to_vec()];
    let updated_date_arr = vec![
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
    ];
    let updated_time_arr = vec![
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(17, 30, 0).unwrap(),
    ];
    let base_date = NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
    let updated_timestamp_arr = vec![
        NaiveDateTime::new(base_date, NaiveTime::from_hms_opt(10, 30, 0).unwrap()),
        NaiveDateTime::new(base_date, NaiveTime::from_hms_opt(15, 45, 0).unwrap()),
    ];
    let updated_timestamptz_arr = vec![
        DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp_arr[0], Utc),
        DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp_arr[1], Utc),
    ];
    let updated_uuid_arr = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let updated_json_arr = vec![
        serde_json::json!({"key1": "value1"}),
        serde_json::json!({"key2": "value2"}),
    ];
    let updated_jsonb_arr = vec![
        serde_json::json!({"jsonb1": "data1"}),
        serde_json::json!({"jsonb2": "data2"}),
    ];
    let updated_oid_arr = vec![12345u32, 67890u32];

    database
        .update_values(
            table_name.clone(),
            &[
                "b_arr", "t_arr", "i2_arr", "i4_arr", "i8_arr", "f4_arr", "f8_arr", "n_arr",
                "by_arr", "d_arr", "ti_arr", "ts_arr", "tstz_arr", "u_arr", "j_arr", "jb_arr",
                "o_arr",
            ],
            &[
                &Some(updated_bool_arr.clone()),
                &Some(updated_text_arr.clone()),
                &Some(updated_i2_arr.clone()),
                &Some(updated_i4_arr.clone()),
                &Some(updated_i8_arr.clone()),
                &Some(updated_f4_arr.clone()),
                &Some(updated_f8_arr.clone()),
                &Some(updated_n_arr.clone()),
                &Some(updated_bytes_arr.clone()),
                &Some(updated_date_arr.clone()),
                &Some(updated_time_arr.clone()),
                &Some(updated_timestamp_arr.clone()),
                &Some(updated_timestamptz_arr.clone()),
                &Some(updated_uuid_arr.clone()),
                &Some(updated_json_arr.clone()),
                &Some(updated_jsonb_arr.clone()),
                &Some(updated_oid_arr.clone()),
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsArray>(table_rows);

    let expected_row = NullableColsArray::with_non_null_values(
        1,
        updated_bool_arr,
        updated_text_arr,
        updated_i2_arr,
        updated_i4_arr,
        updated_i8_arr,
        updated_f4_arr,
        updated_f8_arr,
        updated_n_arr,
        updated_bytes_arr,
        updated_date_arr,
        updated_time_arr,
        updated_timestamp_arr,
        updated_timestamptz_arr,
        updated_uuid_arr,
        updated_json_arr,
        updated_jsonb_arr,
        updated_oid_arr,
    );

    assert_eq!(parsed_table_rows, vec![expected_row]);

    // delete
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    database
        .delete_values(table_name.clone(), &["id"], &["'1'"], "")
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_non_nullable_scalar_columns() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("non_nullable_cols_scalar");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("b", "bool not null"),
                ("t", "text not null"),
                ("i2", "int2 not null"),
                ("i4", "int4 not null"),
                ("i8", "int8 not null"),
                ("f4", "float4 not null"),
                ("f8", "float8 not null"),
                ("n", "numeric not null"),
                ("by", "bytea not null"),
                ("d", "date not null"),
                ("ti", "time not null"),
                ("ts", "timestamp not null"),
                ("tstz", "timestamptz not null"),
                ("u", "uuid not null"),
                ("j", "json not null"),
                ("jb", "jsonb not null"),
                ("o", "oid not null"),
            ],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_non_null".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert with non-null values
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Define test values - all non-null
    let test_bool = true;
    let test_text = "test_text".to_string();
    let test_i2 = 42i16;
    let test_i4 = 1000i32;
    let test_i8 = 123456789i64;
    let test_f4 = 3.15f32; // Avoid clippy warning for PI approximation
    let test_f8 = 2.717f64; // Avoid clippy warning for E approximation
    let test_numeric = PgNumeric::from_str("99.99").unwrap();
    let test_bytes = b"test_bytes".to_vec();
    let test_date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
    let test_time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let test_timestamp = NaiveDateTime::new(test_date, test_time);
    let test_timestamptz = DateTime::<Utc>::from_naive_utc_and_offset(test_timestamp, Utc);
    let test_uuid = uuid::Uuid::new_v4();
    let test_json = serde_json::json!({"key": "value"});
    let test_jsonb = serde_json::json!({"jsonb": "data"});
    let test_oid = 12345u32;

    database
        .insert_values(
            table_name.clone(),
            &[
                "b", "t", "i2", "i4", "i8", "f4", "f8", "n", "by", "d", "ti", "ts", "tstz", "u",
                "j", "jb", "o",
            ],
            &[
                &test_bool,
                &test_text,
                &test_i2,
                &test_i4,
                &test_i8,
                &test_f4,
                &test_f8,
                &test_numeric,
                &test_bytes,
                &test_date,
                &test_time,
                &test_timestamp,
                &test_timestamptz,
                &test_uuid,
                &test_json,
                &test_jsonb,
                &test_oid,
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NonNullableColsScalar>(table_rows);

    let expected_row = NonNullableColsScalar::new(
        1,
        test_bool,
        test_text.clone(),
        test_i2,
        test_i4,
        test_i8,
        test_f4,
        test_f8,
        test_numeric.clone(),
        test_bytes.clone(),
        test_date,
        test_time,
        test_timestamp,
        test_timestamptz,
        test_uuid,
        test_json.clone(),
        test_jsonb.clone(),
        test_oid,
    );

    assert_eq!(parsed_table_rows, vec![expected_row]);

    // update with different non-null values
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Define updated test values
    let updated_bool = false;
    let updated_text = "updated_text".to_string();
    let updated_i2 = 84i16;
    let updated_i4 = 2000i32;
    let updated_i8 = 987654321i64;
    let updated_f4 = 1.41f32;
    let updated_f8 = 3.14160f64;
    let updated_numeric = PgNumeric::from_str("42.42").unwrap();
    let updated_bytes = b"updated_bytes".to_vec();
    let updated_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let updated_time = NaiveTime::from_hms_opt(9, 15, 30).unwrap();
    let updated_timestamp = NaiveDateTime::new(updated_date, updated_time);
    let updated_timestamptz = DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp, Utc);
    let updated_uuid = uuid::Uuid::new_v4();
    let updated_json = serde_json::json!({"updated": "json"});
    let updated_jsonb = serde_json::json!({"updated": "jsonb"});
    let updated_oid = 54321u32;

    database
        .update_values(
            table_name.clone(),
            &[
                "b", "t", "i2", "i4", "i8", "f4", "f8", "n", "by", "d", "ti", "ts", "tstz", "u",
                "j", "jb", "o",
            ],
            &[
                &updated_bool,
                &updated_text,
                &updated_i2,
                &updated_i4,
                &updated_i8,
                &updated_f4,
                &updated_f8,
                &updated_numeric,
                &updated_bytes,
                &updated_date,
                &updated_time,
                &updated_timestamp,
                &updated_timestamptz,
                &updated_uuid,
                &updated_json,
                &updated_jsonb,
                &updated_oid,
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NonNullableColsScalar>(table_rows);

    let expected_updated_row = NonNullableColsScalar::new(
        1,
        updated_bool,
        updated_text,
        updated_i2,
        updated_i4,
        updated_i8,
        updated_f4,
        updated_f8,
        updated_numeric,
        updated_bytes,
        updated_date,
        updated_time,
        updated_timestamp,
        updated_timestamptz,
        updated_uuid,
        updated_json,
        updated_jsonb,
        updated_oid,
    );

    assert_eq!(parsed_table_rows, vec![expected_updated_row]);

    // delete
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    database
        .delete_values(table_name.clone(), &["id"], &["'1'"], "")
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_non_nullable_array_columns() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("non_nullable_cols_array");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("b_arr", "bool[] not null"),
                ("t_arr", "text[] not null"),
                ("i2_arr", "int2[] not null"),
                ("i4_arr", "int4[] not null"),
                ("i8_arr", "int8[] not null"),
                ("f4_arr", "float4[] not null"),
                ("f8_arr", "float8[] not null"),
                ("n_arr", "numeric[] not null"),
                ("by_arr", "bytea[] not null"),
                ("d_arr", "date[] not null"),
                ("ti_arr", "time[] not null"),
                ("ts_arr", "timestamp[] not null"),
                ("tstz_arr", "timestamptz[] not null"),
                ("u_arr", "uuid[] not null"),
                ("j_arr", "json[] not null"),
                ("jb_arr", "jsonb[] not null"),
                ("o_arr", "oid[] not null"),
            ],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_non_null_array".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert with non-null array values
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Define test array values - all non-null
    let test_bool_arr = vec![true, false, true];
    let test_text_arr = vec!["hello".to_string(), "world".to_string()];
    let test_i2_arr = vec![1i16, 2i16, 3i16];
    let test_i4_arr = vec![100i32, 200i32];
    let test_i8_arr = vec![1000i64, 2000i64, 3000i64];
    let test_f4_arr = vec![1.5f32, 2.5f32];
    let test_f8_arr = vec![std::f64::consts::PI, std::f64::consts::E];
    let test_n_arr = vec![
        PgNumeric::from_str("3.141").unwrap(),
        PgNumeric::from_str("2.718").unwrap(),
    ];
    let test_bytes_arr = vec![b"test_bytes1".to_vec(), b"test_bytes2".to_vec()];
    let test_date_arr = vec![
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2023, 12, 31).unwrap(),
    ];
    let test_time_arr = vec![
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(17, 30, 0).unwrap(),
    ];
    let base_date = NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
    let test_timestamp_arr = vec![
        NaiveDateTime::new(base_date, NaiveTime::from_hms_opt(10, 30, 0).unwrap()),
        NaiveDateTime::new(base_date, NaiveTime::from_hms_opt(15, 45, 0).unwrap()),
    ];
    let test_timestamptz_arr = vec![
        DateTime::<Utc>::from_naive_utc_and_offset(test_timestamp_arr[0], Utc),
        DateTime::<Utc>::from_naive_utc_and_offset(test_timestamp_arr[1], Utc),
    ];
    let test_uuid_arr = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let test_json_arr = vec![
        serde_json::json!({"key1": "value1"}),
        serde_json::json!({"key2": "value2"}),
    ];
    let test_jsonb_arr = vec![
        serde_json::json!({"jsonb1": "data1"}),
        serde_json::json!({"jsonb2": "data2"}),
    ];
    let test_oid_arr = vec![12345u32, 67890u32];

    database
        .insert_values(
            table_name.clone(),
            &[
                "b_arr", "t_arr", "i2_arr", "i4_arr", "i8_arr", "f4_arr", "f8_arr", "n_arr",
                "by_arr", "d_arr", "ti_arr", "ts_arr", "tstz_arr", "u_arr", "j_arr", "jb_arr",
                "o_arr",
            ],
            &[
                &test_bool_arr,
                &test_text_arr,
                &test_i2_arr,
                &test_i4_arr,
                &test_i8_arr,
                &test_f4_arr,
                &test_f8_arr,
                &test_n_arr,
                &test_bytes_arr,
                &test_date_arr,
                &test_time_arr,
                &test_timestamp_arr,
                &test_timestamptz_arr,
                &test_uuid_arr,
                &test_json_arr,
                &test_jsonb_arr,
                &test_oid_arr,
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsArray>(table_rows);

    let expected_row = NullableColsArray::with_non_null_values(
        1,
        test_bool_arr.clone(),
        test_text_arr.clone(),
        test_i2_arr.clone(),
        test_i4_arr.clone(),
        test_i8_arr.clone(),
        test_f4_arr.clone(),
        test_f8_arr.clone(),
        test_n_arr.clone(),
        test_bytes_arr.clone(),
        test_date_arr.clone(),
        test_time_arr.clone(),
        test_timestamp_arr.clone(),
        test_timestamptz_arr.clone(),
        test_uuid_arr.clone(),
        test_json_arr.clone(),
        test_jsonb_arr.clone(),
        test_oid_arr.clone(),
    );

    assert_eq!(parsed_table_rows, vec![expected_row]);

    // update with different non-null array values
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Update, 1)])
        .await;

    // Define updated test array values
    let updated_bool_arr = vec![false, true];
    let updated_text_arr = vec![
        "updated".to_string(),
        "arrays".to_string(),
        "test".to_string(),
    ];
    let updated_i2_arr = vec![10i16, 20i16];
    let updated_i4_arr = vec![500i32, 600i32, 700i32];
    let updated_i8_arr = vec![5000i64, 6000i64];
    let updated_f4_arr = vec![std::f32::consts::PI, 2.71f32, 1.41f32];
    let updated_f8_arr = vec![std::f64::consts::E, std::f64::consts::PI];
    let updated_n_arr = vec![
        PgNumeric::from_str("1.2").unwrap(),
        PgNumeric::from_str("2.2").unwrap(),
    ];
    let updated_bytes_arr = vec![b"updated1".to_vec(), b"updated2".to_vec()];
    let updated_date_arr = vec![
        NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
        NaiveDate::from_ymd_opt(2024, 6, 30).unwrap(),
    ];
    let updated_time_arr = vec![
        NaiveTime::from_hms_opt(8, 15, 30).unwrap(),
        NaiveTime::from_hms_opt(18, 45, 15).unwrap(),
    ];
    let updated_base_date = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
    let updated_timestamp_arr = vec![
        NaiveDateTime::new(
            updated_base_date,
            NaiveTime::from_hms_opt(11, 20, 10).unwrap(),
        ),
        NaiveDateTime::new(
            updated_base_date,
            NaiveTime::from_hms_opt(16, 40, 50).unwrap(),
        ),
    ];
    let updated_timestamptz_arr = vec![
        DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp_arr[0], Utc),
        DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp_arr[1], Utc),
    ];
    let updated_uuid_arr = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let updated_json_arr = vec![
        serde_json::json!({"updated": "json1"}),
        serde_json::json!({"updated": "json2"}),
    ];
    let updated_jsonb_arr = vec![
        serde_json::json!({"updated": "jsonb1"}),
        serde_json::json!({"updated": "jsonb2"}),
    ];
    let updated_oid_arr = vec![98765u32, 54321u32];

    database
        .update_values(
            table_name.clone(),
            &[
                "b_arr", "t_arr", "i2_arr", "i4_arr", "i8_arr", "f4_arr", "f8_arr", "n_arr",
                "by_arr", "d_arr", "ti_arr", "ts_arr", "tstz_arr", "u_arr", "j_arr", "jb_arr",
                "o_arr",
            ],
            &[
                &updated_bool_arr,
                &updated_text_arr,
                &updated_i2_arr,
                &updated_i4_arr,
                &updated_i8_arr,
                &updated_f4_arr,
                &updated_f8_arr,
                &updated_n_arr,
                &updated_bytes_arr,
                &updated_date_arr,
                &updated_time_arr,
                &updated_timestamp_arr,
                &updated_timestamptz_arr,
                &updated_uuid_arr,
                &updated_json_arr,
                &updated_jsonb_arr,
                &updated_oid_arr,
            ],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsArray>(table_rows);

    let expected_updated_row = NullableColsArray::with_non_null_values(
        1,
        updated_bool_arr,
        updated_text_arr,
        updated_i2_arr,
        updated_i4_arr,
        updated_i8_arr,
        updated_f4_arr,
        updated_f8_arr,
        updated_n_arr,
        updated_bytes_arr,
        updated_date_arr,
        updated_time_arr,
        updated_timestamp_arr,
        updated_timestamptz_arr,
        updated_uuid_arr,
        updated_json_arr,
        updated_jsonb_arr,
        updated_oid_arr,
    );

    assert_eq!(parsed_table_rows, vec![expected_updated_row]);

    // delete
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Delete, 1)])
        .await;

    database
        .delete_values(table_name.clone(), &["id"], &["'1'"], "")
        .await
        .unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_array_with_null_values() {
    init_test_tracing();
    install_crypto_provider_for_bigquery();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_connection().await;
    let table_name = test_table_name("array_with_nulls");
    let table_id = database
        .create_table(table_name.clone(), true, &[("int_array", "int4[]")])
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let raw_destination = bigquery_database.build_destination(store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_array_nulls".to_string();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert array with null value
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (int_array) VALUES (ARRAY[1, NULL])",
                table_name.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    // We sleep to wait for the event to be processed. This is not ideal, but if we wanted to do
    // this better, we would have to also implement error handling within the apply worker to write
    // in the state store.
    sleep(Duration::from_secs(1)).await;

    // Wait for the pipeline expecting an error to be returned.
    let err = pipeline.shutdown_and_wait().await.err().unwrap();
    assert_eq!(err.kinds().len(), 1);
    assert_eq!(err.kinds()[0], ErrorKind::NullValuesNotSupportedInArray);

    // Reset and try with valid array (no nulls)
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "DELETE FROM {} WHERE true",
                table_name.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    // We have to reset the state of the table and copy it from scratch, otherwise the CDC will contain
    // the inserts and deletes, failing again.
    store.reset_table_state(table_id).await.unwrap();
    // We also clear the events so that it's more idiomatic to wait for them, since we don't have
    // the insert of before.
    destination.clear_events().await;

    // We recreate the pipeline and try again.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification = store
        .notify_on_table_state(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Insert, 1)])
        .await;

    // Insert array without null values
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {} (int_array) VALUES (ARRAY[1, 2, 3])",
                table_name.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the valid array was successfully replicated to BigQuery
    let table_rows = bigquery_database
        .query_table(table_name.clone())
        .await
        .unwrap();

    // Check that there is only the valid row in BigQuery
    assert_eq!(table_rows.len(), 1);

    // Check that the int array contains 3 elements, meaning it must be the second insert with all
    // NON-NULL values
    let row = &table_rows[0];
    if let Some(columns) = &row.columns {
        assert_eq!(columns.len(), 2);
        assert_eq!(
            columns[1].value.clone().unwrap().as_array().unwrap().len(),
            3
        );
    }
}
