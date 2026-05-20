use std::{str::FromStr, sync::Once, time::Duration};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::{
    config::BatchConfig,
    destination::DestinationTypeCompatibility,
    state::table::{TableReplicationPhase, TableReplicationPhaseType},
    store::state::StateStore,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        notifying_store::NotifyingStore,
        pipeline::{create_pipeline, create_pipeline_with_batch_config},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_mock_data, setup_test_database_schema},
    },
    types::{
        Cell, Event, EventType, OldTableRow, PgNumeric, PipelineId, TableRow, UpdatedTableRow,
    },
};
use etl_destinations::bigquery::test_utils::{
    setup_bigquery_database, skip_if_missing_bigquery_env_vars,
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use rand::{Rng, distr::Alphanumeric, random};
use tokio::time::sleep;

/// Ensures crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

/// Installs the default cryptographic provider for rustls in tests.
fn install_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}

use crate::support::bigquery::{
    BigQueryOrder, BigQueryUser, NonNullableColsScalar, NullableColsArray, NullableColsScalar,
    parse_bigquery_table_rows,
};

const REPLICA_IDENTITY_LARGE_TEXT_SIZE_BYTES: usize = 8192;

fn generate_random_ascii_string(length: usize) -> String {
    rand::rng().sample_iter(&Alphanumeric).take(length).map(char::from).collect()
}

fn data_events(events: Vec<Event>) -> Vec<Event> {
    events
        .into_iter()
        .filter(|event| matches!(event, Event::Insert(_) | Event::Update(_) | Event::Delete(_)))
        .collect()
}

fn find_update_event(events: &[Event], update_index: usize) -> &etl::types::UpdateEvent {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Update(update) => Some(update),
            _ => None,
        })
        .nth(update_index)
        .expect("expected update event")
}

fn find_delete_event(events: &[Event]) -> &etl::types::DeleteEvent {
    events
        .iter()
        .find_map(|event| match event {
            Event::Delete(delete) => Some(delete),
            _ => None,
        })
        .expect("expected delete event")
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_streaming_with_restart() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();

    install_crypto_provider();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let bigquery_database = setup_bigquery_database().await;

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
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::compatible(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery directly to get the data which has been inserted by tests.
    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![BigQueryUser::new(1, "user_1", 1), BigQueryUser::new(2, "user_2", 2),]
    );
    let orders_rows =
        bigquery_database.query_table(database_schema.orders_schema().name).await.unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![BigQueryOrder::new(1, "description_1"), BigQueryOrder::new(2, "description_2"),]
    );

    // Rebuild the destination for the restart so the test exercises state/schema
    // recovery instead of relying on a reused, previously shut-down wrapper.
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::strict(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // We restart the pipeline and check that we can process events since we have
    // loaded the table schema from persisted state.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We expect 2 insert events for each table (4 total).
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 4)]).await;

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
    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
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
    let orders_rows =
        bigquery_database.query_table(database_schema.orders_schema().name).await.unwrap();
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
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let bigquery_database = setup_bigquery_database().await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database.build_destination(pipeline_id, store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // Wait for the first insert.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

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
    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_1", 1),]);

    // Wait for the update.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

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
    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_10", 10),]);

    // Wait for the update.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    // Update the row.
    database
        .delete_values(database_schema.users_schema().name.clone(), &["name"], &["'user_10'"], "")
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We query BigQuery to check for deletion.
    let users_rows = bigquery_database.query_table(database_schema.users_schema().name).await;
    assert!(users_rows.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_subsequent_updates() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let mut database_1 = spawn_source_database().await;
    let mut database_2 = database_1.duplicate().await;
    let database_schema = setup_test_database_schema(&database_1, TableSelection::UsersOnly).await;

    let bigquery_database = setup_bigquery_database().await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::compatible(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let mut pipeline = create_pipeline(
        &database_1.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
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

    // Create two transactions A and B on separate connections to make sure that the
    // updates are ordered correctly.
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
    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(1, "user_2", 2),]);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_primary_key_update_rewrites_row() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    database
        .insert_values(
            database_schema.users_schema().name.clone(),
            &["name", "age"],
            &[&"user_1", &1],
        )
        .await
        .unwrap();

    let bigquery_database = setup_bigquery_database().await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database.build_destination(pipeline_id, store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

    // With default primary-key replica identity, changing the source primary
    // key is a valid PostgreSQL shape: the update carries an old key row plus
    // a full new row image.
    let updated_id = 10i64;
    let updated_name = "user_10".to_owned();
    let updated_age = 10i32;
    database
        .update_values_where(
            database_schema.users_schema().name.clone(),
            &["id", "name", "age"],
            &[&updated_id, &updated_name, &updated_age],
            &["id"],
            &["1"],
            "",
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = data_events(destination.get_events().await);
    assert_eq!(events.len(), 1);
    let update = find_update_event(&events, 0);
    assert_eq!(update.old_table_row, Some(OldTableRow::Key(TableRow::new(vec![Cell::I64(1)]))));
    assert_eq!(
        update.updated_table_row,
        UpdatedTableRow::Full(TableRow::new(vec![
            Cell::I64(updated_id),
            Cell::String(updated_name.clone()),
            Cell::I32(updated_age),
        ]))
    );

    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(parsed_users_rows, vec![BigQueryUser::new(10, "user_10", 10),]);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_full_replica_identity_update_preserves_unchanged_toasted_columns() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
    let table_name = test_table_name("full_replica_identity_users");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("large_text", "text not null")],
        )
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[
                TableModification::AlterColumn {
                    name: "large_text",
                    alteration: "set storage external",
                },
                TableModification::ReplicaIdentity { value: "full" },
            ],
        )
        .await
        .unwrap();

    let initial_large_text = generate_random_ascii_string(REPLICA_IDENTITY_LARGE_TEXT_SIZE_BYTES);
    database
        .insert_values(
            table_name.clone(),
            &["name", "large_text"],
            &[&"alice", &initial_large_text],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database.build_destination(pipeline_id, store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_full_replica_identity".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_ready_notify =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    table_ready_notify.notified().await;

    // With REPLICA IDENTITY FULL, PostgreSQL sends a full old row for updates.
    // That means unchanged external TOAST values can be reconstructed and the
    // destination should receive a full new row image.
    let updated_name = "alicia".to_owned();
    let update_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

    database
        .update_values_where(table_name.clone(), &["name"], &[&updated_name], &["id"], &["1"], "")
        .await
        .unwrap();

    update_notify.notified().await;

    let delete_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    database.delete_values(table_name.clone(), &["id"], &["1"], "").await.unwrap();

    delete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = data_events(destination.get_events().await);
    assert_eq!(events.len(), 2);
    let update = find_update_event(&events, 0);
    assert_eq!(
        update.old_table_row,
        Some(OldTableRow::Full(TableRow::new(vec![
            Cell::I64(1),
            Cell::String("alice".to_owned()),
            Cell::String(initial_large_text.clone()),
        ])))
    );
    assert_eq!(
        update.updated_table_row,
        UpdatedTableRow::Full(TableRow::new(vec![
            Cell::I64(1),
            Cell::String(updated_name.clone()),
            Cell::String(initial_large_text.clone()),
        ]))
    );
    let delete = find_delete_event(&events);
    assert_eq!(
        delete.old_table_row,
        Some(OldTableRow::Full(TableRow::new(vec![
            Cell::I64(1),
            Cell::String(updated_name),
            Cell::String(initial_large_text),
        ])))
    );

    let table_rows = bigquery_database.query_table(table_name).await;
    assert!(table_rows.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_truncate_with_batching() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let bigquery_database = setup_bigquery_database().await;

    // We create table `test_users_1` to simulate an error in the system where a
    // table with that name already exists and should be replaced for
    // replication to work correctly.
    bigquery_database.create_table("test_users_1", &[("age", "integer")]).await;

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database.build_destination(pipeline_id, store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    // Start pipeline from scratch.
    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        // We use a batch size > 1, so that we can make sure that interleaved truncate statements
        // work well with multiple batches of events.
        BatchConfig { max_fill_ms: 1000, memory_budget_ratio: 0.2, max_bytes: 8 * 1024 * 1024 },
    );

    // Register notifications for table copy completion.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;
    let orders_state_notify = store
        .notify_on_table_state_type(
            database_schema.orders_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;
    orders_state_notify.notified().await;

    // Wait for the 8 inserts (4 per table + 4 after truncate) and 2 truncates (1
    // per table).
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
    database.truncate_table(database_schema.users_schema().name.clone()).await.unwrap();
    database.truncate_table(database_schema.orders_schema().name.clone()).await.unwrap();

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

    // We query BigQuery directly to get the data which tests have inserted,
    // expecting that only the rows after truncation are there.
    let users_rows =
        bigquery_database.query_table(database_schema.users_schema().name).await.unwrap();
    let parsed_users_rows = parse_bigquery_table_rows::<BigQueryUser>(users_rows);
    assert_eq!(
        parsed_users_rows,
        vec![BigQueryUser::new(3, "user_3", 3), BigQueryUser::new(4, "user_4", 4),]
    );
    let orders_rows =
        bigquery_database.query_table(database_schema.orders_schema().name).await.unwrap();
    let parsed_orders_rows = parse_bigquery_table_rows::<BigQueryOrder>(orders_rows);
    assert_eq!(
        parsed_orders_rows,
        vec![BigQueryOrder::new(3, "description_3"), BigQueryOrder::new(4, "description_4")]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_nullable_scalar_columns() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
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
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::coerce(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // Insert
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsScalar>(table_rows);
    assert_eq!(parsed_table_rows, vec![NullableColsScalar::all_nulls(1),]);

    // Update
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

    // Define test values
    let updated_bool = true;
    let updated_text = "updated_text".to_owned();
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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
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
    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    database.delete_values(table_name.clone(), &["id"], &["'1'"], "").await.unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_nullable_array_columns() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
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
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::preserve(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_array".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert with null arrays
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
    let parsed_table_rows = parse_bigquery_table_rows::<NullableColsArray>(table_rows);
    // Preserve materialization stores arrays as scalar strings so source
    // NULL arrays remain NULL instead of becoming BigQuery empty arrays.
    assert_eq!(parsed_table_rows, vec![NullableColsArray::all_null(1),]);

    // update with array values
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

    // Define test array values
    let updated_bool_arr = vec![true, false, true];
    let updated_text_arr = vec!["hello".to_owned(), "world".to_owned()];
    let updated_i2_arr = vec![1i16, 2i16, 3i16];
    let updated_i4_arr = vec![100i32, 200i32];
    let updated_i8_arr = vec![1000i64, 2000i64, 3000i64];
    let updated_f4_arr = vec![1.5f32, 2.5f32];
    let updated_f8_arr = vec![std::f64::consts::PI, std::f64::consts::E];
    let updated_n_arr =
        vec![PgNumeric::from_str("3.141").unwrap(), PgNumeric::from_str("2.718").unwrap()];
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
    let updated_json_arr =
        vec![serde_json::json!({"key1": "value1"}), serde_json::json!({"key2": "value2"})];
    let updated_jsonb_arr =
        vec![serde_json::json!({"jsonb1": "data1"}), serde_json::json!({"jsonb2": "data2"})];
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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
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
    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    database.delete_values(table_name.clone(), &["id"], &["'1'"], "").await.unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_non_nullable_scalar_columns() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
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
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::coerce(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_non_null".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert with non-null values
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    // Define test values - all non-null
    let test_bool = true;
    let test_text = "test_text".to_owned();
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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
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
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

    // Define updated test values
    let updated_bool = false;
    let updated_text = "updated_text".to_owned();
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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
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
    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    database.delete_values(table_name.clone(), &["id"], &["'1'"], "").await.unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_non_nullable_array_columns() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
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
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::coerce(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_non_null_array".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    // insert with non-null array values
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    // Define test array values - all non-null
    let test_bool_arr = vec![true, false, true];
    let test_text_arr = vec!["hello".to_owned(), "world".to_owned()];
    let test_i2_arr = vec![1i16, 2i16, 3i16];
    let test_i4_arr = vec![100i32, 200i32];
    let test_i8_arr = vec![1000i64, 2000i64, 3000i64];
    let test_f4_arr = vec![1.5f32, 2.5f32];
    let test_f8_arr = vec![std::f64::consts::PI, std::f64::consts::E];
    let test_n_arr =
        vec![PgNumeric::from_str("3.141").unwrap(), PgNumeric::from_str("2.718").unwrap()];
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
    let test_json_arr =
        vec![serde_json::json!({"key1": "value1"}), serde_json::json!({"key2": "value2"})];
    let test_jsonb_arr =
        vec![serde_json::json!({"jsonb1": "data1"}), serde_json::json!({"jsonb2": "data2"})];
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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
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
    let event_notify = destination.wait_for_events_count(vec![(EventType::Update, 1)]).await;

    // Define updated test array values
    let updated_bool_arr = vec![false, true];
    let updated_text_arr = vec!["updated".to_owned(), "arrays".to_owned(), "test".to_owned()];
    let updated_i2_arr = vec![10i16, 20i16];
    let updated_i4_arr = vec![500i32, 600i32, 700i32];
    let updated_i8_arr = vec![5000i64, 6000i64];
    let updated_f4_arr = vec![std::f32::consts::PI, 2.71f32, 1.41f32];
    let updated_f8_arr = vec![std::f64::consts::E, std::f64::consts::PI];
    let updated_n_arr =
        vec![PgNumeric::from_str("1.2").unwrap(), PgNumeric::from_str("2.2").unwrap()];
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
        NaiveDateTime::new(updated_base_date, NaiveTime::from_hms_opt(11, 20, 10).unwrap()),
        NaiveDateTime::new(updated_base_date, NaiveTime::from_hms_opt(16, 40, 50).unwrap()),
    ];
    let updated_timestamptz_arr = vec![
        DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp_arr[0], Utc),
        DateTime::<Utc>::from_naive_utc_and_offset(updated_timestamp_arr[1], Utc),
    ];
    let updated_uuid_arr = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let updated_json_arr =
        vec![serde_json::json!({"updated": "json1"}), serde_json::json!({"updated": "json2"})];
    let updated_jsonb_arr =
        vec![serde_json::json!({"updated": "jsonb1"}), serde_json::json!({"updated": "jsonb2"})];
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

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
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
    let event_notify = destination.wait_for_events_count(vec![(EventType::Delete, 1)]).await;

    database.delete_values(table_name.clone(), &["id"], &["'1'"], "").await.unwrap();

    event_notify.notified().await;

    let table_rows = bigquery_database.query_table(table_name.clone()).await;
    assert!(table_rows.is_none());

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_array_with_null_values() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
    let table_name = test_table_name("array_with_nulls");
    let table_id =
        database.create_table(table_name.clone(), true, &[("int_array", "int4[]")]).await.unwrap();

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::preserve(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_array_nulls".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notification =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_done_notification.notified().await;

    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 3)]).await;

    // BigQuery cannot preserve NULL repeated fields or NULL repeated-field
    // elements, so preserve mode stores arrays as scalar strings.
    let client = database.client.as_ref().unwrap();
    let quoted_table_name = table_name.as_quoted_identifier();
    client
        .execute(&format!("insert into {quoted_table_name} (int_array) values (null)"), &[])
        .await
        .unwrap();
    client
        .execute(
            &format!("insert into {quoted_table_name} (int_array) values (array[]::int4[])"),
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            &format!("insert into {quoted_table_name} (int_array) values (array[1, null])"),
            &[],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let destination_schema =
        bigquery_database.query_table_schema(table_name.clone()).await.unwrap();
    let int_array_type = destination_schema
        .columns()
        .iter()
        .find(|column| column.column_name == "int_array")
        .map(|column| column.data_type.as_str());
    assert_eq!(int_array_type, Some("STRING"));

    let table_rows = bigquery_database.query_table(table_name.clone()).await.unwrap();
    assert_eq!(table_rows.len(), 3);

    let mut replicated_arrays = table_rows
        .into_iter()
        .map(|row| {
            let columns = row.columns.expect("row should contain columns");
            assert_eq!(columns.len(), 2);
            columns[1].value.as_ref().and_then(|value| value.as_str().map(str::to_owned))
        })
        .collect::<Vec<_>>();
    replicated_arrays.sort();

    assert_eq!(replicated_arrays, vec![None, Some("{1,NULL}".to_owned()), Some("{}".to_owned())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_validation_out_of_bounds_values() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;

    // Create tables with different error types
    let huge_numeric_table = test_table_name("huge_numeric");
    let huge_numeric_table_id = database
        .create_table(huge_numeric_table.clone(), true, &[("huge_numeric", "numeric")])
        .await
        .unwrap();

    let infinite_numeric_table = test_table_name("infinite_numeric");
    let infinite_numeric_table_id = database
        .create_table(infinite_numeric_table.clone(), true, &[("infinite_numeric", "numeric")])
        .await
        .unwrap();

    let old_date_table = test_table_name("old_date");
    let old_date_table_id = database
        .create_table(old_date_table.clone(), true, &[("test_date", "date")])
        .await
        .unwrap();

    let nan_array_table = test_table_name("nan_array");
    let nan_array_table_id = database
        .create_table(nan_array_table.clone(), true, &[("numeric_array", "numeric[]")])
        .await
        .unwrap();

    let wide_json_table = test_table_name("wide_json");
    let wide_json_table_id =
        database.create_table(wide_json_table.clone(), true, &[("payload", "json")]).await.unwrap();

    let imprecise_json_integer_table = test_table_name("imprecise_json_integer");
    let imprecise_json_integer_table_id = database
        .create_table(imprecise_json_integer_table.clone(), true, &[("payload", "json")])
        .await
        .unwrap();

    // Insert out-of-bounds data into each table
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "insert into {} (huge_numeric) values ({})",
                huge_numeric_table.as_quoted_identifier(),
                "'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890'"
            ),
            &[],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "insert into {} (infinite_numeric) values ('Infinity'::numeric)",
                infinite_numeric_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "insert into {} (test_date) values ('0001-01-01'::date - interval '1 day')",
                old_date_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "insert into {} (numeric_array) values (array['NaN'::numeric, '123.45'::numeric])",
                nan_array_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "insert into {} (payload) values ('{{\"value\":1e309}}'::json)",
                wide_json_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "insert into {} (payload) values ('{{\"value\":18446744073709551616}}'::json)",
                imprecise_json_integer_table.as_quoted_identifier()
            ),
            &[],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database
        .build_destination_with_compatibility(
            pipeline_id,
            store.clone(),
            DestinationTypeCompatibility::strict(),
        )
        .await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_validation".to_owned();
    database
        .create_publication(
            &publication_name,
            &[
                huge_numeric_table,
                infinite_numeric_table,
                old_date_table,
                nan_array_table,
                wide_json_table,
                imprecise_json_integer_table,
            ],
        )
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    // Register notifications for errored replication phase
    let huge_numeric_error_notify = store
        .notify_on_table_state_type(huge_numeric_table_id, TableReplicationPhaseType::Errored)
        .await;

    let infinite_numeric_error_notify = store
        .notify_on_table_state_type(infinite_numeric_table_id, TableReplicationPhaseType::Errored)
        .await;

    let old_date_error_notify = store
        .notify_on_table_state_type(old_date_table_id, TableReplicationPhaseType::Errored)
        .await;

    let nan_array_error_notify = store
        .notify_on_table_state_type(nan_array_table_id, TableReplicationPhaseType::Errored)
        .await;

    let wide_json_error_notify = store
        .notify_on_table_state_type(wide_json_table_id, TableReplicationPhaseType::Errored)
        .await;

    let imprecise_json_integer_error_notify = store
        .notify_on_table_state_type(
            imprecise_json_integer_table_id,
            TableReplicationPhaseType::Errored,
        )
        .await;

    pipeline.start().await.unwrap();

    // Wait for all tables to enter errored state
    huge_numeric_error_notify.notified().await;
    infinite_numeric_error_notify.notified().await;
    old_date_error_notify.notified().await;
    nan_array_error_notify.notified().await;
    wide_json_error_notify.notified().await;
    imprecise_json_integer_error_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    for table_id in [
        huge_numeric_table_id,
        infinite_numeric_table_id,
        old_date_table_id,
        nan_array_table_id,
        wide_json_table_id,
        imprecise_json_integer_table_id,
    ] {
        let table_state = store.get_table_replication_state(table_id).await.unwrap().unwrap();
        assert!(matches!(table_state, TableReplicationPhase::Errored { .. }));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn table_type_compatibility_modes_handle_same_risky_data() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }

    init_test_tracing();
    install_crypto_provider();

    for (mode_name, type_compatibility, expected_phase) in [
        ("strict", DestinationTypeCompatibility::strict(), TableReplicationPhaseType::Errored),
        (
            "compatible",
            DestinationTypeCompatibility::compatible(),
            TableReplicationPhaseType::Errored,
        ),
        ("preserve", DestinationTypeCompatibility::preserve(), TableReplicationPhaseType::Ready),
        ("coerce", DestinationTypeCompatibility::coerce(), TableReplicationPhaseType::Ready),
    ] {
        let database = spawn_source_database().await;
        let bigquery_database = setup_bigquery_database().await;

        let table_name = test_table_name(&format!("{mode_name}_types"));
        let table_id = database
            .create_table(
                table_name.clone(),
                true,
                &[
                    ("numeric_value", "numeric"),
                    ("infinite_numeric", "numeric"),
                    ("float8_value", "double precision"),
                    ("old_date", "date"),
                    ("time_value", "time"),
                    ("timestamp_value", "timestamp"),
                    ("timestamptz_value", "timestamptz"),
                    ("uuid_value", "uuid"),
                    ("json_value", "json"),
                    ("json_duplicate", "json"),
                    ("numeric_array", "numeric[]"),
                    ("uuid_array", "uuid[]"),
                    ("json_array", "json[]"),
                ],
            )
            .await
            .unwrap();

        database
            .client
            .as_ref()
            .unwrap()
            .execute(
                &format!(
                    "insert into {} (
                        numeric_value,
                        infinite_numeric,
                        float8_value,
                        old_date,
                        time_value,
                        timestamp_value,
                        timestamptz_value,
                        uuid_value,
                        json_value,
                        json_duplicate,
                        numeric_array,
                        uuid_array,
                        json_array
                    ) values (
                        '0.000000000000000000000000000000000000001'::numeric,
                        'Infinity'::numeric,
                        '-0'::double precision,
                        '-infinity'::date,
                        '24:00:00'::time,
                        'infinity'::timestamp,
                        'infinity'::timestamptz,
                        '550e8400-e29b-41d4-a716-446655440000'::uuid,
                        '{{\"value\":18446744073709551616}}'::json,
                        '{{\"value\":1,\"value\":2}}'::json,
                        array['0.000000000000000000000000000000000000001'::numeric],
                        array['550e8400-e29b-41d4-a716-446655440000'::uuid],
                        array['{{\"value\":18446744073709551616}}'::json]
                    )",
                    table_name.as_quoted_identifier()
                ),
                &[],
            )
            .await
            .unwrap();

        let store = NotifyingStore::new();
        let pipeline_id: PipelineId = random();
        let raw_destination = bigquery_database
            .build_destination_with_compatibility(pipeline_id, store.clone(), type_compatibility)
            .await;
        let destination = TestDestinationWrapper::wrap(raw_destination);

        let publication_name = format!("test_pub_{mode_name}_types");
        database
            .create_publication(&publication_name, std::slice::from_ref(&table_name))
            .await
            .expect("Failed to create publication");

        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            publication_name,
            store.clone(),
            destination,
        );

        let unexpected_phase = match expected_phase {
            TableReplicationPhaseType::Errored => TableReplicationPhaseType::Ready,
            TableReplicationPhaseType::Ready => TableReplicationPhaseType::Errored,
            phase => unreachable!("unexpected compatibility test phase: {phase:?}"),
        };
        let table_reached_outcome = store
            .notify_on_table_state(table_id, move |state| {
                let state_type = state.as_type();
                state_type == expected_phase || state_type == unexpected_phase
            })
            .await;

        pipeline.start().await.unwrap();

        tokio::select! {
            _ = table_reached_outcome.notified() => {}
            _ = sleep(Duration::from_secs(180)) => {
                let table_state = store.get_table_replication_state(table_id).await.unwrap();
                pipeline.shutdown_and_wait().await.unwrap();
                panic!("timed out waiting for {mode_name} table outcome: {table_state:?}");
            }
        }
        let table_state = store
            .get_table_replication_state(table_id)
            .await
            .unwrap()
            .expect("table replication state should exist");

        pipeline.shutdown_and_wait().await.unwrap();

        if expected_phase == TableReplicationPhaseType::Errored {
            let TableReplicationPhase::Errored { reason, .. } = table_state else {
                panic!("{mode_name} table replication should have errored: {table_state:?}");
            };

            assert!(
                reason.contains("[UnsupportedValueInDestination]"),
                "{mode_name} mode should reject the risky payload locally: {reason}"
            );
            assert!(
                reason.contains("would be rounded by BigQuery")
                    || reason.contains("JSON integer would lose precision")
                    || reason.contains("JSON integer is outside BigQuery")
                    || reason.contains("has no strict native BigQuery representation"),
                "{mode_name} mode should reject a silent BigQuery conversion risk: {reason}"
            );

            continue;
        }

        if table_state.as_type() != TableReplicationPhaseType::Ready {
            panic!("{mode_name} table replication should be ready: {table_state:?}");
        }

        let destination_schema =
            bigquery_database.query_table_schema(table_name.clone()).await.unwrap();
        let column_type = |column_name: &str| {
            destination_schema
                .columns()
                .iter()
                .find(|column| column.column_name == column_name)
                .map(|column| column.data_type.as_str())
        };

        if type_compatibility.is_preserve() {
            for column_name in [
                "numeric_value",
                "infinite_numeric",
                "float8_value",
                "old_date",
                "time_value",
                "timestamp_value",
                "timestamptz_value",
                "uuid_value",
                "json_value",
                "json_duplicate",
            ] {
                assert_eq!(column_type(column_name), Some("STRING"), "{column_name}");
            }
        } else {
            assert_eq!(column_type("numeric_value"), Some("BIGNUMERIC"));
            assert_eq!(column_type("infinite_numeric"), Some("BIGNUMERIC"));
            assert_eq!(column_type("float8_value"), Some("FLOAT64"));
            assert_eq!(column_type("old_date"), Some("DATE"));
            assert_eq!(column_type("time_value"), Some("TIME"));
            assert_eq!(column_type("timestamp_value"), Some("DATETIME"));
            assert_eq!(column_type("timestamptz_value"), Some("TIMESTAMP"));
            assert_eq!(column_type("uuid_value"), Some("STRING"));
            assert_eq!(column_type("json_value"), Some("JSON"));
            assert_eq!(column_type("json_duplicate"), Some("JSON"));
        }

        if type_compatibility.is_preserve() {
            for column_name in ["numeric_array", "uuid_array", "json_array"] {
                assert_eq!(column_type(column_name), Some("STRING"), "{column_name}");
            }
        } else {
            assert_eq!(column_type("numeric_array"), Some("ARRAY<BIGNUMERIC>"));
            assert_eq!(column_type("uuid_array"), Some("ARRAY<STRING>"));
            assert_eq!(column_type("json_array"), Some("ARRAY<JSON>"));
        }

        let destination_rows = bigquery_database.query_table(table_name).await.unwrap();
        assert_eq!(destination_rows.len(), 1);

        let table_schemas = store.get_table_schemas().await;
        let source_table_schema = table_schemas
            .get(&table_id)
            .and_then(|schemas| schemas.first())
            .map(|(_, table_schema)| table_schema)
            .expect("source schema should be stored");
        let numeric_column = source_table_schema
            .column_schemas
            .iter()
            .find(|column| column.name == "numeric_value")
            .expect("numeric column should exist");
        assert_eq!(numeric_column.typ.name(), "numeric");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_change() {
    if skip_if_missing_bigquery_env_vars() {
        return;
    }
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let bigquery_database = setup_bigquery_database().await;
    let table_name = test_table_name("schema_multi_ops");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null"), ("status", "text")],
        )
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let raw_destination = bigquery_database.build_destination(pipeline_id, store.clone()).await;
    let destination = TestDestinationWrapper::wrap(raw_destination);

    let publication_name = "test_pub_multi_ops".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("Failed to create publication");

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let table_sync_done =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::SyncDone).await;

    pipeline.start().await.unwrap();
    table_sync_done.notified().await;

    // Insert the initial row.
    let event_notify = destination.wait_for_events_count(vec![(EventType::Insert, 1)]).await;

    database
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"Alice", &25, &"active"])
        .await
        .unwrap();

    event_notify.notified().await;
    destination.clear_events().await;

    // Verify initial schema.
    let initial_schema = bigquery_database.query_table_schema(table_name.clone()).await.unwrap();
    initial_schema.assert_columns(&["id", "name", "age", "status"]);

    // Verify destination schema state is applied after initial table creation.
    let initial_state = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination schema state should exist after table creation");
    let initial_snapshot_id = initial_state.snapshot_id;

    // Apply multiple schema changes:
    // 1. Rename name -> full_name
    // 2. Drop the status column
    // 3. Add email column
    //
    // Note: Each DDL change is captured via the DDL event trigger and stored in the
    // schema store, but PostgreSQL sends only ONE Relation message with the
    // final schema when the next DML operation (INSERT) occurs. The schema
    // diffing in handle_relation_event then computes and applies all changes at
    // once.
    let event_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "name", new_name: "full_name" }],
        )
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "status" }])
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .unwrap();

    // Insert row with new schema.
    database
        .insert_values(
            table_name.clone(),
            &["full_name", "age", "email"],
            &[&"Bob", &30, &"bob@example.com"],
        )
        .await
        .unwrap();

    event_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the final schema:
    // - name should be renamed to full_name
    // - status should be dropped
    // - email should be added
    let final_schema = bigquery_database.query_table_schema(table_name.clone()).await.unwrap();
    final_schema.assert_columns(&["id", "full_name", "age", "email"]);
    final_schema.assert_no_column("name");
    final_schema.assert_no_column("status");

    // Verify destination schema state is applied after schema changes.
    let final_state = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("destination schema state should exist after schema change");
    assert!(
        final_state.snapshot_id > initial_snapshot_id,
        "snapshot_id should have increased after schema change"
    );

    // Verify data was inserted correctly.
    let rows = bigquery_database.query_table(table_name).await.unwrap();
    assert_eq!(rows.len(), 2);
}
