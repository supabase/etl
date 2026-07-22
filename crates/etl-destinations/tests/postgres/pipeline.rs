use etl::{
    event::EventType,
    pipeline::PipelineId,
    store::{StateStore, TableStateType},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::EventCondition,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_destinations::postgres::test_utils::{
    TEST_DESTINATION_SCHEMA, setup_postgres_destination_database,
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

use crate::support::crypto::install_crypto_provider;

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_roundtrip() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let table_name = test_table_name("pg_copy_types");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("smallint_col", "smallint not null"),
                ("integer_col", "integer not null"),
                ("bigint_col", "bigint not null"),
                ("real_col", "real not null"),
                ("double_col", "double precision not null"),
                ("numeric_col", "numeric(10,2) not null"),
                ("boolean_col", "boolean not null"),
                ("text_col", "text not null"),
                ("varchar_col", "varchar(100) not null"),
                ("date_col", "date not null"),
                ("timestamp_col", "timestamp not null"),
                ("timestamptz_col", "timestamptz not null"),
                ("uuid_col", "uuid not null"),
                ("jsonb_col", "jsonb not null"),
            ],
        )
        .await
        .expect("create table");

    let publication_name = "test_pub_pg_copy";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("create publication");

    database
        .run_sql(&format!(
            r#"insert into {table} (
                smallint_col, integer_col, bigint_col, real_col, double_col, numeric_col,
                boolean_col, text_col, varchar_col, date_col, timestamp_col, timestamptz_col,
                uuid_col, jsonb_col
            ) values (
                42, 1000, 9999999, 1.5, 2.5, 12345.67,
                true, 'hello text', 'hello varchar', '2024-01-15', '2024-01-15 12:00:00',
                '2024-01-15 12:00:00+00', 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
                '{{"key":"value"}}'
            )"#,
            table = table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert row");

    let dest_db = setup_postgres_destination_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = dest_db.build_destination(store.clone());

    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store,
        destination,
    );
    pipeline.start().await.unwrap();
    ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let rows = dest_db
        .query(&format!(
            "select id, smallint_col, integer_col, bigint_col, text_col, varchar_col, \
             numeric_col::text, boolean_col, uuid_col::text from {schema}.pg_copy_types order by \
             id",
            schema = TEST_DESTINATION_SCHEMA,
        ))
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, i16>(1), 42);
    assert_eq!(rows[0].get::<_, i32>(2), 1000);
    assert_eq!(rows[0].get::<_, i64>(3), 9_999_999);
    assert_eq!(rows[0].get::<_, String>(4), "hello text");
    assert_eq!(rows[0].get::<_, String>(5), "hello varchar");
    assert_eq!(rows[0].get::<_, String>(6), "12345.67");
    assert!(rows[0].get::<_, bool>(7));
    assert_eq!(rows[0].get::<_, String>(8).to_lowercase(), "f47ac10b-58cc-4372-a567-0e02b2c3d479");
}

#[tokio::test(flavor = "multi_thread")]
async fn updates_and_deletes_streamed() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let table_name = test_table_name("pg_update_delete");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("create table");

    let publication_name = "test_pub_pg_upd_del";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("create publication");

    database
        .run_sql(&format!(
            "insert into {} (value) values ('keep'), ('drop')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert");

    let dest_db = setup_postgres_destination_database().await;
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(dest_db.build_destination(store.clone()));
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        publication_name.to_owned(),
        store,
        destination.clone(),
    );
    pipeline.start().await.unwrap();
    ready.notified().await;

    let events = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Update, table_id, 1),
            EventCondition::TableCount(EventType::Delete, table_id, 1),
        ])
        .await;

    database
        .run_sql(&format!(
            "update {} set value = 'updated' where id = 1",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("update");
    database
        .run_sql(&format!("delete from {} where id = 2", table_name.as_quoted_identifier(),))
        .await
        .expect("delete");
    events.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let rows = dest_db
        .query(&format!(
            "select id, value from {schema}.pg_update_delete order by id",
            schema = TEST_DESTINATION_SCHEMA,
        ))
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "updated");
}

#[tokio::test(flavor = "multi_thread")]
async fn truncate_clears_table() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let table_name = test_table_name("pg_truncate");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("create table");

    let publication_name = "test_pub_pg_truncate";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("create publication");
    database
        .run_sql(&format!(
            "insert into {} (value) values ('a'), ('b')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert");

    let dest_db = setup_postgres_destination_database().await;
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(dest_db.build_destination(store.clone()));
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        publication_name.to_owned(),
        store,
        destination.clone(),
    );
    pipeline.start().await.unwrap();
    ready.notified().await;

    let events = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Truncate, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
        .await;
    database
        .run_sql(&format!("truncate {}", table_name.as_quoted_identifier()))
        .await
        .expect("truncate");
    database
        .run_sql(&format!(
            "insert into {} (value) values ('after')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert after truncate");
    events.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let rows = dest_db
        .query(&format!(
            "select id, value from {schema}.pg_truncate order by id",
            schema = TEST_DESTINATION_SCHEMA,
        ))
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "after");
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_change_add_drop_rename() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let table_name = test_table_name("pg_schema_multi");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null"), ("status", "text")],
        )
        .await
        .expect("create table");

    let publication_name = "test_pub_pg_schema_multi";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("create publication");
    database
        .run_sql(&format!(
            "insert into {} (name, age, status) values ('Alice', 25, 'active')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert");

    let dest_db = setup_postgres_destination_database().await;
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(dest_db.build_destination(store.clone()));
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );
    pipeline.start().await.unwrap();
    ready.notified().await;

    let initial_columns = dest_db.column_names(TEST_DESTINATION_SCHEMA, "pg_schema_multi").await;
    assert_eq!(initial_columns, vec!["id", "name", "age", "status"]);
    let initial_snapshot = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata")
        .snapshot_id;

    let events = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "name", new_name: "full_name" }],
        )
        .await
        .unwrap();
    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text" }],
        )
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (full_name, status, email) values ('Bob', 'pending', \
             'bob@example.com')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert bob");
    events.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let final_columns = dest_db.column_names(TEST_DESTINATION_SCHEMA, "pg_schema_multi").await;
    assert_eq!(final_columns, vec!["id", "full_name", "status", "email"]);

    let final_snapshot = store
        .get_applied_destination_table_metadata(table_id)
        .await
        .unwrap()
        .expect("metadata")
        .snapshot_id;
    assert!(final_snapshot > initial_snapshot);

    let rows = dest_db
        .query(&format!(
            "select id, full_name, status, email from {schema}.pg_schema_multi order by id",
            schema = TEST_DESTINATION_SCHEMA,
        ))
        .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(1), "Alice");
    assert_eq!(rows[0].get::<_, Option<String>>(3), None);
    assert_eq!(rows[1].get::<_, String>(1), "Bob");
    assert_eq!(rows[1].get::<_, Option<String>>(3), Some("bob@example.com".to_owned()));
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_reset_drops_destination() {
    init_test_tracing();
    install_crypto_provider();

    let database = spawn_source_database().await;
    let table_name = test_table_name("pg_reset_copy");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "text not null")])
        .await
        .expect("create table");

    let publication_name = "test_pub_pg_reset_copy";
    database
        .create_publication(publication_name, std::slice::from_ref(&table_name))
        .await
        .expect("create publication");
    database
        .run_sql(&format!(
            "insert into {} (value) values ('old-1'), ('old-2')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert");

    let dest_db = setup_postgres_destination_database().await;
    let store = NotifyingStore::new();
    let pipeline_id: PipelineId = random();
    let destination = TestDestinationWrapper::wrap(dest_db.build_destination(store.clone()));
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination,
    );
    pipeline.start().await.unwrap();
    ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let before = dest_db
        .query(&format!(
            "select count(*)::bigint from {schema}.pg_reset_copy",
            schema = TEST_DESTINATION_SCHEMA,
        ))
        .await;
    assert_eq!(before[0].get::<_, i64>(0), 2);

    database
        .run_sql(&format!("delete from {} where true", table_name.as_quoted_identifier()))
        .await
        .expect("delete source rows");
    database
        .run_sql(&format!(
            "insert into {} (value) values ('new-only')",
            table_name.as_quoted_identifier(),
        ))
        .await
        .expect("insert recopy row");

    store.reset_table_state(table_id).await.expect("reset table state");

    let destination = TestDestinationWrapper::wrap(dest_db.build_destination(store.clone()));
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );
    pipeline.start().await.unwrap();
    ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let rows = dest_db
        .query(&format!(
            "select id, value from {schema}.pg_reset_copy order by id",
            schema = TEST_DESTINATION_SCHEMA,
        ))
        .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "new-only");
}
