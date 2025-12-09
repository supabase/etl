#![cfg(feature = "test-utils")]

use etl::destination::memory::MemoryDestination;
use etl::pipeline::Pipeline;
use etl::state::table::TableReplicationPhaseType;
use etl::test_utils::database::{spawn_source_database, test_table_name};
use etl::test_utils::notify::NotifyingStore;
use etl::test_utils::pipeline::create_pipeline;
use etl::test_utils::test_destination_wrapper::TestDestinationWrapper;
use etl::types::{Event, EventType, InsertEvent, PipelineId, RelationEvent, Type};
use etl_postgres::tokio::test_utils::{PgDatabase, TableModification};
use etl_postgres::types::{TableId, TableName};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::Client;

type SchemaChangePipeline = Pipeline<NotifyingStore, TestDestinationWrapper<MemoryDestination>>;

fn last_relation_for_table(events: &[Event], table_id: TableId) -> RelationEvent {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            Event::Relation(relation) if relation.replicated_table_schema.id() == table_id => {
                Some(relation.clone())
            }
            _ => None,
        })
        .expect("expected relation event for table")
}

fn last_insert_for_table(events: &[Event], table_id: TableId) -> InsertEvent {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert.clone())
            }
            _ => None,
        })
        .expect("expected insert event for table")
}

fn assert_all_columns_replicated(
    relation: &RelationEvent,
    expected_names: &[&str],
    expected_types: &[Type],
) {
    assert_eq!(expected_names.len(), expected_types.len());

    let replicated_columns: Vec<_> = relation
        .replicated_table_schema
        .column_schemas()
        .map(|c| (c.name.as_str(), c.typ.clone()))
        .collect();
    assert_eq!(replicated_columns.len(), expected_names.len());
    for ((name, typ), (expected_name, expected_type)) in replicated_columns
        .iter()
        .zip(expected_names.iter().zip(expected_types.iter()))
    {
        assert_eq!(name, expected_name);
        assert_eq!(typ, expected_type);
    }

    let inner = relation.replicated_table_schema.get_inner();
    assert_eq!(inner.column_schemas.len(), expected_names.len());
    assert_eq!(
        inner
            .column_schemas
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        expected_names
    );

    let mask = relation
        .replicated_table_schema
        .replication_mask()
        .as_slice();
    assert_eq!(mask.len(), expected_names.len());
    assert!(mask.iter().all(|&bit| bit == 1));
}

async fn setup_pipeline(
    table_suffix: &str,
    columns: &[(&str, &str)],
) -> (
    PgDatabase<Client>,
    TableName,
    TableId,
    NotifyingStore,
    TestDestinationWrapper<MemoryDestination>,
    SchemaChangePipeline,
    PipelineId,
    String,
) {
    let database = spawn_source_database().await;
    let table_name = test_table_name(table_suffix);
    let table_id = database
        .create_table(table_name.clone(), true, columns)
        .await
        .unwrap();

    let publication_name = format!("pub_{}", random::<u32>());
    database
        .create_publication(&publication_name, &[table_name.clone()])
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    let sync_done = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();
    sync_done.notified().await;

    (
        database,
        table_name,
        table_id,
        store,
        destination,
        pipeline,
        pipeline_id,
        publication_name,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_added() {
    init_test_tracing();

    let (
        database,
        table_name,
        table_id,
        _store,
        destination,
        pipeline,
        _pipeline_id,
        _publication,
    ) = setup_pipeline(
        "schema_add_column",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "email",
                data_type: "text not null",
            }],
        )
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["name", "age", "email"],
            &[&"Alice", &25, &"alice@example.com"],
        )
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation = last_relation_for_table(&events, table_id);
    assert_all_columns_replicated(
        &relation,
        &["id", "name", "age", "email"],
        &[Type::INT8, Type::TEXT, Type::INT4, Type::TEXT],
    );

    let insert = last_insert_for_table(&events, table_id);
    assert_eq!(insert.table_row.values.len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_removed() {
    init_test_tracing();

    let (
        database,
        table_name,
        table_id,
        _store,
        destination,
        pipeline,
        _pipeline_id,
        _publication,
    ) = setup_pipeline(
        "schema_remove_column",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::DropColumn { name: "age" }],
        )
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name"], &[&"Bob"])
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation = last_relation_for_table(&events, table_id);
    assert_all_columns_replicated(&relation, &["id", "name"], &[Type::INT8, Type::TEXT]);

    let insert = last_insert_for_table(&events, table_id);
    assert_eq!(insert.table_row.values.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_renamed() {
    init_test_tracing();

    let (
        database,
        table_name,
        table_id,
        _store,
        destination,
        pipeline,
        _pipeline_id,
        _publication,
    ) = setup_pipeline(
        "schema_rename_column",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "name",
                alteration: "rename to full_name",
            }],
        )
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["full_name", "age"], &[&"Carol", &41])
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation = last_relation_for_table(&events, table_id);
    assert_all_columns_replicated(
        &relation,
        &["id", "full_name", "age"],
        &[Type::INT8, Type::TEXT, Type::INT4],
    );

    let insert = last_insert_for_table(&events, table_id);
    assert_eq!(insert.table_row.values.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_recovers_after_multiple_schema_changes_and_restart() {
    init_test_tracing();

    let (
        database,
        table_name,
        table_id,
        store,
        destination,
        pipeline,
        pipeline_id,
        publication,
    ) = setup_pipeline(
        "schema_restart_after_changes",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    // First schema change: add email column.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "email",
                data_type: "text not null",
            }],
        )
        .await
        .unwrap();

    // Second schema change: rename age -> years.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "age",
                alteration: "rename to years",
            }],
        )
        .await
        .unwrap();

    // Emit data with the latest schema to force a RELATION + INSERT on the stream.
    database
        .insert_values(
            table_name.clone(),
            &["name", "years", "email"],
            &[&"Eve", &30, &"eve@example.com"],
        )
        .await
        .unwrap();

    notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Restart the pipeline with the same state store and destination.
    destination.clear_events().await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    let sync_done = store
        .notify_on_table_state_type(table_id, TableReplicationPhaseType::SyncDone)
        .await;

    pipeline.start().await.unwrap();
    sync_done.notified().await;

    // Wait for relation + insert after restart.
    let notify_after_restart = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["name", "years", "email"],
            &[&"Frank", &31, &"frank@example.com"],
        )
        .await
        .unwrap();

    notify_after_restart.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation = last_relation_for_table(&events, table_id);
    assert_all_columns_replicated(
        &relation,
        &["id", "name", "years", "email"],
        &[Type::INT8, Type::TEXT, Type::INT4, Type::TEXT],
    );

    let insert = last_insert_for_table(&events, table_id);
    assert_eq!(insert.table_row.values.len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_type_changes() {
    init_test_tracing();

    let (
        database,
        table_name,
        table_id,
        _store,
        destination,
        pipeline,
        _pipeline_id,
        _publication,
    ) = setup_pipeline(
        "schema_change_type",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn {
                name: "age",
                alteration: "type bigint",
            }],
        )
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "age"], &[&"Dave", &45_i64])
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation = last_relation_for_table(&events, table_id);
    assert_all_columns_replicated(
        &relation,
        &["id", "name", "age"],
        &[Type::INT8, Type::TEXT, Type::INT8],
    );

    let insert = last_insert_for_table(&events, table_id);
    assert_eq!(insert.table_row.values.len(), 3);
}
