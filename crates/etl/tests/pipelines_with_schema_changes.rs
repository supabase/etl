#![cfg(feature = "test-utils")]

use etl::{
    state::table::TableReplicationPhaseType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::{group_events_by_type, group_events_by_type_and_table_id},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{create_database_and_ready_pipeline_with_table, create_pipeline},
        schema::{
            assert_replicated_schema_column_names_types, assert_schema_snapshots_ordering,
            assert_table_schema_column_names_types,
        },
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::create_partitioned_table,
    },
    types::{Event, EventType, PipelineId, Type},
};
use etl_postgres::{tokio::test_utils::TableModification, types::TableId};
use etl_telemetry::tracing::init_test_tracing;
use rand::random;

fn get_last_relation_event(events: &[Event], table_id: TableId) -> &Event {
    events
        .iter()
        .rev()
        .find(|e| matches!(e, Event::Relation(r) if r.replicated_table_schema.id() == table_id))
        .expect("no relation events for table")
}

fn get_last_insert_event(events: &[Event], table_id: TableId) -> &Event {
    events
        .iter()
        .rev()
        .find(|e| matches!(e, Event::Insert(i) if i.replicated_table_schema.id() == table_id))
        .expect("no insert events for table")
}

fn schema_columns(schema: &etl_postgres::types::TableSchema) -> Vec<(String, Type)> {
    schema.column_schemas.iter().map(|column| (column.name.clone(), column.typ.clone())).collect()
}

fn find_snapshot_index_after(
    snapshots: &[(etl_postgres::types::SnapshotId, etl_postgres::types::TableSchema)],
    start_index: usize,
    expected: &[(&str, Type)],
) -> usize {
    let expected =
        expected.iter().map(|(name, typ)| ((*name).to_owned(), typ.clone())).collect::<Vec<_>>();

    snapshots
        .iter()
        .enumerate()
        .skip(start_index)
        .find_map(|(index, (_, schema))| (schema_columns(schema) == expected).then_some(index))
        .expect("expected schema snapshot in order")
}

async fn wait_for_at_least_events(
    destination: &TestDestinationWrapper<MemoryDestination<NotifyingStore>>,
    conditions: Vec<(EventType, u64)>,
) {
    destination
        .notify_on_events(move |events| {
            let grouped = group_events_by_type(events);
            conditions.iter().all(|(event_type, expected_count)| {
                grouped
                    .get(event_type)
                    .is_some_and(|matched_events| matched_events.len() >= *expected_count as usize)
            })
        })
        .await
        .notified()
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_added() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_add_column",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_received = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text not null" }],
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

    events_received.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );
    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 4);

    // Verify schema snapshots are stored in order.
    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_removed() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_remove_column",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_received = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    database.insert_values(table_name.clone(), &["name"], &[&"Bob"]).await.unwrap();

    events_received.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[("id", Type::INT8), ("name", Type::TEXT)],
    );
    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 2);

    // Verify schema snapshots are stored in order.
    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("name", Type::TEXT)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_renamed() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_rename_column",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_received = destination
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
        .insert_values(table_name.clone(), &["full_name", "age"], &[&"Carol", &41])
        .await
        .unwrap();

    events_received.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[("id", Type::INT8), ("full_name", Type::TEXT), ("age", Type::INT4)],
    );
    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 3);

    // Verify schema snapshots are stored in order.
    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("full_name", Type::TEXT), ("age", Type::INT4)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_type_changes() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_change_type",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_received = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn { name: "age", alteration: "type bigint" }],
        )
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "age"], &[&"Dave", &45_i64])
        .await
        .unwrap();

    events_received.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT8)],
    );
    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 3);

    // Verify schema snapshots are stored in order.
    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT8)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_without_dml_stores_schema_snapshot() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_add_column_no_dml",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let schema_stored = store.notify_on_table_schema_count(table_id, 2).await;

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

    schema_stored.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );

    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

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
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );

    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 4);

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_recovers_after_multiple_schema_changes_and_restart() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_multi_change_restart",
            &[("name", "text not null"), ("age", "integer not null"), ("status", "text not null")],
        )
        .await;

    // Add column + insert, then restart.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn { name: "email", data_type: "text not null" }],
        )
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["name", "age", "status", "email"],
            &[&"Alice", &25, &"active", &"alice@example.com"],
        )
        .await
        .unwrap();

    wait_for_at_least_events(&destination, vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

    destination.clear_events().await;

    // Rename column + change type + insert, then restart.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication.clone(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "age", new_name: "years" }],
        )
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AlterColumn { name: "years", alteration: "type bigint" }],
        )
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["name", "years", "status", "email"],
            &[&"Bob", &30_i64, &"pending", &"bob@example.com"],
        )
        .await
        .unwrap();

    wait_for_at_least_events(&destination, vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

    destination.clear_events().await;

    // Drop column + insert, then restart.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication.clone(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "status" }])
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["name", "years", "email"],
            &[&"Carol", &35_i64, &"carol@example.com"],
        )
        .await
        .unwrap();

    wait_for_at_least_events(&destination, vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

    destination.clear_events().await;

    // Add another column + rename existing + insert, then verify.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "created_at",
                data_type: "timestamp not null default now()",
            }],
        )
        .await
        .unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::RenameColumn { old_name: "email", new_name: "contact_email" }],
        )
        .await
        .unwrap();

    database
        .insert_values(
            table_name.clone(),
            &["name", "years", "contact_email"],
            &[&"Dave", &40_i64, &"dave@example.com"],
        )
        .await
        .unwrap();

    wait_for_at_least_events(&destination, vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("contact_email", Type::TEXT),
            ("created_at", Type::TIMESTAMP),
        ],
    );
    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 5);

    // Verify the expected schema versions are stored in order.
    //
    // Around restarts we may re-observe equivalent schema state, so this test
    // checks that the important versions appear in order instead of pinning the
    // total snapshot count exactly.
    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_schema_snapshots_ordering(snapshots, true);
    let mut index = find_snapshot_index_after(
        snapshots,
        0,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("status", Type::TEXT)],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("status", Type::TEXT)],
    );

    index = find_snapshot_index_after(
        snapshots,
        index + 1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("age", Type::INT4),
            ("status", Type::TEXT),
            ("email", Type::TEXT),
        ],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("age", Type::INT4),
            ("status", Type::TEXT),
            ("email", Type::TEXT),
        ],
    );

    index = find_snapshot_index_after(
        snapshots,
        index + 1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT4),
            ("status", Type::TEXT),
            ("email", Type::TEXT),
        ],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT4),
            ("status", Type::TEXT),
            ("email", Type::TEXT),
        ],
    );

    index = find_snapshot_index_after(
        snapshots,
        index + 1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("status", Type::TEXT),
            ("email", Type::TEXT),
        ],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("status", Type::TEXT),
            ("email", Type::TEXT),
        ],
    );

    index = find_snapshot_index_after(
        snapshots,
        index + 1,
        &[("id", Type::INT8), ("name", Type::TEXT), ("years", Type::INT8), ("email", Type::TEXT)],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[("id", Type::INT8), ("name", Type::TEXT), ("years", Type::INT8), ("email", Type::TEXT)],
    );

    index = find_snapshot_index_after(
        snapshots,
        index + 1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("email", Type::TEXT),
            ("created_at", Type::TIMESTAMP),
        ],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("email", Type::TEXT),
            ("created_at", Type::TIMESTAMP),
        ],
    );

    index = find_snapshot_index_after(
        snapshots,
        index + 1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("contact_email", Type::TEXT),
            ("created_at", Type::TIMESTAMP),
        ],
    );
    assert_table_schema_column_names_types(
        &snapshots[index].1,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("contact_email", Type::TEXT),
            ("created_at", Type::TIMESTAMP),
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn partitioned_table_schema_change_updates_relation_message() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("partitioned_schema_change");
    let partition_specs = [("p1", "from (1) to (100)"), ("p2", "from (100) to (200)")];

    let (parent_table_id, _partition_table_ids) =
        create_partitioned_table(&database, table_name.clone(), &partition_specs).await.unwrap();

    // Insert initial data into partitions.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('event1', 50), ('event2', 150)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let publication_name = "test_partitioned_schema_change_pub".to_owned();
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    let parent_ready = state_store
        .notify_on_table_state_type(parent_table_id, TableReplicationPhaseType::Ready)
        .await;

    pipeline.start().await.unwrap();

    parent_ready.notified().await;

    // Wait for the Relation event (schema change) and Insert event.
    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 1), (EventType::Insert, 1)])
        .await;

    // Add a new column to the partitioned table.
    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "category",
                data_type: "text not null default 'default_category'",
            }],
        )
        .await
        .unwrap();

    // Insert a row with the new column into one of the partitions.
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key, category) values ('event3', 75, 'test_category')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Verify we received exactly 1 Relation event for the parent table.
    assert_eq!(grouped.get(&(EventType::Relation, parent_table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, parent_table_id)).unwrap().len(), 1);

    // Verify the Relation event has the updated schema with the new column.
    let Event::Relation(r) = get_last_relation_event(&events, parent_table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &r.replicated_table_schema,
        &[
            ("id", Type::INT8),
            ("data", Type::TEXT),
            ("partition_key", Type::INT4),
            ("category", Type::TEXT),
        ],
    );

    // Verify the Insert event has 4 columns.
    let Event::Insert(i) = get_last_insert_event(&events, parent_table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), 4);

    // Verify schema snapshots are stored in order.
    let table_schemas = state_store.get_table_schemas().await;
    let snapshots = table_schemas.get(&parent_table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, first_schema) = &snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("data", Type::TEXT), ("partition_key", Type::INT4)],
    );

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[
            ("id", Type::INT8),
            ("data", Type::TEXT),
            ("partition_key", Type::INT4),
            ("category", Type::TEXT),
        ],
    );
}
