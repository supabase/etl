#![cfg(feature = "test-utils")]

use etl::{
    data::{ArrayCell, Cell},
    event::{Event, EventType},
    pipeline::PipelineId,
    schema::{ColumnSchema, TableId},
    store::TableStateType,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::{EventCondition, group_events_by_type_and_table_id},
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
};
use etl_postgres::tokio::test_utils::TableModification;
use etl_telemetry::tracing::init_test_tracing;
use rand::random;
use tokio_postgres::types::Type;

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

fn schema_columns(schema: &etl::schema::TableSchema) -> Vec<(String, Type)> {
    schema.column_schemas.iter().map(|column| (column.name.clone(), column.typ.clone())).collect()
}

fn assert_column_default_contains<'a>(
    columns: impl IntoIterator<Item = &'a ColumnSchema>,
    column_name: &str,
    fragments: &[&str],
) {
    let column = columns
        .into_iter()
        .find(|column| column.name == column_name)
        .unwrap_or_else(|| panic!("expected column {column_name}"));
    let expression = column
        .default_expression
        .as_deref()
        .unwrap_or_else(|| panic!("expected default expression for column {column_name}"));
    let expression = expression.to_ascii_lowercase();

    for fragment in fragments {
        assert!(
            expression.contains(&fragment.to_ascii_lowercase()),
            "expected default expression for column {column_name} to contain {fragment:?}: \
             {expression}"
        );
    }
}

fn find_snapshot_index_after(
    snapshots: &[(etl::schema::SnapshotId, etl::schema::TableSchema)],
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
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
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
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
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
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
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

    // We take the relation events count after we applied the schema change so that
    // we can use that for the next assertion.
    let events_before_restart = destination.get_events().await;
    let grouped_events_before_restart = group_events_by_type_and_table_id(&events_before_restart);
    let relation_count_before_restart =
        grouped_events_before_restart.get(&(EventType::Relation, table_id)).map_or(0, Vec::len);

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    let notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(
                EventType::Relation,
                table_id,
                (relation_count_before_restart + 1) as u64,
            ),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
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
    assert_eq!(
        grouped.get(&(EventType::Relation, table_id)).unwrap().len(),
        relation_count_before_restart + 1
    );
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(relation) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &relation.replicated_table_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );
    let relation_email_column = relation
        .replicated_table_schema
        .column_schemas()
        .find(|column| column.name == "email")
        .unwrap();
    assert_eq!(
        relation_email_column.default_expression.as_deref(),
        Some("'unknown@example.com'::text")
    );

    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(insert.table_row.values().len(), 4);

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
async fn default_expressions_round_trip_through_schema_changes_and_defaulted_insert() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_default_shapes",
            &[("name", "text not null")],
        )
        .await;

    let events_received = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[
                TableModification::AddColumn {
                    name: "status_text",
                    data_type: "text not null default 'pending'::text",
                },
                TableModification::AddColumn {
                    name: "score",
                    data_type: "integer not null default (10 + 5)",
                },
                TableModification::AddColumn {
                    name: "active",
                    data_type: "boolean not null default true",
                },
                TableModification::AddColumn {
                    name: "created_at",
                    data_type: "timestamptz not null default now()",
                },
                TableModification::AddColumn {
                    name: "created_on",
                    data_type: "date not null default date '2026-01-01'",
                },
                TableModification::AddColumn {
                    name: "payload",
                    data_type: "jsonb not null default jsonb_build_object('source', 'api')",
                },
                TableModification::AddColumn {
                    name: "lower_name",
                    data_type: "text not null default lower('USER'::text)",
                },
                TableModification::AddColumn {
                    name: "label",
                    data_type: "text not null default coalesce(null::text, 'fallback'::text)",
                },
                TableModification::AddColumn {
                    name: "tags",
                    data_type: "text[] not null default array['alpha'::text, 'beta'::text]",
                },
                TableModification::AddColumn {
                    name: "fixed_uuid",
                    data_type: "uuid not null default '00000000-0000-0000-0000-000000000001'::uuid",
                },
                TableModification::AddColumn {
                    name: "expires_at",
                    data_type: "timestamptz not null default (now() + interval '30 days')",
                },
            ],
        )
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (name) values ('Alice')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    events_received.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 1);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(relation) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(
        &relation.replicated_table_schema,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("status_text", Type::TEXT),
            ("score", Type::INT4),
            ("active", Type::BOOL),
            ("created_at", Type::TIMESTAMPTZ),
            ("created_on", Type::DATE),
            ("payload", Type::JSONB),
            ("lower_name", Type::TEXT),
            ("label", Type::TEXT),
            ("tags", Type::TEXT_ARRAY),
            ("fixed_uuid", Type::UUID),
            ("expires_at", Type::TIMESTAMPTZ),
        ],
    );

    let relation_columns = relation.replicated_table_schema.column_schemas().collect::<Vec<_>>();
    assert_column_default_contains(relation_columns.iter().copied(), "status_text", &["pending"]);
    assert_column_default_contains(relation_columns.iter().copied(), "score", &["10", "5"]);
    assert_column_default_contains(relation_columns.iter().copied(), "active", &["true"]);
    assert_column_default_contains(relation_columns.iter().copied(), "created_at", &["now"]);
    assert_column_default_contains(relation_columns.iter().copied(), "created_on", &["2026-01-01"]);
    assert_column_default_contains(
        relation_columns.iter().copied(),
        "payload",
        &["jsonb_build_object", "source", "api"],
    );
    assert_column_default_contains(
        relation_columns.iter().copied(),
        "lower_name",
        &["lower", "user"],
    );
    assert_column_default_contains(
        relation_columns.iter().copied(),
        "label",
        &["coalesce", "fallback"],
    );
    assert_column_default_contains(
        relation_columns.iter().copied(),
        "tags",
        &["array", "alpha", "beta"],
    );
    assert_column_default_contains(
        relation_columns.iter().copied(),
        "fixed_uuid",
        &["00000000-0000-0000-0000-000000000001"],
    );
    assert_column_default_contains(
        relation_columns.iter().copied(),
        "expires_at",
        &["now", "30 days"],
    );

    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    let values = insert.table_row.values();
    assert_eq!(values.len(), 13);
    assert!(matches!(values[0], Cell::I64(_)));
    assert_eq!(values[1], Cell::String("Alice".to_owned()));
    assert_eq!(values[2], Cell::String("pending".to_owned()));
    assert_eq!(values[3], Cell::I32(15));
    assert_eq!(values[4], Cell::Bool(true));
    assert!(matches!(values[5], Cell::TimestampTz(_)));
    assert!(matches!(&values[6], Cell::Date(date) if date.to_string() == "2026-01-01"));
    assert_eq!(values[7], Cell::Json(serde_json::json!({ "source": "api" })));
    assert_eq!(values[8], Cell::String("user".to_owned()));
    assert_eq!(values[9], Cell::String("fallback".to_owned()));
    assert_eq!(
        values[10],
        Cell::Array(ArrayCell::String(vec![Some("alpha".to_owned()), Some("beta".to_owned())]))
    );
    assert!(
        matches!(&values[11], Cell::Uuid(uuid) if uuid.to_string() == "00000000-0000-0000-0000-000000000001")
    );
    assert!(matches!(values[12], Cell::TimestampTz(_)));

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(snapshots.len(), 2);
    assert_schema_snapshots_ordering(snapshots, true);

    let (_, second_schema) = &snapshots[1];
    assert_table_schema_column_names_types(
        second_schema,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("status_text", Type::TEXT),
            ("score", Type::INT4),
            ("active", Type::BOOL),
            ("created_at", Type::TIMESTAMPTZ),
            ("created_on", Type::DATE),
            ("payload", Type::JSONB),
            ("lower_name", Type::TEXT),
            ("label", Type::TEXT),
            ("tags", Type::TEXT_ARRAY),
            ("fixed_uuid", Type::UUID),
            ("expires_at", Type::TIMESTAMPTZ),
        ],
    );
    assert_column_default_contains(&second_schema.column_schemas, "status_text", &["pending"]);
    assert_column_default_contains(&second_schema.column_schemas, "score", &["10", "5"]);
    assert_column_default_contains(&second_schema.column_schemas, "active", &["true"]);
    assert_column_default_contains(&second_schema.column_schemas, "created_at", &["now"]);
    assert_column_default_contains(&second_schema.column_schemas, "created_on", &["2026-01-01"]);
    assert_column_default_contains(
        &second_schema.column_schemas,
        "payload",
        &["jsonb_build_object", "source", "api"],
    );
    assert_column_default_contains(&second_schema.column_schemas, "lower_name", &["lower", "user"]);
    assert_column_default_contains(
        &second_schema.column_schemas,
        "label",
        &["coalesce", "fallback"],
    );
    assert_column_default_contains(
        &second_schema.column_schemas,
        "tags",
        &["array", "alpha", "beta"],
    );
    assert_column_default_contains(
        &second_schema.column_schemas,
        "fixed_uuid",
        &["00000000-0000-0000-0000-000000000001"],
    );
    assert_column_default_contains(
        &second_schema.column_schemas,
        "expires_at",
        &["now", "30 days"],
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

    destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
        .await
        .notified()
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

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

    destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 2),
            EventCondition::TableCount(EventType::Insert, table_id, 2),
        ])
        .await
        .notified()
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

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

    destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 3),
            EventCondition::TableCount(EventType::Insert, table_id, 3),
        ])
        .await
        .notified()
        .await;
    pipeline.shutdown_and_wait().await.unwrap();

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

    destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 4),
            EventCondition::TableCount(EventType::Insert, table_id, 4),
        ])
        .await
        .notified()
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

    let parent_ready =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    parent_ready.notified().await;

    // Wait for the Relation event (schema change) and Insert event.
    let notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, parent_table_id, 1),
            EventCondition::TableCount(EventType::Insert, parent_table_id, 1),
        ])
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
