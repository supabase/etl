#![cfg(feature = "test-utils")]

use etl::{
    config::IntoConnectOptions,
    data::{ArrayCell, Cell, Date},
    event::{Event, EventType},
    pipeline::PipelineId,
    postgres::migrations::run_source_migrations,
    schema::{ColumnSchema, SnapshotId, TableId, TableName, TableSchema},
    store::{SchemaStore, StateStore, TableState, TableStateType},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::{EventCondition, group_events_by_type_and_table_id},
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{create_database_and_sync_done_pipeline_with_table, create_pipeline},
        schema::{
            assert_replicated_schema_column_names_types, assert_schema_snapshots_ordering,
            assert_table_schema_column_names_types,
        },
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::create_partitioned_table,
    },
};
use etl_postgres::{
    below_version,
    tokio::test_utils::TableModification,
    version::{POSTGRES_15, POSTGRES_18},
};
use etl_telemetry::{metrics::init_metrics_handle, tracing::init_test_tracing};
use pg_escape::quote_identifier;
use rand::random;
use sqlx::Connection;
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

/// Returns the nearest preceding relation for `table_id` before `index`.
fn get_relation_before_index(events: &[Event], table_id: TableId, index: usize) -> &Event {
    events[..index]
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event,
                Event::Relation(relation) if relation.replicated_table_schema.id() == table_id
            )
        })
        .expect("no relation event before the target event")
}

/// Returns the nearest preceding relation for `table_id` before its last
/// insert.
fn get_relation_before_last_insert(events: &[Event], table_id: TableId) -> &Event {
    let last_insert_index = events
        .iter()
        .rposition(|event| {
            matches!(event, Event::Insert(insert) if insert.replicated_table_schema.id() == table_id)
        })
        .expect("no insert events for table");

    get_relation_before_index(events, table_id, last_insert_index)
}

/// Returns the last truncate event that includes `table_id`.
fn get_last_truncate_event(events: &[Event], table_id: TableId) -> &Event {
    events
        .iter()
        .rev()
        .find(|event| {
            matches!(
                event,
                Event::Truncate(truncate)
                    if truncate.truncated_tables.iter().any(|schema| schema.id() == table_id)
            )
        })
        .expect("no truncate events for table")
}

/// Returns the nearest preceding relation for `table_id` before its last
/// truncate.
fn get_relation_before_last_truncate(events: &[Event], table_id: TableId) -> &Event {
    let last_truncate_index = events
        .iter()
        .rposition(|event| {
            matches!(
                event,
                Event::Truncate(truncate)
                    if truncate.truncated_tables.iter().any(|schema| schema.id() == table_id)
            )
        })
        .expect("no truncate events for table");

    get_relation_before_index(events, table_id, last_truncate_index)
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
        assert!(expression.contains(&fragment.to_ascii_lowercase()));
    }
}

/// Asserts that cleanup retained only the expected current schema.
fn assert_only_schema_snapshot<'a>(
    snapshots: &'a [(SnapshotId, TableSchema)],
    expected: &[(&str, Type)],
) -> &'a TableSchema {
    assert_eq!(snapshots.len(), 1);
    assert_schema_snapshots_ordering(snapshots, false);

    let (_, schema) = &snapshots[0];
    assert_table_schema_column_names_types(schema, expected);

    schema
}

/// Schema mutation exercised by [`run_relation_schema_change`].
#[derive(Clone, Copy)]
enum RelationSchemaChange {
    AddColumn,
    DropColumn,
    RenameColumn,
    ChangeColumnType,
    ChangeColumnDefault,
    ChangeColumnNullability,
}

/// Verifies that one table-schema mutation produces the matching Relation,
/// decodes the following row, and retains the current schema snapshot.
async fn run_relation_schema_change(change: RelationSchemaChange) {
    init_test_tracing();

    let table_suffix = match change {
        RelationSchemaChange::AddColumn => "schema_add_column",
        RelationSchemaChange::DropColumn => "schema_remove_column",
        RelationSchemaChange::RenameColumn => "schema_rename_column",
        RelationSchemaChange::ChangeColumnType => "schema_change_type",
        RelationSchemaChange::ChangeColumnDefault => "schema_change_default",
        RelationSchemaChange::ChangeColumnNullability => "schema_change_nullability",
    };
    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_sync_done_pipeline_with_table(
            table_suffix,
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    let expected_columns = match change {
        RelationSchemaChange::AddColumn => {
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
            vec![
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("email", Type::TEXT),
            ]
        }
        RelationSchemaChange::DropColumn => {
            database
                .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
                .await
                .unwrap();
            database.insert_values(table_name.clone(), &["name"], &[&"Bob"]).await.unwrap();
            vec![("id", Type::INT8), ("name", Type::TEXT)]
        }
        RelationSchemaChange::RenameColumn => {
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
            vec![("id", Type::INT8), ("full_name", Type::TEXT), ("age", Type::INT4)]
        }
        RelationSchemaChange::ChangeColumnType => {
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
            vec![("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT8)]
        }
        RelationSchemaChange::ChangeColumnDefault => {
            database
                .alter_table(
                    table_name.clone(),
                    &[TableModification::AlterColumn { name: "age", alteration: "set default 42" }],
                )
                .await
                .unwrap();
            database.insert_values(table_name.clone(), &["name"], &[&"Eve"]).await.unwrap();
            vec![("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)]
        }
        RelationSchemaChange::ChangeColumnNullability => {
            database
                .alter_table(
                    table_name.clone(),
                    &[TableModification::AlterColumn { name: "age", alteration: "drop not null" }],
                )
                .await
                .unwrap();
            database
                .run_sql(&format!(
                    "insert into {} (name, age) values ('Frank', null)",
                    table_name.as_quoted_identifier()
                ))
                .await
                .unwrap();
            vec![("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)]
        }
    };

    ready_notify.notified().await;
    events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 2);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 1);

    let Event::Relation(r) = get_last_relation_event(&events, table_id) else {
        panic!("expected relation event");
    };
    assert_replicated_schema_column_names_types(&r.replicated_table_schema, &expected_columns);
    let Event::Insert(i) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(i.table_row.values().len(), expected_columns.len());
    match change {
        RelationSchemaChange::ChangeColumnDefault => {
            assert_eq!(i.table_row.values()[2], Cell::I32(42));
        }
        RelationSchemaChange::ChangeColumnNullability => {
            assert_eq!(i.table_row.values()[2], Cell::Null);
        }
        _ => {}
    }
    assert_eq!(
        i.replicated_table_schema.inner().snapshot_id,
        r.replicated_table_schema.inner().snapshot_id
    );

    let relation_age =
        r.replicated_table_schema.column_schemas().find(|column| column.name == "age");
    match change {
        RelationSchemaChange::DropColumn => assert!(relation_age.is_none()),
        RelationSchemaChange::ChangeColumnDefault => {
            assert_eq!(relation_age.unwrap().default_expression.as_deref(), Some("42"));
        }
        RelationSchemaChange::ChangeColumnNullability => {
            assert!(relation_age.unwrap().nullable);
        }
        _ => {}
    }

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    let stored_schema = assert_only_schema_snapshot(snapshots, &expected_columns);
    let stored_age = stored_schema.column_schemas.iter().find(|column| column.name == "age");
    match change {
        RelationSchemaChange::DropColumn => assert!(stored_age.is_none()),
        RelationSchemaChange::ChangeColumnDefault => {
            assert_eq!(stored_age.unwrap().default_expression.as_deref(), Some("42"));
        }
        RelationSchemaChange::ChangeColumnNullability => {
            assert!(stored_age.unwrap().nullable);
        }
        _ => {}
    }
    assert_eq!(snapshots[0].0, r.replicated_table_schema.inner().snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_is_added() {
    run_relation_schema_change(RelationSchemaChange::AddColumn).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_is_removed() {
    run_relation_schema_change(RelationSchemaChange::DropColumn).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_is_renamed() {
    run_relation_schema_change(RelationSchemaChange::RenameColumn).await;
}

/// Intermediate defaults reach the destination even without a later row.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_snapshots_preserve_intermediate_defaults_without_dml() {
    let (mut database, table_name, table_id, _, destination, pipeline, _, _) =
        create_database_and_sync_done_pipeline_with_table(
            "ordered_defaults",
            &[("value", "integer")],
        )
        .await;
    let initial_relations = destination
        .get_events()
        .await
        .iter()
        .filter(|event| matches!(event, Event::Relation(_)))
        .count();
    let applied = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Relation,
            table_id,
            u64::try_from(initial_relations).unwrap() + 2,
        )])
        .await;

    let transaction = database.begin_transaction().await;
    transaction
        .run_sql(&format!(
            "alter table {} add column score integer default 1",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} alter column score set default 2",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.commit_transaction().await;

    applied.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let schemas = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    let changes = &schemas[initial_relations..];
    assert_eq!(changes.len(), 2);
    for (schema, default) in changes.iter().zip(["1", "2"]) {
        assert_column_default_contains(schema.column_schemas(), "score", &[default]);
    }
    assert!(changes[0].inner().snapshot_id < changes[1].inner().snapshot_id);
}

/// Runs an idle-table swap with optional disconnected DDL replay.
async fn run_idle_table_swap(restart: bool) {
    let mut database = spawn_source_database().await;
    let first = test_table_name("swap_orders");
    let second = test_table_name("swap_orders_new");
    let archived = TableName::new(first.schema.clone(), "orders_archived".to_owned());
    let first_id =
        database.create_table(first.clone(), true, &[("value", "integer")]).await.unwrap();
    let second_id =
        database.create_table(second.clone(), true, &[("value", "integer")]).await.unwrap();
    let publication = format!("pub_{}", random::<u32>());
    database.create_publication(&publication, &[first.clone(), second.clone()]).await.unwrap();
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication.clone(),
        store.clone(),
        destination.clone(),
    );
    let first_synced = store.notify_on_table_sync_complete(first_id).await;
    let second_synced = store.notify_on_table_sync_complete(second_id).await;

    pipeline.start().await.unwrap();

    first_synced.notified().await;
    second_synced.notified().await;

    if restart {
        pipeline.shutdown_and_wait().await.unwrap();
        pipeline =
            create_pipeline(&database.config, pipeline_id, publication, store, destination.clone());
    }
    let applied = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, second_id, 1)])
        .await;

    let transaction = database.begin_transaction().await;
    for sql in [
        format!(
            "alter table {} rename to {}",
            first.as_quoted_identifier(),
            quote_identifier(&archived.name)
        ),
        format!(
            "alter table {} rename to {}",
            second.as_quoted_identifier(),
            quote_identifier(&first.name)
        ),
        format!("insert into {} default values", first.as_quoted_identifier()),
    ] {
        transaction.run_sql(&sql).await.unwrap();
    }
    transaction.commit_transaction().await;
    if restart {
        pipeline.start().await.unwrap();
    }

    applied.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let first_rename = events
        .iter()
        .position(|event| {
            matches!(event, Event::Relation(relation)
            if relation.replicated_table_schema.id() == first_id
                && relation.replicated_table_schema.name() == &archived)
        })
        .unwrap();
    let second_rename = events
        .iter()
        .position(|event| {
            matches!(event, Event::Relation(relation)
            if relation.replicated_table_schema.id() == second_id
                && relation.replicated_table_schema.name() == &first)
        })
        .unwrap();
    let insert = events.iter().position(|event| matches!(event, Event::Insert(_))).unwrap();
    assert!(first_rename < second_rename);
    assert!(second_rename < insert);
    assert!(!events.iter().any(|event| matches!(event, Event::Insert(insert)
        if insert.replicated_table_schema.id() == first_id)));
}

/// A table's old name is freed before another table reuses it without DML.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_table_swap_does_not_require_writes_to_archived_table() {
    run_idle_table_swap(false).await;
}

/// Restart reconstructs ordered rename barriers without connection-local masks.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_table_swap_replays_after_restart_without_relation_masks() {
    run_idle_table_swap(true).await;
}

/// Each DDL barrier uses its captured publication columns, including physical
/// columns added while a publication column list hides them.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_snapshots_capture_each_publication_mask() {
    let (mut database, table_name, table_id, _, destination, pipeline, _, publication) =
        create_database_and_sync_done_pipeline_with_table(
            "ordered_publication_masks",
            &[("a", "integer"), ("b", "integer")],
        )
        .await;
    if below_version!(database.server_version(), POSTGRES_15) {
        pipeline.shutdown_and_wait().await.unwrap();
        return;
    }
    let applied = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    let transaction = database.begin_transaction().await;
    let table = table_name.as_quoted_identifier();
    let publication = quote_identifier(&publication);
    for sql in [
        format!("alter publication {publication} set table {table} (id, a)"),
        format!("alter table {table} add column c integer default 7"),
        format!("alter publication {publication} set table {table} (id, a, b, c)"),
        format!("insert into {table} (a, b) values (1, 2)"),
    ] {
        transaction.run_sql(&sql).await.unwrap();
    }
    transaction.commit_transaction().await;

    applied.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let masks = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => {
                Some(relation.replicated_table_schema.replication_mask().as_slice())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    // Three synthetic relations retain each DDL boundary. PostgreSQL's real
    // relation repeats the final snapshot and mask before the insert.
    assert_eq!(masks, [&[1, 1, 0][..], &[1, 1, 0, 0][..], &[1, 1, 1, 1][..], &[1, 1, 1, 1][..]]);
    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        unreachable!();
    };
    assert_eq!(
        insert.table_row.values(),
        &[Cell::I64(1), Cell::I32(1), Cell::I32(2), Cell::I32(7)]
    );
}

/// Verifies that equal current masks retain distinct all-column and explicit
/// filter behavior when a column is added, optionally after a Ready restart.
async fn run_add_column_with_equal_initial_masks(restart: bool) {
    for explicit in [false, true] {
        let database = spawn_source_database().await;
        if explicit && below_version!(database.server_version(), POSTGRES_15) {
            continue;
        }
        let name = test_table_name("equal_initial_masks");
        let table_id =
            database.create_table(name.clone(), true, &[("value", "integer")]).await.unwrap();
        let table = name.as_quoted_identifier();
        let columns = if explicit { " (id, value)" } else { "" };
        database
            .run_sql(&format!("create publication equal_masks_pub for table {table}{columns}"))
            .await
            .unwrap();
        let store = NotifyingStore::new();
        let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
        let pipeline_id = random();
        let mut pipeline = create_pipeline(
            &database.config,
            pipeline_id,
            "equal_masks_pub".to_owned(),
            store.clone(),
            destination.clone(),
        );
        let synced = store.notify_on_table_sync_complete(table_id).await;

        pipeline.start().await.unwrap();

        synced.notified().await;

        let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
        let first_insert = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
            .await;

        database.run_sql(&format!("insert into {table} (value) values (1)")).await.unwrap();

        first_insert.notified().await;
        ready.notified().await;

        if restart {
            // Ready has discarded the persisted handover decoder. The next
            // connection has no previous real relation for this table.
            pipeline.shutdown_and_wait().await.unwrap();
            pipeline = create_pipeline(
                &database.config,
                pipeline_id,
                "equal_masks_pub".to_owned(),
                store,
                destination.clone(),
            );
        }
        let synthetic = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Relation, table_id, 2)])
            .await;

        database
            .run_sql(&format!("alter table {table} add column extra integer default 7"))
            .await
            .unwrap();
        if restart {
            pipeline.start().await.unwrap();
        }

        // The DDL barrier must arrive before any row can cause PostgreSQL to
        // emit a real relation on this connection.
        synthetic.notified().await;

        let second_insert = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
            .await;

        database.run_sql(&format!("insert into {table} (value) values (2)")).await.unwrap();

        second_insert.notified().await;

        pipeline.shutdown_and_wait().await.unwrap();

        let events = destination.get_events().await;
        let relations = events
            .iter()
            .filter_map(|event| match event {
                Event::Relation(relation) => Some(&relation.replicated_table_schema),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(relations.len(), 3);
        assert_eq!(relations[0].replication_mask().as_slice(), &[1, 1]);
        let expected = [1, 1, u8::from(!explicit)];
        for relation in &relations[1..] {
            assert_eq!(relation.replication_mask().as_slice(), &expected);
            assert_eq!(relation.identity_mask().as_slice(), &[1, 0, 0]);
        }
        assert_eq!(relations[1].inner().snapshot_id, relations[2].inner().snapshot_id);
    }
}

/// A real relation cannot distinguish an all-column publication from an
/// explicit list covering every current column; DDL must preserve that policy.
#[tokio::test(flavor = "multi_thread")]
async fn synthetic_relations_preserve_filter_policy_on_add_column() {
    run_add_column_with_equal_initial_masks(false).await;
}

/// DDL replay reconstructs the filter without a retained decoder or a new real
/// relation, including explicit lists that previously selected every column.
#[tokio::test(flavor = "multi_thread")]
async fn synthetic_relations_restore_filter_policy_after_ready_restart() {
    run_add_column_with_equal_initial_masks(true).await;
}

/// A current catalog read or the first real relation after reconnect cannot
/// replace publication metadata captured at each older DDL boundary.
#[tokio::test(flavor = "multi_thread")]
async fn replayed_table_ddl_precedes_later_publication_filter_change() {
    let (mut database, name, table_id, store, destination, mut pipeline, pipeline_id, publication) =
        create_database_and_sync_done_pipeline_with_table(
            "replayed_publication_filter",
            &[("value", "integer")],
        )
        .await;
    if below_version!(database.server_version(), POSTGRES_15) {
        pipeline.shutdown_and_wait().await.unwrap();
        return;
    }
    let table = name.as_quoted_identifier();
    let quoted_publication = quote_identifier(&publication);
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let inserted = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    database
        .run_sql(&format!("alter publication {quoted_publication} set table {table} (id, value)"))
        .await
        .unwrap();
    database.run_sql(&format!("insert into {table} (value) values (1)")).await.unwrap();

    inserted.notified().await;
    ready.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
    destination.clear_events().await;

    let transaction = database.begin_transaction().await;
    for statement in [
        format!("alter table {table} add column extra integer default 1"),
        format!("insert into {table} (value) values (2)"),
        format!("alter table {table} alter column extra set default 2"),
        format!("alter publication {quoted_publication} set table {table} (id, value, extra)"),
        format!("insert into {table} (value) values (3)"),
    ] {
        transaction.run_sql(&statement).await.unwrap();
    }
    transaction.commit_transaction().await;

    pipeline =
        create_pipeline(&database.config, pipeline_id, publication, store, destination.clone());
    let replayed = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    pipeline.start().await.unwrap();

    replayed.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relations.len(), 5);
    for (relation, (expected_mask, default)) in relations.iter().zip([
        ([1, 1, 0], "1"),
        ([1, 1, 0], "1"),
        ([1, 1, 0], "2"),
        ([1, 1, 1], "2"),
        ([1, 1, 1], "2"),
    ]) {
        assert_eq!(relation.replication_mask().as_slice(), &expected_mask);
        assert_eq!(relation.identity_mask().as_slice(), &[1, 0, 0]);
        assert_column_default_contains(&relation.inner().column_schemas, "extra", &[default]);
    }
    let rows = events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) => Some(insert.table_row.values()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [&[Cell::I64(2), Cell::I32(2)][..], &[Cell::I64(3), Cell::I32(3), Cell::I32(2)]]
    );
}

/// Copy and synthetic relations resolve publication attnums against their full
/// snapshot, preserving gaps and column identity across rename and name reuse.
#[tokio::test(flavor = "multi_thread")]
async fn publication_attnums_preserve_dropped_column_gaps_and_renames() {
    let mut database = spawn_source_database().await;
    if below_version!(database.server_version(), POSTGRES_15) {
        return;
    }
    let name = test_table_name("publication_attnum_gaps");
    let table_id = database
        .create_table(name.clone(), true, &[("discarded", "integer"), ("selected", "integer")])
        .await
        .unwrap();
    let table = name.as_quoted_identifier();
    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "alter table {table} drop column discarded; create publication attnum_gaps_pub for \
             table {table} (id, selected); insert into {table} (selected) values (5)"
        ))
        .await
        .unwrap();
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        "attnum_gaps_pub".to_owned(),
        store.clone(),
        destination.clone(),
    );
    let synced = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    synced.notified().await;

    let inserted = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    let transaction = database.begin_transaction().await;
    for statement in [
        format!("alter table {table} rename column selected to renamed"),
        format!("alter table {table} add column discarded integer default 7"),
        format!("insert into {table} (renamed) values (11)"),
    ] {
        transaction.run_sql(&statement).await.unwrap();
    }
    transaction.commit_transaction().await;

    inserted.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let copied = destination.get_table_rows().await;
    assert_eq!(copied[&table_id][0].values(), &[Cell::I64(1), Cell::I32(5)]);
    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relations.len(), 3);
    for (relation, expected_ordinals) in
        relations.iter().zip([vec![1, 3], vec![1, 3, 4], vec![1, 3, 4]])
    {
        let ordinals = relation
            .inner()
            .column_schemas
            .iter()
            .map(|column| column.ordinal_position)
            .collect::<Vec<_>>();
        assert_eq!(ordinals, expected_ordinals);
        assert_eq!(relation.inner().column_schemas[1].name, "renamed");
        assert_eq!(relation.column_schemas().count(), 2);
        assert_eq!(relation.identity_mask().as_slice()[..2], [1, 0]);
    }
    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        unreachable!();
    };
    assert_eq!(insert.table_row.values(), &[Cell::I64(2), Cell::I32(11)]);
}

/// Synthetic relations capture each replica-identity mode exactly as the real
/// PostgreSQL relation before the following row.
#[tokio::test(flavor = "multi_thread")]
async fn synthetic_and_real_relations_preserve_identity_masks() {
    let (database, table_name, table_id, _, destination, pipeline, _, _) =
        create_database_and_sync_done_pipeline_with_table(
            "synthetic_identity_masks",
            &[("value", "integer not null"), ("extra", "integer")],
        )
        .await;
    let table = table_name.as_quoted_identifier();
    database
        .run_sql(&format!("create unique index synthetic_identity_idx on {table} (value)"))
        .await
        .unwrap();
    let modes = [
        ("full", vec![1, 1, 1]),
        ("using index synthetic_identity_idx", vec![0, 1, 0]),
        ("nothing", vec![0, 0, 0]),
        ("default", vec![1, 0, 0]),
    ];
    for (index, (mode, _)) in modes.iter().enumerate() {
        let inserted = destination
            .wait_for_events(vec![EventCondition::TableCount(
                EventType::Insert,
                table_id,
                u64::try_from(index).unwrap() + 1,
            )])
            .await;

        database.run_sql(&format!("alter table {table} replica identity {mode}")).await.unwrap();
        database
            .run_sql(&format!("insert into {table} (value, extra) values ({index}, 7)"))
            .await
            .unwrap();

        inserted.notified().await;
    }
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relations.len(), 2 * modes.len());
    for (pair, (_, expected_mask)) in relations.chunks_exact(2).zip(modes) {
        assert_eq!(pair[0].inner(), pair[1].inner());
        assert_eq!(pair[0].replication_mask(), pair[1].replication_mask());
        assert_eq!(pair[0].identity_mask().as_slice(), expected_mask);
        assert_eq!(pair[0].identity_mask(), pair[1].identity_mask());
    }
}

/// Copy, synthetic relations, and real PostgreSQL relations agree on their
/// publication and identity masks at the same catalog state.
#[tokio::test(flavor = "multi_thread")]
async fn copy_synthetic_and_real_relations_share_decoding_masks() {
    for mode in [
        "default",
        "full",
        "index",
        "index_include",
        "deferrable_index",
        "composite",
        "nothing",
        "filtered",
        "full_filtered",
        "deferrable",
        "inherited",
        "many_publications",
        "quoted_publication",
    ] {
        let database = spawn_source_database().await;
        if matches!(mode, "filtered" | "full_filtered")
            && below_version!(database.server_version(), POSTGRES_15)
        {
            continue;
        }
        let name = test_table_name("decoder_consistency");
        let table_id = database
            .create_table(
                name.clone(),
                true,
                &[("value", "integer not null"), ("extra", "integer")],
            )
            .await
            .unwrap();
        let table = name.as_quoted_identifier();
        let primary_key_name = format!("{}_pkey", name.name);
        let primary_key = quote_identifier(&primary_key_name);
        match mode {
            "full" | "full_filtered" => database
                .run_sql(&format!("alter table {table} replica identity full"))
                .await
                .unwrap(),
            "nothing" => database
                .run_sql(&format!("alter table {table} replica identity nothing"))
                .await
                .unwrap(),
            "index" | "index_include" | "composite" | "deferrable_index" => {
                let index_columns = match mode {
                    "index_include" => "(value) include (extra)",
                    "composite" => {
                        database
                            .run_sql(&format!("alter table {table} alter extra set not null"))
                            .await
                            .unwrap();
                        "(extra, value)"
                    }
                    _ => "(value)",
                };
                database
                    .run_sql(&format!(
                        "create unique index decoder_identity_idx on {table} {index_columns}"
                    ))
                    .await
                    .unwrap();
                database
                    .run_sql(&format!(
                        "alter table {table} replica identity using index decoder_identity_idx"
                    ))
                    .await
                    .unwrap();
                if mode == "deferrable_index" {
                    // Attaching a DEFERRABLE constraint can make an already
                    // selected identity index ineligible without clearing its
                    // indisreplident flag.
                    database
                        .run_sql(&format!(
                            "alter table {table} add constraint decoder_deferred unique using \
                             index decoder_identity_idx deferrable"
                        ))
                        .await
                        .unwrap();
                }
                0
            }
            "deferrable" => database
                .run_sql(&format!(
                    "alter table {table} drop constraint {primary_key}, add primary key (id) \
                     deferrable"
                ))
                .await
                .unwrap(),
            "inherited" => {
                database
                    .run_sql("create table test.decoder_parent (id bigint primary key)")
                    .await
                    .unwrap();
                database
                    .run_sql(&format!("alter table {table} drop constraint {primary_key}"))
                    .await
                    .unwrap();
                database
                    .run_sql(&format!("alter table {table} inherit test.decoder_parent"))
                    .await
                    .unwrap()
            }
            _ => 0,
        };
        let columns = if matches!(mode, "filtered" | "full_filtered") { " (value)" } else { "" };
        let publication =
            if mode == "quoted_publication" { "Decoder's \"pub,旧" } else { "decoder_pub" };
        database
            .run_sql(&format!(
                "create publication {} for table {table}{columns} with (publish = 'insert')",
                quote_identifier(publication),
            ))
            .await
            .unwrap();
        if mode == "many_publications" {
            for index in 0..99 {
                database
                    .run_sql(&format!("create publication decoder_extra_{index} for table {table}"))
                    .await
                    .unwrap();
            }
            let payload: String = database
                .client
                .as_ref()
                .unwrap()
                .query_one(
                    &format!(
                        "select jsonb_object_agg(publication_name, column_attnums)::text from \
                         etl.describe_table_publication_columns({table_id})"
                    ),
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            let publications: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(publications.as_object().unwrap().len(), 100);
            assert!(publications.as_object().unwrap().values().all(serde_json::Value::is_null));
        }
        let store = NotifyingStore::new();
        let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
        let mut pipeline = create_pipeline(
            &database.config,
            random(),
            publication.to_owned(),
            store.clone(),
            destination.clone(),
        );
        let synced = store.notify_on_table_sync_complete(table_id).await;

        pipeline.start().await.unwrap();

        synced.notified().await;

        let expected_replication = if matches!(mode, "filtered" | "full_filtered") {
            vec![0, 1, 0]
        } else {
            vec![1, 1, 1]
        };
        let expected_identity = match mode {
            "full" => vec![1, 1, 1],
            "index" | "index_include" | "full_filtered" => vec![0, 1, 0],
            "composite" => vec![0, 1, 1],
            "nothing" | "filtered" | "deferrable" | "deferrable_index" | "inherited" => {
                vec![0, 0, 0]
            }
            _ => vec![1, 0, 0],
        };
        let current_state = store.get_table_state(table_id).await.unwrap();
        let mut states = store.get_table_state_history(table_id).await;
        states.extend(current_state);
        let decoder = states
            .iter()
            .find_map(|state| match state {
                TableState::SyncDone { table_decoding_state: Some(decoder), .. } => Some(decoder),
                _ => None,
            })
            .unwrap();
        let decoder = serde_json::to_value(decoder).unwrap();

        let inserted = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
            .await;

        database.run_sql(&format!("alter table {table} owner to current_user")).await.unwrap();
        database
            .run_sql(&format!("insert into {table} (value, extra) values (5, 7)"))
            .await
            .unwrap();

        inserted.notified().await;

        pipeline.shutdown_and_wait().await.unwrap();

        let events = destination.get_events().await;
        let relations = events
            .iter()
            .filter_map(|event| match event {
                Event::Relation(relation) => Some(&relation.replicated_table_schema),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(relations.len(), 2, "{mode}");
        // Check the real relation before comparing capture and copy metadata.
        assert_eq!(relations[1].identity_mask().as_slice(), expected_identity, "{mode}");
        assert_eq!(decoder["replication_mask"], serde_json::json!(expected_replication), "{mode}");
        assert_eq!(decoder["identity_mask"], serde_json::json!(expected_identity), "{mode}");
        for relation in relations {
            assert_eq!(relation.replication_mask().as_slice(), expected_replication, "{mode}");
            assert_eq!(relation.identity_mask().as_slice(), expected_identity, "{mode}");
        }
    }
}

/// A real relation refreshes identity after synthetic DDL without selecting a
/// future schema already retained in the store.
#[tokio::test(flavor = "multi_thread")]
async fn real_relation_updates_identity_after_replica_index_drop() {
    let (database, table_name, table_id, store, destination, pipeline, _, _) =
        create_database_and_sync_done_pipeline_with_table(
            "replica_index_drop",
            &[("value", "integer not null")],
        )
        .await;
    let table = table_name.as_quoted_identifier();
    let applied = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "create unique index drop_identity_idx on {table} (value); alter table {table} \
             replica identity using index drop_identity_idx; insert into {table} (value) values \
             (1)"
        ))
        .await
        .unwrap();

    applied.notified().await;
    ready.notified().await;

    // A failed attempt or another worker may have stored schemas this decoder
    // has not observed. Only the local DDL boundary determines its lineage.
    let events = destination.get_events().await;
    let Event::Relation(relation) = get_last_relation_event(&events, table_id) else {
        unreachable!();
    };
    let observed_snapshot_id = relation.replicated_table_schema.inner().snapshot_id;
    let mut future_schema = relation.replicated_table_schema.inner().clone();
    future_schema.snapshot_id = SnapshotId::max();
    future_schema.column_schemas[1].name = "future_value".into();
    store.store_table_schema(future_schema).await.unwrap();

    let applied = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "drop index test.drop_identity_idx; insert into {table} (value) values (2)"
        ))
        .await
        .unwrap();

    applied.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    let identities =
        relations.iter().map(|schema| schema.identity_mask().as_slice()).collect::<Vec<_>>();
    assert_eq!(identities, vec![&[0, 1][..], &[0, 1][..], &[0, 0][..]]);
    for schema in relations {
        assert_eq!(schema.inner().snapshot_id, observed_snapshot_id);
        assert_eq!(schema.inner().column_schemas[1].name, "value");
        assert_eq!(schema.replication_mask().as_slice(), &[1, 1]);
    }
    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        unreachable!();
    };
    assert_eq!(insert.replicated_table_schema.inner().snapshot_id, observed_snapshot_id);
    assert_eq!(insert.table_row.values(), &[Cell::I64(2), Cell::I32(2)]);
}

/// Publication root mode selects the root's filter; leaf mode uses each
/// leaf's own filter. PostgreSQL forbids a root list in leaf mode.
#[tokio::test(flavor = "multi_thread")]
async fn partition_filters_match_copy_synthetic_and_real_relations() {
    for via_root in [false, true] {
        let database = spawn_source_database().await;
        if below_version!(database.server_version(), POSTGRES_15) {
            return;
        }
        let name = test_table_name("partition_filtered_decoder");
        let (root, leaves) = create_partitioned_table(
            &database,
            name.clone(),
            &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
        )
        .await
        .unwrap();
        let table = name.as_quoted_identifier();
        let first_leaf = TableName::new(name.schema.clone(), format!("{}_first", name.name));
        let root_columns = if via_root { " (id, partition_key)" } else { "" };
        database
            .run_sql(&format!(
                "create publication partition_filter_pub for table {table}{root_columns}, {} \
                 (data)
             with (publish = 'insert', publish_via_partition_root = {via_root})",
                first_leaf.as_quoted_identifier(),
            ))
            .await
            .unwrap();
        let expected = if via_root {
            vec![(root, vec![1, 0, 1, 0], vec![1, 0, 1, 0])]
        } else {
            vec![
                (leaves[0], vec![0, 1, 0, 0], vec![0, 0, 0, 0]),
                (leaves[1], vec![1, 1, 1, 1], vec![1, 0, 1, 0]),
            ]
        };
        let store = NotifyingStore::new();
        let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
        let mut pipeline = create_pipeline(
            &database.config,
            random(),
            "partition_filter_pub".to_owned(),
            store.clone(),
            destination.clone(),
        );
        let mut synced = Vec::new();
        for (target, _, _) in &expected {
            synced.push(store.notify_on_table_sync_complete(*target).await);
        }

        pipeline.start().await.unwrap();

        for notification in synced {
            notification.notified().await;
        }
        for (target, mask, identity) in &expected {
            // Read current first: if SyncDone becomes Ready between reads,
            // its decoder is either in this clone or the later history.
            let current_state = store.get_table_state(*target).await.unwrap();
            let mut states = store.get_table_state_history(*target).await;
            states.extend(current_state);
            let decoder = states
                .iter()
                .find_map(|state| match state {
                    TableState::SyncDone { table_decoding_state: Some(decoder), .. } => {
                        Some(decoder)
                    }
                    _ => None,
                })
                .unwrap();
            let decoder = serde_json::to_value(decoder).unwrap();
            assert_eq!(decoder["replication_mask"], serde_json::json!(&mask[..3]));
            assert_eq!(decoder["identity_mask"], serde_json::json!(&identity[..3]));
        }
        let described = destination
            .wait_for_events(
                expected
                    .iter()
                    .map(|(target, _, _)| {
                        EventCondition::TableCount(EventType::Relation, *target, 1)
                    })
                    .collect(),
            )
            .await;

        database
            .run_sql(&format!("alter table {table} add column score integer default 7"))
            .await
            .unwrap();

        described.notified().await;

        let inserted = destination
            .wait_for_events(
                expected
                    .iter()
                    .map(|(target, _, _)| {
                        EventCondition::TableCount(
                            EventType::Insert,
                            *target,
                            if via_root { 2 } else { 1 },
                        )
                    })
                    .collect(),
            )
            .await;

        database
            .run_sql(&format!(
                "insert into {table} (data, partition_key) values ('first', 1), ('second', 101)"
            ))
            .await
            .unwrap();

        inserted.notified().await;
        pipeline.shutdown_and_wait().await.unwrap();

        let events = destination.get_events().await;
        for (target, mask, identity) in expected {
            let relations: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    Event::Relation(relation)
                        if relation.replicated_table_schema.id() == target =>
                    {
                        Some(&relation.replicated_table_schema)
                    }
                    _ => None,
                })
                .collect();
            assert!(relations.len() >= 2);
            for relation in relations {
                assert_eq!(relation.replication_mask().as_slice(), mask);
                assert_eq!(relation.identity_mask().as_slice(), identity);
            }
        }
    }
}

/// PostgreSQL 18 accepts an invalid partitioned primary index as default
/// identity, while earlier supported versions require a valid index.
#[tokio::test(flavor = "multi_thread")]
async fn invalid_partitioned_primary_index_matches_real_identity() {
    let database = spawn_source_database().await;
    let name = test_table_name("invalid_partition_identity");
    let (table_id, _) =
        create_partitioned_table(&database, name.clone(), &[("leaf", "from (0) to (100)")])
            .await
            .unwrap();
    let table = name.as_quoted_identifier();
    let primary_key_name = format!("{}_pkey", name.name);
    let primary_key = quote_identifier(&primary_key_name);
    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(&format!(
            "alter table {table} drop constraint {primary_key};
             alter table only {table} add primary key (id, partition_key);
             create publication invalid_partition_pub for table {table}
                 with (publish = 'insert', publish_via_partition_root = true)"
        ))
        .await
        .unwrap();
    let index_valid: bool = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select indisvalid from pg_catalog.pg_index where indrelid = $1 and indisprimary",
            &[&table_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!index_valid);
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let mut pipeline = create_pipeline(
        &database.config,
        random(),
        "invalid_partition_pub".into(),
        store.clone(),
        destination.clone(),
    );
    let synced = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    synced.notified().await;

    let inserted = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    database.run_sql(&format!("alter table {table} owner to current_user")).await.unwrap();
    database
        .run_sql(&format!("insert into {table} (data, partition_key) values ('row', 1)"))
        .await
        .unwrap();

    inserted.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let expected = if below_version!(database.server_version(), POSTGRES_18) {
        vec![0, 0, 0]
    } else {
        vec![1, 0, 1]
    };
    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relations.len(), 2);
    // The last event comes from pgoutput, providing the native reference.
    assert_eq!(relations[1].identity_mask().as_slice(), expected);
    assert_eq!(relations[0].identity_mask().as_slice(), expected);
    assert_eq!(relations[0].inner(), relations[1].inner());
    let current = store.get_table_state(table_id).await.unwrap();
    let mut states = store.get_table_state_history(table_id).await;
    states.extend(current);
    let decoder = states
        .iter()
        .find_map(|state| match state {
            TableState::SyncDone { table_decoding_state: Some(decoder), .. } => Some(decoder),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(decoder).unwrap()["identity_mask"],
        serde_json::json!(expected)
    );
}

/// Root and leaf DDL refresh complete decoders for every published identity.
#[tokio::test(flavor = "multi_thread")]
async fn tree_ddl_emits_synthetic_relations_for_effective_partition_targets() {
    for via_root in [false, true] {
        let database = spawn_source_database().await;
        let name = test_table_name("partition_ddl_decoder");
        let (root, leaves) = create_partitioned_table(
            &database,
            name.clone(),
            &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
        )
        .await
        .unwrap();
        let table = name.as_quoted_identifier();
        database
            .run_sql(&format!(
                "create publication partition_ddl_pub for table {table} with \
                 (publish_via_partition_root = {via_root})"
            ))
            .await
            .unwrap();
        let leaf_name = format!("{}_first", name.name);
        let leaf =
            etl::schema::TableName::new(name.schema.clone(), leaf_name).as_quoted_identifier();
        let targets = if via_root { vec![root] } else { leaves };
        let store = NotifyingStore::new();
        let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
        let mut pipeline = create_pipeline(
            &database.config,
            random(),
            "partition_ddl_pub".to_owned(),
            store.clone(),
            destination.clone(),
        );
        let mut synced = Vec::new();
        for target in &targets {
            synced.push(store.notify_on_table_sync_complete(*target).await);
        }

        pipeline.start().await.unwrap();

        for ready in synced {
            ready.notified().await;
        }
        let ddl_applied = destination
            .wait_for_events(
                targets
                    .iter()
                    .map(|target| EventCondition::TableCount(EventType::Relation, *target, 2))
                    .collect(),
            )
            .await;

        database
            .client
            .as_ref()
            .unwrap()
            .batch_execute(&format!(
                "alter table {table} add column score int default 1; alter table {leaf} alter \
                 score set default 2"
            ))
            .await
            .unwrap();

        ddl_applied.notified().await;

        let inserted = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, targets[0], 1)])
            .await;

        database
            .run_sql(&format!("insert into {table} (data, partition_key) values ('example', 1)"))
            .await
            .unwrap();

        inserted.notified().await;

        pipeline.shutdown_and_wait().await.unwrap();

        let events = destination.get_events().await;
        for target in &targets {
            let relations = events
                .iter()
                .filter_map(|event| match event {
                    Event::Relation(relation)
                        if relation.replicated_table_schema.id() == *target =>
                    {
                        Some(&relation.replicated_table_schema)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            let count = if *target == targets[0] { 3 } else { 2 };
            assert_eq!(relations.len(), count);
            for relation in &relations {
                assert_eq!(relation.replication_mask().as_slice(), &[1, 1, 1, 1]);
                assert_eq!(relation.identity_mask().as_slice(), &[1, 0, 1, 0]);
            }
            let score = relations[1].inner().column_schemas.last().unwrap();
            let expected_default = if !via_root && *target == targets[0] { "2" } else { "1" };
            assert_eq!(score.default_expression.as_deref(), Some(expected_default));
            if relations.len() == 3 {
                assert_eq!(relations[1].inner(), relations[2].inner());
            }
        }
    }
}

/// Rolled-back DDL never produces a synthetic destination relation.
#[tokio::test(flavor = "multi_thread")]
async fn rolled_back_ddl_does_not_emit_synthetic_relation() {
    let (mut database, table_name, table_id, _, destination, pipeline, _, _) =
        create_database_and_sync_done_pipeline_with_table(
            "rolled_back_synthetic_relation",
            &[("value", "integer")],
        )
        .await;
    let applied = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    let mut transaction = database.begin_transaction().await;
    transaction
        .run_sql(&format!(
            "alter table {} add column discarded integer default 1",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.client.take().unwrap().rollback().await.unwrap();
    drop(transaction);
    database
        .run_sql(&format!("insert into {} (value) values (7)", table_name.as_quoted_identifier()))
        .await
        .unwrap();

    applied.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].inner().snapshot_id, SnapshotId::initial());
    assert_eq!(
        relations[0].column_schemas().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        ["id", "value"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_type_changes() {
    run_relation_schema_change(RelationSchemaChange::ChangeColumnType).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_default_changes() {
    run_relation_schema_change(RelationSchemaChange::ChangeColumnDefault).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn relation_message_updates_when_column_nullability_changes() {
    run_relation_schema_change(RelationSchemaChange::ChangeColumnNullability).await;
}

/// Replays queued pre-synthetic WAL and new DDL under one replica-identity
/// mode without recopying the table.
async fn run_queued_legacy_and_synthetic_relations(explicit_index: bool) {
    init_test_tracing();
    let (
        mut database,
        table_name,
        table_id,
        store,
        destination,
        pipeline,
        pipeline_id,
        publication,
    ) = create_database_and_sync_done_pipeline_with_table(
        "queued_ddl_upgrade",
        &[("value", "integer not null default 10")],
    )
    .await;
    let table = table_name.as_quoted_identifier();
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let warm_insert = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    database.run_sql(&format!("insert into {table} default values")).await.unwrap();

    warm_insert.notified().await;
    ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    let copy_writes = destination.write_table_rows_called().await;
    destination.clear_events().await;

    // Roll the source back to its last pre-synthetic migration while the
    // pipeline is stopped. Its existing slot retains the legacy WAL payload.
    let options: sqlx::postgres::PgConnectOptions = database.config.with_db(None);
    let mut connection = sqlx::PgConnection::connect_with(&options).await.unwrap();
    sqlx::query("set search_path = 'etl'").execute(&mut connection).await.unwrap();
    let mut migrator = sqlx::migrate!("./migrations/source");
    migrator.set_ignore_missing(true);
    migrator.undo(&mut connection, 20261002120000).await.unwrap();
    drop(connection);
    assert!(
        database
            .client
            .as_ref()
            .unwrap()
            .query_one(
                "select pg_catalog.to_regprocedure('etl.describe_table_columns(oid)') is null",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let transaction = database.begin_transaction().await;
    if explicit_index {
        transaction
            .run_sql(&format!("create unique index upgrade_identity on {table} (value)"))
            .await
            .unwrap();
        transaction
            .run_sql(&format!("alter table {table} replica identity using index upgrade_identity"))
            .await
            .unwrap();
    }
    transaction
        .run_sql(&format!("alter table {table} add column legacy integer default 1"))
        .await
        .unwrap();
    transaction.run_sql(&format!("insert into {table} (value) values (20)")).await.unwrap();
    transaction.commit_transaction().await;

    run_source_migrations(&database.config).await.unwrap();

    let transaction = database.begin_transaction().await;
    transaction
        .run_sql(&format!(
            "alter table {table} add column captured integer default 2, alter column captured set \
             default 3"
        ))
        .await
        .unwrap();
    transaction.run_sql(&format!("insert into {table} (value) values (30)")).await.unwrap();
    transaction.commit_transaction().await;

    let replayed = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    replayed.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();
    assert_eq!(destination.write_table_rows_called().await, copy_writes);

    let events = destination.get_events().await;
    let inserts: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert)
            }
            _ => None,
        })
        .collect();
    assert_eq!(inserts.len(), 2);
    assert_eq!(inserts[0].table_row.values(), &[Cell::I64(2), Cell::I32(20), Cell::I32(1)]);
    assert_eq!(
        inserts[1].table_row.values(),
        &[Cell::I64(3), Cell::I32(30), Cell::I32(1), Cell::I32(3)]
    );
    let legacy = &inserts[0].replicated_table_schema;
    let captured = &inserts[1].replicated_table_schema;
    assert_replicated_schema_column_names_types(
        legacy,
        &[("id", Type::INT8), ("value", Type::INT4), ("legacy", Type::INT4)],
    );
    assert_replicated_schema_column_names_types(
        captured,
        &[
            ("id", Type::INT8),
            ("value", Type::INT4),
            ("legacy", Type::INT4),
            ("captured", Type::INT4),
        ],
    );
    assert_eq!(
        legacy
            .inner()
            .column_schemas
            .iter()
            .map(|column| (column.name.as_str(), column.missing_value_expression.is_some()))
            .collect::<Vec<_>>(),
        [("id", false), ("value", false), ("legacy", false)]
    );
    let column =
        captured.inner().column_schemas.iter().find(|column| column.name == "captured").unwrap();
    assert_eq!(column.missing_value_expression.as_deref(), Some("2"));
    assert_eq!(column.default_expression.as_deref(), Some("3"));
    assert_eq!(legacy.replication_mask().as_slice(), &[1, 1, 1]);
    assert_eq!(captured.replication_mask().as_slice(), &[1, 1, 1, 1]);
    let identity = if explicit_index { [0, 1, 0, 0] } else { [1, 0, 0, 0] };
    assert_eq!(legacy.identity_mask().as_slice(), &identity[..3]);
    assert_eq!(captured.identity_mask().as_slice(), &identity);
}

/// Upgrade replay preserves both default and explicit-index identity while
/// consuming old and new payloads from the same retained replication slot.
#[tokio::test(flavor = "multi_thread")]
async fn source_upgrade_replays_queued_legacy_and_synthetic_relations() {
    for explicit_index in [false, true] {
        run_queued_legacy_and_synthetic_relations(explicit_index).await;
    }
}

/// Legacy DDL payloads retain their relation-less row fallback through
/// maintenance, no-op DDL, and truncate.
#[tokio::test(flavor = "multi_thread")]
async fn relationless_noop_schema_changes_reuse_previous_relation_masks() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _, _) =
        create_database_and_sync_done_pipeline_with_table(
            "relationless_noop_schema_changes",
            &[("name", "text not null"), ("age", "integer")],
        )
        .await;

    // Simulate WAL produced before publication columns were captured.
    database
        .client
        .as_ref()
        .unwrap()
        .batch_execute(include_str!(
            "../migrations/source/20260724120000_publication_schema_change_messages.up.sql"
        ))
        .await
        .unwrap();

    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let warm_insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    database.insert_values(table_name.clone(), &["name", "age"], &[&"Before", &1]).await.unwrap();

    ready_notify.notified().await;
    warm_insert_notify.notified().await;

    // Maintenance resends metadata while preserving the current snapshot.
    let quoted_table = table_name.as_quoted_identifier();
    let maintenance = [
        format!("analyze {quoted_table}"),
        format!("create index maintenance_name_idx on {quoted_table} (name)"),
        format!("vacuum {quoted_table}"),
    ];
    for (index, command) in maintenance.iter().enumerate() {
        let inserted = destination
            .wait_for_events(vec![EventCondition::TableCount(
                EventType::Insert,
                table_id,
                u64::try_from(index + 2).unwrap(),
            )])
            .await;

        database.run_sql(command).await.unwrap();
        let age = i32::try_from(index + 2).unwrap();
        database
            .insert_values(table_name.clone(), &["name", "age"], &[&"Before", &age])
            .await
            .unwrap();

        inserted.notified().await;
    }

    // Plain truncate also resends metadata without an ETL DDL snapshot.
    let truncated = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Truncate, table_id, 1)])
        .await;

    database.truncate_table(table_name.clone()).await.unwrap();

    truncated.notified().await;

    let schemas_stored_notify = store.notify_on_table_schema_count(table_id, 3).await;
    let insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 5)])
        .await;

    // Both commands are no-op DDL: they store schema snapshots but do not
    // invalidate pgoutput's relation cache, so the following insert has no
    // protocol relation.
    database
        .run_sql(&format!(
            "alter table {} owner to current_user",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "alter table {} alter column age drop not null",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database.insert_values(table_name.clone(), &["name", "age"], &[&"Alice", &25]).await.unwrap();

    schemas_stored_notify.notified().await;
    insert_notify.notified().await;

    // Truncate after no-op DDL still needs a destination schema barrier.
    // pgoutput emits a protocol relation before truncate, unlike insert.
    let truncate_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Truncate, table_id, 2)])
        .await;

    database
        .run_sql(&format!(
            "alter table {} owner to current_user",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database.truncate_table(table_name).await.unwrap();

    truncate_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let relations = grouped.get(&(EventType::Relation, table_id)).unwrap();
    assert!(relations.len() > 3);
    assert_eq!(grouped.get(&(EventType::Truncate, table_id)).unwrap().len(), 2);

    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    let Event::Truncate(truncate) = get_last_truncate_event(&events, table_id) else {
        panic!("expected truncate event");
    };
    let Event::Relation(warm_relation) = &relations[0] else {
        panic!("expected warm relation event");
    };
    let Event::Relation(insert_relation) = get_relation_before_last_insert(&events, table_id)
    else {
        panic!("expected relation event before last insert");
    };
    let Event::Relation(truncate_relation) = get_relation_before_last_truncate(&events, table_id)
    else {
        panic!("expected relation event before last truncate");
    };
    assert_eq!(insert.table_row.values()[1], Cell::String("Alice".to_owned()));
    assert_eq!(truncate.truncated_tables.len(), 1);
    assert_eq!(truncate.truncated_tables[0].id(), table_id);

    // Every event through the first truncate retains the original decoder.
    let initial = &warm_relation.replicated_table_schema;
    let first_truncate =
        events.iter().position(|event| matches!(event, Event::Truncate(_))).unwrap();
    for event in &events[..=first_truncate] {
        let schema = match event {
            Event::Relation(relation) => &relation.replicated_table_schema,
            Event::Insert(insert) => {
                assert_eq!(insert.tx_ordinal, 1);
                &insert.replicated_table_schema
            }
            Event::Truncate(truncate) => {
                assert_eq!(truncate.tx_ordinal, 1);
                assert_eq!(truncate.truncated_tables.len(), 1);
                &truncate.truncated_tables[0]
            }
            _ => continue,
        };
        assert_eq!(schema.inner(), initial.inner());
        assert_eq!(schema.replication_mask(), initial.replication_mask());
        assert_eq!(schema.identity_mask(), initial.identity_mask());
    }
    let ages = grouped[&(EventType::Insert, table_id)]
        .iter()
        .map(|event| match event {
            Event::Insert(insert) => insert.table_row.values()[2].clone(),
            _ => unreachable!("the group contains only inserts"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ages, vec![Cell::I32(1), Cell::I32(2), Cell::I32(3), Cell::I32(4), Cell::I32(25)]);

    // Insert after no-op DDL has no protocol relation, so apply emits the
    // relation event from the pending schema snapshot. Truncate after no-op DDL
    // receives a protocol relation first; apply does not synthesize one.
    let insert_schema = &insert.replicated_table_schema;
    let truncate_schema = &truncate.truncated_tables[0];
    assert!(insert_schema.inner().snapshot_id > initial.inner().snapshot_id);
    assert!(truncate_schema.inner().snapshot_id > insert_schema.inner().snapshot_id);
    assert_eq!(insert.tx_ordinal, 1);
    assert_eq!(truncate.tx_ordinal, 1);

    for (schema, relation_schema) in [
        (insert_schema, &insert_relation.replicated_table_schema),
        (truncate_schema, &truncate_relation.replicated_table_schema),
    ] {
        assert_eq!(schema.inner(), relation_schema.inner());
        assert_eq!(schema.replication_mask(), relation_schema.replication_mask());
        assert_eq!(schema.identity_mask(), relation_schema.identity_mask());
        assert_eq!(schema.replication_mask(), initial.replication_mask());
        assert_eq!(schema.identity_mask(), initial.identity_mask());
    }

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    let (_, newest_table_schema) =
        snapshots.iter().max_by_key(|(snapshot_id, _)| *snapshot_id).unwrap();
    assert_eq!(truncate_schema.inner(), newest_table_schema);
}

#[tokio::test(flavor = "multi_thread")]
async fn alter_table_without_dml_stores_schema_snapshot() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_sync_done_pipeline_with_table(
            "schema_add_column_no_dml",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let schema_stored_notify = store.notify_on_table_schema_count(table_id, 2).await;

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

    schema_stored_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    // Applying the synthetic relation can prune the initial snapshot before
    // shutdown. The newest durable snapshot must retain the DDL either way.
    let (snapshot_id, newest_schema) = snapshots.last().unwrap();
    assert!(*snapshot_id > SnapshotId::initial());
    assert_table_schema_column_names_types(
        newest_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );

    // We take the relation events count after we applied the schema change so
    // that we can use that for the next assertion.
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

    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["name", "age", "email"],
            &[&"Alice", &25, &"alice@example.com"],
        )
        .await
        .unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    assert!(
        grouped.get(&(EventType::Relation, table_id)).unwrap().len()
            > relation_count_before_restart
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
    assert_eq!(
        insert.replicated_table_schema.inner().snapshot_id,
        relation.replicated_table_schema.inner().snapshot_id
    );

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_only_schema_snapshot(
        snapshots,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );
    assert_eq!(snapshots[0].0, relation.replicated_table_schema.inner().snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn default_expressions_round_trip_through_schema_changes_and_defaulted_insert() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_sync_done_pipeline_with_table(
            "schema_default_shapes",
            &[("name", "text not null")],
        )
        .await;

    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
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

    ready_notify.notified().await;
    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 2);
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
    assert!(
        matches!(&values[6], Cell::Date(Date::Value(date)) if date.to_string() == "2026-01-01")
    );
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
    assert_eq!(
        insert.replicated_table_schema.inner().snapshot_id,
        relation.replicated_table_schema.inner().snapshot_id
    );

    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    let schema = assert_only_schema_snapshot(
        snapshots,
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
    assert_column_default_contains(&schema.column_schemas, "status_text", &["pending"]);
    assert_column_default_contains(&schema.column_schemas, "score", &["10", "5"]);
    assert_column_default_contains(&schema.column_schemas, "active", &["true"]);
    assert_column_default_contains(&schema.column_schemas, "created_at", &["now"]);
    assert_column_default_contains(&schema.column_schemas, "created_on", &["2026-01-01"]);
    assert_column_default_contains(
        &schema.column_schemas,
        "payload",
        &["jsonb_build_object", "source", "api"],
    );
    assert_column_default_contains(&schema.column_schemas, "lower_name", &["lower", "user"]);
    assert_column_default_contains(&schema.column_schemas, "label", &["coalesce", "fallback"]);
    assert_column_default_contains(&schema.column_schemas, "tags", &["array", "alpha", "beta"]);
    assert_column_default_contains(
        &schema.column_schemas,
        "fixed_uuid",
        &["00000000-0000-0000-0000-000000000001"],
    );
    assert_column_default_contains(&schema.column_schemas, "expires_at", &["now", "30 days"]);
    assert_eq!(snapshots[0].0, relation.replicated_table_schema.inner().snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_recovers_after_multiple_schema_changes_and_restart() {
    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_sync_done_pipeline_with_table(
            "schema_multi_change_restart",
            &[("name", "text not null"), ("age", "integer not null"), ("status", "text not null")],
        )
        .await;

    // Add column + insert, then restart.
    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
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
            &["name", "age", "status", "email"],
            &[&"Alice", &25, &"active", &"alice@example.com"],
        )
        .await
        .unwrap();

    ready_notify.notified().await;
    events_notify.notified().await;
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

    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;
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

    events_notify.notified().await;
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

    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 3)])
        .await;
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

    events_notify.notified().await;
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

    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 4)])
        .await;
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

    events_notify.notified().await;
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
    assert_eq!(
        i.replicated_table_schema.inner().snapshot_id,
        r.replicated_table_schema.inner().snapshot_id
    );

    // Verify cleanup across restarts retained only the current schema.
    let table_schemas = store.get_table_schemas().await;
    let snapshots = table_schemas.get(&table_id).unwrap();
    assert_only_schema_snapshot(
        snapshots,
        &[
            ("id", Type::INT8),
            ("name", Type::TEXT),
            ("years", Type::INT8),
            ("contact_email", Type::TEXT),
            ("created_at", Type::TIMESTAMP),
        ],
    );
    assert_eq!(snapshots[0].0, r.replicated_table_schema.inner().snapshot_id);
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

    let parent_sync_complete_notify =
        state_store.notify_on_table_sync_complete(parent_table_id).await;
    let parent_ready_notify =
        state_store.notify_on_table_state_type(parent_table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    parent_sync_complete_notify.notified().await;

    // Wait for the Relation event (schema change) and Insert event.
    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, parent_table_id, 1)])
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

    parent_ready_notify.notified().await;
    events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    // Both the synthetic and real relation describe the updated parent.
    assert_eq!(grouped.get(&(EventType::Relation, parent_table_id)).unwrap().len(), 2);
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
    assert_eq!(
        i.replicated_table_schema.inner().snapshot_id,
        r.replicated_table_schema.inner().snapshot_id
    );

    // Verify cleanup retained the current schema.
    let table_schemas = state_store.get_table_schemas().await;
    let snapshots = table_schemas.get(&parent_table_id).unwrap();
    assert_only_schema_snapshot(
        snapshots,
        &[
            ("id", Type::INT8),
            ("data", Type::TEXT),
            ("partition_key", Type::INT4),
            ("category", Type::TEXT),
        ],
    );
    assert_eq!(snapshots[0].0, r.replicated_table_schema.inner().snapshot_id);
}

/// Verifies bounded DDL metric labels without changing schema application or
/// publication filtering, using real trigger output and crafted logical
/// messages.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_metric_labels_preserve_supported_tags_and_bound_unknown_tags() {
    /// Counter for DDL handling outcomes.
    const DDL_COUNTER: &str = "etl_ddl_schema_changes_total";
    /// Exported observation count for the DDL column histogram.
    const DDL_COLUMN_COUNT: &str = "etl_ddl_schema_change_columns_count";

    init_test_tracing();
    let metrics_handle = init_metrics_handle().unwrap();
    let (mut database, table_name, table_id, store, destination, pipeline, _, publication_name) =
        create_database_and_sync_done_pipeline_with_table(
            "ddl_metric_labels",
            &[("value", "integer not null")],
        )
        .await;
    let ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    // A new supported trigger tag must also be added to the metric mapping.
    let mut trigger_tags: Vec<String> = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select evttags from pg_catalog.pg_event_trigger
             where evtname = 'supabase_etl_ddl_message_trigger'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    trigger_tags.sort();
    assert_eq!(trigger_tags, ["ALTER PUBLICATION", "ALTER TABLE"]);

    // Exercise both tags through the installed trigger with unchanged columns.
    database
        .run_sql(&format!(
            "alter table {} alter column value set default 7",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "alter publication {} set (publish = 'insert')",
            quote_identifier(&publication_name)
        ))
        .await
        .unwrap();

    let client = database.client.as_mut().unwrap();
    let mut payload: serde_json::Value = client
        .query_one(
            "select pg_catalog.jsonb_build_object(
                'nspname', n.nspname,
                'relname', c.relname,
                'oid', c.oid::bigint,
                'identity', etl.describe_table_identity(c.oid),
                'columns', (
                    select pg_catalog.jsonb_agg(
                        pg_catalog.jsonb_build_object(
                            'attname', s.attname,
                            'attnum', s.attnum,
                            'atttypid', s.atttypid::bigint,
                            'atttypmod', s.atttypmod,
                            'attnotnull', s.attnotnull,
                            'default_expression', s.default_expression
                        ) order by s.attnum
                    )
                    from etl.describe_table_schema(c.oid) s
                )
             )
             from pg_catalog.pg_class c
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace
             where c.oid = $1",
            &[&table_id.into_inner()],
        )
        .await
        .unwrap()
        .get(0);

    let unexpected_tags = ["unexpected_a".to_owned(), "unexpected_b".to_owned(), "x".repeat(4096)];
    let transaction = client.transaction().await.unwrap();
    for command_tag in &unexpected_tags {
        payload["command_tag"] = command_tag.clone().into();
        // Unknown tags retain existing behavior for tracked and untracked
        // tables.
        for oid in [table_id.into_inner(), 0] {
            payload["oid"] = oid.into();
            transaction
                .query_one(
                    "select pg_catalog.pg_logical_emit_message(true, 'supabase_etl_ddl', $1::text)",
                    &[&payload.to_string()],
                )
                .await
                .unwrap();
        }
    }

    payload["oid"] = table_id.into_inner().into();
    payload["command_tag"] = "ALTER PUBLICATION".into();
    payload["publication_name"] = "other_publication".into();
    // An invalid schema would break the following row if publication filtering
    // were accidentally bypassed while normalizing labels.
    payload["columns"] = serde_json::json!([]);
    transaction
        .query_one(
            "select pg_catalog.pg_logical_emit_message(true, 'supabase_etl_ddl', $1::text)",
            &[&payload.to_string()],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    // A subsequent row proves that all preceding messages have been handled.
    let inserted = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;
    database.insert_values(table_name, &["value"], &[&7_i32]).await.unwrap();
    inserted.notified().await;
    ready.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let Event::Insert(insert) = get_last_insert_event(&events, table_id) else {
        panic!("expected insert event");
    };
    assert_eq!(insert.table_row.values(), &[Cell::I64(1), Cell::I32(7)]);

    let metrics = metrics_handle.render();
    let samples: Vec<_> = metrics
        .lines()
        .filter(|line| {
            (line.starts_with(&format!("{DDL_COUNTER}{{"))
                || line.starts_with(&format!("{DDL_COLUMN_COUNT}{{")))
                && line.contains("worker_type=\"apply\"")
        })
        .collect();
    let unknown_count = u64::try_from(unexpected_tags.len()).unwrap();
    let expected = [
        (DDL_COUNTER, "ALTER TABLE", Some("applied"), 1),
        (DDL_COUNTER, "ALTER PUBLICATION", Some("applied"), 1),
        (DDL_COUNTER, "ALTER PUBLICATION", Some("skipped_publication"), 1),
        (DDL_COUNTER, "unknown", Some("applied"), unknown_count),
        (DDL_COUNTER, "unknown", Some("skipped"), unknown_count),
        (DDL_COLUMN_COUNT, "ALTER TABLE", None, 1),
        (DDL_COLUMN_COUNT, "ALTER PUBLICATION", None, 1),
        (DDL_COLUMN_COUNT, "unknown", None, unknown_count),
    ];
    assert_eq!(samples.len(), expected.len());
    for (metric, command_tag, outcome, count) in expected {
        let matching: Vec<_> = samples
            .iter()
            .filter(|line| {
                line.starts_with(&format!("{metric}{{"))
                    && line.contains(&format!("command_tag=\"{command_tag}\""))
                    && outcome
                        .is_none_or(|outcome| line.contains(&format!("outcome=\"{outcome}\"")))
            })
            .collect();
        assert_eq!(matching.len(), 1);
        let value: u64 = matching[0].rsplit_once(' ').unwrap().1.parse().unwrap();
        assert_eq!(value, count);
    }
}
