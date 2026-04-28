#![cfg(all(feature = "test-utils", feature = "failpoints"))]

use etl::{
    failpoints::{
        FORCE_SCHEMA_CLEANUP_CONFIRMED_FLUSH_LSN_FP, FORCE_SCHEMA_CLEANUP_FP,
        SEND_STATUS_UPDATE_FP, START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP,
        START_TABLE_SYNC_DURING_DATA_SYNC_FP,
    },
    state::table::{RetryPolicy, TableReplicationPhase, TableReplicationPhaseType},
    store::{schema::SchemaStore, state::StateStore},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::group_events_by_type_and_table_id,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{create_database_and_ready_pipeline_with_table, create_pipeline},
        schema::{
            assert_replicated_schema_column_names_types, assert_schema_snapshots_ordering,
            assert_table_schema_column_names_types,
        },
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, insert_users_data, setup_test_database_schema,
        },
    },
    types::{Event, EventType, InsertEvent, PipelineId, TableId, Type},
};
use etl_postgres::{
    below_version,
    tokio::test_utils::TableModification,
    types::{SnapshotId, TableSchema},
    version::POSTGRES_15,
};
use etl_telemetry::tracing::init_test_tracing;
use fail::FailScenario;
use rand::random;

enum ExpectedReplicatedEvent<'a> {
    Relation(&'a [(&'static str, Type)]),
    Insert(&'a [(&'static str, Type)]),
}

fn collect_table_events(events: &[Event], table_id: TableId) -> Vec<Event> {
    events.iter().filter(|event| event.has_table_id(&table_id)).cloned().collect()
}

fn assert_table_event_sequence(
    events: &[Event],
    table_id: TableId,
    expected: &[ExpectedReplicatedEvent<'_>],
) {
    let table_events = collect_table_events(events, table_id);
    let expected_types = expected
        .iter()
        .map(|event| match event {
            ExpectedReplicatedEvent::Relation(_) => EventType::Relation,
            ExpectedReplicatedEvent::Insert(_) => EventType::Insert,
        })
        .collect::<Vec<_>>();

    assert_eq!(table_events.iter().map(Event::event_type).collect::<Vec<_>>(), expected_types);
    assert_eq!(table_events.len(), expected.len());

    for (actual_event, expected_event) in table_events.iter().zip(expected) {
        match (actual_event, expected_event) {
            (Event::Relation(relation), ExpectedReplicatedEvent::Relation(expected_columns)) => {
                assert_replicated_schema_column_names_types(
                    &relation.replicated_table_schema,
                    expected_columns,
                );
            }
            (Event::Insert(insert), ExpectedReplicatedEvent::Insert(expected_columns)) => {
                assert_replicated_schema_column_names_types(
                    &insert.replicated_table_schema,
                    expected_columns,
                );
            }
            (unexpected_event, ExpectedReplicatedEvent::Relation(_)) => {
                panic!("expected relation event, got {unexpected_event:?}");
            }
            (unexpected_event, ExpectedReplicatedEvent::Insert(_)) => {
                panic!("expected insert event, got {unexpected_event:?}");
            }
        }
    }
}

fn assert_table_schema_snapshots(
    snapshots: &[(SnapshotId, TableSchema)],
    expected_schemas: &[&[(&'static str, Type)]],
) {
    assert_eq!(snapshots.len(), expected_schemas.len());
    assert_schema_snapshots_ordering(snapshots, true);

    for ((_, schema), expected_columns) in snapshots.iter().zip(expected_schemas) {
        assert_table_schema_column_names_types(schema, expected_columns);
    }
}

fn assert_restarted_schema_snapshot_pairs(
    restarted_snapshots: &[(SnapshotId, TableSchema)],
    initial_snapshots: &[(SnapshotId, TableSchema)],
) {
    assert!(initial_snapshots.len() >= 2, "expected at least one non-initial schema snapshot");
    assert_eq!(restarted_snapshots, initial_snapshots);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_fails_after_data_sync_threw_an_error_with_no_retry() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, "1*return(no_retry)").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=rows_inserted).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table sync phases.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Errored,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store
        .get_table_replication_state(database_schema.users_schema().id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        table_state,
        TableReplicationPhase::Errored { retry_policy: RetryPolicy::NoRetry, .. }
    ));

    // Verify no data is there.
    let table_rows = destination.get_table_rows().await;
    assert!(table_rows.is_empty());

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert!(table_schemas.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_fails_after_timed_retry_exceeded_max_attempts() {
    let _scenario = FailScenario::setup();
    // Since we have table_error_retry_max_attempts: 2, we want to fail 3 times, so
    // that on the 3rd time, the system switches to manual retry.
    fail::cfg(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, "3*return(timed_retry)").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=rows_inserted).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for waiting on the manual retry which is expected to
    // be flipped by the max attempts handling.
    let users_state_notify = store
        .notify_on_table_state(database_schema.users_schema().id, |phase| {
            matches!(
                phase,
                TableReplicationPhase::Errored { retry_policy: RetryPolicy::ManualRetry, .. }
            )
        })
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store
        .get_table_replication_state(database_schema.users_schema().id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        table_state,
        TableReplicationPhase::Errored { retry_policy: RetryPolicy::ManualRetry, .. }
    ));

    // Verify no data is there.
    let table_rows = destination.get_table_rows().await;
    assert!(table_rows.is_empty());

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert!(table_schemas.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_is_consistent_after_data_sync_threw_an_error_with_timed_retry() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, "1*return(timed_retry)").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=rows_inserted).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // We expect no errors, since the same table sync worker task is retried.
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(
        *table_schemas.get(&database_schema.users_schema().id).unwrap(),
        database_schema.users_schema()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_is_consistent_during_data_sync_threw_an_error_with_timed_retry() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_DURING_DATA_SYNC_FP, "1*return(timed_retry)").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=rows_inserted).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // We start the pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // We register the interest in waiting for both table syncs to have started.
    let users_state_notify = store
        .notify_on_table_state_type(
            database_schema.users_schema().id,
            TableReplicationPhaseType::Ready,
        )
        .await;

    pipeline.start().await.unwrap();

    users_state_notify.notified().await;

    // We expect no errors, since the same table sync worker task is retried.
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(
        *table_schemas.get(&database_schema.users_schema().id).unwrap(),
        database_schema.users_schema()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_snapshots_are_consistent_after_missing_status_update_with_interleaved_ddl_in_same_transaction()
 {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

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
    ) = create_database_and_ready_pipeline_with_table(
        "schema_add_column",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 3), (EventType::Insert, 3)])
        .await;

    let transaction = database.begin_transaction().await;

    transaction
        .insert_values(table_name.clone(), &["name", "age"], &[&"first", &25])
        .await
        .unwrap();

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
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"second", &28, &"active"])
        .await
        .unwrap();

    transaction
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    transaction
        .insert_values(table_name.clone(), &["name", "status"], &[&"third", &"pending"])
        .await
        .unwrap();

    transaction.commit_transaction().await;

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    assert_table_event_sequence(
        &events,
        table_id,
        &[
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
            ]),
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
        ],
    );

    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert_table_schema_snapshots(
        table_schemas_snapshots,
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ],
            &[("id", Type::INT8), ("name", Type::TEXT), ("status", Type::TEXT)],
        ],
    );
    let initial_events = collect_table_events(&events, table_id);
    let initial_table_schema_snapshots = table_schemas_snapshots.clone();

    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 3), (EventType::Insert, 3)])
        .await;

    pipeline.start().await.unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let restarted_events = destination.get_events().await;
    assert_events_equal(&collect_table_events(&restarted_events, table_id), &initial_events);

    let restarted_table_schemas = store.get_table_schemas().await;
    assert_restarted_schema_snapshot_pairs(
        restarted_table_schemas.get(&table_id).unwrap(),
        &initial_table_schema_snapshots,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_snapshots_are_consistent_after_missing_status_update_with_interleaved_ddl_across_transactions()
 {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_add_column",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 3), (EventType::Insert, 3)])
        .await;

    database.insert_values(table_name.clone(), &["name", "age"], &[&"first", &25]).await.unwrap();

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "status",
                data_type: "text not null default 'pending'",
            }],
        )
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"second", &28, &"active"])
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "status"], &[&"third", &"pending"])
        .await
        .unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    assert_table_event_sequence(
        &events,
        table_id,
        &[
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
            ]),
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
        ],
    );

    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert_table_schema_snapshots(
        table_schemas_snapshots,
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ],
            &[("id", Type::INT8), ("name", Type::TEXT), ("status", Type::TEXT)],
        ],
    );
    let initial_events = collect_table_events(&events, table_id);
    let initial_table_schema_snapshots = table_schemas_snapshots.clone();

    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 3), (EventType::Insert, 3)])
        .await;

    pipeline.start().await.unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let restarted_events = destination.get_events().await;
    assert_events_equal(&collect_table_events(&restarted_events, table_id), &initial_events);

    let restarted_table_schemas = store.get_table_schemas().await;
    assert_restarted_schema_snapshot_pairs(
        restarted_table_schemas.get(&table_id).unwrap(),
        &initial_table_schema_snapshots,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_snapshots_are_consistent_after_missing_status_update_with_interleaved_starting_ddl_in_same_transaction()
 {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

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
    ) = create_database_and_ready_pipeline_with_table(
        "schema_add_column",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

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
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"second", &28, &"active"])
        .await
        .unwrap();

    transaction
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    transaction
        .insert_values(table_name.clone(), &["name", "status"], &[&"third", &"pending"])
        .await
        .unwrap();

    transaction.commit_transaction().await;

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    assert_table_event_sequence(
        &events,
        table_id,
        &[
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
        ],
    );

    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert_table_schema_snapshots(
        table_schemas_snapshots,
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ],
            &[("id", Type::INT8), ("name", Type::TEXT), ("status", Type::TEXT)],
        ],
    );
    let initial_events = collect_table_events(&events, table_id);
    let initial_table_schema_snapshots = table_schemas_snapshots.clone();

    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

    pipeline.start().await.unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let restarted_events = destination.get_events().await;
    assert_events_equal(&collect_table_events(&restarted_events, table_id), &initial_events);

    let restarted_table_schemas = store.get_table_schemas().await;
    assert_restarted_schema_snapshot_pairs(
        restarted_table_schemas.get(&table_id).unwrap(),
        &initial_table_schema_snapshots,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_snapshots_are_consistent_after_missing_status_update_with_interleaved_starting_ddl_across_transactions()
 {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_add_column",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

    database
        .alter_table(
            table_name.clone(),
            &[TableModification::AddColumn {
                name: "status",
                data_type: "text not null default 'pending'",
            }],
        )
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"second", &28, &"active"])
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "status"], &[&"third", &"pending"])
        .await
        .unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    assert_table_event_sequence(
        &events,
        table_id,
        &[
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Relation(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
            ExpectedReplicatedEvent::Insert(&[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("status", Type::TEXT),
            ]),
        ],
    );

    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert_table_schema_snapshots(
        table_schemas_snapshots,
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("status", Type::TEXT),
            ],
            &[("id", Type::INT8), ("name", Type::TEXT), ("status", Type::TEXT)],
        ],
    );
    let initial_events = collect_table_events(&events, table_id);
    let initial_table_schema_snapshots = table_schemas_snapshots.clone();

    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

    pipeline.start().await.unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let restarted_events = destination.get_events().await;
    assert_events_equal(&collect_table_events(&restarted_events, table_id), &initial_events);

    let restarted_table_schemas = store.get_table_schemas().await;
    assert_restarted_schema_snapshot_pairs(
        restarted_table_schemas.get(&table_id).unwrap(),
        &initial_table_schema_snapshots,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_snapshots_are_consistent_after_missing_status_update_with_initial_ddl() {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_ready_pipeline_with_table(
            "schema_add_column",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    // The reason for why we wait for two `Relation` messages is that since we have
    // a DDL event before DML statements, Postgres likely avoids sending an
    // initial `Relation` message since it's already sent given the DDL event.
    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

    // We immediately add a column to the table without any DML, to show the case
    // where we can recover in case we immediately start with a DDL event.
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
        .insert_values(
            table_name.clone(),
            &["name", "age", "email"],
            &[&"Bob", &28, &"bob@example.com"],
        )
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "email"], &[&"Matt", &"matt@example.com"])
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // Assert that we got all the events correctly.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 2);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 2);

    // Assert that we have 3 schema snapshots stored in order (1 base snapshot + 2
    // relation changes).
    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(table_schemas_snapshots.len(), 3);
    assert_schema_snapshots_ordering(table_schemas_snapshots, true);

    // Verify the first snapshot has the initial schema (id, name, age).
    let (_, first_schema) = &table_schemas_snapshots[0];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
    );

    // Verify the first snapshot has the new schema (id, name, age, email).
    let (_, first_schema) = &table_schemas_snapshots[1];
    assert_table_schema_column_names_types(
        first_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
    );

    // Verify the second snapshot doesn't have the age column (id, name, email).
    let (_, second_schema) = &table_schemas_snapshots[2];
    assert_table_schema_column_names_types(
        second_schema,
        &[("id", Type::INT8), ("name", Type::TEXT), ("email", Type::TEXT)],
    );

    // Clear up the events.
    destination.clear_events().await;

    // Restart the pipeline with the failpoint disabled to verify recovery.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    let notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 3)])
        .await;

    database
        .insert_values(
            table_name.clone(),
            &["name", "email"],
            &[&"Charlie", &"charlie@example.com"],
        )
        .await
        .unwrap();

    notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // Assert that we got all the events correctly.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);

    assert_eq!(grouped.get(&(EventType::Relation, table_id)).unwrap().len(), 2);
    assert_eq!(grouped.get(&(EventType::Insert, table_id)).unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn cleaned_schema_snapshots_are_recreated_when_status_update_is_lost() {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();

    let database = spawn_source_database().await;
    let table_name = test_table_name("schema_cleanup_replay");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await
        .unwrap();

    let publication_name = format!("pub_{}", random::<u32>());
    database
        .run_sql(&format!(
            "create publication {publication_name} for table {}",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    let ready_notify =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    ready_notify.notified().await;

    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

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
        .insert_values(
            table_name.clone(),
            &["name", "age", "email"],
            &[&"Bob", &28, &"bob@example.com"],
        )
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "email"], &[&"Matt", &"matt@example.com"])
        .await
        .unwrap();

    events_notify.notified().await;

    let initial_events = collect_table_events(&destination.get_events().await, table_id);
    let relation_snapshots = initial_events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(relation.replicated_table_schema.inner().snapshot_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    let first_snapshot = relation_snapshots[0];
    let latest_snapshot = relation_snapshots[1];
    assert_eq!(relation_snapshots.len(), 2);

    assert!(store.get_table_schema(&table_id, first_snapshot).await.unwrap().is_some());
    assert!(store.get_table_schema(&table_id, latest_snapshot).await.unwrap().is_some());

    let prune_notify = store.notify_on_table_schema_prune().await;

    // Force cleanup during the shutdown status update. The status update itself
    // is still lost, while the confirmed-LSN failpoint lets this test exercise
    // replay after cleanup without relying on PostgreSQL acknowledging it.
    fail::cfg(FORCE_SCHEMA_CLEANUP_CONFIRMED_FLUSH_LSN_FP, "return").unwrap();
    fail::cfg(FORCE_SCHEMA_CLEANUP_FP, "return").unwrap();

    prune_notify.notified().await;

    fail::remove(FORCE_SCHEMA_CLEANUP_FP);
    fail::remove(FORCE_SCHEMA_CLEANUP_CONFIRMED_FLUSH_LSN_FP);

    pipeline.shutdown_and_wait().await.unwrap();

    assert!(store.get_table_schema(&table_id, first_snapshot).await.unwrap().is_none());
    assert!(store.get_table_schema(&table_id, latest_snapshot).await.unwrap().is_some());

    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    );

    let replayed_events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 2), (EventType::Insert, 2)])
        .await;

    pipeline.start().await.unwrap();

    replayed_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let replayed_events = collect_table_events(&destination.get_events().await, table_id);
    assert_events_equal(&initial_events, &replayed_events);

    assert!(store.get_table_schema(&table_id, first_snapshot).await.unwrap().is_some());
    assert!(store.get_table_schema(&table_id, latest_snapshot).await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_replication_masks_are_consistent_after_restart() {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // Create a table with 3 columns (plus auto-generated id).
    let table_name = test_table_name("col_removal");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null"), ("email", "text not null")],
        )
        .await
        .unwrap();

    // Create publication with all 3 columns (plus id) initially.
    let publication_name = format!("pub_{}", random::<u32>());
    database
        .run_sql(&format!(
            "create publication {publication_name} for table {} (id, name, age, email)",
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("Failed to create publication with column filter");

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    // Wait for the table to finish syncing.
    let sync_done_notify =
        store.notify_on_table_state_type(table_id, TableReplicationPhaseType::Ready).await;

    pipeline.start().await.unwrap();

    sync_done_notify.notified().await;

    // We expect 3 relation events (one per publication change) and 3 insert events.
    let events_notify = destination
        .wait_for_events_count(vec![(EventType::Relation, 3), (EventType::Insert, 3)])
        .await;

    // Phase 1: Insert with all 4 columns (id, name, age, email).
    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Alice', 25, 'alice@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Phase 2: Remove email column -> (id, name, age), then insert.
    database
        .run_sql(&format!(
            "alter publication {publication_name} set table {} (id, name, age)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Bob', 30, 'bob@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Phase 3: Remove age column -> (id, name), then insert.
    database
        .run_sql(&format!(
            "alter publication {publication_name} set table {} (id, name)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Charlie', 35, 'charlie@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    events_notify.notified().await;

    // Helper to verify events after each run.
    let verify_events = |events: &[Event], table_id: TableId| {
        let grouped = group_events_by_type_and_table_id(events);

        // Verify we have 3 relation events.
        let relation_events: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::Relation(relation) if relation.replicated_table_schema.id() == table_id => {
                    Some(relation.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            relation_events.len(),
            3,
            "Expected 3 relation events, got {}",
            relation_events.len()
        );

        // Verify relation events have decreasing column counts: 4 -> 3 -> 2.
        let relation_column_counts: Vec<usize> = relation_events
            .iter()
            .map(|r| r.replicated_table_schema.column_schemas().count())
            .collect();
        assert_eq!(
            relation_column_counts,
            vec![4, 3, 2],
            "Expected relation column counts [4, 3, 2], got {relation_column_counts:?}"
        );

        // Verify relation column names for each phase.
        let relation_1_cols: Vec<&str> = relation_events[0]
            .replicated_table_schema
            .column_schemas()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(relation_1_cols, vec!["id", "name", "age", "email"]);

        let relation_2_cols: Vec<&str> = relation_events[1]
            .replicated_table_schema
            .column_schemas()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(relation_2_cols, vec!["id", "name", "age"]);

        let relation_3_cols: Vec<&str> = relation_events[2]
            .replicated_table_schema
            .column_schemas()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(relation_3_cols, vec!["id", "name"]);

        // Verify replication masks.
        assert_eq!(
            relation_events[0].replicated_table_schema.replication_mask().as_slice(),
            &[1, 1, 1, 1]
        );
        assert_eq!(
            relation_events[1].replicated_table_schema.replication_mask().as_slice(),
            &[1, 1, 1, 0]
        );
        assert_eq!(
            relation_events[2].replicated_table_schema.replication_mask().as_slice(),
            &[1, 1, 0, 0]
        );

        // Verify underlying schema always has 4 columns.
        for relation in &relation_events {
            assert_eq!(relation.replicated_table_schema.inner().column_schemas.len(), 4);
        }

        // Verify we have 3 insert events.
        let insert_events = grouped.get(&(EventType::Insert, table_id)).unwrap();
        assert_eq!(insert_events.len(), 3, "Expected 3 insert events, got {}", insert_events.len());

        // Verify insert events have decreasing value counts: 4 -> 3 -> 2.
        let insert_value_counts: Vec<usize> = insert_events
            .iter()
            .filter_map(|event| {
                if let Event::Insert(InsertEvent { table_row, .. }) = event {
                    Some(table_row.values().len())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            insert_value_counts,
            vec![4, 3, 2],
            "Expected insert value counts [4, 3, 2], got {insert_value_counts:?}"
        );
    };

    // Shutdown the pipeline.
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify events from first run.
    let events = destination.get_events().await;
    verify_events(&events, table_id);

    // Verify schema snapshots are stored correctly.
    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert!(!table_schemas_snapshots.is_empty(), "Expected at least 1 schema snapshot");
    assert_schema_snapshots_ordering(table_schemas_snapshots, true);

    // The underlying table schema should always have 4 columns.
    for (_, schema) in table_schemas_snapshots {
        assert_table_schema_column_names_types(
            schema,
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
        );
    }

    // Clear up the events.
    destination.clear_events().await;

    // Restart the pipeline - Postgres will resend the data since we don't track
    // progress exactly.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    // Wait for 3 relation events and 3 insert events again after restart.
    let events_notify_restart = destination
        .wait_for_events_count(vec![(EventType::Relation, 3), (EventType::Insert, 3)])
        .await;

    pipeline.start().await.unwrap();

    events_notify_restart.notified().await;

    // Verify the same events are received after restart.
    let events_after_restart = destination.get_events().await;
    verify_events(&events_after_restart, table_id);

    pipeline.shutdown_and_wait().await.unwrap();
}
