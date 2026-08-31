#![cfg(all(feature = "test-utils", feature = "failpoints"))]

use std::time::Duration;

use etl::{
    data::{Cell, TableRow},
    destination::{
        Destination, DestinationWriteStatus, DropTableForCopyResult, TableCopyBatchId,
        WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    event::{Event, EventType, InsertEvent},
    failpoints::{
        SEND_STATUS_UPDATE_FP, START_TABLE_SYNC_AFTER_FINISHED_COPY_FP,
        START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, START_TABLE_SYNC_DURING_DATA_SYNC_FP,
        STORE_REPLICATION_CHECKPOINT_FP, TABLE_SYNC_WORKER_BEFORE_STREAMING_FP,
    },
    pipeline::PipelineId,
    schema::{ReplicatedTableSchema, SnapshotId, TableId, TableSchema},
    store::{StateStore, TableRetryPolicy, TableState, TableStateType, WorkerType},
    test_utils::{
        database::{
            replication_slot_state, spawn_source_database, test_table_name,
            wait_for_replication_slot_flush_lsn,
        },
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{
            PipelineBuilder, create_database_and_sync_done_pipeline_with_table, create_pipeline,
        },
        schema::{
            assert_columns_names_types, assert_replicated_schema_column_names_types,
            assert_schema_snapshots_ordering, assert_table_schema_column_names_types,
        },
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, build_expected_users_inserts, insert_orders_data,
            insert_users_data, setup_test_database_schema,
        },
    },
};
use etl_postgres::{
    application_name::{apply_worker_application_name, table_sync_worker_application_name},
    below_version,
    slots::EtlReplicationSlot,
    tokio::test_utils::{PgDatabase, TableModification},
    version::POSTGRES_15,
};
use etl_telemetry::tracing::init_test_tracing;
use fail::FailScenario;
use pg_escape::quote_identifier;
use rand::random;
use tokio::sync::mpsc;
use tokio_postgres::{
    Client,
    types::{PgLsn, Type},
};

/// Relevant streaming write observed by [`DeferredEventsDestination`].
enum DeferredEventsWrite {
    /// Target-table event batch accepted at this commit end LSN.
    Accepted { commit_end_lsn: PgLsn },
    /// Empty required-durability write whose result remains held by the test.
    DurabilityBarrier { result: WriteEventsResult },
}

/// Destination test double that accepts one table event batch and holds its
/// barrier.
#[derive(Clone)]
struct DeferredEventsDestination {
    /// Table whose next insert batch should be accepted.
    table_id: TableId,
    /// Channel used to expose the accepted batch and empty barrier to the test.
    writes_tx: mpsc::UnboundedSender<DeferredEventsWrite>,
}

impl DeferredEventsDestination {
    /// Creates a destination and its ordered streaming-write observer.
    fn new(table_id: TableId) -> (Self, mpsc::UnboundedReceiver<DeferredEventsWrite>) {
        let (writes_tx, writes_rx) = mpsc::unbounded_channel();

        (Self { table_id, writes_tx }, writes_rx)
    }
}

impl Destination for DeferredEventsDestination {
    fn name() -> &'static str {
        "deferred_events"
    }

    async fn drop_table_for_copy(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        async_result.send(Ok(()));

        Ok(())
    }

    async fn write_table_rows(
        &self,
        _replicated_table_schema: &ReplicatedTableSchema,
        _batch_id: Option<TableCopyBatchId>,
        _table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        async_result.send(Ok(DestinationWriteStatus::Durable));

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        let contains_target_insert = events.iter().any(|event| {
            matches!(
                event,
                Event::Insert(insert) if insert.replicated_table_schema.id() == self.table_id
            )
        });

        if contains_target_insert {
            assert_eq!(durability, WriteEventsDurability::MayDefer);
            let commit_end_lsn = events
                .iter()
                .rev()
                .find_map(|event| match event {
                    Event::Commit(commit) => Some(commit.end_lsn),
                    _ => None,
                })
                .expect("accepted event batch should contain its commit");

            // Expose A before resolving its result so the test observes protocol order.
            assert!(
                self.writes_tx.send(DeferredEventsWrite::Accepted { commit_end_lsn }).is_ok(),
                "streaming write observer should remain available"
            );

            // Accepted transfers ownership without proving durability, so ETL must carry A.
            async_result.send(Ok(DestinationWriteStatus::Accepted));

            return Ok(());
        }

        if events.is_empty() {
            assert_eq!(durability, WriteEventsDurability::RequireDurable);
            // Hold the empty barrier so the test can prove completion waits for it.
            assert!(
                self.writes_tx
                    .send(DeferredEventsWrite::DurabilityBarrier { result: async_result })
                    .is_ok(),
                "streaming write observer should remain available"
            );

            return Ok(());
        }

        assert_eq!(durability, WriteEventsDurability::MayDefer);
        async_result.send(Ok(DestinationWriteStatus::Durable));

        Ok(())
    }
}

/// Waits until the apply worker confirms the provided WAL target and returns
/// the confirmed position.
async fn wait_for_apply_worker_to_reach(
    database: &PgDatabase<Client>,
    pipeline_id: PipelineId,
    target_lsn: PgLsn,
) -> PgLsn {
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let client = database.client.as_ref().unwrap();

    wait_for_replication_slot_flush_lsn(client, &apply_slot_name, target_lsn).await
}

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

/// Asserts one filtered relation followed by one insert for a table.
fn assert_filtered_table_events(
    events: &[Event],
    table_id: TableId,
    expected_columns: &[(&str, Type)],
    expected_mask: &[u8],
    expected_snapshot_id: SnapshotId,
    expected_values: &[Cell],
) {
    let table_events = collect_table_events(events, table_id);
    assert_eq!(
        table_events.iter().map(Event::event_type).collect::<Vec<_>>(),
        vec![EventType::Relation, EventType::Insert]
    );

    for event in table_events {
        let replicated_table_schema = match event {
            Event::Relation(relation) => relation.replicated_table_schema,
            Event::Insert(insert) => {
                assert_eq!(insert.table_row.values(), expected_values);
                insert.replicated_table_schema
            }
            unexpected => panic!("expected relation or insert, got {unexpected:?}"),
        };

        assert_columns_names_types(replicated_table_schema.column_schemas(), expected_columns);
        assert_eq!(replicated_table_schema.replication_mask().as_slice(), expected_mask);
        assert_eq!(replicated_table_schema.inner().snapshot_id, expected_snapshot_id);
    }
}

/// Asserts that each relation and its following insert use the same schema
/// snapshot and masks.
fn assert_relation_insert_schema_pairs(
    events: &[Event],
    table_id: TableId,
    expected_snapshot_ids: &[SnapshotId],
) {
    let table_events = collect_table_events(events, table_id);
    let relations = table_events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) => Some(&relation.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();
    let inserts = table_events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) => Some(&insert.replicated_table_schema),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(relations.len(), inserts.len());
    assert_eq!(relations.len(), expected_snapshot_ids.len());

    for ((relation_schema, insert_schema), expected_snapshot_id) in
        relations.into_iter().zip(inserts).zip(expected_snapshot_ids)
    {
        assert_eq!(relation_schema.inner().snapshot_id, *expected_snapshot_id);
        assert_eq!(relation_schema.inner().snapshot_id, insert_schema.inner().snapshot_id);
        assert_eq!(relation_schema.replication_mask(), insert_schema.replication_mask());
        assert_eq!(relation_schema.identity_mask(), insert_schema.identity_mask());
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
    assert!(initial_snapshots.len() >= 2);
    assert_eq!(restarted_snapshots, initial_snapshots);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_worker_panic_marks_table_errored() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_BEFORE_DATA_SYNC_SLOT_CREATION_FP, "1*panic").unwrap();

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

    // Register notifications for table sync states.
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Errored)
        .await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state =
        store.get_table_state(database_schema.users_schema().id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::ManualRetry, .. }
    ));

    // Verify no data is there.
    let table_rows = destination.get_table_rows().await;
    assert!(table_rows.is_empty());

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert!(table_schemas.is_empty());
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

    // Register notifications for table sync states.
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Errored)
        .await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state =
        store.get_table_state(database_schema.users_schema().id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::NoRetry, .. }
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
    let users_ready_notify = store
        .notify_on_table_state(database_schema.users_schema().id, |state| {
            matches!(state, TableState::Errored { retry_policy: TableRetryPolicy::ManualRetry, .. })
        })
        .await;

    pipeline.start().await.unwrap();

    users_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state =
        store.get_table_state(database_schema.users_schema().id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::ManualRetry, .. }
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

    // Wait for the retried table sync to complete.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    // We expect no errors, since the same table sync worker task is retried.
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    let mut expected_users_schema = database_schema.users_schema();
    expected_users_schema.column_schemas[0].default_expression =
        Some("nextval('test.users_id_seq'::regclass)".to_owned());
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(
        *table_schemas.get(&database_schema.users_schema().id).unwrap(),
        expected_users_schema
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

    // Wait for the retried table sync to complete.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    // We expect no errors, since the same table sync worker task is retried.
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);

    // Verify table schemas were correctly stored.
    let table_schemas = store.get_latest_table_schemas().await;
    let mut expected_users_schema = database_schema.users_schema();
    expected_users_schema.column_schemas[0].default_expression =
        Some("nextval('test.users_id_seq'::regclass)".to_owned());
    assert_eq!(table_schemas.len(), 1);
    assert_eq!(
        *table_schemas.get(&database_schema.users_schema().id).unwrap(),
        expected_users_schema
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_handover_preserves_decoder_across_post_handoff_noop_ddl() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP, "pause").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;

    let copied_rows = 3;
    let catchup_rows = 2;
    insert_users_data(&mut database, &users_schema.name, 1..=copied_rows).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let finished_copy_notify =
        store.notify_on_table_state_type(table_id, TableStateType::FinishedCopy).await;

    pipeline.start().await.unwrap();

    finished_copy_notify.notified().await;

    let apply_commits_notify = destination
        .wait_for_events(vec![EventCondition::AnyCount(
            EventType::Commit,
            u64::try_from(catchup_rows).unwrap(),
        )])
        .await;

    // Rows inserted while the worker is paused after copy must be replayed by
    // table-sync streaming. Both replication connections receive these WAL
    // records: table sync processes its Relation and rows, while apply skips
    // the table-owned messages but still writes each transaction's commit.
    // Acknowledging the apply commits proves that its pgoutput connection has
    // already sent and cached the table's Relation before handoff.
    insert_users_data(
        &mut database,
        &users_schema.name,
        copied_rows + 1..=copied_rows + catchup_rows,
    )
    .await;

    apply_commits_notify.notified().await;

    let all_rows_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(
            EventType::Insert,
            table_id,
            (copied_rows + catchup_rows) as u64,
        )])
        .await;

    let sync_done_notify =
        store.notify_on_table_state_type(table_id, TableStateType::SyncDone).await;
    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    fail::remove(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP);

    all_rows_notify.notified().await;
    sync_done_notify.notified().await;

    // Make the first apply-owned table event a no-op DDL. ETL stores a new
    // schema snapshot for its transactional DDL message, but pgoutput keeps
    // the warmed relation cache and therefore emits no protocol relation.
    let schema_stored_notify = store.notify_on_table_schema_count(table_id, 2).await;

    database
        .run_sql(&format!(
            "alter table {} owner to current_user",
            users_schema.name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    schema_stored_notify.notified().await;

    // The apply connection already saw and skipped its copy of the relation
    // used by table sync. Because the no-op DDL does not invalidate that
    // connection's relation cache, pgoutput sends no protocol relation. Apply
    // still emits a destination schema barrier before decoding the row with the
    // SyncDone masks and the pending schema snapshot.
    let post_handover_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(
            EventType::Insert,
            table_id,
            (copied_rows + catchup_rows + 1) as u64,
        )])
        .await;

    insert_users_data(
        &mut database,
        &users_schema.name,
        copied_rows + catchup_rows + 1..=copied_rows + catchup_rows + 1,
    )
    .await;

    post_handover_notify.notified().await;
    ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_rows = destination.get_table_rows().await;
    let copied_table_rows = table_rows.get(&table_id).unwrap();
    assert_eq!(copied_table_rows.len(), copied_rows);

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);

    // Table-sync streaming emitted the first relation event. pgoutput did not
    // send another after the no-op DDL, so apply emits a destination schema
    // barrier before the first apply-owned row at the new snapshot.
    let relation_events = grouped_events.get(&(EventType::Relation, table_id)).unwrap();
    assert_eq!(relation_events.len(), 2);

    let catchup_events = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_catchup_events = build_expected_users_inserts(
        (copied_rows + 1) as i64,
        &users_schema,
        vec![("user_4", 4), ("user_5", 5), ("user_6", 6)],
    );
    assert_events_equal(catchup_events, &expected_catchup_events);

    let Event::Relation(table_sync_relation) = &relation_events[0] else {
        unreachable!("grouped relation event should be a relation");
    };
    let Event::Relation(pending_relation) = &relation_events[1] else {
        unreachable!("grouped relation event should be a relation");
    };
    let Event::Insert(post_handoff_insert) = catchup_events.last().unwrap() else {
        unreachable!("grouped insert event should be an insert");
    };

    // The row after the no-op DDL must use the new schema snapshot stored for
    // that DDL, and the destination-visible relation barrier must carry the
    // same snapshot before that row.
    let latest_table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(
        post_handoff_insert.replicated_table_schema.inner(),
        latest_table_schemas.get(&table_id).unwrap()
    );
    assert_eq!(
        post_handoff_insert.replicated_table_schema.inner().snapshot_id,
        pending_relation.replicated_table_schema.inner().snapshot_id
    );
    assert_ne!(
        post_handoff_insert.replicated_table_schema.inner().snapshot_id,
        table_sync_relation.replicated_table_schema.inner().snapshot_id
    );

    // Only the snapshot changed. Since pgoutput sent no replacement protocol
    // relation, both positional masks must come from the decoder persisted in
    // SyncDone, which was built from the table-sync relation above.
    assert_eq!(
        post_handoff_insert.replicated_table_schema.replication_mask(),
        pending_relation.replicated_table_schema.replication_mask()
    );
    assert_eq!(
        post_handoff_insert.replicated_table_schema.identity_mask(),
        pending_relation.replicated_table_schema.identity_mask()
    );
    assert_eq!(
        pending_relation.replicated_table_schema.replication_mask(),
        table_sync_relation.replicated_table_schema.replication_mask()
    );
    assert_eq!(
        pending_relation.replicated_table_schema.identity_mask(),
        table_sync_relation.replicated_table_schema.identity_mask()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tx_ordinals_follow_wal_order_across_table_sync_and_apply_workers() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let users_schema = database_schema.users_schema();
    let orders_schema = database_schema.orders_schema();
    let users_table_id = users_schema.id;
    let orders_table_id = orders_schema.id;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let assert_insert_ordinals = |events: &[Event], occurrence| {
        let insert_for = |table_id| {
            events
                .iter()
                .filter_map(|event| match event {
                    Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                        Some(insert)
                    }
                    _ => None,
                })
                .nth(occurrence)
                .unwrap()
        };
        let users_insert = insert_for(users_table_id);
        let orders_insert = insert_for(orders_table_id);

        assert_eq!(users_insert.commit_lsn, orders_insert.commit_lsn);
        assert_eq!(users_insert.tx_ordinal, 1);
        assert_eq!(orders_insert.tx_ordinal, 2);
    };

    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_max_table_sync_workers(2)
    .build();

    // Holding row writes lets both copy workers establish their snapshots
    // before the two-table transaction is committed.
    let first_copy = destination.hold_next(FaultyOp::WriteTableRows).await;
    let second_copy = destination.hold_next(FaultyOp::WriteTableRows).await;

    let users_finished_copy_notify =
        store.notify_on_table_state_type(users_table_id, TableStateType::FinishedCopy).await;
    let orders_finished_copy_notify =
        store.notify_on_table_state_type(orders_table_id, TableStateType::FinishedCopy).await;

    pipeline.start().await.unwrap();

    tokio::join!(first_copy.wait_reached(), second_copy.wait_reached());

    let mut write_two_table_transaction = async |suffix: &str| {
        let transaction = database.client.as_mut().unwrap().transaction().await.unwrap();
        transaction
            .execute(
                &format!(
                    "insert into {} (name, age) values ('user-{suffix}', 1)",
                    users_schema.name.as_quoted_identifier()
                ),
                &[],
            )
            .await
            .unwrap();
        transaction
            .execute(
                &format!(
                    "insert into {} (description) values ('order-{suffix}')",
                    orders_schema.name.as_quoted_identifier()
                ),
                &[],
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    };

    // Each table-sync worker skips the other table but must still reserve its
    // row ordinal before the ownership filter. Relation messages must not
    // consume ordinals.
    write_two_table_transaction("table-sync").await;

    let table_sync_events_notify = destination
        .wait_for_all_events(vec![
            EventCondition::TableCount(EventType::Insert, users_table_id, 1),
            EventCondition::TableCount(EventType::Insert, orders_table_id, 1),
        ])
        .await;

    first_copy.release_ok();
    second_copy.release_ok();

    users_finished_copy_notify.notified().await;
    orders_finished_copy_notify.notified().await;
    table_sync_events_notify.notified().await;

    assert_insert_ordinals(&destination.get_events().await, 0);

    // The main apply worker must assign the same ordinals after taking
    // ownership of both tables.
    let apply_events_notify = destination
        .wait_for_all_events(vec![
            EventCondition::TableCount(EventType::Insert, users_table_id, 2),
            EventCondition::TableCount(EventType::Insert, orders_table_id, 2),
        ])
        .await;

    write_two_table_transaction("apply").await;

    apply_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert_insert_ordinals(&destination.get_events().await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_ddl_without_relation_fails_before_persisting_sync_done() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP, "pause").unwrap();

    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let finished_copy_notify =
        store.notify_on_table_state_type(table_id, TableStateType::FinishedCopy).await;
    pipeline.start().await.unwrap();
    finished_copy_notify.notified().await;

    // Advance the physical schema while both logical connections are alive,
    // but emit no DML that would make pgoutput send a new Relation. The DDL
    // event trigger emits a transactional logical message, so its transaction
    // has a commit event on every supported PostgreSQL version. Keep the
    // table-sync worker paused until the apply worker has passed that commit,
    // so its later catchup target must include the DDL.
    let apply_commit_notify =
        destination.wait_for_events(vec![EventCondition::AnyCount(EventType::Commit, 1)]).await;
    let schema_stored_notify = store.notify_on_table_schema_count(table_id, 2).await;
    database
        .run_sql(&format!(
            "alter table {} add column handover_value integer not null default 0",
            users_schema.name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    apply_commit_notify.notified().await;

    let errored_notify = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;
    fail::remove(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP);

    schema_stored_notify.notified().await;
    errored_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    let TableState::Errored { retry_policy, source_err, .. } = table_state else {
        panic!("an incomplete row-decoding state must not be persisted as SyncDone");
    };
    assert!(matches!(retry_policy, TableRetryPolicy::ManualRetry));
    assert_eq!(source_err.kind(), ErrorKind::InvalidState);
    assert_eq!(source_err.description(), Some("Table-sync decoding state is incomplete"));

    let state_history = store.get_table_state_history(table_id).await;
    assert!(state_history.iter().all(|state| !matches!(state, TableState::SyncDone { .. })));

    let table_schemas = store.get_table_schemas().await;
    assert_table_schema_snapshots(
        table_schemas.get(&table_id).unwrap(),
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("handover_value", Type::INT4),
            ],
        ],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_quiescent_handover_does_not_persist_received_progress() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP, "pause").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;

    let other_database = spawn_source_database().await;
    let other_database_table = test_table_name("table_sync_keepalive_wal");
    other_database
        .create_table(other_database_table.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    let store = NotifyingStore::new();
    let (destination, mut writes_rx) = DeferredEventsDestination::new(table_id);

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let finished_copy_notify =
        store.notify_on_table_state_type(table_id, TableStateType::FinishedCopy).await;

    pipeline.start().await.unwrap();

    finished_copy_notify.notified().await;

    // Drop apply checkpoint writes before generating the WAL that will become
    // the handover boundary. The apply loop continues running, but the durable
    // checkpoint cannot reach that future boundary.
    fail::cfg(STORE_REPLICATION_CHECKPOINT_FP, "return(apply)").unwrap();

    // Commit one published row at A while the table-sync worker is paused.
    insert_users_data(&mut database, &users_schema.name, 1..=1).await;

    other_database
        .insert_values(other_database_table.clone(), &["value"], &[&1_i32])
        .await
        .unwrap();

    // The sender's next idle keepalive must advertise at least this post-commit
    // cluster WAL frontier even though the transaction emitted no event batch.
    let target_lsn = database.current_wal_flush_lsn().await.unwrap();
    wait_for_apply_worker_to_reach(&database, pipeline_id, target_lsn).await;

    let table_sync_done_notify =
        store.notify_on_table_state_type(table_id, TableStateType::SyncDone).await;

    fail::remove(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP);

    // Accepted(A) must be followed by the empty barrier because no event batch
    // exists at the later catchup target T to settle A's durability debt.
    let (accepted_commit_end_lsn, barrier_result) =
        tokio::time::timeout(Duration::from_secs(30), async {
            let Some(DeferredEventsWrite::Accepted { commit_end_lsn }) = writes_rx.recv().await
            else {
                panic!("expected accepted target-table event batch");
            };
            let Some(DeferredEventsWrite::DurabilityBarrier { result }) = writes_rx.recv().await
            else {
                panic!("expected empty required-durability barrier");
            };

            (commit_end_lsn, result)
        })
        .await
        .expect("timed out waiting for accepted batch and durability barrier");

    // This strict gap distinguishes keepalive-only completion from a terminal
    // event batch, which the existing RequireDurable path already covers.
    assert!(accepted_commit_end_lsn < target_lsn);

    // Catchup and SyncWait are in-memory states. The last persisted state must
    // remain FinishedCopy until the required barrier confirms durability.
    let table_states = store.get_table_states().await;
    assert_eq!(
        table_states.get(&table_id).map(TableState::as_type),
        Some(TableStateType::FinishedCopy)
    );

    barrier_result.send(Ok(DestinationWriteStatus::Durable));

    table_sync_done_notify.notified().await;

    let TableState::SyncDone { lsn: sync_done_lsn, table_decoding_state: Some(_) } =
        store.get_table_state(table_id).await.unwrap().unwrap()
    else {
        panic!("a durable terminal barrier must persist SyncDone with its decoder");
    };
    let checkpoint_before_unpause =
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap();
    assert!(checkpoint_before_unpause.is_none_or(|checkpoint| checkpoint < sync_done_lsn));

    fail::remove(STORE_REPLICATION_CHECKPOINT_FP);

    // Generate fresh unrelated WAL after restoring checkpoint writes and wait
    // until the apply worker reports that received progress to PostgreSQL.
    // Since no destination flush occurred, quiescent coordination must not
    // persist that received LSN as an apply checkpoint.
    other_database.insert_values(other_database_table, &["value"], &[&2_i32]).await.unwrap();
    let target_lsn = database.current_wal_flush_lsn().await.unwrap();
    wait_for_apply_worker_to_reach(&database, pipeline_id, target_lsn).await;

    let persisted_checkpoint_lsn =
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap();
    assert_eq!(persisted_checkpoint_lsn, checkpoint_before_unpause);
    assert!(matches!(
        store.get_table_state(table_id).await.unwrap(),
        Some(TableState::SyncDone { .. })
    ));

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_catchup_error_does_not_block_apply_worker() {
    let _scenario = FailScenario::setup();
    fail::cfg(TABLE_SYNC_WORKER_BEFORE_STREAMING_FP, "1*return(no_retry)").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;
    let users_schema = database_schema.users_schema();
    let orders_schema = database_schema.orders_schema();
    let users_table_id = users_schema.id;
    let orders_table_id = orders_schema.id;

    let copied_rows = 3;
    insert_users_data(&mut database, &users_schema.name, 1..=copied_rows).await;
    insert_orders_data(&mut database, &orders_schema.name, 1..=copied_rows).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_errored_notify =
        store.notify_on_table_state_type(users_table_id, TableStateType::Errored).await;
    let orders_errored_notify =
        store.notify_on_table_state_type(orders_table_id, TableStateType::Errored).await;
    let users_sync_done_notify =
        store.notify_on_table_state_type(users_table_id, TableStateType::SyncDone).await;
    let orders_sync_done_notify =
        store.notify_on_table_state_type(orders_table_id, TableStateType::SyncDone).await;
    let users_ready_notify =
        store.notify_on_table_state_type(users_table_id, TableStateType::Ready).await;
    let orders_ready_notify =
        store.notify_on_table_state_type(orders_table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();

    let (errored_table_id, healthy_table_id, healthy_table_name, sync_done_notify, ready_notify) = tokio::select! {
            () = users_errored_notify.notified() => (
                users_table_id,
                orders_table_id,
                orders_schema.name.clone(),
                orders_sync_done_notify,
                orders_ready_notify,
            ),
            () = orders_errored_notify.notified() => (
                orders_table_id,
                users_table_id,
                users_schema.name.clone(),
                users_sync_done_notify,
                users_ready_notify,
            ),
    };

    sync_done_notify.notified().await;
    let update_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Update, healthy_table_id, 1)])
        .await;
    database
        .run_sql(&format!(
            "update {} set id = id where id = 1",
            healthy_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Prove the apply worker still processes the healthy table after the other
    // table's catchup failure.
    update_notify.notified().await;
    ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store.get_table_state(errored_table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::NoRetry, .. }
    ));

    let table_rows = destination.get_table_rows().await;
    let copied_users_table_rows = table_rows.get(&users_table_id).unwrap();
    assert_eq!(copied_users_table_rows.len(), copied_rows);
    let copied_orders_table_rows = table_rows.get(&orders_table_id).unwrap();
    assert_eq!(copied_orders_table_rows.len(), copied_rows);
}

#[tokio::test(flavor = "multi_thread")]
async fn persisted_checkpoint_prevents_replay_when_status_updates_are_skipped() {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let table_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    let initial_inserts_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=2).await;

    initial_inserts_notify.notified().await;
    table_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // The persisted checkpoint is strictly greater than PostgreSQL's confirmed
    // flush LSN because this test artificially stopped status updates.
    let persisted_checkpoint_lsn =
        store.get_replication_checkpoint(WorkerType::Apply).await.unwrap().unwrap();
    let (confirmed_flush_lsn, _) =
        replication_slot_state(database.client.as_ref().unwrap(), &apply_slot_name).await;
    assert!(confirmed_flush_lsn < persisted_checkpoint_lsn);

    // We check the expected events after the first two inserts.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let inserts = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_inserts = build_expected_users_inserts(
        1,
        &database_schema.users_schema(),
        vec![("user_1", 1), ("user_2", 2)],
    );

    assert_events_equal(inserts, &expected_inserts);

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // We wait until 4 inserts have been reached, the previous ones + the current
    // ones.
    let new_inserts_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 4)])
        .await;

    pipeline.start().await.unwrap();

    insert_users_data(&mut database, &database_schema.users_schema().name, 3..=4).await;

    new_inserts_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check the expected events after all the inserts.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let inserts = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_inserts = build_expected_users_inserts(
        1,
        &database_schema.users_schema(),
        vec![("user_1", 1), ("user_2", 2), ("user_3", 3), ("user_4", 4)],
    );
    assert_events_equal(inserts, &expected_inserts);
}

/// Whether the first replicated row precedes the first schema change.
#[derive(Clone, Copy)]
enum SchemaReplayOrder {
    DmlBeforeDdl,
    DdlBeforeDml,
}

/// Whether the replay sequence is committed atomically or incrementally.
#[derive(Clone, Copy)]
enum SchemaReplayTransactionScope {
    OneTransaction,
    SeparateTransactions,
}

/// Exercises one cell of the schema-replay order and transaction matrix.
async fn run_schema_replay_scenario(
    order: SchemaReplayOrder,
    transaction_scope: SchemaReplayTransactionScope,
) {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();
    fail::cfg(STORE_REPLICATION_CHECKPOINT_FP, "return(apply)").unwrap();

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
        "schema_add_column",
        &[("name", "text not null"), ("age", "integer not null")],
    )
    .await;

    destination.clear_events().await;

    let event_count = match order {
        SchemaReplayOrder::DmlBeforeDdl => 3,
        SchemaReplayOrder::DdlBeforeDml => 2,
    };
    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, event_count),
            EventCondition::TableCount(EventType::Insert, table_id, event_count),
        ])
        .await;

    match transaction_scope {
        SchemaReplayTransactionScope::OneTransaction => {
            let transaction = database.begin_transaction().await;

            if matches!(order, SchemaReplayOrder::DmlBeforeDdl) {
                transaction
                    .insert_values(table_name.clone(), &["name", "age"], &[&"first", &25])
                    .await
                    .unwrap();
            }
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
                .insert_values(
                    table_name.clone(),
                    &["name", "age", "status"],
                    &[&"second", &28, &"active"],
                )
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
        }
        SchemaReplayTransactionScope::SeparateTransactions => {
            if matches!(order, SchemaReplayOrder::DmlBeforeDdl) {
                database
                    .insert_values(table_name.clone(), &["name", "age"], &[&"first", &25])
                    .await
                    .unwrap();
            }
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
                .insert_values(
                    table_name.clone(),
                    &["name", "age", "status"],
                    &[&"second", &28, &"active"],
                )
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
        }
    }

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let initial_columns = [("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)];
    let added_columns =
        [("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("status", Type::TEXT)];
    let final_columns = [("id", Type::INT8), ("name", Type::TEXT), ("status", Type::TEXT)];
    let mut expected_events = Vec::new();
    if matches!(order, SchemaReplayOrder::DmlBeforeDdl) {
        expected_events.extend([
            ExpectedReplicatedEvent::Relation(initial_columns.as_slice()),
            ExpectedReplicatedEvent::Insert(initial_columns.as_slice()),
        ]);
    }
    expected_events.extend([
        ExpectedReplicatedEvent::Relation(added_columns.as_slice()),
        ExpectedReplicatedEvent::Insert(added_columns.as_slice()),
        ExpectedReplicatedEvent::Relation(final_columns.as_slice()),
        ExpectedReplicatedEvent::Insert(final_columns.as_slice()),
    ]);
    assert_table_event_sequence(&events, table_id, &expected_events);

    let table_schemas = store.get_table_schemas().await;
    let table_schemas_snapshots = table_schemas.get(&table_id).unwrap();
    assert_table_schema_snapshots(
        table_schemas_snapshots,
        &[initial_columns.as_slice(), added_columns.as_slice(), final_columns.as_slice()],
    );
    let expected_event_snapshot_ids = match order {
        SchemaReplayOrder::DmlBeforeDdl => table_schemas_snapshots.as_slice(),
        SchemaReplayOrder::DdlBeforeDml => &table_schemas_snapshots[1..],
    }
    .iter()
    .map(|(snapshot_id, _)| *snapshot_id)
    .collect::<Vec<_>>();
    assert_relation_insert_schema_pairs(&events, table_id, &expected_event_snapshot_ids);

    let initial_events = collect_table_events(&events, table_id);
    let initial_table_schema_snapshots = table_schemas_snapshots.clone();

    fail::remove(SEND_STATUS_UPDATE_FP);
    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication,
        store.clone(),
        destination.clone(),
    );

    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, event_count),
            EventCondition::TableCount(EventType::Insert, table_id, event_count),
        ])
        .await;

    pipeline.start().await.unwrap();

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let restarted_events = destination.get_events().await;
    assert_events_equal(&collect_table_events(&restarted_events, table_id), &initial_events);
    assert_relation_insert_schema_pairs(&restarted_events, table_id, &expected_event_snapshot_ids);

    let restarted_table_schemas = store.get_table_schemas().await;
    assert_restarted_schema_snapshot_pairs(
        restarted_table_schemas.get(&table_id).unwrap(),
        &initial_table_schema_snapshots,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_replay_preserves_dml_before_ddl_in_one_transaction() {
    run_schema_replay_scenario(
        SchemaReplayOrder::DmlBeforeDdl,
        SchemaReplayTransactionScope::OneTransaction,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_replay_preserves_dml_before_ddl_across_transactions() {
    run_schema_replay_scenario(
        SchemaReplayOrder::DmlBeforeDdl,
        SchemaReplayTransactionScope::SeparateTransactions,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_replay_preserves_ddl_before_dml_in_one_transaction() {
    run_schema_replay_scenario(
        SchemaReplayOrder::DdlBeforeDml,
        SchemaReplayTransactionScope::OneTransaction,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_replay_preserves_ddl_before_dml_across_transactions() {
    run_schema_replay_scenario(
        SchemaReplayOrder::DdlBeforeDml,
        SchemaReplayTransactionScope::SeparateTransactions,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_snapshots_are_pruned_after_durable_relation_batch() {
    let _scenario = FailScenario::setup();

    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, _pipeline_id, _publication) =
        create_database_and_sync_done_pipeline_with_table(
            "schema_cleanup",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    let prune_notify = store.notify_on_table_schema_prune().await;
    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 2),
            EventCondition::TableCount(EventType::Insert, table_id, 2),
        ])
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
            &[&"Alice", &25, &"alice@example.com"],
        )
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();

    database
        .insert_values(table_name.clone(), &["name", "email"], &[&"Bob", &"bob@example.com"])
        .await
        .unwrap();

    ready_notify.notified().await;
    events_notify.notified().await;
    prune_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_schemas = store.get_table_schemas().await;
    let after_snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(after_snapshots.len(), 1);
    assert_table_schema_column_names_types(
        &after_snapshots[0].1,
        &[("id", Type::INT8), ("name", Type::TEXT), ("email", Type::TEXT)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn first_relation_after_restart_retries_schema_cleanup() {
    let _scenario = FailScenario::setup();
    fail::cfg(STORE_REPLICATION_CHECKPOINT_FP, "return(apply)").unwrap();

    init_test_tracing();

    let (database, table_name, table_id, store, destination, pipeline, pipeline_id, publication) =
        create_database_and_sync_done_pipeline_with_table(
            "schema_cleanup_restart",
            &[("name", "text not null"), ("age", "integer not null")],
        )
        .await;

    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 2),
            EventCondition::TableCount(EventType::Insert, table_id, 2),
        ])
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
            &[&"Alice", &25, &"alice@example.com"],
        )
        .await
        .unwrap();

    database
        .alter_table(table_name.clone(), &[TableModification::DropColumn { name: "age" }])
        .await
        .unwrap();
    database
        .insert_values(table_name.clone(), &["name", "email"], &[&"Bob", &"bob@example.com"])
        .await
        .unwrap();

    events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // Without a persisted checkpoint, cleanup cannot determine a crash-safe
    // retention boundary. All schema versions therefore remain available.
    let table_schemas = store.get_table_schemas().await;
    assert_table_schema_snapshots(
        table_schemas.get(&table_id).unwrap(),
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4)],
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
            &[("id", Type::INT8), ("name", Type::TEXT), ("email", Type::TEXT)],
        ],
    );

    fail::remove(STORE_REPLICATION_CHECKPOINT_FP);

    let mut pipeline =
        create_pipeline(&database.config, pipeline_id, publication, store.clone(), destination);
    let mut prune_notify = store.notify_on_table_schema_prune().await;

    pipeline.start().await.unwrap();

    // The first post-restart DML emits a relation even without another DDL.
    // That relation reconstructs the in-memory cleanup candidate lost at
    // shutdown.
    database
        .insert_values(table_name, &["name", "email"], &[&"Charlie", &"charlie@example.com"])
        .await
        .unwrap();

    loop {
        prune_notify.notified().await;

        // Register the next notification before checking so a cleanup between
        // the check and the next wait cannot be missed.
        let next_prune_notify = store.notify_on_table_schema_prune().await;
        let table_schemas = store.get_table_schemas().await;
        if table_schemas.get(&table_id).unwrap().len() == 1 {
            break;
        }

        prune_notify = next_prune_notify;
    }

    pipeline.shutdown_and_wait().await.unwrap();

    let table_schemas = store.get_table_schemas().await;
    let after_snapshots = table_schemas.get(&table_id).unwrap();
    assert_eq!(after_snapshots.len(), 1);
    assert_table_schema_column_names_types(
        &after_snapshots[0].1,
        &[("id", Type::INT8), ("name", Type::TEXT), ("email", Type::TEXT)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_schema_snapshots_replay_before_each_table_first_relation() {
    let _scenario = FailScenario::setup();
    fail::cfg(SEND_STATUS_UPDATE_FP, "return").unwrap();

    init_test_tracing();
    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    let first_table = test_table_name("publication_replay_first");
    let first_table_id = database
        .create_table(
            first_table.clone(),
            true,
            &[("a", "integer not null"), ("b", "integer not null"), ("c", "integer not null")],
        )
        .await
        .unwrap();
    let second_table = test_table_name("publication_replay_second");
    let second_table_id = database
        .create_table(
            second_table.clone(),
            true,
            &[("x", "integer not null"), ("y", "integer not null"), ("z", "integer not null")],
        )
        .await
        .unwrap();

    let publication_name = format!("PublicationReplay{}", random::<u32>());
    let quoted_publication_name = quote_identifier(&publication_name);
    database
        .run_sql(&format!(
            "create publication {quoted_publication_name} for table {} (id, a, b, c), {} (id, x, \
             y, z)",
            first_table.as_quoted_identifier(),
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    let other_publication_name = format!("other_pub_{}", random::<u32>());
    database
        .run_sql(&format!(
            "create publication {other_publication_name} for table {} (id, a, b, c), {} (id, x, \
             y, z)",
            first_table.as_quoted_identifier(),
            second_table.as_quoted_identifier()
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

    let first_sync_complete_notify = store.notify_on_table_sync_complete(first_table_id).await;
    let second_sync_complete_notify = store.notify_on_table_sync_complete(second_table_id).await;

    pipeline.start().await.unwrap();

    first_sync_complete_notify.notified().await;
    second_sync_complete_notify.notified().await;
    fail::cfg(STORE_REPLICATION_CHECKPOINT_FP, "return").unwrap();

    let first_schema_stored_notify = store.notify_on_table_schema_count(first_table_id, 3).await;
    let second_schema_stored_notify = store.notify_on_table_schema_count(second_table_id, 2).await;
    let ddl_commits_notify =
        destination.wait_for_events(vec![EventCondition::AnyCount(EventType::Commit, 3)]).await;

    // A logical message for another publication is visible in this slot but
    // must not create a schema snapshot or invalidate either table's cache.
    database
        .run_sql(&format!(
            "alter publication {other_publication_name} set table {} (id, b, c), {} (id, x, z)",
            first_table.as_quoted_identifier(),
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // A physical schema change also advances the schema snapshot without
    // synthesizing a Relation because no DML has occurred.
    database
        .run_sql(&format!(
            "alter table {} add column d integer not null default 0",
            first_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Both affected tables are carried by one source transaction as separate
    // self-describing schema messages. No DML has occurred for either table.
    database
        .run_sql(&format!(
            "alter publication {quoted_publication_name} set table {} (id, a, c, d), {} (id, y, z)",
            first_table.as_quoted_identifier(),
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    first_schema_stored_notify.notified().await;
    second_schema_stored_notify.notified().await;
    ddl_commits_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let first_run_events = destination.get_events().await;
    assert!(
        collect_table_events(&first_run_events, first_table_id).is_empty()
            && collect_table_events(&first_run_events, second_table_id).is_empty()
    );

    let first_run_schemas = store.get_table_schemas().await;
    let first_snapshots = first_run_schemas.get(&first_table_id).unwrap();
    let second_snapshots = first_run_schemas.get(&second_table_id).unwrap();
    assert_table_schema_snapshots(
        first_snapshots,
        &[
            &[("id", Type::INT8), ("a", Type::INT4), ("b", Type::INT4), ("c", Type::INT4)],
            &[
                ("id", Type::INT8),
                ("a", Type::INT4),
                ("b", Type::INT4),
                ("c", Type::INT4),
                ("d", Type::INT4),
            ],
            &[
                ("id", Type::INT8),
                ("a", Type::INT4),
                ("b", Type::INT4),
                ("c", Type::INT4),
                ("d", Type::INT4),
            ],
        ],
    );
    assert_table_schema_snapshots(
        second_snapshots,
        &[
            &[("id", Type::INT8), ("x", Type::INT4), ("y", Type::INT4), ("z", Type::INT4)],
            &[("id", Type::INT8), ("x", Type::INT4), ("y", Type::INT4), ("z", Type::INT4)],
        ],
    );
    // Isolate the replay phase without clearing stored schemas. Replayed
    // snapshots are de-duplicated by table and snapshot ID.
    fail::remove(SEND_STATUS_UPDATE_FP);
    destination.clear_events().await;

    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    let row_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, first_table_id, 1),
            EventCondition::TableCount(EventType::Insert, first_table_id, 1),
            EventCondition::TableCount(EventType::Relation, second_table_id, 1),
            EventCondition::TableCount(EventType::Insert, second_table_id, 1),
        ])
        .await;

    pipeline.start().await.unwrap();

    database
        .run_sql(&format!(
            "insert into {} (a, b, c, d) values (1, 2, 3, 4)",
            first_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (x, y, z) values (4, 5, 6)",
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    row_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let replayed_schemas = store.get_table_schemas().await;
    assert_eq!(replayed_schemas, first_run_schemas);

    let events = destination.get_events().await;
    assert_filtered_table_events(
        &events,
        first_table_id,
        &[("id", Type::INT8), ("a", Type::INT4), ("c", Type::INT4), ("d", Type::INT4)],
        &[1, 1, 0, 1, 1],
        first_snapshots[2].0,
        &[Cell::I64(1), Cell::I32(1), Cell::I32(3), Cell::I32(4)],
    );
    assert_filtered_table_events(
        &events,
        second_table_id,
        &[("id", Type::INT8), ("y", Type::INT4), ("z", Type::INT4)],
        &[1, 0, 1, 1],
        second_snapshots[1].0,
        &[Cell::I64(1), Cell::I32(5), Cell::I32(6)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_and_publication_schema_changes_replay_after_restart() {
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

    // Start without a column list so a physical ADD COLUMN is immediately
    // visible to the publication.
    let publication_name = format!("pub_{}", random::<u32>());
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
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

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;
    fail::cfg(STORE_REPLICATION_CHECKPOINT_FP, "return").unwrap();

    // We expect one relation and insert for the initial state, the physical
    // table change, and the publication-filter change.
    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 3),
            EventCondition::TableCount(EventType::Insert, table_id, 3),
        ])
        .await;

    // State 1: Insert with all 4 columns (id, name, age, email).
    database
        .run_sql(&format!(
            "insert into {} (name, age, email) values ('Alice', 25, 'alice@example.com')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // State 2: Add a physical column, then insert with the expanded schema.
    database
        .run_sql(&format!(
            "alter table {} add column status text not null default 'pending'",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (name, age, email, status) values ('Bob', 30, 'bob@example.com', \
             'active')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // State 3: Keep the physical schema but remove age from the publication.
    database
        .run_sql(&format!(
            "alter publication {publication_name} set table {} (id, name, email, status)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    database
        .run_sql(&format!(
            "insert into {} (name, age, email, status) values ('Charlie', 35, \
             'charlie@example.com', 'inactive')",
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
        assert_eq!(relation_events.len(), 3);

        assert!(
            relation_events[0].replicated_table_schema.inner().snapshot_id
                < relation_events[1].replicated_table_schema.inner().snapshot_id
        );
        assert!(
            relation_events[1].replicated_table_schema.inner().snapshot_id
                < relation_events[2].replicated_table_schema.inner().snapshot_id
        );

        // Verify relation column names for each state.
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
        assert_eq!(relation_2_cols, vec!["id", "name", "age", "email", "status"]);

        let relation_3_cols: Vec<&str> = relation_events[2]
            .replicated_table_schema
            .column_schemas()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(relation_3_cols, vec!["id", "name", "email", "status"]);

        // Verify replication masks.
        assert_eq!(
            relation_events[0].replicated_table_schema.replication_mask().as_slice(),
            &[1, 1, 1, 1]
        );
        assert_eq!(
            relation_events[1].replicated_table_schema.replication_mask().as_slice(),
            &[1, 1, 1, 1, 1]
        );
        assert_eq!(
            relation_events[2].replicated_table_schema.replication_mask().as_slice(),
            &[1, 1, 0, 1, 1]
        );

        // The final publication mask removes `age`, but the stored physical
        // schema still contains it.
        assert_eq!(relation_events[2].replicated_table_schema.inner().column_schemas.len(), 5);

        let insert_events = grouped.get(&(EventType::Insert, table_id)).unwrap();

        // Verify exact payloads so a publication-column shift cannot pass by
        // preserving only the number of decoded values.
        let insert_values: Vec<Vec<Cell>> = insert_events
            .iter()
            .filter_map(|event| {
                if let Event::Insert(InsertEvent { table_row, .. }) = event {
                    Some(table_row.values().to_vec())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(
            insert_values,
            vec![
                vec![
                    Cell::I64(1),
                    Cell::String("Alice".to_owned()),
                    Cell::I32(25),
                    Cell::String("alice@example.com".to_owned()),
                ],
                vec![
                    Cell::I64(2),
                    Cell::String("Bob".to_owned()),
                    Cell::I32(30),
                    Cell::String("bob@example.com".to_owned()),
                    Cell::String("active".to_owned()),
                ],
                vec![
                    Cell::I64(3),
                    Cell::String("Charlie".to_owned()),
                    Cell::String("charlie@example.com".to_owned()),
                    Cell::String("inactive".to_owned()),
                ],
            ]
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
    assert_table_schema_snapshots(
        table_schemas_snapshots,
        &[
            &[("id", Type::INT8), ("name", Type::TEXT), ("age", Type::INT4), ("email", Type::TEXT)],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("email", Type::TEXT),
                ("status", Type::TEXT),
            ],
            &[
                ("id", Type::INT8),
                ("name", Type::TEXT),
                ("age", Type::INT4),
                ("email", Type::TEXT),
                ("status", Type::TEXT),
            ],
        ],
    );
    let expected_snapshot_ids =
        table_schemas_snapshots.iter().map(|(snapshot_id, _)| *snapshot_id).collect::<Vec<_>>();
    assert_relation_insert_schema_pairs(&events, table_id, &expected_snapshot_ids);

    let initial_events = collect_table_events(&events, table_id);
    let initial_table_schema_snapshots = table_schemas_snapshots.clone();

    // Discard the first run's events before collecting the replay.
    destination.clear_events().await;

    // Remove the failpoint now that run 1 has shut down. Run 1 never acked
    // progress, so the slot still holds all of run 1's WAL for replay; we no
    // longer need to suppress acks in run 2. Doing so risks wal_sender_timeout
    // firing under slow CI and producing duplicates that invalidate the
    // assertions below.
    fail::remove(SEND_STATUS_UPDATE_FP);

    // Restart the pipeline. PostgreSQL resends the first run because its slot
    // received no progress feedback.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        store.clone(),
        destination.clone(),
    );

    // Wait for 3 relation events and 3 insert events again after restart.
    let restart_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 3),
            EventCondition::TableCount(EventType::Insert, table_id, 3),
        ])
        .await;

    pipeline.start().await.unwrap();

    restart_events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the same events are received after restart.
    let events_after_restart = destination.get_events().await;
    assert_events_equal(&collect_table_events(&events_after_restart, table_id), &initial_events);
    let restarted_table_schemas = store.get_table_schemas().await;
    assert_restarted_schema_snapshot_pairs(
        restarted_table_schemas.get(&table_id).unwrap(),
        &initial_table_schema_snapshots,
    );
    assert_relation_insert_schema_pairs(&events_after_restart, table_id, &expected_snapshot_ids);
}

#[tokio::test(flavor = "multi_thread")]
async fn worker_connections_are_tagged_with_per_worker_application_names() {
    let _scenario = FailScenario::setup();
    fail::cfg(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP, "pause").unwrap();

    init_test_tracing();

    // --- GIVEN: a pipeline whose table sync worker is paused after copy, so both
    // worker connections are alive ---
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let finished_copy_notify =
        store.notify_on_table_state_type(table_id, TableStateType::FinishedCopy).await;

    pipeline.start().await.unwrap();

    finished_copy_notify.notified().await;

    // --- THEN: pg_stat_activity shows both connections under their per-worker
    // application names ---
    //
    // Names are computed with the same helpers used by the workers because the
    // random test pipeline ids can be long enough to clamp the base name.
    let apply_name =
        apply_worker_application_name("supabase_etl_replicator_replication", pipeline_id);
    let table_sync_name = table_sync_worker_application_name(
        "supabase_etl_replicator_replication",
        pipeline_id,
        table_id,
    );

    let client = database.client.as_ref().unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = client
                .query(
                    "select application_name from pg_stat_activity where datname = \
                     current_database() and application_name in ($1, $2)",
                    &[&apply_name, &table_sync_name],
                )
                .await
                .unwrap();
            let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

            if names.contains(&apply_name) && names.contains(&table_sync_name) {
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for per-worker application names in pg_stat_activity");

    let sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    fail::remove(START_TABLE_SYNC_AFTER_FINISHED_COPY_FP);

    sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
}
