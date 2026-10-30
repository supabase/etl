use std::time::Duration;

use etl::{
    data::Cell,
    error::ErrorKind,
    event::{Event, EventType},
    pipeline::PipelineId,
    schema::TableId,
    store::{StateStore, TableRetryPolicy, TableState, TableStateType},
    test_utils::{
        database::{
            WalsenderTermination, replication_slot_state, spawn_source_database,
            terminate_active_walsender, terminate_walsender, wait_for_new_walsender,
        },
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::{FaultAction, FaultyOp},
        memory_destination::MemoryDestination,
        notify::DEFAULT_NOTIFY_TIMEOUT,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, create_pipeline},
        property::{block_on, run_expensive_property},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
};
use etl_postgres::{slots::EtlReplicationSlot, tokio::test_utils::PgDatabase};
use etl_telemetry::tracing::init_test_tracing;
use proptest::prelude::*;
use rand::random;
use tokio_postgres::{Client, types::PgLsn};

use crate::support::cdc_workload::{
    AfterFaultStep, TxPhase, WORKLOAD_PUBLICATION, WORKLOAD_TIMEOUT, Workload, WorkloadCoverage,
    WorkloadDestination, WorkloadHistory, WorkloadRunner, WorkloadTables,
    assert_workload_converged, create_workload_tables, read_source_rows, wait_for_notification,
    workloads,
};

/// Returns the current WAL write position of the source.
async fn current_wal_lsn(client: &Client) -> PgLsn {
    client.query_one("select pg_current_wal_lsn()", &[]).await.unwrap().get(0)
}

/// Returns the commit LSNs of recorded insert events for the table, in order.
fn table_insert_commit_lsns(events: &[Event], table_id: TableId) -> Vec<PgLsn> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert.commit_lsn)
            }
            _ => None,
        })
        .collect()
}

/// Controls write-response handling around the walsender disconnect.
#[derive(Clone, Copy, Debug)]
enum WriteResponseTiming {
    /// Disconnect after the fault transaction is acknowledged.
    Unheld,
    /// Release the in-flight response after disconnect so reconnect can
    /// proceed.
    ReleaseThenWaitForReconnect,
}

/// One generated walsender roulette schedule.
#[derive(Clone, Debug)]
struct WalsenderRouletteCase {
    /// Source transactions around the disconnect.
    workload: Workload,
    /// Write-response schedule around the disconnect.
    response_timing: WriteResponseTiming,
}

/// Generates workloads and disconnect schedules.
fn walsender_roulette_cases() -> impl Strategy<Value = WalsenderRouletteCase> {
    let response_timing = prop_oneof![
        Just(WriteResponseTiming::Unheld),
        Just(WriteResponseTiming::ReleaseThenWaitForReconnect),
    ];

    (workloads(), response_timing)
        .prop_map(|(workload, response_timing)| WalsenderRouletteCase { workload, response_timing })
}

/// Terminates the active apply walsender and maps failures.
async fn terminate_apply_walsender(
    client: &Client,
    apply_slot_name: &str,
) -> Result<i32, TestCaseError> {
    let termination =
        terminate_active_walsender(client, apply_slot_name).await.map_err(|error| {
            TestCaseError::fail(format!("Failed to terminate active walsender: {error}"))
        })?;

    match termination {
        WalsenderTermination::Inactive => {
            Err(TestCaseError::fail("Apply walsender was not active at disconnect"))
        }
        WalsenderTermination::NotTerminated { pid } => {
            Err(TestCaseError::fail(format!("Postgres did not terminate apply walsender {pid}")))
        }
        WalsenderTermination::Terminated { pid } => Ok(pid),
    }
}

/// Waits for a new apply walsender and maps failures.
async fn wait_for_apply_reconnect(
    client: &Client,
    apply_slot_name: &str,
    old_pid: i32,
) -> Result<(), TestCaseError> {
    let reconnect =
        wait_for_new_walsender(client, apply_slot_name, old_pid, WORKLOAD_TIMEOUT).await;
    let new_pid = reconnect.map_err(|error| {
        TestCaseError::fail(format!(
            "Failed to query the apply walsender while waiting for reconnect: {error}"
        ))
    })?;

    prop_assert!(new_pid.is_some(), "Timed out waiting for the apply walsender to reconnect");

    Ok(())
}

/// Runs the generated workload and disconnect schedule against a running
/// pipeline.
async fn run_roulette_workload(
    database: &PgDatabase<Client>,
    tables: &WorkloadTables,
    destination: &WorkloadDestination,
    apply_slot_name: &str,
    case: &WalsenderRouletteCase,
) -> Result<WorkloadHistory, TestCaseError> {
    let client = database.client.as_ref().unwrap();
    let mut runner = WorkloadRunner::new(client, tables);
    for tx in case.workload.before_fault() {
        runner.execute_and_wait(tx, TxPhase::BeforeFault, destination).await?;
    }

    let mut straddle = match case.workload.straddle() {
        Some(inserts) => Some(runner.open_straddle(&database.config, inserts).await?),
        None => None,
    };

    match case.response_timing {
        WriteResponseTiming::Unheld => {
            runner.execute_and_wait(case.workload.fault_tx(), TxPhase::Fault, destination).await?;
            let old_pid = terminate_apply_walsender(client, apply_slot_name).await?;
            wait_for_apply_reconnect(client, apply_slot_name, old_pid).await?;
        }
        WriteResponseTiming::ReleaseThenWaitForReconnect => {
            let hold = destination.hold_next(FaultyOp::WriteEvents).await;
            let delivered = runner
                .execute(case.workload.fault_tx(), TxPhase::Fault, Some(destination))
                .await?
                .expect("the fault transaction always commits");
            tokio::time::timeout(WORKLOAD_TIMEOUT, hold.wait_reached())
                .await
                .map_err(|_| TestCaseError::fail("timed out waiting for held write response"))?;

            let old_pid = terminate_apply_walsender(client, apply_slot_name).await?;

            // The failed apply attempt must observe this response before it
            // can reconnect and replay the uncheckpointed batch.
            hold.release_ok();

            wait_for_notification(&delivered, "delivery of the held fault transaction").await?;
            wait_for_apply_reconnect(client, apply_slot_name, old_pid).await?;
        }
    }

    for step in case.workload.after_fault() {
        match step {
            AfterFaultStep::Tx(tx) => {
                runner.execute_and_wait(tx, TxPhase::AfterFault, destination).await?;
            }
            AfterFaultStep::CommitStraddle => {
                let session = straddle.take().expect("planned straddle should be open");
                runner.commit_straddle(session, destination).await?;
            }
        }
    }

    Ok(runner.into_history())
}

/// Runs one generated workload and fault schedule to convergence.
async fn run_walsender_roulette_case(
    case: &WalsenderRouletteCase,
    coverage: &WorkloadCoverage,
) -> Result<(), TestCaseError> {
    let database = spawn_source_database().await;
    let tables = create_workload_tables(&database, &case.workload).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        WORKLOAD_PUBLICATION.to_owned(),
        store.clone(),
        destination.clone(),
    );
    let mut sync_complete = Vec::new();
    for table_id in tables.ids() {
        sync_complete.push(store.notify_on_table_sync_complete(*table_id).await);
    }

    pipeline
        .start()
        .await
        .map_err(|error| TestCaseError::fail(format!("pipeline failed to start: {error}")))?;
    let workload_result = async {
        for notification in &sync_complete {
            wait_for_notification(notification, "table synchronization to complete").await?;
        }
        run_roulette_workload(&database, &tables, &destination, &apply_slot_name, case).await
    }
    .await;

    let shutdown_result = tokio::time::timeout(WORKLOAD_TIMEOUT, pipeline.shutdown_and_wait())
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for pipeline shutdown"))
        .and_then(|result| {
            result
                .map_err(|error| TestCaseError::fail(format!("pipeline shutdown failed: {error}")))
        });

    let history = workload_result?;
    shutdown_result?;

    let source = read_source_rows(database.client.as_ref().unwrap(), &tables).await?;
    let events = destination.get_events().await;
    let copied_rows = destination.get_table_rows().await;

    // A walsender disconnect may replay any unconfirmed suffix once.
    assert_workload_converged(
        &tables,
        &copied_rows,
        &events,
        &source,
        &history,
        |_| 1..=2,
        coverage,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_shutdown_error_is_returned_by_shutdown_and_wait() {
    init_test_tracing();

    // GIVEN: a healthy pipeline whose destination fails on shutdown
    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    destination
        .inject_fault(
            FaultyOp::Shutdown,
            FaultAction::reject(ErrorKind::DestinationQueryFailed, "injected shutdown failure"),
        )
        .await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    // WHEN: the pipeline shuts down
    let result = pipeline.shutdown_and_wait().await;

    // THEN: the injected shutdown error surfaces and shutdown was invoked
    let err = result.unwrap_err();
    assert!(err.kinds().contains(&ErrorKind::DestinationQueryFailed));
    assert!(destination.shutdown_called().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_retry_reselects_relation_snapshots_after_ambiguous_write() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;

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

    let users_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;
    pipeline.start().await.unwrap();
    users_sync_complete_notify.notified().await;

    // The inner destination applies the transaction, but the apply worker sees
    // a timed-retriable failure and reconnects from its durable start LSN.
    destination
        .inject_fault(
            FaultyOp::WriteEvents,
            FaultAction::fail_after_write(
                ErrorKind::DestinationTimeout,
                "injected ambiguous streaming failure",
            ),
        )
        .await;

    let (_, active_pid) =
        replication_slot_state(database.client.as_ref().unwrap(), &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    let replayed_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 2),
            EventCondition::TableCount(EventType::Insert, table_id, 2),
        ])
        .await;

    // Both schema versions occur in one transaction. On retry, the first
    // Relation must resolve the pre-DDL schema instead of reusing the post-DDL
    // runtime schema cached by the failed attempt.
    let transaction = database.begin_transaction().await;
    transaction
        .insert_values(users_schema.name.clone(), &["name", "age"], &[&"before", &1])
        .await
        .unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} drop column age",
            users_schema.name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.insert_values(users_schema.name.clone(), &["name"], &[&"after"]).await.unwrap();
    transaction.commit_transaction().await;

    users_ready_notify.notified().await;
    assert!(
        wait_for_new_walsender(
            database.client.as_ref().unwrap(),
            &apply_slot_name,
            old_pid,
            DEFAULT_NOTIFY_TIMEOUT,
        )
        .await
        .unwrap()
        .is_some()
    );
    replayed_events_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relation_schemas = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) if relation.replicated_table_schema.id() == table_id => {
                Some(&relation.replicated_table_schema)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(relation_schemas.len(), 2);
    assert_eq!(
        relation_schemas[0].column_schemas().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name", "age"]
    );
    assert_eq!(
        relation_schemas[1].column_schemas().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert!(relation_schemas[0].inner().snapshot_id < relation_schemas[1].inner().snapshot_id);

    let inserts = events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(inserts.len(), 2);
    assert_eq!(
        inserts[0].replicated_table_schema.inner().snapshot_id,
        relation_schemas[0].inner().snapshot_id
    );
    assert_eq!(
        inserts[1].replicated_table_schema.inner().snapshot_id,
        relation_schemas[1].inner().snapshot_id
    );
    assert_eq!(
        inserts.iter().map(|insert| insert.table_row.values().to_vec()).collect::<Vec<_>>(),
        vec![
            vec![Cell::I64(1), Cell::String("before".to_owned()), Cell::I32(1)],
            vec![Cell::I64(2), Cell::String("after".to_owned())],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_rejection_keeps_table_restartable_until_retry() {
    init_test_tracing();

    // GIVEN: a pipeline whose table is Ready.
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let initial_rows = 2;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=initial_rows).await;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    database
        .run_sql(&format!(
            "update {} set age = age where id = 1",
            database_schema.users_schema().name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    users_ready_notify.notified().await;

    // WHEN: a resync starts and the destination rejects the drop
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::reject(ErrorKind::DestinationConnectionFailed, "injected drop rejection"),
        )
        .await;

    let users_errored_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored_notify.notified().await;

    // THEN: the table errors with a timed retry and nothing was torn down
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::TimedRetry { .. }, .. }
    ));
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(store.get_latest_table_schemas().await.contains_key(&table_id));

    // The drop never ran on the inner destination, so its rows are untouched.
    assert_eq!(memory_destination.table_rows().await.get(&table_id).unwrap().len(), initial_rows);

    // THEN: the timed retry drops and recopies the table cleanly
    let users_sync_complete_again_notify = store.notify_on_table_sync_complete(table_id).await;

    users_sync_complete_again_notify.notified().await;

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).unwrap().len(), initial_rows);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_failure_after_write_keeps_table_restartable_until_retry() {
    init_test_tracing();

    // GIVEN: a pipeline whose table is Ready.
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let initial_rows = 2;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=initial_rows).await;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    let users_ready_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    database
        .run_sql(&format!(
            "update {} set age = age where id = 1",
            database_schema.users_schema().name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    users_ready_notify.notified().await;

    // WHEN: a resync starts and the drop fails after being applied
    destination
        .inject_fault(
            FaultyOp::DropTableForCopy,
            FaultAction::fail_after_write(
                ErrorKind::DestinationConnectionFailed,
                "injected drop failure after write",
            ),
        )
        .await;

    let users_errored_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    store.reset_table_state(table_id).await.unwrap();

    users_errored_notify.notified().await;

    // THEN: the table errors with a timed retry and ETL state was not cleared
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(
        table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::TimedRetry { .. }, .. }
    ));
    assert!(store.get_latest_table_schemas().await.contains_key(&table_id));

    // The inner destination applied the drop; the apply loop saw a failure.
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
    assert!(!memory_destination.table_rows().await.contains_key(&table_id));

    // THEN: the timed retry replays the drop and recopies the table cleanly
    let users_sync_complete_again_notify = store.notify_on_table_sync_complete(table_id).await;

    users_sync_complete_again_notify.notified().await;

    assert!(destination.was_table_dropped_for_copy(table_id).await);
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_id).unwrap().len(), initial_rows);

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_pending_write_events_before_destination_shutdown() {
    init_test_tracing();

    // GIVEN: a streaming pipeline whose next write_events response is held
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let users_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    hold.wait_reached().await;

    // WHEN: the pipeline shuts down while the write response is withheld
    let mut shutdown_task = tokio::spawn(pipeline.shutdown_and_wait());

    // THEN: shutdown waits on the pending response instead of proceeding
    if let Ok(result) = tokio::time::timeout(Duration::from_secs(2), &mut shutdown_task).await {
        panic!("shutdown completed while the write response was withheld: {result:?}");
    }
    assert!(!destination.shutdown_called().await);

    // WHEN: the held response is released
    hold.release_ok();

    // THEN: shutdown completes and the write was acknowledged before it
    shutdown_task.await.unwrap().unwrap();
    assert!(destination.shutdown_called().await);

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    assert_eq!(grouped_events.get(&(EventType::Insert, table_id)).map_or(0, Vec::len), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_without_drain_abandons_pending_write_events() {
    init_test_tracing();

    // GIVEN: a streaming pipeline with draining disabled whose next
    // write_events response is held, and a second row buffered behind it
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_drain_on_shutdown(false)
    .build();

    let users_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;
    let first_insert_lsn = current_wal_lsn(database.client.as_ref().unwrap()).await;

    hold.wait_reached().await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 2..=2).await;
    let client = database.client.as_ref().unwrap();
    let second_insert_lsn = current_wal_lsn(client).await;

    // The apply loop reports its last received LSN as written, so this proves
    // the second row is buffered behind the held write before shutdown.
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    tokio::time::timeout(DEFAULT_NOTIFY_TIMEOUT, async {
        loop {
            let write_lsn: Option<PgLsn> = client
                .query_opt(
                    "select s.write_lsn from pg_stat_replication s join pg_replication_slots r on \
                     r.active_pid = s.pid where r.slot_name = $1",
                    &[&apply_slot_name],
                )
                .await
                .unwrap()
                .and_then(|row| row.get(0));
            if write_lsn.is_some_and(|write_lsn| write_lsn >= second_insert_lsn) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // WHEN: the pipeline shuts down while the write response is withheld
    let shutdown_task = tokio::spawn(pipeline.shutdown_and_wait());

    // THEN: shutdown reaches destination teardown without waiting on the
    // response, and only stays blocked there because the wrapper's own
    // shutdown drains its held task
    tokio::time::timeout(DEFAULT_NOTIFY_TIMEOUT, async {
        while !destination.shutdown_called().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown_task.is_finished());

    // WHEN: the held response is released
    hold.release_ok();

    // THEN: shutdown completes, the buffered second row was never dispatched,
    // and the slot confirms neither row, so both replay on the next start
    shutdown_task.await.unwrap().unwrap();

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    assert_eq!(grouped_events.get(&(EventType::Insert, table_id)).map_or(0, Vec::len), 1);

    let (confirmed_flush_lsn, _) = replication_slot_state(client, &apply_slot_name).await;
    assert!(confirmed_flush_lsn < first_insert_lsn);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_disconnect_with_write_released_before_reconnect_recovers_without_loss() {
    init_test_tracing();

    // GIVEN: a streaming pipeline whose next write_events response is held
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;

    let store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(store.clone());
    let destination = TestDestinationWrapper::wrap(memory_destination.clone());

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

    let users_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    let hold = destination.hold_next(FaultyOp::WriteEvents).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    hold.wait_reached().await;

    // WHEN: the apply connection dies and the response releases before
    // reconnect
    let client = database.client.as_ref().unwrap();
    let (_, active_pid) = replication_slot_state(client, &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    let first_insert_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    assert!(terminate_walsender(client, old_pid).await.unwrap());

    hold.release_ok();

    first_insert_notify.notified().await;
    let first_commit_lsn = *table_insert_commit_lsns(&destination.get_events().await, table_id)
        .first()
        .expect("released insert should be recorded");

    assert!(
        wait_for_new_walsender(client, &apply_slot_name, old_pid, DEFAULT_NOTIFY_TIMEOUT)
            .await
            .unwrap()
            .is_some()
    );

    // THEN: streaming continues without loss, replaying the insert at most once
    let second_insert_notify = destination
        .notify_on_events(move |events| {
            table_insert_commit_lsns(events, table_id)
                .last()
                .is_some_and(|last| *last > first_commit_lsn)
        })
        .await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 2..=2).await;

    second_insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let final_lsns = table_insert_commit_lsns(&destination.get_events().await, table_id);
    let first_count = final_lsns.iter().filter(|lsn| **lsn == first_commit_lsn).count();
    assert!(
        (1..=2).contains(&first_count),
        "insert must survive with at most one replay, got {first_count} copies"
    );
    assert_eq!(final_lsns.iter().filter(|lsn| **lsn > first_commit_lsn).count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_disconnect_at_randomized_positions_converges_without_loss() {
    init_test_tracing();

    let strategy = walsender_roulette_cases();
    let coverage = WorkloadCoverage::default();
    let property = "walsender roulette";
    run_expensive_property(property, &strategy, |case| {
        block_on(run_walsender_roulette_case(case, &coverage))
    });
    coverage.report(property);
}

/// A held acknowledgement cannot consume the entire process termination grace.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_deadline_abandons_pending_write_without_checkpointing() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = schema.users_schema().id;
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_shutdown_drain_timeout_ms(200)
    .build();
    let copied = store.notify_on_table_sync_complete(table_id).await;
    pipeline.start().await.unwrap();
    copied.notified().await;
    let hold = destination.hold_next(FaultyOp::WriteEvents).await;
    insert_users_data(&mut database, &schema.users_schema().name, 1..=1).await;
    let client = database.client.as_ref().unwrap();
    let insert_lsn = current_wal_lsn(client).await;
    hold.wait_reached().await;

    let started = std::time::Instant::now();
    let shutdown = tokio::spawn(pipeline.shutdown_and_wait());
    let reached_teardown = tokio::time::timeout(Duration::from_secs(5), async {
        while !destination.shutdown_called().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // Always release the test wrapper's own hold, including on test failure.
    // Production destinations must cancel their abandoned writes in shutdown.
    hold.release_ok();
    shutdown.await.unwrap().unwrap();
    reached_teardown.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(200));
    let slot: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let (confirmed, _) = replication_slot_state(client, &slot).await;
    assert!(confirmed < insert_lsn, "abandoned writes must replay on restart");
}
