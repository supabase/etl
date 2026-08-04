use std::{collections::BTreeMap, time::Duration};

use etl::{
    data::{Cell, TableRow},
    error::ErrorKind,
    event::{Event, EventType},
    pipeline::PipelineId,
    schema::{TableId, TableName},
    store::{StateStore, TableRetryPolicy, TableState, TableStateType},
    test_utils::{
        database::{replication_slot_state, spawn_source_database, wait_for_new_walsender},
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::{FaultAction, FaultyOp},
        materialize::{FromTableRow, materialize_events},
        memory_destination::MemoryDestination,
        notify::TimedNotify,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
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

/// Maximum time for one roulette synchronization point.
const ROULETTE_TIMEOUT: Duration = Duration::from_secs(60);

/// Destination wrapper used by the walsender roulette.
type RouletteDestination = TestDestinationWrapper<MemoryDestination<NotifyingStore>>;

/// Controls write-response handling around the walsender disconnect.
#[derive(Clone, Copy, Debug)]
enum WriteResponseTiming {
    /// Disconnect after the prefix is acknowledged.
    Unheld,
    /// Release the in-flight response, then wait for the reconnect.
    ReleaseThenWaitForReconnect,
    /// Wait for the reconnect, then release the in-flight response.
    WaitForReconnectThenRelease,
}

/// One generated walsender roulette schedule.
#[derive(Clone, Copy, Debug)]
struct WalsenderRouletteCase {
    /// Number of committed single-row transactions.
    transaction_count: usize,
    /// Non-empty proper prefix after which the walsender disconnects.
    disconnect_after: usize,
    /// Write-response schedule around the disconnect.
    response_timing: WriteResponseTiming,
}

/// Generates bounded workloads and disconnect schedules.
fn walsender_roulette_cases() -> impl Strategy<Value = WalsenderRouletteCase> {
    let response_timing = prop_oneof![
        Just(WriteResponseTiming::Unheld),
        Just(WriteResponseTiming::ReleaseThenWaitForReconnect),
        Just(WriteResponseTiming::WaitForReconnectThenRelease),
    ];

    (2usize..=8, response_timing)
        .prop_flat_map(|(transaction_count, response_timing)| {
            (Just(transaction_count), 1usize..transaction_count, Just(response_timing))
        })
        .prop_map(|(transaction_count, disconnect_after, response_timing)| WalsenderRouletteCase {
            transaction_count,
            disconnect_after,
            response_timing,
        })
}

/// Typed state for one row in the users table.
#[derive(Clone, Debug, Eq, PartialEq)]
struct UserRecord {
    /// Source primary key.
    id: i64,
    /// Source user name.
    name: String,
    /// Source user age.
    age: i32,
}

impl FromTableRow for UserRecord {
    type Id = i64;

    fn from_table_row(table_row: &TableRow) -> Option<Self> {
        let [Cell::I64(id), Cell::String(name), Cell::I32(age)] = table_row.values() else {
            return None;
        };

        Some(Self { id: *id, name: name.clone(), age: *age })
    }

    fn id(&self) -> Self::Id {
        self.id
    }
}

/// Returns whether the event history contains an insert for `user_id`.
fn has_user_insert(events: &[Event], table_id: TableId, user_id: i64) -> bool {
    events.iter().any(|event| {
        let Event::Insert(insert) = event else {
            return false;
        };

        insert.replicated_table_schema.id() == table_id
            && matches!(
                insert.table_row.values().first(),
                Some(Cell::I64(recorded_id)) if *recorded_id == user_id
            )
    })
}

/// Registers a notification for one user insert.
async fn notify_on_user_insert(
    destination: &RouletteDestination,
    table_id: TableId,
    user_id: i64,
) -> TimedNotify {
    destination.notify_on_events(move |events| has_user_insert(events, table_id, user_id)).await
}

/// Waits for one bounded roulette synchronization point.
async fn wait_for_notification(
    notification: &TimedNotify,
    description: impl Into<String>,
) -> Result<(), TestCaseError> {
    let description = description.into();
    tokio::time::timeout(ROULETTE_TIMEOUT, notification.inner().notified())
        .await
        .map_err(|_| TestCaseError::fail(format!("timed out waiting for {description}")))
}

/// Terminates the active apply walsender and returns its PID.
async fn terminate_apply_walsender(
    client: &Client,
    apply_slot_name: &str,
) -> Result<i32, TestCaseError> {
    let (_, active_pid) = replication_slot_state(client, apply_slot_name).await;
    let old_pid = active_pid
        .ok_or_else(|| TestCaseError::fail("apply walsender was not active at disconnect"))?;
    let row = client
        .query_one("select pg_terminate_backend($1)", &[&old_pid])
        .await
        .map_err(|error| TestCaseError::fail(format!("failed to terminate walsender: {error}")))?;
    let terminated: bool = row.get(0);

    prop_assert!(terminated, "Postgres did not terminate apply walsender {old_pid}");

    Ok(old_pid)
}

/// Waits for a new apply walsender within the roulette timeout.
async fn wait_for_apply_reconnect(
    client: &Client,
    apply_slot_name: &str,
    old_pid: i32,
) -> Result<(), TestCaseError> {
    tokio::time::timeout(ROULETTE_TIMEOUT, wait_for_new_walsender(client, apply_slot_name, old_pid))
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for the apply walsender to reconnect"))
}

/// Mutable state used to execute one walsender roulette workload.
struct WalsenderRouletteWorkload<'a> {
    /// Source database receiving generated transactions.
    database: &'a mut PgDatabase<Client>,
    /// Qualified users table name.
    users_table_name: &'a TableName,
    /// Destination recording replicated events.
    destination: &'a RouletteDestination,
    /// Users table identifier.
    table_id: TableId,
    /// Apply replication slot name.
    apply_slot_name: &'a str,
}

impl<'a> WalsenderRouletteWorkload<'a> {
    /// Executes the generated transactions and disconnect schedule.
    async fn run(
        &mut self,
        case: WalsenderRouletteCase,
        users_ready: &TimedNotify,
    ) -> Result<(), TestCaseError> {
        wait_for_notification(users_ready, "users table to become ready").await?;

        for user_number in 1..case.disconnect_after {
            self.insert_user(user_number).await?;
        }

        self.disconnect(case.disconnect_after, case.response_timing).await?;

        for user_number in (case.disconnect_after + 1)..=case.transaction_count {
            self.insert_user(user_number).await?;
        }

        Ok(())
    }

    /// Inserts one autocommit user transaction and waits for its delivery.
    async fn insert_user(&mut self, user_number: usize) -> Result<(), TestCaseError> {
        let user_id = i64::try_from(user_number).expect("roulette user number should fit in i64");
        let delivered = notify_on_user_insert(self.destination, self.table_id, user_id).await;

        insert_users_data(self.database, self.users_table_name, user_number..=user_number).await;

        wait_for_notification(&delivered, format!("delivery of user {user_id}")).await
    }

    /// Executes the generated disconnect schedule.
    async fn disconnect(
        &mut self,
        user_number: usize,
        response_timing: WriteResponseTiming,
    ) -> Result<(), TestCaseError> {
        match response_timing {
            WriteResponseTiming::Unheld => self.disconnect_after_delivered_write(user_number).await,
            WriteResponseTiming::ReleaseThenWaitForReconnect
            | WriteResponseTiming::WaitForReconnectThenRelease => {
                self.disconnect_with_held_write(user_number, response_timing).await
            }
        }
    }

    /// Disconnects after the scheduled write reaches the destination.
    async fn disconnect_after_delivered_write(
        &mut self,
        user_number: usize,
    ) -> Result<(), TestCaseError> {
        self.insert_user(user_number).await?;

        let client = self.database.client.as_ref().unwrap();
        let old_pid = terminate_apply_walsender(client, self.apply_slot_name).await?;

        wait_for_apply_reconnect(client, self.apply_slot_name, old_pid).await
    }

    /// Disconnects while the scheduled write response is held.
    async fn disconnect_with_held_write(
        &mut self,
        user_number: usize,
        response_timing: WriteResponseTiming,
    ) -> Result<(), TestCaseError> {
        let user_id = i64::try_from(user_number).expect("roulette user number should fit in i64");
        let delivered = notify_on_user_insert(self.destination, self.table_id, user_id).await;
        let hold = self.destination.hold_next(FaultyOp::WriteEvents).await;

        insert_users_data(self.database, self.users_table_name, user_number..=user_number).await;
        tokio::time::timeout(ROULETTE_TIMEOUT, hold.wait_reached())
            .await
            .map_err(|_| TestCaseError::fail("timed out waiting for held write response"))?;

        let client = self.database.client.as_ref().unwrap();
        let old_pid = terminate_apply_walsender(client, self.apply_slot_name).await?;

        match response_timing {
            WriteResponseTiming::ReleaseThenWaitForReconnect => {
                hold.release_ok();
                wait_for_notification(&delivered, format!("delivery of held user {user_id}"))
                    .await?;
                wait_for_apply_reconnect(client, self.apply_slot_name, old_pid).await
            }
            WriteResponseTiming::WaitForReconnectThenRelease => {
                wait_for_apply_reconnect(client, self.apply_slot_name, old_pid).await?;
                hold.release_ok();
                wait_for_notification(&delivered, format!("delivery of held user {user_id}")).await
            }
            WriteResponseTiming::Unheld => {
                unreachable!("held write disconnect requires a held response timing")
            }
        }
    }
}

/// Reads the committed users from the source table.
async fn read_source_users(
    client: &Client,
    users_table_name: &TableName,
) -> Result<BTreeMap<i64, UserRecord>, TestCaseError> {
    let query = format!(
        "select id, name, age from {} order by id",
        users_table_name.as_quoted_identifier()
    );
    let rows = client
        .query(&query, &[])
        .await
        .map_err(|error| TestCaseError::fail(format!("failed to read source users: {error}")))?;
    let mut users = BTreeMap::new();

    for row in rows {
        let user = UserRecord { id: row.get(0), name: row.get(1), age: row.get(2) };
        let previous = users.insert(user.id, user);
        prop_assert!(previous.is_none(), "source returned a duplicate user ID");
    }

    Ok(users)
}

/// Compares acknowledged destination history with committed source state.
fn assert_users_converged(
    events: &[Event],
    table_id: TableId,
    source_users: &BTreeMap<i64, UserRecord>,
    transaction_count: usize,
) -> Result<(), TestCaseError> {
    let materialized_users = materialize_events::<UserRecord>(events, Some(table_id));
    let table_insert_count = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                Event::Insert(insert) if insert.replicated_table_schema.id() == table_id
            )
        })
        .count();

    prop_assert_eq!(
        materialized_users.len(),
        table_insert_count,
        "every recorded insert must contain a complete users row"
    );

    let mut destination_users = BTreeMap::new();
    let mut delivery_metadata = BTreeMap::<i64, (PgLsn, usize)>::new();

    for event in events {
        let Event::Insert(insert) = event else {
            continue;
        };
        if insert.replicated_table_schema.id() != table_id {
            continue;
        }

        let Some(Cell::I64(user_id)) = insert.table_row.values().first() else {
            return Err(TestCaseError::fail("recorded users insert did not contain an int8 ID"));
        };

        if let Some((first_commit_lsn, delivery_count)) = delivery_metadata.get_mut(user_id) {
            prop_assert_eq!(
                *first_commit_lsn,
                insert.commit_lsn,
                "replayed insert for user {} changed its commit LSN",
                user_id
            );
            *delivery_count += 1;
        } else {
            delivery_metadata.insert(*user_id, (insert.commit_lsn, 1));
        }
    }

    for user in materialized_users {
        if let Some(previous) = destination_users.get(&user.id) {
            prop_assert_eq!(
                previous,
                &user,
                "replayed insert for user {} changed its payload",
                user.id
            );
        } else {
            destination_users.insert(user.id, user);
        }
    }

    prop_assert_eq!(
        source_users.len(),
        transaction_count,
        "source did not contain every committed transaction"
    );
    prop_assert_eq!(
        &destination_users,
        source_users,
        "materialized destination state did not converge to source state"
    );
    prop_assert_eq!(
        delivery_metadata.len(),
        source_users.len(),
        "event history contained missing or unexpected user IDs"
    );

    for user_id in source_users.keys() {
        let delivery_count =
            delivery_metadata.get(user_id).map_or(0, |(_, delivery_count)| *delivery_count);
        prop_assert!(
            (1..=2).contains(&delivery_count),
            "user {user_id} had {delivery_count} deliveries; expected one delivery or one replay"
        );
    }

    Ok(())
}

/// Runs one generated workload and fault schedule to convergence.
async fn run_walsender_roulette_case(case: WalsenderRouletteCase) -> Result<(), TestCaseError> {
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
    let users_ready = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline
        .start()
        .await
        .map_err(|error| TestCaseError::fail(format!("pipeline failed to start: {error}")))?;
    let workload_result = {
        let mut workload = WalsenderRouletteWorkload {
            database: &mut database,
            users_table_name: &users_schema.name,
            destination: &destination,
            table_id,
            apply_slot_name: &apply_slot_name,
        };
        workload.run(case, &users_ready).await
    };

    let shutdown_result = tokio::time::timeout(ROULETTE_TIMEOUT, pipeline.shutdown_and_wait())
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for pipeline shutdown"))
        .and_then(|result| {
            result
                .map_err(|error| TestCaseError::fail(format!("pipeline shutdown failed: {error}")))
        });

    workload_result?;
    shutdown_result?;

    let source_users =
        read_source_users(database.client.as_ref().unwrap(), &users_schema.name).await?;
    let events = destination.get_events().await;

    assert_users_converged(&events, table_id, &source_users, case.transaction_count)
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
    // Relation must resolve the pre-DDL schema instead of reusing the
    // post-DDL runtime schema cached by the failed attempt.
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
    wait_for_new_walsender(database.client.as_ref().unwrap(), &apply_slot_name, old_pid).await;
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
async fn apply_disconnect_with_write_held_until_after_reconnect_replays_without_loss() {
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

    // WHEN: the apply connection dies while the write response is withheld
    let client = database.client.as_ref().unwrap();
    let (flush_lsn_at_kill, active_pid) = replication_slot_state(client, &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    client.query_one("select pg_terminate_backend($1)", &[&old_pid]).await.unwrap();

    wait_for_new_walsender(client, &apply_slot_name, old_pid).await;

    // WHEN: the held response is released only after the reconnect
    let replay_recorded_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 2)])
        .await;

    hold.release_ok();

    // THEN: the insert replays because the acknowledgement never reached the
    // old apply loop.
    replay_recorded_notify.notified().await;

    let commit_lsns = table_insert_commit_lsns(&destination.get_events().await, table_id);
    assert_eq!(commit_lsns.len(), 2);
    assert_eq!(commit_lsns[0], commit_lsns[1]);
    let first_commit_lsn = commit_lsns[0];

    // THEN: the persisted checkpoint never advanced past the unacknowledged write.
    assert!(flush_lsn_at_kill < first_commit_lsn);

    // THEN: streaming continues without loss after the replay.
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
    assert_eq!(final_lsns.iter().filter(|lsn| **lsn == first_commit_lsn).count(), 2);
    assert_eq!(final_lsns.iter().filter(|lsn| **lsn > first_commit_lsn).count(), 1);
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

    // WHEN: the apply connection dies and the response releases before reconnect
    let client = database.client.as_ref().unwrap();
    let (_, active_pid) = replication_slot_state(client, &apply_slot_name).await;
    let old_pid = active_pid.expect("apply walsender should be active");

    let first_insert_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;

    client.query_one("select pg_terminate_backend($1)", &[&old_pid]).await.unwrap();

    hold.release_ok();

    first_insert_notify.notified().await;
    let first_commit_lsn = *table_insert_commit_lsns(&destination.get_events().await, table_id)
        .first()
        .expect("released insert should be recorded");

    wait_for_new_walsender(client, &apply_slot_name, old_pid).await;

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
    run_expensive_property("walsender roulette", &strategy, |case| {
        block_on(run_walsender_roulette_case(*case))
    });
}
