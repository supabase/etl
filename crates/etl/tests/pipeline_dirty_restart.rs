use std::{collections::BTreeMap, time::Duration};

use etl::{
    data::{Cell, TableRow},
    event::Event,
    pipeline::PipelineId,
    schema::{TableId, TableName},
    store::{PostgresStore, StateStore, TableStateType},
    test_utils::{
        database::{replication_slot_state, spawn_source_database},
        faults::FaultyOp,
        materialize::{FromTableRow, materialize_events},
        memory_destination::MemoryDestination,
        notify::TimedNotify,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        property::{block_on, run_expensive_property},
        store::{wait_for_table_state_type, wait_for_table_sync_complete},
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{TableSelection, insert_users_data, setup_test_database_schema},
    },
};
use etl_postgres::{slots::EtlReplicationSlot, tokio::test_utils::PgDatabase};
use etl_telemetry::tracing::init_test_tracing;
use proptest::prelude::*;
use rand::random;
use tokio_postgres::{Client, types::PgLsn};

/// Maximum time for one dirty-restart synchronization point.
const DIRTY_RESTART_TIMEOUT: Duration = Duration::from_secs(60);

/// Destination wrapper used by the dirty-restart property.
type DirtyRestartDestination = TestDestinationWrapper<MemoryDestination<NotifyingStore>>;

/// Controls the destination response state when the pipeline crashes.
#[derive(Clone, Copy, Debug)]
enum CrashTiming {
    /// Crash after the prefix is acknowledged.
    AfterAcknowledgement,
    /// Crash while the prefix's final write response is held.
    WhileWriteResponseHeld,
}

/// One generated dirty-restart workload and crash schedule.
#[derive(Clone, Copy, Debug)]
struct DirtyRestartCase {
    /// Number of committed single-row transactions.
    transaction_count: usize,
    /// Non-empty proper prefix after which the pipeline crashes.
    crash_after: usize,
    /// Destination response state at crash time.
    crash_timing: CrashTiming,
}

/// Generates bounded workloads and dirty-restart schedules.
fn dirty_restart_cases() -> impl Strategy<Value = DirtyRestartCase> {
    let crash_timing = prop_oneof![
        Just(CrashTiming::AfterAcknowledgement),
        Just(CrashTiming::WhileWriteResponseHeld),
    ];

    (2usize..=8, crash_timing)
        .prop_flat_map(|(transaction_count, crash_timing)| {
            (Just(transaction_count), 1usize..transaction_count, Just(crash_timing))
        })
        .prop_map(|(transaction_count, crash_after, crash_timing)| DirtyRestartCase {
            transaction_count,
            crash_after,
            crash_timing,
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

/// Returns whether the wrapper recorded an insert for `user_id`.
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
    destination: &DirtyRestartDestination,
    table_id: TableId,
    user_id: i64,
) -> TimedNotify {
    destination.notify_on_events(move |events| has_user_insert(events, table_id, user_id)).await
}

/// Waits for one bounded dirty-restart synchronization point.
async fn wait_for_notification(
    notification: &TimedNotify,
    description: impl Into<String>,
) -> Result<(), TestCaseError> {
    let description = description.into();
    tokio::time::timeout(DIRTY_RESTART_TIMEOUT, notification.inner().notified())
        .await
        .map_err(|_| TestCaseError::fail(format!("timed out waiting for {description}")))
}

/// Inserts one autocommit user transaction and waits for its acknowledgement.
async fn insert_user_and_wait(
    database: &mut PgDatabase<Client>,
    users_table_name: &TableName,
    destination: &DirtyRestartDestination,
    table_id: TableId,
    user_number: usize,
) -> Result<(), TestCaseError> {
    let user_id = i64::try_from(user_number).expect("dirty-restart user number should fit in i64");
    let delivered = notify_on_user_insert(destination, table_id, user_id).await;

    insert_users_data(database, users_table_name, user_number..=user_number).await;

    wait_for_notification(&delivered, format!("acknowledgement of user {user_id}")).await
}

/// Waits until the old apply worker releases its replication slot connection.
async fn wait_for_apply_disconnect(
    client: &Client,
    apply_slot_name: &str,
) -> Result<(), TestCaseError> {
    tokio::time::timeout(DIRTY_RESTART_TIMEOUT, async {
        loop {
            let (_, active_pid) = replication_slot_state(client, apply_slot_name).await;
            if active_pid.is_none() {
                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| TestCaseError::fail("timed out waiting for the old apply worker to stop"))
}

/// Waits until the finished table sync worker's replication slot is removed.
///
/// The users table becomes `Ready` while the table sync worker can still be
/// deleting its progress row and replication slot. Slot removal is the last
/// cleanup step, so its absence means table sync work has stopped.
async fn wait_for_sync_slot_removal(
    database: &PgDatabase<Client>,
    sync_slot_name: &str,
) -> Result<(), TestCaseError> {
    tokio::time::timeout(DIRTY_RESTART_TIMEOUT, async {
        loop {
            let slot_state = database
                .get_replication_slot_state(sync_slot_name)
                .await
                .expect("failed to read the table sync slot state");
            if slot_state.is_none() {
                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| TestCaseError::fail("timed out waiting for the table sync slot removal"))
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

/// Compares destination history with committed source state.
fn assert_users_converged(
    events: &[Event],
    table_id: TableId,
    source_users: &BTreeMap<i64, UserRecord>,
    case: DirtyRestartCase,
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
        "every destination insert must contain a complete users row"
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
            return Err(TestCaseError::fail("destination users insert did not contain an int8 ID"));
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
        case.transaction_count,
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
        "destination history contained missing or unexpected user IDs"
    );

    let crash_user_id =
        i64::try_from(case.crash_after).expect("generated crash position should fit in i64");

    for user_id in source_users.keys() {
        let delivery_count =
            delivery_metadata.get(user_id).map_or(0, |(_, delivery_count)| *delivery_count);
        if *user_id > crash_user_id {
            prop_assert_eq!(
                delivery_count,
                1,
                "post-restart user {} had {} deliveries; expected one",
                user_id,
                delivery_count
            );
        } else if *user_id == crash_user_id
            && matches!(case.crash_timing, CrashTiming::WhileWriteResponseHeld)
        {
            prop_assert_eq!(
                delivery_count,
                2,
                "user {} whose write response was held had {} deliveries; expected one replay",
                user_id,
                delivery_count
            );
        } else {
            prop_assert!(
                (1..=2).contains(&delivery_count),
                "user {user_id} had {delivery_count} deliveries; expected one delivery or one \
                 replay"
            );
        }
    }

    Ok(())
}

/// Runs one generated workload and dirty-restart schedule to convergence.
async fn run_dirty_restart_case(case: DirtyRestartCase) -> Result<(), TestCaseError> {
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;

    let destination_store = NotifyingStore::new();
    let memory_destination = MemoryDestination::new(destination_store);
    let first_destination = TestDestinationWrapper::wrap(memory_destination.clone());
    let pipeline_id: PipelineId = u64::from(random::<u32>());
    let first_store =
        PostgresStore::new(pipeline_id, database.config.clone()).await.map_err(|error| {
            TestCaseError::fail(format!("failed to create the first Postgres store: {error}"))
        })?;
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let sync_slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into().unwrap();
    let mut first_pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        first_store.clone(),
        first_destination.clone(),
    );

    first_pipeline
        .start()
        .await
        .map_err(|error| TestCaseError::fail(format!("pipeline failed to start: {error}")))?;
    wait_for_table_sync_complete(&first_store, table_id, DIRTY_RESTART_TIMEOUT).await.map_err(
        |error| TestCaseError::fail(format!("failed to wait for table sync completion: {error}")),
    )?;

    // Table sync completes before the worker deletes its progress row and
    // replication slot. Wait for slot removal so the crash below hits a state
    // where only the apply worker serves the users table.
    wait_for_sync_slot_removal(&database, &sync_slot_name).await?;

    for user_number in 1..case.crash_after {
        insert_user_and_wait(
            &mut database,
            &users_schema.name,
            &first_destination,
            table_id,
            user_number,
        )
        .await?;
    }

    let held_response = match case.crash_timing {
        CrashTiming::AfterAcknowledgement => {
            insert_user_and_wait(
                &mut database,
                &users_schema.name,
                &first_destination,
                table_id,
                case.crash_after,
            )
            .await?;
            None
        }
        CrashTiming::WhileWriteResponseHeld => {
            let hold = first_destination.hold_next(FaultyOp::WriteEvents).await;
            insert_users_data(
                &mut database,
                &users_schema.name,
                case.crash_after..=case.crash_after,
            )
            .await;
            tokio::time::timeout(DIRTY_RESTART_TIMEOUT, hold.wait_reached())
                .await
                .map_err(|_| TestCaseError::fail("timed out waiting for held write response"))?;
            Some(hold)
        }
    };

    // Dropping the pipeline simulates a crash instead of a graceful shutdown:
    // dropping `ApplyWorkerHandle` aborts the apply worker task at an arbitrary
    // await point, without shutdown handling, stream draining, or a final
    // status update. Auxiliary tasks stop on the closed shutdown channel. The
    // walsender poll below is the restart barrier: it proves the aborted
    // worker's replication connection is gone.
    drop(first_pipeline);
    wait_for_apply_disconnect(database.client.as_ref().unwrap(), &apply_slot_name).await?;
    drop(first_destination);
    drop(held_response);
    drop(first_store);

    let restarted_store =
        PostgresStore::new(pipeline_id, database.config.clone()).await.map_err(|error| {
            TestCaseError::fail(format!("failed to reopen the Postgres store: {error}"))
        })?;
    let restarted_destination = TestDestinationWrapper::wrap(memory_destination.clone());
    let mut restarted_pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        restarted_store.clone(),
        restarted_destination.clone(),
    );
    restarted_pipeline.start().await.map_err(|error| {
        TestCaseError::fail(format!("restarted pipeline failed to start: {error}"))
    })?;

    for user_number in (case.crash_after + 1)..=case.transaction_count {
        insert_user_and_wait(
            &mut database,
            &users_schema.name,
            &restarted_destination,
            table_id,
            user_number,
        )
        .await?;
    }

    // Destination event recording does not guarantee that apply-side response
    // processing has finished. Wait for the quiescent pass to promote the table
    // before requesting shutdown.
    wait_for_table_state_type(
        &restarted_store,
        table_id,
        TableStateType::Ready,
        DIRTY_RESTART_TIMEOUT,
    )
    .await
    .map_err(|error| {
        TestCaseError::fail(format!("failed to wait for users table readiness: {error}"))
    })?;

    tokio::time::timeout(DIRTY_RESTART_TIMEOUT, restarted_pipeline.shutdown_and_wait())
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for restarted pipeline shutdown"))?
        .map_err(|error| {
            TestCaseError::fail(format!("restarted pipeline shutdown failed: {error}"))
        })?;

    let table_state = restarted_store
        .get_table_state(table_id)
        .await
        .map_err(|error| TestCaseError::fail(format!("failed to read table state: {error}")))?
        .ok_or_else(|| TestCaseError::fail("users table state was missing after restart"))?;
    prop_assert_eq!(
        TableStateType::from(&table_state),
        TableStateType::Ready,
        "users table did not remain ready after restart"
    );
    prop_assert_eq!(
        restarted_destination.write_table_rows_called().await,
        0,
        "restart unexpectedly recopied the users table"
    );
    prop_assert!(
        !restarted_destination.was_table_dropped_for_copy(table_id).await,
        "restart unexpectedly dropped the users table for copy"
    );

    let source_users =
        read_source_users(database.client.as_ref().unwrap(), &users_schema.name).await?;
    let events = memory_destination.events().await;

    assert_users_converged(&events, table_id, &source_users, case)
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_store_dirty_restart_at_randomized_positions_converges_without_recopy() {
    init_test_tracing();

    let strategy = dirty_restart_cases();
    run_expensive_property("Postgres store dirty restart convergence", &strategy, |case| {
        block_on(run_dirty_restart_case(*case))
    });
}
