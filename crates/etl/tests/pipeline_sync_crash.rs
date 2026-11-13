//! Crash properties for the initial table sync.
//!
//! The other crash properties fault only after every table finished syncing.
//! Here the pipeline crashes while a table copy or the following catchup is
//! in flight, with transactions committing during the copy. After a restart
//! the pipeline must recopy or resume as needed and converge to the source:
//! no lost rows, no rows applied twice, and a copy never mixed with events it
//! already contains.

use std::time::Duration;

use etl::{
    pipeline::PipelineId,
    store::{PostgresStore, StateStore, TableStateType},
    test_utils::{
        cdc_workload::{
            AfterFaultStep, DeliveryOrder, TxPhase, WORKLOAD_TIMEOUT, Workload, WorkloadCoverage,
            WorkloadRunner, assert_workload_converged, create_workload_pipeline,
            create_workload_tables, read_source_rows, sync_workloads,
        },
        database::spawn_source_database,
        faults::{FaultyOp, HoldHandle},
        memory_destination::MemoryDestination,
        pipeline::random_pipeline_id,
        property::{block_on, run_expensive_property},
        store::{wait_for_table_state_type, wait_for_table_sync_complete},
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_postgres::tokio::test_utils::PgDatabase;
use etl_telemetry::tracing::init_test_tracing;
use proptest::prelude::*;
use tokio_postgres::Client;

/// How long the catchup crash waits for an event write before crashing anyway.
///
/// Transactions a copy absorbs produce no events, so a case may have nothing
/// to catch up; it then crashes right after the copy instead.
const CATCHUP_WRITE_WAIT: Duration = Duration::from_secs(10);

/// Where the initial sync is when the pipeline crashes.
#[derive(Clone, Copy, Debug)]
enum SyncCrashPoint {
    /// A table copy's write response is held.
    DuringCopy,
    /// The copy finished and an event write after it is held.
    AfterCopy,
}

/// One generated workload and sync crash point.
#[derive(Clone, Debug)]
struct SyncCrashCase {
    /// Source transactions around the crash.
    workload: Workload,
    /// Sync position at crash time.
    crash_point: SyncCrashPoint,
}

/// Generates workloads and sync crash points.
fn sync_crash_cases() -> impl Strategy<Value = SyncCrashCase> {
    let crash_point =
        prop_oneof![Just(SyncCrashPoint::DuringCopy), Just(SyncCrashPoint::AfterCopy)];

    (sync_workloads(), crash_point)
        .prop_map(|(workload, crash_point)| SyncCrashCase { workload, crash_point })
}

/// Waits until no replication slot of the test database has a live walsender.
///
/// The crashed pipeline's apply and copy connections must be gone before the
/// restart reuses their slots.
async fn wait_for_slots_released(client: &Client) -> Result<(), TestCaseError> {
    tokio::time::timeout(WORKLOAD_TIMEOUT, async {
        loop {
            let row = client
                .query_one(
                    "select count(*) from pg_replication_slots where database = \
                     current_database() and active",
                    &[],
                )
                .await
                .expect("failed to read replication slot activity");
            if row.get::<_, i64>(0) == 0 {
                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| TestCaseError::fail("timed out waiting for the crashed pipeline's slots"))
}

/// Waits for a held write to be reached.
async fn wait_for_hold(hold: &HoldHandle, description: &str) -> Result<(), TestCaseError> {
    tokio::time::timeout(WORKLOAD_TIMEOUT, hold.wait_reached())
        .await
        .map_err(|_| TestCaseError::fail(format!("timed out waiting for {description}")))
}

/// Runs one generated workload and sync crash to convergence.
async fn run_sync_crash_case(
    case: &SyncCrashCase,
    coverage: &WorkloadCoverage,
) -> Result<(), TestCaseError> {
    let database: PgDatabase<Client> = spawn_source_database().await;
    let tables = create_workload_tables(&database, &case.workload).await;
    let table_ids = *tables.ids();
    let client = database.client.as_ref().unwrap();

    let pipeline_id: PipelineId = random_pipeline_id().into();
    let first_store = PostgresStore::new(pipeline_id.clone(), database.config.clone())
        .await
        .map_err(|error| {
            TestCaseError::fail(format!("failed to create the first Postgres store: {error}"))
        })?;
    // The destination records its table metadata in the pipeline's store, as
    // real destinations do. A restarted copy finds that metadata and drops the
    // partial table before copying again.
    let memory_destination = MemoryDestination::new(first_store.clone());
    let first_destination = TestDestinationWrapper::wrap(memory_destination.clone());
    let mut first_pipeline = create_workload_pipeline(
        &database.config,
        pipeline_id.clone(),
        first_store.clone(),
        first_destination.clone(),
    );

    // Hold the first copy write, so the crash lands mid-sync.
    let copy_hold = first_destination.hold_next(FaultyOp::WriteTableRows).await;
    first_pipeline
        .start()
        .await
        .map_err(|error| TestCaseError::fail(format!("pipeline failed to start: {error}")))?;
    wait_for_hold(&copy_hold, "the held copy write").await?;

    // These commit after the held copy's snapshot. A later copy may absorb
    // them instead of delivering their events.
    let mut runner = WorkloadRunner::new(client, &tables);
    tokio::time::timeout(WORKLOAD_TIMEOUT, async {
        for tx in case.workload.before_fault() {
            runner.execute(tx, TxPhase::BeforeFault).await?;
        }

        runner.execute(case.workload.fault_tx(), TxPhase::Fault).await
    })
    .await
    .map_err(|_| TestCaseError::fail("timed out running transactions during the copy"))??;

    let event_hold = match case.crash_point {
        SyncCrashPoint::DuringCopy => None,
        SyncCrashPoint::AfterCopy => {
            let event_hold = first_destination.hold_next(FaultyOp::WriteEvents).await;
            copy_hold.release_ok();
            // A case may have no events left to write; it then crashes after
            // the copy without a held event write.
            let _ = tokio::time::timeout(CATCHUP_WRITE_WAIT, event_hold.wait_reached()).await;
            Some(event_hold)
        }
    };

    // Dropping the pipeline aborts its workers at arbitrary await points,
    // like a process crash.
    drop(first_pipeline);
    wait_for_slots_released(client).await?;
    drop(first_destination);
    drop(event_hold);
    drop(first_store);

    let restarted_store =
        PostgresStore::new(pipeline_id.clone(), database.config.clone()).await.map_err(
            |error| TestCaseError::fail(format!("failed to reopen the Postgres store: {error}")),
        )?;
    let restarted_destination = TestDestinationWrapper::wrap(memory_destination.clone());
    let mut restarted_pipeline = create_workload_pipeline(
        &database.config,
        pipeline_id,
        restarted_store.clone(),
        restarted_destination.clone(),
    );
    restarted_pipeline.start().await.map_err(|error| {
        TestCaseError::fail(format!("restarted pipeline failed to start: {error}"))
    })?;

    // Finish every sync before writing more, so later transactions are
    // streamed rather than absorbed by a recopy.
    for table_id in table_ids {
        wait_for_table_sync_complete(&restarted_store, table_id, WORKLOAD_TIMEOUT).await.map_err(
            |error| {
                TestCaseError::fail(format!("table {table_id} did not finish syncing: {error}"))
            },
        )?;
    }

    for step in case.workload.after_fault() {
        let AfterFaultStep::Tx(tx) = step else {
            unreachable!("generated workloads have no straddling transaction");
        };

        runner.execute_and_wait(tx, TxPhase::AfterFault, &restarted_destination).await?;
    }

    let history = runner.into_history();

    // Apply promotes a table to `Ready` only once a change on the current
    // connection materializes its decoder, so only written tables must get
    // there before shutdown.
    let written_after_fault = case.workload.tables_written_after_fault();
    for (table_id, written) in table_ids.into_iter().zip(written_after_fault) {
        if written {
            wait_for_table_state_type(
                &restarted_store,
                table_id,
                TableStateType::Ready,
                WORKLOAD_TIMEOUT,
            )
            .await
            .map_err(|error| {
                TestCaseError::fail(format!("table {table_id} did not become ready: {error}"))
            })?;
        }
    }

    tokio::time::timeout(WORKLOAD_TIMEOUT, restarted_pipeline.shutdown_and_wait())
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for restarted pipeline shutdown"))?
        .map_err(|error| {
            TestCaseError::fail(format!("restarted pipeline shutdown failed: {error}"))
        })?;

    for table_id in table_ids {
        let table_state = restarted_store
            .get_table_state(table_id)
            .await
            .map_err(|error| TestCaseError::fail(format!("failed to read table state: {error}")))?
            .ok_or_else(|| TestCaseError::fail("table state was missing after restart"))?;
        let table_state_type = TableStateType::from(&table_state);
        prop_assert!(
            matches!(table_state_type, TableStateType::SyncDone | TableStateType::Ready),
            "table {} ended in {:?} after restart",
            table_id,
            table_state_type
        );
    }

    // A copy interrupted mid-write must start over from a fresh snapshot.
    if matches!(case.crash_point, SyncCrashPoint::DuringCopy) {
        let mut recopied = false;

        for table_id in table_ids {
            recopied |= restarted_destination.was_table_dropped_for_copy(table_id).await;
        }

        prop_assert!(recopied, "restart did not recopy the table whose copy crashed");
    }

    let source = read_source_rows(client, &tables).await?;
    let events = memory_destination.events().await;
    let copied_rows = memory_destination.table_rows().await;

    assert_workload_converged(
        &tables,
        &copied_rows,
        &events,
        &source,
        &history,
        |phase| match phase {
            // A recopy may absorb these, and a finished table may replay them
            // once after the restart.
            TxPhase::BeforeFault | TxPhase::Fault => 0..=2,
            TxPhase::AfterFault => 1..=1,
        },
        // A table catching up after its copy lags tables already streaming.
        DeliveryOrder::PerTable,
        coverage,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_during_initial_sync_converges() {
    init_test_tracing();

    let strategy = sync_crash_cases();
    let coverage = WorkloadCoverage::default();
    let property = "initial sync crash convergence";
    run_expensive_property(property, &strategy, |case| {
        block_on(run_sync_crash_case(case, &coverage))
    });
    // Copies absorb most changes here, so partial updates are rare; print the
    // counts without the partial-update floor.
    println!("property '{property}' coverage: {coverage:?}");
}
