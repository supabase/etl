use std::time::Duration;

use etl::{
    pipeline::PipelineId,
    store::{PostgresStore, StateStore, TableStateType},
    test_utils::{
        database::{replication_slot_state, spawn_source_database},
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::create_pipeline,
        property::{block_on, run_expensive_property},
        store::{wait_for_table_state_type, wait_for_table_sync_complete},
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_postgres::{slots::EtlReplicationSlot, tokio::test_utils::PgDatabase};
use etl_telemetry::tracing::init_test_tracing;
use proptest::prelude::*;
use rand::random;
use tokio_postgres::Client;

use crate::support::cdc_workload::{
    AfterFaultStep, TxPhase, WORKLOAD_PUBLICATION, WORKLOAD_TIMEOUT, Workload, WorkloadCoverage,
    WorkloadRunner, assert_phase_delivered_once, assert_workload_converged, create_workload_tables,
    notify_on_commit, read_source_rows, wait_for_notification, workloads,
};

/// Controls the destination response state when the pipeline crashes.
#[derive(Clone, Copy, Debug)]
enum CrashTiming {
    /// Crash after the fault transaction is acknowledged.
    AfterAcknowledgement,
    /// Crash while the fault transaction's write response is held.
    WhileWriteResponseHeld,
}

/// One generated dirty-restart workload and crash schedule.
#[derive(Clone, Debug)]
struct DirtyRestartCase {
    /// Source transactions around the crash.
    workload: Workload,
    /// Destination response state at crash time.
    crash_timing: CrashTiming,
}

/// Generates workloads and dirty-restart schedules.
fn dirty_restart_cases() -> impl Strategy<Value = DirtyRestartCase> {
    let crash_timing = prop_oneof![
        Just(CrashTiming::AfterAcknowledgement),
        Just(CrashTiming::WhileWriteResponseHeld),
    ];

    (workloads(), crash_timing)
        .prop_map(|(workload, crash_timing)| DirtyRestartCase { workload, crash_timing })
}

/// Waits until the old apply worker releases its replication slot connection.
async fn wait_for_apply_disconnect(
    client: &Client,
    apply_slot_name: &str,
) -> Result<(), TestCaseError> {
    tokio::time::timeout(WORKLOAD_TIMEOUT, async {
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
/// Slot cleanup is allowed at [`TableStateType::SyncDone`] or
/// [`TableStateType::Ready`]. Its absence means table-sync work has stopped,
/// not that apply has already marked the table ready.
async fn wait_for_sync_slot_removal(
    database: &PgDatabase<Client>,
    sync_slot_name: &str,
) -> Result<(), TestCaseError> {
    tokio::time::timeout(WORKLOAD_TIMEOUT, async {
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

/// Runs one generated workload and dirty-restart schedule to convergence.
async fn run_dirty_restart_case(
    case: &DirtyRestartCase,
    coverage: &WorkloadCoverage,
) -> Result<(), TestCaseError> {
    let database = spawn_source_database().await;
    let tables = create_workload_tables(&database, &case.workload).await;
    let table_ids = *tables.ids();

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
    let mut first_pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        WORKLOAD_PUBLICATION.to_owned(),
        first_store.clone(),
        first_destination.clone(),
    );

    first_pipeline
        .start()
        .await
        .map_err(|error| TestCaseError::fail(format!("pipeline failed to start: {error}")))?;
    for table_id in table_ids {
        wait_for_table_sync_complete(&first_store, table_id, WORKLOAD_TIMEOUT).await.map_err(
            |error| {
                TestCaseError::fail(format!("failed to wait for table sync completion: {error}"))
            },
        )?;

        // Table sync completes at `SyncDone` before the worker deletes its
        // slot. Wait for slot removal so the crash cannot race a still-running
        // copy worker. Apply may still promote the table to `Ready` later.
        let sync_slot_name: String =
            EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into().unwrap();
        wait_for_sync_slot_removal(&database, &sync_slot_name).await?;
    }

    let client = database.client.as_ref().unwrap();
    let mut runner = WorkloadRunner::new(client, &tables);
    for tx in case.workload.before_fault() {
        runner.execute_and_wait(tx, TxPhase::BeforeFault, &first_destination).await?;
    }

    let mut straddle = match case.workload.straddle() {
        Some(inserts) => Some(runner.open_straddle(&database.config, inserts).await?),
        None => None,
    };

    let held_response = match case.crash_timing {
        CrashTiming::AfterAcknowledgement => {
            runner
                .execute_and_wait(case.workload.fault_tx(), TxPhase::Fault, &first_destination)
                .await?;
            None
        }
        CrashTiming::WhileWriteResponseHeld => {
            let hold = first_destination.hold_next(FaultyOp::WriteEvents).await;
            runner.execute(case.workload.fault_tx(), TxPhase::Fault, None).await?;
            tokio::time::timeout(WORKLOAD_TIMEOUT, hold.wait_reached())
                .await
                .map_err(|_| TestCaseError::fail("timed out waiting for held write response"))?;
            Some(hold)
        }
    };

    // Dropping the pipeline simulates a crash instead of a graceful shutdown:
    // dropping `ApplyWorkerHandle` aborts the apply worker task at an arbitrary
    // await point, without shutdown handling, stream draining, or a final
    // status update. The walsender poll below is the restart barrier: it proves
    // the aborted worker's replication connection is gone.
    drop(first_pipeline);
    wait_for_apply_disconnect(client, &apply_slot_name).await?;
    let pre_restart_event_count = memory_destination.events().await.len();
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
        WORKLOAD_PUBLICATION.to_owned(),
        restarted_store.clone(),
        restarted_destination.clone(),
    );

    // The held fault transaction was never acknowledged, so the restarted
    // pipeline must replay it. Wait for that replay explicitly: later steps
    // may all roll back and give the restart nothing else to stream.
    let fault_replayed = match case.crash_timing {
        CrashTiming::WhileWriteResponseHeld => {
            let fault_xid = runner.fault_xid().expect("the fault transaction always commits");
            Some(notify_on_commit(&restarted_destination, fault_xid).await)
        }
        CrashTiming::AfterAcknowledgement => None,
    };

    restarted_pipeline.start().await.map_err(|error| {
        TestCaseError::fail(format!("restarted pipeline failed to start: {error}"))
    })?;

    if let Some(fault_replayed) = &fault_replayed {
        wait_for_notification(fault_replayed, "replay of the held fault transaction").await?;
    }

    for step in case.workload.after_fault() {
        match step {
            AfterFaultStep::Tx(tx) => {
                runner.execute_and_wait(tx, TxPhase::AfterFault, &restarted_destination).await?;
            }
            AfterFaultStep::CommitStraddle => {
                let session = straddle.take().expect("planned straddle should be open");
                runner.commit_straddle(session, &restarted_destination).await?;
            }
        }
    }
    let history = runner.into_history();

    // Apply promotes a table to `Ready` only once a relation or change on the
    // restarted connection materializes its decoder; a table with no
    // post-restart changes may correctly stay in `SyncDone`. Destination
    // event recording also does not guarantee that apply-side response
    // processing has finished, so wait for the quiescent pass to promote the
    // written tables before requesting shutdown.
    let written_after_fault = case.workload.tables_written_after_fault();
    for (table_id, written) in table_ids.into_iter().zip(written_after_fault) {
        if !written {
            continue;
        }
        wait_for_table_state_type(
            &restarted_store,
            table_id,
            TableStateType::Ready,
            WORKLOAD_TIMEOUT,
        )
        .await
        .map_err(|error| {
            TestCaseError::fail(format!("failed to wait for table {table_id} readiness: {error}"))
        })?;
    }

    tokio::time::timeout(WORKLOAD_TIMEOUT, restarted_pipeline.shutdown_and_wait())
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for restarted pipeline shutdown"))?
        .map_err(|error| {
            TestCaseError::fail(format!("restarted pipeline shutdown failed: {error}"))
        })?;

    for (table_id, written) in table_ids.into_iter().zip(written_after_fault) {
        let table_state = restarted_store
            .get_table_state(table_id)
            .await
            .map_err(|error| TestCaseError::fail(format!("failed to read table state: {error}")))?
            .ok_or_else(|| TestCaseError::fail("table state was missing after restart"))?;
        let table_state_type = TableStateType::from(&table_state);
        let expected = if written {
            matches!(table_state_type, TableStateType::Ready)
        } else {
            matches!(table_state_type, TableStateType::SyncDone | TableStateType::Ready)
        };
        prop_assert!(
            expected,
            "table {} ended in {:?} after restart (written after fault: {})",
            table_id,
            table_state_type,
            written
        );
        prop_assert!(
            !restarted_destination.was_table_dropped_for_copy(table_id).await,
            "restart unexpectedly dropped table {} for copy",
            table_id
        );
    }
    prop_assert_eq!(
        restarted_destination.write_table_rows_called().await,
        0,
        "restart unexpectedly recopied a table"
    );

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
            TxPhase::BeforeFault | TxPhase::Fault => 1..=2,
            TxPhase::AfterFault => 1..=1,
        },
        coverage,
    )?;

    // The held transaction was never acknowledged, so the restart must replay
    // all of it exactly once, however its events were split across writes.
    if matches!(case.crash_timing, CrashTiming::WhileWriteResponseHeld) {
        assert_phase_delivered_once(&events[pre_restart_event_count..], &history, TxPhase::Fault)?;
        coverage.record_held_replay();
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_store_dirty_restart_at_randomized_positions_converges_without_recopy() {
    init_test_tracing();

    let strategy = dirty_restart_cases();
    let coverage = WorkloadCoverage::default();
    let property = "Postgres store dirty restart convergence";
    run_expensive_property(property, &strategy, |case| {
        block_on(run_dirty_restart_case(case, &coverage))
    });
    coverage.report(property);
}
