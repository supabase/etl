//! Crash-and-replay property shared by every real destination.
//!
//! Each case runs a generated CDC workload from
//! [`etl::test_utils::cdc_workload`] through a pipeline into a real
//! destination and injects one fault:
//!
//! - a crash around the fault transaction: the pipeline is dropped without a
//!   graceful shutdown, then restarted with a reopened store and a fresh
//!   destination instance over the same destination storage;
//! - a lost acknowledgement of the first copy write or of the fault
//!   transaction's streaming write: the destination applies the write, but the
//!   pipeline observes a timeout;
//! - a rejection of either write: the destination refuses it.
//!
//! A failed streaming write is replayed from the apply loop's durable
//! position. A failed copy write errors the table, and its retry drops the
//! table and copies it again.
//!
//! After the run, each table's current state in the destination must equal
//! the committed source rows, with every key visible at most once. Where a
//! destination exposes its stored change log, a copied row may be stored only
//! once, and a replayed change may be stored again only as an identical copy,
//! since the destination's current-state rule cannot tell such copies apart.
//! Delivery to the destination is also checked with
//! [`assert_workload_converged`], so a failure there points at the pipeline
//! and a failure only in the destination state points at the destination.

use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeInclusive,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use etl::{
    data::TableRow,
    destination::{Destination, PipelineDestination},
    error::ErrorKind,
    event::Event,
    pipeline::{Pipeline, PipelineId},
    schema::{TableId, TableName},
    store::{PostgresStore, StateStore, TableStateType},
    test_utils::{
        cdc_workload::{
            AfterFaultStep, DeliveryOrder, TxPhase, WORKLOAD_PUBLICATION, WORKLOAD_TIMEOUT,
            Workload, WorkloadCoverage, WorkloadRecord, WorkloadRunner, WorkloadShape,
            WorkloadTables, assert_workload_converged, create_workload_tables, has_committed_xid,
            notify_on_commit, read_source_rows, wait_for_apply_disconnect, wait_for_notification,
            wait_for_sync_slot_removal, workloads,
        },
        database::spawn_source_database,
        faults::{FaultAction, FaultyOp},
        materialize::FromTableRow,
        pipeline::{create_pipeline, random_pipeline_id},
        property::{block_on, run_expensive_property},
        store::{wait_for_table_state_type, wait_for_table_sync_complete},
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_postgres::{slots::EtlReplicationSlot, tokio::test_utils::PgDatabase};
use proptest::prelude::*;
use tokio_postgres::Client;

/// Longest delay between committing the fault transaction and a
/// [`CrashTiming::DuringDelivery`] crash.
const MAX_DURING_DELIVERY_CRASH_DELAY_MS: u64 = 200;

/// Pause between reads of a destination whose state settles asynchronously.
const SETTLE_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Moment a crash hits relative to the fault transaction's delivery.
#[derive(Clone, Copy, Debug)]
enum CrashTiming {
    /// The destination acknowledged the fault transaction.
    AfterAcknowledgement,
    /// The destination applied the fault transaction, but its acknowledgement
    /// is held, so the restarted pipeline must replay it.
    WhileAcknowledgementHeld,
    /// A fixed delay after the fault transaction commits, without waiting on
    /// the destination: its write may not have started, be in flight, or be
    /// done.
    DuringDelivery {
        /// Delay after the commit in milliseconds.
        delay_ms: u64,
    },
}

/// Destination write a lost acknowledgement or rejection targets.
#[derive(Clone, Copy, Debug)]
enum FaultedWrite {
    /// The first initial-copy write. A failed copy write errors the table, and
    /// its retry drops the table and copies it again under a new attempt.
    Copy,
    /// The streaming write that carries the fault transaction. The apply loop
    /// reconnects and replays from its durable position.
    Stream,
}

/// Fault injected into one case.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// The pipeline crashes and restarts around the fault transaction.
    Crash(CrashTiming),
    /// The destination applies the write, then reports a timeout, so the write
    /// succeeds but its confirmation is lost.
    LostAcknowledgement(FaultedWrite),
    /// The destination refuses the write with a timeout before doing any work.
    RejectedWrite(FaultedWrite),
}

impl Fault {
    /// Returns the write the fault targets, when it is a scripted response.
    fn scripted_write(self) -> Option<(FaultedWrite, FaultAction)> {
        match self {
            Self::Crash(_) => None,
            Self::LostAcknowledgement(write) => Some((
                write,
                FaultAction::fail_after_write(
                    ErrorKind::DestinationTimeout,
                    "injected lost acknowledgement",
                ),
            )),
            Self::RejectedWrite(write) => Some((
                write,
                FaultAction::reject(ErrorKind::DestinationTimeout, "injected rejected write"),
            )),
        }
    }
}

/// One generated workload and fault.
#[derive(Clone, Debug)]
struct CrashReplayCase {
    /// Source transactions around the fault.
    workload: Workload,
    /// Fault injected into the case.
    fault: Fault,
}

/// Generates faults, weighted toward the replay of applied writes.
fn faults() -> impl Strategy<Value = Fault> {
    prop_oneof![
        1 => Just(Fault::Crash(CrashTiming::AfterAcknowledgement)),
        2 => Just(Fault::Crash(CrashTiming::WhileAcknowledgementHeld)),
        1 => (0..=MAX_DURING_DELIVERY_CRASH_DELAY_MS)
            .prop_map(|delay_ms| Fault::Crash(CrashTiming::DuringDelivery { delay_ms })),
        2 => Just(Fault::LostAcknowledgement(FaultedWrite::Stream)),
        1 => Just(Fault::RejectedWrite(FaultedWrite::Stream)),
        1 => Just(Fault::LostAcknowledgement(FaultedWrite::Copy)),
        1 => Just(Fault::RejectedWrite(FaultedWrite::Copy)),
    ]
}

/// Generates workloads within `shape` and the fault each one injects.
fn crash_replay_cases(shape: WorkloadShape) -> impl Strategy<Value = CrashReplayCase> {
    (workloads(shape), faults()).prop_map(|(workload, fault)| CrashReplayCase { workload, fault })
}

/// Where a stored version came from.
///
/// Only the ClickHouse target reads back a change log of versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(not(all(feature = "clickhouse", feature = "test-utils")), expect(dead_code))]
pub(crate) enum VersionOrigin {
    /// The initial table copy.
    Copy,
    /// A streamed change.
    Change,
}

/// One version of a row in a destination's stored change log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredVersion {
    /// Whether the version was copied or streamed.
    origin: VersionOrigin,
    /// Primary key the version belongs to.
    id: i64,
    /// Source position of the change, ordered like the destination orders
    /// versions of one key. Copied versions sort before every change.
    position: String,
    /// Row after the change, or [`None`] when the change deleted the key.
    row: Option<WorkloadRecord>,
}

impl StoredVersion {
    /// Creates a stored version of key `id` at `position`.
    #[cfg_attr(not(all(feature = "clickhouse", feature = "test-utils")), expect(dead_code))]
    pub(crate) fn new(
        origin: VersionOrigin,
        id: i64,
        position: String,
        row: Option<WorkloadRecord>,
    ) -> Self {
        Self { origin, id, position, row }
    }
}

/// A real destination under crash-and-replay test.
///
/// One value owns the isolated destination storage of one case.
pub(crate) trait CrashReplayTarget: Sized {
    /// Settings shared by every case, such as a table engine.
    type Config: Copy;

    /// Destination built for each pipeline start.
    type Destination: PipelineDestination;

    /// Change shapes the destination preserves.
    const SHAPE: WorkloadShape;

    /// How long the destination's readable state may lag a finished pipeline.
    const SETTLE_TIMEOUT: Duration;

    /// Creates isolated destination storage for one case.
    async fn setup(config: Self::Config, tables: &WorkloadTables, pipeline_id: &PipelineId)
    -> Self;

    /// Builds a destination instance over the case storage, as a freshly
    /// started process would.
    async fn build_destination(&self, store: PostgresStore) -> Self::Destination;

    /// Reads the current rows of the destination table for `table`.
    ///
    /// A missing table is an error, not an empty one: table sync writes every
    /// table at least once, so each synced table must exist.
    async fn current_rows(&self, table: &TableName) -> Result<Vec<WorkloadRecord>, TestCaseError>;

    /// Reads every stored version of the destination table for `table`, or
    /// [`None`] when the destination keeps no readable change log.
    async fn stored_versions(
        &self,
        _table: &TableName,
    ) -> Result<Option<Vec<StoredVersion>>, TestCaseError> {
        Ok(None)
    }

    /// Prepares destination tables once the initial sync finished, before
    /// any streamed change is written.
    async fn after_tables_synced(&self, _tables: &WorkloadTables) -> Result<(), TestCaseError> {
        Ok(())
    }

    /// Releases case storage that is not released on drop.
    async fn teardown(self) {}
}

/// Counts which faults passing cases injected, on top of workload coverage.
#[derive(Debug, Default)]
struct CrashReplayCoverage {
    /// Workload paths reached.
    workload: WorkloadCoverage,
    /// Crashes after an acknowledged write.
    crashes_after_acknowledgement: AtomicUsize,
    /// Crashes while an applied write's acknowledgement was held.
    crashes_while_held: AtomicUsize,
    /// Crashes at a delay into delivery.
    crashes_during_delivery: AtomicUsize,
    /// Streaming writes whose acknowledgement was lost.
    lost_stream_acknowledgements: AtomicUsize,
    /// Streaming writes the destination refused.
    rejected_stream_writes: AtomicUsize,
    /// Copy writes whose acknowledgement was lost.
    lost_copy_acknowledgements: AtomicUsize,
    /// Copy writes the destination refused.
    rejected_copy_writes: AtomicUsize,
    /// Identical replayed changes found in stored change logs.
    replay_copies: AtomicUsize,
}

impl CrashReplayCoverage {
    /// Prints the counts and checks workload coverage.
    fn report(&self, property: &str) {
        println!("property '{property}' fault coverage: {self:?}");
        self.workload.report(property);
    }

    /// Adds one passing case's fault.
    fn record_fault(&self, fault: Fault) {
        let counter = match fault {
            Fault::Crash(CrashTiming::AfterAcknowledgement) => &self.crashes_after_acknowledgement,
            Fault::Crash(CrashTiming::WhileAcknowledgementHeld) => &self.crashes_while_held,
            Fault::Crash(CrashTiming::DuringDelivery { .. }) => &self.crashes_during_delivery,
            Fault::LostAcknowledgement(FaultedWrite::Stream) => &self.lost_stream_acknowledgements,
            Fault::RejectedWrite(FaultedWrite::Stream) => &self.rejected_stream_writes,
            Fault::LostAcknowledgement(FaultedWrite::Copy) => &self.lost_copy_acknowledgements,
            Fault::RejectedWrite(FaultedWrite::Copy) => &self.rejected_copy_writes,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Pipeline that writes through the fault-injecting wrapper.
type CasePipeline<D> = Pipeline<PostgresStore, TestDestinationWrapper<D>>;

/// Opens the pipeline store, as a starting process would.
async fn open_store(
    pipeline_id: &PipelineId,
    database: &PgDatabase<Client>,
) -> Result<PostgresStore, TestCaseError> {
    PostgresStore::new(pipeline_id, database.config.clone())
        .await
        .map_err(|error| TestCaseError::fail(format!("failed to open the Postgres store: {error}")))
}

/// Builds a destination instance and a pipeline over it, arms `copy_fault`
/// for the first copy write, then starts the pipeline.
async fn start_pipeline<T: CrashReplayTarget>(
    database: &PgDatabase<Client>,
    pipeline_id: &PipelineId,
    target: &T,
    store: &PostgresStore,
    copy_fault: Option<FaultAction>,
) -> Result<(CasePipeline<T::Destination>, TestDestinationWrapper<T::Destination>), TestCaseError> {
    let destination = TestDestinationWrapper::wrap(target.build_destination(store.clone()).await);
    if let Some(action) = copy_fault {
        destination.inject_fault(FaultyOp::WriteTableRows, action).await;
    }
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        WORKLOAD_PUBLICATION.to_owned(),
        store.clone(),
        destination.clone(),
    );
    pipeline
        .start()
        .await
        .map_err(|error| TestCaseError::fail(format!("pipeline failed to start: {error}")))?;

    Ok((pipeline, destination))
}

/// Events and copied rows that destination instances acknowledged.
#[derive(Default)]
struct Deliveries {
    /// Acknowledged events, in delivery order across instances.
    events: Vec<Event>,
    /// Acknowledged copied rows per table.
    copied_rows: HashMap<TableId, Vec<TableRow>>,
}

impl Deliveries {
    /// Keeps what a destination instance acknowledged and releases it.
    async fn record<D>(&mut self, destination: TestDestinationWrapper<D>) {
        self.events.extend(destination.get_events().await);
        for (table_id, rows) in destination.get_table_rows().await {
            self.copied_rows.entry(table_id).or_default().extend(rows);
        }
    }
}

/// Shuts down a destination instance whose pipeline crashed.
///
/// This releases the dead instance's connections and lets its in-flight
/// requests finish, like a remote server finishing requests of a crashed
/// client, before a new instance starts. The result is ignored: an instance
/// cut off mid-write may fail to shut down cleanly.
async fn shut_down_crashed<D: PipelineDestination>(destination: &TestDestinationWrapper<D>) {
    let _ = tokio::time::timeout(WORKLOAD_TIMEOUT, destination.shutdown()).await;
}

/// Returns how often a transaction of `phase` may be acknowledged.
///
/// An unacknowledged suffix may replay once after any fault. Transactions
/// after the fault run only once the fault transaction is delivered.
fn delivery_counts(phase: TxPhase) -> RangeInclusive<usize> {
    match phase {
        TxPhase::BeforeFault | TxPhase::Fault => 1..=2,
        TxPhase::AfterFault => 1..=1,
    }
}

/// Checks that rows read from the destination equal the source rows of
/// `table`, with each key present once.
fn compare_current_rows(
    table: usize,
    rows: Vec<WorkloadRecord>,
    source: &BTreeMap<i64, WorkloadRecord>,
) -> Result<(), TestCaseError> {
    let mut current = BTreeMap::new();
    for row in rows {
        let id = row.id();
        if let Some(previous) = current.insert(id, row) {
            return Err(TestCaseError::fail(format!(
                "table {table} shows key {id} more than once, including {previous:?}"
            )));
        }
    }
    prop_assert_eq!(&current, source, "table {} destination state differs from the source", table);

    Ok(())
}

/// Checks a stored change log and returns how many replayed copies it holds.
///
/// A copied row may be stored once: a failed copy drops the table before
/// copying it again. A replayed change may be stored again, but only as an
/// identical copy.
fn assert_replay_copies_allowed(
    table: usize,
    versions: &[StoredVersion],
) -> Result<usize, TestCaseError> {
    let mut first_copies = HashMap::<(VersionOrigin, i64, &str), &StoredVersion>::new();
    let mut copies = 0;
    for version in versions {
        let key = (version.origin, version.id, version.position.as_str());
        let Some(first) = first_copies.get(&key) else {
            first_copies.insert(key, version);
            continue;
        };

        prop_assert!(
            matches!(version.origin, VersionOrigin::Change),
            "table {} stores copied key {} more than once",
            table,
            version.id
        );
        prop_assert_eq!(
            *first,
            version,
            "table {} stores different versions of key {} at {}",
            table,
            version.id,
            &version.position
        );
        copies += 1;
    }

    Ok(copies)
}

/// Checks the destination's state of `table` against its source rows.
///
/// Destinations whose state settles asynchronously are read until it matches
/// or [`CrashReplayTarget::SETTLE_TIMEOUT`] elapses; read failures within that
/// window are retried as well, since a just-written table may not be readable
/// yet.
async fn assert_destination_converged<T: CrashReplayTarget>(
    target: &T,
    table: usize,
    name: &TableName,
    source: &BTreeMap<i64, WorkloadRecord>,
    coverage: &CrashReplayCoverage,
) -> Result<(), TestCaseError> {
    let deadline = Instant::now() + T::SETTLE_TIMEOUT;
    loop {
        let result = target
            .current_rows(name)
            .await
            .and_then(|rows| compare_current_rows(table, rows, source));
        match result {
            Ok(()) => break,
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => tokio::time::sleep(SETTLE_POLL_INTERVAL).await,
        }
    }

    if let Some(versions) = target.stored_versions(name).await? {
        let copies = assert_replay_copies_allowed(table, &versions)?;
        coverage.replay_copies.fetch_add(copies, Ordering::Relaxed);
    }

    Ok(())
}

/// Runs one case against prepared source tables and destination storage.
async fn run_case_on_target<T: CrashReplayTarget>(
    database: &PgDatabase<Client>,
    tables: &WorkloadTables,
    pipeline_id: &PipelineId,
    target: &T,
    case: &CrashReplayCase,
    coverage: &CrashReplayCoverage,
) -> Result<(), TestCaseError> {
    let client = database.client.as_ref().unwrap();
    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();

    // A copy fault must be armed before the pipeline starts copying.
    let (copy_fault, mut stream_fault) = match case.fault.scripted_write() {
        Some((FaultedWrite::Copy, action)) => (Some(action), None),
        Some((FaultedWrite::Stream, action)) => (None, Some(action)),
        None => (None, None),
    };
    // A copy write that landed must be cleared before the retry copies again.
    // A refused one may leave nothing to clear, and a retry without stored
    // destination metadata skips the reset. No other fault may recopy.
    let expected_drop = match case.fault {
        Fault::LostAcknowledgement(FaultedWrite::Copy) => Some(true),
        Fault::RejectedWrite(FaultedWrite::Copy) => None,
        _ => Some(false),
    };

    let mut store = open_store(pipeline_id, database).await?;
    let (mut pipeline, mut destination) =
        start_pipeline(database, pipeline_id, target, &store, copy_fault).await?;
    for table_id in tables.ids() {
        wait_for_table_sync_complete(&store, *table_id, WORKLOAD_TIMEOUT).await.map_err(
            |error| TestCaseError::fail(format!("failed to wait for table sync: {error}")),
        )?;

        // A crash must not race a copy worker that is still finishing.
        let sync_slot_name: String =
            EtlReplicationSlot::for_table_sync_worker(pipeline_id, *table_id).try_into().unwrap();
        wait_for_sync_slot_removal(database, &sync_slot_name).await?;
    }
    target.after_tables_synced(tables).await?;

    let mut runner = WorkloadRunner::new(client, tables);
    for tx in case.workload.before_fault() {
        runner.execute_and_wait(tx, TxPhase::BeforeFault, &destination).await?;
    }

    let mut straddle = match case.workload.straddle() {
        Some(inserts) => Some(runner.open_straddle(&database.config, inserts).await?),
        None => None,
    };

    let mut deliveries = Deliveries::default();
    let mut copy_writes_after_fault = destination.write_table_rows_called().await;
    match case.fault {
        Fault::Crash(timing) => {
            let held_acknowledgement = match timing {
                CrashTiming::AfterAcknowledgement => {
                    runner
                        .execute_and_wait(case.workload.fault_tx(), TxPhase::Fault, &destination)
                        .await?;
                    None
                }
                CrashTiming::WhileAcknowledgementHeld => {
                    let hold = destination.hold_next(FaultyOp::WriteEvents).await;
                    runner.execute(case.workload.fault_tx(), TxPhase::Fault).await?;
                    tokio::time::timeout(WORKLOAD_TIMEOUT, hold.wait_reached())
                        .await
                        .map_err(|_| TestCaseError::fail("timed out waiting for the held write"))?;
                    Some(hold)
                }
                CrashTiming::DuringDelivery { delay_ms } => {
                    runner.execute(case.workload.fault_tx(), TxPhase::Fault).await?;
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    None
                }
            };

            // Dropping the pipeline aborts the apply worker at an arbitrary
            // await point, without shutdown handling or a final status update.
            // The walsender poll proves the aborted worker's replication
            // connection is gone before the restart.
            drop(pipeline);
            wait_for_apply_disconnect(client, &apply_slot_name).await?;
            drop(held_acknowledgement);
            shut_down_crashed(&destination).await;
            deliveries.record(destination).await;
            drop(store);

            store = open_store(pipeline_id, database).await?;
            let restarted =
                TestDestinationWrapper::wrap(target.build_destination(store.clone()).await);

            // An unacknowledged fault transaction must replay. Wait for it
            // explicitly: later steps may all roll back and leave the restart
            // nothing else to stream.
            let fault_xid = runner.fault_xid().expect("the fault transaction always commits");
            let fault_replayed = if has_committed_xid(&deliveries.events, fault_xid) {
                None
            } else {
                Some(notify_on_commit(&restarted, fault_xid).await)
            };

            pipeline = create_pipeline(
                &database.config,
                pipeline_id,
                WORKLOAD_PUBLICATION.to_owned(),
                store.clone(),
                restarted.clone(),
            );
            pipeline.start().await.map_err(|error| {
                TestCaseError::fail(format!("restarted pipeline failed to start: {error}"))
            })?;
            destination = restarted;
            copy_writes_after_fault = 0;

            if let Some(fault_replayed) = &fault_replayed {
                wait_for_notification(fault_replayed, "replay of the fault transaction").await?;
            }
        }
        Fault::LostAcknowledgement(_) | Fault::RejectedWrite(_) => {
            if let Some(action) = stream_fault.take() {
                destination.inject_fault(FaultyOp::WriteEvents, action).await;
            }
            runner.execute_and_wait(case.workload.fault_tx(), TxPhase::Fault, &destination).await?;
        }
    }

    for step in case.workload.after_fault() {
        match step {
            AfterFaultStep::Tx(tx) => {
                runner.execute_and_wait(tx, TxPhase::AfterFault, &destination).await?;
            }
            AfterFaultStep::CommitStraddle => {
                let session = straddle.take().expect("planned straddle should be open");
                runner.commit_straddle(session, &destination).await?;
            }
        }
    }
    let history = runner.into_history();

    // Apply promotes a table to `Ready` only once a change on the current
    // connection materializes its decoder, so only written tables must reach
    // it. Wait before shutdown so the promotion is not cut short.
    let written_after_fault = case.workload.tables_written_after_fault();
    for (table_id, written) in tables.ids().iter().zip(written_after_fault) {
        if written {
            wait_for_table_state_type(&store, *table_id, TableStateType::Ready, WORKLOAD_TIMEOUT)
                .await
                .map_err(|error| {
                    TestCaseError::fail(format!("failed to wait for table readiness: {error}"))
                })?;
        }
    }

    tokio::time::timeout(WORKLOAD_TIMEOUT, pipeline.shutdown_and_wait())
        .await
        .map_err(|_| TestCaseError::fail("timed out waiting for pipeline shutdown"))?
        .map_err(|error| TestCaseError::fail(format!("pipeline shutdown failed: {error}")))?;

    let mut dropped_for_copy = false;
    for table_id in tables.ids() {
        let table_state = store
            .get_table_state(*table_id)
            .await
            .map_err(|error| TestCaseError::fail(format!("failed to read table state: {error}")))?
            .ok_or_else(|| TestCaseError::fail("table state was missing after the run"))?;
        let table_state_type = TableStateType::from(&table_state);
        prop_assert!(
            matches!(table_state_type, TableStateType::SyncDone | TableStateType::Ready),
            "table {} ended in {:?}",
            table_id,
            table_state_type
        );
        dropped_for_copy |= destination.was_table_dropped_for_copy(*table_id).await;
    }
    if let Some(expected_drop) = expected_drop {
        prop_assert_eq!(
            dropped_for_copy,
            expected_drop,
            "a table drop for copy did not match the {:?} fault",
            case.fault
        );
    }
    prop_assert_eq!(
        destination.write_table_rows_called().await,
        copy_writes_after_fault,
        "the fault made a table recopy after the initial sync"
    );

    // Every destination instance and the store are released before reading,
    // because some destinations only expose their final state after their
    // writers close.
    deliveries.record(destination).await;
    drop(store);

    let source = read_source_rows(client, tables).await?;
    assert_workload_converged(
        tables,
        &deliveries.copied_rows,
        &deliveries.events,
        &source,
        &history,
        delivery_counts,
        DeliveryOrder::Source,
        &coverage.workload,
    )?;

    for (table, name) in tables.names().iter().enumerate() {
        assert_destination_converged(target, table, name, &source[table], coverage).await?;
    }

    Ok(())
}

/// Runs one case on fresh source and destination storage.
async fn run_case<T: CrashReplayTarget>(
    config: T::Config,
    case: &CrashReplayCase,
    coverage: &CrashReplayCoverage,
) -> Result<(), TestCaseError> {
    let database = spawn_source_database().await;
    let tables = create_workload_tables(&database, &case.workload).await;
    let pipeline_id: PipelineId = random_pipeline_id().into();
    let target = T::setup(config, &tables, &pipeline_id).await;

    let result =
        run_case_on_target(&database, &tables, &pipeline_id, &target, case, coverage).await;
    target.teardown().await;
    if result.is_ok() {
        coverage.record_fault(case.fault);
    }

    result
}

/// Runs the crash-and-replay property against target `T`.
pub(crate) fn run_crash_replay_property<T: CrashReplayTarget>(property: &str, config: T::Config) {
    let strategy = crash_replay_cases(T::SHAPE);
    let coverage = CrashReplayCoverage::default();
    run_expensive_property(property, &strategy, |case| {
        block_on(run_case::<T>(config, case, &coverage))
    });
    coverage.report(property);
}
