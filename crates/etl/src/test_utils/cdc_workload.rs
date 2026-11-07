//! Randomized CDC workloads and a convergence oracle for fault properties.
//!
//! A [`Workload`] seeds two tables before the pipeline starts, then runs
//! generated transactions around one fault point. Transactions mix inserts,
//! value and key updates, counter-only updates that leave TOASTed notes
//! unchanged, deletes, truncates, and rollbacks. Each table uses replica
//! identity `DEFAULT` or `FULL` per case. A straddling transaction can open on
//! a second session before the fault and commit after it, so the slot must
//! keep its changes replayable across the fault.
//!
//! Generation plans every operation against a model of committed rows, so each
//! committed transaction emits exactly one data event per planned operation.
//! A [`WorkloadShape`] keeps generated changes inside what a destination can
//! preserve. [`assert_workload_converged`] checks delivery per transaction,
//! replay stability, and the materialized destination state against the
//! source.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    ops::RangeInclusive,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use etl_config::shared::PgConnectionConfig;
use etl_postgres::tokio::test_utils::{PgDatabase, TableModification, connect_to_pg_database};
use pg_escape::quote_literal;
use proptest::{collection::vec, prelude::*, sample::Index};
use tokio_postgres::{Client, types::PgLsn};
use uuid::Uuid;

use crate::{
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    event::{Event, UpdateEvent},
    schema::{ReplicatedTableSchema, TableId, TableName},
    test_utils::{
        database::{replication_slot_state, test_table_name},
        materialize::{FromTableRow, materialize_events},
        notify::TimedNotify,
        test_destination_wrapper::TestDestinationWrapper,
    },
};

/// Number of tables each workload writes.
pub const TABLE_COUNT: usize = 2;

/// Publication covering every workload table.
pub const WORKLOAD_PUBLICATION: &str = "cdc_workload_pub";

/// Maximum time for one workload synchronization point.
pub const WORKLOAD_TIMEOUT: Duration = Duration::from_secs(60);

/// Large-note size, above the out-of-line TOAST threshold for
/// `storage external` columns.
const LARGE_NOTE_BYTES: usize = 4096;

/// Change shapes a workload may generate.
///
/// Destinations that cannot preserve a shape reject it by design, so their
/// properties generate workloads without it instead of expecting failures.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadShape {
    /// Whether tables may use replica identity `DEFAULT`, so updates that
    /// leave a TOASTed note unchanged arrive as partial rows.
    partial_updates: bool,
    /// Whether updates may move a row to a new primary key.
    key_changes: bool,
}

impl WorkloadShape {
    /// Allows every generated change shape.
    pub const fn all() -> Self {
        Self { partial_updates: true, key_changes: true }
    }

    /// Uses replica identity `FULL` for every table, so every update carries
    /// a full new row.
    pub const fn without_partial_updates(self) -> Self {
        Self { partial_updates: false, ..self }
    }

    /// Keeps every update on its row's primary key.
    pub const fn without_key_changes(self) -> Self {
        Self { key_changes: false, ..self }
    }
}

/// Replica identity configured for one workload table.
#[derive(Clone, Copy, Debug)]
pub enum IdentityMode {
    /// Primary-key identity: deletes carry key rows and unchanged TOAST
    /// values can arrive as partial updates.
    Default,
    /// Full-row identity: old rows are complete.
    Full,
}

/// Note value shape requested by a generated operation.
#[derive(Clone, Copy, Debug)]
enum NoteIntent {
    /// SQL `NULL`.
    Null,
    /// Short inline text.
    Small,
    /// Text stored out of line in TOAST.
    Large,
}

/// One generated operation before it is planned against the model.
#[derive(Clone, Copy, Debug)]
enum OpIntent {
    /// Inserts a new row.
    Insert { table: usize, note: NoteIntent },
    /// Replaces an existing row's note.
    UpdateNote { table: usize, target: Index, note: NoteIntent },
    /// Increments an existing row's counter, leaving its note unchanged.
    UpdateCounter { table: usize, target: Index },
    /// Moves an existing row to a new primary key.
    UpdateKey { table: usize, target: Index },
    /// Deletes an existing row.
    Delete { table: usize, target: Index },
    /// Truncates a table.
    Truncate { table: usize },
}

/// One generated transaction before planning.
#[derive(Clone, Debug)]
struct TxIntent {
    /// Generated operations in execution order.
    ops: Vec<OpIntent>,
    /// Whether the transaction rolls back.
    rollback: bool,
}

/// One generated workload before planning.
#[derive(Clone, Debug)]
struct WorkloadIntent {
    /// Replica identity per table.
    identities: [IdentityMode; TABLE_COUNT],
    /// Rows present before the initial copy.
    initial_rows: Vec<(usize, NoteIntent)>,
    /// Transactions before the fault; the last one is forced to commit.
    before_fault: Vec<TxIntent>,
    /// Inserts of a transaction that stays open across the fault.
    straddle: Option<Vec<(usize, NoteIntent)>>,
    /// Transactions after the fault.
    after_fault: Vec<TxIntent>,
    /// Position among the post-fault transactions where the straddle commits.
    straddle_commit: Index,
    /// Whether a table copy is open while pre-fault transactions run.
    copy_open_before_fault: bool,
}

/// Note value identified by a unique sequence number.
///
/// Rendering lazily keeps generated cases readable in failure output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Note {
    /// SQL `NULL`.
    Null,
    /// Short inline text.
    Small(u32),
    /// Text stored out of line in TOAST.
    Large(u32),
}

impl Note {
    /// Returns the SQL literal for this note.
    fn sql_literal(self) -> String {
        match self {
            Self::Null => "null".to_owned(),
            Self::Small(sequence) => quote_literal(&format!("small-{sequence}")),
            Self::Large(sequence) => {
                quote_literal(&format!("large-{sequence}-{}", "x".repeat(LARGE_NOTE_BYTES)))
            }
        }
    }
}

/// One planned source row.
#[derive(Clone, Copy, Debug)]
struct PlannedRow {
    /// Primary key.
    id: i64,
    /// Note column.
    note: Note,
    /// Counter column.
    counter: i32,
}

/// One planned operation with concrete row identities.
#[derive(Clone, Copy, Debug)]
enum PlannedOp {
    /// Inserts `row`.
    Insert { table: usize, row: PlannedRow },
    /// Sets the note of row `id`.
    UpdateNote { table: usize, id: i64, note: Note },
    /// Increments the counter of row `id`.
    UpdateCounter { table: usize, id: i64 },
    /// Changes the primary key of row `old_id` to `new_id`.
    UpdateKey { table: usize, old_id: i64, new_id: i64 },
    /// Deletes row `id`.
    Delete { table: usize, id: i64 },
    /// Truncates the table.
    Truncate { table: usize },
}

impl PlannedOp {
    /// Returns the table this operation writes.
    fn table(&self) -> usize {
        match *self {
            Self::Insert { table, .. }
            | Self::UpdateNote { table, .. }
            | Self::UpdateCounter { table, .. }
            | Self::UpdateKey { table, .. }
            | Self::Delete { table, .. }
            | Self::Truncate { table } => table,
        }
    }

    /// Returns the SQL statement for this operation.
    fn sql(&self, tables: &WorkloadTables) -> String {
        match *self {
            Self::Insert { table, row } => format!(
                "insert into {} (id, note, counter) values ({}, {}, {})",
                tables.quoted(table),
                row.id,
                row.note.sql_literal(),
                row.counter
            ),
            Self::UpdateNote { table, id, note } => format!(
                "update {} set note = {} where id = {id}",
                tables.quoted(table),
                note.sql_literal()
            ),
            Self::UpdateCounter { table, id } => {
                format!("update {} set counter = counter + 1 where id = {id}", tables.quoted(table))
            }
            Self::UpdateKey { table, old_id, new_id } => {
                format!("update {} set id = {new_id} where id = {old_id}", tables.quoted(table))
            }
            Self::Delete { table, id } => {
                format!("delete from {} where id = {id}", tables.quoted(table))
            }
            Self::Truncate { table } => format!("truncate {}", tables.quoted(table)),
        }
    }
}

/// One planned transaction.
#[derive(Clone, Debug)]
pub struct PlannedTx {
    /// Operations in execution order; each emits one data event on commit.
    ops: Vec<PlannedOp>,
    /// Whether the transaction rolls back.
    rollback: bool,
}

/// One step after the fault point.
#[derive(Clone, Debug)]
pub enum AfterFaultStep {
    /// Runs a transaction.
    Tx(PlannedTx),
    /// Commits the straddling transaction.
    CommitStraddle,
}

/// A planned workload ready to execute.
#[derive(Clone, Debug)]
pub struct Workload {
    /// Replica identity per table.
    identities: [IdentityMode; TABLE_COUNT],
    /// Inserts committed before the pipeline starts.
    initial_rows: Vec<PlannedOp>,
    /// Transactions before the fault transaction.
    before_fault: Vec<PlannedTx>,
    /// Inserts of the transaction that stays open across the fault.
    straddle: Option<PlannedTx>,
    /// Committed transaction whose delivery the fault interrupts.
    fault_tx: PlannedTx,
    /// Steps after the fault.
    after_fault: Vec<AfterFaultStep>,
}

impl Workload {
    /// Returns the transactions before the fault transaction.
    pub fn before_fault(&self) -> &[PlannedTx] {
        &self.before_fault
    }

    /// Returns the straddling transaction's inserts, if any.
    pub fn straddle(&self) -> Option<&PlannedTx> {
        self.straddle.as_ref()
    }

    /// Returns the transaction whose delivery the fault interrupts.
    pub fn fault_tx(&self) -> &PlannedTx {
        &self.fault_tx
    }

    /// Returns the steps after the fault.
    pub fn after_fault(&self) -> &[AfterFaultStep] {
        &self.after_fault
    }

    /// Returns which tables receive committed changes after the fault.
    ///
    /// Apply promotes a table to `Ready` only after its decoder materializes
    /// from a relation or change on the current connection, so tables without
    /// post-fault changes may legitimately stay in `SyncDone`.
    pub fn tables_written_after_fault(&self) -> [bool; TABLE_COUNT] {
        let mut written = [false; TABLE_COUNT];
        let committed_after_fault = self.after_fault.iter().filter_map(|step| match step {
            AfterFaultStep::Tx(tx) if !tx.rollback => Some(tx),
            AfterFaultStep::Tx(_) => None,
            AfterFaultStep::CommitStraddle => self.straddle.as_ref(),
        });
        for tx in committed_after_fault {
            for op in &tx.ops {
                written[op.table()] = true;
            }
        }

        written
    }
}

/// Plans generated intents against a model of committed rows.
struct Planner {
    /// Committed rows per table.
    tables: [BTreeMap<i64, PlannedRow>; TABLE_COUNT],
    /// Next unused primary key across tables.
    next_id: i64,
    /// Next unused note sequence number.
    next_note: u32,
    /// Whether an open transaction or table copy holds a lock that blocks
    /// truncate.
    truncate_blocked: bool,
    /// Whether updates may change a row's primary key.
    key_changes: bool,
}

impl Planner {
    /// Creates a planner over empty tables.
    fn new(shape: WorkloadShape) -> Self {
        Self {
            tables: Default::default(),
            next_id: 1,
            next_note: 1,
            truncate_blocked: false,
            key_changes: shape.key_changes,
        }
    }

    /// Allocates a unique note for `intent`.
    fn note(&mut self, intent: NoteIntent) -> Note {
        let sequence = self.next_note;
        self.next_note += 1;
        match intent {
            NoteIntent::Null => Note::Null,
            NoteIntent::Small => Note::Small(sequence),
            NoteIntent::Large => Note::Large(sequence),
        }
    }

    /// Allocates a unique primary key.
    fn id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Plans an insert of a new row without applying it to the model.
    fn new_row(&mut self, table: usize, note: NoteIntent) -> PlannedOp {
        let row = PlannedRow { id: self.id(), note: self.note(note), counter: 0 };
        PlannedOp::Insert { table, row }
    }

    /// Applies a planned operation to the model.
    fn apply(&mut self, op: PlannedOp) {
        match op {
            PlannedOp::Insert { table, row } => {
                self.tables[table].insert(row.id, row);
            }
            PlannedOp::UpdateNote { table, id, note } => {
                self.tables[table].get_mut(&id).expect("planned row should exist").note = note;
            }
            PlannedOp::UpdateCounter { table, id } => {
                self.tables[table].get_mut(&id).expect("planned row should exist").counter += 1;
            }
            PlannedOp::UpdateKey { table, old_id, new_id } => {
                let mut row = self.tables[table].remove(&old_id).expect("planned row should exist");
                row.id = new_id;
                self.tables[table].insert(new_id, row);
            }
            PlannedOp::Delete { table, id } => {
                self.tables[table].remove(&id);
            }
            PlannedOp::Truncate { table } => self.tables[table].clear(),
        }
    }

    /// Returns the row `target` selects in `table`, if the table has rows.
    fn target(&self, table: usize, target: Index) -> Option<i64> {
        let rows = &self.tables[table];
        if rows.is_empty() {
            return None;
        }

        rows.keys().nth(target.index(rows.len())).copied()
    }

    /// Plans one operation and applies it to the model.
    ///
    /// Operations that need a row degrade to inserts on an empty table, a key
    /// update degrades to a counter update when the shape forbids key changes,
    /// and truncate degrades to an insert while an open transaction or table
    /// copy holds a lock that would block it.
    fn plan_op(&mut self, intent: OpIntent) -> PlannedOp {
        let op = match intent {
            OpIntent::Insert { table, note } => self.new_row(table, note),
            OpIntent::UpdateNote { table, target, note } => match self.target(table, target) {
                Some(id) => PlannedOp::UpdateNote { table, id, note: self.note(note) },
                None => self.new_row(table, note),
            },
            OpIntent::UpdateCounter { table, target } => match self.target(table, target) {
                Some(id) => PlannedOp::UpdateCounter { table, id },
                None => self.new_row(table, NoteIntent::Large),
            },
            OpIntent::UpdateKey { table, target } => match self.target(table, target) {
                Some(id) if !self.key_changes => PlannedOp::UpdateCounter { table, id },
                Some(old_id) => PlannedOp::UpdateKey { table, old_id, new_id: self.id() },
                None => self.new_row(table, NoteIntent::Large),
            },
            OpIntent::Delete { table, target } => match self.target(table, target) {
                Some(id) => PlannedOp::Delete { table, id },
                None => self.new_row(table, NoteIntent::Small),
            },
            OpIntent::Truncate { table } if self.truncate_blocked => {
                self.new_row(table, NoteIntent::Small)
            }
            OpIntent::Truncate { table } => PlannedOp::Truncate { table },
        };
        self.apply(op);

        op
    }

    /// Plans one transaction; rolled-back transactions leave the model as is.
    fn plan_tx(&mut self, intent: &TxIntent, force_commit: bool) -> PlannedTx {
        let rollback = intent.rollback && !force_commit;
        let committed_tables = rollback.then(|| self.tables.clone());
        let ops = intent.ops.iter().map(|op| self.plan_op(*op)).collect();
        if let Some(committed_tables) = committed_tables {
            self.tables = committed_tables;
        }

        PlannedTx { ops, rollback }
    }

    /// Plans a full workload in execution order.
    fn plan(mut self, intent: WorkloadIntent) -> Workload {
        let initial_rows = intent
            .initial_rows
            .iter()
            .map(|(table, note)| {
                let op = self.new_row(*table, *note);
                self.apply(op);
                op
            })
            .collect();

        let (fault_intent, before_intents) =
            intent.before_fault.split_last().expect("generated workloads have a fault transaction");
        self.truncate_blocked = intent.copy_open_before_fault;
        let before_fault = before_intents.iter().map(|tx| self.plan_tx(tx, false)).collect();

        let straddle = intent.straddle.as_ref().map(|inserts| {
            self.truncate_blocked = true;
            let ops = inserts.iter().map(|(table, note)| self.new_row(*table, *note)).collect();
            PlannedTx { ops, rollback: false }
        });
        let fault_tx = self.plan_tx(fault_intent, true);
        self.truncate_blocked = straddle.is_some();

        let straddle_commit = intent.straddle_commit.index(intent.after_fault.len() + 1);
        let mut after_fault = Vec::with_capacity(intent.after_fault.len() + 1);
        for (position, tx) in intent.after_fault.iter().enumerate() {
            if position == straddle_commit {
                self.commit_straddle(straddle.as_ref(), &mut after_fault);
            }
            after_fault.push(AfterFaultStep::Tx(self.plan_tx(tx, false)));
        }
        if straddle_commit == intent.after_fault.len() {
            self.commit_straddle(straddle.as_ref(), &mut after_fault);
        }

        Workload {
            identities: intent.identities,
            initial_rows,
            before_fault,
            straddle,
            fault_tx,
            after_fault,
        }
    }

    /// Applies the straddling inserts to the model and schedules their commit.
    fn commit_straddle(&mut self, straddle: Option<&PlannedTx>, steps: &mut Vec<AfterFaultStep>) {
        let Some(straddle) = straddle else {
            return;
        };

        for op in &straddle.ops {
            self.apply(*op);
        }
        self.truncate_blocked = false;
        steps.push(AfterFaultStep::CommitStraddle);
    }
}

/// Generates note shapes, biased toward TOASTed values.
fn note_intents() -> impl Strategy<Value = NoteIntent> {
    prop_oneof![
        1 => Just(NoteIntent::Null),
        2 => Just(NoteIntent::Small),
        2 => Just(NoteIntent::Large),
    ]
}

/// Generates one operation intent.
fn op_intents() -> impl Strategy<Value = OpIntent> {
    let table = 0..TABLE_COUNT;
    prop_oneof![
        4 => (table.clone(), note_intents())
            .prop_map(|(table, note)| OpIntent::Insert { table, note }),
        2 => (table.clone(), any::<Index>(), note_intents())
            .prop_map(|(table, target, note)| OpIntent::UpdateNote { table, target, note }),
        2 => (table.clone(), any::<Index>())
            .prop_map(|(table, target)| OpIntent::UpdateCounter { table, target }),
        1 => (table.clone(), any::<Index>())
            .prop_map(|(table, target)| OpIntent::UpdateKey { table, target }),
        2 => (table.clone(), any::<Index>())
            .prop_map(|(table, target)| OpIntent::Delete { table, target }),
        1 => table.prop_map(|table| OpIntent::Truncate { table }),
    ]
}

/// Generates one transaction intent.
fn tx_intents() -> impl Strategy<Value = TxIntent> {
    (vec(op_intents(), 1..=4), prop::bool::weighted(0.15))
        .prop_map(|(ops, rollback)| TxIntent { ops, rollback })
}

/// Generates one replica identity allowed by `shape`.
fn identity_modes(shape: WorkloadShape) -> BoxedStrategy<IdentityMode> {
    if shape.partial_updates {
        prop_oneof![Just(IdentityMode::Default), Just(IdentityMode::Full)].boxed()
    } else {
        Just(IdentityMode::Full).boxed()
    }
}

/// Generates planned workloads within `shape`, sometimes with a transaction
/// left open across the fault.
pub fn workloads(shape: WorkloadShape) -> impl Strategy<Value = Workload> {
    plan_workloads(
        shape,
        prop::option::weighted(0.4, vec((0..TABLE_COUNT, note_intents()), 1..=2)),
        false,
    )
}

/// Generates planned workloads whose pre-fault transactions run while a table
/// copy is open.
///
/// There is no transaction left open across the fault, because recovery can
/// create a replication slot and slot creation waits for open transactions.
/// Pre-fault transactions never truncate, because truncate waits for the open
/// copy's lock.
pub fn sync_workloads() -> impl Strategy<Value = Workload> {
    plan_workloads(WorkloadShape::all(), Just(None), true)
}

/// Plans workloads within `shape` whose straddling transaction comes from
/// `straddle`.
///
/// `copy_open_before_fault` keeps truncates out of pre-fault transactions.
fn plan_workloads(
    shape: WorkloadShape,
    straddle: impl Strategy<Value = Option<Vec<(usize, NoteIntent)>>>,
    copy_open_before_fault: bool,
) -> impl Strategy<Value = Workload> {
    (
        [identity_modes(shape), identity_modes(shape)],
        vec((0..TABLE_COUNT, note_intents()), 0..=4),
        vec(tx_intents(), 1..=4),
        straddle,
        vec(tx_intents(), 0..=4),
        any::<Index>(),
    )
        .prop_map(
            move |(
                identities,
                initial_rows,
                before_fault,
                straddle,
                after_fault,
                straddle_commit,
            )| {
                Planner::new(shape).plan(WorkloadIntent {
                    identities,
                    initial_rows,
                    before_fault,
                    straddle,
                    after_fault,
                    straddle_commit,
                    copy_open_before_fault,
                })
            },
        )
}

/// Source tables written by a workload.
pub struct WorkloadTables {
    /// Table names, indexed like planned operations.
    names: [TableName; TABLE_COUNT],
    /// Table IDs, indexed like planned operations.
    ids: [TableId; TABLE_COUNT],
    /// Replica identity per table, indexed like planned operations.
    identities: [IdentityMode; TABLE_COUNT],
}

impl WorkloadTables {
    /// Returns the table names.
    pub fn names(&self) -> &[TableName; TABLE_COUNT] {
        &self.names
    }

    /// Returns the table IDs.
    pub fn ids(&self) -> &[TableId; TABLE_COUNT] {
        &self.ids
    }

    /// Returns the quoted name of `table`.
    fn quoted(&self, table: usize) -> String {
        self.names[table].as_quoted_identifier()
    }

    /// Returns whether any table can emit partial updates.
    fn has_default_identity(&self) -> bool {
        self.identities.iter().any(|identity| matches!(identity, IdentityMode::Default))
    }
}

/// Creates the workload tables and publication, then commits initial rows.
///
/// Table names carry a random suffix, so destinations that share storage
/// across cases never see the same name twice.
pub async fn create_workload_tables(
    database: &PgDatabase<Client>,
    workload: &Workload,
) -> WorkloadTables {
    let suffix = Uuid::new_v4().simple().to_string();
    let names = std::array::from_fn(|table| {
        test_table_name(&format!("cdc_workload_{}_{table}", &suffix[..12]))
    });
    let mut ids = [TableId::new(0); TABLE_COUNT];

    for (table, name) in names.iter().enumerate() {
        ids[table] = database
            .create_table(name.clone(), true, &[("note", "text"), ("counter", "int4 not null")])
            .await
            .unwrap();

        // External storage keeps large notes out of line without
        // compression, so counter-only updates leave them unchanged TOAST.
        let mut modifications = vec![TableModification::AlterColumn {
            name: "note",
            alteration: "set storage external",
        }];
        if matches!(workload.identities[table], IdentityMode::Full) {
            modifications.push(TableModification::ReplicaIdentity { value: "full" });
        }
        database.alter_table(name.clone(), &modifications).await.unwrap();
    }
    database.create_publication(WORKLOAD_PUBLICATION, &names).await.unwrap();

    let tables = WorkloadTables { names, ids, identities: workload.identities };
    let client = database.client.as_ref().unwrap();
    for op in &workload.initial_rows {
        client.batch_execute(&op.sql(&tables)).await.unwrap();
    }

    tables
}

/// Stage of a committed transaction relative to the fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxPhase {
    /// Committed and delivered before the fault transaction.
    BeforeFault,
    /// The transaction whose delivery the fault interrupts.
    Fault,
    /// Committed after the fault.
    AfterFault,
}

/// One executed committed transaction.
#[derive(Debug)]
struct CommittedTx {
    /// Source transaction ID truncated to the 32 bits pgoutput carries.
    xid: u32,
    /// Number of data events the transaction emits.
    event_count: usize,
    /// Stage relative to the fault.
    phase: TxPhase,
}

/// Source-side record of an executed workload.
#[derive(Debug, Default)]
pub struct WorkloadHistory {
    /// Committed transactions in commit order.
    committed: Vec<CommittedTx>,
    /// Transaction IDs of rolled-back transactions.
    rolled_back: Vec<u32>,
    /// Whether a transaction left open across the fault committed.
    straddle_committed: bool,
}

/// A transaction left open on its own session across the fault.
pub struct StraddleSession {
    /// Session holding the open transaction.
    client: Client,
    /// Open transaction ID.
    xid: u32,
    /// Number of data events the transaction emits.
    event_count: usize,
}

/// Returns whether `events` contain the commit of transaction `xid`.
pub fn has_committed_xid(events: &[Event], xid: u32) -> bool {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Begin(begin) if begin.xid == xid => Some(begin.commit_lsn),
            _ => None,
        })
        .any(|commit_lsn| {
            events.iter().any(
                |event| matches!(event, Event::Commit(commit) if commit.commit_lsn == commit_lsn),
            )
        })
}

/// Registers a notification for the delivery of transaction `xid`'s commit.
///
/// Register before the commit or restart that can deliver it.
pub async fn notify_on_commit<D>(destination: &TestDestinationWrapper<D>, xid: u32) -> TimedNotify {
    destination.notify_on_events(move |events| has_committed_xid(events, xid)).await
}

/// Waits for one bounded workload synchronization point.
pub async fn wait_for_notification(
    notification: &TimedNotify,
    description: impl Into<String>,
) -> Result<(), TestCaseError> {
    let description = description.into();
    tokio::time::timeout(WORKLOAD_TIMEOUT, notification.inner().notified())
        .await
        .map_err(|_| TestCaseError::fail(format!("timed out waiting for {description}")))
}

/// Waits until the old apply worker releases its replication slot connection.
pub async fn wait_for_apply_disconnect(
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
/// Slot cleanup is allowed at `SyncDone` or `Ready`. Its absence means
/// table-sync work has stopped, not that apply has already marked the table
/// ready.
pub async fn wait_for_sync_slot_removal(
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

/// Returns the current transaction ID truncated to pgoutput's 32 bits.
async fn current_xid(client: &Client) -> Result<u32, TestCaseError> {
    let row = client
        .query_one("select (pg_current_xact_id()::text::bigint % 4294967296)::bigint", &[])
        .await
        .map_err(|error| TestCaseError::fail(format!("failed to read xid: {error}")))?;
    let xid: i64 = row.get(0);

    Ok(u32::try_from(xid).expect("xid modulo 2^32 should fit in u32"))
}

/// Executes one SQL batch and maps failures.
async fn execute(client: &Client, sql: &str) -> Result<(), TestCaseError> {
    client
        .batch_execute(sql)
        .await
        .map_err(|error| TestCaseError::fail(format!("workload statement failed: {error}")))
}

/// Executes workload transactions against the source and records history.
pub struct WorkloadRunner<'a> {
    /// Session for regular transactions.
    client: &'a Client,
    /// Tables the workload writes.
    tables: &'a WorkloadTables,
    /// Executed transactions.
    history: WorkloadHistory,
}

impl<'a> WorkloadRunner<'a> {
    /// Creates a runner over `tables`.
    pub fn new(client: &'a Client, tables: &'a WorkloadTables) -> Self {
        Self { client, tables, history: WorkloadHistory::default() }
    }

    /// Executes `tx` without waiting for its delivery.
    pub async fn execute(&mut self, tx: &PlannedTx, phase: TxPhase) -> Result<(), TestCaseError> {
        if let Some(xid) = self.run_until_commit(tx).await? {
            self.commit(tx, phase, xid).await?;
        }

        Ok(())
    }

    /// Executes `tx` and returns a commit notification for `destination`, or
    /// [`None`] when the transaction rolls back.
    ///
    /// The notification is armed before `COMMIT`, so it cannot miss delivery.
    pub async fn execute_notified<D>(
        &mut self,
        tx: &PlannedTx,
        phase: TxPhase,
        destination: &TestDestinationWrapper<D>,
    ) -> Result<Option<TimedNotify>, TestCaseError> {
        let Some(xid) = self.run_until_commit(tx).await? else {
            return Ok(None);
        };
        let committed = notify_on_commit(destination, xid).await;
        self.commit(tx, phase, xid).await?;

        Ok(Some(committed))
    }

    /// Runs `tx` up to its end, rolling back when planned, and returns the
    /// transaction ID of a transaction that still needs to commit.
    async fn run_until_commit(&mut self, tx: &PlannedTx) -> Result<Option<u32>, TestCaseError> {
        execute(self.client, "begin").await?;
        for op in &tx.ops {
            execute(self.client, &op.sql(self.tables)).await?;
        }
        let xid = current_xid(self.client).await?;

        if tx.rollback {
            execute(self.client, "rollback").await?;
            self.history.rolled_back.push(xid);
            return Ok(None);
        }

        Ok(Some(xid))
    }

    /// Commits the open transaction `xid` and records it in the history.
    async fn commit(
        &mut self,
        tx: &PlannedTx,
        phase: TxPhase,
        xid: u32,
    ) -> Result<(), TestCaseError> {
        execute(self.client, "commit").await?;
        self.history.committed.push(CommittedTx { xid, event_count: tx.ops.len(), phase });

        Ok(())
    }

    /// Executes `tx` and waits until `destination` records its commit.
    pub async fn execute_and_wait<D>(
        &mut self,
        tx: &PlannedTx,
        phase: TxPhase,
        destination: &TestDestinationWrapper<D>,
    ) -> Result<(), TestCaseError> {
        if let Some(committed) = self.execute_notified(tx, phase, destination).await? {
            wait_for_notification(&committed, format!("delivery of {phase:?} transaction")).await?;
        }

        Ok(())
    }

    /// Opens the straddling transaction on a new session and runs its inserts.
    pub async fn open_straddle(
        &self,
        config: &PgConnectionConfig,
        straddle: &PlannedTx,
    ) -> Result<StraddleSession, TestCaseError> {
        let (client, _) = connect_to_pg_database(config).await;
        execute(&client, "begin").await?;
        for op in &straddle.ops {
            execute(&client, &op.sql(self.tables)).await?;
        }
        let xid = current_xid(&client).await?;

        Ok(StraddleSession { client, xid, event_count: straddle.ops.len() })
    }

    /// Commits the straddling transaction and waits for its delivery.
    pub async fn commit_straddle<D>(
        &mut self,
        straddle: StraddleSession,
        destination: &TestDestinationWrapper<D>,
    ) -> Result<(), TestCaseError> {
        let xid = straddle.xid;
        let committed = notify_on_commit(destination, xid).await;
        execute(&straddle.client, "commit").await?;
        self.history.committed.push(CommittedTx {
            xid,
            event_count: straddle.event_count,
            phase: TxPhase::AfterFault,
        });
        self.history.straddle_committed = true;

        wait_for_notification(&committed, "delivery of the straddling transaction").await
    }

    /// Returns the committed fault transaction ID, once it has run.
    pub fn fault_xid(&self) -> Option<u32> {
        self.history.committed.iter().find(|tx| tx.phase == TxPhase::Fault).map(|tx| tx.xid)
    }

    /// Returns the executed history.
    pub fn into_history(self) -> WorkloadHistory {
        self.history
    }
}

/// One materialized workload row.
#[derive(Clone, PartialEq, Eq)]
pub struct WorkloadRecord {
    /// Primary key.
    id: i64,
    /// Note column.
    note: Option<String>,
    /// Counter column.
    counter: i32,
}

impl WorkloadRecord {
    /// Creates a record from the row's column values.
    pub fn new(id: i64, note: Option<String>, counter: i32) -> Self {
        Self { id, note, counter }
    }
}

impl fmt::Debug for WorkloadRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Large notes would drown failure output, so show a bounded prefix.
        let note = self.note.as_deref().map(|note| match note.get(..24) {
            Some(prefix) if note.len() > 24 => format!("{prefix}…({} bytes)", note.len()),
            _ => note.to_owned(),
        });
        f.debug_struct("WorkloadRecord")
            .field("id", &self.id)
            .field("note", &note)
            .field("counter", &self.counter)
            .finish()
    }
}

impl FromTableRow for WorkloadRecord {
    type Id = i64;

    fn from_table_row(table_row: &TableRow) -> Option<Self> {
        let [Cell::I64(id), note, Cell::I32(counter)] = table_row.values() else {
            return None;
        };
        let note = match note {
            Cell::Null => None,
            Cell::String(note) => Some(note.clone()),
            _ => return None,
        };

        Some(Self { id: *id, note, counter: *counter })
    }

    fn id(&self) -> Self::Id {
        self.id
    }

    fn id_from_key_row(key_row: &TableRow) -> Option<Self::Id> {
        let [Cell::I64(id)] = key_row.values() else {
            return None;
        };

        Some(*id)
    }
}

/// Reads the committed rows of every workload table.
pub async fn read_source_rows(
    client: &Client,
    tables: &WorkloadTables,
) -> Result<[BTreeMap<i64, WorkloadRecord>; TABLE_COUNT], TestCaseError> {
    let mut source = <[BTreeMap<i64, WorkloadRecord>; TABLE_COUNT]>::default();

    for (table, rows) in source.iter_mut().enumerate() {
        let query = format!("select id, note, counter from {} order by id", tables.quoted(table));
        let result = client
            .query(&query, &[])
            .await
            .map_err(|error| TestCaseError::fail(format!("failed to read source rows: {error}")))?;
        for row in result {
            let record = WorkloadRecord { id: row.get(0), note: row.get(1), counter: row.get(2) };
            rows.insert(record.id, record);
        }
    }

    Ok(source)
}

/// Returns the tables a data event changes.
fn data_event_table_ids(event: &Event) -> Vec<TableId> {
    match event {
        Event::Insert(insert) => vec![insert.replicated_table_schema.id()],
        Event::Update(update) => vec![update.replicated_table_schema.id()],
        Event::Delete(delete) => vec![delete.replicated_table_schema.id()],
        Event::Truncate(truncate) => {
            truncate.truncated_tables.iter().map(ReplicatedTableSchema::id).collect()
        }
        _ => Vec::new(),
    }
}

/// Renders the replay-relevant payload of a data event.
fn data_event_payload(event: &Event) -> Option<(PgLsn, u64, String)> {
    let old_row = |row: &Option<OldTableRow>| match row {
        Some(OldTableRow::Full(row)) => format!("full {:?}", row.values()),
        Some(OldTableRow::Key(row)) => format!("key {:?}", row.values()),
        None => "none".to_owned(),
    };

    match event {
        Event::Insert(insert) => Some((
            insert.commit_lsn,
            insert.tx_ordinal,
            format!(
                "insert {} {:?}",
                insert.replicated_table_schema.id(),
                insert.table_row.values()
            ),
        )),
        Event::Update(update) => {
            let new_row = match &update.updated_table_row {
                UpdatedTableRow::Full(row) => format!("full {:?}", row.values()),
                UpdatedTableRow::Partial(row) => {
                    format!("partial {:?} missing {:?}", row.values(), row.missing_column_indexes())
                }
            };
            Some((
                update.commit_lsn,
                update.tx_ordinal,
                format!(
                    "update {} {new_row} old {}",
                    update.replicated_table_schema.id(),
                    old_row(&update.old_table_row)
                ),
            ))
        }
        Event::Delete(delete) => Some((
            delete.commit_lsn,
            delete.tx_ordinal,
            format!(
                "delete {} old {}",
                delete.replicated_table_schema.id(),
                old_row(&delete.old_table_row)
            ),
        )),
        Event::Truncate(truncate) => {
            let table_ids: Vec<_> =
                truncate.truncated_tables.iter().map(ReplicatedTableSchema::id).collect();
            Some((truncate.commit_lsn, truncate.tx_ordinal, format!("truncate {table_ids:?}")))
        }
        _ => None,
    }
}

/// Checks that `events` contain every data event of each committed `phase`
/// transaction exactly once.
///
/// Use it on the history recorded after a restart to prove that an
/// unacknowledged transaction replays in full without duplication, however
/// its events were split across destination writes.
pub fn assert_phase_delivered_once(
    events: &[Event],
    history: &WorkloadHistory,
    phase: TxPhase,
) -> Result<(), TestCaseError> {
    for tx in history.committed.iter().filter(|tx| tx.phase == phase) {
        let commit_lsn = events
            .iter()
            .find_map(|event| match event {
                Event::Begin(begin) if begin.xid == tx.xid => Some(begin.commit_lsn),
                _ => None,
            })
            .ok_or_else(|| {
                TestCaseError::fail(format!("{phase:?} transaction {} was not replayed", tx.xid))
            })?;

        let mut counts = BTreeMap::<u64, usize>::new();
        for (event_commit_lsn, tx_ordinal, _) in events.iter().filter_map(data_event_payload) {
            if event_commit_lsn == commit_lsn {
                *counts.entry(tx_ordinal).or_default() += 1;
            }
        }

        prop_assert_eq!(
            counts.len(),
            tx.event_count,
            "{:?} transaction {} replayed the wrong number of data events",
            phase,
            tx.xid
        );
        prop_assert!(
            counts.values().all(|count| *count == 1),
            "{:?} transaction {} replayed an event more than once: {:?}",
            phase,
            tx.xid,
            counts
        );
    }

    Ok(())
}

/// Cases with a `DEFAULT` identity table a property must pass before it must
/// have seen a partial update.
///
/// 300-second runs passed 33 to 50 cases with 14 to 17 partial updates, so
/// missing every one across 30 cases meant the TOAST path stopped firing, not
/// bad luck. About three in four cases have a `DEFAULT` identity table, so 22
/// of them match the earlier 30-case gate. Short default budgets pass fewer
/// cases and skip the check, and so do shapes without partial updates.
const MIN_CASES_FOR_PARTIAL_UPDATE_CHECK: usize = 22;

/// Counts how often passing cases reached the workload's hard paths.
///
/// A property can pass without exercising a path at all; for example, a
/// large note small enough to be stored inline stops producing partial
/// updates. The counts make that visible.
#[derive(Debug, Default)]
pub struct WorkloadCoverage {
    /// Passing cases.
    cases: AtomicUsize,
    /// Passing cases with at least one `DEFAULT` identity table, the only
    /// cases that can produce partial updates.
    default_identity_cases: AtomicUsize,
    /// Committed transactions.
    committed_transactions: AtomicUsize,
    /// Rolled-back transactions.
    rolled_back_transactions: AtomicUsize,
    /// Transactions left open across the fault that committed afterwards.
    straddle_commits: AtomicUsize,
    /// Updates delivered as partial rows because of unchanged TOAST.
    partial_updates: AtomicUsize,
    /// Updates that changed the primary key.
    key_changes: AtomicUsize,
    /// Truncate events.
    truncates: AtomicUsize,
    /// Held writes replayed exactly once after a restart.
    held_replays: AtomicUsize,
}

impl WorkloadCoverage {
    /// Records a held write that replayed exactly once after a restart.
    pub fn record_held_replay(&self) {
        self.held_replays.fetch_add(1, Ordering::Relaxed);
    }

    /// Prints the counts and checks that partial updates were reached.
    ///
    /// # Panics
    ///
    /// Panics when at least [`MIN_CASES_FOR_PARTIAL_UPDATE_CHECK`] cases with
    /// a `DEFAULT` identity table passed without a single partial update.
    pub fn report(&self, property: &str) {
        let default_identity_cases = self.default_identity_cases.load(Ordering::Relaxed);
        let partial_updates = self.partial_updates.load(Ordering::Relaxed);
        println!("property '{property}' coverage: {self:?}");

        assert!(
            default_identity_cases < MIN_CASES_FOR_PARTIAL_UPDATE_CHECK || partial_updates > 0,
            "property '{property}' passed {default_identity_cases} cases with a DEFAULT identity \
             table without a partial update: {self:?}"
        );
    }

    /// Adds one passing case's tables, history, and first deliveries.
    fn record(
        &self,
        tables: &WorkloadTables,
        history: &WorkloadHistory,
        first_deliveries: &[Event],
    ) {
        let add = |counter: &AtomicUsize, count: usize| {
            counter.fetch_add(count, Ordering::Relaxed);
        };

        add(&self.cases, 1);
        add(&self.default_identity_cases, usize::from(tables.has_default_identity()));
        add(&self.committed_transactions, history.committed.len());
        add(&self.rolled_back_transactions, history.rolled_back.len());
        add(&self.straddle_commits, usize::from(history.straddle_committed));

        for event in first_deliveries {
            match event {
                Event::Update(update) => {
                    if matches!(update.updated_table_row, UpdatedTableRow::Partial(_)) {
                        add(&self.partial_updates, 1);
                    }
                    if update_changes_key(update) {
                        add(&self.key_changes, 1);
                    }
                }
                Event::Truncate(_) => add(&self.truncates, 1),
                _ => {}
            }
        }
    }
}

/// Returns whether an update moved its row to a new primary key.
///
/// The primary key is the first column of every workload row and key image,
/// and it is never unchanged TOAST, so it is always the first present value.
fn update_changes_key(update: &UpdateEvent) -> bool {
    let old_id = match &update.old_table_row {
        Some(OldTableRow::Full(row) | OldTableRow::Key(row)) => row.values().first(),
        None => return false,
    };
    let new_id = match &update.updated_table_row {
        UpdatedTableRow::Full(row) => row.values().first(),
        UpdatedTableRow::Partial(row) => row.values().first(),
    };

    old_id != new_id
}

/// Source order that first deliveries of data events must follow.
#[derive(Clone, Copy, Debug)]
pub enum DeliveryOrder {
    /// One apply stream delivers every table, so all first deliveries follow
    /// source position order.
    Source,
    /// Each table follows source position order, but tables may interleave: a
    /// table catching up after its copy delivers positions older than another
    /// table's latest.
    PerTable,
}

/// Checks destination history against the executed workload and source.
///
/// Every committed transaction must be delivered with exactly its planned
/// data events, each delivered a number of times within `deliveries(phase)`
/// with an identical payload on replay. When `deliveries(phase)` admits zero,
/// a table copy may have absorbed the transaction, so it may be missing or
/// partial. First deliveries must follow `order`.
/// Rolled-back and unknown transactions must not appear. Copied rows plus the
/// first delivery of each event must equal the committed source rows of every
/// table; replays are checked for identity instead of being applied again.
/// Passing cases are added to `coverage`.
#[expect(clippy::too_many_arguments)]
pub fn assert_workload_converged(
    tables: &WorkloadTables,
    copied_rows: &HashMap<TableId, Vec<TableRow>>,
    events: &[Event],
    source: &[BTreeMap<i64, WorkloadRecord>; TABLE_COUNT],
    history: &WorkloadHistory,
    deliveries: impl Fn(TxPhase) -> RangeInclusive<usize>,
    order: DeliveryOrder,
    coverage: &WorkloadCoverage,
) -> Result<(), TestCaseError> {
    let mut commit_lsns = BTreeMap::<u32, PgLsn>::new();
    for event in events {
        let Event::Begin(begin) = event else {
            continue;
        };
        let previous = commit_lsns.insert(begin.xid, begin.commit_lsn);
        prop_assert!(
            previous.is_none_or(|previous| previous == begin.commit_lsn),
            "transaction {} was replayed at a different commit LSN",
            begin.xid
        );
    }

    for xid in &history.rolled_back {
        prop_assert!(!commit_lsns.contains_key(xid), "rolled-back transaction {xid} was delivered");
    }

    // Delivery count and first payload per data event position, plus the
    // first deliveries in arrival order for materialization.
    let mut positions = BTreeMap::<(PgLsn, u64), (String, usize)>::new();
    // Last first-delivered position per ordered stream: one shared stream for
    // source order, one per table otherwise.
    let mut last_positions = BTreeMap::<Option<TableId>, (PgLsn, u64)>::new();
    let mut first_deliveries = Vec::new();
    for event in events {
        let Some((commit_lsn, tx_ordinal, payload)) = data_event_payload(event) else {
            continue;
        };
        match positions.get_mut(&(commit_lsn, tx_ordinal)) {
            Some((first_payload, count)) => {
                prop_assert!(
                    *first_payload == payload,
                    "replayed event {commit_lsn}/{tx_ordinal} changed its payload"
                );
                *count += 1;
            }
            None => {
                let streams = match order {
                    DeliveryOrder::Source => vec![None],
                    DeliveryOrder::PerTable => {
                        data_event_table_ids(event).into_iter().map(Some).collect()
                    }
                };
                for stream in streams {
                    if let Some((last_lsn, last_ordinal)) =
                        last_positions.insert(stream, (commit_lsn, tx_ordinal))
                    {
                        prop_assert!(
                            (last_lsn, last_ordinal) < (commit_lsn, tx_ordinal),
                            "event {commit_lsn}/{tx_ordinal} was first delivered after \
                             {last_lsn}/{last_ordinal} ({order:?} order)"
                        );
                    }
                }
                positions.insert((commit_lsn, tx_ordinal), (payload, 1));
                first_deliveries.push(event.clone());
            }
        }
    }

    let mut expected_lsns = BTreeSet::new();
    for tx in &history.committed {
        let allowed = deliveries(tx.phase);
        // A range that admits zero deliveries marks transactions a table copy
        // may absorb, so they can be missing or reach only some tables.
        let may_be_absorbed = allowed.contains(&0);
        let Some(&commit_lsn) = commit_lsns.get(&tx.xid) else {
            prop_assert!(
                may_be_absorbed,
                "{:?} transaction {} was never delivered",
                tx.phase,
                tx.xid
            );
            continue;
        };
        expected_lsns.insert(commit_lsn);

        let tx_positions: Vec<usize> = positions
            .range((commit_lsn, 0)..=(commit_lsn, u64::MAX))
            .map(|(_, (_, count))| *count)
            .collect();
        if !may_be_absorbed {
            prop_assert_eq!(
                tx_positions.len(),
                tx.event_count,
                "{:?} transaction {} delivered the wrong number of data events",
                tx.phase,
                tx.xid
            );
        }

        for count in tx_positions {
            prop_assert!(
                allowed.contains(&count),
                "{:?} transaction {} had an event delivered {} times; expected {:?}",
                tx.phase,
                tx.xid,
                count,
                allowed
            );
        }
    }

    for (commit_lsn, tx_ordinal) in positions.keys() {
        prop_assert!(
            expected_lsns.contains(commit_lsn),
            "event {commit_lsn}/{tx_ordinal} belongs to no committed workload transaction"
        );
    }

    for (table, table_id) in tables.ids.iter().enumerate() {
        let copied = copied_rows.get(table_id).map_or(&[][..], Vec::as_slice);
        let materialized = materialize_events::<WorkloadRecord>(
            copied,
            &first_deliveries,
            *table_id,
        )
        .map_err(|error| {
            TestCaseError::fail(format!("table {table} history did not materialize: {error}"))
        })?;
        prop_assert_eq!(
            &materialized,
            &source[table],
            "table {} destination state did not converge to source",
            table
        );
    }

    coverage.record(tables, history, &first_deliveries);

    Ok(())
}
