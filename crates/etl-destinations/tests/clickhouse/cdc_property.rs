//! Generated CDC histories and SQL quoting round-trips through the ClickHouse
//! destination.
//!
//! The CDC properties generate a copy snapshot and a valid Postgres change
//! history over a small primary-key domain, write it through the production
//! destination, simulate a crash and at-least-once redelivery, and compare the
//! reconstructed current state with an in-memory model of the source table.
//!
//! The quoting properties create columns whose source names and text defaults
//! contain SQL-sensitive characters, and use ClickHouse itself as the oracle
//! for what the destination's DDL and inserts meant.
//!
//! Every property runs new random cases until a wall-clock budget elapses,
//! using the shared runner in `etl::test_utils::property`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use etl::{
    data::{Cell, OldTableRow, TableRow, UpdatedTableRow},
    error::EtlResult,
    event::{BeginEvent, CommitEvent, DeleteEvent, Event, InsertEvent, TruncateEvent, UpdateEvent},
    schema::{
        ColumnSchema, IdentityMask, PgLsn, ReplicatedTableSchema, ReplicationMask, TableId,
        TableName, TableSchema, Type,
    },
    store::{MemoryStore, SchemaStore},
    test_utils::property::{block_on, pg_text, run_expensive_property},
};
use etl_config::shared::ClickHouseEngine;
use etl_destinations::clickhouse::{
    ClickHouseDestination, ClickHouseInserterConfig,
    test_utils::{ClickHouseTestDatabase, redact_clickhouse_error, setup_clickhouse_database},
};
use etl_telemetry::tracing::init_test_tracing;
use proptest::{option, prelude::*, sample::select};

use crate::support::crypto::install_crypto_provider;

/// Source primary key `(tenant, id)` in primary-key ordinal order.
type Key = (i32, i64);

/// Every primary key a generated history may use.
///
/// The domain is small so generated histories reuse, move, and delete the same
/// keys often.
const KEYS: [Key; 6] = [(0, 1), (0, 2), (0, 3), (1, 1), (1, 2), (1, 3)];

/// Largest number of generated steps in one history.
const MAX_STEPS: usize = 32;

/// One current-state row read back as `(tenant, id, value, amount)`.
type CurrentRow = (i32, i64, Option<String>, i64);

/// Non-key column values of one source row.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RowValues {
    value: Option<String>,
    amount: i64,
}

/// Source replica identity, which decides the old-row images Postgres sends.
#[derive(Debug, Clone, Copy)]
enum Identity {
    /// `REPLICA IDENTITY DEFAULT`: deletes and key changes send the old key,
    /// updates that keep the key send no old row.
    PrimaryKey,
    /// `REPLICA IDENTITY FULL`: updates and deletes send the full old row.
    Full,
}

/// One abstract change, resolved against the model state when the history is
/// built so every resolved change is valid for a table with a primary key.
///
/// Slots pick a key from the present or absent keys by index modulo their
/// count.
#[derive(Debug, Clone)]
enum OpChoice {
    /// Inserts an absent key, or updates a present key when none is absent.
    Insert { slot: usize, row: RowValues },
    /// Updates a present key, moving it to an absent key when `new_slot` is
    /// set and one exists. Inserts when no key is present.
    Update { slot: usize, new_slot: Option<usize>, row: RowValues },
    /// Deletes a present key, or inserts when no key is present.
    Delete { slot: usize, row: RowValues },
    /// Moves a present key to an absent key and then inserts the freed key in
    /// the same transaction.
    MoveAndReuse { slot: usize, new_slot: usize, moved: RowValues, reused: RowValues },
    /// Truncates the table.
    Truncate,
}

/// One generated step with its position gaps and transaction boundary.
#[derive(Debug, Clone)]
struct StepChoice {
    op: OpChoice,
    /// Whether the transaction commits after this step.
    commit_after: bool,
    /// Commit LSN distance from the previous transaction, used when this step
    /// opens a transaction.
    lsn_gap: u64,
    /// Ordinal distance before each event this step emits.
    ordinal_gap: u64,
}

/// One generated CDC scenario.
#[derive(Debug, Clone)]
struct CdcCase {
    identity: Identity,
    /// Copied row for each key in [`KEYS`], if the key exists at copy time.
    copy: Vec<Option<RowValues>>,
    /// Number of copy rows in the first copy batch, which is redelivered.
    copy_split: usize,
    lsn_base: u64,
    steps: Vec<StepChoice>,
    /// Raw batch boundaries, reduced modulo the event count.
    batch_cuts: Vec<usize>,
    /// Raw number of events written before the simulated crash.
    crash_choice: usize,
    /// Raw choice of the transaction the restarted stream resumes from.
    replay_choice: usize,
    /// Raw transaction range redelivered late, after the full stream.
    stale_choice: (usize, usize),
    max_bytes_per_insert: u64,
}

/// A resolved change history and the source state it leaves behind.
struct History {
    events: Vec<Event>,
    /// Index of each transaction's `Begin` event.
    tx_starts: Vec<usize>,
    /// Whether each transaction contains a truncate.
    tx_truncates: Vec<bool>,
    /// Human-readable history for failure messages.
    log: String,
    /// Source rows after the history, ordered by primary key.
    expected: Vec<CurrentRow>,
}

/// Resolves [`StepChoice`]s into valid events while tracking the source table.
struct HistoryBuilder<'a> {
    schema: &'a ReplicatedTableSchema,
    identity: Identity,
    model: BTreeMap<Key, RowValues>,
    events: Vec<Event>,
    tx_starts: Vec<usize>,
    tx_truncates: Vec<bool>,
    log: String,
    /// Commit LSN of the latest opened transaction.
    lsn: u64,
    /// Ordinal of the latest event in the open transaction, if one is open.
    open_ordinal: Option<u64>,
}

impl<'a> HistoryBuilder<'a> {
    /// Starts a history over the copied source rows.
    fn new(
        schema: &'a ReplicatedTableSchema,
        identity: Identity,
        model: BTreeMap<Key, RowValues>,
        lsn_base: u64,
    ) -> Self {
        Self {
            schema,
            identity,
            model,
            events: Vec::new(),
            tx_starts: Vec::new(),
            tx_truncates: Vec::new(),
            log: String::new(),
            lsn: lsn_base,
            open_ordinal: None,
        }
    }

    /// Opens a transaction unless one is already open.
    fn begin_if_needed(&mut self, lsn_gap: u64) {
        if self.open_ordinal.is_some() {
            return;
        }
        self.lsn += lsn_gap;
        self.tx_starts.push(self.events.len());
        self.tx_truncates.push(false);
        self.events.push(Event::Begin(BeginEvent {
            commit_lsn: PgLsn::from(self.lsn),
            tx_ordinal: 0,
            timestamp: 0,
            xid: u32::try_from(self.tx_starts.len()).unwrap(),
        }));
        self.open_ordinal = Some(0);
        write!(self.log, "\ntx lsn={}:", self.lsn).unwrap();
    }

    /// Returns the next ordinal in the open transaction.
    fn next_ordinal(&mut self, gap: u64) -> u64 {
        let ordinal = self.open_ordinal.unwrap() + gap;
        self.open_ordinal = Some(ordinal);
        ordinal
    }

    /// Commits the open transaction, if any.
    fn commit(&mut self) {
        if let Some(ordinal) = self.open_ordinal.take() {
            self.events.push(Event::Commit(CommitEvent {
                commit_lsn: PgLsn::from(self.lsn),
                tx_ordinal: ordinal + 1,
                flags: 0,
                end_lsn: PgLsn::from(self.lsn + 1),
                timestamp: 0,
            }));
        }
    }

    /// Keys currently present in the source table.
    fn present(&self) -> Vec<Key> {
        self.model.keys().copied().collect()
    }

    /// Keys currently absent from the source table.
    fn absent(&self) -> Vec<Key> {
        KEYS.iter().copied().filter(|key| !self.model.contains_key(key)).collect()
    }

    /// Applies one step.
    fn apply(&mut self, step: &StepChoice) {
        self.begin_if_needed(step.lsn_gap);
        let present = self.present();
        let absent = self.absent();
        let gap = step.ordinal_gap;
        match &step.op {
            OpChoice::Insert { slot, row } => {
                if absent.is_empty() {
                    let key = present[slot % present.len()];
                    self.update(key, key, row.clone(), gap);
                } else {
                    self.insert(absent[slot % absent.len()], row.clone(), gap);
                }
            }
            OpChoice::Update { slot, new_slot, row } => {
                if present.is_empty() {
                    self.insert(absent[slot % absent.len()], row.clone(), gap);
                } else {
                    let key = present[slot % present.len()];
                    let new_key = match new_slot {
                        Some(new_slot) if !absent.is_empty() => absent[new_slot % absent.len()],
                        _ => key,
                    };
                    self.update(key, new_key, row.clone(), gap);
                }
            }
            OpChoice::Delete { slot, row } => {
                if present.is_empty() {
                    self.insert(absent[slot % absent.len()], row.clone(), gap);
                } else {
                    self.delete(present[slot % present.len()], gap);
                }
            }
            OpChoice::MoveAndReuse { slot, new_slot, moved, reused } => {
                if present.is_empty() {
                    self.insert(absent[slot % absent.len()], moved.clone(), gap);
                } else if absent.is_empty() {
                    let key = present[slot % present.len()];
                    self.update(key, key, moved.clone(), gap);
                } else {
                    let key = present[slot % present.len()];
                    self.update(key, absent[new_slot % absent.len()], moved.clone(), gap);
                    self.insert(key, reused.clone(), gap);
                }
            }
            OpChoice::Truncate => {
                let tx_ordinal = self.next_ordinal(gap);
                self.events.push(Event::Truncate(TruncateEvent {
                    commit_lsn: PgLsn::from(self.lsn),
                    tx_ordinal,
                    options: 0,
                    truncated_tables: vec![self.schema.clone()],
                }));
                *self.tx_truncates.last_mut().unwrap() = true;
                self.model.clear();
                self.log.push_str(" truncate;");
            }
        }
        if step.commit_after {
            self.commit();
        }
    }

    /// Emits an insert of an absent key.
    fn insert(&mut self, key: Key, row: RowValues, gap: u64) {
        let tx_ordinal = self.next_ordinal(gap);
        self.events.push(Event::Insert(InsertEvent {
            commit_lsn: PgLsn::from(self.lsn),
            tx_ordinal,
            replicated_table_schema: self.schema.clone(),
            table_row: full_row(key, &row),
        }));
        write!(self.log, " ins {key:?}={row:?};").unwrap();
        self.model.insert(key, row);
    }

    /// Emits an update of a present key, possibly moving it to an absent key.
    fn update(&mut self, key: Key, new_key: Key, row: RowValues, gap: u64) {
        let tx_ordinal = self.next_ordinal(gap);
        let old = self.model.remove(&key).unwrap();
        let old_table_row = match self.identity {
            Identity::Full => Some(OldTableRow::Full(full_row(key, &old))),
            Identity::PrimaryKey if key != new_key => Some(OldTableRow::Key(key_row(key))),
            Identity::PrimaryKey => None,
        };
        self.events.push(Event::Update(UpdateEvent {
            commit_lsn: PgLsn::from(self.lsn),
            tx_ordinal,
            replicated_table_schema: self.schema.clone(),
            updated_table_row: UpdatedTableRow::Full(full_row(new_key, &row)),
            old_table_row,
        }));
        write!(self.log, " upd {key:?}->{new_key:?}={row:?};").unwrap();
        self.model.insert(new_key, row);
    }

    /// Emits a delete of a present key.
    fn delete(&mut self, key: Key, gap: u64) {
        let tx_ordinal = self.next_ordinal(gap);
        let old = self.model.remove(&key).unwrap();
        let old_table_row = match self.identity {
            Identity::Full => OldTableRow::Full(full_row(key, &old)),
            Identity::PrimaryKey => OldTableRow::Key(key_row(key)),
        };
        self.events.push(Event::Delete(DeleteEvent {
            commit_lsn: PgLsn::from(self.lsn),
            tx_ordinal,
            replicated_table_schema: self.schema.clone(),
            old_table_row: Some(old_table_row),
        }));
        write!(self.log, " del {key:?};").unwrap();
    }

    /// Commits the last transaction and returns the finished history.
    fn finish(mut self) -> History {
        self.commit();
        let expected = self
            .model
            .into_iter()
            .map(|((tenant, id), row)| (tenant, id, row.value, row.amount))
            .collect();
        History {
            events: self.events,
            tx_starts: self.tx_starts,
            tx_truncates: self.tx_truncates,
            log: self.log,
            expected,
        }
    }
}

/// Builds a full source row in table column order `(id, tenant, value,
/// amount)`.
fn full_row((tenant, id): Key, row: &RowValues) -> TableRow {
    TableRow::new(vec![
        Cell::I64(id),
        Cell::I32(tenant),
        row.value.clone().map_or(Cell::Null, Cell::String),
        Cell::I64(row.amount),
    ])
}

/// Builds a key-only old row: primary-key values in table column order, as the
/// Postgres decoder normalizes key images.
fn key_row((tenant, id): Key) -> TableRow {
    TableRow::new(vec![Cell::I64(id), Cell::I32(tenant)])
}

/// Builds the CDC test schema.
///
/// The primary key is `(tenant, id)` while the table lists `id` first, so key
/// images and the ClickHouse sort key must follow primary-key ordinals rather
/// than column order. `amount` is `NOT NULL`, so key-only tombstones must fill
/// it with a placeholder that never reaches the current state.
fn cdc_schema(table_id: u32, table: &str, identity: Identity) -> ReplicatedTableSchema {
    let table_schema = Arc::new(TableSchema::new(
        TableId::new(table_id),
        TableName::new("public".to_owned(), table.to_owned()),
        vec![
            ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(2),
            ColumnSchema::new("tenant".to_owned(), Type::INT4, -1, 2, false).with_primary_key(1),
            ColumnSchema::new("value".to_owned(), Type::TEXT, -1, 3, true),
            ColumnSchema::new("amount".to_owned(), Type::INT8, -1, 4, false),
        ],
    ));
    match identity {
        Identity::PrimaryKey => ReplicatedTableSchema::all(table_schema),
        Identity::Full => {
            let replication_mask = ReplicationMask::all(&table_schema);
            let identity_mask = IdentityMask::from_bytes(vec![1; 4]);
            ReplicatedTableSchema::from_masks(table_schema, replication_mask, identity_mask)
        }
    }
}

/// Non-key values from small domains, so updates that rewrite equal values and
/// text that needs escaping both occur.
fn row_values() -> impl Strategy<Value = RowValues> {
    let value = option::of(prop_oneof![
        3 => select(vec!["", "a", "b", "it's", "back\\slash", "line\nbreak"]).prop_map(str::to_owned),
        1 => pg_text(),
    ]);
    let amount = prop_oneof![-2i64..3, Just(i64::MIN), Just(i64::MAX)];
    (value, amount).prop_map(|(value, amount)| RowValues { value, amount })
}

/// Generated steps, weighted toward key reuse and multi-event transactions.
fn step_choice() -> impl Strategy<Value = StepChoice> {
    let slot = 0usize..KEYS.len();
    let op = prop_oneof![
        4 => (slot.clone(), row_values()).prop_map(|(slot, row)| OpChoice::Insert { slot, row }),
        4 => (slot.clone(), option::weighted(0.4, slot.clone()), row_values())
            .prop_map(|(slot, new_slot, row)| OpChoice::Update { slot, new_slot, row }),
        3 => (slot.clone(), row_values()).prop_map(|(slot, row)| OpChoice::Delete { slot, row }),
        2 => (slot.clone(), slot, row_values(), row_values()).prop_map(
            |(slot, new_slot, moved, reused)| OpChoice::MoveAndReuse { slot, new_slot, moved, reused }
        ),
        1 => Just(OpChoice::Truncate),
    ];
    let lsn_gap = prop_oneof![4 => 2u64..100, 1 => 2u64..(1 << 32)];
    let ordinal_gap = prop_oneof![9 => Just(1u64), 1 => 1u64..(1 << 33)];
    (op, any::<bool>(), lsn_gap, ordinal_gap).prop_map(
        |(op, commit_after, lsn_gap, ordinal_gap)| StepChoice {
            op,
            commit_after,
            lsn_gap,
            ordinal_gap,
        },
    )
}

/// Generated CDC scenarios.
///
/// LSNs start either small or anywhere below the top of the `u64` range,
/// leaving room for every generated gap, so packed versions see high bits.
fn cdc_case() -> impl Strategy<Value = CdcCase> {
    let identity = prop_oneof![Just(Identity::PrimaryKey), Just(Identity::Full)];
    let copy = prop::collection::vec(option::of(row_values()), KEYS.len());
    let lsn_base = prop_oneof![1u64..1_000, 1u64..(u64::MAX - (1 << 40))];
    let steps = prop::collection::vec(step_choice(), 1..=MAX_STEPS);
    let batch_cuts = prop::collection::vec(any::<usize>(), 0..4);
    let max_bytes = prop_oneof![3 => Just(100 * 1024 * 1024), 1 => Just(1u64)];
    (
        (identity, copy, any::<usize>(), lsn_base, steps),
        (batch_cuts, any::<usize>(), any::<usize>(), (any::<usize>(), any::<usize>()), max_bytes),
    )
        .prop_map(
            |(
                (identity, copy, copy_split, lsn_base, steps),
                (batch_cuts, crash_choice, replay_choice, stale_choice, max_bytes_per_insert),
            )| CdcCase {
                identity,
                copy,
                copy_split,
                lsn_base,
                steps,
                batch_cuts,
                crash_choice,
                replay_choice,
                stale_choice,
                max_bytes_per_insert,
            },
        )
}

/// One ClickHouse database receiving one fresh table per generated case.
struct CdcHarness {
    database: ClickHouseTestDatabase,
    store: MemoryStore,
    engine: ClickHouseEngine,
    next_table: AtomicU32,
}

impl CdcHarness {
    /// Creates an isolated database for one engine.
    async fn create(engine: ClickHouseEngine) -> Self {
        init_test_tracing();
        install_crypto_provider();
        Self {
            database: setup_clickhouse_database().await,
            store: MemoryStore::new(),
            engine,
            next_table: AtomicU32::new(1),
        }
    }

    /// Builds a destination, as a pipeline (re)start does.
    async fn destination(&self, max_bytes_per_insert: u64) -> ClickHouseDestination<MemoryStore> {
        let config = ClickHouseInserterConfig { engine: self.engine, max_bytes_per_insert };
        self.database.build_destination_with_config(self.store.clone(), config).await
    }

    /// Reads the reconstructed current state of `table`.
    ///
    /// ReplacingMergeTree reads the destination's `__current` view.
    /// MergeTree keeps the latest event per key by `(cdc_lsn,
    /// cdc_tx_ordinal)` and drops keys whose latest event is a delete. The
    /// latest values travel in one tuple because `argMax` skips `NULL`
    /// arguments, which would surface an older non-null `value`.
    async fn current_state(&self, table: &str) -> Vec<CurrentRow> {
        let sql = match self.engine {
            ClickHouseEngine::ReplacingMergeTree => format!(
                "select tenant, id, value, amount from \"{table}__current\" order by tenant, id"
            ),
            ClickHouseEngine::MergeTree => format!(
                "select tenant, id, latest.1, latest.2 from (select tenant, id, \
                 argMax(tuple(value, amount, cdc_operation), tuple(cdc_lsn, cdc_tx_ordinal)) as \
                 latest from \"{table}\" group by tenant, id) where latest.3 != 'DELETE' order by \
                 tenant, id"
            ),
        };
        self.database.query::<CurrentRow>(&sql).await
    }

    /// Runs one generated case and checks the reconstructed state.
    async fn check(&self, case: &CdcCase) -> Result<(), TestCaseError> {
        let table_id = self.next_table.fetch_add(1, Ordering::Relaxed);
        let source_table = format!("cdc{table_id}");
        let clickhouse_table = format!("public_{source_table}");
        let schema = cdc_schema(table_id, &source_table, case.identity);
        self.store.store_table_schema(schema.inner().clone()).await.unwrap();

        let copy_rows: Vec<(Key, RowValues)> = KEYS
            .iter()
            .zip(&case.copy)
            .filter_map(|(key, row)| row.clone().map(|row| (*key, row)))
            .collect();
        let mut builder = HistoryBuilder::new(
            &schema,
            case.identity,
            copy_rows.iter().cloned().collect(),
            case.lsn_base,
        );
        for step in &case.steps {
            builder.apply(step);
        }
        let history = builder.finish();
        let events = &history.events;

        let len = events.len();
        let cuts: BTreeSet<usize> = case.batch_cuts.iter().map(|cut| cut % len).collect();
        let crash = case.crash_choice % (len + 1);
        let resumable = history.tx_starts.iter().filter(|start| **start <= crash).count().max(1);
        let replay_from = history.tx_starts[case.replay_choice % resumable];
        // Late duplicates come only from transactions after the last
        // truncate. A truncate erases the newer versions that make an older
        // event lose, so a pre-truncate transaction delivered after it would
        // come back. Production never does that: a restart redelivers from a
        // transaction boundary through the end, so a replayed pre-truncate
        // transaction is always followed by the truncate again, which the
        // redelivery phase covers.
        let tx_count = history.tx_starts.len();
        let first_after_truncate =
            history.tx_truncates.iter().rposition(|truncates| *truncates).map_or(0, |tx| tx + 1);
        let stale_range = (first_after_truncate < tx_count).then(|| {
            let stale_first =
                first_after_truncate + case.stale_choice.0 % (tx_count - first_after_truncate);
            let stale_last = stale_first + case.stale_choice.1 % (tx_count - stale_first);
            let stale_end = history.tx_starts.get(stale_last + 1).copied().unwrap_or(len);
            history.tx_starts[stale_first]..stale_end
        });
        let plan = format!(
            "{case:?}\nhistory:{}\nevents={len} cuts={cuts:?} crash={crash} \
             replay_from={replay_from} stale={stale_range:?}\nexpected={:?}",
            history.log, history.expected
        );
        let fail = |phase: &str, err: &dyn std::fmt::Display| {
            TestCaseError::fail(format!("{phase} failed: {err}\n{plan}"))
        };

        // Copy, with the first batch delivered twice as a retried copy batch
        // would be without a deduplication token.
        let copy_table_rows: Vec<TableRow> =
            copy_rows.iter().map(|(key, row)| full_row(*key, row)).collect();
        let split = case.copy_split % (copy_table_rows.len() + 1);
        let first = self.destination(case.max_bytes_per_insert).await;
        for batch in
            [&copy_table_rows[..split], &copy_table_rows[split..], &copy_table_rows[..split]]
        {
            first
                .write_table_rows(&schema, batch.to_vec())
                .await
                .map_err(|err| fail("copy", &err))?;
        }

        // Stream until the simulated crash, splitting batches anywhere,
        // including inside transactions.
        write_batches(&first, events, &cuts, 0..crash)
            .await
            .map_err(|err| fail("stream before crash", &err))?;
        drop(first);

        // A restarted pipeline resumes from a transaction boundary at or
        // before the crash and redelivers everything after it.
        let restarted = self.destination(case.max_bytes_per_insert).await;
        write_batches(&restarted, events, &cuts, replay_from..len)
            .await
            .map_err(|err| fail("redelivery", &err))?;
        let actual = self.current_state(&clickhouse_table).await;
        prop_assert_eq!(&actual, &history.expected, "after redelivery\n{}", plan);

        // A late duplicate of older transactions must not resurrect deleted
        // keys or roll back updates.
        if let Some(stale_range) = stale_range {
            write_batches(&restarted, events, &BTreeSet::new(), stale_range)
                .await
                .map_err(|err| fail("stale redelivery", &err))?;
            let actual = self.current_state(&clickhouse_table).await;
            prop_assert_eq!(&actual, &history.expected, "after stale redelivery\n{}", plan);
        }

        Ok(())
    }
}

/// Writes `events[range]` through the destination, starting a new batch at
/// every cut inside the range.
async fn write_batches(
    destination: &ClickHouseDestination<MemoryStore>,
    events: &[Event],
    cuts: &BTreeSet<usize>,
    range: std::ops::Range<usize>,
) -> EtlResult<()> {
    let mut start = range.start;
    for &cut in cuts.range(range.clone()).filter(|cut| **cut > range.start) {
        destination.write_events(events[start..cut].to_vec()).await?;
        start = cut;
    }
    if start < range.end {
        destination.write_events(events[start..range.end].to_vec()).await?;
    }
    Ok(())
}

/// Runs the CDC model property against one engine.
async fn cdc_history_matches_source_model(engine: ClickHouseEngine) {
    let harness = CdcHarness::create(engine).await;
    run_expensive_property(
        &format!("clickhouse {engine:?} cdc history matches source model"),
        &cdc_case(),
        |case| block_on(harness.check(case)),
    );
}

/// ReplacingMergeTree's current view equals the source table after any copy,
/// change history, crash, and redelivery.
#[tokio::test(flavor = "multi_thread")]
async fn cdc_history_matches_source_model_replacing_merge_tree() {
    cdc_history_matches_source_model(ClickHouseEngine::ReplacingMergeTree).await;
}

/// The latest MergeTree event per key equals the source table after any copy,
/// change history, crash, and redelivery.
#[tokio::test(flavor = "multi_thread")]
async fn cdc_history_matches_source_model_merge_tree() {
    cdc_history_matches_source_model(ClickHouseEngine::MergeTree).await;
}

/// Fragments that SQL lexers, escape decoders, or client-side templating may
/// treat specially.
const SQL_FRAGMENTS: [&str; 38] = [
    "?",
    "??",
    "\\",
    "'",
    "''",
    "\"",
    "`",
    "fields",
    "{",
    "}",
    "{0}",
    "{name:String}",
    "\u{8}",
    "\u{c}",
    "\u{1b}",
    "\u{b}",
    "\n",
    "\r",
    "\t",
    "\\x41",
    "\\N",
    "\\0",
    "\\u0041",
    "$1",
    "::",
    "select ",
    "--",
    "/*",
    "*/",
    ";",
    ".",
    " ",
    "a",
    "Z",
    "é",
    "\u{7f}",
    "\u{feff}",
    "\u{2028}",
];

/// Text that is valid in Postgres and leans toward SQL-sensitive characters.
fn sql_text() -> impl Strategy<Value = String> {
    prop_oneof![
        1 => pg_text(),
        2 => prop::collection::vec(select(SQL_FRAGMENTS.to_vec()), 0..10)
            .prop_map(|fragments| fragments.concat()),
    ]
}

/// Postgres identifiers: non-empty and at most `NAMEDATALEN - 1` (63) bytes,
/// because Postgres truncates longer names.
fn pg_identifier() -> impl Strategy<Value = String> {
    sql_text()
        .prop_map(|mut name| {
            while name.len() > 63 {
                name.pop();
            }
            name
        })
        .prop_filter("postgres identifiers are non-empty", |name| !name.is_empty())
}

/// Column names the destination rejects by design because ETL owns them, plus
/// the generated primary key's name.
const RESERVED_COLUMN_NAMES: [&str; 6] =
    ["id", "cdc_operation", "cdc_lsn", "cdc_tx_ordinal", "_etl_version", "_etl_deleted"];

/// Distinct generated `(column name, text default)` pairs.
fn quoted_columns() -> impl Strategy<Value = Vec<(String, String)>> {
    prop::collection::vec((pg_identifier(), sql_text()), 1..=6)
        .prop_map(|columns| {
            let mut seen = BTreeSet::new();
            columns
                .into_iter()
                .filter(|(name, _)| {
                    !RESERVED_COLUMN_NAMES.contains(&name.as_str()) && seen.insert(name.clone())
                })
                .collect::<Vec<_>>()
        })
        .prop_filter("at least one usable column", |columns| !columns.is_empty())
}

/// Renders `value` as the default expression Postgres reports for a `text`
/// column, which doubles single quotes and keeps every other character as is
/// under `standard_conforming_strings`.
fn postgres_text_default(value: &str) -> String {
    format!("'{}'::text", value.replace('\'', "''"))
}

/// Returns whether the shared default parser drops `value` as non-portable.
///
/// The parser skips defaults containing `::` or `select ` even inside a
/// string literal, so ClickHouse evaluates the column's type default instead.
/// That loss is a deliberate conservative choice, not a quoting defect.
fn default_dropped_by_design(value: &str) -> bool {
    value.contains("::") || value.to_ascii_lowercase().contains("select ")
}

/// User-column values of one row read back in column position order.
#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct QuotedRow {
    id: i64,
    quoted_values: Vec<String>,
}

/// Runs the quoting property against one engine.
///
/// Each case creates a table whose text columns have generated names and
/// defaults, writes row 1 through the destination, inserts row 2 with only
/// `id` so ClickHouse evaluates every default, and reads both back. Reading
/// by position catches names that changed or moved, and the default values
/// expose any string literal ClickHouse parsed differently from the source.
async fn quoted_names_and_defaults_roundtrip(engine: ClickHouseEngine) {
    init_test_tracing();
    install_crypto_provider();
    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let destination = database.build_destination_with_engine(store.clone(), engine).await;
    let next_table = AtomicU32::new(1);

    run_expensive_property(
        &format!("clickhouse {engine:?} quoted names and defaults roundtrip"),
        &quoted_columns(),
        |columns| {
            block_on(async {
                let table_id = next_table.fetch_add(1, Ordering::Relaxed);
                let source_table = format!("quoted{table_id}");
                let clickhouse_table = format!("public_{source_table}");
                let mut column_schemas = vec![
                    ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false)
                        .with_primary_key(1),
                ];
                for (index, (name, default)) in columns.iter().enumerate() {
                    column_schemas.push(
                        ColumnSchema::new(
                            name.clone(),
                            Type::TEXT,
                            -1,
                            i32::try_from(index + 2).unwrap(),
                            false,
                        )
                        .with_default_expression(postgres_text_default(default)),
                    );
                }
                let table_schema = store
                    .store_table_schema(TableSchema::new(
                        TableId::new(table_id),
                        TableName::new("public".to_owned(), source_table),
                        column_schemas,
                    ))
                    .await
                    .unwrap();
                let schema = ReplicatedTableSchema::all(table_schema);
                let written: Vec<String> =
                    (0..columns.len()).map(|index| format!("written {index}")).collect();
                let mut cells = vec![Cell::I64(1)];
                cells.extend(written.iter().cloned().map(Cell::String));
                destination.write_table_rows(&schema, vec![TableRow::new(cells)]).await.map_err(
                    |err| TestCaseError::fail(format!("destination write failed: {err}")),
                )?;
                database
                    .db_client()
                    .query(&format!("insert into \"{clickhouse_table}\" (id) values (2)"))
                    .execute()
                    .await
                    .map_err(redact_clickhouse_error)
                    .unwrap();

                let mut expected_names = vec!["id".to_owned()];
                expected_names.extend(columns.iter().map(|(name, _)| name.clone()));
                prop_assert_eq!(&database.column_names(&clickhouse_table).await, &expected_names);

                // The current view exposes only user columns.
                let (relation, etl_columns) = match engine {
                    ClickHouseEngine::MergeTree => {
                        (clickhouse_table.clone(), ", cdc_operation, cdc_lsn, cdc_tx_ordinal")
                    }
                    ClickHouseEngine::ReplacingMergeTree => {
                        let view = format!("{clickhouse_table}__current");
                        prop_assert_eq!(&database.column_names(&view).await, &expected_names);
                        (view, "")
                    }
                };
                let rows = database
                    .query::<QuotedRow>(&format!(
                        "select id, array(* except (id{etl_columns})) as quoted_values from \
                         \"{relation}\" order by id"
                    ))
                    .await;
                prop_assert_eq!(rows.len(), 2);
                prop_assert_eq!(rows[0].id, 1);
                prop_assert_eq!(&rows[0].quoted_values, &written);
                prop_assert_eq!(rows[1].id, 2);
                for ((name, default), stored) in columns.iter().zip(&rows[1].quoted_values) {
                    if !(stored == default
                        || default_dropped_by_design(default) && stored.is_empty())
                    {
                        return Err(TestCaseError::fail(format!(
                            "column {name:?} default {default:?} evaluated to {stored:?}"
                        )));
                    }
                }
                Ok(())
            })
        },
    );
}

/// Generated column names and text defaults survive MergeTree DDL and inserts.
#[tokio::test(flavor = "multi_thread")]
async fn quoted_names_and_defaults_roundtrip_merge_tree() {
    quoted_names_and_defaults_roundtrip(ClickHouseEngine::MergeTree).await;
}

/// Generated column names and text defaults survive ReplacingMergeTree DDL,
/// inserts, and the current view.
#[tokio::test(flavor = "multi_thread")]
async fn quoted_names_and_defaults_roundtrip_replacing_merge_tree() {
    quoted_names_and_defaults_roundtrip(ClickHouseEngine::ReplacingMergeTree).await;
}

/// Creates `public.<table>` with `id` and one `NOT NULL` text column through a
/// MergeTree destination, inserts a row with only `id`, and returns the stored
/// column names and the value ClickHouse filled in from the column's default.
async fn create_text_column_and_read_default(
    table: &str,
    column_name: &str,
    default_value: &str,
) -> EtlResult<(Vec<String>, String)> {
    init_test_tracing();
    install_crypto_provider();
    let database = setup_clickhouse_database().await;
    let store = MemoryStore::new();
    let table_schema = store
        .store_table_schema(TableSchema::new(
            TableId::new(1),
            TableName::new("public".to_owned(), table.to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT8, -1, 1, false).with_primary_key(1),
                ColumnSchema::new(column_name.to_owned(), Type::TEXT, -1, 2, false)
                    .with_default_expression(postgres_text_default(default_value)),
            ],
        ))
        .await
        .unwrap();
    let destination =
        database.build_destination_with_engine(store, ClickHouseEngine::MergeTree).await;
    destination.write_table_rows(&ReplicatedTableSchema::all(table_schema), vec![]).await?;
    let clickhouse_table = format!("public_{table}");
    database
        .db_client()
        .query(&format!("insert into \"{clickhouse_table}\" (id) values (1)"))
        .execute()
        .await
        .map_err(redact_clickhouse_error)
        .unwrap();
    let names = database.column_names(&clickhouse_table).await;
    let stored = database
        .query::<String>(&format!(
            "select * except (id, cdc_operation, cdc_lsn, cdc_tx_ordinal) from \
             \"{clickhouse_table}\""
        ))
        .await
        .remove(0);
    Ok((names, stored))
}

/// A source column whose name contains `?` gets a ClickHouse table.
///
/// The `clickhouse` client treats `?` in query text as a bind placeholder, so
/// DDL text must escape it or `CREATE TABLE` fails as an unbound argument.
#[tokio::test(flavor = "multi_thread")]
async fn question_mark_in_column_name_creates_table() {
    let (names, _) = create_text_column_and_read_default("asked", "done?", "").await.unwrap();

    assert_eq!(names, vec!["id".to_owned(), "done?".to_owned()]);
}

/// A text default containing `?` gets a ClickHouse table.
#[tokio::test(flavor = "multi_thread")]
async fn question_mark_in_text_default_creates_table() {
    let (_, stored) = create_text_column_and_read_default("asked", "status", "?").await.unwrap();

    assert_eq!(stored, "?");
}

/// A text default containing `??` keeps both characters.
///
/// The `clickhouse` client reads `??` in query text as one `?`, so unescaped
/// DDL would silently store a different default.
#[tokio::test(flavor = "multi_thread")]
async fn doubled_question_mark_in_text_default_is_kept() {
    let (_, stored) = create_text_column_and_read_default("asked", "status", "??").await.unwrap();

    assert_eq!(stored, "??");
}
