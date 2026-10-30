//! Resource isolation when initial synchronization fails at the destination.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use etl::{
    data::TableRow,
    destination::{
        Destination, DropTableForCopyResult, TableCopyBatchId, WriteEventsDurability,
        WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::{Event, EventType},
    schema::{ReplicatedTableSchema, TableId},
    store::{StateStore, TableRetryPolicy, TableState, TableStateType},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        event::EventCondition,
        faults::FaultyOp,
        memory_destination::MemoryDestination,
        notifying_store::NotifyingStore,
        pipeline::{PipelineBuilder, wait_for_pipeline_error},
        test_destination_wrapper::TestDestinationWrapper,
    },
};
use etl_postgres::slots::EtlReplicationSlot;
use tokio_postgres::Client;

use crate::support::read_replica::wait_until;

/// More conflicting tables than the number of concurrent attempts.
const CONFLICTING_TABLES: usize = 12;
/// Available table-sync capacity, independent of publication size.
const SYNC_WORKERS: u16 = 2;

/// Reports conflicts through the completion channel, like destination setup.
#[derive(Clone)]
struct ConflictingDestination {
    /// Successful writes use the ordinary in-memory destination.
    inner: MemoryDestination<NotifyingStore>,
    /// Only this table is allowed to initialize.
    healthy_table: TableId,
    /// Independent connection observing source resources at each write.
    source: Arc<Client>,
    /// Exact slot names for this pipeline's publication.
    sync_slots: Arc<Vec<String>>,
    /// Largest observed slot count, including abandoned attempts.
    peak_slots: Arc<AtomicI64>,
}

impl Destination for ConflictingDestination {
    fn name() -> &'static str {
        "conflicting"
    }

    async fn drop_table_for_copy(
        &self,
        schema: &ReplicatedTableSchema,
        result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.inner.drop_table_for_copy(schema, result).await
    }

    async fn write_table_rows(
        &self,
        schema: &ReplicatedTableSchema,
        batch_id: Option<TableCopyBatchId>,
        rows: Vec<TableRow>,
        result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let count: i64 = self
            .source
            .query_one(
                "select count(*) from pg_replication_slots where slot_name = any($1)",
                &[self.sync_slots.as_ref()],
            )
            .await?
            .get(0);
        self.peak_slots.fetch_max(count, Ordering::SeqCst);
        if schema.id() != self.healthy_table {
            result.send(Err(etl_error!(
                ErrorKind::DestinationTableAlreadyExists,
                "Destination table already exists",
                "Test destination object belongs to another owner"
            )));
            return Ok(());
        }
        self.inner.write_table_rows(schema, batch_id, rows, result).await
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.inner.write_events(events, durability, result).await
    }
}

/// Destination conflicts preserve their diagnosis without exhausting source
/// slots or preventing an unrelated table from copying and streaming.
#[tokio::test(flavor = "multi_thread")]
async fn destination_conflicts_release_slots_and_preserve_healthy_progress() {
    let mut database = spawn_source_database().await;
    let mut table_ids = Vec::new();
    let mut table_names = Vec::new();
    for index in 0..=CONFLICTING_TABLES {
        let name = test_table_name(&format!("slot_cleanup_{index}"));
        let id =
            database.create_table(name.clone(), true, &[("value", "int4 not null")]).await.unwrap();
        // Exercise both a nonempty copy and empty-table initialization.
        if index % 2 == 0 {
            database.insert_values(name.clone(), &["value"], &[&1_i32]).await.unwrap();
        }
        table_ids.push(id);
        table_names.push(name);
    }
    let publication = "slot_cleanup_publication";
    database.create_publication(publication, &table_names).await.unwrap();
    let pipeline_id = rand::random();
    let sync_slots = Arc::new(
        table_ids
            .iter()
            .map(|&table_id| {
                String::try_from(EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id))
                    .unwrap()
            })
            .collect::<Vec<_>>(),
    );
    let source = Arc::new(database.client.take().unwrap());
    let healthy_table = table_ids[CONFLICTING_TABLES];
    let store = NotifyingStore::new();
    let peak_slots = Arc::new(AtomicI64::new(0));
    let destination = TestDestinationWrapper::wrap(ConflictingDestination {
        inner: MemoryDestination::new(store.clone()),
        healthy_table,
        source: Arc::clone(&source),
        sync_slots: Arc::clone(&sync_slots),
        peak_slots: Arc::clone(&peak_slots),
    });
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_max_table_sync_workers(SYNC_WORKERS)
    .with_max_copy_connections_per_table(1)
    .build();
    let mut errors = Vec::new();
    for &table_id in &table_ids[..CONFLICTING_TABLES] {
        errors.push(store.notify_on_table_state_type(table_id, TableStateType::Errored).await);
    }
    let synced = store.notify_on_table_sync_complete(healthy_table).await;

    pipeline.start().await.unwrap();

    let progress = tokio::time::timeout(Duration::from_secs(60), async {
        for error in errors {
            error.notified().await;
        }
        synced.notified().await;
        let delivered = destination
            .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, healthy_table, 1)])
            .await;

        source
            .execute(
                &format!(
                    "insert into {} (value) values (2)",
                    table_names[CONFLICTING_TABLES].as_quoted_identifier()
                ),
                &[],
            )
            .await
            .unwrap();

        delivered.notified().await;
        // Release must happen during normal operation, not only at shutdown.
        wait_until("table-sync slot release", || async {
            let count: i64 = source
                .query_one(
                    "select count(*) from pg_replication_slots where slot_name = any($1)",
                    &[sync_slots.as_ref()],
                )
                .await?
                .get(0);
            Ok(count == 0)
        })
        .await;
    })
    .await;
    pipeline.shutdown_and_wait().await.unwrap();
    progress.unwrap();

    for &table_id in &table_ids[..CONFLICTING_TABLES] {
        let TableState::Errored { source_err, retry_policy, reason, .. } =
            store.get_table_state(table_id).await.unwrap().unwrap()
        else {
            panic!("Expected a destination conflict");
        };
        assert_eq!(source_err.kind(), ErrorKind::DestinationTableAlreadyExists);
        assert_eq!(retry_policy, TableRetryPolicy::ManualRetry);
        assert!(reason.contains("Test destination object belongs to another owner"));
    }
    assert_eq!(destination.get_table_rows().await.get(&healthy_table).unwrap().len(), 1);
    assert!(destination.get_events().await.iter().any(|event| {
        matches!(event, Event::Insert(insert) if insert.replicated_table_schema.id() == healthy_table)
    }));
    assert!(
        peak_slots.load(Ordering::SeqCst) <= i64::from(SYNC_WORKERS),
        "Observed {} table-sync slots",
        peak_slots.load(Ordering::SeqCst)
    );
    let remaining: i64 = source
        .query_one(
            "select count(*) from pg_replication_slots where slot_name = any($1)",
            &[sync_slots.as_ref()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
    let apply_slot: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let temporary: bool = source
        .query_one(
            "select temporary from pg_replication_slots where slot_name = $1",
            &[&apply_slot],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!temporary);
}

/// A one-off failure to persist a copy error stops the pipeline while queued
/// tables remain unadmitted, even though subsequent store operations succeed.
#[tokio::test(flavor = "multi_thread")]
async fn error_persistence_failure_stops_pipeline_without_reusing_capacity() {
    let database = spawn_source_database().await;
    let mut table_names = Vec::new();
    for index in 0..2 {
        let name = test_table_name(&format!("persistence_failure_{index}"));
        database.create_table(name.clone(), true, &[("value", "int4 not null")]).await.unwrap();
        table_names.push(name);
    }
    let publication = "persistence_failure_publication";
    database.create_publication(publication, &table_names).await.unwrap();
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let hold = destination.hold_next(FaultyOp::WriteTableRows).await;
    let pipeline_id = rand::random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_max_table_sync_workers(1)
    .with_max_copy_connections_per_table(1)
    .build();

    pipeline.start().await.unwrap();

    hold.wait_reached().await;
    // Discover the admitted table rather than depending on worker spawn order.
    let states = store.get_table_states().await;
    let (&table_id, original_state) =
        states.iter().find(|(_, state)| state.as_type() == TableStateType::DataSync).unwrap();
    let slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into().unwrap();
    let temporary: bool = database
        .client
        .as_ref()
        .unwrap()
        .query_one("select temporary from pg_replication_slots where slot_name = $1", &[&slot_name])
        .await
        .unwrap()
        .get(0);
    assert!(temporary);
    let persistence_error = etl_error!(
        ErrorKind::SourceQueryFailed,
        "Test table error persistence failed",
        source: std::io::Error::new(std::io::ErrorKind::TimedOut, "Test state-store outage")
    );
    store
        .fail_next_table_state_update(table_id, TableStateType::Errored, persistence_error.clone())
        .await;

    hold.release_err(ErrorKind::DestinationTableAlreadyExists, "Test destination conflict");

    let error = wait_for_pipeline_error(&pipeline).await;
    assert_eq!(error.kind(), persistence_error.kind());
    assert_eq!(error.to_string(), persistence_error.to_string());
    assert_eq!(std::error::Error::source(&error).unwrap().to_string(), "Test state-store outage");
    assert_eq!(destination.write_table_rows_called().await, 1);
    assert!(!destination.shutdown_called().await);
    assert_eq!(
        store.get_table_state(table_id).await.unwrap().unwrap().as_type(),
        TableStateType::DataSync
    );
    // The store is healthy again; the failure was isolated to the Errored
    // write.
    store.update_table_state(table_id, original_state.clone()).await.unwrap();
    destination.shutdown().await.unwrap();
}

/// A fatal persistence failure pre-empts an active graceful drain without
/// checkpointing the destination write whose response is still outstanding.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread")]
async fn error_persistence_failure_preempts_graceful_drain() {
    let _scenario = fail::FailScenario::setup();
    let draining = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::clone(&draining);
    fail::cfg_callback(etl::failpoints::APPLY_LOOP_ENTERED_SHUTDOWN_DRAIN_FP, move || {
        entered.notify_one();
    })
    .unwrap();

    let database = spawn_source_database().await;
    let first_name = test_table_name("fatal_drain_first");
    let first_id = database
        .create_table(first_name.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let second_name = test_table_name("fatal_drain_second");
    let second_id = database
        .create_table(second_name.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    let publication = "fatal_drain_publication";
    database
        .create_publication(publication, &[first_name.clone(), second_name.clone()])
        .await
        .unwrap();
    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let copy = destination.hold_next(FaultyOp::WriteTableRows).await;
    let first_synced = store.notify_on_table_sync_complete(first_id).await;
    let second_synced = store.notify_on_table_sync_complete(second_id).await;
    let first_ready = store.notify_on_table_state_type(first_id, TableStateType::Ready).await;
    let second_ready = store.notify_on_table_state_type(second_id, TableStateType::Ready).await;
    let pipeline_id = rand::random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication.to_owned(),
        store.clone(),
        destination.clone(),
    )
    .with_max_table_sync_workers(2)
    .with_max_copy_connections_per_table(1)
    .with_shutdown_drain_timeout_ms(30_000)
    .build();

    pipeline.start().await.unwrap();

    copy.wait_reached().await;
    // Whichever copy is not held must finish before arranging the CDC drain.
    let (failed_id, healthy_id, healthy_name, healthy_ready) = tokio::select! {
        () = first_synced.notified() => (second_id, first_id, first_name, first_ready),

        () = second_synced.notified() => (first_id, second_id, second_name, second_ready),
    };
    let delivered = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, healthy_id, 1)])
        .await;

    database.insert_values(healthy_name.clone(), &["value"], &[&0_i32]).await.unwrap();

    // Finish the SyncDone -> Ready handoff before withholding a CDC response.
    delivered.notified().await;
    healthy_ready.notified().await;
    let write = destination.hold_next(FaultyOp::WriteEvents).await;

    database.insert_values(healthy_name, &["value"], &[&1_i32]).await.unwrap();

    write.wait_reached().await;
    let client = database.client.as_ref().unwrap();
    let insert_lsn: tokio_postgres::types::PgLsn =
        client.query_one("select pg_current_wal_lsn()", &[]).await.unwrap().get(0);
    assert_eq!(
        store.get_table_state(healthy_id).await.unwrap().unwrap().as_type(),
        TableStateType::Ready
    );
    let sync_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, failed_id).try_into().unwrap();
    let temporary: bool = client
        .query_one("select temporary from pg_replication_slots where slot_name = $1", &[&sync_slot])
        .await
        .unwrap()
        .get(0);
    assert!(temporary);
    let persistence_error = etl_error!(
        ErrorKind::SourceQueryFailed,
        "Test table error persistence failed during drain",
        source: std::io::Error::new(std::io::ErrorKind::TimedOut, "Test state-store outage")
    );
    let persistence = store
        .hold_next_table_state_update_failure(
            failed_id,
            TableStateType::Errored,
            persistence_error.clone(),
        )
        .await;

    copy.release_err(ErrorKind::DestinationTableAlreadyExists, "Test destination conflict");

    // Enter error persistence before shutdown can cancel the copy wait.
    persistence.wait_reached().await;
    let completion = pipeline.wait();
    tokio::pin!(completion);

    pipeline.shutdown();

    tokio::select! {
        result = &mut completion => panic!("Pipeline stopped before entering drain: {result:?}"),

        () = draining.notified() => {}
    }

    persistence.release_ok();

    // This must finish while the CDC response is held and well before the
    // configured drain deadline. Release the test wrapper even on failure.
    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut completion).await;
    let teardown_called = destination.shutdown_called().await;
    // Observe progress while the response is still withheld; releasing it for
    // test cleanup must not change which work these assertions cover.
    let apply_slot: String = EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();
    let (confirmed, _) =
        etl::test_utils::database::replication_slot_state(client, &apply_slot).await;
    let checkpoint = store.get_replication_checkpoint(etl::store::WorkerType::Apply).await.unwrap();
    write.release_ok();
    destination.shutdown().await.unwrap();
    let error = outcome.unwrap().unwrap_err();
    assert_eq!(error.kind(), persistence_error.kind());
    assert_eq!(error.to_string(), persistence_error.to_string());
    assert_eq!(std::error::Error::source(&error).unwrap().to_string(), "Test state-store outage");
    assert!(!teardown_called);
    assert_eq!(
        store.get_table_state(failed_id).await.unwrap().unwrap().as_type(),
        TableStateType::DataSync
    );
    assert!(confirmed < insert_lsn);
    assert!(checkpoint.is_none_or(|checkpoint| checkpoint < insert_lsn));
}
