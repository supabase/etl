use std::{sync::Arc, time::Duration};

use etl::{
    data::{Cell, TableRow},
    destination::{
        Destination, DestinationWriteStatus, DropTableForCopyResult, PipelineDestination,
        WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::{ErrorKind, EtlResult},
    etl_error,
    event::{Event, EventType},
    pipeline::PipelineId,
    schema::{ColumnSchema, ReplicatedTableSchema, TableId},
    store::{SchemaStore, StateStore, TableRetryPolicy, TableState, TableStateType, WorkerType},
    test_utils::{
        database::{
            replication_slot_state, spawn_source_database, terminate_walsender, test_table_name,
            wait_for_new_walsender,
        },
        event::{EventCondition, group_events_by_type_and_table_id},
        faults::{FaultAction, FaultyOp},
        memory_destination::MemoryDestination,
        notify::{DEFAULT_NOTIFY_TIMEOUT, TimedNotify},
        notifying_store::NotifyingStore,
        pipeline::{
            PipelineBuilder, create_pipeline, create_pipeline_with_batch_config,
            create_pipeline_with_table_sync_copy_config,
        },
        schema::assert_table_schema_columns,
        test_destination_wrapper::TestDestinationWrapper,
        test_schema::{
            TableSelection, assert_events_equal, build_expected_orders_inserts,
            build_expected_users_inserts, get_n_integers_sum, get_users_age_sum_from_rows,
            insert_mock_data, insert_orders_data, insert_users_data, setup_test_database_schema,
        },
    },
};
use etl_config::shared::{BatchConfig, InvalidatedSlotBehavior, TableSyncCopyConfig};
use etl_postgres::{
    below_version,
    slots::EtlReplicationSlot,
    tokio::test_utils::{ReplicationSlotState, id_column_schema},
    version::POSTGRES_15,
};
use etl_telemetry::tracing::init_test_tracing;
use pg_escape::{quote_identifier, quote_literal};
use rand::random;
use tokio::{
    sync::{Mutex, Notify},
    time::sleep,
};
use tokio_postgres::types::{PgLsn, Type};

/// Creates a test column schema with sensible defaults.
fn test_column(
    name: &str,
    typ: Type,
    ordinal_position: i32,
    nullable: bool,
    primary_key: bool,
) -> ColumnSchema {
    ColumnSchema::new(name.to_owned(), typ, -1, ordinal_position, nullable)
        .with_primary_key_ordinal_position(if primary_key { Some(1) } else { None })
}

/// State used by [`DeferredCopyDestination`] to control copy durability.
#[derive(Default)]
struct DeferredCopyDestinationState {
    /// Number of nonempty copy writes received.
    nonempty_writes: usize,
    /// Rows retained after the first copy write is accepted.
    accepted_rows: Vec<TableRow>,
    /// Held terminal durability barrier result.
    barrier_result: Option<WriteTableRowsResult>,
}

/// Destination test double that defers the first copy write's durability.
#[derive(Clone)]
struct DeferredCopyDestination<D> {
    /// Immediate destination used for writes after the first copy batch.
    inner: D,
    /// Shared durability-control state.
    state: Arc<Mutex<DeferredCopyDestinationState>>,
    /// Notification emitted after the terminal barrier is held.
    barrier_reached_notify: Arc<Notify>,
}

impl<D> DeferredCopyDestination<D> {
    /// Wraps an immediate destination with deferred copy durability behavior.
    fn wrap(inner: D) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(DeferredCopyDestinationState::default())),
            barrier_reached_notify: Arc::new(Notify::new()),
        }
    }

    /// Returns a notification for the next terminal durability barrier.
    fn notify_on_barrier(&self) -> TimedNotify {
        TimedNotify::new(Arc::clone(&self.barrier_reached_notify))
    }

    /// Returns the number of nonempty copy writes received.
    async fn nonempty_writes(&self) -> usize {
        self.state.lock().await.nonempty_writes
    }

    /// Completes the held terminal durability barrier with `status`.
    async fn complete_barrier(&self, status: DestinationWriteStatus) {
        let barrier_result = self
            .state
            .lock()
            .await
            .barrier_result
            .take()
            .expect("terminal copy durability barrier should be held");
        barrier_result.send(Ok(status));
    }
}

impl<D> Destination for DeferredCopyDestination<D>
where
    D: PipelineDestination,
{
    fn name() -> &'static str {
        "deferred_copy"
    }

    async fn startup(&self) -> EtlResult<()> {
        self.inner.startup().await
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.inner.shutdown().await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        self.inner.drop_table_for_copy(replicated_table_schema, async_result).await
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        mut table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            // Hold the terminal empty write so the test can inspect table state
            // before durability is confirmed.
            let mut state = self.state.lock().await;
            assert!(
                state.barrier_result.is_none(),
                "only one terminal copy durability barrier should be pending"
            );
            state.barrier_result = Some(async_result);
            drop(state);

            self.barrier_reached_notify.notify_one();

            return Ok(());
        }

        let table_rows = {
            let mut state = self.state.lock().await;
            state.nonempty_writes += 1;

            if state.nonempty_writes == 1 {
                // Take ownership of the first batch without making it durable.
                state.accepted_rows = table_rows;
                None
            } else {
                // Make the accepted rows durable with a later batch. ETL must
                // still remember that the terminal barrier is required.
                state.accepted_rows.append(&mut table_rows);
                Some(std::mem::take(&mut state.accepted_rows))
            }
        };

        let Some(table_rows) = table_rows else {
            async_result.send(Ok(DestinationWriteStatus::Accepted));

            return Ok(());
        };

        self.inner.write_table_rows(replicated_table_schema, table_rows, async_result).await
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.inner.write_events(events, durability, async_result).await
    }
}

/// Destination copy operation whose async result is stalled.
#[derive(Clone, Copy)]
enum StallTarget {
    /// A destination drop before a fresh table copy.
    DropTableForCopy,
    /// A nonempty table-copy row write.
    WriteTableRows,
}

/// Behavior applied to the selected async result.
#[derive(Clone, Copy)]
enum StallMode {
    /// Drops the result without completing it.
    DropResult,
    /// Retains the result without completing it.
    HoldResult,
}

/// Destination test double that stalls a copy result after dispatch succeeds.
///
/// This differs intentionally from [`FaultAction::HoldResponse`], which holds
/// the destination method itself. These tests need the method to return `Ok`
/// while ETL waits separately for the async completion handle.
#[derive(Clone)]
struct StalledCopyResultDestination<D> {
    /// Destination used for operations other than the selected target.
    inner: D,
    /// Destination operation whose result is stalled.
    target: StallTarget,
    /// Behavior applied to the selected result.
    mode: StallMode,
    /// Held destination-drop result.
    pending_drop_result: Arc<Mutex<Option<DropTableForCopyResult<()>>>>,
    /// Held table-copy-write result.
    pending_write_result: Arc<Mutex<Option<WriteTableRowsResult>>>,
    /// Notification emitted after the result is dropped or held.
    stall_reached_notify: Arc<Notify>,
}

impl<D> StalledCopyResultDestination<D> {
    /// Wraps a destination with the selected result behavior.
    fn wrap(inner: D, target: StallTarget, mode: StallMode) -> Self {
        Self {
            inner,
            target,
            mode,
            pending_drop_result: Arc::new(Mutex::new(None)),
            pending_write_result: Arc::new(Mutex::new(None)),
            stall_reached_notify: Arc::new(Notify::new()),
        }
    }

    /// Returns a notification for the next stalled result.
    fn notify_on_stall(&self) -> TimedNotify {
        TimedNotify::new(Arc::clone(&self.stall_reached_notify))
    }
}

impl<D> Destination for StalledCopyResultDestination<D>
where
    D: PipelineDestination,
{
    fn name() -> &'static str {
        "stalled_copy"
    }

    async fn startup(&self) -> EtlResult<()> {
        self.inner.startup().await
    }

    async fn shutdown(&self) -> EtlResult<()> {
        self.inner.shutdown().await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        if !matches!(self.target, StallTarget::DropTableForCopy) {
            return self.inner.drop_table_for_copy(replicated_table_schema, async_result).await;
        }

        match self.mode {
            StallMode::DropResult => drop(async_result),
            StallMode::HoldResult => {
                let mut pending_result = self.pending_drop_result.lock().await;
                assert!(pending_result.is_none());
                *pending_result = Some(async_result);
            }
        }
        self.stall_reached_notify.notify_one();

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        if !matches!(self.target, StallTarget::WriteTableRows) {
            return self
                .inner
                .write_table_rows(replicated_table_schema, table_rows, async_result)
                .await;
        }

        match self.mode {
            StallMode::DropResult => drop(async_result),
            StallMode::HoldResult => {
                let mut pending_result = self.pending_write_result.lock().await;
                assert!(pending_result.is_none());
                *pending_result = Some(async_result);
            }
        }
        self.stall_reached_notify.notify_one();

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.inner.write_events(events, durability, async_result).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_rejects_second_start_before_destination_startup() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

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

    let table_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();
    table_sync_complete_notify.notified().await;

    destination
        .inject_fault(
            FaultyOp::Startup,
            FaultAction::reject(
                ErrorKind::DestinationError,
                "Second destination startup should not run",
            ),
        )
        .await;

    let second_start_result = pipeline.start().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let err = second_start_result.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidState);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_shutdown_calls_destination_shutdown() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

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

    let table_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    // Shutdown should not have been called yet.
    assert!(!destination.shutdown_called().await);

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify that shutdown was called on the destination.
    assert!(destination.shutdown_called().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_errors_when_async_result_is_dropped() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    let store = NotifyingStore::new();
    let immediate_destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let destination = StalledCopyResultDestination::wrap(
        immediate_destination,
        StallTarget::WriteTableRows,
        StallMode::DropResult,
    );

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination,
    );

    let table_errored_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    pipeline.start().await.unwrap();

    table_errored_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    let TableState::Errored { source_err, .. } = table_state else {
        panic!("dropped async result should put the table in an errored state");
    };
    assert_eq!(source_err.kind(), ErrorKind::DestinationError);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_shutdown_interrupts_pending_result_wait() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    let store = NotifyingStore::new();
    let immediate_destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let destination = StalledCopyResultDestination::wrap(
        immediate_destination,
        StallTarget::WriteTableRows,
        StallMode::HoldResult,
    );

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let write_reached_notify = destination.notify_on_stall();

    pipeline.start().await.unwrap();

    write_reached_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(table_state, TableState::DataSync));
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_errors_when_async_result_is_dropped() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    let store = NotifyingStore::new();
    let immediate_destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        immediate_destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    store.reset_table_state(table_id).await.unwrap();

    let destination = StalledCopyResultDestination::wrap(
        immediate_destination,
        StallTarget::DropTableForCopy,
        StallMode::DropResult,
    );
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination,
    );

    let table_errored_notify =
        store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    pipeline.start().await.unwrap();

    table_errored_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    let TableState::Errored { source_err, .. } = table_state else {
        panic!("dropped destination-drop result should put the table in an errored state");
    };
    assert_eq!(source_err.kind(), ErrorKind::DestinationError);
    assert!(store.get_destination_table_metadata(table_id).await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_for_copy_shutdown_interrupts_pending_result_wait() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    let store = NotifyingStore::new();
    let immediate_destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        immediate_destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;
    pipeline.start().await.unwrap();
    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    store.reset_table_state(table_id).await.unwrap();

    let destination = StalledCopyResultDestination::wrap(
        immediate_destination,
        StallTarget::DropTableForCopy,
        StallMode::HoldResult,
    );
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let drop_reached_notify = destination.notify_on_stall();

    pipeline.start().await.unwrap();

    drop_reached_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    assert!(matches!(table_state, TableState::Init));
    assert!(store.get_destination_table_metadata(table_id).await.unwrap().is_some());
}

/// Verifies that resetting a table during an active copy is overwritten by the
/// active worker.
///
/// Real-time resets are not supported yet: the active worker currently owns a
/// separate in-memory state and can overwrite the stored `Init` state when it
/// advances. Supporting resets without restarting the pipeline requires
/// coordinating the reset with that worker and changing this expectation so a
/// fresh copy drops the existing destination table.
#[tokio::test(flavor = "multi_thread")]
async fn reset_during_active_copy_is_overwritten_by_active_worker() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let held_write = destination.hold_next(FaultyOp::WriteTableRows).await;

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_done_notify =
        store.notify_on_table_state_type(table_id, TableStateType::SyncDone).await;

    pipeline.start().await.unwrap();

    held_write.wait_reached().await;

    // We reset the table state while the write table rows method is blocked.
    store.reset_table_state(table_id).await.unwrap();

    // We release the hold, which causes the system to continue its work and
    // overrides the `Init` state which was set by the reset.
    held_write.release_ok();

    // The active worker completes the copy and overwrites the stored Init state.
    table_sync_done_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    assert!(matches!(
        store.get_table_state(table_id).await.unwrap(),
        Some(TableState::SyncDone { .. })
    ));
    assert!(!destination.was_table_dropped_for_copy(table_id).await);
}

/// Verifies that invalidating a table-sync slot during an active copy aborts
/// the copy and persists a manually retryable table error.
// Serialized via nextest test-group "shared-pg" (mutates cluster-wide WAL settings).
#[tokio::test(flavor = "multi_thread")]
async fn exclusive_table_copy_fails_when_slot_invalidated() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let users_schema = database_schema.users_schema();
    let table_id = users_schema.id;
    insert_users_data(&mut database, &users_schema.name, 1..=3).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let held_write = destination.hold_next(FaultyOp::WriteTableRows).await;

    let pipeline_id: PipelineId = random();
    let slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_id).try_into().unwrap();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination,
    )
    .with_table_sync_monitor_refresh_interval_ms(100)
    .build();

    let errored = store.notify_on_table_state_type(table_id, TableStateType::Errored).await;

    pipeline.start().await.unwrap();

    held_write.wait_reached().await;

    database.invalidate_slot(&slot_name).await.unwrap();

    errored.notified().await;

    held_write.release_ok();

    pipeline.shutdown_and_wait().await.unwrap();

    let state = store.get_table_state(table_id).await.unwrap().unwrap();
    let TableState::Errored { retry_policy, source_err, .. } = state else {
        panic!("table copy should be marked errored after its slot is invalidated");
    };
    assert!(matches!(retry_policy, TableRetryPolicy::ManualRetry));
    assert_eq!(source_err.kind(), ErrorKind::ReplicationSlotInvalidated);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_recreates_missing_apply_slot_with_mixed_table_states() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=3).await;

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

    // Materialize apply-owned decoding state so this table is genuinely Ready
    // before testing slot recreation with mixed table states.
    let table_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let table_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;
    database
        .run_sql(&format!(
            "update {} set age = age where id = 1",
            database_schema.users_schema().name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    table_ready_notify.notified().await;

    // Add two tables while the original apply slot is still active. Publication
    // membership is reconciled on restart, when one table will be initialized as
    // `Init` and the other will already have a persisted `Errored` state.
    let init_table_name = test_table_name("init_before_slot_loss");
    let init_table_id = database
        .create_table(init_table_name.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    database.insert_values(init_table_name.clone(), &["value"], &[&1]).await.unwrap();

    let errored_table_name = test_table_name("errored_before_slot_loss");
    let errored_table_id = database
        .create_table(errored_table_name.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();
    database.insert_values(errored_table_name.clone(), &["value"], &[&1]).await.unwrap();

    database
        .run_sql(&format!(
            "alter publication {} add table {}, {}",
            quote_identifier(&database_schema.publication_name()),
            init_table_name.as_quoted_identifier(),
            errored_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    pipeline.shutdown_and_wait().await.unwrap();

    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows[&database_schema.users_schema().id].len(), 3);
    assert_eq!(store.get_table_state(init_table_id).await.unwrap(), None);
    assert_eq!(store.get_table_state(errored_table_id).await.unwrap(), None);
    let table_schemas = store.get_table_schemas().await;
    assert!(!table_schemas.contains_key(&init_table_id));
    assert!(!table_schemas.contains_key(&errored_table_id));

    // Verify that the replication slot for the apply worker exists and is inactive.
    database.wait_for_slot_inactive(&apply_slot_name).await;

    let slot_state = database.get_replication_slot_state(&apply_slot_name).await.unwrap();
    assert_eq!(slot_state, Some(ReplicationSlotState::Inactive));

    store
        .update_table_state(
            errored_table_id,
            TableState::Errored {
                reason: "Injected test error".to_owned(),
                solution: None,
                retry_policy: TableRetryPolicy::NoRetry,
                source_err: etl_error!(ErrorKind::Unknown, "Injected test error"),
            },
        )
        .await
        .unwrap();

    // Simulate a persisted checkpoint from an unrelated WAL lineage. The
    // replacement slot must not use this position even when it is ahead of the
    // new slot's consistent point.
    let stale_checkpoint = PgLsn::from(u64::MAX);
    store.upsert_replication_checkpoint(WorkerType::Apply, stale_checkpoint).await.unwrap();

    // Delete the apply worker slot after pausing the pipeline. No source changes
    // occur between the pause and slot recreation.
    database
        .run_sql(&format!("select pg_drop_replication_slot({})", quote_literal(&apply_slot_name)))
        .await
        .unwrap();
    let slot_state = database.get_replication_slot_state(&apply_slot_name).await.unwrap();
    assert_eq!(slot_state, None);

    // The Init table should be copied, the Ready users table should continue
    // from the new slot, and the Errored table should remain stopped.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    let init_table_sync_complete_notify = store.notify_on_table_sync_complete(init_table_id).await;

    pipeline.start().await.unwrap();

    init_table_sync_complete_notify.notified().await;

    let users_insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            1,
        )])
        .await;

    // Put both changes in one transaction so observing the users event also
    // proves the apply worker processed the errored-table change.
    let mut transaction = database.begin_transaction().await;
    transaction.insert_values(errored_table_name, &["value"], &[&2]).await.unwrap();
    insert_users_data(&mut transaction, &database_schema.users_schema().name, 4..=4).await;
    transaction.commit_transaction().await;

    users_insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // The Ready table was not recopied, the Init table was copied, and the
    // Errored table remained stopped.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows[&database_schema.users_schema().id].len(), 3);
    assert_eq!(table_rows[&init_table_id].len(), 1);
    assert!(!table_rows.contains_key(&errored_table_id));

    let errored_table_state =
        store.get_table_state(errored_table_id).await.unwrap().expect("table state should exist");
    assert!(matches!(
        errored_table_state,
        TableState::Errored { retry_policy: TableRetryPolicy::NoRetry, .. }
    ));

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts =
        grouped_events.get(&(EventType::Insert, database_schema.users_schema().id)).unwrap();
    let expected_users_inserts =
        build_expected_users_inserts(4, &database_schema.users_schema(), vec![("user_4", 4)]);
    assert_events_equal(users_inserts, &expected_users_inserts);
    assert!(!grouped_events.contains_key(&(EventType::Insert, errored_table_id)));

    let persisted_checkpoint_lsn = store
        .get_replication_checkpoint(WorkerType::Apply)
        .await
        .unwrap()
        .expect("the recreated apply worker should persist its new checkpoint");
    assert_ne!(persisted_checkpoint_lsn, stale_checkpoint);
}

// Serialized via nextest test-group "shared-pg" (shares the source PG cluster).
#[tokio::test(flavor = "multi_thread")]
async fn exclusive_pipeline_fails_when_slot_invalidated_with_error_behavior() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();

    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();

    // Create pipeline with default Error behavior for invalidated slots.
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_invalidated_slot_behavior(InvalidatedSlotBehavior::Error)
    .build();

    // Wait for initial sync to complete before invalidating the inactive slot.
    let table_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Wait for the slot to become inactive.
    database.wait_for_slot_inactive(&apply_slot_name).await;

    // Invalidate the slot.
    database.invalidate_slot(&apply_slot_name).await.unwrap();

    // Restart the pipeline, it should fail because the slot is invalidated
    // and error behavior is configured.
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_invalidated_slot_behavior(InvalidatedSlotBehavior::Error)
    .build();

    pipeline.start().await.unwrap();

    // The error surfaces when we wait for the pipeline to complete
    let wait_result = pipeline.shutdown_and_wait().await;
    assert!(wait_result.is_err());
    let err = wait_result.unwrap_err();
    assert!(err.kinds().contains(&ErrorKind::ReplicationSlotInvalidated));
}

// Serialized via nextest test-group "shared-pg" (shares the source PG cluster).
#[tokio::test(flavor = "multi_thread")]
async fn exclusive_pipeline_recovers_when_slot_invalidated_with_recreate_behavior() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    // Insert some initial data.
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=5).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();

    let apply_slot_name: String =
        EtlReplicationSlot::for_apply_worker(pipeline_id).try_into().unwrap();

    // Create pipeline with Recreate behavior for invalidated slots.
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_invalidated_slot_behavior(InvalidatedSlotBehavior::Recreate)
    .build();

    // Wait for the initial copy to complete.
    let table_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Validate that we have users data.
    let table_rows = destination.get_table_rows().await;
    let users_table_copied_rows =
        table_rows.get(&database_schema.users_schema().id).map_or(0, Vec::len);
    assert_eq!(users_table_copied_rows, 5);

    // Wait for the slot to become inactive.
    database.wait_for_slot_inactive(&apply_slot_name).await;

    // Invalidate the slot.
    database.invalidate_slot(&apply_slot_name).await.unwrap();

    // Verify the slot is invalidated.
    let slot_state = database.get_replication_slot_state(&apply_slot_name).await.unwrap();
    assert_eq!(slot_state, Some(ReplicationSlotState::Invalidated));

    // Restart the pipeline using the same store, this simulates a real restart
    // where state persists. The pipeline should detect the invalidated slot,
    // recreate it, and reset all table states to Init.
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_invalidated_slot_behavior(InvalidatedSlotBehavior::Recreate)
    .build();

    // Wait for the recreated slot's resync to complete.
    let table_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    // Validate that we have users data.
    let table_rows = destination.get_table_rows().await;
    let users_table_copied_rows =
        table_rows.get(&database_schema.users_schema().id).map_or(0, Vec::len);
    assert_eq!(users_table_copied_rows, 5);

    // Verify the slot was recreated and is active.
    let slot_state = database.get_replication_slot_state(&apply_slot_name).await.unwrap();
    assert_eq!(slot_state, Some(ReplicationSlotState::Active));

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_replicates_many_rows_with_parallel_connections() {
    init_test_tracing();

    let database = spawn_source_database().await;

    // Create a table with a primary key and a value column.
    let table_name = test_table_name("large_table");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "int4 not null")])
        .await
        .unwrap();

    // Create a publication for the table.
    let publication_name = format!("pub_{}", random::<u32>());
    database
        .create_publication(&publication_name, std::slice::from_ref(&table_name))
        .await
        .unwrap();

    // Insert 100k rows using generate_series.
    let total_rows: i64 = 100000;
    let rows_affected = database
        .insert_generate_series(table_name.clone(), &["value"], 1, total_rows, 1)
        .await
        .unwrap();
    assert_eq!(rows_affected, total_rows as u64);

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // Create a pipeline with many parallel copy connections.
    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    )
    .with_max_copy_connections_per_table(100)
    .with_batch_config(BatchConfig {
        max_fill_ms: 1000,
        memory_budget_ratio: 0.2,
        max_bytes: 8 * 1024 * 1024,
    })
    .build();

    // Wait for the copy to complete.
    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify that all 100k rows were copied.
    let table_rows = destination.get_table_rows().await;
    let copied_rows = table_rows.get(&table_id).map_or(0, Vec::len);
    assert_eq!(copied_rows, total_rows as usize);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_waits_for_durable_terminal_barrier_after_accepted_write() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;
    let table_id = database_schema.users_schema().id;
    let table_name = database_schema.users_schema().name.clone();

    // Force at least two copy batches so the first can return Accepted and a
    // later batch Durable. The terminal barrier must still be required.
    insert_users_data(&mut database, &table_name, 0..=2).await;

    let store = NotifyingStore::new();
    let immediate_destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));
    let destination = DeferredCopyDestination::wrap(immediate_destination);

    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    )
    .with_max_copy_connections_per_table(1)
    .with_batch_config(BatchConfig { max_fill_ms: 1000, memory_budget_ratio: 0.2, max_bytes: 1 })
    .build();

    // Arm notifications before starting because they only observe future
    // transitions.
    let barrier_reached_notify = destination.notify_on_barrier();
    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    // The destination now holds the terminal barrier result.
    barrier_reached_notify.notified().await;

    // A later Durable batch must not advance the table while the terminal
    // barrier remains pending.
    assert!(destination.nonempty_writes().await >= 2);
    let table_states = store.get_table_states().await;
    assert_eq!(
        table_states.get(&table_id).map(TableState::as_type),
        Some(TableStateType::DataSync)
    );

    // Confirming the terminal barrier unlocks normal table-state progression.
    destination.complete_barrier(DestinationWriteStatus::Durable).await;
    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_with_row_filter_and_parallel_connections() {
    init_test_tracing();

    let database = spawn_source_database().await;

    // Row filters in publications are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for row filters");
        return;
    }

    // Create a table with a primary key and an age column.
    let table_name = test_table_name("filtered_table");
    let table_id =
        database.create_table(table_name.clone(), true, &[("age", "int4 not null")]).await.unwrap();

    // Create a publication with a row filter (age >= 18).
    let publication_name = format!("pub_{}", random::<u32>());
    database
        .run_sql(&format!(
            "create publication {} for table {} where (age >= 18)",
            quote_identifier(&publication_name),
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Insert 10000 rows: age 1..=10000.
    let total_rows: i64 = 10000;
    let rows_affected = database
        .insert_generate_series(table_name.clone(), &["age"], 1, total_rows, 1)
        .await
        .unwrap();
    assert_eq!(rows_affected, total_rows as u64);

    // Only rows with age >= 18 should be replicated (18..=10000 = 9983 rows).
    let expected_rows = (total_rows - 18 + 1) as usize;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // Create a pipeline with parallel copy connections.
    let pipeline_id: PipelineId = random();
    let mut pipeline = PipelineBuilder::new(
        database.config.clone(),
        pipeline_id,
        publication_name,
        store.clone(),
        destination.clone(),
    )
    .with_max_copy_connections_per_table(100)
    .with_batch_config(BatchConfig {
        max_fill_ms: 1000,
        memory_budget_ratio: 0.2,
        max_bytes: BatchConfig::DEFAULT_MAX_BYTES,
    })
    .build();

    // Wait for the filtered copy to complete.
    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify that only rows matching the filter were copied.
    let table_rows = destination.get_table_rows().await;
    let copied_rows = table_rows.get(&table_id).map_or(0, Vec::len);
    assert_eq!(copied_rows, expected_rows);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_schema_copy_survives_pipeline_restarts() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

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

    // Wait for both table states to reach SyncDone.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let orders_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.orders_schema().id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    orders_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let mut expected_users_schema = database_schema.users_schema();
    expected_users_schema.column_schemas[0].default_expression =
        Some("nextval('test.users_id_seq'::regclass)".to_owned());
    let mut expected_orders_schema = database_schema.orders_schema();
    expected_orders_schema.column_schemas[0].default_expression =
        Some("nextval('test.orders_id_seq'::regclass)".to_owned());

    // We check that the table schemas have been stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert_eq!(*table_schemas.get(&expected_users_schema.id).unwrap(), expected_users_schema);
    assert_eq!(*table_schemas.get(&expected_orders_schema.id).unwrap(), expected_orders_schema);

    // We recreate a pipeline, assuming the other one was stopped, using the same
    // state and destination.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    pipeline.start().await.unwrap();

    // We wait for two inserts to be processed, one for `users` and one for
    // `orders`.
    let insert_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Insert, database_schema.users_schema().id, 1),
            EventCondition::TableCount(EventType::Insert, database_schema.orders_schema().id, 1),
        ])
        .await;

    // Insert a single row for each table.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        // 1 element.
        0..=0,
        true,
    )
    .await;

    insert_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We check that both inserts were received, and we know that we can receive
    // them only when the table schemas are available.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts =
        grouped_events.get(&(EventType::Insert, database_schema.users_schema().id)).unwrap();
    let orders_inserts =
        grouped_events.get(&(EventType::Insert, database_schema.orders_schema().id)).unwrap();

    assert_eq!(users_inserts.len(), 1);
    assert_eq!(orders_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_changes_are_correctly_handled() {
    init_test_tracing();

    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    // Create two tables in the test schema and a publication for that schema.
    let table_1 = test_table_name("table_1");
    let table_1_id =
        database.create_table(table_1.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    let table_2 = test_table_name("table_2");
    let table_2_id =
        database.create_table(table_2.clone(), true, &[("value", "int4 not null")]).await.unwrap();

    let publication_name = "test_pub_cleanup";
    database.create_publication_for_all(publication_name, Some(&table_1.schema)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    // Wait for initial copy completion for both tables.
    let table_1_sync_complete_notify = store.notify_on_table_sync_complete(table_1_id).await;
    let table_2_sync_complete_notify = store.notify_on_table_sync_complete(table_2_id).await;

    pipeline.start().await.unwrap();

    table_1_sync_complete_notify.notified().await;
    table_2_sync_complete_notify.notified().await;

    // Insert one row in each table and wait for two insert events.
    let inserts_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Insert, table_1_id, 1),
            EventCondition::TableCount(EventType::Insert, table_2_id, 1),
        ])
        .await;

    database.insert_values(table_1.clone(), &["value"], &[&1]).await.unwrap();
    database.insert_values(table_2.clone(), &["value"], &[&1]).await.unwrap();

    inserts_notify.notified().await;

    // Drop table_2 so it's no longer part of the publication.
    database
        .client
        .as_ref()
        .unwrap()
        .execute(&format!("drop table {}", table_2.as_quoted_identifier()), &[])
        .await
        .unwrap();

    // Shutdown pipeline after the table was dropped. We do this to show that the
    // dropping of a table doesn't cause issues with the pipeline since the
    // change is picked up on pipeline restart.
    pipeline.shutdown_and_wait().await.unwrap();

    // The destination should have the insert event for each original table
    // before the restart.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let table_1_inserts = grouped.get(&(EventType::Insert, table_1_id)).cloned().unwrap();
    assert_eq!(table_1_inserts.len(), 1);
    let table_2_inserts = grouped.get(&(EventType::Insert, table_2_id)).cloned().unwrap();
    assert_eq!(table_2_inserts.len(), 1);

    // Create table_3 which is going to be added to the publication.
    let table_3 = test_table_name("table_3");
    let table_3_id =
        database.create_table(table_3.clone(), true, &[("value", "int4 not null")]).await.unwrap();

    // Restart pipeline; it should detect table_2 is gone and purge its state
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    // Wait for the table_3 to be done.
    let table_3_sync_complete_notify = store.notify_on_table_sync_complete(table_3_id).await;

    pipeline.start().await.unwrap();

    table_3_sync_complete_notify.notified().await;

    // Insert one row in table_1 and table_3 and wait for the new events.
    let inserts_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Insert, table_1_id, 2),
            EventCondition::TableCount(EventType::Insert, table_3_id, 1),
        ])
        .await;

    database.insert_values(table_1.clone(), &["value"], &[&2]).await.unwrap();
    database.insert_values(table_3.clone(), &["value"], &[&1]).await.unwrap();

    inserts_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Assert that table_2 state is gone but destination data remains.
    let states = store.get_table_states().await;
    assert!(states.contains_key(&table_1_id));
    assert!(!states.contains_key(&table_2_id));
    assert!(states.contains_key(&table_3_id));

    // Assert that the table sync slot for table_2 is also deleted.
    let table_2_slot_name: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, table_2_id).try_into().unwrap();
    let slot_state = database.get_replication_slot_state(&table_2_slot_name).await.unwrap();
    assert_eq!(slot_state, None);

    // The destination should have the new event for table_1 and table_3.
    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let table_1_inserts = grouped.get(&(EventType::Insert, table_1_id)).cloned().unwrap();
    assert_eq!(table_1_inserts.len(), 2);
    let table_3_inserts = grouped.get(&(EventType::Insert, table_3_id)).cloned().unwrap();
    assert_eq!(table_3_inserts.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_reconnect_does_not_replay_already_flushed_events() {
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

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

    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();
    users_sync_complete_notify.notified().await;

    let first_insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            1,
        )])
        .await;
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=1).await;
    first_insert_notify.notified().await;
    users_ready_notify.notified().await;

    let first_insert = destination
        .get_events()
        .await
        .into_iter()
        .find_map(|event| match event {
            Event::Insert(insert)
                if insert.replicated_table_schema.id() == database_schema.users_schema().id =>
            {
                Some(insert)
            }
            _ => None,
        })
        .expect("expected first streamed insert event");

    let client = database.client.as_ref().unwrap();
    let terminated_pid = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (confirmed_flush_lsn, active_pid) =
                replication_slot_state(client, &apply_slot_name).await;

            if confirmed_flush_lsn >= first_insert.commit_lsn
                && let Some(active_pid) = active_pid
            {
                break active_pid;
            }

            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("timed out waiting for confirmed_flush_lsn to advance after flush");

    assert!(terminate_walsender(client, terminated_pid).await.unwrap());

    assert!(
        wait_for_new_walsender(client, &apply_slot_name, terminated_pid, DEFAULT_NOTIFY_TIMEOUT,)
            .await
            .unwrap()
            .is_some()
    );

    let second_insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            2,
        )])
        .await;
    let duplicate_insert_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            3,
        )])
        .await;

    insert_users_data(&mut database, &database_schema.users_schema().name, 2..=2).await;
    second_insert_notify.notified().await;

    assert!(
        tokio::time::timeout(Duration::from_secs(3), duplicate_insert_notify.notified())
            .await
            .is_err()
    );

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped = group_events_by_type_and_table_id(&events);
    let users_inserts =
        grouped.get(&(EventType::Insert, database_schema.users_schema().id)).cloned().unwrap();
    assert_eq!(users_inserts.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_for_all_tables_in_schema_ignores_new_tables_until_restart() {
    init_test_tracing();

    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for FOR TABLES IN SCHEMA");
        return;
    }

    // Create first table and insert one row.
    let table_1 = test_table_name("table_1");
    let table_1_id =
        database.create_table(table_1.clone(), true, &[("name", "text not null")]).await.unwrap();
    database.insert_values(table_1.clone(), &["name"], &[&"test_name_1".to_owned()]).await.unwrap();

    // Create a publication for all tables in the test schema.
    let publication_name = "test_pub_all_schema";
    database.create_publication_for_all(publication_name, Some(&table_1.schema)).await.unwrap();

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_1_id).await;
    let table_ready_notify =
        store.notify_on_table_state_type(table_1_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    // Wait for an insert event in table 1.
    let insert_events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_1_id, 1)])
        .await;

    database.insert_values(table_1.clone(), &["name"], &[&"test_name_2".to_owned()]).await.unwrap();

    insert_events_notify.notified().await;
    table_ready_notify.notified().await;

    // Create a new table in the same schema and insert a row.
    let table_2 = test_table_name("table_2");
    let table_2_id =
        database.create_table(table_2.clone(), true, &[("value", "int4 not null")]).await.unwrap();
    database.insert_values(table_2.clone(), &["value"], &[&1_i32]).await.unwrap();

    // Wait for the events to come in from the new table to make sure the pipeline
    // reacts to them gracefully even if they are not replicated.
    sleep(Duration::from_secs(2)).await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that only the schemas of the first table were stored.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 1);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(!table_schemas.contains_key(&table_2_id));

    // Verify the table rows and events inserted into table 1.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_1_id).unwrap().len(), 1);
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_1_id)).unwrap();
    assert_eq!(insert_events.len(), 1);

    // We restart the pipeline and verify that the new table is now processed.
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.to_owned(),
        store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = store.notify_on_table_sync_complete(table_2_id).await;
    let table_ready_notify =
        store.notify_on_table_state_type(table_2_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    // Wait for an insert event in table 2.
    let insert_events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_2_id, 1)])
        .await;

    database.insert_values(table_2.clone(), &["value"], &[&2_i32]).await.unwrap();

    insert_events_notify.notified().await;
    table_ready_notify.notified().await;

    // Shutdown and verify no errors occurred.
    pipeline.shutdown_and_wait().await.unwrap();

    // Check that both schemas exist.
    let table_schemas = store.get_latest_table_schemas().await;
    assert_eq!(table_schemas.len(), 2);
    assert!(table_schemas.contains_key(&table_1_id));
    assert!(table_schemas.contains_key(&table_2_id));

    // Verify the table rows and events inserted into table 2.
    let table_rows = destination.get_table_rows().await;
    assert_eq!(table_rows.get(&table_2_id).unwrap().len(), 1);
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let insert_events = grouped_events.get(&(EventType::Insert, table_2_id)).unwrap();
    assert_eq!(insert_events.len(), 1);
}

async fn run_table_sync_copy_case<F>(
    table_sync_copy_fn: F,
    expected_users_copied_rows: usize,
    expected_orders_copied_rows: usize,
) where
    F: FnOnce(TableId, TableId) -> TableSyncCopyConfig,
{
    init_test_tracing();

    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let users_table_id = database_schema.users_schema().id;
    let orders_table_id = database_schema.orders_schema().id;
    let users_table_name = database_schema.users_schema().name.clone();
    let orders_table_name = database_schema.orders_schema().name.clone();

    // We insert a single user and order.
    insert_users_data(&mut database, &users_table_name, 0..=0).await;
    insert_orders_data(&mut database, &orders_table_name, 0..=0).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    let pipeline_id: PipelineId = random();
    let table_sync_copy = table_sync_copy_fn(users_table_id, orders_table_id);
    let mut pipeline = create_pipeline_with_table_sync_copy_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        table_sync_copy,
    );

    let users_sync_complete_notify = store.notify_on_table_sync_complete(users_table_id).await;
    let orders_sync_complete_notify = store.notify_on_table_sync_complete(orders_table_id).await;
    let users_table_ready_notify =
        store.notify_on_table_state_type(users_table_id, TableStateType::Ready).await;
    let orders_table_ready_notify =
        store.notify_on_table_state_type(orders_table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    orders_sync_complete_notify.notified().await;

    // We wait for the two inserts.
    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Insert, users_table_id, 1),
            EventCondition::TableCount(EventType::Insert, orders_table_id, 1),
        ])
        .await;

    // We insert additional data.
    insert_users_data(&mut database, &users_table_name, 1..=1).await;
    insert_orders_data(&mut database, &orders_table_name, 1..=1).await;

    events_notify.notified().await;
    users_table_ready_notify.notified().await;
    orders_table_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // We validate that the table rows are correct.
    let table_rows = destination.get_table_rows().await;
    let users_table_copied_rows = table_rows.get(&users_table_id).map_or(0, Vec::len);
    let orders_table_copied_rows = table_rows.get(&orders_table_id).map_or(0, Vec::len);
    assert_eq!(users_table_copied_rows, expected_users_copied_rows);
    assert_eq!(orders_table_copied_rows, expected_orders_copied_rows);
    // We always expect the method to be called since the downstream table should be
    // created nonetheless.
    assert_eq!(destination.write_table_rows_called().await, 2);

    // We validate that the single insert was received.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    assert_eq!(grouped_events.get(&(EventType::Insert, users_table_id)).unwrap().len(), 1);
    assert_eq!(grouped_events.get(&(EventType::Insert, orders_table_id)).unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_include_all_tables() {
    run_table_sync_copy_case(|_, _| TableSyncCopyConfig::IncludeAllTables, 1, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_skip_all_tables() {
    run_table_sync_copy_case(|_, _| TableSyncCopyConfig::SkipAllTables, 0, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_include_only_specified_tables() {
    run_table_sync_copy_case(
        |users_table_id, _| TableSyncCopyConfig::IncludeTables {
            table_ids: vec![users_table_id.into_inner()],
        },
        1,
        0,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_copy_skip_only_specified_tables() {
    run_table_sync_copy_case(
        |users_table_id, _| TableSyncCopyConfig::SkipTables {
            table_ids: vec![users_table_id.into_inner()],
        },
        0,
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_replicates_existing_data() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=rows_inserted,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register notifications for table copy completion.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let orders_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.orders_schema().id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    orders_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify copied data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);

    // Check that the replication slots for the two tables have been removed.
    let users_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.users_schema().id)
            .try_into()
            .unwrap();
    let orders_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.orders_schema().id)
            .try_into()
            .unwrap();
    assert_eq!(database.get_replication_slot_state(&users_replication_slot).await.unwrap(), None);
    assert_eq!(database.get_replication_slot_state(&orders_replication_slot).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_and_sync_streams_new_data() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    // Insert initial test data.
    let rows_inserted = 10;
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=rows_inserted,
        false,
    )
    .await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
    );

    // Register copy-completion and steady-state notifications before startup.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let orders_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.orders_schema().id).await;
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_ready_notify = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    orders_sync_complete_notify.notified().await;

    // Insert additional data to test streaming.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (rows_inserted + 1)..=(rows_inserted + 2),
        true,
    )
    .await;
    users_ready_notify.notified().await;
    orders_ready_notify.notified().await;

    // We wait for all the inserts to be received.
    let events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Insert, database_schema.users_schema().id, 4),
            EventCondition::TableCount(EventType::Insert, database_schema.orders_schema().id, 4),
        ])
        .await;

    // Insert more data to test apply worker processing.
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (rows_inserted + 3)..=(rows_inserted + 4),
        true,
    )
    .await;

    events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify initial table copy data.
    let table_rows = destination.get_table_rows().await;
    let users_table_rows = table_rows.get(&database_schema.users_schema().id).unwrap();
    let orders_table_rows = table_rows.get(&database_schema.orders_schema().id).unwrap();
    assert_eq!(users_table_rows.len(), rows_inserted);
    assert_eq!(orders_table_rows.len(), rows_inserted);

    // Verify age sum calculation.
    let expected_age_sum = get_n_integers_sum(rows_inserted);
    let age_sum =
        get_users_age_sum_from_rows(&destination, database_schema.users_schema().id).await;
    assert_eq!(age_sum, expected_age_sum);

    // Get all the events that were produced to the destination and assert them
    // individually by table since the only thing we are guaranteed is that the
    // order of operations is preserved within the same table but not across
    // tables given the asynchronous nature of the pipeline (e.g., we could
    // start streaming earlier on a table for data which was inserted after another
    // table which was modified before this one)
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts =
        grouped_events.get(&(EventType::Insert, database_schema.users_schema().id)).unwrap();
    let orders_inserts =
        grouped_events.get(&(EventType::Insert, database_schema.orders_schema().id)).unwrap();

    // Build expected events for verification
    let expected_users_inserts = build_expected_users_inserts(
        11,
        &database_schema.users_schema(),
        vec![("user_11", 11), ("user_12", 12), ("user_13", 13), ("user_14", 14)],
    );
    let expected_orders_inserts = build_expected_orders_inserts(
        11,
        &database_schema.orders_schema(),
        vec!["description_11", "description_12", "description_13", "description_14"],
    );
    assert_events_equal(users_inserts, &expected_users_inserts);
    assert_events_equal(orders_inserts, &expected_orders_inserts);

    // Check that the replication slots for the two tables have been removed.
    let users_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.users_schema().id)
            .try_into()
            .unwrap();
    let orders_replication_slot: String =
        EtlReplicationSlot::for_table_sync_worker(pipeline_id, database_schema.orders_schema().id)
            .try_into()
            .unwrap();
    assert_eq!(database.get_replication_slot_state(&users_replication_slot).await.unwrap(), None);
    assert_eq!(database.get_replication_slot_state(&orders_replication_slot).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_streams_new_data_with_batch_timeout_expired() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::UsersOnly).await;

    let store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(store.clone()));

    // Start pipeline from scratch.
    let pipeline_id: PipelineId = random();
    // We set a batch of 1000 elements to check if after 1000ms we still get the
    // batch which is < 1000 elements.
    let batch_config = BatchConfig {
        max_fill_ms: 1000,
        memory_budget_ratio: 0.2,
        max_bytes: BatchConfig::DEFAULT_MAX_BYTES,
    };
    let mut pipeline = create_pipeline_with_batch_config(
        &database.config,
        pipeline_id,
        database_schema.publication_name(),
        store.clone(),
        destination.clone(),
        batch_config,
    );

    // Register copy-completion and steady-state notifications before startup.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;

    let rows_inserted = 5;
    let events_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            5,
        )])
        .await;

    // Insert additional data to test streaming.
    insert_users_data(&mut database, &database_schema.users_schema().name, 1..=rows_inserted).await;

    events_notify.notified().await;
    users_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let users_inserts =
        grouped_events.get(&(EventType::Insert, database_schema.users_schema().id)).unwrap();
    // Build expected events for verification
    let expected_users_inserts = build_expected_users_inserts(
        1,
        &database_schema.users_schema(),
        vec![("user_1", 1), ("user_2", 2), ("user_3", 3), ("user_4", 4), ("user_5", 5)],
    );
    assert_events_equal(users_inserts, &expected_users_inserts);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_sync_no_event_handover_waits_for_first_dml_before_ready() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
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

    let sync_done_notify =
        store.notify_on_table_state_type(table_id, TableStateType::SyncDone).await;
    let ready_notify = store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    sync_done_notify.notified().await;
    let table_state = store.get_table_state(table_id).await.unwrap().unwrap();
    let TableState::SyncDone { table_decoding_state, .. } = table_state else {
        panic!("an eventless handover should remain in SyncDone");
    };
    assert!(table_decoding_state.is_some());

    let first_streamed_row_notify = destination
        .wait_for_events(vec![EventCondition::TableCount(EventType::Insert, table_id, 1)])
        .await;
    insert_users_data(&mut database, &users_schema.name, 1..=1).await;

    first_streamed_row_notify.notified().await;
    ready_notify.notified().await;
    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);
    let inserts = grouped_events.get(&(EventType::Insert, table_id)).unwrap();
    let expected_inserts = build_expected_users_inserts(1, &users_schema, vec![("user_1", 1)]);
    assert_events_equal(inserts, &expected_inserts);
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_column_filter_changes_update_snapshots_without_shifting_dml_ordinals() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Column filters in publication are only available from Postgres 15+.
    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    // Create a table with multiple columns.
    let table_name = test_table_name("users");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[
                ("name", "text not null"),
                ("age", "integer not null"),
                ("email", "text not null"),
                ("phone", "text not null"),
            ],
        )
        .await
        .unwrap();

    // Create publication with only a subset of columns.
    let publication_name = "test_pub".to_owned();
    database
        .run_sql(&format!(
            "create publication {} for table {} (id, name, age)",
            quote_identifier(&publication_name),
            table_name.as_quoted_identifier()
        ))
        .await
        .expect("Failed to create publication with column filter");

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name.clone(),
        state_store.clone(),
        destination.clone(),
    );

    let table_sync_complete_notify = state_store.notify_on_table_sync_complete(table_id).await;
    let table_ready_notify =
        state_store.notify_on_table_state_type(table_id, TableStateType::Ready).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    // Wait for an insert event to be processed.
    let insert_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 1),
            EventCondition::TableCount(EventType::Insert, table_id, 1),
        ])
        .await;

    // Insert test data with all columns (including email and phone).
    database
        .run_sql(&format!(
            "insert into {} (name, age, email, phone) values ('Alice', 25, 'alice@example.com', \
             '555-0001')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    insert_events_notify.notified().await;
    table_ready_notify.notified().await;

    // Add email column to publication -> (id, name, age, email).
    database
        .run_sql(&format!(
            "alter publication {publication_name} set table {} (id, name, age, email)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for 1 insert event with 4 columns.
    let insert_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 2),
            EventCondition::TableCount(EventType::Insert, table_id, 2),
        ])
        .await;

    database
        .run_sql(&format!(
            "insert into {} (name, age, email, phone) values ('Charlie', 35, \
             'charlie@example.com', '555-0003')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    insert_notify.notified().await;

    // Remove age column from publication -> (id, name, email).
    database
        .run_sql(&format!(
            "alter publication {publication_name} set table {} (id, name, email)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // Wait for 1 insert event with 3 columns (different set than before).
    let insert_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(EventType::Relation, table_id, 3),
            EventCondition::TableCount(EventType::Insert, table_id, 3),
        ])
        .await;

    database
        .run_sql(&format!(
            "insert into {} (name, age, email, phone) values ('Diana', 40, 'diana@example.com', \
             '555-0004')",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    insert_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    let events = destination.get_events().await;
    let relations = events
        .iter()
        .filter_map(|event| match event {
            Event::Relation(relation) if relation.replicated_table_schema.id() == table_id => {
                Some(relation)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let inserts = events
        .iter()
        .filter_map(|event| match event {
            Event::Insert(insert) if insert.replicated_table_schema.id() == table_id => {
                Some(insert)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(relations.len(), 3);
    assert_eq!(inserts.len(), 3);

    let expected_column_names = [
        vec!["id", "name", "age"],
        vec!["id", "name", "age", "email"],
        vec!["id", "name", "email"],
    ];
    let expected_masks = [vec![1_u8, 1, 1, 0, 0], vec![1, 1, 1, 1, 0], vec![1, 1, 0, 1, 0]];

    for ((relation, expected_names), expected_mask) in
        relations.iter().zip(expected_column_names).zip(expected_masks)
    {
        let column_names = relation
            .replicated_table_schema
            .column_schemas()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(column_names, expected_names);
        assert_eq!(relation.replicated_table_schema.replication_mask().as_slice(), expected_mask);
        assert_eq!(relation.replicated_table_schema.inner().column_schemas.len(), 5);
    }

    let relation_snapshot_ids = relations
        .iter()
        .map(|relation| relation.replicated_table_schema.inner().snapshot_id)
        .collect::<Vec<_>>();
    assert!(relation_snapshot_ids.windows(2).all(|window| window[0] < window[1]));

    // BEGIN consumes ordinal 0. Connection-local Relation messages must not
    // consume ordinals, including after either publication change.
    assert!(inserts.iter().all(|insert| insert.tx_ordinal == 1));
    let insert_snapshot_ids = inserts
        .iter()
        .map(|insert| insert.replicated_table_schema.inner().snapshot_id)
        .collect::<Vec<_>>();
    assert_eq!(insert_snapshot_ids, relation_snapshot_ids);
    let insert_values =
        inserts.iter().map(|insert| insert.table_row.values().to_vec()).collect::<Vec<_>>();
    assert_eq!(
        insert_values,
        vec![
            vec![Cell::I64(1), Cell::String("Alice".to_owned()), Cell::I32(25)],
            vec![
                Cell::I64(2),
                Cell::String("Charlie".to_owned()),
                Cell::I32(35),
                Cell::String("charlie@example.com".to_owned()),
            ],
            vec![
                Cell::I64(3),
                Cell::String("Diana".to_owned()),
                Cell::String("diana@example.com".to_owned()),
            ],
        ]
    );

    let stored_snapshots = state_store.get_table_schemas().await;
    let stored_snapshot_ids =
        stored_snapshots[&table_id].iter().map(|(snapshot_id, _)| *snapshot_id).collect::<Vec<_>>();
    assert_eq!(stored_snapshot_ids, vec![relation_snapshot_ids[2]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_tables_are_created_at_destination() {
    init_test_tracing();
    let database = spawn_source_database().await;

    // Create an empty table with a primary key.
    let table_name = test_table_name("empty_table");
    let table_id = database
        .create_table(table_name.clone(), true, &[("name", "text"), ("created_at", "timestamp")])
        .await
        .unwrap();

    // Create publication for the table.
    let publication_name = format!("pub_{}", random::<u32>());
    database
        .run_sql(&format!(
            "create publication {} for table {}",
            quote_identifier(&publication_name),
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let state_store = NotifyingStore::new();
    let destination = TestDestinationWrapper::wrap(MemoryDestination::new(state_store.clone()));

    // Start the pipeline.
    let pipeline_id: PipelineId = random();
    let mut pipeline = create_pipeline(
        &database.config,
        pipeline_id,
        publication_name,
        state_store.clone(),
        destination.clone(),
    );

    // An empty table has no apply-owned row from which to materialize a local
    // decoder, so its completed handover intentionally remains in SyncDone.
    let table_sync_complete_notify = state_store.notify_on_table_sync_complete(table_id).await;

    pipeline.start().await.unwrap();

    table_sync_complete_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the table schema was stored.
    let table_schemas = state_store.get_latest_table_schemas().await;
    let table_schema = table_schemas.get(&table_id).unwrap();
    assert_eq!(table_schema.id, table_id);
    assert_eq!(table_schema.name, table_name);
    assert_table_schema_columns(
        table_schema,
        &[
            id_column_schema(),
            test_column("name", Type::TEXT, 2, true, false),
            test_column("created_at", Type::TIMESTAMP, 3, true, false),
        ],
    );

    // Verify no rows were written (table was empty).
    let all_table_rows = destination.get_table_rows().await;
    let empty_vec = vec![];
    let table_rows = all_table_rows.get(&table_id).unwrap_or(&empty_vec);
    assert!(table_rows.is_empty());

    // Verify that the write table rows method was called nonetheless.
    assert_eq!(destination.write_table_rows_called().await, 1);
}

/// Tests that resetting a table's state to Init triggers a table sync that
/// drops the destination table before re-copying data. This ensures no
/// duplicate data after a state reset.
///
/// Test flow:
/// 1. Initial table sync: 5 rows (ids 1-5) written to table_rows for both users
///    and orders
/// 2. CDC phase: 2 rows (ids 6-7) written as events for both tables
/// 3. Reset users table state to Init
/// 4. Insert 3 new rows (ids 100-102) for users only
/// 5. Verify: users has 10 total rows (table_rows + events), orders unchanged
#[tokio::test(flavor = "multi_thread")]
async fn table_sync_drops_destination_table_after_state_reset() {
    init_test_tracing();
    let mut database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

    let initial_rows = 5;
    let cdc_rows = 2;
    let new_rows_after_reset = 3;

    // Insert initial test data (ids 1-5).
    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        1..=initial_rows,
        false,
    )
    .await;

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

    // Register readiness before startup because initial sync may stop at
    // SyncDone. Reset only after the apply worker owns the table so the
    // finishing table-sync worker cannot overwrite the new Init state.
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let orders_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.orders_schema().id).await;

    pipeline.start().await.unwrap();

    users_sync_complete_notify.notified().await;
    orders_sync_complete_notify.notified().await;

    // Insert CDC data (ids 6-7) for both tables.
    let cdc_events_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(
                EventType::Insert,
                database_schema.users_schema().id,
                cdc_rows as u64,
            ),
            EventCondition::TableCount(
                EventType::Insert,
                database_schema.orders_schema().id,
                cdc_rows as u64,
            ),
        ])
        .await;

    insert_mock_data(
        &mut database,
        &database_schema.users_schema().name,
        &database_schema.orders_schema().name,
        (initial_rows + 1)..=(initial_rows + cdc_rows),
        true,
    )
    .await;

    cdc_events_notify.notified().await;
    users_ready_notify.notified().await;

    // Verify state before reset: table_rows has initial data, events has CDC data.
    let table_rows_before = destination.get_table_rows().await;
    let users_rows_before =
        table_rows_before.get(&database_schema.users_schema().id).unwrap().len();
    let orders_rows_before =
        table_rows_before.get(&database_schema.orders_schema().id).unwrap().len();
    assert_eq!(users_rows_before, initial_rows);
    assert_eq!(orders_rows_before, initial_rows);

    let events_before = destination.get_events().await;
    let grouped_events_before = group_events_by_type_and_table_id(&events_before);
    let users_events_before = grouped_events_before
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .unwrap()
        .len();
    let orders_events_before = grouped_events_before
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .unwrap()
        .len();
    assert_eq!(users_events_before, cdc_rows);
    assert_eq!(orders_events_before, cdc_rows);

    let orders_total_before = orders_rows_before + orders_events_before;

    let expected_users_resync_writes = initial_rows + cdc_rows + new_rows_after_reset;

    // Register waits before resetting state and producing the rows that drive
    // the resync.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let users_drop_held = destination.hold_next(FaultyOp::DropTableForCopy).await;
    let all_users_events_notify = destination
        .wait_for_all_events(vec![EventCondition::TableCount(
            EventType::Insert,
            database_schema.users_schema().id,
            expected_users_resync_writes as u64,
        )])
        .await;

    // Reset users table state to Init, triggering a fresh table sync.
    store.reset_table_state(database_schema.users_schema().id).await.unwrap();
    users_drop_held.wait_reached().await;
    users_drop_held.release_ok();

    // Insert new users (ids 100-102) after reset.
    for id in 100i64..103i64 {
        database
            .insert_values(
                database_schema.users_schema().name.clone(),
                &["id", "name", "age"],
                &[&id, &format!("user_{id}"), &(id as i32)],
            )
            .await
            .unwrap();
    }

    users_sync_complete_notify.notified().await;
    all_users_events_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Verify the final state.
    let table_rows_after = destination.get_table_rows().await;
    let events_after = destination.get_events().await;
    let grouped_events_after = group_events_by_type_and_table_id(&events_after);

    // Dropping users for a fresh copy removes that table's prior history from
    // the wrapper. The replacement writes may be split between copy and
    // streaming events.
    let users_rows = table_rows_after.get(&database_schema.users_schema().id).unwrap().len();
    let users_events = grouped_events_after
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .map_or(0, Vec::len);
    assert_eq!(users_rows + users_events, expected_users_resync_writes);

    // Orders are not resynced, so their cumulative history remains unchanged.
    let orders_rows = table_rows_after.get(&database_schema.orders_schema().id).map_or(0, Vec::len);
    let orders_events = grouped_events_after
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .map_or(0, Vec::len);
    assert_eq!(orders_rows + orders_events, orders_total_before);

    // Verify the destination table was dropped for users but not for orders.
    assert!(destination.was_table_dropped_for_copy(database_schema.users_schema().id).await);
    assert!(!destination.was_table_dropped_for_copy(database_schema.orders_schema().id).await);

    let user_schemas = SchemaStore::get_table_schemas(&store)
        .await
        .unwrap()
        .into_iter()
        .filter(|schema| schema.id == database_schema.users_schema().id)
        .collect::<Vec<_>>();
    assert_eq!(user_schemas.len(), 1);
    assert_eq!(user_schemas[0].snapshot_id, etl::schema::SnapshotId::initial());
}

#[tokio::test(flavor = "multi_thread")]
async fn pipeline_processes_concurrent_inserts_during_startup() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let database_schema = setup_test_database_schema(&database, TableSelection::Both).await;

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

    let rows_to_insert = 10;

    // Register startup and Ready notifications before starting the pipeline so
    // we do not miss state transitions or events that happen during startup.
    // `notify_on_*` and `wait_for_*` only fire on updates that occur after
    // registration. Every concurrent row may be consumed by table sync, so
    // completion does not require an apply-owned row that would advance
    // SyncDone to Ready.
    let users_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.users_schema().id).await;
    let orders_sync_complete_notify =
        store.notify_on_table_sync_complete(database_schema.orders_schema().id).await;
    let users_ready_notify = store
        .notify_on_table_state_type(database_schema.users_schema().id, TableStateType::Ready)
        .await;
    let orders_ready_notify = store
        .notify_on_table_state_type(database_schema.orders_schema().id, TableStateType::Ready)
        .await;

    // Wait for all rows to be processed (either as table copy or streaming
    // inserts), requiring the expected count for each table.
    let all_events_notify = destination
        .wait_for_all_events(vec![
            EventCondition::TableCount(
                EventType::Insert,
                database_schema.users_schema().id,
                rows_to_insert as u64,
            ),
            EventCondition::TableCount(
                EventType::Insert,
                database_schema.orders_schema().id,
                rows_to_insert as u64,
            ),
        ])
        .await;

    // Start the pipeline only after all notifications are registered so we
    // cannot miss fast SyncDone transitions on CI.
    pipeline.start().await.unwrap();

    // Spawn a task that inserts data concurrently using a separate connection.
    // This creates a race condition where some rows may be captured during table
    // copy and others during streaming replication.
    let users_table_name = database_schema.users_schema().name.clone();
    let orders_table_name = database_schema.orders_schema().name.clone();
    let mut duplicate_database = database.duplicate().await;

    // Use a JoinHandle to ensure the task completes and the database isn't dropped
    // prematurely.
    let insert_handle = tokio::spawn(async move {
        insert_mock_data(
            &mut duplicate_database,
            &users_table_name,
            &orders_table_name,
            1..=rows_to_insert,
            true,
        )
        .await;

        // Return the database to prevent it from being dropped and destroying the test
        // database.
        duplicate_database
    });

    users_sync_complete_notify.notified().await;
    orders_sync_complete_notify.notified().await;
    all_events_notify.notified().await;

    // Wait for the insert task to complete and retrieve the database connection.
    let duplicate_database = insert_handle.await.unwrap();

    // Validate that the sum of table rows (from copy) + insert events (from
    // streaming) equals expected count.
    let table_rows = destination.get_table_rows().await;
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);

    let users_copied_rows = table_rows.get(&database_schema.users_schema().id).map_or(0, Vec::len);
    let users_insert_events = grouped_events
        .get(&(EventType::Insert, database_schema.users_schema().id))
        .map_or(0, Vec::len);
    let total_users = users_copied_rows + users_insert_events;

    let orders_copied_rows =
        table_rows.get(&database_schema.orders_schema().id).map_or(0, Vec::len);
    let orders_insert_events = grouped_events
        .get(&(EventType::Insert, database_schema.orders_schema().id))
        .map_or(0, Vec::len);
    let total_orders = orders_copied_rows + orders_insert_events;

    assert_eq!(total_users, rows_to_insert);
    assert_eq!(total_orders, rows_to_insert);

    // Every startup insert may have been consumed by table sync, leaving no
    // apply-owned row from which to materialize a local decoder. If a row
    // crossed the ownership boundary, the table may already be Ready.
    let states = store.get_table_states().await;
    assert!(matches!(
        states.get(&database_schema.users_schema().id),
        Some(TableState::SyncDone { .. } | TableState::Ready)
    ));
    assert!(matches!(
        states.get(&database_schema.orders_schema().id),
        Some(TableState::SyncDone { .. } | TableState::Ready)
    ));

    // Spawn a task to perform updates and deletes.
    let rows_to_update = 5;
    let rows_to_delete = 3;
    let users_table_name = database_schema.users_schema().name.clone();
    let orders_table_name = database_schema.orders_schema().name.clone();

    // The first apply-owned update restores each stored SyncDone decoder. The
    // Ready notifications are already armed; register the update/delete event
    // notification before producing that DML.
    let updates_deletes_notify = destination
        .wait_for_events(vec![
            EventCondition::TableCount(
                EventType::Update,
                database_schema.users_schema().id,
                rows_to_update as u64,
            ),
            EventCondition::TableCount(
                EventType::Update,
                database_schema.orders_schema().id,
                rows_to_update as u64,
            ),
            EventCondition::TableCount(
                EventType::Delete,
                database_schema.users_schema().id,
                rows_to_delete as u64,
            ),
            EventCondition::TableCount(
                EventType::Delete,
                database_schema.orders_schema().id,
                rows_to_delete as u64,
            ),
        ])
        .await;

    let update_delete_handle = tokio::spawn(async move {
        // Update rows 1-5 for both tables.
        for i in 1..=rows_to_update {
            duplicate_database
                .update_with_expressions(
                    users_table_name.clone(),
                    &["age = age + 100"],
                    &["id"],
                    &[&i.to_string()],
                    " and ",
                )
                .await
                .unwrap();

            duplicate_database
                .update_with_expressions(
                    orders_table_name.clone(),
                    &["description = description || '_updated'"],
                    &["id"],
                    &[&i.to_string()],
                    " and ",
                )
                .await
                .unwrap();
        }

        // Delete rows 6-8 for both tables.
        for i in 6..=(6 + rows_to_delete - 1) {
            duplicate_database
                .delete_values(users_table_name.clone(), &["id"], &[&i.to_string()], " and ")
                .await
                .unwrap();

            duplicate_database
                .delete_values(orders_table_name.clone(), &["id"], &[&i.to_string()], " and ")
                .await
                .unwrap();
        }

        duplicate_database
    });

    updates_deletes_notify.notified().await;
    users_ready_notify.notified().await;
    orders_ready_notify.notified().await;

    pipeline.shutdown_and_wait().await.unwrap();

    // Wait for the update/delete task to complete.
    let _duplicate_database = update_delete_handle.await.unwrap();

    // Validate that both tables are in Ready state.
    let states = store.get_table_states().await;
    assert_eq!(states.get(&database_schema.users_schema().id), Some(&TableState::Ready));
    assert_eq!(states.get(&database_schema.orders_schema().id), Some(&TableState::Ready));

    // Validate the update and delete events were received correctly.
    let events = destination.get_events().await;
    let grouped_events = group_events_by_type_and_table_id(&events);

    let users_updates = grouped_events
        .get(&(EventType::Update, database_schema.users_schema().id))
        .map_or(0, Vec::len);
    let users_deletes = grouped_events
        .get(&(EventType::Delete, database_schema.users_schema().id))
        .map_or(0, Vec::len);

    let orders_updates = grouped_events
        .get(&(EventType::Update, database_schema.orders_schema().id))
        .map_or(0, Vec::len);
    let orders_deletes = grouped_events
        .get(&(EventType::Delete, database_schema.orders_schema().id))
        .map_or(0, Vec::len);

    assert_eq!(users_updates, rows_to_update);
    assert_eq!(users_deletes, rows_to_delete);
    assert_eq!(orders_updates, rows_to_update);
    assert_eq!(orders_deletes, rows_to_delete);
}
