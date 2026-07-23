use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
    time::Instant,
};

use tokio::{
    runtime::Handle,
    sync::{Notify, RwLock},
};

use crate::{
    data::TableRow,
    destination::{
        ApplyLoopAsyncResultMetadata, Destination, DropTableForCopyResult, EventBatchMetrics,
        PipelineDestination, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
    },
    error::EtlResult,
    event::Event,
    runtime::concurrency::TaskSet,
    schema::{ReplicatedTableSchema, TableId},
    test_utils::{
        event::{EventCondition, check_all_event_conditions, check_event_conditions},
        faults::{FaultAction, FaultInjector, FaultyOp, HoldHandle, apply_response_fault},
        notify::TimedNotify,
    },
};

type EventsCheckFn = Box<dyn Fn(&[Event]) -> bool + Send + Sync>;
type AllEventsCheckFn =
    Box<dyn Fn(&[Event], &HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync>;

struct Inner<D> {
    wrapped_destination: D,
    events: Vec<Event>,
    table_rows: HashMap<TableId, Vec<TableRow>>,
    tables_dropped_for_copy: HashSet<TableId>,
    event_notifications: Vec<(EventsCheckFn, Arc<Notify>)>,
    all_event_notifications: Vec<(AllEventsCheckFn, Arc<Notify>)>,
    write_table_rows_called: u64,
    shutdown_called: bool,
}

impl<D> Inner<D> {
    fn check_conditions(&mut self) {
        // Check event conditions.
        let events = &self.events;
        self.event_notifications.retain(|(condition, notify)| {
            let should_retain = !condition(events);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });

        let table_rows = &self.table_rows;
        // Check conditions spanning streaming events and table-copy rows.
        self.all_event_notifications.retain(|(condition, notify)| {
            let should_retain = !condition(events, table_rows);
            if !should_retain {
                notify.notify_one();
            }
            should_retain
        });
    }
}

/// Test wrapper for [`Destination`] implementations that tracks all operations.
///
/// [`TestDestinationWrapper`] wraps any destination implementation and records
/// all rows, events, truncations, and shutdown calls flowing through it. This
/// enables test assertions on pipeline behavior without requiring complex
/// destination setup.
///
/// Notification helpers only observe writes that happen after registration.
/// Register the returned [`TimedNotify`] before starting the producer that is
/// expected to satisfy it.
///
/// Faults from [`crate::test_utils::faults`] can be scripted per operation
/// through [`TestDestinationWrapper::inject_fault`]. The wrapper records what
/// was acknowledged to the apply loop: on an injected failure after write the
/// inner destination has applied the write but the wrapper does not record it,
/// so ground truth for what the destination actually holds is read from the
/// inner destination directly.
#[derive(Clone)]
pub struct TestDestinationWrapper<D> {
    inner: Arc<RwLock<Inner<D>>>,
    tasks: TaskSet,
    faults: FaultInjector,
}

impl<D: fmt::Debug> fmt::Debug for TestDestinationWrapper<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = tokio::task::block_in_place(move || {
            Handle::current().block_on(async move { self.inner.read().await })
        });
        f.debug_struct("TestDestinationWrapper")
            .field("wrapped_destination", &inner.wrapped_destination)
            .field("events", &inner.events)
            .field("table_rows", &inner.table_rows)
            .finish()
    }
}

impl<D> TestDestinationWrapper<D> {
    /// Creates a new test wrapper around any destination implementation.
    ///
    /// The wrapper forwards every operation to the wrapped destination and
    /// keeps an in-memory history for assertions.
    pub fn wrap(destination: D) -> Self {
        let inner = Inner {
            wrapped_destination: destination,
            events: Vec::new(),
            table_rows: HashMap::new(),
            tables_dropped_for_copy: HashSet::new(),
            event_notifications: Vec::new(),
            all_event_notifications: Vec::new(),
            write_table_rows_called: 0,
            shutdown_called: false,
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
            tasks: TaskSet::new(),
            faults: FaultInjector::new(),
        }
    }

    /// Returns all table rows written through the wrapper.
    pub async fn get_table_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.read().await.table_rows.clone()
    }

    /// Returns all events written through the wrapper.
    pub async fn get_events(&self) -> Vec<Event> {
        self.inner.read().await.events.clone()
    }

    /// Registers a notification that fires when future events match a
    /// condition.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the condition is not met. This prevents tests
    /// from hanging indefinitely.
    pub async fn notify_on_events<F>(&self, condition: F) -> TimedNotify
    where
        F: Fn(&[Event]) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.event_notifications.push((Box::new(condition), Arc::clone(&notify)));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when future events satisfy all
    /// requested conditions.
    ///
    /// Only streaming events are counted. Use
    /// [`TestDestinationWrapper::wait_for_all_events`] when copied table rows
    /// should also count as inserts.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected event count is not reached. This
    /// prevents tests from hanging indefinitely.
    pub async fn wait_for_events(&self, conditions: Vec<EventCondition>) -> TimedNotify {
        self.notify_on_events(move |events| check_event_conditions(events, &conditions)).await
    }

    /// Registers a notification that fires when future events and table-copy
    /// rows satisfy a condition.
    ///
    /// Table-copy rows are presented separately from streaming events so the
    /// condition can decide how to interpret them.
    pub async fn notify_on_all_events<F>(&self, condition: F) -> TimedNotify
    where
        F: Fn(&[Event], &HashMap<TableId, Vec<TableRow>>) -> bool + Send + Sync + 'static,
    {
        let notify = Arc::new(Notify::new());
        let mut inner = self.inner.write().await;
        inner.all_event_notifications.push((Box::new(condition), Arc::clone(&notify)));

        TimedNotify::new(notify)
    }

    /// Registers a notification that fires when future events and table rows
    /// satisfy all requested conditions.
    ///
    /// Supports two condition types:
    /// - [`EventCondition::AnyCount`]: counts events across all tables
    /// - [`EventCondition::TableCount`]: counts events for a specific table
    ///   only
    ///
    /// Copied table rows are treated as insert events. Other event types only
    /// count streaming events.
    ///
    /// Returns a [`TimedNotify`] that will automatically timeout after the
    /// specified timeout if the expected count is not reached.
    pub async fn wait_for_all_events(&self, conditions: Vec<EventCondition>) -> TimedNotify {
        self.notify_on_all_events(move |events, table_rows| {
            check_all_event_conditions(events, table_rows, &conditions)
        })
        .await
    }

    /// Clears the recorded table rows without touching the wrapped destination.
    pub async fn clear_table_rows(&self) {
        let mut inner = self.inner.write().await;
        inner.table_rows.clear();
    }

    /// Clears the recorded events without touching the wrapped destination.
    pub async fn clear_events(&self) {
        let mut inner = self.inner.write().await;
        inner.events.clear();
    }

    /// Returns whether the table was dropped for a fresh copy through the
    /// wrapper.
    pub async fn was_table_dropped_for_copy(&self, table_id: TableId) -> bool {
        self.inner.read().await.tables_dropped_for_copy.contains(&table_id)
    }

    /// Returns how many times [`Destination::write_table_rows`] was called.
    pub async fn write_table_rows_called(&self) -> u64 {
        self.inner.read().await.write_table_rows_called
    }

    /// Returns whether the shutdown method was called on the destination.
    pub async fn shutdown_called(&self) -> bool {
        self.inner.read().await.shutdown_called
    }

    /// Queues a fault for the next call of the given destination operation.
    pub async fn inject_fault(&self, op: FaultyOp, action: FaultAction) {
        self.faults.inject(op, action).await;
    }

    /// Holds the next call of the given operation and returns the handle that
    /// observes and releases it.
    pub async fn hold_next(&self, op: FaultyOp) -> HoldHandle {
        let (action, handle) = FaultAction::hold();
        self.faults.inject(op, action).await;
        handle
    }

    /// Consumes the next fault for the operation, applying rejections here.
    async fn take_fault(&self, op: FaultyOp) -> EtlResult<Option<FaultAction>> {
        match self.faults.next(op).await {
            Some(FaultAction::Reject(injected)) => Err(injected.to_etl_error()),
            fault => Ok(fault),
        }
    }
}

impl<D> Destination for TestDestinationWrapper<D>
where
    D: PipelineDestination,
{
    fn name() -> &'static str {
        "wrapper"
    }

    async fn startup(&self) -> EtlResult<()> {
        let fault = self.take_fault(FaultyOp::Startup).await?;

        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };
        let result = destination.startup().await;

        apply_response_fault(fault, result).await
    }

    async fn drop_table_for_copy(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        let fault = self.take_fault(FaultyOp::DropTableForCopy).await?;

        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let (wrapped_drop_result, pending_drop_result) = DropTableForCopyResult::new(());
        destination.drop_table_for_copy(replicated_table_schema, wrapped_drop_result).await?;

        // We send the result back before doing the internal checks for this utility, to
        // avoid checking before the apply loop received the result.
        let result = apply_response_fault(fault, pending_drop_result.await.into_result()).await;
        let should_record_drop = result.is_ok();
        async_result.send(result);

        let mut inner = self.inner.write().await;

        let table_id = replicated_table_schema.id();
        if should_record_drop {
            inner.tables_dropped_for_copy.insert(table_id);
            inner.table_rows.remove(&table_id);
            inner.events.retain_mut(|event| {
                let has_table_id = event.has_table_id(&table_id);
                if let Event::Truncate(truncate_event) = event
                    && has_table_id
                {
                    truncate_event.truncated_tables.retain(|s| s.id() != table_id);
                    if truncate_event.truncated_tables.is_empty() {
                        return false;
                    }

                    return true;
                }

                !has_table_id
            });
        }

        Ok(())
    }

    async fn write_table_rows(
        &self,
        replicated_table_schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        let fault = self.take_fault(FaultyOp::WriteTableRows).await?;

        let destination = {
            let mut inner = self.inner.write().await;
            inner.write_table_rows_called += 1;
            inner.wrapped_destination.clone()
        };

        let (wrapped_flush_result, pending_flush_result) = WriteTableRowsResult::new(());
        destination
            .write_table_rows(replicated_table_schema, table_rows.clone(), wrapped_flush_result)
            .await?;

        // We send the result back before doing the internal checks for this utility, to
        // avoid checking before the apply loop received the result.
        let result = apply_response_fault(fault, pending_flush_result.await.into_result()).await;
        let should_record_table_rows = result.is_ok();
        async_result.send(result);

        {
            let table_id = replicated_table_schema.id();
            let mut inner = self.inner.write().await;
            if should_record_table_rows {
                inner.table_rows.entry(table_id).or_default().extend(table_rows);
            }

            inner.check_conditions();
        }

        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        self.tasks.try_reap().await?;

        let fault = self.take_fault(FaultyOp::WriteEvents).await?;

        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let (wrapped_flush_result, pending_flush_result) =
            WriteEventsResult::new(ApplyLoopAsyncResultMetadata {
                commit_end_lsn: None,
                durability,
                metrics: EventBatchMetrics {
                    event_count: events.len(),
                    streaming_payload: Default::default(),
                    dispatched_at: Instant::now(),
                },
            });
        destination.write_events(events.clone(), durability, wrapped_flush_result).await?;

        // We spawn a task to handle the result, this way the wrapper behaves like a
        // transparent layer that doesn't block on the result of the inner
        // destination, effectively exhibiting the fully asynchronous behavior
        // that a destination could have.
        //
        // For the other destination methods with async result this is not needed since
        // the methods on the outside block on the result right after calling
        // the method, so it's not needed to simulate asynchronous work to make
        // the code continue and do something else in the meanwhile.
        let inner = Arc::clone(&self.inner);
        self.tasks
            .spawn(async move {
                // We send the result back before doing the internal checks for this utility, to
                // avoid checking before the apply loop received the result.
                let result =
                    apply_response_fault(fault, pending_flush_result.await.into_result()).await;
                let should_record_events = result.is_ok();
                async_result.send(result);

                {
                    let mut inner = inner.write().await;
                    if should_record_events {
                        inner.events.extend(events);
                    }

                    inner.check_conditions();
                }
            })
            .await;

        Ok(())
    }

    async fn shutdown(&self) -> EtlResult<()> {
        // Record the invocation before applying faults so tests can assert the
        // pipeline called shutdown even when the shutdown itself fails.
        {
            let mut inner = self.inner.write().await;
            inner.shutdown_called = true;
        }

        let fault = self.take_fault(FaultyOp::Shutdown).await?;

        let destination = {
            let inner = self.inner.read().await;
            inner.wrapped_destination.clone()
        };

        let mut errors = Vec::new();
        if let Err(err) = destination.shutdown().await {
            errors.push(err);
        }

        match self.tasks.drain().await {
            Ok(guard) => drop(guard),
            Err(err) => errors.push(err),
        }

        let result = if errors.is_empty() { Ok(()) } else { Err(errors.into()) };
        apply_response_fault(fault, result).await
    }
}
