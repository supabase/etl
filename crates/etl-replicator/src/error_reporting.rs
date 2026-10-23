use std::{collections::BTreeMap, sync::Arc};

use etl::{
    destination::DestinationTableMetadata,
    error::{EtlError, EtlResult},
    schema::{PgLsn, SnapshotId, TableId, TableSchema},
    store::{
        CachedStore, SchemaStore, StateStore, TableRetryPolicy, TableState,
        TableStateLifecycleStore, TableStateOperation, TableStates, WorkerType,
    },
    task::TaskRegistry,
};
use tracing::info;

use crate::{error_notification::ErrorNotificationClient, sentry};

/// State store decorator that reports persisted table replication errors.
///
/// After [`StateStore::update_table_states`] succeeds, this wrapper reports
/// each terminal [`TableState::Errored`] update in a background task to Sentry
/// and, when configured, to the Supabase error-notification endpoint. Errors
/// with a timed retry remain visible in the state store but are not reported
/// while automatic recovery is still in progress.
#[derive(Debug, Clone)]
pub(crate) struct ErrorReportingStateStore<S> {
    inner: S,
    /// Client used for sending notifications.
    notification_client: Option<Arc<ErrorNotificationClient>>,
    /// Owned error-reporting tasks shared by all store clones.
    reporting_tasks: TaskRegistry,
}

/// Persisted table error waiting to be reported.
#[derive(Debug)]
struct ReportableTableError {
    /// Table whose table state was persisted as errored.
    table_id: TableId,
    /// Source ETL error captured in the persisted table state.
    source_err: EtlError,
}

impl<S> ErrorReportingStateStore<S> {
    /// Creates a reporting wrapper around `inner`.
    pub(crate) fn new(inner: S, notification_client: Option<ErrorNotificationClient>) -> Self {
        Self {
            inner,
            notification_client: notification_client.map(Arc::new),
            reporting_tasks: TaskRegistry::new(),
        }
    }

    /// Aborts and joins best-effort reporting after pipeline workers stop.
    pub(crate) async fn shutdown(&self) -> EtlResult<()> {
        self.reporting_tasks.shutdown().await
    }

    /// Reports persisted errors without waiting for email delivery.
    async fn report_errored_updates(&self, updates: Vec<ReportableTableError>) -> EtlResult<()> {
        self.reporting_tasks.try_reap().await?;

        for update in updates {
            info!(table_id = update.table_id.0, "reporting table replication error");

            // Capture only the client, never the store that owns the task
            // registry.
            let notification_client = self.notification_client.clone();
            self.reporting_tasks
                .spawn(async move {
                    sentry::capture_table_error(update.table_id, &update.source_err);

                    if let Some(notification_client) = notification_client {
                        notification_client
                            .notify_error(update.source_err.to_string(), &update.source_err)
                            .await;
                    }
                })
                .await;
        }

        Ok(())
    }

    /// Extracts only the errored state updates that need post-persistence
    /// reporting.
    fn collect_reportable_errors(updates: &[(TableId, TableState)]) -> Vec<ReportableTableError> {
        updates
            .iter()
            .filter_map(|(table_id, state)| match state {
                TableState::Errored {
                    retry_policy: TableRetryPolicy::TimedRetry { .. }, ..
                } => None,
                TableState::Errored { source_err, .. } => Some(ReportableTableError {
                    table_id: *table_id,
                    source_err: source_err.clone(),
                }),
                _ => None,
            })
            .collect()
    }
}

impl<S> CachedStore for ErrorReportingStateStore<S>
where
    S: CachedStore + Sync,
{
    async fn load_cache(&self) -> EtlResult<()> {
        self.inner.load_cache().await
    }
}

impl<S> StateStore for ErrorReportingStateStore<S>
where
    S: StateStore + Sync,
{
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        self.inner.get_table_state(table_id).await
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        self.inner.get_table_states().await
    }

    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        // We collect all errors in advance, to avoid cloning the whole set of
        // updates.
        let reportable_errors = Self::collect_reportable_errors(&updates);

        self.inner.update_table_states(updates).await?;

        // We optimistically report the error updates without blocking the method.
        self.report_errored_updates(reportable_errors).await?;

        Ok(())
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        self.inner.rollback_table_state(table_id).await
    }

    async fn get_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> EtlResult<Option<PgLsn>> {
        self.inner.get_replication_checkpoint(worker_type).await
    }

    async fn upsert_replication_checkpoint(
        &self,
        worker_type: WorkerType,
        checkpoint_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        self.inner.upsert_replication_checkpoint(worker_type, checkpoint_lsn).await
    }

    async fn delete_replication_checkpoint(&self, worker_type: WorkerType) -> EtlResult<()> {
        self.inner.delete_replication_checkpoint(worker_type).await
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        self.inner.get_destination_table_metadata(table_id).await
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        self.inner.store_destination_table_metadata(table_id, metadata).await
    }
}

impl<S> SchemaStore for ErrorReportingStateStore<S>
where
    S: SchemaStore + Sync,
{
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        self.inner.get_table_schema(table_id, snapshot_id).await
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        self.inner.get_table_schemas().await
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        self.inner.store_table_schema(table_schema).await
    }

    async fn prune_table_schemas(
        &self,
        retention_snapshot_ids: BTreeMap<TableId, SnapshotId>,
    ) -> EtlResult<u64> {
        self.inner.prune_table_schemas(retention_snapshot_ids).await
    }
}

impl<S> TableStateLifecycleStore for ErrorReportingStateStore<S>
where
    S: TableStateLifecycleStore + Sync,
{
    async fn apply_table_state_operation(&self, operation: TableStateOperation) -> EtlResult<()> {
        self.inner.apply_table_state_operation(operation).await
    }

    async fn prepare_table_state_for_copy(&self, table_id: TableId) -> EtlResult<()> {
        self.inner.prepare_table_state_for_copy(table_id).await
    }

    async fn delete_table_state(&self, table_id: TableId) -> EtlResult<()> {
        self.inner.delete_table_state(table_id).await
    }
}
