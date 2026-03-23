use etl::error::{EtlError, EtlResult};
use etl::state::table::TableReplicationPhase;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::info;

use crate::error_notification::ErrorNotificationClient;
use crate::sentry;

/// State store decorator that reports persisted table replication errors.
///
/// After [`StateStore::update_table_replication_states`] succeeds, this wrapper
/// reports each [`TableReplicationPhase::Errored`] update to Sentry and, when
/// configured, to the Supabase error-notification endpoint.
#[derive(Debug, Clone)]
pub struct ErrorReportingStateStore<S> {
    inner: S,
    notification_client: Option<Arc<ErrorNotificationClient>>,
}

/// Persisted table error waiting to be reported.
#[derive(Debug)]
struct ReportableTableError {
    /// Table whose replication state was persisted as errored.
    table_id: TableId,
    /// Source ETL error captured in the persisted table phase.
    source_err: EtlError,
}

impl<S> ErrorReportingStateStore<S> {
    /// Creates a reporting wrapper around `inner`.
    pub fn new(inner: S, notification_client: Option<ErrorNotificationClient>) -> Self {
        Self {
            inner,
            notification_client: notification_client.map(Arc::new),
        }
    }

    /// Reports persisted errored table state updates.
    async fn report_errored_updates(&self, updates: Vec<ReportableTableError>) {
        let notification_client = self.notification_client.as_ref();

        for update in updates {
            info!(
                table_id = update.table_id.0,
                "reporting table replication error"
            );

            sentry::capture_table_error(update.table_id, &update.source_err);
            if let Some(notification_client) = notification_client {
                notification_client
                    .notify_error(update.source_err.to_string(), &update.source_err)
                    .await;
            }
        }
    }

    /// Extracts only the errored state updates that need post-persistence reporting.
    fn collect_reportable_errors(
        updates: &[(TableId, TableReplicationPhase)],
    ) -> Vec<ReportableTableError> {
        updates
            .iter()
            .filter_map(|(table_id, phase)| match phase {
                TableReplicationPhase::Errored { source_err, .. } => Some(ReportableTableError {
                    table_id: *table_id,
                    source_err: source_err.clone(),
                }),
                _ => None,
            })
            .collect()
    }
}

impl<S> StateStore for ErrorReportingStateStore<S>
where
    S: StateStore + Send + Sync,
{
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        self.inner.get_table_replication_state(table_id).await
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        self.inner.get_table_replication_states().await
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        self.inner.load_table_replication_states().await
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        // We collect all errors in advance, to avoid cloning the whole set of updates.
        let reportable_errors = Self::collect_reportable_errors(&updates);

        self.inner.update_table_replication_states(updates).await?;

        // This operation must be infallible or at least not propagate failures, otherwise the
        // error thrown here, will be caught and handled by the core of etl itself. There is no
        // infinite recursion problem, but it might make the system harder to understand.
        self.report_errored_updates(reportable_errors).await;

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        self.inner.rollback_table_replication_state(table_id).await
    }

    async fn get_table_mapping(&self, source_table_id: &TableId) -> EtlResult<Option<String>> {
        self.inner.get_table_mapping(source_table_id).await
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        self.inner.get_table_mappings().await
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        self.inner.load_table_mappings().await
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        self.inner
            .store_table_mapping(source_table_id, destination_table_id)
            .await
    }
}

impl<S> SchemaStore for ErrorReportingStateStore<S>
where
    S: SchemaStore + Send + Sync,
{
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        self.inner.get_table_schema(table_id).await
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        self.inner.get_table_schemas().await
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        self.inner.load_table_schemas().await
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        self.inner.store_table_schema(table_schema).await
    }
}

impl<S> CleanupStore for ErrorReportingStateStore<S>
where
    S: CleanupStore + Send + Sync,
{
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        self.inner.cleanup_table_state(table_id).await
    }
}
