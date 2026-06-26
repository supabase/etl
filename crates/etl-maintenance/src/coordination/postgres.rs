use async_trait::async_trait;
use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use sqlx::{Executor, PgPool, Row, migrate::Migrator, types::Json};

use super::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenancePause, ExternalMaintenancePausePolicy,
    ExternalMaintenanceReplicatorStatus, ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun,
    ExternalMaintenanceState, ExternalMaintenanceStore,
};

const CREATE_MIGRATION_SCHEMA_SQL: &str = "create schema if not exists etl;";
const SET_MIGRATION_SEARCH_PATH_SQL: &str = "set search_path = etl, public;";

fn sqlx_error(error: sqlx::Error, message: &'static str) -> EtlError {
    etl_error!(ErrorKind::SourceQueryFailed, message, source: error)
}

fn migration_error(error: sqlx::migrate::MigrateError, message: &'static str) -> EtlError {
    etl_error!(ErrorKind::SourceQueryFailed, message, source: error)
}

fn postgres_migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations/postgres");
    migrator.set_ignore_missing(true);
    migrator
}

/// Persisted Postgres maintenance policy document.
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PostgresExternalMaintenancePolicy {
    /// Pause policy.
    #[serde(default)]
    pause: ExternalMaintenancePausePolicy,
    /// Operation policy.
    #[serde(default)]
    operations: ExternalMaintenanceOperationPolicy,
}

impl PostgresExternalMaintenancePolicy {
    /// Builds a policy document from backend-neutral policy sections.
    fn new(
        pause: ExternalMaintenancePausePolicy,
        operations: ExternalMaintenanceOperationPolicy,
    ) -> Self {
        Self { pause, operations }
    }
}

/// Decodes current nested policy documents and legacy flat operation policies.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum StoredPostgresExternalMaintenancePolicy {
    /// Current nested policy document.
    Current(PostgresExternalMaintenancePolicy),
    /// Legacy operation-only policy document.
    Legacy(ExternalMaintenanceOperationPolicy),
}

impl StoredPostgresExternalMaintenancePolicy {
    /// Returns backend-neutral policy sections.
    fn into_parts(self) -> (ExternalMaintenancePausePolicy, ExternalMaintenanceOperationPolicy) {
        match self {
            Self::Current(policy) => (policy.pause, policy.operations),
            Self::Legacy(operations) => (ExternalMaintenancePausePolicy::default(), operations),
        }
    }
}

/// Postgres-backed external maintenance coordination store.
#[derive(Clone)]
pub struct PostgresExternalMaintenanceStore {
    pipeline_id: i64,
    pool: PgPool,
}

impl PostgresExternalMaintenanceStore {
    /// Creates a Postgres-backed maintenance store.
    pub fn new(pipeline_id: i64, pool: PgPool) -> Self {
        Self { pipeline_id, pool }
    }

    /// Runs the Postgres maintenance migrations.
    pub async fn ensure_schema(&self) -> EtlResult<()> {
        let mut connection = self.pool.acquire().await.map_err(|error| {
            sqlx_error(error, "Failed to acquire external maintenance migration connection")
        })?;
        connection.execute(CREATE_MIGRATION_SCHEMA_SQL).await.map_err(|error| {
            sqlx_error(error, "Failed to create external maintenance migration schema")
        })?;
        connection.execute(SET_MIGRATION_SEARCH_PATH_SQL).await.map_err(|error| {
            sqlx_error(error, "Failed to configure external maintenance migration search path")
        })?;

        postgres_migrator().run_direct(None, &mut *connection, false).await.map_err(|error| {
            migration_error(error, "Failed to run external maintenance migrations")
        })?;

        Ok(())
    }

    /// Ensures state exists for one pipeline.
    pub async fn ensure_pipeline_state(
        &self,
        pause_policy: ExternalMaintenancePausePolicy,
        operation_policy: ExternalMaintenanceOperationPolicy,
    ) -> EtlResult<()> {
        let policy = Json(PostgresExternalMaintenancePolicy::new(pause_policy, operation_policy));
        sqlx::query(
            r#"
            insert into etl.external_maintenance_state (pipeline_id, operation_policy)
            values ($1, $2)
            on conflict (pipeline_id)
            do update set operation_policy = excluded.operation_policy, updated_at = now()
            "#,
        )
        .bind(self.pipeline_id)
        .bind(policy)
        .execute(&self.pool)
        .await
        .map_err(|error| sqlx_error(error, "Failed to upsert external maintenance state"))?;

        Ok(())
    }

    /// Ensures state exists for one pipeline without replacing an existing
    /// maintenance policy.
    pub async fn ensure_pipeline_state_if_missing(
        &self,
        pause_policy: ExternalMaintenancePausePolicy,
        operation_policy: ExternalMaintenanceOperationPolicy,
    ) -> EtlResult<()> {
        let policy = Json(PostgresExternalMaintenancePolicy::new(pause_policy, operation_policy));
        sqlx::query(
            r#"
            insert into etl.external_maintenance_state (pipeline_id, operation_policy)
            values ($1, $2)
            on conflict (pipeline_id) do nothing
            "#,
        )
        .bind(self.pipeline_id)
        .bind(policy)
        .execute(&self.pool)
        .await
        .map_err(|error| sqlx_error(error, "Failed to insert external maintenance state"))?;

        Ok(())
    }

    /// Deletes state for one pipeline.
    pub async fn delete_pipeline_state(&self) -> EtlResult<()> {
        sqlx::query("delete from etl.external_maintenance_state where pipeline_id = $1")
            .bind(self.pipeline_id)
            .execute(&self.pool)
            .await
            .map_err(|error| sqlx_error(error, "Failed to delete external maintenance state"))?;

        Ok(())
    }
}

#[async_trait]
impl ExternalMaintenanceStore for PostgresExternalMaintenanceStore {
    async fn load_state(&self) -> EtlResult<ExternalMaintenanceState> {
        let Some(row) = sqlx::query(
            r#"
            select
                active_run,
                pause_request,
                operation_request,
                replicator,
                last_successful_operations,
                last_completed_at,
                operation_policy
            from etl.external_maintenance_state
            where pipeline_id = $1
            "#,
        )
        .bind(self.pipeline_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| sqlx_error(error, "Failed to load external maintenance state"))?
        else {
            return Ok(ExternalMaintenanceState::default());
        };
        let (pause_policy, operation_policy) = row
            .try_get::<Json<StoredPostgresExternalMaintenancePolicy>, _>("operation_policy")
            .map_err(|error| sqlx_error(error, "Failed to decode external maintenance policy"))?
            .0
            .into_parts();

        Ok(ExternalMaintenanceState {
            exists: true,
            active_run: row
                .try_get::<Option<Json<ExternalMaintenanceRun>>, _>("active_run")
                .map_err(|error| sqlx_error(error, "Failed to decode external maintenance run"))?
                .map(|value| value.0),
            pause_request: row
                .try_get::<Option<Json<ExternalMaintenancePause>>, _>("pause_request")
                .map_err(|error| sqlx_error(error, "Failed to decode external maintenance pause"))?
                .map(|value| value.0),
            operation_request: row
                .try_get::<Option<Json<ExternalMaintenanceOperationRequest>>, _>(
                    "operation_request",
                )
                .map_err(|error| {
                    sqlx_error(error, "Failed to decode external maintenance operation request")
                })?
                .map(|value| value.0),
            replicator: row
                .try_get::<Option<Json<ExternalMaintenanceReplicatorStatus>>, _>("replicator")
                .map_err(|error| {
                    sqlx_error(error, "Failed to decode external maintenance replicator status")
                })?
                .map(|value| value.0),
            last_successful_operations: row
                .try_get::<Json<ExternalMaintenanceOperationHistory>, _>(
                    "last_successful_operations",
                )
                .map_err(|error| {
                    sqlx_error(error, "Failed to decode external maintenance operation history")
                })?
                .0,
            last_completed_at: row.try_get("last_completed_at").map_err(|error| {
                sqlx_error(error, "Failed to decode external maintenance completed timestamp")
            })?,
            pause_policy,
            operation_policy,
        })
    }

    async fn request_operations(
        &self,
        request: ExternalMaintenanceOperationRequest,
    ) -> EtlResult<ExternalMaintenanceRequestOutcome> {
        let mut tx =
            self.pool.begin().await.map_err(|error| {
                sqlx_error(error, "Failed to begin external maintenance request")
            })?;

        let Some(row) = sqlx::query(
            r#"
            select active_run, operation_request
            from etl.external_maintenance_state
            where pipeline_id = $1
            for update
            "#,
        )
        .bind(self.pipeline_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| sqlx_error(error, "Failed to lock external maintenance state"))?
        else {
            tx.rollback()
                .await
                .map_err(|error| sqlx_error(error, "Failed to roll back missing state request"))?;
            return Ok(ExternalMaintenanceRequestOutcome::MissingState);
        };

        let active_run = row
            .try_get::<Option<Json<ExternalMaintenanceRun>>, _>("active_run")
            .map_err(|error| sqlx_error(error, "Failed to decode active maintenance run"))?;
        if active_run.is_some() {
            tx.rollback()
                .await
                .map_err(|error| sqlx_error(error, "Failed to roll back active run request"))?;
            return Ok(ExternalMaintenanceRequestOutcome::RejectedActiveRun);
        }

        let existing_request = row
            .try_get::<Option<Json<ExternalMaintenanceOperationRequest>>, _>("operation_request")
            .map_err(|error| sqlx_error(error, "Failed to decode operation request"))?
            .map(|value| value.0);
        if existing_request
            .as_ref()
            .is_some_and(|existing| existing.operations.covers(request.operations))
        {
            tx.rollback()
                .await
                .map_err(|error| sqlx_error(error, "Failed to roll back covered request"))?;
            return Ok(ExternalMaintenanceRequestOutcome::AlreadyCovered);
        }

        let merged_request = if let Some(mut existing_request) = existing_request {
            existing_request.operations = existing_request.operations.merge(request.operations);
            existing_request.inline_flush_min_inlined_bytes = request
                .inline_flush_min_inlined_bytes
                .or(existing_request.inline_flush_min_inlined_bytes);
            existing_request.rewrite_data_files_min_active_data_files = request
                .rewrite_data_files_min_active_data_files
                .or(existing_request.rewrite_data_files_min_active_data_files);
            existing_request.requested_at = request.requested_at;
            existing_request
        } else {
            request
        };

        sqlx::query(
            r#"
            update etl.external_maintenance_state
            set operation_request = $2, updated_at = now()
            where pipeline_id = $1
            "#,
        )
        .bind(self.pipeline_id)
        .bind(Json(merged_request))
        .execute(&mut *tx)
        .await
        .map_err(|error| sqlx_error(error, "Failed to update external maintenance request"))?;

        tx.commit()
            .await
            .map_err(|error| sqlx_error(error, "Failed to commit external maintenance request"))?;

        Ok(ExternalMaintenanceRequestOutcome::Created)
    }

    async fn report_replicator_status(
        &self,
        status: ExternalMaintenanceReplicatorStatus,
    ) -> EtlResult<()> {
        sqlx::query(
            r#"
            update etl.external_maintenance_state
            set replicator = $2, updated_at = now()
            where pipeline_id = $1
            "#,
        )
        .bind(self.pipeline_id)
        .bind(Json(status))
        .execute(&self.pool)
        .await
        .map_err(|error| sqlx_error(error, "Failed to report external maintenance status"))?;

        Ok(())
    }

    async fn clear_replicator_status(&self) -> EtlResult<()> {
        sqlx::query(
            r#"
            update etl.external_maintenance_state
            set replicator = null, updated_at = now()
            where pipeline_id = $1
            "#,
        )
        .bind(self.pipeline_id)
        .execute(&self.pool)
        .await
        .map_err(|error| sqlx_error(error, "Failed to clear external maintenance status"))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_policy_decodes_current_pause_and_operation_policy() {
        let policy = serde_json::json!({
            "pause": {
                "maxDurationSeconds": 900,
            },
            "operations": {
                "inlineFlushEnabled": false,
                "mergeAdjacentFilesEnabled": true,
                "rewriteDataFilesEnabled": false,
                "expireSnapshotsEnabled": true,
                "cleanupOldFilesEnabled": false,
            }
        });

        let (pause, operations) =
            serde_json::from_value::<StoredPostgresExternalMaintenancePolicy>(policy)
                .expect("current policy should decode")
                .into_parts();

        assert_eq!(pause.max_duration_seconds, 900);
        assert_eq!(
            operations,
            ExternalMaintenanceOperationPolicy {
                inline_flush_enabled: false,
                merge_adjacent_files_enabled: true,
                rewrite_data_files_enabled: false,
                expire_snapshots_enabled: true,
                cleanup_old_files_enabled: false,
            }
        );
    }

    #[test]
    fn postgres_policy_decodes_legacy_operation_policy() {
        let policy = serde_json::json!({
            "inlineFlushEnabled": false,
            "mergeAdjacentFilesEnabled": true,
            "rewriteDataFilesEnabled": false,
            "expireSnapshotsEnabled": true,
            "cleanupOldFilesEnabled": false,
        });

        let (pause, operations) =
            serde_json::from_value::<StoredPostgresExternalMaintenancePolicy>(policy)
                .expect("legacy policy should decode")
                .into_parts();

        assert_eq!(pause, ExternalMaintenancePausePolicy::default());
        assert_eq!(
            operations,
            ExternalMaintenanceOperationPolicy {
                inline_flush_enabled: false,
                merge_adjacent_files_enabled: true,
                rewrite_data_files_enabled: false,
                expire_snapshots_enabled: true,
                cleanup_old_files_enabled: false,
            }
        );
    }
}
