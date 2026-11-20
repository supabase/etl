use std::time::Duration;

use chrono::{DateTime, Utc};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
    core::{ApiResource, DynamicObject, GroupVersionKind},
};
use serde_json::{Value, json};
use tokio::time;
use tracing::info;

use super::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenanceOperationRun,
    ExternalMaintenanceOperations, ExternalMaintenancePause, ExternalMaintenancePausePolicy,
    ExternalMaintenanceReplicatorStatus, ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun,
    ExternalMaintenanceState, ExternalMaintenanceStore,
};

const CR_NAME_ENV: &str = "ETL_DUCKLAKE_MAINTENANCE_CR_NAME";
const CR_NAMESPACE_ENV: &str = "ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE";
const KUBERNETES_API_TIMEOUT_SECONDS_ENV: &str =
    "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_KUBERNETES_API_TIMEOUT_SECONDS";

/// Kubernetes CR-backed external maintenance store.
#[derive(Clone)]
pub struct KubernetesExternalMaintenanceStore {
    api: Api<DynamicObject>,
    name: String,
    timeout: Duration,
}

impl KubernetesExternalMaintenanceStore {
    /// Creates a Kubernetes store from environment variables.
    pub async fn from_env(default_timeout: Duration) -> EtlResult<Option<Self>> {
        let Some(name) = std::env::var(CR_NAME_ENV).ok().filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let Some(namespace) =
            std::env::var(CR_NAMESPACE_ENV).ok().filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };
        let timeout = std::env::var(KUBERNETES_API_TIMEOUT_SECONDS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .map_or(default_timeout, Duration::from_secs);

        let client = Client::try_default().await.map_err(|error| {
            etl_error!(
                ErrorKind::ConfigError,
                "Failed to build Kubernetes client for external maintenance",
                source: error
            )
        })?;
        let api: Api<DynamicObject> =
            Api::namespaced_with(client, &namespace, &ducklake_maintenance_api_resource());

        info!(
            ducklake_maintenance = %name,
            namespace,
            "ducklake kubernetes external maintenance store configured"
        );

        Ok(Some(Self { api, name, timeout }))
    }

    fn kube_error(error: kube::Error, message: &'static str) -> etl::error::EtlError {
        etl_error!(ErrorKind::SourceQueryFailed, message, source: error)
    }

    async fn load_resource(&self) -> EtlResult<Option<DynamicObject>> {
        time::timeout(self.timeout, self.api.get_opt(&self.name))
            .await
            .map_err(|_| {
                etl_error!(
                    ErrorKind::SourceQueryFailed,
                    "Timed out loading DuckLake maintenance CR"
                )
            })?
            .map_err(|error| Self::kube_error(error, "Failed to load DuckLake maintenance CR"))
    }
}

/// Merges newly requested operations without changing thresholds for pending
/// work.
fn operation_request_patch(
    version: &str,
    pending: ExternalMaintenanceOperations,
    request: &ExternalMaintenanceOperationRequest,
) -> Value {
    let mut operations = json!(pending.merge(request.operations));
    operations["requestedAt"] = json!(request.requested_at);
    if request.operations.inline_flush && !pending.inline_flush {
        operations["inlineFlushMinInlinedBytes"] = json!(request.inline_flush_min_inlined_bytes);
    }
    if request.operations.rewrite_data_files && !pending.rewrite_data_files {
        operations["rewriteDataFilesMinActiveDataFiles"] =
            json!(request.rewrite_data_files_min_active_data_files);
    }

    json!({"metadata": {"resourceVersion": version}, "status": {"operationRequests": operations}})
}

#[async_trait::async_trait]
impl ExternalMaintenanceStore for KubernetesExternalMaintenanceStore {
    async fn load_state(&self) -> EtlResult<ExternalMaintenanceState> {
        Ok(self
            .load_resource()
            .await?
            .as_ref()
            .map(state_from_ducklake_maintenance_cr)
            .unwrap_or_default())
    }

    async fn request_operations(
        &self,
        request: ExternalMaintenanceOperationRequest,
    ) -> EtlResult<ExternalMaintenanceRequestOutcome> {
        let Some(resource) = self.load_resource().await? else {
            return Ok(ExternalMaintenanceRequestOutcome::MissingState);
        };
        if !resource.data["status"]["activeRun"].is_null() {
            return Ok(ExternalMaintenanceRequestOutcome::RejectedActiveRun);
        }

        let pending = operations_from_value(&resource.data["status"]["operationRequests"]);
        if pending.covers(request.operations) {
            return Ok(ExternalMaintenanceRequestOutcome::AlreadyCovered);
        }

        let Some(version) = resource.metadata.resource_version.as_deref().filter(|v| !v.is_empty())
        else {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "DuckLake maintenance CR is missing its resource version"
            ));
        };

        let patch = operation_request_patch(version, pending, &request);
        let params = PatchParams::default();
        let patch = Patch::Merge(&patch);
        match time::timeout(self.timeout, self.api.patch_status(&self.name, &params, &patch)).await
        {
            Ok(Ok(_)) => Ok(ExternalMaintenanceRequestOutcome::Created),
            Ok(Err(kube::Error::Api(error))) if error.code == 409 => {
                Ok(ExternalMaintenanceRequestOutcome::StateChanged)
            }
            Ok(Err(kube::Error::Api(error))) if error.code == 404 => {
                Ok(ExternalMaintenanceRequestOutcome::MissingState)
            }
            Ok(Err(error)) => {
                Err(Self::kube_error(error, "Failed to request DuckLake maintenance operations"))
            }
            Err(_) => Err(etl_error!(
                ErrorKind::SourceQueryFailed,
                "Timed out requesting DuckLake maintenance operations"
            )),
        }
    }

    async fn report_replicator_status(
        &self,
        status: ExternalMaintenanceReplicatorStatus,
    ) -> EtlResult<()> {
        let patch = json!({
            "status": {
                "replicator": {
                    "state": status.state_name(),
                    "observedRunId": status.observed_run_id,
                    "quiescedAt": status.quiesced_at.map(|time| time.to_rfc3339()),
                }
            }
        });
        patch_status(&self.api, &self.name, &patch, self.timeout).await
    }

    async fn clear_replicator_status(&self) -> EtlResult<()> {
        let patch = json!({
            "status": {
                "replicator": {
                    "state": "Running",
                    "observedRunId": null,
                    "quiescedAt": null,
                }
            }
        });
        patch_status(&self.api, &self.name, &patch, self.timeout).await
    }
}

async fn patch_status(
    api: &Api<DynamicObject>,
    name: &str,
    patch: &Value,
    timeout: Duration,
) -> EtlResult<()> {
    let params = PatchParams::default();
    match time::timeout(timeout, api.patch_status(name, &params, &Patch::Merge(patch))).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(KubernetesExternalMaintenanceStore::kube_error(
            error,
            "Failed to patch DuckLake maintenance CR status",
        )),
        Err(_) => Err(etl_error!(
            ErrorKind::SourceQueryFailed,
            "Timed out patching DuckLake maintenance CR status"
        )),
    }
}

fn state_from_ducklake_maintenance_cr(resource: &DynamicObject) -> ExternalMaintenanceState {
    let mut state = ExternalMaintenanceState::present();
    state.pause_request = active_pause_request(resource);
    state.active_run = active_run(resource);
    state.operation_request = operation_request(resource);
    state.replicator = replicator_status(resource);
    state.last_successful_operations = operation_history(resource);
    state.last_completed_at = last_completed_at(resource);
    state.pause_policy = pause_policy(resource);
    state.operation_policy = operation_policy(resource);
    state
}

fn active_pause_request(resource: &DynamicObject) -> Option<ExternalMaintenancePause> {
    let pause = resource.data.get("status")?.get("pauseRequest")?;
    if pause.is_null() {
        return None;
    }

    Some(ExternalMaintenancePause {
        run_id: pause.get("runId")?.as_str()?.to_owned(),
        requested_at: parse_rfc3339_value(pause.get("requestedAt")),
        expires_at: parse_rfc3339_value(pause.get("expiresAt"))?,
    })
}

fn active_run(resource: &DynamicObject) -> Option<ExternalMaintenanceRun> {
    let active_run = resource.data.get("status")?.get("activeRun")?;
    if active_run.is_null() {
        return None;
    }

    Some(ExternalMaintenanceRun {
        run_id: active_run.get("runId")?.as_str()?.to_owned(),
        started_at: parse_rfc3339_value(active_run.get("startedAt")),
        operations: active_run
            .get("operations")
            .map_or_else(|| operations_from_value(active_run), operations_from_value),
    })
}

fn operation_request(resource: &DynamicObject) -> Option<ExternalMaintenanceOperationRequest> {
    let requests = resource.data.get("status")?.get("operationRequests")?;
    if requests.is_null() {
        return None;
    }

    Some(ExternalMaintenanceOperationRequest {
        operations: operations_from_value(requests),
        inline_flush_min_inlined_bytes: requests
            .get("inlineFlushMinInlinedBytes")
            .and_then(Value::as_u64),
        rewrite_data_files_min_active_data_files: requests
            .get("rewriteDataFilesMinActiveDataFiles")
            .and_then(Value::as_i64),
        requested_at: parse_rfc3339_value(requests.get("requestedAt"))?,
    })
}

fn operations_from_value(value: &Value) -> ExternalMaintenanceOperations {
    ExternalMaintenanceOperations {
        inline_flush: bool_value(value, "inlineFlush", false),
        merge_adjacent_files: bool_value(value, "mergeAdjacentFiles", false),
        rewrite_data_files: bool_value(value, "rewriteDataFiles", false),
        expire_snapshots: bool_value(value, "expireSnapshots", false),
        cleanup_old_files: bool_value(value, "cleanupOldFiles", false),
    }
}

fn replicator_status(resource: &DynamicObject) -> Option<ExternalMaintenanceReplicatorStatus> {
    let replicator = resource.data.get("status")?.get("replicator")?;
    if replicator.is_null() {
        return None;
    }

    Some(ExternalMaintenanceReplicatorStatus::from_state_name(
        replicator.get("state")?.as_str()?,
        replicator.get("observedRunId").and_then(Value::as_str).map(ToOwned::to_owned),
        parse_rfc3339_value(replicator.get("quiescedAt")),
    ))
}

fn operation_history(resource: &DynamicObject) -> ExternalMaintenanceOperationHistory {
    let Some(history) =
        resource.data.get("status").and_then(|status| status.get("lastSuccessfulOperationRuns"))
    else {
        return ExternalMaintenanceOperationHistory::default();
    };

    ExternalMaintenanceOperationHistory {
        inline_flush: operation_run(history.get("inlineFlush")),
        merge_adjacent_files: operation_run(history.get("mergeAdjacentFiles")),
        rewrite_data_files: operation_run(history.get("rewriteDataFiles")),
        expire_snapshots: operation_run(history.get("expireSnapshots")),
        cleanup_old_files: operation_run(history.get("cleanupOldFiles")),
    }
}

fn operation_run(value: Option<&Value>) -> Option<ExternalMaintenanceOperationRun> {
    let value = value?;
    if value.is_null() {
        return None;
    }

    Some(ExternalMaintenanceOperationRun {
        run_id: value.get("runId").and_then(Value::as_str).map(ToOwned::to_owned),
        completed_at: parse_rfc3339_value(value.get("completedAt"))?,
    })
}

fn last_completed_at(resource: &DynamicObject) -> Option<DateTime<Utc>> {
    parse_rfc3339_value(
        resource
            .data
            .get("status")
            .and_then(|status| status.get("lastCompletedRun"))
            .and_then(|run| run.get("completedAt")),
    )
}

fn operation_policy(resource: &DynamicObject) -> ExternalMaintenanceOperationPolicy {
    let operations = resource.data.get("spec").and_then(|spec| spec.get("operations"));
    let inline_flush = operations.and_then(|ops| ops.get("inlineFlush"));
    let merge_adjacent_files = operations.and_then(|ops| ops.get("mergeAdjacentFiles"));
    let rewrite_data_files = operations.and_then(|ops| ops.get("rewriteDataFiles"));
    let expire_snapshots = operations.and_then(|ops| ops.get("expireSnapshots"));
    let cleanup_old_files = operations.and_then(|ops| ops.get("cleanupOldFiles"));

    ExternalMaintenanceOperationPolicy {
        inline_flush_enabled: enabled_value(inline_flush, true),
        merge_adjacent_files_enabled: enabled_value(merge_adjacent_files, true),
        rewrite_data_files_enabled: enabled_value(rewrite_data_files, true),
        expire_snapshots_enabled: enabled_value(expire_snapshots, true),
        cleanup_old_files_enabled: enabled_value(cleanup_old_files, true),
    }
}

/// Reads the trusted pause policy from the DuckLake maintenance spec.
fn pause_policy(resource: &DynamicObject) -> ExternalMaintenancePausePolicy {
    let max_duration_seconds = resource
        .data
        .get("spec")
        .and_then(|spec| spec.get("pause"))
        .and_then(|pause| pause.get("maxDurationSeconds"))
        .and_then(Value::as_u64)
        .filter(|seconds| *seconds > 0)
        .unwrap_or_else(|| ExternalMaintenancePausePolicy::default().max_duration_seconds);

    ExternalMaintenancePausePolicy { max_duration_seconds }
}

fn enabled_value(value: Option<&Value>, default: bool) -> bool {
    value.and_then(|value| value.get("enabled")).and_then(Value::as_bool).unwrap_or(default)
}

fn bool_value(value: &Value, key: &str, default: bool) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn parse_rfc3339_value(value: Option<&Value>) -> Option<DateTime<Utc>> {
    value
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|time| time.with_timezone(&Utc))
}

fn ducklake_maintenance_api_resource() -> ApiResource {
    let gvk = GroupVersionKind::gvk("etl.supabase.com", "v1alpha1", "DuckLakeMaintenance");
    ApiResource::from_gvk(&gvk)
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, Router,
        http::StatusCode,
        routing::{get, patch},
    };
    use serde_json::json;

    use super::*;

    const RESOURCE: &str =
        "/apis/etl.supabase.com/v1alpha1/namespaces/default/ducklakemaintenances/test";

    fn test_store(service: Router) -> KubernetesExternalMaintenanceStore {
        let api = ducklake_maintenance_api_resource();

        KubernetesExternalMaintenanceStore {
            api: Api::namespaced_with(Client::new(service, "default"), "default", &api),
            name: "test".into(),
            timeout: Duration::from_secs(5),
        }
    }

    fn flush_request() -> ExternalMaintenanceOperationRequest {
        ExternalMaintenanceOperationRequest {
            operations: ExternalMaintenanceOperations { inline_flush: true, ..Default::default() },
            inline_flush_min_inlined_bytes: Some(128),
            rewrite_data_files_min_active_data_files: Some(32),
            requested_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn concurrent_admission_defers_the_merged_request_until_the_next_poll() {
        let service = Router::new()
            .route(
                RESOURCE,
                get(|| async {
                    Json(json!({
                        "apiVersion": "etl.supabase.com/v1alpha1",
                        "kind": "DuckLakeMaintenance",
                        "metadata": {"name": "test", "resourceVersion": "41"},
                        "status": {"operationRequests": {
                            "rewriteDataFiles": true, "rewriteDataFilesMinActiveDataFiles": 9
                        }}
                    }))
                }),
            )
            .route(
                &format!("{RESOURCE}/status"),
                patch(|Json(body): Json<Value>| async move {
                    assert_eq!(body["metadata"]["resourceVersion"], "41");

                    let operations = &body["status"]["operationRequests"];
                    assert_eq!(operations["inlineFlush"], true);
                    assert_eq!(operations["inlineFlushMinInlinedBytes"], 128);
                    assert_eq!(operations["rewriteDataFiles"], true);
                    assert!(operations.get("rewriteDataFilesMinActiveDataFiles").is_none());

                    // Admission won the race with this write.
                    (
                        StatusCode::CONFLICT,
                        Json(json!({
                            "kind": "Status", "apiVersion": "v1", "status": "Failure",
                            "message": "resource version changed", "reason": "Conflict", "code": 409
                        })),
                    )
                }),
            );

        let outcome = test_store(service).request_operations(flush_request()).await.unwrap();

        assert_eq!(outcome, ExternalMaintenanceRequestOutcome::StateChanged);
    }

    #[tokio::test]
    async fn retry_after_an_unconfirmed_write_respects_the_current_state() {
        use ExternalMaintenanceRequestOutcome::{AlreadyCovered, RejectedActiveRun};

        for (status, expected) in [
            (json!({"operationRequests": {"inlineFlush": true}}), AlreadyCovered),
            (json!({"activeRun": {"runId": "run-1"}}), RejectedActiveRun),
        ] {
            // The previous PATCH lost its response. Only a GET is allowed on
            // retry.
            let service = Router::new().route(
                RESOURCE,
                get(move || async move {
                    Json(json!({
                        "metadata": {"name": "test", "resourceVersion": "42"},
                        "status": status,
                    }))
                }),
            );

            let outcome = test_store(service).request_operations(flush_request()).await.unwrap();

            assert_eq!(outcome, expected);
        }
    }

    #[test]
    fn operation_policy_defaults_expire_snapshots_enabled() {
        let resource: DynamicObject = serde_json::from_value(json!({
            "apiVersion": "etl.supabase.com/v1alpha1",
            "kind": "DuckLakeMaintenance",
            "metadata": {
                "name": "pipeline-maintenance"
            },
            "spec": {
                "operations": {
                    "inlineFlush": {},
                    "rewriteDataFiles": {}
                }
            }
        }))
        .unwrap();

        let policy = operation_policy(&resource);

        assert!(policy.inline_flush_enabled);
        assert!(policy.rewrite_data_files_enabled);
        assert!(policy.expire_snapshots_enabled);
    }

    #[test]
    fn pause_policy_reads_spec_max_duration_seconds() {
        let resource: DynamicObject = serde_json::from_value(json!({
            "apiVersion": "etl.supabase.com/v1alpha1",
            "kind": "DuckLakeMaintenance",
            "metadata": {
                "name": "pipeline-maintenance"
            },
            "spec": {
                "pause": {
                    "maxDurationSeconds": 900
                }
            }
        }))
        .unwrap();

        assert_eq!(pause_policy(&resource).max_duration_seconds, 900);
    }

    #[test]
    fn active_run_reads_nested_operations() {
        let resource: DynamicObject = serde_json::from_value(json!({
            "apiVersion": "etl.supabase.com/v1alpha1",
            "kind": "DuckLakeMaintenance",
            "metadata": {
                "name": "pipeline-maintenance"
            },
            "status": {
                "activeRun": {
                    "runId": "run-1",
                    "startedAt": "2026-05-12T12:00:00Z",
                    "operations": {
                        "inlineFlush": true,
                        "expireSnapshots": true
                    }
                }
            }
        }))
        .unwrap();

        let run = active_run(&resource).expect("expected active run");

        assert_eq!(run.run_id, "run-1");
        assert!(run.operations.inline_flush);
        assert!(run.operations.expire_snapshots);
        assert!(!run.operations.rewrite_data_files);
    }
}
