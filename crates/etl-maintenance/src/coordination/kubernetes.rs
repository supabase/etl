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
use crate::coordination::report::MaintenanceRunnerReport;

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

    /// Reads an already committed report so a retried Job does not repeat work.
    pub async fn completed_report(
        &self,
        pipeline_id: &str,
        project_ref: &str,
        run_id: &str,
    ) -> EtlResult<Option<MaintenanceRunnerReport>> {
        let resource = time::timeout(self.timeout, self.api.get(&self.name))
            .await
            .map_err(|_| {
                etl_error!(ErrorKind::SourceQueryFailed, "Timed out loading maintenance report")
            })?
            .map_err(|error| Self::kube_error(error, "Failed to load maintenance report"))?;
        validate_runner_scope(&resource, pipeline_id, project_ref, run_id)?;
        Ok(resource
            .data
            .get("status")
            .and_then(|status| status.get("runnerReport"))
            .cloned()
            .and_then(|value| serde_json::from_value::<MaintenanceRunnerReport>(value).ok())
            .filter(|report| report.is_valid() && report.run_id == run_id))
    }

    /// Persists one successful attempt, fenced by active run and resource
    /// version.
    ///
    /// Retries only report delivery. Maintenance work must not be repeated when
    /// the API temporarily rejects or times out a status write.
    pub async fn report_result(
        &self,
        pipeline_id: &str,
        project_ref: &str,
        report: &MaintenanceRunnerReport,
    ) -> EtlResult<()> {
        if !report.is_valid() {
            return Err(etl_error!(ErrorKind::InvalidState, "Invalid maintenance runner report"));
        }
        for attempt in 0..8 {
            let operation = async {
                let resource = self.api.get(&self.name).await?;
                Ok::<_, kube::Error>(resource)
            };
            let resource = match time::timeout(self.timeout, operation).await {
                Ok(Ok(resource)) => resource,
                Ok(Err(error)) if !retryable_report_error(&error) => {
                    return Err(Self::kube_error(
                        error,
                        "Failed to load maintenance report target",
                    ));
                }
                _ => {
                    time::sleep(Duration::from_millis(250 * (attempt + 1))).await;
                    continue;
                }
            };
            validate_runner_scope(&resource, pipeline_id, project_ref, &report.run_id)?;
            let patch = json!({
                "metadata": { "resourceVersion": resource.metadata.resource_version },
                "status": { "runnerReport": report }
            });
            match time::timeout(
                self.timeout,
                self.api.patch_status(&self.name, &PatchParams::default(), &Patch::Merge(&patch)),
            )
            .await
            {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(error)) if !retryable_report_error(&error) => {
                    return Err(Self::kube_error(error, "Failed to persist maintenance report"));
                }
                _ => time::sleep(Duration::from_millis(250 * (attempt + 1))).await,
            }
        }
        Err(etl_error!(
            ErrorKind::SourceQueryFailed,
            "Maintenance report delivery retries exhausted"
        ))
    }
}

/// Limits delivery retries to conflicts, throttling, and transient API errors.
fn retryable_report_error(error: &kube::Error) -> bool {
    match error {
        kube::Error::Api(error) => error.code == 409 || error.code == 429 || error.code >= 500,
        _ => true,
    }
}

/// Rejects wrong-project, wrong-pipeline, stale-run, and deleting resources.
fn validate_runner_scope(
    resource: &DynamicObject,
    pipeline_id: &str,
    project_ref: &str,
    run_id: &str,
) -> EtlResult<()> {
    let pipeline = &resource.data["spec"]["pipelineRef"];
    let observed_pipeline = pipeline["pipelineId"]
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| pipeline["pipelineId"].as_i64().map(|id| id.to_string()));
    if project_ref.is_empty()
        || pipeline_id.is_empty()
        || run_id.is_empty()
        || resource.metadata.resource_version.is_none()
        || resource.metadata.deletion_timestamp.is_some()
        || pipeline["tenantId"].as_str() != Some(project_ref)
        || observed_pipeline.as_deref() != Some(pipeline_id)
        || resource.data["status"]["activeRun"]["runId"].as_str() != Some(run_id)
    {
        return Err(etl_error!(
            ErrorKind::InvalidState,
            "Maintenance report attribution or active run mismatch"
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
impl ExternalMaintenanceStore for KubernetesExternalMaintenanceStore {
    async fn load_state(&self) -> EtlResult<ExternalMaintenanceState> {
        match time::timeout(self.timeout, self.api.get(&self.name)).await {
            Ok(Ok(resource)) => Ok(state_from_ducklake_maintenance_cr(&resource)),
            Ok(Err(kube::Error::Api(error))) if error.code == 404 => {
                Ok(ExternalMaintenanceState::default())
            }
            Ok(Err(error)) => {
                Err(Self::kube_error(error, "Failed to load DuckLake maintenance CR"))
            }
            Err(_) => Err(etl_error!(
                ErrorKind::SourceQueryFailed,
                "Timed out loading DuckLake maintenance CR"
            )),
        }
    }

    async fn request_operations(
        &self,
        request: ExternalMaintenanceOperationRequest,
    ) -> EtlResult<ExternalMaintenanceRequestOutcome> {
        let state = self.load_state().await?;
        if !state.exists {
            return Ok(ExternalMaintenanceRequestOutcome::MissingState);
        }
        if state.active_run.is_some() {
            return Ok(ExternalMaintenanceRequestOutcome::RejectedActiveRun);
        }
        if state
            .operation_request
            .as_ref()
            .is_some_and(|existing| existing.operations.covers(request.operations))
        {
            return Ok(ExternalMaintenanceRequestOutcome::AlreadyCovered);
        }

        let patch = json!({
            "status": {
                "operationRequests": {
                    "inlineFlush": request.operations.inline_flush,
                    "mergeAdjacentFiles": request.operations.merge_adjacent_files,
                    "rewriteDataFiles": request.operations.rewrite_data_files,
                    "expireSnapshots": request.operations.expire_snapshots,
                    "cleanupOldFiles": request.operations.cleanup_old_files,
                    "inlineFlushMinInlinedBytes": request.inline_flush_min_inlined_bytes,
                    "rewriteDataFilesMinActiveDataFiles":
                        request.rewrite_data_files_min_active_data_files,
                    "requestedAt": request.requested_at.to_rfc3339(),
                }
            }
        });
        patch_status(&self.api, &self.name, &patch, self.timeout).await?;
        Ok(ExternalMaintenanceRequestOutcome::Created)
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
    use serde_json::json;

    use super::*;

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

#[cfg(test)]
mod reporting_tests {
    use std::{
        convert::Infallible,
        sync::{Arc, Mutex},
    };

    use http::{Request, Response};
    use kube::client::Body;
    use tower::service_fn;

    use super::*;
    use crate::coordination::report::{InlineFlushSummaries, MaintenanceResults};

    fn target() -> DynamicObject {
        serde_json::from_value(json!({
            "apiVersion": "etl.supabase.com/v1alpha1", "kind": "DuckLakeMaintenance",
            "metadata": {"name": "synthetic", "uid": "uid-1", "resourceVersion": "42"},
            "spec": {"pipelineRef": {"pipelineId": 7, "tenantId": "synthetic-project"}},
            "status": {"activeRun": {"runId": "run-1"}}
        }))
        .unwrap()
    }

    #[test]
    fn runner_attribution_and_active_run_are_required() {
        let resource = target();
        assert!(validate_runner_scope(&resource, "7", "synthetic-project", "run-1").is_ok());
        for (pipeline, project, run) in [
            ("8", "synthetic-project", "run-1"),
            ("7", "other-project", "run-1"),
            ("7", "synthetic-project", "run-2"),
            ("7", "", "run-1"),
        ] {
            assert!(validate_runner_scope(&resource, pipeline, project, run).is_err());
        }
        for pipeline in ["00123", "pipeline-example"] {
            let mut resource = target();
            resource.data["spec"]["pipelineRef"]["pipelineId"] = json!(pipeline);
            assert!(
                validate_runner_scope(&resource, pipeline, "synthetic-project", "run-1").is_ok()
            );
        }
        let mut resource = target();
        resource.metadata.resource_version = None;
        assert!(validate_runner_scope(&resource, "7", "synthetic-project", "run-1").is_err());
        let mut resource = target();
        resource.data["status"]["activeRun"] = Value::Null;
        assert!(validate_runner_scope(&resource, "7", "synthetic-project", "run-1").is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn report_delivery_retries_conflicts_without_repeating_work() {
        let resource = Arc::new(Mutex::new(target()));
        let observed = Arc::clone(&resource);
        let patches = Arc::new(Mutex::new(Vec::<Value>::new()));
        let captured = Arc::clone(&patches);
        let client = Client::new(
            service_fn(move |request: Request<Body>| {
                let observed = Arc::clone(&observed);
                let captured = Arc::clone(&captured);
                async move {
                    let mut code = 200;
                    if request.method() == "PATCH" {
                        assert!(request.uri().path().ends_with("/synthetic/status"));
                        let body = request.into_body().collect_bytes().await.unwrap();
                        let patch: Value = serde_json::from_slice(&body).unwrap();
                        let mut patches = captured.lock().unwrap();
                        patches.push(patch.clone());
                        let mut resource = observed.lock().unwrap();
                        if patches.len() == 1 {
                            resource.metadata.resource_version = Some("43".to_owned());
                            code = 409;
                        } else {
                            assert_eq!(patch["metadata"]["resourceVersion"], "43");
                            assert_eq!(patch["status"].as_object().unwrap().len(), 1);
                            resource.data["status"]["runnerReport"] =
                                patch["status"]["runnerReport"].clone();
                        }
                    }
                    let response = if code == 409 {
                        json!({"apiVersion": "v1", "kind": "Status", "status": "Failure",
                        "message": "synthetic conflict", "reason": "Conflict", "code": 409})
                    } else {
                        serde_json::to_value(observed.lock().unwrap().clone()).unwrap()
                    };
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(code)
                            .body(Body::from(serde_json::to_vec(&response).unwrap()))
                            .unwrap(),
                    )
                }
            }),
            "synthetic",
        );
        let store = KubernetesExternalMaintenanceStore {
            api: Api::namespaced_with(client, "synthetic", &ducklake_maintenance_api_resource()),
            name: "synthetic".to_owned(),
            timeout: Duration::from_secs(1),
        };
        let report = MaintenanceRunnerReport {
            version: 1,
            run_id: "run-1".to_owned(),
            inline_flush: InlineFlushSummaries::default(),
            results: MaintenanceResults::default(),
        };
        store.report_result("7", "synthetic-project", &report).await.unwrap();
        assert_eq!(patches.lock().unwrap().len(), 2);
        assert_eq!(patches.lock().unwrap()[0]["metadata"]["resourceVersion"], "42");
        assert!(store.completed_report("7", "synthetic-project", "run-1").await.unwrap().is_some());
        assert!(store.completed_report("7", "other-project", "run-1").await.is_err());
    }
}
