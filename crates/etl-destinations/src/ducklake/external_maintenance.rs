use std::{env, time::Duration};

use chrono::{DateTime, Utc};
use etl::store::{schema::SchemaStore, state::StateStore};
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
    core::{ApiResource, DynamicObject, GroupVersionKind},
};
use metrics::{counter, histogram};
use serde_json::json;
use tokio::time;
use tracing::{debug, info, warn};

use crate::ducklake::{
    DuckLakeDestination, DuckLakeExternalMaintenancePause,
    metrics::{
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_DURATION_SECONDS,
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_TRIGGERED_TOTAL, MAINTENANCE_OPERATION_LABEL,
        MAINTENANCE_OUTCOME_LABEL, MAINTENANCE_REASON_LABEL,
    },
};

const CR_NAME_ENV: &str = "ETL_DUCKLAKE_MAINTENANCE_CR_NAME";
const CR_NAMESPACE_ENV: &str = "ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE";
const POLL_SECONDS_ENV: &str = "ETL_DUCKLAKE_MAINTENANCE_POLL_SECONDS";
const INLINE_FLUSH_MIN_INLINED_BYTES_ENV: &str =
    "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_INLINE_FLUSH_MIN_INLINED_BYTES";
const REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES_ENV: &str =
    "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES";
const REQUEST_COOLDOWN_SECONDS_ENV: &str =
    "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_REQUEST_COOLDOWN_SECONDS";
const KUBERNETES_API_TIMEOUT_SECONDS_ENV: &str =
    "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_KUBERNETES_API_TIMEOUT_SECONDS";
const DEFAULT_POLL_SECONDS: u64 = 5;
const DEFAULT_INLINE_FLUSH_MIN_INLINED_BYTES: u64 = 10_000_000;
const DEFAULT_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES: i64 = 40;
const DEFAULT_REQUEST_COOLDOWN_SECONDS: u64 = 300;
const DEFAULT_KUBERNETES_API_TIMEOUT_SECONDS: u64 = 10;
const OPERATION_INLINE_FLUSH: &str = "flush_inlined_data";
const OPERATION_REWRITE_DATA_FILES: &str = "rewrite_data_files";
const REASON_PENDING_INLINED_DATA_BYTES_THRESHOLD: &str = "pending_inlined_data_bytes_threshold";
const REASON_ACTIVE_DATA_FILES_THRESHOLD: &str = "active_data_files_threshold";

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ExternalMaintenanceOperations {
    pub(super) inline_flush: bool,
    pub(super) rewrite_data_files: bool,
}

impl ExternalMaintenanceOperations {
    fn covers(self, requested: Self) -> bool {
        (!requested.inline_flush || self.inline_flush)
            && (!requested.rewrite_data_files || self.rewrite_data_files)
    }
}

struct PauseRequest {
    run_id: String,
    expires_at: DateTime<Utc>,
}

struct HeldPause {
    run_id: String,
    expires_at: DateTime<Utc>,
    quiesced_at: DateTime<Utc>,
    quiesced_reported: bool,
    _pause: DuckLakeExternalMaintenancePause,
}

#[derive(Clone)]
struct WatcherConfig {
    name: String,
    namespace: String,
    poll_interval: Duration,
    request_cooldown: Duration,
    kubernetes_api_timeout: Duration,
    inline_flush_min_inlined_bytes: u64,
    rewrite_data_files_min_active_data_files: i64,
}

struct OperationPolicy {
    inline_flush_enabled: bool,
    rewrite_data_files_enabled: bool,
}

pub(super) fn external_maintenance_configured() -> bool {
    WatcherConfig::from_env().is_some()
}

pub(super) async fn run_external_maintenance_watcher<S>(
    destination: DuckLakeDestination<S>,
) -> Result<(), kube::Error>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    let Some(config) = WatcherConfig::from_env() else {
        return Ok(());
    };
    let client = Client::try_default().await?;
    let api: Api<DynamicObject> =
        Api::namespaced_with(client, &config.namespace, &ducklake_maintenance_api_resource());
    let mut held_pause: Option<HeldPause> = None;

    info!(
        ducklake_maintenance = %config.name,
        namespace = %config.namespace,
        "ducklake external maintenance watcher started: ducklake_maintenance={}, namespace={}",
        config.name,
        config.namespace
    );

    loop {
        match time::timeout(config.kubernetes_api_timeout, api.get(&config.name)).await {
            Ok(resource) => {
                let resource = match resource {
                    Ok(resource) => resource,
                    Err(kube::Error::Api(error)) if error.code == 404 => {
                        if let Some(held) = held_pause.take() {
                            info!(
                                ducklake_maintenance = %config.name,
                                run_id = %held.run_id,
                                "ducklake maintenance resource disappeared, resuming foreground mutations: \
                                 ducklake_maintenance={}, run_id={}",
                                config.name,
                                held.run_id
                            );
                            release_held_pause(&config.name, held, "resource_deleted");
                        }
                        time::sleep(config.poll_interval).await;
                        continue;
                    }
                    Err(error) => {
                        warn!(
                            error = %error,
                            ducklake_maintenance = %config.name,
                            timeout_ms = config.kubernetes_api_timeout.as_millis() as u64,
                            "failed to read ducklake maintenance resource: ducklake_maintenance={}, \
                             timeout_ms={}, error={}",
                            config.name,
                            config.kubernetes_api_timeout.as_millis(),
                            error
                        );
                        release_expired_pause_if_needed(&api, &config, &mut held_pause).await;
                        time::sleep(config.poll_interval).await;
                        continue;
                    }
                };
                let active_pause = active_pause_request(&resource);
                reconcile_pause(&api, &config, &destination, &mut held_pause, active_pause).await;
                maybe_request_operations(&api, &config.name, &destination, &resource, &config)
                    .await;
            }
            Err(_) => {
                warn!(
                    ducklake_maintenance = %config.name,
                    timeout_ms = config.kubernetes_api_timeout.as_millis() as u64,
                    "timed out reading ducklake maintenance resource: ducklake_maintenance={}, \
                     timeout_ms={}",
                    config.name,
                    config.kubernetes_api_timeout.as_millis()
                );
                release_expired_pause_if_needed(&api, &config, &mut held_pause).await;
            }
        }

        time::sleep(config.poll_interval).await;
    }
}

async fn reconcile_pause<S>(
    api: &Api<DynamicObject>,
    config: &WatcherConfig,
    destination: &DuckLakeDestination<S>,
    held_pause: &mut Option<HeldPause>,
    active_pause: Option<PauseRequest>,
) where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    let name = config.name.as_str();
    let Some(pause) = active_pause.filter(|pause| pause.expires_at > Utc::now()) else {
        if let Some(held) = held_pause.take() {
            info!(
                ducklake_maintenance = %name,
                run_id = %held.run_id,
                "ducklake external maintenance pause cleared, resuming foreground mutations: \
                 ducklake_maintenance={}, run_id={}",
                name,
                held.run_id
            );
            release_held_pause(name, held, "cleared");
            patch_replicator_status(
                api,
                name,
                "Running",
                None,
                None,
                config.kubernetes_api_timeout,
            )
            .await;
        }
        return;
    };

    if let Some(held) = held_pause.as_mut()
        && held.run_id == pause.run_id
    {
        if !held.quiesced_reported
            && patch_replicator_status(
                api,
                name,
                "Quiesced",
                Some(&held.run_id),
                Some(held.quiesced_at),
                config.kubernetes_api_timeout,
            )
            .await
        {
            held.quiesced_reported = true;
        }
        return;
    }

    if let Some(held) = held_pause.take() {
        info!(
            ducklake_maintenance = %name,
            previous_run_id = %held.run_id,
            next_run_id = %pause.run_id,
            "ducklake external maintenance pause replaced, resuming previous run before pausing again: \
             ducklake_maintenance={}, previous_run_id={}, next_run_id={}",
            name,
            held.run_id,
            pause.run_id
        );
        release_held_pause(name, held, "replaced");
        patch_replicator_status(api, name, "Running", None, None, config.kubernetes_api_timeout)
            .await;
    }

    info!(
        ducklake_maintenance = %name,
        run_id = %pause.run_id,
        expires_at = %pause.expires_at.to_rfc3339(),
        "ducklake external maintenance pause requested, waiting for foreground mutations to drain: \
         ducklake_maintenance={}, run_id={}, expires_at={}",
        name,
        pause.run_id,
        pause.expires_at.to_rfc3339()
    );
    patch_replicator_status(
        api,
        name,
        "Pausing",
        Some(&pause.run_id),
        None,
        config.kubernetes_api_timeout,
    )
    .await;

    let external_pause = destination.acquire_external_maintenance_pause().await;
    if pause.expires_at <= Utc::now() {
        info!(
            ducklake_maintenance = %name,
            run_id = %pause.run_id,
            expires_at = %pause.expires_at.to_rfc3339(),
            "ducklake external maintenance pause expired before quiescence, resuming foreground mutations: \
             ducklake_maintenance={}, run_id={}, expires_at={}",
            name,
            pause.run_id,
            pause.expires_at.to_rfc3339()
        );
        release_held_pause(
            name,
            HeldPause {
                run_id: pause.run_id,
                expires_at: pause.expires_at,
                quiesced_at: Utc::now(),
                quiesced_reported: false,
                _pause: external_pause,
            },
            "expired",
        );
        patch_replicator_status(api, name, "Running", None, None, config.kubernetes_api_timeout)
            .await;
        return;
    }

    let quiesced_at = Utc::now();

    info!(
        ducklake_maintenance = %name,
        run_id = %pause.run_id,
        quiesced_at = %quiesced_at.to_rfc3339(),
        expires_at = %pause.expires_at.to_rfc3339(),
        "ducklake external maintenance quiesced, foreground mutations are paused: \
         ducklake_maintenance={}, run_id={}, quiesced_at={}, expires_at={}",
        name,
        pause.run_id,
        quiesced_at.to_rfc3339(),
        pause.expires_at.to_rfc3339()
    );
    let quiesced_reported = patch_replicator_status(
        api,
        name,
        "Quiesced",
        Some(&pause.run_id),
        Some(quiesced_at),
        config.kubernetes_api_timeout,
    )
    .await;
    if !quiesced_reported {
        warn!(
            ducklake_maintenance = %name,
            run_id = %pause.run_id,
            timeout_ms = config.kubernetes_api_timeout.as_millis() as u64,
            "ducklake external maintenance quiesced status was not confirmed; keeping foreground \
             mutations paused until the status patch succeeds or the pause expires: \
             ducklake_maintenance={}, run_id={}, timeout_ms={}",
            name,
            pause.run_id,
            config.kubernetes_api_timeout.as_millis()
        );
    }

    *held_pause = Some(HeldPause {
        run_id: pause.run_id,
        expires_at: pause.expires_at,
        quiesced_at,
        quiesced_reported,
        _pause: external_pause,
    });
}

async fn release_expired_pause_if_needed(
    api: &Api<DynamicObject>,
    config: &WatcherConfig,
    held_pause: &mut Option<HeldPause>,
) {
    if held_pause.as_ref().is_none_or(|pause| pause.expires_at > Utc::now()) {
        return;
    }

    let Some(expired) = held_pause.take() else {
        return;
    };
    warn!(
        ducklake_maintenance = %config.name,
        run_id = %expired.run_id,
        "ducklake maintenance pause expired while Kubernetes API was unavailable: \
         ducklake_maintenance={}, run_id={}",
        config.name,
        expired.run_id
    );
    release_held_pause(&config.name, expired, "expired");
    patch_replicator_status(
        api,
        &config.name,
        "Running",
        None,
        None,
        config.kubernetes_api_timeout,
    )
    .await;
}

fn release_held_pause(name: &str, held: HeldPause, outcome: &'static str) {
    let held_ms =
        Utc::now().signed_duration_since(held.quiesced_at).num_milliseconds().max(0) as u64;
    record_external_maintenance_pause_duration(&held, outcome);
    info!(
        ducklake_maintenance = %name,
        run_id = %held.run_id,
        outcome,
        held_ms,
        "ducklake external maintenance pause guard released: ducklake_maintenance={}, run_id={}, \
         outcome={}, held_ms={}",
        name,
        held.run_id,
        outcome,
        held_ms
    );
}

fn record_external_maintenance_pause_duration(held: &HeldPause, outcome: &'static str) {
    let duration_seconds =
        Utc::now().signed_duration_since(held.quiesced_at).num_milliseconds().max(0) as f64
            / 1_000.0;
    histogram!(
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_DURATION_SECONDS,
        MAINTENANCE_OUTCOME_LABEL => outcome,
    )
    .record(duration_seconds);
}

async fn maybe_request_operations<S>(
    api: &Api<DynamicObject>,
    name: &str,
    destination: &DuckLakeDestination<S>,
    resource: &DynamicObject,
    config: &WatcherConfig,
) where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    if active_run_exists(resource) {
        return;
    }
    if completed_run_in_cooldown(resource, config.request_cooldown) {
        return;
    }

    let policy = operation_policy(resource);
    let requested = match destination
        .sample_external_maintenance_operations(
            config.inline_flush_min_inlined_bytes,
            config.rewrite_data_files_min_active_data_files,
        )
        .await
    {
        Ok(mut operations) => {
            operations.inline_flush &= policy.inline_flush_enabled;
            operations.rewrite_data_files &= policy.rewrite_data_files_enabled;
            operations
        }
        Err(error) => {
            warn!(
                error = ?error,
                ducklake_maintenance = %name,
                "failed to sample ducklake external maintenance operations"
            );
            return;
        }
    };

    if !requested.inline_flush && !requested.rewrite_data_files {
        debug!(
            ducklake_maintenance = %name,
            inline_flush = requested.inline_flush,
            rewrite_data_files = requested.rewrite_data_files,
            inline_flush_min_inlined_bytes = config.inline_flush_min_inlined_bytes,
            rewrite_data_files_min_active_data_files =
                config.rewrite_data_files_min_active_data_files,
            "ducklake external maintenance sampled no requested operations"
        );
        return;
    }

    let already_requested = requested_operations(resource);
    if already_requested.covers(requested) {
        debug!(
            ducklake_maintenance = %name,
            inline_flush = requested.inline_flush,
            rewrite_data_files = requested.rewrite_data_files,
            "ducklake external maintenance request already exists"
        );
        return;
    }

    info!(
        ducklake_maintenance = %name,
        inline_flush = requested.inline_flush,
        rewrite_data_files = requested.rewrite_data_files,
        inline_flush_min_inlined_bytes = config.inline_flush_min_inlined_bytes,
        rewrite_data_files_min_active_data_files = config.rewrite_data_files_min_active_data_files,
        "ducklake external maintenance requesting operations"
    );
    if patch_operation_requests(api, name, requested, config).await {
        record_external_maintenance_triggers(requested, already_requested);
    }
}

async fn patch_replicator_status(
    api: &Api<DynamicObject>,
    name: &str,
    state: &str,
    observed_run_id: Option<&str>,
    quiesced_at: Option<DateTime<Utc>>,
    timeout: Duration,
) -> bool {
    let patch = json!({
        "status": {
            "replicator": {
                "state": state,
                "observedRunId": observed_run_id,
                "quiescedAt": quiesced_at.map(|time| time.to_rfc3339()),
            }
        }
    });
    let params = PatchParams::default();

    match time::timeout(timeout, api.patch_status(name, &params, &Patch::Merge(&patch))).await {
        Ok(Ok(_)) => true,
        Ok(Err(error)) => {
            warn!(
                error = %error,
                ducklake_maintenance = %name,
                state,
                "failed to patch ducklake maintenance replicator status: \
                 ducklake_maintenance={}, state={}, error={}",
                name,
                state,
                error
            );
            false
        }
        Err(_) => {
            warn!(
                ducklake_maintenance = %name,
                state,
                timeout_ms = timeout.as_millis() as u64,
                "timed out patching ducklake maintenance replicator status: \
                 ducklake_maintenance={}, state={}, timeout_ms={}",
                name,
                state,
                timeout.as_millis()
            );
            false
        }
    }
}

async fn patch_operation_requests(
    api: &Api<DynamicObject>,
    name: &str,
    requested: ExternalMaintenanceOperations,
    config: &WatcherConfig,
) -> bool {
    let patch = json!({
        "status": {
            "operationRequests": {
                "inlineFlush": requested.inline_flush,
                "rewriteDataFiles": requested.rewrite_data_files,
                "inlineFlushMinInlinedBytes": config.inline_flush_min_inlined_bytes,
                "rewriteDataFilesMinActiveDataFiles": config.rewrite_data_files_min_active_data_files,
                "requestedAt": Utc::now().to_rfc3339(),
            }
        }
    });
    let params = PatchParams::default();

    match time::timeout(
        config.kubernetes_api_timeout,
        api.patch_status(name, &params, &Patch::Merge(&patch)),
    )
    .await
    {
        Ok(Ok(_)) => true,
        Ok(Err(error)) => {
            warn!(
                error = %error,
                ducklake_maintenance = %name,
                "failed to patch ducklake maintenance operation request: \
                 ducklake_maintenance={}, error={}",
                name,
                error
            );
            false
        }
        Err(_) => {
            warn!(
                ducklake_maintenance = %name,
                timeout_ms = config.kubernetes_api_timeout.as_millis() as u64,
                "timed out patching ducklake maintenance operation request: \
                 ducklake_maintenance={}, timeout_ms={}",
                name,
                config.kubernetes_api_timeout.as_millis()
            );
            false
        }
    }
}

fn record_external_maintenance_triggers(
    requested: ExternalMaintenanceOperations,
    already_requested: ExternalMaintenanceOperations,
) {
    if requested.inline_flush && !already_requested.inline_flush {
        counter!(
            ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_TRIGGERED_TOTAL,
            MAINTENANCE_OPERATION_LABEL => OPERATION_INLINE_FLUSH,
            MAINTENANCE_REASON_LABEL => REASON_PENDING_INLINED_DATA_BYTES_THRESHOLD,
        )
        .increment(1);
    }

    if requested.rewrite_data_files && !already_requested.rewrite_data_files {
        counter!(
            ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_TRIGGERED_TOTAL,
            MAINTENANCE_OPERATION_LABEL => OPERATION_REWRITE_DATA_FILES,
            MAINTENANCE_REASON_LABEL => REASON_ACTIVE_DATA_FILES_THRESHOLD,
        )
        .increment(1);
    }
}

fn requested_operations(resource: &DynamicObject) -> ExternalMaintenanceOperations {
    let Some(requests) =
        resource.data.get("status").and_then(|status| status.get("operationRequests"))
    else {
        return ExternalMaintenanceOperations::default();
    };

    ExternalMaintenanceOperations {
        inline_flush: requests
            .get("inlineFlush")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        rewrite_data_files: requests
            .get("rewriteDataFiles")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    }
}

fn active_pause_request(resource: &DynamicObject) -> Option<PauseRequest> {
    let pause = resource.data.get("status")?.get("pauseRequest")?;
    if pause.is_null() {
        return None;
    }

    let run_id = pause.get("runId")?.as_str()?.to_owned();
    let expires_at =
        DateTime::parse_from_rfc3339(pause.get("expiresAt")?.as_str()?).ok()?.with_timezone(&Utc);

    Some(PauseRequest { run_id, expires_at })
}

fn active_run_exists(resource: &DynamicObject) -> bool {
    resource
        .data
        .get("status")
        .and_then(|status| status.get("activeRun"))
        .is_some_and(|active_run| !active_run.is_null())
}

fn completed_run_in_cooldown(resource: &DynamicObject, cooldown: Duration) -> bool {
    if cooldown.is_zero() {
        return false;
    }

    let Some(completed_at) = resource
        .data
        .get("status")
        .and_then(|status| status.get("lastCompletedRun"))
        .and_then(|run| run.get("completedAt"))
        .and_then(serde_json::Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|time| time.with_timezone(&Utc))
    else {
        return false;
    };

    let elapsed = Utc::now().signed_duration_since(completed_at);
    elapsed.to_std().is_ok_and(|elapsed| elapsed < cooldown)
}

fn operation_policy(resource: &DynamicObject) -> OperationPolicy {
    let operations = resource.data.get("spec").and_then(|spec| spec.get("operations"));
    let inline_flush = operations.and_then(|ops| ops.get("inlineFlush"));
    let rewrite_data_files = operations.and_then(|ops| ops.get("rewriteDataFiles"));

    OperationPolicy {
        inline_flush_enabled: inline_flush
            .and_then(|value| value.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        rewrite_data_files_enabled: rewrite_data_files
            .and_then(|value| value.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
    }
}

fn ducklake_maintenance_api_resource() -> ApiResource {
    let gvk = GroupVersionKind::gvk("etl.supabase.com", "v1alpha1", "DuckLakeMaintenance");
    ApiResource::from_gvk(&gvk)
}

impl WatcherConfig {
    fn from_env() -> Option<Self> {
        let name = env::var(CR_NAME_ENV).ok().filter(|value| !value.is_empty())?;
        let namespace = env::var(CR_NAMESPACE_ENV).ok().filter(|value| !value.is_empty())?;
        let poll_seconds = env::var(POLL_SECONDS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .unwrap_or(DEFAULT_POLL_SECONDS);
        let inline_flush_min_inlined_bytes = env::var(INLINE_FLUSH_MIN_INLINED_BYTES_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_INLINE_FLUSH_MIN_INLINED_BYTES);
        let rewrite_data_files_min_active_data_files =
            env::var(REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES_ENV)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(DEFAULT_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES);
        let request_cooldown = env::var(REQUEST_COOLDOWN_SECONDS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_REQUEST_COOLDOWN_SECONDS);
        let kubernetes_api_timeout = env::var(KUBERNETES_API_TIMEOUT_SECONDS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|seconds| *seconds > 0)
            .unwrap_or(DEFAULT_KUBERNETES_API_TIMEOUT_SECONDS);

        Some(Self {
            name,
            namespace,
            poll_interval: Duration::from_secs(poll_seconds),
            request_cooldown: Duration::from_secs(request_cooldown),
            kubernetes_api_timeout: Duration::from_secs(kubernetes_api_timeout),
            inline_flush_min_inlined_bytes,
            rewrite_data_files_min_active_data_files,
        })
    }
}
