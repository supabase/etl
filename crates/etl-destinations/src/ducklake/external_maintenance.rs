use std::time::Duration;

use chrono::{DateTime, Utc};
use etl::{
    error::EtlResult,
    store::{schema::SchemaStore, state::StateStore},
};
#[cfg(feature = "ducklake-kubernetes")]
pub use etl_maintenance::KubernetesExternalMaintenanceStore;
pub use etl_maintenance::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenanceOperationRun,
    ExternalMaintenanceOperations, ExternalMaintenancePause, ExternalMaintenanceReplicatorState,
    ExternalMaintenanceReplicatorStatus, ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun,
    ExternalMaintenanceState, ExternalMaintenanceStore, ExternalMaintenanceWatcherConfig,
    PostgresExternalMaintenanceStore,
};
use metrics::{counter, histogram};
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

const OPERATION_INLINE_FLUSH: &str = "flush_inlined_data";
const OPERATION_REWRITE_DATA_FILES: &str = "rewrite_data_files";
const OPERATION_EXPIRE_SNAPSHOTS: &str = "expire_snapshots";
const REASON_PENDING_INLINED_DATA_BYTES_THRESHOLD: &str = "pending_inlined_data_bytes_threshold";
const REASON_ACTIVE_DATA_FILES_THRESHOLD: &str = "active_data_files_threshold";
const REASON_SNAPSHOT_RETENTION_THRESHOLD: &str = "snapshot_retention_threshold";

struct HeldPause {
    run_id: String,
    expires_at: DateTime<Utc>,
    quiesced_at: DateTime<Utc>,
    quiesced_reported: bool,
    _pause: DuckLakeExternalMaintenancePause,
}

#[cfg(feature = "ducklake-kubernetes")]
pub(super) async fn run_kubernetes_external_maintenance_watcher<S>(
    destination: DuckLakeDestination<S>,
) -> EtlResult<()>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
{
    let config = ExternalMaintenanceWatcherConfig::from_env();
    let Some(store) = KubernetesExternalMaintenanceStore::from_env(config.store_timeout).await?
    else {
        info!("ducklake Kubernetes external maintenance watcher disabled because CR env is absent");
        return Ok(());
    };

    run_external_maintenance_watcher(destination, store, config).await
}

pub async fn run_external_maintenance_watcher<S, M>(
    destination: DuckLakeDestination<S>,
    store: M,
    config: ExternalMaintenanceWatcherConfig,
) -> EtlResult<()>
where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    M: ExternalMaintenanceStore,
{
    let mut held_pause: Option<HeldPause> = None;

    info!(
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        request_cooldown_ms = config.request_cooldown.as_millis() as u64,
        store_timeout_ms = config.store_timeout.as_millis() as u64,
        "ducklake external maintenance watcher started: poll_interval_ms={}, \
         request_cooldown_ms={}, store_timeout_ms={}",
        config.poll_interval.as_millis(),
        config.request_cooldown.as_millis(),
        config.store_timeout.as_millis()
    );

    loop {
        match time::timeout(config.store_timeout, store.load_state()).await {
            Ok(Ok(state)) => {
                if !state.exists {
                    release_missing_state_pause(&store, &mut held_pause).await;
                    time::sleep(config.poll_interval).await;
                    continue;
                }

                reconcile_pause(&store, &config, &destination, &mut held_pause, &state).await;
                maybe_request_operations(&store, &destination, &state, &config).await;
            }
            Ok(Err(error)) => {
                warn!(
                    error = %error,
                    timeout_ms = config.store_timeout.as_millis() as u64,
                    "failed to read ducklake external maintenance state: timeout_ms={}, error={}",
                    config.store_timeout.as_millis(),
                    error
                );
                release_expired_pause_if_needed(&store, &config, &mut held_pause).await;
            }
            Err(_) => {
                warn!(
                    timeout_ms = config.store_timeout.as_millis() as u64,
                    "timed out reading ducklake external maintenance state: timeout_ms={}",
                    config.store_timeout.as_millis()
                );
                release_expired_pause_if_needed(&store, &config, &mut held_pause).await;
            }
        }

        time::sleep(config.poll_interval).await;
    }
}

async fn release_missing_state_pause<M>(store: &M, held_pause: &mut Option<HeldPause>)
where
    M: ExternalMaintenanceStore,
{
    if let Some(held) = held_pause.take() {
        info!(
            run_id = %held.run_id,
            "ducklake maintenance state disappeared, resuming foreground mutations: run_id={}",
            held.run_id
        );
        release_held_pause(held, "state_missing");
        if let Err(error) = store.clear_replicator_status().await {
            warn!(
                error = %error,
                "failed to clear ducklake maintenance replicator status after state disappeared: \
                 error={}",
                error
            );
        }
    }
}

async fn reconcile_pause<S, M>(
    store: &M,
    config: &ExternalMaintenanceWatcherConfig,
    destination: &DuckLakeDestination<S>,
    held_pause: &mut Option<HeldPause>,
    state: &ExternalMaintenanceState,
) where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    M: ExternalMaintenanceStore,
{
    let active_pause = state.pause_request.clone().filter(|pause| pause.expires_at > Utc::now());
    let Some(pause) = active_pause else {
        if let Some(held) = held_pause.take() {
            info!(
                run_id = %held.run_id,
                "ducklake external maintenance pause cleared, resuming foreground mutations: \
                 run_id={}",
                held.run_id
            );
            release_held_pause(held, "cleared");
            report_running(store, config.store_timeout).await;
        }
        return;
    };

    if let Some(held) = held_pause.as_mut()
        && held.run_id == pause.run_id
    {
        if !held.quiesced_reported
            && report_quiesced(store, &held.run_id, held.quiesced_at, config.store_timeout).await
        {
            held.quiesced_reported = true;
        }
        return;
    }

    if let Some(held) = held_pause.take() {
        info!(
            previous_run_id = %held.run_id,
            next_run_id = %pause.run_id,
            "ducklake external maintenance pause replaced, resuming previous run before pausing again: \
             previous_run_id={}, next_run_id={}",
            held.run_id,
            pause.run_id
        );
        release_held_pause(held, "replaced");
        report_running(store, config.store_timeout).await;
    }

    info!(
        run_id = %pause.run_id,
        expires_at = %pause.expires_at.to_rfc3339(),
        "ducklake external maintenance pause requested, waiting for foreground mutations to drain: \
         run_id={}, expires_at={}",
        pause.run_id,
        pause.expires_at.to_rfc3339()
    );
    report_pausing(store, &pause.run_id, config.store_timeout).await;

    let external_pause = destination.acquire_external_maintenance_pause().await;
    if pause.expires_at <= Utc::now() {
        info!(
            run_id = %pause.run_id,
            expires_at = %pause.expires_at.to_rfc3339(),
            "ducklake external maintenance pause expired before quiescence, resuming foreground \
             mutations: run_id={}, expires_at={}",
            pause.run_id,
            pause.expires_at.to_rfc3339()
        );
        release_held_pause(
            HeldPause {
                run_id: pause.run_id,
                expires_at: pause.expires_at,
                quiesced_at: Utc::now(),
                quiesced_reported: false,
                _pause: external_pause,
            },
            "expired",
        );
        report_running(store, config.store_timeout).await;
        return;
    }

    let quiesced_at = Utc::now();

    info!(
        run_id = %pause.run_id,
        quiesced_at = %quiesced_at.to_rfc3339(),
        expires_at = %pause.expires_at.to_rfc3339(),
        "ducklake external maintenance quiesced, foreground mutations are paused: run_id={}, \
         quiesced_at={}, expires_at={}",
        pause.run_id,
        quiesced_at.to_rfc3339(),
        pause.expires_at.to_rfc3339()
    );
    let quiesced_reported =
        report_quiesced(store, &pause.run_id, quiesced_at, config.store_timeout).await;
    if !quiesced_reported {
        warn!(
            run_id = %pause.run_id,
            timeout_ms = config.store_timeout.as_millis() as u64,
            "ducklake external maintenance quiesced status was not confirmed; keeping foreground \
             mutations paused until the status patch succeeds or the pause expires: run_id={}, \
             timeout_ms={}",
            pause.run_id,
            config.store_timeout.as_millis()
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

async fn release_expired_pause_if_needed<M>(
    store: &M,
    config: &ExternalMaintenanceWatcherConfig,
    held_pause: &mut Option<HeldPause>,
) where
    M: ExternalMaintenanceStore,
{
    if held_pause.as_ref().is_none_or(|pause| pause.expires_at > Utc::now()) {
        return;
    }

    let Some(expired) = held_pause.take() else {
        return;
    };
    warn!(
        run_id = %expired.run_id,
        "ducklake maintenance pause expired while external maintenance store was unavailable: \
         run_id={}",
        expired.run_id
    );
    release_held_pause(expired, "expired");
    report_running(store, config.store_timeout).await;
}

fn release_held_pause(held: HeldPause, outcome: &'static str) {
    let held_ms =
        Utc::now().signed_duration_since(held.quiesced_at).num_milliseconds().max(0) as u64;
    record_external_maintenance_pause_duration(&held, outcome);
    info!(
        run_id = %held.run_id,
        outcome,
        held_ms,
        "ducklake external maintenance pause guard released: run_id={}, outcome={}, held_ms={}",
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

async fn maybe_request_operations<S, M>(
    store: &M,
    destination: &DuckLakeDestination<S>,
    state: &ExternalMaintenanceState,
    config: &ExternalMaintenanceWatcherConfig,
) where
    S: StateStore + SchemaStore + Clone + Send + Sync + 'static,
    M: ExternalMaintenanceStore,
{
    if state.active_run.is_some() {
        return;
    }
    if completed_run_in_cooldown(state, config.request_cooldown) {
        return;
    }

    let requested = match destination
        .sample_external_maintenance_operations(
            config.inline_flush_min_inlined_bytes,
            config.rewrite_data_files_min_active_data_files,
        )
        .await
    {
        Ok(mut operations) => {
            operations.inline_flush &= state.operation_policy.inline_flush_enabled;
            operations.merge_adjacent_files &= state.operation_policy.merge_adjacent_files_enabled;
            operations.rewrite_data_files &= state.operation_policy.rewrite_data_files_enabled;
            operations.expire_snapshots &= state.operation_policy.expire_snapshots_enabled;
            operations.cleanup_old_files &= state.operation_policy.cleanup_old_files_enabled;
            operations
        }
        Err(error) => {
            warn!(
                error = %error,
                "failed to sample ducklake external maintenance operations: error={}",
                error
            );
            return;
        }
    };

    if requested.is_empty() {
        debug!(
            inline_flush = requested.inline_flush,
            merge_adjacent_files = requested.merge_adjacent_files,
            rewrite_data_files = requested.rewrite_data_files,
            expire_snapshots = requested.expire_snapshots,
            cleanup_old_files = requested.cleanup_old_files,
            inline_flush_min_inlined_bytes = config.inline_flush_min_inlined_bytes,
            rewrite_data_files_min_active_data_files =
                config.rewrite_data_files_min_active_data_files,
            "ducklake external maintenance sampled no requested operations"
        );
        return;
    }

    let already_requested = state
        .operation_request
        .as_ref()
        .map_or(ExternalMaintenanceOperations::default(), |request| request.operations);
    if already_requested.covers(requested) {
        debug!(
            inline_flush = requested.inline_flush,
            merge_adjacent_files = requested.merge_adjacent_files,
            rewrite_data_files = requested.rewrite_data_files,
            expire_snapshots = requested.expire_snapshots,
            cleanup_old_files = requested.cleanup_old_files,
            "ducklake external maintenance request already exists"
        );
        return;
    }

    info!(
        inline_flush = requested.inline_flush,
        merge_adjacent_files = requested.merge_adjacent_files,
        rewrite_data_files = requested.rewrite_data_files,
        expire_snapshots = requested.expire_snapshots,
        cleanup_old_files = requested.cleanup_old_files,
        inline_flush_min_inlined_bytes = config.inline_flush_min_inlined_bytes,
        rewrite_data_files_min_active_data_files = config.rewrite_data_files_min_active_data_files,
        "ducklake external maintenance requesting operations"
    );

    let request = ExternalMaintenanceOperationRequest {
        operations: requested,
        inline_flush_min_inlined_bytes: Some(config.inline_flush_min_inlined_bytes),
        rewrite_data_files_min_active_data_files: Some(
            config.rewrite_data_files_min_active_data_files,
        ),
        requested_at: Utc::now(),
    };
    match time::timeout(config.store_timeout, store.request_operations(request)).await {
        Ok(Ok(ExternalMaintenanceRequestOutcome::Created)) => {
            record_external_maintenance_triggers(requested, already_requested);
        }
        Ok(Ok(ExternalMaintenanceRequestOutcome::AlreadyCovered)) => {
            debug!("ducklake external maintenance request became already covered");
        }
        Ok(Ok(ExternalMaintenanceRequestOutcome::RejectedActiveRun)) => {
            debug!("ducklake external maintenance request rejected because an active run exists");
        }
        Ok(Ok(ExternalMaintenanceRequestOutcome::MissingState)) => {
            debug!("ducklake external maintenance request ignored because state is missing");
        }
        Ok(Err(error)) => {
            warn!(
                error = %error,
                "failed to request ducklake external maintenance operations: error={}",
                error
            );
        }
        Err(_) => {
            warn!(
                timeout_ms = config.store_timeout.as_millis() as u64,
                "timed out requesting ducklake external maintenance operations: timeout_ms={}",
                config.store_timeout.as_millis()
            );
        }
    }
}

async fn report_pausing<M>(store: &M, run_id: &str, timeout: Duration)
where
    M: ExternalMaintenanceStore,
{
    report_replicator_status(
        store,
        ExternalMaintenanceReplicatorStatus {
            state: ExternalMaintenanceReplicatorState::Pausing,
            observed_run_id: Some(run_id.to_owned()),
            quiesced_at: None,
        },
        timeout,
    )
    .await;
}

async fn report_quiesced<M>(
    store: &M,
    run_id: &str,
    quiesced_at: DateTime<Utc>,
    timeout: Duration,
) -> bool
where
    M: ExternalMaintenanceStore,
{
    report_replicator_status(
        store,
        ExternalMaintenanceReplicatorStatus {
            state: ExternalMaintenanceReplicatorState::Quiesced,
            observed_run_id: Some(run_id.to_owned()),
            quiesced_at: Some(quiesced_at),
        },
        timeout,
    )
    .await
}

async fn report_running<M>(store: &M, timeout: Duration)
where
    M: ExternalMaintenanceStore,
{
    report_replicator_status(
        store,
        ExternalMaintenanceReplicatorStatus {
            state: ExternalMaintenanceReplicatorState::Running,
            observed_run_id: None,
            quiesced_at: None,
        },
        timeout,
    )
    .await;
}

async fn report_replicator_status<M>(
    store: &M,
    status: ExternalMaintenanceReplicatorStatus,
    timeout: Duration,
) -> bool
where
    M: ExternalMaintenanceStore,
{
    let state = status.state;
    match time::timeout(timeout, store.report_replicator_status(status)).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(
                error = %error,
                state = ?state,
                "failed to report ducklake maintenance replicator status: state={:?}, error={}",
                state,
                error
            );
            false
        }
        Err(_) => {
            warn!(
                state = ?state,
                timeout_ms = timeout.as_millis() as u64,
                "timed out reporting ducklake maintenance replicator status: state={:?}, \
                 timeout_ms={}",
                state,
                timeout.as_millis()
            );
            false
        }
    }
}

fn completed_run_in_cooldown(state: &ExternalMaintenanceState, cooldown: Duration) -> bool {
    if cooldown.is_zero() {
        return false;
    }

    let Some(completed_at) = state.last_completed_at else {
        return false;
    };

    let elapsed = Utc::now().signed_duration_since(completed_at);
    elapsed.to_std().is_ok_and(|elapsed| elapsed < cooldown)
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

    if requested.expire_snapshots && !already_requested.expire_snapshots {
        counter!(
            ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_TRIGGERED_TOTAL,
            MAINTENANCE_OPERATION_LABEL => OPERATION_EXPIRE_SNAPSHOTS,
            MAINTENANCE_REASON_LABEL => REASON_SNAPSHOT_RETENTION_THRESHOLD,
        )
        .increment(1);
    }
}
