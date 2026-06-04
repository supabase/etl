use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use etl::{error::EtlResult, store::DestinationStore};
pub use etl_maintenance::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenanceOperationRun,
    ExternalMaintenanceOperations, ExternalMaintenancePause, ExternalMaintenancePausePolicy,
    ExternalMaintenanceReplicatorState, ExternalMaintenanceReplicatorStatus,
    ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun, ExternalMaintenanceState,
    ExternalMaintenanceStore, ExternalMaintenanceWatcherConfig, KubernetesExternalMaintenanceStore,
    PostgresExternalMaintenanceStore,
};
use metrics::{counter, gauge, histogram};
use sqlx::PgPool;
use tokio::time;
use tracing::{debug, info, warn};

use crate::ducklake::{
    DuckLakeDestination, DuckLakeExternalMaintenancePause,
    metrics::{
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_ACTIVE,
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_DURATION_SECONDS,
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_TRIGGERED_TOTAL, MAINTENANCE_OPERATION_LABEL,
        MAINTENANCE_OUTCOME_LABEL, MAINTENANCE_REASON_LABEL,
    },
};

const EXPIRE_SNAPSHOTS_MIN_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
const OPERATION_INLINE_FLUSH: &str = "flush_inlined_data";
const OPERATION_MERGE_ADJACENT_FILES: &str = "merge_adjacent_files";
const OPERATION_REWRITE_DATA_FILES: &str = "rewrite_data_files";
const OPERATION_EXPIRE_SNAPSHOTS: &str = "expire_snapshots";
const OPERATION_CLEANUP_OLD_FILES: &str = "cleanup_old_files";
const OPERATION_UNKNOWN: &str = "unknown";
const ALL_PAUSE_OPERATION_LABELS: [&str; 6] = [
    OPERATION_INLINE_FLUSH,
    OPERATION_MERGE_ADJACENT_FILES,
    OPERATION_REWRITE_DATA_FILES,
    OPERATION_EXPIRE_SNAPSHOTS,
    OPERATION_CLEANUP_OLD_FILES,
    OPERATION_UNKNOWN,
];
const REASON_PENDING_INLINED_DATA_BYTES_THRESHOLD: &str = "pending_inlined_data_bytes_threshold";
const REASON_ACTIVE_DATA_FILES_THRESHOLD: &str = "active_data_files_threshold";
const REASON_SNAPSHOT_RETENTION_THRESHOLD: &str = "snapshot_retention_threshold";

struct HeldPause {
    run_id: String,
    expires_at: DateTime<Utc>,
    operations: ExternalMaintenanceOperations,
    quiesced_at: DateTime<Utc>,
    quiesced_reported: bool,
    _pause: DuckLakeExternalMaintenancePause,
}

/// Suppresses duplicate expire-snapshots requests inside the daily controller
/// interval.
#[derive(Default)]
struct ExpireSnapshotsRequestGate {
    initialized_from_state: bool,
    next_request_after: Option<DateTime<Utc>>,
}

pub(super) async fn run_kubernetes_external_maintenance_watcher<S>(
    destination: DuckLakeDestination<S>,
) -> EtlResult<()>
where
    S: DestinationStore,
{
    let config = ExternalMaintenanceWatcherConfig::from_env();
    let Some(store) = KubernetesExternalMaintenanceStore::from_env(config.store_timeout).await?
    else {
        info!("ducklake Kubernetes external maintenance watcher disabled because CR env is absent");
        return Ok(());
    };

    run_external_maintenance_watcher(destination, store, config).await
}

pub(super) async fn run_postgres_external_maintenance_watcher<S>(
    destination: DuckLakeDestination<S>,
    pipeline_id: i64,
    pool: PgPool,
) -> EtlResult<()>
where
    S: DestinationStore,
{
    let config = ExternalMaintenanceWatcherConfig::from_env();
    let store = PostgresExternalMaintenanceStore::new(pipeline_id, pool);
    store.ensure_schema().await?;
    store.ensure_pipeline_state_if_missing(ExternalMaintenanceOperationPolicy::default()).await?;

    info!(
        pipeline_id,
        "ducklake Postgres external maintenance watcher configured: pipeline_id={}", pipeline_id
    );

    run_external_maintenance_watcher(destination, store, config).await
}

pub async fn run_external_maintenance_watcher<S, M>(
    destination: DuckLakeDestination<S>,
    store: M,
    config: ExternalMaintenanceWatcherConfig,
) -> EtlResult<()>
where
    S: DestinationStore,
    M: ExternalMaintenanceStore,
{
    let mut held_pause: Option<HeldPause> = None;
    let mut expire_snapshots_gate = ExpireSnapshotsRequestGate::default();
    record_external_maintenance_pause_active(ExternalMaintenanceOperations::default(), false);

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

                expire_snapshots_gate.initialize_from_state_once(&state);
                reconcile_pause(&store, &config, &destination, &mut held_pause, &state).await;
                maybe_request_operations(
                    &store,
                    &destination,
                    &state,
                    &config,
                    &mut expire_snapshots_gate,
                )
                .await;
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
    S: DestinationStore,
    M: ExternalMaintenanceStore,
{
    let active_pause = bounded_active_pause_request(state, Utc::now());
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
        let operations = active_pause_operations(state, &pause);
        if !operations.is_empty() && held.operations != operations {
            record_external_maintenance_pause_active(held.operations, false);
            held.operations = operations;
            record_external_maintenance_pause_active(held.operations, true);
        }
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
    let operations = active_pause_operations(state, &pause);
    record_external_maintenance_pause_active(operations, true);

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
                operations,
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
        operations,
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
    record_external_maintenance_pause_active(held.operations, false);
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

fn active_pause_operations(
    state: &ExternalMaintenanceState,
    pause: &ExternalMaintenancePause,
) -> ExternalMaintenanceOperations {
    if let Some(operations) = state
        .active_run
        .as_ref()
        .filter(|run| run.run_id == pause.run_id)
        .map(|run| run.operations)
        .filter(|operations| !operations.is_empty())
    {
        return operations;
    }

    state
        .operation_request
        .as_ref()
        .map_or(ExternalMaintenanceOperations::default(), |request| request.operations)
}

/// Returns a pause request whose expiry is bounded by trusted policy.
fn bounded_active_pause_request(
    state: &ExternalMaintenanceState,
    now: DateTime<Utc>,
) -> Option<ExternalMaintenancePause> {
    let mut pause = state.pause_request.clone()?;
    let requested_at = pause.requested_at?;
    if requested_at > now {
        return None;
    }

    let max_pause_seconds = i64::try_from(state.pause_policy.max_duration_seconds).ok()?;
    let max_pause_duration = TimeDelta::try_seconds(max_pause_seconds)?;
    let max_expires_at = requested_at.checked_add_signed(max_pause_duration)?;
    if pause.expires_at > max_expires_at {
        pause.expires_at = max_expires_at;
    }

    (pause.expires_at > now).then_some(pause)
}

fn operation_label_values(operations: ExternalMaintenanceOperations) -> Vec<&'static str> {
    let mut labels = Vec::with_capacity(ALL_PAUSE_OPERATION_LABELS.len());
    if operations.inline_flush {
        labels.push(OPERATION_INLINE_FLUSH);
    }
    if operations.merge_adjacent_files {
        labels.push(OPERATION_MERGE_ADJACENT_FILES);
    }
    if operations.rewrite_data_files {
        labels.push(OPERATION_REWRITE_DATA_FILES);
    }
    if operations.expire_snapshots {
        labels.push(OPERATION_EXPIRE_SNAPSHOTS);
    }
    if operations.cleanup_old_files {
        labels.push(OPERATION_CLEANUP_OLD_FILES);
    }
    if labels.is_empty() {
        labels.push(OPERATION_UNKNOWN);
    }
    labels
}

fn record_external_maintenance_pause_active(
    operations: ExternalMaintenanceOperations,
    active: bool,
) {
    let active_operations = operation_label_values(operations);
    for operation in ALL_PAUSE_OPERATION_LABELS {
        let value = if active && active_operations.contains(&operation) { 1.0 } else { 0.0 };
        gauge!(
            ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_ACTIVE,
            MAINTENANCE_OPERATION_LABEL => operation,
        )
        .set(value);
    }
}

fn record_external_maintenance_pause_duration(held: &HeldPause, outcome: &'static str) {
    let duration_seconds =
        Utc::now().signed_duration_since(held.quiesced_at).num_milliseconds().max(0) as f64
            / 1_000.0;
    for operation in operation_label_values(held.operations) {
        histogram!(
            ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_DURATION_SECONDS,
            MAINTENANCE_OPERATION_LABEL => operation,
            MAINTENANCE_OUTCOME_LABEL => outcome,
        )
        .record(duration_seconds);
    }
}

impl ExpireSnapshotsRequestGate {
    fn initialize_from_state_once(&mut self, state: &ExternalMaintenanceState) {
        if self.initialized_from_state {
            return;
        }
        self.initialized_from_state = true;

        let Some(completed_at) =
            state.last_successful_operations.expire_snapshots.as_ref().map(|run| run.completed_at)
        else {
            debug!(
                "ducklake expire-snapshots request gate initialized without previous successful \
                 run"
            );
            return;
        };

        self.next_request_after =
            Some(completed_at + chrono::Duration::seconds(EXPIRE_SNAPSHOTS_MIN_INTERVAL_SECONDS));
        debug!(
            completed_at = %completed_at,
            next_request_after = ?self.next_request_after,
            "ducklake expire-snapshots request gate initialized from maintenance state: \
             completed_at={}, next_request_after={:?}",
            completed_at,
            self.next_request_after
        );
    }

    fn is_suppressed(&self, now: DateTime<Utc>) -> bool {
        self.next_request_after.is_some_and(|next_request_after| now < next_request_after)
    }

    fn record_requested(&mut self, now: DateTime<Utc>) {
        self.next_request_after =
            Some(now + chrono::Duration::seconds(EXPIRE_SNAPSHOTS_MIN_INTERVAL_SECONDS));
    }
}

async fn maybe_request_operations<S, M>(
    store: &M,
    destination: &DuckLakeDestination<S>,
    state: &ExternalMaintenanceState,
    config: &ExternalMaintenanceWatcherConfig,
    expire_snapshots_gate: &mut ExpireSnapshotsRequestGate,
) where
    S: DestinationStore,
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
            if operations.expire_snapshots && expire_snapshots_gate.is_suppressed(Utc::now()) {
                debug!("ducklake expire-snapshots request suppressed by local daily gate");
                operations.expire_snapshots = false;
            }
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
            if requested.expire_snapshots && !already_requested.expire_snapshots {
                expire_snapshots_gate.record_requested(Utc::now());
            }
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

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use etl_telemetry::metrics::init_metrics_handle;

    use super::{
        ExpireSnapshotsRequestGate, ExternalMaintenanceOperationRequest,
        ExternalMaintenanceOperationRun, ExternalMaintenanceOperations, ExternalMaintenancePause,
        ExternalMaintenancePausePolicy, ExternalMaintenanceRun, ExternalMaintenanceState,
        OPERATION_EXPIRE_SNAPSHOTS, OPERATION_INLINE_FLUSH, OPERATION_REWRITE_DATA_FILES,
        OPERATION_UNKNOWN, active_pause_operations, bounded_active_pause_request,
        record_external_maintenance_pause_active,
    };
    use crate::ducklake::metrics::{
        ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_ACTIVE, register_metrics,
    };

    fn state_with_expire_snapshots_completed_at(
        completed_at: DateTime<Utc>,
    ) -> ExternalMaintenanceState {
        let mut state = ExternalMaintenanceState::present();
        state.last_successful_operations.expire_snapshots = Some(ExternalMaintenanceOperationRun {
            run_id: Some("run-1".to_owned()),
            completed_at,
        });
        state
    }

    fn operation_request(
        operations: ExternalMaintenanceOperations,
    ) -> ExternalMaintenanceOperationRequest {
        ExternalMaintenanceOperationRequest {
            operations,
            inline_flush_min_inlined_bytes: Some(10_000_000),
            rewrite_data_files_min_active_data_files: Some(40),
            requested_at: Utc::now(),
        }
    }

    fn pause_active_gauge_value(rendered: &str, operation: &str) -> Option<f64> {
        let label = format!("operation=\"{operation}\"");
        rendered.lines().find_map(|line| {
            if line.starts_with(ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_PAUSE_ACTIVE)
                && line.contains(&label)
            {
                line.split_whitespace().last()?.parse::<f64>().ok()
            } else {
                None
            }
        })
    }

    #[test]
    fn expire_snapshots_gate_initializes_from_state_once() {
        let now = Utc.with_ymd_and_hms(2026, 5, 12, 12, 0, 0).unwrap();
        let first = state_with_expire_snapshots_completed_at(now - chrono::Duration::hours(1));
        let second = state_with_expire_snapshots_completed_at(now - chrono::Duration::hours(25));
        let mut gate = ExpireSnapshotsRequestGate::default();

        gate.initialize_from_state_once(&first);
        gate.initialize_from_state_once(&second);

        assert!(gate.is_suppressed(now));
        assert!(gate.is_suppressed(now + chrono::Duration::hours(22)));
        assert!(!gate.is_suppressed(now + chrono::Duration::hours(23)));
    }

    #[test]
    fn expire_snapshots_gate_records_local_request() {
        let now = Utc.with_ymd_and_hms(2026, 5, 12, 12, 0, 0).unwrap();
        let mut gate = ExpireSnapshotsRequestGate::default();

        gate.record_requested(now);

        assert!(gate.is_suppressed(now + chrono::Duration::hours(23)));
        assert!(!gate.is_suppressed(now + chrono::Duration::hours(24)));
    }

    #[tokio::test]
    async fn recording_external_maintenance_pause_active_exports_gauge_value() {
        let handle = init_metrics_handle().expect("failed to initialize prometheus handle");
        register_metrics();

        let operations = ExternalMaintenanceOperations {
            inline_flush: true,
            expire_snapshots: true,
            ..ExternalMaintenanceOperations::default()
        };
        record_external_maintenance_pause_active(operations, true);
        let rendered = handle.render();
        assert_eq!(pause_active_gauge_value(&rendered, OPERATION_INLINE_FLUSH), Some(1.0));
        assert_eq!(pause_active_gauge_value(&rendered, OPERATION_EXPIRE_SNAPSHOTS), Some(1.0));
        assert_eq!(pause_active_gauge_value(&rendered, OPERATION_REWRITE_DATA_FILES), Some(0.0));
        assert_eq!(pause_active_gauge_value(&rendered, OPERATION_UNKNOWN), Some(0.0));

        record_external_maintenance_pause_active(operations, false);
        let rendered = handle.render();
        assert_eq!(pause_active_gauge_value(&rendered, OPERATION_INLINE_FLUSH), Some(0.0));
        assert_eq!(pause_active_gauge_value(&rendered, OPERATION_EXPIRE_SNAPSHOTS), Some(0.0));
    }

    #[test]
    fn active_pause_operations_prefers_matching_active_run() {
        let active_operations = ExternalMaintenanceOperations {
            expire_snapshots: true,
            ..ExternalMaintenanceOperations::default()
        };
        let request_operations = ExternalMaintenanceOperations {
            inline_flush: true,
            ..ExternalMaintenanceOperations::default()
        };
        let pause = ExternalMaintenancePause {
            run_id: "run-1".to_owned(),
            requested_at: None,
            expires_at: Utc::now() + chrono::Duration::minutes(1),
        };
        let state = ExternalMaintenanceState {
            exists: true,
            active_run: Some(ExternalMaintenanceRun {
                run_id: "run-1".to_owned(),
                started_at: None,
                operations: active_operations,
            }),
            operation_request: Some(operation_request(request_operations)),
            ..ExternalMaintenanceState::default()
        };

        assert_eq!(active_pause_operations(&state, &pause), active_operations);
    }

    #[test]
    fn active_pause_operations_uses_pending_request_when_active_run_has_no_operations() {
        let request_operations = ExternalMaintenanceOperations {
            inline_flush: true,
            rewrite_data_files: true,
            ..ExternalMaintenanceOperations::default()
        };
        let pause = ExternalMaintenancePause {
            run_id: "run-1".to_owned(),
            requested_at: None,
            expires_at: Utc::now() + chrono::Duration::minutes(1),
        };
        let state = ExternalMaintenanceState {
            exists: true,
            active_run: Some(ExternalMaintenanceRun {
                run_id: "run-1".to_owned(),
                started_at: None,
                operations: ExternalMaintenanceOperations::default(),
            }),
            operation_request: Some(operation_request(request_operations)),
            ..ExternalMaintenanceState::default()
        };

        assert_eq!(active_pause_operations(&state, &pause), request_operations);
    }

    #[test]
    fn bounded_active_pause_request_clamps_status_expiry_to_policy() {
        let requested_at = Utc.with_ymd_and_hms(2026, 5, 12, 12, 0, 0).unwrap();
        let now = requested_at + chrono::Duration::minutes(10);
        let state = ExternalMaintenanceState {
            pause_request: Some(ExternalMaintenancePause {
                run_id: "run-1".to_owned(),
                requested_at: Some(requested_at),
                expires_at: requested_at + chrono::Duration::hours(12),
            }),
            pause_policy: ExternalMaintenancePausePolicy { max_duration_seconds: 2700 },
            ..ExternalMaintenanceState::present()
        };

        let pause = bounded_active_pause_request(&state, now).expect("pause should be active");

        assert_eq!(pause.expires_at, requested_at + chrono::Duration::seconds(2700));
    }

    #[test]
    fn bounded_active_pause_request_rejects_unbounded_status() {
        let requested_at = Utc.with_ymd_and_hms(2026, 5, 12, 12, 0, 0).unwrap();
        let now = requested_at + chrono::Duration::hours(1);
        let state = ExternalMaintenanceState {
            pause_request: Some(ExternalMaintenancePause {
                run_id: "run-1".to_owned(),
                requested_at: Some(requested_at),
                expires_at: requested_at + chrono::Duration::hours(12),
            }),
            pause_policy: ExternalMaintenancePausePolicy { max_duration_seconds: 2700 },
            ..ExternalMaintenanceState::present()
        };

        assert!(bounded_active_pause_request(&state, now).is_none());
    }

    #[test]
    fn bounded_active_pause_request_rejects_missing_or_future_requested_at() {
        let now = Utc.with_ymd_and_hms(2026, 5, 12, 12, 0, 0).unwrap();
        let missing_requested_at = ExternalMaintenanceState {
            pause_request: Some(ExternalMaintenancePause {
                run_id: "run-1".to_owned(),
                requested_at: None,
                expires_at: now + chrono::Duration::hours(12),
            }),
            ..ExternalMaintenanceState::present()
        };
        let future_requested_at = ExternalMaintenanceState {
            pause_request: Some(ExternalMaintenancePause {
                run_id: "run-1".to_owned(),
                requested_at: Some(now + chrono::Duration::minutes(1)),
                expires_at: now + chrono::Duration::hours(12),
            }),
            ..ExternalMaintenanceState::present()
        };

        assert!(bounded_active_pause_request(&missing_requested_at, now).is_none());
        assert!(bounded_active_pause_request(&future_requested_at, now).is_none());
    }
}
