use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use etl::error::EtlResult;
use serde::{Deserialize, Serialize};

const DEFAULT_POLL_SECONDS: u64 = 5;
const DEFAULT_INLINE_FLUSH_MIN_INLINED_BYTES: u64 = 10_000_000;
const DEFAULT_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES: i64 = 40;
const DEFAULT_REQUEST_COOLDOWN_SECONDS: u64 = 300;
const DEFAULT_STORE_TIMEOUT_SECONDS: u64 = 10;

/// Backend-neutral runtime state for external maintenance.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceState {
    /// Whether the coordination backend has state for this pipeline.
    pub exists: bool,
    /// Currently active maintenance run, if any.
    pub active_run: Option<ExternalMaintenanceRun>,
    /// Controller-owned pause lease observed by the replicator.
    pub pause_request: Option<ExternalMaintenancePause>,
    /// Replicator-owned operation request sampled from destination state.
    pub operation_request: Option<ExternalMaintenanceOperationRequest>,
    /// Last reported replicator maintenance state.
    pub replicator: Option<ExternalMaintenanceReplicatorStatus>,
    /// Last successful run per operation.
    pub last_successful_operations: ExternalMaintenanceOperationHistory,
    /// Last completed run timestamp, regardless of outcome.
    pub last_completed_at: Option<DateTime<Utc>>,
    /// Backend-neutral operation enablement policy.
    pub operation_policy: ExternalMaintenanceOperationPolicy,
}

impl ExternalMaintenanceState {
    /// Returns a default state row marked as existing.
    pub fn present() -> Self {
        Self { exists: true, ..Self::default() }
    }
}

/// One active maintenance attempt.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceRun {
    /// Stable run identifier.
    pub run_id: String,
    /// Run start timestamp.
    pub started_at: Option<DateTime<Utc>>,
}

/// Backend-neutral operation request flags.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceOperations {
    /// Whether inline data should be flushed.
    pub inline_flush: bool,
    /// Whether adjacent files should be merged.
    pub merge_adjacent_files: bool,
    /// Whether data files should be rewritten.
    pub rewrite_data_files: bool,
    /// Whether snapshots should be expired.
    pub expire_snapshots: bool,
    /// Whether old files should be cleaned up.
    pub cleanup_old_files: bool,
}

impl ExternalMaintenanceOperations {
    /// Returns whether no operation is requested.
    pub fn is_empty(self) -> bool {
        !self.inline_flush
            && !self.merge_adjacent_files
            && !self.rewrite_data_files
            && !self.expire_snapshots
            && !self.cleanup_old_files
    }

    /// Returns whether this operation set covers all requested operations.
    pub fn covers(self, requested: Self) -> bool {
        (!requested.inline_flush || self.inline_flush)
            && (!requested.merge_adjacent_files || self.merge_adjacent_files)
            && (!requested.rewrite_data_files || self.rewrite_data_files)
            && (!requested.expire_snapshots || self.expire_snapshots)
            && (!requested.cleanup_old_files || self.cleanup_old_files)
    }

    /// Returns an idempotent union of two operation sets.
    pub fn merge(self, requested: Self) -> Self {
        Self {
            inline_flush: self.inline_flush || requested.inline_flush,
            merge_adjacent_files: self.merge_adjacent_files || requested.merge_adjacent_files,
            rewrite_data_files: self.rewrite_data_files || requested.rewrite_data_files,
            expire_snapshots: self.expire_snapshots || requested.expire_snapshots,
            cleanup_old_files: self.cleanup_old_files || requested.cleanup_old_files,
        }
    }
}

/// Controller-owned bounded pause lease.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenancePause {
    /// Run identifier that owns the pause.
    pub run_id: String,
    /// Pause request timestamp.
    pub requested_at: Option<DateTime<Utc>>,
    /// Pause expiry timestamp.
    pub expires_at: DateTime<Utc>,
}

/// Replicator-owned request for a future maintenance run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceOperationRequest {
    /// Requested operations.
    pub operations: ExternalMaintenanceOperations,
    /// Inline flush threshold observed by the replicator.
    pub inline_flush_min_inlined_bytes: Option<u64>,
    /// Rewrite threshold observed by the replicator.
    pub rewrite_data_files_min_active_data_files: Option<i64>,
    /// Request timestamp.
    pub requested_at: DateTime<Utc>,
}

/// Replicator acknowledgement written to the coordination backend.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceReplicatorStatus {
    /// Replicator pause state.
    pub state: ExternalMaintenanceReplicatorState,
    /// Observed run identifier.
    pub observed_run_id: Option<String>,
    /// Timestamp at which foreground writes became quiesced.
    pub quiesced_at: Option<DateTime<Utc>>,
}

/// Replicator pause state.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ExternalMaintenanceReplicatorState {
    Running,
    Pausing,
    Quiesced,
}

impl ExternalMaintenanceReplicatorState {
    /// Returns the Kubernetes-compatible status string for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "Running",
            Self::Pausing => "Pausing",
            Self::Quiesced => "Quiesced",
        }
    }
}

impl From<&str> for ExternalMaintenanceReplicatorState {
    fn from(value: &str) -> Self {
        match value {
            "Pausing" => Self::Pausing,
            "Quiesced" => Self::Quiesced,
            _ => Self::Running,
        }
    }
}

impl ExternalMaintenanceReplicatorStatus {
    /// Returns the Kubernetes-compatible status string for this status.
    pub fn state_name(&self) -> &'static str {
        self.state.as_str()
    }

    /// Builds a status from a Kubernetes-compatible state string.
    pub fn from_state_name(
        state_name: &str,
        observed_run_id: Option<String>,
        quiesced_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self { state: state_name.into(), observed_run_id, quiesced_at }
    }
}

/// Last successful run history for each operation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceOperationHistory {
    /// Last successful inline flush run.
    pub inline_flush: Option<ExternalMaintenanceOperationRun>,
    /// Last successful merge-adjacent-files run.
    pub merge_adjacent_files: Option<ExternalMaintenanceOperationRun>,
    /// Last successful rewrite-data-files run.
    pub rewrite_data_files: Option<ExternalMaintenanceOperationRun>,
    /// Last successful expire-snapshots run.
    pub expire_snapshots: Option<ExternalMaintenanceOperationRun>,
    /// Last successful cleanup-old-files run.
    pub cleanup_old_files: Option<ExternalMaintenanceOperationRun>,
}

/// Last successful run metadata for one operation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceOperationRun {
    /// Run identifier.
    pub run_id: Option<String>,
    /// Completion timestamp.
    pub completed_at: DateTime<Utc>,
}

/// Backend-neutral operation enablement policy.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalMaintenanceOperationPolicy {
    /// Whether inline flush can be requested.
    pub inline_flush_enabled: bool,
    /// Whether merge-adjacent-files can be requested.
    pub merge_adjacent_files_enabled: bool,
    /// Whether rewrite-data-files can be requested.
    pub rewrite_data_files_enabled: bool,
    /// Whether expire-snapshots can be requested.
    pub expire_snapshots_enabled: bool,
    /// Whether cleanup-old-files can be requested.
    pub cleanup_old_files_enabled: bool,
}

impl Default for ExternalMaintenanceOperationPolicy {
    fn default() -> Self {
        Self {
            inline_flush_enabled: true,
            merge_adjacent_files_enabled: true,
            rewrite_data_files_enabled: true,
            expire_snapshots_enabled: false,
            cleanup_old_files_enabled: true,
        }
    }
}

/// Result of asking the coordination backend to create an operation request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalMaintenanceRequestOutcome {
    Created,
    AlreadyCovered,
    RejectedActiveRun,
    MissingState,
}

/// Runtime coordination backend used by the replicator.
#[async_trait]
pub trait ExternalMaintenanceStore: Clone + Send + Sync + 'static {
    /// Loads the current maintenance state.
    async fn load_state(&self) -> EtlResult<ExternalMaintenanceState>;

    /// Requests maintenance operations.
    async fn request_operations(
        &self,
        request: ExternalMaintenanceOperationRequest,
    ) -> EtlResult<ExternalMaintenanceRequestOutcome>;

    /// Reports the current replicator maintenance status.
    async fn report_replicator_status(
        &self,
        status: ExternalMaintenanceReplicatorStatus,
    ) -> EtlResult<()>;

    /// Clears the current replicator maintenance status.
    async fn clear_replicator_status(&self) -> EtlResult<()>;
}

/// Polling and threshold settings for an external maintenance watcher.
#[derive(Clone, Debug)]
pub struct ExternalMaintenanceWatcherConfig {
    /// Poll interval.
    pub poll_interval: Duration,
    /// Request cooldown after a completed run.
    pub request_cooldown: Duration,
    /// Coordination store operation timeout.
    pub store_timeout: Duration,
    /// Inline flush trigger threshold.
    pub inline_flush_min_inlined_bytes: u64,
    /// Rewrite trigger threshold.
    pub rewrite_data_files_min_active_data_files: i64,
}

impl Default for ExternalMaintenanceWatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(DEFAULT_POLL_SECONDS),
            request_cooldown: Duration::from_secs(DEFAULT_REQUEST_COOLDOWN_SECONDS),
            store_timeout: Duration::from_secs(DEFAULT_STORE_TIMEOUT_SECONDS),
            inline_flush_min_inlined_bytes: DEFAULT_INLINE_FLUSH_MIN_INLINED_BYTES,
            rewrite_data_files_min_active_data_files:
                DEFAULT_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES,
        }
    }
}

impl ExternalMaintenanceWatcherConfig {
    /// Builds watcher config from the Kubernetes-compatible environment.
    #[cfg(feature = "kubernetes")]
    pub fn from_env() -> Self {
        const POLL_SECONDS_ENV: &str = "ETL_DUCKLAKE_MAINTENANCE_POLL_SECONDS";
        const INLINE_FLUSH_MIN_INLINED_BYTES_ENV: &str =
            "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_INLINE_FLUSH_MIN_INLINED_BYTES";
        const REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES_ENV: &str =
            "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES";
        const REQUEST_COOLDOWN_SECONDS_ENV: &str =
            "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_REQUEST_COOLDOWN_SECONDS";
        const STORE_TIMEOUT_SECONDS_ENV: &str =
            "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_STORE_TIMEOUT_SECONDS";

        let poll_seconds = env_u64(POLL_SECONDS_ENV)
            .filter(|seconds| *seconds > 0)
            .unwrap_or(DEFAULT_POLL_SECONDS);
        let inline_flush_min_inlined_bytes = env_u64(INLINE_FLUSH_MIN_INLINED_BYTES_ENV)
            .unwrap_or(DEFAULT_INLINE_FLUSH_MIN_INLINED_BYTES);
        let rewrite_data_files_min_active_data_files =
            std::env::var(REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES_ENV)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(DEFAULT_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES);
        let request_cooldown =
            env_u64(REQUEST_COOLDOWN_SECONDS_ENV).unwrap_or(DEFAULT_REQUEST_COOLDOWN_SECONDS);
        let store_timeout = env_u64(STORE_TIMEOUT_SECONDS_ENV)
            .filter(|seconds| *seconds > 0)
            .unwrap_or(DEFAULT_STORE_TIMEOUT_SECONDS);

        Self {
            poll_interval: Duration::from_secs(poll_seconds),
            request_cooldown: Duration::from_secs(request_cooldown),
            store_timeout: Duration::from_secs(store_timeout),
            inline_flush_min_inlined_bytes,
            rewrite_data_files_min_active_data_files,
        }
    }
}

mod postgres;

pub use postgres::PostgresExternalMaintenanceStore;

#[cfg(feature = "kubernetes")]
mod kubernetes;

#[cfg(feature = "kubernetes")]
pub use kubernetes::KubernetesExternalMaintenanceStore;

#[cfg(feature = "kubernetes")]
fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|value| value.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeDelta;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Clone)]
    struct InMemoryExternalMaintenanceStore {
        state: Arc<Mutex<ExternalMaintenanceState>>,
    }

    impl InMemoryExternalMaintenanceStore {
        fn new(state: ExternalMaintenanceState) -> Self {
            Self { state: Arc::new(Mutex::new(state)) }
        }
    }

    #[async_trait]
    impl ExternalMaintenanceStore for InMemoryExternalMaintenanceStore {
        async fn load_state(&self) -> EtlResult<ExternalMaintenanceState> {
            Ok(self.state.lock().await.clone())
        }

        async fn request_operations(
            &self,
            request: ExternalMaintenanceOperationRequest,
        ) -> EtlResult<ExternalMaintenanceRequestOutcome> {
            let mut state = self.state.lock().await;
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

            let merged_request = if let Some(mut existing) = state.operation_request.take() {
                existing.operations = existing.operations.merge(request.operations);
                existing.inline_flush_min_inlined_bytes = request
                    .inline_flush_min_inlined_bytes
                    .or(existing.inline_flush_min_inlined_bytes);
                existing.rewrite_data_files_min_active_data_files = request
                    .rewrite_data_files_min_active_data_files
                    .or(existing.rewrite_data_files_min_active_data_files);
                existing.requested_at = request.requested_at;
                existing
            } else {
                request
            };
            state.operation_request = Some(merged_request);

            Ok(ExternalMaintenanceRequestOutcome::Created)
        }

        async fn report_replicator_status(
            &self,
            status: ExternalMaintenanceReplicatorStatus,
        ) -> EtlResult<()> {
            self.state.lock().await.replicator = Some(status);
            Ok(())
        }

        async fn clear_replicator_status(&self) -> EtlResult<()> {
            self.state.lock().await.replicator = None;
            Ok(())
        }
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

    #[test]
    fn operation_coverage_requires_every_requested_operation() {
        let existing = ExternalMaintenanceOperations {
            inline_flush: true,
            rewrite_data_files: true,
            ..ExternalMaintenanceOperations::default()
        };

        assert!(existing.covers(ExternalMaintenanceOperations {
            inline_flush: true,
            ..ExternalMaintenanceOperations::default()
        }));
        assert!(!existing.covers(ExternalMaintenanceOperations {
            inline_flush: true,
            expire_snapshots: true,
            ..ExternalMaintenanceOperations::default()
        }));
    }

    #[test]
    fn operation_merge_is_idempotent_union() {
        let existing = ExternalMaintenanceOperations {
            inline_flush: true,
            ..ExternalMaintenanceOperations::default()
        };
        let requested = ExternalMaintenanceOperations {
            rewrite_data_files: true,
            cleanup_old_files: true,
            ..ExternalMaintenanceOperations::default()
        };

        let merged = existing.merge(requested);

        assert!(merged.inline_flush);
        assert!(merged.rewrite_data_files);
        assert!(merged.cleanup_old_files);
        assert!(!merged.expire_snapshots);
    }

    #[test]
    fn replicator_state_round_trips_kubernetes_names() {
        assert_eq!(ExternalMaintenanceReplicatorState::Running.as_str(), "Running");
        assert_eq!(
            ExternalMaintenanceReplicatorState::from("Pausing"),
            ExternalMaintenanceReplicatorState::Pausing
        );
        assert_eq!(
            ExternalMaintenanceReplicatorState::from("Quiesced"),
            ExternalMaintenanceReplicatorState::Quiesced
        );
        assert_eq!(
            ExternalMaintenanceReplicatorState::from("unknown"),
            ExternalMaintenanceReplicatorState::Running
        );

        let status = ExternalMaintenanceReplicatorStatus {
            state: ExternalMaintenanceReplicatorState::Quiesced,
            observed_run_id: None,
            quiesced_at: None,
        };
        assert_eq!(status.state_name(), "Quiesced");

        let status = ExternalMaintenanceReplicatorStatus::from_state_name("Pausing", None, None);
        assert_eq!(status.state, ExternalMaintenanceReplicatorState::Pausing);
    }

    #[test]
    fn completed_run_cooldown_uses_last_completed_at() {
        let state = ExternalMaintenanceState {
            last_completed_at: Some(Utc::now() - TimeDelta::seconds(30)),
            ..ExternalMaintenanceState::default()
        };

        assert!(completed_run_in_cooldown_for_tests(&state, Duration::from_secs(60)));
        assert!(!completed_run_in_cooldown_for_tests(&state, Duration::from_secs(10)));
    }

    fn completed_run_in_cooldown_for_tests(
        state: &ExternalMaintenanceState,
        cooldown: Duration,
    ) -> bool {
        let Some(completed_at) = state.last_completed_at else {
            return false;
        };

        Utc::now()
            .signed_duration_since(completed_at)
            .to_std()
            .is_ok_and(|elapsed| elapsed < cooldown)
    }

    #[tokio::test]
    async fn mock_store_creates_and_merges_operation_requests() {
        let store = InMemoryExternalMaintenanceStore::new(ExternalMaintenanceState::present());

        let inline_flush = ExternalMaintenanceOperations {
            inline_flush: true,
            ..ExternalMaintenanceOperations::default()
        };
        let outcome = store.request_operations(operation_request(inline_flush)).await.unwrap();
        assert_eq!(outcome, ExternalMaintenanceRequestOutcome::Created);

        let outcome = store.request_operations(operation_request(inline_flush)).await.unwrap();
        assert_eq!(outcome, ExternalMaintenanceRequestOutcome::AlreadyCovered);

        let rewrite = ExternalMaintenanceOperations {
            rewrite_data_files: true,
            cleanup_old_files: true,
            ..ExternalMaintenanceOperations::default()
        };
        let outcome = store.request_operations(operation_request(rewrite)).await.unwrap();
        assert_eq!(outcome, ExternalMaintenanceRequestOutcome::Created);

        let state = store.load_state().await.unwrap();
        let operations = state.operation_request.unwrap().operations;
        assert!(operations.inline_flush);
        assert!(operations.rewrite_data_files);
        assert!(operations.cleanup_old_files);
        assert!(!operations.expire_snapshots);
    }

    #[tokio::test]
    async fn mock_store_rejects_requests_when_missing_or_running() {
        let missing_store =
            InMemoryExternalMaintenanceStore::new(ExternalMaintenanceState::default());
        let outcome = missing_store
            .request_operations(operation_request(ExternalMaintenanceOperations {
                inline_flush: true,
                ..ExternalMaintenanceOperations::default()
            }))
            .await
            .unwrap();
        assert_eq!(outcome, ExternalMaintenanceRequestOutcome::MissingState);

        let active_store = InMemoryExternalMaintenanceStore::new(ExternalMaintenanceState {
            exists: true,
            active_run: Some(ExternalMaintenanceRun {
                run_id: "run-1".to_owned(),
                started_at: Some(Utc::now()),
            }),
            ..ExternalMaintenanceState::default()
        });
        let outcome = active_store
            .request_operations(operation_request(ExternalMaintenanceOperations {
                rewrite_data_files: true,
                ..ExternalMaintenanceOperations::default()
            }))
            .await
            .unwrap();
        assert_eq!(outcome, ExternalMaintenanceRequestOutcome::RejectedActiveRun);
    }

    #[tokio::test]
    async fn mock_store_round_trips_replicator_status() {
        let store = InMemoryExternalMaintenanceStore::new(ExternalMaintenanceState::present());
        let quiesced_at = Utc::now();

        store
            .report_replicator_status(ExternalMaintenanceReplicatorStatus {
                state: ExternalMaintenanceReplicatorState::Quiesced,
                observed_run_id: Some("run-1".to_owned()),
                quiesced_at: Some(quiesced_at),
            })
            .await
            .unwrap();

        let status = store.load_state().await.unwrap().replicator.unwrap();
        assert_eq!(status.state, ExternalMaintenanceReplicatorState::Quiesced);
        assert_eq!(status.observed_run_id.as_deref(), Some("run-1"));
        assert_eq!(status.quiesced_at, Some(quiesced_at));

        store.clear_replicator_status().await.unwrap();
        assert!(store.load_state().await.unwrap().replicator.is_none());
    }
}
