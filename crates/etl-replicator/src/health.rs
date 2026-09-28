//! Read-only HTTP probes of pipeline lifecycle and worker activity.

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use etl::activity::{self, ActivityKind};
use etl_config::shared::ReplicatorHealthConfig;

use crate::core::ReplicatorState;

/// Activity verdict, keeping absence of observations distinct from healthy
/// work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActivityStatus {
    /// There is currently no active work to evaluate.
    Unobserved,
    /// Every monitored operation is within its inactivity allowance.
    Active,
    /// At least one monitored operation exceeded its inactivity allowance.
    Inactive,
}

impl ActivityStatus {
    /// Evaluates each operation independently, including its keepalive
    /// allowance.
    fn evaluate(
        activities: impl IntoIterator<Item = (Duration, ActivityKind)>,
        stall_timeout: Duration,
    ) -> Self {
        let mut status = Self::Unobserved;

        for (inactive_for, kind) in activities {
            let allowance = match kind {
                // Quiet sources need a full idle window for keepalive processing.
                ActivityKind::WalApply { wal_sender_timeout } => {
                    stall_timeout.max(wal_sender_timeout)
                }
                ActivityKind::InitialTableCopy => stall_timeout,
            };

            if inactive_for >= allowance {
                return Self::Inactive;
            }

            status = Self::Active;
        }

        status
    }

    /// Returns whether Kubernetes should allow this process to keep running.
    ///
    /// Repeated liveness failures cause Kubernetes to restart the container.
    /// We fail only when monitored work exceeds its inactivity allowance;
    /// initialization and intentional waits without observations remain live.
    /// An idle source remains live through completed keepalive processing.
    ///
    /// Draining stays live even if activity is stale, so shutdown itself does
    /// not request another restart. Kubernetes still enforces the termination
    /// grace period. This observation never initiates shutdown itself.
    fn liveness(self, replicator_state: ReplicatorState) -> (StatusCode, &'static str) {
        match (replicator_state, self) {
            (ReplicatorState::Stopping, _) => (StatusCode::OK, "ok"),
            (_, Self::Inactive) => (StatusCode::SERVICE_UNAVAILABLE, "inactive"),
            (_, Self::Unobserved | Self::Active) => (StatusCode::OK, "ok"),
        }
    }

    /// Returns whether Kubernetes should mark this pipeline ready.
    ///
    /// Readiness failure marks the Pod unready and removes it from normal
    /// Service routing; it does not restart the container or pause replication.
    /// For this background worker, ready means the pipeline has started, at
    /// least one operation is monitored, and none has exceeded its inactivity
    /// allowance. Initial sync can still be running; ready does not mean caught
    /// up or that all accepted writes are durable.
    ///
    /// Initialization, absence of observations, and graceful shutdown are
    /// unready. Neither probe performs source or destination network checks.
    fn readiness(self, replicator_state: ReplicatorState) -> (StatusCode, &'static str) {
        match (replicator_state, self) {
            (ReplicatorState::Stopping, _) => (StatusCode::SERVICE_UNAVAILABLE, "stopping"),
            (_, Self::Inactive) => (StatusCode::SERVICE_UNAVAILABLE, "inactive"),
            (ReplicatorState::Initializing, _) | (_, Self::Unobserved) => {
                (StatusCode::SERVICE_UNAVAILABLE, "initializing")
            }
            (ReplicatorState::Running, Self::Active) => (StatusCode::OK, "ok"),
        }
    }
}

/// Inputs observed by the HTTP probes; only the pipeline runner can change
/// lifecycle state.
#[derive(Clone)]
pub(crate) struct ReplicatorHealth {
    /// Replicator lifecycle shared by the runner and probe handlers.
    replicator_state: Arc<Mutex<ReplicatorState>>,
    /// Configured minimum inactivity allowance.
    stall_timeout: Duration,
}

impl ReplicatorHealth {
    /// Creates shared probe state before pipeline initialization starts.
    pub(crate) fn new(health_config: ReplicatorHealthConfig) -> Self {
        Self {
            replicator_state: Arc::new(Mutex::new(ReplicatorState::Initializing)),
            stall_timeout: Duration::from_millis(health_config.stall_timeout_ms),
        }
    }

    /// Updates the lifecycle observed by subsequent probe requests.
    pub(crate) fn set_replicator_state(&self, replicator_state: ReplicatorState) {
        *self.replicator_state.lock().unwrap_or_else(PoisonError::into_inner) = replicator_state;
    }

    /// Copies the lifecycle without holding the lock during activity
    /// collection.
    fn replicator_state(&self) -> ReplicatorState {
        *self.replicator_state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads approximate in-memory activity without querying either dependency.
    fn activity_status(&self) -> ActivityStatus {
        let now = Instant::now();
        ActivityStatus::evaluate(
            activity::snapshot().iter().map(|entry| (entry.inactive_for(now), entry.kind())),
            self.stall_timeout,
        )
    }
}

/// Returns liveness, allowing initialization, slot acquisition, and draining.
async fn live(State(replicator_health): State<ReplicatorHealth>) -> (StatusCode, &'static str) {
    replicator_health.activity_status().liveness(replicator_health.replicator_state())
}

/// Returns readiness from pipeline lifecycle and currently monitored activity.
async fn ready(State(replicator_health): State<ReplicatorHealth>) -> (StatusCode, &'static str) {
    replicator_health.activity_status().readiness(replicator_health.replicator_state())
}

/// Builds read-only probes from the pipeline runner's lifecycle observations.
pub(crate) fn router(replicator_health: ReplicatorHealth) -> Router {
    Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .with_state(replicator_health)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::StatusCode;
    use etl::activity::ActivityKind;

    use crate::{core::ReplicatorState, health::ActivityStatus};

    /// A stalled operation fails health without masking other timeout policies.
    #[test]
    fn inactivity_boundaries_and_independent_workers() {
        let stall_timeout = Duration::from_secs(600);
        let copy = ActivityKind::InitialTableCopy;
        assert_eq!(ActivityStatus::evaluate([], stall_timeout), ActivityStatus::Unobserved);
        assert_eq!(
            ActivityStatus::evaluate([(Duration::from_secs(599), copy)], stall_timeout),
            ActivityStatus::Active
        );
        assert_eq!(
            ActivityStatus::evaluate(
                [(Duration::ZERO, copy), (stall_timeout, copy)],
                stall_timeout
            ),
            ActivityStatus::Inactive
        );
        let apply = ActivityKind::WalApply { wal_sender_timeout: Duration::from_secs(800) };
        assert_eq!(
            ActivityStatus::evaluate([(stall_timeout, apply)], stall_timeout),
            ActivityStatus::Active
        );
        assert_eq!(
            ActivityStatus::evaluate([(Duration::from_secs(800), apply)], stall_timeout),
            ActivityStatus::Inactive
        );
    }

    /// Lifecycle and activity jointly determine both probe responses.
    #[test]
    fn probes_follow_replicator_lifecycle_and_activity() {
        let ok = (StatusCode::OK, "ok");
        let initializing = (StatusCode::SERVICE_UNAVAILABLE, "initializing");
        let inactive = (StatusCode::SERVICE_UNAVAILABLE, "inactive");
        let stopping = (StatusCode::SERVICE_UNAVAILABLE, "stopping");

        for (replicator_state, activity_status, expected_live, expected_ready) in [
            (ReplicatorState::Initializing, ActivityStatus::Unobserved, ok, initializing),
            (ReplicatorState::Initializing, ActivityStatus::Active, ok, initializing),
            (ReplicatorState::Initializing, ActivityStatus::Inactive, inactive, inactive),
            (ReplicatorState::Running, ActivityStatus::Unobserved, ok, initializing),
            (ReplicatorState::Running, ActivityStatus::Active, ok, ok),
            (ReplicatorState::Running, ActivityStatus::Inactive, inactive, inactive),
            (ReplicatorState::Stopping, ActivityStatus::Unobserved, ok, stopping),
            (ReplicatorState::Stopping, ActivityStatus::Active, ok, stopping),
            (ReplicatorState::Stopping, ActivityStatus::Inactive, ok, stopping),
        ] {
            assert_eq!(activity_status.liveness(replicator_state), expected_live);
            assert_eq!(activity_status.readiness(replicator_state), expected_ready);
        }
    }
}
