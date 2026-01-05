//! Reconnection handling for PostgreSQL replication connections.
//!
//! Provides automatic reconnection with exponential backoff when
//! replication connections are interrupted. This module handles
//! transient network failures, database restarts, and other
//! recoverable disconnection scenarios.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use etl_config::shared::{PipelineConfig, ReconnectionConfig};
use rand::Rng;
use tokio_postgres::types::PgLsn;
use tracing::{error, info, warn};

use crate::concurrency::shutdown::ShutdownRx;
use crate::destination::Destination;
use crate::error::{EtlError, EtlResult};
use crate::replication::apply::{ApplyLoopHook, ApplyLoopResult, start_apply_loop};
use crate::replication::client::PgReplicationClient;
use crate::store::schema::SchemaStore;
use crate::types::PipelineId;

/// Current status of the reconnection state machine.
#[derive(Debug, Clone)]
pub enum ReconnectionStatus {
    /// Actively connected and streaming replication data.
    Connected,
    /// Attempting to reconnect after a connection failure.
    Reconnecting {
        /// Current reconnection attempt number (1-indexed).
        attempt: u32,
    },
    /// Terminal failure state - reconnection attempts exhausted or non-retryable error.
    Failed {
        /// Human-readable description of why reconnection failed.
        reason: String,
    },
}

/// Tracks the current state of reconnection attempts.
///
/// This struct maintains all state needed to implement exponential backoff
/// with jitter and enforce maximum retry duration limits.
#[derive(Debug)]
pub struct ReconnectionState {
    /// Current connection status.
    pub status: ReconnectionStatus,
    /// Number of reconnection attempts made.
    pub attempt_count: u32,
    /// When the first failure in the current sequence occurred.
    pub first_failure_time: Option<Instant>,
    /// Most recent error encountered during reconnection.
    pub last_error: Option<EtlError>,
    /// Last successfully confirmed LSN position (for logging/diagnostics).
    pub last_confirmed_lsn: Option<PgLsn>,
}

impl ReconnectionState {
    /// Creates a new reconnection state in the connected status.
    pub fn new() -> Self {
        Self {
            status: ReconnectionStatus::Connected,
            attempt_count: 0,
            first_failure_time: None,
            last_error: None,
            last_confirmed_lsn: None,
        }
    }

    /// Records a connection failure and transitions to reconnecting state.
    pub fn record_failure(&mut self, error: EtlError) {
        if self.first_failure_time.is_none() {
            self.first_failure_time = Some(Instant::now());
        }
        self.attempt_count += 1;
        self.last_error = Some(error);
        self.status = ReconnectionStatus::Reconnecting {
            attempt: self.attempt_count,
        };
    }

    /// Records a successful reconnection and resets state.
    pub fn record_success(&mut self) {
        self.status = ReconnectionStatus::Connected;
        self.attempt_count = 0;
        self.first_failure_time = None;
        self.last_error = None;
    }

    /// Marks reconnection as permanently failed.
    pub fn mark_failed(&mut self, reason: String) {
        self.status = ReconnectionStatus::Failed { reason };
    }

    /// Returns the elapsed time since the first failure, if any.
    pub fn elapsed_since_first_failure(&self) -> Option<Duration> {
        self.first_failure_time.map(|t| t.elapsed())
    }

    /// Updates the last confirmed LSN position.
    pub fn set_last_confirmed_lsn(&mut self, lsn: PgLsn) {
        self.last_confirmed_lsn = Some(lsn);
    }
}

impl Default for ReconnectionState {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages reconnection attempts with exponential backoff.
///
/// The `ReconnectionManager` encapsulates the logic for determining when
/// to retry, calculating backoff delays, and tracking retry state.
#[derive(Debug)]
pub struct ReconnectionManager {
    /// Configuration for reconnection behavior.
    config: ReconnectionConfig,
    /// Current reconnection state.
    state: ReconnectionState,
}

impl ReconnectionManager {
    /// Creates a new reconnection manager with the given configuration.
    pub fn new(config: ReconnectionConfig) -> Self {
        Self {
            config,
            state: ReconnectionState::new(),
        }
    }

    /// Returns a reference to the current reconnection state.
    pub fn state(&self) -> &ReconnectionState {
        &self.state
    }

    /// Returns a mutable reference to the current reconnection state.
    pub fn state_mut(&mut self) -> &mut ReconnectionState {
        &mut self.state
    }

    /// Returns a reference to the reconnection configuration.
    pub fn config(&self) -> &ReconnectionConfig {
        &self.config
    }

    /// Determines if reconnection should be attempted for the given error.
    ///
    /// Returns `true` if:
    /// - Reconnection is enabled in configuration
    /// - The error kind is retryable
    /// - Maximum retry duration has not been exceeded
    pub fn should_retry(&self, error: &EtlError) -> bool {
        if !self.config.enabled {
            return false;
        }

        if !error.kind().is_connection_retryable() {
            return false;
        }

        !self.is_exhausted()
    }

    /// Checks if the maximum retry duration has been exceeded.
    pub fn is_exhausted(&self) -> bool {
        if let Some(elapsed) = self.state.elapsed_since_first_failure() {
            elapsed >= self.config.max_retry_duration()
        } else {
            false
        }
    }

    /// Calculates the next backoff delay with jitter.
    ///
    /// Uses exponential backoff: delay = initial_delay * multiplier^attempt
    /// Adds random jitter of up to 30% to prevent thundering herd.
    /// Caps delay at max_retry_delay.
    pub fn calculate_backoff(&self) -> Duration {
        let attempt = self.state.attempt_count.saturating_sub(1);
        let multiplier = self.config.backoff_multiplier.powi(attempt as i32);
        let base_delay_ms = self.config.initial_retry_delay_ms as f64 * multiplier;

        // Cap at max delay
        let capped_delay_ms = base_delay_ms.min(self.config.max_retry_delay_ms as f64);

        // Add jitter: random factor between 0 and 0.3
        let jitter_factor = rand::rng().random::<f64>() * 0.3;
        let jittered_delay_ms = capped_delay_ms * (1.0 + jitter_factor);

        Duration::from_millis(jittered_delay_ms as u64)
    }

    /// Records a connection failure and updates internal state.
    pub fn record_failure(&mut self, error: EtlError) {
        self.state.record_failure(error);
    }

    /// Records a successful reconnection and resets retry state.
    pub fn record_success(&mut self) {
        self.state.record_success();
    }

    /// Marks reconnection as permanently failed.
    pub fn mark_failed(&mut self, reason: String) {
        self.state.mark_failed(reason);
    }

    /// Returns the total time spent attempting to reconnect.
    pub fn total_retry_duration(&self) -> Option<Duration> {
        self.state.elapsed_since_first_failure()
    }

    /// Returns the current attempt count.
    pub fn attempt_count(&self) -> u32 {
        self.state.attempt_count
    }
}

/// Wraps the apply loop with automatic reconnection on connection failures.
///
/// This function implements the reconnection logic for the apply loop. When
/// the inner apply loop fails with a retryable connection error, this wrapper
/// will attempt to reconnect using exponential backoff with jitter.
///
/// # Type Parameters
///
/// * `S` - Schema store type for caching table schemas
/// * `D` - Destination type for writing replicated data
/// * `H` - Hook type for customizing apply loop behavior
/// * `F` - Future type for the start LSN callback
/// * `G` - Function type for creating the hook
///
/// # Arguments
///
/// * `pipeline_id` - Unique identifier for this pipeline
/// * `config` - Pipeline configuration including reconnection settings
/// * `schema_store` - Store for caching table schemas
/// * `destination` - Destination for writing replicated data
/// * `shutdown_rx` - Receiver for shutdown signals
/// * `get_start_lsn` - Callback to get the starting LSN (called on each reconnect)
/// * `create_hook` - Factory function to create the apply loop hook
///
/// # Returns
///
/// Returns `Ok(ApplyLoopResult)` if the apply loop completes successfully or
/// is paused for shutdown. Returns `Err` if a non-retryable error occurs or
/// if reconnection attempts are exhausted.
pub async fn reconnection_loop<S, D, H, F, G>(
    pipeline_id: PipelineId,
    config: Arc<PipelineConfig>,
    schema_store: S,
    destination: D,
    shutdown_rx: ShutdownRx,
    get_start_lsn: impl Fn() -> F,
    create_hook: G,
) -> EtlResult<ApplyLoopResult>
where
    S: SchemaStore + Clone + Send + 'static,
    D: Destination + Clone + Send + 'static,
    H: ApplyLoopHook,
    F: Future<Output = EtlResult<(PgReplicationClient, PgLsn)>>,
    G: Fn() -> H,
{
    let mut manager = ReconnectionManager::new(config.reconnection.clone());

    loop {
        // Get a fresh connection and start LSN for each attempt
        let (replication_client, start_lsn) = match get_start_lsn().await {
            Ok((client, lsn)) => {
                // If we successfully connected after a failure, log success
                if manager.attempt_count() > 0 {
                    let downtime = manager.total_retry_duration().unwrap_or_default();
                    info!(
                        pipeline_id = %pipeline_id,
                        resume_lsn = %lsn,
                        total_downtime_ms = downtime.as_millis() as u64,
                        "reconnection successful, resuming replication"
                    );
                }
                manager.record_success();
                (client, lsn)
            }
            Err(err) => {
                // Connection failed during reconnection attempt
                if !manager.should_retry(&err) {
                    if !err.kind().is_connection_retryable() {
                        error!(
                            pipeline_id = %pipeline_id,
                            error = %err,
                            "non-retryable error during reconnection"
                        );
                    } else {
                        let duration = manager.total_retry_duration().unwrap_or_default();
                        error!(
                            pipeline_id = %pipeline_id,
                            attempts = manager.attempt_count(),
                            total_duration_ms = duration.as_millis() as u64,
                            "reconnection failed, max retries exceeded"
                        );
                    }
                    return Err(err);
                }

                manager.record_failure(err.clone());
                let delay = manager.calculate_backoff();

                warn!(
                    pipeline_id = %pipeline_id,
                    error = %err,
                    attempt = manager.attempt_count(),
                    delay_ms = delay.as_millis() as u64,
                    "connection failed, retrying after backoff"
                );

                tokio::time::sleep(delay).await;
                continue;
            }
        };

        // Create a fresh hook for each apply loop attempt
        let hook = create_hook();

        // Run the apply loop
        match start_apply_loop(
            pipeline_id,
            start_lsn,
            config.clone(),
            replication_client,
            schema_store.clone(),
            destination.clone(),
            hook,
            shutdown_rx.clone(),
        )
        .await
        {
            Ok(result) => {
                // Apply loop completed normally (paused or completed)
                return Ok(result);
            }
            Err(err) => {
                // Check if reconnection is enabled and error is retryable
                if !manager.should_retry(&err) {
                    if !err.kind().is_connection_retryable() {
                        error!(
                            pipeline_id = %pipeline_id,
                            error = %err,
                            "non-retryable error, stopping replication"
                        );
                    } else {
                        let duration = manager.total_retry_duration().unwrap_or_default();
                        error!(
                            pipeline_id = %pipeline_id,
                            attempts = manager.attempt_count(),
                            total_duration_ms = duration.as_millis() as u64,
                            "reconnection failed, max retries exceeded"
                        );
                    }
                    return Err(err);
                }

                // Record the failure and calculate backoff
                let last_lsn = manager.state().last_confirmed_lsn;
                manager.record_failure(err.clone());
                let delay = manager.calculate_backoff();

                warn!(
                    pipeline_id = %pipeline_id,
                    error = %err,
                    last_lsn = ?last_lsn,
                    attempt = manager.attempt_count(),
                    delay_ms = delay.as_millis() as u64,
                    "replication connection lost, initiating reconnection"
                );

                // Wait before attempting reconnection
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    fn test_config() -> ReconnectionConfig {
        ReconnectionConfig {
            enabled: true,
            initial_retry_delay_ms: 100,
            max_retry_delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_retry_duration_ms: 5000,
        }
    }

    #[test]
    fn test_reconnection_state_new() {
        let state = ReconnectionState::new();
        assert!(matches!(state.status, ReconnectionStatus::Connected));
        assert_eq!(state.attempt_count, 0);
        assert!(state.first_failure_time.is_none());
        assert!(state.last_error.is_none());
    }

    #[test]
    fn test_reconnection_state_record_failure() {
        let mut state = ReconnectionState::new();
        let error = EtlError::from((ErrorKind::SourceConnectionFailed, "test error"));

        state.record_failure(error);

        assert!(matches!(
            state.status,
            ReconnectionStatus::Reconnecting { attempt: 1 }
        ));
        assert_eq!(state.attempt_count, 1);
        assert!(state.first_failure_time.is_some());
        assert!(state.last_error.is_some());
    }

    #[test]
    fn test_reconnection_state_record_success() {
        let mut state = ReconnectionState::new();
        let error = EtlError::from((ErrorKind::SourceConnectionFailed, "test error"));

        state.record_failure(error);
        state.record_success();

        assert!(matches!(state.status, ReconnectionStatus::Connected));
        assert_eq!(state.attempt_count, 0);
        assert!(state.first_failure_time.is_none());
        assert!(state.last_error.is_none());
    }

    #[test]
    fn test_reconnection_manager_should_retry_retryable_error() {
        let manager = ReconnectionManager::new(test_config());
        let error = EtlError::from((ErrorKind::SourceConnectionFailed, "connection lost"));

        assert!(manager.should_retry(&error));
    }

    #[test]
    fn test_reconnection_manager_should_not_retry_non_retryable_error() {
        let manager = ReconnectionManager::new(test_config());
        let error = EtlError::from((ErrorKind::AuthenticationError, "invalid credentials"));

        assert!(!manager.should_retry(&error));
    }

    #[test]
    fn test_reconnection_manager_should_not_retry_when_disabled() {
        let config = ReconnectionConfig {
            enabled: false,
            ..test_config()
        };
        let manager = ReconnectionManager::new(config);
        let error = EtlError::from((ErrorKind::SourceConnectionFailed, "connection lost"));

        assert!(!manager.should_retry(&error));
    }

    #[test]
    fn test_reconnection_manager_calculate_backoff() {
        let mut manager = ReconnectionManager::new(test_config());

        // First attempt: base delay with jitter
        manager.record_failure(EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "test",
        )));
        let delay1 = manager.calculate_backoff();
        // Should be between 100ms and 130ms (100 * 1.0 to 100 * 1.3)
        assert!(delay1.as_millis() >= 100 && delay1.as_millis() <= 130);

        // Second attempt: 2x base delay with jitter
        manager.record_failure(EtlError::from((
            ErrorKind::SourceConnectionFailed,
            "test",
        )));
        let delay2 = manager.calculate_backoff();
        // Should be between 200ms and 260ms (200 * 1.0 to 200 * 1.3)
        assert!(delay2.as_millis() >= 200 && delay2.as_millis() <= 260);
    }

    #[test]
    fn test_reconnection_manager_calculate_backoff_caps_at_max() {
        let config = ReconnectionConfig {
            initial_retry_delay_ms: 500,
            max_retry_delay_ms: 1000,
            backoff_multiplier: 4.0,
            ..test_config()
        };
        let mut manager = ReconnectionManager::new(config);

        // After several attempts, delay should be capped
        for _ in 0..10 {
            manager.record_failure(EtlError::from((
                ErrorKind::SourceConnectionFailed,
                "test",
            )));
        }

        let delay = manager.calculate_backoff();
        // Should be capped at max_retry_delay (1000ms) plus up to 30% jitter
        assert!(delay.as_millis() <= 1300);
    }
}
