//! Heartbeat worker for maintaining replication slot activity.
//!
//! When replicating from a read replica (PostgreSQL 15+), the replication slot on the
//! primary can become inactive during idle periods. This worker periodically emits
//! `pg_logical_emit_message()` calls to the primary to keep the slot active.

use crate::concurrency::shutdown::ShutdownRx;
use crate::error::ErrorKind;
use crate::etl_error;
use crate::metrics::{
    ETL_HEARTBEAT_CONSECUTIVE_FAILURES, ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL,
    ETL_HEARTBEAT_EMISSIONS_TOTAL, ETL_HEARTBEAT_FAILURES_TOTAL,
    ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP, PIPELINE_ID_LABEL,
};
use crate::replication::client::PgReplicationClient;
use crate::types::PipelineId;
use crate::workers::base::WorkerHandle;
use etl_config::shared::{HeartbeatConfig, PgConnectionConfig};
use metrics::{counter, gauge};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Errors that can occur during heartbeat operations.
#[derive(Debug, Error)]
pub enum HeartbeatError {
    /// Failed to connect to the primary database.
    #[error("failed to connect to primary: {0}")]
    ConnectionFailed(String),

    /// Failed to emit heartbeat message.
    #[error("failed to emit heartbeat: {0}")]
    EmitFailed(String),

    /// The heartbeat worker was shut down.
    #[error("heartbeat worker shutdown")]
    Shutdown,
}

/// Handle to a running heartbeat worker.
///
/// Provides methods to wait for worker completion.
pub struct HeartbeatWorkerHandle {
    join_handle: Option<JoinHandle<()>>,
}

impl WorkerHandle<()> for HeartbeatWorkerHandle {
    /// Returns the state of the heartbeat worker (unit type as heartbeat has no state).
    fn state(&self) -> () {}

    /// Waits for the heartbeat worker to complete.
    ///
    /// Returns when the worker has shut down, either gracefully or due to an error.
    async fn wait(mut self) -> crate::error::EtlResult<()> {
        let Some(handle) = self.join_handle.take() else {
            return Ok(());
        };

        handle.await.map_err(|err| {
            etl_error!(
                ErrorKind::HeartbeatWorkerPanic,
                "Heartbeat worker panicked",
                err
            )
        })?;

        Ok(())
    }
}

/// Worker that maintains replication slot activity on the primary database.
///
/// When replicating from a read replica, the replication slot on the primary can become
/// inactive during idle periods since WAL is generated on the primary but consumed from
/// the replica. This worker periodically emits heartbeat messages to generate WAL activity.
pub struct HeartbeatWorker {
    pipeline_id: PipelineId,
    primary_config: PgConnectionConfig,
    heartbeat_config: HeartbeatConfig,
    shutdown_rx: ShutdownRx,
    consecutive_failures: u32,
}

impl HeartbeatWorker {
    /// Creates a new heartbeat worker.
    ///
    /// # Arguments
    ///
    /// * `pipeline_id` - The pipeline ID for metrics labeling.
    /// * `primary_config` - Connection configuration for the primary database.
    /// * `heartbeat_config` - Configuration for heartbeat timing and backoff.
    /// * `shutdown_rx` - Receiver for shutdown signals.
    pub fn new(
        pipeline_id: PipelineId,
        primary_config: PgConnectionConfig,
        heartbeat_config: HeartbeatConfig,
        shutdown_rx: ShutdownRx,
    ) -> Self {
        Self {
            pipeline_id,
            primary_config,
            heartbeat_config,
            shutdown_rx,
            consecutive_failures: 0,
        }
    }

    /// Starts the heartbeat worker in a background task.
    ///
    /// Returns a handle that can be used to wait for the worker to complete.
    pub fn start(self) -> HeartbeatWorkerHandle {
        let join_handle = tokio::spawn(async move {
            self.run().await;
        });

        HeartbeatWorkerHandle {
            join_handle: Some(join_handle),
        }
    }

    /// Main loop for the heartbeat worker.
    async fn run(mut self) {
        info!(
            pipeline_id = %self.pipeline_id,
            interval_ms = self.heartbeat_config.interval_ms,
            "starting heartbeat worker"
        );

        loop {
            // Check for shutdown before attempting connection.
            if self.is_shutdown_requested() {
                info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down");
                break;
            }

            // Attempt to connect and run heartbeat loop.
            match self.connect_and_heartbeat().await {
                Ok(()) => {
                    // Clean shutdown requested.
                    info!(pipeline_id = %self.pipeline_id, "heartbeat worker completed");
                    break;
                }
                Err(HeartbeatError::Shutdown) => {
                    info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down");
                    break;
                }
                Err(e) => {
                    self.consecutive_failures += 1;
                    self.record_failure();

                    warn!(
                        pipeline_id = %self.pipeline_id,
                        error = %e,
                        consecutive_failures = self.consecutive_failures,
                        "heartbeat error, will retry"
                    );

                    // Calculate backoff with jitter.
                    let backoff = self.calculate_backoff();

                    info!(
                        pipeline_id = %self.pipeline_id,
                        backoff_ms = backoff.as_millis(),
                        "waiting before retry"
                    );

                    // Wait for backoff duration or shutdown.
                    if self.wait_with_shutdown(backoff).await {
                        info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down during backoff");
                        break;
                    }
                }
            }
        }

        // Reset metrics on shutdown.
        gauge!(
            ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .set(0.0);
    }

    /// Connects to the primary and runs the heartbeat loop.
    async fn connect_and_heartbeat(&mut self) -> Result<(), HeartbeatError> {
        // Record connection attempt.
        counter!(
            ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .increment(1);

        info!(
            pipeline_id = %self.pipeline_id,
            host = %self.primary_config.host,
            port = self.primary_config.port,
            "connecting to primary for heartbeat"
        );

        // Connect in regular (non-replication) mode.
        let client = PgReplicationClient::connect_regular(self.primary_config.clone())
            .await
            .map_err(|e| HeartbeatError::ConnectionFailed(e.to_string()))?;

        info!(
            pipeline_id = %self.pipeline_id,
            "connected to primary, starting heartbeat loop"
        );

        // Reset consecutive failures on successful connection.
        self.consecutive_failures = 0;
        self.update_consecutive_failures_metric();

        // Run heartbeat loop.
        self.heartbeat_loop(&client).await
    }

    /// Runs the heartbeat emission loop.
    ///
    /// Emits an initial heartbeat immediately, then continues emitting at the configured interval.
    async fn heartbeat_loop(
        &mut self,
        client: &tokio_postgres::Client,
    ) -> Result<(), HeartbeatError> {
        let interval = Duration::from_millis(self.heartbeat_config.interval_ms);

        loop {
            // Check if connection is still alive before emitting.
            if client.is_closed() {
                return Err(HeartbeatError::ConnectionFailed(
                    "connection closed unexpectedly".to_string(),
                ));
            }

            // Emit heartbeat message.
            self.emit_heartbeat(client).await?;

            // Wait for interval or shutdown.
            if self.wait_with_shutdown(interval).await {
                return Ok(());
            }
        }
    }

    /// Emits a heartbeat message to the primary.
    ///
    /// Uses `pg_logical_emit_message()` to generate a WAL record that will flow
    /// through the replication chain without affecting any tables.
    async fn emit_heartbeat(&self, client: &tokio_postgres::Client) -> Result<(), HeartbeatError> {
        // Use non-transactional message (false) so it's immediately visible in WAL.
        // The 'etl_heartbeat' prefix identifies these messages.
        // Empty payload minimizes WAL size.
        let result = client
            .simple_query("SELECT pg_logical_emit_message(false, 'etl_heartbeat', '')")
            .await;

        match result {
            Ok(_) => {
                self.record_emission();
                Ok(())
            }
            Err(e) => {
                // Note: record_failure() is called by the caller in run() to avoid double counting
                Err(HeartbeatError::EmitFailed(e.to_string()))
            }
        }
    }

    /// Calculates the backoff duration with jitter.
    fn calculate_backoff(&self) -> Duration {
        let min_backoff = Duration::from_millis(self.heartbeat_config.min_backoff_ms);
        let max_backoff = Duration::from_millis(self.heartbeat_config.max_backoff_ms);

        // Exponential backoff: min * 2^failures, capped at max.
        let exp_backoff = min_backoff
            .saturating_mul(2u32.saturating_pow(self.consecutive_failures.saturating_sub(1)));
        let base_backoff = exp_backoff.min(max_backoff);

        // Add jitter to prevent thundering herd.
        self.apply_jitter(base_backoff)
    }

    /// Applies jitter to a duration.
    fn apply_jitter(&self, base_backoff: Duration) -> Duration {
        // Simple jitter using timestamp nanoseconds as pseudo-random source.
        let jitter_fraction = self.heartbeat_config.jitter_percent as f64 / 100.0;
        let jitter_range = base_backoff.as_secs_f64() * jitter_fraction;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();

        // subsec_nanos() returns 0..1_000_000_000, normalize to -1.0..1.0.
        let normalized = (nanos as f64 / 1_000_000_000.0) * 2.0 - 1.0;
        let jitter = normalized * jitter_range;

        let jittered_secs = (base_backoff.as_secs_f64() + jitter).max(0.1);
        Duration::from_secs_f64(jittered_secs)
    }

    /// Checks if shutdown has been requested.
    fn is_shutdown_requested(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

    /// Waits for a duration or until shutdown is requested.
    ///
    /// Returns `true` if shutdown was requested, `false` if the duration elapsed.
    async fn wait_with_shutdown(&mut self, duration: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(duration) => false,
            result = self.shutdown_rx.changed() => {
                // Channel closed or value changed to true.
                result.is_err() || *self.shutdown_rx.borrow()
            }
        }
    }

    /// Records a successful heartbeat emission in metrics.
    fn record_emission(&self) {
        counter!(
            ETL_HEARTBEAT_EMISSIONS_TOTAL,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .increment(1);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        gauge!(
            ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .set(timestamp);
    }

    /// Records a heartbeat failure in metrics.
    fn record_failure(&self) {
        counter!(
            ETL_HEARTBEAT_FAILURES_TOTAL,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .increment(1);

        self.update_consecutive_failures_metric();
    }

    /// Updates the consecutive failures gauge metric.
    fn update_consecutive_failures_metric(&self) {
        gauge!(
            ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .set(self.consecutive_failures as f64);
    }
}
