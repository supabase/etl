//! Heartbeat worker for maintaining replication slot activity on read replicas.
//!
//! When replicating from a read replica, the replication slot exists on the primary
//! database but the ETL pipeline connects to the replica for WAL streaming. Without
//! activity on the primary's slot, PostgreSQL may invalidate it. The heartbeat worker
//! prevents this by periodically calling `pg_logical_emit_message()` on the primary.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     WAL Stream      ┌─────────────┐
//! │   Primary   │ ──────────────────► │   Replica   │
//! │  Database   │                     │  Database   │
//! └─────────────┘                     └─────────────┘
//!       ▲                                   │
//!       │ Heartbeat                         │ ETL Pipeline
//!       │ (pg_logical_emit_message)         │ reads from
//!       │                                   ▼
//! ┌─────────────┐                     ┌─────────────┐
//! │  Heartbeat  │                     │    Apply    │
//! │   Worker    │                     │   Worker    │
//! └─────────────┘                     └─────────────┘
//! ```
//!
//! # PostgreSQL Version Requirements
//!
//! The `pg_logical_emit_message()` function requires PostgreSQL 15 or later.
//! Earlier versions will cause the heartbeat worker to fail during startup.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{counter, gauge};
use rand::Rng;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio_postgres::Client;
use tracing::{debug, error, info, warn};

use crate::concurrency::shutdown::ShutdownRx;
use crate::metrics::{
    ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL, ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
    ETL_HEARTBEAT_EMISSIONS_TOTAL, ETL_HEARTBEAT_FAILURES_TOTAL,
    ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP, PIPELINE_ID_LABEL,
};
use crate::replication::client::PgReplicationClient;
use crate::types::PipelineId;
use etl_config::shared::{HeartbeatConfig, PgConnectionConfig};

/// Minimum PostgreSQL version required for `pg_logical_emit_message()`.
/// Format: major * 10000 + minor * 100 (e.g., 15.0.0 = 150000)
const MIN_PG_VERSION: i32 = 15_00_00;

/// Errors that can occur during heartbeat operations.
#[derive(Debug, Error)]
pub enum HeartbeatError {
    /// Failed to connect to the primary database.
    #[error("failed to connect to primary: {0}")]
    ConnectionFailed(String),

    /// PostgreSQL version is too old for heartbeat functionality.
    #[error("PostgreSQL version {0} is below minimum required version 15")]
    UnsupportedVersion(String),

    /// Failed to emit heartbeat message.
    #[error("failed to emit heartbeat: {0}")]
    EmitFailed(String),

    /// Failed to query PostgreSQL version.
    #[error("failed to query PostgreSQL version: {0}")]
    VersionQueryFailed(String),
}

/// Connection state for the heartbeat worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Not connected to the primary database.
    Disconnected,
    /// Attempting to establish a connection.
    Connecting,
    /// Connected and actively sending heartbeats.
    Connected,
}

impl ConnectionState {
    fn as_str(&self) -> &'static str {
        match self {
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Connecting => "connecting",
            ConnectionState::Connected => "connected",
        }
    }
}

/// Handle to a running heartbeat worker.
///
/// Provides methods to monitor the worker's state and wait for completion.
#[derive(Debug)]
pub struct HeartbeatWorkerHandle {
    join_handle: JoinHandle<Result<(), HeartbeatError>>,
}

impl HeartbeatWorkerHandle {
    /// Waits for the heartbeat worker to complete.
    ///
    /// Returns `Ok(())` if the worker shut down gracefully, or an error if it failed.
    pub async fn wait(self) -> Result<(), HeartbeatError> {
        match self.join_handle.await {
            Ok(result) => result,
            Err(e) => {
                error!(error = %e, "heartbeat worker task panicked");
                Err(HeartbeatError::EmitFailed(format!("worker panicked: {e}")))
            }
        }
    }
}

/// Worker that maintains replication slot activity by sending heartbeats to the primary.
///
/// The heartbeat worker connects to the primary database and periodically calls
/// `pg_logical_emit_message()` to generate WAL activity. This prevents the replication
/// slot from being invalidated due to inactivity when replicating from a read replica.
pub struct HeartbeatWorker {
    pipeline_id: PipelineId,
    primary_config: PgConnectionConfig,
    heartbeat_config: HeartbeatConfig,
    shutdown_rx: ShutdownRx,
    state_tx: watch::Sender<ConnectionState>,
    consecutive_failures: u32,
}

impl HeartbeatWorker {
    /// Creates a new heartbeat worker.
    ///
    /// # Arguments
    ///
    /// * `pipeline_id` - Pipeline identifier for metrics labeling
    /// * `primary_config` - Connection configuration for the primary database
    /// * `heartbeat_config` - Heartbeat timing and retry configuration
    /// * `shutdown_rx` - Receiver for shutdown signals
    pub fn new(
        pipeline_id: PipelineId,
        primary_config: PgConnectionConfig,
        heartbeat_config: HeartbeatConfig,
        shutdown_rx: ShutdownRx,
    ) -> Self {
        let (state_tx, _) = watch::channel(ConnectionState::Disconnected);

        Self {
            pipeline_id,
            primary_config,
            heartbeat_config,
            shutdown_rx,
            state_tx,
            consecutive_failures: 0,
        }
    }

    /// Starts the heartbeat worker in a background task.
    ///
    /// Returns a handle that can be used to wait for the worker to complete.
    pub fn start(self) -> HeartbeatWorkerHandle {
        let join_handle = tokio::spawn(self.run());
        HeartbeatWorkerHandle { join_handle }
    }

    /// Main worker loop that manages connection and heartbeat emission.
    async fn run(mut self) -> Result<(), HeartbeatError> {
        info!(
            pipeline_id = %self.pipeline_id,
            interval_secs = %self.heartbeat_config.interval_secs,
            "starting heartbeat worker"
        );

        let mut current_backoff = Duration::from_secs(self.heartbeat_config.initial_backoff_secs);

        loop {
            // Check for shutdown before attempting connection
            if self.shutdown_rx.is_shutdown() {
                info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down");
                return Ok(());
            }

            // Attempt to connect and run heartbeat loop
            match self.connect_and_heartbeat().await {
                Ok(()) => {
                    // Graceful shutdown requested
                    return Ok(());
                }
                Err(e) => {
                    self.consecutive_failures += 1;
                    self.update_state(ConnectionState::Disconnected);

                    gauge!(
                        ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
                        PIPELINE_ID_LABEL => self.pipeline_id.to_string()
                    )
                    .set(self.consecutive_failures as f64);

                    counter!(
                        ETL_HEARTBEAT_FAILURES_TOTAL,
                        PIPELINE_ID_LABEL => self.pipeline_id.to_string()
                    )
                    .increment(1);

                    warn!(
                        pipeline_id = %self.pipeline_id,
                        error = %e,
                        consecutive_failures = %self.consecutive_failures,
                        backoff_secs = %current_backoff.as_secs(),
                        "heartbeat failed, will retry after backoff"
                    );

                    // Wait with backoff before retrying
                    let jittered_backoff = self.calculate_backoff(current_backoff);

                    tokio::select! {
                        _ = sleep(jittered_backoff) => {},
                        _ = self.shutdown_rx.wait_for_shutdown() => {
                            info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down during backoff");
                            return Ok(());
                        }
                    }

                    // Increase backoff for next failure (exponential)
                    let max_backoff = Duration::from_secs(self.heartbeat_config.max_backoff_secs);
                    current_backoff = (current_backoff * 2).min(max_backoff);
                }
            }
        }
    }

    /// Connects to the primary and runs the heartbeat emission loop.
    ///
    /// Returns `Ok(())` when shutdown is requested, or an error if connection/emission fails.
    async fn connect_and_heartbeat(&mut self) -> Result<(), HeartbeatError> {
        self.update_state(ConnectionState::Connecting);

        counter!(
            ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .increment(1);

        debug!(pipeline_id = %self.pipeline_id, "connecting to primary for heartbeat");

        // Connect to the primary database
        let client = self.connect_to_primary().await?;

        // Verify PostgreSQL version supports pg_logical_emit_message
        self.verify_pg_version(&client).await?;

        self.update_state(ConnectionState::Connected);
        info!(pipeline_id = %self.pipeline_id, "connected to primary, starting heartbeat loop");

        // Reset backoff and failure count on successful connection
        self.consecutive_failures = 0;
        gauge!(
            ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        )
        .set(0.0);

        // Run heartbeat emission loop
        self.heartbeat_loop(&client).await
    }

    /// Connects to the primary database using the configured connection settings.
    async fn connect_to_primary(&self) -> Result<Client, HeartbeatError> {
        let client = PgReplicationClient::connect_regular(self.primary_config.clone())
            .await
            .map_err(|e| HeartbeatError::ConnectionFailed(e.to_string()))?;

        Ok(client)
    }

    /// Verifies that the PostgreSQL version supports `pg_logical_emit_message()`.
    async fn verify_pg_version(&self, client: &Client) -> Result<(), HeartbeatError> {
        let row = client
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .await
            .map_err(|e| HeartbeatError::VersionQueryFailed(e.to_string()))?;

        let version: i32 = row.get(0);

        if version < MIN_PG_VERSION {
            let version_str = format!(
                "{}.{}.{}",
                version / 10000,
                (version / 100) % 100,
                version % 100
            );
            return Err(HeartbeatError::UnsupportedVersion(version_str));
        }

        debug!(
            pipeline_id = %self.pipeline_id,
            pg_version = %version,
            "PostgreSQL version verified for heartbeat support"
        );

        Ok(())
    }

    /// Runs the heartbeat emission loop until shutdown or error.
    async fn heartbeat_loop(&self, client: &Client) -> Result<(), HeartbeatError> {
        let mut heartbeat_interval =
            interval(Duration::from_secs(self.heartbeat_config.interval_secs));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    self.emit_heartbeat(client).await?;
                }
                _ = self.shutdown_rx.wait_for_shutdown() => {
                    info!(pipeline_id = %self.pipeline_id, "heartbeat loop received shutdown signal");
                    return Ok(());
                }
            }
        }
    }

    /// Emits a heartbeat message using `pg_logical_emit_message()`.
    ///
    /// This function calls the PostgreSQL function with:
    /// - `transactional = false`: Message is written immediately without waiting for commit
    /// - `prefix = 'etl_heartbeat'`: Identifies the message source
    /// - `content = ''`: Empty content since we only need WAL activity
    async fn emit_heartbeat(&self, client: &Client) -> Result<(), HeartbeatError> {
        client
            .execute(
                "SELECT pg_logical_emit_message(false, 'etl_heartbeat', '')",
                &[],
            )
            .await
            .map_err(|e| HeartbeatError::EmitFailed(e.to_string()))?;

        // Update metrics
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

        debug!(pipeline_id = %self.pipeline_id, "heartbeat emitted successfully");

        Ok(())
    }

    /// Calculates backoff duration with jitter.
    ///
    /// Applies random jitter (±jitter_percent%) to prevent thundering herd
    /// when multiple workers reconnect simultaneously.
    fn calculate_backoff(&self, base_backoff: Duration) -> Duration {
        let jitter_fraction = self.heartbeat_config.jitter_percent as f64 / 100.0;
        let jitter_range = base_backoff.as_secs_f64() * jitter_fraction;

        let mut rng = rand::rng();
        let jitter = rng.random_range(-jitter_range..=jitter_range);

        let jittered_secs = (base_backoff.as_secs_f64() + jitter).max(0.1);
        Duration::from_secs_f64(jittered_secs)
    }

    /// Updates the connection state and notifies any watchers.
    fn update_state(&self, state: ConnectionState) {
        let _ = self.state_tx.send(state);
        debug!(
            pipeline_id = %self.pipeline_id,
            state = %state.as_str(),
            "heartbeat worker state changed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_as_str() {
        assert_eq!(ConnectionState::Disconnected.as_str(), "disconnected");
        assert_eq!(ConnectionState::Connecting.as_str(), "connecting");
        assert_eq!(ConnectionState::Connected.as_str(), "connected");
    }

    #[test]
    fn test_min_pg_version_constant() {
        // PostgreSQL 15.0.0 = 150000
        assert_eq!(MIN_PG_VERSION, 150000);
    }

    #[test]
    fn test_calculate_backoff_within_bounds() {
        use crate::concurrency::shutdown::create_shutdown_channel;

        let config = HeartbeatConfig {
            interval_secs: 30,
            initial_backoff_secs: 1,
            max_backoff_secs: 60,
            jitter_percent: 25,
        };

        let pg_config = PgConnectionConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "test".to_string(),
            username: "test".to_string(),
            password: None,
            tls: etl_config::shared::TlsConfig::disabled(),
            keepalive: None,
        };

        let (shutdown_tx, _) = create_shutdown_channel();
        let worker = HeartbeatWorker::new(1, pg_config, config, shutdown_tx.subscribe());

        let base = Duration::from_secs(10);
        for _ in 0..100 {
            let backoff = worker.calculate_backoff(base);
            // With 25% jitter, should be between 7.5 and 12.5 seconds
            assert!(backoff >= Duration::from_secs_f64(7.5));
            assert!(backoff <= Duration::from_secs_f64(12.5));
        }
    }
}
