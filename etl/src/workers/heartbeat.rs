//! Heartbeat worker for maintaining replication slot activity on read replicas.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{counter, gauge};
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

const MIN_PG_VERSION: i32 = 15_00_00;

/// Errors that can occur during heartbeat operations.
#[derive(Debug, Error)]
pub enum HeartbeatError {
    /// Failed to establish connection to the primary database.
    #[error("failed to connect to primary: {0}")]
    ConnectionFailed(String),
    /// The primary database version is below the minimum required (PG 15+).
    #[error("PostgreSQL version {0} is below minimum required version 15")]
    UnsupportedVersion(String),
    /// Failed to emit a heartbeat message.
    #[error("failed to emit heartbeat: {0}")]
    EmitFailed(String),
    /// Failed to query the PostgreSQL server version.
    #[error("failed to query PostgreSQL version: {0}")]
    VersionQueryFailed(String),
}

/// Tracks the heartbeat worker's connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Not connected to the primary database.
    Disconnected,
    /// Connection attempt in progress.
    Connecting,
    /// Successfully connected and heartbeating.
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

/// Handle returned by [`HeartbeatWorker::start`] for awaiting worker completion.
#[derive(Debug)]
pub struct HeartbeatWorkerHandle {
    join_handle: JoinHandle<Result<(), HeartbeatError>>,
}

impl HeartbeatWorkerHandle {
    /// Waits for the heartbeat worker to complete.
    ///
    /// Returns `Ok(())` on graceful shutdown or an error if the worker failed.
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

/// Worker that emits periodic heartbeats to the primary database.
///
/// When replicating from a read replica, this worker maintains the replication
/// slot's activity by emitting `pg_logical_emit_message` calls to the primary.
/// This ensures WAL flows through the replication chain even during idle periods.
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
    /// * `pipeline_id` - The pipeline identifier for metrics labeling
    /// * `primary_config` - Connection configuration for the primary database
    /// * `heartbeat_config` - Heartbeat interval and backoff settings
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
    /// Returns a handle that can be used to await the worker's completion.
    pub fn start(self) -> HeartbeatWorkerHandle {
        let join_handle = tokio::spawn(self.run());
        HeartbeatWorkerHandle { join_handle }
    }

    async fn run(mut self) -> Result<(), HeartbeatError> {
        info!(
            pipeline_id = %self.pipeline_id,
            interval_secs = %self.heartbeat_config.interval_secs,
            "starting heartbeat worker"
        );

        let mut current_backoff = Duration::from_secs(self.heartbeat_config.initial_backoff_secs);

        loop {
            if self.shutdown_rx.has_changed().unwrap_or(false) {
                info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down");
                return Ok(());
            }

            match self.connect_and_heartbeat().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    self.consecutive_failures += 1;
                    self.update_state(ConnectionState::Disconnected);

                    gauge!(
                        ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
                        PIPELINE_ID_LABEL => self.pipeline_id.to_string()
                    ).set(self.consecutive_failures as f64);

                    counter!(
                        ETL_HEARTBEAT_FAILURES_TOTAL,
                        PIPELINE_ID_LABEL => self.pipeline_id.to_string()
                    ).increment(1);

                    warn!(
                        pipeline_id = %self.pipeline_id,
                        error = %e,
                        consecutive_failures = %self.consecutive_failures,
                        backoff_secs = %current_backoff.as_secs(),
                        "heartbeat failed, will retry after backoff"
                    );

                    let jittered_backoff = self.calculate_backoff(current_backoff);

                    tokio::select! {
                        _ = sleep(jittered_backoff) => {},
                        _ = self.shutdown_rx.changed() => {
                            info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down during backoff");
                            return Ok(());
                        }
                    }

                    let max_backoff = Duration::from_secs(self.heartbeat_config.max_backoff_secs);
                    current_backoff = (current_backoff * 2).min(max_backoff);
                }
            }
        }
    }

    async fn connect_and_heartbeat(&mut self) -> Result<(), HeartbeatError> {
        self.update_state(ConnectionState::Connecting);

        counter!(
            ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        ).increment(1);

        debug!(pipeline_id = %self.pipeline_id, "connecting to primary for heartbeat");

        let client = self.connect_to_primary().await?;
        self.verify_pg_version(&client).await?;

        self.update_state(ConnectionState::Connected);
        info!(pipeline_id = %self.pipeline_id, "connected to primary, starting heartbeat loop");

        self.consecutive_failures = 0;
        gauge!(
            ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        ).set(0.0);

        self.heartbeat_loop(&client).await
    }

    async fn connect_to_primary(&self) -> Result<Client, HeartbeatError> {
        PgReplicationClient::connect_regular(self.primary_config.clone())
            .await
            .map_err(|e| HeartbeatError::ConnectionFailed(e.to_string()))
    }

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

        debug!(pipeline_id = %self.pipeline_id, pg_version = %version, "PostgreSQL version verified");
        Ok(())
    }

    async fn heartbeat_loop(&mut self, client: &Client) -> Result<(), HeartbeatError> {
        let mut heartbeat_interval = interval(Duration::from_secs(self.heartbeat_config.interval_secs));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    self.emit_heartbeat(client).await?;
                }
                _ = self.shutdown_rx.changed() => {
                    info!(pipeline_id = %self.pipeline_id, "heartbeat loop received shutdown signal");
                    return Ok(());
                }
            }
        }
    }

    async fn emit_heartbeat(&self, client: &Client) -> Result<(), HeartbeatError> {
        client
            .execute("SELECT pg_logical_emit_message(false, 'etl_heartbeat', '')", &[])
            .await
            .map_err(|e| HeartbeatError::EmitFailed(e.to_string()))?;

        counter!(
            ETL_HEARTBEAT_EMISSIONS_TOTAL,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        ).increment(1);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        gauge!(
            ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP,
            PIPELINE_ID_LABEL => self.pipeline_id.to_string()
        ).set(timestamp);

        debug!(pipeline_id = %self.pipeline_id, "heartbeat emitted successfully");
        Ok(())
    }

    fn calculate_backoff(&self, base_backoff: Duration) -> Duration {
        // Simple jitter using timestamp nanoseconds as pseudo-random source.
        let jitter_fraction = self.heartbeat_config.jitter_percent as f64 / 100.0;
        let jitter_range = base_backoff.as_secs_f64() * jitter_fraction;

        // Use nanoseconds from current time as a simple source of variation.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        // subsec_nanos() returns 0..1_000_000_000, normalize to -1.0..1.0
        let normalized = (nanos as f64 / 1_000_000_000.0) * 2.0 - 1.0;
        let jitter = normalized * jitter_range;

        let jittered_secs = (base_backoff.as_secs_f64() + jitter).max(0.1);
        Duration::from_secs_f64(jittered_secs)
    }

    fn update_state(&self, state: ConnectionState) {
        let _ = self.state_tx.send(state);
        debug!(pipeline_id = %self.pipeline_id, state = %state.as_str(), "heartbeat worker state changed");
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
        assert_eq!(MIN_PG_VERSION, 150000);
    }
}
