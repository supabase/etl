//! Heartbeat worker for read replica support.
//!
//! When ETL pipelines connect to a read replica, the replication slot on the replica
//! can become invalidated if the primary database recycles WAL segments before the
//! replica consumes them. The heartbeat worker prevents this by periodically emitting
//! messages to the primary database using `pg_logical_emit_message()`.
//!
//! # How It Works
//!
//! 1. The worker maintains a connection to the **primary** database (not the replica)
//! 2. Periodically calls `pg_logical_emit_message(false, 'etl_heartbeat', '')`
//! 3. This generates WAL activity that flows to replicas
//! 4. The WAL activity prevents the primary from recycling segments the replica needs
//!
//! # PostgreSQL Version Requirements
//!
//! - **PostgreSQL 14**: `pg_logical_emit_message()` requires superuser privileges
//! - **PostgreSQL 15+**: Function available to users with `pg_write_server_files` role
//!
//! This worker requires PostgreSQL 15 or later to function properly.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etl_config::shared::{ETL_HEARTBEAT_OPTIONS, HeartbeatConfig, IntoConnectOptions, PgConnectionConfig};
use metrics::{counter, gauge};
use rand::Rng;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use tracing::{debug, error, info, warn};

use crate::concurrency::shutdown::ShutdownRx;
use crate::error::{EtlError, EtlResult};
use crate::metrics::{
    ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL, ETL_HEARTBEAT_CONSECUTIVE_FAILURES,
    ETL_HEARTBEAT_EMISSIONS_TOTAL, ETL_HEARTBEAT_FAILURES_TOTAL,
    ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP, PIPELINE_ID_LABEL,
};
use crate::replication::tls::create_tls_connector;
use crate::types::PipelineId;
use crate::workers::base::WorkerHandle;

/// Minimum PostgreSQL version required for heartbeat functionality.
///
/// PostgreSQL 15 relaxed the privilege requirements for `pg_logical_emit_message()`,
/// making it available to users with the `pg_write_server_files` role rather than
/// requiring superuser privileges.
const MIN_PG_VERSION: i32 = 15_00_00;

/// Errors that can occur during heartbeat operations.
#[derive(Debug, Error)]
pub enum HeartbeatError {
    /// Failed to connect to the primary database.
    #[error("failed to connect to primary database: {0}")]
    ConnectionFailed(#[source] tokio_postgres::Error),

    /// Failed to create TLS connector.
    #[error("failed to create TLS connector: {0}")]
    TlsError(String),

    /// PostgreSQL version is too old for heartbeat functionality.
    #[error("PostgreSQL version {0} is below minimum required version 15")]
    UnsupportedVersion(String),

    /// Failed to emit heartbeat message.
    #[error("failed to emit heartbeat: {0}")]
    EmitFailed(#[source] tokio_postgres::Error),

    /// Failed to query PostgreSQL version.
    #[error("failed to query PostgreSQL version: {0}")]
    VersionQueryFailed(#[source] tokio_postgres::Error),
}

impl From<HeartbeatError> for EtlError {
    fn from(err: HeartbeatError) -> Self {
        EtlError::new(
            crate::error::ErrorKind::ConnectionError,
            "Heartbeat error",
            err.to_string(),
        )
    }
}

/// Connection state for the heartbeat worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatConnectionState {
    /// Not connected to the primary database.
    Disconnected,
    /// Attempting to establish a connection.
    Connecting,
    /// Connected and ready to emit heartbeats.
    Connected,
    /// Currently emitting a heartbeat message.
    Emitting,
}

impl std::fmt::Display for HeartbeatConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeartbeatConnectionState::Disconnected => write!(f, "disconnected"),
            HeartbeatConnectionState::Connecting => write!(f, "connecting"),
            HeartbeatConnectionState::Connected => write!(f, "connected"),
            HeartbeatConnectionState::Emitting => write!(f, "emitting"),
        }
    }
}

impl From<HeartbeatConnectionState> for &'static str {
    fn from(state: HeartbeatConnectionState) -> Self {
        match state {
            HeartbeatConnectionState::Disconnected => "disconnected",
            HeartbeatConnectionState::Connecting => "connecting",
            HeartbeatConnectionState::Connected => "connected",
            HeartbeatConnectionState::Emitting => "emitting",
        }
    }
}

/// Worker that emits periodic heartbeat messages to prevent replication slot invalidation.
///
/// This worker is designed for read replica configurations where the ETL pipeline
/// connects to a replica but needs to keep the primary's WAL segments from being
/// recycled prematurely.
pub struct HeartbeatWorker {
    pipeline_id: PipelineId,
    primary_config: PgConnectionConfig,
    heartbeat_config: HeartbeatConfig,
    shutdown_rx: ShutdownRx,
}

impl HeartbeatWorker {
    /// Creates a new heartbeat worker.
    ///
    /// # Arguments
    ///
    /// * `pipeline_id` - The pipeline identifier for metrics labeling
    /// * `primary_config` - Connection configuration for the primary database
    /// * `heartbeat_config` - Configuration for heartbeat timing and retries
    /// * `shutdown_rx` - Receiver for shutdown signals
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
        }
    }

    /// Starts the heartbeat worker in a background task.
    ///
    /// Returns a handle that can be used to wait for the worker to complete.
    pub async fn start(self) -> EtlResult<HeartbeatWorkerHandle> {
        let pipeline_id = self.pipeline_id;
        let (ready_tx, ready_rx) = oneshot::channel();

        let join_handle = tokio::spawn(async move {
            // Signal that we're starting (not necessarily connected yet)
            let _ = ready_tx.send(());
            self.run().await
        });

        // Wait for the worker to signal it has started
        let _ = ready_rx.await;

        Ok(HeartbeatWorkerHandle {
            pipeline_id,
            join_handle,
        })
    }

    /// Main run loop for the heartbeat worker.
    async fn run(mut self) -> EtlResult<()> {
        let pipeline_id_str = self.pipeline_id.to_string();
        let mut consecutive_failures: u64 = 0;
        let mut current_backoff_secs = self.heartbeat_config.initial_backoff_secs;

        info!(
            pipeline_id = %self.pipeline_id,
            interval_secs = %self.heartbeat_config.interval_secs,
            "heartbeat worker started"
        );

        loop {
            // Try to connect and emit heartbeats
            match self.connect_and_run(&pipeline_id_str).await {
                Ok(()) => {
                    // Clean shutdown requested
                    info!(pipeline_id = %self.pipeline_id, "heartbeat worker shutting down");
                    return Ok(());
                }
                Err(err) => {
                    consecutive_failures += 1;
                    gauge!(ETL_HEARTBEAT_CONSECUTIVE_FAILURES, PIPELINE_ID_LABEL => pipeline_id_str.clone())
                        .set(consecutive_failures as f64);
                    counter!(ETL_HEARTBEAT_FAILURES_TOTAL, PIPELINE_ID_LABEL => pipeline_id_str.clone())
                        .increment(1);

                    error!(
                        pipeline_id = %self.pipeline_id,
                        error = %err,
                        consecutive_failures = %consecutive_failures,
                        "heartbeat connection/emission failed"
                    );

                    // Calculate backoff with jitter
                    let backoff = self.calculate_backoff(current_backoff_secs);

                    warn!(
                        pipeline_id = %self.pipeline_id,
                        backoff_secs = %backoff.as_secs_f64(),
                        "waiting before reconnection attempt"
                    );

                    // Wait for backoff or shutdown
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {
                            // Update backoff for next iteration (exponential)
                            current_backoff_secs = std::cmp::min(
                                current_backoff_secs * 2,
                                self.heartbeat_config.max_backoff_secs
                            );
                        }
                        _ = self.shutdown_rx.wait_for_shutdown() => {
                            info!(pipeline_id = %self.pipeline_id, "shutdown received during backoff");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Connects to the primary database and runs the heartbeat loop.
    ///
    /// Returns `Ok(())` if shutdown was requested, or an error if the connection
    /// failed or was lost.
    async fn connect_and_run(&mut self, pipeline_id_str: &str) -> Result<(), HeartbeatError> {
        counter!(ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL, PIPELINE_ID_LABEL => pipeline_id_str.to_string())
            .increment(1);

        debug!(pipeline_id = %self.pipeline_id, "connecting to primary database");

        let client = self.connect().await?;

        // Verify PostgreSQL version
        self.verify_pg_version(&client).await?;

        info!(
            pipeline_id = %self.pipeline_id,
            host = %self.primary_config.host,
            port = %self.primary_config.port,
            "connected to primary database for heartbeats"
        );

        // Reset consecutive failures on successful connection
        gauge!(ETL_HEARTBEAT_CONSECUTIVE_FAILURES, PIPELINE_ID_LABEL => pipeline_id_str.to_string())
            .set(0.0);

        // Run heartbeat loop
        self.heartbeat_loop(&client, pipeline_id_str).await
    }

    /// Establishes a connection to the primary database.
    async fn connect(&self) -> Result<Client, HeartbeatError> {
        let config = self
            .primary_config
            .with_db(Some(&ETL_HEARTBEAT_OPTIONS));

        let tls_connector = create_tls_connector(&self.primary_config.tls)
            .map_err(|e| HeartbeatError::TlsError(e.to_string()))?;

        let (client, connection) = config
            .connect(tls_connector)
            .await
            .map_err(HeartbeatError::ConnectionFailed)?;

        // Spawn connection task to handle async messages
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = %e, "heartbeat connection error");
            }
        });

        Ok(client)
    }

    /// Verifies that the PostgreSQL version supports heartbeat functionality.
    async fn verify_pg_version(&self, client: &Client) -> Result<(), HeartbeatError> {
        let row = client
            .query_one("SHOW server_version_num", &[])
            .await
            .map_err(HeartbeatError::VersionQueryFailed)?;

        let version_str: &str = row.get(0);
        let version: i32 = version_str
            .parse()
            .unwrap_or(0);

        if version < MIN_PG_VERSION {
            return Err(HeartbeatError::UnsupportedVersion(version_str.to_string()));
        }

        debug!(
            pipeline_id = %self.pipeline_id,
            pg_version = %version_str,
            "PostgreSQL version verified for heartbeat support"
        );

        Ok(())
    }

    /// Runs the heartbeat emission loop.
    ///
    /// Emits heartbeats at the configured interval until shutdown is requested
    /// or an error occurs.
    async fn heartbeat_loop(
        &mut self,
        client: &Client,
        pipeline_id_str: &str,
    ) -> Result<(), HeartbeatError> {
        let interval = Duration::from_secs(self.heartbeat_config.interval_secs);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    self.emit_heartbeat(client, pipeline_id_str).await?;
                }
                _ = self.shutdown_rx.wait_for_shutdown() => {
                    debug!(pipeline_id = %self.pipeline_id, "shutdown requested, stopping heartbeat loop");
                    return Ok(());
                }
            }
        }
    }

    /// Emits a single heartbeat message to the primary database.
    async fn emit_heartbeat(
        &self,
        client: &Client,
        pipeline_id_str: &str,
    ) -> Result<(), HeartbeatError> {
        debug!(pipeline_id = %self.pipeline_id, "emitting heartbeat");

        // Use pg_logical_emit_message with transient=false to generate WAL
        // The message is minimal - we just need WAL activity
        client
            .execute(
                "SELECT pg_logical_emit_message(false, 'etl_heartbeat', '')",
                &[],
            )
            .await
            .map_err(HeartbeatError::EmitFailed)?;

        // Update metrics
        counter!(ETL_HEARTBEAT_EMISSIONS_TOTAL, PIPELINE_ID_LABEL => pipeline_id_str.to_string())
            .increment(1);

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        gauge!(ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP, PIPELINE_ID_LABEL => pipeline_id_str.to_string())
            .set(timestamp);

        debug!(pipeline_id = %self.pipeline_id, "heartbeat emitted successfully");

        Ok(())
    }

    /// Calculates the backoff duration with jitter.
    fn calculate_backoff(&self, base_secs: u64) -> Duration {
        let jitter_range = (base_secs as f64 * self.heartbeat_config.jitter_percent as f64) / 100.0;
        let jitter = rand::rng().random_range(-jitter_range..=jitter_range);
        let backoff_secs = (base_secs as f64 + jitter).max(0.0);
        Duration::from_secs_f64(backoff_secs)
    }
}

/// Handle for a running heartbeat worker.
///
/// Use this handle to wait for the worker to complete or to monitor its status.
#[derive(Debug)]
pub struct HeartbeatWorkerHandle {
    pipeline_id: PipelineId,
    join_handle: JoinHandle<EtlResult<()>>,
}

impl WorkerHandle for HeartbeatWorkerHandle {
    async fn wait(self) -> EtlResult<()> {
        match self.join_handle.await {
            Ok(result) => result,
            Err(join_error) => {
                error!(
                    pipeline_id = %self.pipeline_id,
                    error = %join_error,
                    "heartbeat worker task panicked"
                );
                Err(EtlError::new(
                    crate::error::ErrorKind::InternalError,
                    "Heartbeat worker panic",
                    join_error.to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_display() {
        assert_eq!(HeartbeatConnectionState::Disconnected.to_string(), "disconnected");
        assert_eq!(HeartbeatConnectionState::Connecting.to_string(), "connecting");
        assert_eq!(HeartbeatConnectionState::Connected.to_string(), "connected");
        assert_eq!(HeartbeatConnectionState::Emitting.to_string(), "emitting");
    }

    #[test]
    fn test_connection_state_into_str() {
        let state: &'static str = HeartbeatConnectionState::Disconnected.into();
        assert_eq!(state, "disconnected");

        let state: &'static str = HeartbeatConnectionState::Connected.into();
        assert_eq!(state, "connected");
    }

    #[test]
    fn test_calculate_backoff_bounds() {
        use crate::concurrency::shutdown::create_shutdown_channel;

        let config = HeartbeatConfig {
            interval_secs: 30,
            initial_backoff_secs: 10,
            max_backoff_secs: 60,
            jitter_percent: 25,
        };

        let primary_config = PgConnectionConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "test".to_string(),
            username: "test".to_string(),
            password: None,
            tls: etl_config::shared::TlsConfig::disabled(),
            keepalive: None,
        };

        let (shutdown_tx, _) = create_shutdown_channel();
        let worker = HeartbeatWorker::new(1, primary_config, config, shutdown_tx.subscribe());

        // Test that backoff is within expected bounds
        for _ in 0..100 {
            let backoff = worker.calculate_backoff(10);
            let secs = backoff.as_secs_f64();
            // With 25% jitter on 10 seconds: 7.5 to 12.5 seconds
            assert!(secs >= 7.5, "backoff {secs} should be >= 7.5");
            assert!(secs <= 12.5, "backoff {secs} should be <= 12.5");
        }
    }

    #[test]
    fn test_min_pg_version() {
        // PostgreSQL 15.0.0 should be the minimum
        assert_eq!(MIN_PG_VERSION, 15_00_00);
    }
}
