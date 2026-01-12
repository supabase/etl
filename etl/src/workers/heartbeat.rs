//! Heartbeat worker for read replica replication support.
//!
//! When streaming from a read replica, the heartbeat worker maintains a connection
//! to the primary database and periodically sends heartbeat messages via
//! `pg_logical_emit_message()` to prevent WAL accumulation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use etl_config::shared::{
    ETL_HEARTBEAT_OPTIONS, HeartbeatConfig, IntoConnectOptions, PgConnectionConfig,
};
use metrics::{counter, gauge, histogram};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config as TokioPgConfig, NoTls};
use tracing::{debug, error, info, info_span, warn};

use crate::error::{ErrorKind, EtlResult};
use crate::types::PipelineId;

/// Heartbeat metric names.
pub const ETL_HEARTBEAT_EMISSIONS_TOTAL: &str = "etl_heartbeat_emissions_total";
pub const ETL_HEARTBEAT_FAILURES_TOTAL: &str = "etl_heartbeat_failures_total";
pub const ETL_HEARTBEAT_DURATION_SECONDS: &str = "etl_heartbeat_duration_seconds";
pub const ETL_HEARTBEAT_CONNECTION_STATE: &str = "etl_heartbeat_connection_state";
pub const ETL_HEARTBEAT_LAST_SUCCESS_TIMESTAMP: &str = "etl_heartbeat_last_success_timestamp";

/// Label key for pipeline id.
pub const PIPELINE_ID_LABEL: &str = "pipeline_id";
/// Label key for error type.
pub const ERROR_TYPE_LABEL: &str = "error_type";

/// Connection states for the heartbeat worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HeartbeatConnectionState {
    /// Initial state, no connection attempted.
    Disconnected = 0,
    /// Actively establishing connection.
    Connecting = 1,
    /// Connection active, heartbeats being sent.
    Connected = 2,
    /// Connection lost, attempting to reconnect with backoff.
    Reconnecting = 3,
    /// Heartbeat disabled due to PostgreSQL version < 15.
    Degraded = 4,
}

impl From<u8> for HeartbeatConnectionState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Disconnected,
            1 => Self::Connecting,
            2 => Self::Connected,
            3 => Self::Reconnecting,
            4 => Self::Degraded,
            _ => Self::Disconnected,
        }
    }
}

/// Shared state for the heartbeat worker.
#[derive(Debug)]
pub struct HeartbeatState {
    state: AtomicU8,
}

impl HeartbeatState {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(HeartbeatConnectionState::Disconnected as u8),
        }
    }

    /// Returns the current connection state.
    pub fn connection_state(&self) -> HeartbeatConnectionState {
        self.state.load(Ordering::Relaxed).into()
    }

    fn set_state(&self, state: HeartbeatConnectionState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }
}

/// Handle to a running heartbeat worker.
#[derive(Debug)]
pub struct HeartbeatWorkerHandle {
    state: Arc<HeartbeatState>,
    join_handle: JoinHandle<EtlResult<()>>,
}

impl HeartbeatWorkerHandle {
    /// Returns the current connection state.
    pub fn state(&self) -> HeartbeatConnectionState {
        self.state.connection_state()
    }

    /// Waits for the worker to complete.
    pub async fn wait(self) -> EtlResult<()> {
        self.join_handle.await.map_err(|e| {
            crate::error::EtlError::from((
                ErrorKind::InvalidState,
                "Heartbeat worker panicked",
                e.to_string(),
            ))
        })?
    }
}

/// Heartbeat worker for sending periodic heartbeats to the primary database.
pub struct HeartbeatWorker {
    pipeline_id: PipelineId,
    config: HeartbeatConfig,
    connection_config: PgConnectionConfig,
    shutdown_rx: watch::Receiver<()>,
}

impl HeartbeatWorker {
    /// Creates a new heartbeat worker.
    pub fn new(
        pipeline_id: PipelineId,
        config: HeartbeatConfig,
        connection_config: PgConnectionConfig,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            pipeline_id,
            config,
            connection_config,
            shutdown_rx,
        }
    }

    /// Starts the heartbeat worker and returns a handle.
    pub fn start(self) -> HeartbeatWorkerHandle {
        let state = Arc::new(HeartbeatState::new());
        let state_clone = state.clone();

        let join_handle = tokio::spawn(async move { self.run(state_clone).await });

        HeartbeatWorkerHandle { state, join_handle }
    }

    async fn run(mut self, state: Arc<HeartbeatState>) -> EtlResult<()> {
        let span = info_span!(
            "heartbeat_worker",
            pipeline_id = %self.pipeline_id,
        );
        let _guard = span.enter();

        info!("starting heartbeat worker");

        let mut consecutive_failures: u32 = 0;
        let mut last_heartbeat_warning: Option<Instant> = None;

        loop {
            // Check for shutdown
            if self.shutdown_rx.has_changed().unwrap_or(true) {
                info!("shutdown signal received, stopping heartbeat worker");
                state.set_state(HeartbeatConnectionState::Disconnected);
                break;
            }

            // Try to connect
            state.set_state(if consecutive_failures > 0 {
                HeartbeatConnectionState::Reconnecting
            } else {
                HeartbeatConnectionState::Connecting
            });

            self.update_state_metric(&state);

            match self
                .connect_and_run_loop(
                    &state,
                    &mut consecutive_failures,
                    &mut last_heartbeat_warning,
                )
                .await
            {
                Ok(should_continue) => {
                    if !should_continue {
                        break;
                    }
                }
                Err(e) => {
                    consecutive_failures += 1;
                    let backoff = self.calculate_backoff(consecutive_failures);

                    warn!(
                        error = %e,
                        consecutive_failures,
                        backoff_secs = backoff.as_secs(),
                        "heartbeat connection failed, will retry"
                    );

                    counter!(ETL_HEARTBEAT_FAILURES_TOTAL, PIPELINE_ID_LABEL => self.pipeline_id.to_string(), ERROR_TYPE_LABEL => "connection").increment(1);
                    state.set_state(HeartbeatConnectionState::Reconnecting);
                    self.update_state_metric(&state);

                    // Wait before retrying, but check shutdown
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.shutdown_rx.changed() => {
                            info!("shutdown during backoff, stopping heartbeat worker");
                            state.set_state(HeartbeatConnectionState::Disconnected);
                            break;
                        }
                    }
                }
            }
        }

        info!("heartbeat worker stopped");
        Ok(())
    }

    async fn connect_and_run_loop(
        &mut self,
        state: &Arc<HeartbeatState>,
        consecutive_failures: &mut u32,
        last_heartbeat_warning: &mut Option<Instant>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Connect to primary
        let connect_options: TokioPgConfig =
            self.connection_config.with_db(Some(&ETL_HEARTBEAT_OPTIONS));

        let (client, connection) = connect_options.connect(NoTls).await?;

        // Spawn connection handler
        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = %e, "heartbeat connection error");
            }
        });

        // Check PostgreSQL version
        let pg_version = self.get_pg_version(&client).await?;
        if pg_version < 15 {
            warn!(
                pg_version,
                "PostgreSQL version < 15 detected. pg_logical_emit_message() is not available. \
                 Heartbeat disabled. WAL management must be done manually."
            );
            state.set_state(HeartbeatConnectionState::Degraded);
            self.update_state_metric(state);

            // In degraded mode, just wait for shutdown
            let _ = self.shutdown_rx.changed().await;
            drop(client);
            conn_handle.abort();
            return Ok(false);
        }

        // Verify this is the primary (not a replica)
        if self.is_in_recovery(&client).await? {
            error!(
                "primary_connection points to a replica (pg_is_in_recovery() = true). \
                    Heartbeat requires connection to the actual primary database."
            );
            return Err(
                "primary_connection must point to the primary database, not a replica".into(),
            );
        }

        info!(pg_version, "connected to primary database");
        state.set_state(HeartbeatConnectionState::Connected);
        self.update_state_metric(state);
        *consecutive_failures = 0;

        // Run heartbeat loop
        let interval = Duration::from_secs(self.config.interval_secs);
        let mut interval_timer = tokio::time::interval(interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    match self.emit_heartbeat(&client).await {
                        Ok(()) => {
                            debug!("heartbeat emitted successfully");
                            *last_heartbeat_warning = None;
                        }
                        Err(e) => {
                            error!(error = %e, "failed to emit heartbeat");
                            counter!(ETL_HEARTBEAT_FAILURES_TOTAL, PIPELINE_ID_LABEL => self.pipeline_id.to_string(), ERROR_TYPE_LABEL => "emit").increment(1);

                            // Check if we should warn about missed heartbeats
                            let now = Instant::now();
                            let warn_threshold = Duration::from_secs(300); // 5 minutes
                            if last_heartbeat_warning.is_none_or(|t| now.duration_since(t) > warn_threshold) {
                                warn!(
                                    "no successful heartbeat for over 5 minutes. \
                                     WAL may accumulate on primary. Check primary connection."
                                );
                                *last_heartbeat_warning = Some(now);
                            }

                            // Connection might be broken, exit loop to reconnect
                            drop(client);
                            conn_handle.abort();
                            return Ok(true);
                        }
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    info!("shutdown received, stopping heartbeat loop");
                    drop(client);
                    conn_handle.abort();
                    return Ok(false);
                }
            }
        }
    }

    async fn emit_heartbeat(&self, client: &Client) -> Result<(), tokio_postgres::Error> {
        let start = Instant::now();

        // pg_logical_emit_message(transactional bool, prefix text, content text)
        // transactional=false means it's written immediately without waiting for transaction commit
        client
            .execute(
                "SELECT pg_logical_emit_message(false, 'etl_heartbeat', '')",
                &[],
            )
            .await?;

        let duration = start.elapsed();
        histogram!(ETL_HEARTBEAT_DURATION_SECONDS, PIPELINE_ID_LABEL => self.pipeline_id.to_string()).record(duration.as_secs_f64());
        counter!(ETL_HEARTBEAT_EMISSIONS_TOTAL, PIPELINE_ID_LABEL => self.pipeline_id.to_string())
            .increment(1);
        gauge!(ETL_HEARTBEAT_LAST_SUCCESS_TIMESTAMP, PIPELINE_ID_LABEL => self.pipeline_id.to_string()).set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
        );

        Ok(())
    }

    async fn get_pg_version(&self, client: &Client) -> Result<i32, tokio_postgres::Error> {
        let row = client.query_one("SHOW server_version_num", &[]).await?;
        let version_str: &str = row.get(0);
        // server_version_num is like "150000" for PG 15.0
        let version: i32 = version_str.parse().unwrap_or(0) / 10000;
        Ok(version)
    }

    async fn is_in_recovery(&self, client: &Client) -> Result<bool, tokio_postgres::Error> {
        let row = client.query_one("SELECT pg_is_in_recovery()", &[]).await?;
        Ok(row.get(0))
    }

    fn calculate_backoff(&self, consecutive_failures: u32) -> Duration {
        let base_secs = self.config.initial_backoff_secs as f64;
        let max_secs = self.config.max_backoff_secs as f64;

        // Exponential backoff: initial * 2^(failures-1)
        let backoff_secs = base_secs * 2.0_f64.powi(consecutive_failures.saturating_sub(1) as i32);
        let clamped_secs = backoff_secs.min(max_secs);

        // Add jitter using simple time-based pseudo-random
        // This avoids requiring the rand crate as a dependency
        let jitter_factor = self.config.jitter_percent as f64 / 100.0;
        let jitter_range = clamped_secs * jitter_factor;
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as f64;
        // Simple hash-like value from 0.0 to 1.0
        let jitter_ratio = (now_nanos % 1_000_000.0) / 1_000_000.0;
        // Map to range [-jitter_range, +jitter_range]
        let jitter = (jitter_ratio * 2.0 - 1.0) * jitter_range;

        Duration::from_secs_f64((clamped_secs + jitter).max(0.1))
    }

    fn update_state_metric(&self, state: &Arc<HeartbeatState>) {
        gauge!(ETL_HEARTBEAT_CONNECTION_STATE, PIPELINE_ID_LABEL => self.pipeline_id.to_string())
            .set(state.connection_state() as u8 as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_conversion() {
        assert_eq!(
            HeartbeatConnectionState::from(0),
            HeartbeatConnectionState::Disconnected
        );
        assert_eq!(
            HeartbeatConnectionState::from(1),
            HeartbeatConnectionState::Connecting
        );
        assert_eq!(
            HeartbeatConnectionState::from(2),
            HeartbeatConnectionState::Connected
        );
        assert_eq!(
            HeartbeatConnectionState::from(3),
            HeartbeatConnectionState::Reconnecting
        );
        assert_eq!(
            HeartbeatConnectionState::from(4),
            HeartbeatConnectionState::Degraded
        );
        // Unknown values default to Disconnected
        assert_eq!(
            HeartbeatConnectionState::from(255),
            HeartbeatConnectionState::Disconnected
        );
    }

    #[test]
    fn test_calculate_backoff() {
        let config = HeartbeatConfig {
            interval_secs: 30,
            initial_backoff_secs: 1,
            max_backoff_secs: 60,
            jitter_percent: 0, // No jitter for predictable tests
        };

        let (_tx, rx) = watch::channel(());
        let worker = HeartbeatWorker::new(
            1,
            config,
            PgConnectionConfig {
                host: "localhost".to_string(),
                port: 5432,
                name: "test".to_string(),
                username: "test".to_string(),
                password: None,
                tls: etl_config::shared::TlsConfig::disabled(),
                keepalive: None,
            },
            rx,
        );

        // First failure: 1 second
        let backoff = worker.calculate_backoff(1);
        assert_eq!(backoff.as_secs(), 1);

        // Second failure: 2 seconds
        let backoff = worker.calculate_backoff(2);
        assert_eq!(backoff.as_secs(), 2);

        // Third failure: 4 seconds
        let backoff = worker.calculate_backoff(3);
        assert_eq!(backoff.as_secs(), 4);

        // Capped at max (60 seconds)
        let backoff = worker.calculate_backoff(10);
        assert_eq!(backoff.as_secs(), 60);
    }

    #[test]
    fn test_heartbeat_state() {
        let state = HeartbeatState::new();
        assert_eq!(
            state.connection_state(),
            HeartbeatConnectionState::Disconnected
        );

        state.set_state(HeartbeatConnectionState::Connected);
        assert_eq!(
            state.connection_state(),
            HeartbeatConnectionState::Connected
        );

        state.set_state(HeartbeatConnectionState::Reconnecting);
        assert_eq!(
            state.connection_state(),
            HeartbeatConnectionState::Reconnecting
        );
    }
}
