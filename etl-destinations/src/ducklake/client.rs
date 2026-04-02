use std::sync::Arc;
#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use duckdb::Config;
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use metrics::histogram;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::trace;

use crate::ducklake::metrics::{
    ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS, ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS,
    ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS,
};

/// Timeout applied to each foreground DuckLake blocking operation.
pub(super) const FOREGROUND_QUERY_TIMEOUT: Duration = Duration::from_secs(2 * 60);
/// Timeout applied to each maintenance DuckLake blocking operation.
pub(super) const MAINTENANCE_QUERY_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Timeout applied to each background DuckLake metrics query.
pub(super) const METRICS_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout class applied to one DuckDB blocking operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckDbBlockingOperationKind {
    Foreground,
    Maintenance,
    Metrics,
}

impl DuckDbBlockingOperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Maintenance => "maintenance",
            Self::Metrics => "metrics",
        }
    }

    pub(super) fn timeout(self) -> Duration {
        match self {
            Self::Foreground => FOREGROUND_QUERY_TIMEOUT,
            Self::Maintenance => MAINTENANCE_QUERY_TIMEOUT,
            Self::Metrics => METRICS_QUERY_TIMEOUT,
        }
    }
}

/// Async watchdog that interrupts one timed DuckDB query when its deadline expires.
pub(super) struct DuckDbQueryWatchdog {
    timed_out: Arc<AtomicBool>,
    interrupt_tx: Option<oneshot::Sender<Arc<duckdb::InterruptHandle>>>,
    done_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl DuckDbQueryWatchdog {
    fn spawn(deadline: Instant) -> Self {
        let timed_out = Arc::new(AtomicBool::new(false));
        let timeout_flag = Arc::clone(&timed_out);
        let (interrupt_tx, interrupt_rx) = oneshot::channel::<Arc<duckdb::InterruptHandle>>();
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interrupt_rx = Box::pin(interrupt_rx);
            let mut done_rx = Box::pin(done_rx);
            let interrupt_handle = tokio::select! {
                biased;
                _ = &mut done_rx => return,
                result = &mut interrupt_rx => match result {
                    Ok(handle) => handle,
                    Err(_) => return,
                },
                _ = tokio::time::sleep_until(deadline) => {
                    timeout_flag.store(true, Ordering::Relaxed);
                    // If we didn't receive the interrupt_rx yet, make sure to get it to call interrupt() later
                    tokio::select! {
                        biased;
                        _ = &mut done_rx => return,
                        result = &mut interrupt_rx => match result {
                            Ok(handle) => handle,
                            Err(_) => return,
                        },
                    }
                },
            };

            if timeout_flag.load(Ordering::Relaxed) {
                interrupt_handle.interrupt();
                return;
            }

            tokio::select! {
                biased;
                _ = &mut done_rx => {}
                _ = tokio::time::sleep_until(deadline) => {
                    timeout_flag.store(true, Ordering::Relaxed);
                    interrupt_handle.interrupt();
                }
            }
        });

        Self {
            timed_out,
            interrupt_tx: Some(interrupt_tx),
            done_tx: Some(done_tx),
            task: Some(task),
        }
    }

    fn publish_interrupt_handle(&mut self, handle: Arc<duckdb::InterruptHandle>) {
        if let Some(interrupt_tx) = self.interrupt_tx.take() {
            let _ = interrupt_tx.send(handle);
        }
    }

    fn finish(&mut self) {
        if let Some(done_tx) = self.done_tx.take() {
            let _ = done_tx.send(());
        }
    }

    fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::Relaxed)
    }

    fn async_task_handle(&mut self) -> EtlResult<JoinHandle<()>> {
        self.task.take().ok_or_else(|| {
            etl_error!(
                ErrorKind::DestinationError,
                "Cannot get async task handle from watchdog: task is None"
            )
        })
    }
}

/// Custom r2d2 connection manager that opens an in-memory DuckDB connection and
/// attaches the DuckLake catalog on every `connect()` call.
///
/// Each opened connection is independent and attaches the same catalog, which
/// is safe: DuckLake (backed by a PostgreSQL catalog) supports concurrent writers.
#[derive(Clone)]
pub(super) struct DuckLakeConnectionManager {
    /// SQL executed immediately after a new connection is opened.
    /// Loads required extensions and attaches the DuckLake catalog.
    pub(super) setup_sql: Arc<String>,
    /// Disables DuckDB extension autoload/autoinstall when vendored local
    /// extensions are required.
    pub(super) disable_extension_autoload: bool,
    /// Counts successfully initialized DuckDB connections for tests.
    #[cfg(feature = "test-utils")]
    pub(super) open_count: Arc<AtomicUsize>,
}

/// DuckDB connection state tracked while a pooled connection is checked out.
pub(super) struct ManagedDuckLakeConnection {
    conn: duckdb::Connection,
    broken: bool,
}

impl DuckLakeConnectionManager {
    /// Opens one fully initialized DuckDB connection and attaches the lake catalog.
    pub(super) fn open_duckdb_connection(&self) -> Result<duckdb::Connection, duckdb::Error> {
        let conn = if self.disable_extension_autoload {
            duckdb::Connection::open_in_memory_with_flags(
                Config::default().enable_autoload_extension(false)?,
            )?
        } else {
            duckdb::Connection::open_in_memory()?
        };
        conn.execute_batch(&self.setup_sql)?;
        #[cfg(feature = "test-utils")]
        self.open_count.fetch_add(1, Ordering::Relaxed);
        Ok(conn)
    }

    /// Returns the number of successfully initialized DuckDB connections.
    #[cfg(feature = "test-utils")]
    pub(super) fn open_count_for_tests(&self) -> usize {
        self.open_count.load(Ordering::Relaxed)
    }
}

impl r2d2::ManageConnection for DuckLakeConnectionManager {
    type Connection = ManagedDuckLakeConnection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<ManagedDuckLakeConnection, duckdb::Error> {
        Ok(ManagedDuckLakeConnection {
            conn: self.open_duckdb_connection()?,
            broken: false,
        })
    }

    fn is_valid(&self, conn: &mut ManagedDuckLakeConnection) -> Result<(), duckdb::Error> {
        conn.conn.execute_batch("SELECT 1")?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut ManagedDuckLakeConnection) -> bool {
        conn.broken
    }
}

/// Formats a DuckDB query failure so the displayed [`EtlError`] includes
/// both the SQL statement and the underlying DuckDB error message.
pub(super) fn format_query_error_detail(sql: &str, error: &duckdb::Error) -> String {
    let compact_sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("sql: {compact_sql}; source: {error}")
}

/// Builds and warms an r2d2 pool of initialized DuckDB connections.
pub(super) async fn build_warm_ducklake_pool(
    manager: DuckLakeConnectionManager,
    pool_size: u32,
    purpose: &'static str,
) -> EtlResult<r2d2::Pool<DuckLakeConnectionManager>> {
    tokio::task::spawn_blocking(move || -> EtlResult<_> {
        let started = Instant::now();
        let pool = r2d2::Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(pool_size))
            .test_on_check_out(true)
            .build(manager)
            .map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to build DuckLake connection pool",
                    source: e
                )
            })?;

        let mut warmed_connections = Vec::with_capacity(pool_size as usize);
        for _ in 0..pool_size {
            let conn = pool.get().map_err(|e| {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to warm DuckLake connection pool",
                    source: e
                )
            })?;
            warmed_connections.push(conn);
        }
        drop(warmed_connections);

        trace!(
            purpose,
            pool_size,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "ducklake connection pool warmed"
        );

        Ok(pool)
    })
    .await
    .map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake connection pool initialization task panicked"
        )
    })?
}

/// Builds a consistent timeout error for one blocking DuckDB stage.
#[inline]
pub(super) fn duckdb_blocking_timeout_error(
    operation_kind: DuckDbBlockingOperationKind,
    timeout: Duration,
    stage: &'static str,
) -> EtlError {
    etl_error!(
        ErrorKind::DestinationQueryFailed,
        "DuckLake blocking operation timed out",
        format!(
            "operation_kind={}, stage={stage}, timeout_ms={}",
            operation_kind.as_str(),
            timeout.as_millis()
        )
    )
}

/// Runs one DuckDB operation on Tokio's blocking pool after acquiring a permit
/// that matches the configured DuckDB concurrency limit and then checking out a
/// warm pooled DuckDB connection.
pub(super) async fn run_duckdb_blocking<R, F>(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    operation_kind: DuckDbBlockingOperationKind,
    operation: F,
) -> EtlResult<R>
where
    R: Send + 'static,
    F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
{
    run_duckdb_blocking_with_timeout(
        pool,
        blocking_slots,
        operation_kind,
        operation_kind.timeout(),
        operation,
    )
    .await
}

/// Runs one DuckDB operation with an explicit timeout budget.
pub(super) async fn run_duckdb_blocking_with_timeout<R, F>(
    pool: Arc<r2d2::Pool<DuckLakeConnectionManager>>,
    blocking_slots: Arc<Semaphore>,
    operation_kind: DuckDbBlockingOperationKind,
    timeout: Duration,
    operation: F,
) -> EtlResult<R>
where
    R: Send + 'static,
    F: FnOnce(&duckdb::Connection) -> EtlResult<R> + Send + 'static,
{
    let deadline = Instant::now() + timeout;
    let slot_wait_started = Instant::now();
    let permit = tokio::time::timeout_at(deadline, blocking_slots.acquire_owned())
        .await
        .map_err(|_| duckdb_blocking_timeout_error(operation_kind, timeout, "slot_wait"))?
        .map_err(|_| {
            etl_error!(
                ErrorKind::ApplyWorkerPanic,
                "DuckLake blocking slot acquisition failed"
            )
        })?;
    histogram!(ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS)
        .record(slot_wait_started.elapsed().as_secs_f64());
    trace!(
        wait_ms = slot_wait_started.elapsed().as_millis() as u64,
        "wait for ducklake blocking slot"
    );

    // This is needed to make sure we properly interrupt the blocking operation if it exceeds the timeout, we don't just cancel the task and leave the connection active.
    let mut watchdog = DuckDbQueryWatchdog::spawn(deadline);
    let watchdog_task = watchdog.async_task_handle()?;

    let blocking_result = tokio::task::spawn_blocking(move || -> EtlResult<R> {
        // Please if you modify the code inside this blocking task do not add any
        // blocking operations that could delay other tasks waiting on this slot.
        let _permit = permit;
        let checkout_timeout = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if checkout_timeout.is_zero() {
            return Err(duckdb_blocking_timeout_error(
                operation_kind,
                timeout,
                "pool_checkout",
            ));
        }
        let checkout_started = Instant::now();
        let mut pooled_conn = pool.get_timeout(checkout_timeout).map_err(|e| {
            if Instant::now() >= deadline {
                duckdb_blocking_timeout_error(operation_kind, timeout, "pool_checkout")
            } else {
                etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to check out DuckLake connection",
                    source: e
                )
            }
        })?;
        histogram!(ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS)
            .record(checkout_started.elapsed().as_secs_f64());
        trace!(
            wait_ms = checkout_started.elapsed().as_millis() as u64,
            "wait for ducklake pool checkout"
        );
        let operation_timeout = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if operation_timeout.is_zero() {
            return Err(duckdb_blocking_timeout_error(
                operation_kind,
                timeout,
                "query_execution",
            ));
        }
        let interrupt_handle = pooled_conn.conn.interrupt_handle();
        watchdog.publish_interrupt_handle(interrupt_handle);
        if watchdog.timed_out() {
            pooled_conn.broken = true;
            return Err(duckdb_blocking_timeout_error(
                operation_kind,
                timeout,
                "query_execution",
            ));
        }
        let operation_started = Instant::now();
        let res = operation(&pooled_conn.conn);
        watchdog.finish();
        histogram!(ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS)
            .record(operation_started.elapsed().as_secs_f64());
        trace!(
            duration_ms = operation_started.elapsed().as_millis() as u64,
            "ducklake blocking operation finished"
        );
        if watchdog.timed_out() {
            pooled_conn.broken = true;
            return Err(duckdb_blocking_timeout_error(
                operation_kind,
                timeout,
                "query_execution",
            ));
        }
        if res.is_err() {
            pooled_conn.broken = true;
        }

        res
    })
    .await;

    // Await the watchdog so it cannot outlive the finished blocking task and
    // accidentally interrupt a later operation that reuses the connection.
    watchdog_task.await.map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake query watchdog task panicked"
        )
    })?;

    blocking_result.map_err(|_| {
        etl_error!(
            ErrorKind::ApplyWorkerPanic,
            "DuckLake blocking operation task panicked"
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use tokio::sync::Semaphore;

    fn make_blocking_test_manager() -> DuckLakeConnectionManager {
        DuckLakeConnectionManager {
            setup_sql: Arc::new(String::new()),
            disable_extension_autoload: cfg!(target_os = "linux"),
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[test]
    fn test_duckdb_blocking_operation_kind_timeouts() {
        assert_eq!(
            DuckDbBlockingOperationKind::Foreground.timeout(),
            FOREGROUND_QUERY_TIMEOUT
        );
        assert_eq!(
            DuckDbBlockingOperationKind::Maintenance.timeout(),
            MAINTENANCE_QUERY_TIMEOUT
        );
        assert_eq!(
            DuckDbBlockingOperationKind::Metrics.timeout(),
            METRICS_QUERY_TIMEOUT
        );
    }

    #[tokio::test]
    async fn test_run_duckdb_blocking_timeout_releases_resources_for_follow_up_queries() {
        let pool = Arc::new(
            build_warm_ducklake_pool(make_blocking_test_manager(), 1, "test")
                .await
                .expect("failed to build blocking test pool"),
        );
        let blocking_slots = Arc::new(Semaphore::new(1));

        let error = run_duckdb_blocking_with_timeout(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            Duration::from_millis(50),
            |conn| -> EtlResult<()> {
                conn.query_row(
                    "SELECT COUNT(*) FROM range(10000000) t1, range(1000000) t2;",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|source| {
                    etl_error!(
                        ErrorKind::DestinationQueryFailed,
                        "DuckLake timeout test query failed",
                        source: source
                    )
                })?;
                Ok(())
            },
        )
        .await
        .expect_err("expected blocking timeout");

        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(
            error.description(),
            Some("DuckLake blocking operation timed out")
        );
        // This timeout budget covers pool checkout and query execution. Under
        // full-suite load the deadline can expire before the long-running query
        // starts, so either stage is acceptable as long as the pool remains
        // usable for the follow-up query below.
        assert!(
            error.detail().is_some_and(|detail| {
                detail.contains("stage=pool_checkout") || detail.contains("stage=query_execution")
            }),
            "unexpected error detail: {error:?}"
        );

        let value = run_duckdb_blocking_with_timeout(
            Arc::clone(&pool),
            Arc::clone(&blocking_slots),
            DuckDbBlockingOperationKind::Foreground,
            Duration::from_secs(1),
            |conn| -> EtlResult<i64> {
                conn.query_row("SELECT 1;", [], |row| row.get::<_, i64>(0))
                    .map_err(|source| {
                        etl_error!(
                            ErrorKind::DestinationQueryFailed,
                            "DuckLake timeout verification query failed",
                            source: source
                        )
                    })
            },
        )
        .await
        .expect("expected follow-up query to succeed");

        assert_eq!(value, 1);
    }

    #[test]
    fn test_format_query_error_detail_compacts_sql_and_includes_source() {
        let sql = r#"CREATE TABLE lake."orders" ("id" INTEGER NOT NULL)"#;
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("parser error".to_string()),
        );

        assert_eq!(
            format_query_error_detail(sql, &error),
            "sql: CREATE TABLE lake.\"orders\" (\"id\" INTEGER NOT NULL); source: parser error"
        );
    }

    #[tokio::test]
    async fn test_query_watchdog_marks_timeout_when_handle_arrives_after_deadline() {
        let conn = make_blocking_test_manager()
            .open_duckdb_connection()
            .expect("failed to open blocking test connection");
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + Duration::from_millis(10));
        let watchdog_task = watchdog
            .async_task_handle()
            .expect("failed to extract watchdog task");

        tokio::time::sleep(Duration::from_millis(20)).await;
        watchdog.publish_interrupt_handle(conn.interrupt_handle());

        watchdog_task.await.expect("watchdog task should not panic");

        assert!(
            watchdog.timed_out(),
            "watchdog should remain timed out when the interrupt handle is published after the deadline"
        );
    }
}
