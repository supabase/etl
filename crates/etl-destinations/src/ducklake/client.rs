#[cfg(feature = "test-utils")]
use std::sync::atomic::AtomicUsize;
use std::{
    borrow::Cow,
    error, fmt, process,
    sync::{
        Arc, LazyLock, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use duckdb::Config;
use etl::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};
use metrics::histogram;
use regex::Regex;
use tokio::{
    sync::{Semaphore, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tracing::{info, trace, warn};

use crate::ducklake::{
    config::{DuckLakeSetupPlan, DuckLakeSetupStep},
    metrics::{
        ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS, ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS,
        ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS,
    },
};

/// Monotonic identifier assigned to each DuckLake connection setup attempt.
static NEXT_CONNECTION_INIT_ID: AtomicU64 = AtomicU64::new(1);
/// Monotonic identifier assigned to each timed DuckDB blocking operation.
static NEXT_DUCKDB_BLOCKING_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
/// Matches one libpq password field inside a conninfo string.
static POSTGRES_PASSWORD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"password=(?:'([^'\\]|\\.)*'|[^\s,);]+)")
        .expect("postgres password redaction regex should compile")
});

/// Timeout applied to each foreground DuckLake blocking operation.
pub(super) const FOREGROUND_QUERY_TIMEOUT: Duration = Duration::from_secs(3 * 60);
/// Extra time allowed for a timed-out DuckDB operation to return after
/// interrupt() has been called. If the operation is still stuck after this,
/// the process is no longer safe to keep running.
const BLOCKING_ABORT_GRACE: Duration = Duration::from_secs(30);

trait DuckDbQueryInterrupt: Send + Sync + 'static {
    fn interrupt(&self);
}

impl DuckDbQueryInterrupt for duckdb::InterruptHandle {
    fn interrupt(&self) {
        duckdb::InterruptHandle::interrupt(self);
    }
}

type DuckDbQueryInterruptHandle = Arc<dyn DuckDbQueryInterrupt>;

fn remaining_ms_until(deadline: Instant) -> u64 {
    deadline.checked_duration_since(Instant::now()).unwrap_or(Duration::ZERO).as_millis() as u64
}

/// Timeout class applied to one DuckDB blocking operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DuckDbBlockingOperationKind {
    Foreground,
}

impl DuckDbBlockingOperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
        }
    }

    pub(super) fn timeout(self) -> Duration {
        match self {
            Self::Foreground => FOREGROUND_QUERY_TIMEOUT,
        }
    }
}

/// Async watchdog that interrupts one timed DuckDB query when its deadline
/// expires.
pub(super) struct DuckDbQueryWatchdog {
    operation_id: u64,
    operation_kind: DuckDbBlockingOperationKind,
    timeout: Duration,
    timed_out: Arc<AtomicBool>,
    interrupt_tx: Option<oneshot::Sender<DuckDbQueryInterruptHandle>>,
    done_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl DuckDbQueryWatchdog {
    #[cfg(test)]
    fn spawn(deadline: Instant) -> Self {
        Self::spawn_with_context(
            deadline,
            0,
            DuckDbBlockingOperationKind::Foreground,
            Duration::ZERO,
        )
    }

    fn spawn_with_context(
        deadline: Instant,
        operation_id: u64,
        operation_kind: DuckDbBlockingOperationKind,
        timeout: Duration,
    ) -> Self {
        let timed_out = Arc::new(AtomicBool::new(false));
        let timeout_flag = Arc::clone(&timed_out);
        let (interrupt_tx, interrupt_rx) = oneshot::channel::<DuckDbQueryInterruptHandle>();
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            info!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                timeout_ms = timeout.as_millis() as u64,
                deadline_remaining_ms = remaining_ms_until(deadline),
                "ducklake query watchdog task started: operation_id={}, operation_kind={}, \
                 timeout_ms={}, deadline_remaining_ms={}",
                operation_id,
                operation_kind.as_str(),
                timeout.as_millis(),
                remaining_ms_until(deadline)
            );
            let mut interrupt_rx = Box::pin(interrupt_rx);
            let mut done_rx = Box::pin(done_rx);
            let interrupt_handle = tokio::select! {
                biased;
                _ = &mut done_rx => {
                    info!(
                        operation_id,
                        operation_kind = operation_kind.as_str(),
                        "ducklake query watchdog finished before interrupt handle: operation_id={}, operation_kind={}",
                        operation_id,
                        operation_kind.as_str()
                    );
                    return;
                },
                result = &mut interrupt_rx => match result {
                    Ok(handle) => {
                        info!(
                            operation_id,
                            operation_kind = operation_kind.as_str(),
                            deadline_remaining_ms = remaining_ms_until(deadline),
                            "ducklake query watchdog received interrupt handle before deadline: \
                             operation_id={}, operation_kind={}, deadline_remaining_ms={}",
                            operation_id,
                            operation_kind.as_str(),
                            remaining_ms_until(deadline)
                        );
                        handle
                    },
                    Err(_) => {
                        warn!(
                            operation_id,
                            operation_kind = operation_kind.as_str(),
                            "ducklake query watchdog interrupt sender dropped before deadline: \
                             operation_id={}, operation_kind={}",
                            operation_id,
                            operation_kind.as_str()
                        );
                        return;
                    },
                },
                _ = tokio::time::sleep_until(deadline) => {
                    timeout_flag.store(true, Ordering::Relaxed);
                    warn!(
                        operation_id,
                        operation_kind = operation_kind.as_str(),
                        timeout_ms = timeout.as_millis() as u64,
                        "ducklake query watchdog deadline elapsed before interrupt handle: \
                         operation_id={}, operation_kind={}, timeout_ms={}",
                        operation_id,
                        operation_kind.as_str(),
                        timeout.as_millis()
                    );
                    // If we didn't receive the interrupt_rx yet, make sure to get it to call interrupt() later
                    tokio::select! {
                        biased;
                        _ = &mut done_rx => {
                            info!(
                                operation_id,
                                operation_kind = operation_kind.as_str(),
                                "ducklake query watchdog received done after deadline before interrupt handle: \
                                 operation_id={}, operation_kind={}",
                                operation_id,
                                operation_kind.as_str()
                            );
                            return;
                        },
                        result = &mut interrupt_rx => match result {
                            Ok(handle) => {
                                warn!(
                                    operation_id,
                                    operation_kind = operation_kind.as_str(),
                                    "ducklake query watchdog received interrupt handle after deadline: \
                                     operation_id={}, operation_kind={}",
                                    operation_id,
                                    operation_kind.as_str()
                                );
                                handle
                            },
                            Err(_) => {
                                warn!(
                                    operation_id,
                                    operation_kind = operation_kind.as_str(),
                                    "ducklake query watchdog interrupt sender dropped after deadline: \
                                     operation_id={}, operation_kind={}",
                                    operation_id,
                                    operation_kind.as_str()
                                );
                                return;
                            },
                        },
                    }
                },
            };

            if timeout_flag.load(Ordering::Relaxed) {
                warn!(
                    operation_id,
                    operation_kind = operation_kind.as_str(),
                    "ducklake query watchdog calling interrupt after timeout: operation_id={}, \
                     operation_kind={}",
                    operation_id,
                    operation_kind.as_str()
                );
                interrupt_handle.interrupt();
                warn!(
                    operation_id,
                    operation_kind = operation_kind.as_str(),
                    "ducklake query watchdog interrupt returned after timeout: operation_id={}, \
                     operation_kind={}",
                    operation_id,
                    operation_kind.as_str()
                );
                return;
            }

            tokio::select! {
                biased;
                _ = &mut done_rx => {
                    info!(
                        operation_id,
                        operation_kind = operation_kind.as_str(),
                        deadline_remaining_ms = remaining_ms_until(deadline),
                        "ducklake query watchdog received done before deadline after interrupt handle: \
                         operation_id={}, operation_kind={}, deadline_remaining_ms={}",
                        operation_id,
                        operation_kind.as_str(),
                        remaining_ms_until(deadline)
                    );
                }
                _ = tokio::time::sleep_until(deadline) => {
                    timeout_flag.store(true, Ordering::Relaxed);
                    warn!(
                        operation_id,
                        operation_kind = operation_kind.as_str(),
                        timeout_ms = timeout.as_millis() as u64,
                        "ducklake query watchdog deadline elapsed after interrupt handle; calling interrupt: \
                         operation_id={}, operation_kind={}, timeout_ms={}",
                        operation_id,
                        operation_kind.as_str(),
                        timeout.as_millis()
                    );
                    interrupt_handle.interrupt();
                    warn!(
                        operation_id,
                        operation_kind = operation_kind.as_str(),
                        "ducklake query watchdog interrupt returned after handle/deadline path: \
                         operation_id={}, operation_kind={}",
                        operation_id,
                        operation_kind.as_str()
                    );
                }
            }
        });

        Self {
            operation_id,
            operation_kind,
            timeout,
            timed_out,
            interrupt_tx: Some(interrupt_tx),
            done_tx: Some(done_tx),
            task: Some(task),
        }
    }

    fn publish_interrupt_handle(&mut self, handle: Arc<duckdb::InterruptHandle>) {
        self.publish_query_interrupt_handle(handle);
    }

    fn publish_query_interrupt_handle(&mut self, handle: DuckDbQueryInterruptHandle) {
        if let Some(interrupt_tx) = self.interrupt_tx.take() {
            info!(
                operation_id = self.operation_id,
                operation_kind = self.operation_kind.as_str(),
                timeout_ms = self.timeout.as_millis() as u64,
                "ducklake query watchdog publishing interrupt handle: operation_id={}, \
                 operation_kind={}, timeout_ms={}",
                self.operation_id,
                self.operation_kind.as_str(),
                self.timeout.as_millis()
            );
            let _ = interrupt_tx.send(handle);
        } else {
            warn!(
                operation_id = self.operation_id,
                operation_kind = self.operation_kind.as_str(),
                "ducklake query watchdog interrupt handle publish skipped because sender is gone: \
                 operation_id={}, operation_kind={}",
                self.operation_id,
                self.operation_kind.as_str()
            );
        }
    }

    fn finish(&mut self) {
        if let Some(done_tx) = self.done_tx.take() {
            info!(
                operation_id = self.operation_id,
                operation_kind = self.operation_kind.as_str(),
                timed_out = self.timed_out(),
                "ducklake query watchdog finish signal sent: operation_id={}, operation_kind={}, \
                 timed_out={}",
                self.operation_id,
                self.operation_kind.as_str(),
                self.timed_out()
            );
            let _ = done_tx.send(());
        } else {
            warn!(
                operation_id = self.operation_id,
                operation_kind = self.operation_kind.as_str(),
                timed_out = self.timed_out(),
                "ducklake query watchdog finish skipped because sender is gone: operation_id={}, \
                 operation_kind={}, timed_out={}",
                self.operation_id,
                self.operation_kind.as_str(),
                self.timed_out()
            );
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

/// Shared registry of interrupt handles for live DuckLake connections.
#[derive(Default)]
pub(super) struct DuckLakeInterruptRegistry {
    handles: Mutex<Vec<Weak<duckdb::InterruptHandle>>>,
}

impl DuckLakeInterruptRegistry {
    /// Registers one live DuckLake connection interrupt handle.
    fn register(&self, handle: &Arc<duckdb::InterruptHandle>) {
        self.handles
            .lock()
            .expect("ducklake interrupt registry mutex should not be poisoned")
            .push(Arc::downgrade(handle));
    }

    /// Interrupts all currently live registered DuckLake connections.
    pub(super) fn interrupt_all(&self) -> usize {
        let mut interrupted = 0;
        let mut handles =
            self.handles.lock().expect("ducklake interrupt registry mutex should not be poisoned");
        handles.retain(|handle| {
            let Some(handle) = handle.upgrade() else {
                return false;
            };
            handle.interrupt();
            interrupted += 1;
            true
        });
        interrupted
    }
}

/// Custom r2d2 connection manager that opens an in-memory DuckDB connection and
/// attaches the DuckLake catalog on every `connect()` call.
///
/// Each opened connection is independent and attaches the same catalog, which
/// is safe: DuckLake (backed by a PostgreSQL catalog) supports concurrent
/// writers.
#[derive(Clone)]
pub(super) struct DuckLakeConnectionManager {
    /// Ordered setup phases executed immediately after a new connection opens.
    pub(super) setup_plan: Arc<DuckLakeSetupPlan>,
    /// Disables DuckDB extension autoload/autoinstall when vendored local
    /// extensions are required.
    pub(super) disable_extension_autoload: bool,
    /// Shared registry of interrupt handles for live connections.
    pub(super) interrupt_registry: Arc<DuckLakeInterruptRegistry>,
    /// Counts successfully initialized DuckDB connections for tests.
    #[cfg(feature = "test-utils")]
    pub(super) open_count: Arc<AtomicUsize>,
}

/// DuckDB connection state tracked while a pooled connection is checked out.
pub(super) struct ManagedDuckLakeConnection {
    conn: duckdb::Connection,
    broken: bool,
}

/// Error returned while opening or validating one DuckLake pooled connection.
#[derive(Debug)]
pub(super) struct DuckLakeConnectionError {
    message: String,
}

impl DuckLakeConnectionError {
    /// Creates one setup-phase error and redacts a PostgreSQL catalog password
    /// when present.
    fn setup_phase(step: &DuckLakeSetupStep, error: duckdb::Error) -> Self {
        let error_message = error.to_string();
        let error_message = if attach_step_uses_postgres_catalog(step) {
            redact_ducklake_connection_error_message(&error_message)
        } else {
            Cow::Owned(error_message)
        };

        Self {
            message: format!(
                "ducklake duckdb connection setup phase `{}` failed: {error_message}",
                step.label
            ),
        }
    }

    /// Creates one connection validation error.
    fn validation(error: duckdb::Error) -> Self {
        Self { message: format!("DuckLake DuckDB connection validation failed: {error}") }
    }
}

impl fmt::Display for DuckLakeConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str(&self.message)
    }
}

impl error::Error for DuckLakeConnectionError {}

impl DuckLakeConnectionManager {
    /// Interrupts all currently live managed DuckLake connections.
    pub(super) fn interrupt_all_connections(&self) -> usize {
        self.interrupt_registry.interrupt_all()
    }

    /// Opens one fully initialized DuckDB connection and attaches the lake
    /// catalog.
    pub(super) fn open_duckdb_connection(
        &self,
    ) -> Result<duckdb::Connection, DuckLakeConnectionError> {
        let connection_init_id = NEXT_CONNECTION_INIT_ID.fetch_add(1, Ordering::Relaxed);
        let conn = if self.disable_extension_autoload {
            duckdb::Connection::open_in_memory_with_flags(
                Config::default()
                    .enable_autoload_extension(false)
                    .map_err(DuckLakeConnectionError::validation)?,
            )
            .map_err(DuckLakeConnectionError::validation)?
        } else {
            duckdb::Connection::open_in_memory().map_err(DuckLakeConnectionError::validation)?
        };
        self.interrupt_registry.register(&conn.interrupt_handle());
        for step in self.setup_plan.steps() {
            let phase_started = Instant::now();
            info!(
                connection_init_id,
                phase = step.label,
                "starting ducklake duckdb connection setup phase"
            );
            if let Err(error) = conn.execute_batch(&step.sql) {
                return Err(DuckLakeConnectionError::setup_phase(step, error));
            }
            info!(
                connection_init_id,
                phase = step.label,
                elapsed_ms = phase_started.elapsed().as_millis() as u64,
                "ducklake duckdb connection setup phase finished"
            );
        }
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
    type Error = DuckLakeConnectionError;

    fn connect(&self) -> Result<ManagedDuckLakeConnection, DuckLakeConnectionError> {
        Ok(ManagedDuckLakeConnection { conn: self.open_duckdb_connection()?, broken: false })
    }

    fn is_valid(
        &self,
        conn: &mut ManagedDuckLakeConnection,
    ) -> Result<(), DuckLakeConnectionError> {
        conn.conn.execute_batch("SELECT 1").map_err(DuckLakeConnectionError::validation)?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut ManagedDuckLakeConnection) -> bool {
        conn.broken
    }
}

/// Formats owned query context for a DuckDB query failure.
pub(super) fn format_query_error_detail(sql: &str) -> String {
    let compact_sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("sql: {compact_sql}")
}

/// Returns whether one setup step attaches a PostgreSQL-backed catalog.
fn attach_step_uses_postgres_catalog(step: &DuckLakeSetupStep) -> bool {
    step.label == "attach_catalog" && step.sql.contains("ducklake:postgres:")
}

/// Redacts PostgreSQL passwords from one DuckDB or r2d2 error message.
fn redact_ducklake_connection_error_message(message: &str) -> Cow<'_, str> {
    POSTGRES_PASSWORD_REGEX.replace_all(message, "password='[redacted]'")
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
            .min_idle(Some(0))
            .connection_timeout(Duration::from_mins(4))
            .test_on_check_out(true)
            // Callers log the returned pool initialization failure once, so
            // suppress r2d2's internal per-attempt logging here.
            .error_handler(Box::new(r2d2::NopErrorHandler))
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
            "Operation kind={}, stage={stage}, timeout_ms={}",
            operation_kind.as_str(),
            timeout.as_millis()
        )
    )
}

fn abort_stuck_duckdb_blocking_operation(
    operation_id: u64,
    operation_kind: DuckDbBlockingOperationKind,
    timeout: Duration,
    abort_grace: Duration,
) -> ! {
    tracing::error!(
        operation_id,
        operation_kind = operation_kind.as_str(),
        timeout_ms = timeout.as_millis() as u64,
        abort_grace_ms = abort_grace.as_millis() as u64,
        "ducklake blocking operation did not return after timeout interrupt; aborting process: \
         operation_id={}, operation_kind={}, timeout_ms={}, abort_grace_ms={}",
        operation_id,
        operation_kind.as_str(),
        timeout.as_millis(),
        abort_grace.as_millis()
    );
    process::abort();
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
    let operation_id = NEXT_DUCKDB_BLOCKING_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
    let deadline = Instant::now() + timeout;
    info!(
        operation_id,
        operation_kind = operation_kind.as_str(),
        timeout_ms = timeout.as_millis() as u64,
        abort_grace_ms = BLOCKING_ABORT_GRACE.as_millis() as u64,
        available_permits = blocking_slots.available_permits(),
        "ducklake blocking operation starting: operation_id={}, operation_kind={}, timeout_ms={}, \
         abort_grace_ms={}, available_permits={}",
        operation_id,
        operation_kind.as_str(),
        timeout.as_millis(),
        BLOCKING_ABORT_GRACE.as_millis(),
        blocking_slots.available_permits()
    );
    let slot_wait_started = Instant::now();
    info!(
        operation_id,
        operation_kind = operation_kind.as_str(),
        deadline_remaining_ms = remaining_ms_until(deadline),
        available_permits = blocking_slots.available_permits(),
        "ducklake blocking operation waiting for semaphore slot: operation_id={}, \
         operation_kind={}, deadline_remaining_ms={}, available_permits={}",
        operation_id,
        operation_kind.as_str(),
        remaining_ms_until(deadline),
        blocking_slots.available_permits()
    );
    let permit = match tokio::time::timeout_at(
        deadline,
        Arc::clone(&blocking_slots).acquire_owned(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => {
            tracing::error!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                "ducklake blocking operation semaphore closed: operation_id={}, operation_kind={}",
                operation_id,
                operation_kind.as_str()
            );
            return Err(etl_error!(
                ErrorKind::ApplyWorkerPanic,
                "DuckLake blocking slot acquisition failed"
            ));
        }
        Err(_) => {
            warn!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                timeout_ms = timeout.as_millis() as u64,
                slot_wait_ms = slot_wait_started.elapsed().as_millis() as u64,
                "ducklake blocking operation timed out waiting for semaphore slot: \
                 operation_id={}, operation_kind={}, timeout_ms={}, slot_wait_ms={}",
                operation_id,
                operation_kind.as_str(),
                timeout.as_millis(),
                slot_wait_started.elapsed().as_millis()
            );
            return Err(duckdb_blocking_timeout_error(operation_kind, timeout, "slot_wait"));
        }
    };
    histogram!(ETL_DUCKLAKE_BLOCKING_SLOT_WAIT_SECONDS)
        .record(slot_wait_started.elapsed().as_secs_f64());
    info!(
        operation_id,
        operation_kind = operation_kind.as_str(),
        slot_wait_ms = slot_wait_started.elapsed().as_millis() as u64,
        deadline_remaining_ms = remaining_ms_until(deadline),
        "ducklake blocking operation acquired semaphore slot: operation_id={}, operation_kind={}, \
         slot_wait_ms={}, deadline_remaining_ms={}",
        operation_id,
        operation_kind.as_str(),
        slot_wait_started.elapsed().as_millis(),
        remaining_ms_until(deadline)
    );
    trace!(
        wait_ms = slot_wait_started.elapsed().as_millis() as u64,
        "wait for ducklake blocking slot"
    );

    // This is needed to make sure we properly interrupt the blocking operation if
    // it exceeds the timeout, we don't just cancel the task and leave the
    // connection active.
    let mut watchdog =
        DuckDbQueryWatchdog::spawn_with_context(deadline, operation_id, operation_kind, timeout);
    let watchdog_task = watchdog.async_task_handle()?;
    let watchdog_timed_out = Arc::clone(&watchdog.timed_out);
    let abort_deadline = deadline + BLOCKING_ABORT_GRACE;

    let blocking_task = tokio::task::spawn_blocking(move || -> EtlResult<R> {
        info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            deadline_remaining_ms = remaining_ms_until(deadline),
            "ducklake blocking operation entered spawn_blocking task: operation_id={}, \
             operation_kind={}, deadline_remaining_ms={}",
            operation_id,
            operation_kind.as_str(),
            remaining_ms_until(deadline)
        );
        // Please if you modify the code inside this blocking task do not add any
        // blocking operations that could delay other tasks waiting on this slot.
        let _permit = permit;
        let checkout_timeout =
            deadline.checked_duration_since(Instant::now()).unwrap_or(Duration::ZERO);
        if checkout_timeout.is_zero() {
            warn!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                "ducklake blocking operation deadline reached before pool checkout: \
                 operation_id={}, operation_kind={}",
                operation_id,
                operation_kind.as_str()
            );
            return Err(duckdb_blocking_timeout_error(operation_kind, timeout, "pool_checkout"));
        }
        let checkout_started = Instant::now();
        info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            checkout_timeout_ms = checkout_timeout.as_millis() as u64,
            "ducklake blocking operation checking out pooled connection: operation_id={}, \
             operation_kind={}, checkout_timeout_ms={}",
            operation_id,
            operation_kind.as_str(),
            checkout_timeout.as_millis()
        );
        let mut pooled_conn = match pool.get_timeout(checkout_timeout) {
            Ok(pooled_conn) => pooled_conn,
            Err(e) if Instant::now() >= deadline => {
                warn!(
                    operation_id,
                    operation_kind = operation_kind.as_str(),
                    checkout_wait_ms = checkout_started.elapsed().as_millis() as u64,
                    timeout_ms = timeout.as_millis() as u64,
                    error = %e,
                    "ducklake blocking operation timed out checking out pooled connection: \
                     operation_id={}, operation_kind={}, checkout_wait_ms={}, timeout_ms={}, error={}",
                    operation_id,
                    operation_kind.as_str(),
                    checkout_started.elapsed().as_millis(),
                    timeout.as_millis(),
                    e
                );
                return Err(duckdb_blocking_timeout_error(
                    operation_kind,
                    timeout,
                    "pool_checkout",
                ));
            }
            Err(e) => {
                warn!(
                    operation_id,
                    operation_kind = operation_kind.as_str(),
                    checkout_wait_ms = checkout_started.elapsed().as_millis() as u64,
                    error = %e,
                    "ducklake blocking operation failed checking out pooled connection: operation_id={}, \
                     operation_kind={}, checkout_wait_ms={}, error={}",
                    operation_id,
                    operation_kind.as_str(),
                    checkout_started.elapsed().as_millis(),
                    e
                );
                return Err(etl_error!(
                    ErrorKind::DestinationConnectionFailed,
                    "Failed to check out DuckLake connection",
                    source: e
                ));
            }
        };
        histogram!(ETL_DUCKLAKE_POOL_CHECKOUT_WAIT_SECONDS)
            .record(checkout_started.elapsed().as_secs_f64());
        info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            checkout_wait_ms = checkout_started.elapsed().as_millis() as u64,
            deadline_remaining_ms = remaining_ms_until(deadline),
            "ducklake blocking operation checked out pooled connection: operation_id={}, \
             operation_kind={}, checkout_wait_ms={}, deadline_remaining_ms={}",
            operation_id,
            operation_kind.as_str(),
            checkout_started.elapsed().as_millis(),
            remaining_ms_until(deadline)
        );
        trace!(
            wait_ms = checkout_started.elapsed().as_millis() as u64,
            "wait for ducklake pool checkout"
        );
        let operation_timeout =
            deadline.checked_duration_since(Instant::now()).unwrap_or(Duration::ZERO);
        if operation_timeout.is_zero() {
            warn!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                "ducklake blocking operation deadline reached before query execution: \
                 operation_id={}, operation_kind={}",
                operation_id,
                operation_kind.as_str()
            );
            return Err(duckdb_blocking_timeout_error(operation_kind, timeout, "query_execution"));
        }
        let interrupt_handle = pooled_conn.conn.interrupt_handle();
        info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            operation_timeout_ms = operation_timeout.as_millis() as u64,
            "ducklake blocking operation publishing interrupt handle before query execution: \
             operation_id={}, operation_kind={}, operation_timeout_ms={}",
            operation_id,
            operation_kind.as_str(),
            operation_timeout.as_millis()
        );
        watchdog.publish_interrupt_handle(interrupt_handle);
        if watchdog.timed_out() {
            warn!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                "ducklake blocking operation timed out before query started; marking pooled \
                 connection broken: operation_id={}, operation_kind={}",
                operation_id,
                operation_kind.as_str()
            );
            pooled_conn.broken = true;
            return Err(duckdb_blocking_timeout_error(operation_kind, timeout, "query_execution"));
        }
        let operation_started = Instant::now();
        info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            deadline_remaining_ms = remaining_ms_until(deadline),
            "ducklake blocking operation invoking DuckDB closure: operation_id={}, \
             operation_kind={}, deadline_remaining_ms={}",
            operation_id,
            operation_kind.as_str(),
            remaining_ms_until(deadline)
        );
        let res = operation(&pooled_conn.conn);
        let operation_duration_ms = operation_started.elapsed().as_millis() as u64;
        info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            duration_ms = operation_duration_ms,
            timed_out = watchdog.timed_out(),
            result_is_error = res.is_err(),
            "ducklake blocking operation DuckDB closure returned: operation_id={}, \
             operation_kind={}, duration_ms={}, timed_out={}, result_is_error={}",
            operation_id,
            operation_kind.as_str(),
            operation_duration_ms,
            watchdog.timed_out(),
            res.is_err()
        );
        watchdog.finish();
        histogram!(ETL_DUCKLAKE_BLOCKING_OPERATION_DURATION_SECONDS)
            .record(operation_started.elapsed().as_secs_f64());
        trace!(
            duration_ms = operation_started.elapsed().as_millis() as u64,
            "ducklake blocking operation finished"
        );
        if watchdog.timed_out() {
            warn!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                duration_ms = operation_duration_ms,
                "ducklake blocking operation returned after timeout; marking pooled connection \
                 broken: operation_id={}, operation_kind={}, duration_ms={}",
                operation_id,
                operation_kind.as_str(),
                operation_duration_ms
            );
            pooled_conn.broken = true;
            return Err(duckdb_blocking_timeout_error(operation_kind, timeout, "query_execution"));
        }
        if res.is_err() {
            warn!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                duration_ms = operation_duration_ms,
                "ducklake blocking operation returned error; marking pooled connection broken: \
                 operation_id={}, operation_kind={}, duration_ms={}",
                operation_id,
                operation_kind.as_str(),
                operation_duration_ms
            );
            pooled_conn.broken = true;
        } else {
            info!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                duration_ms = operation_duration_ms,
                "ducklake blocking operation returned success; pooled connection remains healthy: \
                 operation_id={}, operation_kind={}, duration_ms={}",
                operation_id,
                operation_kind.as_str(),
                operation_duration_ms
            );
        }

        res
    });

    info!(
        operation_id,
        operation_kind = operation_kind.as_str(),
        abort_deadline_remaining_ms = remaining_ms_until(abort_deadline),
        "ducklake blocking operation waiting for blocking task or abort deadline: \
         operation_id={}, operation_kind={}, abort_deadline_remaining_ms={}",
        operation_id,
        operation_kind.as_str(),
        remaining_ms_until(abort_deadline)
    );
    let blocking_result = tokio::select! {
        biased;
        result = blocking_task => result,
        _ = tokio::time::sleep_until(abort_deadline) => {
            // The blocking task still owns the pooled connection here, so the
            // async side cannot mark it broken and return it to r2d2 for
            // eviction. If DuckDB does not return after interrupt plus grace,
            // the stuck native call also keeps holding its semaphore permit and
            // blocking thread, so restarting the process is the recoverable
            // boundary.
            abort_stuck_duckdb_blocking_operation(
                operation_id,
                operation_kind,
                timeout,
                BLOCKING_ABORT_GRACE,
            );
        }
    };

    match &blocking_result {
        Ok(Ok(_)) => info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            timed_out = watchdog_timed_out.load(Ordering::Relaxed),
            "ducklake blocking operation task joined with success: operation_id={}, \
             operation_kind={}, timed_out={}",
            operation_id,
            operation_kind.as_str(),
            watchdog_timed_out.load(Ordering::Relaxed)
        ),
        Ok(Err(error)) => warn!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            timed_out = watchdog_timed_out.load(Ordering::Relaxed),
            error = ?error,
            "ducklake blocking operation task joined with error: operation_id={}, operation_kind={}, \
             timed_out={}, error={:?}",
            operation_id,
            operation_kind.as_str(),
            watchdog_timed_out.load(Ordering::Relaxed),
            error
        ),
        Err(error) => tracing::error!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            timed_out = watchdog_timed_out.load(Ordering::Relaxed),
            error = %error,
            "ducklake blocking operation task join failed: operation_id={}, operation_kind={}, \
             timed_out={}, error={}",
            operation_id,
            operation_kind.as_str(),
            watchdog_timed_out.load(Ordering::Relaxed),
            error
        ),
    }

    // Await the watchdog so it cannot outlive the finished blocking task and
    // accidentally interrupt a later operation that reuses the connection.
    info!(
        operation_id,
        operation_kind = operation_kind.as_str(),
        timed_out = watchdog_timed_out.load(Ordering::Relaxed),
        "ducklake blocking operation awaiting watchdog task: operation_id={}, operation_kind={}, \
         timed_out={}",
        operation_id,
        operation_kind.as_str(),
        watchdog_timed_out.load(Ordering::Relaxed)
    );
    match watchdog_task.await {
        Ok(()) => info!(
            operation_id,
            operation_kind = operation_kind.as_str(),
            timed_out = watchdog_timed_out.load(Ordering::Relaxed),
            "ducklake blocking operation watchdog task joined: operation_id={}, \
             operation_kind={}, timed_out={}",
            operation_id,
            operation_kind.as_str(),
            watchdog_timed_out.load(Ordering::Relaxed)
        ),
        Err(error) => {
            tracing::error!(
                operation_id,
                operation_kind = operation_kind.as_str(),
                timed_out = watchdog_timed_out.load(Ordering::Relaxed),
                error = %error,
                "ducklake blocking operation watchdog task panicked: operation_id={}, operation_kind={}, \
                 timed_out={}, error={}",
                operation_id,
                operation_kind.as_str(),
                watchdog_timed_out.load(Ordering::Relaxed),
                error
            );
            return Err(etl_error!(
                ErrorKind::ApplyWorkerPanic,
                "DuckLake query watchdog task panicked"
            ));
        }
    }

    blocking_result.map_err(|_| {
        etl_error!(ErrorKind::ApplyWorkerPanic, "DuckLake blocking operation task panicked")
    })?
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{Barrier, Semaphore, oneshot};

    use super::*;

    const WATCHDOG_DEADLINE: Duration = Duration::from_millis(20);
    const WATCHDOG_JOIN_TIMEOUT: Duration = Duration::from_secs(1);

    fn make_blocking_test_manager() -> DuckLakeConnectionManager {
        DuckLakeConnectionManager {
            setup_plan: Arc::new(DuckLakeSetupPlan::default()),
            disable_extension_autoload: cfg!(target_os = "linux"),
            interrupt_registry: Arc::new(DuckLakeInterruptRegistry::default()),
            #[cfg(feature = "test-utils")]
            open_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn watchdog_interrupt_handle() -> (duckdb::Connection, DuckDbQueryInterruptHandle) {
        let conn =
            duckdb::Connection::open_in_memory().expect("failed to open watchdog test connection");
        let handle = conn.interrupt_handle();
        (conn, handle)
    }

    struct DelayedInterruptHandle {
        delay: Duration,
        calls: std::sync::atomic::AtomicUsize,
        started: AtomicBool,
        completed: AtomicBool,
    }

    impl DelayedInterruptHandle {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                calls: std::sync::atomic::AtomicUsize::new(0),
                started: AtomicBool::new(false),
                completed: AtomicBool::new(false),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn started(&self) -> bool {
            self.started.load(Ordering::Relaxed)
        }

        fn completed(&self) -> bool {
            self.completed.load(Ordering::Relaxed)
        }
    }

    impl DuckDbQueryInterrupt for DelayedInterruptHandle {
        fn interrupt(&self) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.started.store(true, Ordering::Relaxed);
            std::thread::sleep(self.delay);
            self.completed.store(true, Ordering::Relaxed);
        }
    }

    fn delayed_interrupt_handle(
        delay: Duration,
    ) -> (Arc<DelayedInterruptHandle>, DuckDbQueryInterruptHandle) {
        let handle = Arc::new(DelayedInterruptHandle::new(delay));
        (Arc::clone(&handle), handle)
    }

    async fn wait_for_delayed_interrupt_to_start(handle: &DelayedInterruptHandle) {
        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, async {
            while !handle.started() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("delayed interrupt should start");
    }

    #[derive(Clone, Copy, Debug)]
    enum WatchdogSignal {
        PublishInterrupt,
        Finish,
        DropInterruptSender,
        DropDoneSender,
    }

    type WatchdogSignalCase =
        (&'static str, &'static [WatchdogSignal], bool, &'static [WatchdogSignal], bool);

    fn apply_watchdog_signal(
        watchdog: &mut DuckDbQueryWatchdog,
        handle: &DuckDbQueryInterruptHandle,
        signal: WatchdogSignal,
    ) {
        match signal {
            WatchdogSignal::PublishInterrupt => {
                watchdog.publish_query_interrupt_handle(Arc::clone(handle));
            }
            WatchdogSignal::Finish => {
                watchdog.finish();
            }
            WatchdogSignal::DropInterruptSender => {
                drop(watchdog.interrupt_tx.take());
            }
            WatchdogSignal::DropDoneSender => {
                drop(watchdog.done_tx.take());
            }
        }
    }

    async fn assert_watchdog_sequence_completes(
        name: &str,
        before_deadline: &[WatchdogSignal],
        wait_past_deadline: bool,
        after_deadline: &[WatchdogSignal],
        expected_timed_out: bool,
    ) {
        let (_conn, interrupt_handle) = watchdog_interrupt_handle();
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let watchdog_task = watchdog.async_task_handle().expect("failed to extract watchdog task");

        for signal in before_deadline {
            apply_watchdog_signal(&mut watchdog, &interrupt_handle, *signal);
            tokio::task::yield_now().await;
        }

        if wait_past_deadline {
            tokio::time::sleep(WATCHDOG_DEADLINE * 2).await;
        }

        for signal in after_deadline {
            apply_watchdog_signal(&mut watchdog, &interrupt_handle, *signal);
            tokio::task::yield_now().await;
        }

        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, watchdog_task)
            .await
            .unwrap_or_else(|_| panic!("watchdog sequence `{name}` deadlocked"))
            .unwrap_or_else(|error| panic!("watchdog sequence `{name}` panicked: {error}"));

        assert_eq!(
            watchdog.timed_out(),
            expected_timed_out,
            "unexpected timeout state for watchdog sequence `{name}`"
        );
    }

    #[test]
    fn duckdb_blocking_operation_kind_timeouts() {
        assert_eq!(DuckDbBlockingOperationKind::Foreground.timeout(), FOREGROUND_QUERY_TIMEOUT);
    }

    #[tokio::test]
    async fn run_duckdb_blocking_timeout_releases_resources_for_follow_up_queries() {
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
            |_conn| -> EtlResult<()> {
                std::thread::sleep(Duration::from_millis(100));
                Ok(())
            },
        )
        .await
        .expect_err("expected blocking timeout");

        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(error.description(), Some("DuckLake blocking operation timed out"));
        // This timeout budget covers pool checkout and query execution. Under
        // full-suite load the deadline can expire before the blocking operation
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
                conn.query_row("SELECT 1;", [], |row| row.get::<_, i64>(0)).map_err(|source| {
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
    fn format_query_error_detail_compacts_sql() {
        let sql = r#"CREATE TABLE lake."orders" ("id" INTEGER NOT NULL)"#;

        assert_eq!(
            format_query_error_detail(sql),
            "sql: CREATE TABLE lake.\"orders\" (\"id\" INTEGER NOT NULL)"
        );
    }

    #[test]
    fn redact_ducklake_connection_error_message_masks_quoted_password() {
        let message = "timed out waiting for connection: attach failed: postgres:host='localhost' \
                       user='ducklake' password='pa\\'ss\\\\word' dbname='catalog'";

        assert_eq!(
            redact_ducklake_connection_error_message(message),
            "timed out waiting for connection: attach failed: postgres:host='localhost' \
             user='ducklake' password='[redacted]' dbname='catalog'"
        );
    }

    #[test]
    fn redact_ducklake_connection_error_message_keeps_non_sensitive_messages() {
        let message = "timed out waiting for connection: parser error";

        assert_eq!(redact_ducklake_connection_error_message(message), message);
    }

    #[test]
    fn ducklake_connection_error_redacts_postgres_catalog_password() {
        let step = DuckLakeSetupStep {
            label: "attach_catalog",
            sql: "ATTACH 'ducklake:postgres:host=''localhost'' user=''ducklake'' \
                  password=''secret''' AS lake;"
                .to_owned(),
        };
        let error = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "attach failed: postgres:host='localhost' user='ducklake' password='secret'"
                    .to_owned(),
            ),
        );

        assert_eq!(
            DuckLakeConnectionError::setup_phase(&step, error).to_string(),
            "ducklake duckdb connection setup phase `attach_catalog` failed: attach failed: \
             postgres:host='localhost' user='ducklake' password='[redacted]'"
        );
    }

    #[tokio::test]
    async fn query_watchdog_signal_order_matrix_does_not_deadlock() {
        use WatchdogSignal::{DropDoneSender, DropInterruptSender, Finish, PublishInterrupt};

        let cases: &[WatchdogSignalCase] = &[
            ("finish_before_interrupt_handle", &[Finish], false, &[], false),
            ("done_sender_dropped_before_interrupt_handle", &[DropDoneSender], false, &[], false),
            ("interrupt_sender_dropped_before_deadline", &[DropInterruptSender], false, &[], false),
            (
                "interrupt_handle_then_finish_before_deadline",
                &[PublishInterrupt, Finish],
                false,
                &[],
                false,
            ),
            (
                "interrupt_handle_then_done_sender_drop_before_deadline",
                &[PublishInterrupt, DropDoneSender],
                false,
                &[],
                false,
            ),
            (
                "finish_then_interrupt_handle_before_deadline",
                &[Finish, PublishInterrupt],
                false,
                &[],
                false,
            ),
            (
                "interrupt_handle_before_deadline_then_deadline",
                &[PublishInterrupt],
                true,
                &[],
                true,
            ),
            ("deadline_then_interrupt_handle", &[], true, &[PublishInterrupt], true),
            ("deadline_then_finish", &[], true, &[Finish], true),
            ("deadline_then_interrupt_sender_drop", &[], true, &[DropInterruptSender], true),
            ("deadline_then_done_sender_drop", &[], true, &[DropDoneSender], true),
            (
                "deadline_then_both_senders_drop",
                &[],
                true,
                &[DropDoneSender, DropInterruptSender],
                true,
            ),
            (
                "interrupt_handle_before_deadline_then_finish_after_deadline",
                &[PublishInterrupt],
                true,
                &[Finish],
                true,
            ),
        ];

        for (name, before_deadline, wait_past_deadline, after_deadline, expected_timed_out) in cases
        {
            assert_watchdog_sequence_completes(
                name,
                before_deadline,
                *wait_past_deadline,
                after_deadline,
                *expected_timed_out,
            )
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_watchdog_concurrent_signal_races_do_not_deadlock() {
        for signal_after_deadline in [false, true] {
            for iteration in 0..50 {
                let (_conn, interrupt_handle) = watchdog_interrupt_handle();
                let deadline = if signal_after_deadline {
                    Instant::now() + WATCHDOG_DEADLINE
                } else {
                    Instant::now() + Duration::from_secs(1)
                };
                let mut watchdog = DuckDbQueryWatchdog::spawn(deadline);
                let interrupt_tx =
                    watchdog.interrupt_tx.take().expect("watchdog interrupt sender should exist");
                let done_tx = watchdog.done_tx.take().expect("watchdog done sender should exist");
                let watchdog_task =
                    watchdog.async_task_handle().expect("failed to extract watchdog task");
                let barrier = Arc::new(Barrier::new(3));

                let interrupt_sender = tokio::spawn({
                    let barrier = Arc::clone(&barrier);
                    let interrupt_handle = Arc::clone(&interrupt_handle);
                    async move {
                        barrier.wait().await;
                        let _ = interrupt_tx.send(interrupt_handle);
                    }
                });
                let done_sender = tokio::spawn({
                    let barrier = Arc::clone(&barrier);
                    async move {
                        barrier.wait().await;
                        let _ = done_tx.send(());
                    }
                });

                if signal_after_deadline {
                    tokio::time::sleep(WATCHDOG_DEADLINE * 2).await;
                }

                barrier.wait().await;

                tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, watchdog_task)
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "watchdog concurrent signal race deadlocked: \
                             signal_after_deadline={signal_after_deadline}, iteration={iteration}"
                        )
                    })
                    .unwrap_or_else(|error| {
                        panic!(
                            "watchdog concurrent signal race panicked: \
                             signal_after_deadline={signal_after_deadline}, \
                             iteration={iteration}, error={error}"
                        )
                    });
                interrupt_sender.await.expect("interrupt sender task should not panic");
                done_sender.await.expect("done sender task should not panic");

                assert_eq!(
                    watchdog.timed_out(),
                    signal_after_deadline,
                    "unexpected timeout state for concurrent signal race: \
                     signal_after_deadline={signal_after_deadline}, iteration={iteration}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_watchdog_slow_interrupt_before_deadline_does_not_deadlock() {
        let (delayed_handle, interrupt_handle) =
            delayed_interrupt_handle(Duration::from_millis(100));
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let watchdog_task = watchdog.async_task_handle().expect("failed to extract watchdog task");

        watchdog.publish_query_interrupt_handle(interrupt_handle);

        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, watchdog_task)
            .await
            .expect("watchdog with slow interrupt should not deadlock")
            .expect("watchdog task should not panic");

        assert!(watchdog.timed_out());
        assert_eq!(delayed_handle.calls(), 1);
        assert!(delayed_handle.completed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_watchdog_slow_interrupt_after_deadline_does_not_deadlock() {
        let (delayed_handle, interrupt_handle) =
            delayed_interrupt_handle(Duration::from_millis(100));
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let watchdog_task = watchdog.async_task_handle().expect("failed to extract watchdog task");

        tokio::time::sleep(WATCHDOG_DEADLINE * 2).await;
        watchdog.publish_query_interrupt_handle(interrupt_handle);

        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, watchdog_task)
            .await
            .expect("watchdog with slow late interrupt should not deadlock")
            .expect("watchdog task should not panic");

        assert!(watchdog.timed_out());
        assert_eq!(delayed_handle.calls(), 1);
        assert!(delayed_handle.completed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_watchdog_finish_while_slow_interrupt_is_running_does_not_deadlock() {
        let (delayed_handle, interrupt_handle) =
            delayed_interrupt_handle(Duration::from_millis(100));
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let watchdog_task = watchdog.async_task_handle().expect("failed to extract watchdog task");

        watchdog.publish_query_interrupt_handle(interrupt_handle);
        wait_for_delayed_interrupt_to_start(&delayed_handle).await;
        watchdog.finish();

        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, watchdog_task)
            .await
            .expect("watchdog should finish after slow interrupt returns")
            .expect("watchdog task should not panic");

        assert!(watchdog.timed_out());
        assert_eq!(delayed_handle.calls(), 1);
        assert!(delayed_handle.completed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_watchdog_slow_interrupt_starves_single_worker_runtime_until_it_returns() {
        let interrupt_delay = Duration::from_millis(100);
        let (delayed_handle, interrupt_handle) = delayed_interrupt_handle(interrupt_delay);
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let watchdog_task = watchdog.async_task_handle().expect("failed to extract watchdog task");
        let (observer_started_tx, observer_started_rx) = oneshot::channel();
        let started_at = std::time::Instant::now();
        let observer_task = tokio::spawn(async move {
            observer_started_tx.send(()).expect("observer start receiver should be open");
            tokio::time::sleep(WATCHDOG_DEADLINE * 2).await;
            std::time::Instant::now()
        });

        observer_started_rx.await.expect("observer should start");
        watchdog.publish_query_interrupt_handle(interrupt_handle);

        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, watchdog_task)
            .await
            .expect("watchdog should not deadlock on a single worker runtime")
            .expect("watchdog task should not panic");
        let observer_finished_at = observer_task.await.expect("observer task should not panic");

        assert!(watchdog.timed_out());
        assert_eq!(delayed_handle.calls(), 1);
        assert!(delayed_handle.completed());
        assert!(
            observer_finished_at.duration_since(started_at) >= interrupt_delay,
            "single worker runtime should not poll other async tasks while interrupt() blocks"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_watchdog_slow_interrupts_serialize_on_single_worker_runtime() {
        let interrupt_delay = Duration::from_millis(100);
        let (first_delayed_handle, first_interrupt_handle) =
            delayed_interrupt_handle(interrupt_delay);
        let (second_delayed_handle, second_interrupt_handle) =
            delayed_interrupt_handle(interrupt_delay);
        let mut first_watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let mut second_watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + WATCHDOG_DEADLINE);
        let first_watchdog_task =
            first_watchdog.async_task_handle().expect("failed to extract first watchdog task");
        let second_watchdog_task =
            second_watchdog.async_task_handle().expect("failed to extract second watchdog task");
        let started_at = std::time::Instant::now();

        first_watchdog.publish_query_interrupt_handle(first_interrupt_handle);
        second_watchdog.publish_query_interrupt_handle(second_interrupt_handle);

        tokio::time::timeout(WATCHDOG_JOIN_TIMEOUT, async {
            let (first_result, second_result) =
                tokio::join!(first_watchdog_task, second_watchdog_task);
            first_result.expect("first watchdog task should not panic");
            second_result.expect("second watchdog task should not panic");
        })
        .await
        .expect("serialized slow interrupts should not deadlock on a single worker runtime");

        assert!(first_watchdog.timed_out());
        assert!(second_watchdog.timed_out());
        assert_eq!(first_delayed_handle.calls(), 1);
        assert_eq!(second_delayed_handle.calls(), 1);
        assert!(first_delayed_handle.completed());
        assert!(second_delayed_handle.completed());
        assert!(
            started_at.elapsed() >= interrupt_delay * 2,
            "single worker runtime should serialize blocking interrupt() calls"
        );
    }

    #[tokio::test]
    async fn query_watchdog_marks_timeout_when_handle_arrives_after_deadline() {
        let conn = make_blocking_test_manager()
            .open_duckdb_connection()
            .expect("failed to open blocking test connection");
        let mut watchdog = DuckDbQueryWatchdog::spawn(Instant::now() + Duration::from_millis(10));
        let watchdog_task = watchdog.async_task_handle().expect("failed to extract watchdog task");

        tokio::time::sleep(Duration::from_millis(20)).await;
        watchdog.publish_interrupt_handle(conn.interrupt_handle());

        watchdog_task.await.expect("watchdog task should not panic");

        assert!(
            watchdog.timed_out(),
            "watchdog should remain timed out when the interrupt handle is published after the \
             deadline"
        );
    }

    #[tokio::test]
    async fn interrupt_all_connections_cancels_running_query() {
        let manager = make_blocking_test_manager();
        let pool = Arc::new(
            build_warm_ducklake_pool(manager.clone(), 1, "test")
                .await
                .expect("failed to build blocking test pool"),
        );
        let blocking_slots = Arc::new(Semaphore::new(1));
        let (query_started_tx, query_started_rx) = oneshot::channel();

        let query_task = tokio::spawn({
            let pool = Arc::clone(&pool);
            let blocking_slots = Arc::clone(&blocking_slots);
            async move {
                run_duckdb_blocking_with_timeout(
                    pool,
                    blocking_slots,
                    DuckDbBlockingOperationKind::Foreground,
                    Duration::from_secs(30),
                    move |conn| -> EtlResult<()> {
                        let _ = query_started_tx.send(());
                        conn.query_row(
                            "SELECT COUNT(*) FROM range(10000000) t1, range(1000000) t2;",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(|source| {
                            etl_error!(
                                ErrorKind::DestinationQueryFailed,
                                "DuckLake interrupt test query failed",
                                source: source
                            )
                        })?;
                        Ok(())
                    },
                )
                .await
            }
        });

        query_started_rx.await.expect("query should start before interrupt");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            manager.interrupt_all_connections() >= 1,
            "expected at least one registered connection to be interrupted"
        );

        let error = tokio::time::timeout(Duration::from_secs(5), query_task)
            .await
            .expect("interrupted query should finish promptly")
            .expect("interrupt test task should not panic")
            .expect_err("interrupted query should fail");

        assert_eq!(error.kind(), ErrorKind::DestinationQueryFailed);
        assert_eq!(error.description(), Some("DuckLake interrupt test query failed"));
    }
}
