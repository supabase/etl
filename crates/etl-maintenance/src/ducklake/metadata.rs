//! Catalog metadata reads with a direct fallback when the pooler fails.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use etl_config::{DuckLakeCatalogConnectOptionsError, ducklake_catalog_metadata_connect_options};
use metrics::Counter;
use sqlx::{
    AssertSqlSafe, Connection, Error, PgConnection, PgPool, Postgres, error::DatabaseError,
    pool::PoolConnection,
};
use tokio::time::timeout;
use tracing::warn;
use url::Url;

use crate::ducklake::metrics::record_metadata_read_error;

/// Caps the time a metadata attempt can occupy a checked-out connection.
const METADATA_READ_TIMEOUT: Duration = Duration::from_secs(7);
/// Stops server work after client cancellation; keep below the read deadline
/// to leave time for the response and connection cleanup.
const METADATA_STATEMENT_TIMEOUT: Duration = Duration::from_secs(6);

/// A metadata query future borrowing the connection and its inputs.
type MetadataQueryFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Drops an interrupted connection without flushing SQLx's queued rollback.
struct MetadataConnection(Option<PoolConnection<Postgres>>);

impl Drop for MetadataConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.0.take() {
            drop(connection.detach());
        }
    }
}

/// Executes one metadata attempt with separate checkout and read deadlines.
async fn read_metadata<T, Args>(
    pool: &PgPool,
    read_timeout: Duration,
    args: &Args,
    query: impl for<'a> FnOnce(&'a mut PgConnection, &'a Args) -> MetadataQueryFuture<'a, T>,
) -> Result<T, Error> {
    let mut checked_out = MetadataConnection(Some(pool.acquire().await?));
    timeout(read_timeout, async {
        let connection = checked_out.0.as_mut().expect("Metadata connection was just acquired");
        let mut transaction = connection.begin().await?;

        let statement_timeout_sql =
            format!("set local statement_timeout = '{}ms'", METADATA_STATEMENT_TIMEOUT.as_millis());
        sqlx::raw_sql(AssertSqlSafe(statement_timeout_sql)).execute(&mut *transaction).await?;

        let result = query(&mut transaction, args).await?;
        transaction.commit().await?;

        // SQLx's default Drop spawns an unbounded pool-return ping. Its hidden
        // return future keeps the connection and slot under our deadline.
        let return_to_pool = connection.return_to_pool();
        checked_out.0 = None;
        return_to_pool.await;

        Ok(result)
    })
    .await
    .map_err(|_| {
        Error::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "DuckLake catalog metadata read timed out",
        ))
    })?
}

/// Describes a SQLx failure without exposing provider-controlled text.
fn metadata_error_category(error: &Error) -> &'static str {
    match error {
        Error::Configuration(_) => "Connection configuration error",
        Error::Database(_) => "Database error",
        Error::Io(error) if error.kind() == io::ErrorKind::TimedOut => "Metadata read timed out",
        Error::Io(_) => "Connection I/O error",
        Error::Tls(_) => "TLS connection error",
        Error::Protocol(_) => "Protocol error",
        Error::PoolTimedOut => "Connection pool timeout",
        Error::PoolClosed => "Connection pool closed",
        Error::WorkerCrashed => "Connection worker crashed",
        _ => "Metadata query error",
    }
}

/// Shared routing for read-only DuckLake catalog queries.
///
/// Except for server query cancellations, the first failed pooled read is
/// retried through the direct pool. All clones then use the direct pool until
/// this reader is recreated, avoiding repeated pooler delays and warnings.
/// The direct pool retains its connection limit.
#[derive(Clone)]
pub struct DuckLakeMetadataPool {
    /// Existing direct pool, also shared with foreground catalog operations.
    direct: PgPool,
    /// Independently bounded pool used until fallback is required.
    pooler: Option<PgPool>,
    /// Whether any clone has switched to direct reads.
    fallback: Arc<AtomicBool>,
    /// Optional caller-owned metric for the first transition to direct reads.
    fallback_counter: Counter,
}

impl DuckLakeMetadataPool {
    /// Builds a lazy pooler connection while preserving direct connection
    /// options.
    pub fn new(
        direct: PgPool,
        pooler_url: Option<&Url>,
    ) -> Result<Self, DuckLakeCatalogConnectOptionsError> {
        let pooler = pooler_url
            .map(|url| {
                let options = ducklake_catalog_metadata_connect_options(url)?;
                // PgBouncer rejects untracked startup parameters by default.
                Ok(direct.options().clone().connect_lazy_with(options.extra_float_digits(None)))
            })
            .transpose()?;
        Ok(Self {
            direct,
            pooler,
            fallback: Arc::new(AtomicBool::new(false)),
            fallback_counter: Counter::noop(),
        })
    }

    /// Attaches a counter before cloning the reader and publishes its initial
    /// value without resetting an existing count. Any clone switching to
    /// direct reads increments it once.
    pub fn with_fallback_counter(mut self, counter: Counter) -> Self {
        counter.increment(0);
        self.fallback_counter = counter;
        self
    }

    /// Runs a read-only query in a pinned transaction, retrying once on the
    /// direct pool after a pooled failure other than query cancellation.
    ///
    /// Each attempt has seven seconds after checkout to run the transaction and
    /// release the connection. A transaction-local six-second statement
    /// timeout also stops server work after a client disconnect. The query
    /// must be read-only and disable statement persistence for transaction
    /// poolers. Keep borrowed query inputs in `args` so the callback can borrow
    /// them alongside the connection.
    pub async fn read<T, Args>(
        &self,
        operation: &'static str,
        args: Args,
        query: impl for<'a> Fn(&'a mut PgConnection, &'a Args) -> MetadataQueryFuture<'a, T>,
    ) -> Result<T, Error> {
        self.read_with_timeout(operation, METADATA_READ_TIMEOUT, args, query).await
    }

    /// Routes an operation through the preferred endpoint with the given read
    /// deadline.
    async fn read_with_timeout<T, Args>(
        &self,
        operation: &'static str,
        read_timeout: Duration,
        args: Args,
        query: impl for<'a> Fn(&'a mut PgConnection, &'a Args) -> MetadataQueryFuture<'a, T>,
    ) -> Result<T, Error> {
        if let Some(pooler) = &self.pooler
            && !self.fallback.load(Ordering::Relaxed)
        {
            match read_metadata(pooler, read_timeout, &args, &query).await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    record_metadata_read_error("pooler", &error);

                    // A canceled query does not indicate an unhealthy pooler.
                    if error.as_database_error().and_then(DatabaseError::code).as_deref()
                        == Some("57014")
                    {
                        return Err(error);
                    }

                    if !self.fallback.swap(true, Ordering::Relaxed) {
                        self.fallback_counter.increment(1);
                        // Provider messages can echo credentials or row values.
                        // Keep the category and code without their raw text.
                        warn!(
                            operation,
                            error = %metadata_error_category(&error),
                            sqlstate = error.as_database_error().and_then(DatabaseError::code).as_deref(),
                            "ducklake catalog pooler read failed; falling back to the direct catalog connection"
                        );
                    }
                }
            }
        }
        read_metadata(&self.direct, read_timeout, &args, &query)
            .await
            .inspect_err(|error| record_metadata_read_error("direct", error))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use etl_telemetry::metrics::init_metrics_handle;
    use metrics::{Counter, atomics::AtomicU64};
    use sqlx::{
        Connection, Error, PgConnection, PgPool,
        postgres::{PgPoolOptions, PgSslMode},
    };
    use tokio::{
        io::{self as tokio_io, AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::Notify,
        time::timeout,
    };
    use tokio_util::task::AbortOnDropHandle;
    use url::Url;

    use crate::ducklake::{
        DuckLakeMetadataPool,
        metadata::{METADATA_STATEMENT_TIMEOUT, MetadataQueryFuture},
        metrics::ETL_DUCKLAKE_CATALOG_METADATA_READ_ERRORS_TOTAL,
    };

    /// Deadline for exercising stalled reads without using the production
    /// budget.
    const TEST_READ_TIMEOUT: Duration = Duration::from_secs(1);
    /// Fails a stall test promptly if the reader never resolves.
    const TEST_STALL_LIMIT: Duration = Duration::from_secs(5);

    /// Captures emitted warnings without installing a process-wide subscriber.
    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Determines where a test backend stops responding.
    #[derive(Clone, Copy)]
    enum BackendResponse {
        Healthy,
        FailSecondQuery,
        CancelQuery,
        StallQuery,
        StallReturn,
    }

    /// Runs the SQLx wire exchanges needed to test a metadata attempt.
    struct TestBackend {
        url: Url,
        stalled: Arc<Notify>,
        _task: AbortOnDropHandle<()>,
    }

    impl TestBackend {
        /// Starts a listener with one scripted response per accepted
        /// connection.
        async fn start<const N: usize>(responses: [BackendResponse; N]) -> Self {
            let (listener, url) = listen().await;
            let stalled = Arc::new(Notify::new());
            let signal = Arc::clone(&stalled);
            let task = AbortOnDropHandle::new(tokio::spawn(async move {
                let statement_timeout_sql = format!(
                    "set local statement_timeout = '{}ms'\0",
                    METADATA_STATEMENT_TIMEOUT.as_millis()
                );

                for response in responses {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    read_body(&mut socket).await;
                    frame(&mut socket, b'R', &0_u32.to_be_bytes()).await;
                    frame(&mut socket, b'S', b"server_version\x0016.0\0").await;
                    frame(&mut socket, b'Z', b"I").await;

                    let mut committed = false;
                    let mut queried = false;
                    while let Ok(tag) = socket.read_u8().await {
                        let body = read_body(&mut socket).await;
                        match (tag, body.as_slice()) {
                            (b'Q', b"BEGIN\0") => {
                                frame(&mut socket, b'C', b"BEGIN\0").await;
                                frame(&mut socket, b'Z', b"T").await;
                            }
                            (b'Q', body) if body == statement_timeout_sql.as_bytes() => {
                                frame(&mut socket, b'C', b"SET\0").await;
                                frame(&mut socket, b'Z', b"T").await;
                            }
                            (b'Q', b"select 42\0")
                                if matches!(response, BackendResponse::StallQuery) =>
                            {
                                wait_for_disconnect(&mut socket, &signal).await;
                                break;
                            }
                            (b'Q', b"select 42\0")
                                if queried
                                    && matches!(response, BackendResponse::FailSecondQuery) =>
                            {
                                frame(
                                    &mut socket,
                                    b'E',
                                    b"SERROR\0C08006\0MSynthetic unavailable\0\0",
                                )
                                .await;
                                frame(&mut socket, b'Z', b"E").await;
                            }
                            (b'Q', b"select 42\0")
                                if matches!(response, BackendResponse::CancelQuery) =>
                            {
                                frame(
                                    &mut socket,
                                    b'E',
                                    b"SERROR\0C57014\0MSynthetic query canceled\0\0",
                                )
                                .await;
                                frame(&mut socket, b'Z', b"E").await;
                            }
                            (b'Q', b"select 42\0") => {
                                frame(&mut socket, b'C', b"SELECT 1\0").await;
                                frame(&mut socket, b'Z', b"T").await;
                                queried = true;
                            }
                            (b'Q', b"COMMIT\0") => {
                                frame(&mut socket, b'C', b"COMMIT\0").await;
                                frame(&mut socket, b'Z', b"I").await;
                                committed = true;
                            }
                            (b'S', b"")
                                if committed
                                    && matches!(response, BackendResponse::StallReturn) =>
                            {
                                wait_for_disconnect(&mut socket, &signal).await;
                                break;
                            }
                            (b'S', b"") => frame(&mut socket, b'Z', b"I").await,
                            (b'X', b"") => break,
                            _ => panic!("Unexpected frontend message"),
                        }
                    }
                }
            }));

            Self { url, stalled, _task: task }
        }

        /// Creates a one-connection pool pointed at this backend.
        fn pool(&self) -> PgPool {
            PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(TEST_READ_TIMEOUT)
                .connect_lazy(self.url.as_str())
                .unwrap()
        }
    }

    /// Binds a plaintext PostgreSQL test endpoint.
    async fn listen() -> (TcpListener, Url) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "postgres://reader@{}/catalog?sslmode=disable",
            listener.local_addr().unwrap()
        ))
        .unwrap();

        (listener, url)
    }

    /// Reads one length-prefixed message.
    async fn read_body(socket: &mut TcpStream) -> Vec<u8> {
        let length = socket.read_u32().await.unwrap();
        let mut body = vec![0; usize::try_from(length - 4).unwrap()];
        socket.read_exact(&mut body).await.unwrap();

        body
    }

    /// Sends a PostgreSQL backend frame.
    async fn frame(socket: &mut TcpStream, tag: u8, body: &[u8]) {
        socket.write_u8(tag).await.unwrap();
        socket.write_u32(u32::try_from(body.len() + 4).unwrap()).await.unwrap();
        socket.write_all(body).await.unwrap();
    }

    /// Leaves the connection open without sending a response.
    async fn wait_for_disconnect(socket: &mut TcpStream, stalled: &Notify) {
        stalled.notify_one();
        tokio_io::copy(socket, &mut tokio_io::sink()).await.unwrap();
    }

    /// Executes a real SQLx query and returns its command result.
    fn select<'a>(connection: &'a mut PgConnection, _: &'a ()) -> MetadataQueryFuture<'a, u64> {
        Box::pin(async move {
            Ok(sqlx::raw_sql("select 42").execute(connection).await?.rows_affected())
        })
    }

    /// Checks every exported metadata error series, regardless of label order.
    fn assert_read_errors(rendered: &str, expected: &[(&str, &str, u64)]) {
        let samples: Vec<_> = rendered
            .lines()
            .filter(|line| line.starts_with(ETL_DUCKLAKE_CATALOG_METADATA_READ_ERRORS_TOTAL))
            .collect();

        assert_eq!(samples.len(), expected.len());

        for (route, reason, count) in expected {
            let sample = samples
                .iter()
                .find(|line| {
                    line.contains(&format!("route=\"{route}\""))
                        && line.contains(&format!("reason=\"{reason}\""))
                })
                .unwrap();

            assert_eq!(sample.split_whitespace().last().unwrap(), count.to_string());
        }
    }

    /// Healthy reads reuse the pooler connection and keep its own options.
    #[tokio::test]
    async fn healthy_pooler_remains_preferred() {
        let metrics = init_metrics_handle().unwrap();
        let backend = TestBackend::start([BackendResponse::Healthy]).await;
        let direct = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://direct@direct.example.com/catalog?sslmode=require")
            .unwrap();
        let fallbacks = Arc::new(AtomicU64::new(0));
        let reader = DuckLakeMetadataPool::new(direct.clone(), Some(&backend.url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));
        let pooler = reader.pooler.as_ref().unwrap();
        let options = pooler.connect_options();

        assert_eq!(options.get_host(), "127.0.0.1");
        assert_eq!(options.get_port(), backend.url.port().unwrap());
        assert_eq!(options.get_username(), "reader");
        assert!(matches!(options.get_ssl_mode(), PgSslMode::Disable));
        for _ in 0..2 {
            assert_eq!(reader.read("list_tables", (), select).await.unwrap(), 1);
            assert_eq!(pooler.num_idle(), 1);
        }

        pooler.close().await;
        assert!(!direct.is_closed());
        assert_eq!(direct.options().get_max_connections(), 1);
        assert_eq!(fallbacks.load(Ordering::Relaxed), 0);
        assert_read_errors(&metrics.render(), &[]);

        let invalid = Url::parse("file:///tmp/catalog").unwrap();
        assert!(DuckLakeMetadataPool::new(direct, Some(&invalid)).is_err());
    }

    /// Server cancellations are counted without retrying directly or changing
    /// the route used by subsequent reads through a clone.
    #[tokio::test]
    async fn canceled_pooler_query_keeps_pooler_preferred() {
        let metrics = init_metrics_handle().unwrap();
        let backend =
            TestBackend::start([BackendResponse::CancelQuery, BackendResponse::Healthy]).await;
        let direct = backend.pool();
        direct.close().await;
        let fallbacks = Arc::new(AtomicU64::new(0));
        let reader = DuckLakeMetadataPool::new(direct, Some(&backend.url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));

        let error = reader.read("list_tables", (), select).await.unwrap_err();

        assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("57014"));
        assert_eq!(reader.clone().read("list_tables", (), select).await.unwrap(), 1);
        assert_eq!(fallbacks.load(Ordering::Relaxed), 0);
        assert_read_errors(&metrics.render(), &[("pooler", "query_canceled", 1)]);
    }

    /// Direct errors are returned to the caller without a retry.
    #[tokio::test]
    async fn direct_only_errors_are_returned_without_retry() {
        let metrics = init_metrics_handle().unwrap();
        let backend = TestBackend::start([BackendResponse::Healthy]).await;
        let reader = DuckLakeMetadataPool::new(backend.pool(), None).unwrap();
        let calls = AtomicUsize::new(0);

        let error = reader
            .read("list_tables", &calls, |_, calls| {
                calls.fetch_add(1, Ordering::Relaxed);
                Box::pin(async { Err::<(), _>(Error::RowNotFound) })
            })
            .await
            .unwrap_err();

        assert!(matches!(error, Error::RowNotFound));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_read_errors(&metrics.render(), &[("direct", "other", 1)]);
    }

    /// Replays the pooler's wire error and checks fallback, shared routing, and
    /// safe logs.
    #[tokio::test]
    async fn checkout_failure_falls_back_and_logs_without_provider_secrets() {
        let metrics = init_metrics_handle().unwrap();
        let (listener, url) = listen().await;
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_body(&mut socket).await;
            // Include provider-controlled text that must never appear in logs.
            let body = b"SFATAL\0CECHECKOUTRETRIES\0Mplaceholder-secret postgres://reader:placeholder-token@example.com/catalog\0\0";
            frame(&mut socket, b'E', body).await;
        }));
        let direct = TestBackend::start([BackendResponse::Healthy]).await;
        let buffer = LogBuffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let fallbacks = Arc::new(AtomicU64::new(0));
        let reader = DuckLakeMetadataPool::new(direct.pool(), Some(&url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));

        assert_eq!(reader.read("list_tables", (), select).await.unwrap(), 1);
        server.await.unwrap();
        assert_eq!(reader.clone().read("inline_size", (), select).await.unwrap(), 1);

        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("falling back").count(), 1);
        assert!(logs.contains("list_tables"));
        assert!(logs.contains("ECHECKOUTRETRIES"));
        assert!(!logs.contains("placeholder-secret"));
        assert!(!logs.contains("placeholder-token"));
        assert!(!logs.contains("postgres://"));
        assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
        assert_read_errors(&metrics.render(), &[("pooler", "other", 1)]);
    }

    /// A returned query error switches routing and preserves a direct retry
    /// error.
    #[tokio::test]
    async fn later_pooler_failure_preserves_direct_error() {
        let metrics = init_metrics_handle().unwrap();
        let backend = TestBackend::start([BackendResponse::FailSecondQuery]).await;
        let direct = backend.pool();
        direct.close().await;
        let fallbacks = Arc::new(AtomicU64::new(0));
        let reader = DuckLakeMetadataPool::new(direct, Some(&backend.url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));

        assert_eq!(reader.read("list_tables", (), select).await.unwrap(), 1);
        let error = reader.read("list_tables", (), select).await.unwrap_err();

        assert!(matches!(error, Error::PoolClosed));
        assert_eq!(reader.pooler.as_ref().unwrap().size(), 0);
        assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
        assert_read_errors(&metrics.render(), &[("pooler", "other", 1), ("direct", "other", 1)]);
    }

    /// A checkout timeout is counted separately and does not prevent recovery.
    #[tokio::test]
    async fn checkout_timeout_is_counted() {
        let metrics = init_metrics_handle().unwrap();
        let backend = TestBackend::start([BackendResponse::Healthy]).await;
        let direct = backend.pool();
        let reader = DuckLakeMetadataPool::new(direct.clone(), None).unwrap();
        let checked_out = direct.acquire().await.unwrap();

        let error = reader.read("list_tables", (), select).await.unwrap_err();

        assert!(matches!(error, Error::PoolTimedOut));
        assert_read_errors(&metrics.render(), &[("direct", "checkout_timeout", 1)]);

        drop(checked_out);

        assert_eq!(reader.read("list_tables", (), select).await.unwrap(), 1);
        assert_read_errors(&metrics.render(), &[("direct", "checkout_timeout", 1)]);
    }

    /// A stalled pooler triggers fallback once, while repeated direct timeouts
    /// remain observable across clones.
    #[tokio::test]
    async fn stalled_pooler_read_falls_back_once() {
        let metrics = init_metrics_handle().unwrap();
        let pooler = TestBackend::start([BackendResponse::StallQuery]).await;
        let direct = TestBackend::start([
            BackendResponse::StallQuery,
            BackendResponse::StallQuery,
            BackendResponse::Healthy,
        ])
        .await;
        let fallbacks = Arc::new(AtomicU64::new(0));
        let reader = DuckLakeMetadataPool::new(direct.pool(), Some(&pooler.url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));

        for direct_timeouts in 1..=2 {
            let error = timeout(
                TEST_STALL_LIMIT,
                reader.clone().read_with_timeout("list_tables", TEST_READ_TIMEOUT, (), select),
            )
            .await
            .unwrap()
            .unwrap_err();

            assert!(matches!(error, Error::Io(error) if error.kind() == io::ErrorKind::TimedOut));
            assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
            assert_read_errors(
                &metrics.render(),
                &[("pooler", "client_timeout", 1), ("direct", "client_timeout", direct_timeouts)],
            );
        }

        timeout(TEST_READ_TIMEOUT, pooler.stalled.notified()).await.unwrap();
        assert_eq!(reader.pooler.as_ref().unwrap().size(), 0);
        assert_eq!(reader.clone().read("inline_size", (), select).await.unwrap(), 1);
        assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
        assert_read_errors(
            &metrics.render(),
            &[("pooler", "client_timeout", 1), ("direct", "client_timeout", 2)],
        );
    }

    /// Timeouts and cancellation release the direct slot during query
    /// execution and pool return.
    #[tokio::test]
    async fn interrupted_direct_reads_release_capacity() {
        let metrics = init_metrics_handle().unwrap();
        let mut timeouts = 0;

        for cancel_read in [false, true] {
            for stall in [BackendResponse::StallQuery, BackendResponse::StallReturn] {
                let backend = TestBackend::start([stall, BackendResponse::Healthy]).await;
                let direct = backend.pool();
                let reader = DuckLakeMetadataPool::new(direct.clone(), None).unwrap();

                if cancel_read {
                    let read = reader.read("list_tables", (), select);
                    tokio::pin!(read);
                    tokio::select! {
                        stalled = timeout(TEST_STALL_LIMIT, backend.stalled.notified()) => stalled.unwrap(),

                        result = &mut read => panic!("Read finished before cancellation: {result:?}"),
                    }
                } else {
                    let error = timeout(
                        TEST_STALL_LIMIT,
                        reader.read_with_timeout("list_tables", TEST_READ_TIMEOUT, (), select),
                    )
                    .await
                    .unwrap()
                    .unwrap_err();

                    assert!(
                        matches!(error, Error::Io(error) if error.kind() == io::ErrorKind::TimedOut)
                    );
                    timeout(TEST_READ_TIMEOUT, backend.stalled.notified()).await.unwrap();
                    timeouts += 1;
                }

                assert_read_errors(&metrics.render(), &[("direct", "client_timeout", timeouts)]);
                assert_eq!(direct.size(), 0);
                direct.acquire().await.unwrap().ping().await.unwrap();
            }
        }
    }
}
