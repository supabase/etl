//! Catalog metadata reads with a direct fallback when the pooler fails.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use etl_config::{DuckLakeCatalogConnectOptionsError, ducklake_catalog_metadata_connect_options};
use metrics::Counter;
use sqlx::{Error, PgPool, error::DatabaseError};
use tracing::warn;
use url::Url;

/// Describes a SQLx failure without exposing provider-controlled text.
fn metadata_error_category(error: &Error) -> &'static str {
    match error {
        Error::Configuration(_) => "Connection configuration error",
        Error::Database(_) => "Database error",
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
/// The first failed pooled read is retried through the direct pool. All clones
/// then use the direct pool until this reader is recreated, avoiding repeated
/// pooler delays and warnings. The direct pool retains its connection limit.
#[derive(Clone)]
pub struct DuckLakeMetadataPool {
    /// Existing direct pool, also shared with foreground catalog operations.
    direct: PgPool,
    /// Independently bounded pool used until a read fails.
    pooler: Option<PgPool>,
    /// Whether a pooled read has failed in any clone.
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

    /// Runs a read-only operation, retrying once on the direct pool after a
    /// pooled failure.
    ///
    /// The operation must include its entire transaction and must not write:
    /// an error can occur after the server has already executed the query.
    /// Callers retain transaction pinning and disable statement persistence for
    /// transaction-pooler compatibility. A direct failure is returned
    /// unchanged.
    pub async fn read<T, F, Fut>(&self, operation: &'static str, query: F) -> Result<T, Error>
    where
        F: Fn(PgPool) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        if let Some(pooler) = &self.pooler
            && !self.fallback.load(Ordering::Relaxed)
        {
            match query(pooler.clone()).await {
                Ok(result) => return Ok(result),
                Err(error) => {
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
        query(self.direct.clone()).await
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
    };

    use metrics::{Counter, atomics::AtomicU64};
    use sqlx::{
        Error, PgPool,
        postgres::{PgPoolOptions, PgSslMode},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_util::task::AbortOnDropHandle;
    use url::Url;

    use crate::ducklake::DuckLakeMetadataPool;

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

    /// Creates a lazy direct pool whose connection identity is observable by
    /// tests.
    fn direct_pool() -> PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://direct@direct.example.com/catalog?sslmode=require")
            .unwrap()
    }

    /// The pooler stays preferred while reads succeed and keeps its own
    /// options.
    #[tokio::test]
    async fn healthy_pooler_remains_preferred() {
        let direct = direct_pool();
        let url = Url::parse("postgres://reader@pooler.example.com:6543/catalog?sslmode=require")
            .unwrap();
        let fallbacks = Arc::new(AtomicU64::new(0));
        let pool = DuckLakeMetadataPool::new(direct.clone(), Some(&url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));
        for _ in 0..2 {
            pool.read("list_tables", |pool| async move {
                let options = pool.connect_options();
                assert_eq!(options.get_host(), "pooler.example.com");
                assert_eq!(options.get_port(), 6543);
                assert_eq!(options.get_username(), "reader");
                assert!(matches!(options.get_ssl_mode(), PgSslMode::Require));
                Ok(())
            })
            .await
            .unwrap();
        }
        pool.pooler.as_ref().unwrap().close().await;
        assert!(!direct.is_closed());
        assert_eq!(direct.options().get_max_connections(), 1);
        assert_eq!(fallbacks.load(Ordering::Relaxed), 0);
        let invalid = Url::parse("file:///tmp/catalog").unwrap();
        assert!(DuckLakeMetadataPool::new(direct, Some(&invalid)).is_err());
    }

    /// Unconfigured poolers share the direct pool and never retry direct
    /// errors.
    #[tokio::test]
    async fn direct_only_errors_are_returned_without_retry() {
        let direct = direct_pool();
        let fallbacks = Arc::new(AtomicU64::new(0));
        let pool = DuckLakeMetadataPool::new(direct.clone(), None)
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));
        let calls = AtomicUsize::new(0);
        let error = pool
            .read("list_tables", |pool| {
                calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    assert_eq!(pool.connect_options().get_host(), "direct.example.com");
                    Err::<(), _>(Error::RowNotFound)
                }
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::RowNotFound));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(fallbacks.load(Ordering::Relaxed), 0);
        pool.direct.close().await;
        assert!(direct.is_closed());
    }

    /// Replays the pooler's wire error and checks fallback, shared routing, and
    /// safe logs.
    #[tokio::test]
    async fn checkout_failure_falls_back_and_logs_without_provider_secrets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "postgres://reader@{}/catalog?sslmode=disable",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = AbortOnDropHandle::new(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let length = socket.read_u32().await.unwrap();
            let mut startup = vec![0; usize::try_from(length - 4).unwrap()];
            socket.read_exact(&mut startup).await.unwrap();
            // Include provider-controlled text that must never appear in logs.
            let body = b"SFATAL\0CECHECKOUTRETRIES\0Mplaceholder-secret postgres://reader:placeholder-token@example.com/catalog\0\0";
            socket.write_u8(b'E').await.unwrap();
            socket.write_u32(u32::try_from(body.len() + 4).unwrap()).await.unwrap();
            socket.write_all(body).await.unwrap();
        }));
        let buffer = LogBuffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let fallbacks = Arc::new(AtomicU64::new(0));
        let pool = DuckLakeMetadataPool::new(direct_pool(), Some(&url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));
        let calls = AtomicUsize::new(0);
        let value = pool
            .read("list_tables", |pool| {
                calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    if pool.connect_options().get_host() == "direct.example.com" {
                        Ok(42)
                    } else {
                        let _transaction = pool.begin().await?;
                        Ok(0)
                    }
                }
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(value, 42);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        pool.clone()
            .read("inline_size", |pool| async move {
                assert_eq!(pool.connect_options().get_host(), "direct.example.com");
                Ok(())
            })
            .await
            .unwrap();
        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logs.matches("falling back").count(), 1);
        assert!(logs.contains("list_tables"));
        assert!(logs.contains("ECHECKOUTRETRIES"));
        assert!(!logs.contains("placeholder-secret"));
        assert!(!logs.contains("placeholder-token"));
        assert!(!logs.contains("postgres://"));
        assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
    }

    /// A later failure switches routing too, and a failed direct retry remains
    /// an error.
    #[tokio::test]
    async fn later_pooler_failure_preserves_direct_error() {
        let url = Url::parse("postgres://reader@pooler.example.com/catalog").unwrap();
        let fallbacks = Arc::new(AtomicU64::new(0));
        let pool = DuckLakeMetadataPool::new(direct_pool(), Some(&url))
            .unwrap()
            .with_fallback_counter(Counter::from_arc(Arc::clone(&fallbacks)));
        pool.read("list_tables", |_| async { Ok(()) }).await.unwrap();
        let calls = AtomicUsize::new(0);
        let error = pool
            .read("table_storage_metrics", |pool| {
                calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    if pool.connect_options().get_host() == "pooler.example.com" {
                        Err::<(), _>(Error::PoolTimedOut)
                    } else {
                        Err(Error::RowNotFound)
                    }
                }
            })
            .await
            .unwrap_err();
        assert!(matches!(error, Error::RowNotFound));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(pool.fallback.load(Ordering::Relaxed));
        assert_eq!(fallbacks.load(Ordering::Relaxed), 1);
    }
}
