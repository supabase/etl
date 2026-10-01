//! Named SQLx pools with query duration and count metrics.
//!
//! Each polled request records one sample on completion or cancellation,
//! including pool wait and result consumption. Explicit BEGIN and COMMIT
//! requests count separately; implicit SQLx operations, including rollback on
//! drop, do not. Raw SQL batches count once, and unpolled requests do not
//! count. Labels contain only the fixed pool name.

use std::{task::Poll, time::Instant};

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::BoxStream};
use metrics::{Histogram, histogram};
use sqlx::{
    Describe, Either, Execute, Executor, PgConnection, PgPool, Postgres, SqlStr, Transaction,
    postgres::{PgQueryResult, PgRow, PgStatement, PgTypeInfo},
};

use crate::observability::{ETL_POSTGRES_QUERY_DURATION_SECONDS, POOL_LABEL, register_metrics};

/// Records one sample when a request finishes or is cancelled.
#[derive(Debug)]
struct QueryTimer {
    /// Registered histogram for the originating pool.
    duration: Histogram,
    /// Request start, before pool acquisition or execution.
    started_at: Instant,
}

impl QueryTimer {
    /// Starts timing one request.
    fn new(duration: Histogram) -> Self {
        Self { duration, started_at: Instant::now() }
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        self.duration.record(self.started_at.elapsed().as_secs_f64());
    }
}

/// Instruments SQLx requests through a pool or transaction connection.
#[derive(Debug)]
pub(crate) struct InstrumentedPgExecutor<E> {
    /// Wrapped executor.
    inner: E,
    /// Registered histogram reused without a per-query metric lookup.
    duration: Histogram,
}

impl<'c, E: Executor<'c, Database = Postgres> + 'c> Executor<'c> for InstrumentedPgExecutor<E> {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, Q>(
        self,
        query: Q,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, sqlx::Error>>
    where
        'c: 'e,
        Q: 'q + Execute<'q, Postgres>,
    {
        // Start on first poll, then keep one timer for the whole result stream.
        futures::stream::once(async move {
            let mut timer = Some(QueryTimer::new(self.duration));
            let mut stream = self.inner.fetch_many(query);
            futures::stream::poll_fn(move |cx| {
                let result = stream.as_mut().poll_next(cx);
                if matches!(result, Poll::Ready(None | Some(Err(_)))) {
                    timer.take();
                }

                result
            })
        })
        .flatten()
        .boxed()
    }

    fn fetch_optional<'e, 'q: 'e, Q>(
        self,
        query: Q,
    ) -> BoxFuture<'e, Result<Option<PgRow>, sqlx::Error>>
    where
        'c: 'e,
        Q: 'q + Execute<'q, Postgres>,
    {
        async move {
            let _timer = QueryTimer::new(self.duration);

            self.inner.fetch_optional(query).await
        }
        .boxed()
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement, sqlx::Error>>
    where
        'c: 'e,
    {
        async move {
            let _timer = QueryTimer::new(self.duration);

            self.inner.prepare_with(sql, parameters).await
        }
        .boxed()
    }

    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Postgres>, sqlx::Error>>
    where
        'c: 'e,
    {
        async move {
            let _timer = QueryTimer::new(self.duration);

            self.inner.describe(sql).await
        }
        .boxed()
    }
}

/// A SQLx pool with request durations labeled by pool name.
#[derive(Debug, Clone)]
pub(crate) struct InstrumentedPgPool {
    /// Pool kept private to prevent accidentally bypassing instrumentation.
    inner: PgPool,
    /// Registered histogram reused by this pool and its transactions.
    duration: Histogram,
}

impl InstrumentedPgPool {
    /// Wraps an existing pool without changing its connection settings.
    ///
    /// Initialize the metrics recorder first; the histogram handle is cached.
    pub(crate) fn new(inner: PgPool, name: &'static str) -> Self {
        register_metrics();

        Self {
            inner,
            duration: histogram!(ETL_POSTGRES_QUERY_DURATION_SECONDS, POOL_LABEL => name),
        }
    }

    /// Returns an instrumented executor for one request through the pool.
    pub(crate) fn executor(&self) -> InstrumentedPgExecutor<&PgPool> {
        InstrumentedPgExecutor { inner: &self.inner, duration: self.duration.clone() }
    }

    /// Begins a transaction whose statements retain this pool's metric name.
    pub(crate) async fn begin(&self) -> Result<InstrumentedPgTransaction, sqlx::Error> {
        let _timer = QueryTimer::new(self.duration.clone());
        let inner = self.inner.begin().await?;

        Ok(InstrumentedPgTransaction { inner, duration: self.duration.clone() })
    }
}

/// A transaction that retains its originating pool's query instrumentation.
#[derive(Debug)]
pub(crate) struct InstrumentedPgTransaction {
    /// SQLx transaction, including its normal rollback-on-drop behavior.
    inner: Transaction<'static, Postgres>,
    /// Registered histogram inherited from the pool.
    duration: Histogram,
}

impl InstrumentedPgTransaction {
    /// Returns an instrumented executor borrowing this transaction.
    pub(crate) fn executor(&mut self) -> InstrumentedPgExecutor<&mut PgConnection> {
        InstrumentedPgExecutor { inner: &mut self.inner, duration: self.duration.clone() }
    }

    /// Commits the transaction and records its duration.
    pub(crate) async fn commit(self) -> Result<(), sqlx::Error> {
        let _timer = QueryTimer::new(self.duration);

        self.inner.commit().await
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use etl_postgres::source::connect_to_source_database;
    use etl_telemetry::metrics::init_metrics_handle;
    use futures::TryStreamExt;
    use sqlx::{Executor, Statement};

    use crate::{
        observability::{ETL_POSTGRES_QUERY_DURATION_SECONDS, POOL_LABEL},
        postgres::pool::InstrumentedPgPool,
        test_utils::database::spawn_source_database,
    };

    /// Distinct fixed pool names used to verify metric attribution.
    const TEST_STORE_POOL: &str = "test_store";

    /// Second pool name sharing the same underlying SQLx pool.
    const TEST_OUT_OF_BAND_POOL: &str = "test_out_of_band";

    /// Pool name used for failed and abandoned requests.
    const TEST_QUERY_POOL: &str = "test_queries";

    /// Reads a pool's query count without depending on Prometheus label
    /// ordering. Returns zero when the pool has not emitted a sample.
    fn query_count(rendered: &str, pool: &str) -> u64 {
        let metric_prefix = format!("{ETL_POSTGRES_QUERY_DURATION_SECONDS}_count{{");
        let pool_label = format!("{POOL_LABEL}=\"{pool}\"");

        rendered
            .lines()
            .find(|line| line.starts_with(&metric_prefix) && line.contains(&pool_label))
            .map_or(0, |line| line.rsplit_once(' ').unwrap().1.parse().unwrap())
    }

    /// Query methods and transactions retain SQLx behavior while counting
    /// requests.
    #[tokio::test(flavor = "multi_thread")]
    async fn exclusive_named_pools_count_queries_and_transactions() {
        let handle = init_metrics_handle().unwrap();
        let database = spawn_source_database().await;
        let raw = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
        let pool = InstrumentedPgPool::new(raw.clone(), TEST_STORE_POOL);
        let other = InstrumentedPgPool::new(raw, TEST_OUT_OF_BAND_POOL);

        sqlx::query("create table test.query_metrics (id integer primary key, value text)")
            .execute(pool.executor())
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("insert into test.query_metrics values ($1, $2)")
            .bind(1)
            .bind("first")
            .execute(tx.executor())
            .await
            .unwrap();
        sqlx::query("update test.query_metrics set value = $1 where id = $2")
            .bind("updated")
            .bind(1)
            .execute(tx.executor())
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let statement = pool
            .executor()
            .prepare(sqlx::SqlStr::from_static(
                "select value from test.query_metrics where id = $1",
            ))
            .await
            .unwrap();
        let value: String =
            statement.query_scalar().bind(1).fetch_one(pool.executor()).await.unwrap();
        assert_eq!(value, "updated");

        let missing: Option<String> =
            sqlx::query_scalar("select value from test.query_metrics where id = $1")
                .bind(2)
                .persistent(false)
                .fetch_optional(pool.executor())
                .await
                .unwrap();
        assert_eq!(missing, None);

        let values: Vec<String> = sqlx::query_scalar("select value from test.query_metrics")
            .fetch_all(other.executor())
            .await
            .unwrap();
        assert_eq!(values, ["updated"]);

        let mut tx = pool.begin().await.unwrap();
        sqlx::query("delete from test.query_metrics").execute(tx.executor()).await.unwrap();
        drop(tx);

        // With one connection, this also waits for SQLx's queued rollback.
        let count: i64 = sqlx::query_scalar("select count(*) from test.query_metrics")
            .fetch_one(pool.executor())
            .await
            .unwrap();
        assert_eq!(count, 1);

        let rendered = handle.render();
        assert_eq!(query_count(&rendered, TEST_STORE_POOL), 11);
        assert_eq!(query_count(&rendered, TEST_OUT_OF_BAND_POOL), 1);
        let sum_prefix = format!("{ETL_POSTGRES_QUERY_DURATION_SECONDS}_sum{{");
        let pool_label = format!("{POOL_LABEL}=\"{TEST_STORE_POOL}\"");
        let duration: f64 = rendered
            .lines()
            .find(|line| line.starts_with(&sum_prefix) && line.contains(&pool_label))
            .unwrap()
            .rsplit_once(' ')
            .unwrap()
            .1
            .parse()
            .unwrap();
        assert!(duration.is_finite() && duration > 0.0);
        assert!(!rendered.contains("query_metrics"));
    }

    /// Errors and abandoned result streams count once; unpolled queries do not
    /// count.
    #[tokio::test(flavor = "multi_thread")]
    async fn exclusive_failed_and_abandoned_queries_are_counted_once() {
        let handle = init_metrics_handle().unwrap();
        let database = spawn_source_database().await;
        let raw = connect_to_source_database(&database.config, 0, 1, None).await.unwrap();
        let pool = InstrumentedPgPool::new(raw, TEST_QUERY_POOL);

        sqlx::query("select 1 / 0").execute(pool.executor()).await.unwrap_err();
        assert_eq!(query_count(&handle.render(), TEST_QUERY_POOL), 1);

        let unpolled = sqlx::query("select 1").execute(pool.executor());
        drop(unpolled);
        assert_eq!(query_count(&handle.render(), TEST_QUERY_POOL), 1);

        let mut stream = sqlx::query("select generate_series(1, 3)").fetch(pool.executor());
        assert!(stream.try_next().await.unwrap().is_some());
        drop(stream);
        assert_eq!(query_count(&handle.render(), TEST_QUERY_POOL), 2);

        let rows =
            sqlx::query("select generate_series(1, 3)").fetch_all(pool.executor()).await.unwrap();
        assert_eq!(rows.len(), 3);
        let rendered = handle.render();
        assert_eq!(query_count(&rendered, TEST_QUERY_POOL), 3);
    }
}
