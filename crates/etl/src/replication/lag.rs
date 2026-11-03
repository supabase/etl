//! Pipeline-owned WAL lag sampling shared by initial copy and CDC workers.
//!
//! Worker progress is captured coherently before querying PostgreSQL. All
//! workers use the same source sample. Dead registrations are removed on each
//! snapshot; a worker that stops during the query can contribute one final
//! sample. Concurrent table-sync workers aggregate by maximum distance within
//! each copy or CDC path, never sum.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use etl_postgres::lag::{PipelineLagMetrics, SlotLagMetrics, SlotWalStatus};
use metrics::gauge;
use tokio::time::MissedTickBehavior;
use tokio_postgres::types::PgLsn;
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

use crate::{
    config::PgConnectionConfig,
    error::{ErrorKind, EtlResult},
    etl_error,
    observability::{
        CDC_REPLICATION_PATH, COPY_REPLICATION_PATH, ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES,
        ETL_REPLICATION_WAL_LAG_BYTES, PIPELINE_ID_LABEL, REPLICATION_PATH_LABEL,
        WORKER_TYPE_LABEL,
    },
    pipeline::PipelineId,
    postgres::{OutOfBandSourcePool, client::SlotState},
    replication::WorkerType,
    schema::TableId,
    task::spawn_best_effort_task,
};

/// Upper bound on a best-effort metrics query, including pool acquisition.
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);
/// Worker paths refreshed even when their last registration has stopped.
const WORKER_PATHS: [(&str, &str); 3] = [
    ("apply", CDC_REPLICATION_PATH),
    ("table_sync", COPY_REPLICATION_PATH),
    ("table_sync", CDC_REPLICATION_PATH),
];
/// Sentinel for unavailable distances; dashboards must filter negative values.
const UNAVAILABLE_LAG: f64 = -1.0;

/// Returns whether PostgreSQL reports the slot's required WAL as usable.
fn slot_is_usable(slot: &SlotLagMetrics) -> bool {
    matches!(
        slot.wal_status,
        Some(SlotWalStatus::Reserved | SlotWalStatus::Extended | SlotWalStatus::Unreserved)
    )
}

/// Returns the maximum only when every participating value is known.
fn known_max(values: &[Option<u64>]) -> f64 {
    if values.is_empty() || values.iter().any(Option::is_none) {
        UNAVAILABLE_LAG
    } else {
        values.iter().flatten().copied().max().map_or(UNAVAILABLE_LAG, |value| value as f64)
    }
}

/// Exports a WAL distance or the negative unavailable sentinel.
fn emit_lag_measurement(
    metric: &'static str,
    pipeline_id: &str,
    worker_type: &'static str,
    replication_path: &'static str,
    value: f64,
) {
    gauge!(
        metric,
        PIPELINE_ID_LABEL => pipeline_id.to_owned(),
        WORKER_TYPE_LABEL => worker_type,
        REPLICATION_PATH_LABEL => replication_path
    )
    .set(value);
}

/// Coherent received and safe frontiers and the worker's initial-copy phase.
#[derive(Debug, Clone, Copy)]
struct WorkerProgress {
    /// Highest consumed logical-stream position.
    received: u64,
    /// Monotonic safe consumer frontier, including quiescent progress.
    safe: u64,
    /// Whether this worker still holds its fixed initial-copy boundary.
    copying: bool,
}

impl WorkerProgress {
    /// Computes source-dependent distances only for a consistent reference.
    ///
    /// Local progress was captured before the query. A source head behind it
    /// indicates a mismatched or changed source frontier, not zero backlog.
    fn source_distances(self, head: u64) -> Option<(u64, u64)> {
        Some((head.checked_sub(self.safe)?, head.checked_sub(self.received)?))
    }
}

/// Worker-owned progress; the sampler retains only weak registrations.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationLagMetrics {
    /// One short lock makes the related positions an atomic snapshot.
    inner: Arc<Mutex<WorkerProgress>>,
}

impl ReplicationLagMetrics {
    /// Creates progress without registering it with a source sampler.
    pub(crate) fn create(initial_lsn: PgLsn, copying: bool) -> Self {
        let lsn = u64::from(initial_lsn);

        Self { inner: Arc::new(Mutex::new(WorkerProgress { received: lsn, safe: lsn, copying })) }
    }

    /// Publishes related frontiers coherently and without moving backward.
    ///
    /// The safe frontier is the quiescent-or-flushed candidate selected by the
    /// apply loop. Its high-water mark matches monotonic PostgreSQL feedback,
    /// rather than dropping back to an older flush when new work starts.
    pub(crate) fn update_progress(&self, received: PgLsn, safe: PgLsn) {
        let mut progress = self.inner.lock().unwrap();

        progress.received = progress.received.max(u64::from(received));
        progress.safe = progress.safe.max(u64::from(safe));

        debug_assert!(progress.safe <= progress.received);
    }
}

/// A pre-query snapshot that does not extend a worker's lifetime.
#[derive(Debug)]
struct WorkerSample {
    /// Worker identity used to find its slot and aggregate its metrics.
    worker: WorkerType,
    /// Coherent frontiers captured before the source query.
    progress: WorkerProgress,
}

/// Marks lag unavailable when the owned sampler exits or is aborted.
#[derive(Debug)]
struct ReplicationLagSamplerLifetime {
    /// Pipeline whose series must no longer claim live measurements.
    pipeline_id: PipelineId,
}

impl Drop for ReplicationLagSamplerLifetime {
    fn drop(&mut self) {
        let pipeline_id = self.pipeline_id.to_string();

        for (worker_type, replication_path) in WORKER_PATHS {
            for metric in [ETL_REPLICATION_WAL_LAG_BYTES, ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES] {
                if replication_path == COPY_REPLICATION_PATH
                    && metric == ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES
                {
                    continue;
                }

                emit_lag_measurement(
                    metric,
                    &pipeline_id,
                    worker_type,
                    replication_path,
                    UNAVAILABLE_LAG,
                );
            }
        }
    }
}

/// Shared source access and weak progress registrations for one pipeline.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationLagMonitor {
    /// Pipeline identity scopes both slot selection and exported series.
    pipeline_id: PipelineId,
    /// Bounded source SQL pool shared with copy-slot validity checks.
    source_pool: OutOfBandSourcePool,
    /// Weak entries cannot keep completed workers alive.
    workers: Arc<Mutex<HashMap<WorkerType, Weak<Mutex<WorkerProgress>>>>>,
    /// Sampling cadence configured by the pipeline.
    refresh_interval: Duration,
}

impl ReplicationLagMonitor {
    /// Creates a lazy monitor; its owner starts and joins the sampling task.
    pub(crate) fn new(
        pipeline_id: PipelineId,
        connection: &PgConnectionConfig,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            pipeline_id,
            source_pool: OutOfBandSourcePool::new(connection, refresh_interval),
            workers: Arc::new(Mutex::new(HashMap::new())),
            refresh_interval,
        }
    }

    /// Registers CDC progress for an apply or table-sync worker attempt.
    pub(crate) fn register_cdc(
        &self,
        worker: WorkerType,
        initial_lsn: PgLsn,
    ) -> ReplicationLagMetrics {
        self.register(worker, ReplicationLagMetrics::create(initial_lsn, false))
    }

    /// Holds the snapshot boundary through copy, durability, and catchup wait.
    ///
    /// Copying rows does not consume WAL. Receive lag is therefore unavailable
    /// until this worker starts CDC.
    pub(crate) fn register_copy(
        &self,
        table_id: TableId,
        consistent_point: PgLsn,
    ) -> ReplicationLagMetrics {
        self.register(
            WorkerType::TableSync { table_id },
            ReplicationLagMetrics::create(consistent_point, true),
        )
    }

    /// Checks copy-slot validity without using the replication connection.
    pub(crate) async fn get_slot_state(&self, slot_name: &str) -> EtlResult<SlotState> {
        self.source_pool.get_slot_state(slot_name).await
    }

    /// Starts the single best-effort sampler owned by the pipeline.
    pub(crate) fn spawn_replication_lag_metrics_task(&self) -> AbortOnDropHandle<()> {
        let monitor = self.clone();
        let lifetime = ReplicationLagSamplerLifetime { pipeline_id: self.pipeline_id.clone() };

        spawn_best_effort_task("replication lag sampler", async move {
            let _lifetime_guard = lifetime;
            let mut interval = tokio::time::interval(monitor.refresh_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let workers = monitor.snapshot();
                let sample = match tokio::time::timeout(
                    SAMPLE_TIMEOUT,
                    monitor.source_pool.sample_pipeline_lag(&monitor.pipeline_id),
                )
                .await
                {
                    Ok(sample) => sample,
                    Err(error) => Err(etl_error!(
                        ErrorKind::SourceConnectionFailed,
                        "Source lag sample timed out",
                        source: error
                    )),
                };

                match sample {
                    Ok(source) => monitor.emit(&workers, Some(&source)),
                    Err(error) => {
                        warn!(
                            error = %error,
                            "replication lag sampler failed to poll source database"
                        );

                        monitor.emit(&workers, None);
                    }
                }
            }
        })
    }

    /// Installs a new attempt, superseding any previous registration.
    fn register(
        &self,
        worker: WorkerType,
        progress: ReplicationLagMetrics,
    ) -> ReplicationLagMetrics {
        self.workers.lock().unwrap().insert(worker, Arc::downgrade(&progress.inner));

        progress
    }

    /// Captures progress before the source query to avoid mixing future local
    /// frontiers with an earlier source head.
    fn snapshot(&self) -> Vec<WorkerSample> {
        let mut workers = self.workers.lock().unwrap();
        workers.retain(|_, registration| registration.strong_count() > 0);

        workers
            .iter()
            .filter_map(|(&worker, registration)| {
                let progress = *registration.upgrade()?.lock().unwrap();
                Some(WorkerSample { worker, progress })
            })
            .collect()
    }

    /// Exports conservative lag maxima separately for each worker path.
    fn emit(&self, workers: &[WorkerSample], source: Option<&PipelineLagMetrics>) {
        let pipeline_id = self.pipeline_id.to_string();

        for (worker_type, replication_path) in WORKER_PATHS {
            let mut total = Vec::new();
            let mut received = Vec::new();

            for sample in workers.iter().filter(|sample| {
                sample.worker.as_str() == worker_type
                    && sample.progress.copying == (replication_path == COPY_REPLICATION_PATH)
            }) {
                let progress = sample.progress;

                let slot = source.and_then(|source| match sample.worker {
                    WorkerType::Apply => source.apply.as_ref(),
                    WorkerType::TableSync { table_id } => source.table_sync.get(&table_id),
                });

                let head = source
                    .and_then(|source| source.source_wal.decoding_lsn())
                    .filter(|_| slot.is_some_and(slot_is_usable));

                let distances = head.and_then(|head| progress.source_distances(head));
                total.push(distances.map(|(total, _)| total));

                if !progress.copying {
                    received.push(distances.map(|(_, received)| received));
                }
            }

            for (metric, values) in [
                (ETL_REPLICATION_WAL_LAG_BYTES, total),
                (ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES, received),
            ] {
                if replication_path == COPY_REPLICATION_PATH
                    && metric == ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES
                {
                    continue;
                }

                emit_lag_measurement(
                    metric,
                    &pipeline_id,
                    worker_type,
                    replication_path,
                    known_max(&values),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::{
        config::{PgConnectionConfig, TcpKeepaliveConfig, TlsConfig},
        replication::{ReplicationLagMetrics, ReplicationLagMonitor, WorkerType, lag::known_max},
        schema::TableId,
    };

    /// Builds a lazy monitor without opening a database connection.
    fn test_monitor() -> ReplicationLagMonitor {
        ReplicationLagMonitor::new(
            1.into(),
            &PgConnectionConfig {
                host: "127.0.0.1".into(),
                hostaddr: None,
                port: 1,
                name: "unused".into(),
                username: "unused".into(),
                password: None,
                tls: TlsConfig::disabled(),
                keepalive: TcpKeepaliveConfig::default(),
            },
            Duration::from_secs(60),
        )
    }

    /// Irrelevant idle WAL advances safe progress without inventing a flush.
    #[test]
    fn safe_progress_remains_monotonic_when_new_work_starts() {
        let progress = ReplicationLagMetrics::create(100.into(), false);
        progress.update_progress(200.into(), 200.into());
        progress.update_progress(300.into(), 100.into());
        let snapshot = *progress.inner.lock().unwrap();
        assert_eq!(snapshot.received, 300);
        assert_eq!(snapshot.safe, 200);

        progress.update_progress(300.into(), 300.into());
        let snapshot = *progress.inner.lock().unwrap();
        assert_eq!(snapshot.received - snapshot.safe, 0);
    }

    /// Snapshots do not keep workers alive; later snapshots remove stopped
    /// workers and use the latest registration.
    #[test]
    fn snapshots_recycle_stopped_workers_and_use_latest_registrations() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _entered = runtime.enter();
        let monitor = test_monitor();
        let worker = WorkerType::TableSync { table_id: TableId::new(1) };
        let copying = monitor.register_copy(TableId::new(1), 100.into());
        let old_sample = monitor.snapshot();
        assert!(old_sample[0].progress.copying);

        drop(copying);
        assert!(monitor.snapshot().is_empty());

        let first = monitor.register_cdc(worker, 200.into());
        let old_sample = monitor.snapshot();
        let second = monitor.register_cdc(worker, 300.into());

        assert_eq!(old_sample[0].progress.safe, 200);
        assert_eq!(monitor.snapshot()[0].progress.safe, 300);

        drop(first);
        assert_eq!(monitor.snapshot()[0].progress.safe, 300);

        drop(second);
        assert!(monitor.snapshot().is_empty());
    }

    /// Overlapping WAL distances aggregate by maximum; unknown never means
    /// zero.
    #[test]
    fn maxima_preserve_unknown_and_do_not_sum_overlapping_slots() {
        assert_eq!(known_max(&[Some(100), Some(900), Some(200)]), 900.0);
        assert_eq!(known_max(&[Some(0)]), 0.0);
        assert_eq!(known_max(&[]), -1.0);
        assert_eq!(known_max(&[Some(100), None]), -1.0);
    }

    /// Owner teardown interrupts a pending handshake without retaining a
    /// worker.
    #[tokio::test]
    async fn sampler_shutdown_interrupts_a_blocked_source_query() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut monitor = test_monitor();
        monitor.source_pool = crate::postgres::OutOfBandSourcePool::new(
            &PgConnectionConfig {
                host: "127.0.0.1".into(),
                hostaddr: None,
                port: listener.local_addr().unwrap().port(),
                name: "unused".into(),
                username: "unused".into(),
                password: None,
                tls: TlsConfig::disabled(),
                keepalive: TcpKeepaliveConfig::default(),
            },
            Duration::from_secs(60),
        );
        let registration = monitor.register_cdc(WorkerType::Apply, 100.into());

        let task = monitor.spawn_replication_lag_metrics_task();
        let (_connection, _) =
            tokio::time::timeout(Duration::from_secs(1), listener.accept()).await.unwrap().unwrap();

        task.abort();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled()
        );
        assert_eq!(Arc::strong_count(&registration.inner), 1);
    }

    /// A failed source poll replaces previous distances with unavailable
    /// values.
    #[test]
    fn failed_samples_clear_source_distances() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _entered = runtime.enter();
        let handle = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = test_monitor();
        monitor.emit(&[], Some(&etl_postgres::lag::PipelineLagMetrics::default()));

        let read = |name: &str| {
            handle
                .render()
                .lines()
                .find_map(|line| {
                    let (series, value) = line.split_once(' ')?;
                    if series.contains("worker_type=") && !series.contains("worker_type=\"apply\"")
                    {
                        return None;
                    }
                    (series.starts_with(&format!("{name}{{")))
                        .then(|| value.parse::<f64>().unwrap())
                })
                .unwrap()
        };

        for metric in [
            crate::observability::ETL_REPLICATION_WAL_LAG_BYTES,
            crate::observability::ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES,
        ] {
            metrics::gauge!(metric,
                crate::observability::PIPELINE_ID_LABEL => "1",
                crate::observability::WORKER_TYPE_LABEL => "apply",
                crate::observability::REPLICATION_PATH_LABEL => "cdc")
            .set(123.0);
            assert_eq!(read(metric), 123.0);
        }

        let _progress = monitor.register_cdc(WorkerType::Apply, 100.into());
        monitor.emit(&monitor.snapshot(), None);

        assert_eq!(read(crate::observability::ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES), -1.0);
        assert_eq!(read(crate::observability::ETL_REPLICATION_WAL_LAG_BYTES), -1.0);
    }

    /// A sampler panic completes without failing its owner and invalidates all
    /// lag series instead of leaving the previous values frozen.
    #[tokio::test]
    async fn sampler_panic_invalidates_metrics_without_failing_owner() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let mut monitor = test_monitor();
        // Fail before sampling PostgreSQL, independently of source
        // availability.
        monitor.refresh_interval = Duration::ZERO;
        let names = [
            crate::observability::ETL_REPLICATION_WAL_LAG_BYTES,
            crate::observability::ETL_REPLICATION_RECEIVE_WAL_LAG_BYTES,
        ];
        for name in names {
            metrics::gauge!(name,
                crate::observability::PIPELINE_ID_LABEL => "1",
                crate::observability::WORKER_TYPE_LABEL => "apply",
                crate::observability::REPLICATION_PATH_LABEL => "cdc")
            .set(123.0);
        }

        monitor.spawn_replication_lag_metrics_task().await.unwrap();

        let rendered = metrics.render();
        for name in names {
            let series: Vec<_> =
                rendered.lines().filter(|line| line.starts_with(&format!("{name}{{"))).collect();
            assert!(!series.is_empty());
            assert!(series.iter().all(|line| line.ends_with(" -1")));
        }
    }

    /// Copy and CDC paths retain separate maxima and recycle the old copy
    /// series.
    #[cfg(feature = "test-utils")]
    #[tokio::test(flavor = "multi_thread")]
    async fn exclusive_copy_and_cdc_lag_have_distinct_series() {
        let database = crate::test_utils::database::spawn_source_database().await;
        let client = database.client.as_ref().unwrap();
        let handle = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let pipeline_id = rand::random::<u64>();
        let monitor = ReplicationLagMonitor::new(
            pipeline_id.into(),
            &database.config,
            Duration::from_secs(60),
        );
        let mut slots = Vec::new();
        let mut points = Vec::new();

        for table_id in [TableId::new(1), TableId::new(2)] {
            let name =
                String::try_from(etl_postgres::slots::EtlReplicationSlot::for_table_sync_worker(
                    pipeline_id,
                    table_id,
                ))
                .unwrap();
            let point: tokio_postgres::types::PgLsn = client
                .query_one(
                    "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&name],
                )
                .await
                .unwrap()
                .get(0);
            slots.push(name);
            points.push(point);

            client
                .query_one(
                    "select pg_logical_emit_message(false, 'path_probe', repeat('x', 1024))",
                    &[],
                )
                .await
                .unwrap();
        }

        let copy = monitor.register_copy(TableId::new(1), points[0]);
        let cdc =
            monitor.register_cdc(WorkerType::TableSync { table_id: TableId::new(2) }, points[1]);
        let workers = monitor.snapshot();
        let source = monitor.source_pool.sample_pipeline_lag(&pipeline_id.into()).await.unwrap();
        let head = source.source_wal.decoding_lsn().unwrap();
        let read = |name: &str, path: &str| {
            handle.render().lines().find_map(|line| {
                let (series, value) = line.split_once(' ')?;
                (series.starts_with(&format!("{name}{{"))
                    && series.contains("worker_type=\"table_sync\"")
                    && series.contains(&format!("replication_path=\"{path}\"")))
                .then(|| value.parse::<f64>().unwrap())
            })
        };

        monitor.emit(&workers, Some(&source));

        assert!(points[1] > points[0]);
        assert_eq!(
            read("etl_replication_wal_lag_bytes", "copy"),
            Some((head - u64::from(points[0])) as f64)
        );
        assert_eq!(
            read("etl_replication_wal_lag_bytes", "cdc"),
            Some((head - u64::from(points[1])) as f64)
        );
        assert_eq!(
            read("etl_replication_receive_wal_lag_bytes", "cdc"),
            Some((head - u64::from(points[1])) as f64)
        );
        assert_eq!(read("etl_replication_receive_wal_lag_bytes", "copy"), None);

        let catching_up =
            monitor.register_cdc(WorkerType::TableSync { table_id: TableId::new(1) }, points[0]);
        drop(copy);
        monitor.emit(&monitor.snapshot(), Some(&source));

        assert_eq!(read("etl_replication_wal_lag_bytes", "copy"), Some(-1.0));
        assert_eq!(
            read("etl_replication_wal_lag_bytes", "cdc"),
            Some((head - u64::from(points[0])) as f64)
        );

        drop(catching_up);
        drop(cdc);
        monitor.emit(&monitor.snapshot(), Some(&source));

        assert_eq!(read("etl_replication_wal_lag_bytes", "cdc"), Some(-1.0));
        assert_eq!(read("etl_replication_receive_wal_lag_bytes", "cdc"), Some(-1.0));

        for name in slots {
            client.query_one("select pg_drop_replication_slot($1)", &[&name]).await.unwrap();
        }
    }

    /// A changed source reference is unknown rather than a false zero.
    #[test]
    fn source_head_behind_captured_progress_is_unavailable() {
        let progress =
            crate::replication::lag::WorkerProgress { received: 200, safe: 150, copying: false };
        assert_eq!(progress.source_distances(300), Some((150, 100)));
        assert_eq!(progress.source_distances(200), Some((50, 0)));
        assert_eq!(progress.source_distances(199), None);
    }
}
