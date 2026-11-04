//! Client-observed slot creation attempts and their live oldest age.
//!
//! One pipeline-owned sampler observes both worker types. Registrations exist
//! only while a command future is alive; cancellation does not prove that the
//! server has stopped executing the command.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use metrics::{gauge, histogram};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::task::AbortOnDropHandle;

use crate::{
    observability::{
        ETL_REPLICATION_SLOT_CREATION_AGE_SECONDS, ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS,
        OUTCOME_LABEL, PIPELINE_ID_LABEL, WORKER_TYPE_LABEL,
    },
    pipeline::PipelineId,
    replication::WorkerType,
    task::spawn_best_effort_task,
};

/// Sampling is independent of pending PostgreSQL command responses.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// Stopped observation must not look like an idle worker.
const UNAVAILABLE_AGE: f64 = -1.0;
/// Bounded worker labels, including idle workers.
const WORKER_TYPES: [&str; 2] = ["apply", "table_sync"];

/// Identity and monotonic start time of one live command attempt.
#[derive(Debug)]
struct CreationStart {
    /// Worker category without table or slot identity.
    worker_type: &'static str,
    /// Time immediately before polling the creation command.
    started_at: Instant,
}

/// Coherent registrations and sampler availability for one pipeline.
#[derive(Debug, Default)]
struct CreationState {
    /// Whether the pipeline's independent sampler is running.
    observing: bool,
    /// Only active commands are retained, bounded by worker concurrency.
    attempts: Vec<Arc<CreationStart>>,
}

impl CreationState {
    /// Returns the oldest active age, idle zero, or unavailable sentinel.
    fn age(&self, worker_type: &str) -> f64 {
        if !self.observing {
            return UNAVAILABLE_AGE;
        }

        self.attempts
            .iter()
            .filter(|attempt| attempt.worker_type == worker_type)
            .map(|attempt| attempt.started_at)
            .min()
            .map_or(0.0, |started_at| started_at.elapsed().as_secs_f64())
    }
}

/// Pipeline-owned slot creation observations shared by every worker retry.
#[derive(Clone, Debug)]
pub(crate) struct SlotCreationMonitor {
    /// Explicit identity supports multiple library-hosted pipelines.
    pipeline_id: PipelineId,
    /// Short synchronous locks cover registrations and gauge publication only.
    state: Arc<Mutex<CreationState>>,
}

impl SlotCreationMonitor {
    /// Creates a monitor without starting background work.
    pub(crate) fn new(pipeline_id: PipelineId) -> Self {
        Self { pipeline_id, state: Arc::default() }
    }

    /// Registers a command; dropping its guard records a canceled attempt.
    pub(super) fn start(&self, worker: WorkerType) -> SlotCreationAttempt {
        let start =
            Arc::new(CreationStart { worker_type: worker.as_str(), started_at: Instant::now() });
        let mut state = self.state.lock().unwrap();
        state.attempts.push(Arc::clone(&start));
        self.emit(&state);

        SlotCreationAttempt { monitor: self.clone(), start, outcome: "cancelled" }
    }

    /// Starts the single sampler, which the pipeline must abort and join.
    pub(crate) fn spawn_slot_creation_metrics_task(&self) -> AbortOnDropHandle<()> {
        let lifetime = SlotCreationSamplerLifetime(self.clone());
        {
            let mut state = self.state.lock().unwrap();
            state.observing = true;
            self.emit(&state);
        }

        spawn_best_effort_task("slot creation monitor", async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let state = lifetime.0.state.lock().unwrap();
                lifetime.0.emit(&state);
            }
        })
    }

    /// Publishes both worker categories while holding the registration lock.
    fn emit(&self, state: &CreationState) {
        for worker_type in WORKER_TYPES {
            gauge!(
                ETL_REPLICATION_SLOT_CREATION_AGE_SECONDS,
                PIPELINE_ID_LABEL => self.pipeline_id.to_string(),
                WORKER_TYPE_LABEL => worker_type,
            )
            .set(state.age(worker_type));
        }
    }
}

/// Owns an attempt until a response, error, or cancellation ends observation.
pub(super) struct SlotCreationAttempt {
    /// Pipeline monitor receiving completion and registration removal.
    monitor: SlotCreationMonitor,
    /// Unique allocation distinguishes attempts even with identical starts.
    start: Arc<CreationStart>,
    /// Defaults to cancellation until the command returns a response.
    outcome: &'static str,
}

impl SlotCreationAttempt {
    /// Records the command response outcome and ends this attempt.
    pub(super) fn finish(mut self, succeeded: bool) {
        self.outcome = if succeeded { "success" } else { "error" };
    }
}

impl Drop for SlotCreationAttempt {
    fn drop(&mut self) {
        let elapsed = self.start.started_at.elapsed().as_secs_f64();
        let mut state = self.monitor.state.lock().unwrap();
        state.attempts.retain(|attempt| !Arc::ptr_eq(attempt, &self.start));
        self.monitor.emit(&state);

        histogram!(
            ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS,
            PIPELINE_ID_LABEL => self.monitor.pipeline_id.to_string(),
            WORKER_TYPE_LABEL => self.start.worker_type,
            OUTCOME_LABEL => self.outcome,
        )
        .record(elapsed);
    }
}

/// Marks cached ages unavailable even if the sampler is aborted before polling.
struct SlotCreationSamplerLifetime(SlotCreationMonitor);

impl Drop for SlotCreationSamplerLifetime {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.observing = false;
        self.0.emit(&state);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        observability::{
            ETL_REPLICATION_SLOT_CREATION_AGE_SECONDS,
            ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS,
        },
        postgres::client::SlotCreationMonitor,
        replication::WorkerType,
        schema::TableId,
        task::abort_and_join,
    };

    /// Reads one exported series without depending on label ordering.
    fn sample(rendered: &str, metric: &str, labels: &[(&str, &str)]) -> Option<f64> {
        rendered.lines().find_map(|line| {
            (line.starts_with(&format!("{metric}{{"))
                && labels.iter().all(|(key, value)| line.contains(&format!("{key}=\"{value}\""))))
            .then(|| line.rsplit_once(' ').unwrap().1.parse().unwrap())
        })
    }

    /// Reads an age by pipeline and bounded worker category.
    fn age(rendered: &str, pipeline: &str, worker: &str) -> f64 {
        sample(
            rendered,
            ETL_REPLICATION_SLOT_CREATION_AGE_SECONDS,
            &[("pipeline_id", pipeline), ("worker_type", worker)],
        )
        .unwrap()
    }

    /// Pending commands age without any command progress or completion sample.
    #[tokio::test(start_paused = true)]
    async fn pending_command_exports_live_age() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = SlotCreationMonitor::new("41".into());
        let task = monitor.spawn_slot_creation_metrics_task();
        let attempt = monitor.start(WorkerType::Apply);
        tokio::task::yield_now().await;

        for seconds in [15, 1905] {
            tokio::time::advance(Duration::from_secs(seconds)).await;
            tokio::task::yield_now().await;
        }
        let rendered = metrics.render();
        assert_eq!(age(&rendered, "41", "apply"), 1920.0);
        assert_eq!(age(&rendered, "41", "table_sync"), 0.0);
        assert!(
            !rendered
                .contains(&format!("{ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS}_count{{"))
        );

        attempt.finish(true);
        assert_eq!(age(&metrics.render(), "41", "apply"), 0.0);
        abort_and_join(task).await.unwrap();
        assert_eq!(age(&metrics.render(), "41", "apply"), -1.0);
    }

    /// Ending one concurrent copy must preserve the oldest remaining attempt.
    #[tokio::test(start_paused = true)]
    async fn concurrent_attempts_are_isolated_by_worker_and_pipeline() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = SlotCreationMonitor::new("42".into());
        let other = SlotCreationMonitor::new("43".into());
        let task = monitor.spawn_slot_creation_metrics_task();
        let other_task = other.spawn_slot_creation_metrics_task();
        let first = monitor.start(WorkerType::TableSync { table_id: TableId(100) });
        // Identical monotonic start times must still have independent
        // identities.
        let second = monitor.start(WorkerType::TableSync { table_id: TableId(101) });
        tokio::time::advance(Duration::from_secs(10)).await;
        let third = monitor.start(WorkerType::TableSync { table_id: TableId(102) });
        let main = monitor.start(WorkerType::Apply);
        let other_attempt = other.start(WorkerType::Apply);
        tokio::time::advance(Duration::from_secs(5)).await;

        second.finish(true);
        assert_eq!(age(&metrics.render(), "42", "table_sync"), 15.0);
        first.finish(true);
        assert_eq!(age(&metrics.render(), "42", "table_sync"), 5.0);
        third.finish(true);
        assert_eq!(age(&metrics.render(), "42", "table_sync"), 0.0);
        assert_eq!(age(&metrics.render(), "42", "apply"), 5.0);
        main.finish(true);
        other_attempt.finish(true);
        let rendered = metrics.render();
        assert_eq!(age(&rendered, "42", "apply"), 0.0);
        assert_eq!(age(&rendered, "43", "apply"), 0.0);
        assert!(!rendered.contains("table_id="));
        assert!(!rendered.contains("slot_name="));

        abort_and_join(task).await.unwrap();
        abort_and_join(other_task).await.unwrap();
    }

    /// The first scrape includes completed samples with separate outcomes.
    #[tokio::test(start_paused = true)]
    async fn completions_before_first_scrape_preserve_duration_and_outcome() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = SlotCreationMonitor::new("44".into());
        for (seconds, outcome) in [(2, Some(true)), (3, Some(false)), (5, None)] {
            let attempt = monitor.start(WorkerType::Apply);
            tokio::time::advance(Duration::from_secs(seconds)).await;
            if let Some(succeeded) = outcome {
                attempt.finish(succeeded);
            } else {
                drop(attempt);
            }
        }

        let rendered = metrics.render();
        for (outcome, seconds) in [("success", 2.0), ("error", 3.0), ("cancelled", 5.0)] {
            let labels = [("pipeline_id", "44"), ("worker_type", "apply"), ("outcome", outcome)];
            assert_eq!(
                sample(
                    &rendered,
                    &format!("{ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS}_count"),
                    &labels
                ),
                Some(1.0)
            );
            assert_eq!(
                sample(
                    &rendered,
                    &format!("{ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS}_sum"),
                    &labels
                ),
                Some(seconds)
            );
        }
        assert!(monitor.state.lock().unwrap().attempts.is_empty());
    }

    /// Cancellation clears only its attempt and a retry receives a fresh age.
    #[tokio::test(start_paused = true)]
    async fn cancellation_and_retry_do_not_retain_abandoned_age() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = SlotCreationMonitor::new("45".into());
        let task = monitor.spawn_slot_creation_metrics_task();
        let attempt = monitor.start(WorkerType::Apply);
        tokio::time::advance(Duration::from_secs(30)).await;
        drop(attempt);
        assert_eq!(age(&metrics.render(), "45", "apply"), 0.0);

        let retry = monitor.start(WorkerType::Apply);
        tokio::time::advance(Duration::from_secs(7)).await;
        tokio::task::yield_now().await;
        assert_eq!(age(&metrics.render(), "45", "apply"), 7.0);
        abort_and_join(task).await.unwrap();
        retry.finish(false);
        assert_eq!(age(&metrics.render(), "45", "apply"), -1.0);
        assert!(monitor.state.lock().unwrap().attempts.is_empty());
    }

    /// Aborting an unpolled sampler also invalidates its initially idle gauge.
    #[tokio::test(start_paused = true)]
    async fn stopping_unpolled_sampler_marks_observation_unavailable() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = SlotCreationMonitor::new("46".into());
        let task = monitor.spawn_slot_creation_metrics_task();
        abort_and_join(task).await.unwrap();
        assert_eq!(age(&metrics.render(), "46", "apply"), -1.0);
        assert_eq!(age(&metrics.render(), "46", "table_sync"), -1.0);
    }

    /// Both real command paths record once; lookups and later commit do not.
    #[tokio::test]
    async fn exclusive_postgres_commands_record_both_worker_types_and_errors() {
        use crate::{
            error::ErrorKind,
            postgres::client::PgReplicationClient,
            test_utils::{
                database::{spawn_source_database, test_table_name},
                pipeline::test_slot_name,
            },
        };

        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let database = spawn_source_database().await;
        let table_id = database
            .create_table(test_table_name("slot_metrics"), false, &[("id", "bigint primary key")])
            .await
            .unwrap();
        let monitor = SlotCreationMonitor::new("47".into());
        let task = monitor.spawn_slot_creation_metrics_task();
        let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
        client.set_slot_creation_monitor(monitor.clone());
        let main_slot = test_slot_name("metrics_main");
        client.create_slot(&main_slot, &Default::default()).await.unwrap();
        client.get_slot(&main_slot).await.unwrap();
        let error = client.create_slot(&main_slot, &Default::default()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ReplicationSlotAlreadyExists);

        let copy_slot = test_slot_name("metrics_copy");
        let (transaction, _) =
            client.create_table_copy_slot(&copy_slot, table_id, &Default::default()).await.unwrap();
        transaction.commit().await.unwrap();
        let rendered = metrics.render();
        for (worker, outcome) in
            [("apply", "success"), ("apply", "error"), ("table_sync", "success")]
        {
            assert_eq!(
                sample(
                    &rendered,
                    &format!("{ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS}_count"),
                    &[("pipeline_id", "47"), ("worker_type", worker), ("outcome", outcome)],
                ),
                Some(1.0),
            );
            assert_eq!(age(&rendered, "47", worker), 0.0);
        }
        client.delete_slot_if_exists(&copy_slot).await.unwrap();
        client.delete_slot_if_exists(&main_slot).await.unwrap();
        abort_and_join(task).await.unwrap();
    }

    /// A transaction-blocked command stays observable until its future is
    /// dropped.
    #[tokio::test]
    async fn exclusive_cancelling_blocked_postgres_command_clears_its_live_age() {
        use crate::{
            postgres::client::PgReplicationClient,
            test_utils::{database::spawn_source_database, pipeline::test_slot_name},
        };

        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let database = spawn_source_database().await;
        let source = database.client.as_ref().unwrap();
        source.batch_execute("begin; select pg_current_xact_id()").await.unwrap();
        let monitor = SlotCreationMonitor::new("48".into());
        let sampler = monitor.spawn_slot_creation_metrics_task();
        let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
        client.set_slot_creation_monitor(monitor.clone());
        let slot_name = test_slot_name("metrics_cancelled");
        let command = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            client.create_slot(&slot_name, &Default::default()).await
        }));

        // The open transaction is the barrier; elapsed time never releases it.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if age(&metrics.render(), "48", "apply") >= 1.0 {
                    break;
                }
                assert!(!command.is_finished());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        abort_and_join(command).await.unwrap();
        source.batch_execute("rollback").await.unwrap();

        let rendered = metrics.render();
        assert_eq!(age(&rendered, "48", "apply"), 0.0);
        assert_eq!(
            sample(
                &rendered,
                &format!("{ETL_REPLICATION_SLOT_CREATION_DURATION_SECONDS}_count"),
                &[("pipeline_id", "48"), ("worker_type", "apply"), ("outcome", "cancelled")],
            ),
            Some(1.0),
        );
        assert!(monitor.state.lock().unwrap().attempts.is_empty());
        abort_and_join(sampler).await.unwrap();
    }
}
