//! Live main apply-worker durability observations, independent of write waits.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use metrics::gauge;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_postgres::types::PgLsn;
use tokio_util::task::AbortOnDropHandle;

use crate::{
    observability::{
        CDC_REPLICATION_PATH, ETL_DESTINATION_DURABILITY_AGE_SECONDS, PIPELINE_ID_LABEL,
        REPLICATION_PATH_LABEL, WORKER_TYPE_LABEL,
    },
    pipeline::PipelineId,
    task::spawn_best_effort_task,
};

/// Sampling must continue even when the destination's write future is pending.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// Unavailable observations must never look like durable completion.
const UNAVAILABLE_AGE: f64 = -1.0;

/// Constant-space summary of an outstanding ordered-stream interval.
#[derive(Debug)]
struct OutstandingDurability {
    /// First dispatch that has not been covered by observed durability.
    since: Instant,
    /// Highest transaction frontier dispatched in this interval, including
    /// retries.
    dispatched_lsn: PgLsn,
    /// A retry must durably replay through the previous attempt's frontier.
    replay_until: Option<PgLsn>,
}

/// Coherent state shared by the main apply worker and its independent sampler.
#[derive(Debug, Default)]
struct DurabilityState {
    /// Whether the main apply loop has completed its replication handshake.
    observing: bool,
    /// Work remains pending through errors, retries, and shutdown draining.
    outstanding: Option<OutstandingDurability>,
}

impl DurabilityState {
    /// Returns a monotonic age, idle zero, or the unavailable sentinel.
    fn age(&self) -> f64 {
        if !self.observing {
            UNAVAILABLE_AGE
        } else {
            self.outstanding.as_ref().map_or(0.0, |pending| pending.since.elapsed().as_secs_f64())
        }
    }
}

/// Pipeline-owned observation shared across main apply-worker attempts.
///
/// Normal durable results cover all earlier accepted writes in their ordered
/// stream. After a retry, earlier replay results cannot close the interval
/// until a durable commit covers the previous dispatch frontier. This
/// conservatively retains the original age while part of that interval may
/// already be replayed. No per-batch history or table labels accumulate during
/// an indefinite stall.
#[derive(Clone, Debug)]
pub(crate) struct DurabilityMonitor {
    /// Explicit pipeline identity also supports library-hosted pipelines.
    pipeline_id: PipelineId,
    /// Locks cover only local state and gauge updates, never asynchronous work.
    state: Arc<Mutex<DurabilityState>>,
}

impl DurabilityMonitor {
    /// Creates an unavailable observation until the main apply loop starts.
    pub(crate) fn new(pipeline_id: PipelineId) -> Self {
        Self { pipeline_id, state: Arc::default() }
    }

    /// Starts an attempt without declaring abandoned work durable.
    pub(crate) fn begin_attempt(&self) {
        let mut state = self.state.lock().unwrap();
        state.observing = true;
        if let Some(pending) = &mut state.outstanding {
            pending.replay_until = Some(pending.dispatched_lsn);
        }
        self.emit(state.age());
    }

    /// Opens or extends an interval before a nonempty destination call.
    pub(crate) fn dispatched(&self, replay_frontier: PgLsn) {
        let mut state = self.state.lock().unwrap();
        let pending = state.outstanding.get_or_insert_with(|| OutstandingDurability {
            since: Instant::now(),
            dispatched_lsn: replay_frontier,
            replay_until: None,
        });
        pending.dispatched_lsn = pending.dispatched_lsn.max(replay_frontier);
        self.emit(state.age());
    }

    /// Closes only intervals covered by a successfully handled durable result.
    pub(crate) fn confirmed(&self, commit_end_lsn: Option<PgLsn>) {
        let mut state = self.state.lock().unwrap();
        if state.outstanding.as_ref().is_some_and(|pending| {
            pending.replay_until.is_none_or(|frontier| commit_end_lsn >= Some(frontier))
        }) {
            state.outstanding = None;
        }
        self.emit(state.age());
    }

    /// Starts the sampler whose owner must abort and join it during teardown.
    pub(crate) fn spawn_durability_metrics_task(&self) -> AbortOnDropHandle<()> {
        let lifetime = DurabilitySamplerLifetime(self.clone());
        spawn_best_effort_task("durability monitor", async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let state = lifetime.0.state.lock().unwrap();
                lifetime.0.emit(state.age());
            }
        })
    }

    /// Publishes age with a bounded label set.
    fn emit(&self, age: f64) {
        let labels = [
            (PIPELINE_ID_LABEL, self.pipeline_id.to_string()),
            (WORKER_TYPE_LABEL, "apply".to_owned()),
            (REPLICATION_PATH_LABEL, CDC_REPLICATION_PATH.to_owned()),
        ];
        gauge!(ETL_DESTINATION_DURABILITY_AGE_SECONDS, &labels).set(age);
    }
}

/// Replaces cached values with unavailable when observation stops, even on
/// abort.
struct DurabilitySamplerLifetime(DurabilityMonitor);

impl Drop for DurabilitySamplerLifetime {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.observing = false;
        self.0.emit(UNAVAILABLE_AGE);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{replication::DurabilityMonitor, task::abort_and_join};

    /// Reads the age actually exported by the independent task.
    fn exported_age(rendered: &str) -> f64 {
        rendered
            .lines()
            .find(|line| {
                line.starts_with("etl_destination_durability_age_seconds{")
                    && line.contains("pipeline_id=\"42\"")
            })
            .unwrap()
            .split_whitespace()
            .last()
            .unwrap()
            .parse()
            .unwrap()
    }

    /// Withheld confirmation cannot freeze sampling, including past alert
    /// thresholds.
    #[tokio::test(start_paused = true)]
    async fn pending_confirmation_exports_increasing_age() {
        let metrics = etl_telemetry::metrics::init_metrics_handle().unwrap();
        let monitor = DurabilityMonitor::new(42);
        let task = monitor.spawn_durability_metrics_task();
        monitor.begin_attempt();
        tokio::task::yield_now().await;
        assert_eq!(exported_age(&metrics.render()), 0.0);
        monitor.dispatched(100.into());

        // Neither an Accepted response nor a source heartbeat touches this
        // state.
        for seconds in [301, 1201] {
            tokio::time::advance(Duration::from_secs(seconds)).await;
            tokio::task::yield_now().await;
            assert!(exported_age(&metrics.render()) >= seconds as f64);
        }
        monitor.confirmed(Some(100.into()));
        assert_eq!(exported_age(&metrics.render()), 0.0);
        tokio::time::advance(Duration::from_secs(1800)).await;
        tokio::task::yield_now().await;
        assert_eq!(exported_age(&metrics.render()), 0.0);
        abort_and_join(task).await.unwrap();
        assert_eq!(exported_age(&metrics.render()), -1.0);
    }

    /// Additional writes retain the first dispatch; replay must cover prior
    /// work.
    #[tokio::test(start_paused = true)]
    async fn retry_preserves_age_until_durable_replay_covers_prior_dispatch() {
        let monitor = DurabilityMonitor::new(43);
        monitor.begin_attempt();
        monitor.dispatched(100.into());
        tokio::time::advance(Duration::from_secs(60)).await;
        monitor.dispatched(200.into());
        monitor.begin_attempt();
        monitor.dispatched(100.into());
        monitor.confirmed(Some(100.into()));
        monitor.confirmed(None);
        assert_eq!(monitor.state.lock().unwrap().age(), 60.0);
        tokio::time::advance(Duration::from_secs(60)).await;
        monitor.dispatched(200.into());
        monitor.confirmed(Some(200.into()));
        assert_eq!(monitor.state.lock().unwrap().age(), 0.0);
        monitor.dispatched(300.into());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(monitor.state.lock().unwrap().age(), 5.0);
    }
}
