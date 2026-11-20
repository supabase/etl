//! Durable, bounded maintenance observations shared through the maintenance CR.

use std::collections::BTreeMap;
#[cfg(feature = "ducklake")]
use std::sync::Once;

#[cfg(feature = "ducklake")]
use metrics::{describe_gauge, gauge};
use serde::{Deserialize, Serialize};

#[cfg(feature = "ducklake")]
const STARTED: &str = "etl_ducklake_maintenance_last_started_timestamp_seconds";
#[cfg(feature = "ducklake")]
const AVAILABLE: &str = "etl_ducklake_maintenance_results_available";
#[cfg(feature = "ducklake")]
const COMPLETED: &str = "etl_ducklake_maintenance_runs_completed_total";
#[cfg(feature = "ducklake")]
const DURATION_SUM: &str = "etl_ducklake_maintenance_run_duration_seconds_sum";
#[cfg(feature = "ducklake")]
const DURATION_COUNT: &str = "etl_ducklake_maintenance_run_duration_seconds_count";
#[cfg(feature = "ducklake")]
const LAST_COMPLETED: &str = "etl_ducklake_maintenance_last_completed_timestamp_seconds";
#[cfg(feature = "ducklake")]
const ROWS_SUM: &str = "etl_ducklake_inline_flush_rows_sum";
#[cfg(feature = "ducklake")]
const ROWS_COUNT: &str = "etl_ducklake_inline_flush_rows_count";
#[cfg(feature = "ducklake")]
const FLUSH_SUM: &str = "etl_ducklake_inline_flush_duration_seconds_sum";
#[cfg(feature = "ducklake")]
const FLUSH_COUNT: &str = "etl_ducklake_inline_flush_duration_seconds_count";
#[cfg(feature = "ducklake")]
const TRIGGER: &str = "trigger";
#[cfg(feature = "ducklake")]
const OUTCOME: &str = "outcome";
#[cfg(feature = "ducklake")]
const RESULT: &str = "result";
#[cfg(feature = "ducklake")]
const REPLAY_HELPER_ROWS_DELETED: &str =
    "etl_ducklake_maintenance_replay_helper_rows_deleted_total";
#[cfg(feature = "ducklake")]
const REPLAY_HELPER_FILES_CREATED: &str =
    "etl_ducklake_maintenance_replay_helper_files_created_total";
#[cfg(feature = "ducklake")]
const REPLAY_HELPER_TABLES_REWRITTEN: &str =
    "etl_ducklake_maintenance_replay_helper_tables_rewritten_total";
#[cfg(feature = "ducklake")]
const MERGE_ADJACENT_FILES_CREATED: &str =
    "etl_ducklake_maintenance_merge_adjacent_files_created_total";
#[cfg(feature = "ducklake")]
const REWRITE_DATA_FILES_CREATED: &str =
    "etl_ducklake_maintenance_rewrite_data_files_created_total";
#[cfg(feature = "ducklake")]
const EXPIRED_SNAPSHOTS: &str = "etl_ducklake_maintenance_expired_snapshots_total";
#[cfg(feature = "ducklake")]
const CLEANED_UP_FILES: &str = "etl_ducklake_maintenance_cleaned_up_files_total";

/// One result category in the inline-flush summaries.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InlineFlushSummary {
    /// Successful flush calls, including calls that found no rows.
    pub count: u64,
    /// Rows returned by successful flush calls.
    pub rows: u64,
    /// Total time inside successful flush calls.
    pub duration_seconds: f64,
}

/// Inline flush summaries partitioned by their bounded result.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InlineFlushSummaries {
    /// Flush calls that materialized rows.
    pub flushed: InlineFlushSummary,
    /// Flush calls that found no rows to materialize.
    pub noop: InlineFlushSummary,
}

/// Quantities returned by a completely successful maintenance attempt.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceResults {
    /// Cumulative replay helper rows deleted reported by successful runs.
    pub replay_helper_rows_deleted: u64,
    /// Cumulative replay helper files created reported by successful runs.
    pub replay_helper_files_created: u64,
    /// Cumulative replay helper tables rewritten reported by successful runs.
    pub replay_helper_tables_rewritten: u64,
    /// Cumulative merge adjacent files created reported by successful runs.
    pub merge_adjacent_files_created: u64,
    /// Cumulative rewrite data files created reported by successful runs.
    pub rewrite_data_files_created: u64,
    /// Cumulative expired snapshots reported by successful runs.
    pub expired_snapshots: u64,
    /// Cumulative cleaned up files reported by successful runs.
    pub cleaned_up_files: u64,
}

/// A runner-owned report persisted before the Job can succeed.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceRunnerReport {
    /// Wire contract version.
    pub version: u32,
    /// Controller run identity, retained in status rather than metric labels.
    pub run_id: String,
    /// Inline-flush measurements from this attempt.
    pub inline_flush: InlineFlushSummaries,
    /// Actual results from this attempt.
    pub results: MaintenanceResults,
}

impl MaintenanceRunnerReport {
    /// Validates the supported report before skipping already completed work.
    pub fn is_valid(&self) -> bool {
        self.version == 1
            && !self.run_id.is_empty()
            && [self.inline_flush.flushed.duration_seconds, self.inline_flush.noop.duration_seconds]
                .into_iter()
                .all(|duration| duration.is_finite() && duration >= 0.0)
    }
}

/// Controller-selected source of a maintenance run.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceTrigger {
    /// A request raised by the replicator.
    Requested,
    /// A scheduled maintenance run.
    Scheduled,
}

impl MaintenanceTrigger {
    /// Returns the bounded metric label.
    #[cfg(feature = "ducklake")]
    fn label(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Scheduled => "scheduled",
        }
    }
}

/// Terminal result confirmed by the controller.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOutcome {
    /// Kubernetes confirmed the job succeeded.
    Succeeded,
    /// The job failed, disappeared, or exceeded its pause deadline.
    Failed,
}

impl MaintenanceOutcome {
    /// Returns the bounded metric label.
    #[cfg(feature = "ducklake")]
    fn label(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Durable totals for one trigger and outcome.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceRunSummary {
    /// Completed runs.
    pub count: u64,
    /// Total elapsed run time, including draining and controller observation.
    pub duration_seconds: f64,
    /// Unix seconds of the latest controller-confirmed completion.
    pub last_completed_at: f64,
}

/// Controller-owned cumulative observations, bounded independently of run
/// count.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceMetrics {
    /// Wire contract version.
    pub version: u32,
    /// Completed run observations with at most four label combinations.
    pub runs: BTreeMap<MaintenanceTrigger, BTreeMap<MaintenanceOutcome, MaintenanceRunSummary>>,
    /// Cumulative inline-flush observations.
    pub inline_flush: InlineFlushSummaries,
    /// Cumulative results of completely successful reported runs.
    pub results: MaintenanceResults,
    /// Whether quantities are known for the most recent completed run.
    pub results_available: bool,
    /// Last controller run start as Unix seconds.
    pub last_started_at: f64,
}

impl MaintenanceMetrics {
    /// Rejects unsupported versions and non-finite or negative observations.
    pub fn is_valid(&self) -> bool {
        let valid = |value: f64| value.is_finite() && value >= 0.0;
        self.version == 1
            && valid(self.last_started_at)
            && valid(self.inline_flush.flushed.duration_seconds)
            && valid(self.inline_flush.noop.duration_seconds)
            && self
                .runs
                .values()
                .flat_map(BTreeMap::values)
                .all(|run| valid(run.duration_seconds) && valid(run.last_completed_at))
    }
}

/// Registers the durable-value gauges once per process.
#[cfg(feature = "ducklake")]
fn register_metrics() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        describe_gauge!(
            STARTED,
            "Unix seconds from the latest controller-confirmed completed run."
        );
        describe_gauge!(
            AVAILABLE,
            "Whether the latest completed run has a valid, successful runner report."
        );
        describe_gauge!(
            COMPLETED,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            DURATION_SUM,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            DURATION_COUNT,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            LAST_COMPLETED,
            "Unix seconds from the latest controller-confirmed completed run."
        );
        describe_gauge!(
            ROWS_SUM,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            ROWS_COUNT,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            FLUSH_SUM,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            FLUSH_COUNT,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            REPLAY_HELPER_ROWS_DELETED,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            REPLAY_HELPER_FILES_CREATED,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            REPLAY_HELPER_TABLES_REWRITTEN,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            MERGE_ADJACENT_FILES_CREATED,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            REWRITE_DATA_FILES_CREATED,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            EXPIRED_SNAPSHOTS,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
        describe_gauge!(
            CLEANED_UP_FILES,
            "Persisted cumulative maintenance observation from controller-confirmed runs; resets \
             on CR replacement."
        );
    });
}

/// Publishes the durable summary components and counters without replaying
/// samples.
///
/// Components are gauges of persisted cumulative values. Repeated observations
/// cannot increment them; CR replacement produces a visible reset. Consumers
/// use rate/increase before aggregating source identities, just as for summary
/// sums and counts. Missing or invalid state clears result availability.
#[cfg(feature = "ducklake")]
pub fn record_maintenance_metrics(snapshot: Option<&MaintenanceMetrics>) {
    register_metrics();

    let Some(snapshot) = snapshot.filter(|snapshot| snapshot.is_valid()) else {
        gauge!(AVAILABLE).set(0.0);
        return;
    };
    gauge!(STARTED).set(snapshot.last_started_at);
    gauge!(AVAILABLE).set(if snapshot.results_available { 1.0 } else { 0.0 });
    for (trigger, outcomes) in &snapshot.runs {
        for (outcome, run) in outcomes {
            for (name, value) in [
                (COMPLETED, run.count as f64),
                (DURATION_SUM, run.duration_seconds),
                (DURATION_COUNT, run.count as f64),
                (LAST_COMPLETED, run.last_completed_at),
            ] {
                gauge!(name, TRIGGER => trigger.label(), OUTCOME => outcome.label()).set(value);
            }
        }
    }
    for (result, summary) in
        [("flushed", &snapshot.inline_flush.flushed), ("noop", &snapshot.inline_flush.noop)]
    {
        for (name, value) in [
            (ROWS_SUM, summary.rows as f64),
            (ROWS_COUNT, summary.count as f64),
            (FLUSH_SUM, summary.duration_seconds),
            (FLUSH_COUNT, summary.count as f64),
        ] {
            gauge!(name, RESULT => result).set(value);
        }
    }
    gauge!(REPLAY_HELPER_ROWS_DELETED).set(snapshot.results.replay_helper_rows_deleted as f64);
    gauge!(REPLAY_HELPER_FILES_CREATED).set(snapshot.results.replay_helper_files_created as f64);
    gauge!(REPLAY_HELPER_TABLES_REWRITTEN)
        .set(snapshot.results.replay_helper_tables_rewritten as f64);
    gauge!(MERGE_ADJACENT_FILES_CREATED).set(snapshot.results.merge_adjacent_files_created as f64);
    gauge!(REWRITE_DATA_FILES_CREATED).set(snapshot.results.rewrite_data_files_created as f64);
    gauge!(EXPIRED_SNAPSHOTS).set(snapshot.results.expired_snapshots as f64);
    gauge!(CLEANED_UP_FILES).set(snapshot.results.cleaned_up_files as f64);
}

#[cfg(all(test, feature = "ducklake"))]
mod tests {
    use etl_telemetry::metrics::init_metrics_handle;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn durable_export_does_not_recount_and_invalidates_missing_state() {
        let handle = init_metrics_handle().unwrap();
        let mut snapshot: MaintenanceMetrics = serde_json::from_value(json!({
            "version": 1, "lastStartedAt": 100, "resultsAvailable": true,
            "runs": {"scheduled": {"succeeded": {"count": 1, "durationSeconds": 5, "lastCompletedAt": 105}}},
            "inlineFlush": {"flushed": {"count": 2, "rows": 12, "durationSeconds": 0.5},
                "noop": {"count": 1, "rows": 0, "durationSeconds": 0.1}},
            "results": {"replayHelperRowsDeleted": 3, "replayHelperFilesCreated": 0,
                "replayHelperTablesRewritten": 1, "mergeAdjacentFilesCreated": 2,
                "rewriteDataFilesCreated": 4, "expiredSnapshots": 6, "cleanedUpFiles": 8}
        })).unwrap();
        for _ in 0..3 {
            record_maintenance_metrics(Some(&snapshot));
        }
        let text = handle.render();
        assert!(text.contains("etl_ducklake_maintenance_cleaned_up_files_total 8"));
        assert!(text.contains("etl_ducklake_inline_flush_rows_sum{result=\"flushed\"} 12"));
        assert!(!text.contains("run_id"));
        record_maintenance_metrics(None);
        assert!(handle.render().contains("etl_ducklake_maintenance_results_available 0"));
        snapshot.results.cleaned_up_files = 1;
        record_maintenance_metrics(Some(&snapshot));
        assert!(handle.render().contains("etl_ducklake_maintenance_cleaned_up_files_total 1"));
        snapshot.inline_flush.flushed.duration_seconds = -1.0;
        record_maintenance_metrics(Some(&snapshot));
        assert!(handle.render().contains("etl_ducklake_maintenance_results_available 0"));
    }
}
