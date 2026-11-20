//! Bounded runner results handed off to the maintenance controller.

use serde::{Deserialize, Serialize};

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
    /// replay helper rows deleted returned by this successful attempt.
    pub replay_helper_rows_deleted: u64,
    /// replay helper files created returned by this successful attempt.
    pub replay_helper_files_created: u64,
    /// replay helper tables rewritten returned by this successful attempt.
    pub replay_helper_tables_rewritten: u64,
    /// merge adjacent files created returned by this successful attempt.
    pub merge_adjacent_files_created: u64,
    /// rewrite data files created returned by this successful attempt.
    pub rewrite_data_files_created: u64,
    /// expired snapshots returned by this successful attempt.
    pub expired_snapshots: u64,
    /// cleaned up files returned by this successful attempt.
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
