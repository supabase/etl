//! Bounded runner results handed off to the maintenance controller.

use std::{io, path::Path};

use serde::{Deserialize, Serialize};
use tracing::warn;

const MAX_REPORT_BYTES: usize = 4096;

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
    /// Files created by merge-adjacent-files in this successful attempt.
    pub merge_adjacent_files_created: u64,
    /// Files created by rewrite-data-files in this successful attempt.
    pub rewrite_data_files_created: u64,
    /// Snapshots expired in this successful attempt.
    pub expired_snapshots: u64,
    /// Files removed in this successful attempt.
    pub cleaned_up_files: u64,
}

/// A runner-owned report delivered best-effort after maintenance succeeds.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceRunnerReport {
    /// Wire contract version.
    pub version: u32,
    /// Controller run identity, never included in metric labels.
    pub run_id: String,
    /// Project from the loaded runner configuration.
    pub project_ref: String,
    /// Pipeline from the loaded runner configuration.
    pub pipeline_id: String,
    /// Inline-flush measurements from this attempt.
    pub inline_flush: InlineFlushSummaries,
    /// Actual results from this attempt.
    pub results: MaintenanceResults,
}

impl MaintenanceRunnerReport {
    /// Writes bounded successful results for the kubelet to copy into Pod
    /// status.
    ///
    /// Delivery is best-effort: it must never retry successful maintenance or
    /// change its exit status. The controller validates ownership and
    /// attribution.
    pub async fn write_termination_message(&self, path: &Path) {
        if self.try_write_termination_message(path).await.is_err() {
            // File paths, report payloads and underlying errors may be
            // sensitive.
            warn!("maintenance succeeded but result delivery failed; keeping job successful");
        }
    }

    async fn try_write_termination_message(&self, path: &Path) -> io::Result<()> {
        if !self.is_valid() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        let bytes =
            serde_json::to_vec(self).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        if bytes.len() > MAX_REPORT_BYTES {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }

        tokio::fs::write(path, bytes).await
    }

    /// Validates attribution and numeric observations before writing a report.
    fn is_valid(&self) -> bool {
        self.version == 1
            && !self.run_id.is_empty()
            && !self.project_ref.is_empty()
            && !self.pipeline_id.is_empty()
            && [self.inline_flush.flushed.duration_seconds, self.inline_flush.noop.duration_seconds]
                .into_iter()
                .all(|duration| duration.is_finite() && duration >= 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> MaintenanceRunnerReport {
        MaintenanceRunnerReport {
            version: 1,
            run_id: "run-1".to_owned(),
            project_ref: "synthetic-project".to_owned(),
            pipeline_id: "7".to_owned(),
            inline_flush: InlineFlushSummaries::default(),
            results: MaintenanceResults { cleaned_up_files: 7, ..Default::default() },
        }
    }

    #[tokio::test]
    async fn report_file_contains_the_bounded_versioned_contract() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("termination-message");

        report().write_termination_message(&path).await;

        let bytes = tokio::fs::read(&path).await.unwrap();
        assert!(bytes.len() <= MAX_REPORT_BYTES);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["projectRef"], "synthetic-project");
        assert_eq!(value["pipelineId"], "7");
        assert_eq!(value["runId"], "run-1");
        assert_eq!(value["results"]["cleanedUpFiles"], 7);
        assert_eq!(value["inlineFlush"]["flushed"]["durationSeconds"], 0.0);
    }

    #[tokio::test]
    async fn invalid_or_oversized_reports_do_not_write_partial_results() {
        for case in ["run", "project", "pipeline", "duration", "nan", "version", "oversized"] {
            let mut report = report();
            match case {
                "run" => report.run_id.clear(),
                "project" => report.project_ref.clear(),
                "pipeline" => report.pipeline_id.clear(),
                "duration" => report.inline_flush.flushed.duration_seconds = -1.0,
                "nan" => report.inline_flush.flushed.duration_seconds = f64::NAN,
                "version" => report.version = 2,
                "oversized" => report.run_id = "r".repeat(MAX_REPORT_BYTES),
                _ => unreachable!(),
            }
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("termination-message");

            report.write_termination_message(&path).await;

            assert!(!path.exists(), "{case}");
        }
    }

    #[tokio::test]
    async fn delivery_failure_does_not_fail_successful_maintenance() {
        let dir = tempfile::tempdir().unwrap();

        // Writing to a directory fails deterministically, without real
        // credentials.
        report().write_termination_message(dir.path()).await;
    }
}
