//! Typed GitHub REST payloads read by the CI metrics collector.
//!
//! Only fields the collector stores are declared; serde ignores the rest, so
//! unexpected or sensitive fields never reach the database.

use chrono::{DateTime, Utc};
use serde::Deserialize;

/// `GET /actions/workflows`.
#[derive(Debug, Deserialize)]
pub(super) struct WorkflowPage {
    pub(super) workflows: Vec<Workflow>,
}

/// One workflow definition.
#[derive(Debug, Deserialize)]
pub(super) struct Workflow {
    pub(super) id: i64,
    pub(super) name: String,
    pub(super) path: String,
}

/// `GET /actions/runs`.
#[derive(Debug, Deserialize)]
pub(super) struct RunPage {
    pub(super) total_count: u64,
    pub(super) workflow_runs: Vec<Run>,
}

/// One workflow run attempt, as returned by the run listing (latest attempt)
/// or by `GET /actions/runs/{id}/attempts/{n}`.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct Run {
    pub(super) id: i64,
    pub(super) run_attempt: i32,
    pub(super) workflow_id: i64,
    pub(super) display_title: String,
    pub(super) event: String,
    pub(super) head_branch: Option<String>,
    pub(super) head_sha: String,
    pub(super) path: String,
    pub(super) status: String,
    pub(super) conclusion: Option<String>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) updated_at: DateTime<Utc>,
    pub(super) run_started_at: Option<DateTime<Utc>>,
    pub(super) repository: Repository,
    pub(super) head_repository: Option<Repository>,
}

impl Run {
    /// Returns whether the run's code came from a fork of the repository.
    ///
    /// A missing head repository is treated as a fork, so unknown origins never
    /// gain manifest trust.
    pub(super) fn is_fork(&self) -> bool {
        self.head_repository.as_ref().is_none_or(|head| head.full_name != self.repository.full_name)
    }

    /// Returns the workflow path without a `@ref` suffix.
    pub(super) fn workflow_path(&self) -> &str {
        self.path.split_once('@').map_or(self.path.as_str(), |(path, _)| path)
    }
}

/// Repository identity on a run.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct Repository {
    pub(super) full_name: String,
}

/// `GET /actions/runs/{id}/jobs`.
#[derive(Debug, Deserialize)]
pub(super) struct JobPage {
    pub(super) total_count: u64,
    pub(super) jobs: Vec<Job>,
}

/// One job of one run attempt.
#[derive(Debug, Deserialize)]
pub(super) struct Job {
    pub(super) id: i64,
    pub(super) run_id: i64,
    pub(super) run_attempt: i32,
    pub(super) name: String,
    pub(super) status: String,
    pub(super) conclusion: Option<String>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) started_at: Option<DateTime<Utc>>,
    pub(super) completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(super) labels: Vec<String>,
    #[serde(default)]
    pub(super) steps: Vec<Step>,
}

/// One step of a job.
#[derive(Debug, Deserialize)]
pub(super) struct Step {
    pub(super) name: String,
    pub(super) number: i32,
    pub(super) conclusion: Option<String>,
}

/// `GET /actions/runs/{id}/artifacts`.
#[derive(Debug, Deserialize)]
pub(super) struct ArtifactPage {
    pub(super) total_count: u64,
    pub(super) artifacts: Vec<Artifact>,
}

/// One uploaded artifact.
#[derive(Debug, Deserialize)]
pub(super) struct Artifact {
    pub(super) id: i64,
    pub(super) name: String,
    pub(super) size_in_bytes: u64,
    pub(super) expired: bool,
    pub(super) archive_download_url: String,
}

/// One code-scanning alert, structural metadata only.
#[derive(Debug, Deserialize)]
pub(super) struct CodeScanningAlert {
    pub(super) number: i32,
    pub(super) state: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) updated_at: DateTime<Utc>,
    pub(super) fixed_at: Option<DateTime<Utc>>,
    pub(super) dismissed_at: Option<DateTime<Utc>>,
    pub(super) rule: AlertRule,
    pub(super) tool: Tool,
}

/// The rule behind an alert.
#[derive(Debug, Deserialize)]
pub(super) struct AlertRule {
    pub(super) id: Option<String>,
    pub(super) severity: Option<String>,
    pub(super) security_severity_level: Option<String>,
}

/// The analysis tool behind an alert or analysis.
#[derive(Debug, Deserialize)]
pub(super) struct Tool {
    pub(super) name: String,
}

/// One code-scanning analysis upload.
#[derive(Debug, Deserialize)]
pub(super) struct CodeScanningAnalysis {
    pub(super) id: i64,
    pub(super) created_at: DateTime<Utc>,
    pub(super) category: Option<String>,
    #[serde(default)]
    pub(super) error: String,
    #[serde(default)]
    pub(super) warning: String,
    pub(super) tool: Tool,
}
