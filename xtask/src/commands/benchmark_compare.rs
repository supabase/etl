use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::Deserialize;
use serde_json::Value;

const DEFAULT_ARTIFACT_NAME: &str = "benchmark-results";
const DEFAULT_CURRENT_DIR: &str = "target/bench-results";
const DEFAULT_WORKFLOW: &str = "benchmark.yml";
const GITHUB_API_BASE: &str = "https://api.github.com";
const GITHUB_API_VERSION: &str = "2022-11-28";
const GITHUB_ACCEPT: &str = "application/vnd.github+json";
const GITHUB_USER_AGENT: &str = "supabase-etl-benchmark-compare";

const TABLE_COPY_METRICS: &[Metric] = &[
    Metric { key: "copied_rows", label: "Rows copied", policy: RegressionPolicy::Informational },
    Metric {
        key: "estimated_copied_mib",
        label: "Est. decoded MiB",
        policy: RegressionPolicy::Informational,
    },
    Metric {
        key: "rows_per_second",
        label: "Rows/s",
        policy: RegressionPolicy::HigherIsBetter { max_drop_pct: 15.0 },
    },
    Metric {
        key: "estimated_mib_per_second",
        label: "Est. decoded MiB/s",
        policy: RegressionPolicy::HigherIsBetter { max_drop_pct: 15.0 },
    },
    Metric {
        key: "copy_wait_ms",
        label: "Copy wait ms",
        policy: RegressionPolicy::LowerIsBetter { max_increase_pct: 20.0 },
    },
    Metric {
        key: "total_ms",
        label: "Total ms",
        policy: RegressionPolicy::LowerIsBetter { max_increase_pct: 20.0 },
    },
];
const TABLE_STREAMING_METRICS: &[Metric] = &[
    Metric {
        key: "produced_events",
        label: "Produced events",
        policy: RegressionPolicy::Informational,
    },
    Metric {
        key: "observed_cdc_events",
        label: "Observed CDC events",
        policy: RegressionPolicy::Informational,
    },
    Metric {
        key: "end_to_end_with_shutdown_events_per_second",
        label: "Events/s",
        policy: RegressionPolicy::HigherIsBetter { max_drop_pct: 15.0 },
    },
    Metric {
        key: "end_to_end_with_shutdown_estimated_mib_per_second",
        label: "Est. decoded MiB/s",
        policy: RegressionPolicy::HigherIsBetter { max_drop_pct: 15.0 },
    },
    Metric {
        key: "total_ms",
        label: "Total ms",
        policy: RegressionPolicy::LowerIsBetter { max_increase_pct: 20.0 },
    },
];
const CONFIG_KEYS: &[&str] = &[
    "destination",
    "workload",
    "table_count",
    "duration_seconds",
    "tpcc_warehouses",
    "tpcc_threads",
    "drain_quiet_ms",
    "drain_poll_ms",
    "batch_max_fill_ms",
    "memory_budget_ratio",
    "memory_backpressure_enabled",
    "max_table_sync_workers",
    "max_copy_connections_per_table",
];

type Reports = BTreeMap<String, Value>;

#[derive(Args)]
pub(crate) struct BenchmarkCompareArgs {
    /// Current benchmark results directory.
    #[arg(long, default_value = DEFAULT_CURRENT_DIR)]
    current_dir: PathBuf,
    /// Previous benchmark results directory for local comparisons.
    #[arg(long)]
    previous_dir: Option<PathBuf>,
    /// Markdown output path. Defaults to benchmark-comparison.md in
    /// current-dir.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Workflow file to inspect in GitHub Actions mode.
    #[arg(long, default_value = DEFAULT_WORKFLOW)]
    workflow: String,
    /// Artifact name to download in GitHub Actions mode.
    #[arg(long, default_value = DEFAULT_ARTIFACT_NAME)]
    artifact_name: String,
    /// Git ref name to compare within in GitHub Actions mode.
    #[arg(long = "ref")]
    ref_name: Option<String>,
}

#[derive(Clone, Copy)]
struct Metric {
    key: &'static str,
    label: &'static str,
    policy: RegressionPolicy,
}

#[derive(Clone, Copy)]
enum RegressionPolicy {
    HigherIsBetter { max_drop_pct: f64 },
    LowerIsBetter { max_increase_pct: f64 },
    Informational,
}

#[derive(Deserialize)]
struct WorkflowRunsResponse {
    workflow_runs: Vec<WorkflowRun>,
}

#[derive(Clone, Deserialize)]
struct WorkflowRun {
    id: u64,
    run_number: u64,
    html_url: String,
    conclusion: Option<String>,
    head_sha: Option<String>,
}

#[derive(Deserialize)]
struct ArtifactsResponse {
    artifacts: Vec<Artifact>,
}

#[derive(Deserialize)]
struct Artifact {
    name: String,
    expired: bool,
    archive_download_url: String,
}

struct PreviousSource {
    reports: Reports,
    run: Option<WorkflowRun>,
    code_compare: Option<CodeCompare>,
}

struct Comparison {
    markdown: String,
    failures: Vec<String>,
}

struct CodeCompare {
    previous_sha: String,
    current_sha: String,
    url: String,
}

struct GitHubContext {
    token: String,
    repo: String,
    current_run_id: u64,
    ref_name: String,
    current_sha: Option<String>,
}

struct GitHubClient {
    client: reqwest::Client,
    token: String,
    repo: String,
}

struct TempDir {
    path: PathBuf,
}

impl BenchmarkCompareArgs {
    pub(crate) async fn run(self) -> Result<()> {
        let current_reports = load_reports(&self.current_dir).with_context(|| {
            format!("failed to load reports from {}", self.current_dir.display())
        })?;
        let output =
            self.output.clone().unwrap_or_else(|| self.current_dir.join("benchmark-comparison.md"));
        let comparison = match self.previous_source().await? {
            Some(previous) => compare_reports(
                &previous.reports,
                &current_reports,
                previous.run.as_ref(),
                previous.code_compare.as_ref(),
            ),
            None => Comparison { markdown: render_missing_previous(&self), failures: Vec::new() },
        };

        if let Some(parent) = output.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&output, format!("{}\n", comparison.markdown))
            .with_context(|| format!("failed to write {}", output.display()))?;
        println!("{}", comparison.markdown);
        if !comparison.failures.is_empty() {
            bail!(
                "benchmark comparison failed with {} regression(s): {}",
                comparison.failures.len(),
                comparison.failures.join("; ")
            );
        }

        Ok(())
    }

    async fn previous_source(&self) -> Result<Option<PreviousSource>> {
        if let Some(previous_dir) = &self.previous_dir {
            let reports = load_reports(previous_dir).with_context(|| {
                format!("failed to load reports from {}", previous_dir.display())
            })?;
            return Ok(Some(PreviousSource { reports, run: None, code_compare: None }));
        }

        let Some(context) = self.github_context() else {
            eprintln!(
                "GitHub Actions environment is incomplete; pass --previous-dir for local \
                 comparisons."
            );
            return Ok(None);
        };

        let client = GitHubClient::new(context.token.clone(), context.repo.clone())?;
        let Some((reports, run, code_compare)) =
            self.load_github_previous_reports(&client, &context).await?
        else {
            return Ok(None);
        };

        Ok(Some(PreviousSource { reports, run: Some(run), code_compare }))
    }

    fn github_context(&self) -> Option<GitHubContext> {
        let token = env::var("GITHUB_TOKEN").ok()?;
        let repo = env::var("GITHUB_REPOSITORY").ok()?;
        let current_run_id = env::var("GITHUB_RUN_ID").ok()?.parse().ok()?;
        let ref_name = self.ref_name.clone().or_else(|| env::var("GITHUB_REF_NAME").ok())?;
        let current_sha = env::var("GITHUB_SHA").ok();
        Some(GitHubContext { token, repo, current_run_id, ref_name, current_sha })
    }

    async fn load_github_previous_reports(
        &self,
        client: &GitHubClient,
        context: &GitHubContext,
    ) -> Result<Option<(Reports, WorkflowRun, Option<CodeCompare>)>> {
        let runs = client.workflow_runs(&self.workflow, &context.ref_name).await?;
        for run in runs {
            if run.id == context.current_run_id || run.conclusion.as_deref() != Some("success") {
                continue;
            }

            let Some(artifact) = client.find_artifact(run.id, &self.artifact_name).await? else {
                continue;
            };

            let temp_dir = TempDir::new("etl-benchmark-previous")?;
            let archive_path = temp_dir.path.join("benchmark-results.zip");
            let extract_dir = temp_dir.path.join("extract");
            fs::create_dir_all(&extract_dir)
                .with_context(|| format!("failed to create {}", extract_dir.display()))?;
            client.download_artifact(&artifact.archive_download_url, &archive_path).await?;
            unzip_archive(&archive_path, &extract_dir)?;
            let reports = load_reports(&extract_dir).with_context(|| {
                format!(
                    "failed to load benchmark reports from artifact {} in run {}",
                    self.artifact_name, run.run_number
                )
            })?;
            let code_compare = code_compare(&context.repo, &run, context.current_sha.as_deref());
            return Ok(Some((reports, run, code_compare)));
        }

        eprintln!(
            "No previous successful benchmark artifact named '{}' was found for ref '{}'.",
            self.artifact_name, context.ref_name
        );
        Ok(None)
    }
}

impl GitHubClient {
    fn new(token: String, repo: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(GITHUB_USER_AGENT)
            .build()
            .context("failed to build GitHub HTTP client")?;
        Ok(Self { client, token, repo })
    }

    async fn workflow_runs(&self, workflow: &str, ref_name: &str) -> Result<Vec<WorkflowRun>> {
        let url = format!(
            "{GITHUB_API_BASE}/repos/{}/actions/workflows/{}/runs",
            self.repo,
            encode_path_segment(workflow)
        );
        let response = self
            .client
            .get(url)
            .query(&[
                ("branch", ref_name),
                ("event", "workflow_dispatch"),
                ("status", "completed"),
                ("per_page", "30"),
            ])
            .bearer_auth(&self.token)
            .header("Accept", GITHUB_ACCEPT)
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .send()
            .await
            .context("failed to list benchmark workflow runs")?;
        let response = response.error_for_status().context("GitHub workflow run listing failed")?;
        let payload: WorkflowRunsResponse =
            response.json().await.context("failed to parse GitHub workflow run listing")?;
        Ok(payload.workflow_runs)
    }

    async fn find_artifact(&self, run_id: u64, artifact_name: &str) -> Result<Option<Artifact>> {
        let url = format!("{GITHUB_API_BASE}/repos/{}/actions/runs/{run_id}/artifacts", self.repo);
        let response = self
            .client
            .get(url)
            .query(&[("per_page", "100")])
            .bearer_auth(&self.token)
            .header("Accept", GITHUB_ACCEPT)
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .send()
            .await
            .with_context(|| format!("failed to list artifacts for workflow run {run_id}"))?;
        let response = response
            .error_for_status()
            .with_context(|| format!("GitHub artifact listing failed for run {run_id}"))?;
        let payload: ArtifactsResponse = response
            .json()
            .await
            .with_context(|| format!("failed to parse artifacts for workflow run {run_id}"))?;
        Ok(payload
            .artifacts
            .into_iter()
            .find(|artifact| artifact.name == artifact_name && !artifact.expired))
    }

    async fn download_artifact(&self, url: &str, output_path: &Path) -> Result<()> {
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .header("Accept", GITHUB_ACCEPT)
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .send()
            .await
            .context("failed to download benchmark artifact")?;
        let response = response.error_for_status().context("GitHub artifact download failed")?;
        let bytes = response.bytes().await.context("failed to read benchmark artifact")?;
        fs::write(output_path, bytes)
            .with_context(|| format!("failed to write {}", output_path.display()))
    }
}

impl TempDir {
    fn new(prefix: &str) -> Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_millis();
        let path = env::temp_dir().join(format!("{prefix}-{timestamp}-{}", std::process::id()));
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self { path })
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn load_reports(dir: &Path) -> Result<Reports> {
    let mut reports = Reports::new();
    let entries = fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }

        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let report: Value = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        let benchmark = report
            .get("benchmark")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| path.file_stem().and_then(|stem| stem.to_str()).map(ToOwned::to_owned))
            .with_context(|| {
                format!("failed to determine benchmark name for {}", path.display())
            })?;
        reports.insert(benchmark, report);
    }

    if reports.is_empty() {
        bail!("no benchmark JSON reports found in {}", dir.display());
    }

    Ok(reports)
}

fn render_missing_previous(args: &BenchmarkCompareArgs) -> String {
    format!(
        "## 📈 Benchmark comparison\n\nℹ️ No previous benchmark artifact was available. In GitHub \
         Actions, this compares against the most recent successful `{}` artifact from `{}` on the \
         same ref. Locally, pass `--previous-dir`.",
        args.artifact_name, args.workflow
    )
}

fn compare_reports(
    previous_reports: &Reports,
    current_reports: &Reports,
    previous_run: Option<&WorkflowRun>,
    code_compare: Option<&CodeCompare>,
) -> Comparison {
    let mut lines = vec!["## 📈 Benchmark comparison".to_owned(), String::new()];
    let mut failures = Vec::new();
    if let Some(previous_run) = previous_run {
        lines.push(format!(
            "Compared with previous successful run [#{}]({}) at `{}`.",
            previous_run.run_number,
            previous_run.html_url,
            previous_run.head_sha.as_deref().map_or("unknown commit", short_sha)
        ));
        if let Some(code_compare) = code_compare {
            lines.push(format!(
                "Code diff: [`{}...{}`]({}).",
                short_sha(&code_compare.previous_sha),
                short_sha(&code_compare.current_sha),
                code_compare.url
            ));
        }
    } else {
        lines.push("Compared with local previous benchmark results.".to_owned());
    }
    lines.push(String::new());

    let common_benchmarks = previous_reports
        .keys()
        .filter(|benchmark| current_reports.contains_key(*benchmark))
        .cloned()
        .collect::<Vec<_>>();
    if common_benchmarks.is_empty() {
        lines.push("No benchmark reports had matching names between the two runs.".to_owned());
        return Comparison { markdown: lines.join("\n"), failures };
    }

    for benchmark in common_benchmarks {
        let previous = previous_reports.get(&benchmark).expect("benchmark key exists");
        let current = current_reports.get(&benchmark).expect("benchmark key exists");
        lines.push(format!("### {}", benchmark_title(&benchmark)));

        let changes = config_changes(previous, current);
        if !changes.is_empty() {
            lines.push(String::new());
            lines.push(format!("> ⚠️ Benchmark configuration differs: {}.", changes.join(", ")));
        }

        lines.push(String::new());
        lines.push("| Metric | Previous | Current | Change | Result |".to_owned());
        lines.push("| --- | ---: | ---: | ---: | --- |".to_owned());
        for metric in metrics_for(&benchmark) {
            let Some(previous_value) = previous.get(metric.key) else {
                continue;
            };
            let Some(current_value) = current.get(metric.key) else {
                continue;
            };

            lines.push(format!(
                "| {} | {} | {} | {} | {} |",
                metric.label,
                format_value(previous_value),
                format_value(current_value),
                format_delta(previous_value, current_value),
                format_change_result(metric.policy, previous_value, current_value)
            ));
        }

        let benchmark_failures = if changes.is_empty() {
            regression_failures(&benchmark, previous, current)
        } else {
            Vec::new()
        };
        lines.push(String::new());
        if changes.is_empty() {
            if benchmark_failures.is_empty() {
                lines.push("✅ Regression gate: passed.".to_owned());
            } else {
                lines.push("🚨 Regression gate: failed.".to_owned());
                for failure in &benchmark_failures {
                    lines.push(format!("- 🚨 {failure}"));
                }
                failures.extend(benchmark_failures);
            }
        } else {
            lines.push(
                "⚠️ Regression gate: skipped because benchmark configuration differs.".to_owned(),
            );
        }
        lines.push(String::new());
    }

    let previous_only = previous_reports
        .keys()
        .filter(|benchmark| !current_reports.contains_key(*benchmark))
        .cloned()
        .collect::<Vec<_>>();
    let current_only = current_reports
        .keys()
        .filter(|benchmark| !previous_reports.contains_key(*benchmark))
        .cloned()
        .collect::<Vec<_>>();
    if !previous_only.is_empty() {
        lines.push(format!("ℹ️ Previous-only reports: {}.", previous_only.join(", ")));
    }
    if !current_only.is_empty() {
        lines.push(format!("ℹ️ Current-only reports: {}.", current_only.join(", ")));
    }

    Comparison { markdown: lines.join("\n"), failures }
}

fn metrics_for(benchmark: &str) -> &'static [Metric] {
    match benchmark {
        "table_copy" => TABLE_COPY_METRICS,
        "table_streaming" => TABLE_STREAMING_METRICS,
        _ => &[],
    }
}

fn benchmark_title(benchmark: &str) -> String {
    match benchmark {
        "table_copy" => "📦 Table copy".to_owned(),
        "table_streaming" => "🔁 Table streaming".to_owned(),
        benchmark => benchmark.to_owned(),
    }
}

fn code_compare(
    repo: &str,
    previous_run: &WorkflowRun,
    current_sha: Option<&str>,
) -> Option<CodeCompare> {
    let previous_sha = previous_run.head_sha.as_deref()?.trim();
    let current_sha = current_sha?.trim();
    if previous_sha.is_empty() || current_sha.is_empty() {
        return None;
    }

    Some(CodeCompare {
        previous_sha: previous_sha.to_owned(),
        current_sha: current_sha.to_owned(),
        url: format!("https://github.com/{repo}/compare/{previous_sha}...{current_sha}"),
    })
}

fn short_sha(sha: &str) -> &str {
    sha.get(..7).unwrap_or(sha)
}

fn config_changes(previous: &Value, current: &Value) -> Vec<String> {
    CONFIG_KEYS
        .iter()
        .filter_map(|key| {
            let previous = previous.get(*key);
            let current = current.get(*key);
            if previous.is_none() && current.is_none() {
                return None;
            }

            (previous != current).then(|| {
                format!(
                    "`{key}` {} -> {}",
                    format_optional_value(previous),
                    format_optional_value(current)
                )
            })
        })
        .collect()
}

fn regression_failures(benchmark: &str, previous: &Value, current: &Value) -> Vec<String> {
    metrics_for(benchmark)
        .iter()
        .filter_map(|metric| {
            let previous = previous.get(metric.key)?;
            let current = current.get(metric.key)?;
            regression_failure(benchmark, metric, previous, current)
        })
        .collect()
}

fn regression_failure(
    benchmark: &str,
    metric: &Metric,
    previous: &Value,
    current: &Value,
) -> Option<String> {
    match metric.policy {
        RegressionPolicy::HigherIsBetter { max_drop_pct } => {
            let previous = numeric_value(previous)?;
            let current = numeric_value(current)?;
            if previous <= 0.0 {
                return None;
            }

            let drop_pct = ((previous - current) / previous) * 100.0;
            (drop_pct > max_drop_pct).then(|| {
                format!(
                    "{benchmark} {} dropped by {:.2}% from {} to {} (allowed {:.2}%)",
                    metric.label,
                    drop_pct,
                    format_number(previous),
                    format_number(current),
                    max_drop_pct
                )
            })
        }
        RegressionPolicy::LowerIsBetter { max_increase_pct } => {
            let previous = numeric_value(previous)?;
            let current = numeric_value(current)?;
            if previous <= 0.0 {
                return None;
            }

            let increase_pct = ((current - previous) / previous) * 100.0;
            (increase_pct > max_increase_pct).then(|| {
                format!(
                    "{benchmark} {} increased by {:.2}% from {} to {} (allowed {:.2}%)",
                    metric.label,
                    increase_pct,
                    format_number(previous),
                    format_number(current),
                    max_increase_pct
                )
            })
        }
        RegressionPolicy::Informational => None,
    }
}

fn format_delta(previous: &Value, current: &Value) -> String {
    let Some(previous) = numeric_value(previous) else {
        return String::new();
    };
    let Some(current) = numeric_value(current) else {
        return String::new();
    };

    let delta = current - previous;
    let sign = if delta.is_sign_negative() { "" } else { "+" };
    let pct = if previous == 0.0 { None } else { Some((delta / previous) * 100.0) };
    match pct {
        Some(pct) => format!("{sign}{} ({sign}{pct:.2}%)", format_number(delta)),
        None => format!("{sign}{}", format_number(delta)),
    }
}

fn format_change_result(
    policy: RegressionPolicy,
    previous: &Value,
    current: &Value,
) -> &'static str {
    match policy {
        RegressionPolicy::Informational => "ℹ️ info",
        RegressionPolicy::HigherIsBetter { max_drop_pct } => {
            let Some(previous) = numeric_value(previous) else {
                return "";
            };
            let Some(current) = numeric_value(current) else {
                return "";
            };
            if current > previous {
                "✅ better"
            } else if current < previous {
                if previous <= 0.0 {
                    "⚠️ worse"
                } else {
                    let drop_pct = ((previous - current) / previous) * 100.0;
                    if drop_pct > max_drop_pct {
                        "🚨 regression"
                    } else {
                        "⚠️ within threshold"
                    }
                }
            } else {
                "➖ same"
            }
        }
        RegressionPolicy::LowerIsBetter { max_increase_pct } => {
            let Some(previous) = numeric_value(previous) else {
                return "";
            };
            let Some(current) = numeric_value(current) else {
                return "";
            };
            if current < previous {
                "✅ better"
            } else if current > previous {
                if previous <= 0.0 {
                    "⚠️ worse"
                } else {
                    let increase_pct = ((current - previous) / previous) * 100.0;
                    if increase_pct > max_increase_pct {
                        "🚨 regression"
                    } else {
                        "⚠️ within threshold"
                    }
                }
            } else {
                "➖ same"
            }
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => match value.as_f64() {
            Some(value) => format_number(value),
            None => value.to_string(),
        },
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}

fn format_optional_value(value: Option<&Value>) -> String {
    value.map_or_else(|| "<missing>".to_owned(), format_value)
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else if value.abs() >= 100.0 {
        format!("{value:.2}")
    } else if value.abs() >= 10.0 {
        format!("{value:.3}")
    } else {
        format!("{value:.4}")
    }
}

fn numeric_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn unzip_archive(archive_path: &Path, output_dir: &Path) -> Result<()> {
    let status = Command::new("unzip")
        .arg("-qq")
        .arg(archive_path)
        .arg("-d")
        .arg(output_dir)
        .status()
        .context("failed to run unzip")?;
    if !status.success() {
        bail!("failed to unzip benchmark artifact");
    }

    Ok(())
}

fn encode_path_segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                char::from(byte).to_string()
            }
            byte => format!("%{byte:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::commands::benchmark_compare::{
        CodeCompare, Reports, WorkflowRun, compare_reports, config_changes, regression_failures,
    };

    #[test]
    fn config_changes_detects_changed_and_missing_fields() {
        let previous = json!({
            "destination": "null",
            "memory_budget_ratio": 0.2
        });
        let current = json!({
            "destination": "null",
            "memory_budget_ratio": 0.3,
            "memory_backpressure_enabled": false
        });

        let changes = config_changes(&previous, &current);

        assert_eq!(
            changes,
            vec![
                "`memory_budget_ratio` 0.2000 -> 0.3000",
                "`memory_backpressure_enabled` <missing> -> false"
            ]
        );
    }

    #[test]
    fn table_copy_regression_gate_flags_threshold_failures() {
        let previous = json!({
            "copied_rows": 1_000,
            "rows_per_second": 1_000.0,
            "estimated_mib_per_second": 100.0,
            "copy_wait_ms": 1_000.0,
            "total_ms": 1_000.0
        });
        let current = json!({
            "copied_rows": 900,
            "rows_per_second": 800.0,
            "estimated_mib_per_second": 90.0,
            "copy_wait_ms": 1_250.0,
            "total_ms": 1_100.0
        });

        let failures = regression_failures("table_copy", &previous, &current);

        assert_eq!(failures.len(), 2);
        assert!(failures[0].contains("Rows/s dropped by 20.00%"));
        assert!(failures[1].contains("Copy wait ms increased by 25.00%"));
    }

    #[test]
    fn table_streaming_regression_gate_allows_improvements() {
        let previous = json!({
            "produced_events": 1_000,
            "observed_cdc_events": 1_000,
            "end_to_end_with_shutdown_events_per_second": 1_000.0,
            "end_to_end_with_shutdown_estimated_mib_per_second": 100.0,
            "total_ms": 1_000.0
        });
        let current = json!({
            "produced_events": 2_000,
            "observed_cdc_events": 2_000,
            "end_to_end_with_shutdown_events_per_second": 1_200.0,
            "end_to_end_with_shutdown_estimated_mib_per_second": 120.0,
            "total_ms": 900.0
        });

        let failures = regression_failures("table_streaming", &previous, &current);

        assert!(failures.is_empty());
    }

    #[test]
    fn compare_reports_labels_changes_by_metric_direction() {
        let mut previous = Reports::new();
        previous.insert(
            "table_copy".to_owned(),
            json!({
                "benchmark": "table_copy",
                "destination": "null",
                "table_count": 8,
                "rows_per_second": 1_000.0,
                "estimated_mib_per_second": 100.0,
                "copy_wait_ms": 1_000.0,
                "total_ms": 1_000.0
            }),
        );

        let mut current = Reports::new();
        current.insert(
            "table_copy".to_owned(),
            json!({
                "benchmark": "table_copy",
                "destination": "null",
                "table_count": 8,
                "rows_per_second": 1_100.0,
                "estimated_mib_per_second": 90.0,
                "copy_wait_ms": 900.0,
                "total_ms": 1_100.0
            }),
        );

        let comparison = compare_reports(&previous, &current, None, None);

        assert!(
            comparison.markdown.contains("| Rows/s | 1000 | 1100 | +100 (+10.00%) | ✅ better |")
        );
        assert!(
            comparison.markdown.contains(
                "| Est. decoded MiB/s | 100 | 90 | -10 (-10.00%) | ⚠️ within threshold |"
            )
        );
        assert!(
            comparison
                .markdown
                .contains("| Copy wait ms | 1000 | 900 | -100 (-10.00%) | ✅ better |")
        );
        assert!(
            comparison
                .markdown
                .contains("| Total ms | 1000 | 1100 | +100 (+10.00%) | ⚠️ within threshold |")
        );
    }

    #[test]
    fn compare_reports_labels_threshold_breaking_changes_as_regressions() {
        let mut previous = Reports::new();
        previous.insert(
            "table_streaming".to_owned(),
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "table_count": 8,
                "duration_seconds": 60,
                "end_to_end_with_shutdown_events_per_second": 1_000.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 100.0,
                "total_ms": 1_000.0
            }),
        );

        let mut current = Reports::new();
        current.insert(
            "table_streaming".to_owned(),
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "table_count": 8,
                "duration_seconds": 60,
                "end_to_end_with_shutdown_events_per_second": 800.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 80.0,
                "total_ms": 1_250.0
            }),
        );

        let comparison = compare_reports(&previous, &current, None, None);

        assert!(
            comparison
                .markdown
                .contains("| Events/s | 1000 | 800 | -200 (-20.00%) | 🚨 regression |")
        );
        assert!(
            comparison
                .markdown
                .contains("| Total ms | 1000 | 1250 | +250 (+25.00%) | 🚨 regression |")
        );
    }

    #[test]
    fn compare_reports_skips_regression_gate_when_config_differs() {
        let mut previous = Reports::new();
        previous.insert(
            "table_streaming".to_owned(),
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "memory_budget_ratio": 0.2,
                "end_to_end_with_shutdown_events_per_second": 1_000.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 100.0,
                "total_ms": 1_000.0
            }),
        );

        let mut current = Reports::new();
        current.insert(
            "table_streaming".to_owned(),
            json!({
                "benchmark": "table_streaming",
                "destination": "null",
                "memory_budget_ratio": 0.3,
                "end_to_end_with_shutdown_events_per_second": 100.0,
                "end_to_end_with_shutdown_estimated_mib_per_second": 10.0,
                "total_ms": 10_000.0
            }),
        );

        let comparison = compare_reports(&previous, &current, None, None);

        assert!(comparison.failures.is_empty());
        assert!(
            comparison
                .markdown
                .contains("Regression gate: skipped because benchmark configuration differs.")
        );
    }

    #[test]
    fn compare_reports_links_previous_run_and_commit_diff() {
        let mut previous = Reports::new();
        previous.insert(
            "table_copy".to_owned(),
            json!({
                "benchmark": "table_copy",
                "destination": "null",
                "table_count": 8,
                "rows_per_second": 1_000.0
            }),
        );

        let mut current = Reports::new();
        current.insert(
            "table_copy".to_owned(),
            json!({
                "benchmark": "table_copy",
                "destination": "null",
                "table_count": 8,
                "rows_per_second": 1_100.0
            }),
        );

        let previous_run = WorkflowRun {
            id: 1,
            run_number: 42,
            html_url: "https://github.com/supabase/etl/actions/runs/42".to_owned(),
            conclusion: Some("success".to_owned()),
            head_sha: Some("abc123456789".to_owned()),
        };
        let code_compare = CodeCompare {
            previous_sha: "abc123456789".to_owned(),
            current_sha: "def567890123".to_owned(),
            url: "https://github.com/supabase/etl/compare/abc123456789...def567890123".to_owned(),
        };

        let comparison =
            compare_reports(&previous, &current, Some(&previous_run), Some(&code_compare));

        assert!(
            comparison.markdown.contains(
                "Compared with previous successful run [#42](https://github.com/supabase/etl/actions/runs/42) at `abc1234`."
            )
        );
        assert!(comparison.markdown.contains(
            "Code diff: [`abc1234...def5678`](https://github.com/supabase/etl/compare/abc123456789...def567890123)."
        ));
    }
}
