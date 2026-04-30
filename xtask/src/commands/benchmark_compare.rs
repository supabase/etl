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
    Metric { key: "copied_rows", label: "Rows copied" },
    Metric { key: "estimated_copied_mib", label: "Estimated MiB" },
    Metric { key: "rows_per_second", label: "Rows/s" },
    Metric { key: "estimated_mib_per_second", label: "Estimated MiB/s" },
    Metric { key: "copy_wait_ms", label: "Copy wait ms" },
    Metric { key: "total_ms", label: "Total ms" },
];
const TABLE_STREAMING_METRICS: &[Metric] = &[
    Metric { key: "produced_events", label: "Produced events" },
    Metric { key: "observed_cdc_events", label: "Observed CDC events" },
    Metric { key: "producer_events_per_second", label: "Producer events/s" },
    Metric { key: "end_to_end_events_per_second", label: "Dispatch events/s" },
    Metric {
        key: "end_to_end_with_shutdown_events_per_second",
        label: "Destination-drained events/s",
    },
    Metric { key: "end_to_end_estimated_mib_per_second", label: "Dispatch MiB/s" },
    Metric {
        key: "end_to_end_with_shutdown_estimated_mib_per_second",
        label: "Destination-drained MiB/s",
    },
    Metric { key: "drain_events_per_second", label: "Catch-up drain events/s" },
    Metric { key: "total_ms", label: "Total ms" },
];
const CONFIG_KEYS: &[&str] = &[
    "destination",
    "table_count",
    "expected_row_count",
    "requested_event_count",
    "duration_seconds",
    "insert_batch_size",
    "producer_concurrency",
    "batch_max_fill_ms",
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
}

struct GitHubContext {
    token: String,
    repo: String,
    current_run_id: u64,
    ref_name: String,
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
        let markdown = match self.previous_source().await? {
            Some(previous) => {
                render_comparison(&previous.reports, &current_reports, previous.run.as_ref())
            }
            None => render_missing_previous(&self),
        };

        if let Some(parent) = output.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(&output, format!("{markdown}\n"))
            .with_context(|| format!("failed to write {}", output.display()))?;
        println!("{markdown}");
        Ok(())
    }

    async fn previous_source(&self) -> Result<Option<PreviousSource>> {
        if let Some(previous_dir) = &self.previous_dir {
            let reports = load_reports(previous_dir).with_context(|| {
                format!("failed to load reports from {}", previous_dir.display())
            })?;
            return Ok(Some(PreviousSource { reports, run: None }));
        }

        let Some(context) = self.github_context() else {
            eprintln!(
                "GitHub Actions environment is incomplete; pass --previous-dir for local \
                 comparisons."
            );
            return Ok(None);
        };

        let client = GitHubClient::new(context.token.clone(), context.repo.clone())?;
        let Some((reports, run)) = self.load_github_previous_reports(&client, &context).await?
        else {
            return Ok(None);
        };

        Ok(Some(PreviousSource { reports, run: Some(run) }))
    }

    fn github_context(&self) -> Option<GitHubContext> {
        let token = env::var("GITHUB_TOKEN").ok()?;
        let repo = env::var("GITHUB_REPOSITORY").ok()?;
        let current_run_id = env::var("GITHUB_RUN_ID").ok()?.parse().ok()?;
        let ref_name = self.ref_name.clone().or_else(|| env::var("GITHUB_REF_NAME").ok())?;
        Some(GitHubContext { token, repo, current_run_id, ref_name })
    }

    async fn load_github_previous_reports(
        &self,
        client: &GitHubClient,
        context: &GitHubContext,
    ) -> Result<Option<(Reports, WorkflowRun)>> {
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
            return Ok(Some((reports, run)));
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
        "## Benchmark Comparison\n\nNo previous benchmark artifact was available. In GitHub \
         Actions, this compares against the most recent successful `{}` artifact from `{}` on the \
         same ref. Locally, pass `--previous-dir`.",
        args.artifact_name, args.workflow
    )
}

fn render_comparison(
    previous_reports: &Reports,
    current_reports: &Reports,
    previous_run: Option<&WorkflowRun>,
) -> String {
    let mut lines = vec!["## Benchmark Comparison".to_owned(), String::new()];
    if let Some(previous_run) = previous_run {
        lines.push(format!(
            "Compared with previous successful run [#{}]({}).",
            previous_run.run_number, previous_run.html_url
        ));
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
        return lines.join("\n");
    }

    for benchmark in common_benchmarks {
        let previous = previous_reports.get(&benchmark).expect("benchmark key exists");
        let current = current_reports.get(&benchmark).expect("benchmark key exists");
        lines.push(format!("### {benchmark}"));

        let changes = config_changes(previous, current);
        if !changes.is_empty() {
            lines.push(String::new());
            lines.push(format!("> Benchmark configuration differs: {}.", changes.join(", ")));
        }

        lines.push(String::new());
        lines.push("| Metric | Previous | Current | Change |".to_owned());
        lines.push("| --- | ---: | ---: | ---: |".to_owned());
        for metric in metrics_for(&benchmark) {
            let Some(previous_value) = previous.get(metric.key) else {
                continue;
            };
            let Some(current_value) = current.get(metric.key) else {
                continue;
            };

            lines.push(format!(
                "| {} | {} | {} | {} |",
                metric.label,
                format_value(previous_value),
                format_value(current_value),
                format_delta(previous_value, current_value)
            ));
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
        lines.push(format!("Previous-only reports: {}.", previous_only.join(", ")));
    }
    if !current_only.is_empty() {
        lines.push(format!("Current-only reports: {}.", current_only.join(", ")));
    }

    lines.join("\n")
}

fn metrics_for(benchmark: &str) -> &'static [Metric] {
    match benchmark {
        "table_copy" => TABLE_COPY_METRICS,
        "table_streaming" => TABLE_STREAMING_METRICS,
        _ => &[],
    }
}

fn config_changes(previous: &Value, current: &Value) -> Vec<String> {
    CONFIG_KEYS
        .iter()
        .filter_map(|key| {
            let previous = previous.get(*key)?;
            let current = current.get(*key)?;
            (previous != current)
                .then(|| format!("`{key}` {} -> {}", format_value(previous), format_value(current)))
        })
        .collect()
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
