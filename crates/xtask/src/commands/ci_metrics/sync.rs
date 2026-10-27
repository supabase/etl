//! The level-triggered CI metrics sync.
//!
//! Each day is processed in two phases: every GitHub request first, then one
//! write transaction. A transaction is never held open across network calls,
//! and a crash leaves each day either fully old or fully new.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Days, NaiveDate, TimeDelta, Utc};
use reqwest::StatusCode;
use sqlx::{Connection, PgConnection};
use tokio::{io::AsyncReadExt, process::Command};

use crate::{
    commands::ci_metrics::{
        manifest::{
            self, BENCHMARK_ARTIFACT_PREFIX, MANIFEST_FILE_NAME, MAX_ARCHIVE_BYTES,
            MAX_ENTRY_BYTES, Rejection,
        },
        model::{
            ArtifactPage, CodeScanningAlert, CodeScanningAnalysis, Job, JobPage, Run, RunPage,
            WorkflowPage,
        },
        plan::{self, DayTask},
        store::{self, ManifestRow, RejectionRow},
    },
    github::{GitHubClient, GitHubResponse},
};

/// GitHub page size for every listing.
const PER_PAGE: usize = 100;
/// GitHub returns at most this many results for one run listing query.
const LISTING_CAP: u64 = 1000;
/// Stop before GitHub's own limit leaves less than this many requests.
const RATE_LIMIT_RESERVE: u64 = 50;
/// Requests held back from day syncing so a long backfill never starves the
/// code-scanning refresh.
const CODE_SCANNING_RESERVE: u64 = 20;
/// Attempts per request for transient failures.
const MAX_TRIES: u32 = 3;
/// Longest `retry-after` the sync waits for instead of stopping.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(60);
/// Workflows whose runs upload manifests.
const MANIFEST_WORKFLOWS: &[&str] = &[".github/workflows/benchmark.yml"];
/// Code-scanning alert states listed separately, which does not rely on the
/// unfiltered listing's default. The `fixed` filter also returns dismissed
/// alerts; upserting by alert number makes the overlap harmless.
const ALERT_STATES: &[&str] = &["open", "fixed", "dismissed"];

/// Why a unit of work stopped.
enum SyncError {
    /// The request budget or GitHub's rate limit is spent; resume next sync.
    Exhausted,
    /// The day failed and will back off; other days continue.
    Day { kind: &'static str, source: anyhow::Error },
    /// The sync cannot continue: auth, database or schema errors.
    Fatal(anyhow::Error),
}

impl From<sqlx::Error> for SyncError {
    fn from(error: sqlx::Error) -> Self {
        Self::Fatal(error.into())
    }
}

impl From<anyhow::Error> for SyncError {
    fn from(error: anyhow::Error) -> Self {
        Self::Fatal(error)
    }
}

/// Totals reported at the end of a sync.
#[derive(Default)]
struct Totals {
    days: u64,
    failed_days: u64,
    runs: u64,
    detailed_runs: u64,
    jobs: u64,
    manifests: u64,
    rejections: u64,
    deleted_runs: u64,
    stale_runs: u64,
    alerts: u64,
    new_analyses: u64,
}

/// Inputs of one sync.
pub(super) struct SyncConfig {
    pub(super) history_since: Option<NaiveDate>,
    pub(super) default_history_since: NaiveDate,
    pub(super) max_requests: u64,
}

/// Everything fetched for one day before it is written.
#[derive(Default)]
struct DayBatch {
    runs: Vec<Run>,
    attempts: Vec<Run>,
    details: Vec<RunDetails>,
    listing_complete: bool,
}

/// Jobs and manifests fetched for one run.
struct RunDetails {
    run_id: i64,
    run_attempt: i32,
    jobs: Vec<Job>,
    manifests: Vec<ManifestRow>,
    rejections: Vec<RejectionRow>,
    artifact_status: Option<&'static str>,
}

/// State of one sync run.
struct Syncer<'a> {
    client: &'a GitHubClient,
    /// Requests the current phase may reach, counted from the sync's start.
    request_limit: u64,
    /// Set once GitHub's remaining rate limit drops below the reserve.
    low_rate_limit: bool,
    /// Wall-clock time the sync started, used for every decision it makes.
    now: DateTime<Utc>,
    totals: Totals,
}

/// Runs one sync. Returns `Ok` when another sync holds the lock.
pub(super) async fn run(
    conn: &mut PgConnection,
    client: &GitHubClient,
    config: SyncConfig,
) -> Result<()> {
    if !store::try_lock(conn).await? {
        eprintln!("ci-metrics: another sync holds the lock; nothing to do");
        return Ok(());
    }
    let result = run_locked(conn, client, config).await;
    store::unlock(conn).await?;
    result
}

/// Runs the sync phases while holding the lock: workflows, planned days, then
/// code scanning.
async fn run_locked(
    conn: &mut PgConnection,
    client: &GitHubClient,
    config: SyncConfig,
) -> Result<()> {
    store::check_schema(conn).await?;
    let settings =
        store::load_settings(conn, config.history_since, config.default_history_since).await?;
    let mut syncer = Syncer {
        client,
        request_limit: config.max_requests.saturating_sub(CODE_SCANNING_RESERVE),
        low_rate_limit: false,
        now: Utc::now(),
        totals: Totals::default(),
    };

    let mut exhausted = false;
    match syncer.sync_workflows(conn).await {
        Ok(()) => {}
        Err(SyncError::Exhausted) => exhausted = true,
        Err(SyncError::Fatal(error)) => return Err(error),
        Err(SyncError::Day { source, .. }) => {
            return Err(source.context("Failed to sync workflows"));
        }
    }

    if !exhausted {
        let states = store::load_day_states(conn).await?;
        let tasks = plan::plan_days(syncer.now, settings.history_since, &states);
        for task in tasks {
            match syncer.sync_day(conn, task).await {
                Ok(()) => syncer.totals.days += 1,
                Err(SyncError::Exhausted) => {
                    exhausted = true;
                    break;
                }
                Err(SyncError::Fatal(error)) => return Err(error),
                Err(SyncError::Day { kind, source }) => {
                    syncer.totals.failed_days += 1;
                    eprintln!("ci-metrics: day {} failed ({kind}): {source:#}", task.day);
                    let failures = store::record_day_failure(conn, task.day, kind).await?;
                    store::set_retry_after(conn, task.day, plan::retry_after(syncer.now, failures))
                        .await?;
                }
            }
        }
    }

    // Code scanning runs even when days spent their budget: it gets the
    // reserve, unless GitHub's own rate limit is low.
    syncer.request_limit = config.max_requests;
    if !syncer.low_rate_limit {
        match syncer
            .sync_code_scanning(conn, settings.history_since, settings.analyses_backfilled)
            .await
        {
            Ok(()) | Err(SyncError::Exhausted) => {}
            Err(SyncError::Fatal(error)) => return Err(error),
            Err(SyncError::Day { source, .. }) => {
                eprintln!("ci-metrics: code scanning sync failed: {source:#}");
            }
        }
    }

    store::record_success(conn, Utc::now()).await?;
    let totals = &syncer.totals;
    eprintln!(
        "ci-metrics: synced {} days ({} failed), {} runs ({} detailed), {} jobs, {} manifests, {} \
         rejections, {} deleted, {} stale, {} alerts, {} new analyses using {} requests{}",
        totals.days,
        totals.failed_days,
        totals.runs,
        totals.detailed_runs,
        totals.jobs,
        totals.manifests,
        totals.rejections,
        totals.deleted_runs,
        totals.stale_runs,
        totals.alerts,
        totals.new_analyses,
        client.requests_sent(),
        if exhausted { "; budget spent, resuming next sync" } else { "" },
    );
    Ok(())
}

impl Syncer<'_> {
    /// Sends one budgeted request, retrying transient failures.
    ///
    /// Returns successful responses plus `404` and `410`, which callers treat
    /// as missing or expired data.
    async fn request(
        &mut self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<GitHubResponse, SyncError> {
        self.send(Target::Path(path), query).await
    }

    /// Downloads an artifact archive, bounded by [`MAX_ARCHIVE_BYTES`].
    async fn download(&mut self, url: &str) -> Result<GitHubResponse, SyncError> {
        self.send(Target::Url(url), &[]).await
    }

    /// Sends a request to `target`, enforcing the budget and retrying
    /// transient failures and short rate-limit waits.
    async fn send(
        &mut self,
        target: Target<'_>,
        query: &[(&str, String)],
    ) -> Result<GitHubResponse, SyncError> {
        for attempt in 1..=MAX_TRIES {
            if self.low_rate_limit || self.client.requests_sent() >= self.request_limit {
                return Err(SyncError::Exhausted);
            }
            let result = match target {
                Target::Path(path) => self.client.get(path, query).await,
                Target::Url(url) => {
                    self.client
                        .get_url(url, usize::try_from(MAX_ARCHIVE_BYTES).unwrap_or(usize::MAX))
                        .await
                }
            };
            let response = match result {
                Ok(response) => response,
                Err(error) if attempt < MAX_TRIES => {
                    eprintln!("ci-metrics: transient request failure, retrying: {error:#}");
                    tokio::time::sleep(backoff(attempt)).await;
                    continue;
                }
                Err(error) => return Err(SyncError::Day { kind: "network", source: error }),
            };

            if response
                .rate_limit_remaining()
                .is_some_and(|remaining| remaining < RATE_LIMIT_RESERVE)
            {
                self.low_rate_limit = true;
            }

            if response.is_rate_limited() {
                match response.retry_after() {
                    Some(wait) if wait <= MAX_RETRY_WAIT && attempt < MAX_TRIES => {
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    _ => return Err(SyncError::Exhausted),
                }
            }

            let status = response.status();
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                return Err(SyncError::Fatal(anyhow!(
                    "GitHub denied access with HTTP status {status}"
                )));
            }
            if status.is_server_error() {
                if attempt < MAX_TRIES {
                    tokio::time::sleep(backoff(attempt)).await;
                    continue;
                }
                return Err(SyncError::Day {
                    kind: "github_server_error",
                    source: anyhow!("GitHub returned HTTP status {status}"),
                });
            }
            if status.is_success() || status == StatusCode::NOT_FOUND || status == StatusCode::GONE
            {
                return Ok(response);
            }
            return Err(SyncError::Day {
                kind: "github_status",
                source: anyhow!("GitHub returned HTTP status {status}"),
            });
        }
        Err(SyncError::Exhausted)
    }

    /// Fetches a page and fails the day unless it succeeded.
    async fn fetch_json<T: serde::de::DeserializeOwned>(
        &mut self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<(T, bool), SyncError> {
        let response = self.request(path, query).await?;
        if !response.status().is_success() {
            return Err(SyncError::Day {
                kind: "github_status",
                source: anyhow!("GitHub returned HTTP status {}", response.status()),
            });
        }
        let has_next = response.has_next_page();
        let value =
            response.json().map_err(|source| SyncError::Day { kind: "github_payload", source })?;
        Ok((value, has_next))
    }

    /// Upserts every workflow definition so runs can reference them.
    async fn sync_workflows(&mut self, conn: &mut PgConnection) -> Result<(), SyncError> {
        let mut page = 1;
        loop {
            let (listing, has_next): (WorkflowPage, bool) =
                self.fetch_json("actions/workflows", &page_query(page)).await?;
            for workflow in &listing.workflows {
                store::upsert_workflow(conn, workflow).await?;
            }
            if !has_next {
                return Ok(());
            }
            page += 1;
        }
    }

    /// Fetches one day's runs and their changed details, then writes them in
    /// one transaction with the day's sync state.
    async fn sync_day(&mut self, conn: &mut PgConnection, task: DayTask) -> Result<(), SyncError> {
        let (start, end) = day_bounds(task.day)?;

        // Network phase.
        let mut batch = self.list_day(start, end).await?;
        let stored = store::load_stored_runs(conn, start, end).await?;
        let known_artifacts = store::load_known_artifacts(conn, start, end).await?;
        for run in &batch.runs {
            let stored_run = stored.get(&run.id);
            if !plan::needs_details(
                stored_run.map(|(run, _)| run),
                run.run_attempt,
                run.updated_at,
                task.refetch_details,
            ) {
                continue;
            }
            let stored_attempts =
                stored_run.map(|(_, attempts)| attempts.as_slice()).unwrap_or_default();
            for attempt in plan::missing_attempts(stored_attempts, run.run_attempt) {
                let response = self
                    .request(&format!("actions/runs/{}/attempts/{attempt}", run.id), &[])
                    .await?;
                if response.status().is_success() {
                    let attempt_run = response
                        .json()
                        .map_err(|source| SyncError::Day { kind: "github_payload", source })?;
                    batch.attempts.push(attempt_run);
                }
            }
            let jobs = self.fetch_jobs(run.id).await?;
            let (manifests, rejections, artifact_status) = if accepts_manifests(run) {
                let known = known_artifacts.get(&run.id).cloned().unwrap_or_default();
                self.fetch_manifests(run, &jobs, &known).await?
            } else {
                (Vec::new(), Vec::new(), None)
            };
            batch.details.push(RunDetails {
                run_id: run.id,
                run_attempt: run.run_attempt,
                jobs,
                manifests,
                rejections,
                artifact_status,
            });
        }

        // Write phase.
        let mut tx = conn.begin().await?;
        for run in batch.attempts.iter().chain(&batch.runs) {
            store::upsert_run(&mut tx, run).await?;
        }
        for details in &batch.details {
            for job in &details.jobs {
                // Jobs of an attempt GitHub no longer returns cannot be stored.
                if !store::run_attempt_exists(&mut tx, job.run_id, job.run_attempt).await? {
                    continue;
                }
                let failed_step = job
                    .steps
                    .iter()
                    .find(|step| step.conclusion.as_deref() == Some("failure"))
                    .map(|step| (step.name.as_str(), step.number));
                store::upsert_job(&mut tx, job, detail_status(self.now, job), failed_step).await?;
                self.totals.jobs += 1;
            }
            for manifest in &details.manifests {
                store::insert_manifest(&mut tx, manifest).await?;
            }
            for rejection in &details.rejections {
                store::insert_rejection(&mut tx, rejection, self.now).await?;
            }
            if let Some(status) = details.artifact_status {
                store::set_artifact_status(&mut tx, details.run_id, status).await?;
            }
            store::mark_details_fetched(&mut tx, details.run_id, details.run_attempt, self.now)
                .await?;
            self.totals.manifests += details.manifests.len() as u64;
            self.totals.rejections += details.rejections.len() as u64;
        }

        let complete = batch.listing_complete;
        let mut final_at = None;
        if complete {
            let ids: Vec<i64> = batch.runs.iter().map(|run| run.id).collect();
            self.totals.deleted_runs +=
                store::mark_deleted(&mut tx, start, end, &ids, self.now).await?;
            if plan::listing_finalizes(task.day, self.now) {
                self.totals.stale_runs += store::mark_stale(&mut tx, start, end).await?;
                final_at = Some(self.now);
            }
        }
        let run_count = i32::try_from(batch.runs.len()).unwrap_or(i32::MAX);
        let refetched_at = (complete && task.refetch_details).then_some(self.now);
        store::record_day(
            &mut tx,
            task.day,
            complete.then_some(self.now),
            run_count,
            final_at,
            refetched_at,
        )
        .await?;
        tx.commit().await?;

        self.totals.runs += batch.runs.len() as u64;
        self.totals.detailed_runs += batch.details.len() as u64;
        Ok(())
    }

    /// Lists every run created in `[start, end)`, splitting the window when
    /// GitHub's result cap would truncate it.
    async fn list_day(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<DayBatch, SyncError> {
        let mut runs: BTreeMap<i64, Run> = BTreeMap::new();
        let mut complete = true;
        let mut windows = vec![(start, end)];
        while let Some((window_start, window_end)) = windows.pop() {
            let created = format!(
                "{}..{}",
                window_start.format("%Y-%m-%dT%H:%M:%SZ"),
                (window_end - TimeDelta::seconds(1)).format("%Y-%m-%dT%H:%M:%SZ")
            );
            let mut page = 1;
            let mut seen = HashSet::new();
            let mut total = None;
            loop {
                let mut query = page_query(page);
                query.push(("created", created.clone()));
                let (listing, has_next): (RunPage, bool) =
                    self.fetch_json("actions/runs", &query).await?;
                if listing.total_count > LISTING_CAP {
                    if window_end - window_start <= TimeDelta::hours(1) {
                        return Err(SyncError::Day {
                            kind: "listing_cap",
                            source: anyhow!(
                                "More than {LISTING_CAP} runs were created within one hour"
                            ),
                        });
                    }
                    let middle = window_start + (window_end - window_start) / 2;
                    windows.push((window_start, middle));
                    windows.push((middle, window_end));
                    break;
                }
                total = Some(listing.total_count);
                seen.extend(listing.workflow_runs.iter().map(|run| run.id));
                for run in listing.workflow_runs {
                    // Pages can shift while listing; keep the newest snapshot.
                    match runs.get(&run.id) {
                        Some(existing) if existing.updated_at > run.updated_at => {}
                        _ => {
                            runs.insert(run.id, run);
                        }
                    }
                }
                if !has_next {
                    break;
                }
                page += 1;
            }
            if let Some(total) = total
                && (seen.len() as u64) < total
            {
                complete = false;
            }
        }
        // A shifted page can hide a run; only a listing covering every reported
        // run counts as complete.
        Ok(DayBatch {
            runs: runs.into_values().collect(),
            listing_complete: complete,
            ..DayBatch::default()
        })
    }

    /// Fetches every job of every attempt of a run.
    async fn fetch_jobs(&mut self, run_id: i64) -> Result<Vec<Job>, SyncError> {
        let mut jobs = Vec::new();
        let mut page = 1;
        loop {
            let mut query = page_query(page);
            query.push(("filter", "all".to_owned()));
            let path = format!("actions/runs/{run_id}/jobs");
            let response = self.request(&path, &query).await?;
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(jobs);
            }
            let has_next = response.has_next_page();
            let listing: JobPage = response
                .json()
                .map_err(|source| SyncError::Day { kind: "github_payload", source })?;
            let done = !has_next
                || listing.jobs.is_empty()
                || jobs.len() as u64 + listing.jobs.len() as u64 >= listing.total_count;
            jobs.extend(listing.jobs);
            if done {
                return Ok(jobs);
            }
            page += 1;
        }
    }

    /// Fetches and validates manifest artifacts of a run. Artifacts already
    /// stored or rejected are skipped, because artifacts are immutable.
    async fn fetch_manifests(
        &mut self,
        run: &Run,
        jobs: &[Job],
        known: &HashMap<i64, bool>,
    ) -> Result<(Vec<ManifestRow>, Vec<RejectionRow>, Option<&'static str>), SyncError> {
        let mut manifests = Vec::new();
        let mut rejections = Vec::new();
        let mut expired = false;
        let mut page = 1;
        loop {
            let path = format!("actions/runs/{}/artifacts", run.id);
            let (listing, has_next): (ArtifactPage, bool) =
                self.fetch_json(&path, &page_query(page)).await?;
            let done = !has_next
                || listing.artifacts.is_empty()
                || u64::try_from(page * PER_PAGE).unwrap_or(u64::MAX) >= listing.total_count;
            for artifact in listing.artifacts {
                if !artifact.name.starts_with(BENCHMARK_ARTIFACT_PREFIX)
                    || known.contains_key(&artifact.id)
                {
                    continue;
                }
                if artifact.expired {
                    expired = true;
                    continue;
                }
                let mut reject = |reason: Rejection, run_attempt: Option<i32>| {
                    rejections.push(RejectionRow {
                        artifact_id: artifact.id,
                        run_id: run.id,
                        run_attempt,
                        artifact_name: artifact.name.clone(),
                        reason: reason.as_str(),
                    });
                };
                if artifact.size_in_bytes > MAX_ARCHIVE_BYTES {
                    reject(Rejection::TooLarge, None);
                    continue;
                }
                let response = self.download(&artifact.archive_download_url).await?;
                if response.status() == StatusCode::GONE
                    || response.status() == StatusCode::NOT_FOUND
                {
                    expired = true;
                    continue;
                }
                if response.truncated() {
                    reject(Rejection::TooLarge, None);
                    continue;
                }
                let archive = response.into_body();
                match read_benchmark_archive(&archive, run, jobs).await {
                    Ok(Ok(row)) => manifests.push(row.with_artifact(artifact.id)),
                    Ok(Err((reason, run_attempt))) => reject(reason, run_attempt),
                    Err(source) => return Err(SyncError::Day { kind: "artifact_io", source }),
                }
            }
            if done {
                break;
            }
            page += 1;
        }

        let has_stored = known.values().any(|stored| *stored) || !manifests.is_empty();
        let status = if has_stored {
            Some("available")
        } else if expired {
            Some("expired")
        } else if known.is_empty() && rejections.is_empty() {
            Some("none")
        } else {
            None
        };
        Ok((manifests, rejections, status))
    }

    /// Refreshes code-scanning alerts and pages analyses newest first until
    /// stored ones or the history start are reached.
    async fn sync_code_scanning(
        &mut self,
        conn: &mut PgConnection,
        history_since: NaiveDate,
        analyses_backfilled: bool,
    ) -> Result<(), SyncError> {
        let mut alert_numbers = HashSet::new();
        for state in ALERT_STATES {
            let mut page = 1;
            loop {
                let mut query = page_query(page);
                query.push(("state", (*state).to_owned()));
                let (alerts, has_next): (Vec<CodeScanningAlert>, bool) =
                    self.fetch_json("code-scanning/alerts", &query).await?;
                for alert in &alerts {
                    store::upsert_alert(conn, alert).await?;
                    alert_numbers.insert(alert.number);
                }
                if !has_next {
                    break;
                }
                page += 1;
            }
        }
        self.totals.alerts = alert_numbers.len() as u64;

        let history_start = history_since.and_hms_opt(0, 0, 0).map(|start| start.and_utc());
        let mut page = 1;
        loop {
            let (analyses, has_next): (Vec<CodeScanningAnalysis>, bool) =
                self.fetch_json("code-scanning/analyses", &page_query(page)).await?;
            let mut new = 0;
            for analysis in &analyses {
                if store::insert_analysis(conn, analysis).await? {
                    new += 1;
                }
            }
            self.totals.new_analyses += new;
            let reached_history = analyses
                .iter()
                .map(|analysis| analysis.created_at)
                .min()
                .zip(history_start)
                .is_some_and(|(oldest, start)| oldest < start);
            if !has_next || reached_history {
                if !analyses_backfilled {
                    store::set_analyses_backfilled(conn).await?;
                }
                return Ok(());
            }
            // Pages are newest first: once a page brings nothing new and the
            // history is complete, older pages are already stored.
            if new == 0 && analyses_backfilled {
                return Ok(());
            }
            page += 1;
        }
    }
}

/// Destination of a budgeted GitHub request.
enum Target<'a> {
    Path(&'a str),
    Url(&'a str),
}

impl ManifestRow {
    /// Sets the artifact id once the archive has been validated.
    fn with_artifact(mut self, artifact_id: i64) -> Self {
        self.artifact_id = artifact_id;
        self
    }
}

/// Reads and validates a benchmark results archive.
///
/// The outer `Result` carries local I/O failures, which retry later. The
/// inner one carries validation outcomes, which are recorded; only
/// `unmatched_job` is retried on a later fetch.
async fn read_benchmark_archive(
    archive: &[u8],
    run: &Run,
    jobs: &[Job],
) -> Result<Result<ManifestRow, (Rejection, Option<i32>)>> {
    let mut file = tempfile::NamedTempFile::new().context("Failed to create artifact file")?;
    std::io::Write::write_all(&mut file, archive).context("Failed to write artifact file")?;
    let path = file.path();

    let listing = run_unzip(&["-l"], path, None, 64 * 1024).await?;
    let Some(listing) = listing.and_then(|bytes| String::from_utf8(bytes).ok()) else {
        return Ok(Err((Rejection::InvalidArchive, None)));
    };
    let entries = match manifest::parse_archive_listing(&listing) {
        Ok(entries) => entries,
        Err(reason) => return Ok(Err((reason, None))),
    };

    let Some(manifest_bytes) = read_entry(path, MANIFEST_FILE_NAME).await? else {
        return Ok(Err((Rejection::TooLarge, None)));
    };
    let parsed = match manifest::parse_benchmark_manifest(
        &manifest_bytes,
        &run.repository.full_name,
        run.id,
    ) {
        Ok(parsed) => parsed,
        Err(reason) => return Ok(Err((reason, None))),
    };
    let run_attempt = parsed.run_attempt();

    let mut reports = BTreeMap::new();
    for name in parsed.reports() {
        if !entries.iter().any(|entry| &entry.name == name) {
            return Ok(Err((Rejection::Invalid, Some(run_attempt))));
        }
        let Some(bytes) = read_entry(path, name).await? else {
            return Ok(Err((Rejection::TooLarge, Some(run_attempt))));
        };
        reports.insert(name.clone(), bytes);
    }
    let payload = match manifest::benchmark_payload(&parsed, &reports) {
        Ok(payload) => payload,
        Err(reason) => return Ok(Err((reason, Some(run_attempt)))),
    };

    let Some(job_id) = resolve_job(jobs, run_attempt, parsed.job_id(), &parsed.job_name()) else {
        return Ok(Err((Rejection::UnmatchedJob, Some(run_attempt))));
    };

    Ok(Ok(ManifestRow {
        artifact_id: 0,
        run_id: run.id,
        run_attempt,
        job_id,
        kind: "benchmark",
        schema_version: parsed.schema_version(),
        commit_sha: parsed.commit_sha().to_owned(),
        payload,
    }))
}

/// Resolves the job that produced a manifest: by id when recorded, else by
/// display name when exactly one job of the attempt matches.
fn resolve_job(jobs: &[Job], run_attempt: i32, job_id: Option<i64>, job_name: &str) -> Option<i64> {
    let attempt_jobs = || jobs.iter().filter(move |job| job.run_attempt == run_attempt);
    if let Some(job_id) = job_id {
        return attempt_jobs().find(|job| job.id == job_id).map(|job| job.id);
    }
    let mut matches = attempt_jobs().filter(|job| job.name == job_name);
    let job = matches.next()?;
    matches.next().is_none().then_some(job.id)
}

/// Reads one archive entry, returning `None` when it exceeds the size limit.
async fn read_entry(path: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    run_unzip(&["-p"], path, Some(name), MAX_ENTRY_BYTES).await
}

/// Runs `unzip` and reads at most `max_bytes` of its output. Returns `None`
/// when the output is longer or `unzip` fails.
async fn run_unzip(
    flags: &[&str],
    archive: &Path,
    entry: Option<&str>,
    max_bytes: u64,
) -> Result<Option<Vec<u8>>> {
    let mut command = Command::new("unzip");
    command.args(flags).arg(archive);
    if let Some(entry) = entry {
        command.arg(entry);
    }
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("Failed to run unzip")?;
    let stdout = child.stdout.take().context("Failed to capture unzip output")?;
    let mut output = Vec::new();
    stdout
        .take(max_bytes + 1)
        .read_to_end(&mut output)
        .await
        .context("Failed to read unzip output")?;
    if output.len() as u64 > max_bytes {
        return Ok(None);
    }
    let status = child.wait().await.context("Failed to wait for unzip")?;
    Ok(status.success().then_some(output))
}

/// Returns whether a run's manifests may be ingested: manifest workflows from
/// this repository's trusted events only, never forks.
fn accepts_manifests(run: &Run) -> bool {
    let trusted_event = match run.event.as_str() {
        "schedule" | "workflow_dispatch" => true,
        "push" => run.head_branch.as_deref() == Some("main"),
        "pull_request" => true,
        _ => false,
    };
    run.status == "completed"
        && !run.is_fork()
        && trusted_event
        && MANIFEST_WORKFLOWS.contains(&run.workflow_path())
}

/// Classifies how much step detail GitHub still returns for a job.
fn detail_status(now: DateTime<Utc>, job: &Job) -> &'static str {
    if !job.steps.is_empty() {
        "available"
    } else if plan::artifacts_expired(now, job.created_at) {
        "expired"
    } else {
        "none"
    }
}

/// Returns the paging query for `page`.
fn page_query(page: usize) -> Vec<(&'static str, String)> {
    vec![("per_page", PER_PAGE.to_string()), ("page", page.to_string())]
}

/// Returns the UTC bounds `[start, end)` of `day`.
fn day_bounds(day: NaiveDate) -> Result<(DateTime<Utc>, DateTime<Utc>), SyncError> {
    let start = day.and_hms_opt(0, 0, 0).map(|start| start.and_utc());
    let end = day
        .checked_add_days(Days::new(1))
        .and_then(|end| end.and_hms_opt(0, 0, 0))
        .map(|end| end.and_utc());
    start.zip(end).ok_or_else(|| SyncError::Fatal(anyhow!("Day {day} is out of range")))
}

/// Returns the wait before retry `attempt`.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1 << attempt.min(5))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: i64, run_attempt: i32, name: &str) -> Job {
        Job {
            id,
            run_id: 1,
            run_attempt,
            name: name.to_owned(),
            status: "completed".to_owned(),
            conclusion: Some("success".to_owned()),
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            labels: Vec::new(),
            steps: Vec::new(),
        }
    }

    #[test]
    fn job_resolution_prefers_ids_and_requires_unique_names() {
        let jobs = [
            job(10, 1, "Benchmark (snowflake)"),
            job(20, 2, "Benchmark (snowflake)"),
            job(21, 2, "Benchmark (null)"),
            job(30, 3, "Benchmark (snowflake)"),
            job(31, 3, "Benchmark (snowflake)"),
        ];

        assert_eq!(resolve_job(&jobs, 2, Some(20), "ignored"), Some(20));
        // An id from another attempt never matches.
        assert_eq!(resolve_job(&jobs, 2, Some(10), "Benchmark (snowflake)"), None);
        assert_eq!(resolve_job(&jobs, 2, None, "Benchmark (snowflake)"), Some(20));
        assert_eq!(resolve_job(&jobs, 3, None, "Benchmark (snowflake)"), None);
        assert_eq!(resolve_job(&jobs, 2, None, "Benchmark (bigquery)"), None);
    }
}
