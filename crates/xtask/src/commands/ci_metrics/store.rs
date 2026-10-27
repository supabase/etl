//! Postgres access for the CI metrics collector.
//!
//! Every write is an upsert keyed on GitHub ids, so repeating a sync is a
//! no-op. Guards in the SQL keep older snapshots from replacing newer ones.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;
use sqlx::{PgConnection, Row, migrate::Migrator, types::Json};

use crate::commands::ci_metrics::{
    manifest::Rejection,
    model::{CodeScanningAlert, CodeScanningAnalysis, Job, Run, Workflow},
    plan::{DayState, StoredRun},
};

/// Advisory lock key that serializes syncs against one database.
const SYNC_LOCK_KEY: i64 = 0x6369_6d65_7472_6963;

/// Returns the embedded CI metrics migrations.
pub(super) fn migrator() -> Migrator {
    sqlx::migrate!("./migrations/ci_metrics")
}

/// Applies migrations. Runs as the owner role, by hand.
pub(super) async fn migrate(conn: &mut PgConnection) -> Result<()> {
    sqlx::query("set client_min_messages = warning").execute(&mut *conn).await?;
    sqlx::query("create schema if not exists ci").execute(&mut *conn).await?;
    // Keeps sqlx's `_sqlx_migrations` table inside `ci`.
    sqlx::query("set search_path = ci").execute(&mut *conn).await?;
    migrator()
        .run_direct(None, &mut *conn, false)
        .await
        .context("Failed to apply CI metrics migrations")?;
    Ok(())
}

/// Fails unless the newest embedded migration is applied.
pub(super) async fn check_schema(conn: &mut PgConnection) -> Result<()> {
    let expected = migrator().iter().map(|migration| migration.version).max().unwrap_or_default();
    let applied: Option<i64> =
        sqlx::query_scalar("select max(version) from ci._sqlx_migrations where success")
            .fetch_one(&mut *conn)
            .await
            .context(
                "Failed to read the CI metrics schema version; run `cargo x ci-metrics migrate`",
            )?;
    if applied != Some(expected) {
        bail!(
            "CI metrics schema version mismatch: expected {expected}, found {}; run `cargo x \
             ci-metrics migrate`",
            applied.map_or_else(|| "none".to_owned(), |version| version.to_string())
        );
    }
    Ok(())
}

/// Tries to take the session-level sync lock.
pub(super) async fn try_lock(conn: &mut PgConnection) -> Result<bool> {
    Ok(sqlx::query_scalar("select pg_try_advisory_lock($1)")
        .bind(SYNC_LOCK_KEY)
        .fetch_one(&mut *conn)
        .await?)
}

/// Releases the session-level sync lock.
pub(super) async fn unlock(conn: &mut PgConnection) -> Result<()> {
    sqlx::query("select pg_advisory_unlock($1)").bind(SYNC_LOCK_KEY).execute(&mut *conn).await?;
    Ok(())
}

/// Stored collector settings.
pub(super) struct SyncSettings {
    pub(super) history_since: NaiveDate,
    pub(super) analyses_backfilled: bool,
}

/// Reads settings, storing `history_since` when given.
pub(super) async fn load_settings(
    conn: &mut PgConnection,
    history_since: Option<NaiveDate>,
    default_history_since: NaiveDate,
) -> Result<SyncSettings> {
    let row = sqlx::query(
        "insert into ci.sync_state (history_since) values (coalesce($1, $2))
         on conflict (id) do update set history_since = coalesce($1, ci.sync_state.history_since)
         returning history_since, analyses_backfilled",
    )
    .bind(history_since)
    .bind(default_history_since)
    .fetch_one(&mut *conn)
    .await?;
    Ok(SyncSettings {
        history_since: row.try_get("history_since")?,
        analyses_backfilled: row.try_get("analyses_backfilled")?,
    })
}

/// Reads every stored day state.
pub(super) async fn load_day_states(
    conn: &mut PgConnection,
) -> Result<BTreeMap<NaiveDate, DayState>> {
    let rows = sqlx::query(
        "select day, listed_at, final_at, detail_refetched_at, retry_after from ci.sync_day",
    )
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("day")?,
                DayState {
                    listed_at: row.try_get("listed_at")?,
                    final_at: row.try_get("final_at")?,
                    detail_refetched_at: row.try_get("detail_refetched_at")?,
                    retry_after: row.try_get("retry_after")?,
                },
            ))
        })
        .collect()
}

/// Stored runs of one day: the latest attempt's state plus every stored
/// attempt number.
pub(super) async fn load_stored_runs(
    conn: &mut PgConnection,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<HashMap<i64, (StoredRun, Vec<i32>)>> {
    let rows = sqlx::query(
        "select run_id, run_attempt, updated_at, status, detail_fetched_at
         from ci.run
         where created_at >= $1 and created_at < $2
         order by run_id, run_attempt",
    )
    .bind(start)
    .bind(end)
    .fetch_all(&mut *conn)
    .await?;

    let mut runs: HashMap<i64, (StoredRun, Vec<i32>)> = HashMap::new();
    for row in rows {
        let run_id: i64 = row.try_get("run_id")?;
        let stored = StoredRun {
            latest_attempt: row.try_get("run_attempt")?,
            updated_at: row.try_get("updated_at")?,
            status: row.try_get("status")?,
            detail_fetched_at: row.try_get("detail_fetched_at")?,
        };
        let attempt = stored.latest_attempt;
        // Rows arrive in attempt order, so the last one is the latest.
        let entry = runs.entry(run_id).or_insert_with(|| (stored.clone(), Vec::new()));
        entry.0 = stored;
        entry.1.push(attempt);
    }
    Ok(runs)
}

/// Artifact ids already processed for runs created in a day, mapped to
/// whether they produced a stored manifest.
///
/// `unmatched_job` rejections are left out: the producing job may simply not
/// be visible yet, so those artifacts are retried whenever the run's details
/// are fetched again.
pub(super) async fn load_known_artifacts(
    conn: &mut PgConnection,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<HashMap<i64, HashMap<i64, bool>>> {
    let rows = sqlx::query(
        "select m.run_id, m.artifact_id, true as stored
         from ci.manifest m
         where m.run_id in (select run_id from ci.run where created_at >= $1 and created_at < $2)
         union all
         select r.run_id, r.artifact_id, false
         from ci.manifest_rejection r
         where r.run_id in (select run_id from ci.run where created_at >= $1 and created_at < $2)
             and r.reason <> $3",
    )
    .bind(start)
    .bind(end)
    .bind(Rejection::UnmatchedJob.as_str())
    .fetch_all(&mut *conn)
    .await?;

    let mut known: HashMap<i64, HashMap<i64, bool>> = HashMap::new();
    for row in rows {
        known
            .entry(row.try_get("run_id")?)
            .or_default()
            .insert(row.try_get("artifact_id")?, row.try_get("stored")?);
    }
    Ok(known)
}

/// Upserts a workflow definition from the workflow listing.
pub(super) async fn upsert_workflow(conn: &mut PgConnection, workflow: &Workflow) -> Result<()> {
    sqlx::query(
        "insert into ci.workflow (workflow_id, path, name) values ($1, $2, $3)
         on conflict (workflow_id) do update set path = excluded.path, name = excluded.name",
    )
    .bind(workflow.id)
    .bind(&workflow.path)
    .bind(&workflow.name)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Inserts a placeholder for a workflow the listing no longer returns, such
/// as a deleted workflow file. Never overwrites a listed workflow.
async fn ensure_workflow(conn: &mut PgConnection, run: &Run) -> Result<()> {
    sqlx::query(
        "insert into ci.workflow (workflow_id, path, name) values ($1, $2, $2)
         on conflict (workflow_id) do nothing",
    )
    .bind(run.workflow_id)
    .bind(run.workflow_path())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Upserts one run attempt. An older snapshot never replaces a newer one, and
/// a completed attempt never regresses.
pub(super) async fn upsert_run(conn: &mut PgConnection, run: &Run) -> Result<()> {
    ensure_workflow(conn, run).await?;
    sqlx::query(
        "insert into ci.run (
             run_id, run_attempt, workflow_id, event, head_branch, head_sha, display_title,
             from_fork, status, conclusion, created_at, run_started_at, updated_at
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         on conflict (run_id, run_attempt) do update set
             workflow_id = excluded.workflow_id,
             event = excluded.event,
             head_branch = excluded.head_branch,
             head_sha = excluded.head_sha,
             display_title = excluded.display_title,
             from_fork = excluded.from_fork,
             status = excluded.status,
             conclusion = coalesce(excluded.conclusion, ci.run.conclusion),
             run_started_at = coalesce(excluded.run_started_at, ci.run.run_started_at),
             updated_at = excluded.updated_at,
             deleted_at = null
         where excluded.updated_at >= ci.run.updated_at
             and not (ci.run.status = 'completed' and excluded.status <> 'completed')",
    )
    .bind(run.id)
    .bind(run.run_attempt)
    .bind(run.workflow_id)
    .bind(&run.event)
    .bind(&run.head_branch)
    .bind(&run.head_sha)
    .bind(&run.display_title)
    .bind(run.is_fork())
    .bind(&run.status)
    .bind(&run.conclusion)
    .bind(run.created_at)
    .bind(run.run_started_at)
    .bind(run.updated_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Records when a run's details were last fetched.
pub(super) async fn mark_details_fetched(
    conn: &mut PgConnection,
    run_id: i64,
    run_attempt: i32,
    fetched_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query("update ci.run set detail_fetched_at = $3 where run_id = $1 and run_attempt = $2")
        .bind(run_id)
        .bind(run_attempt)
        .bind(fetched_at)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Sets the manifest artifact status on every attempt of a run.
pub(super) async fn set_artifact_status(
    conn: &mut PgConnection,
    run_id: i64,
    status: &str,
) -> Result<()> {
    sqlx::query("update ci.run set artifact_status = $2 where run_id = $1")
        .bind(run_id)
        .bind(status)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Returns whether a run attempt row exists.
pub(super) async fn run_attempt_exists(
    conn: &mut PgConnection,
    run_id: i64,
    run_attempt: i32,
) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "select exists (select from ci.run where run_id = $1 and run_attempt = $2)",
    )
    .bind(run_id)
    .bind(run_attempt)
    .fetch_one(&mut *conn)
    .await?)
}

/// Upserts a job. Jobs have no `updated_at`, so the guard is monotonic: a
/// completed job is never replaced by a non-completed snapshot, set fields
/// never clear, and available step details never become unavailable.
pub(super) async fn upsert_job(
    conn: &mut PgConnection,
    job: &Job,
    detail_status: &str,
    failed_step: Option<(&str, i32)>,
) -> Result<()> {
    sqlx::query(
        "insert into ci.job (
             job_id, run_id, run_attempt, name, runner_label, status, conclusion, created_at,
             started_at, completed_at, failed_step_name, failed_step_number, detail_status
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         on conflict (job_id) do update set
             name = excluded.name,
             runner_label = coalesce(excluded.runner_label, ci.job.runner_label),
             status = excluded.status,
             conclusion = coalesce(excluded.conclusion, ci.job.conclusion),
             started_at = coalesce(excluded.started_at, ci.job.started_at),
             completed_at = coalesce(excluded.completed_at, ci.job.completed_at),
             failed_step_name = coalesce(excluded.failed_step_name, ci.job.failed_step_name),
             failed_step_number = coalesce(excluded.failed_step_number, ci.job.failed_step_number),
             detail_status = case
                 when ci.job.detail_status = 'available' then 'available'
                 else excluded.detail_status
             end
         where not (ci.job.status = 'completed' and excluded.status <> 'completed')",
    )
    .bind(job.id)
    .bind(job.run_id)
    .bind(job.run_attempt)
    .bind(&job.name)
    .bind(job.labels.first())
    .bind(&job.status)
    .bind(&job.conclusion)
    .bind(job.created_at)
    .bind(job.started_at)
    .bind(job.completed_at)
    .bind(failed_step.map(|(name, _)| name))
    .bind(failed_step.map(|(_, number)| number))
    .bind(detail_status)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A validated manifest ready to store.
pub(super) struct ManifestRow {
    pub(super) artifact_id: i64,
    pub(super) run_id: i64,
    pub(super) run_attempt: i32,
    pub(super) job_id: i64,
    pub(super) kind: &'static str,
    pub(super) schema_version: i32,
    pub(super) commit_sha: String,
    pub(super) payload: Value,
}

/// Inserts a manifest. Artifacts are immutable, so an existing row is kept.
pub(super) async fn insert_manifest(conn: &mut PgConnection, manifest: &ManifestRow) -> Result<()> {
    sqlx::query(
        "insert into ci.manifest (
             artifact_id, run_id, run_attempt, job_id, kind, schema_version, commit_sha, payload
         ) values ($1, $2, $3, $4, $5, $6, $7, $8)
         on conflict (artifact_id) do nothing",
    )
    .bind(manifest.artifact_id)
    .bind(manifest.run_id)
    .bind(manifest.run_attempt)
    .bind(manifest.job_id)
    .bind(manifest.kind)
    .bind(manifest.schema_version)
    .bind(&manifest.commit_sha)
    .bind(Json(&manifest.payload))
    .execute(&mut *conn)
    .await?;
    // Clears a retryable rejection recorded by an earlier attempt.
    sqlx::query("delete from ci.manifest_rejection where artifact_id = $1")
        .bind(manifest.artifact_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// An artifact that failed validation.
pub(super) struct RejectionRow {
    pub(super) artifact_id: i64,
    pub(super) run_id: i64,
    pub(super) run_attempt: Option<i32>,
    pub(super) artifact_name: String,
    pub(super) reason: &'static str,
}

/// Records a rejected artifact. A retryable `unmatched_job` rejection is
/// replaced by a later outcome; any other rejection is final.
pub(super) async fn insert_rejection(
    conn: &mut PgConnection,
    rejection: &RejectionRow,
    rejected_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        "insert into ci.manifest_rejection (
             artifact_id, run_id, run_attempt, artifact_name, reason, rejected_at
         ) values ($1, $2, $3, $4, $5, $6)
         on conflict (artifact_id) do update set
             run_attempt = excluded.run_attempt,
             reason = excluded.reason,
             rejected_at = excluded.rejected_at
         where ci.manifest_rejection.reason = $7",
    )
    .bind(rejection.artifact_id)
    .bind(rejection.run_id)
    .bind(rejection.run_attempt)
    .bind(&rejection.artifact_name)
    .bind(rejection.reason)
    .bind(rejected_at)
    .bind(Rejection::UnmatchedJob.as_str())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Marks runs of a completely listed day that the listing no longer returns.
pub(super) async fn mark_deleted(
    conn: &mut PgConnection,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    listed_run_ids: &[i64],
    deleted_at: DateTime<Utc>,
) -> Result<u64> {
    let result = sqlx::query(
        "update ci.run set deleted_at = $4
         where created_at >= $1 and created_at < $2 and deleted_at is null
             and not (run_id = any($3))",
    )
    .bind(start)
    .bind(end)
    .bind(listed_run_ids)
    .bind(deleted_at)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
}

/// Marks runs of a finalizing day that never completed as stale.
pub(super) async fn mark_stale(
    conn: &mut PgConnection,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64> {
    let result = sqlx::query(
        "update ci.run set stale = true
         where created_at >= $1 and created_at < $2 and status <> 'completed' and deleted_at is \
         null",
    )
    .bind(start)
    .bind(end)
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected())
}

/// Records the outcome of a processed day and clears its failure state.
pub(super) async fn record_day(
    conn: &mut PgConnection,
    day: NaiveDate,
    listed_at: Option<DateTime<Utc>>,
    run_count: i32,
    final_at: Option<DateTime<Utc>>,
    refetched_at: Option<DateTime<Utc>>,
) -> Result<()> {
    sqlx::query(
        "insert into ci.sync_day (day, listed_at, run_count, final_at, detail_refetched_at)
         values ($1, $2, $3, $4, $5)
         on conflict (day) do update set
             listed_at = coalesce(excluded.listed_at, ci.sync_day.listed_at),
             run_count = excluded.run_count,
             final_at = coalesce(ci.sync_day.final_at, excluded.final_at),
             detail_refetched_at = coalesce(excluded.detail_refetched_at, \
         ci.sync_day.detail_refetched_at),
             failures = 0,
             retry_after = null,
             last_failure_kind = null",
    )
    .bind(day)
    .bind(listed_at)
    .bind(run_count)
    .bind(final_at)
    .bind(refetched_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Counts a failure of `day` and returns the new failure count.
pub(super) async fn record_day_failure(
    conn: &mut PgConnection,
    day: NaiveDate,
    kind: &str,
) -> Result<u32> {
    let failures: i32 = sqlx::query_scalar(
        "insert into ci.sync_day (day, failures, last_failure_kind) values ($1, 1, $2)
         on conflict (day) do update set
             failures = ci.sync_day.failures + 1,
             last_failure_kind = excluded.last_failure_kind
         returning failures",
    )
    .bind(day)
    .bind(kind)
    .fetch_one(&mut *conn)
    .await?;
    Ok(u32::try_from(failures).unwrap_or(0))
}

/// Sets when a failed day may be retried.
pub(super) async fn set_retry_after(
    conn: &mut PgConnection,
    day: NaiveDate,
    retry_after: DateTime<Utc>,
) -> Result<()> {
    sqlx::query("update ci.sync_day set retry_after = $2 where day = $1")
        .bind(day)
        .bind(retry_after)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Upserts a code-scanning alert unless a newer snapshot is stored.
pub(super) async fn upsert_alert(conn: &mut PgConnection, alert: &CodeScanningAlert) -> Result<()> {
    let severity = alert.rule.security_severity_level.as_deref().or(alert.rule.severity.as_deref());
    sqlx::query(
        "insert into ci.code_scanning_alert (
             number, tool, rule_id, severity, state, created_at, fixed_at, dismissed_at, updated_at
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         on conflict (number) do update set
             tool = excluded.tool,
             rule_id = excluded.rule_id,
             severity = excluded.severity,
             state = excluded.state,
             fixed_at = excluded.fixed_at,
             dismissed_at = excluded.dismissed_at,
             updated_at = excluded.updated_at
         where excluded.updated_at >= ci.code_scanning_alert.updated_at",
    )
    .bind(alert.number)
    .bind(&alert.tool.name)
    .bind(alert.rule.id.as_deref().unwrap_or("unknown"))
    .bind(severity)
    .bind(&alert.state)
    .bind(alert.created_at)
    .bind(alert.fixed_at)
    .bind(alert.dismissed_at)
    .bind(alert.updated_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Inserts an analysis and returns whether it was new.
pub(super) async fn insert_analysis(
    conn: &mut PgConnection,
    analysis: &CodeScanningAnalysis,
) -> Result<bool> {
    let result = sqlx::query(
        "insert into ci.code_scanning_analysis (
             analysis_id, tool, category, created_at, has_error, has_warning
         ) values ($1, $2, $3, $4, $5, $6)
         on conflict (analysis_id) do nothing",
    )
    .bind(analysis.id)
    .bind(&analysis.tool.name)
    .bind(analysis.category.as_deref().unwrap_or(""))
    .bind(analysis.created_at)
    .bind(!analysis.error.is_empty())
    .bind(!analysis.warning.is_empty())
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Records that analyses are paged back to `history_since`.
pub(super) async fn set_analyses_backfilled(conn: &mut PgConnection) -> Result<()> {
    sqlx::query("update ci.sync_state set analyses_backfilled = true").execute(&mut *conn).await?;
    Ok(())
}

/// Records a sync that finished without fatal errors.
pub(super) async fn record_success(conn: &mut PgConnection, at: DateTime<Utc>) -> Result<()> {
    sqlx::query("update ci.sync_state set last_success_at = $1")
        .bind(at)
        .execute(&mut *conn)
        .await?;
    Ok(())
}
