-- CI metrics store for supabase/etl GitHub Actions data.
--
-- `ci` holds the raw tables written by `cargo x ci-metrics sync`. `ci_report`
-- holds views, which are the only objects Grafana may read. Views run with
-- their owner's privileges, so `ci_reader` never needs access to `ci`.
--
-- Run by hand as the owner role (`cargo x ci-metrics migrate`). The migration
-- connection sets `search_path = ci`, so sqlx's `_sqlx_migrations` table lives
-- in `ci`. Roles are created without login: provisioning sets login and
-- password verifiers separately, so migrations never handle secrets.

revoke all on schema ci from public;

create schema ci_report;
revoke all on schema ci_report from public;

do $$
begin
    if not exists (select from pg_roles where rolname = 'ci_writer') then
        create role ci_writer nologin;
    end if;
    if not exists (select from pg_roles where rolname = 'ci_reader') then
        create role ci_reader nologin;
    end if;
end
$$;

-- Workflows by GitHub id. Runs are grouped by `workflow_id`, never by run
-- name, because dynamic workflows (CodeQL, Copilot) name runs after triggers.
create table ci.workflow (
    workflow_id bigint primary key,
    path text not null,
    name text not null
);

-- One row per run attempt. `updated_at` guards upserts so an older snapshot
-- never replaces a newer one.
create table ci.run (
    run_id bigint not null,
    run_attempt integer not null check (run_attempt >= 1),
    workflow_id bigint not null references ci.workflow,
    event text not null,
    head_branch text,
    head_sha text not null,
    display_title text not null,
    from_fork boolean not null,
    status text not null,
    conclusion text,
    created_at timestamptz not null,
    run_started_at timestamptz,
    updated_at timestamptz not null,
    -- Last time jobs and manifests were fetched for this run; drives settling.
    detail_fetched_at timestamptz,
    artifact_status text not null default 'none'
        check (artifact_status in ('none', 'available', 'expired')),
    -- Set only from a complete day listing that no longer returns the run.
    deleted_at timestamptz,
    -- Not completed when its day became final, past GitHub's run lifetime.
    stale boolean not null default false,
    primary key (run_id, run_attempt)
);

create index run_created_at_idx on ci.run (created_at);
create index run_workflow_created_at_idx on ci.run (workflow_id, created_at);

-- One row per job. GitHub gives each attempt new job ids. Upserts are
-- monotonic: a completed job never regresses and set fields never clear.
create table ci.job (
    job_id bigint primary key,
    run_id bigint not null,
    run_attempt integer not null,
    name text not null,
    runner_label text,
    status text not null,
    conclusion text,
    created_at timestamptz not null,
    started_at timestamptz,
    completed_at timestamptz,
    failed_step_name text,
    failed_step_number integer,
    -- `expired` once GitHub has dropped step details (90-day log retention).
    detail_status text not null check (detail_status in ('none', 'available', 'expired')),
    foreign key (run_id, run_attempt) references ci.run
);

create index job_run_idx on ci.job (run_id, run_attempt);
create index job_completed_at_idx on ci.job (completed_at);

-- Validated manifests. Artifacts are immutable, so `artifact_id` is the key
-- and rows are never replaced or deleted by a later, emptier fetch.
create table ci.manifest (
    artifact_id bigint primary key,
    run_id bigint not null,
    run_attempt integer not null,
    job_id bigint not null references ci.job,
    kind text not null,
    schema_version integer not null,
    -- The measured commit, which differs from the run's head for backfills.
    commit_sha text not null,
    payload jsonb not null,
    foreign key (run_id, run_attempt) references ci.run
);

create index manifest_run_idx on ci.manifest (run_id, run_attempt);

-- Artifacts that failed validation. Recorded instead of failing the sync, so
-- one bad artifact cannot block later syncs.
create table ci.manifest_rejection (
    artifact_id bigint primary key,
    run_id bigint not null,
    -- Null when the manifest could not be parsed far enough to read it.
    run_attempt integer,
    artifact_name text not null,
    reason text not null,
    rejected_at timestamptz not null
);

-- List prices per runner label. Cost is an estimate; the invoice is the
-- authority. Labels without a price yield a null cost, never zero.
create table ci.runner_price (
    label text not null,
    effective_from date not null,
    usd_per_minute numeric(10, 5) not null check (usd_per_minute >= 0),
    primary key (label, effective_from)
);

-- Blacksmith list prices read from blacksmith.sh/pricing on 2026-10-27.
-- GitHub-hosted runners are free for public repositories.
insert into ci.runner_price (label, effective_from, usd_per_minute) values
    ('blacksmith-2vcpu-ubuntu-2404', '2024-01-01', 0.004),
    ('blacksmith-4vcpu-ubuntu-2404', '2024-01-01', 0.008),
    ('blacksmith-8vcpu-ubuntu-2404', '2024-01-01', 0.016),
    ('blacksmith-16vcpu-ubuntu-2404', '2024-01-01', 0.032),
    ('blacksmith-32vcpu-ubuntu-2404', '2024-01-01', 0.064),
    ('blacksmith-2vcpu-ubuntu-2404-arm', '2024-01-01', 0.0025),
    ('blacksmith-4vcpu-ubuntu-2404-arm', '2024-01-01', 0.005),
    ('blacksmith-8vcpu-ubuntu-2404-arm', '2024-01-01', 0.01),
    ('blacksmith-16vcpu-ubuntu-2404-arm', '2024-01-01', 0.02),
    ('blacksmith-32vcpu-ubuntu-2404-arm', '2024-01-01', 0.04),
    ('ubuntu-latest', '2024-01-01', 0),
    ('ubuntu-24.04', '2024-01-01', 0),
    ('ubuntu-22.04', '2024-01-01', 0);

-- Code-scanning alerts as structural metadata only: no paths or messages.
create table ci.code_scanning_alert (
    number integer primary key,
    tool text not null,
    rule_id text not null,
    severity text,
    state text not null,
    created_at timestamptz not null,
    fixed_at timestamptz,
    dismissed_at timestamptz,
    updated_at timestamptz not null
);

-- Analyses are immutable once created.
create table ci.code_scanning_analysis (
    analysis_id bigint primary key,
    tool text not null,
    category text not null,
    created_at timestamptz not null,
    has_error boolean not null,
    has_warning boolean not null
);

create index code_scanning_analysis_created_at_idx on ci.code_scanning_analysis (created_at);

-- Per-UTC-day sync state. The planner compares desired coverage with these
-- rows; there is no global cursor.
create table ci.sync_day (
    day date primary key,
    -- Time of the last listing of this day that finished in full.
    listed_at timestamptz,
    run_count integer,
    -- Set once from a complete listing taken more than 35 days after the day.
    final_at timestamptz,
    -- Last forced re-fetch of every run's jobs (daily, for recent days).
    detail_refetched_at timestamptz,
    failures integer not null default 0 check (failures >= 0),
    retry_after timestamptz,
    last_failure_kind text
);

create table ci.sync_state (
    id boolean primary key default true check (id),
    history_since date not null,
    last_success_at timestamptz,
    -- Whether code-scanning analyses have been paged back to `history_since`.
    analyses_backfilled boolean not null default false
);

-- Latest attempt of each live run.
create view ci_report.run_latest as
select distinct on (r.run_id)
    r.*
from ci.run r
where r.deleted_at is null
order by r.run_id, r.run_attempt desc;

create view ci_report.run_summary as
select
    r.run_id,
    r.run_attempt,
    r.workflow_id,
    w.path as workflow_path,
    w.name as workflow_name,
    r.event,
    r.head_branch,
    r.head_sha,
    r.display_title,
    r.status,
    r.conclusion,
    r.created_at,
    r.run_started_at,
    j.last_completed_at as completed_at,
    extract(epoch from j.last_completed_at - coalesce(r.run_started_at, r.created_at))
        as wall_seconds,
    r.artifact_status,
    r.stale,
    r.run_attempt = max(r.run_attempt) over (partition by r.run_id) as is_latest_attempt,
    'https://github.com/supabase/etl/actions/runs/' || r.run_id || '/attempts/' || r.run_attempt
        as run_url,
    'https://github.com/supabase/etl/commit/' || r.head_sha as commit_url
from ci.run r
join ci.workflow w using (workflow_id)
left join lateral (
    select max(job.completed_at) as last_completed_at
    from ci.job job
    where job.run_id = r.run_id and job.run_attempt = r.run_attempt
        -- Jobs carried over from an earlier attempt; see `job_cost`.
        and (r.run_started_at is null or job.started_at >= r.run_started_at - interval '1 minute')
) j on true
where r.deleted_at is null;

-- Job timings and estimated cost. Minutes are rounded up per job, matching
-- GitHub's billing convention; Blacksmith's exact rounding is not modelled.
--
-- "Re-run failed jobs" copies successful jobs into the new attempt under new
-- job ids with their original start and end times, so a copy started before
-- its attempt did. Copies did not run again: they cost nothing and have no
-- queue or run time. The one-minute margin absorbs clock skew between job and
-- attempt timestamps.
create view ci_report.job_cost as
select
    j.job_id,
    j.run_id,
    j.run_attempt,
    r.workflow_id,
    w.path as workflow_path,
    w.name as workflow_name,
    r.event,
    r.head_branch,
    r.head_sha,
    j.name as job_name,
    j.runner_label,
    j.status,
    j.conclusion,
    j.created_at as queued_at,
    j.started_at,
    j.completed_at,
    coalesce(j.started_at < r.run_started_at - interval '1 minute', false) as carried_over,
    -- Sub-minute skew can put `started_at` just before `created_at`; clamp it
    -- so latency aggregates never average negative waits.
    case
        when not coalesce(j.started_at < r.run_started_at - interval '1 minute', false)
            then greatest(extract(epoch from j.started_at - j.created_at), 0)
    end as queue_seconds,
    case
        when not coalesce(j.started_at < r.run_started_at - interval '1 minute', false)
            then extract(epoch from j.completed_at - j.started_at)
    end as run_seconds,
    price.usd_per_minute,
    case
        when coalesce(j.started_at < r.run_started_at - interval '1 minute', false) then 0
        else ceil(extract(epoch from j.completed_at - j.started_at) / 60.0) * price.usd_per_minute
    end as estimated_usd,
    j.failed_step_name,
    j.detail_status,
    'https://github.com/supabase/etl/actions/runs/' || j.run_id || '/job/' || j.job_id as job_url,
    case
        when j.failed_step_number is not null
            then 'https://github.com/supabase/etl/actions/runs/' || j.run_id || '/job/' || j.job_id
                || '#step:' || j.failed_step_number || ':1'
    end as failed_step_url
from ci.job j
join ci.run r using (run_id, run_attempt)
join ci.workflow w on w.workflow_id = r.workflow_id
left join lateral (
    select p.usd_per_minute
    from ci.runner_price p
    where p.label = j.runner_label
        and p.effective_from <= (j.started_at at time zone 'utc')::date
    order by p.effective_from desc
    limit 1
) price on true
where r.deleted_at is null;

-- Daily outcome of scheduled workflows, using each run's latest attempt.
create view ci_report.scheduled_daily as
select
    (r.created_at at time zone 'utc')::date as day,
    r.workflow_id,
    w.name as workflow_name,
    count(*) as runs,
    count(*) filter (where r.conclusion = 'success') as successes,
    count(*) filter (where r.conclusion = 'failure') as failures,
    count(*) filter (where r.conclusion = 'cancelled') as cancellations,
    count(*) filter (where r.status <> 'completed') as unfinished,
    (array_agg(
        'https://github.com/supabase/etl/actions/runs/' || r.run_id || '/attempts/' || r.run_attempt
        order by r.created_at desc
    ) filter (where r.conclusion = 'failure'))[1] as latest_failure_url
from ci_report.run_latest r
join ci.workflow w using (workflow_id)
where r.event = 'schedule'
group by 1, 2, 3;

-- One row per benchmark metric value.
create view ci_report.benchmark_metric as
select
    m.artifact_id,
    m.run_id,
    m.run_attempt,
    m.job_id,
    m.commit_sha,
    r.created_at,
    m.payload ->> 'trigger' as trigger,
    m.payload ->> 'destination' as destination,
    m.payload -> 'runner' ->> 'label' as runner_label,
    report.key as report,
    metric.key as metric,
    (metric.value #>> '{}')::double precision as value,
    'https://github.com/supabase/etl/actions/runs/' || m.run_id || '/attempts/' || m.run_attempt
        as run_url
from ci.manifest m
join ci.run r using (run_id, run_attempt)
cross join lateral jsonb_each(m.payload -> 'reports') report
cross join lateral jsonb_each(report.value -> 'metrics') metric
where m.kind = 'benchmark' and r.deleted_at is null;

-- Open code-scanning alerts per day, reconstructed from alert timestamps.
create view ci_report.security_daily as
select
    day::date as day,
    a.tool,
    coalesce(a.severity, 'unknown') as severity,
    count(*) as open_alerts
from generate_series(
    date_trunc('day', (select coalesce(min(created_at), now()) from ci.code_scanning_alert) at time zone 'utc'),
    date_trunc('day', now() at time zone 'utc'),
    interval '1 day'
) day
-- An alert counts as open on a UTC day when it was open at the day's end.
join ci.code_scanning_alert a
    on a.created_at < (day + interval '1 day') at time zone 'utc'
    and (a.fixed_at is null or a.fixed_at >= (day + interval '1 day') at time zone 'utc')
    and (a.dismissed_at is null or a.dismissed_at >= (day + interval '1 day') at time zone 'utc')
group by 1, 2, 3;

create view ci_report.code_scanning_analysis_daily as
select
    (created_at at time zone 'utc')::date as day,
    tool,
    category,
    count(*) as analyses,
    count(*) filter (where has_error) as errors,
    count(*) filter (where has_warning) as warnings
from ci.code_scanning_analysis
group by 1, 2, 3;

-- Coverage of the desired history window by sync state.
create view ci_report.sync_coverage as
select
    count(*) filter (where d.final_at is not null) as final_days,
    count(*) filter (where d.final_at is null and d.listed_at is not null) as listed_days,
    count(*) filter (where d.listed_at is null) as missing_days,
    count(*) filter (where d.retry_after > now()) as blocked_days,
    min(want.day) filter (where d.listed_at is null)::date as oldest_missing_day
from ci.sync_state s
cross join lateral generate_series(
    s.history_since::timestamp,
    (now() at time zone 'utc')::date::timestamp,
    interval '1 day'
) want(day)
left join ci.sync_day d on d.day = want.day::date;

create view ci_report.sync_freshness as
select
    last_success_at,
    extract(epoch from now() - last_success_at) as seconds_since_success
from ci.sync_state;

-- Writer: data in `ci` only, no DDL, read-only access to the migration table
-- for the schema-version check.
grant usage on schema ci to ci_writer;
grant select, insert, update, delete on all tables in schema ci to ci_writer;
revoke insert, update, delete on ci._sqlx_migrations from ci_writer;
alter default privileges in schema ci grant select, insert, update, delete on tables to ci_writer;

-- Reader: the report views only.
grant usage on schema ci_report to ci_reader;
grant select on all tables in schema ci_report to ci_reader;
alter default privileges in schema ci_report grant select on tables to ci_reader;

alter role ci_writer connection limit 3;
alter role ci_writer set statement_timeout = '60s';
alter role ci_writer set lock_timeout = '10s';
alter role ci_writer set idle_in_transaction_session_timeout = '60s';

alter role ci_reader connection limit 5;
alter role ci_reader set default_transaction_read_only = on;
alter role ci_reader set statement_timeout = '10s';
