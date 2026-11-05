-- Dependabot alerts, so the dashboard's security counts include vulnerable
-- dependencies and not only code-scanning findings.
--
-- `ci.dependabot_alert` holds structural metadata per alert: the package, its
-- manifest, the advisory id and severity, and lifecycle timestamps. The sync
-- relists every alert each run and upserts by alert number, so the table
-- starts empty and fills on the first sync after this migration.
--
-- `ci_report.security_daily` keeps its columns and gains Dependabot rows with
-- `tool = 'Dependabot'`; existing code-scanning rows are unchanged. `create or
-- replace view` keeps the view's grants to `ci_reader`.
--
-- Deploy: the collector refuses to sync until its newest embedded migration
-- is applied, so run `cargo x ci-metrics migrate` as the owner when the
-- collector change reaches `main`. Syncs started in between fail and the next
-- hourly sync catches up. Rollback: revert the collector, restore the
-- previous view definition, drop the table, and delete version
-- 20261105000000 from `ci._sqlx_migrations`, because the reverted collector
-- requires its own newest migration to be the newest applied one. No other
-- object depends on the table.

create table ci.dependabot_alert (
    number integer primary key,
    ecosystem text not null,
    package text not null,
    manifest_path text not null,
    advisory_id text not null,
    severity text not null,
    state text not null,
    created_at timestamptz not null,
    fixed_at timestamptz,
    dismissed_at timestamptz,
    auto_dismissed_at timestamptz,
    updated_at timestamptz not null
);

-- Default privileges already cover tables the owner creates in `ci`; the grant
-- keeps the writer's access explicit for this table.
grant select, insert, update, delete on ci.dependabot_alert to ci_writer;

-- An alert counts as open on a UTC day when it was open at the day's end. It
-- closes at its earliest fix or dismissal; `least` ignores null timestamps.
create or replace view ci_report.security_daily as
with alert as (
    select tool, severity, created_at, least(fixed_at, dismissed_at) as closed_at
    from ci.code_scanning_alert
    union all
    select 'Dependabot', severity, created_at, least(fixed_at, dismissed_at, auto_dismissed_at)
    from ci.dependabot_alert
)
select
    day::date as day,
    a.tool,
    coalesce(a.severity, 'unknown') as severity,
    count(*) as open_alerts
from generate_series(
    date_trunc('day', (select coalesce(min(created_at), now()) from alert) at time zone 'utc'),
    date_trunc('day', now() at time zone 'utc'),
    interval '1 day'
) day
join alert a
    on a.created_at < (day + interval '1 day') at time zone 'utc'
    and (a.closed_at is null or a.closed_at >= (day + interval '1 day') at time zone 'utc')
group by 1, 2, 3;
