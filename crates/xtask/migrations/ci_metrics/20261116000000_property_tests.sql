-- Nightly property-test results, so the dashboard can chart the property-test
-- workflow's outcome, runtime and failing tests over time.
--
-- The property-test workflow runs the full suite through the same reusable
-- test job as CI, with every property's wall-clock budget raised, and uploads
-- the same `test-results-<job id>` manifest. The sync now stores those
-- manifests from runs on `main` with every result, and the manifest payload
-- records `property_budget_secs` (null when the job set no budget). Recent
-- runs are re-fetched daily, so runs of the last week whose artifacts have not
-- expired fill in on the first sync after deploy; older runs keep no results.
--
-- `ci_report.test_result` gains `workflow_path` and `property_budget_secs` as
-- trailing columns, so `create or replace view` keeps dependents and grants.
-- `test_duration_daily` and `flaky_test` keep their meaning by reading CI
-- results only: property durations follow the budget, and a property that
-- fails and then passes on a re-run found a counterexample the re-run did not
-- draw again, which is not a flake. `ci_report.property_test_run` has one row
-- per completed property-test run attempt on `main`.
--
-- Deploy: the collector refuses to sync until its newest embedded migration
-- is applied, so run `cargo x ci-metrics migrate` as the owner once the
-- collector change is on `main`. Rollback: revert the collector; drop
-- `property_test_run`, `flaky_test`, `test_duration_daily` and `test_result`
-- from `ci_report`, recreate the last three from 20261027000000_init.sql and
-- grant them to `ci_reader`; delete `ci.test_result` and `ci.manifest` rows of
-- property-test runs; and delete version 20261116000000 from
-- `ci._sqlx_migrations`.

create or replace view ci_report.test_result as
select
    tr.job_id,
    j.run_id,
    j.run_attempt,
    r.event,
    r.head_branch,
    r.head_sha,
    j.name as job_name,
    j.completed_at,
    t.binary_id,
    t.name as test_name,
    tr.status,
    tr.duration_ms,
    coalesce((m.payload ->> 'coverage')::boolean, false) as coverage,
    'https://github.com/supabase/etl/actions/runs/' || j.run_id || '/job/' || j.job_id as job_url,
    w.path as workflow_path,
    (m.payload ->> 'property_budget_secs')::bigint as property_budget_secs
from ci.test_result tr
join ci.test t using (test_id)
join ci.job j using (job_id)
join ci.run r on r.run_id = j.run_id and r.run_attempt = j.run_attempt
join ci.workflow w on w.workflow_id = r.workflow_id
join ci.manifest m on m.job_id = tr.job_id and m.kind = 'test'
where r.deleted_at is null;

-- Daily duration percentiles per test from passing CI runs on `main`,
-- excluding coverage jobs. Pull requests store failures only and never appear
-- here.
create or replace view ci_report.test_duration_daily as
select
    (completed_at at time zone 'utc')::date as day,
    binary_id,
    test_name,
    count(*) as results,
    percentile_cont(0.5) within group (order by duration_ms) / 1000.0 as p50_seconds,
    percentile_cont(0.9) within group (order by duration_ms) / 1000.0 as p90_seconds
from ci_report.test_result
where status = 'passed'
    and not coverage
    and head_branch = 'main'
    and workflow_path = '.github/workflows/ci.yml'
group by 1, 2, 3;

-- CI tests that failed in a job whose same-named job passed in a later
-- attempt of the same run: the rule of `flaky_job`, per test. One row per
-- failure.
create or replace view ci_report.flaky_test as
select
    f.run_id,
    f.run_attempt,
    f.event,
    f.head_branch,
    case when f.head_branch = 'main' then 'main' else 'pull request' end as scope,
    f.job_name,
    f.binary_id,
    f.test_name,
    f.completed_at,
    f.job_url
from ci_report.test_result f
where f.status = 'failed'
    and f.workflow_path = '.github/workflows/ci.yml'
    and exists (
        select
        from ci.job retried
        where retried.run_id = f.run_id
            and retried.name = f.job_name
            and retried.run_attempt > f.run_attempt
            and retried.conclusion = 'success'
    );

-- One row per completed attempt of the property-test workflow on `main` and
-- test job with a stored manifest. `conclusion` and `wall_seconds` cover the
-- whole run, including the build; the remaining columns come from the test
-- job's manifest and are null when none is stored: the run failed before its
-- tests, the upload failed, or the artifact expired before a sync read it.
-- Failing tests are in `ci_report.test_result` by `job_id`.
create view ci_report.property_test_run as
select
    r.run_id,
    r.run_attempt,
    r.is_latest_attempt,
    r.event,
    r.head_sha,
    r.created_at,
    r.completed_at,
    r.conclusion,
    r.wall_seconds,
    m.job_id,
    j.run_seconds as test_seconds,
    (m.payload ->> 'property_budget_secs')::bigint as property_budget_secs,
    (m.payload ->> 'tests')::integer as tests,
    (m.payload ->> 'failed')::integer as failed,
    r.run_url,
    j.job_url
from ci_report.run_summary r
left join ci.manifest m
    on m.run_id = r.run_id and m.run_attempt = r.run_attempt and m.kind = 'test'
left join ci_report.job_cost j on j.job_id = m.job_id
where r.workflow_path = '.github/workflows/property-tests.yml'
    and r.head_branch = 'main'
    and r.status = 'completed';

grant select on ci_report.property_test_run to ci_reader;
