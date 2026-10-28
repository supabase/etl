-- Expose when the measured commit was made, so benchmark backfills of older
-- commits plot at the commit's date rather than the run's.
--
-- `commit.committed_at` is an RFC 3339 string. New manifests store it in UTC;
-- rows ingested earlier keep the committer's offset, which the cast also
-- accepts. `create or replace view` may only append columns, so the existing
-- column order is unchanged and grants are preserved.

create or replace view ci_report.benchmark_metric as
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
        as run_url,
    (m.payload -> 'commit' ->> 'committed_at')::timestamptz as committed_at
from ci.manifest m
join ci.run r using (run_id, run_attempt)
cross join lateral jsonb_each(m.payload -> 'reports') report
cross join lateral jsonb_each(report.value -> 'metrics') metric
where m.kind = 'benchmark' and r.deleted_at is null;
