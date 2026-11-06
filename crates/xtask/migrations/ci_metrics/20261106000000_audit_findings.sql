-- Dependency audit findings, so the dashboard can show RustSec advisories that
-- `cargo deny` reports for `main`, including ones Dependabot does not raise.
--
-- The dependency audit workflow uploads an `audit-results-<job id>` manifest
-- from runs on `main`; the sync stores it in `ci.manifest` with kind `audit`
-- (payload: tool, version and diagnostic counts) and one `ci.audit_finding`
-- row per advisory and affected crate version. Earlier audit runs uploaded no
-- manifest, so the table starts empty and fills from the first audit run on
-- `main` after the workflow change.
--
-- `ci_report.dependency_audit` has one row per stored audit, including clean
-- ones, so "no findings" is distinguishable from "not run";
-- `ci_report.audit_finding` lists the findings with their run.
--
-- Deploy: the collector refuses to sync until its newest embedded migration
-- is applied, so run `cargo x ci-metrics migrate` as the owner once the
-- collector change is on `main`. Rollback: revert the collector, drop both
-- views and the table, delete `ci.manifest` rows of kind `audit`, and delete
-- version 20261106000000 from `ci._sqlx_migrations`.

create table ci.audit_finding (
    job_id bigint not null references ci.job,
    advisory_id text not null,
    package text not null,
    version text not null,
    kind text not null,
    severity text not null check (severity in ('error', 'warning', 'note', 'help')),
    title text not null,
    primary key (job_id, advisory_id, package, version)
);

-- Default privileges already cover tables the owner creates in `ci`; the grant
-- keeps the writer's access explicit for this table.
grant select, insert, update, delete on ci.audit_finding to ci_writer;

create view ci_report.dependency_audit as
select
    m.run_id,
    m.run_attempt,
    m.job_id,
    r.event,
    r.head_branch,
    m.commit_sha,
    j.completed_at,
    j.conclusion,
    m.payload ->> 'tool_version' as tool_version,
    (m.payload ->> 'errors')::integer as errors,
    (m.payload ->> 'warnings')::integer as warnings,
    (m.payload ->> 'notes')::integer as notes,
    (m.payload ->> 'findings')::integer as findings,
    'https://github.com/supabase/etl/actions/runs/' || m.run_id || '/job/' || m.job_id as job_url
from ci.manifest m
join ci.job j using (job_id)
join ci.run r on r.run_id = m.run_id and r.run_attempt = m.run_attempt
where m.kind = 'audit' and r.deleted_at is null;

create view ci_report.audit_finding as
select
    f.job_id,
    j.run_id,
    j.run_attempt,
    r.event,
    r.head_branch,
    r.head_sha,
    j.completed_at,
    f.advisory_id,
    f.kind,
    f.severity,
    f.package,
    f.version,
    f.title,
    case
        when f.advisory_id like 'RUSTSEC-%'
            then 'https://rustsec.org/advisories/' || f.advisory_id || '.html'
    end as advisory_url,
    'https://github.com/supabase/etl/actions/runs/' || j.run_id || '/job/' || f.job_id as job_url
from ci.audit_finding f
join ci.job j using (job_id)
join ci.run r on r.run_id = j.run_id and r.run_attempt = j.run_attempt
where r.deleted_at is null;

grant select on ci_report.dependency_audit, ci_report.audit_finding to ci_reader;
