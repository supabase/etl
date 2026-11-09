# Builds the CI metrics audit manifest from the JSON lines that
# `cargo deny --format json check advisories` prints, read as raw inputs.
#
# Run with `jq -n -f`. The job identity comes from the GitHub Actions
# environment plus `JOB_CHECK_RUN_ID`, set from `job.check_run_id` because
# GitHub exports no job id; `CARGO_DENY_VERSION` names the tool version.
# Every diagnostic that names an advisory becomes a finding, one per advisory
# and affected crate version. Titles are cut to 128 characters, the collector's
# limit. A run without a summary line fails, so a crashed check uploads nothing.

[inputs] as $lines
| ($lines | map(select(.type == "summary")) | last | .fields.advisories) as $summary
| if $summary == null then error("cargo-deny printed no advisories summary") else . end
| {
    schema_version: 1,
    kind: "audit",
    github: {
      repository: $ENV.GITHUB_REPOSITORY,
      run_id: ($ENV.GITHUB_RUN_ID | tonumber),
      run_attempt: ($ENV.GITHUB_RUN_ATTEMPT | tonumber),
      job_id: ($ENV.JOB_CHECK_RUN_ID | tonumber)
    },
    tool: { name: "cargo-deny", version: $ENV.CARGO_DENY_VERSION },
    summary: {
      errors: $summary.errors,
      warnings: $summary.warnings,
      notes: $summary.notes,
      helps: $summary.helps
    },
    findings: (
      [
        $lines[]
        | select(.type == "diagnostic" and .fields.advisory != null)
        | .fields as $diagnostic
        | ($diagnostic.graphs[0].Krate // {}) as $krate
        | {
            advisory_id: $diagnostic.advisory.id,
            kind: $diagnostic.code,
            severity: $diagnostic.severity,
            package: ($krate.name // $diagnostic.advisory.package),
            version: ($krate.version // "unknown"),
            title: ($diagnostic.advisory.title | .[0:128])
          }
      ]
      | unique_by([.advisory_id, .package, .version])
    )
  }
