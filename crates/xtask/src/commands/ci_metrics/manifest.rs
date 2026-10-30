//! Validation of manifest artifacts uploaded by CI jobs.
//!
//! Artifacts are untrusted input: same-repo pull requests control their
//! contents. Parsing never panics, keeps only allowlisted fields, and reports
//! every failure as a [`Rejection`] that the caller records instead of
//! failing the sync.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, FixedOffset, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    commands::benchmark_compare::CONFIG_KEYS,
    test_manifest::{self, TestManifest},
};

/// Artifact name prefixes of benchmark result uploads: nightly and manual
/// runs, and backfills of older commits. Backfills are never comparison
/// baselines, but they are benchmark history; the manifest `trigger` tells
/// them apart.
const BENCHMARK_ARTIFACT_PREFIXES: &[&str] = &["benchmark-results-", "benchmark-backfill-"];
/// Largest artifact archive downloaded.
pub(super) const MAX_ARCHIVE_BYTES: u64 = 4 * 1024 * 1024;
/// Largest single file read from an archive. A test manifest of about 1,900
/// tests is about 300 KiB.
pub(super) const MAX_ENTRY_BYTES: u64 = 4 * 1024 * 1024;
/// Most tests accepted in one test manifest.
const MAX_TESTS: usize = 20_000;
/// Longest test binary id or test name accepted.
const MAX_TEST_NAME_LEN: usize = 256;
/// Most files accepted in one archive.
const MAX_ENTRIES: usize = 16;
/// Manifest file name inside a results archive.
pub(super) const MANIFEST_FILE_NAME: &str = "manifest.json";
/// Longest free-form string kept from a manifest or report.
const MAX_STRING_LEN: usize = 128;
/// Report fields dropped even though they are numeric.
const DROPPED_METRICS: &[&str] = &["pipeline_id"];

/// Why an artifact was not ingested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rejection {
    /// The archive or one of its files exceeds a size limit.
    TooLarge,
    /// The archive listing is malformed or contains unexpected files.
    InvalidArchive,
    /// The manifest or a report is not valid for its schema.
    Invalid,
    /// The manifest names a different repository or run.
    WrongRun,
    /// The manifest's job could not be resolved to exactly one job.
    UnmatchedJob,
    /// The manifest kind or schema version is not supported.
    Unsupported,
}

impl Rejection {
    /// Returns the stored reason code.
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::TooLarge => "too_large",
            Self::InvalidArchive => "invalid_archive",
            Self::Invalid => "invalid",
            Self::WrongRun => "wrong_run",
            Self::UnmatchedJob => "unmatched_job",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Kind of manifest an artifact carries, from its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArtifactKind {
    /// Benchmark reports.
    Benchmark,
    /// Per-test results of the job with this id.
    Test { job_id: i64 },
}

/// Returns the kind of manifest an artifact carries, or `None` for other
/// artifacts.
pub(super) fn artifact_kind(name: &str) -> Option<ArtifactKind> {
    if BENCHMARK_ARTIFACT_PREFIXES.iter().any(|prefix| name.starts_with(prefix)) {
        return Some(ArtifactKind::Benchmark);
    }
    let job_id = name.strip_prefix(test_manifest::ARTIFACT_PREFIX)?;
    if !job_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    job_id.parse().ok().filter(|id| *id > 0).map(|job_id| ArtifactKind::Test { job_id })
}

/// One file named in an archive listing.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ArchiveEntry {
    pub(super) name: String,
    pub(super) declared_size: u64,
}

/// Returns whether `name` is a flat `*.json` file name of safe characters.
fn is_safe_json_name(name: &str) -> bool {
    name.len() <= 64
        && name.ends_with(".json")
        && name.len() > ".json".len()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        && !name.starts_with('.')
        && !name.contains("..")
}

/// Parses `unzip -l` output into entries and checks them against the limits.
///
/// Declared sizes come from the archive header and can lie; callers must also
/// bound each read. Only flat `*.json` names made of safe characters are
/// accepted, which also keeps them free of `unzip` wildcard characters.
pub(super) fn parse_archive_listing(listing: &str) -> Result<Vec<ArchiveEntry>, Rejection> {
    let mut sections = listing.split("\n---------");
    // Header, entries, then totals.
    let entries_section = sections.nth(1).ok_or(Rejection::InvalidArchive)?;
    let mut entries = Vec::new();
    // The first line is the remainder of the separator row.
    for line in entries_section.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let size =
            fields.next().and_then(|size| size.parse().ok()).ok_or(Rejection::InvalidArchive)?;
        // Date and time.
        fields.next().ok_or(Rejection::InvalidArchive)?;
        fields.next().ok_or(Rejection::InvalidArchive)?;
        let name = fields.next().ok_or(Rejection::InvalidArchive)?;
        if fields.next().is_some() || !is_safe_json_name(name) {
            return Err(Rejection::InvalidArchive);
        }
        // A repeated name would make `unzip -p` concatenate several files.
        if entries.iter().any(|entry: &ArchiveEntry| entry.name == name) {
            return Err(Rejection::InvalidArchive);
        }
        if size > MAX_ENTRY_BYTES {
            return Err(Rejection::TooLarge);
        }
        entries.push(ArchiveEntry { name: name.to_owned(), declared_size: size });
    }
    if entries.len() > MAX_ENTRIES {
        return Err(Rejection::InvalidArchive);
    }
    if !entries.iter().any(|entry| entry.name == MANIFEST_FILE_NAME) {
        return Err(Rejection::InvalidArchive);
    }
    Ok(entries)
}

/// Benchmark manifest, schema version 1.
#[derive(Debug, Deserialize)]
pub(super) struct BenchmarkManifest {
    schema_version: u32,
    kind: String,
    trigger: String,
    destination: String,
    commit: ManifestCommit,
    github: ManifestGithub,
    runner: ManifestRunner,
    reports: Vec<String>,
}

/// Commit measured by a benchmark run.
#[derive(Debug, Deserialize)]
struct ManifestCommit {
    sha: String,
    /// RFC 3339 with the committer's offset; stored normalized to UTC.
    committed_at: Option<DateTime<FixedOffset>>,
}

/// Run identity recorded by the producing job.
#[derive(Debug, Deserialize)]
struct ManifestGithub {
    repository: String,
    run_id: i64,
    run_attempt: i32,
    #[serde(default)]
    job_id: Option<i64>,
}

/// Runner that executed the benchmark.
#[derive(Debug, Deserialize)]
struct ManifestRunner {
    label: String,
    cpus: Option<u32>,
    memory_bytes: Option<u64>,
}

impl BenchmarkManifest {
    /// Returns the run attempt that produced the manifest.
    pub(super) fn run_attempt(&self) -> i32 {
        self.github.run_attempt
    }

    /// Returns the producing job id, when the manifest records it.
    pub(super) fn job_id(&self) -> Option<i64> {
        self.github.job_id
    }

    /// Returns the expected display name of the producing job, used only
    /// when the manifest has no job id.
    pub(super) fn job_name(&self) -> String {
        format!("Benchmark ({})", self.destination)
    }

    /// Returns the measured commit.
    pub(super) fn commit_sha(&self) -> &str {
        &self.commit.sha
    }

    /// Returns the schema version.
    pub(super) fn schema_version(&self) -> i32 {
        // Validated to equal 1 by `parse_benchmark_manifest`.
        i32::try_from(self.schema_version).unwrap_or(i32::MAX)
    }

    /// Returns the report file names the manifest describes.
    pub(super) fn reports(&self) -> &[String] {
        &self.reports
    }
}

/// Parses and validates a benchmark manifest from `repo` run `run_id`.
pub(super) fn parse_benchmark_manifest(
    bytes: &[u8],
    repo: &str,
    run_id: i64,
) -> Result<BenchmarkManifest, Rejection> {
    let manifest: BenchmarkManifest =
        serde_json::from_slice(bytes).map_err(|_| Rejection::Invalid)?;
    if manifest.kind != "benchmark" || manifest.schema_version != 1 {
        return Err(Rejection::Unsupported);
    }
    if manifest.github.repository != repo || manifest.github.run_id != run_id {
        return Err(Rejection::WrongRun);
    }
    let valid = manifest.github.run_attempt >= 1
        && manifest.github.job_id.is_none_or(|id| id > 0)
        && is_identifier(&manifest.trigger)
        && is_identifier(&manifest.destination)
        && is_commit_sha(&manifest.commit.sha)
        && is_identifier(&manifest.runner.label)
        && !manifest.reports.is_empty()
        && manifest.reports.len() < MAX_ENTRIES
        && manifest
            .reports
            .iter()
            .all(|name| is_safe_json_name(name) && name != MANIFEST_FILE_NAME);
    if !valid {
        return Err(Rejection::Invalid);
    }
    Ok(manifest)
}

/// Builds the stored payload from a validated manifest and its report files.
///
/// Reports keep allowlisted configuration keys with scalar values and every
/// other top-level finite number as a metric. Strings outside the config
/// allowlist, nested objects and arrays are dropped.
pub(super) fn benchmark_payload(
    manifest: &BenchmarkManifest,
    reports: &BTreeMap<String, Vec<u8>>,
) -> Result<Value, Rejection> {
    let mut stored_reports = Map::new();
    for name in &manifest.reports {
        let bytes = reports.get(name).ok_or(Rejection::Invalid)?;
        let Value::Object(report) =
            serde_json::from_slice(bytes).map_err(|_| Rejection::Invalid)?
        else {
            return Err(Rejection::Invalid);
        };

        let mut config = Map::new();
        let mut metrics = Map::new();
        for (key, value) in report {
            if CONFIG_KEYS.contains(&key.as_str()) {
                let keep = match &value {
                    Value::String(text) => text.len() <= MAX_STRING_LEN,
                    Value::Number(_) | Value::Bool(_) => true,
                    _ => false,
                };
                if keep {
                    config.insert(key, value);
                }
            } else if !DROPPED_METRICS.contains(&key.as_str())
                && value.as_f64().is_some_and(f64::is_finite)
            {
                metrics.insert(key, value);
            }
        }

        let stem = name.strip_suffix(".json").unwrap_or(name);
        stored_reports.insert(stem.to_owned(), json!({ "config": config, "metrics": metrics }));
    }

    Ok(json!({
        "trigger": manifest.trigger,
        "destination": manifest.destination,
        "commit": {
            "sha": manifest.commit.sha,
            "committed_at": manifest
                .commit
                .committed_at
                .map(|at| at.with_timezone(&Utc).to_rfc3339_opts(SecondsFormat::Secs, true)),
        },
        "runner": {
            "label": manifest.runner.label,
            "cpus": manifest.runner.cpus,
            "memory_bytes": manifest.runner.memory_bytes,
        },
        "reports": stored_reports,
    }))
}

/// Parses and validates a test manifest from `repo` run `run_id`, uploaded as
/// the artifact of job `job_id`.
pub(super) fn parse_test_manifest(
    bytes: &[u8],
    repo: &str,
    run_id: i64,
    job_id: i64,
) -> Result<TestManifest, Rejection> {
    let manifest: TestManifest = serde_json::from_slice(bytes).map_err(|_| Rejection::Invalid)?;
    if !manifest.is_supported() {
        return Err(Rejection::Unsupported);
    }
    let github = manifest.github();
    if github.repository() != repo || github.run_id() != run_id || github.job_id() != job_id {
        return Err(Rejection::WrongRun);
    }
    let tests = manifest.tests();
    let mut seen = HashSet::new();
    let valid = github.run_attempt() >= 1
        && tests.len() <= MAX_TESTS
        && tests.iter().all(|test| {
            is_test_name(test.binary())
                && is_test_name(test.name())
                // Stored as `integer`; a larger value would fail the insert.
                && i32::try_from(test.duration_ms()).is_ok()
                && seen.insert((test.binary(), test.name()))
        });
    if !valid {
        return Err(Rejection::Invalid);
    }
    Ok(manifest)
}

/// Returns whether `value` is a plausible nextest binary id or Rust test name.
fn is_test_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_TEST_NAME_LEN
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':' | b'/' | b'.' | b'-')
        })
}

/// Returns whether `value` is a short lowercase identifier.
fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
}

/// Returns whether `value` is a full lowercase hex commit SHA.
fn is_commit_sha(value: &str) -> bool {
    value.len() == 40
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPO: &str = "supabase/etl";
    const RUN_ID: i64 = 40_430_353_543;
    const JOB_ID: i64 = 56_250_883_441;

    fn manifest_json(job_id: Value) -> Value {
        json!({
            "schema_version": 1,
            "kind": "benchmark",
            "trigger": "nightly",
            "destination": "snowflake",
            "commit": { "sha": "ec5e169ae80ccd664826b298791de64a88e9aa61", "committed_at": "2026-10-27T10:04:12+09:00" },
            "workflow_sha": "ec5e169ae80ccd664826b298791de64a88e9aa61",
            "github": {
                "repository": REPO,
                "workflow": "Benchmark",
                "event_name": "schedule",
                "ref": "refs/heads/main",
                "run_id": RUN_ID,
                "run_attempt": 2,
                "run_number": 16,
                "job": "benchmark",
                "job_id": job_id,
            },
            "runner": { "label": "blacksmith-16vcpu-ubuntu-2404", "name": "runner-xyz", "cpus": 16, "memory_bytes": 1 },
            "reports": ["table_copy.json"],
            "created_at": "2026-10-27T05:38:25+00:00",
        })
    }

    fn parse(value: &Value) -> Result<BenchmarkManifest, Rejection> {
        parse_benchmark_manifest(&serde_json::to_vec(value).unwrap(), REPO, RUN_ID)
    }

    #[test]
    fn listing_accepts_flat_json_entries() {
        let listing =
            "Archive:  a.zip\n  Length      Date    Time    Name\n---------  ---------- -----   \
             ----\n     3823  10-27-2026 05:32   table_copy.json\n      914  10-27-2026 05:38   \
             manifest.json\n---------                     -------\n     4737                     \
             2 files\n";
        let entries = parse_archive_listing(listing).unwrap();
        assert_eq!(
            entries,
            vec![
                ArchiveEntry { name: "table_copy.json".to_owned(), declared_size: 3823 },
                ArchiveEntry { name: "manifest.json".to_owned(), declared_size: 914 },
            ]
        );
    }

    #[test]
    fn listing_rejects_unsafe_or_oversized_entries() {
        let entry = |line: &str| {
            format!(
                "  Length      Date    Time    Name\n---------  ---------- -----   ----\n      \
                 914  10-27-2026 05:38   manifest.json\n{line}\n---------                     \
                 -------\n"
            )
        };
        for (line, expected) in [
            ("       10  10-27-2026 05:38   ../evil.json", Rejection::InvalidArchive),
            ("       10  10-27-2026 05:38   dir/report.json", Rejection::InvalidArchive),
            ("       10  10-27-2026 05:38   report.bin", Rejection::InvalidArchive),
            ("       10  10-27-2026 05:38   rep*rt.json", Rejection::InvalidArchive),
            ("       10  10-27-2026 05:38   two words.json", Rejection::InvalidArchive),
            ("       10  10-27-2026 05:38   rapport_é.json", Rejection::InvalidArchive),
            ("  5000000  10-27-2026 05:38   big.json", Rejection::TooLarge),
            ("notanumber  10-27-2026 05:38   report.json", Rejection::InvalidArchive),
            ("      914  10-27-2026 05:38   manifest.json", Rejection::InvalidArchive),
        ] {
            assert_eq!(parse_archive_listing(&entry(line)), Err(expected), "{line}");
        }
    }

    #[test]
    fn listing_requires_manifest_and_structure() {
        assert_eq!(parse_archive_listing(""), Err(Rejection::InvalidArchive));
        assert_eq!(parse_archive_listing("garbage"), Err(Rejection::InvalidArchive));
        let without_manifest =
            "  Length      Date    Time    Name\n---------  ---------- -----   ----\n       10  \
             10-27-2026 05:38   table_copy.json\n---------                     -------\n";
        assert_eq!(parse_archive_listing(without_manifest), Err(Rejection::InvalidArchive));
    }

    #[test]
    fn artifact_kind_follows_name() {
        assert_eq!(artifact_kind("benchmark-results-snowflake"), Some(ArtifactKind::Benchmark));
        assert_eq!(artifact_kind("benchmark-backfill-null"), Some(ArtifactKind::Benchmark));
        assert_eq!(
            artifact_kind("test-results-56250883441"),
            Some(ArtifactKind::Test { job_id: 56_250_883_441 })
        );
        for name in [
            "benchmark-failed-snowflake",
            "fuzz-artifacts-parse_text_cell",
            "test-results-",
            "test-results-0",
            "test-results-+5",
            "test-results-12x",
            "test-results-99999999999999999999",
            "coverage",
        ] {
            assert_eq!(artifact_kind(name), None, "{name}");
        }
    }

    /// A test manifest as `cargo x nextest --test-manifest` writes it.
    fn test_manifest_json() -> Value {
        json!({
            "schema_version": 1,
            "kind": "test",
            "coverage": false,
            "github": { "repository": REPO, "run_id": RUN_ID, "run_attempt": 1, "job_id": JOB_ID },
            "tests": [
                { "binary": "etl::main", "name": "replication::copy", "status": "passed", "duration_ms": 1200 },
                { "binary": "xtask::bin/xtask", "name": "tests::plan", "status": "failed", "duration_ms": 3 },
            ],
        })
    }

    fn parse_tests(value: &Value) -> Result<TestManifest, Rejection> {
        parse_test_manifest(&serde_json::to_vec(value).unwrap(), REPO, RUN_ID, JOB_ID)
    }

    #[test]
    fn tests_manifest_parses_producer_output() {
        let manifest = parse_tests(&test_manifest_json()).unwrap();
        assert!(!manifest.coverage());
        assert_eq!(manifest.github().run_attempt(), 1);
        let tests: Vec<_> = manifest
            .tests()
            .iter()
            .map(|test| (test.binary(), test.name(), test.status().as_str(), test.duration_ms()))
            .collect();
        assert_eq!(
            tests,
            [
                ("etl::main", "replication::copy", "passed", 1200),
                ("xtask::bin/xtask", "tests::plan", "failed", 3),
            ]
        );
    }

    #[test]
    fn tests_manifest_rejects_other_jobs_and_versions() {
        for (pointer, value) in [
            ("/github/job_id", json!(1)),
            ("/github/run_id", json!(1)),
            ("/github/repository", json!("someone/fork")),
        ] {
            let mut manifest = test_manifest_json();
            *manifest.pointer_mut(pointer).unwrap() = value;
            assert_eq!(parse_tests(&manifest).unwrap_err(), Rejection::WrongRun, "{pointer}");
        }
        for (pointer, value) in [("/schema_version", json!(2)), ("/kind", json!("benchmark"))] {
            let mut manifest = test_manifest_json();
            *manifest.pointer_mut(pointer).unwrap() = value;
            assert_eq!(parse_tests(&manifest).unwrap_err(), Rejection::Unsupported, "{pointer}");
        }
    }

    #[test]
    fn tests_manifest_rejects_malformed_results() {
        for (pointer, value) in [
            ("/github/run_attempt", json!(0)),
            ("/tests/0/status", json!("skipped")),
            ("/tests/0/duration_ms", json!(-1)),
            ("/tests/0/duration_ms", json!(1.5)),
            ("/tests/0/duration_ms", json!(3_000_000_000_u64)),
            ("/tests/0/name", json!("")),
            ("/tests/0/name", json!("drop table; --")),
            ("/tests/0/name", json!("x".repeat(257))),
            ("/tests/0/binary", json!("etl main")),
            (
                "/tests/1",
                json!({ "binary": "etl::main", "name": "replication::copy", "status": "failed", "duration_ms": 1 }),
            ),
        ] {
            let mut manifest = test_manifest_json();
            *manifest.pointer_mut(pointer).unwrap() = value;
            assert_eq!(parse_tests(&manifest).unwrap_err(), Rejection::Invalid, "{pointer}");
        }
        assert_eq!(
            parse_test_manifest(b"{\"kind\": \"te", REPO, RUN_ID, JOB_ID).unwrap_err(),
            Rejection::Invalid
        );
    }

    #[test]
    fn commit_time_is_normalized_to_utc_or_rejected() {
        let manifest = parse(&manifest_json(Value::Null)).unwrap();
        let reports = BTreeMap::from([("table_copy.json".to_owned(), b"{}".to_vec())]);
        let payload = benchmark_payload(&manifest, &reports).unwrap();
        assert_eq!(payload["commit"]["committed_at"], json!("2026-10-27T01:04:12Z"));

        let mut missing = manifest_json(Value::Null);
        missing["commit"]["committed_at"] = Value::Null;
        let payload = benchmark_payload(&parse(&missing).unwrap(), &reports).unwrap();
        assert_eq!(payload["commit"]["committed_at"], Value::Null);

        for value in ["yesterday", "2026-10-27", "2026-13-01T00:00:00Z", ""] {
            let mut invalid = manifest_json(Value::Null);
            invalid["commit"]["committed_at"] = json!(value);
            assert_eq!(parse(&invalid).unwrap_err(), Rejection::Invalid, "{value}");
        }
    }

    #[test]
    fn manifest_parses_with_and_without_job_id() {
        let with_id = parse(&manifest_json(json!(122_027_243_622_i64))).unwrap();
        assert_eq!(with_id.job_id(), Some(122_027_243_622));
        assert_eq!(with_id.run_attempt(), 2);

        let without_id = parse(&manifest_json(Value::Null)).unwrap();
        assert_eq!(without_id.job_id(), None);
        assert_eq!(without_id.job_name(), "Benchmark (snowflake)");
    }

    #[test]
    fn manifest_rejects_other_runs_and_versions() {
        let mut other_run = manifest_json(Value::Null);
        other_run["github"]["run_id"] = json!(1);
        assert_eq!(parse(&other_run).unwrap_err(), Rejection::WrongRun);

        let mut other_repo = manifest_json(Value::Null);
        other_repo["github"]["repository"] = json!("someone/fork");
        assert_eq!(parse(&other_repo).unwrap_err(), Rejection::WrongRun);

        let mut newer = manifest_json(Value::Null);
        newer["schema_version"] = json!(2);
        assert_eq!(parse(&newer).unwrap_err(), Rejection::Unsupported);
    }

    #[test]
    fn manifest_rejects_malformed_fields() {
        assert_eq!(parse_benchmark_manifest(b"", REPO, RUN_ID).unwrap_err(), Rejection::Invalid);
        assert_eq!(
            parse_benchmark_manifest(b"{\"kind\": \"bench", REPO, RUN_ID).unwrap_err(),
            Rejection::Invalid
        );
        assert_eq!(
            parse_benchmark_manifest(&[0xff, 0xfe], REPO, RUN_ID).unwrap_err(),
            Rejection::Invalid
        );

        for (pointer, value) in [
            ("/destination", json!("db.example.com:5432")),
            ("/destination", json!("Snowflake")),
            ("/commit/sha", json!("not-a-sha")),
            ("/github/run_attempt", json!(0)),
            ("/github/job_id", json!(-1)),
            ("/reports", json!([])),
            ("/reports", json!(["../x.json"])),
            ("/reports", json!(["manifest.json"])),
        ] {
            let mut manifest = manifest_json(Value::Null);
            *manifest.pointer_mut(pointer).unwrap() = value;
            assert_eq!(parse(&manifest).unwrap_err(), Rejection::Invalid, "{pointer}");
        }

        let mut overflow = manifest_json(Value::Null);
        overflow["github"]["run_attempt"] = json!(u64::MAX);
        assert_eq!(parse(&overflow).unwrap_err(), Rejection::Invalid);
    }

    #[test]
    fn payload_keeps_allowlisted_config_and_numeric_metrics() {
        let manifest = parse(&manifest_json(Value::Null)).unwrap();
        let report = json!({
            "benchmark": "table_copy",
            "destination": "snowflake",
            "table_count": 9,
            "memory_backpressure_enabled": false,
            "rows_per_second": 139_652.5,
            "total_ms": 35_000,
            "pipeline_id": 1_793_078,
            "publication_name": "bench_pub",
            "table_ids": [1, 2],
            "destination_stats": { "account": "placeholder" },
        });
        let reports =
            BTreeMap::from([("table_copy.json".to_owned(), serde_json::to_vec(&report).unwrap())]);

        let payload = benchmark_payload(&manifest, &reports).unwrap();

        assert_eq!(
            payload["reports"]["table_copy"],
            json!({
                "config": { "destination": "snowflake", "table_count": 9, "memory_backpressure_enabled": false },
                "metrics": { "rows_per_second": 139_652.5, "total_ms": 35_000 },
            })
        );
        assert_eq!(
            payload["runner"],
            json!({ "label": "blacksmith-16vcpu-ubuntu-2404", "cpus": 16, "memory_bytes": 1 })
        );
    }

    #[test]
    fn payload_rejects_missing_or_malformed_reports() {
        let manifest = parse(&manifest_json(Value::Null)).unwrap();
        assert_eq!(benchmark_payload(&manifest, &BTreeMap::new()).unwrap_err(), Rejection::Invalid);

        for body in [&b"[]"[..], b"{", b"", &[0xff]] {
            let reports = BTreeMap::from([("table_copy.json".to_owned(), body.to_vec())]);
            assert_eq!(benchmark_payload(&manifest, &reports).unwrap_err(), Rejection::Invalid);
        }
    }
}
