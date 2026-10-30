//! Per-test results that CI uploads after `cargo x nextest` and the CI metrics
//! collector ingests.
//!
//! The producer converts nextest's JUnit reports to this JSON format, so the
//! collector reads JSON only. Coverage runs are flagged because instrumented
//! binaries run slower and their durations are not comparable.

use std::{collections::HashSet, env};

use anyhow::{Context, Result, bail};
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use serde::{Deserialize, Serialize};

/// Artifact name prefix. The suffix is the producing job's id.
pub(crate) const ARTIFACT_PREFIX: &str = "test-results-";
/// Manifest kind.
pub(crate) const KIND: &str = "test";
/// Manifest schema version.
pub(crate) const SCHEMA_VERSION: u32 = 1;

/// Results of every test one CI job ran.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TestManifest {
    schema_version: u32,
    kind: String,
    /// Whether the tests ran instrumented for coverage.
    coverage: bool,
    github: TestJob,
    tests: Vec<TestCase>,
}

impl TestManifest {
    /// Builds a manifest of the current format.
    pub(crate) fn new(coverage: bool, github: TestJob, tests: Vec<TestCase>) -> Self {
        Self { schema_version: SCHEMA_VERSION, kind: KIND.to_owned(), coverage, github, tests }
    }

    /// Returns whether the manifest has a supported kind and schema version.
    pub(crate) fn is_supported(&self) -> bool {
        self.kind == KIND && self.schema_version == SCHEMA_VERSION
    }

    /// Returns whether the tests ran instrumented for coverage.
    pub(crate) fn coverage(&self) -> bool {
        self.coverage
    }

    /// Returns the job that ran the tests.
    pub(crate) fn github(&self) -> &TestJob {
        &self.github
    }

    /// Returns the test results.
    pub(crate) fn tests(&self) -> &[TestCase] {
        &self.tests
    }

    /// Returns the test results, consuming the manifest.
    pub(crate) fn into_tests(self) -> Vec<TestCase> {
        self.tests
    }
}

/// GitHub identity of the job that ran the tests.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TestJob {
    repository: String,
    run_id: i64,
    run_attempt: i32,
    /// The jobs API `id`, from the workflow's `job.check_run_id`.
    job_id: i64,
}

impl TestJob {
    /// Reads the job identity from the GitHub Actions environment.
    /// `JOB_CHECK_RUN_ID` must be set from `job.check_run_id` by the workflow,
    /// because GitHub exports no job id.
    pub(crate) fn from_env() -> Result<Self> {
        fn read<T: std::str::FromStr>(name: &str) -> Result<T> {
            env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .with_context(|| format!("{name} is missing or invalid"))
        }
        Ok(Self {
            repository: read("GITHUB_REPOSITORY")?,
            run_id: read("GITHUB_RUN_ID")?,
            run_attempt: read("GITHUB_RUN_ATTEMPT")?,
            job_id: read("JOB_CHECK_RUN_ID")?,
        })
    }

    /// Returns the `owner/name` repository.
    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }

    /// Returns the workflow run id.
    pub(crate) fn run_id(&self) -> i64 {
        self.run_id
    }

    /// Returns the run attempt.
    pub(crate) fn run_attempt(&self) -> i32 {
        self.run_attempt
    }

    /// Returns the job id.
    pub(crate) fn job_id(&self) -> i64 {
        self.job_id
    }
}

/// One test's result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TestCase {
    /// Nextest binary id, such as `etl-destinations::main`.
    binary: String,
    name: String,
    status: TestStatus,
    duration_ms: u32,
}

impl TestCase {
    /// Returns the nextest binary id.
    pub(crate) fn binary(&self) -> &str {
        &self.binary
    }

    /// Returns the test name within its binary.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the outcome.
    pub(crate) fn status(&self) -> TestStatus {
        self.status
    }

    /// Returns the duration in milliseconds.
    pub(crate) fn duration_ms(&self) -> u32 {
        self.duration_ms
    }
}

/// Outcome of a test that ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TestStatus {
    /// The test passed, possibly after retries.
    Passed,
    /// The test failed, timed out, or could not run.
    Failed,
}

impl TestStatus {
    /// Returns the stored status name.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

/// A `<testcase>` element read so far.
struct OpenCase {
    case: TestCase,
    skipped: bool,
}

/// Reads the tests of a nextest JUnit report. Skipped tests are left out.
pub(crate) fn parse_junit(xml: &str) -> Result<Vec<TestCase>> {
    let mut reader = Reader::from_str(xml);
    let mut cases = Vec::new();
    let mut open: Option<OpenCase> = None;
    loop {
        match reader.read_event().context("Malformed JUnit report")? {
            Event::Start(element) if element.name().as_ref() == b"testcase" => {
                open = Some(OpenCase { case: read_case(&element)?, skipped: false });
            }
            Event::Empty(element) if element.name().as_ref() == b"testcase" => {
                cases.push(read_case(&element)?);
            }
            Event::Start(element) | Event::Empty(element) => {
                if let Some(open) = open.as_mut() {
                    match element.name().as_ref() {
                        b"failure" | b"error" => open.case.status = TestStatus::Failed,
                        b"skipped" => open.skipped = true,
                        _ => {}
                    }
                }
            }
            Event::End(element) if element.name().as_ref() == b"testcase" => {
                let open = open.take().context("Unbalanced testcase element in JUnit report")?;
                if !open.skipped {
                    cases.push(open.case);
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if open.is_some() {
        bail!("Unterminated testcase element in JUnit report");
    }

    let mut seen = HashSet::new();
    if let Some(case) = cases.iter().find(|case| !seen.insert((&case.binary, &case.name))) {
        bail!("JUnit report lists test {} {} twice", case.binary, case.name);
    }
    Ok(cases)
}

/// Reads a `<testcase>` element's attributes as a passing test.
fn read_case(element: &BytesStart<'_>) -> Result<TestCase> {
    let attribute = |name: &str| -> Result<String> {
        let value = element
            .try_get_attribute(name)
            .context("Malformed testcase attribute in JUnit report")?
            .with_context(|| format!("JUnit testcase has no {name} attribute"))?;
        Ok(value.unescape_value().context("Malformed testcase attribute in JUnit report")?.into())
    };
    let seconds: f64 = attribute("time")?.parse().context("Invalid JUnit testcase time")?;
    let millis = (seconds * 1000.0).round();
    if !(0.0..=f64::from(u32::MAX)).contains(&millis) {
        bail!("JUnit testcase time {seconds} is out of range");
    }
    Ok(TestCase {
        binary: attribute("classname")?,
        name: attribute("name")?,
        status: TestStatus::Passed,
        // Range-checked above; the cast only drops the zero fraction.
        duration_ms: millis as u32,
    })
}

#[cfg(test)]
mod tests {
    use crate::test_manifest::{TestCase, TestStatus, parse_junit};

    fn case(binary: &str, name: &str, status: TestStatus, duration_ms: u32) -> TestCase {
        TestCase { binary: binary.to_owned(), name: name.to_owned(), status, duration_ms }
    }

    /// Mirrors nextest 0.9 output with failure output not stored.
    #[test]
    fn junit_reads_outcomes_and_durations() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="5" failures="2" errors="0">
    <testsuite name="etl::main" tests="5" disabled="0" errors="0" failures="2">
        <testcase name="replication::copy_ok" classname="etl::main" timestamp="2026-10-30T00:00:00Z" time="1.2346">
        </testcase>
        <testcase name="replication::copy_fails" classname="etl::main" time="0.500">
            <failure type="test failure with exit code 101"/>
        </testcase>
        <testcase name="replication::times_out" classname="etl::main" time="60.000">
            <failure type="test timeout">timed out</failure>
        </testcase>
        <testcase name="replication::flaky" classname="etl::main" time="0.001">
            <flakyFailure type="test failure"/>
        </testcase>
        <testcase name="replication::ignored" classname="etl::main" time="0.000">
            <skipped/>
        </testcase>
        <testcase name="a&amp;b" classname="xtask::bin/xtask" time="0"/>
    </testsuite>
</testsuites>"#;

        assert_eq!(
            parse_junit(xml).unwrap(),
            vec![
                case("etl::main", "replication::copy_ok", TestStatus::Passed, 1235),
                case("etl::main", "replication::copy_fails", TestStatus::Failed, 500),
                case("etl::main", "replication::times_out", TestStatus::Failed, 60_000),
                case("etl::main", "replication::flaky", TestStatus::Passed, 1),
                case("xtask::bin/xtask", "a&b", TestStatus::Passed, 0),
            ]
        );
    }

    #[test]
    fn junit_rejects_malformed_reports() {
        for xml in [
            r#"<testsuites><testcase name="a" classname="b" time="1">"#,
            r#"<testsuites><testcase name="a" classname="b"/></testsuites>"#,
            r#"<testsuites><testcase name="a" classname="b" time="-1"/></testsuites>"#,
            r#"<testsuites><testcase name="a" classname="b" time="NaN"/></testsuites>"#,
            r#"<testsuites><testcase name="a" classname="b" time="1e12"/></testsuites>"#,
            r#"<testsuites><testcase name="a" classname="b" time="1"/><testcase name="a" classname="b" time="2"/></testsuites>"#,
            r#"<testsuites><testcase name="a" classname="b" time="1"></testsuites>"#,
            r#"<testsuites><testcase name="a&bogus;" classname="b" time="1"/></testsuites>"#,
        ] {
            assert!(parse_junit(xml).is_err(), "{xml}");
        }
    }
}
