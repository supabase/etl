use std::{env, fs, path::Path, process::Command};

use etl_config::Environment;
use etl_telemetry::tracing::{
    LogFlusher, TracingError, init_test_tracing, init_tracing, init_tracing_for_environment,
    init_tracing_with_top_level_fields,
};
use serde_json::Value;
use tempfile::{TempDir, tempdir};

/// Selects the tracing entry point inside a fresh test process.
const INITIALIZER_ENV: &str = "ETL_TRACING_TEST_INITIALIZER";
/// Confirms the requested child test actually ran.
const COMPLETED_MESSAGE: &str = "tracing subprocess completed";
/// Info event used to observe logging output and filtering.
const INFO_MESSAGE: &str = "tracing info event";
/// Debug event used to observe logging output and filtering.
const DEBUG_MESSAGE: &str = "tracing debug event";

/// Launches tracing with environment and working-directory isolation.
fn run_tracing(
    initializer: &str,
    environment: Option<&str>,
    enable_tracing: bool,
    filter: Option<&str>,
) -> (TempDir, String) {
    let directory = tempdir().unwrap();
    // The subscriber, logger, and panic hook are process globals. Configure the
    // child before startup instead of mutating a running test's environment.
    let mut command = Command::new(env::current_exe().unwrap());
    command
        .args(["--exact", "tracing::tracing_subprocess", "--ignored", "--nocapture"])
        .current_dir(directory.path())
        .env(INITIALIZER_ENV, initializer)
        .env_remove("APP_ENVIRONMENT")
        .env_remove("ENABLE_TRACING")
        .env_remove("RUST_LOG");
    if let Some(environment) = environment {
        command.env("APP_ENVIRONMENT", environment);
    }
    if enable_tracing {
        command.env("ENABLE_TRACING", "1");
    }
    if let Some(filter) = filter {
        command.env("RUST_LOG", filter);
    }

    let output = command.output().unwrap();
    assert!(output.status.success(), "Tracing subprocess failed: {output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(COMPLETED_MESSAGE));
    (directory, stdout)
}

/// Reads the JSON events after the child's log flusher has been dropped.
fn read_file_events(directory: &Path) -> Vec<Value> {
    let files: Vec<_> =
        fs::read_dir(directory.join("logs")).unwrap().map(|entry| entry.unwrap().path()).collect();
    assert_eq!(files.len(), 1);
    fs::read_to_string(&files[0])
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Runs one initializer without sharing global tracing state with other tests.
#[test]
#[ignore = "Invoked by parent tests with a controlled process environment"]
fn tracing_subprocess() {
    let environment = env::var_os("APP_ENVIRONMENT");
    let initializer = env::var(INITIALIZER_ENV).unwrap();
    let log_flusher = match initializer.as_str() {
        "test" => {
            init_test_tracing();
            init_test_tracing();
            LogFlusher::NullFlusher
        }
        "development" => init_tracing_for_environment("test", Environment::Dev).unwrap(),
        "production" => init_tracing_for_environment("test", Environment::Prod).unwrap(),
        "configured" => init_tracing("test").unwrap(),
        "fields" => {
            init_tracing_with_top_level_fields("test", Some("test-project"), Some(42)).unwrap()
        }
        "invalid" => {
            assert!(matches!(init_tracing("test"), Err(TracingError::Io(_))));
            assert_eq!(env::var_os("APP_ENVIRONMENT"), environment);
            println!("{COMPLETED_MESSAGE}");
            return;
        }
        _ => panic!("Parent tests select a known tracing initializer"),
    };
    assert_eq!(env::var_os("APP_ENVIRONMENT"), environment);
    tracing::info!("{INFO_MESSAGE}");
    tracing::debug!("{DEBUG_MESSAGE}");
    // File events must survive orderly shutdown, not just reach a queue.
    drop(log_flusher);
    println!("{COMPLETED_MESSAGE}");
}

/// Enabled test tracing writes to the console without changing application
/// mode.
#[test]
fn enabled_tracing_preserves_process_environment() {
    for environment in [None, Some("prod")] {
        let (directory, stdout) = run_tracing("test", environment, true, Some("debug"));
        assert!(stdout.contains(INFO_MESSAGE));
        assert!(stdout.contains(DEBUG_MESSAGE));
        assert!(!directory.path().join("logs").exists());
    }
}

/// The test tracing helper remains inactive unless explicitly enabled.
#[test]
fn disabled_tracing_does_not_initialize_logging() {
    let (directory, stdout) = run_tracing("test", Some("prod"), false, None);
    assert!(!stdout.contains(INFO_MESSAGE));
    assert!(!stdout.contains(DEBUG_MESSAGE));
    assert!(!directory.path().join("logs").exists());
}

/// Explicit development mode does not need a valid process environment.
#[test]
fn explicit_development_ignores_process_environment() {
    let (directory, stdout) = run_tracing("development", Some("invalid"), false, None);
    assert!(stdout.contains(INFO_MESSAGE));
    assert!(!stdout.contains(DEBUG_MESSAGE));
    assert!(!directory.path().join("logs").exists());
}

/// Explicit production mode overrides the process mode and flushes filtered
/// JSON.
#[test]
fn explicit_production_honors_filter_and_flushes_files() {
    let (directory, stdout) = run_tracing("production", Some("dev"), false, Some("debug"));
    assert!(!stdout.contains(INFO_MESSAGE));
    assert!(!stdout.contains(DEBUG_MESSAGE));
    let events = read_file_events(directory.path());
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["level"], "INFO");
    assert_eq!(events[0]["fields"]["message"], INFO_MESSAGE);
    assert_eq!(events[1]["level"], "DEBUG");
    assert_eq!(events[1]["fields"]["message"], DEBUG_MESSAGE);
}

/// Existing initialization keeps development logs on the console.
#[test]
fn configured_development_keeps_console_output() {
    let (directory, stdout) = run_tracing("configured", Some("dev"), false, None);
    assert!(stdout.contains(INFO_MESSAGE));
    assert!(!stdout.contains(DEBUG_MESSAGE));
    assert!(!directory.path().join("logs").exists());
}

/// The default, production, and staging modes keep JSON file output.
#[test]
fn configured_production_modes_keep_file_output() {
    for environment in [None, Some("prod"), Some("staging")] {
        let (directory, stdout) = run_tracing("configured", environment, false, None);
        assert!(!stdout.contains(INFO_MESSAGE));
        assert!(!stdout.contains(DEBUG_MESSAGE));
        let events = read_file_events(directory.path());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["fields"]["message"], INFO_MESSAGE);
    }
}

/// Production logging retains the project and pipeline fields supplied by
/// callers.
#[test]
fn configured_tracing_keeps_top_level_fields() {
    let (directory, _) = run_tracing("fields", Some("prod"), false, None);
    let events = read_file_events(directory.path());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["project"], "test-project");
    assert_eq!(events[0]["pipeline_id"], 42);
}

/// Existing initialization still rejects unsupported environment identifiers.
#[test]
fn configured_tracing_rejects_invalid_environment() {
    let (directory, _) = run_tracing("invalid", Some("invalid"), false, None);
    assert!(!directory.path().join("logs").exists());
}
