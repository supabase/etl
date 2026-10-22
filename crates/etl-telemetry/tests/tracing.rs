use std::{env, process::Command};

use etl_telemetry::tracing::init_test_tracing;

/// Log message that confirms the child ran and initialized console tracing.
const INFO_MESSAGE: &str = "tracing info event";

/// Exercises test tracing in a process whose environment is set before startup.
#[test]
#[ignore = "Invoked by the parent test with a controlled process environment"]
fn tracing_subprocess() {
    let environment = env::var_os("APP_ENVIRONMENT");
    init_test_tracing();
    init_test_tracing();
    assert_eq!(env::var_os("APP_ENVIRONMENT"), environment);
    tracing::info!("{INFO_MESSAGE}");
    tracing::debug!(target: "etl::replication", "safe application debug event");
    tracing::warn!(target: "reqsign::aws", "safe dependency warning");
    tracing::debug!(target: "reqsign::aws", "placeholder-signing-secret");
    tracing::trace!(target: "tokio_postgres::query", "placeholder-sql-parameter");
    tracing_log::log::debug!(target: "yup_oauth2::authenticator", "placeholder-oauth-token");
    tracing_log::log::error!(target: "configcat::fetch::fetcher", "placeholder-sdk-key");
}

/// Enabled test tracing writes to the console without changing application
/// mode.
#[test]
fn enabled_tracing_preserves_process_environment() {
    for environment in [None, Some("prod")] {
        // Configure the child before startup so the test does not mutate its
        // own process environment or share global tracing state.
        let mut command = Command::new(env::current_exe().unwrap());
        command
            .args(["--exact", "tracing::tracing_subprocess", "--ignored", "--nocapture"])
            .env("ENABLE_TRACING", "1")
            .env("RUST_LOG", "info")
            .env_remove("APP_ENVIRONMENT");
        if let Some(environment) = environment {
            command.env("APP_ENVIRONMENT", environment);
        }

        let output = command.output().unwrap();
        assert!(output.status.success(), "Tracing subprocess failed: {output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains(INFO_MESSAGE));
    }
}

/// Explicit nested directives cannot enable sensitive dependency diagnostics.
#[test]
fn verbose_tracing_omits_credentials_and_sql_parameters() {
    let output = Command::new(env::current_exe().unwrap())
        .args(["--exact", "tracing::tracing_subprocess", "--ignored", "--nocapture"])
        .env("ENABLE_TRACING", "1")
        .env(
            "RUST_LOG",
            "trace,reqsign::aws=trace,tokio_postgres::query=trace,yup_oauth2::authenticator=trace",
        )
        .output()
        .unwrap();

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(stdout.contains("safe application debug event"));
    assert!(stdout.contains("safe dependency warning"));

    for secret in [
        "placeholder-signing-secret",
        "placeholder-sql-parameter",
        "placeholder-oauth-token",
        "placeholder-sdk-key",
    ] {
        assert!(!stdout.contains(secret));
    }
}
