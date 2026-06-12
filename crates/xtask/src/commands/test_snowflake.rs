use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use xshell::{Cmd, Shell, cmd};

use crate::utils::{
    CargoFeatureSelection, DEFAULT_BASE_PORT, DefaultFeatureBehavior, DestinationPreset,
    maybe_with_sccache,
};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";
const SNOWFLAKE_CONNECTION_ENV: &str = "TESTS_SNOWFLAKE_CONNECTION";

#[derive(Args)]
pub(crate) struct TestSnowflakeArgs {
    /// How to handle Snowflake credentialed tests.
    #[arg(long, value_enum, default_value = "auto")]
    credentials: CredentialMode,

    /// Enable sccache for cargo builds (also enabled by ETL_SCCACHE=1).
    #[arg(long)]
    sccache: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CredentialMode {
    /// Run credentialed tests when credentials are available.
    Auto,
    /// Require credentials and fail if they are missing.
    Required,
    /// Skip credentialed tests even when credentials are available.
    Skip,
}

impl TestSnowflakeArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;

        let sccache = self.sccache;
        let credential_mode = self.credentials;
        let pg_env = PgEnv::from_env();
        let has_snowflake_connection =
            !matches!(credential_mode, CredentialMode::Skip) && has_snowflake_connection_env();

        if matches!(credential_mode, CredentialMode::Required) && !has_snowflake_connection {
            bail!("Snowflake credentialed tests require {SNOWFLAKE_CONNECTION_ENV}.");
        }

        check_postgres_ready(&pg_env.host, &pg_env.port, &pg_env.username)?;

        eprintln!(
            "{GREEN}🧪 running Snowflake destination preset tests (no credentials needed).{RESET}"
        );
        let tests = CargoFeatureSelection::for_destination(Some(DestinationPreset::Snowflake))
            .apply_to(cmd!(sh, "cargo nextest run --no-fail-fast"), DefaultFeatureBehavior::All)
            .arg("snowflake");
        with_pg_env(maybe_with_sccache(tests, sccache), &pg_env).run()?;

        if matches!(credential_mode, CredentialMode::Skip) {
            eprintln!(
                "{YELLOW}⏭ skipping Snowflake credentialed tests (--credentials skip).{RESET}"
            );
            return Ok(());
        }

        if !has_snowflake_connection {
            eprintln!(
                "{YELLOW}⏭ skipping Snowflake credentialed tests: Snowflake credentials are \
                 missing.\n   Set {SNOWFLAKE_CONNECTION_ENV}, or see .env.example and \
                 crates/etl-destinations/src/snowflake/README.md for setup instructions.{RESET}"
            );
            return Ok(());
        }

        eprintln!("{GREEN}🔑 running Snowflake API validator integration tests.{RESET}");
        let tests = CargoFeatureSelection::new(
            true,
            vec!["snowflake".to_owned()],
            vec!["etl-api".to_owned()],
        )
        .apply_to(
            cmd!(sh, "cargo nextest run --no-fail-fast --run-ignored only"),
            DefaultFeatureBehavior::CargoDefault,
        )
        .arg("snowflake");
        with_pg_env(maybe_with_sccache(tests, sccache), &pg_env).run()?;

        eprintln!("{GREEN}❄️  running Snowflake destination integration tests.{RESET}");
        let tests = CargoFeatureSelection::new(
            true,
            vec!["snowflake".to_owned(), "test-utils".to_owned()],
            vec!["etl-destinations".to_owned()],
        )
        .apply_to(
            cmd!(sh, "cargo nextest run --no-fail-fast --run-ignored only"),
            DefaultFeatureBehavior::CargoDefault,
        )
        .arg("snowflake");
        with_pg_env(maybe_with_sccache(tests, sccache), &pg_env).run()?;

        Ok(())
    }
}

struct PgEnv {
    host: String,
    port: String,
    username: String,
    password: String,
}

impl PgEnv {
    fn from_env() -> Self {
        Self {
            host: env_or("TESTS_DATABASE_HOST", "localhost"),
            port: env_or("TESTS_DATABASE_PORT", &DEFAULT_BASE_PORT.to_string()),
            username: env_or("TESTS_DATABASE_USERNAME", "postgres"),
            password: env_or("TESTS_DATABASE_PASSWORD", "postgres"),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn with_pg_env<'a>(cmd: Cmd<'a>, pg_env: &PgEnv) -> Cmd<'a> {
    cmd.env("TESTS_DATABASE_HOST", &pg_env.host)
        .env("TESTS_DATABASE_PORT", &pg_env.port)
        .env("TESTS_DATABASE_USERNAME", &pg_env.username)
        .env("TESTS_DATABASE_PASSWORD", &pg_env.password)
}

fn has_snowflake_connection_env() -> bool {
    std::env::var(SNOWFLAKE_CONNECTION_ENV).is_ok_and(|value| !value.trim().is_empty())
}

fn check_postgres_ready(host: &str, port: &str, user: &str) -> Result<()> {
    let status = Command::new("pg_isready")
        .args(["-h", host, "-p", port, "-U", user])
        .stdout(Stdio::null())
        .status()
        .context("failed to run pg_isready -- is it installed?")?;

    if !status.success() {
        bail!("local Postgres is not ready on {host}:{port}; run \"cargo x init\" first.");
    }

    Ok(())
}
