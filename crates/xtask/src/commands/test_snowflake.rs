use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use clap::Args;
use xshell::{Cmd, Shell, cmd};

use crate::utils::{
    CargoFeatureSelection, DEFAULT_BASE_PORT, DefaultFeatureBehavior, DestinationPreset,
    maybe_with_sccache,
};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";

#[derive(Args)]
pub(crate) struct TestSnowflakeArgs {
    /// Run only tests that do not require Snowflake credentials.
    #[arg(long)]
    no_credentials: bool,

    /// Enable sccache for cargo builds (also enabled by ETL_SCCACHE=1).
    #[arg(long)]
    sccache: bool,
}

impl TestSnowflakeArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;

        let sccache = self.sccache;
        let pg_env = PgEnv::from_env();

        check_postgres_ready(&pg_env.host, &pg_env.port, &pg_env.username)?;

        eprintln!(
            "{GREEN}🧪 running Snowflake destination preset tests (no credentials needed).{RESET}"
        );
        let tests = CargoFeatureSelection::for_destination(Some(DestinationPreset::Snowflake))
            .apply_to(cmd!(sh, "cargo nextest run --no-fail-fast"), DefaultFeatureBehavior::All)
            .arg("snowflake");
        with_pg_env(maybe_with_sccache(tests, sccache), &pg_env).run()?;

        if self.no_credentials {
            eprintln!("{YELLOW}⏭ skipping Snowflake credentialed tests (--no-credentials).{RESET}");
            return Ok(());
        }

        let has_account =
            std::env::var("TESTS_SNOWFLAKE_ACCOUNT").ok().filter(|s| !s.is_empty()).is_some();
        let has_user =
            std::env::var("TESTS_SNOWFLAKE_USER").ok().filter(|s| !s.is_empty()).is_some();
        let key_path =
            std::env::var("TESTS_SNOWFLAKE_PRIVATE_KEY_PATH").ok().filter(|s| !s.is_empty());

        if !has_account || !has_user || key_path.is_none() {
            eprintln!(
                "{YELLOW}⏭ skipping Snowflake credentialed tests: one or more of \
                 TESTS_SNOWFLAKE_ACCOUNT, TESTS_SNOWFLAKE_USER, TESTS_SNOWFLAKE_PRIVATE_KEY_PATH \
                 is missing.\n   See .env.example and \
                 crates/etl-destinations/src/snowflake/README.md for setup instructions.{RESET}"
            );
            return Ok(());
        }

        let key_path = key_path.unwrap();
        if std::fs::File::open(&key_path).is_err() {
            bail!(
                "TESTS_SNOWFLAKE_PRIVATE_KEY_PATH={key_path} is not readable. Check that the file \
                 exists and has correct permissions."
            );
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
