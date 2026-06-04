use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use clap::Args;
use xshell::{Shell, cmd};

use crate::utils::{DEFAULT_BASE_PORT, maybe_with_sccache};

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
        let db_host = env_or("TESTS_DATABASE_HOST", "localhost");
        let db_port = env_or("TESTS_DATABASE_PORT", &DEFAULT_BASE_PORT.to_string());
        let db_user = env_or("TESTS_DATABASE_USERNAME", "postgres");
        let db_pass = env_or("TESTS_DATABASE_PASSWORD", "postgres");

        check_postgres_ready(&db_host, &db_port, &db_user)?;

        eprintln!("{GREEN}🧪 running Snowflake API tests (no credentials needed).{RESET}");
        maybe_with_sccache(cmd!(sh, "cargo test -p etl-api -- snowflake"), sccache)
            .env("TESTS_DATABASE_HOST", &db_host)
            .env("TESTS_DATABASE_PORT", &db_port)
            .env("TESTS_DATABASE_USERNAME", &db_user)
            .env("TESTS_DATABASE_PASSWORD", &db_pass)
            .run()?;

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
        maybe_with_sccache(
            cmd!(sh, "cargo test -p etl-api -- snowflake --ignored --nocapture --test-threads=1"),
            sccache,
        )
        .env("TESTS_DATABASE_HOST", &db_host)
        .env("TESTS_DATABASE_PORT", &db_port)
        .env("TESTS_DATABASE_USERNAME", &db_user)
        .env("TESTS_DATABASE_PASSWORD", &db_pass)
        .run()?;

        eprintln!("{GREEN}❄️  running Snowflake destination integration tests.{RESET}");
        maybe_with_sccache(
            cmd!(sh, "cargo test -p etl-destinations --features snowflake,test-utils --test main"),
            sccache,
        )
        .args(["--", "snowflake", "--ignored", "--nocapture", "--test-threads=1"])
        .env("TESTS_DATABASE_HOST", &db_host)
        .env("TESTS_DATABASE_PORT", &db_port)
        .env("TESTS_DATABASE_USERNAME", &db_user)
        .env("TESTS_DATABASE_PASSWORD", &db_pass)
        .run()?;

        Ok(())
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
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
