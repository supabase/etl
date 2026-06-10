use std::{thread, time::Duration};

use anyhow::{Result, bail};
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct TestClickhouseArgs {
    #[arg(long, default_value = "./scripts/docker-compose.yaml")]
    compose_file: String,

    #[arg(long, default_value = "docker-compose")]
    docker_compose_bin: String,

    #[arg(long, env = "SKIP_DOCKER")]
    skip_docker: bool,

    #[arg(long, default_value = "source-postgres")]
    postgres_service: String,

    #[arg(long, default_value = "clickhouse")]
    clickhouse_service: String,

    #[arg(long, default_value_t = 5430)]
    postgres_port: u16,

    #[arg(long, default_value = "postgres")]
    postgres_user: String,

    #[arg(long, default_value = "postgres")]
    postgres_password: String,

    #[arg(long, default_value_t = 8123)]
    clickhouse_http_port: u16,

    #[arg(long, default_value = "etl")]
    clickhouse_user: String,

    #[arg(long, default_value = "etl")]
    clickhouse_password: String,

    #[arg(long)]
    toolchain: Option<String>,

    #[arg(default_value = "clickhouse")]
    filter: String,
}

impl TestClickhouseArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;

        if !self.skip_docker {
            println!("🐳 Starting local Postgres and ClickHouse services...");
            let docker_compose = &self.docker_compose_bin;
            let compose_file = &self.compose_file;
            let pg_service = &self.postgres_service;
            let ch_service = &self.clickhouse_service;

            cmd!(sh, "{docker_compose} -f {compose_file} up -d {pg_service} {ch_service}").run()?;

            println!("⏳ Waiting for Postgres to be ready...");
            let pg_user = &self.postgres_user;
            let pg_check =
                ["exec", "-T", pg_service.as_str(), "pg_isready", "-U", pg_user.as_str()];
            let mut pg_ready = false;
            for _ in 0..60 {
                if cmd!(sh, "{docker_compose} -f {compose_file} {pg_check...}")
                    .quiet()
                    .run()
                    .is_ok()
                {
                    pg_ready = true;
                    break;
                }
                thread::sleep(Duration::from_secs(1));
            }
            if !pg_ready {
                bail!("Postgres failed to become ready");
            }

            println!("⏳ Waiting for ClickHouse to be ready...");
            let ch_user = &self.clickhouse_user;
            let ch_pass = &self.clickhouse_password;
            let ch_check = [
                "exec",
                "-T",
                ch_service.as_str(),
                "clickhouse-client",
                "--user",
                ch_user.as_str(),
                "--password",
                ch_pass.as_str(),
                "--query",
                "SELECT 1",
            ];
            let mut ch_ready = false;
            for _ in 0..60 {
                if cmd!(sh, "{docker_compose} -f {compose_file} {ch_check...}")
                    .quiet()
                    .run()
                    .is_ok()
                {
                    ch_ready = true;
                    break;
                }
                thread::sleep(Duration::from_secs(1));
            }
            if !ch_ready {
                bail!("ClickHouse failed to become ready");
            }
        }

        // Set environment variables for tests, respecting existing ones.
        if sh.var("TESTS_DATABASE_HOST").is_err() {
            sh.set_var("TESTS_DATABASE_HOST", "localhost");
        }
        if sh.var("TESTS_DATABASE_PORT").is_err() {
            sh.set_var("TESTS_DATABASE_PORT", self.postgres_port.to_string());
        }
        if sh.var("TESTS_DATABASE_USERNAME").is_err() {
            sh.set_var("TESTS_DATABASE_USERNAME", &self.postgres_user);
        }
        if sh.var("TESTS_DATABASE_PASSWORD").is_err() {
            sh.set_var("TESTS_DATABASE_PASSWORD", &self.postgres_password);
        }
        if sh.var("TESTS_CLICKHOUSE_URL").is_err() {
            sh.set_var(
                "TESTS_CLICKHOUSE_URL",
                format!("http://localhost:{}", self.clickhouse_http_port),
            );
        }
        if sh.var("TESTS_CLICKHOUSE_USER").is_err() {
            sh.set_var("TESTS_CLICKHOUSE_USER", &self.clickhouse_user);
        }
        if sh.var("TESTS_CLICKHOUSE_PASSWORD").is_err() {
            sh.set_var("TESTS_CLICKHOUSE_PASSWORD", &self.clickhouse_password);
        }

        println!("🧪 Running ClickHouse destination tests...");
        let filter = &self.filter;

        let mut cargo_args = vec![
            "test",
            "-p",
            "etl-destinations",
            "--features",
            "clickhouse,test-utils",
            "--test",
            "main",
        ];
        if !filter.is_empty() {
            cargo_args.push(filter);
        }
        cargo_args.extend(["--", "--nocapture"]);

        if let Some(tc) = &self.toolchain {
            let tc_arg = format!("+{tc}");
            cmd!(sh, "cargo {tc_arg} {cargo_args...}").run()?;
        } else {
            cmd!(sh, "cargo {cargo_args...}").run()?;
        }

        Ok(())
    }
}
