use std::{
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

use super::shared::{DEFAULT_BASE_PORT, DEFAULT_PG_SHARD_COUNT};

const COMPOSE_FILE: &str = "./scripts/docker-compose.yaml";

/// Returns the program and initial args for docker compose.
/// Prefers `docker compose` (v2 plugin) and falls back to `docker-compose`
/// (standalone).
fn docker_compose_command() -> (&'static str, &'static [&'static str]) {
    if Command::new("docker")
        .args(["compose", "version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
    {
        ("docker", &["compose"])
    } else {
        ("docker-compose", &[])
    }
}
const DEFAULT_PG_VERSION: &str = "18";
const DEFAULT_PG_USER: &str = "postgres";
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const TEST_SAFE_SLOT_WAL_KEEP_SIZE: &str = "ALTER SYSTEM SET max_slot_wal_keep_size = '-1'";
const TEST_SAFE_IDLE_REPLICATION_SLOT_TIMEOUT: &str =
    "ALTER SYSTEM SET idle_replication_slot_timeout = '0'";

#[derive(Args)]
pub(crate) struct PostgresArgs {
    #[command(subcommand)]
    command: PostgresCommand,
}

#[derive(Subcommand)]
enum PostgresCommand {
    /// Start sharded Postgres clusters for testing.
    Start(StartArgs),
}

#[derive(Args)]
struct StartArgs {
    /// Postgres image tag.
    #[arg(long, env = "POSTGRES_VERSION", default_value = DEFAULT_PG_VERSION)]
    pg_version: String,

    /// Number of Postgres clusters to start.
    #[arg(long, env = "NUM_LOCAL_DATABASES", default_value_t = DEFAULT_PG_SHARD_COUNT)]
    shards: u16,

    /// Base port for the first cluster. Additional clusters use consecutive
    /// ports.
    #[arg(long, env = "TESTS_DATABASE_START_PORT", default_value_t = DEFAULT_BASE_PORT)]
    base_port: u16,

    /// Postgres user for health checks.
    #[arg(long, env = "POSTGRES_USER", default_value = DEFAULT_PG_USER)]
    pg_user: String,
}

impl PostgresArgs {
    pub(crate) fn run(self) -> Result<()> {
        match self.command {
            PostgresCommand::Start(args) => args.run(),
        }
    }
}

impl StartArgs {
    fn run(self) -> Result<()> {
        if self.shards == 0 {
            bail!("--shards must be at least 1");
        }

        if self.base_port.checked_add(self.shards - 1).is_none() {
            bail!("--base-port + --shards exceeds the valid port range");
        }

        eprintln!(
            "starting {} Postgres {} clusters on ports {}..{}.",
            self.shards,
            self.pg_version,
            self.base_port,
            self.base_port + self.shards - 1,
        );

        // Start the full stack (Postgres, Lakekeeper, MinIO, etc.) on the base port.
        self.docker_compose_up(&self.pg_version, None, &[])?;

        // Start additional source-postgres containers on subsequent ports.
        for shard in 2..=self.shards {
            let port = self.base_port + shard - 1;
            let project = format!("etl-stack-pg-{}-shard-{shard}", self.pg_version);
            self.docker_compose_up(&self.pg_version, Some((&project, port)), &["source-postgres"])?;
        }

        // Wait for all clusters to accept connections.
        for shard in 1..=self.shards {
            let port = self.base_port + shard - 1;
            self.wait_for_pg(port)?;
            self.configure_pg_for_tests(port, &self.pg_version)?;
        }

        Ok(())
    }

    fn docker_compose_up(
        &self,
        pg_version: &str,
        project_and_port: Option<(&str, u16)>,
        services: &[&str],
    ) -> Result<()> {
        let postgres_image =
            std::env::var("POSTGRES_IMAGE").unwrap_or_else(|_| format!("postgres:{pg_version}"));
        let (program, compose_args) = docker_compose_command();
        let mut cmd = Command::new(program);
        cmd.args(compose_args);
        cmd.args(["-f", COMPOSE_FILE]);
        cmd.env("POSTGRES_VERSION", pg_version);
        cmd.env("POSTGRES_IMAGE", &postgres_image);

        if let Some((project, port)) = project_and_port {
            cmd.args(["-p", project]);
            cmd.env("POSTGRES_PORT", port.to_string());
        }

        cmd.args(["up", "-d"]);
        cmd.args(services);

        let status = cmd.status().context("failed to run docker compose")?;

        if !status.success() {
            bail!("docker compose up failed");
        }

        Ok(())
    }

    fn wait_for_pg(&self, port: u16) -> Result<()> {
        loop {
            let status = Command::new("pg_isready")
                .args(["-h", "localhost", "-p", &port.to_string(), "-U", &self.pg_user])
                .status()
                .context("failed to run pg_isready")?;

            if status.success() {
                return Ok(());
            }

            thread::sleep(HEALTH_CHECK_INTERVAL);
        }
    }

    fn configure_pg_for_tests(&self, port: u16, pg_version: &str) -> Result<()> {
        let mut statements = vec![TEST_SAFE_SLOT_WAL_KEEP_SIZE];
        if supports_idle_replication_slot_timeout(pg_version) {
            statements.push(TEST_SAFE_IDLE_REPLICATION_SLOT_TIMEOUT);
        }

        for statement in statements {
            let status = Command::new("psql")
                .args([
                    "-h",
                    "localhost",
                    "-p",
                    &port.to_string(),
                    "-U",
                    &self.pg_user,
                    "-d",
                    "postgres",
                    "-c",
                    statement,
                ])
                .env(
                    "PGPASSWORD",
                    std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_owned()),
                )
                .status()
                .context("failed to configure Postgres test setting")?;

            if !status.success() {
                bail!("failed to apply Postgres test setting on port {port}: {statement}");
            }
        }

        let status = Command::new("psql")
            .args([
                "-h",
                "localhost",
                "-p",
                &port.to_string(),
                "-U",
                &self.pg_user,
                "-d",
                "postgres",
                "-c",
                "SELECT pg_reload_conf()",
            ])
            .env(
                "PGPASSWORD",
                std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "postgres".to_owned()),
            )
            .status()
            .context("failed to reload Postgres config")?;

        if !status.success() {
            bail!("failed to reload Postgres config on port {port}");
        }

        Ok(())
    }
}

fn supports_idle_replication_slot_timeout(pg_version: &str) -> bool {
    pg_version.parse::<u16>().is_ok_and(|major| major >= 18)
}
