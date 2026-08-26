//! Local Multigres cluster management.

use std::{
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

/// Docker Compose file for the local Multigres cluster.
const COMPOSE_FILE: &str = "./scripts/docker/docker-compose-multigres.yaml";
/// Default number of Multigres cells used for graceful switchover testing.
const DEFAULT_CELLS: u16 = 2;
/// Default PostgreSQL wire-protocol port exposed by the multigateway.
const DEFAULT_GATEWAY_PORT: u16 = 15_432;
/// Interval between multigateway readiness checks.
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Maximum time to wait for the Multigres cluster to become ready.
const MULTIGRES_READY_TIMEOUT: Duration = Duration::from_secs(300);
/// Maximum number of fresh-cluster starts attempted by a test command.
const TEST_CLUSTER_START_ATTEMPTS: u8 = 2;

/// Arguments for managing the local Multigres cluster.
#[derive(Args)]
pub(crate) struct MultigresArgs {
    /// Multigres lifecycle command to run.
    #[command(subcommand)]
    command: MultigresCommand,
}

/// Supported Multigres lifecycle commands.
#[derive(Subcommand)]
enum MultigresCommand {
    /// Pull and start a local Multigres cluster.
    Start(StartArgs),
    /// Run the isolated Multigres tests.
    Test(TestArgs),
}

/// Arguments for starting the local Multigres cluster.
#[derive(Args)]
struct StartArgs {
    /// Number of cells to start.
    #[arg(long, env = "MULTIGRES_NUM_CELLS", default_value_t = DEFAULT_CELLS)]
    cells: u16,
    /// Host port for the multigateway PostgreSQL endpoint.
    #[arg(long, env = "MULTIGRES_GATEWAY_PORT", default_value_t = DEFAULT_GATEWAY_PORT)]
    gateway_port: u16,
}

/// Arguments for running the isolated Multigres test.
#[derive(Args)]
struct TestArgs {
    /// Host port for the multigateway PostgreSQL endpoint.
    #[arg(long, env = "MULTIGRES_GATEWAY_PORT", default_value_t = DEFAULT_GATEWAY_PORT)]
    gateway_port: u16,
}

/// Resolved Docker Compose command.
struct DockerCompose {
    /// Compose program name.
    program: &'static str,
    /// Arguments preceding the Compose subcommand.
    args: &'static [&'static str],
}

impl MultigresArgs {
    /// Runs the requested Multigres lifecycle command.
    pub(crate) fn run(self) -> Result<()> {
        match self.command {
            MultigresCommand::Start(args) => args.run(),
            MultigresCommand::Test(args) => args.run(),
        }
    }
}

impl StartArgs {
    /// Pulls, starts, and waits for a local Multigres cluster.
    fn run(self) -> Result<()> {
        if !(2..=3).contains(&self.cells) {
            bail!("The --cells value must be between 2 and 3");
        }

        let compose = DockerCompose::detect()?;
        eprintln!(
            "starting a {}-cell Multigres cluster with its gateway on 127.0.0.1:{}.",
            self.cells, self.gateway_port
        );

        let mut pull = compose.command(&self);
        pull.arg("pull");
        run_command(pull, "Failed to pull the Multigres image")?;

        let mut up = compose.command(&self);
        up.args(["up", "-d"]);
        run_command(up, "Failed to start the Multigres cluster")?;

        self.wait_for_gateway(&compose)?;
        eprintln!(
            "Multigres is ready. Tests that support its fixed database can use \
             TESTS_DATABASE_HOST=127.0.0.1 TESTS_DATABASE_PORT={} \
             TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres.",
            self.gateway_port
        );

        Ok(())
    }

    /// Waits for the multigateway to accept PostgreSQL connections.
    fn wait_for_gateway(&self, compose: &DockerCompose) -> Result<()> {
        let started_at = Instant::now();

        loop {
            let mut readiness = compose.command(self);
            readiness.args([
                "exec",
                "-T",
                "multigres",
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-p",
                "15432",
                "-U",
                "postgres",
                "-d",
                "postgres",
            ]);
            let status = readiness
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .context("Failed to run Multigres pg_isready")?;

            if status.success() {
                return Ok(());
            }

            if started_at.elapsed() >= MULTIGRES_READY_TIMEOUT {
                eprintln!("Multigres readiness timed out; dumping Docker Compose diagnostics.");

                let mut ps = compose.command(self);
                ps.arg("ps");
                let _ = ps.status();

                let mut logs = compose.command(self);
                logs.args(["logs", "--tail", "200", "multigres"]);
                let _ = logs.status();

                bail!(
                    "Multigres on 127.0.0.1:{} did not become ready within \
                     {MULTIGRES_READY_TIMEOUT:?}",
                    self.gateway_port
                );
            }

            thread::sleep(HEALTH_CHECK_INTERVAL);
        }
    }
}

impl TestArgs {
    /// Starts the supported topology and runs the Multigres tests.
    fn run(self) -> Result<()> {
        self.recreate_cluster()?;

        let mut test = Command::new("cargo");
        test.args([
            "nextest",
            "run",
            "-p",
            "etl",
            "--features",
            "test-utils",
            "--test",
            "multigres",
            "--run-ignored",
            "only",
            "--no-capture",
        ]);
        test.env("MULTIGRES_GW_HOST", "127.0.0.1");
        test.env("MULTIGRES_GW_PORT", self.gateway_port.to_string());
        test.env("MULTIGRES_GW_USER", "postgres");
        test.env("MULTIGRES_GW_PASSWORD", "postgres");
        test.env("MULTIGRES_GW_DBNAME", "postgres");
        run_command(test, "Multigres tests failed")
    }

    /// Recreates the test cluster, retrying one failed image bootstrap.
    fn recreate_cluster(&self) -> Result<()> {
        let compose = DockerCompose::detect()?;

        for attempt in 1..=TEST_CLUSTER_START_ATTEMPTS {
            let start = StartArgs { cells: DEFAULT_CELLS, gateway_port: self.gateway_port };
            eprintln!(
                "recreating the dedicated ephemeral Multigres test cluster (attempt \
                 {attempt}/{TEST_CLUSTER_START_ATTEMPTS})."
            );

            let mut down = compose.command(&start);
            down.args(["down", "--volumes", "--remove-orphans"]);
            run_command(down, "Failed to remove the previous Multigres test cluster")?;

            match start.run() {
                Ok(()) => return Ok(()),
                Err(error) if attempt < TEST_CLUSTER_START_ATTEMPTS => {
                    eprintln!("Multigres startup failed; retrying with a fresh cluster: {error:#}");
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("the bounded Multigres startup loop should return")
    }
}

impl DockerCompose {
    /// Detects Docker Compose v2 or the legacy standalone binary.
    fn detect() -> Result<Self> {
        if Command::new("docker")
            .args(["compose", "version"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok(Self { program: "docker", args: &["compose"] });
        }

        if Command::new("docker-compose")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok(Self { program: "docker-compose", args: &[] });
        }

        bail!("Docker Compose is not installed");
    }

    /// Creates a Docker Compose command configured for the local cluster.
    fn command(&self, config: &StartArgs) -> Command {
        let mut cmd = Command::new(self.program);
        cmd.args(self.args);
        cmd.args(["-f", COMPOSE_FILE]);
        cmd.env("MULTIGRES_NUM_CELLS", config.cells.to_string());
        cmd.env("MULTIGRES_GATEWAY_PORT", config.gateway_port.to_string());
        cmd
    }
}

/// Runs a command and fails when it exits unsuccessfully.
fn run_command(mut command: Command, context: &'static str) -> Result<()> {
    let status = command.status().context(context)?;

    if !status.success() {
        bail!(context);
    }

    Ok(())
}
