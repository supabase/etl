use std::{
    env,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::utils::{maybe_configure_sccache, workspace_root};

/// Docker Compose file used by the local development stack.
const DEFAULT_COMPOSE_FILE: &str = "scripts/docker/docker-compose.yaml";
/// Default Docker Compose service for source Postgres.
const DEFAULT_POSTGRES_SERVICE: &str = "source-postgres";
/// Default Docker Compose service for ClickHouse.
const DEFAULT_CLICKHOUSE_SERVICE: &str = "clickhouse";
/// Default Postgres host port.
const DEFAULT_POSTGRES_PORT: &str = "5430";
/// Default Postgres user.
const DEFAULT_POSTGRES_USER: &str = "postgres";
/// Default Postgres password.
const DEFAULT_POSTGRES_PASSWORD: &str = "postgres";
/// Default ClickHouse HTTP port.
const DEFAULT_CLICKHOUSE_HTTP_PORT: &str = "8123";
/// Default ClickHouse user.
const DEFAULT_CLICKHOUSE_USER: &str = "etl";
/// Default ClickHouse password.
const DEFAULT_CLICKHOUSE_PASSWORD: &str = "etl";
/// Default Cargo integration test target.
const DEFAULT_TEST_TARGET: &str = "main";
/// Default Cargo integration test filter.
const DEFAULT_TEST_NAME_FILTER: &str = "clickhouse_pipeline";
/// Default Cargo package under test.
const DEFAULT_CARGO_PACKAGE: &str = "etl-destinations";
/// Default features for ClickHouse tests.
const DEFAULT_FEATURES: &str = "clickhouse,test-utils";
/// Interval between readiness probes.
const READINESS_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Arguments for running local ClickHouse destination tests.
#[derive(Args)]
pub(crate) struct TestClickhouseArgs {
    /// Enable sccache for cargo builds (also enabled by ETL_SCCACHE=1).
    #[arg(long)]
    sccache: bool,
}

impl TestClickhouseArgs {
    /// Starts local services when needed and runs ClickHouse tests.
    pub(crate) fn run(self) -> Result<()> {
        let TestClickhouseArgs { sccache } = self;
        let workspace_root = workspace_root()?;
        let config = TestClickhouseConfig::from_env(&workspace_root);

        require_command("cargo", &["--version"], "Cargo is not installed")?;

        let compose = if config.skip_docker { None } else { Some(DockerCompose::detect()?) };
        if let Some(compose) = &compose {
            start_services(compose, &config)?;
            wait_for_services(compose, &config)?;
        }

        run_clickhouse_tests(&config, sccache)
    }
}

/// Runtime configuration for ClickHouse tests.
struct TestClickhouseConfig {
    /// Docker Compose file path.
    compose_file: PathBuf,
    /// Docker Compose Postgres service name.
    postgres_service: String,
    /// Docker Compose ClickHouse service name.
    clickhouse_service: String,
    /// Postgres host port.
    postgres_port: String,
    /// Postgres user.
    postgres_user: String,
    /// Postgres password.
    postgres_password: String,
    /// ClickHouse HTTP port.
    clickhouse_http_port: String,
    /// ClickHouse user.
    clickhouse_user: String,
    /// ClickHouse password.
    clickhouse_password: String,
    /// Optional Cargo toolchain override.
    cargo_toolchain: String,
    /// Cargo integration test target.
    test_target: String,
    /// Optional test name filter.
    test_name_filter: String,
    /// Cargo package under test.
    cargo_package: String,
    /// Cargo features enabled for the test.
    features: String,
    /// Whether Docker Compose startup should be skipped.
    skip_docker: bool,
}

impl TestClickhouseConfig {
    /// Loads test settings from environment variables.
    fn from_env(workspace_root: &Path) -> Self {
        let compose_file = optional_env("COMPOSE_FILE")
            .map_or_else(|| workspace_root.join(DEFAULT_COMPOSE_FILE), PathBuf::from);

        Self {
            compose_file,
            postgres_service: env_or("POSTGRES_SERVICE", DEFAULT_POSTGRES_SERVICE),
            clickhouse_service: env_or("CLICKHOUSE_SERVICE", DEFAULT_CLICKHOUSE_SERVICE),
            postgres_port: env_or("POSTGRES_PORT", DEFAULT_POSTGRES_PORT),
            postgres_user: env_or("POSTGRES_USER", DEFAULT_POSTGRES_USER),
            postgres_password: env_or("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
            clickhouse_http_port: env_or("CLICKHOUSE_HTTP_PORT", DEFAULT_CLICKHOUSE_HTTP_PORT),
            clickhouse_user: env_or("CLICKHOUSE_USER", DEFAULT_CLICKHOUSE_USER),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", DEFAULT_CLICKHOUSE_PASSWORD),
            cargo_toolchain: env_or("CARGO_TOOLCHAIN", ""),
            test_target: env_or("TEST_TARGET", DEFAULT_TEST_TARGET),
            test_name_filter: env_or_unset("TEST_NAME_FILTER", DEFAULT_TEST_NAME_FILTER),
            cargo_package: env_or("CARGO_PACKAGE", DEFAULT_CARGO_PACKAGE),
            features: env_or("FEATURES", DEFAULT_FEATURES),
            skip_docker: optional_env("SKIP_DOCKER").is_some(),
        }
    }
}

/// Environment passed to ClickHouse tests.
struct TestEnvironment {
    /// Test database host.
    database_host: String,
    /// Test database port.
    database_port: String,
    /// Test database user.
    database_username: String,
    /// Test database password.
    database_password: String,
    /// ClickHouse HTTP URL.
    clickhouse_url: String,
    /// ClickHouse user.
    clickhouse_user: String,
    /// ClickHouse password.
    clickhouse_password: String,
}

impl TestEnvironment {
    /// Loads test environment variables from configured defaults.
    fn from_config(config: &TestClickhouseConfig) -> Self {
        Self {
            database_host: env_or("TESTS_DATABASE_HOST", "localhost"),
            database_port: env_or("TESTS_DATABASE_PORT", &config.postgres_port),
            database_username: env_or("TESTS_DATABASE_USERNAME", &config.postgres_user),
            database_password: env_or("TESTS_DATABASE_PASSWORD", &config.postgres_password),
            clickhouse_url: env_or(
                "TESTS_CLICKHOUSE_URL",
                &format!("http://localhost:{}", config.clickhouse_http_port),
            ),
            clickhouse_user: env_or("TESTS_CLICKHOUSE_USER", &config.clickhouse_user),
            clickhouse_password: env_or("TESTS_CLICKHOUSE_PASSWORD", &config.clickhouse_password),
        }
    }

    /// Adds test environment variables to a command.
    fn apply_to(&self, cmd: &mut Command) {
        cmd.env("TESTS_DATABASE_HOST", &self.database_host);
        cmd.env("TESTS_DATABASE_PORT", &self.database_port);
        cmd.env("TESTS_DATABASE_USERNAME", &self.database_username);
        cmd.env("TESTS_DATABASE_PASSWORD", &self.database_password);
        cmd.env("TESTS_CLICKHOUSE_URL", &self.clickhouse_url);
        cmd.env("TESTS_CLICKHOUSE_USER", &self.clickhouse_user);
        cmd.env("TESTS_CLICKHOUSE_PASSWORD", &self.clickhouse_password);
    }
}

/// Resolved Docker Compose command.
struct DockerCompose {
    /// Executable name.
    program: String,
    /// Prefix arguments before `-f`.
    args: Vec<String>,
}

impl DockerCompose {
    /// Detects Docker Compose v2, a configured binary, or the legacy binary.
    fn detect() -> Result<Self> {
        if let Some(program) = optional_env("DOCKER_COMPOSE_BIN") {
            require_command(&program, &["--version"], "Docker Compose is not installed")?;
            return Ok(Self { program, args: Vec::new() });
        }

        if command_succeeds("docker", &["compose", "version"]) {
            return Ok(Self { program: "docker".to_owned(), args: vec!["compose".to_owned()] });
        }

        if command_succeeds("docker-compose", &["--version"]) {
            return Ok(Self { program: "docker-compose".to_owned(), args: Vec::new() });
        }

        bail!("Docker Compose is not installed");
    }

    /// Creates a Docker Compose command configured with the compose file.
    fn command(&self, compose_file: &Path) -> Command {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.args);
        cmd.arg("-f");
        cmd.arg(compose_file);
        cmd
    }
}

/// Starts local Postgres and ClickHouse services.
fn start_services(compose: &DockerCompose, config: &TestClickhouseConfig) -> Result<()> {
    println!("🐳 Starting local Postgres and ClickHouse services...");

    let mut cmd = compose.command(&config.compose_file);
    cmd.args(["up", "-d"]);
    cmd.arg(&config.postgres_service);
    cmd.arg(&config.clickhouse_service);
    run_command(cmd, "Failed to start local Postgres and ClickHouse services")
}

/// Waits for local Postgres and ClickHouse services to accept connections.
fn wait_for_services(compose: &DockerCompose, config: &TestClickhouseConfig) -> Result<()> {
    println!("⏳ Waiting for Postgres to be ready...");
    wait_until("Postgres", || {
        let mut cmd = compose.command(&config.compose_file);
        cmd.args(["exec", "-T"]);
        cmd.arg(&config.postgres_service);
        cmd.args(["pg_isready", "-U"]);
        cmd.arg(&config.postgres_user);
        command_succeeds_silent(&mut cmd)
    })?;

    println!("⏳ Waiting for ClickHouse to be ready...");
    wait_until("ClickHouse", || {
        let mut cmd = compose.command(&config.compose_file);
        cmd.args(["exec", "-T"]);
        cmd.arg(&config.clickhouse_service);
        cmd.args(["clickhouse-client", "--user"]);
        cmd.arg(&config.clickhouse_user);
        cmd.arg("--password");
        cmd.arg(&config.clickhouse_password);
        cmd.args(["--query", "SELECT 1"]);
        command_succeeds_silent(&mut cmd)
    })
}

/// Runs the ClickHouse destination test command.
fn run_clickhouse_tests(config: &TestClickhouseConfig, sccache: bool) -> Result<()> {
    let test_env = TestEnvironment::from_config(config);
    let test_args = test_args(config);
    let cargo_display = cargo_display(&config.cargo_toolchain);

    println!("🧪 Running ClickHouse destination test with:");
    println!("   TESTS_DATABASE_HOST={}", test_env.database_host);
    println!("   TESTS_DATABASE_PORT={}", test_env.database_port);
    println!("   TESTS_DATABASE_USERNAME={}", test_env.database_username);
    println!("   TESTS_CLICKHOUSE_URL={}", test_env.clickhouse_url);
    println!("   TESTS_CLICKHOUSE_USER={}", test_env.clickhouse_user);
    println!(
        "   TESTS_CLICKHOUSE_PASSWORD={}",
        if test_env.clickhouse_password.is_empty() { "" } else { "[set]" }
    );
    println!(
        "   cargo toolchain={}",
        if config.cargo_toolchain.is_empty() {
            "project default"
        } else {
            config.cargo_toolchain.as_str()
        }
    );

    let mut cmd = Command::new("cargo");
    if !config.cargo_toolchain.is_empty() {
        cmd.arg(format!("+{}", config.cargo_toolchain));
    }
    cmd.args(&test_args);
    test_env.apply_to(&mut cmd);
    maybe_configure_sccache(&mut cmd, sccache);

    println!("🚀 {cargo_display} {}", test_args.join(" "));

    run_command(cmd, "Failed to run ClickHouse destination tests")
}

/// Builds Cargo test arguments.
fn test_args(config: &TestClickhouseConfig) -> Vec<String> {
    let mut args = vec![
        "test".to_owned(),
        "-p".to_owned(),
        config.cargo_package.clone(),
        "--features".to_owned(),
        config.features.clone(),
        "--test".to_owned(),
        config.test_target.clone(),
    ];

    if !config.test_name_filter.is_empty() {
        args.push(config.test_name_filter.clone());
    }

    args.extend(["--".to_owned(), "--nocapture".to_owned()]);
    args
}

/// Returns the displayed Cargo command.
fn cargo_display(cargo_toolchain: &str) -> String {
    if cargo_toolchain.is_empty() {
        "cargo".to_owned()
    } else {
        format!("cargo +{cargo_toolchain}")
    }
}

/// Waits until a readiness probe succeeds.
fn wait_until<F>(description: &str, mut ready: F) -> Result<()>
where
    F: FnMut() -> Result<bool>,
{
    loop {
        if ready()? {
            return Ok(());
        }

        println!("Waiting for {description}...");
        thread::sleep(READINESS_CHECK_INTERVAL);
    }
}

/// Requires a command to be callable.
fn require_command(program: &str, args: &[&str], message: &'static str) -> Result<()> {
    if command_succeeds(program, args) {
        return Ok(());
    }

    bail!("{message}");
}

/// Returns whether a command exits successfully.
fn command_succeeds(program: &str, args: &[&str]) -> bool {
    let mut cmd = Command::new(program);
    cmd.args(args);
    command_succeeds_silent(&mut cmd).unwrap_or(false)
}

/// Returns whether a command exits successfully while discarding output.
fn command_succeeds_silent(cmd: &mut Command) -> Result<bool> {
    let status = cmd.stdout(Stdio::null()).stderr(Stdio::null()).status()?;
    Ok(status.success())
}

/// Runs a command and fails when it exits unsuccessfully.
fn run_command(mut cmd: Command, context: &'static str) -> Result<()> {
    let status = cmd.status().context(context)?;

    if !status.success() {
        bail!("{context}");
    }

    Ok(())
}

/// Returns a non-empty environment variable or a default value.
fn env_or(key: &str, default: &str) -> String {
    env::var(key).ok().filter(|value| !value.is_empty()).unwrap_or_else(|| default.to_owned())
}

/// Returns an environment variable or a default value when unset.
fn env_or_unset(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Returns a non-empty environment variable.
fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}
