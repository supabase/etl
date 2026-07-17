use std::{
    env, fs,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::Args;

use super::migrate;
use crate::utils::workspace_root;

/// Docker Compose file used by the local development stack.
const COMPOSE_FILE: &str = "scripts/docker/docker-compose.yaml";
/// Local Kubernetes manifests required by the API data plane.
const LOCAL_K8S_DIR: &str = "scripts/k8s/local";
/// Local Kubernetes context used for OrbStack.
const ORBSTACK_CONTEXT: &str = "orbstack";
/// Kubernetes namespace configured by the local manifests.
const DATA_PLANE_NAMESPACE: &str = "etl-data-plane";
/// Docker Compose service for the source Postgres instance.
const SOURCE_POSTGRES_SERVICE: &str = "source-postgres";
/// Docker Compose service for the source Postgres read replica.
const SOURCE_POSTGRES_READ_REPLICA_SERVICE: &str = "source-postgres-read-replica";
/// Docker Compose service for ClickHouse.
const CLICKHOUSE_SERVICE: &str = "clickhouse";
/// Interval between readiness probes.
const READINESS_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Arguments for setting up the local development environment.
#[derive(Args)]
pub(crate) struct InitArgs {}

impl InitArgs {
    /// Starts local dependencies, runs migrations, and applies local k8s
    /// manifests.
    pub(crate) fn run(self) -> Result<()> {
        let workspace_root = workspace_root()?;
        env::set_current_dir(&workspace_root).with_context(|| {
            format!("Failed to change directory to {}", workspace_root.display())
        })?;

        require_command("psql", &["--version"], "Postgres client (psql) is not installed")?;
        require_command("kubectl", &["version", "--client"], "Kubectl is not installed")?;

        println!("🔧 Configuring database settings...");
        let config = InitConfig::from_env();
        let compose = if config.skip_docker { None } else { Some(DockerCompose::detect()?) };

        if let Some(compose) = &compose {
            start_docker_stack(compose, &config)?;
        }

        let database_url = config.database_url();
        if let Some(compose) = &compose {
            wait_for_docker_services(compose, &config)?;
        } else {
            wait_for_external_postgres(&database_url)?;
        }

        if compose.is_some() {
            println!(
                "🧪 Postgres test env: TESTS_DATABASE_HOST={} TESTS_DATABASE_PORT={} \
                 TESTS_DATABASE_REPLICA_HOST={} TESTS_DATABASE_REPLICA_PORT={} \
                 TESTS_DATABASE_USERNAME={}",
                config.db_host,
                config.db_port,
                config.db_host,
                config.db_replica_port,
                config.db_user,
            );
        }

        run_migrations(&config, &database_url)?;
        seed_default_replicator_image(&database_url)?;
        configure_kubernetes()?;

        println!("✨ Complete development environment setup finished! Ready to go!");
        Ok(())
    }
}

/// Runtime configuration for local initialization.
struct InitConfig {
    /// Postgres user.
    db_user: String,
    /// Postgres password.
    db_password: String,
    /// Postgres database name.
    db_name: String,
    /// Host port for source Postgres.
    db_port: String,
    /// Host port for source Postgres read replica.
    db_replica_port: String,
    /// Postgres host.
    db_host: String,
    /// Host port for the ClickHouse HTTP API.
    clickhouse_http_port: String,
    /// Host port for the ClickHouse native protocol.
    clickhouse_native_port: String,
    /// ClickHouse user.
    clickhouse_user: String,
    /// ClickHouse password.
    clickhouse_password: String,
    /// Optional Postgres persistent storage path.
    postgres_data_volume: Option<String>,
    /// Optional Postgres read replica persistent storage path.
    postgres_replica_data_volume: Option<String>,
    /// Optional ClickHouse persistent storage path.
    clickhouse_data_volume: Option<String>,
    /// Whether to skip Docker Compose startup.
    skip_docker: bool,
}

impl InitConfig {
    /// Loads initialization settings from environment variables.
    fn from_env() -> Self {
        Self {
            db_user: env_or("POSTGRES_USER", "postgres"),
            db_password: env_or("POSTGRES_PASSWORD", "postgres"),
            db_name: env_or("POSTGRES_DB", "postgres"),
            db_port: env_or("POSTGRES_PORT", "5430"),
            db_replica_port: env_or("POSTGRES_REPLICA_PORT", "6430"),
            db_host: env_or("POSTGRES_HOST", "localhost"),
            clickhouse_http_port: env_or("CLICKHOUSE_HTTP_PORT", "8123"),
            clickhouse_native_port: env_or("CLICKHOUSE_NATIVE_PORT", "9001"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "etl"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", "etl"),
            postgres_data_volume: optional_env("POSTGRES_DATA_VOLUME"),
            postgres_replica_data_volume: optional_env("POSTGRES_REPLICA_DATA_VOLUME"),
            clickhouse_data_volume: optional_env("CLICKHOUSE_DATA_VOLUME"),
            skip_docker: optional_env("SKIP_DOCKER").is_some(),
        }
    }

    /// Returns the local Postgres connection string.
    fn database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.db_user, self.db_password, self.db_host, self.db_port, self.db_name
        )
    }
}

/// Resolved Docker Compose command.
struct DockerCompose {
    /// Executable name.
    program: &'static str,
    /// Prefix arguments before `-f`.
    args: &'static [&'static str],
}

impl DockerCompose {
    /// Detects Docker Compose v2 or the legacy standalone binary.
    fn detect() -> Result<Self> {
        if command_succeeds("docker", &["compose", "version"]) {
            return Ok(Self { program: "docker", args: &["compose"] });
        }

        if command_succeeds("docker-compose", &["--version"]) {
            return Ok(Self { program: "docker-compose", args: &[] });
        }

        bail!("Docker Compose is not installed");
    }

    /// Creates a Docker Compose command configured for the local stack.
    fn command(&self, config: &InitConfig) -> Command {
        let mut cmd = Command::new(self.program);
        cmd.args(self.args);
        cmd.args(["-f", COMPOSE_FILE]);
        apply_compose_env(&mut cmd, config);
        cmd
    }
}

/// Starts local services with Docker Compose.
fn start_docker_stack(compose: &DockerCompose, config: &InitConfig) -> Result<()> {
    println!("🐳 Starting all services with Docker Compose...");

    prepare_volume("Postgres", config.postgres_data_volume.as_deref())?;
    prepare_volume("Postgres read replica", config.postgres_replica_data_volume.as_deref())?;
    prepare_volume("ClickHouse", config.clickhouse_data_volume.as_deref())?;

    println!("📥 Pulling latest images...");
    let mut pull = compose.command(config);
    pull.arg("pull");
    run_command(pull, "Failed to pull Docker Compose images")?;

    let mut up = compose.command(config);
    up.args(["up", "-d"]);
    run_command(up, "Failed to start Docker Compose services")?;

    println!("✅ All services started");
    Ok(())
}

/// Waits for Docker-managed services to accept connections.
fn wait_for_docker_services(compose: &DockerCompose, config: &InitConfig) -> Result<()> {
    wait_until("Postgres", || {
        let mut cmd = compose.command(config);
        cmd.args(["exec", "-T", SOURCE_POSTGRES_SERVICE, "pg_isready", "-U", &config.db_user]);
        command_succeeds_silent(&mut cmd)
    })?;
    println!("✅ Postgres is up and running on port {}", config.db_port);

    wait_until("Postgres read replica", || {
        let mut cmd = compose.command(config);
        cmd.args([
            "exec",
            "-T",
            SOURCE_POSTGRES_READ_REPLICA_SERVICE,
            "pg_isready",
            "-U",
            &config.db_user,
        ]);
        command_succeeds_silent(&mut cmd)
    })?;
    println!("✅ Postgres read replica is up and running on port {}", config.db_replica_port);

    wait_until("ClickHouse", || {
        let mut cmd = compose.command(config);
        cmd.args([
            "exec",
            "-T",
            CLICKHOUSE_SERVICE,
            "clickhouse-client",
            "--user",
            &config.clickhouse_user,
            "--password",
            &config.clickhouse_password,
            "--query",
            "SELECT 1",
        ]);
        command_succeeds_silent(&mut cmd)
    })?;
    println!("✅ ClickHouse is up and running on port {}", config.clickhouse_http_port);
    println!("🔗 ClickHouse HTTP URL: http://localhost:{}", config.clickhouse_http_port);
    println!(
        "🧪 ClickHouse test env: TESTS_CLICKHOUSE_URL=http://localhost:{} TESTS_CLICKHOUSE_USER={}",
        config.clickhouse_http_port, config.clickhouse_user,
    );

    Ok(())
}

/// Waits for an externally managed Postgres instance.
fn wait_for_external_postgres(database_url: &str) -> Result<()> {
    wait_until("Postgres", || {
        let mut cmd = Command::new("psql");
        cmd.args([database_url, "-c", "select 1"]);
        command_succeeds_silent(&mut cmd)
    })?;

    println!("✅ Postgres is up and running");
    println!("ℹ️  SKIP_DOCKER is set; skipping ClickHouse readiness checks.");
    Ok(())
}

/// Runs database migrations through the xtask entrypoint.
fn run_migrations(config: &InitConfig, database_url: &str) -> Result<()> {
    println!("🔄 Running database migrations...");

    let envs = [
        ("POSTGRES_USER", config.db_user.as_str()),
        ("POSTGRES_PASSWORD", config.db_password.as_str()),
        ("POSTGRES_DB", config.db_name.as_str()),
        ("POSTGRES_PORT", config.db_port.as_str()),
        ("POSTGRES_HOST", config.db_host.as_str()),
        ("DATABASE_URL", database_url),
    ];
    migrate::run_migrations_with_env(&[], envs).context("Failed to run database migrations")
}

/// Seeds the default replicator image through the API database function.
fn seed_default_replicator_image(database_url: &str) -> Result<()> {
    println!("🖼️ Seeding default replicator image...");

    let image = env_or("REPLICATOR_IMAGE", "public.ecr.aws/supabase/etl-replicator:latest");
    let sql = format!("select app.update_default_image({});", sql_string_literal(&image));

    let mut cmd = Command::new("psql");
    cmd.args([database_url, "-v", "ON_ERROR_STOP=1", "-c", &sql]);
    run_command(cmd, "Failed to seed default replicator image")
}

/// Ensures local Kubernetes resources exist in OrbStack.
fn configure_kubernetes() -> Result<()> {
    if !kubectl_context_exists(ORBSTACK_CONTEXT)? {
        bail!(
            "Kubernetes context '{ORBSTACK_CONTEXT}' not found. Install OrbStack and enable \
             Kubernetes in its settings."
        );
    }

    let mut nodes = Command::new("kubectl");
    nodes.args(["--context", ORBSTACK_CONTEXT, "get", "nodes"]);
    if !command_succeeds_silent(&mut nodes)? {
        bail!(
            "OrbStack Kubernetes cluster is not reachable. Start OrbStack and ensure Kubernetes \
             is enabled and running."
        );
    }

    println!("☸️ Configuring Kubernetes environment...");
    let mut apply = Command::new("kubectl");
    apply.args(["--context", ORBSTACK_CONTEXT, "apply", "-f", LOCAL_K8S_DIR]);
    run_command(apply, "Failed to apply local Kubernetes manifests")?;

    println!("✅ Configured Kubernetes namespace {DATA_PLANE_NAMESPACE}");
    Ok(())
}

/// Adds Docker Compose environment variables to a command.
fn apply_compose_env(cmd: &mut Command, config: &InitConfig) {
    apply_database_env(cmd, config, &config.database_url());
    cmd.env("POSTGRES_REPLICA_PORT", &config.db_replica_port);
    cmd.env("CLICKHOUSE_HTTP_PORT", &config.clickhouse_http_port);
    cmd.env("CLICKHOUSE_NATIVE_PORT", &config.clickhouse_native_port);
    cmd.env("CLICKHOUSE_USER", &config.clickhouse_user);
    cmd.env("CLICKHOUSE_PASSWORD", &config.clickhouse_password);

    if let Some(value) = &config.postgres_data_volume {
        cmd.env("POSTGRES_DATA_VOLUME", value);
    }
    if let Some(value) = &config.postgres_replica_data_volume {
        cmd.env("POSTGRES_REPLICA_DATA_VOLUME", value);
    }
    if let Some(value) = &config.clickhouse_data_volume {
        cmd.env("CLICKHOUSE_DATA_VOLUME", value);
    }
}

/// Adds database environment variables to a command.
fn apply_database_env(cmd: &mut Command, config: &InitConfig, database_url: &str) {
    cmd.env("POSTGRES_USER", &config.db_user);
    cmd.env("POSTGRES_PASSWORD", &config.db_password);
    cmd.env("POSTGRES_DB", &config.db_name);
    cmd.env("POSTGRES_PORT", &config.db_port);
    cmd.env("POSTGRES_HOST", &config.db_host);
    cmd.env("DATABASE_URL", database_url);
}

/// Creates a persistent volume directory when configured.
fn prepare_volume(name: &str, path: Option<&str>) -> Result<()> {
    match path {
        Some(path) => {
            println!("📁 Setting up {name} persistent storage at {path}");
            fs::create_dir_all(path)
                .with_context(|| format!("Failed to create persistent storage directory {path}"))?;
        }
        None => println!("📁 No {name} storage path specified, using default Docker volume"),
    }

    Ok(())
}

/// Waits until a readiness probe succeeds.
fn wait_until<F>(description: &str, mut ready: F) -> Result<()>
where
    F: FnMut() -> Result<bool>,
{
    println!("⏳ Waiting for {description} to be ready...");
    loop {
        if ready()? {
            return Ok(());
        }

        println!("waiting for {description}...");
        thread::sleep(READINESS_CHECK_INTERVAL);
    }
}

/// Returns whether a kubectl context exists.
fn kubectl_context_exists(context: &str) -> Result<bool> {
    let output = Command::new("kubectl")
        .args(["config", "get-contexts", "-o", "name"])
        .output()
        .context("Failed to list Kubernetes contexts")?;

    if !output.status.success() {
        bail!("Failed to list Kubernetes contexts");
    }

    let contexts = String::from_utf8_lossy(&output.stdout);
    Ok(contexts.lines().any(|line| line == context))
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

/// Returns an environment variable or a default value.
fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Returns a non-empty environment variable.
fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

/// Escapes a value as a SQL string literal.
fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
