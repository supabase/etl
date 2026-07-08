use std::{
    env, fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use serde_json::json;

use crate::utils::workspace_root;

/// Local Kubernetes context used for OrbStack.
const LOCAL_CONTEXT: &str = "orbstack";
/// Kubernetes namespace used by the local data plane.
const DEFAULT_NAMESPACE: &str = "etl-data-plane";
/// Default Kubernetes resource name prefix.
const DEFAULT_NAME: &str = "local-replicator";
/// Default locally built replicator image tag.
const DEFAULT_IMAGE: &str = "etl-replicator:local";
/// Default application environment loaded by the replicator.
const DEFAULT_APP_ENVIRONMENT: &str = "dev";
/// Default CPU request for the local replicator pod.
const DEFAULT_CPU_REQUEST: &str = "125m";
/// Default CPU limit for the local replicator pod.
const DEFAULT_CPU_LIMIT: &str = "250m";
/// Default memory request for the local replicator pod.
const DEFAULT_MEMORY_REQUEST: &str = "250Mi";
/// Default memory limit for the local replicator pod.
const DEFAULT_MEMORY_LIMIT: &str = "300Mi";
/// Default log filter used by the local replicator.
const DEFAULT_RUST_LOG: &str = "info";
/// Default tracing flag used by the local replicator.
const DEFAULT_ENABLE_TRACING: &str = "1";
/// Workspace paths copied into the curated Docker build context.
const BUILD_CONTEXT_PATHS: &[&str] = &["Cargo.toml", "Cargo.lock", ".cargo", "crates", "scripts"];
/// Base config file name mounted into the replicator config directory.
const BASE_CONFIG_FILE: &str = "base.json";
/// Default local pipeline id.
const DEFAULT_PIPELINE_ID: &str = "1";
/// Default publication created by `cargo x seed`.
const DEFAULT_PUBLICATION_NAME: &str = "seed_pub";
/// Default host name for reaching host-published services from local k8s pods.
const DEFAULT_POSTGRES_HOST: &str = "host.docker.internal";
/// Default host-published local Postgres port.
const DEFAULT_POSTGRES_PORT: &str = "5430";
/// Default database created by `cargo x seed`.
const DEFAULT_POSTGRES_DATABASE: &str = "etl_testdata";
/// Default local Postgres user.
const DEFAULT_POSTGRES_USER: &str = "postgres";
/// Default local Postgres password.
const DEFAULT_POSTGRES_PASSWORD: &str = "postgres";
/// Default local ClickHouse URL from inside local k8s pods.
const DEFAULT_CLICKHOUSE_URL: &str = "http://host.docker.internal:8123";
/// Default local ClickHouse user.
const DEFAULT_CLICKHOUSE_USER: &str = "etl";
/// Default local ClickHouse password.
const DEFAULT_CLICKHOUSE_PASSWORD: &str = "etl";
/// Default local ClickHouse database.
const DEFAULT_CLICKHOUSE_DATABASE: &str = "default";

/// Arguments for deploying the replicator to a local Kubernetes cluster.
#[derive(Args)]
#[command(after_help = "\
Notes:
  The command generates local replicator config from REPLICATOR_* environment variables.
  Defaults expect `cargo x seed` data (`etl_testdata`, `seed_pub`) and host-published local \
                        services.
  Override with REPLICATOR_PIPELINE_ID, REPLICATOR_PUBLICATION_NAME, REPLICATOR_POSTGRES_HOST,
  REPLICATOR_POSTGRES_PORT, REPLICATOR_POSTGRES_DB, REPLICATOR_POSTGRES_USER,
  REPLICATOR_POSTGRES_PASSWORD, REPLICATOR_CLICKHOUSE_URL, REPLICATOR_CLICKHOUSE_USER,
  REPLICATOR_CLICKHOUSE_PASSWORD, or REPLICATOR_CLICKHOUSE_DATABASE.
")]
pub(crate) struct DeployLocalArgs {
    /// Kubernetes context (defaults to K8S_CONTEXT or orbstack).
    #[arg(long, value_name = "NAME")]
    context: Option<String>,
    /// Kubernetes namespace (defaults to K8S_NAMESPACE or etl-data-plane).
    #[arg(long, value_name = "NAME")]
    namespace: Option<String>,
    /// Resource name prefix (defaults to REPLICATOR_NAME or local-replicator).
    #[arg(long, value_name = "NAME")]
    name: Option<String>,
    /// Docker image tag (defaults to REPLICATOR_IMAGE or etl-replicator:local).
    #[arg(long, value_name = "NAME:TAG")]
    image: Option<String>,
    /// Application environment (defaults to APP_ENVIRONMENT or dev).
    #[arg(long, value_name = "ENV")]
    app_environment: Option<String>,
    /// CPU request (defaults to CPU_REQUEST, MAC_CPU_REQUEST, or 125m).
    #[arg(long, value_name = "VALUE")]
    cpu_request: Option<String>,
    /// CPU limit (defaults to CPU_LIMIT, MAC_CPU_LIMIT, or 250m).
    #[arg(long, value_name = "VALUE")]
    cpu_limit: Option<String>,
    /// Memory request (defaults to MEMORY_REQUEST, MAC_MEMORY_REQUEST, or
    /// 250Mi).
    #[arg(long, value_name = "VALUE")]
    memory_request: Option<String>,
    /// Memory limit (defaults to MEMORY_LIMIT, MAC_MEMORY_LIMIT, or 300Mi).
    #[arg(long, value_name = "VALUE")]
    memory_limit: Option<String>,
    /// Skip the local Docker image build.
    #[arg(long)]
    skip_build: bool,
    /// Skip waiting for the StatefulSet rollout.
    #[arg(long)]
    skip_wait: bool,
    /// Allow deployment to a Kubernetes context other than orbstack.
    #[arg(long)]
    allow_non_local_context: bool,
}

impl DeployLocalArgs {
    /// Builds the local replicator image when requested and applies local
    /// Kubernetes resources.
    pub(crate) fn run(self) -> Result<()> {
        let workspace_root = workspace_root()?;
        let config = DeployLocalConfig::from_args(self, workspace_root)?;

        validate_local_context(&config)?;
        require_command("kubectl", &["version", "--client"], "Kubectl is required")?;
        if !config.skip_build {
            require_command("docker", &["--version"], "Docker is required")?;
            require_command("rsync", &["--version"], "Rsync is required")?;
        }
        require_kubernetes_context(&config.context)?;

        let files = DeployFiles::prepare(&config)?;
        if !config.skip_build {
            build_image(&config, &files)?;
        }

        apply_configmaps(&config, &files)?;
        apply_workload(&config)?;
        if !config.skip_wait {
            wait_for_rollout(&config)?;
        }

        print_summary(&config);
        Ok(())
    }
}

/// Runtime configuration for a local deploy.
struct DeployLocalConfig {
    /// Repository root.
    workspace_root: PathBuf,
    /// Kubernetes context.
    context: String,
    /// Kubernetes namespace.
    namespace: String,
    /// Kubernetes resource name prefix.
    name: String,
    /// Replicator image tag.
    image: String,
    /// Application environment.
    app_environment: String,
    /// Log filter.
    rust_log: String,
    /// Tracing enablement value.
    enable_tracing: String,
    /// Pod CPU request.
    cpu_request: String,
    /// Pod CPU limit.
    cpu_limit: String,
    /// Pod memory request.
    memory_request: String,
    /// Pod memory limit.
    memory_limit: String,
    /// Whether to skip Docker image building.
    skip_build: bool,
    /// Whether to skip rollout waiting.
    skip_wait: bool,
    /// Whether a non-OrbStack context is allowed.
    allow_non_local_context: bool,
    /// Pipeline id written to the generated local config.
    pipeline_id: u64,
    /// Publication name written to the generated local config.
    publication_name: String,
    /// Source Postgres host written to the generated local config.
    postgres_host: String,
    /// Source Postgres port written to the generated local config.
    postgres_port: u16,
    /// Source Postgres database written to the generated local config.
    postgres_database: String,
    /// Source Postgres user written to the generated local config.
    postgres_user: String,
    /// Source Postgres password written to the generated local config.
    postgres_password: String,
    /// ClickHouse URL written to the generated local config.
    clickhouse_url: String,
    /// ClickHouse user written to the generated local config.
    clickhouse_user: String,
    /// ClickHouse password written to the generated local config.
    clickhouse_password: String,
    /// ClickHouse database written to the generated local config.
    clickhouse_database: String,
}

impl DeployLocalConfig {
    /// Builds deploy configuration from command-line arguments and environment.
    fn from_args(args: DeployLocalArgs, workspace_root: PathBuf) -> Result<Self> {
        Ok(Self {
            workspace_root,
            context: args.context.unwrap_or_else(|| env_or("K8S_CONTEXT", LOCAL_CONTEXT)),
            namespace: args.namespace.unwrap_or_else(|| env_or("K8S_NAMESPACE", DEFAULT_NAMESPACE)),
            name: args.name.unwrap_or_else(|| env_or("REPLICATOR_NAME", DEFAULT_NAME)),
            image: args.image.unwrap_or_else(|| env_or("REPLICATOR_IMAGE", DEFAULT_IMAGE)),
            app_environment: args
                .app_environment
                .unwrap_or_else(|| env_or("APP_ENVIRONMENT", DEFAULT_APP_ENVIRONMENT)),
            rust_log: env_or("RUST_LOG", DEFAULT_RUST_LOG),
            enable_tracing: env_or("ENABLE_TRACING", DEFAULT_ENABLE_TRACING),
            cpu_request: args.cpu_request.unwrap_or_else(|| {
                env_or_with_fallback("CPU_REQUEST", "MAC_CPU_REQUEST", DEFAULT_CPU_REQUEST)
            }),
            cpu_limit: args.cpu_limit.unwrap_or_else(|| {
                env_or_with_fallback("CPU_LIMIT", "MAC_CPU_LIMIT", DEFAULT_CPU_LIMIT)
            }),
            memory_request: args.memory_request.unwrap_or_else(|| {
                env_or_with_fallback("MEMORY_REQUEST", "MAC_MEMORY_REQUEST", DEFAULT_MEMORY_REQUEST)
            }),
            memory_limit: args.memory_limit.unwrap_or_else(|| {
                env_or_with_fallback("MEMORY_LIMIT", "MAC_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT)
            }),
            skip_build: args.skip_build,
            skip_wait: args.skip_wait,
            allow_non_local_context: args.allow_non_local_context,
            pipeline_id: parse_env_or("REPLICATOR_PIPELINE_ID", DEFAULT_PIPELINE_ID)?,
            publication_name: env_or("REPLICATOR_PUBLICATION_NAME", DEFAULT_PUBLICATION_NAME),
            postgres_host: env_or("REPLICATOR_POSTGRES_HOST", DEFAULT_POSTGRES_HOST),
            postgres_port: parse_env_or("REPLICATOR_POSTGRES_PORT", DEFAULT_POSTGRES_PORT)?,
            postgres_database: env_or("REPLICATOR_POSTGRES_DB", DEFAULT_POSTGRES_DATABASE),
            postgres_user: env_or("REPLICATOR_POSTGRES_USER", DEFAULT_POSTGRES_USER),
            postgres_password: env_or("REPLICATOR_POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
            clickhouse_url: env_or("REPLICATOR_CLICKHOUSE_URL", DEFAULT_CLICKHOUSE_URL),
            clickhouse_user: env_or("REPLICATOR_CLICKHOUSE_USER", DEFAULT_CLICKHOUSE_USER),
            clickhouse_password: env_or(
                "REPLICATOR_CLICKHOUSE_PASSWORD",
                DEFAULT_CLICKHOUSE_PASSWORD,
            ),
            clickhouse_database: env_or(
                "REPLICATOR_CLICKHOUSE_DATABASE",
                DEFAULT_CLICKHOUSE_DATABASE,
            ),
        })
    }

    /// Returns the config map that stores mounted configuration files.
    fn config_map_name(&self) -> String {
        format!("{}-config", self.name)
    }

    /// Returns the config map that stores process environment values.
    fn env_config_map_name(&self) -> String {
        format!("{}-env", self.name)
    }

    /// Returns the headless service name.
    fn headless_service_name(&self) -> String {
        format!("{}-headless", self.name)
    }

    /// Returns the StatefulSet name.
    fn stateful_set_name(&self) -> String {
        format!("{}-statefulset", self.name)
    }
}

/// Temporary files prepared for the deploy.
struct DeployFiles {
    /// Temporary directory removed when this value is dropped.
    _temp_dir: TempDir,
    /// Generated base config path.
    base_path: PathBuf,
    /// Generated environment config path.
    environment_path: PathBuf,
    /// Generated environment config file name.
    environment_file_name: String,
    /// Curated Docker build context path.
    build_context_dir: PathBuf,
}

impl DeployFiles {
    /// Writes required config files into a temporary deploy directory.
    fn prepare(config: &DeployLocalConfig) -> Result<Self> {
        let temp_dir = TempDir::create("etl-deploy-local")?;
        let base_path = temp_dir.path().join(BASE_CONFIG_FILE);
        let environment_file_name = format!("{}.json", config.app_environment);
        let environment_path = temp_dir.path().join(&environment_file_name);
        let build_context_dir = temp_dir.path().join("build-context");

        fs::write(&base_path, "{}\n")
            .with_context(|| format!("Failed to write {}", base_path.display()))?;
        fs::write(&environment_path, local_replicator_config(config)?)
            .with_context(|| format!("Failed to write {}", environment_path.display()))?;

        Ok(Self {
            _temp_dir: temp_dir,
            base_path,
            environment_path,
            environment_file_name,
            build_context_dir,
        })
    }
}

/// Temporary directory that removes itself on drop.
struct TempDir {
    /// Temporary directory path.
    path: PathBuf,
}

impl TempDir {
    /// Creates a unique temporary directory.
    fn create(prefix: &str) -> Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("System clock is before the Unix epoch")?
            .as_nanos();
        let path = env::temp_dir().join(format!("{prefix}-{}-{timestamp}", std::process::id()));
        fs::create_dir_all(&path)
            .with_context(|| format!("Failed to create temporary directory {}", path.display()))?;
        Ok(Self { path })
    }

    /// Returns the temporary directory path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    /// Removes the temporary directory.
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Fails if the deploy targets a non-local context without explicit consent.
fn validate_local_context(config: &DeployLocalConfig) -> Result<()> {
    if config.context == LOCAL_CONTEXT || config.allow_non_local_context {
        return Ok(());
    }

    bail!(
        "Refusing to deploy to Kubernetes context `{}`. `cargo x deploy-local` defaults to `{}`; \
         pass `--allow-non-local-context` if this is intentional.",
        config.context,
        LOCAL_CONTEXT
    )
}

/// Ensures the configured Kubernetes context exists.
fn require_kubernetes_context(context: &str) -> Result<()> {
    let output = Command::new("kubectl")
        .args(["config", "get-contexts", "-o", "name"])
        .output()
        .context("Failed to list Kubernetes contexts")?;

    if !output.status.success() {
        bail!("Failed to list Kubernetes contexts");
    }

    let contexts = String::from_utf8_lossy(&output.stdout);
    if contexts.lines().any(|line| line == context) {
        return Ok(());
    }

    bail!("Kubernetes context `{context}` not found")
}

/// Creates the curated Docker build context and builds the replicator image.
fn build_image(config: &DeployLocalConfig, files: &DeployFiles) -> Result<()> {
    fs::create_dir_all(&files.build_context_dir).with_context(|| {
        format!("Failed to create Docker build context {}", files.build_context_dir.display())
    })?;

    for relative_path in BUILD_CONTEXT_PATHS {
        copy_to_build_context(config, files, relative_path)?;
    }

    println!("building image {}...", config.image);
    let mut cmd = Command::new("docker");
    cmd.arg("build");
    cmd.arg("-f");
    cmd.arg(files.build_context_dir.join("crates/etl-replicator/Dockerfile"));
    cmd.arg("-t");
    cmd.arg(&config.image);
    cmd.arg(&files.build_context_dir);
    run_command(cmd, "Failed to build local replicator image")
}

/// Copies one workspace path into the curated Docker build context.
fn copy_to_build_context(
    config: &DeployLocalConfig,
    files: &DeployFiles,
    relative_path: &str,
) -> Result<()> {
    let source = config.workspace_root.join(relative_path);
    if !source.exists() {
        return Ok(());
    }

    let relative = Path::new(relative_path);
    let parent = relative
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let destination_dir = files.build_context_dir.join(parent);
    fs::create_dir_all(&destination_dir).with_context(|| {
        format!("Failed to create build context directory {}", destination_dir.display())
    })?;

    let mut cmd = Command::new("rsync");
    cmd.arg("-a");
    cmd.args(["--exclude", ".git"]);
    cmd.args(["--exclude", "target"]);
    cmd.args(["--exclude", ".DS_Store"]);
    cmd.arg(&source);
    cmd.arg(format!("{}/", destination_dir.display()));
    run_command(cmd, "Failed to copy workspace path into Docker build context")
}

/// Applies config maps used by the local replicator.
fn apply_configmaps(config: &DeployLocalConfig, files: &DeployFiles) -> Result<()> {
    println!("applying configmaps...");
    apply_yaml(
        kubectl_apply_command(config),
        &render_config_files_configmap(config, files)?,
        "Failed to apply file configmap",
    )?;
    apply_yaml(
        kubectl_apply_command(config),
        &render_env_configmap(config)?,
        "Failed to apply environment configmap",
    )
}

/// Renders the config map containing mounted configuration files.
fn render_config_files_configmap(
    config: &DeployLocalConfig,
    files: &DeployFiles,
) -> Result<Vec<u8>> {
    let mut cmd = kubectl_command(config);
    cmd.args(["-n", &config.namespace, "create", "configmap"]);
    cmd.arg(config.config_map_name());
    cmd.arg(format!("--from-file={BASE_CONFIG_FILE}={}", files.base_path.display()));
    cmd.arg(format!(
        "--from-file={}={}",
        files.environment_file_name,
        files.environment_path.display()
    ));
    cmd.args(["--dry-run=client", "-o", "yaml"]);
    capture_stdout(cmd, "Failed to render file configmap")
}

/// Renders the config map containing process environment values.
fn render_env_configmap(config: &DeployLocalConfig) -> Result<Vec<u8>> {
    let mut cmd = kubectl_command(config);
    cmd.args(["-n", &config.namespace, "create", "configmap"]);
    cmd.arg(config.env_config_map_name());
    cmd.arg("--from-literal=APP_CONFIG_DIR=/etc/etl/replicator-config");
    cmd.arg(format!("--from-literal=APP_ENVIRONMENT={}", config.app_environment));
    cmd.arg(format!("--from-literal=RUST_LOG={}", config.rust_log));
    cmd.arg(format!("--from-literal=ENABLE_TRACING={}", config.enable_tracing));
    cmd.arg(format!("--from-literal=APP_VERSION={}", config.image));
    cmd.args(["--dry-run=client", "-o", "yaml"]);
    capture_stdout(cmd, "Failed to render environment configmap")
}

/// Applies the local service and StatefulSet.
fn apply_workload(config: &DeployLocalConfig) -> Result<()> {
    println!("applying service + statefulset...");
    apply_yaml(
        kubectl_apply_command(config),
        workload_manifest(config).as_bytes(),
        "Failed to apply service and StatefulSet",
    )
}

/// Waits for the local StatefulSet rollout to finish.
fn wait_for_rollout(config: &DeployLocalConfig) -> Result<()> {
    println!("waiting for rollout...");
    let mut cmd = kubectl_command(config);
    cmd.args(["-n", &config.namespace, "rollout", "status"]);
    cmd.arg(format!("statefulset/{}", config.stateful_set_name()));
    cmd.arg("--timeout=180s");
    run_command(cmd, "Failed waiting for local replicator rollout")
}

/// Prints the final deploy summary.
fn print_summary(config: &DeployLocalConfig) {
    println!("done.");
    println!("statefulset: {}", config.stateful_set_name());
    println!("configmap (files): {}", config.config_map_name());
    println!("configmap (env): {}", config.env_config_map_name());
    println!("logs:");
    println!(
        "  kubectl --context {} -n {} logs -f statefulset/{}",
        config.context,
        config.namespace,
        config.stateful_set_name()
    );
}

/// Returns a kubectl command with the configured context.
fn kubectl_command(config: &DeployLocalConfig) -> Command {
    let mut cmd = Command::new("kubectl");
    cmd.arg("--context");
    cmd.arg(&config.context);
    cmd
}

/// Returns a kubectl apply command that reads YAML from standard input.
fn kubectl_apply_command(config: &DeployLocalConfig) -> Command {
    let mut cmd = kubectl_command(config);
    cmd.args(["-n", &config.namespace, "apply", "-f", "-"]);
    cmd
}

/// Applies a YAML document through kubectl standard input.
fn apply_yaml(mut cmd: Command, yaml: &[u8], context: &str) -> Result<()> {
    let mut child = cmd.stdin(Stdio::piped()).spawn().with_context(|| context.to_owned())?;
    {
        let stdin = child.stdin.as_mut().context("Failed to open kubectl stdin")?;
        stdin.write_all(yaml).context("Failed to write manifest to kubectl stdin")?;
    }

    let status = child.wait().with_context(|| context.to_owned())?;
    if !status.success() {
        bail!("{context}");
    }

    Ok(())
}

/// Captures standard output from a command.
fn capture_stdout(mut cmd: Command, context: &str) -> Result<Vec<u8>> {
    let output = cmd.output().with_context(|| context.to_owned())?;
    if output.status.success() {
        return Ok(output.stdout);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        bail!("{context}");
    }

    bail!("{context}: {stderr}")
}

/// Runs a command and fails when it exits unsuccessfully.
fn run_command(mut cmd: Command, context: &str) -> Result<()> {
    let status = cmd.status().with_context(|| context.to_owned())?;
    if !status.success() {
        bail!("{context}");
    }

    Ok(())
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
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::null());
    cmd.status().is_ok_and(|status| status.success())
}

/// Returns an environment variable value or a default.
fn env_or(key: &str, default: &str) -> String {
    optional_env(key).unwrap_or_else(|| default.to_owned())
}

/// Parses an environment variable value or a default.
fn parse_env_or<T>(key: &str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = env_or(key, default);
    value.parse().map_err(|error| anyhow::anyhow!("Failed to parse {key} value `{value}`: {error}"))
}

/// Returns an environment variable, a fallback environment variable, or a
/// default.
fn env_or_with_fallback(key: &str, fallback_key: &str, default: &str) -> String {
    optional_env(key).or_else(|| optional_env(fallback_key)).unwrap_or_else(|| default.to_owned())
}

/// Returns a non-empty environment variable.
fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

/// Renders local replicator configuration.
fn local_replicator_config(config: &DeployLocalConfig) -> Result<Vec<u8>> {
    let config = json!({
        "destination": {
            "clickhouse": {
                "url": config.clickhouse_url,
                "user": config.clickhouse_user,
                "password": config.clickhouse_password,
                "database": config.clickhouse_database,
            }
        },
        "pipeline": {
            "id": config.pipeline_id,
            "publication_name": config.publication_name,
            "pg_connection": {
                "host": config.postgres_host,
                "port": config.postgres_port,
                "name": config.postgres_database,
                "username": config.postgres_user,
                "password": config.postgres_password,
                "tls": {
                    "trusted_root_certs": "",
                    "enabled": false,
                }
            }
        }
    });

    serde_json::to_vec_pretty(&config).context("Failed to render local replicator config")
}

/// Renders the service and StatefulSet manifest.
fn workload_manifest(config: &DeployLocalConfig) -> String {
    format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: {headless_service_name}
  labels:
    app.kubernetes.io/name: {name}
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: {name}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {stateful_set_name}
  labels:
    app.kubernetes.io/name: {name}
spec:
  serviceName: {headless_service_name}
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: {name}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: {name}
    spec:
      terminationGracePeriodSeconds: 30
      containers:
      - name: replicator
        image: {image}
        imagePullPolicy: IfNotPresent
        envFrom:
        - configMapRef:
            name: {env_config_map_name}
        resources:
          requests:
            cpu: "{cpu_request}"
            memory: "{memory_request}"
          limits:
            cpu: "{cpu_limit}"
            memory: "{memory_limit}"
        volumeMounts:
        - name: replicator-config
          mountPath: /etc/etl/replicator-config
          readOnly: true
      volumes:
      - name: replicator-config
        configMap:
          name: {config_map_name}
"#,
        config_map_name = config.config_map_name(),
        cpu_limit = config.cpu_limit,
        cpu_request = config.cpu_request,
        env_config_map_name = config.env_config_map_name(),
        headless_service_name = config.headless_service_name(),
        image = config.image,
        memory_limit = config.memory_limit,
        memory_request = config.memory_request,
        name = config.name,
        stateful_set_name = config.stateful_set_name(),
    )
}
