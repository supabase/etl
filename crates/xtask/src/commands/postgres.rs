use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

use super::shared::{DEFAULT_BASE_PORT, DEFAULT_PG_SHARD_COUNT, READ_REPLICA_PORT_OFFSET};

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
const TLS_CERT_DIR: &str = "target/postgres-tls";
const TLS_ROOT_CERT_FILE: &str = "root.crt";
const TLS_ROOT_KEY_FILE: &str = "root.key";
const TLS_SERVER_CERT_FILE: &str = "server.crt";
const TLS_SERVER_KEY_FILE: &str = "server.key";
const TLS_SERVER_CSR_FILE: &str = "server.csr";
const TLS_SERVER_EXT_FILE: &str = "server.ext";
const POSTGRES_TLS_CERT_PATH: &str = "/var/lib/postgresql/server.crt";
const POSTGRES_TLS_KEY_PATH: &str = "/var/lib/postgresql/server.key";
const POSTGRES_SSL_ARGS: &str = "-c ssl=on -c ssl_cert_file=/var/lib/postgresql/server.crt -c \
                                 ssl_key_file=/var/lib/postgresql/server.key";
const SOURCE_POSTGRES_SERVICE: &str = "source-postgres";
const SOURCE_POSTGRES_READ_REPLICA_SERVICE: &str = "source-postgres-read-replica";
const SOURCE_POSTGRES_SERVICES: [&str; 2] =
    [SOURCE_POSTGRES_SERVICE, SOURCE_POSTGRES_READ_REPLICA_SERVICE];

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
    /// Start only source Postgres instead of the full local development stack.
    #[arg(long, default_value_t = false)]
    source_only: bool,
    /// Skip TLS support in the spawned source Postgres containers.
    #[arg(long, default_value_t = false)]
    no_tls: bool,
}

/// Paths to generated TLS files used by local Postgres containers.
struct TlsFiles {
    /// Server certificate copied into Postgres containers.
    server_cert: PathBuf,
    /// Server private key copied into Postgres containers.
    server_key: PathBuf,
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

        if self
            .base_port
            .checked_add(READ_REPLICA_PORT_OFFSET)
            .and_then(|port| port.checked_add(self.shards - 1))
            .is_none()
        {
            bail!("--base-port + --shards + read replica port offset exceeds the valid port range");
        }

        eprintln!(
            "starting {} Postgres {} clusters on ports {}..{} with read replicas on ports \
             {}..{}{}.",
            self.shards,
            self.pg_version,
            self.base_port,
            self.base_port + self.shards - 1,
            self.base_port + READ_REPLICA_PORT_OFFSET,
            self.base_port + READ_REPLICA_PORT_OFFSET + self.shards - 1,
            if self.source_only { " with source services only" } else { "" },
        );

        let tls_files = if self.no_tls { None } else { Some(self.prepare_tls_files()?) };

        // Start the full stack on the base port unless the caller only needs source
        // Postgres.
        let first_shard_services =
            if self.source_only { &SOURCE_POSTGRES_SERVICES[..] } else { &[] };
        self.start_cluster(None, self.base_port, first_shard_services, tls_files.as_ref())?;

        // Start additional source-postgres containers on subsequent ports.
        for shard in 2..=self.shards {
            let port = self.base_port + shard - 1;
            let project = format!("etl-stack-pg-{}-shard-{shard}", self.pg_version);
            self.start_cluster(
                Some((&project, port)),
                port,
                &SOURCE_POSTGRES_SERVICES,
                tls_files.as_ref(),
            )?;
        }

        // Wait for all clusters to accept connections.
        for shard in 1..=self.shards {
            let port = self.base_port + shard - 1;
            self.wait_for_pg(port)?;
            self.wait_for_pg(port + READ_REPLICA_PORT_OFFSET)?;
        }

        if tls_files.is_some() {
            eprintln!(
                "source Postgres services support TLS. Local tests use plaintext by default; set \
                 TESTS_DATABASE_TLS_ENABLED=true to require verified TLS.",
            );
        }

        Ok(())
    }

    /// Starts one source Postgres cluster, enabling server-side TLS when
    /// certificate files are available.
    fn start_cluster(
        &self,
        project_and_port: Option<(&str, u16)>,
        port: u16,
        services: &[&str],
        tls_files: Option<&TlsFiles>,
    ) -> Result<()> {
        self.docker_compose_up(project_and_port, services, false, false)?;

        if let Some(tls_files) = tls_files {
            self.wait_for_pg(port)?;
            self.wait_for_pg(port + READ_REPLICA_PORT_OFFSET)?;

            for service in SOURCE_POSTGRES_SERVICES {
                self.install_tls_files(project_and_port, tls_files, service)?;
            }
            self.docker_compose_up(project_and_port, &SOURCE_POSTGRES_SERVICES, true, true)?;
        }

        Ok(())
    }

    /// Generates a local certificate authority and server certificate for the
    /// source Postgres test containers.
    fn prepare_tls_files(&self) -> Result<TlsFiles> {
        let cert_dir = PathBuf::from(TLS_CERT_DIR);
        let root_cert = cert_dir.join(TLS_ROOT_CERT_FILE);
        let root_key = cert_dir.join(TLS_ROOT_KEY_FILE);
        let server_cert = cert_dir.join(TLS_SERVER_CERT_FILE);
        let server_key = cert_dir.join(TLS_SERVER_KEY_FILE);
        let server_csr = cert_dir.join(TLS_SERVER_CSR_FILE);
        let server_ext = cert_dir.join(TLS_SERVER_EXT_FILE);

        if root_cert.is_file() && server_cert.is_file() && server_key.is_file() {
            return Ok(TlsFiles { server_cert, server_key });
        }

        fs::create_dir_all(&cert_dir).context("failed to create Postgres TLS certificate dir")?;
        fs::write(
            &server_ext,
            "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1\nextendedKeyUsage=serverAuth\n",
        )
        .context("failed to write Postgres TLS certificate extension file")?;

        run_openssl(["genrsa", "-out", path_str(&root_key)?, "2048"])?;
        run_openssl([
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-key",
            path_str(&root_key)?,
            "-sha256",
            "-days",
            "3650",
            "-subj",
            "/CN=etl local test root",
            "-out",
            path_str(&root_cert)?,
        ])?;
        run_openssl(["genrsa", "-out", path_str(&server_key)?, "2048"])?;
        run_openssl([
            "req",
            "-new",
            "-key",
            path_str(&server_key)?,
            "-subj",
            "/CN=localhost",
            "-out",
            path_str(&server_csr)?,
        ])?;
        run_openssl([
            "x509",
            "-req",
            "-in",
            path_str(&server_csr)?,
            "-CA",
            path_str(&root_cert)?,
            "-CAkey",
            path_str(&root_key)?,
            "-CAcreateserial",
            "-out",
            path_str(&server_cert)?,
            "-days",
            "3650",
            "-sha256",
            "-extfile",
            path_str(&server_ext)?,
        ])?;

        Ok(TlsFiles { server_cert, server_key })
    }

    fn docker_compose_up(
        &self,
        project_and_port: Option<(&str, u16)>,
        services: &[&str],
        tls: bool,
        force_recreate: bool,
    ) -> Result<()> {
        let postgres_image = std::env::var("POSTGRES_IMAGE")
            .unwrap_or_else(|_| format!("postgres:{}", self.pg_version));
        let mut cmd = self.docker_compose_base_command(project_and_port);
        cmd.env("POSTGRES_IMAGE", &postgres_image);

        if tls {
            cmd.env("POSTGRES_SSL_ARGS", POSTGRES_SSL_ARGS);
        }

        cmd.args(["up", "-d"]);
        if force_recreate {
            cmd.arg("--force-recreate");
        }
        cmd.args(services);

        let status = cmd.status().context("failed to run docker compose")?;

        if !status.success() {
            bail!("docker compose up failed");
        }

        Ok(())
    }

    /// Copies generated TLS files into a source Postgres service and fixes
    /// ownership and permissions for the Postgres server.
    fn install_tls_files(
        &self,
        project_and_port: Option<(&str, u16)>,
        tls_files: &TlsFiles,
        service: &str,
    ) -> Result<()> {
        self.docker_compose_cp(
            project_and_port,
            service,
            &tls_files.server_cert,
            POSTGRES_TLS_CERT_PATH,
        )?;
        self.docker_compose_cp(
            project_and_port,
            service,
            &tls_files.server_key,
            POSTGRES_TLS_KEY_PATH,
        )?;

        let permissions_command = format!(
            "chown postgres:postgres {POSTGRES_TLS_CERT_PATH} {POSTGRES_TLS_KEY_PATH} && chmod \
             0644 {POSTGRES_TLS_CERT_PATH} && chmod 0600 {POSTGRES_TLS_KEY_PATH}"
        );
        let mut cmd = self.docker_compose_base_command(project_and_port);
        cmd.args(["exec", "-T", "-u", "root", service, "sh", "-c", &permissions_command]);
        run_command(cmd, "failed to prepare Postgres TLS files")?;

        Ok(())
    }

    /// Copies a file from the host into a source Postgres service container.
    fn docker_compose_cp(
        &self,
        project_and_port: Option<(&str, u16)>,
        service: &str,
        source: &Path,
        destination: &str,
    ) -> Result<()> {
        let mut cmd = self.docker_compose_base_command(project_and_port);
        cmd.args(["cp", path_str(source)?, &format!("{service}:{destination}")]);
        run_command(cmd, "failed to copy Postgres TLS file into container")
    }

    /// Returns a configured docker compose command for this Postgres version.
    fn docker_compose_base_command(&self, project_and_port: Option<(&str, u16)>) -> Command {
        let (program, compose_args) = docker_compose_command();
        let mut cmd = Command::new(program);
        cmd.args(compose_args);
        cmd.args(["-f", COMPOSE_FILE]);
        cmd.env("POSTGRES_VERSION", &self.pg_version);

        let port = project_and_port.map_or(self.base_port, |(_, port)| port);
        cmd.env("POSTGRES_PORT", port.to_string());
        cmd.env("POSTGRES_REPLICA_PORT", (port + READ_REPLICA_PORT_OFFSET).to_string());

        if let Some((project, _)) = project_and_port {
            cmd.args(["-p", project]);
        }

        cmd
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
}

/// Converts a path to UTF-8 for command arguments.
fn path_str(path: &Path) -> Result<&str> {
    path.to_str().context("path is not valid UTF-8")
}

/// Runs an openssl command and fails if it exits unsuccessfully.
fn run_openssl<const N: usize>(args: [&str; N]) -> Result<()> {
    let mut cmd = Command::new("openssl");
    cmd.args(args);
    run_command(cmd, "failed to run openssl")
}

/// Runs a command and fails if it exits unsuccessfully.
fn run_command(mut cmd: Command, context: &'static str) -> Result<()> {
    let status = cmd.status().context(context)?;

    if !status.success() {
        bail!("{context}");
    }

    Ok(())
}
