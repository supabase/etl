use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    thread,
};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};

/// Nextest filter expression that selects tests requiring a Postgres cluster.
///
/// This must stay in sync with the `shared-pg` test group in
/// `.config/nextest.toml`.
const SHARED_PG_FILTER: &str = "\
    test(exclusive_) | binary_id(etl::main) | (binary_id(etl-destinations::main) & \
                                test(/^(bigquery_pipeline|ducklake_pipeline|iceberg_destination)::/\
                                ))";

use super::shared::{DEFAULT_BASE_PORT, DEFAULT_PG_SHARD_COUNT};

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Mode {
    /// Run tests via `cargo nextest run`.
    Run,
    /// Run tests with coverage via `cargo llvm-cov nextest`.
    LlvmCov,
}

#[derive(Args)]
pub(crate) struct NextestArgs {
    /// Whether to collect coverage.
    #[arg(value_enum)]
    mode: Mode,

    /// Number of Postgres clusters to shard across.
    #[arg(long, env = "NUM_LOCAL_DATABASES", default_value_t = DEFAULT_PG_SHARD_COUNT)]
    shards: u16,

    /// Base port for the first Postgres cluster. Additional clusters are
    /// allocated on consecutive ports.
    #[arg(long, env = "TESTS_DATABASE_START_PORT", default_value_t = DEFAULT_BASE_PORT)]
    base_port: u16,

    /// Extra arguments forwarded to every nextest invocation.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    extra: Vec<String>,
}

impl NextestArgs {
    pub(crate) fn run(self) -> Result<()> {
        if self.shards == 0 {
            bail!("--shards must be at least 1");
        }

        if self.base_port.checked_add(self.shards - 1).is_none() {
            bail!("--base-port + --shards exceeds the valid port range");
        }

        let pg_env = PgEnv::from_env();

        eprintln!(
            "running sharded {} with {} Postgres clusters on ports {}..{}.",
            self.mode_label(),
            self.shards,
            self.base_port,
            self.base_port + self.shards - 1,
        );

        if matches!(self.mode, Mode::LlvmCov) {
            install_llvm_tools()?;
        }

        if matches!(self.mode, Mode::Run) {
            prebuild_test_binaries()?;
        }

        let mut lanes: Vec<Lane> = Vec::with_capacity(1 + self.shards as usize);

        // Non-Postgres lane: everything that doesn't need a cluster.
        lanes.push(Lane {
            name: "non-pg".to_owned(),
            filter: format!("not ({SHARED_PG_FILTER})"),
            partition: None,
            pg_port: None,
        });

        // One lane per Postgres shard, each on a dedicated port.
        for shard in 1..=self.shards {
            lanes.push(Lane {
                name: format!("pg-{shard}"),
                filter: SHARED_PG_FILTER.to_owned(),
                partition: Some(format!("hash:{shard}/{}", self.shards)),
                pg_port: Some(self.base_port + shard - 1),
            });
        }

        let handles: Vec<_> = lanes
            .into_iter()
            .map(|lane| {
                let mode = self.mode;
                let extra = self.extra.clone();
                let pg_env = pg_env.clone();
                thread::spawn(move || run_lane(&lane, mode, &extra, &pg_env))
            })
            .collect();

        let mut failed = false;
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    eprintln!("lane error: {e:#}");
                    failed = true;
                }
                Err(_) => {
                    eprintln!("a lane thread panicked");
                    failed = true;
                }
            }
        }

        if failed {
            bail!("one or more test lanes failed");
        }

        Ok(())
    }

    fn mode_label(&self) -> &'static str {
        match self.mode {
            Mode::Run => "nextest",
            Mode::LlvmCov => "llvm-cov nextest",
        }
    }
}

/// A test lane: an independent nextest invocation with its own filter and
/// optional Postgres cluster binding.
struct Lane {
    /// Display name used as a prefix in output (e.g. `[pg-1]`).
    name: String,
    /// Nextest filter expression (`-E`) selecting which tests this lane runs.
    filter: String,
    /// Nextest hash partition (e.g. `hash:1/3`). `None` for unpartitioned
    /// lanes.
    partition: Option<String>,
    /// Port of the Postgres cluster for this lane. `None` for non-Postgres
    /// lanes.
    pg_port: Option<u16>,
}

/// Postgres connection defaults, read once from the environment.
///
/// Only the port varies per shard; host, username, and password are shared.
/// These are local test defaults for Docker Compose, not real credentials.
#[derive(Clone)]
struct PgEnv {
    host: String,
    username: String,
    password: String,
}

impl PgEnv {
    fn from_env() -> Self {
        Self {
            host: std::env::var("TESTS_DATABASE_HOST").unwrap_or_else(|_| "localhost".to_owned()),
            username: std::env::var("TESTS_DATABASE_USERNAME")
                .unwrap_or_else(|_| "postgres".to_owned()),
            password: std::env::var("TESTS_DATABASE_PASSWORD")
                .unwrap_or_else(|_| "postgres".to_owned()),
        }
    }
}

/// Builds a nextest `Command` with mode-specific args and the common flags
/// shared by all lanes (`--workspace --all-features --no-fail-fast`).
fn nextest_command(mode: Mode) -> Command {
    let mut cmd = Command::new("cargo");

    match mode {
        Mode::Run => cmd.args(["nextest", "run"]),
        Mode::LlvmCov => cmd.args(["llvm-cov", "nextest"]),
    };

    cmd.args(["--workspace", "--all-features", "--no-fail-fast"]);

    if matches!(mode, Mode::LlvmCov) {
        cmd.arg("--no-report");
    }

    cmd
}

fn run_lane(lane: &Lane, mode: Mode, extra: &[String], pg_env: &PgEnv) -> Result<()> {
    let mut cmd = nextest_command(mode);
    cmd.args(["-E", &lane.filter]);

    if let Some(partition) = &lane.partition {
        cmd.args(["--partition", partition]);
    }

    if let Some(port) = lane.pg_port {
        cmd.env("TESTS_DATABASE_HOST", &pg_env.host);
        cmd.env("TESTS_DATABASE_PORT", port.to_string());
        cmd.env("TESTS_DATABASE_USERNAME", &pg_env.username);
        cmd.env("TESTS_DATABASE_PASSWORD", &pg_env.password);
    }

    cmd.args(extra);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to spawn nextest")?;

    let prefix = format!("[{}]", lane.name);

    let stdout = child.stdout.take().context("stdout not piped")?;
    let stderr = child.stderr.take().context("stderr not piped")?;

    let prefix_out = prefix.clone();
    let out_thread = thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            eprintln!("{prefix_out} {line}");
        }
    });

    let prefix_err = prefix.clone();
    let err_thread = thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            eprintln!("{prefix_err} {line}");
        }
    });

    out_thread.join().expect("stdout reader panicked");
    err_thread.join().expect("stderr reader panicked");

    let status = child.wait().context("failed to wait for nextest")?;
    if status.success() {
        Ok(())
    } else {
        bail!("lane {} failed", lane.name);
    }
}

/// Compiles all test binaries up front so the parallel shard lanes don't race
/// on cargo file locks during compilation.
fn prebuild_test_binaries() -> Result<()> {
    eprintln!("prebuilding test binaries.");
    let status = Command::new("cargo")
        .args(["nextest", "run", "--workspace", "--all-features", "--no-run"])
        .status()
        .context("failed to prebuild test binaries")?;

    if !status.success() {
        bail!("prebuild failed");
    }

    Ok(())
}

fn install_llvm_tools() -> Result<()> {
    eprintln!("installing llvm-tools-preview.");
    let status = Command::new("rustup")
        .args(["component", "add", "llvm-tools-preview"])
        .status()
        .context("failed to install llvm-tools-preview")?;

    if !status.success() {
        bail!("rustup component add failed");
    }

    Ok(())
}
