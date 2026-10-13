use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
};

use anyhow::{Context, Result, bail};
use clap::{Args, ValueEnum};
use tempfile::TempDir;

use crate::utils::{DEFAULT_BASE_PORT, DEFAULT_PG_SHARD_COUNT, READ_REPLICA_PORT_OFFSET};

/// Nextest filter expression that selects tests requiring a Postgres cluster.
///
/// This must stay in sync with the `shared-pg` test group in
/// `.config/nextest.toml`.
const SHARED_PG_FILTER: &str =
    "\
    test(exclusive_) | binary_id(etl::main) | (binary_id(etl-destinations::main) & \
     test(/^(bigquery|ducklake|iceberg)::/) & not test(/^bigquery::destination::/)) | \
     (binary_id(etl-destinations::main) & test(/^clickhouse::pipeline/)) | \
     (binary_id(etl-destinations) & test(/ducklake::core::tests::postgres_backed::/))";

/// Test execution mode.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Mode {
    /// Run tests via `cargo nextest run`.
    Run,
    /// Run tests with coverage via `cargo llvm-cov nextest`.
    LlvmCov,
}

/// Arguments for running isolated test lanes, optionally from a shared build.
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

    /// Run previously compiled tests from a nextest archive without rebuilding.
    #[arg(long)]
    archive_file: Option<PathBuf>,

    /// Extra arguments forwarded to every nextest invocation.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    extra: Vec<String>,
}

impl NextestArgs {
    /// Builds or restores tests, then waits for every isolated lane.
    pub(crate) fn run(self) -> Result<()> {
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

        let pg_env = PgEnv::from_env();

        eprintln!(
            "running sharded {} with {} Postgres clusters on ports {}..{} and read replicas on \
             ports {}..{}.",
            self.mode_label(),
            self.shards,
            self.base_port,
            self.base_port + self.shards - 1,
            self.base_port + READ_REPLICA_PORT_OFFSET,
            self.base_port + READ_REPLICA_PORT_OFFSET + self.shards - 1,
        );

        if self.archive_file.is_some() && matches!(self.mode, Mode::LlvmCov) {
            bail!("Archive reuse requires run mode; coverage must use instrumented builds");
        }

        if matches!(self.mode, Mode::LlvmCov) {
            install_llvm_tools()?;
            let status = Command::new("cargo")
                .args(["llvm-cov", "clean", "--locked", "--workspace"])
                .status()
                .context("Failed to clean previous coverage data")?;
            if !status.success() {
                bail!("Failed to clean previous coverage data");
            }
        }

        let archive = if let Some(archive) = &self.archive_file {
            Some(extract_archive(archive)?)
        } else {
            None
        };

        if matches!(self.mode, Mode::Run) && archive.is_none() {
            prebuild_test_binaries()?;
        }

        let archive_target = archive.as_ref().map(|directory| directory.path().join("target"));

        let mut lanes: Vec<Lane> = Vec::with_capacity(1 + self.shards as usize);

        // Every compatibility job runs the full suite, including destinations.
        lanes.push(Lane {
            name: "non-pg".to_owned(),
            filter: format!("not ({SHARED_PG_FILTER})"),
            partition: None,
            pg_port: None,
        });

        // Round-robin slices balance test counts across dedicated clusters.
        for shard in 1..=self.shards {
            lanes.push(Lane {
                name: format!("pg-{shard}"),
                filter: SHARED_PG_FILTER.to_owned(),
                partition: Some(format!("slice:{shard}/{}", self.shards)),
                pg_port: Some(self.base_port + shard - 1),
            });
        }

        let handles: Vec<_> = lanes
            .into_iter()
            .map(|lane| {
                let mode = self.mode;
                let extra = self.extra.clone();
                let pg_env = pg_env.clone();
                let archive_target = archive_target.clone();
                thread::spawn(move || {
                    run_lane(&lane, mode, &extra, &pg_env, archive_target.as_deref())
                })
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

    /// Returns the command name used in progress output.
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
    /// Nextest round-robin partition (e.g. `slice:1/3`). `None` for
    /// unpartitioned lanes.
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
    replica_host: String,
    username: String,
    password: String,
}

impl PgEnv {
    /// Reads local connection defaults before worker threads start.
    fn from_env() -> Self {
        let host = std::env::var("TESTS_DATABASE_HOST").unwrap_or_else(|_| "localhost".to_owned());
        let replica_host =
            std::env::var("TESTS_DATABASE_REPLICA_HOST").unwrap_or_else(|_| host.clone());

        Self {
            host,
            replica_host,
            username: std::env::var("TESTS_DATABASE_USERNAME")
                .unwrap_or_else(|_| "postgres".to_owned()),
            password: std::env::var("TESTS_DATABASE_PASSWORD")
                .unwrap_or_else(|_| "postgres".to_owned()),
        }
    }
}

/// Extracts an archive once before parallel lanes read its binaries and
/// metadata.
fn extract_archive(archive: &Path) -> Result<TempDir> {
    let target = std::env::current_dir()?.join("target");
    std::fs::create_dir_all(&target).context("Failed to create target directory")?;
    // Each invocation owns its extraction until all shard lanes have joined.
    let destination = tempfile::Builder::new()
        .prefix("nextest-archive-")
        .tempdir_in(target)
        .context("Failed to create archive extraction directory")?;
    let status = Command::new("cargo")
        .args(["nextest", "list", "--list-type", "binaries-only", "--archive-file"])
        .arg(archive)
        .arg("--extract-to")
        .arg(destination.path())
        .args(["--workspace-remap", "."])
        .stdout(Stdio::null())
        .status()
        .context("Failed to extract nextest archive")?;
    if !status.success() {
        bail!("Failed to extract nextest archive");
    }
    Ok(destination)
}

/// Builds a nextest command, preserving an archive's original build selection.
fn nextest_command(mode: Mode, archive_target: Option<&Path>) -> Command {
    let mut cmd = Command::new("cargo");

    match mode {
        Mode::Run => cmd.args(["nextest", "run"]),
        Mode::LlvmCov => cmd.args(["llvm-cov", "nextest"]),
    };

    if let Some(target) = archive_target {
        cmd.arg("--cargo-metadata")
            .arg(target.join("nextest/cargo-metadata.json"))
            .arg("--binaries-metadata")
            .arg(target.join("nextest/binaries-metadata.json"))
            .arg("--target-dir-remap")
            .arg(target)
            .args(["--workspace-remap", "."]);
    } else {
        cmd.args(["--locked", "--workspace", "--all-features"]);
    }

    if matches!(mode, Mode::LlvmCov) {
        // --no-report also disables cleaning. Clean once before the lanes run
        // so each lane preserves the profiles written by the others.
        cmd.arg("--no-report");
    }

    cmd
}

/// Builds one lane's command with its isolated cluster binding.
fn lane_command(
    lane: &Lane,
    mode: Mode,
    extra: &[String],
    pg_env: &PgEnv,
    archive_target: Option<&Path>,
) -> Command {
    let mut cmd = nextest_command(mode, archive_target);
    cmd.arg("--no-fail-fast");
    cmd.args(["-E", &lane.filter]);

    if let Some(partition) = &lane.partition {
        cmd.args(["--partition", partition]);
    }

    if let Some(port) = lane.pg_port {
        cmd.env("TESTS_DATABASE_HOST", &pg_env.host);
        cmd.env("TESTS_DATABASE_PORT", port.to_string());
        cmd.env("TESTS_DATABASE_REPLICA_HOST", &pg_env.replica_host);
        cmd.env("TESTS_DATABASE_REPLICA_PORT", (port + READ_REPLICA_PORT_OFFSET).to_string());
        cmd.env("TESTS_DATABASE_USERNAME", &pg_env.username);
        cmd.env("TESTS_DATABASE_PASSWORD", &pg_env.password);
    }

    cmd.args(extra);
    cmd
}

/// Runs one lane with its own cluster binding and prefixed output.
fn run_lane(
    lane: &Lane,
    mode: Mode,
    extra: &[String],
    pg_env: &PgEnv,
    archive_target: Option<&Path>,
) -> Result<()> {
    let mut cmd = lane_command(lane, mode, extra, pg_env, archive_target);
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
    let err_thread = thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            eprintln!("{prefix} {line}");
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
    let status = nextest_command(Mode::Run, None)
        .arg("--no-run")
        .status()
        .context("failed to prebuild test binaries")?;

    if !status.success() {
        bail!("prebuild failed");
    }

    Ok(())
}

/// Installs the active toolchain's coverage tools.
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, ffi::OsStr, path::Path};

    use clap::Parser;

    use crate::commands::nextest::{
        Lane, Mode, NextestArgs, PgEnv, SHARED_PG_FILTER, lane_command, nextest_command,
    };

    /// Standalone parser for testing task-runner options and passthrough.
    #[derive(Parser)]
    struct Cli {
        /// Sharded test options.
        #[command(flatten)]
        args: NextestArgs,
    }

    /// Archive and lane options must be consumed before nextest passthrough.
    #[test]
    fn archive_options_preserve_passthrough() {
        let cli = Cli::try_parse_from([
            "nextest",
            "run",
            "--archive-file",
            "tests.tar.zst",
            "--shards",
            "2",
            "--",
            "--test-threads",
            "1",
        ])
        .unwrap();
        assert!(matches!(cli.args.mode, Mode::Run));
        assert_eq!(cli.args.archive_file.as_deref(), Some(Path::new("tests.tar.zst")));
        assert_eq!(cli.args.shards, 2);
        assert_eq!(cli.args.extra, ["--test-threads", "1"]);
    }

    /// An ordinary archive cannot silently produce an uninstrumented coverage
    /// run.
    #[test]
    fn coverage_rejects_archive_before_running_commands() {
        let cli = Cli::try_parse_from(["nextest", "llvm-cov", "--archive-file", "missing.tar.zst"])
            .unwrap();
        assert_eq!(
            cli.args.run().unwrap_err().to_string(),
            "Archive reuse requires run mode; coverage must use instrumented builds"
        );
    }

    /// Reusing a build must not override its package or feature selection.
    #[test]
    fn archive_execution_preserves_build_selection() {
        let command = nextest_command(Mode::Run, Some(Path::new("target/archive/target")));
        let args: Vec<_> = command.get_args().collect();
        for build_arg in ["--locked", "--workspace", "--all-features", "--no-run"] {
            assert!(!args.contains(&std::ffi::OsStr::new(build_arg)));
        }
        assert!(args.contains(&std::ffi::OsStr::new("--cargo-metadata")));
        assert!(args.contains(&std::ffi::OsStr::new("--binaries-metadata")));
        assert!(args.contains(&std::ffi::OsStr::new("--workspace-remap")));
        assert!(args.contains(&std::ffi::OsStr::new("--target-dir-remap")));
    }

    /// Build-only invocations must not receive execution-only flags.
    #[test]
    fn prebuild_omits_execution_options() {
        let command = nextest_command(Mode::Run, None);
        let args: Vec<_> = command.get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("--locked")));
        assert!(args.contains(&std::ffi::OsStr::new("--workspace")));
        assert!(args.contains(&std::ffi::OsStr::new("--all-features")));
        assert!(!args.contains(&std::ffi::OsStr::new("--no-fail-fast")));
    }

    /// Ordinary, archive, and coverage execution preserve shard isolation.
    #[test]
    fn lane_commands_bind_each_primary_and_replica() {
        let pg_env = PgEnv {
            host: "127.0.0.1".to_owned(),
            replica_host: "localhost".to_owned(),
            username: "postgres".to_owned(),
            password: "postgres".to_owned(),
        };
        for (mode, archive) in [
            (Mode::Run, None),
            (Mode::Run, Some(Path::new("target/archive/target"))),
            (Mode::LlvmCov, None),
        ] {
            for shard in 1..=4 {
                let lane = Lane {
                    name: format!("pg-{shard}"),
                    filter: SHARED_PG_FILTER.to_owned(),
                    partition: Some(format!("slice:{shard}/4")),
                    pg_port: Some(5430 + shard - 1),
                };
                let command = lane_command(&lane, mode, &[], &pg_env, archive);
                let env: BTreeMap<_, _> = command.get_envs().collect();
                for (key, value) in [
                    ("TESTS_DATABASE_HOST", "127.0.0.1".to_owned()),
                    ("TESTS_DATABASE_REPLICA_HOST", "localhost".to_owned()),
                    ("TESTS_DATABASE_PORT", (5430 + shard - 1).to_string()),
                    ("TESTS_DATABASE_REPLICA_PORT", (6430 + shard - 1).to_string()),
                ] {
                    assert_eq!(env[OsStr::new(key)], Some(OsStr::new(&value)));
                }
                let args: Vec<_> = command.get_args().collect();
                assert!(args.windows(2).any(|pair| pair == ["-E", SHARED_PG_FILTER]));
                assert!(
                    args.windows(2).any(|pair| {
                        pair == ["--partition", lane.partition.as_deref().unwrap()]
                    })
                );
                assert!(args.contains(&OsStr::new("--no-fail-fast")));
            }
        }
    }

    /// Sharding and in-process serialization must classify the same tests.
    #[test]
    fn postgres_filter_matches_nextest_group() {
        let config: toml::Table = include_str!("../../../../.config/nextest.toml").parse().unwrap();
        let group = config["profile"]["default"]["overrides"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["test-group"].as_str() == Some("shared-pg"))
            .unwrap();
        assert_eq!(
            SHARED_PG_FILTER.split_whitespace().collect::<String>(),
            group["filter"].as_str().unwrap().split_whitespace().collect::<String>()
        );
    }

    /// Remote-only BigQuery tests have their own bounded concurrency group.
    #[test]
    fn bigquery_destination_group_is_separate_and_bounded() {
        let config: toml::Table = include_str!("../../../../.config/nextest.toml").parse().unwrap();
        assert_eq!(config["test-groups"]["shared-pg"]["max-threads"].as_integer(), Some(1));
        assert_eq!(
            config["test-groups"]["bigquery-destination"]["max-threads"].as_integer(),
            Some(1)
        );
        let group = config["profile"]["default"]["overrides"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["test-group"].as_str() == Some("bigquery-destination"))
            .unwrap();
        assert_eq!(
            group["filter"].as_str().unwrap(),
            "binary_id(etl-destinations::main) & test(/^bigquery::destination::/)"
        );
        assert!(group["priority"].as_integer().unwrap() > 0);
    }
}
