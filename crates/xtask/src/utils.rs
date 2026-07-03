use std::{
    io::{self, Write as _},
    path::PathBuf,
    process::Command,
};

use anyhow::{Context, Result};
use clap::ValueEnum;
use xshell::Cmd;

/// Default number of local Postgres shards used by xtask commands.
pub(crate) const DEFAULT_PG_SHARD_COUNT: u16 = 3;
/// Default port for the first local Postgres shard.
pub(crate) const DEFAULT_BASE_PORT: u16 = 5430;
/// Offset applied to local read-replica Postgres ports.
pub(crate) const READ_REPLICA_PORT_OFFSET: u16 = 1000;
/// Nightly rustfmt toolchain used by formatting commands.
pub(crate) const NIGHTLY_TOOLCHAIN: &str = "nightly-2026-04-15";

/// Packages covered by destination-focused xtask presets.
const DESTINATION_PACKAGES: &[&str] = &["etl-destinations", "etl-api", "etl-replicator"];
/// ANSI yellow foreground color.
const YELLOW: &str = "\x1b[33m";
/// ANSI cyan foreground color.
const CYAN: &str = "\x1b[36m";
/// ANSI reset sequence.
const RESET: &str = "\x1b[0m";

/// Returns the workspace root containing the xtask crate.
pub(crate) fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates_dir| crates_dir.parent())
        .map(PathBuf::from)
        .context("Failed to determine workspace root")
}

/// Default feature behavior for a Cargo command.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DefaultFeatureBehavior {
    /// Pass `--all-features` unless explicitly disabled.
    All,
    /// Leave Cargo's default feature behavior unchanged unless explicitly
    /// disabled.
    CargoDefault,
}

/// Package and feature selection for destination-aware Cargo commands.
#[derive(Debug, Default)]
pub(crate) struct CargoFeatureSelection {
    no_default_features: bool,
    features: Vec<String>,
    packages: Vec<String>,
}

impl CargoFeatureSelection {
    /// Creates a package and feature selection from command-line arguments.
    pub(crate) fn new(
        no_default_features: bool,
        features: Vec<String>,
        packages: Vec<String>,
    ) -> Self {
        Self { no_default_features, features, packages }
    }

    /// Creates a package and feature selection for an optional destination
    /// preset.
    pub(crate) fn for_destination(destination: Option<DestinationPreset>) -> Self {
        Self::default().with_destination(destination)
    }

    /// Applies an optional destination preset to this selection.
    pub(crate) fn with_destination(mut self, destination: Option<DestinationPreset>) -> Self {
        if let Some(destination) = destination {
            self.apply_destination(destination);
        }
        self
    }

    /// Applies this selection to a Cargo command.
    pub(crate) fn apply_to(
        self,
        mut cmd: Cmd<'_>,
        default_features: DefaultFeatureBehavior,
    ) -> Cmd<'_> {
        for package in &self.packages {
            cmd = cmd.args(["-p", package]);
        }

        if self.no_default_features {
            cmd = cmd.arg("--no-default-features");
        } else if matches!(default_features, DefaultFeatureBehavior::All) {
            cmd = cmd.arg("--all-features");
        }

        let features = self.features.join(",");
        if !features.is_empty() {
            cmd = cmd.args(["--features", &features]);
        }

        cmd
    }

    /// Applies a destination preset to this selection.
    fn apply_destination(&mut self, destination: DestinationPreset) {
        self.no_default_features = true;
        push_unique(&mut self.features, destination.feature());
        for package in DESTINATION_PACKAGES {
            push_unique(&mut self.packages, package);
        }
    }
}

/// Destination-specific xtask preset.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum DestinationPreset {
    /// BigQuery destination.
    #[value(name = "bigquery")]
    BigQuery,
    /// ClickHouse destination.
    #[value(name = "clickhouse")]
    ClickHouse,
    /// DuckLake destination.
    #[value(name = "ducklake")]
    DuckLake,
    /// Iceberg destination.
    #[value(name = "iceberg")]
    Iceberg,
    /// Snowflake destination.
    #[value(name = "snowflake")]
    Snowflake,
}

impl DestinationPreset {
    /// Returns the Cargo feature for this destination.
    fn feature(self) -> &'static str {
        match self {
            DestinationPreset::BigQuery => "bigquery",
            DestinationPreset::ClickHouse => "clickhouse",
            DestinationPreset::DuckLake => "ducklake",
            DestinationPreset::Iceberg => "iceberg",
            DestinationPreset::Snowflake => "snowflake",
        }
    }
}

/// Returns whether sccache should be enabled for the spawned build.
///
/// Enabled when the `--sccache` flag is passed (`flag`) or when the
/// `ETL_SCCACHE` environment variable is set to `1`.
pub(crate) fn sccache_enabled(flag: bool) -> bool {
    flag || std::env::var("ETL_SCCACHE").is_ok_and(|value| value == "1")
}

/// Conditionally injects the sccache environment into a cargo command.
///
/// `CARGO_INCREMENTAL=0` is required because sccache cannot cache incremental
/// compilation. When disabled, the command is returned unchanged so the default
/// inner loop keeps incremental.
pub(crate) fn maybe_with_sccache(cmd: Cmd<'_>, flag: bool) -> Cmd<'_> {
    let Some(env) = sccache_env(flag) else {
        return cmd;
    };

    env.apply_to_xshell(cmd)
}

/// Conditionally injects the sccache environment into a process command.
pub(crate) fn maybe_configure_sccache(cmd: &mut Command, flag: bool) {
    if let Some(env) = sccache_env(flag) {
        env.apply_to_process(cmd);
    }
}

/// Pushes a value unless it is already present.
fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|item| item == value) {
        values.push(value.to_owned());
    }
}

/// Flushes stdout after status messages before command echoes are printed.
fn flush_stdout() {
    let _ = io::stdout().flush();
}

/// Environment values needed to route builds through sccache.
struct SccacheEnv {
    /// C compiler invoked through sccache.
    cc: String,
    /// C++ compiler invoked through sccache.
    cxx: String,
}

impl SccacheEnv {
    /// Applies this sccache configuration to an xshell command.
    fn apply_to_xshell(self, cmd: Cmd<'_>) -> Cmd<'_> {
        cmd.env("RUSTC_WRAPPER", "sccache")
            .env("CC", format!("sccache {}", self.cc))
            .env("CXX", format!("sccache {}", self.cxx))
            .env("CARGO_INCREMENTAL", "0")
            .env("SCCACHE_CACHE_SIZE", "20G")
    }

    /// Applies this sccache configuration to a process command.
    fn apply_to_process(&self, cmd: &mut Command) {
        cmd.env("RUSTC_WRAPPER", "sccache");
        cmd.env("CC", format!("sccache {}", self.cc));
        cmd.env("CXX", format!("sccache {}", self.cxx));
        cmd.env("CARGO_INCREMENTAL", "0");
        cmd.env("SCCACHE_CACHE_SIZE", "20G");
    }
}

/// Resolves the sccache environment requested for a cargo command.
fn sccache_env(flag: bool) -> Option<SccacheEnv> {
    if !sccache_enabled(flag) {
        println!(
            "{YELLOW}⏭ sccache disabled for cargo builds. Set ETL_SCCACHE=1 or pass --sccache to \
             enable.{RESET}"
        );
        flush_stdout();
        return None;
    }

    if !sccache_available() {
        println!(
            "{YELLOW}⚠️  sccache requested (--sccache / ETL_SCCACHE=1) but not found on PATH; \
             continuing without it. Install it with `brew install sccache`.{RESET}"
        );
        flush_stdout();
        return None;
    }

    println!("{CYAN}⚡ sccache enabled for cargo builds.{RESET}");
    flush_stdout();

    Some(SccacheEnv {
        cc: std::env::var("CC").unwrap_or_else(|_| "cc".into()),
        cxx: std::env::var("CXX").unwrap_or_else(|_| "c++".into()),
    })
}

/// Returns whether the `sccache` binary is callable on the current PATH.
fn sccache_available() -> bool {
    Command::new("sccache").arg("--version").output().is_ok()
}
