//! Local process runners for the API and standalone replicator.

use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};

use crate::{
    commands::setup::{
        api_config_dir, api_config_exists, detect_replicator_destination, replicator_config_dir,
        replicator_config_exists, replicator_config_has_placeholders,
    },
    utils::{DestinationPreset, workspace_root},
};

/// Arguments for `cargo x run`.
#[derive(Args)]
#[command(after_help = "\
Examples:
  cargo x init
  cargo x setup api && cargo x run api
  cargo x setup replicator && cargo x seed && cargo x run replicator
")]
pub(crate) struct RunArgs {
    #[command(subcommand)]
    target: RunTarget,
}

/// Service that can be started with generated local configuration.
#[derive(Subcommand)]
enum RunTarget {
    /// Start etl-api with the generated local configuration.
    Api,
    /// Start etl-replicator with the generated local configuration.
    Replicator(RunReplicatorArgs),
}

/// Options for `cargo x run replicator`.
#[derive(Args)]
struct RunReplicatorArgs {
    /// Destination feature to compile. Defaults to the destination in the
    /// generated config.
    #[arg(long, value_enum)]
    destination: Option<DestinationPreset>,
}

impl RunArgs {
    /// Starts the selected local service.
    pub(crate) fn run(self) -> Result<()> {
        match self.target {
            RunTarget::Api => run_api(),
            RunTarget::Replicator(args) => run_replicator(args),
        }
    }
}

/// Starts etl-api using generated local configuration.
fn run_api() -> Result<()> {
    let workspace_root = workspace_root()?;
    std::env::set_current_dir(&workspace_root)
        .with_context(|| format!("Failed to change directory to {}", workspace_root.display()))?;

    if !api_config_exists()? {
        bail!(
            "No local API configuration found.\n\nInitialize the API first (Postgres, Kubernetes \
             resources, and config):\n  cargo x setup api\n\nThen start it:\n  cargo x run api"
        );
    }

    let config_dir = api_config_dir()?;
    println!("▶️  Starting etl-api");
    println!("   config: {}", config_dir.display());
    println!("   health: http://127.0.0.1:8010/health_check");
    println!("   swagger: http://127.0.0.1:8010/swagger-ui");
    let status = Command::new("cargo")
        .args(["run", "-p", "etl-api", "--bin", "etl-api"])
        .env("APP_ENVIRONMENT", "dev")
        .env("APP_CONFIG_DIR", &config_dir)
        .status()
        .context("Failed to start etl-api")?;

    if !status.success() {
        bail!("etl-api exited unsuccessfully");
    }

    Ok(())
}

/// Starts etl-replicator using generated local configuration.
fn run_replicator(args: RunReplicatorArgs) -> Result<()> {
    let workspace_root = workspace_root()?;
    std::env::set_current_dir(&workspace_root)
        .with_context(|| format!("Failed to change directory to {}", workspace_root.display()))?;

    if !replicator_config_exists()? {
        bail!(
            "No local replicator configuration found.\n\nInitialize ClickHouse (the local \
             default):\n  cargo x setup replicator\n  cargo x seed\n\nOr pass --destination \
             bigquery, ducklake, iceberg, or snowflake.\n\nThen start it:\n  cargo x run \
             replicator"
        );
    }

    let config_dir = replicator_config_dir()?;
    let detected = detect_replicator_destination(&config_dir)?;
    let destination = resolve_replicator_destination(args.destination, detected)?;
    if replicator_config_has_placeholders(&config_dir)? {
        bail!(
            "Replicator configuration in {} still contains placeholder secrets.\nDo not put real \
             secrets in tracked files. Re-run:\n  cargo x setup replicator --destination {} \
             --interactive --force\nOr set APP_DESTINATION__* environment variables.",
            config_dir.display(),
            destination.feature()
        );
    }

    let feature = destination.feature();
    println!("▶️  Starting etl-replicator ({feature})");
    println!("   config: {}", config_dir.display());
    let status = Command::new("cargo")
        .args([
            "run",
            "-p",
            "etl-replicator",
            "--bin",
            "etl-replicator",
            "--no-default-features",
            "--features",
            feature,
        ])
        .env("APP_ENVIRONMENT", "dev")
        .env("APP_CONFIG_DIR", &config_dir)
        .status()
        .context("Failed to start etl-replicator")?;

    if !status.success() {
        bail!("etl-replicator exited unsuccessfully");
    }

    Ok(())
}

/// Resolves the destination feature to compile from the requested flag and the
/// generated config.
fn resolve_replicator_destination(
    requested: Option<DestinationPreset>,
    detected: DestinationPreset,
) -> Result<DestinationPreset> {
    match requested {
        Some(requested) if requested != detected => bail!(
            "--destination {} does not match config ({}). Re-run `cargo x setup replicator \
             --destination {}` or omit --destination.",
            requested.feature(),
            detected.feature(),
            detected.feature()
        ),
        Some(requested) => Ok(requested),
        None => Ok(detected),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_replicator_destination_uses_detected_when_unspecified() {
        let destination =
            resolve_replicator_destination(None, DestinationPreset::ClickHouse).unwrap();
        assert_eq!(destination, DestinationPreset::ClickHouse);
    }

    #[test]
    fn resolve_replicator_destination_accepts_matching_flag() {
        let destination = resolve_replicator_destination(
            Some(DestinationPreset::BigQuery),
            DestinationPreset::BigQuery,
        )
        .unwrap();
        assert_eq!(destination, DestinationPreset::BigQuery);
    }

    #[test]
    fn resolve_replicator_destination_rejects_mismatch() {
        let error = resolve_replicator_destination(
            Some(DestinationPreset::BigQuery),
            DestinationPreset::ClickHouse,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("--destination bigquery does not match config (clickhouse)")
        );
    }
}
