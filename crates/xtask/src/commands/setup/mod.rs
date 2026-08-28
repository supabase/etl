//! Local configuration generation for the API and standalone replicator.

mod api;
mod replicator;

use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use api::write_api_config;
use clap::{Args, Subcommand};
use replicator::{IcebergCatalog, ReplicatorSetup, write_replicator_config};
pub(crate) use replicator::{detect_replicator_destination, replicator_config_has_placeholders};

use crate::{
    commands::local::{
        InitConfig, configure_kubernetes, prepare_local_databases, require_command,
        seed_default_replicator_image, wait_for_iceberg_catalog,
    },
    utils::{DestinationPreset, workspace_root},
};

/// Arguments for `cargo x setup`.
#[derive(Args)]
#[command(after_help = "\
Examples:
  cargo x init
  cargo x setup api && cargo x run api
  cargo x setup replicator && cargo x seed && cargo x run replicator
")]
pub(crate) struct SetupArgs {
    #[command(subcommand)]
    target: SetupTarget,
}

/// Service that can be initialized for local development.
#[derive(Subcommand)]
enum SetupTarget {
    /// Install API prerequisites and generate gitignored local configuration.
    Api(SetupApiArgs),
    /// Generate gitignored local replicator configuration.
    Replicator(SetupReplicatorArgs),
}

/// Options for `cargo x setup api`.
#[derive(Args)]
struct SetupApiArgs {
    /// Overwrite an existing API configuration directory.
    #[arg(long)]
    force: bool,
}

/// Options for `cargo x setup replicator`.
#[derive(Args)]
struct SetupReplicatorArgs {
    /// Destination to configure. Defaults to ClickHouse from `cargo x init`.
    #[arg(long, value_enum)]
    destination: Option<DestinationPreset>,
    /// Iceberg catalog kind. Used only with `--destination iceberg`.
    #[arg(long, value_enum)]
    iceberg_catalog: Option<IcebergCatalog>,
    /// Prompt for Postgres, publication, and destination values.
    #[arg(long)]
    interactive: bool,
    /// Overwrite an existing replicator configuration directory.
    #[arg(long)]
    force: bool,
    /// Postgres publication name.
    #[arg(long)]
    publication: Option<String>,
    /// Path to a BigQuery service-account JSON file.
    #[arg(long)]
    bq_sa_key_file: Option<PathBuf>,
}

impl SetupArgs {
    /// Initializes local configuration for the selected service.
    pub(crate) fn run(self) -> Result<()> {
        match self.target {
            SetupTarget::Api(args) => args.run(),
            SetupTarget::Replicator(args) => args.run(),
        }
    }
}

impl SetupApiArgs {
    /// Writes API configuration and applies Kubernetes resources.
    fn run(self) -> Result<()> {
        let workspace_root = workspace_root()?;
        std::env::set_current_dir(&workspace_root).with_context(|| {
            format!("Failed to change directory to {}", workspace_root.display())
        })?;

        print_banner("ETL API local setup");
        println!(
            "This writes gitignored API configuration and seeds the default replicator image."
        );
        println!("If the local stack is not running, it starts Docker and Kubernetes first.");
        println!();

        require_command("psql", &["--version"], "Postgres client (psql) is not installed")?;
        require_command("kubectl", &["version", "--client"], "Kubectl is not installed")?;

        let config = prepare_local_databases()?;
        seed_default_replicator_image(&config.database_url())?;
        configure_kubernetes()?;
        write_api_config(&config, self.force)?;

        println!();
        println!("✅ ETL API is ready to start.");
        print_api_next_steps();
        Ok(())
    }
}

impl SetupReplicatorArgs {
    /// Writes gitignored replicator configuration for the selected destination.
    fn run(self) -> Result<()> {
        let workspace_root = workspace_root()?;
        std::env::set_current_dir(&workspace_root).with_context(|| {
            format!("Failed to change directory to {}", workspace_root.display())
        })?;

        require_command("psql", &["--version"], "Postgres client (psql) is not installed")?;

        print_banner("ETL replicator local setup");
        println!("This writes gitignored files in crates/etl-replicator/configuration/.");
        println!("Local destinations use Docker values from cargo x init.");
        println!(
            "Cloud destinations get fake identifiers; override secrets with APP_DESTINATION__*."
        );
        println!();
        DestinationPreset::print_choices();

        let defaults = InitConfig::from_env();
        let destination = if self.interactive && self.destination.is_none() {
            prompt_destination(DestinationPreset::local_default())?
        } else {
            self.destination.unwrap_or(DestinationPreset::local_default())
        };
        if self.destination.is_none() && !self.interactive {
            println!(
                "Using ClickHouse (local default from cargo x init). Pass --destination bigquery \
                 for BigQuery."
            );
            println!();
        }

        let iceberg_catalog = match destination {
            DestinationPreset::Iceberg if self.interactive && self.iceberg_catalog.is_none() => {
                prompt_iceberg_catalog(IcebergCatalog::Rest)?
            }
            DestinationPreset::Iceberg => self.iceberg_catalog.unwrap_or(IcebergCatalog::Rest),
            _ => {
                if self.iceberg_catalog.is_some() {
                    println!("Ignoring --iceberg-catalog because the destination is not iceberg.");
                    println!();
                }
                IcebergCatalog::Rest
            }
        };

        print_destination_prerequisites(destination, iceberg_catalog);
        println!();

        let mut options = ReplicatorSetup::for_destination(&defaults, destination, iceberg_catalog);
        if let Some(publication) = self.publication {
            options.publication = publication;
        }
        if let Some(path) = &self.bq_sa_key_file {
            options.set_bigquery_service_account_key(
                &fs::read_to_string(path)
                    .with_context(|| format!("Failed to read {}", path.display()))?,
            )?;
        }
        if self.interactive {
            println!("Interactive mode: press Enter to keep each default.");
            options.prompt_destination()?;
            println!();
        }

        if options.needs_local_iceberg_catalog() {
            wait_for_iceberg_catalog()?;
        }

        write_replicator_config(&options, self.force)?;
        options.release_conflicting_apply_slot()?;

        println!();
        println!("✅ Replicator configuration is ready.");
        print_replicator_next_steps(options.preset(), iceberg_catalog);
        Ok(())
    }
}

/// Prints a section heading for local setup output.
fn print_banner(title: &str) {
    println!();
    println!("== {title} ==");
}

/// Prints destination-specific setup notes before files are written.
fn print_destination_prerequisites(
    destination: DestinationPreset,
    iceberg_catalog: IcebergCatalog,
) {
    match destination {
        DestinationPreset::BigQuery => {
            println!("BigQuery setup writes fake project/dataset ids and a placeholder key.");
            println!("Override the key before a real run:");
            println!("  export APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY=\"$(cat sa.json)\"");
        }
        DestinationPreset::ClickHouse => {
            println!("ClickHouse setup uses the Docker Compose service from cargo x init.");
            println!("No cloud credentials are required.");
            println!("After setup: cargo x seed && cargo x run replicator");
        }
        DestinationPreset::DuckLake => {
            println!("DuckLake setup writes catalog and data-path fields only.");
            println!(
                "Create the catalog database if it does not exist, then supply object-storage"
            );
            println!("credentials with APP_DESTINATION__DUCKLAKE__S3_*.");
        }
        DestinationPreset::Iceberg => match iceberg_catalog {
            IcebergCatalog::Rest => {
                println!(
                    "Iceberg REST setup uses the Lakekeeper catalog and MinIO from cargo x init."
                );
                println!(
                    "After setup: cargo x seed && cargo x run replicator --destination iceberg"
                );
            }
            IcebergCatalog::Supabase => {
                println!(
                    "Iceberg Supabase setup writes a fake project ref and placeholder secrets."
                );
                println!("Override APP_DESTINATION__ICEBERG__SUPABASE__* before a real run.");
            }
        },
        DestinationPreset::Snowflake => {
            println!("Snowflake setup writes a fake account and a placeholder private key.");
            println!("Override APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY before a real run.");
        }
    }
}

/// Prompts for a destination, using `default` when the user presses Enter.
fn prompt_destination(default: DestinationPreset) -> Result<DestinationPreset> {
    let selected = prompt(
        "Destination (clickhouse, bigquery, ducklake, iceberg, snowflake)",
        default.feature(),
    )?;
    DestinationPreset::parse(&selected)
}

/// Prompts for an Iceberg catalog, using `default` when the user presses Enter.
fn prompt_iceberg_catalog(default: IcebergCatalog) -> Result<IcebergCatalog> {
    let selected = prompt("Iceberg catalog (rest, supabase)", default.as_str())?;
    IcebergCatalog::parse(&selected)
}

/// Prints how to start the API after setup.
fn print_api_next_steps() {
    println!("API:");
    println!("  cargo x run api");
    println!("  Health      http://127.0.0.1:8010/health_check");
    println!("  Swagger UI  http://127.0.0.1:8010/swagger-ui");
    println!("  Internal    http://127.0.0.1:8081/health_check");
    println!("  Keys        crates/etl-api/configuration/dev.yaml (gitignored)");
    println!("  Rotate      cargo x setup api --force");
}

/// Prints how to start the replicator after setup.
fn print_replicator_next_steps(destination: DestinationPreset, iceberg_catalog: IcebergCatalog) {
    println!("Replicator:");
    match destination {
        DestinationPreset::ClickHouse => {
            println!("  cargo x seed");
            println!("  cargo x run replicator");
        }
        DestinationPreset::BigQuery => {
            println!("  export APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY=\"$(cat sa.json)\"");
            println!("  cargo x run replicator --destination bigquery");
        }
        DestinationPreset::DuckLake => {
            println!("  export APP_DESTINATION__DUCKLAKE__S3_ACCESS_KEY_ID");
            println!("  export APP_DESTINATION__DUCKLAKE__S3_SECRET_ACCESS_KEY");
            println!("  cargo x run replicator --destination ducklake");
        }
        DestinationPreset::Iceberg => match iceberg_catalog {
            IcebergCatalog::Rest => {
                println!("  cargo x seed");
                println!("  cargo x run replicator --destination iceberg");
            }
            IcebergCatalog::Supabase => {
                println!("  export APP_DESTINATION__ICEBERG__SUPABASE__CATALOG_TOKEN");
                println!("  export APP_DESTINATION__ICEBERG__SUPABASE__S3_ACCESS_KEY_ID");
                println!("  export APP_DESTINATION__ICEBERG__SUPABASE__S3_SECRET_ACCESS_KEY");
                println!("  cargo x run replicator --destination iceberg");
            }
        },
        DestinationPreset::Snowflake => {
            println!("  export APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY=\"$(cat rsa_key.p8)\"");
            println!("  cargo x run replicator --destination snowflake");
        }
    }
}

/// Reads one line from stdin, using `default` when the user presses Enter.
pub(super) fn prompt(label: &str, default: &str) -> Result<String> {
    print!("{label} [{default}]: ");
    finish_prompt(default)
}

/// Reads a secret from stdin without echoing `default`.
///
/// Empty input keeps `default`. Passwords, usernames, account identifiers, and
/// connection strings that embed credentials must use this so they are not
/// written to the terminal.
pub(super) fn prompt_secret(label: &str, default: &str) -> Result<String> {
    print!("{label} [hidden]: ");
    finish_prompt(default)
}

/// Flushes the prompt and returns the trimmed line or `default`.
fn finish_prompt(default: &str) -> Result<String> {
    io::stdout().flush().context("Failed to flush prompt")?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).context("Failed to read prompt")?;
    let trimmed = line.trim();
    if trimmed.is_empty() { Ok(default.to_owned()) } else { Ok(trimmed.to_owned()) }
}

/// Returns the gitignored API configuration directory.
pub(crate) fn api_config_dir() -> Result<PathBuf> {
    Ok(workspace_root()?.join("crates/etl-api/configuration"))
}

/// Returns the gitignored replicator configuration directory.
pub(crate) fn replicator_config_dir() -> Result<PathBuf> {
    Ok(workspace_root()?.join("crates/etl-replicator/configuration"))
}

/// Returns whether local API configuration already exists.
pub(crate) fn api_config_exists() -> Result<bool> {
    Ok(api_config_dir()?.join("dev.yaml").is_file())
}

/// Returns whether local replicator configuration already exists.
pub(crate) fn replicator_config_exists() -> Result<bool> {
    Ok(replicator_config_dir()?.join("dev.yaml").is_file())
}

/// Creates `directory` when it is missing.
pub(super) fn ensure_config_dir(directory: &Path) -> Result<()> {
    if directory.is_dir() {
        return Ok(());
    }

    fs::create_dir_all(directory)
        .with_context(|| format!("Failed to create {}", directory.display()))?;
    println!("📁 Created {}", directory.display());
    Ok(())
}

/// Writes `contents` unless the file exists and `force` is false.
pub(super) fn write_file(path: &Path, contents: &str, force: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_config_dir(parent)?;
    }

    if path.is_file() && !force {
        println!("♻️  Keeping existing {}", path.display());
        return Ok(());
    }

    fs::write(path, contents).with_context(|| format!("Failed to write {}", path.display()))?;
    println!("📝 Wrote {}", path.display());
    Ok(())
}

/// Quotes a YAML string value.
pub(super) fn yaml_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_destination_accepts_every_built_in() {
        for destination in DestinationPreset::all() {
            assert_eq!(DestinationPreset::parse(destination.feature()).unwrap(), destination);
        }
    }

    #[test]
    fn local_default_destination_is_clickhouse() {
        assert_eq!(DestinationPreset::local_default(), DestinationPreset::ClickHouse);
        assert_eq!(DestinationPreset::all()[0], DestinationPreset::ClickHouse);
    }

    #[test]
    fn ensure_config_dir_creates_missing_directory() {
        let directory = std::env::temp_dir()
            .join(format!("etl-xtask-config-{}", std::process::id()))
            .join("configuration");
        let _ = fs::remove_dir_all(directory.parent().unwrap());
        assert!(!directory.exists());
        ensure_config_dir(&directory).unwrap();
        assert!(directory.is_dir());
        fs::remove_dir_all(directory.parent().unwrap()).unwrap();
    }
}
