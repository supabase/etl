use anyhow::{Context, Result};
use clap::Args;

use crate::{
    commands::local::{prepare_local_databases, require_command},
    utils::{DestinationPreset, workspace_root},
};

/// Arguments for setting up the local development environment.
#[derive(Args)]
pub(crate) struct InitArgs {}

impl InitArgs {
    /// Starts local Docker services and runs migrations.
    ///
    /// Does not write API or replicator configuration, and does not apply
    /// Kubernetes resources.
    pub(crate) fn run(self) -> Result<()> {
        let workspace_root = workspace_root()?;
        std::env::set_current_dir(&workspace_root).with_context(|| {
            format!("Failed to change directory to {}", workspace_root.display())
        })?;

        require_command("psql", &["--version"], "Postgres client (psql) is not installed")?;

        println!("== Local development environment ==");
        println!("This starts Postgres, ClickHouse, and the Iceberg catalog, and runs migrations.");
        println!(
            "It does not write API or replicator configuration or apply Kubernetes resources."
        );
        println!();

        let _config = prepare_local_databases()?;

        println!();
        println!("✨ Local environment is ready.");
        println!();
        print_next_steps();
        Ok(())
    }
}

/// Prints how to set up a service after `cargo x init`.
fn print_next_steps() {
    println!("If you want to set up the API:");
    println!("  cargo x setup api");
    println!("  cargo x run api");
    println!();
    println!("If you want a local replicator (ClickHouse by default):");
    println!("  cargo x setup replicator");
    println!("  cargo x seed");
    println!("  cargo x run replicator");
    println!();
    println!("Other destinations:");
    DestinationPreset::print_choices();
}
