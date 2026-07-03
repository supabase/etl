use anyhow::Result;
use clap::{Args, Subcommand};

use super::{RemoveHostaddrArgs, SetDucklakeMaintenanceModeArgs};

#[derive(Args)]
pub(crate) struct FixPipelineArgs {
    #[command(subcommand)]
    command: FixPipelineCommand,
}

#[derive(Subcommand)]
enum FixPipelineCommand {
    /// Remove the hostaddr field from a source configuration to force DNS
    /// resolution.
    #[command(name = "remove-hostaddr")]
    RemoveHostaddr(RemoveHostaddrArgs),
    /// Set the DuckLake maintenance mode on a pipeline's destination config.
    #[command(name = "set-ducklake-maintenance-mode")]
    SetDucklakeMaintenanceMode(SetDucklakeMaintenanceModeArgs),
}

impl FixPipelineArgs {
    pub(crate) async fn run(self) -> Result<()> {
        match self.command {
            FixPipelineCommand::RemoveHostaddr(cmd) => cmd.run().await,
            FixPipelineCommand::SetDucklakeMaintenanceMode(cmd) => cmd.run().await,
        }
    }
}
