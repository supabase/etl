//! Thin entry point for the independent experiment runner.

use std::process::Command;

use anyhow::{Result, ensure};
use clap::Args;

use crate::utils::maybe_configure_sccache;

/// Arguments forwarded unchanged to the experiment CLI.
#[derive(Args)]
pub(crate) struct ExperimentArgs {
    /// Experiment subcommand and arguments; secrets belong in environment
    /// variables.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl ExperimentArgs {
    /// Builds with the repository cache policy and runs the independent crate.
    pub(crate) fn run(self) -> Result<()> {
        let mut command = Command::new("cargo");
        command.args(["run", "--locked", "-p", "etl-experiments", "--"]).args(self.args);
        maybe_configure_sccache(&mut command, false);
        ensure!(command.status()?.success(), "Experiment command failed");
        Ok(())
    }
}
