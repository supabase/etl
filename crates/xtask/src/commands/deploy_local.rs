use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct DeployLocalArgs {
    /// Arguments passed to the deploy script
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl DeployLocalArgs {
    // TODO: port scripts/deploy-local-replicator-orbstack.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let args = &self.args;
        cmd!(sh, "./scripts/deploy-local-replicator-orbstack.sh {args...}").run()?;
        Ok(())
    }
}
