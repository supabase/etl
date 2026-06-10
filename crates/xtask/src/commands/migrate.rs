use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct MigrateArgs {
    /// Migration targets (etl-api, etl, all)
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl MigrateArgs {
    // TODO: port scripts/run-migrations.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let args = &self.args;
        cmd!(sh, "./scripts/run-migrations.sh {args...}").run()?;
        Ok(())
    }
}
