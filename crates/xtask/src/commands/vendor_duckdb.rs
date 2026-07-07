use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct VendorDuckdbArgs {
    /// Arguments passed to the DuckDB extension vendor helper.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl VendorDuckdbArgs {
    /// Runs the shared vendor helper used by both xtask and Docker builds.
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let args = &self.args;
        cmd!(sh, "./scripts/bin/vendor-duckdb-extensions.sh {args...}").run()?;
        Ok(())
    }
}
