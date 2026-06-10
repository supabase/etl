use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct VendorDuckdbArgs {
    /// Arguments passed to the vendor script
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl VendorDuckdbArgs {
    // TODO: port scripts/vendor-duckdb-extensions.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let args = &self.args;
        cmd!(sh, "./scripts/vendor-duckdb-extensions.sh {args...}").run()?;
        Ok(())
    }
}
