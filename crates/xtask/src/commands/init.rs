use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct InitArgs {}

impl InitArgs {
    // TODO: port scripts/init.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        cmd!(sh, "./scripts/init.sh").run()?;
        Ok(())
    }
}
