use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct InitArgs {}

impl InitArgs {
    // TODO: port scripts/bin/init.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        cmd!(sh, "./scripts/bin/init.sh").run()?;
        Ok(())
    }
}
