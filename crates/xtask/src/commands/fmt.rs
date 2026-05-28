use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use super::shared::NIGHTLY_TOOLCHAIN;

#[derive(Args)]
pub(crate) struct FmtArgs {
    /// Check formatting without modifying files
    #[arg(long)]
    check: bool,
}

impl FmtArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let toolchain = format!(
            "+{}",
            std::env::var("RUSTFMT_NIGHTLY_TOOLCHAIN")
                .unwrap_or_else(|_| NIGHTLY_TOOLCHAIN.to_owned())
        );
        if self.check {
            cmd!(sh, "cargo {toolchain} fmt --all -- --check").run()?;
        } else {
            cmd!(sh, "cargo {toolchain} fmt --all").run()?;
        }
        Ok(())
    }
}
