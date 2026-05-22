use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use super::shared::NIGHTLY_TOOLCHAIN;

#[derive(Args)]
pub(crate) struct FixArgs {}

impl FixArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;

        eprintln!("[clippy fix]");
        cmd!(sh, "cargo clippy --fix --allow-dirty --allow-staged").run()?;

        eprintln!("[fmt]");
        let toolchain = std::env::var("RUSTFMT_NIGHTLY_TOOLCHAIN")
            .unwrap_or_else(|_| NIGHTLY_TOOLCHAIN.to_owned());
        let toolchain = format!("+{toolchain}");
        cmd!(sh, "cargo {toolchain} fmt --all").run()?;

        eprintln!("[sort]");
        cmd!(sh, "cargo sort --workspace --grouped").run()?;

        Ok(())
    }
}
