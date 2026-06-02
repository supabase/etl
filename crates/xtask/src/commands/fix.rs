use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use super::shared::{NIGHTLY_TOOLCHAIN, maybe_with_sccache};

#[derive(Args)]
pub(crate) struct FixArgs {
    /// Enable `sccache`.
    /// Also enabled via `ETL_SCCACHE=1`.
    #[arg(long)]
    sccache: bool,
}

impl FixArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;

        println!("[clippy fix]");
        maybe_with_sccache(
            cmd!(sh, "cargo clippy --fix --allow-dirty --allow-staged"),
            self.sccache,
        )
        .run()?;

        println!("[fmt]");
        let toolchain = std::env::var("RUSTFMT_NIGHTLY_TOOLCHAIN")
            .unwrap_or_else(|_| NIGHTLY_TOOLCHAIN.to_owned());
        let toolchain = format!("+{toolchain}");
        cmd!(sh, "cargo {toolchain} fmt --all").run()?;

        println!("[sort]");
        cmd!(sh, "cargo sort --workspace --grouped").run()?;

        Ok(())
    }
}
