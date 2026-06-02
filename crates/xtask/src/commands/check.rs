use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use super::shared::{NIGHTLY_TOOLCHAIN, maybe_with_sccache};

#[derive(Args)]
pub(crate) struct CheckArgs {
    /// Enable `sccache`.
    /// Also enabled via `ETL_SCCACHE=1`.
    #[arg(long)]
    sccache: bool,
}

impl CheckArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let toolchain = std::env::var("RUSTFMT_NIGHTLY_TOOLCHAIN")
            .unwrap_or_else(|_| NIGHTLY_TOOLCHAIN.to_owned());
        let toolchain = format!("+{toolchain}");

        println!("[fmt]");
        cmd!(sh, "cargo {toolchain} fmt --all -- --check").run()?;

        println!("[sort]");
        cmd!(sh, "cargo sort --workspace --grouped --check").run()?;

        println!("[clippy]");
        maybe_with_sccache(
            cmd!(sh, "cargo clippy --all-targets --all-features --no-deps"),
            self.sccache,
        )
        .run()?;

        Ok(())
    }
}
