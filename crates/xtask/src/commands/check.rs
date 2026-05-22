use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use super::shared::NIGHTLY_TOOLCHAIN;

#[derive(Args)]
pub(crate) struct CheckArgs {
    /// Extra arguments passed to nextest
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    nextest_args: Vec<String>,
}

impl CheckArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let toolchain = std::env::var("RUSTFMT_NIGHTLY_TOOLCHAIN")
            .unwrap_or_else(|_| NIGHTLY_TOOLCHAIN.to_owned());
        let toolchain = format!("+{toolchain}");

        eprintln!("[fmt]");
        cmd!(sh, "cargo {toolchain} fmt --all -- --check").run()?;

        eprintln!("[sort]");
        cmd!(sh, "cargo sort --workspace --grouped --check").run()?;

        eprintln!("[clippy]");
        cmd!(sh, "cargo clippy --all-targets --all-features --no-deps").run()?;

        eprintln!("[nextest]");
        let args = &self.nextest_args;
        cmd!(sh, "cargo xtask nextest run {args...}").run()?;

        Ok(())
    }
}
