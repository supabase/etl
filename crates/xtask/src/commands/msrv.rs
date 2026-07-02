use std::{env, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use xshell::{Shell, cmd};

/// Arguments for verifying workspace MSRV consistency.
#[derive(Args)]
pub(crate) struct MsrvArgs {
    /// Also verify that the workspace compiles with the declared MSRV.
    #[arg(long)]
    verify: bool,
}

impl MsrvArgs {
    /// Verifies that all MSRV declarations agree.
    pub(crate) fn run(self) -> Result<()> {
        let workspace_root = workspace_root()?;
        env::set_current_dir(&workspace_root).with_context(|| {
            format!("failed to change directory to {}", workspace_root.display())
        })?;

        let sh = Shell::new()?;

        if cmd!(sh, "which cargo-msrv").quiet().run().is_err() {
            bail!("cargo-msrv is not installed. Install it with: cargo install cargo-msrv");
        }

        let cargo_toml = sh.read_file("Cargo.toml").context("failed to read Cargo.toml")?;
        let cargo_doc: toml::Table = cargo_toml.parse().context("failed to parse Cargo.toml")?;
        let cargo_msrv = cargo_doc
            .get("workspace")
            .and_then(|w| w.get("package"))
            .and_then(|p| p.get("rust-version"))
            .and_then(|v| v.as_str())
            .context("failed to read workspace.package.rust-version from Cargo.toml")?
            .to_owned();

        let toolchain_toml =
            sh.read_file("rust-toolchain.toml").context("failed to read rust-toolchain.toml")?;
        let toolchain_doc: toml::Table =
            toolchain_toml.parse().context("failed to parse rust-toolchain.toml")?;
        let toolchain_msrv = toolchain_doc
            .get("toolchain")
            .and_then(|t| t.get("channel"))
            .and_then(|v| v.as_str())
            .context("failed to read toolchain.channel from rust-toolchain.toml")?
            .to_owned();

        let resolved_msrv = cmd!(
            sh,
            "cargo msrv show --manifest-path crates/etl/Cargo.toml --output-format minimal"
        )
        .read()
        .context("failed to run cargo msrv show")?;
        let resolved_msrv = resolved_msrv.trim().to_owned();

        if cargo_msrv != toolchain_msrv {
            bail!(
                "workspace rust-version ({cargo_msrv}) does not match rust-toolchain channel \
                 ({toolchain_msrv})"
            );
        }

        if cargo_msrv != resolved_msrv {
            bail!(
                "workspace rust-version ({cargo_msrv}) does not match cargo-msrv output \
                 ({resolved_msrv})"
            );
        }

        println!("verified msrv sync at rust {cargo_msrv}");

        if self.verify {
            println!("verifying workspace compiles with MSRV {cargo_msrv}...");
            cmd!(
                sh,
                "cargo msrv verify
                    --manifest-path crates/etl/Cargo.toml
                    --output-format minimal
                    -- cargo check --workspace --all-features --locked"
            )
            .run()
            .context("cargo msrv verify failed")?;
            println!("verified workspace compiles with MSRV {cargo_msrv}");
        }

        Ok(())
    }
}

/// Returns the workspace root containing the xtask crate.
fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|crates_dir| crates_dir.parent())
        .map(PathBuf::from)
        .context("failed to determine workspace root")
}
