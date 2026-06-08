use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use crate::utils::{
    CargoFeatureSelection, DefaultFeatureBehavior, DestinationPreset, NIGHTLY_TOOLCHAIN,
    maybe_with_sccache,
};

#[derive(Args)]
pub(crate) struct FixArgs {
    /// Enable `sccache`.
    /// Also enabled via `ETL_SCCACHE=1`.
    #[arg(long)]
    sccache: bool,

    /// Destination preset for the clippy fix pass.
    #[arg(long, value_enum)]
    destination: Option<DestinationPreset>,

    /// Disable default features for the clippy fix pass.
    #[arg(long)]
    no_default_features: bool,

    /// Space or comma separated features to enable for the clippy fix pass.
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    features: Vec<String>,

    /// Package to run the clippy fix pass for.
    #[arg(short = 'p', long = "package")]
    packages: Vec<String>,
}

impl FixArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let FixArgs { sccache, destination, no_default_features, features, packages } = self;

        let selection = CargoFeatureSelection::new(no_default_features, features, packages)
            .with_destination(destination);

        println!("[clippy fix]");
        let clippy = selection.apply_to(
            cmd!(sh, "cargo clippy --fix --allow-dirty --allow-staged"),
            DefaultFeatureBehavior::CargoDefault,
        );
        maybe_with_sccache(clippy, sccache).run()?;

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
