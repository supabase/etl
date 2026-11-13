use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use crate::{
    commands::fmt::{FormatMode, format_workspace},
    utils::{CargoFeatureSelection, DefaultFeatureBehavior, DestinationPreset, maybe_with_sccache},
};

#[derive(Args)]
pub(crate) struct CheckArgs {
    /// Enable `sccache`. Also enabled via `ETL_SCCACHE=1`.
    #[arg(long)]
    sccache: bool,

    /// Destination preset for the clippy check pass.
    #[arg(long, value_enum)]
    destination: Option<DestinationPreset>,
}

impl CheckArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        println!("[fmt]");
        format_workspace(&sh, FormatMode::Check)?;

        println!("[sort]");
        cmd!(sh, "cargo sort --workspace --grouped --check").run()?;

        println!("[clippy]");
        let CheckArgs { sccache, destination } = self;
        let clippy = CargoFeatureSelection::for_destination(destination)
            .apply_to(cmd!(sh, "cargo clippy --locked --all-targets"), DefaultFeatureBehavior::All)
            .args(["--no-deps", "--", "-D", "warnings"]);
        maybe_with_sccache(clippy, sccache).run()?;

        Ok(())
    }
}
