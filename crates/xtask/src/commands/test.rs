use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use crate::utils::{
    CargoFeatureSelection, DefaultFeatureBehavior, DestinationPreset, maybe_with_sccache,
};

#[derive(Args)]
pub(crate) struct TestArgs {
    /// Enable `sccache`.
    /// Also enabled via `ETL_SCCACHE=1`.
    #[arg(long)]
    sccache: bool,

    /// Destination preset for the test pass.
    #[arg(long, value_enum)]
    destination: Option<DestinationPreset>,

    /// Disable default features for the test pass.
    #[arg(long)]
    no_default_features: bool,

    /// Space or comma separated features to enable for the test pass.
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    features: Vec<String>,

    /// Package to run tests for.
    #[arg(short = 'p', long = "package")]
    packages: Vec<String>,

    /// Nextest filterset expression.
    #[arg(short = 'E', long = "filterset")]
    filterset: Option<String>,

    /// Build test binaries without running them.
    #[arg(long)]
    no_run: bool,

    /// Test name filters forwarded to nextest.
    filters: Vec<String>,

    /// Extra libtest-style arguments forwarded after `--`.
    #[arg(last = true)]
    extra: Vec<String>,
}

impl TestArgs {
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let TestArgs {
            sccache,
            destination,
            no_default_features,
            features,
            packages,
            filterset,
            no_run,
            filters,
            extra,
        } = self;

        let uses_explicit_packages = destination.is_some() || !packages.is_empty();
        let selection = CargoFeatureSelection::new(no_default_features, features, packages)
            .with_destination(destination);

        println!("[nextest]");
        let mut nextest = cmd!(sh, "cargo nextest run");
        if !no_run {
            nextest = nextest.arg("--no-fail-fast");
        }

        if !uses_explicit_packages {
            nextest = nextest.arg("--workspace");
        }

        nextest = selection.apply_to(nextest, DefaultFeatureBehavior::All);

        if let Some(filterset) = filterset {
            nextest = nextest.args(["--filterset", &filterset]);
        }

        if no_run {
            nextest = nextest.arg("--no-run");
        }

        nextest = nextest.args(filters);

        if !extra.is_empty() {
            nextest = nextest.arg("--").args(extra);
        }

        maybe_with_sccache(nextest, sccache).run()?;

        Ok(())
    }
}
