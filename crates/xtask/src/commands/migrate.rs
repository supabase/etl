use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct MigrateArgs {
    /// Migration targets (etl-api, etl, all)
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

impl MigrateArgs {
    // TODO: port scripts/bin/run-migrations.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        run_migrations(&self.args)
    }
}

/// Runs database migrations with the provided migration targets.
pub(super) fn run_migrations(args: &[String]) -> Result<()> {
    run_migrations_with_env(args, std::iter::empty::<(&str, &str)>())
}

/// Runs database migrations with extra environment variables.
pub(super) fn run_migrations_with_env<'a>(
    args: &[String],
    envs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<()> {
    let sh = Shell::new()?;
    let mut cmd = cmd!(sh, "./scripts/bin/run-migrations.sh {args...}");
    for (key, value) in envs {
        cmd = cmd.env(key, value);
    }

    cmd.run()?;
    Ok(())
}
