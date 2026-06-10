use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

#[derive(Args)]
pub(crate) struct TestClickhouseArgs {}

impl TestClickhouseArgs {
    // TODO: port scripts/bin/test-clickhouse.sh to native Rust
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        cmd!(sh, "./scripts/bin/test-clickhouse.sh").run()?;
        Ok(())
    }
}
