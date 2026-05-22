mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{
    BenchmarkArgs, BenchmarkCompareArgs, ChaosArgs, CheckArgs, FixArgs, FmtArgs, MsrvArgs,
    NextestArgs, PostgresArgs,
};

#[derive(Parser)]
#[command(name = "xtask", about = "Project task runner")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Prepare and run ETL benchmarks.
    Benchmark(Box<BenchmarkArgs>),
    /// Compare benchmark reports with a previous run.
    #[command(name = "benchmark-compare")]
    BenchmarkCompare(BenchmarkCompareArgs),
    /// Run chaos testing scenarios against the Kubernetes cluster.
    Chaos(ChaosArgs),
    /// Pre-PR gate: fmt, sort, clippy.
    Check(CheckArgs),
    /// Auto-fix: clippy --fix, fmt, sort.
    Fix(FixArgs),
    /// Format code with nightly rustfmt.
    Fmt(FmtArgs),
    /// Verify MSRV consistency across Cargo.toml, rust-toolchain.toml, and
    /// cargo-msrv.
    Msrv(MsrvArgs),
    /// Run tests via nextest, sharded across multiple Postgres clusters.
    Nextest(NextestArgs),
    /// Manage test Postgres clusters.
    Postgres(PostgresArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Benchmark(cmd) => cmd.run(),
        Command::BenchmarkCompare(cmd) => cmd.run().await,
        Command::Chaos(cmd) => cmd.run().await,
        Command::Check(cmd) => cmd.run(),
        Command::Fix(cmd) => cmd.run(),
        Command::Fmt(cmd) => cmd.run(),
        Command::Msrv(cmd) => cmd.run(),
        Command::Nextest(cmd) => cmd.run(),
        Command::Postgres(cmd) => cmd.run(),
    }
}
