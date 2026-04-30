mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{BenchmarkArgs, BenchmarkCompareArgs, ChaosArgs, NextestArgs, PostgresArgs};

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
    /// Run tests via nextest, sharded across multiple Postgres clusters
    Nextest(NextestArgs),
    /// Manage test Postgres clusters
    Postgres(PostgresArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Benchmark(cmd) => cmd.run(),
        Command::BenchmarkCompare(cmd) => cmd.run().await,
        Command::Chaos(cmd) => cmd.run().await,
        Command::Nextest(cmd) => cmd.run(),
        Command::Postgres(cmd) => cmd.run(),
    }
}
