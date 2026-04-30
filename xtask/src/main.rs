mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{BenchmarkArgs, ChaosArgs, NextestArgs, PostgresArgs};

#[derive(Parser)]
#[command(name = "xtask", about = "Project task runner")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Prepare and run ETL benchmarks.
    Benchmark(BenchmarkArgs),
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
        Command::Chaos(cmd) => cmd.run().await,
        Command::Nextest(cmd) => cmd.run(),
        Command::Postgres(cmd) => cmd.run(),
    }
}
