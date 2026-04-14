mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::chaos::ChaosArgs;
use commands::nextest::NextestArgs;
use commands::postgres::PostgresArgs;

#[derive(Parser)]
#[command(name = "xtask", about = "Project task runner")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run chaos testing scenarios against the Kubernetes cluster
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
        Command::Chaos(cmd) => cmd.run().await,
        Command::Nextest(cmd) => cmd.run(),
        Command::Postgres(cmd) => cmd.run(),
    }
}
