mod commands;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::chaos::ChaosArgs;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Chaos(cmd) => cmd.run().await,
    }
}
