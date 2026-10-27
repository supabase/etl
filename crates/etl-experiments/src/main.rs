//! Controlled staging experiments against ordinary hosted replication
//! pipelines.

mod journal;
mod observe;
mod plan;
mod platform;
mod run;
mod snowflake;
mod source;
mod staging;
mod workload;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::plan::{Plan, Profile};

/// Controlled experiment commands; no command implicitly deploys
/// infrastructure.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

/// Planning and reporting are offline; other commands require explicit staging
/// credentials.
#[derive(Subcommand)]
enum Command {
    /// Derive a fresh smoke, baseline, or double plan from a calibrated
    /// baseline.
    Plan {
        #[arg(long)]
        template: PathBuf,
        #[arg(long, value_enum)]
        profile: Profile,
        #[arg(long)]
        output: PathBuf,
    },
    /// Create and seed a fresh source schema, publication, and Snowflake
    /// schema.
    Prepare {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Run a previously prepared experiment; automated Platform launch is the
    /// default.
    Run {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Observe a run independently and stop it on runner loss or WAL budget
    /// breaches.
    Observe {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Combine recovered runner and observer evidence into a self-contained
    /// report.
    Report {
        #[arg(long,num_args=1..)]
        artifacts: Vec<PathBuf>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Render staging manifests locally; does not contact Kubernetes.
    Render {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        image: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, value_enum)]
        phase: staging::Phase,
    },
    /// Delete the owned pipeline and slots; optionally drop this run's
    /// synthetic schemas.
    Cleanup {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        drop_data: bool,
        #[arg(long)]
        abandoned: bool,
    },
}

/// Selects one TLS provider explicitly because HTTP and database dependencies
/// can enable both rustls providers in the same binary.
fn initialize_tls() -> Result<()> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .map_err(|_| anyhow::anyhow!("TLS provider initialization failed"))?;
    }
    Ok(())
}

/// Keeps diagnostics to the runner's sanitized errors, without dumping nested
/// provider responses.
async fn execute() -> Result<()> {
    initialize_tls()?;
    let args = Args::parse();
    match args.command {
        Command::Plan { template, profile, output } => {
            let plan = Plan::read(&template)?.derive(profile)?;
            let bytes = serde_json::to_vec_pretty(&plan)?;
            use std::io::Write;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output)?
                .write_all(&bytes)?;
            println!(
                "Run {}: {} tables, {} initial text bytes",
                plan.run_id,
                plan.table_count(),
                plan.fixture_bytes()?
            );
            Ok(())
        }
        Command::Prepare { plan, output } => run::prepare(&Plan::read(&plan)?, &output).await,
        Command::Run { plan, output } => run::run(&Plan::read(&plan)?, &output).await,
        Command::Observe { plan, output } => observe::observe(&Plan::read(&plan)?, &output).await,
        Command::Report { artifacts, output } => journal::report(&artifacts, &output),
        Command::Render { plan, image, output, phase } => {
            staging::render(&Plan::read(&plan)?, &image, &output, phase)
        }
        Command::Cleanup { plan, drop_data, abandoned } => {
            run::cleanup(&Plan::read(&plan)?, drop_data, abandoned).await
        }
    }
}

/// Prints only the sanitized outer diagnostic, never a provider source chain.
#[tokio::main]
async fn main() -> std::process::ExitCode {
    match execute().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::initialize_tls;

    #[test]
    fn initializes_tls_with_multiple_compiled_providers() {
        initialize_tls().unwrap();
        let _config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
    }
}
