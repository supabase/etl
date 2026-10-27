//! `cargo x ci-metrics`: collects GitHub Actions data into Postgres for the
//! CI dashboard.
//!
//! `migrate` runs by hand as the database owner. `sync` runs hourly as
//! `ci_writer`, or locally for backfills. Both read the connection string from
//! `CI_METRICS_DATABASE_URL`; `sync` reads `GITHUB_TOKEN`. Neither value is
//! logged.

mod manifest;
mod model;
mod plan;
mod store;
mod sync;

use std::{env, str::FromStr};

use anyhow::{Context, Result};
use chrono::{Days, NaiveDate, Utc};
use clap::{Args, Subcommand};
use secrecy::{ExposeSecret, SecretString};
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};

use crate::github::GitHubClient;

/// Environment variable holding the database URL.
const DATABASE_URL_ENV: &str = "CI_METRICS_DATABASE_URL";
/// Environment variable holding the GitHub token.
const GITHUB_TOKEN_ENV: &str = "GITHUB_TOKEN";
/// User agent sent to GitHub.
const USER_AGENT: &str = "supabase-etl-ci-metrics";
/// Postgres `application_name` of collector sessions.
const APPLICATION_NAME: &str = "etl-ci-metrics";
/// Default history window, a little over six months.
const DEFAULT_HISTORY_DAYS: u64 = 190;
/// Default requests per sync; `GITHUB_TOKEN` allows 1,000 per hour.
const DEFAULT_MAX_REQUESTS: u64 = 800;

/// Collect GitHub Actions data into the CI metrics database.
#[derive(Args)]
pub(crate) struct CiMetricsArgs {
    #[command(subcommand)]
    command: CiMetricsCommand,
}

/// `ci-metrics` subcommands.
#[derive(Subcommand)]
enum CiMetricsCommand {
    /// Apply schema migrations. Run by hand as the database owner.
    Migrate,
    /// Sync runs, jobs, manifests and code-scanning data.
    Sync(SyncArgs),
}

/// Options of `ci-metrics sync`.
#[derive(Args)]
struct SyncArgs {
    /// Repository to collect, as `owner/name`.
    #[arg(long, default_value = "supabase/etl")]
    repo: String,
    /// First UTC day to cover. Stored; later syncs reuse it when omitted.
    #[arg(long)]
    history_since: Option<NaiveDate>,
    /// Most GitHub requests this sync may send.
    #[arg(long, default_value_t = DEFAULT_MAX_REQUESTS)]
    max_requests: u64,
}

impl CiMetricsArgs {
    pub(crate) async fn run(self) -> Result<()> {
        let mut conn = connect().await?;
        match self.command {
            CiMetricsCommand::Migrate => {
                store::migrate(&mut conn).await?;
                eprintln!("ci-metrics: migrations applied");
                Ok(())
            }
            CiMetricsCommand::Sync(args) => {
                let token = SecretString::from(
                    env::var(GITHUB_TOKEN_ENV)
                        .with_context(|| format!("{GITHUB_TOKEN_ENV} is not set"))?,
                );
                let client = GitHubClient::new(token, args.repo, USER_AGENT)?;
                let default_history_since = Utc::now()
                    .date_naive()
                    .checked_sub_days(Days::new(DEFAULT_HISTORY_DAYS))
                    .context("Default history start is out of range")?;
                sync::run(
                    &mut conn,
                    &client,
                    sync::SyncConfig {
                        history_since: args.history_since,
                        default_history_since,
                        max_requests: args.max_requests,
                    },
                )
                .await
            }
        }
    }
}

/// Connects using `CI_METRICS_DATABASE_URL` without echoing it.
async fn connect() -> Result<PgConnection> {
    let url = SecretString::from(
        env::var(DATABASE_URL_ENV).with_context(|| format!("{DATABASE_URL_ENV} is not set"))?,
    );
    let options = PgConnectOptions::from_str(url.expose_secret())
        .with_context(|| format!("{DATABASE_URL_ENV} is not a valid Postgres URL"))?
        .application_name(APPLICATION_NAME);
    PgConnection::connect_with(&options)
        .await
        .context("Failed to connect to the CI metrics database")
}
