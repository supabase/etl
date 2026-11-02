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

use anyhow::{Context, Result, bail};
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
/// Default requests per sync. `GITHUB_TOKEN` allows 1,000 per hour per
/// repository, shared with every other workflow; an hourly sync needs about
/// 50-110 in steady state, and the daily recent-days refetch spreads over
/// several syncs.
const DEFAULT_MAX_REQUESTS: u64 = 400;
/// Port of Supabase's transaction-mode pooler. Each statement may run on a
/// different server connection there, which breaks the sync's session-level
/// advisory lock.
const TRANSACTION_POOLER_PORT: u16 = 6543;

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
    /// Runs the command. Errors are flattened and stripped of the database
    /// user, project ref and host, because they reach public CI logs: the
    /// Supabase pooler echoes `user.project_ref` when it cannot find a project.
    pub(crate) async fn run(self) -> Result<()> {
        let identifiers = connection_identifiers();
        self.run_unredacted().await.map_err(|error| redact(&error, &identifiers))
    }

    /// Runs the command with errors as they are.
    async fn run_unredacted(self) -> Result<()> {
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

/// Fails for transaction-mode pooler connections, where session state such as
/// the sync lock does not persist between statements.
fn require_session_connection(port: u16) -> Result<()> {
    if port == TRANSACTION_POOLER_PORT {
        bail!(
            "{DATABASE_URL_ENV} uses the transaction pooler (port {TRANSACTION_POOLER_PORT}); use \
             the session pooler or a direct connection"
        );
    }
    Ok(())
}

/// Returns the parts of `CI_METRICS_DATABASE_URL` that identify the database,
/// longest first: the host and, for pooler users of the form
/// `role.project_ref`, the user and the project ref. Postgres never echoes
/// passwords, so they are not listed.
fn connection_identifiers() -> Vec<String> {
    let Some(options) =
        env::var(DATABASE_URL_ENV).ok().and_then(|url| PgConnectOptions::from_str(&url).ok())
    else {
        return Vec::new();
    };
    let mut identifiers = vec![options.get_host().to_owned()];
    let user = options.get_username();
    if let Some((_, project_ref)) = user.split_once('.') {
        identifiers.extend([user.to_owned(), project_ref.to_owned()]);
    }
    identifiers.retain(|identifier| !identifier.is_empty());
    identifiers.sort_by_key(|identifier| std::cmp::Reverse(identifier.len()));
    identifiers
}

/// Flattens `error` and its causes into one message with `identifiers`
/// replaced.
fn redact(error: &anyhow::Error, identifiers: &[String]) -> anyhow::Error {
    let mut message = format!("{error:#}");
    for identifier in identifiers {
        message = message.replace(identifier.as_str(), "<redacted>");
    }
    anyhow::anyhow!(message)
}

/// Connects using `CI_METRICS_DATABASE_URL` without echoing it.
async fn connect() -> Result<PgConnection> {
    let url = SecretString::from(
        env::var(DATABASE_URL_ENV).with_context(|| format!("{DATABASE_URL_ENV} is not set"))?,
    );
    let options = PgConnectOptions::from_str(url.expose_secret())
        .with_context(|| format!("{DATABASE_URL_ENV} is not a valid Postgres URL"))?
        .application_name(APPLICATION_NAME);
    require_session_connection(options.get_port())?;
    PgConnection::connect_with(&options)
        .await
        .context("Failed to connect to the CI metrics database")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_pooler_is_refused() {
        assert!(require_session_connection(TRANSACTION_POOLER_PORT).is_err());
        assert!(require_session_connection(5432).is_ok());
    }

    /// Mirrors the pooler's error for an unknown project.
    #[test]
    fn errors_hide_database_identifiers() {
        let identifiers = vec![
            "aws-0-region.pooler.example.com".to_owned(),
            "ci_writer.abcdefghijklmnopqrst".to_owned(),
            "abcdefghijklmnopqrst".to_owned(),
        ];
        let error = anyhow::anyhow!(
            "(ENOTFOUND) tenant/user ci_writer.abcdefghijklmnopqrst not found at \
             aws-0-region.pooler.example.com, ref abcdefghijklmnopqrst"
        )
        .context("Failed to connect to the CI metrics database");

        assert_eq!(
            redact(&error, &identifiers).to_string(),
            "Failed to connect to the CI metrics database: (ENOTFOUND) tenant/user <redacted> not \
             found at <redacted>, ref <redacted>"
        );
    }
}
