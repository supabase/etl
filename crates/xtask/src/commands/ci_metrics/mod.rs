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

use std::{env, net::IpAddr, str::FromStr};

use anyhow::{Context, Result, bail};
use chrono::{Days, NaiveDate, Utc};
use clap::{Args, Subcommand};
use secrecy::{ExposeSecret, SecretString};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};

use crate::github::GitHubClient;

/// Environment variable holding the database URL.
const DATABASE_URL_ENV: &str = "CI_METRICS_DATABASE_URL";
/// Environment variable naming the CA file that verifies the database server.
/// Overrides any `sslrootcert` in the URL, so the trusted CA is visible in the
/// workflow rather than hidden in a secret.
const SSL_ROOT_CERT_ENV: &str = "CI_METRICS_SSL_ROOT_CERT";
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

/// Fails unless the connection verifies the server's certificate and host
/// name. Loopback hosts and Unix sockets are exempt for local development.
fn require_verified_tls(host: &str, mode: PgSslMode) -> Result<()> {
    let local = host.starts_with('/')
        || host == "localhost"
        || host.trim_matches(['[', ']']).parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
    if local || matches!(mode, PgSslMode::VerifyFull) {
        return Ok(());
    }
    bail!("{DATABASE_URL_ENV} must use sslmode=verify-full for non-local hosts");
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

/// Connects using `CI_METRICS_DATABASE_URL` without echoing it.
async fn connect() -> Result<PgConnection> {
    let url = SecretString::from(
        env::var(DATABASE_URL_ENV).with_context(|| format!("{DATABASE_URL_ENV} is not set"))?,
    );
    let mut options = PgConnectOptions::from_str(url.expose_secret())
        .with_context(|| format!("{DATABASE_URL_ENV} is not a valid Postgres URL"))?
        .application_name(APPLICATION_NAME);
    if let Ok(path) = env::var(SSL_ROOT_CERT_ENV) {
        options = options.ssl_root_cert(path);
    }
    require_verified_tls(options.get_host(), options.get_ssl_mode())?;
    require_session_connection(options.get_port())?;
    PgConnection::connect_with(&options)
        .await
        .context("Failed to connect to the CI metrics database")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_hosts_require_verify_full() {
        let host = "pooler.example.com";
        assert!(require_verified_tls(host, PgSslMode::VerifyFull).is_ok());
        for mode in [
            PgSslMode::Disable,
            PgSslMode::Allow,
            PgSslMode::Prefer,
            PgSslMode::Require,
            PgSslMode::VerifyCa,
        ] {
            assert!(require_verified_tls(host, mode).is_err(), "{mode:?}");
        }
    }

    #[test]
    fn local_hosts_may_skip_tls() {
        for host in ["localhost", "127.0.0.1", "::1", "[::1]", "/var/run/postgresql"] {
            assert!(require_verified_tls(host, PgSslMode::Disable).is_ok(), "{host}");
        }
        // A name that merely starts like a loopback host is remote.
        assert!(require_verified_tls("localhost.example.com", PgSslMode::Require).is_err());
        assert!(require_verified_tls("127.0.0.1.example.com", PgSslMode::Require).is_err());
    }

    #[test]
    fn transaction_pooler_is_refused() {
        assert!(require_session_connection(TRANSACTION_POOLER_PORT).is_err());
        assert!(require_session_connection(5432).is_ok());
    }
}
