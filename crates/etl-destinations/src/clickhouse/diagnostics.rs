use std::fmt;

use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, info, warn};

use crate::clickhouse::client::ClickHouseClient;

/// How the ClickHouse server is deployed, as reported by its `cloud_mode`
/// setting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClickHouseDeployment {
    /// ClickHouse Cloud, which sets `cloud_mode` to `1`.
    Cloud,
    /// A server with `cloud_mode` at its default of `0`.
    SelfHosted,
    /// The setting was missing, unreadable, or had an unexpected value.
    Unknown,
}

impl ClickHouseDeployment {
    /// Classifies a `cloud_mode` setting value.
    ///
    /// [`None`] means the server has no such setting, which older servers
    /// lack.
    fn from_cloud_mode(value: Option<&str>) -> Self {
        match value {
            Some("1") => Self::Cloud,
            Some("0") => Self::SelfHosted,
            _ => Self::Unknown,
        }
    }

    /// Returns the log value for this deployment.
    fn as_str(self) -> &'static str {
        match self {
            Self::Cloud => "cloud",
            Self::SelfHosted => "self_hosted",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for ClickHouseDeployment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Spawns a one-shot task that logs ClickHouse server facts for debugging.
///
/// The task never affects replication: each query failure is logged and
/// reported as an unknown value. It ends after at most two
/// connectivity-check timeouts, or when the returned handle is dropped.
pub(super) fn spawn_server_diagnostics(client: ClickHouseClient) -> AbortOnDropHandle<()> {
    AbortOnDropHandle::new(tokio::spawn(log_server_diagnostics(client).in_current_span()))
}

/// Queries server facts and logs them in one `clickhouse server detected`
/// line.
async fn log_server_diagnostics(client: ClickHouseClient) {
    let status = client
        .server_status()
        .await
        .inspect_err(|error| warn!(error = %error, "clickhouse server status query failed"))
        .ok();

    let deployment = match client.cloud_mode().await {
        Ok(value) => ClickHouseDeployment::from_cloud_mode(value.as_deref()),
        Err(error) => {
            warn!(error = %error, "clickhouse deployment detection failed");
            ClickHouseDeployment::Unknown
        }
    };

    info!(
        deployment = %deployment,
        server_version = status.as_ref().map(|status| status.version.as_str()),
        server_uptime_secs = status.as_ref().map(|status| status.uptime.as_secs()),
        round_trip_ms = status.as_ref().map(|status| status.round_trip.as_millis() as u64),
        "clickhouse server detected"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_follows_cloud_mode_and_is_unknown_otherwise() {
        for (value, expected) in [
            (Some("1"), ClickHouseDeployment::Cloud),
            (Some("0"), ClickHouseDeployment::SelfHosted),
            (Some("2"), ClickHouseDeployment::Unknown),
            (Some(""), ClickHouseDeployment::Unknown),
            (None, ClickHouseDeployment::Unknown),
        ] {
            assert_eq!(ClickHouseDeployment::from_cloud_mode(value), expected, "{value:?}");
        }
    }
}
