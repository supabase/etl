//! Opt-in Hotpath profiling and integration with the existing metrics
//! endpoints.
//!
//! Hotpath 0.25.1 exposes its Prometheus renderer only over HTTP. The bridge
//! reads that loopback endpoint and appends its classic histograms to ETL's
//! scrape response. It never installs a second global metrics recorder.

use std::{
    env,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    num::ParseIntError,
    sync::{Once, OnceLock},
    time::Duration,
};

use hotpath::{HotpathGuard, HotpathGuardBuilder};
use metrics::{describe_gauge, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue, InvalidHeaderValue},
    redirect::Policy,
};
use thiserror::Error;
use tracing::warn;

use crate::metrics::MetricsError;

/// Whether the latest scrape reached the profiler successfully.
const EXPORTER_UP: &str = "etl_hotpath_exporter_up";
/// Maximum time profiling may add to an ETL scrape.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(2);
/// Registers the exporter health metric once.
static REGISTER_METRICS: Once = Once::new();
/// Process-wide client, initialized before starting the profiler.
static BRIDGE: OnceLock<PrometheusBridge> = OnceLock::new();

/// Errors configuring local profiling.
#[derive(Debug, Error)]
pub enum ProfilingError {
    /// Hotpath must remain reachable through a loopback address.
    #[error("Hotpath Prometheus host must be 127.0.0.1")]
    NonLoopbackHost,
    /// An invalid exporter port was configured.
    #[error("Invalid Hotpath Prometheus port")]
    InvalidPort(#[from] ParseIntError),
    /// The profiler cannot report a dynamically allocated port to the bridge.
    #[error("Hotpath Prometheus port must be nonzero")]
    ZeroPort,
    /// An environment variable could not be read.
    #[error("Failed to read Hotpath environment variable {name}")]
    Environment {
        /// Name of the invalid variable; its value is never retained.
        name: &'static str,
    },
    /// An invalid authorization header was configured.
    #[error("Invalid Hotpath Prometheus authorization header")]
    Authorization(#[from] InvalidHeaderValue),
    /// Hotpath rejects whitespace and non-ASCII authorization tokens.
    #[error("Hotpath Prometheus token must contain only printable ASCII without whitespace")]
    InvalidToken,
    /// The local exporter address is unavailable.
    #[error("Hotpath Prometheus port is unavailable; choose a distinct HOTPATH_PROMETHEUS_PORT")]
    Listener(#[from] std::io::Error),
    /// The loopback HTTP client could not be built.
    #[error("Failed to build Hotpath Prometheus client")]
    Client(#[from] reqwest::Error),
    /// A profiler already owns the process-wide state.
    #[error("Hotpath profiling has already been initialized")]
    AlreadyInitialized,
}

/// Reads an optional environment variable without ignoring invalid Unicode.
fn optional_env(name: &'static str) -> Result<Option<String>, ProfilingError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        // VarError::NotUnicode owns the original value, which may be a token.
        Err(_) => Err(ProfilingError::Environment { name }),
    }
}

/// Fetches classic Prometheus exposition from this process's Hotpath exporter.
struct PrometheusBridge {
    /// HTTP client with redirects and environment proxies disabled.
    client: Client,
    /// Loopback exporter address, without credentials or query parameters.
    url: Url,
}

impl PrometheusBridge {
    /// Validates the exporter configuration before Hotpath reads it.
    fn from_env() -> Result<(Self, SocketAddr), ProfilingError> {
        let host = optional_env("HOTPATH_PROMETHEUS_HOST")?;
        if !matches!(host.as_deref(), None | Some("127.0.0.1")) {
            return Err(ProfilingError::NonLoopbackHost);
        }
        let port = optional_env("HOTPATH_PROMETHEUS_PORT")?
            .map_or(Ok(6772), |port| port.parse::<u16>())?;
        if port == 0 {
            return Err(ProfilingError::ZeroPort);
        }
        let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let url = Url::parse(&format!("http://{address}/metrics"))
            .expect("a numeric socket address forms a valid HTTP URL");
        let mut headers = HeaderMap::new();
        if let Some(token) = optional_env("HOTPATH_PROMETHEUS_AUTH_TOKEN")?
            && !token.is_empty()
        {
            if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
                return Err(ProfilingError::InvalidToken);
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .timeout(SCRAPE_TIMEOUT)
            .default_headers(headers)
            .build()?;
        Ok((Self { client, url }, address))
    }

    /// Reads one complete snapshot without forwarding the caller's headers.
    async fn scrape(&self) -> Result<String, reqwest::Error> {
        self.client
            .get(self.url.clone())
            .header(reqwest::header::ACCEPT, "text/plain; version=0.0.4")
            .send()
            .await?
            .error_for_status()?
            .text()
            .await
    }
}

/// Starts profiling once. Retain the returned guard until application shutdown.
///
/// Call before starting application work. A port check prevents accidentally
/// scraping another locally running service's profiler. Concurrent local
/// services need distinct `HOTPATH_PROMETHEUS_PORT` values.
pub fn init() -> Result<HotpathGuard, ProfilingError> {
    let (bridge, address) = PrometheusBridge::from_env()?;
    // Hotpath binds asynchronously and otherwise only logs port conflicts.
    // Check first so an existing listener causes an explicit startup failure.
    let listener = TcpListener::bind(address)?;
    BRIDGE.set(bridge).map_err(|_| ProfilingError::AlreadyInitialized)?;
    drop(listener);
    Ok(HotpathGuardBuilder::new("etl").build())
}

/// Appends profiler metrics while preserving ETL metrics if profiling is down.
///
/// Failure is visible through `etl_hotpath_exporter_up = 0`; profiler counters
/// are omitted rather than fabricated as zero. Global labels are attached to
/// profiler samples just as they are to the existing recorder's samples.
pub async fn render_metrics(handle: &PrometheusHandle, global_labels: &[(&str, String)]) -> String {
    render_snapshot(handle, global_labels, BRIDGE.get()).await
}

/// Combines one optional profiler snapshot with the existing recorder.
async fn render_snapshot(
    handle: &PrometheusHandle,
    global_labels: &[(&str, String)],
    bridge: Option<&PrometheusBridge>,
) -> String {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(EXPORTER_UP, "Whether the latest Hotpath Prometheus scrape succeeded");
    });
    let snapshot = if let Some(bridge) = bridge {
        match bridge.scrape().await {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                warn!(error = %error, "failed to scrape local hotpath metrics");
                None
            }
        }
    } else {
        None
    };
    gauge!(EXPORTER_UP).set(if snapshot.is_some() { 1.0 } else { 0.0 });
    let mut rendered = handle.render();
    if let Some(snapshot) = snapshot {
        append_labeled_snapshot(&mut rendered, &snapshot, global_labels);
    }
    rendered
}

/// Escapes a Prometheus text label value.
fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\n', "\\n").replace('"', "\\\"")
}

/// Adds known ETL global labels to Hotpath's classic exposition samples.
///
/// Hotpath owns its metric names and uses no `project`, `pipeline_id`, or
/// `destination` labels. Metadata lines remain unchanged. Only the metric-name
/// prefix is inspected, so braces and spaces inside label values are preserved.
fn append_labeled_snapshot(output: &mut String, snapshot: &str, global_labels: &[(&str, String)]) {
    if global_labels.is_empty() {
        output.push_str(snapshot);
        return;
    }
    let labels = global_labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{}\"", escape_label(value)))
        .collect::<Vec<_>>()
        .join(",");
    for line in snapshot.lines() {
        if !line.is_empty() && !line.starts_with('#') {
            if let Some(index) = line.find(['{', ' ']) {
                let (name, rest) = line.split_at(index);
                output.push_str(name);
                output.push('{');
                output.push_str(&labels);
                if let Some(rest) = rest.strip_prefix('{') {
                    if !rest.starts_with('}') {
                        output.push(',');
                    }
                    output.push_str(rest);
                } else {
                    output.push('}');
                    output.push_str(rest);
                }
            } else {
                output.push_str(line);
            }
        } else {
            output.push_str(line);
        }
        output.push('\n');
    }
}

/// Runs the combined scrape handler on the replicator's existing port.
///
/// The replicator initializes metrics before it starts Tokio, so the listener
/// owns a small runtime, matching the normal Prometheus exporter's lifecycle.
pub(crate) fn install_metrics_listener(
    builder: PrometheusBuilder,
    global_labels: Vec<(&'static str, String)>,
) -> Result<(), MetricsError> {
    let listener = TcpListener::bind((std::net::Ipv6Addr::UNSPECIFIED, 9000))?;
    listener.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let handle = builder.install_recorder()?;
    std::thread::Builder::new().name("etl-metrics".to_owned()).spawn(move || {
        runtime.block_on(async move {
            let listener = match tokio::net::TcpListener::from_std(listener) {
                Ok(listener) => listener,
                Err(error) => {
                    tracing::error!(error = %error, "failed to initialize metrics listener");
                    return;
                }
            };
            let upkeep_handle = handle.clone();
            let upkeep_task = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    upkeep_handle.run_upkeep();
                }
            });
            let app = metrics_router(handle, global_labels);
            if let Err(error) = axum::serve(listener, app).await {
                tracing::error!(error = %error, "metrics listener stopped");
            }
            upkeep_task.abort();
        });
    })?;
    Ok(())
}

/// Builds the standalone metrics endpoint using the shared scrape renderer.
fn metrics_router(
    handle: PrometheusHandle,
    global_labels: Vec<(&'static str, String)>,
) -> axum::Router {
    axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let handle = handle.clone();
            let labels = global_labels.clone();
            async move {
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    render_metrics(&handle, &labels).await,
                )
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        routing::get,
    };
    use metrics::counter;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use reqwest::Client;
    use tower::ServiceExt;

    use crate::profiling::{
        PrometheusBridge, append_labeled_snapshot, metrics_router, render_snapshot,
    };

    /// Preserves metadata and label syntax while attaching replicator identity.
    #[test]
    fn global_labels_preserve_exposition_and_escape_values() {
        let mut output = String::new();
        append_labeled_snapshot(
            &mut output,
            "# TYPE hotpath_example counter\nhotpath_example{route=\"GET /items/{id}\"} \
             7\nhotpath_uptime_seconds 3\n",
            &[("project", "example\"\\\nproject".to_owned()), ("pipeline_id", "42".to_owned())],
        );
        assert_eq!(
            output,
            "# TYPE hotpath_example \
             counter\nhotpath_example{project=\"example\\\"\\\\\\nproject\",pipeline_id=\"42\",\
             route=\"GET /items/{id}\"} \
             7\nhotpath_uptime_seconds{project=\"example\\\"\\\\\\nproject\",pipeline_id=\"42\"} \
             3\n"
        );
    }

    /// Keeps the standalone endpoint useful before profiling is initialized.
    #[tokio::test]
    async fn standalone_endpoint_preserves_metrics_and_global_labels() {
        let handle = PrometheusBuilder::new()
            .add_global_label("project", "example-project")
            .add_global_label("pipeline_id", "42")
            .add_global_label("destination", "clickhouse")
            .install_recorder()
            .unwrap();
        counter!("etl_profiling_test_events_total").increment(3);
        let response = metrics_router(handle, Vec::new())
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body =
            String::from_utf8(to_bytes(response.into_body(), 1024 * 1024).await.unwrap().to_vec())
                .unwrap();
        assert!(body.lines().any(|line| line.starts_with("etl_profiling_test_events_total{")
            && line.contains("project=\"example-project\"")
            && line.contains("pipeline_id=\"42\"")
            && line.contains("destination=\"clickhouse\"")
            && line.ends_with(" 3")));
        assert!(
            body.lines()
                .any(|line| line.starts_with("etl_hotpath_exporter_up{") && line.ends_with(" 0"))
        );
    }

    /// A failed upstream scrape must not discard existing ETL measurements.
    #[tokio::test]
    async fn profiler_failure_preserves_metrics_without_exporting_error_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/metrics",
                    get(|| async {
                        (StatusCode::SERVICE_UNAVAILABLE, "placeholder-upstream-body")
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let bridge = PrometheusBridge {
            client: Client::builder().no_proxy().timeout(Duration::from_secs(1)).build().unwrap(),
            url: format!("http://{address}/metrics").parse().unwrap(),
        };
        let handle = PrometheusBuilder::new().install_recorder().unwrap();
        counter!("etl_profiling_test_events_total").increment(4);
        let snapshot = render_snapshot(&handle, &[], Some(&bridge)).await;
        server.abort();
        assert!(snapshot.contains("etl_profiling_test_events_total 4"));
        assert!(snapshot.contains("etl_hotpath_exporter_up 0"));
        assert!(!snapshot.contains("placeholder-upstream-body"));
    }
}
