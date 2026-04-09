//! Active network probes for the replicator.
//!
//! Polls a small set of config-derived endpoints to distinguish DNS, TCP
//! connect, and optional HTTP request latency from downstream processing time.

use std::net::SocketAddr;
use std::sync::Once;
use std::time::Duration;

use etl_config::shared::{DestinationConfig, IcebergConfig, PgConnectionConfig, ReplicatorConfig};
use etl_config::{Environment, parse_ducklake_url};
use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
use reqwest::{Client, redirect::Policy};
use tokio::net::{TcpStream, lookup_host};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, warn};
use url::Url;

use crate::metrics::{APP_TYPE_LABEL, APP_TYPE_VALUE, PIPELINE_ID_LABEL};

/// Registers network probe metrics once per process.
static REGISTER_METRICS: Once = Once::new();

/// Gauge tracking whether the latest probe attempt succeeded.
const ETL_NETWORK_PROBE_UP: &str = "etl_network_probe_up";
/// Histogram tracking successful DNS resolution duration.
const ETL_NETWORK_DNS_RESOLVE_SECONDS: &str = "etl_network_dns_resolve_seconds";
/// Histogram tracking successful TCP connect duration.
const ETL_NETWORK_TCP_CONNECT_SECONDS: &str = "etl_network_tcp_connect_seconds";
/// Histogram tracking successful HTTP probe duration.
const ETL_NETWORK_HTTP_PROBE_SECONDS: &str = "etl_network_http_probe_seconds";
/// Counter tracking failed probe attempts by stage and error kind.
const ETL_NETWORK_PROBE_FAILURES_TOTAL: &str = "etl_network_probe_failures_total";

/// Polling interval for active network probes.
const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Timeout applied to one DNS lookup.
const DNS_TIMEOUT: Duration = Duration::from_secs(3);
/// Timeout applied to one TCP connect attempt.
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Timeout applied to one HTTP probe request.
const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Label key for the probed dependency.
const TARGET_LABEL: &str = "target";
/// Label key for the probe stage that failed.
const STAGE_LABEL: &str = "stage";
/// Label key describing the probe failure kind.
const ERROR_KIND_LABEL: &str = "error_kind";

/// Target label for the source Postgres connection.
const SOURCE_POSTGRES_TARGET: &str = "source_postgres";
/// Target label for the DuckLake catalog endpoint.
const DUCKLAKE_CATALOG_TARGET: &str = "ducklake_catalog";
/// Target label for the DuckLake object-store endpoint.
const DUCKLAKE_OBJECT_STORE_TARGET: &str = "ducklake_object_store";
/// Target label for the BigQuery API endpoint.
const BIGQUERY_API_TARGET: &str = "bigquery_api";
/// Target label for the Iceberg catalog endpoint.
const ICEBERG_CATALOG_TARGET: &str = "iceberg_catalog";
/// Target label for the Iceberg object-store endpoint.
const ICEBERG_OBJECT_STORE_TARGET: &str = "iceberg_object_store";

/// Default DuckLake S3 region used when no explicit region is configured.
const DEFAULT_DUCKLAKE_S3_REGION: &str = "us-east-1";
/// Default BigQuery API probe URL.
const BIGQUERY_API_URL: &str = "https://bigquery.googleapis.com/";
/// Default Google Cloud Storage probe URL.
const GOOGLE_CLOUD_STORAGE_URL: &str = "https://storage.googleapis.com/";

/// One active network probe target derived from replicator configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NetworkProbeTarget {
    /// Low-cardinality label exposed in Prometheus.
    label: &'static str,
    /// Hostname or IP used for DNS and TCP probing.
    host: String,
    /// Port used for TCP probing.
    port: u16,
    /// Optional HTTP URL used for end-to-end request probing.
    http_url: Option<Url>,
}

impl NetworkProbeTarget {
    /// Builds a TCP-only probe target.
    fn tcp(label: &'static str, host: String, port: u16) -> Self {
        Self {
            label,
            host,
            port,
            http_url: None,
        }
    }

    /// Builds a probe target that also performs an HTTP request.
    fn http(label: &'static str, url: Url) -> Option<Self> {
        let host = url.host_str()?.to_string();
        let port = url.port_or_known_default()?;
        Some(Self {
            label,
            host,
            port,
            http_url: Some(url),
        })
    }
}

/// Registers network probe metric descriptions.
fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            ETL_NETWORK_PROBE_UP,
            Unit::Count,
            "Latest config-derived dependency probe result. Uses 1 for success and 0 for failure."
        );
        describe_histogram!(
            ETL_NETWORK_DNS_RESOLVE_SECONDS,
            Unit::Seconds,
            "Time spent resolving one dependency hostname before probing it."
        );
        describe_histogram!(
            ETL_NETWORK_TCP_CONNECT_SECONDS,
            Unit::Seconds,
            "Time spent establishing one TCP connection to a dependency after DNS resolution."
        );
        describe_histogram!(
            ETL_NETWORK_HTTP_PROBE_SECONDS,
            Unit::Seconds,
            "Time spent receiving one HTTP response from a dependency after DNS and TCP probes."
        );
        describe_counter!(
            ETL_NETWORK_PROBE_FAILURES_TOTAL,
            Unit::Count,
            "Dependency probe failures grouped by target, stage, and error kind."
        );
    });
}

/// Builds the set of active network probes for one replicator configuration.
pub(crate) fn build_network_probe_targets(
    replicator_config: &ReplicatorConfig,
) -> Vec<NetworkProbeTarget> {
    let mut targets = Vec::new();
    push_postgres_probe_target(
        &mut targets,
        SOURCE_POSTGRES_TARGET,
        &replicator_config.pipeline.pg_connection,
    );

    match &replicator_config.destination {
        DestinationConfig::BigQuery { .. } => {
            push_http_probe_target_from_url_literal(
                &mut targets,
                BIGQUERY_API_TARGET,
                BIGQUERY_API_URL,
            );
        }
        DestinationConfig::Iceberg { config } => match config {
            IcebergConfig::Supabase { project_ref, .. } => {
                let environment = match Environment::load() {
                    Ok(environment) => environment,
                    Err(error) => {
                        warn!(
                            error = %error,
                            "failed to load environment for iceberg network probes"
                        );
                        return targets;
                    }
                };
                let domain = environment.get_supabase_domain();
                push_http_probe_target_from_url_literal(
                    &mut targets,
                    ICEBERG_CATALOG_TARGET,
                    &format!("https://{project_ref}.storage.{domain}/storage/v1/iceberg"),
                );
                push_http_probe_target_from_url_literal(
                    &mut targets,
                    ICEBERG_OBJECT_STORE_TARGET,
                    &format!("https://{project_ref}.storage.{domain}/storage/v1/s3"),
                );
            }
            IcebergConfig::Rest {
                catalog_uri,
                s3_endpoint,
                ..
            } => {
                push_http_probe_target_from_url_literal(
                    &mut targets,
                    ICEBERG_CATALOG_TARGET,
                    catalog_uri,
                );
                push_http_probe_target_from_endpoint(
                    &mut targets,
                    ICEBERG_OBJECT_STORE_TARGET,
                    s3_endpoint,
                    None,
                );
            }
        },
        DestinationConfig::Ducklake {
            catalog_url,
            data_path,
            s3_region,
            s3_endpoint,
            s3_use_ssl,
            ..
        } => {
            match parse_ducklake_url(catalog_url) {
                Ok(catalog_url) => {
                    if matches!(catalog_url.scheme(), "postgres" | "postgresql") {
                        push_tcp_probe_target_from_url(
                            &mut targets,
                            DUCKLAKE_CATALOG_TARGET,
                            &catalog_url,
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to parse ducklake catalog url for network probes"
                    );
                }
            }

            match parse_ducklake_url(data_path) {
                Ok(data_path) => {
                    if let Some(object_store_url) = ducklake_object_store_probe_url(
                        &data_path,
                        s3_endpoint.as_deref(),
                        s3_region.as_deref(),
                        s3_use_ssl.unwrap_or(false),
                    ) {
                        push_http_probe_target_from_url(
                            &mut targets,
                            DUCKLAKE_OBJECT_STORE_TARGET,
                            object_store_url,
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "failed to parse ducklake data path for network probes"
                    );
                }
            }
        }
    }

    targets
}

/// Spawns the background task that polls config-derived dependency probes.
pub(crate) fn spawn_network_metrics_task(pipeline_id: u64, probe_targets: Vec<NetworkProbeTarget>) {
    register_metrics();

    if probe_targets.is_empty() {
        debug!("no network probe targets configured");
        return;
    }

    let client = match Client::builder()
        .connect_timeout(HTTP_PROBE_TIMEOUT)
        .timeout(HTTP_PROBE_TIMEOUT)
        .redirect(Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "failed to build network probe client");
            return;
        }
    };
    let pipeline_id_str = pipeline_id.to_string();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            for target in &probe_targets {
                probe_target(&client, &pipeline_id_str, target).await;
            }
        }
    });
}

/// Probes one dependency target and updates metrics for the latest result.
async fn probe_target(client: &Client, pipeline_id: &str, probe_target: &NetworkProbeTarget) {
    let addresses = match resolve_target(pipeline_id, probe_target).await {
        Some(addresses) => addresses,
        None => {
            set_probe_up_metric(pipeline_id, probe_target.label, false);
            return;
        }
    };

    if !probe_tcp_target(pipeline_id, probe_target, addresses).await {
        set_probe_up_metric(pipeline_id, probe_target.label, false);
        return;
    }

    if let Some(http_url) = &probe_target.http_url
        && !probe_http_target(client, pipeline_id, probe_target.label, http_url).await
    {
        set_probe_up_metric(pipeline_id, probe_target.label, false);
        return;
    }

    set_probe_up_metric(pipeline_id, probe_target.label, true);
}

/// Resolves one probe target to socket addresses.
async fn resolve_target(
    pipeline_id: &str,
    probe_target: &NetworkProbeTarget,
) -> Option<Vec<SocketAddr>> {
    let started = Instant::now();
    let lookup = lookup_host((probe_target.host.as_str(), probe_target.port));
    match tokio::time::timeout(DNS_TIMEOUT, lookup).await {
        Ok(Ok(addresses)) => {
            let addresses = addresses.collect::<Vec<_>>();
            if addresses.is_empty() {
                record_probe_failure(pipeline_id, probe_target.label, "dns", "empty");
                return None;
            }

            histogram!(
                ETL_NETWORK_DNS_RESOLVE_SECONDS,
                PIPELINE_ID_LABEL => pipeline_id.to_string(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
                TARGET_LABEL => probe_target.label,
            )
            .record(started.elapsed().as_secs_f64());
            Some(addresses)
        }
        Ok(Err(error)) => {
            debug!(
                target = probe_target.label,
                host = %probe_target.host,
                port = probe_target.port,
                error = %error,
                "network dns probe failed"
            );
            record_probe_failure(pipeline_id, probe_target.label, "dns", "resolve");
            None
        }
        Err(_) => {
            record_probe_failure(pipeline_id, probe_target.label, "dns", "timeout");
            None
        }
    }
}

/// Probes TCP reachability for one dependency target.
async fn probe_tcp_target(
    pipeline_id: &str,
    probe_target: &NetworkProbeTarget,
    addresses: Vec<SocketAddr>,
) -> bool {
    let started = Instant::now();
    let mut last_error_kind = "io";

    for address in addresses {
        match tokio::time::timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                drop(stream);
                histogram!(
                    ETL_NETWORK_TCP_CONNECT_SECONDS,
                    PIPELINE_ID_LABEL => pipeline_id.to_string(),
                    APP_TYPE_LABEL => APP_TYPE_VALUE,
                    TARGET_LABEL => probe_target.label,
                )
                .record(started.elapsed().as_secs_f64());
                return true;
            }
            Ok(Err(error)) => {
                debug!(
                    target = probe_target.label,
                    address = %address,
                    error = %error,
                    "network tcp probe failed"
                );
                last_error_kind = "io";
            }
            Err(_) => {
                last_error_kind = "timeout";
            }
        }
    }

    record_probe_failure(
        pipeline_id,
        probe_target.label,
        "tcp_connect",
        last_error_kind,
    );
    false
}

/// Probes HTTP reachability for one dependency target.
async fn probe_http_target(
    client: &Client,
    pipeline_id: &str,
    target_label: &'static str,
    http_url: &Url,
) -> bool {
    let started = Instant::now();
    match client.get(http_url.clone()).send().await {
        Ok(response) => {
            debug!(
                target = target_label,
                status = %response.status(),
                "network http probe completed"
            );
            histogram!(
                ETL_NETWORK_HTTP_PROBE_SECONDS,
                PIPELINE_ID_LABEL => pipeline_id.to_string(),
                APP_TYPE_LABEL => APP_TYPE_VALUE,
                TARGET_LABEL => target_label,
            )
            .record(started.elapsed().as_secs_f64());
            true
        }
        Err(error) => {
            debug!(
                target = target_label,
                url = http_url.as_str(),
                error = %error,
                "network http probe failed"
            );
            let error_kind = if error.is_timeout() {
                "timeout"
            } else if error.is_connect() {
                "connect"
            } else {
                "request"
            };
            record_probe_failure(pipeline_id, target_label, "http_request", error_kind);
            false
        }
    }
}

/// Records the latest up/down state for one probe target.
fn set_probe_up_metric(pipeline_id: &str, target_label: &'static str, is_up: bool) {
    gauge!(
        ETL_NETWORK_PROBE_UP,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        APP_TYPE_LABEL => APP_TYPE_VALUE,
        TARGET_LABEL => target_label,
    )
    .set(if is_up { 1.0 } else { 0.0 });
}

/// Records one probe failure counter sample.
fn record_probe_failure(
    pipeline_id: &str,
    target_label: &'static str,
    stage: &'static str,
    error_kind: &'static str,
) {
    counter!(
        ETL_NETWORK_PROBE_FAILURES_TOTAL,
        PIPELINE_ID_LABEL => pipeline_id.to_string(),
        APP_TYPE_LABEL => APP_TYPE_VALUE,
        TARGET_LABEL => target_label,
        STAGE_LABEL => stage,
        ERROR_KIND_LABEL => error_kind,
    )
    .increment(1);
}

/// Adds a Postgres TCP probe target when the host format supports it.
fn push_postgres_probe_target(
    targets: &mut Vec<NetworkProbeTarget>,
    label: &'static str,
    pg_connection: &PgConnectionConfig,
) {
    if pg_connection.host.is_empty() || pg_connection.host.contains('/') {
        warn!(
            target = label,
            host = %pg_connection.host,
            "skipping network probe for unsupported postgres host"
        );
        return;
    }

    targets.push(NetworkProbeTarget::tcp(
        label,
        pg_connection.host.clone(),
        pg_connection.port,
    ));
}

/// Adds an HTTP probe target from a URL string literal.
fn push_http_probe_target_from_url_literal(
    targets: &mut Vec<NetworkProbeTarget>,
    label: &'static str,
    url_literal: &str,
) {
    match Url::parse(url_literal) {
        Ok(url) => push_http_probe_target_from_url(targets, label, url),
        Err(error) => {
            warn!(
                target = label,
                url = url_literal,
                error = %error,
                "skipping network probe for invalid url"
            );
        }
    }
}

/// Adds an HTTP probe target from a parsed URL.
fn push_http_probe_target_from_url(
    targets: &mut Vec<NetworkProbeTarget>,
    label: &'static str,
    url: Url,
) {
    match NetworkProbeTarget::http(label, url.clone()) {
        Some(target) => targets.push(target),
        None => {
            warn!(
                target = label,
                url = url.as_str(),
                "skipping network probe for url without a routable host"
            );
        }
    }
}

/// Adds a TCP probe target from a parsed URL.
fn push_tcp_probe_target_from_url(
    targets: &mut Vec<NetworkProbeTarget>,
    label: &'static str,
    url: &Url,
) {
    let Some(host) = url.host_str() else {
        warn!(
            target = label,
            url = url.as_str(),
            "skipping network probe for url without a host"
        );
        return;
    };
    let Some(port) = url.port_or_known_default() else {
        warn!(
            target = label,
            url = url.as_str(),
            "skipping network probe for url without a known port"
        );
        return;
    };

    targets.push(NetworkProbeTarget::tcp(label, host.to_string(), port));
}

/// Adds an HTTP probe target from an endpoint string.
fn push_http_probe_target_from_endpoint(
    targets: &mut Vec<NetworkProbeTarget>,
    label: &'static str,
    endpoint: &str,
    default_scheme: Option<&str>,
) {
    let Some(url) = endpoint_probe_url(endpoint, default_scheme) else {
        warn!(
            target = label,
            endpoint, "skipping network probe for unsupported endpoint"
        );
        return;
    };
    push_http_probe_target_from_url(targets, label, url);
}

/// Builds the HTTP probe URL for one endpoint string.
fn endpoint_probe_url(endpoint: &str, default_scheme: Option<&str>) -> Option<Url> {
    if endpoint.contains("://") {
        return Url::parse(endpoint).ok();
    }

    let scheme = default_scheme?;
    Url::parse(&format!("{scheme}://{endpoint}")).ok()
}

/// Builds the DuckLake object-store probe URL from the configured data path.
fn ducklake_object_store_probe_url(
    data_path: &Url,
    s3_endpoint: Option<&str>,
    s3_region: Option<&str>,
    use_ssl: bool,
) -> Option<Url> {
    match data_path.scheme() {
        "s3" => {
            if let Some(endpoint) = s3_endpoint {
                let scheme = if use_ssl { "https" } else { "http" };
                endpoint_probe_url(endpoint, Some(scheme))
            } else {
                let region = s3_region.unwrap_or(DEFAULT_DUCKLAKE_S3_REGION);
                let endpoint = if region == DEFAULT_DUCKLAKE_S3_REGION {
                    "https://s3.amazonaws.com/".to_string()
                } else {
                    format!("https://s3.{region}.amazonaws.com/")
                };
                Url::parse(&endpoint).ok()
            }
        }
        "gs" => Url::parse(GOOGLE_CLOUD_STORAGE_URL).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use etl_config::shared::{
        BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, TableSyncCopyConfig,
        TcpKeepaliveConfig, TlsConfig,
    };
    use secrecy::SecretString;

    fn make_pg_connection(host: &str, port: u16) -> PgConnectionConfig {
        PgConnectionConfig {
            host: host.to_string(),
            port,
            name: "postgres".to_string(),
            username: "postgres".to_string(),
            password: None,
            tls: TlsConfig::disabled(),
            keepalive: TcpKeepaliveConfig::default(),
        }
    }

    fn make_replicator_config(destination: DestinationConfig) -> ReplicatorConfig {
        ReplicatorConfig {
            destination,
            pipeline: etl_config::shared::PipelineConfig {
                id: 42,
                publication_name: "all_tables".to_string(),
                pg_connection: make_pg_connection("source-db.internal", 5432),
                batch: BatchConfig::default(),
                table_error_retry_delay_ms: 1_000,
                table_error_retry_max_attempts: 5,
                max_table_sync_workers: 4,
                max_copy_connections_per_table: 2,
                memory_refresh_interval_ms: 100,
                memory_backpressure: Some(MemoryBackpressureConfig::default()),
                table_sync_copy: TableSyncCopyConfig::default(),
                invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            },
            sentry: None,
            supabase: None,
        }
    }

    #[test]
    fn test_build_network_probe_targets_adds_ducklake_targets() {
        let config = make_replicator_config(DestinationConfig::Ducklake {
            catalog_url: "postgres://user:pass@catalog.internal:5432/ducklake".to_string(),
            data_path: "s3://bucket/path".to_string(),
            pool_size: 4,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: Some("eu-west-1".to_string()),
            s3_endpoint: Some("127.0.0.1:5000/s3".to_string()),
            s3_url_style: Some("path".to_string()),
            s3_use_ssl: Some(false),
            metadata_schema: None,
            enable_maintenances: false,
        });

        let targets = build_network_probe_targets(&config);

        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].label, SOURCE_POSTGRES_TARGET);
        assert_eq!(targets[0].host, "source-db.internal");
        assert_eq!(targets[1].label, DUCKLAKE_CATALOG_TARGET);
        assert_eq!(targets[1].host, "catalog.internal");
        assert_eq!(targets[2].label, DUCKLAKE_OBJECT_STORE_TARGET);
        assert_eq!(targets[2].host, "127.0.0.1");
        assert_eq!(
            targets[2]
                .http_url
                .as_ref()
                .expect("ducklake object-store probe should have an http url")
                .as_str(),
            "http://127.0.0.1:5000/s3"
        );
    }

    #[test]
    fn test_build_network_probe_targets_adds_bigquery_api_target() {
        let config = make_replicator_config(DestinationConfig::BigQuery {
            project_id: "project".to_string(),
            dataset_id: "dataset".to_string(),
            service_account_key: SecretString::from(String::new()),
            max_staleness_mins: None,
            connection_pool_size: 4,
        });

        let targets = build_network_probe_targets(&config);

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[1].label, BIGQUERY_API_TARGET);
        assert_eq!(targets[1].host, "bigquery.googleapis.com");
    }

    #[test]
    fn test_build_network_probe_targets_adds_iceberg_rest_targets() {
        let config = make_replicator_config(DestinationConfig::Iceberg {
            config: IcebergConfig::Rest {
                catalog_uri: "https://catalog.example.com/iceberg".to_string(),
                warehouse_name: "warehouse".to_string(),
                namespace: None,
                s3_access_key_id: SecretString::from(String::new()),
                s3_secret_access_key: SecretString::from(String::new()),
                s3_endpoint: "http://minio.internal:9000".to_string(),
            },
        });

        let targets = build_network_probe_targets(&config);

        assert_eq!(targets.len(), 3);
        assert_eq!(targets[1].label, ICEBERG_CATALOG_TARGET);
        assert_eq!(targets[1].host, "catalog.example.com");
        assert_eq!(targets[2].label, ICEBERG_OBJECT_STORE_TARGET);
        assert_eq!(targets[2].host, "minio.internal");
    }

    #[test]
    fn test_ducklake_object_store_probe_url_uses_region_default() {
        let data_path = Url::parse("s3://bucket/path").expect("s3 url should parse");

        let url = ducklake_object_store_probe_url(&data_path, None, Some("eu-west-3"), false)
            .expect("default aws probe url should be built");

        assert_eq!(url.as_str(), "https://s3.eu-west-3.amazonaws.com/");
    }
}
