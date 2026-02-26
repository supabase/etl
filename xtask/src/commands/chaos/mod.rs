pub(crate) mod install;
pub(crate) mod scenario;

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use clap::Args;
use k8s_openapi::api::core::v1::Service;
use kube::CustomResource;
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client, Discovery};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Args)]
pub(crate) struct ChaosArgs {
    #[command(subcommand)]
    scenario: scenario::Scenario,

    /// Label selector for the pod to inject chaos on; repeat for multiple labels.
    /// Example: --selector app.kubernetes.io/name=local-replicator
    #[arg(long = "selector", default_value = "app=replicator", global = true)]
    selector: Vec<String>,

    /// Kubernetes namespace
    #[arg(long, default_value = "default", global = true)]
    namespace: String,

    /// Delete the chaos resource instead of applying it
    #[arg(long, global = true)]
    delete: bool,

    /// Traffic direction: "to" (egress) or "from" (ingress)
    #[arg(long, default_value = "to", global = true)]
    direction: String,

    /// Print the generated NetworkChaos JSON instead of applying it
    #[arg(long, global = true)]
    print: bool,
}

impl ChaosArgs {
    pub(crate) async fn run(self) -> Result<()> {
        // Install is handled before connecting to the cluster — Chaos Mesh is not
        // yet present so ChaosClient::new (which checks for the CRD group) would fail.
        if let scenario::Scenario::Install { runtime } = &self.scenario {
            return install::install(runtime);
        }

        let labels = parse_selector(&self.selector)?;
        let client = ChaosClient::new(&self.namespace).await?;
        let client = if self.print {
            client.with_dry_run()
        } else {
            client
        };
        if self.delete {
            self.scenario.delete(&client, &labels).await?;
            println!("Chaos deleted.");
        } else {
            self.scenario.run(&client, labels, &self.direction).await?;
            if !self.print {
                println!("Chaos applied.");
            }
        }
        Ok(())
    }
}

// ─── CRD types ────────────────────────────────────────────────────────────────

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "chaos-mesh.org",
    version = "v1alpha1",
    kind = "NetworkChaos",
    plural = "networkchaos",
    namespaced
)]
pub(crate) struct NetworkChaosSpec {
    pub(crate) action: String,
    pub(crate) mode: String,
    pub(crate) selector: Selector,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) loss: Option<LossSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) delay: Option<DelaySpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) corrupt: Option<CorruptSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bandwidth: Option<BandwidthSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) duration: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) direction: Option<String>, // "to", "from", "both"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) target: Option<PodTarget>, // for directional chaos (in-cluster pod selector)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "externalTargets")]
    pub(crate) external_targets: Option<Vec<String>>, // IPs, CIDRs, hostnames, or "host:port"
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub(crate) struct Selector {
    pub(crate) namespaces: Vec<String>,
    #[serde(rename = "labelSelectors")]
    pub(crate) label_selectors: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub(crate) struct PodTarget {
    pub(crate) selector: Selector,
    pub(crate) mode: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub(crate) struct LossSpec {
    pub(crate) loss: String,
    pub(crate) correlation: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub(crate) struct DelaySpec {
    pub(crate) latency: String,
    pub(crate) jitter: String,
    pub(crate) correlation: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub(crate) struct CorruptSpec {
    pub(crate) corrupt: String,
    pub(crate) correlation: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub(crate) struct BandwidthSpec {
    pub(crate) rate: String, // e.g. "1mbps"
    pub(crate) limit: u32,
    pub(crate) buffer: u32,
}

// ─── Chaos client ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ChaosClient {
    pub(crate) api: Api<NetworkChaos>,
    pub(crate) namespace: String,
    client: Client,
    dry_run: bool,
}

impl ChaosClient {
    pub(crate) async fn new(namespace: &str) -> Result<Self> {
        let client = Client::try_default().await?;

        let discovery = Discovery::new(client.clone()).run().await?;
        if !discovery.has_group("chaos-mesh.org") {
            anyhow::bail!(
                "Chaos Mesh is not installed in this cluster (chaos-mesh.org API group not found).\n\
                 Install it with: helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace"
            );
        }

        Ok(Self {
            api: Api::namespaced(client.clone(), namespace),
            namespace: namespace.to_string(),
            client,
            dry_run: false,
        })
    }

    /// Return a copy of this client that prints the JSON manifest instead of
    /// actually applying it to the cluster.
    pub(crate) fn with_dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Resolve a Kubernetes service name to its ClusterIP.
    ///
    /// This is the correct target for pod-to-service traffic: kube-proxy's DNAT
    /// runs at the node level (outside the pod's network namespace), so Chaos Mesh
    /// must match the ClusterIP — not the pod IP — to intercept the traffic.
    pub(crate) async fn resolve_svc_clusterip(&self, svc_name: &str) -> Result<String> {
        let svc_api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        let svc = svc_api.get(svc_name).await.map_err(|e| {
            anyhow::anyhow!(
                "service '{svc_name}' not found in namespace '{}': {e}",
                self.namespace
            )
        })?;
        svc.spec
            .and_then(|s| s.cluster_ip)
            .filter(|ip| ip != "None" && !ip.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("service '{svc_name}' has no ClusterIP (headless service?)")
            })
    }

    pub(crate) async fn delete(&self, name: &str) -> Result<()> {
        self.api.delete(name, &DeleteParams::default()).await?;
        Ok(())
    }

    pub(crate) async fn apply(&self, chaos: NetworkChaos) -> Result<()> {
        if self.dry_run {
            println!("{}", serde_json::to_string_pretty(&chaos)?);
            return Ok(());
        }
        assert_safe_context();
        let name = chaos.metadata.name.clone().unwrap();

        // Clean up any leftover resource from a previous run before creating.
        // After issuing the delete we poll until the object is truly gone —
        // a plain sleep is not enough because Kubernetes may keep the object in
        // Terminating state and reject the subsequent create with AlreadyExists.
        match self.api.delete(&name, &DeleteParams::default()).await {
            Ok(_) => {
                let deadline = std::time::Instant::now() + Duration::from_secs(30);
                loop {
                    match self.api.get(&name).await {
                        Err(kube::Error::Api(ref e)) if e.code == 404 => break,
                        _ if std::time::Instant::now() >= deadline => {
                            eprintln!(
                                "warning: timed out waiting for {name} to be deleted, proceeding anyway"
                            );
                            break;
                        }
                        _ => tokio::time::sleep(Duration::from_millis(200)).await,
                    }
                }
            }
            Err(kube::Error::Api(ref e)) if e.code == 404 => {}
            Err(e) => eprintln!("warning: could not delete existing chaos resource {name}: {e}"),
        }

        self.api.create(&PostParams::default(), &chaos).await?;
        // Give tc netem time to propagate
        tokio::time::sleep(Duration::from_millis(800)).await;
        Ok(())
    }

    // ── Convenience constructors ──────────────────────────────────────────────

    /// Inject packet loss from `source` pods toward `target`.
    ///
    /// `loss_pct` is clamped to `0..=100`. Use [`Target::All`] to affect all
    /// traffic in the given `direction` without a specific destination filter.
    pub(crate) fn drop_to(
        &self,
        name: &str,
        source: Labels,
        target: Target,
        loss_pct: u8,
        direction: &str,
    ) -> NetworkChaos {
        let (direction_val, pod_target, external_targets) = match target {
            Target::All => (Some(direction.to_string()), None, None),
            Target::Pods(labels) => (
                Some(direction.to_string()),
                Some(PodTarget {
                    selector: Selector {
                        namespaces: vec![self.namespace.clone()],
                        label_selectors: labels.into_iter().collect(),
                    },
                    mode: "all".into(),
                }),
                None,
            ),
            Target::Url(url) => (Some(direction.to_string()), None, Some(vec![url])),
            // ipset hash:net,port rejects 0.0.0.0/0 (/0 mask is invalid as a hash key).
            // Split into two /1 CIDRs that together cover all IPv4 addresses.
            Target::Port(port) => (
                Some(direction.to_string()),
                None,
                Some(vec![
                    format!("0.0.0.0/1:{port}"),
                    format!("128.0.0.0/1:{port}"),
                ]),
            ),
        };
        NetworkChaos::new(
            name,
            NetworkChaosSpec {
                action: "loss".into(),
                mode: "all".into(),
                selector: sel(&self.namespace, source),
                loss: Some(LossSpec {
                    loss: loss_pct.to_string(),
                    correlation: "25".into(),
                }),
                direction: direction_val,
                target: pod_target,
                external_targets,
                delay: None,
                corrupt: None,
                bandwidth: None,
                duration: None,
            },
        )
    }

    pub(crate) fn latency(
        &self,
        name: &str,
        selector: Labels,
        target: Target,
        latency_ms: u32,
        jitter_ms: u32,
        direction: &str,
    ) -> NetworkChaos {
        let (direction_val, pod_target, external_targets) = match target {
            Target::All => (Some(direction.to_string()), None, None),
            Target::Pods(labels) => (
                Some(direction.to_string()),
                Some(PodTarget {
                    selector: Selector {
                        namespaces: vec![self.namespace.clone()],
                        label_selectors: labels.into_iter().collect(),
                    },
                    mode: "all".into(),
                }),
                None,
            ),
            Target::Url(url) => (Some(direction.to_string()), None, Some(vec![url])),
            Target::Port(port) => (
                Some(direction.to_string()),
                None,
                Some(vec![
                    format!("0.0.0.0/1:{port}"),
                    format!("128.0.0.0/1:{port}"),
                ]),
            ),
        };
        NetworkChaos::new(
            name,
            NetworkChaosSpec {
                action: "delay".into(),
                mode: "all".into(),
                selector: sel(&self.namespace, selector),
                delay: Some(DelaySpec {
                    latency: format!("{latency_ms}ms"),
                    jitter: format!("{jitter_ms}ms"),
                    correlation: "25".into(),
                }),
                loss: None,
                corrupt: None,
                bandwidth: None,
                duration: None,
                direction: direction_val,
                target: pod_target,
                external_targets,
            },
        )
    }

    pub(crate) fn bandwidth(
        &self,
        name: &str,
        selector: Labels,
        target: Target,
        rate: &str,
        direction: &str,
    ) -> NetworkChaos {
        let (direction_val, pod_target, external_targets) = match target {
            Target::All => (Some(direction.to_string()), None, None),
            Target::Pods(labels) => (
                Some(direction.to_string()),
                Some(PodTarget {
                    selector: Selector {
                        namespaces: vec![self.namespace.clone()],
                        label_selectors: labels.into_iter().collect(),
                    },
                    mode: "all".into(),
                }),
                None,
            ),
            Target::Url(url) => (Some(direction.to_string()), None, Some(vec![url])),
            Target::Port(port) => (
                Some(direction.to_string()),
                None,
                Some(vec![
                    format!("0.0.0.0/1:{port}"),
                    format!("128.0.0.0/1:{port}"),
                ]),
            ),
        };
        NetworkChaos::new(
            name,
            NetworkChaosSpec {
                action: "bandwidth".into(),
                mode: "all".into(),
                selector: sel(&self.namespace, selector),
                bandwidth: Some(BandwidthSpec {
                    rate: rate.into(),
                    limit: 10000,
                    buffer: 10000,
                }),
                loss: None,
                delay: None,
                corrupt: None,
                duration: None,
                direction: direction_val,
                target: pod_target,
                external_targets,
            },
        )
    }

    pub(crate) fn corrupt(
        &self,
        name: &str,
        selector: Labels,
        corrupt_pct: u8,
        direction: &str,
    ) -> NetworkChaos {
        NetworkChaos::new(
            name,
            NetworkChaosSpec {
                action: "corrupt".into(),
                mode: "all".into(),
                selector: sel(&self.namespace, selector),
                corrupt: Some(CorruptSpec {
                    corrupt: corrupt_pct.to_string(),
                    correlation: "25".into(),
                }),
                loss: None,
                delay: None,
                bandwidth: None,
                duration: None,
                direction: Some(direction.to_string()),
                target: None,
                external_targets: None,
            },
        )
    }
}

// ─── Target ───────────────────────────────────────────────────────────────────

/// Specifies the destination for a [`ChaosClient::drop_to`] call.
pub(crate) enum Target {
    /// All traffic in the given direction — no specific destination filter.
    All,

    /// In-cluster pods matching these labels (resolved within the client's namespace).
    Pods(Vec<(String, String)>),

    /// External hostname, IP address, CIDR block, or `"host:port"` string.
    ///
    /// Passed directly as a Chaos Mesh `externalTargets` entry.
    /// Examples: `"postgres"`, `"10.0.0.1"`, `"10.0.0.0/8"`, `"clickhouse:8123"`
    Url(String),

    /// All traffic to a specific destination port, regardless of host.
    ///
    /// Uses `"0.0.0.0/0:PORT"` notation, which requires Chaos Mesh ≥ 2.3 with
    /// iptables MARK support enabled on the node.
    Port(u16),
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

pub(crate) type Labels = Vec<(String, String)>;

/// Parse a slice of `"key=value"` strings into a [`Labels`] vec.
pub(crate) fn parse_selector(selectors: &[String]) -> anyhow::Result<Labels> {
    selectors
        .iter()
        .map(|s| {
            s.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .ok_or_else(|| anyhow::anyhow!("invalid selector '{s}', expected key=value"))
        })
        .collect()
}

/// Derive a short name suitable for a Kubernetes resource name from a label set.
/// Uses the value of the first label, e.g. `app.kubernetes.io/name=etl` → `"etl"`.
pub(crate) fn app_name_from_labels(labels: &Labels) -> &str {
    labels.first().map(|(_, v)| v.as_str()).unwrap_or("app")
}

fn sel(namespace: &str, labels: Labels) -> Selector {
    Selector {
        namespaces: vec![namespace.to_string()],
        label_selectors: labels.into_iter().collect(),
    }
}

/// Guard that fails the test if the current kubectl context looks like prod
pub(crate) fn assert_safe_context() {
    let out = std::process::Command::new("kubectl")
        .args(["config", "current-context"])
        .output()
        .expect("kubectl not found");
    let ctx = String::from_utf8(out.stdout).unwrap();
    assert!(
        ctx.contains("orbstack")
            || ctx.contains("staging")
            || ctx.contains("dev")
            || ctx.contains("test")
            || ctx.contains("local"),
        "Refusing to run chaos tests against context: {ctx}"
    );
}
