use std::time::Duration;

use anyhow::Result;
use clap::Subcommand;

use super::{ChaosClient, Labels, Target, app_name_from_labels};

fn resolve_target(
    url: Option<String>,
    port: Option<u16>,
    pod_labels: Vec<String>,
) -> anyhow::Result<Target> {
    match (url, port, pod_labels) {
        (Some(url), _, _) => Ok(Target::Url(url)),
        (_, Some(port), _) => Ok(Target::Port(port)),
        (_, _, labels) if !labels.is_empty() => {
            let pairs = labels
                .iter()
                .map(|s| {
                    s.split_once('=')
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .ok_or_else(|| anyhow::anyhow!("invalid label '{s}', expected key=value"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Target::Pods(pairs))
        }
        _ => Ok(Target::All),
    }
}

#[derive(Subcommand)]
pub(crate) enum Scenario {
    /// Inject % packet loss on the app pod's traffic
    PacketLoss {
        /// Packet loss percentage (0–100); not required when using --delete
        #[arg(value_parser = clap::value_parser!(u8).range(0..=100))]
        loss: Option<u8>,

        /// Target hostname, IP, CIDR, or "host:port" (e.g. "postgres:5432")
        #[arg(long, conflicts_with_all = ["target_port", "target_pod", "target_svc"])]
        target_url: Option<String>,

        /// Target destination port on any host (e.g. 5432)
        #[arg(long, conflicts_with_all = ["target_url", "target_pod", "target_svc"])]
        target_port: Option<u16>,

        /// Target in-cluster pods matching this label "key=value"; repeatable
        #[arg(long, conflicts_with_all = ["target_url", "target_port", "target_svc"])]
        target_pod: Vec<String>,

        /// Target a Kubernetes service by name — resolves its ClusterIP automatically.
        /// Use this instead of --target-pod for services accessed via ClusterIP.
        #[arg(long, conflicts_with_all = ["target_url", "target_port", "target_pod"])]
        target_svc: Option<String>,
    },
    /// Total network partition from the app pod (100% directional loss)
    Partition,
    /// Inject Xms latency with 100ms jitter on the app pod's traffic
    Latency {
        /// Latency introduced in ms
        #[arg(value_parser = clap::value_parser!(u32))]
        latency_ms: Option<u32>,

        /// Target hostname, IP, CIDR, or "host:port"
        #[arg(long, conflicts_with_all = ["target_port", "target_pod", "target_svc"])]
        target_url: Option<String>,

        /// Target destination port on any host
        #[arg(long, conflicts_with_all = ["target_url", "target_pod", "target_svc"])]
        target_port: Option<u16>,

        /// Target in-cluster pods matching this label "key=value"; repeatable
        #[arg(long, conflicts_with_all = ["target_url", "target_port", "target_svc"])]
        target_pod: Vec<String>,

        /// Target a Kubernetes service by name — resolves its ClusterIP automatically
        #[arg(long, conflicts_with_all = ["target_url", "target_port", "target_pod"])]
        target_svc: Option<String>,
    },
    /// Repeatedly flap the app pod's network (chaos on → off cycles)
    ConnectionFlapping {
        /// Number of flap cycles to run
        #[arg(long, default_value = "5")]
        flaps: u32,

        /// Duration of each chaos-on period in seconds
        #[arg(long, default_value = "2")]
        chaos_secs: u64,

        /// Duration of each recovery period in seconds
        #[arg(long, default_value = "2")]
        recovery_secs: u64,
    },
    /// Corrupt x% of packets on the app pod's wire
    PacketCorruption {
        /// Corrupted packet percentage (0–100); not required when using --delete
        #[arg(value_parser = clap::value_parser!(u8).range(0..=100))]
        percentage: Option<u8>,
    },
    /// Limit the network bandwidth available to the app pod
    Bandwidth {
        /// Maximum throughput, e.g. "1mbps", "500kbps", "10mbps".
        /// Not required when using --delete.
        rate: Option<String>,

        /// Target hostname, IP, CIDR, or "host:port"
        #[arg(long, conflicts_with_all = ["target_port", "target_pod", "target_svc"])]
        target_url: Option<String>,

        /// Target destination port on any host
        #[arg(long, conflicts_with_all = ["target_url", "target_pod", "target_svc"])]
        target_port: Option<u16>,

        /// Target in-cluster pods matching this label "key=value"; repeatable
        #[arg(long, conflicts_with_all = ["target_url", "target_port", "target_svc"])]
        target_pod: Vec<String>,

        /// Target a Kubernetes service by name — resolves its ClusterIP automatically
        #[arg(long, conflicts_with_all = ["target_url", "target_port", "target_pod"])]
        target_svc: Option<String>,
    },
    /// Install Chaos Mesh in the current kubectl context cluster via Helm
    Install {
        /// Container runtime on the cluster nodes
        #[arg(long, default_value = "docker")]
        runtime: String,
    },
}

// ─── CRD resource name suffixes ───────────────────────────────────────────────

const NAME_PACKET_LOSS: &str = "packet-los";
const NAME_PARTITION: &str = "partition";
const NAME_LATENCY: &str = "latency";
const NAME_FLAP: &str = "flap";
const NAME_PACKET_CORRUPTION: &str = "packet-corruption";
const NAME_BANDWIDTH: &str = "bandwidth";

// ─── Scenario methods ─────────────────────────────────────────────────────────

impl Scenario {
    fn name(&self, labels: &Labels) -> String {
        let prefix = app_name_from_labels(labels);
        match self {
            Self::PacketLoss { .. } => format!("{prefix}-{NAME_PACKET_LOSS}"),
            Self::Partition => format!("{prefix}-{NAME_PARTITION}"),
            Self::Latency { .. } => format!("{prefix}-{NAME_LATENCY}"),
            Self::ConnectionFlapping { .. } => format!("{prefix}-{NAME_FLAP}"),
            Self::PacketCorruption { .. } => format!("{prefix}-{NAME_PACKET_CORRUPTION}"),
            Self::Bandwidth { .. } => format!("{prefix}-{NAME_BANDWIDTH}"),
            Self::Install { .. } => unreachable!("install is handled before Scenario::run"),
        }
    }

    pub(crate) async fn run(
        &self,
        client: &ChaosClient,
        labels: Labels,
        direction: &str,
    ) -> Result<()> {
        let chaos_name = self.name(&labels);
        match self {
            Self::PacketLoss {
                loss,
                target_url,
                target_port,
                target_pod,
                target_svc,
            } => {
                let loss = loss
                    .ok_or_else(|| anyhow::anyhow!("<loss> is required when not using --delete"))?;
                let effective_url = match target_svc {
                    Some(svc) => Some(client.resolve_svc_clusterip(svc).await?),
                    None => target_url.clone(),
                };
                let target = resolve_target(effective_url, *target_port, target_pod.clone())?;
                packet_loss(client, labels, &chaos_name, loss, target, direction).await?;
            }
            Scenario::Partition => {
                partition(client, labels, &chaos_name).await?;
            }
            Scenario::Latency {
                latency_ms,
                target_url,
                target_port,
                target_pod,
                target_svc,
            } => {
                let latency_ms = latency_ms.ok_or_else(|| {
                    anyhow::anyhow!("<latency_ms> is required when not using --delete")
                })?;
                let effective_url = match target_svc {
                    Some(svc) => Some(client.resolve_svc_clusterip(svc).await?),
                    None => target_url.clone(),
                };
                let target = resolve_target(effective_url, *target_port, target_pod.clone())?;
                latency(client, labels, &chaos_name, target, latency_ms, direction).await?;
            }
            Scenario::PacketCorruption { percentage } => {
                let percentage = percentage.ok_or_else(|| {
                    anyhow::anyhow!("<percentage> is required when not using --delete")
                })?;
                packet_corruption(client, labels, &chaos_name, percentage, direction).await?;
            }
            Scenario::ConnectionFlapping {
                flaps,
                chaos_secs,
                recovery_secs,
            } => {
                connection_flapping(
                    client,
                    labels,
                    &chaos_name,
                    *flaps,
                    *chaos_secs,
                    *recovery_secs,
                )
                .await?;
            }
            Scenario::Bandwidth {
                rate,
                target_url,
                target_port,
                target_pod,
                target_svc,
            } => {
                let rate = rate.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("<rate> is required when not using --delete (e.g. \"1mbps\")")
                })?;
                let effective_url = match target_svc {
                    Some(svc) => Some(client.resolve_svc_clusterip(svc).await?),
                    None => target_url.clone(),
                };
                let target = resolve_target(effective_url, *target_port, target_pod.clone())?;
                bandwidth(client, labels, &chaos_name, target, rate, direction).await?;
            }
            Scenario::Install { .. } => {
                unreachable!("install is handled before Scenario::run")
            }
        }
        Ok(())
    }

    pub(crate) async fn delete(&self, client: &ChaosClient, labels: &Labels) -> Result<()> {
        let name = self.name(labels);
        match self {
            // Flapping creates one resource per cycle — clean them all up.
            Self::ConnectionFlapping { flaps, .. } => {
                for i in 0..*flaps {
                    let _ = client.delete(&format!("{name}-{i}")).await;
                }
                Ok(())
            }
            Self::Install { .. } => {
                unreachable!("install is handled before Scenario::delete")
            }
            _ => client.delete(&name).await,
        }
    }
}

// ─── Direction / target compatibility validation ──────────────────────────────

/// Chaos Mesh netem actions (loss/delay/corrupt) have two hard constraints when
/// `direction` is `"from"` or `"both"`:
///   1. `externalTargets` is not supported → `Target::Url` / `Target::Port` are invalid.
///   2. A pod `target` selector is mandatory → `Target::All` is invalid.
///
/// Call this before building any netem `NetworkChaos` spec.
fn check_netem_direction(direction: &str, target: &Target) -> Result<()> {
    if direction != "from" && direction != "both" {
        return Ok(());
    }
    match target {
        Target::Pods(_) => Ok(()),
        Target::Url(_) | Target::Port(_) => anyhow::bail!(
            "--direction '{direction}' is incompatible with --target-url / --target-port.\n\
             Chaos Mesh netem actions do not support externalTargets with 'from'/'both' direction.\n\
             Use --target-pod to specify the source pod instead, e.g.:\n\
             \n  --direction from --target-pod app.kubernetes.io/name=<source-app>"
        ),
        Target::All => anyhow::bail!(
            "--direction '{direction}' requires --target-pod.\n\
             Chaos Mesh netem actions require a pod target selector when direction is 'from'/'both'.\n\
             Specify the source pod with --target-pod, e.g.:\n\
             \n  --direction from --target-pod app.kubernetes.io/name=<source-app>"
        ),
    }
}

// ─── Standalone scenario functions ───────────────────────────────────────────

/// Inject x% packet loss toward `target` in `direction`.
pub(crate) async fn packet_loss(
    client: &ChaosClient,
    labels: Labels,
    chaos_name: &str,
    loss: u8,
    target: Target,
    direction: &str,
) -> Result<()> {
    check_netem_direction(direction, &target)?;
    let chaos = client.drop_to(chaos_name, labels, target, loss, direction);
    client.apply(chaos).await
}

/// Apply a total network partition from the app pod (100% egress loss to all).
pub(crate) async fn partition(
    client: &ChaosClient,
    labels: Labels,
    chaos_name: &str,
) -> Result<()> {
    let chaos = client.drop_to(chaos_name, labels, Target::All, 100, "to");
    client.apply(chaos).await
}

/// Inject `latency_ms` ms latency with 100ms jitter on the app pod's traffic.
pub(crate) async fn latency(
    client: &ChaosClient,
    labels: Labels,
    chaos_name: &str,
    target: Target,
    latency_ms: u32,
    direction: &str,
) -> Result<()> {
    check_netem_direction(direction, &target)?;
    let chaos = client.latency(chaos_name, labels, target, latency_ms, 100, direction);
    client.apply(chaos).await
}

/// Flap the app pod's network `flaps` times with configurable chaos/recovery durations.
pub(crate) async fn connection_flapping(
    client: &ChaosClient,
    labels: Labels,
    chaos_name: &str,
    flaps: u32,
    chaos_secs: u64,
    recovery_secs: u64,
) -> Result<()> {
    let last = flaps.saturating_sub(1);
    for i in 0..flaps {
        let flap_name = format!("{chaos_name}-{i}");
        let chaos = client.drop_to(&flap_name, labels.clone(), Target::All, 100, "to");
        client.apply(chaos).await?;
        println!("Flap {i}/{last}: chaos applied, waiting {chaos_secs}s...");
        tokio::time::sleep(Duration::from_secs(chaos_secs)).await;
        client.delete(&flap_name).await?;
        println!("Flap {i}/{last}: recovered, waiting {recovery_secs}s...");
        tokio::time::sleep(Duration::from_secs(recovery_secs)).await;
    }
    Ok(())
}

/// Limit the throughput of the app pod's traffic to `rate` (e.g. `"1mbps"`).
pub(crate) async fn bandwidth(
    client: &ChaosClient,
    labels: Labels,
    chaos_name: &str,
    target: Target,
    rate: &str,
    direction: &str,
) -> Result<()> {
    check_netem_direction(direction, &target)?;
    let chaos = client.bandwidth(chaos_name, labels, target, rate, direction);
    client.apply(chaos).await
}

/// Corrupt x% of packets on the app pod's wire.
pub(crate) async fn packet_corruption(
    client: &ChaosClient,
    labels: Labels,
    chaos_name: &str,
    percentage: u8,
    direction: &str,
) -> Result<()> {
    // Same constraint as latency: no --target-pod support, so 'from'/'both' is invalid.
    check_netem_direction(direction, &Target::All)?;
    let chaos = client.corrupt(chaos_name, labels, percentage, direction);
    client.apply(chaos).await
}
