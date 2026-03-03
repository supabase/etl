# xtask — Chaos Testing CLI

Network chaos injection for Kubernetes, powered by [Chaos Mesh](https://chaos-mesh.org/).

> **Important:** All commands run against the **current kubectl context**. Always verify
> your context before running chaos to avoid accidentally targeting the wrong cluster.
>
> ```sh
> kubectl config current-context
> kubectl config use-context <context-name>  # switch if needed
> ```
>
> The tool will refuse to run against any context that doesn't contain `orbstack`,
> `staging`, `dev`, `test`, or `local` in its name.

---

## Prerequisites

- `helm` CLI available in `$PATH`
- `kubectl` configured and pointing at the right context
- `cargo` with the workspace checked out
- Chaos Mesh installed in the cluster — use `cargo xtask chaos install` (see below)

---

## Installation

Install Chaos Mesh into the current kubectl context cluster:

```sh
cargo xtask chaos install
```

| Flag | Default | Description |
|------|---------|-------------|
| `--runtime <RUNTIME>` | `docker` | Container runtime on the cluster nodes: `docker`, `containerd`, or `crio`. |

The command runs sequentially:
1. `helm repo add chaos-mesh https://charts.chaos-mesh.org`
2. `helm repo update chaos-mesh`
3. `helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace`
4. Waits for `chaos-controller-manager` and `chaos-daemon` to finish rolling out

**Common runtimes by environment:**

| Environment | Runtime |
|-------------|---------|
| OrbStack / Docker Desktop | `docker` (default) |
| k3s, most cloud clusters (EKS, GKE, AKS) | `containerd` |
| OpenShift | `crio` |

```sh
# OrbStack / Docker Desktop (default)
cargo xtask chaos install

# k3s / cloud clusters
cargo xtask chaos install --runtime containerd

# OpenShift
cargo xtask chaos install --runtime crio
```

> The install command respects the same context safety check as all other commands —
> it will refuse to run against a context not containing `orbstack`, `staging`, `dev`,
> `test`, or `local` in its name.

---

## Global flags

These flags apply to every chaos scenario (not to `install`) and can be placed
anywhere in the command.

| Flag | Default | Description |
|------|---------|-------------|
| `--selector <KEY=VALUE>` | `app=replicator` | Label selector identifying the **pod where chaos is injected**. Repeat for multiple labels. |
| `--namespace <NS>` | `default` | Kubernetes namespace of the target pod. |
| `--direction <to\|from>` | `to` | `to` = egress (packets **leaving** the pod). `from` = ingress (packets **arriving** at the pod). |
| `--delete` | — | Delete the active chaos resource instead of creating one. |
| `--print` | — | Print the generated `NetworkChaos` JSON without applying it. Useful for debugging. |

### Understanding `--direction`

The `--selector` pod is always where the chaos is **injected**.
`--direction` controls which side of that pod's traffic is disrupted:

```
--direction to   →  pod ──[chaos]──▶ destination   (outgoing, egress)
--direction from →  source ──[chaos]──▶ pod         (incoming, ingress)
```

### Finding the right `--selector`

```sh
# List all pods with their labels in a namespace
kubectl get pods -n <namespace> --show-labels

# Find the label for a specific pod
kubectl get pod <pod-name> -n <namespace> --show-labels

# Example output:
# local-replicator-statefulset-0   app.kubernetes.io/name=local-replicator,...
# clickhouse-d597dbc6f-gwlvl       app=clickhouse,...
# postgres-7cc8bf5dcf-t59qx        app=postgres,...
```

### Target flags

Each scenario that supports targeting accepts the following mutually exclusive flags:

| Flag | When to use |
|------|-------------|
| `--target-svc <NAME>` | In-cluster **service** as destination (`--direction to` only). Resolves the service's ClusterIP automatically — this is required because kube-proxy's DNAT runs outside the pod's network namespace. |
| `--target-pod <KEY=VALUE>` | In-cluster **pods** as source or destination. Use for `--direction from` (response packets carry the pod IP, not the ClusterIP). Repeatable for multiple labels. |
| `--target-url <HOST\|IP\|CIDR>` | External hostname, IP, or CIDR (e.g. `203.0.113.5`, `10.0.0.0/8`, `db.example.com`). |
| `--target-port <PORT>` | External destination port on any host. **Does not work for in-cluster services** — use `--target-svc` instead. |

> **Why `--target-svc` instead of `--target-pod` for egress?**
> When a pod sends traffic to a Kubernetes service it uses the ClusterIP.
> kube-proxy rewrites that to the pod IP *at the node level*, outside the originating
> pod's network namespace. Chaos Mesh injects rules inside the pod's namespace, so it
> only sees the ClusterIP. `--target-svc` resolves and uses the ClusterIP automatically.
>
> For `--direction from` (ingress) the situation is reversed: response packets arrive
> with the **pod IP** as source (no ClusterIP involved), so `--target-pod` is correct.

---

## Scenarios

### `packet-loss` — Drop a percentage of packets

```sh
cargo xtask chaos [GLOBAL FLAGS] packet-loss <LOSS_%> [TARGET FLAG]
```

| Argument | Description |
|----------|-------------|
| `<LOSS_%>` | Packet loss percentage, 0–100. Not required with `--delete`. |

#### Egress: replicator → clickhouse (in-cluster service)

```sh
# Drop 100% of packets from local-replicator to the clickhouse service
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-loss 100 --target-svc clickhouse

# Drop 30% of packets (partial degradation)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-loss 30 --target-svc clickhouse
```

#### Egress: replicator → external target by IP or hostname

```sh
# Drop all packets to an external IP
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-loss 100 --target-url 203.0.113.42

# Drop all packets to an external hostname
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-loss 100 --target-url bigquery.googleapis.com
```

#### Egress: replicator → external target by port

```sh
# Drop all packets going to port 443 (any external host)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-loss 100 --target-port 443
```

#### Ingress: postgres → replicator (in-cluster pod as source)

```sh
# Drop all packets arriving at local-replicator that come from the postgres pod
# (replicator sends queries fine but never receives responses)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  --direction from \
  packet-loss 100 --target-pod app=postgres
```

#### Ingress: external source → replicator (by source pod label)

```sh
# Drop packets arriving from any pod labelled role=load-generator
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  --direction from \
  packet-loss 100 --target-pod role=load-generator
```

#### Delete

```sh
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-loss --delete
```

---

### `latency` — Add artificial delay

```sh
cargo xtask chaos [GLOBAL FLAGS] latency <LATENCY_MS> [TARGET FLAG]
```

| Argument | Description |
|----------|-------------|
| `<LATENCY_MS>` | Base latency in milliseconds. A ±100ms jitter is applied on top. Not required with `--delete`. |

#### Egress: replicator → clickhouse (in-cluster service)

```sh
# Add 5 seconds of latency from local-replicator to the clickhouse service
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  latency 5000 --target-svc clickhouse

# Add 200ms latency (simulate a slow network)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  latency 200 --target-svc clickhouse
```

#### Egress: replicator → external target

```sh
# Add 1 second latency to an external BigQuery endpoint
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  latency 1000 --target-url bigquery.googleapis.com

# Add 500ms latency to all traffic on port 443
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  latency 500 --target-port 443
```

#### Ingress: postgres → replicator

```sh
# Add 2 seconds latency on responses coming from postgres
# (queries are sent instantly, but responses are slow)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  --direction from \
  latency 2000 --target-pod app=postgres
```

#### Delete

```sh
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  latency --delete
```

---

### `packet-corruption` — Corrupt a percentage of packets

```sh
cargo xtask chaos [GLOBAL FLAGS] packet-corruption <PERCENTAGE>
```

| Argument | Description |
|----------|-------------|
| `<PERCENTAGE>` | Percentage of packets to corrupt, 0–100. Not required with `--delete`. |

Corrupting packets causes TCP checksum failures and retransmits, stressing the
application's error-handling and retry logic without fully dropping connections.

#### Egress: replicator → clickhouse

```sh
# Corrupt 10% of packets from local-replicator to all destinations
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-corruption 10
```

#### Ingress: postgres → replicator

```sh
# Corrupt 20% of packets arriving from postgres
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  --direction from \
  packet-corruption 20
```

#### Delete

```sh
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  packet-corruption --delete
```

---

### `partition` — Total network partition (100% egress loss)

```sh
cargo xtask chaos [GLOBAL FLAGS] partition
```

Drops 100% of all outgoing traffic from the selected pod. No target flag — affects
all egress. Use `packet-loss 100 --target-svc <NAME>` if you want to partition
toward a specific service only.

```sh
# Fully isolate local-replicator from the network
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  partition

# Delete
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  partition --delete
```

---

### `bandwidth` — Throttle network throughput

```sh
cargo xtask chaos [GLOBAL FLAGS] bandwidth <RATE> [TARGET FLAG]
```

| Argument | Description |
|----------|-------------|
| `<RATE>` | Maximum throughput, e.g. `"1mbps"`, `"500kbps"`, `"10mbps"`. Not required with `--delete`. |

Uses Linux `tc` TBF (Token Bucket Filter) to cap the pod's throughput. Unlike
`packet-loss`, packets are not dropped — they are queued and released at the
capped rate, so the connection stays alive but becomes slow.

Useful for simulating:
- A congested or rate-limited uplink
- Cloud egress throttling
- Slow destination (e.g. an overloaded clickhouse under write pressure)

#### Egress: replicator → clickhouse (in-cluster service)

```sh
# Throttle local-replicator's writes to clickhouse to 1 Mbit/s
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  bandwidth 1mbps --target-svc clickhouse

# Simulate a very slow link (100 Kbit/s)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  bandwidth 100kbps --target-svc clickhouse
```

#### Egress: replicator → external target

```sh
# Throttle all traffic to an external BigQuery endpoint
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  bandwidth 500kbps --target-url bigquery.googleapis.com

# Throttle all traffic on port 443 to any external host
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  bandwidth 1mbps --target-port 443
```

#### Ingress: postgres → replicator

```sh
# Throttle responses arriving from postgres to 2 Mbit/s
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  --direction from \
  bandwidth 2mbps --target-pod app=postgres
```

#### Delete

```sh
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  bandwidth --delete
```

---

### `connection-flapping` — Repeatedly flap the network

```sh
cargo xtask chaos [GLOBAL FLAGS] connection-flapping [OPTIONS]
```

Repeatedly applies a total partition then removes it, cycling `--flaps` times.
Tests reconnection logic and connection pool behaviour under repeated disruptions.

| Flag | Default | Description |
|------|---------|-------------|
| `--flaps <N>` | `5` | Number of chaos-on / recovery cycles to run. |
| `--chaos-secs <N>` | `2` | Duration of each chaos-on period in seconds. |
| `--recovery-secs <N>` | `2` | Duration of each recovery period in seconds. |

```sh
# Default: 5 flaps, 2s chaos, 2s recovery
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  connection-flapping

# 10 flaps, 5s chaos, 3s recovery
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  connection-flapping --flaps 10 --chaos-secs 5 --recovery-secs 3

# Single long outage followed by a long recovery (useful for testing reconnect backoff)
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  connection-flapping --flaps 1 --chaos-secs 30 --recovery-secs 60
```

---

## Inspecting active chaos

```sh
# List all active NetworkChaos resources in a namespace
kubectl get networkchaos -n etl-data-plane

# Describe a specific resource (shows status, injected pods, events)
kubectl describe networkchaos <NAME> -n etl-data-plane

# Check whether chaos was successfully injected
kubectl get networkchaos <NAME> -n etl-data-plane \
  -o jsonpath='{.status.conditions}'
# AllInjected: True  → rules are active
# AllInjected: False → check the events for errors
```

## Dry-run / preview

Before applying, use `--print` to inspect the exact `NetworkChaos` manifest that
will be sent to the cluster:

```sh
cargo xtask chaos \
  --selector app.kubernetes.io/name=local-replicator \
  --namespace etl-data-plane \
  --print \
  packet-loss 100 --target-svc clickhouse
```

## Force-deleting stuck resources

If a resource is stuck in `Terminating` (e.g. after a node crash), strip its
finalizer and force-delete it:

```sh
kubectl patch networkchaos <NAME> -n <NS> \
  -p '{"metadata":{"finalizers":[]}}' --type=merge

kubectl delete networkchaos <NAME> -n <NS> --force --grace-period=0

# Nuke everything in a namespace
kubectl get networkchaos -n <NS> -o name | xargs -I{} \
  kubectl patch {} -n <NS> -p '{"metadata":{"finalizers":[]}}' --type=merge
kubectl delete networkchaos --all -n <NS> --force --grace-period=0
```
