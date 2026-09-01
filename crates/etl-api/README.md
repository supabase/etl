# `etl` - API

This API service provides a RESTful interface for managing Postgres replication pipelines. It enables you to:

- Create and manage replication pipelines between Postgres sources and destinations
- Handle multi-tenant replication configurations
- Manage publications and tables for replication
- Control pipeline lifecycle (start/stop/status)
- Secure configuration with encryption
- Deploy and manage replicators in Kubernetes

## Features

- RESTful API endpoints for pipeline management
- Axum HTTP runtime with tower middleware
- Multi-tenant support with isolated configurations
- Prometheus request metrics and Sentry capture for server-error responses
- Kubernetes deployment support
- Secure configuration management
- Database schema versioning with migrations
- OpenAPI descriptors generated from `utoipa` route macros
- Integration with the core ETL system

## Table of Contents

- [Prerequisites](#prerequisites)
- [Development](#development)
- [API Documentation](#api-documentation)
- [Environment Variables](#environment-variables)
- [Authentication](#authentication)

## Prerequisites

Before running the API, you must have:

- A running Postgres instance reachable via `DATABASE_URL`.
- The `etl-api` database schema applied (SQLx migrations).
- An active Kubernetes cluster accessible through the runtime's default
  Kubernetes client configuration. Local development uses the `orbstack`
  context.
- The configured replicator namespace and ServiceAccount already created in
  that cluster.
- The `autoscaling.k8s.io/v1` Vertical Pod Autoscaler CRDs installed. The API
  checks this prerequisite at startup before it can create pipeline workloads.

ETL API validates its Kubernetes connection and shared prerequisites during
startup. It exits instead of serving requests when Kubernetes initialization or
preflight validation fails.

For the full local development stack, use the setup script to start Postgres,
run migrations, and apply the local Kubernetes resources.

```bash
cargo x init
```

Alternative: if you already have a Postgres database, set `DATABASE_URL` and apply migrations manually:

```bash
export DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DB
sqlx migrate run --source crates/etl-api/migrations
```

## Configuration

### Configuration Directory

The configuration directory is determined by:
- **`APP_CONFIG_DIR`** environment variable: If set, use this absolute path as the configuration directory
- **Fallback**: `configuration/` directory relative to the binary location

Configuration files are loaded in this order:
1. `base.(yaml|yml|json)` - Base configuration for all environments
2. `{environment}.(yaml|yml|json)` - Environment-specific overrides (environment defaults to `prod` unless `APP_ENVIRONMENT` is set to `dev`, `staging`, or `prod`)
3. `APP_`-prefixed environment variables - Runtime overrides (nested keys use `__`, lists split on `,`)

### Examples

Using default configuration directory:
```bash
# Looks for configuration files in ./configuration/
./etl-api
```

Using custom configuration directory:
```bash
# Looks for configuration files in /etc/etl-api/config/
export APP_CONFIG_DIR=/etc/etl-api/config
./etl-api
```

### Replicator Resources

The ETL API configuration must define the default Kubernetes requests for
replicator and Vector containers:

```yaml
k8s:
  replicator_namespace: etl-data-plane
  replicator_service_account_name: etl-replicator
  replicator_node_selectors:
    - key: example.com/node-pool
      value: data
  replicator_tolerations:
    - key: example.com/node-pool
      value: data
      effect: NoSchedule
  replicator_resources:
    cpu_request_millicores: 2000
    memory_request_mib: 8192
  replicator_autoscaling:
    initial_update_mode: off
    min_cpu_millicores: 250
    max_cpu_millicores: 2000
    min_memory_mib: 768
    max_memory_mib: 8192
  vector_image: timberio/vector:0.55.0-distroless-libc
  vector_resources:
    cpu_request_millicores: 75
    memory_request_mib: 192
```

`replicator_namespace` controls where all generated replicator StatefulSets,
ConfigMaps, Secrets, and DuckLake maintenance resources are created.
`replicator_service_account_name` is assigned to generated replicator Pods, and
`vector_image` selects the Vector sidecar image.

`replicator_node_selectors` and `replicator_tolerations` are optional and passed
through independently to generated replicator Pods. When omitted, replicators
do not receive scheduling constraints from the ETL API. The ETL API does not
validate that selectors and tolerations correspond to one another or to
available cluster nodes. Replicator tolerations always use Kubernetes'
`Equal` operator; selector `key` and `value` and toleration `key`, `value`, and
`effect` are passed through unchanged.

The global replicator and Vector request defaults are mandatory. Destination
defaults are optional and may replace either replicator request for `bigquery`,
`clickhouse`, `ducklake`, `iceberg`, or `snowflake`. Missing destination fields
inherit the corresponding global replicator default. Vector resources are
configured only at the API level.

Pipeline configuration may override either replicator request:

```yaml
replicator_resources:
  cpu_request_millicores: 750
  memory_request_mib: 1536
```

CPU and memory resolve independently. StatefulSet startup requests use this
precedence, from highest to lowest:

1. Pipeline request override.
2. Destination-specific API default.
3. Mandatory API-wide default.

The generated CPU and memory limits always equal their resolved requests, for
both the replicator and Vector containers, so every generated Pod has
Kubernetes Guaranteed QoS. Limits are derived from requests and are not
independently configurable.

The API always creates a VPA, and its CPU and memory bounds also resolve
independently:

1. A pipeline request override pins that resource with `minAllowed` equal to
   `maxAllowed`.
2. Otherwise, a supplied `replicator_autoscaling` block provides that
   resource's interval.
3. If `replicator_autoscaling` is omitted, the resolved StatefulSet startup
   request is used for both bounds, producing a fixed allocation.

The autoscaling interval does not clamp or otherwise change the StatefulSet's
startup requests. VPA controls both requests and limits, preserving their 1:1
ratio when recommendations are applied. `initial_update_mode` accepts the
supported Kubernetes VPA update modes below and applies only when a VPA is
first created:

- `off` publishes recommendations without changing Pods.
- `initial` applies recommendations only when Pods are created.
- `recreate` also updates running Pods by recreating them.
- `in_place_or_recreate` tries an in-place update before recreating the Pod.
- `in_place` only updates in place and requires the upstream VPA feature gate.

Subsequent API reconciliation preserves the live update mode, allowing an
operator or separate Kubernetes controller to manage transitions without the
API resetting them. The default is `off`; omitting `replicator_autoscaling`
also creates new VPAs in `off` mode.
`minReplicas: 1` permits disruption-aware recreation for enabled modes that may
fall back to it. Deployments that start with VPA `Off` may enable updates after
their chosen observation policy has collected representative usage. Deployments
that want immediate actuation may configure `in_place_or_recreate` instead.

Before restarting a running replicator, the API uses durable table state to
predict whether the restart will repeat an initial table copy. If any table is
before `SyncDone` and is not stopped in an error state, the API deletes the VPA
before reconciling the pipeline. Reconciliation then recreates the VPA from the
current resource configuration and initial update mode. Deleting the VPA
resource does not guarantee that a running upstream recommender forgets its
in-memory usage aggregates. `SyncDone` and `Ready` tables keep their destination
data across restart and therefore preserve the existing VPA and its live update
mode. This applies to explicit restarts and configuration updates that restart
a running replicator. Source connection, query, or state-decoding failures
preserve the VPA rather than making the restart less reliable.

Kubernetes- or controller-initiated Pod restarts do not pass through the API
restart path and therefore keep the learned VPA recommendation. Stopping and
starting a pipeline still deletes the autoscaler resource: stop deletes the
StatefulSet and VPA, and start recreates both in the configured initial mode.

### Encryption Keys

Sensitive source and destination config fields are encrypted before being stored
in the API database. New encrypted values use the configured key with the
highest `id`, while reads use the `id` stored with each encrypted value.

Encryption keys are configured as a non-empty list:

```yaml
encryption_keys:
  - id: 1
    key: <base64-encoded 32-byte key>
```

To rotate, add a new entry with a higher `id`. Existing rows can then be
re-encrypted with:

```bash
APP_CONFIG_DIR=/path/to/etl-api/configuration \
APP_ENVIRONMENT=prod \
cargo x rotate-encryption-key --dry-run

APP_CONFIG_DIR=/path/to/etl-api/configuration \
APP_ENVIRONMENT=prod \
cargo x rotate-encryption-key
```

To test one project/tenant first, pass its tenant id:

```bash
APP_CONFIG_DIR=/path/to/etl-api/configuration \
APP_ENVIRONMENT=prod \
cargo x rotate-encryption-key --dry-run --tenant-id <project-ref>
```

The command uses the keys and database connection from the API configuration.
It decrypts rows using the stored key ids and writes updated configs with the
highest configured key id.

In Kubernetes, run the command from an image that contains the workspace binary
and mount the same API configuration directory used by `etl-api`. For example,
mount the `base.yaml` and environment YAML at `/app/configuration`, mount or
inject the same secrets used for `database` and `encryption_keys`, then run:

```bash
APP_CONFIG_DIR=/app/configuration \
APP_ENVIRONMENT=prod \
cargo x rotate-encryption-key --dry-run
```

After the dry run reports the expected rows, run the same command without
`--dry-run`. The command has no separate database or key flags on purpose; the
API config remains the source of truth for the target database and keyring.

## Development

### API Documentation

The service exposes the generated OpenAPI document at
`/api-docs/openapi.json` and Swagger UI at `/swagger-ui`. Route descriptors are
generated from the `utoipa` macros attached to the handlers, so changes to
routes, parameters, request bodies, or responses should be reflected in those
attributes as part of the same code change.

### Observability

The service exposes Prometheus metrics at `/metrics`. HTTP request middleware
records `http_requests_total` and `http_requests_duration_seconds` with
`endpoint`, `method`, and `status` labels. Server-error HTTP responses are
captured as Sentry events while preserving sensitive-route payload scrubbing.

### Database Migrations

#### Adding a New Migration

To create a new migration file:

```bash
sqlx migrate add <migration-name>
```

#### Running Migrations

To apply all pending migrations:

```bash
sqlx migrate run --source crates/etl-api/migrations
```

#### Resetting Database

To reset the database to its initial state:

```bash
sqlx migrate reset
```

#### Updating SQLx Metadata

After making changes to the database schema, update the SQLx metadata:

```bash
cargo sqlx prepare
```

## Authentication

- The API uses Bearer token auth via the `Authorization` header.
- Configure authentication with `api_keys` (each is base64 of 32 random bytes). All listed keys are accepted, enabling seamless key rotation.

Config example (YAML):

```yaml
api_keys:
  - XOUbHmWbt9h7nWl15wWwyWQnctmFGNjpawMc3lT5CFs=
  - h1QqT7u+8t4q0t3m8rjOa2qK7F8w6h9C1xYzPqL7pmc=
```
