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

The global replicator and Vector defaults are mandatory and request-only.
Destination defaults are optional and may override either request field for
`bigquery`, `clickhouse`, `ducklake`, `iceberg`, or `snowflake`. Missing
destination fields fall back to the global replicator defaults. Vector
resources are configured only at the API level; pipeline configuration may
override replicator resources only.

Pipeline configuration may override any replicator resource field:

```yaml
replicator_resources:
  cpu_request_millicores: 750
  memory_request_mib: 1536
  cpu_limit_millicores: 1500
  memory_limit_mib: 1843
```

All pipeline resource fields are optional. Request precedence is pipeline
override, destination-kind default, then global default. If a limit is omitted,
it matches the final request. If a limit is supplied, the larger of the request
and limit becomes the allocation emitted as both request and limit. The
allocation is clamped to `replicator_autoscaling` bounds. The same bounds are
written to the VPA resource policy. CPU/memory requests always equal limits so
every generated replicator Pod has Kubernetes Guaranteed QoS. VPA controls both
requests and limits, preserving their initial 1:1 ratio while applying
recommendations with `InPlaceOrRecreate`; blocked in-place updates may therefore
fall back to a disruption-aware Pod recreation. `minReplicas: 1` permits that
fallback for the single-replica StatefulSet. The global defaults should normally
match the autoscaling maximum, so a new replicator begins with the full copy
allocation. Explicit destination and pipeline overrides remain authoritative.

This max-first behavior is intentional for the current design. A fresh pipeline
starts oversized to absorb the initial burst of data without an early OOM and
to give table copy as much throughput as the configured operating envelope
allows. After a short observation period, VPA converges CPU and memory toward
the measured workload so long-running streaming remains efficient. This is the
interim phase model until the replicator can explicitly signal copy and
streaming transitions to autoscaling.

Before restarting a running replicator, the API uses durable table state to
predict whether the restart will repeat an initial table copy. If any table is
before `SyncDone` and is not stopped in an error state, the API deletes the VPA
before reconciling the pipeline. Reconciliation then recreates the VPA with no
steady-state recommendation history, so the restarted Pod begins with the
initial copy allocation. `SyncDone` and `Ready` tables keep their destination
data across restart and therefore preserve the existing VPA. This applies to
explicit restarts and configuration updates that restart a running replicator.
Source connection, query, or state-decoding failures preserve the VPA rather
than making the restart less reliable.

Kubernetes- or controller-initiated Pod restarts do not pass through the API
restart path and therefore keep the learned VPA recommendation. Stopping and
starting a pipeline still resets autoscaling unconditionally: stop deletes the
StatefulSet and VPA, and start recreates both with fresh recommendation history.

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
