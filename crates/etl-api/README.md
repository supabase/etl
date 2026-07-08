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
replicator containers:

```yaml
k8s:
  replicator_resources:
    replicator_cpu_request_millicores: 500
    replicator_memory_request_mib: 2000
```

These defaults are mandatory and request-only. The ETL API derives Kubernetes
limits from the final request values with static multipliers in the Kubernetes
materializer.

Pipeline configuration may override any replicator resource field:

```yaml
replicator_resources:
  cpu_request_millicores: 750
  memory_request_mib: 1536
  cpu_limit_millicores: 1500
  memory_limit_mib: 1843
```

All pipeline resource fields are optional. If a request is omitted, the API
configuration default is used. If a limit is omitted, the ETL API computes it
from the final request and the static multiplier.

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
