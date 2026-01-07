# Development Guide

This guide covers setting up your development environment, running migrations, and common development workflows for the ETL project.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Database Setup](#database-setup)
  - [Using the Setup Script](#using-the-setup-script)
  - [Manual Setup](#manual-setup)
- [Database Migrations](#database-migrations)
  - [ETL API Migrations](#etl-api-migrations)
  - [ETL Replicator Migrations](#etl-replicator-migrations)
- [Running the Services](#running-the-services)
- [Kubernetes Setup](#kubernetes-setup)
- [Common Development Tasks](#common-development-tasks)

## Prerequisites

Before starting, ensure you have the following installed:

### Required Tools

- **Rust** (latest stable): [Install Rust](https://rustup.rs/)
- **PostgreSQL client** (`psql`): Required for database operations
- **Docker Compose**: For running PostgreSQL and other services
- **kubectl**: For Kubernetes operations
- **SQLx CLI**: For database migrations

Install SQLx CLI:

```bash
cargo install --version='~0.8.6' sqlx-cli --no-default-features --features rustls,postgres
```

### Optional Tools

- **OrbStack**: Recommended for local Kubernetes development (alternative to Docker Desktop)
  - [Install OrbStack](https://orbstack.dev)
  - Enable Kubernetes in OrbStack settings

## Quick Start

The fastest way to get started is using the setup script:

```bash
# From the project root
./scripts/init.sh
```

This script will:
1. Start PostgreSQL via Docker Compose
2. Run etl-api migrations
3. Seed the default replicator image
4. Configure the Kubernetes environment (OrbStack)

## Database Setup

### Using the Setup Script

The `scripts/init.sh` script provides a complete development environment setup:

```bash
# Use default settings (Postgres on port 5430)
./scripts/init.sh

# Customize database settings
POSTGRES_PORT=5432 POSTGRES_DB=mydb ./scripts/init.sh

# Skip Docker if you already have Postgres running
SKIP_DOCKER=1 ./scripts/init.sh

# Use persistent storage
POSTGRES_DATA_VOLUME=/path/to/data ./scripts/init.sh
```

**Environment Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Database password |
| `POSTGRES_DB` | `postgres` | Database name |
| `POSTGRES_PORT` | `5430` | Database port |
| `POSTGRES_HOST` | `localhost` | Database host |
| `SKIP_DOCKER` | (empty) | Skip Docker Compose if set |
| `POSTGRES_DATA_VOLUME` | (empty) | Path for persistent storage |
| `REPLICATOR_IMAGE` | `ramsup/etl-replicator:latest` | Default replicator image |

### Manual Setup

If you prefer manual setup or have an existing PostgreSQL instance:

**Important:** The etl-api and etl-replicator migrations can run on **separate databases**. You might have:
- The etl-api using its own dedicated Postgres instance for the control plane
- The etl-replicator state store on the same database you're replicating from (source database)
- Or both on the same database (for simpler local development setups)

#### Single Database Setup

If using one database for both the API and etl state:

```bash
export DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DB

# Run all migrations on the same database
./scripts/run_migrations.sh
```

#### Separate Database Setup

If using separate databases (recommended for production):

```bash
# API migrations on the control plane database
export DATABASE_URL=postgres://USER:PASSWORD@API_HOST:PORT/API_DB
./scripts/run_migrations.sh etl-api

# ETL migrations on the source database
export DATABASE_URL=postgres://USER:PASSWORD@SOURCE_HOST:PORT/SOURCE_DB
./scripts/run_migrations.sh etl
```

This separation allows you to:
- Scale the control plane independently from replication workloads
- Keep the etl state close to the source data
- Isolate concerns between infrastructure management and data replication

## Database Migrations

The project uses SQLx for database migrations. There are two sets of migrations:

### ETL API Migrations

Located in `etl-api/migrations/`, these create the control plane schema (`app` schema) for managing tenants, sources, destinations, and pipelines.

**Running API migrations:**

```bash
# From project root
./scripts/run_migrations.sh etl-api

# Or manually with SQLx CLI
sqlx migrate run --source etl-api/migrations
```

**Creating a new API migration:**

```bash
cd etl-api
sqlx migrate add <migration_name>
```

**Resetting the API database:**

```bash
cd etl-api
sqlx migrate revert
```

**Updating SQLx metadata after schema changes:**

```bash
cd etl-api
cargo sqlx prepare
```

### ETL Migrations

Located in `etl/migrations/`, these create the etl state store schema (`etl` schema) for tracking replication state, table schemas, and mappings.

**Running etl migrations:**

```bash
# From project root
./scripts/run_migrations.sh etl

# Or manually with SQLx CLI (requires setting search_path)
psql $DATABASE_URL -c "create schema if not exists etl;"
sqlx migrate run --source etl/migrations --database-url "${DATABASE_URL}?options=-csearch_path%3Detl"
```

**Important:** Migrations are run automatically when using the `etl-replicator` binary (see `etl-replicator/src/migrations.rs:16`). However, if you integrate the `etl` crate directly into your own application as a library, you should run these migrations manually before starting your pipeline. This design decision ensures:
- The standalone replicator binary works out-of-the-box
- Library users have explicit control over when migrations run
- CI/CD pipelines can pre-apply migrations independently

**When to run migrations manually:**
- Integrating `etl` as a library in your own application
- Pre-creating the state store schema before deployment
- Testing migrations independently
- CI/CD pipelines that separate migration and deployment steps

**Creating a new etl migration:**

```bash
cd etl
sqlx migrate add <migration_name>
```

## Running the Services

Both `etl-api` and `etl-replicator` binaries use hierarchical configuration loading from the `configuration/` directory within each crate. Configuration is loaded in this order:

1. **Base configuration**: `configuration/base.yaml` (always loaded)
2. **Environment-specific**: `configuration/{environment}.yaml` (e.g., `dev.yaml`, `prod.yaml`)
3. **Environment variable overrides**: Prefixed with `APP_` (e.g., `APP_DATABASE__URL`)

**Environment Selection:**

The environment is determined by the `APP_ENVIRONMENT` variable:
- **Default**: `prod` (if `APP_ENVIRONMENT` is not set)
- **Available**: `dev`, `staging`, `prod`

```bash
# Run with dev environment
APP_ENVIRONMENT=dev cargo run

# Run with production environment (default)
cargo run

# Override specific config values
APP_ENVIRONMENT=dev APP_DATABASE__URL=postgres://localhost/mydb cargo run
```

### ETL API

#### Running from Source

```bash
cd etl-api
APP_ENVIRONMENT=dev cargo run
```

The API loads configuration from `etl-api/configuration/{environment}.yaml`. See `etl-api/README.md` for available configuration options.

#### Running with Docker

Docker images are available for the etl-api. You must mount the configuration files and can override settings via environment variables:

```bash
docker run \
  -v $(pwd)/etl-api/configuration/base.yaml:/app/configuration/base.yaml \
  -v $(pwd)/etl-api/configuration/dev.yaml:/app/configuration/dev.yaml \
  -e APP_ENVIRONMENT=dev \
  -p 8080:8080 \
  ramsup/etl-api:latest
```

**Configuration requirements:**
- Mount both `base.yaml` and your environment-specific config file (e.g., `dev.yaml`)
- Set `APP_ENVIRONMENT` to match your mounted environment file
- Override specific values using `APP_` prefixed environment variables

#### Kubernetes Setup (ETL API Only)

The etl-api manages replicator deployments on Kubernetes by dynamically creating StatefulSets, Secrets, and ConfigMaps. The etl-api requires Kubernetes, but the **etl-replicator binary can run independently without any Kubernetes setup**.

**Prerequisites:**
- OrbStack with Kubernetes enabled (or another local Kubernetes cluster)
- `kubectl` configured with the `orbstack` context
- Pre-defined Kubernetes resources (see below)

**Required Pre-Defined Resources:**

The etl-api expects these resources to exist before it can deploy replicators:

1. **Namespace**: `etl-data-plane` - Where all replicator pods and related resources are created
2. **ConfigMap**: `trusted-root-certs-config` - Provides trusted root certificates for TLS connections

These are defined in `scripts/` and should be applied before running the API:

```bash
kubectl --context orbstack apply -f scripts/etl-data-plane.yaml
kubectl --context orbstack apply -f scripts/trusted-root-certs-config.yaml
```

**Note:** For the complete list of expected Kubernetes resources and their specifications, refer to the constants and resource creation logic in `etl-api/src/k8s/http.rs`.

### ETL Replicator

The replicator can run as a standalone binary without Kubernetes.

#### Running from Source

```bash
cd etl-replicator
APP_ENVIRONMENT=dev cargo run
```

The replicator loads configuration from `etl-replicator/configuration/{environment}.yaml`.

#### Running with Docker

Docker images are available for the etl-replicator. You must mount the configuration files and can override settings via environment variables:

```bash
docker run \
  -v $(pwd)/etl-replicator/configuration/base.yaml:/app/configuration/base.yaml \
  -v $(pwd)/etl-replicator/configuration/dev.yaml:/app/configuration/dev.yaml \
  -e APP_ENVIRONMENT=dev \
  etl-replicator:latest
```

**Configuration requirements:**
- Mount both `base.yaml` and your environment-specific config file (e.g., `dev.yaml`)
- Set `APP_ENVIRONMENT` to match your mounted environment file
- Override specific values using `APP_` prefixed environment variables

**Note:** While the replicator is typically deployed as a Kubernetes pod managed by the etl-api, it does not require Kubernetes to function. You can run it as a standalone process on any machine with the appropriate configuration.

## Running Tests

The project includes comprehensive test suites that require a PostgreSQL database. Tests use environment variables for database configuration to ensure isolation and reproducibility.

### Test Environment Variables

#### PostgreSQL Test Variables

All tests that interact with PostgreSQL require the following environment variables to be set:

| Variable | Required | Description |
|----------|----------|-------------|
| `TESTS_DATABASE_HOST` | **Yes** | PostgreSQL server hostname (e.g., `localhost`) |
| `TESTS_DATABASE_PORT` | **Yes** | PostgreSQL server port (e.g., `5430`) |
| `TESTS_DATABASE_USERNAME` | **Yes** | Database user (e.g., `postgres`) |
| `TESTS_DATABASE_PASSWORD` | No | Database password (optional) |

**Note:** Each test creates a unique database with a UUID-based name to ensure test isolation. The test databases are automatically cleaned up after tests complete.

#### BigQuery Test Variables

BigQuery destination tests require Google Cloud credentials:

| Variable | Required | Description |
|----------|----------|-------------|
| `TESTS_BIGQUERY_PROJECT_ID` | **Yes** | GCP project ID for BigQuery |
| `TESTS_BIGQUERY_SA_KEY_PATH` | **Yes** | Path to service account JSON key file |

**Note:** BigQuery tests are only run when the `bigquery` and `test-utils` features are enabled. Each test creates a unique dataset with a UUID-based name for isolation.

#### Iceberg Test Variables

Iceberg destination tests use local MinIO and Lakekeeper instances. The following services must be running:

- **Lakekeeper**: `http://localhost:8182` (REST catalog)
- **MinIO**: `http://localhost:9010` (S3-compatible storage)
  - Username: `minio-admin`
  - Password: `minio-admin-password`

**Note:** Iceberg tests are only run when the `iceberg` and `test-utils` features are enabled. These use hardcoded local URLs and do not require environment variables.

#### Test Output and Logging

| Variable | Description |
|----------|-------------|
| `ENABLE_TRACING=1` | Enable tracing output during test execution (useful for debugging) |
| `RUST_LOG` | Control log level (e.g., `debug`, `info`, `warn`, `error`) |

**Example:**
```bash
# Run tests with debug output
ENABLE_TRACING=1 RUST_LOG=debug cargo test test_name -- --nocapture
```

### Setting Up Test Environment

#### Option 1: Inline Environment Variables (Recommended)

The most reliable way is to set environment variables directly in the test command:

```bash
TESTS_DATABASE_HOST=localhost TESTS_DATABASE_PORT=5430 TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres cargo test -p etl-api
```

#### Option 2: Export in Current Shell Session

Export variables in your current shell session, then run tests:

```bash
# PostgreSQL test configuration
export TESTS_DATABASE_HOST=localhost
export TESTS_DATABASE_PORT=5430
export TESTS_DATABASE_USERNAME=postgres
export TESTS_DATABASE_PASSWORD=postgres

# BigQuery test configuration (optional - only needed for BigQuery tests)
export TESTS_BIGQUERY_PROJECT_ID=your-gcp-project-id
export TESTS_BIGQUERY_SA_KEY_PATH=/path/to/service-account-key.json

# Enable test output (optional)
export ENABLE_TRACING=1
export RUST_LOG=info

# Now run tests
cargo test -p etl-api
```

#### Option 3: Use a `.env` File

Create a `.env.test` file and source it:

```bash
# .env.test

# PostgreSQL (required for most tests)
TESTS_DATABASE_HOST=localhost
TESTS_DATABASE_PORT=5430
TESTS_DATABASE_USERNAME=postgres
TESTS_DATABASE_PASSWORD=postgres

# BigQuery (optional - only for BigQuery tests)
TESTS_BIGQUERY_PROJECT_ID=your-gcp-project-id
TESTS_BIGQUERY_SA_KEY_PATH=/path/to/service-account-key.json

# Test output (optional)
ENABLE_TRACING=1
RUST_LOG=info
```

```bash
# Source the file and run tests
source .env.test
cargo test -p etl-api
```

### Running Tests

**Important:** Environment variables must be set in the same command as `cargo test`, or exported in your current shell session before running tests.

```bash
# Run all tests (requires env variables)
TESTS_DATABASE_HOST=localhost TESTS_DATABASE_PORT=5430 TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres cargo test

# Run tests for a specific package
TESTS_DATABASE_HOST=localhost TESTS_DATABASE_PORT=5430 TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres cargo test -p etl-api

# Run tests for packages with test-utils feature (etl, etl-postgres, etl-destinations)
TESTS_DATABASE_HOST=localhost TESTS_DATABASE_PORT=5430 TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres cargo test -p etl --features test-utils

# Run a specific test
TESTS_DATABASE_HOST=localhost TESTS_DATABASE_PORT=5430 TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres cargo test -p etl-api --test tenants tenant_can_be_created

# Run tests with tracing output for debugging
TESTS_DATABASE_HOST=localhost TESTS_DATABASE_PORT=5430 TESTS_DATABASE_USERNAME=postgres TESTS_DATABASE_PASSWORD=postgres ENABLE_TRACING=1 RUST_LOG=info cargo test -p etl-api --test tenants tenant_can_be_created -- --nocapture
```

**Packages requiring `--features test-utils`:**
- `etl`
- `etl-postgres`
- `etl-destinations`

**Packages that don't require feature flags:**
- `etl-api`
- `etl-config`
- `etl-telemetry`
- `etl-replicator`

**Note:** Ensure PostgreSQL is running and accessible at the configured host and port before running tests. The test suite will fail if it cannot connect to the database or if the required environment variables are not set.

## Troubleshooting

### Database Connection Issues

If you encounter connection issues:

1. Verify PostgreSQL is running:
   ```bash
   docker-compose -f scripts/docker-compose.yaml ps
   ```

2. Check the connection:
   ```bash
   psql $DATABASE_URL -c "SELECT 1;"
   ```

3. Ensure the correct port is used (default: 5430)

### Migration Issues

If migrations fail:

1. Check if the database exists:
   ```bash
   psql $DATABASE_URL -c "\l"
   ```

2. Verify SQLx CLI is installed:
   ```bash
   sqlx --version
   ```

3. Check migration history:
   ```bash
   psql $DATABASE_URL -c "SELECT * FROM _sqlx_migrations;"
   ```

### Kubernetes Issues

If Kubernetes resources aren't deploying:

1. Verify context:
   ```bash
   kubectl config current-context
   ```

2. Check cluster status:
   ```bash
   kubectl cluster-info
   ```

3. View events:
   ```bash
   kubectl get events -n etl-control-plane --sort-by='.lastTimestamp'
   ```
