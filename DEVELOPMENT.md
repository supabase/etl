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
cargo install --version='~0.7' sqlx-cli --no-default-features --features rustls,postgres
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

1. **Set the database URL:**

```bash
export DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DB
```

2. **Run etl-api migrations:**

```bash
./etl-api/scripts/run_migrations.sh
```

3. **Run etl-replicator migrations:**

```bash
./etl-replicator/scripts/run_migrations.sh
```

## Database Migrations

The project uses SQLx for database migrations. There are two sets of migrations:

### ETL API Migrations

Located in `etl-api/migrations/`, these create the control plane schema (`app` schema) for managing tenants, sources, destinations, and pipelines.

**Running API migrations:**

```bash
# From project root
./etl-api/scripts/run_migrations.sh

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

### ETL Replicator Migrations

Located in `etl-replicator/migrations/`, these create the replicator's state store schema (`etl` schema) for tracking replication state, table schemas, and mappings.

**Running replicator migrations:**

```bash
# From project root
./etl-replicator/scripts/run_migrations.sh

# Or manually with SQLx CLI (requires setting search_path)
psql $DATABASE_URL -c "create schema if not exists etl;"
sqlx migrate run --source etl-replicator/migrations --database-url "${DATABASE_URL}?options=-csearch_path%3Detl"
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

**Creating a new replicator migration:**

```bash
cd etl-replicator
sqlx migrate add <migration_name>
```

## Running the Services

### ETL API

```bash
cd etl-api
cargo run
```

The API requires the `DATABASE_URL` environment variable and a valid configuration file. See `etl-api/README.md` for configuration details.

### ETL Replicator

The replicator is typically deployed as a Kubernetes pod, but can be run locally for testing:

```bash
cd etl-replicator
cargo run
```

## Kubernetes Setup

The project uses Kubernetes for deploying replicators. The setup script configures the necessary resources.

**Prerequisites:**
- OrbStack with Kubernetes enabled (or another local Kubernetes cluster)
- `kubectl` configured with the `orbstack` context

**Manual Kubernetes setup:**

```bash
kubectl --context orbstack apply -f scripts/etl-data-plane.yaml
kubectl --context orbstack apply -f scripts/trusted-root-certs-config.yaml
```

**Checking deployed resources:**

```bash
# List replicator pods
kubectl get pods -n etl-control-plane -l app=etl-api

# View logs
kubectl logs -n etl-control-plane -l app=etl-api --tail=100

# Describe a specific pod
kubectl describe pod <pod-name> -n etl-control-plane
```

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
