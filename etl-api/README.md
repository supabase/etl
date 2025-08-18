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
- Multi-tenant support with isolated configurations
- Kubernetes deployment support
- Secure configuration management
- Database schema versioning with migrations
- Integration with the core ETL system

## Table of Contents

- [Prerequisites](#prerequisites)
- [Development](#development)
- [Environment Variables](#environment-variables)

## Prerequisites

Before running the API, you must have:

- A running Postgres instance reachable via `DATABASE_URL`.
- The `etl-api` database schema applied (SQLx migrations).

Quickest way: use the setup script to start Postgres (via Docker) and run migrations automatically.

```bash
# Starts a local Postgres (if needed) and applies etl-api migrations
./scripts/init_db.sh
```

Alternative: if you already have a Postgres database, set `DATABASE_URL` and apply migrations manually:

```bash
export DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DB
sqlx migrate run --source etl-api/migrations
```

## Development

### Database Migrations

#### Adding a New Migration

To create a new migration file:

```bash
sqlx migrate add <migration-name>
```

#### Running Migrations

To apply all pending migrations:

```bash
sqlx migrate run --source etl-api/migrations
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
