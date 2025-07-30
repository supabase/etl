# `etl` API

This API service provides a RESTful interface for managing PostgreSQL replication pipelines. It enables you to:

- Create and manage replication pipelines between PostgreSQL sources and destinations
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

Before you begin, please refer to our [Database Setup Guide](../docs/guides/database-setup.md).

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
sqlx migrate run
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
