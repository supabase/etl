# `etl` - Postgres

This crate provides PostgreSQL-specific functionality and utilities for the ETL system. It includes:

- Database connections options for both `tokio-postgres` and `sqlx` (both are used since for using the logical replication protocol we need to use `tokio-postgres`)
- Shared types

## Features

| Feature      | Description                                                               |
|--------------|---------------------------------------------------------------------------|
| `sqlx`       | Enables SQLx-specific database connection options and utilities           |
| `tokio`      | Enables tokio-postgres-specific database connection options and utilities |
| `test-utils` | Enables test utilities for both SQLx and tokio-postgres implementations   |