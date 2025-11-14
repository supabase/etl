# etl-mysql

MySQL database connection utilities for all crates.

This crate provides database connection options and utilities for working with MySQL.
It supports the `sqlx` crate through feature flags and includes utilities for MySQL binlog-based replication.

## Features

- `sqlx` - Enables sqlx MySQL support
- `tokio` - Enables tokio-based utilities
- `replication` - Enables MySQL binlog replication support
- `test-utils` - Enables test utilities
- `bigquery` - Enables BigQuery integration support
