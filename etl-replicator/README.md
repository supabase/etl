# `etl` - Replicator

Long-lived process that performs Postgres logical replication using the `etl` crate.

## Configuration

- Looks for `configuration/base.(yaml|yml|json)` plus `configuration/{environment}.(yaml|yml|json)` where `environment` is driven by `APP_ENVIRONMENT` (defaults to `prod`).
- Environment variables prefixed with `APP_` override parsed values (`__` delimits nested keys, lists are comma-separated).
