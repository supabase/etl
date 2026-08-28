# `etl-replicator`

Ready-made, long-lived application that runs one [Supabase ETL](https://supabase.github.io/etl/)
pipeline with a Postgres-backed state store and one configured built-in
destination.

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `clickhouse` | ClickHouse | In progress |
| `ducklake` | DuckLake | In progress |
| `snowflake` | Snowflake | In progress |
| `iceberg` | Apache Iceberg | Deprecated |

BigQuery is the stable, recommended default. Compare maturity and limitations in
the [Destinations reference](https://supabase.github.io/etl/reference/destinations/).
For production configuration and credential handling, see the
[Standalone Replicator](https://supabase.github.io/etl/guides/standalone-replicator/)
guide.

## Local development

```bash
cargo x setup replicator --destination clickhouse
cargo x seed
cargo x run replicator
```

Destinations: `bigquery`, `clickhouse`, `ducklake`, `iceberg`, `snowflake`.
See [DEVELOPMENT.md](../../DEVELOPMENT.md). Do not commit
`crates/etl-replicator/configuration/`.

## Configuration

### Configuration Directory

The configuration directory is determined by:
- **`APP_CONFIG_DIR`** environment variable: If set, use this absolute path as the configuration directory
- **Fallback**: `configuration/` directory relative to the binary location

Configuration files are loaded in this order:
1. `base.(yaml|yml|json)` - Base configuration for all environments
2. `{environment}.(yaml|yml|json)` - Environment-specific overrides (environment defaults to `prod` unless `APP_ENVIRONMENT` is set to `dev`, `staging`, or `prod`)
3. `APP_`-prefixed environment variables - Runtime overrides (nested keys use `__`, lists are comma-separated)

### Examples

Using default configuration directory:
```bash
# Looks for configuration files in ./configuration/
./etl-replicator
```

Using custom configuration directory:
```bash
# Looks for configuration files in /etc/etl/replicator-config/
export APP_CONFIG_DIR=/etc/etl/replicator-config
./etl-replicator
```
