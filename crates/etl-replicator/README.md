# `etl` - Replicator

Long-lived process that performs Postgres logical replication using the `etl` crate.

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
