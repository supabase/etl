# `etl-replicator`

Ready-made, long-lived application that runs one [Supabase ETL](https://supabase.github.io/etl/)
pipeline with a Postgres-backed state store and one configured built-in
destination.

| Feature | Destination | Status |
| --- | --- | --- |
| `clickhouse` | ClickHouse | In progress |
| `bigquery` | Google BigQuery | Stable |
| `ducklake` | DuckLake | In progress |
| `snowflake` | Snowflake | In progress |
| `iceberg` | Apache Iceberg | Deprecated |

ClickHouse is the local default: `cargo x init` starts it, and `cargo x setup
replicator` configures it. BigQuery is the most mature cloud destination.
Compare maturity and limitations in the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/).
For production configuration and credential handling, see the
[Standalone Replicator](https://supabase.github.io/etl/guides/standalone-replicator/)
guide.

## Local development

```bash
cargo x setup replicator
cargo x seed
cargo x run replicator
```

Destinations: `bigquery`, `clickhouse`, `ducklake`, `iceberg`, `snowflake`.
See [DEVELOPMENT.md](../../DEVELOPMENT.md). Do not commit
`crates/etl-replicator/configuration/`.

## Shutdown

SIGINT (Ctrl-C) and SIGTERM cancel pending asynchronous initialization. If a
signal interrupts startup of a constructed pipeline, its destination is shut
down. Initialization errors return immediately.

After startup, signals request shutdown and await the existing pipeline
completion future. WAL apply drains pending writes under its existing retry
and durability rules; interrupted initial copies restart from a fresh snapshot.
Workers finish before destination cleanup, then background samplers and the
health server are stopped and joined. Memory sampling remains active during
draining; enabled probes report unready while liveness stays healthy.

Requested task cancellations are silently accepted. The first worker or
teardown failure returns immediately and drops remaining owned handles to
request abort without awaiting further cleanup. Panics and unexpected
cancellations remain failures. Existing table-error and retry policies still
apply; uncheckpointed work is replayed after restart. A process restart does not
clear persisted table errors, including a timed retry interrupted by shutdown.

There is no internal shutdown grace timer or second-signal force-exit policy.
Destination or store operations can delay exit; Kubernetes supplies the external
SIGKILL limit. Aborting tasks cannot undo remote writes or stop native work.
DuckLake interruption belongs to destination teardown and query deadlines,
not an independent signal handler. Its watchdogs survive caller cancellation
but require a running async runtime; runtime teardown still waits for native
work to return.

Postgres drivers stop through client/observer channel closure without explicit
joins. Process-wide exporters and SDK workers retain their library lifecycles.
Failed pipelines skip destination teardown; embedded owners must explicitly
clean up destinations with ownership cycles.

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
