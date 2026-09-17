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

SIGINT (Ctrl-C) and SIGTERM cancel pending asynchronous store, destination,
and pipeline initialization. A constructed pipeline also shuts down its
destination if initialization fails or is cancelled.

After startup, signals request pipeline shutdown and await its existing
completion future. WAL apply stops intake and drains pending writes. Initial
copy aborts and joins its child tasks; an interrupted copy is redone on restart.
Background samplers are aborted and joined, and errors returned by workers or
cleanup propagate to the process exit status. Existing error policies still
apply: table errors can be recorded in the store, and shutdown during a timed
retry wait ends that retry successfully. Memory sampling stays active while
workers and the destination drain, then the pipeline aborts and joins its sampler.
DuckLake does not handle process signals independently: native connections are
interrupted by destination teardown after apply draining, or by query deadlines.

When enabled, activity probes stay live and report unready during draining.
The probe server is stopped and joined after pipeline teardown, including on
initialization failure or cancellation.

There is no internal grace-period timer or second-signal force-exit policy.
An in-flight apply handler or destination drain can delay shutdown. Kubernetes
enforces its termination grace period with SIGKILL, which cannot run cleanup;
restart recovery uses persisted progress and destination replay semantics.
Aborting an async task also cannot undo a remote write or stop native work
already running through `spawn_blocking`. DuckDB query interruption and deadline
watchdogs survive cancellation of the async caller while the runtime is running.
Runtime teardown stops polling async watchdogs and still waits for native work,
including setup, to return; SIGKILL remains the external limit for a stuck process.

Postgres connection drivers terminate through client/observer channel closure;
these drivers are not explicitly joined. Process-wide exporters and third-party
SDK workers retain their library/runtime lifecycles rather than being stopped
by an individual pipeline.

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
