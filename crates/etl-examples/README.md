# etl-examples

Example binaries demonstrating Postgres-to-destination replication using the ETL pipeline.

Each binary is feature-gated so you only compile the dependencies you need. Some destinations (e.g. `ducklake`) pull in heavy native dependencies that can take several minutes to compile, there's no reason to pay that cost when you only want to try BigQuery.

## Examples

| Binary       | Feature flag | Destination                        |
| ------------ | ------------ | ---------------------------------- |
| `bigquery`   | `bigquery`   | Google BigQuery                    |
| `clickhouse` | `clickhouse` | ClickHouse                         |
| `ducklake`   | `ducklake`   | DuckLake (DuckDB-backed data lake) |

## Building and running

### Single example

```bash
# Build
cargo build --bin bigquery -p etl-examples --features bigquery

# Run
cargo run --bin bigquery -p etl-examples --features bigquery -- [flags]
```

Replace `bigquery` with `clickhouse` or `ducklake` as needed.

### All examples

```bash
cargo build -p etl-examples --all-features
```

## Example-specific flags

Run any binary with `--help` to see its full flag list:

```bash
cargo run --bin bigquery -p etl-examples --features bigquery -- --help
```
