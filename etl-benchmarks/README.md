# `etl` - Benchmarks

Performance benchmarks for the ETL system to measure and track replication performance across different scenarios and configurations.

## Available Benchmarks

- **table_copies**: Measures performance of initial table copying operations

## Prerequisites

Before running benchmarks, ensure you have:
- A PostgreSQL database set up
- A publication created with the tables you want to benchmark
- For BigQuery benchmarks: GCP project, dataset, and service account key file

## Quick Start

### 1. Prepare Your Environment

First, clean up any existing replication slots:

```bash
cargo bench --bench table_copies -- --log-target terminal prepare \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass
```

### 2. Run Basic Benchmark (Null Destination)

Test with fastest performance using a null destination that discards data:

```bash
cargo bench --bench table_copies -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null
```

### 3. Run BigQuery Benchmark

Test with real BigQuery destination:

```bash
cargo bench --bench table_copies --features bigquery -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination big-query \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json
```

## Command Reference

### Common Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--host` | PostgreSQL host | `localhost` |
| `--port` | PostgreSQL port | `5432` |
| `--database` | Database name | `bench` |
| `--username` | PostgreSQL username | `postgres` |
| `--password` | PostgreSQL password | (optional) |
| `--publication-name` | Publication to replicate from | `bench_pub` |
| `--table-ids` | Comma-separated table IDs to replicate | (required) |
| `--destination` | Destination type (`null` or `big-query`) | `null` |

### Performance Tuning Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--batch-max-size` | Maximum batch size | `100000` |
| `--batch-max-fill-ms` | Maximum batch fill time (ms) | `10000` |
| `--max-table-sync-workers` | Max concurrent table sync workers | `8` |

### BigQuery Parameters

| Parameter | Description | Required for BigQuery |
|-----------|-------------|----------------------|
| `--bq-project-id` | GCP project ID | Yes |
| `--bq-dataset-id` | BigQuery dataset ID | Yes |
| `--bq-sa-key-file` | Service account key file path | Yes |
| `--bq-max-staleness-mins` | Max staleness in minutes | No |

### Logging Options

| Parameter | Description |
|-----------|-------------|
| `--log-target terminal` | Colorized terminal output (default) |
| `--log-target file` | Write logs to `logs/` directory |

Set `RUST_LOG` environment variable to control log levels (default: `info`):
```bash
RUST_LOG=debug cargo bench --bench table_copies -- run ...
```

## Complete Examples

### Production-like Testing with File Logging
```bash
cargo bench --bench table_copies -- --log-target file run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null
```

### High-throughput BigQuery Test
```bash
cargo bench --bench table_copies --features bigquery -- --log-target terminal run \
  --host localhost --port 5432 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3,4,5 \
  --destination big-query \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json \
  --batch-max-size 50000 \
  --max-table-sync-workers 16
```

The benchmark will measure the time it takes to complete the initial table copy phase for all specified tables.