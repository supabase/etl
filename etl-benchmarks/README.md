# `etl` - Benchmarks

Performance benchmarks for the ETL system to measure and track replication performance across different scenarios and configurations.

## Available Benchmarks

- **table_copies**: Measures performance of initial table copying operations

## Prerequisites

Before running benchmarks, ensure you have:

- A Postgres database set up
- A publication created with the tables you want to benchmark
- For BigQuery benchmarks: GCP project, dataset, and service account key file

## How Benchmarks Work

Benchmarks automatically handle setup and cleanup:

1. **Setup**: Each benchmark creates the necessary replication slots when the pipeline starts
2. **Execution**: The benchmark runs the table copy operation and measures performance
3. **Cleanup**: After completion, all replication slots are automatically dropped to ensure a clean state for the next run

No manual cleanup is required between benchmark runs.

## Quick Start

### Run Basic Benchmark (Null Destination)

Test with fastest performance using a null destination that discards data:

> **Note:** Replication slots are automatically cleaned up after each benchmark run. No manual preparation step is needed.

```bash
cargo bench --bench table_copies -- --log-target terminal run \
  --host localhost --port 5430 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null
```

### Run BigQuery Benchmark

Test with real BigQuery destination:

```bash
cargo bench --bench table_copies --features bigquery -- --log-target terminal run \
  --host localhost --port 5430 --database bench \
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

| Parameter            | Description                              | Default     |
| -------------------- | ---------------------------------------- | ----------- |
| `--host`             | Postgres host                            | `localhost` |
| `--port`             | Postgres port                            | `5430`      |
| `--database`         | Database name                            | `bench`     |
| `--username`         | Postgres username                        | `postgres`  |
| `--password`         | Postgres password                        | (optional)  |
| `--publication-name` | Publication to replicate from            | `bench_pub` |
| `--table-ids`        | Comma-separated table IDs to replicate   | (required)  |
| `--destination`      | Destination type (`null` or `big-query`) | `null`      |

### Performance Tuning Parameters

| Parameter                          | Description                                          | Default   |
| ---------------------------------- | ---------------------------------------------------- | --------- |
| `--batch-max-size`                 | Maximum batch size                                   | `1000000` |
| `--batch-max-fill-ms`              | Maximum batch fill time (ms)                         | `10000`   |
| `--max-table-sync-workers`         | Max concurrent table sync workers                    | `8`       |
| `--max-copy-connections-per-table` | Number of parallel connections per table for copying | `1`       |

### BigQuery Parameters

| Parameter                     | Description                        | Required for BigQuery |
| ----------------------------- | ---------------------------------- | --------------------- |
| `--bq-project-id`             | GCP project ID                     | Yes                   |
| `--bq-dataset-id`             | BigQuery dataset ID                | Yes                   |
| `--bq-sa-key-file`            | Service account key file path      | Yes                   |
| `--bq-max-staleness-mins`     | Max staleness in minutes           | No                    |
| `--bq-max-concurrent-streams` | Max concurrent BigQuery streams    | No                    |

### Logging Options

| Parameter               | Description                         |
| ----------------------- | ----------------------------------- |
| `--log-target terminal` | Colorized terminal output (default) |
| `--log-target file`     | Write logs to `logs/` directory     |

Set `RUST_LOG` environment variable to control log levels (default: `info`):

```bash
RUST_LOG=debug cargo bench --bench table_copies -- run ...
```

## Using the Benchmark Script

The `benchmark.sh` script provides a convenient wrapper around the benchmark with additional features. It accepts environment variables for configuration:

### Database Configuration

| Environment Variable | Description       | Default     |
| -------------------- | ----------------- | ----------- |
| `POSTGRES_HOST`      | Postgres host     | `localhost` |
| `POSTGRES_PORT`      | Postgres port     | `5430`      |
| `POSTGRES_DB`        | Database name     | `bench`     |
| `POSTGRES_USER`      | Postgres username | `postgres`  |
| `POSTGRES_PASSWORD`  | Postgres password | `postgres`  |

### Benchmark Configuration

| Environment Variable              | Description                                                       | Default                                        |
| --------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------- |
| `HYPERFINE_RUNS`                  | Number of benchmark runs                                          | `1`                                            |
| `PUBLICATION_NAME`                | Publication name                                                  | `bench_pub`                                    |
| `BATCH_MAX_SIZE`                  | Maximum batch size                                                | `1000000`                                      |
| `BATCH_MAX_FILL_MS`               | Maximum batch fill time (ms)                                      | `10000`                                        |
| `MAX_TABLE_SYNC_WORKERS`          | Max concurrent table sync workers                                 | `8`                                            |
| `MAX_COPY_CONNECTIONS_PER_TABLE`  | Number of parallel connections per table for copying              | `1`                                            |
| `TPCC_TABLES`                     | Comma-separated list of TPC-C tables to benchmark                 | `customer,district,item,new_order,...` (all 8) |
| `DESTINATION`                     | Destination type (`null` or `big-query`)                          | `null`                                         |
| `LOG_TARGET`                      | Where to send logs (`terminal` or `file`)                         | `terminal`                                     |
| `DRY_RUN`                         | Show commands without executing (`true` or `false`)               | `false`                                        |
| `PREPARE_TPCC`                    | Automatically run prepare_tpcc.sh if tables don't exist           | `true`                                         |

### BigQuery Configuration (when DESTINATION=big-query)

| Environment Variable       | Description                     | Required |
| -------------------------- | ------------------------------- | -------- |
| `BQ_PROJECT_ID`            | GCP project ID                  | Yes      |
| `BQ_DATASET_ID`            | BigQuery dataset ID             | Yes      |
| `BQ_SA_KEY_FILE`           | Service account key file path   | Yes      |
| `BQ_MAX_STALENESS_MINS`    | Max staleness in minutes        | No       |
| `BQ_MAX_CONCURRENT_STREAMS`| Max concurrent BigQuery streams | No       |

### Script Examples

```bash
# Run with default settings
./etl-benchmarks/scripts/benchmark.sh

# Test with 8 parallel connections per table
MAX_COPY_CONNECTIONS_PER_TABLE=8 ./etl-benchmarks/scripts/benchmark.sh

# Benchmark only specific tables
TPCC_TABLES=stock,order_line ./etl-benchmarks/scripts/benchmark.sh

# Run multiple iterations
HYPERFINE_RUNS=5 ./etl-benchmarks/scripts/benchmark.sh

# Dry run to see what would be executed
DRY_RUN=true ./etl-benchmarks/scripts/benchmark.sh
```

## Complete Examples

### Production-like Testing with File Logging

```bash
cargo bench --bench table_copies -- --log-target file run \
  --host localhost --port 5430 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null
```

### Parallel Table Copy Test

Test parallel copy performance with multiple connections per table:

```bash
cargo bench --bench table_copies -- --log-target terminal run \
  --host localhost --port 5430 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --max-copy-connections-per-table 4 \
  --destination null
```

### High-throughput BigQuery Test

```bash
cargo bench --bench table_copies --features bigquery -- --log-target terminal run \
  --host localhost --port 5430 --database bench \
  --username postgres --password mypass \
  --publication-name bench_pub \
  --table-ids 1,2,3,4,5 \
  --destination big-query \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json \
  --batch-max-size 50000 \
  --max-table-sync-workers 16 \
  --max-copy-connections-per-table 8
```

The benchmark will measure the time it takes to complete the initial table copy phase for all specified tables.
