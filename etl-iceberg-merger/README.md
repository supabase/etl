# ETL Iceberg Merger

Background merge runner that reads raw CDC events from Iceberg changelog tables, uses a Puffin-file-backed secondary index to deduplicate and merge events, and produces clean mirror tables.

## Overview

The merger service runs periodically (configurable interval) and processes configured changelog tables:

1. **Loads Puffin Index**: Reads the secondary index from changelog table metadata
2. **Scans Changelog**: Reads raw CDC events from Parquet files
3. **Deduplicates**: Uses the index to keep only the latest record per primary key
4. **Compacts**: Filters out DELETE operations and builds clean mirror data
5. **Writes Mirror**: Commits deduplicated data to mirror table with new index

## Architecture

### Puffin-Based Index

Adapted from moonlink's GlobalIndex design, the index structure:

- Maps primary key hashes to file locations (file_path, row_offset)
- Serialized as Parquet within Puffin files
- Stored in Iceberg table metadata
- Enables efficient deduplication during merge

### Components

- **Scheduler**: Periodic execution loop with graceful shutdown
- **Index**: Puffin-based secondary index for PK→location mapping
- **Merge**: Core deduplication and compaction logic
- **Config**: Configuration loading and validation

## Configuration

Configuration is loaded from a YAML file (see `k8s/configmap.yaml` for example):

```yaml
project_ref: "example-project"
environment: "production"

iceberg:
  catalog_url: "https://catalog.example.com/api/v1"
  catalog_token: "${ICEBERG_CATALOG_TOKEN}"
  warehouse: "main"
  s3_endpoint: "https://s3.us-east-1.amazonaws.com"
  s3_access_key_id: "${ICEBERG_S3_ACCESS_KEY_ID}"
  s3_secret_access_key: "${ICEBERG_S3_SECRET_ACCESS_KEY}"
  s3_region: "us-east-1"

tables:
  - namespace: "public"
    changelog_table: "users_changelog"
    mirror_table: "users"
    primary_keys:
      - "id"
    sequence_column: "cdc_sequence_number"
    operation_column: "cdc_operation"

merge_interval: "1h"
batch_size: 10000

sentry:
  dsn: "${SENTRY_DSN}"
```

### Environment Variables

Sensitive values should be provided via environment variables:

- `ICEBERG_CATALOG_TOKEN`: Catalog authentication token
- `ICEBERG_S3_ACCESS_KEY_ID`: S3 access key
- `ICEBERG_S3_SECRET_ACCESS_KEY`: S3 secret key
- `SENTRY_DSN`: Sentry DSN for error reporting (optional)
- `APP_VERSION`: Application version for tagging (optional)

## Deployment

### Kubernetes CronJob

The merger runs as a Kubernetes CronJob for periodic execution:

```bash
# Apply configuration and secrets
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml

# Deploy the CronJob
kubectl apply -f k8s/cronjob.yaml

# Check status
kubectl get cronjobs
kubectl get jobs
kubectl logs -l app=iceberg-merger
```

### Docker Build

```bash
# Build the image
docker build -t etl-iceberg-merger:latest -f etl-iceberg-merger/Dockerfile .

# Run locally (requires config file)
docker run -v $(pwd)/config.yaml:/etc/merger/config.yaml \
  -e CONFIG_FILE=/etc/merger/config.yaml \
  -e ICEBERG_CATALOG_TOKEN=your-token \
  -e ICEBERG_S3_ACCESS_KEY_ID=your-key \
  -e ICEBERG_S3_SECRET_ACCESS_KEY=your-secret \
  etl-iceberg-merger:latest
```

## Metrics

The merger exports Prometheus metrics:

### Merge Metrics

- `merger.cycle.completed`: Total merge cycles completed
- `merger.cycle.duration_seconds`: Duration of each merge cycle
- `merger.table.success`: Successful table merges (by table)
- `merger.table.failure`: Failed table merges (by table)
- `merger.table.duration_seconds`: Duration per table (by table)
- `merger.rows.scanned`: Rows scanned from changelog (by table)
- `merger.rows.deduplicated`: Rows after deduplication (by table)
- `merger.merge.duration_seconds`: Merge operation duration (by table)

### System Metrics

- `jemalloc.allocated`: Allocated memory bytes
- `jemalloc.resident`: Resident memory (RSS)
- `jemalloc.metadata`: Metadata overhead
- `jemalloc.retained`: Retained but unused memory

## Observability

### Tracing

Structured logging with OpenTelemetry-compatible spans:

- Enable debug logs: `RUST_LOG=etl_iceberg_merger=debug`
- Enable trace logs: `ENABLE_TRACING=1`

### Sentry

Error reporting to Sentry:

- Configure DSN in config or via `SENTRY_DSN` env var
- All panics and errors are automatically captured
- Tagged with service: `iceberg-merger`

## Development

### Build

```bash
cargo build -p etl-iceberg-merger
```

### Run

```bash
# With config file
CONFIG_FILE=config.yaml cargo run -p etl-iceberg-merger

# With environment overrides
RUST_LOG=debug cargo run -p etl-iceberg-merger
```

### Test

```bash
cargo test -p etl-iceberg-merger
```

## Further Considerations

### Index Compaction

As mirror tables grow, Puffin index files will accumulate. Future enhancements:

- Periodic rebuild/merge of index files (similar to moonlink's `build_from_merge`)
- Rely on Iceberg's snapshot expiration to clean old indices
- Configurable index compaction strategy

### Partial Failure Handling

Current behavior on failure:

- Merge succeeds → entire operation committed atomically
- Merge fails → error logged, metrics updated, Sentry notified
- Retry on next scheduled run

Future enhancements:

- Track last processed CDC sequence number
- Resume from checkpoint on retry
- Partial batch commits with progress tracking

### Schema Evolution

Current behavior:

- Mirror schema derived from changelog on first creation
- Schema changes require manual intervention

Future enhancements:

- Automatic schema drift detection
- Propagate ADD COLUMN / DROP COLUMN to mirror tables
- Configurable evolution policies (strict vs permissive)
