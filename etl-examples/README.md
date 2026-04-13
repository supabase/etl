# `etl` — Examples

This crate contains practical examples demonstrating how to replicate data from
Postgres to various destinations using the ETL pipeline.

## Available Examples

| Example | Binary | Destination |
|---------|--------|-------------|
| [BigQuery](#bigquery) | `bigquery` | Google BigQuery (cloud data warehouse) |
| [DuckLake](#ducklake) | `ducklake` | DuckLake (open data lake format) |

---

## Prerequisites (all examples)

All examples require a Postgres database with **logical replication** enabled:

```sql
-- postgresql.conf (or ALTER SYSTEM)
wal_level = logical
```

Create a publication for the tables you want to replicate:

```sql
-- Specific tables
CREATE PUBLICATION my_pub FOR TABLE orders, customers;

-- All tables in the database
CREATE PUBLICATION my_pub FOR ALL TABLES;
```

The Postgres user must have the `REPLICATION` role:

```sql
ALTER USER my_user REPLICATION;
```

---

## DuckLake

Replicates a Postgres publication into a **DuckLake** data lake.

DuckLake separates storage into two components:

| Component | Role | Example |
|-----------|------|---------|
| **Catalog** | Metadata (tables, snapshots, stats) | PostgreSQL database |
| **Data** | Row data as Parquet files | Local directory or S3 / S3-compatible object storage |

The destination loads the required DuckDB extensions before attaching the lake.
Each batch of rows is committed as a single Parquet snapshot so the lake stays
consistent and queryable at all times.

### How it works

1. The pipeline connects to Postgres and performs an initial bulk copy of every
   table covered by the publication.
2. It then streams real-time INSERT / UPDATE / DELETE changes using logical
   replication.
3. Every Postgres table becomes a DuckLake table. The name is derived from the
   source schema and table name:

   | Postgres | DuckLake |
   |----------|----------|
   | `public.orders` | `public_orders` |
   | `my_schema.events` | `my__schema_events` |

### Prerequisites

1. A **PostgreSQL database** to act as the DuckLake catalog — create one if
   you don't have one already:
   ```sql
   CREATE DATABASE ducklake_catalog;
   ```
2. A **data directory** (local) or an S3 / S3-compatible bucket where Parquet
   files will be written.

### Run (local data)

```bash
cargo run --bin ducklake -p etl-examples -- \
    --db-host localhost \
    --db-port 5432 \
    --db-name mydb \
    --db-username postgres \
    --db-password mypassword \
    --catalog-url postgres://user:pass@localhost:5432/ducklake_catalog \
    --data-path file:///absolute/path/to/lake_data \
    --publication my_pub
```

The CLI also accepts plain local paths such as `./lake_data/` and normalizes
them to absolute `file://` URLs before constructing the destination.

### Example configuration

This is a fuller local example that also enables a dedicated DuckDB log dump on
shutdown:

```bash
cargo run --bin ducklake -p etl-examples -- \
    --db-host postgres.etl-data-plane.svc.cluster.local \
    --db-port 5432 \
    --db-name mydb \
    --db-username postgres \
    --db-password password \
    --catalog-url "postgres://postgres:password@postgres.etl-data-plane.svc.cluster.local:5432/mydb?sslmode=disable" \
    --data-path /Users/bnj/misc/parquet_files \
    --publication my_pub \
    --metadata-schema ducklake \
    --pool-size 4 \
    --max-batch-fill-duration-ms 5000 \
    --max-table-sync-workers 4 \
    --duckdb-log-storage-path /tmp/duckdb_logs \
    --duckdb-log-dump-path /tmp/duckdb_logs_dump.csv
```

In this example:

- `--catalog-url` points to the PostgreSQL database that stores DuckLake metadata.
- `--data-path` is a plain local path and will be normalized to a `file://` URL.
- `--metadata-schema ducklake` keeps DuckLake metadata tables in a dedicated Postgres schema.
- `--duckdb-log-storage-path` enables `CALL enable_logging(storage_path = ...)` for each DuckDB connection.
- `--duckdb-log-dump-path` writes a CSV dump of `SELECT * FROM duckdb_logs` during graceful shutdown.

### Vendored DuckDB extensions

For offline local development on Linux or macOS, you can prefetch the required
DuckDB extensions into the repository and point the destination at them:

```bash
./scripts/vendor_duckdb_extensions.sh
ETL_DUCKDB_EXTENSION_ROOT="$(pwd)/vendor/duckdb/extensions" \
  cargo run --bin ducklake -p etl-examples -- [flags]
```

If `ETL_DUCKDB_EXTENSION_ROOT` is unset, the destination also checks the
repository-local `vendor/duckdb/extensions` directory automatically. Docker
images do not need the env var because they already ship vendored extensions at
`/app/duckdb_extensions`.

### Run (S3 / S3-compatible data)

```bash
cargo run --bin ducklake -p etl-examples -- \
    --db-host <pg-host> \
    --db-port <pg-port> \
    --db-name <pg-database> \
    --db-username <pg-user> \
    --db-password <pg-password> \
    --catalog-url "postgres://<pg-user>:<pg-password>@<pg-host>:<pg-port>/<pg-database>?sslmode=disable" \
    --data-path s3://<bucket-name>/ \
    --publication <publication-name> \
    --s3-access-key-id <access-key-id> \
    --s3-secret-access-key <secret-access-key> \
    --s3-region <region> \
    --s3-endpoint <host>:<port>/<path> \
    --metadata-schema <schema-name>
```

The example CLI exposes S3 / S3-compatible cloud credentials today. For
`s3://` data paths, the destination loads DuckDB's `httpfs` extension during
connection setup.

### All flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db-host` | *(required)* | Postgres host |
| `--db-port` | `5432` | Postgres port |
| `--db-name` | *(required)* | Postgres database name |
| `--db-username` | *(required)* | Postgres user (must have REPLICATION) |
| `--db-password` | — | Postgres password (omit for trust auth) |
| `--catalog-url` | *(required)* | DuckLake catalog URL (`postgres://...` or `file://...`) |
| `--data-path` | *(required)* | Local path / `file://` URL or `s3://` URI for Parquet files |
| `--pool-size` | `4` | DuckDB connection pool size |
| `--max-batch-fill-duration-ms` | `5000` | Max time to wait before flushing a batch |
| `--max-table-sync-workers` | `4` | Concurrent workers during initial copy |
| `--publication` | *(required)* | Postgres publication name |
| `--s3-access-key-id` | — | S3 access key ID (required for private S3 buckets) |
| `--s3-secret-access-key` | — | S3 secret access key |
| `--s3-region` | `us-east-1` | S3 region |
| `--s3-endpoint` | — | Custom S3 endpoint, e.g. `127.0.0.1:5000/s3` for Supabase Storage |
| `--s3-url-style` | `path` | URL style: `path` (MinIO/Supabase) or `vhost` (AWS) |
| `--s3-use-ssl` | `false` | Enable TLS for the S3 connection |
| `--metadata-schema` | — | Postgres schema for DuckLake metadata tables (e.g. `ducklake`) |
| `--duckdb-log-storage-path` | — | Enables DuckDB file-backed logging for each DuckDB connection |
| `--duckdb-log-dump-path` | — | CSV file written from `duckdb_logs` during graceful shutdown |

### Query the replicated data

Use the DuckDB CLI (install with `brew install duckdb` on macOS) to query the
lake at any time — even while the pipeline is running:

```bash
duckdb :memory: -c "
  INSTALL ducklake; LOAD ducklake;
  ATTACH 'ducklake:postgres:host=''localhost'' port=''5432'' dbname=''ducklake_catalog'' user=''user'' password=''pass'''
    AS lake (DATA_PATH 'file:///absolute/path/to/lake_data');
  SELECT * FROM lake.public_orders;
  SELECT COUNT(*) FROM lake.public_customers;
"
```

### Verbose logging

```bash
RUST_LOG=debug cargo run --bin ducklake -p etl-examples -- [flags]
```

---

## BigQuery

Replicates a Postgres publication to a Google BigQuery dataset.

### Prerequisites

1. A Google Cloud project with the BigQuery API enabled.
2. A service account with the **BigQuery Data Editor** and **BigQuery Job
   User** roles.
3. The service account key file downloaded from the GCP Console
   (`IAM & Admin → Service Accounts → Keys → Add Key → JSON`).
4. A BigQuery dataset created in your project.

### Run

```bash
cargo run --bin bigquery -p etl-examples -- \
    --db-host localhost \
    --db-port 5432 \
    --db-name postgres \
    --db-username postgres \
    --db-password password \
    --bq-sa-key-file /path/to/service-account-key.json \
    --bq-project-id your-gcp-project-id \
    --bq-dataset-id your_bigquery_dataset_id \
    --publication my_pub
```

### All flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db-host` | *(required)* | Postgres host |
| `--db-port` | *(required)* | Postgres port |
| `--db-name` | *(required)* | Postgres database name |
| `--db-username` | *(required)* | Postgres user |
| `--db-password` | — | Postgres password |
| `--bq-sa-key-file` | *(required)* | Path to GCP service account key JSON |
| `--bq-project-id` | *(required)* | GCP project ID |
| `--bq-dataset-id` | *(required)* | BigQuery dataset ID |
| `--max-batch-fill-duration-ms` | `5000` | Max time to wait before flushing a batch |
| `--max-table-sync-workers` | `4` | Concurrent workers during initial copy |
| `--publication` | *(required)* | Postgres publication name |
