# `etl` — Examples

This crate contains practical examples demonstrating how to replicate data from
Postgres to various destinations using the ETL pipeline.

## Available Examples

| Example | Binary | Destination |
|---------|--------|-------------|
| [BigQuery](#bigquery) | `etl-examples` | Google BigQuery (cloud data warehouse) |
| [DuckDB](#duckdb) | `duckdb` | DuckDB (embedded local database) |

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

## DuckDB

Replicates a Postgres publication into a local **DuckDB** `.db` file. No cloud
account or external service is required — DuckDB is compiled directly into the
binary via the `bundled` feature.

### How it works

1. The pipeline connects to Postgres and performs an initial bulk copy of every
   table covered by the publication.
2. It then streams real-time INSERT / UPDATE / DELETE changes using logical
   replication.
3. Every Postgres table becomes a DuckDB table. The table name is derived from
   the source schema and table name:

   | Postgres | DuckDB |
   |----------|--------|
   | `public.orders` | `public_orders` |
   | `my_schema.events` | `my__schema_events` |


### Run

```bash
cargo run --bin duckdb -p etl-examples -- \
    --db-host localhost \
    --db-port 5432 \
    --db-name mydb \
    --db-username postgres \
    --db-password mypassword \
    --duckdb-path ./replication.db \
    --publication my_pub
```

The database file is created automatically if it does not exist. The pipeline
runs until you press **Ctrl+C**.

### All flags

| Flag | Default | Description |
|------|---------|-------------|
| `--db-host` | *(required)* | Postgres host |
| `--db-port` | `5432` | Postgres port |
| `--db-name` | *(required)* | Postgres database name |
| `--db-username` | *(required)* | Postgres user (must have REPLICATION) |
| `--db-password` | — | Postgres password (omit for trust auth) |
| `--duckdb-path` | *(required)* | Path to the DuckDB `.db` file |
| `--duckdb-pool-size` | `4` | DuckDB connection pool size |
| `--max-batch-fill-duration-ms` | `5000` | Max time to wait before flushing a batch |
| `--max-table-sync-workers` | `4` | Concurrent workers during initial copy |
| `--publication` | *(required)* | Postgres publication name |

### Query the replicated data

Once the pipeline is running you can open the `.db` file at any time with the
DuckDB CLI (install with `brew install duckdb` on macOS):

```bash
duckdb ./replication.db
```

```sql
-- List replicated tables
SHOW TABLES;

-- Query a replicated table
SELECT * FROM public_orders;

-- Count rows per table
SELECT COUNT(*) FROM public_orders;
```

### Verbose logging

```bash
RUST_LOG=debug cargo run --bin duckdb -p etl-examples -- [flags]
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
cargo run -p etl-examples -- \
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
