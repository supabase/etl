# `etl-examples`

Runnable binaries that replicate a Postgres publication to a built-in
destination using [Supabase ETL](https://supabase.github.io/etl/). ClickHouse is
the fastest local path: `cargo x init` starts it, and no cloud account is
required. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
for maturity and limitations, and the
[First Pipeline](https://supabase.github.io/etl/guides/first-pipeline/) tutorial
if you want to embed the `etl` crate instead.

## Available Examples

| Example                     | Binary       | Feature      | Destination                            | Status      |
| --------------------------- | ------------ | ------------ | -------------------------------------- | ----------- |
| [ClickHouse](#clickhouse)   | `clickhouse` | `clickhouse` | ClickHouse                             | In progress |
| [BigQuery](#bigquery)       | `bigquery`   | `bigquery`   | Google BigQuery                        | Stable      |
| [DuckLake](#ducklake)       | `ducklake`   | `ducklake`   | DuckLake                               | In progress |
| [Snowflake](#snowflake)     | `snowflake`  | `snowflake`  | Snowflake                              | In progress |

Iceberg is deprecated and is not recommended for new deployments. It has no
example here.

## Running an Example

The quickest path is `cargo x example` after sourcing `.env` (see `.env.example`):

```bash
source .env
cargo x example clickhouse
cargo x example bigquery
cargo x example ducklake
cargo x example snowflake
```

This handles the `-p etl-examples --features <name>` boilerplate, injects
`TESTS_DATABASE_*` as `--db-*` flags, and defaults `--db-name` to `etl_testdata`
and `--publication` to `seed_pub` (matching `cargo x seed`).

Override any flag by passing it explicitly:

```bash
cargo x example snowflake --db-name mydb --publication my_pub
```

### Building Manually

Each binary is feature-gated so you only compile the dependencies you need. Some
destinations (for example `ducklake`) pull in heavy native dependencies that can
take several minutes to compile.

```bash
cargo build --bin clickhouse -p etl-examples --features clickhouse
cargo run --bin clickhouse -p etl-examples --features clickhouse -- [flags]
```

Replace `clickhouse` with `bigquery`, `ducklake`, or `snowflake` as needed.

```bash
cargo build -p etl-examples --all-features
```

---

## Prerequisites

All examples require PostgreSQL 14 through 18 with logical replication enabled
and a user that has the `REPLICATION` role. See
[Configure Postgres](https://supabase.github.io/etl/guides/configure-postgres/)
for production settings. Do not publish ETL-owned tables in the `etl` schema.

```sql
-- postgresql.conf (or ALTER SYSTEM)
wal_level = logical

ALTER USER my_user REPLICATION;
```

### Quick Database Setup

The fastest way to get a seeded database with a publication is:

```bash
# Start the dev Postgres primary (port 5430) and read replica (port 6430)
cargo x init

# Create and seed a database with 3 tables (users, orders, events) and a publication
cargo x seed                          # defaults: etl_testdata, 1000 rows
cargo x seed --rows 100000            # more data
cargo x seed --database mydb --force  # custom name, recreate if exists
```

This creates `users`, `orders`, and `events` in the `public` schema and a
`seed_pub` publication for them. The destination-specific commands below use
that local default (`localhost:5430`, database `etl_testdata`, user `postgres`,
publication `seed_pub`). Change the flags if you use a different database.

### Manual Setup

```sql
CREATE PUBLICATION my_pub FOR TABLE orders, customers;
```

Avoid `FOR ALL TABLES` when the source database also stores ETL state; that
publication would include the `etl` schema. Prefer an explicit table list or
`FOR TABLES IN SCHEMA ...` for customer-owned schemas.

---

## Start with ClickHouse

`cargo x init` starts ClickHouse at `http://localhost:8123` (`etl` / `etl`).
After `cargo x seed`:

```bash
cargo x example clickhouse
```

No cloud account is required. Full flags and table-engine notes are in
[ClickHouse](#clickhouse) below. Use BigQuery, DuckLake, or Snowflake only when
you already have those services.

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
cargo run --bin bigquery -p etl-examples --features bigquery -- \
    --db-host localhost \
    --db-port 5430 \
    --db-name etl_testdata \
    --db-username postgres \
    --db-password postgres \
    --bq-sa-key-file /path/to/service-account-key.json \
    --bq-project-id your-gcp-project-id \
    --bq-dataset-id your_bigquery_dataset_id \
    --publication seed_pub
```

### All Flags

| Flag                           | Default      | Description                              |
| ------------------------------ | ------------ | ---------------------------------------- |
| `--db-host`                    | _(required)_ | Postgres host                            |
| `--db-port`                    | _(required)_ | Postgres port                            |
| `--db-name`                    | _(required)_ | Postgres database name                   |
| `--db-username`                | _(required)_ | Postgres user                            |
| `--db-password`                | —            | Postgres password                        |
| `--bq-sa-key-file`             | _(required)_ | Path to GCP service account key JSON     |
| `--bq-project-id`              | _(required)_ | GCP project ID                           |
| `--bq-dataset-id`              | _(required)_ | BigQuery dataset ID                      |
| `--max-batch-fill-duration-ms` | `5000`       | Max time to wait before flushing a batch |
| `--max-table-sync-workers`     | `4`          | Concurrent workers during initial copy   |
| `--publication`                | _(required)_ | Postgres publication name                |

---

## ClickHouse

Replicates a Postgres publication to ClickHouse over HTTP(S). ClickHouse **23.5
or newer** is required for the default `ReplacingMergeTree` engine.

`cargo x init` starts a local ClickHouse on `http://localhost:8123` with user
`etl` / password `etl`.

### Run

```bash
cargo run --bin clickhouse -p etl-examples --features clickhouse -- \
    --db-host localhost \
    --db-port 5430 \
    --db-name etl_testdata \
    --db-username postgres \
    --db-password postgres \
    --clickhouse-url http://localhost:8123 \
    --clickhouse-user etl \
    --clickhouse-password etl \
    --clickhouse-database default \
    --publication seed_pub
```

For HTTPS, pass an `https://` URL. TLS uses webpki root certificates.

### All Flags

| Flag                           | Default                 | Description                                              |
| ------------------------------ | ----------------------- | -------------------------------------------------------- |
| `--db-host`                    | _(required)_            | Postgres host                                            |
| `--db-port`                    | _(required)_            | Postgres port                                            |
| `--db-name`                    | _(required)_            | Postgres database name                                   |
| `--db-username`                | _(required)_            | Postgres user                                            |
| `--db-password`                | —                       | Postgres password                                        |
| `--clickhouse-url`             | _(required)_            | ClickHouse HTTP(S) URL                                   |
| `--clickhouse-user`            | _(required)_            | ClickHouse user                                          |
| `--clickhouse-password`        | —                       | ClickHouse password                                      |
| `--clickhouse-database`        | _(required)_            | ClickHouse database                                      |
| `--clickhouse-engine`          | `replacing_merge_tree`  | `replacing_merge_tree` or `merge_tree`                   |
| `--max-batch-fill-duration-ms` | `5000`                  | Max time to wait before flushing a batch                 |
| `--max-table-sync-workers`     | `4`                     | Concurrent workers during initial copy                   |
| `--publication`                | _(required)_            | Postgres publication name                                |

### Table Engines

Choose the layout per pipeline with `--clickhouse-engine`:

| Flag value                       | Engine               | Use it for                                              |
| -------------------------------- | -------------------- | ------------------------------------------------------- |
| `replacing_merge_tree` (default) | `ReplacingMergeTree` | Current-state replicas. Source must have a primary key. |
| `merge_tree`                     | `MergeTree`          | Append-only event log. Works for PK-less source tables. |

Table names are derived from the Postgres schema and table name using
double-underscore escaping (`public.orders` → `public_orders`,
`my_schema.t` → `my__schema_t`).

#### ReplacingMergeTree (default)

Each replicated table is created as `ReplacingMergeTree(_etl_version, _etl_deleted)`
keyed on the source primary key:

- `_etl_version UInt128` — packed `(commit_lsn << 64) | tx_ordinal`. Higher
  values win during a `FINAL` merge.
- `_etl_deleted UInt8` — `1` for DELETE events, `0` otherwise.

A companion `<table>__current` view hides those internals:

```sql
CREATE VIEW IF NOT EXISTS "public_orders__current" AS
SELECT <user columns>
FROM "public_orders" FINAL
WHERE _etl_deleted = 0
```

Prefer the `__current` view for current-state queries. The replicator never
runs `OPTIMIZE ... FINAL CLEANUP`; run that yourself when you want to reclaim
deleted rows on disk.

#### MergeTree

Each replicated table is created as `MergeTree() ORDER BY tuple()` with:

- `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`
- `cdc_lsn`: the Postgres commit LSN

Current state per primary key:

```sql
SELECT <user columns> FROM (
    SELECT * FROM "public_orders"
    ORDER BY cdc_lsn DESC LIMIT 1 BY (id)
)
WHERE cdc_operation != 'DELETE'
```

---

## DuckLake

Replicates a Postgres publication into a DuckLake data lake. DuckLake separates
storage into a **catalog** (metadata) and **data** (Parquet files). The
destination module can use `file`, `s3`, or `gs` data paths; this example binary
requires an `s3://` data path and S3-compatible credentials.

| Component   | Role                                | Example                               |
| ----------- | ----------------------------------- | ------------------------------------- |
| **Catalog** | Metadata (tables, snapshots, stats) | PostgreSQL database or a `file://` DB |
| **Data**    | Row data as Parquet files           | S3 / S3-compatible object storage     |

Each batch of rows is committed as a single Parquet snapshot. Source tables map
to DuckLake names with double-underscore escaping (`public.orders` →
`public_orders`, `my_schema.events` → `my__schema_events`).

### Prerequisites

1. A PostgreSQL database to act as the DuckLake catalog:
   ```sql
   CREATE DATABASE ducklake_catalog;
   ```
2. An S3 or S3-compatible bucket for Parquet files.

### Run

```bash
cargo run --bin ducklake -p etl-examples --features ducklake -- \
    --db-host localhost \
    --db-port 5430 \
    --db-name etl_testdata \
    --db-username postgres \
    --db-password postgres \
    --catalog-url postgres://postgres:postgres@localhost:5430/ducklake_catalog \
    --data-path s3://bucket/lake_data \
    --publication seed_pub \
    --s3-access-key-id placeholder-access-key \
    --s3-secret-access-key placeholder-secret-key
```

### S3-Compatible Endpoint

```bash
cargo run --bin ducklake -p etl-examples --features ducklake -- \
    --db-host localhost \
    --db-port 5430 \
    --db-name etl_testdata \
    --db-username postgres \
    --db-password postgres \
    --catalog-url "postgres://postgres:postgres@localhost:5430/ducklake_catalog?sslmode=disable" \
    --data-path s3://bucket-name/ \
    --publication seed_pub \
    --s3-access-key-id placeholder-access-key \
    --s3-secret-access-key placeholder-secret-key \
    --s3-region us-east-1 \
    --s3-endpoint 127.0.0.1:5000/s3 \
    --metadata-schema ducklake
```

Optional DuckDB logging on shutdown:

```bash
    --duckdb-log-storage-path /tmp/duckdb_logs \
    --duckdb-log-dump-path /tmp/duckdb_logs_dump.csv
```

### Vendored DuckDB Extensions

For offline local development on Linux or macOS:

```bash
cargo x vendor-duckdb
ETL_DUCKDB_EXTENSION_ROOT="$(pwd)/vendor/duckdb/extensions" \
  cargo run --bin ducklake -p etl-examples --features ducklake -- [flags]
```

If `ETL_DUCKDB_EXTENSION_ROOT` is unset, the destination also checks the
repository-local `vendor/duckdb/extensions` directory. Docker images ship
vendored extensions at `/app/duckdb_extensions`.

### All Flags

| Flag                           | Default      | Description                                                       |
| ------------------------------ | ------------ | ----------------------------------------------------------------- |
| `--db-host`                    | _(required)_ | Postgres host                                                     |
| `--db-port`                    | `5432`       | Postgres port                                                     |
| `--db-name`                    | _(required)_ | Postgres database name                                            |
| `--db-username`                | _(required)_ | Postgres user (must have REPLICATION)                             |
| `--db-password`                | —            | Postgres password (omit for trust auth)                           |
| `--catalog-url`                | _(required)_ | DuckLake catalog URL (`postgres://...` or `file://...`)           |
| `--data-path`                  | _(required)_ | `s3://` URI for Parquet files                                     |
| `--pool-size`                  | `4`          | DuckDB connection pool size                                       |
| `--max-batch-fill-duration-ms` | `5000`       | Max time to wait before flushing a batch                          |
| `--max-table-sync-workers`     | `4`          | Concurrent workers during initial copy                            |
| `--publication`                | _(required)_ | Postgres publication name                                         |
| `--s3-access-key-id`           | _(required)_ | S3 access key ID                                                  |
| `--s3-secret-access-key`       | _(required)_ | S3 secret access key                                              |
| `--s3-region`                  | `us-east-1`  | S3 region                                                         |
| `--s3-endpoint`                | —            | Custom S3 endpoint, e.g. `127.0.0.1:5000/s3` for Supabase Storage |
| `--s3-url-style`               | `path`       | URL style: `path` (MinIO/Supabase) or `vhost` (AWS)               |
| `--s3-use-ssl`                 | `false`      | Enable TLS for the S3 connection                                  |
| `--metadata-schema`            | —            | Postgres schema for DuckLake metadata tables (e.g. `ducklake`)    |
| `--duckdb-log-storage-path`    | —            | Enables DuckDB file-backed logging for each DuckDB connection     |
| `--duckdb-log-dump-path`       | —            | CSV file written from `duckdb_logs` during graceful shutdown      |

### Query the Replicated Data

```bash
duckdb :memory: -c "
  INSTALL ducklake; LOAD ducklake;
  ATTACH 'ducklake:postgres:host=''localhost'' port=''5430'' dbname=''ducklake_catalog'' user=''postgres'' password=''postgres'''
    AS lake (DATA_PATH 's3://bucket/lake_data');
  SELECT * FROM lake.public_orders;
"
```

```bash
RUST_LOG=debug cargo run --bin ducklake -p etl-examples --features ducklake -- [flags]
```

---

## Snowflake

Replicates a Postgres publication to a Snowflake database via Snowpipe Streaming.

### Prerequisites

1. A Snowflake account with a user configured for **key-pair authentication**.
2. `TESTS_SNOWFLAKE_CONNECTION` set with the Snowflake JSON connection string.
3. A target database and schema created in Snowflake.
4. A role with USAGE on warehouse/database/schema and CREATE TABLE, CREATE STAGE,
   CREATE PIPE on the schema.

See `crates/etl-destinations/src/snowflake/README.md` for setup instructions and SQL commands.

### Run

```bash
source .env
cargo run --bin snowflake -p etl-examples --features snowflake -- \
    --db-host localhost \
    --db-port 5430 \
    --db-name etl_testdata \
    --db-username postgres \
    --db-password postgres \
    --publication seed_pub
```

The Snowflake example reads credentials from `TESTS_SNOWFLAKE_CONNECTION` only.
The JSON object must contain `account`, `user`, `database`, `schema`, and
`private_key`; `role` and `private_key_passphrase` are optional. Put
`private_key` last so the target account details remain easy to inspect before
the secret material.

### All Flags

| Flag                           | Default      | Description                              |
| ------------------------------ | ------------ | ---------------------------------------- |
| `--db-host`                    | _(required)_ | Postgres host                            |
| `--db-port`                    | _(required)_ | Postgres port                            |
| `--db-name`                    | _(required)_ | Postgres database name                   |
| `--db-username`                | _(required)_ | Postgres user                            |
| `--db-password`                | —            | Postgres password                        |
| `--max-batch-fill-duration-ms` | `5000`       | Max time to wait before flushing a batch |
| `--max-table-sync-workers`     | `4`          | Concurrent workers during initial copy   |
| `--publication`                | _(required)_ | Postgres publication name                |
