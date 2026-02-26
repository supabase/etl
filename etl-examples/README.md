# `etl` - Examples

This crate contains practical examples demonstrating how to use the ETL system for data replication from Postgres to various destinations.

## Available Examples

- **BigQuery Integration**: Demonstrates replicating Postgres data to Google BigQuery
- **ClickHouse Integration**: Demonstrates replicating Postgres data to ClickHouse

## Quick Start

To quickly try out `etl`, you can run the BigQuery example. First, create a publication in Postgres which includes the tables you want to replicate:

```sql
create publication my_publication
for table table1, table2;
```

Then run the BigQuery example:

```bash
cargo run -p etl-examples -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name postgres \
        --db-username postgres \
        --db-password password \
        --bq-sa-key-file /path/to/your/service-account-key.json \
        --bq-project-id your-gcp-project-id \
        --bq-dataset-id your_bigquery_dataset_id \
        --publication my_publication
```

In the above example, `etl` connects to a Postgres database named `postgres` running on `localhost:5432` with a username `postgres` and password `password`.

## ClickHouse Setup

To run the ClickHouse example, you'll need a running ClickHouse instance accessible over HTTP(S).

Create a publication in Postgres:

```sql
create publication my_pub
for table table1, table2;
```

Then run the ClickHouse example:

```bash
cargo run -p etl-examples --bin clickhouse -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name postgres \
        --db-username postgres \
        --db-password password \
        --ch-url http://localhost:8123 \
        --ch-user default \
        --ch-database default \
        --publication my_pub
```

Each Postgres table is replicated as an append-only `MergeTree` table. Two CDC metadata
columns are appended to every row:

- `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`
- `cdc_lsn`: the Postgres LSN at the time of the change

Table names are derived from the Postgres schema and table name using double-underscore
escaping (e.g. `public.orders` → `public_orders`, `my_schema.t` → `my__schema_t`).

For HTTPS connections, provide an `https://` URL — TLS is handled automatically using
webpki root certificates. Use `--ch-password` if your ClickHouse instance requires
authentication.

## Prerequisites

Before running the examples, you'll need to set up a Postgres database with logical replication enabled.

## BigQuery Setup

To run the BigQuery example, you'll need:

1. A Google Cloud Project with BigQuery API enabled
2. A service account key file with BigQuery permissions
3. A BigQuery dataset created in your project

The example will automatically create tables in the specified dataset based on your Postgres schema.
