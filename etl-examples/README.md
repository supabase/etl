# `etl` - Examples

This crate contains practical examples demonstrating how to use the ETL system for data replication from Postgres to various destinations.

## Available Examples

- **BigQuery Integration**: Demonstrates replicating Postgres data to Google BigQuery

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

## Prerequisites

Before running the examples, you'll need to set up a Postgres database with logical replication enabled.

## BigQuery Setup

To run the BigQuery example, you'll need:

1. A Google Cloud Project with BigQuery API enabled
2. A service account key file with BigQuery permissions
3. A BigQuery dataset created in your project

The example will automatically create tables in the specified dataset based on your Postgres schema.
