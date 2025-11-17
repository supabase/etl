# `etl` - Examples

This crate contains practical examples demonstrating how to use the ETL system for data replication from Postgres to various destinations.

## Available Examples

- **BigQuery Integration**: Demonstrates replicating Postgres data to Google BigQuery
- **Redis Integration**: Demonstrates replicating Postgres data to Redis (to benefit from Redis as a cache for auth tokens for example)

## Prerequisites

Before running the examples, you'll need to set up a Postgres database with logical replication enabled.


## BigQuery

### Quick Start

To quickly try out `etl`, you can run the BigQuery example. First, create a publication in Postgres which includes the tables you want to replicate:

```sql
create publication my_publication
for table table1, table2;
```

Then run the BigQuery example:

```bash
cargo run --bin bigquery -p etl-examples -- \
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

### BigQuery Setup

To run the BigQuery example, you'll need:

1. A Google Cloud Project with BigQuery API enabled
2. A service account key file with BigQuery permissions
3. A BigQuery dataset created in your project

The example will automatically create tables in the specified dataset based on your Postgres schema.


## Redis

### Quick Start

To quickly try out `etl`, you can run the Redis example. First, create a publication in Postgres which includes the tables you want to replicate:

```sql
create publication my_publication
for table table1, table2;
```

Then run the Redis example:

```bash
cargo run --bin redis -p etl-examples -- \
        --db-host localhost \
        --db-port 5432 \
        --db-name postgres \
        --db-username postgres \
        --db-password password \
        --publication my_publication
```

Usage of the cli:

```bash
Usage: redis [OPTIONS] --db-name <DB_NAME> --db-username <DB_USERNAME> --publication <PUBLICATION>

Options:
      --db-host <DB_HOST>
          Host on which Postgres is running (e.g., localhost or IP address) (default: 127.0.0.1) [default: 127.0.0.1]
      --db-port <DB_PORT>
          Port on which Postgres is running (default: 5432) [default: 5432]
      --db-name <DB_NAME>
          Postgres database name to connect to
      --db-username <DB_USERNAME>
          Postgres database user name (must have REPLICATION privileges)
      --db-password <DB_PASSWORD>
          Postgres database user password (optional if using trust authentication)
      --redis-host <REDIS_HOST>
          Host on which Redis is running (e.g., localhost or IP address) (default: 127.0.0.1) [default: 127.0.0.1]
      --redis-port <REDIS_PORT>
          Port on which Redis is running (default: 6379) [default: 6379]
      --redis-username <REDIS_USERNAME>
          Redis database user name
      --redis-password <REDIS_PASSWORD>
          Redis database user password (optional if using trust authentication)
      --redis-ttl <REDIS_TTL>
          Set a TTL (in seconds) for data replicated in Redis (optional)
      --publication <PUBLICATION>
          Postgres publication name (must be created beforehand with CREATE PUBLICATION)
  -h, --help
          Print help
  -V, --version
          Print version
```

In the above example, `etl` connects to a Postgres database named `postgres` running on `localhost:5432` with a username `postgres` and password `password` on a local instance of Redis.
