# Snowflake CDC Benchmark

Streams 1M+ rows from Postgres to Snowflake via Snowpipe Streaming, then measures CDC throughput.

Demonstrates the Snowflake destination's performance characteristics.

## Prerequisites

1. The project's dev stack running (provides source Postgres with logical replication)
2. Snowflake account with:
   - RSA key pair configured for key-pair auth
   - Target database and schema created (`ETL_BENCH.CDC`)
   - A role with appropriate permissions (USAGE on warehouse/DB/schema, CREATE TABLE, etc.)
3. Rust toolchain (cargo)

## Quick Start

### 1. Start the dev stack

From the repo root:

```bash
./scripts/init.sh
```

This starts `source-postgres` on port 5430 with `wal_level=logical` and all replication settings configured. See `scripts/docker-compose.yaml` for details.

### 2. Create a benchmark database

```bash
psql -h localhost -p 5430 -U postgres -c "CREATE DATABASE etl_bench;"
```

### 3. Seed 1M rows

```bash
cargo run -p etl-examples --features snowflake --bin snowflake-loadgen -- seed \
  --db-url postgres://postgres:postgres@localhost:5430/etl_bench \
  --rows 1000000
```

Takes ~2 minutes. Creates 3 tables (users, orders, events) in a dedicated `bench` Postgres schema and scopes the publication to it, so the ETL pipeline's internal state tables (in `public`) are not replicated.

### 4. Configure Snowflake credentials

Fill in the `BENCH_SNOWFLAKE_*` variables in `.env` (see `.env.example`):

```env
BENCH_SNOWFLAKE_ACCOUNT=ORG-ACCOUNT
BENCH_SNOWFLAKE_USER=ETL_USER
BENCH_SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
BENCH_SNOWFLAKE_DATABASE=ETL_BENCH
BENCH_SNOWFLAKE_SCHEMA=CDC
BENCH_SNOWFLAKE_ROLE=ETL_ROLE
```

Then load them: `source .env`

### 5. Start the pipeline

Snowflake args are picked up from `BENCH_SNOWFLAKE_*` env vars automatically:

```bash
cargo run -p etl-examples --features snowflake --bin snowflake -- \
  --db-host localhost \
  --db-port 5430 \
  --db-name etl_bench \
  --db-username postgres \
  --db-password postgres \
  --publication bench_pub
```

The terminal dashboard shows table copy progress and throughput in real-time.

### 6. Start CDC load generator (separate terminal)

```bash
cargo run -p etl-examples --features snowflake --bin snowflake-loadgen -- generate \
  --db-url postgres://postgres:postgres@localhost:5430/etl_bench \
  --rate 5000 \
  --mix 40/40/20 \
  --duration 300s
```

## What to Expect

- **Table copy phase**: Copies all 1M rows to Snowflake. Expect 10,000-20,000 rows/sec depending on network and row size.
- **CDC phase**: After initial copy, streams changes in real-time. Latency is typically 2-10 seconds from Postgres commit to queryable in Snowflake.

## Verifying in Snowflake

Table names in Snowflake follow the pattern `{pg_schema}_{pg_table}` uppercased, so Postgres `bench.events` becomes `ETL_BENCH.CDC.BENCH_EVENTS`. All examples below use fully qualified names (`ETL_BENCH.CDC.*`) matching the default config.

**Important**: Column names are preserved as lowercase (quoted identifiers). You must double-quote them in Snowflake SQL, e.g. `"id"` not `id`.

```sql
-- List all tables
SHOW TABLES IN ETL_BENCH.CDC;

-- Count total rows (includes all CDC versions)
SELECT COUNT(*) FROM ETL_BENCH.CDC.BENCH_EVENTS;

-- Check CDC metadata columns
SELECT "id", "event_type", "_cdc_operation", "_cdc_sequence_number"
FROM ETL_BENCH.CDC.BENCH_EVENTS
ORDER BY "_cdc_sequence_number" DESC
LIMIT 20;

-- Materialize current state (latest version of each row)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY "id"
        ORDER BY "_cdc_sequence_number" DESC
    ) AS rn
    FROM ETL_BENCH.CDC.BENCH_EVENTS
)
WHERE rn = 1 AND "_cdc_operation" != 'delete';

-- Count current (live) rows only
SELECT COUNT(*) FROM (
    SELECT "id", "_cdc_operation", ROW_NUMBER() OVER (
        PARTITION BY "id"
        ORDER BY "_cdc_sequence_number" DESC
    ) AS rn
    FROM ETL_BENCH.CDC.BENCH_EVENTS
)
WHERE rn = 1 AND "_cdc_operation" != 'delete';

-- Create a Dynamic Table for auto-refreshing current state
CREATE DYNAMIC TABLE ETL_BENCH.CDC.BENCH_EVENTS_CURRENT
    TARGET_LAG = '1 minute'
    WAREHOUSE = MY_WH
AS
    SELECT * EXCLUDE ("_cdc_operation", "_cdc_sequence_number", rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY "id"
            ORDER BY "_cdc_sequence_number" DESC
        ) AS rn
        FROM ETL_BENCH.CDC.BENCH_EVENTS
    )
    WHERE rn = 1 AND "_cdc_operation" != 'delete';
```

## Verifying in Postgres (source comparison)

Run these against the source database to compare row counts with Snowflake:

```bash
psql -h localhost -p 5430 -U postgres -d etl_bench
```

```sql
-- Total rows in source
SELECT COUNT(*) FROM bench.events;

-- If running the CDC load generator, compare live row counts:
-- This should match the "current state" count from Snowflake above.
SELECT COUNT(*) FROM bench.events;

-- All tables:
SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS approx_rows
FROM pg_stat_user_tables
WHERE schemaname = 'bench'
ORDER BY relname;
```

## Cleanup

```bash
# Stop the pipeline (Ctrl+C)
# Drop the benchmark database
psql -h localhost -p 5430 -U postgres -c "DROP DATABASE etl_bench;"
```

In Snowflake:

```sql
DROP TABLE IF EXISTS ETL_BENCH.CDC.BENCH_EVENTS;
DROP TABLE IF EXISTS ETL_BENCH.CDC.BENCH_USERS;
DROP TABLE IF EXISTS ETL_BENCH.CDC.BENCH_ORDERS;
DROP DYNAMIC TABLE IF EXISTS ETL_BENCH.CDC.BENCH_EVENTS_CURRENT;
```

## Comparison with Snowflake's CDC Guide

This benchmark is modeled after Snowflake's official CDC guide (https://www.snowflake.com/en/developers/guides/cdc-snowpipestreaming-dynamictables/).

Key differences:

- Pure Rust pipeline (no Java SDK sidecar)
- Uses the Snowpipe Streaming REST API directly
- Supports all Postgres types, schema evolution, and automatic recovery
- Append-only changelog model with Dynamic Tables for materialization
