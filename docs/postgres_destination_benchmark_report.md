# Postgres destination benchmark report

Generated: 2026-05-20 10:54:32 CEST.

This report summarizes the current Postgres destination benchmark after rerunning with separate source and destination Postgres containers on the same machine. Main numbers are rounded for readability; exact values are in the JSON files listed below.

## Setup

Source Postgres:

| Setting | Value |
| --- | --- |
| Host | `localhost` |
| Port | `5430` |
| Database | `bench` |
| Container | `etl-stack-pg-18-source-postgres-1` |
| Version | PostgreSQL 18.1 |
| `wal_level` | `logical` |

Destination Postgres:

| Setting | Value |
| --- | --- |
| Host | `localhost` |
| Port | `5431` |
| Database | `bench` |
| Container | `etl-benchmark-destination-postgres` |
| Image | `postgres:18` |
| Version | PostgreSQL 18.1 |
| `wal_level` | `replica` |

Machine:

| Setting | Value |
| --- | --- |
| Host | Apple M4 MacBook Pro |
| CPUs | 10 |
| Memory | 32 GiB |
| OS | macOS 26.4.1 |
| Rust | 1.93.1 |

Destination container command:

```bash
docker run -d \
  --name etl-benchmark-destination-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=bench \
  -p 5431:5432 \
  postgres:18
```

## What Was Benchmarked

Dataset:

| Setting | Value |
| --- | --- |
| Source dataset | TPC-C, 1 warehouse |
| Tables copied | 8 |
| Rows copied | 1,387,362 |
| Decoded data estimate | 657.7 MiB |
| Measured samples | 5 |
| Warmup samples | 1 |
| Table streaming | Disabled |

Destination variants:

| Variant | Meaning |
| --- | --- |
| `null` | No-op destination. Useful as a rough source/decode reference. |
| `postgres logged` | Normal WAL-logged destination tables, with destination session `synchronous_commit = off`. |
| `postgres unlogged` | `create unlogged table`, also with destination session `synchronous_commit = off`. |

The Postgres destination is not using `COPY FROM STDIN` yet. It writes real Postgres rows with pooled multi-row parameterized `INSERT` statements, capped at 2,000 rows and 30,000 bind parameters per statement.

## Results

| Variant | Median rows/s | Range | Spread | Median MiB/s | Median total |
| --- | ---: | ---: | ---: | ---: | ---: |
| `null` | 132k | 89k-310k | 248% | 63 MiB/s | 10.5s |
| `postgres logged` | 122k | 108k-127k | 17.6% | 58 MiB/s | 11.6s |
| `postgres unlogged` | 123k | 120k-125k | 4.6% | 58 MiB/s | 11.3s |

Interpretation:

- With source and destination split into two local containers, the current Postgres destination is around **122k-123k rows/s**.
- Unlogged tables were only slightly faster than logged tables, so destination WAL was not the dominant bottleneck in this run.
- The two-instance result is much slower than the previous same-instance local run. That is expected on Docker Desktop/macOS because the destination writes now cross container/host networking instead of writing back into the same Postgres server.
- The `null` baseline is still noisy and should not be treated as a clean destination comparison. It remains useful as a rough source/decode reference.

## Raw Results

Exact aggregate JSON reports:

| Variant | JSON |
| --- | --- |
| `null` | `target/bench-results-two-instance-null-20260520/table_copy.json` |
| `postgres logged` | `target/bench-results-two-instance-postgres-logged-20260520/table_copy.json` |
| `postgres unlogged` | `target/bench-results-two-instance-postgres-unlogged-20260520/table_copy.json` |

Useful verification command:

```bash
for f in \
  target/bench-results-two-instance-null-20260520/table_copy.json \
  target/bench-results-two-instance-postgres-logged-20260520/table_copy.json \
  target/bench-results-two-instance-postgres-unlogged-20260520/table_copy.json
do
  echo "$f"
  jq '{
    destination,
    sample_count,
    warmup_sample_count,
    copied_rows,
    estimated_copied_mib,
    rows_per_second,
    estimated_mib_per_second,
    total_ms,
    rows_summary: .sample_summary.rows_per_second,
    mib_summary: .sample_summary.estimated_mib_per_second,
    total_summary: .sample_summary.total_ms
  }' "$f"
done
```

## Reproduce Locally

These commands assume:

- source Postgres is running on `localhost:5430`;
- destination Postgres is running on `localhost:5431`;
- both use `postgres/postgres`;
- both have a `bench` database;
- source TPC-C tables have already been prepared.

Shared benchmark shape:

```bash
--warehouses 1 \
--skip-prepare \
--samples 5 \
--warmup-samples 1 \
--skip-table-streaming \
--max-table-sync-workers 2 \
--max-copy-connections-per-table 1 \
--batch-max-fill-ms 100
```

Null baseline:

```bash
cargo xtask benchmark \
  --destination null \
  --warehouses 1 \
  --skip-prepare \
  --samples 5 \
  --warmup-samples 1 \
  --skip-table-streaming \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1 \
  --batch-max-fill-ms 100 \
  --output-dir target/bench-results-two-instance-null-20260520
```

Postgres logged, separate destination:

```bash
cargo xtask benchmark \
  --destination postgres \
  --pg-destination-host localhost \
  --pg-destination-port 5431 \
  --pg-destination-database bench \
  --pg-destination-username postgres \
  --pg-destination-password postgres \
  --pg-destination-schema etl_benchmark_destination_two_instance_logged \
  --warehouses 1 \
  --skip-prepare \
  --samples 5 \
  --warmup-samples 1 \
  --skip-table-streaming \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1 \
  --batch-max-fill-ms 100 \
  --output-dir target/bench-results-two-instance-postgres-logged-20260520
```

Postgres unlogged, separate destination:

```bash
cargo xtask benchmark \
  --destination postgres \
  --pg-destination-host localhost \
  --pg-destination-port 5431 \
  --pg-destination-database bench \
  --pg-destination-username postgres \
  --pg-destination-password postgres \
  --pg-destination-schema etl_benchmark_destination_two_instance_unlogged \
  --pg-destination-unlogged \
  --warehouses 1 \
  --skip-prepare \
  --samples 5 \
  --warmup-samples 1 \
  --skip-table-streaming \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1 \
  --batch-max-fill-ms 100 \
  --output-dir target/bench-results-two-instance-postgres-unlogged-20260520
```

Note: `xtask benchmark` now derives a unique Postgres destination schema for each sample. That avoids stale target tables contaminating multi-sample Postgres results.

## External COPY Context

Very short version:

| Source | Useful comparison |
| --- | --- |
| [PostgreSQL populate docs](https://www.postgresql.org/docs/current/populate.html) | Official guidance says `COPY` is almost always faster than `INSERT` for large loads, even with prepared/batched inserts. |
| [PostgreSQL COPY docs](https://www.postgresql.org/docs/current/sql-copy.html) | Binary COPY is described as somewhat faster than text/CSV, but type-specific and less portable. |
| [Cybertec bulk-load benchmark](https://www.cybertec-postgresql.com/en/bulk-load-performance-in-postgresql/) | 10M rows: bulk INSERT 52s, COPY 14s on the author's laptop. |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | 1M rows: COPY binary ~712k rows/s, COPY CSV ~360k rows/s, multi-value INSERT ~253k rows/s. |
| [Gold Lapel pgx benchmark](https://goldlapel.com/grounds/go-postgres/pgx-bulk-insert-benchmarks) | 50k rows: pgx `CopyFrom` ~357k rows/s. |
| [pgcopydb docs](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_copy.html) | Postgres-to-Postgres copy streams source `COPY TO` directly into target `COPY FROM`, avoiding disk. |

The local two-instance destination result, around 122k-123k rows/s, is a multi-row INSERT result over local container networking. It should not be treated as the ceiling for a dedicated Postgres-to-Postgres COPY path.

## How Good Is This Result?

Short answer: it is a useful first baseline, but it is not fast compared with optimized
Postgres bulk-copy paths.

| Benchmark | Method | Result | Comparison |
| --- | --- | ---: | --- |
| This ETL benchmark | Multi-row `INSERT`, separate local source/destination containers | ~122k rows/s | Baseline |
| SQG Java | Multi-value `INSERT`, direct ingest | ~253k rows/s | About 2x faster |
| SQG Java | COPY CSV, direct ingest | ~360k rows/s | About 3x faster |
| SQG Java | COPY binary, direct ingest | ~712k rows/s | About 6x faster |
| Cybertec | Bulk `INSERT`, direct local load | ~192k rows/s | About 1.6x faster |
| Cybertec | COPY, direct local load | ~714k rows/s | About 6x faster |
| Gold Lapel pgx | Binary COPY via `CopyFrom`, direct local ingest | ~357k rows/s | About 3x faster |

This is not an apples-to-apples comparison. The public benchmarks usually measure direct
client-to-Postgres ingestion. This ETL benchmark also includes source table copy, ETL
decoding, type conversion, SQL generation, bind-parameter serialization, and Docker Desktop
networking between two local containers.

The conclusion is:

1. The current destination is respectable as a compatibility baseline.
2. It is probably not bottlenecked mostly on destination WAL, because logged and unlogged
   were almost identical.
3. It is not close to the expected ceiling for Postgres-to-Postgres copies.
4. A real `COPY FROM STDIN` path should plausibly be multiple times faster, especially if
   the implementation can stream source `COPY TO STDOUT` bytes directly into destination
   `COPY FROM STDIN`.

## Next Performance Step

The fastest Postgres-to-Postgres path should bypass decoded row serialization when possible:

1. Create the target table.
2. Open `COPY ... TO STDOUT WITH (FORMAT BINARY)` on the source.
3. Open `COPY ... FROM STDIN WITH (FORMAT BINARY)` on the target.
4. Pipe bytes directly when column order and types match.
5. Use table-level parallelism first, and same-table chunking only for very large tables.

For transformed ETL rows, the next best path is a destination-side `COPY FROM STDIN` encoder, with binary COPY as the high-performance version and text/CSV COPY as the simpler fallback.
