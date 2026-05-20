# Postgres single-table copy benchmark

Generated: 2026-05-20 11:17:29 CEST.

This report focuses only on copying one TPC-C table, `order_line`, from a source
Postgres container to a separate destination Postgres container on the same machine.
Main numbers are rounded for readability; exact values are in the JSON files listed
below.

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

## Benchmark Shape

| Setting | Value |
| --- | --- |
| Source table | `order_line` |
| Table selection flag | `--tpcc-tables order_line` |
| Rows copied | 1,037,461 |
| Decoded data estimate | 457.5 MiB |
| Destination mode | Normal logged table |
| Destination write path | Multi-row parameterized `INSERT` |
| Samples | 5 measured, 1 warmup |
| Table streaming | Disabled |
| Table sync workers | 1 |
| Copy connections per table | Swept across 1, 2, 4, 8, 16 |

The Postgres destination is not using `COPY FROM STDIN` yet. It writes real Postgres
rows with pooled multi-row parameterized `INSERT` statements, capped at 2,000 rows
and 30,000 bind parameters per statement.

## Results

| Copy connections/table | Median rows/s | Range | Spread | Median MiB/s | Median total |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 209k | 209k-214k | 2.1% | 92 MiB/s | 5.0s |
| 2 | 342k | 331k-366k | 10.6% | 151 MiB/s | 3.1s |
| 4 | 488k | 466k-540k | 15.9% | 215 MiB/s | 2.2s |
| 8 | 513k | 426k-569k | 33.5% | 226 MiB/s | 2.1s |
| 16 | 392k | 309k-638k | 106.6% | 173 MiB/s | 2.7s |

## Metric Definitions

The aggregate JSON files are median reports produced from 5 measured samples. The 1 warmup
sample is discarded.

Important fields:

| Field | Meaning |
| --- | --- |
| `copied_rows` | Median copied row count across measured samples. Every sample in this sweep copied 1,037,461 rows. |
| `estimated_copied_bytes` | Sum of ETL `SizeHint::size_hint` for decoded `TableRow`s observed by the benchmark destination wrapper. This is an in-memory decoded payload estimate, not physical Postgres table size, WAL size, wire bytes, or disk bytes. |
| `estimated_copied_mib` | `estimated_copied_bytes / 1024 / 1024`. For this table: `479,697,880 / 1024 / 1024 = 457.4755 MiB`. |
| `rows_per_second` | `copied_rows / copy_wait_duration_seconds` for each sample, then median across measured samples. |
| `estimated_mib_per_second` | `estimated_copied_mib / copy_wait_duration_seconds` for each sample, then median across measured samples. |
| `copy_wait_ms` | Time spent waiting for the table copy to finish after pipeline start. This is the denominator for throughput. |
| `total_ms` | End-to-end benchmark sample time, including startup/shutdown overhead. |
| `sample_summary` | Min, median, max, and spread across the measured samples for each numeric field. |

Because top-level aggregate fields are medians of per-sample values, `estimated_mib_per_second`
is not recomputed from the rounded top-level `estimated_copied_mib` and `copy_wait_ms`.
Small differences are expected if you recompute it manually from rounded millisecond values.

Destination row counts were checked directly in the destination Postgres after the sweep:

| Checked target tables | Min rows | Max rows | Mismatches vs 1,037,461 |
| ---: | ---: | ---: | ---: |
| 30 | 1,037,461 | 1,037,461 | 0 |

Interpretation:

- Same-table parallel copy matters a lot for `order_line`.
- 4 copy connections were about 2.3x faster than 1 connection.
- 8 copy connections had the best median, but it was noticeably more variable than 4.
- 16 copy connections over-drove this local setup: it produced some very fast samples,
  but the median regressed and spread was high.
- For this machine and dataset, `--max-copy-connections-per-table 4` looks like the best
  stable setting; `8` is the peak-throughput experiment.

## Raw Results

| Copy connections/table | JSON |
| ---: | --- |
| 1 | `target/bench-results-single-order-line-c1-20260520/table_copy.json` |
| 2 | `target/bench-results-single-order-line-c2-20260520/table_copy.json` |
| 4 | `target/bench-results-single-order-line-c4-20260520/table_copy.json` |
| 8 | `target/bench-results-single-order-line-c8-20260520/table_copy.json` |
| 16 | `target/bench-results-single-order-line-c16-20260520/table_copy.json` |

Verification command:

```bash
for c in 1 2 4 8 16; do
  f="target/bench-results-single-order-line-c${c}-20260520/table_copy.json"
  echo "$f"
  jq --arg c "$c" '{
    copy_connections: ($c | tonumber),
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

Run the full copy-connection sweep:

```bash
for connections in 1 2 4 8 16; do
  cargo xtask benchmark \
    --destination postgres \
    --pg-destination-host localhost \
    --pg-destination-port 5431 \
    --pg-destination-database bench \
    --pg-destination-username postgres \
    --pg-destination-password postgres \
    --pg-destination-schema "etl_benchmark_single_order_line_c${connections}" \
    --tpcc-tables order_line \
    --warehouses 1 \
    --skip-prepare \
    --samples 5 \
    --warmup-samples 1 \
    --skip-table-streaming \
    --max-table-sync-workers 1 \
    --max-copy-connections-per-table "${connections}" \
    --batch-max-fill-ms 100 \
    --output-dir "target/bench-results-single-order-line-c${connections}-20260520"
done
```

Table selection behavior:

- `--tpcc-tables order_line` runs this single-table benchmark.
- A comma-separated list selects multiple tables, for example
  `--tpcc-tables order_line,orders,stock`.
- Omitting `--tpcc-tables` uses the default full TPC-C table set, but that is not the
  benchmark covered by this report.

Note: `xtask benchmark` derives a unique Postgres destination schema for each sample.
That avoids stale target tables contaminating multi-sample Postgres results.

## External Context

This single-table result is much stronger than the earlier full-table default-set run
because all copy parallelism is concentrated on the large `order_line` table.

| Benchmark | Method | Result | Comparison |
| --- | --- | ---: | --- |
| This ETL benchmark, 4 copy connections | Multi-row `INSERT`, separate local source/destination containers | ~488k rows/s | Stable local result |
| This ETL benchmark, 8 copy connections | Multi-row `INSERT`, separate local source/destination containers | ~513k rows/s | Best median, more variable |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | Multi-value `INSERT`, direct ingest | ~253k rows/s | Slower than this single-table run |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | COPY CSV, direct ingest | ~360k rows/s | Slower than this single-table run |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | COPY binary, direct ingest | ~712k rows/s | About 1.4x faster than 8 connections |
| [Cybertec bulk-load benchmark](https://www.cybertec-postgresql.com/en/bulk-load-performance-in-postgresql/) | COPY, direct local load | ~714k rows/s | About 1.4x faster than 8 connections |
| [Gold Lapel pgx benchmark](https://goldlapel.com/grounds/go-postgres/pgx-bulk-insert-benchmarks) | Binary COPY via `CopyFrom`, direct local ingest | ~357k rows/s | Slower than this single-table run |

This is not an apples-to-apples comparison. The public benchmarks usually measure direct
client-to-Postgres ingestion. This benchmark includes source table copy, ETL decoding,
type conversion, SQL generation, bind-parameter serialization, and Docker Desktop
networking between two local containers. It also uses same-table source parallelism,
which can make the aggregate table throughput higher than a single direct-ingest stream.

The result is encouraging for the current multi-row `INSERT` destination, but it still
does not replace a real Postgres bulk-copy path. A dedicated `COPY FROM STDIN` path should
remain the next performance target.

## Next Performance Step

The fastest Postgres-to-Postgres path should bypass decoded row serialization when possible:

1. Create the target table.
2. Open `COPY ... TO STDOUT WITH (FORMAT BINARY)` on the source.
3. Open `COPY ... FROM STDIN WITH (FORMAT BINARY)` on the target.
4. Pipe bytes directly when column order and types match.
5. Use table-level parallelism first, and same-table chunking only for very large tables.

For transformed ETL rows, the next best path is a destination-side `COPY FROM STDIN`
encoder, with binary COPY as the high-performance version and text/CSV COPY as the simpler
fallback.
