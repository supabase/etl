# Postgres Single-Table Copy Benchmark

Generated: 2026-05-20 11:31:26 CEST.

This report measures the current ETL Postgres destination while copying one large TPC-C
table, `order_line`, from a source Postgres container to a separate destination Postgres
container on the same local machine.

The headline result is encouraging for the current multi-row `INSERT` implementation,
but this is a **local Docker Desktop benchmark**. Production Postgres-to-Postgres copies
over a real network can behave differently depending on latency, bandwidth, TLS, instance
CPU, storage, WAL settings, and source/destination contention.

## Executive Summary

| Question | Answer |
| --- | --- |
| What was copied? | One table: `order_line` |
| How much data? | 1,037,461 rows, estimated 457.5 MiB decoded ETL payload |
| Best stable local setting | 4 copy connections per table |
| Best stable result | ~488k rows/s, ~215 MiB/s, 2.2s median total |
| Peak median result | ~513k rows/s at 8 copy connections, but with higher variance |
| Current destination write path | Multi-row parameterized `INSERT`, not `COPY FROM STDIN` |
| Main takeaway | Current implementation is a strong baseline, but COPY should still be the next performance target |

Leadership read: the current implementation can move this one large table very quickly
on a local setup, especially when same-table copy parallelism is enabled.

Engineering read: 4 copy connections looks like the best stable local setting; 8 can win by median
but is noisier; 16 over-drives the local setup.

## Results And Comparison

Benchmark shape:

| Setting | Value |
| --- | --- |
| Source table | `order_line` |
| Source Postgres | `localhost:5430`, PostgreSQL 18.1, `wal_level=logical` |
| Destination Postgres | `localhost:5431`, PostgreSQL 18.1, `wal_level=replica` |
| Destination table mode | Normal logged table |
| Destination write path | Multi-row parameterized `INSERT` |
| Samples | 5 measured, 1 warmup |
| Table streaming | Disabled |
| Table sync workers | 1 |
| Copy connections per table | Swept across 1, 2, 4, 8, 16 |
| Machine | Apple M4 MacBook Pro, 10 CPUs, 32 GiB memory, macOS 26.4.1 |

Aggregate results:

| Copy connections/table | Median rows/s | Range | Spread | Median MiB/s | Median total |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 209k | 209k-214k | 2.1% | 92 MiB/s | 5.0s |
| 2 | 342k | 331k-366k | 10.6% | 151 MiB/s | 3.1s |
| 4 | 488k | 466k-540k | 15.9% | 215 MiB/s | 2.2s |
| 8 | 513k | 426k-569k | 33.5% | 226 MiB/s | 2.1s |
| 16 | 392k | 309k-638k | 106.6% | 173 MiB/s | 2.7s |

Interpretation:

- Same-table parallel copy matters a lot for `order_line`.
- 4 copy connections were about 2.3x faster than 1 connection and stayed reasonably stable.
- 8 copy connections had the best median, but variance increased materially.
- 16 copy connections was too much for this local setup: some samples were very fast, but
  the median regressed and spread was high.
- For this table on this machine, use `--max-copy-connections-per-table 4` as the stable
  local benchmark setting. Use `8` only as a peak-throughput experiment.

Comparison with public benchmarks:

| Benchmark | Method | Result | Comparison |
| --- | --- | ---: | --- |
| This ETL benchmark, 4 copy connections | Multi-row `INSERT`, local source/destination containers | ~488k rows/s | Stable local result |
| This ETL benchmark, 8 copy connections | Multi-row `INSERT`, local source/destination containers | ~513k rows/s | Best median, more variable |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | Multi-value `INSERT`, direct ingest | ~253k rows/s | Slower than this run |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | COPY CSV, direct ingest | ~360k rows/s | Slower than this run |
| [SQG Java benchmark](https://sqg.dev/blog/java-postgres-insert-benchmark/) | COPY binary, direct ingest | ~712k rows/s | About 1.4x faster than 8 connections |
| [Cybertec bulk-load benchmark](https://www.cybertec-postgresql.com/en/bulk-load-performance-in-postgresql/) | COPY, direct local load | ~714k rows/s | About 1.4x faster than 8 connections |
| [Gold Lapel pgx benchmark](https://goldlapel.com/grounds/go-postgres/pgx-bulk-insert-benchmarks) | Binary COPY via `CopyFrom`, direct ingest | ~357k rows/s | Slower than this run |

This comparison is directional, not apples-to-apples. Public benchmarks usually measure
direct client-to-Postgres ingestion. This benchmark includes source table copy, ETL
decoding, type conversion, SQL generation, bind-parameter serialization, and local Docker
Desktop networking between two containers. It also uses same-table source parallelism,
which can make aggregate throughput higher than a single direct-ingest stream.

## What The Numbers Mean

The aggregate JSON files are median reports produced from 5 measured samples. The 1 warmup
sample is discarded.

Important fields:

| Field | Meaning |
| --- | --- |
| `copied_rows` | Median copied row count across measured samples. Every sample in this sweep copied 1,037,461 rows. |
| `estimated_copied_bytes` | Sum of ETL `SizeHint::size_hint` for decoded `TableRow`s observed by the benchmark destination wrapper. This is an in-memory decoded payload estimate, not physical Postgres table size, WAL size, network bytes, or disk bytes. |
| `estimated_copied_mib` | `estimated_copied_bytes / 1024 / 1024`. For this table: `479,697,880 / 1024 / 1024 = 457.4755 MiB`. |
| `rows_per_second` | `copied_rows / copy_wait_duration_seconds` for each sample, then median across measured samples. |
| `estimated_mib_per_second` | `estimated_copied_mib / copy_wait_duration_seconds` for each sample, then median across measured samples. |
| `copy_wait_ms` | Time spent waiting for table copy to finish after pipeline start. This is the denominator for throughput. |
| `total_ms` | End-to-end benchmark sample time, including startup and shutdown overhead. |
| `sample_summary` | Min, median, max, and spread across measured samples for each numeric field. |

Because top-level aggregate fields are medians of per-sample values,
`estimated_mib_per_second` is not recomputed from the rounded top-level
`estimated_copied_mib` and `copy_wait_ms`. Small differences are expected if manually
recomputed from rounded millisecond values.

Correctness check:

| Checked target tables | Min rows | Max rows | Mismatches vs 1,037,461 |
| ---: | ---: | ---: | ---: |
| 30 | 1,037,461 | 1,037,461 | 0 |

All warmup and measured destination tables had the expected row count.

Raw result files:

| Copy connections/table | JSON |
| ---: | --- |
| 1 | `target/bench-results-single-order-line-c1-20260520/table_copy.json` |
| 2 | `target/bench-results-single-order-line-c2-20260520/table_copy.json` |
| 4 | `target/bench-results-single-order-line-c4-20260520/table_copy.json` |
| 8 | `target/bench-results-single-order-line-c8-20260520/table_copy.json` |
| 16 | `target/bench-results-single-order-line-c16-20260520/table_copy.json` |

## Production Caveat

This benchmark was intentionally local:

- source Postgres and destination Postgres were separate containers on the same Mac;
- destination traffic went through local Docker Desktop networking;
- no cross-region or cloud network path was involved;
- the destination was a fresh local `postgres:18` container;
- the measured write path was multi-row `INSERT`, not `COPY FROM STDIN`.

Production results may be lower or higher. A networked destination can be slower because
of round trips, TLS, bandwidth limits, noisy neighbors, and storage/WAL pressure. It can
also be faster if the production destination has more CPU, better storage, native Linux
networking, and tuned Postgres settings. Treat this benchmark as a local implementation
signal and concurrency-shape test, not a production capacity guarantee.

## Reproduce The Benchmark

These commands assume:

- source Postgres is running on `localhost:5430`;
- destination Postgres is running on `localhost:5431`;
- both use `postgres/postgres`;
- both have a `bench` database;
- source TPC-C tables have already been prepared.

Start the destination container:

```bash
docker run -d \
  --name etl-benchmark-destination-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=bench \
  -p 5431:5432 \
  postgres:18
```

Run the single-table copy-connection sweep:

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

Verify the aggregate JSON:

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

Table selection behavior:

- `--tpcc-tables order_line` runs this single-table benchmark.
- A comma-separated list selects multiple tables, for example
  `--tpcc-tables order_line,orders,stock`.
- Omitting `--tpcc-tables` uses the default full TPC-C table set, but that is not the
  benchmark covered by this report.

Note: `xtask benchmark` derives a unique Postgres destination schema for each sample.
That avoids stale target tables contaminating multi-sample Postgres results.

## Next Performance Step

The current result is good for a multi-row `INSERT` destination, but it should not be the
end state. The fastest Postgres-to-Postgres path should bypass decoded row serialization
when possible:

1. Create the target table.
2. Open `COPY ... TO STDOUT WITH (FORMAT BINARY)` on the source.
3. Open `COPY ... FROM STDIN WITH (FORMAT BINARY)` on the target.
4. Pipe bytes directly when column order and types match.
5. Use table-level parallelism first, and same-table chunking only for very large tables.

For transformed ETL rows, the next best path is a destination-side `COPY FROM STDIN`
encoder, with binary COPY as the high-performance version and text/CSV COPY as the simpler
fallback.
