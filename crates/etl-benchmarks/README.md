# `etl` Benchmarks

Performance benchmarks for ETL replication pipelines.

The preferred entrypoint is:

```bash
cargo xtask benchmark
```

`xtask` prepares the source Postgres database, loads TPC-C data when needed,
creates the publications, runs the benchmark binaries, and writes JSON reports.
Use the direct benchmark binaries only when developing the benchmark code itself.

## What Is Measured

- `table_copy`: initial table-copy throughput for selected TPC-C tables.
- `table_streaming`: CDC throughput for a TPC-C transaction workload.

Both benchmarks support:

- `null`: acknowledges and discards writes. Use this to measure source extraction,
  decoding, batching, and pipeline overhead without destination write cost.
- `bigquery`: writes to BigQuery. Use this for end-to-end destination throughput.
- `snowflake`: writes to Snowflake. Use this for end-to-end destination throughput.
- `clickhouse`: writes to ClickHouse. Use this for end-to-end destination throughput.

Both benchmarks print a human-readable summary. When `--report-path` is set, they
also persist a pretty JSON report for automation. `xtask` always sets
`--report-path` so GitHub CI can compare stable files instead of scraping
terminal output.

## Prerequisites

Local runs need:

- Rust from the repository `rust-toolchain.toml`.
- Docker, for `cargo xtask postgres start`.
- `go-tpc`, for preparing TPC-C data.

Install the pinned `go-tpc` version used by CI:

```bash
go install github.com/pingcap/go-tpc/cmd/go-tpc@v1.0.12
```

Make sure `$(go env GOPATH)/bin` is on `PATH`.

## Local Source Database

The local benchmark source database defaults to:

- host: `localhost`
- port: `5430`
- database: `bench`
- user: `postgres`
- password: `postgres`

Start the local source Postgres instance:

```bash
cargo xtask postgres start --shards 1 --base-port 5430 --source-only
```

`xtask benchmark` creates the `bench` database if it does not exist. TPC-C data
is generated into that database by `go-tpc`.

`xtask benchmark` disables ETL memory backpressure by default so throughput
smoke runs do not park behind local memory heuristics. Pass
`--enable-memory-backpressure` when you explicitly want to include that behavior
in a benchmark run.

The stream batch memory budget is separate from backpressure and remains active
by default. `--memory-budget-ratio` defaults to `0.2`, which means the ideal
stream batch byte budget is computed as:

```text
detected_memory_limit * 0.2 / active_streams
```

The detector uses the cgroup memory limit when the benchmark is running inside a
limited container, and host memory otherwise. If memory backpressure is enabled,
it activates at 85% used memory and resumes at 75%, with memory refreshed every
100ms.

## Quick Smoke Run

Use this while iterating on benchmark code. It prepares one TPC-C warehouse,
runs table copy, runs a 10-second TPC-C streaming workload with inserts,
updates, and deletes, drains CDC, and writes reports under
`target/bench-results-smoke/`.

```bash
cargo xtask benchmark \
  --warehouses 1 \
  --streaming-duration-seconds 10 \
  --batch-max-fill-ms 100 \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1 \
  --output-dir target/bench-results-smoke
```

If the TPC-C tables already exist, preparation is skipped. Add
`--force-prepare` to drop and regenerate them.

## HotPath Profiling

Use HotPath when you want timing, allocation, Tokio runtime, thread, and
destination-flush visibility from the benchmark binaries. The null destination
simulates destination batch flushing by waiting a random 10-100ms for both
table-copy row batches and streaming event batches before dropping the batch.
Streaming event batches are flushed from a spawned task so the apply loop's
pending destination write path is exercised. Benchmark JSON reports also include
a final Tokio runtime metrics snapshot with worker counts, live tasks, global
queue depth, per-worker park counts, and per-worker busy time.

Enable HotPath through explicit benchmark flags only:

- `--hotpath`: timing, Tokio, thread, future, lock, and SQL profiling support.
- `--hotpath-alloc`: allocation profiling.
- `--hotpath-cpu`: CPU sampling and Samply flamegraph output.
- `--hotpath-mcp-benchmarks`: live unauthenticated MCP access for selected
  benchmark binaries.

For commands, MCP probes, artifact analysis, CI behavior, CPU flamegraphs, and
regression triage, see [`HOTPATH_RUNBOOK.md`](HOTPATH_RUNBOOK.md).

## Larger Local Run

This is closer to the default CI-sized run, but still practical on a strong
developer machine:

```bash
cargo xtask benchmark \
  --force-prepare \
  --warehouses 10 \
  --streaming-duration-seconds 300 \
  --samples 3 \
  --warmup-samples 1 \
  --batch-max-fill-ms 1000 \
  --max-table-sync-workers 8 \
  --max-copy-connections-per-table 4 \
  --output-dir target/bench-results-local
```

Use `--force-prepare` when changing the warehouse count for an existing local
benchmark database. Without it, `xtask` reuses the existing TPC-C tables.

`--samples` repeats each selected benchmark and writes the median result to
`table_copy.json` and `table_streaming.json`. `--warmup-samples` runs extra
samples before the measured samples and discards them. Use at least three
samples for CI or other noisy hosts; the JSON reports include
`sample_summary` with min, median, max, and spread for each numeric top-level
metric.

`--tpcc-threads` is optional and applies to both `go-tpc tpcc prepare` and
`go-tpc tpcc run`. When omitted, `xtask` derives it as:

```text
max(warehouses * 8, available_cpus * 2), clamped to 8..64
```

Override it only when the benchmark host and Postgres instance can sustain more
workload concurrency:

```bash
cargo xtask benchmark --warehouses 20 --tpcc-threads 64
```

## Controlling Row Counts

Table-copy row count is controlled by TPC-C warehouse count:

- `--warehouses`: controls the TPC-C dataset size loaded into Postgres.
- `--force-prepare`: drops and regenerates the TPC-C tables. Use this when
  changing `--warehouses` on an existing benchmark database.
- `--tpcc-threads`: controls `go-tpc` load and streaming concurrency, not dataset
  size.
- `--tpcc-tables`: controls which prepared TPC-C tables are copied.

The default TPC-C table set intentionally excludes `history` because `go-tpc`
creates it without a primary key in Postgres, while ETL requires primary keys
for replicated tables.

TPC-C streaming is controlled by:

- `go-tpc tpcc run` runs against the prepared TPC-C tables after the ETL
  pipeline is ready.
- `--streaming-duration-seconds`: duration for the TPC-C workload. The default
  is 60 seconds.
- `--streaming-drain-quiet-ms`: quiet period with no new CDC events before the
  stream is considered drained. The default is 2 seconds.

The workload itself determines the event mix via `go-tpc`'s NewOrder, Payment,
OrderStatus, Delivery, and StockLevel transactions. The report infers the
processed event count from destination observations after the stream drains.

## Filling A Table To A Target Size

Use `pg-fill-table` when you need raw bulk-load throughput rather than TPC-C
semantics. It creates one logged table with an identity primary key and runs
parallel `copy from stdin` workers until the committed generated payload reaches
the requested target. The command also prints `pg_total_relation_size` so
relation bloat and physical growth are visible during the run:

```bash
cargo xtask pg-fill-table \
  --host my-db.example.com \
  --port 5432 \
  --database bench \
  --username postgres \
  --password "$POSTGRES_PASSWORD" \
  --table bulk_fill \
  --target-size 450gb \
  --parallelism 64 \
  --row-bytes 65536 \
  --rows-per-copy 64 \
  --force
```

The command defaults to a logged table with an identity primary key so it can be
used with logical replication. It still uses `synchronous_commit=off`, TLS
connection parameters, and `storage external` for the payload column. Pass
`--unlogged` only for non-replication experiments where crash safety and logical
replication do not matter.

## Copy-Only And Streaming-Only

Run only the table-copy benchmark:

```bash
cargo xtask benchmark \
  --skip-table-streaming \
  --force-prepare \
  --warehouses 10 \
  --output-dir target/bench-results-copy
```

Run only the table-streaming benchmark:

```bash
cargo xtask benchmark \
  --skip-table-copy \
  --skip-prepare \
  --streaming-duration-seconds 300 \
  --output-dir target/bench-results-streaming
```

## BigQuery Runs

For BigQuery, provide destination configuration and service account credentials:

```bash
cargo xtask benchmark \
  --destination bigquery \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json \
  --warehouses 10 \
  --streaming-duration-seconds 300
```

The `null` destination is useful for measuring how fast ETL can produce data.
BigQuery runs are the comparable end-to-end destination benchmark.
The BigQuery dataset must already exist; the benchmark creates destination
tables and views inside it, but it does not create the dataset.

## Snowflake Runs

Snowflake credentials are read from environment variables.

Set `BENCH_SNOWFLAKE_CONNECTION` in `.env` (see `crates/etl-destinations/src/snowflake/README.md`):

```bash
cargo xtask benchmark \
  --destination snowflake \
  --warehouses 1 \
  --streaming-duration-seconds 10
```

`BENCH_SNOWFLAKE_CONNECTION` is the only Snowflake benchmark credential input. It is a JSON object with `account`, `user`, `database`, `schema`, optional `role`, optional `private_key_passphrase`, and `private_key`. Snowflake credentials are read from the environment and are not accepted as benchmark CLI arguments.

## ClickHouse Runs

Start the local ClickHouse server from `scripts/docker/docker-compose.yaml`:

```bash
docker compose -f scripts/docker/docker-compose.yaml up -d clickhouse
```

The defaults in `.env.example` (`BENCH_CLICKHOUSE_URL=http://localhost:8123`,
`BENCH_CLICKHOUSE_USER=etl`, `BENCH_CLICKHOUSE_PASSWORD=etl`,
`BENCH_CLICKHOUSE_DATABASE=default`) match the compose service. Override them
to point at any other ClickHouse 23.5 or newer (required by the default
`ReplacingMergeTree` engine).

Run the benchmark:

```bash
cargo xtask benchmark \
  --destination clickhouse \
  --warehouses 1 \
  --streaming-duration-seconds 10
```

Required environment variables (or their `--clickhouse-*` CLI equivalents):

- `BENCH_CLICKHOUSE_URL`: ClickHouse HTTP URL.
- `BENCH_CLICKHOUSE_USER`: ClickHouse user.
- `BENCH_CLICKHOUSE_DATABASE`: target database. Must already exist; the
  destination creates tables but not databases.

Optional:

- `BENCH_CLICKHOUSE_PASSWORD`: ClickHouse password. Omit if the user has none.

## GitHub Actions

The HotPath benchmark workflow runs for pull requests labeled `benchmark` and
can also be started manually through `workflow_dispatch`. This keeps the default
PR loop light while making performance profiling self-serve when a PR needs it.

From the GitHub UI, open **Actions**, select **hotpath-profile**, then choose
**Run workflow** for a manual null-destination profile run.

With the GitHub CLI:

```bash
gh workflow run hotpath-profile.yml
```

For a PR run, add the `benchmark` label. The workflow also listens for new
commits on PRs that already have that label, so benchmark comments stay tied to
the latest pushed revision.

The workflow starts source Postgres, installs pinned `go-tpc`, and runs both
`table_copy` and `table_streaming` through `cargo xtask benchmark` with
HotPath timing and allocation profiling enabled. Labeled pull-request runs
profile the head commit and the base commit, upload all benchmark and HotPath
artifacts, and then the `hotpath-comment` workflow posts HotPath comparison
comments for:

- `table_copy_null`
- `table_streaming_null`

The CI profile uses the null destination, all default replicated TPC-C tables,
four warehouses, 20 seconds of streaming, one measured sample, no warmup
samples, four table-sync workers, and two copy connections per table. The
default replicated TPC-C table set has eight tables, so the profile exercises
table-sync worker scheduling instead of starting every table at once.

The GitHub step summary includes the benchmark environment plus the generated
`benchmark_artifacts.md` for the head run and, on pull requests, the base run.
The uploaded `hotpath-profile-metrics` artifact contains:

- `head/table_copy.json` and `head/table_streaming.json`: HotPath reports used
  by `hotpath-utils profile-pr`.
- `head-benchmark/*.json` and `head-benchmark/benchmark_artifacts.*`: benchmark
  reports and human/agent summaries.
- `base/...` and `base-benchmark/...` equivalents on pull requests.

Destination-specific manual benchmarking for BigQuery, ClickHouse, and
Snowflake is now run locally with `cargo xtask benchmark --destination ...`
rather than through the CI benchmark workflow.

The comparison is also available locally when you have two result directories:

```bash
cargo xtask benchmark-compare \
  --previous-dir target/bench-results-old \
  --current-dir target/bench-results \
  --output target/bench-results/benchmark-comparison.md
```

In GitHub Actions mode, `benchmark-compare` uses `GITHUB_TOKEN`,
`GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, and `GITHUB_REF_NAME` to find the previous
successful workflow run configured by its `--workflow` and `--artifact-name`
arguments.

## Reading The Output

Example table-copy summary:

```text
Table copy benchmark
  Destination   null
  Publication   bench_pub
  Tables        8

  Data
    Rows copied       568,405
    Rows expected     568,405
    Decoded estimate  305.18 MiB

  Throughput
    Rows/s              87,738.47
    Est. decoded MiB/s  47.11
```

Example table-streaming summary:

```text
Table streaming benchmark
  Destination   null
  Workload      tpcc
  Publication   bench_streaming_pub
  Source tables  8 TPC-C tables

  CDC
    Produced       inferred from observed CDC
    Observed       42,318 events
    Decoded estimate  11.94 MiB

  Throughput
    Events/s             6,842.31
    Est. decoded MiB/s   1.93
    Elapsed              6.18 s
```

The main fields to compare across runs are:

- rows or CDC events processed
- rows/events per second
- estimated decoded MiB per second
- copy wait, streaming elapsed, shutdown, and total timings
- destination batch counts and max event batch size
- CDC event mix: inserts, updates, deletes, relations, and transaction events

Byte values are estimates based on ETL's decoded in-memory row/event size hints.
They are useful for comparing benchmark runs in this repository, but they are
not raw WAL bytes, network bytes, or destination billing bytes.

## Direct Benchmark Binaries

Prefer `cargo xtask benchmark`. Direct invocation expects that the database,
tables, publications, and table IDs already exist.

To find TPC-C table IDs for a direct `table_copy` run:

```bash
psql "postgres://postgres:postgres@localhost:5430/bench" \
  -c "select oid, relname from pg_class where relname in ('customer','district','item','new_order','order_line','orders','stock','warehouse') order by relname;"
```

Then run:

```bash
cargo run -p etl-benchmarks --release --bin table_copy -- --log-target terminal run \
  --host localhost \
  --port 5430 \
  --database bench \
  --username postgres \
  --password postgres \
  --publication-name bench_pub \
  --table-ids <customer_oid>,<district_oid>,<item_oid>,<new_order_oid>,<order_line_oid>,<orders_oid>,<stock_oid>,<warehouse_oid> \
  --destination null
```

Replace the placeholder OIDs with values returned by the `psql` query.
Add `--report-path target/bench-results/table_copy.json` when you want a
machine-readable report from a direct run.

Direct TPC-C streaming benchmark:

```bash
cargo run -p etl-benchmarks --release --bin table_streaming -- --log-target terminal run \
  --host localhost \
  --port 5430 \
  --database bench \
  --username postgres \
  --password postgres \
  --publication-name bench_streaming_pub \
  --table-ids <customer_oid>,<district_oid>,<item_oid>,<new_order_oid>,<order_line_oid>,<orders_oid>,<stock_oid>,<warehouse_oid> \
  --duration-seconds 60 \
  --tpcc-warehouses 1 \
  --tpcc-threads 8 \
  --destination null
```

Add `--report-path target/bench-results/table_streaming.json` when you want a
machine-readable report from a direct run.
