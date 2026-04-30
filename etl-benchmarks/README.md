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

## Larger Local Run

This is closer to the default CI-sized run, but still practical on a strong
developer machine:

```bash
cargo xtask benchmark \
  --force-prepare \
  --warehouses 10 \
  --streaming-duration-seconds 300 \
  --batch-max-fill-ms 1000 \
  --max-table-sync-workers 8 \
  --max-copy-connections-per-table 4 \
  --output-dir target/bench-results-local
```

Use `--force-prepare` when changing the warehouse count for an existing local
benchmark database. Without it, `xtask` reuses the existing TPC-C tables.

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

## GitHub Actions

The `Benchmark` workflow is manual and runs through `workflow_dispatch`.

From the GitHub UI, open **Actions**, select **Benchmark**, choose **Run
workflow**, then set the inputs.

With the GitHub CLI:

```bash
gh workflow run benchmark.yml \
  -f benchmark_runner=blacksmith-16vcpu-ubuntu-2404 \
  -f warehouses=10 \
  -f streaming_duration_seconds=300 \
  -f max_table_sync_workers=8 \
  -f max_copy_connections_per_table=4 \
  -f batch_max_fill_ms=1000 \
  -f memory_budget_ratio=0.2 \
  -f destination=null
```

Important workflow inputs:

- `benchmark_runner`: `blacksmith-8vcpu-ubuntu-2404`,
  `blacksmith-16vcpu-ubuntu-2404`, or `blacksmith-32vcpu-ubuntu-2404`.
- `warehouses`: number of TPC-C warehouses to prepare.
- `tpcc_threads`: optional `go-tpc` prepare and streaming threads. Empty means
  xtask derives it.
- `streaming_duration_seconds`: TPC-C workload duration. Defaults to 60.
- `streaming_drain_quiet_ms`: CDC quiet period before TPC-C drain completes.
- `max_table_sync_workers`: table-copy worker parallelism.
- `max_copy_connections_per_table`: per-table copy connection parallelism.
- `batch_max_fill_ms`: stream batch fill timeout.
- `memory_budget_ratio`: ratio of detected memory reserved for stream batch
  bytes. Defaults to `0.2`.
- `enable_memory_backpressure`: opt into ETL memory backpressure. Defaults to
  `false` for benchmark runs.
- `destination`: `null` or `bigquery`.
- `force_prepare`: drop and regenerate TPC-C tables before running.
- `skip_table_copy`: skip table copy.
- `skip_table_streaming`: skip table streaming.

For BigQuery workflow runs, set the repository secret
`BENCHMARK_BQ_SA_KEY_JSON`, then pass `destination=bigquery`,
`bq_project_id`, and an existing `bq_dataset_id`.

The workflow starts only source Postgres, installs pinned `go-tpc`, runs
`cargo xtask benchmark`, compares the new reports against the most recent
successful `benchmark-results` artifact on the same ref, and uploads
`target/bench-results/*.json` plus `target/bench-results/*.md`. It also writes a
benchmark environment note with the selected runner, CPU count, host memory,
cgroup memory limit, memory budget ratio, and memory backpressure setting.

If no previous successful run exists, the comparison writes a "no previous
benchmark artifact" summary and passes. If a comparable previous run exists, the
comparison fails the workflow when exact copy count metrics change or when
throughput and timing metrics regress beyond their per-metric thresholds.
TPC-C streaming event counts are informational because the transaction workload
is duration-based and naturally varies between runs. If benchmark configuration
differs, the comparison still prints the diff table but skips the regression
gate for that benchmark.

The comparison is also available locally when you have two result directories:

```bash
cargo xtask benchmark-compare \
  --previous-dir target/bench-results-old \
  --current-dir target/bench-results \
  --output target/bench-results/benchmark-comparison.md
```

In GitHub Actions mode, `benchmark-compare` uses `GITHUB_TOKEN`,
`GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, and `GITHUB_REF_NAME` to find the previous
successful `workflow_dispatch` run for `benchmark.yml`.

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
