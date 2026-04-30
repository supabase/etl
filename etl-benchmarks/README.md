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
- `table_streaming`: CDC insert streaming throughput for a single benchmark table.

Both benchmarks support:

- `null`: acknowledges and discards writes. Use this to measure source extraction,
  decoding, batching, and pipeline overhead without destination write cost.
- `bigquery`: writes to BigQuery. Use this for end-to-end destination throughput.

Both benchmarks print a human-readable summary and a machine-readable
`BENCHMARK_RESULT {...}` JSON line. `xtask` also writes pretty JSON files to the
output directory.

## Prerequisites

Local runs need:

- Rust from the repository `rust-toolchain.toml`.
- Docker, for `cargo xtask postgres start`.
- `go-tpc`, for preparing TPC-C data.

Install the pinned `go-tpc` version used by CI:

```bash
go install github.com/pingcap/go-tpc@v1.0.12
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

## Quick Smoke Run

Use this while iterating on benchmark code. It prepares one TPC-C warehouse,
runs table copy, inserts 1,000 CDC rows, waits until ETL observes those rows,
and writes reports under `target/bench-results-smoke/`.

```bash
cargo xtask benchmark \
  --warehouses 1 \
  --streaming-events 1000 \
  --streaming-insert-batch-size 100 \
  --streaming-producer-concurrency 4 \
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
  --streaming-events 1000000 \
  --streaming-insert-batch-size 5000 \
  --batch-max-fill-ms 1000 \
  --max-table-sync-workers 8 \
  --max-copy-connections-per-table 4 \
  --output-dir target/bench-results-local
```

Use `--force-prepare` when changing the warehouse count for an existing local
benchmark database. Without it, `xtask` reuses the existing TPC-C tables.

`--tpcc-threads` is optional. When omitted, `xtask` derives the `go-tpc` loader
concurrency as:

```text
max(warehouses * 8, available_cpus * 4), clamped to 8..128
```

Override it only when the benchmark host and Postgres instance can sustain more
loader concurrency:

```bash
cargo xtask benchmark --warehouses 20 --tpcc-threads 128
```

## Controlling Row Counts

Table-copy row count is controlled by TPC-C warehouse count:

- `--warehouses`: controls the TPC-C dataset size loaded into Postgres.
- `--force-prepare`: drops and regenerates the TPC-C tables. Use this when
  changing `--warehouses` on an existing benchmark database.
- `--tpcc-threads`: controls `go-tpc` load concurrency, not dataset size.
- `--tpcc-tables`: controls which prepared TPC-C tables are copied.

CDC streaming row insertion is controlled separately:

- `--streaming-events`: exact number of source rows to insert in count mode.
- `--streaming-duration-seconds`: insert for a fixed duration instead of a fixed
  row count.
- `--streaming-insert-batch-size`: rows inserted per producer transaction.
- `--streaming-producer-concurrency`: number of concurrent insert producers.

In count mode, the benchmark arms the destination counter for
`--streaming-events`, inserts that many rows into the source table, then waits
until ETL observes the same number of CDC data events. In duration mode, it
inserts until the duration expires, records the produced row count, and then
waits for ETL to drain that produced count.

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
  --streaming-events 1000000 \
  --streaming-insert-batch-size 5000 \
  --streaming-producer-concurrency 16 \
  --output-dir target/bench-results-streaming
```

Use `--streaming-duration-seconds` instead of `--streaming-events` to run the
producer for a fixed duration and then wait for ETL to drain all produced events:

```bash
cargo xtask benchmark \
  --skip-table-copy \
  --skip-prepare \
  --streaming-duration-seconds 300 \
  --streaming-insert-batch-size 5000
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
  --streaming-events 1000000
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
  -f streaming_events=1000000 \
  -f streaming_insert_batch_size=5000 \
  -f max_table_sync_workers=8 \
  -f max_copy_connections_per_table=4 \
  -f batch_max_fill_ms=1000 \
  -f destination=null
```

Important workflow inputs:

- `benchmark_runner`: `blacksmith-8vcpu-ubuntu-2404`,
  `blacksmith-16vcpu-ubuntu-2404`, or `blacksmith-32vcpu-ubuntu-2404`.
- `warehouses`: number of TPC-C warehouses to prepare.
- `tpcc_threads`: optional `go-tpc` prepare threads. Empty means xtask derives it.
- `streaming_events`: fixed CDC insert count.
- `streaming_duration_seconds`: optional duration mode. Empty uses
  `streaming_events`.
- `streaming_insert_batch_size`: rows per producer transaction.
- `streaming_producer_concurrency`: optional producer concurrency. Empty means
  xtask derives it.
- `max_table_sync_workers`: table-copy worker parallelism.
- `max_copy_connections_per_table`: per-table copy connection parallelism.
- `batch_max_fill_ms`: stream batch fill timeout.
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
`target/bench-results/*.json` plus `target/bench-results/*.md`.

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
  destination: null
  copied: 568405 rows across 8 table(s), 305.18 MiB estimated
  throughput: 87738.47 rows/s, 47.11 estimated MiB/s
  timing: start=38ms, copy_wait=6478ms, shutdown=4ms, total=6724ms
  batches: 41 table-row batch(es)
```

Example table-streaming summary:

```text
Table streaming benchmark
  destination: null
  CDC events: produced=1000, observed=1000, estimated payload=0.28 MiB
  event mix: inserts=1000, updates=0, deletes=0, relations=1, tx=20, total=1021
  producer: 25903.41 events/s over 38ms
  dispatch: 8326.44 events/s, 2.35 estimated MiB/s over 120ms
  destination-drained: 8275.61 events/s, 2.34 estimated MiB/s over 120ms
  catch-up drain: 12270.84 events/s, 3.46 estimated MiB/s over 81ms
```

The main fields to compare across runs are:

- rows or CDC events processed
- rows/events per second
- estimated MiB per second
- copy wait, producer, catch-up drain, destination-drain, shutdown, and total
  timings
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
  --table-ids <customer_oid>,<district_oid> \
  --destination null
```

Replace the placeholder OIDs with values returned by the `psql` query.

Direct streaming benchmark:

```bash
cargo run -p etl-benchmarks --release --bin table_streaming -- --log-target terminal run \
  --host localhost \
  --port 5430 \
  --database bench \
  --username postgres \
  --password postgres \
  --publication-name bench_streaming_pub \
  --table-name etl_streaming_benchmark \
  --event-count 1000000 \
  --insert-batch-size 5000 \
  --producer-concurrency 16 \
  --destination null
```
