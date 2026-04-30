# `etl` Benchmarks

Performance benchmarks for ETL replication pipelines.

## Benchmarks

- `table_copy`: measures initial table copy throughput for selected TPC-C tables.
- `table_streaming`: measures CDC insert streaming throughput against a simple benchmark table.

Both benchmarks print a machine-readable `BENCHMARK_RESULT {...}` JSON line. The preferred entrypoint is the workspace task runner:

```bash
cargo xtask benchmark
```

## Local Smoke Run

Start a local Postgres cluster, prepare a tiny TPC-C dataset, run table copy, then run CDC streaming:

```bash
cargo xtask postgres start --shards 1 --base-port 5430

cargo xtask benchmark \
  --warehouses 1 \
  --streaming-events 1000 \
  --streaming-insert-batch-size 100 \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1
```

The command writes JSON reports to `target/bench-results/`.

Reports include both human-readable summaries and pretty JSON artifacts. The
headline values are:

- rows/events processed and rows/events per second
- estimated payload bytes, MiB, and MiB per second
- copy, producer, catch-up drain, destination-drain, and total timings
- destination batch counts and max event batch size
- CDC event mix: inserts, updates, deletes, relations, and transaction events

Byte values are estimates based on ETL's decoded in-memory row/event size hints.
They are useful for comparing benchmark runs in this repo, but they are not raw
WAL bytes or network bytes.

## Larger Run

```bash
cargo xtask benchmark \
  --warehouses 100 \
  --streaming-events 1000000 \
  --streaming-insert-batch-size 5000 \
  --max-table-sync-workers 8 \
  --max-copy-connections-per-table 4
```

`--tpcc-threads` is optional. When omitted, `xtask` derives an aggressive `go-tpc`
loader concurrency from both warehouse count and available CPU parallelism:
`max(warehouses * 8, available_cpus * 4)`, clamped to `8..128`. Override it
explicitly if a benchmark host can sustain more database write concurrency.

## Duration-Based Streaming

To run the streaming producer for a fixed duration and then wait for ETL to drain all produced events:

```bash
cargo xtask benchmark \
  --warehouses 10 \
  --streaming-duration-seconds 300 \
  --streaming-insert-batch-size 5000
```

## BigQuery Destination

```bash
cargo xtask benchmark \
  --destination big-query \
  --bq-project-id my-gcp-project \
  --bq-dataset-id my_dataset \
  --bq-sa-key-file /path/to/service-account-key.json
```

## Direct Benchmark Binaries

The `xtask` command prepares data and publications for you. Direct invocation is useful while developing benchmark internals:

```bash
cargo bench --bench table_copy -- --log-target terminal run \
  --host localhost --port 5430 --database bench \
  --username postgres --password postgres \
  --publication-name bench_pub \
  --table-ids 1,2,3 \
  --destination null

cargo bench --bench table_streaming -- --log-target terminal run \
  --host localhost --port 5430 --database bench \
  --username postgres --password postgres \
  --publication-name bench_streaming_pub \
  --table-name etl_streaming_benchmark \
  --event-count 1000000 \
  --destination null
```

## GitHub Actions

The `Benchmark` workflow can be launched with `workflow_dispatch`. It defaults
to `blacksmith-16vcpu-ubuntu-2404`, 10 TPC-C warehouses, 1,000,000 streaming
events, 1,000 ms batch fill, 8 table-sync workers, and 4 copy connections per
table. The runner can be changed at dispatch time to the 8, 16, or 32 vCPU
Blacksmith Ubuntu 24.04 runner.

The workflow starts only source Postgres, not the full local development stack,
then uploads `target/bench-results/*.json` as the `benchmark-results` artifact.
It exposes inputs for TPC-C warehouses, optional TPC-C prepare threads, streaming
event count or duration, producer concurrency, batch size, copy parallelism, and
destination.

For BigQuery runs, configure the repository secret `BENCHMARK_BQ_SA_KEY_JSON` and pass the BigQuery project and dataset inputs.
