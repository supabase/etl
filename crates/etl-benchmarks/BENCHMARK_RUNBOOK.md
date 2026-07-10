# Benchmark Profiling Runbook

This runbook explains how to run ETL benchmarks with profiling, collect
end-to-end profiling data, query the live MCP server, and turn the outputs into
debugging evidence. It is written for both human operators and LLM agents.

## Goals

Use this workflow when you need to answer one or more of these questions:

- Is table copy or table streaming slower than before?
- Which ETL functions spend the most wall-clock time?
- Which functions allocate the most memory?
- Are Tokio tasks, futures, worker threads, locks, or SQL calls contributing to
  the slowdown?
- Does the null destination behave enough like a real destination to exercise
  backpressure and destination flush delay?
- Can an agent inspect live profiler data through MCP while the benchmark is
  still running?

Profiling is optional. Normal production builds do not include it unless the
benchmark features and flags are explicitly enabled.

## What Gets Profiled

The profiling benchmark build enables these data sources:

- Function timing for benchmark orchestration, pipeline startup and shutdown,
  table sync workers, apply workers, batch flushing, destination writes, status
  updates, and setup helpers.
- Allocation profiling when `--hotpath-alloc` is enabled.
- CPU profiling and Samply flamegraphs when `--hotpath-cpu` is enabled and the
  host allows profiler attachment.
- Tokio runtime snapshots in benchmark JSON reports.
- Tokio runtime live data through profiling MCP.
- Future, thread, mutex, RwLock, and SQL sections from profile reports where
  the relevant profiling feature is active and the profiled code emits data.
- Null destination write delay. Both `write_table_rows` and `write_events`
  wait before dropping a full destination batch. The default range is 10-100ms,
  `0..0` disables the artificial delay, and equal min/max values create a fixed
  delay. The streaming event path flushes from a spawned task so pending
  destination writes and apply-loop scheduling are visible.

## Prerequisites

Start source Postgres and install `go-tpc` if the machine does not already have
them:

```bash
POSTGRES_IMAGE=postgres:18 \
POSTGRES_VERSION=18 \
  cargo xtask postgres start --shards 1 --base-port 5430 --source-only

go install github.com/pingcap/go-tpc/cmd/go-tpc@v1.0.12
```

For CPU profiling:

```bash
cargo install samply --locked
cargo install hotpath --version 0.21.1 --bin hotpath-samply
```

On macOS, also run:

```bash
samply setup
```

Then verify the shell can attach to a local process before debugging ETL:

```bash
sleep 60 &
samply record --pid "$!"
```

If that fails with `task_for_pid` or an attach error, fix the local terminal or
developer-tools permission first. CPU profiler output will not produce a useful
`hp.json.gz` file until Samply can attach.

## Fast Local Timing And Allocation Run

Use this first when iterating on instrumentation or checking whether reports are
healthy:

```bash
cargo xtask benchmark \
  --force-prepare \
  --warehouses 1 \
  --streaming-duration-seconds 10 \
  --streaming-drain-quiet-ms 1000 \
  --batch-max-fill-ms 1000 \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1 \
  --destination null \
  --samples 1 \
  --warmup-samples 0 \
  --output-dir target/bench-results-profile-smoke \
  --hotpath \
  --hotpath-alloc \
  --hotpath-output-dir target/bench-results-profile-smoke/profile
```

Expected files:

- `target/bench-results-profile-smoke/table_copy.json`
- `target/bench-results-profile-smoke/table_streaming.json`
- `target/bench-results-profile-smoke/profile/table_copy.json`
- `target/bench-results-profile-smoke/profile/table_streaming.json`
- `target/bench-results-profile-smoke/profile/.table_copy_sample_*.json`
- `target/bench-results-profile-smoke/profile/.table_streaming_sample_*.json`
- `target/bench-results-profile-smoke/benchmark_artifacts.md`
- `target/bench-results-profile-smoke/benchmark_artifacts.json`
- `target/bench-results-profile-smoke/cpu-profiles/*.json.gz` when
  `--hotpath-cpu` is enabled and Samply succeeds.
- `target/bench-results-profile-smoke/cpu-profiles/*.sample-summary.md` and
  `*.sample-summary.json` when the copied Samply profile can be summarized.

Start with `benchmark_artifacts.md`. It is intentionally optimized for humans
and agents: it lists throughput, destination counters, Tokio runtime snapshots,
stage timings, destination batch/write distributions, profile section counts,
and top rows from timing, allocation, futures, mutexes, RwLocks, SQL, threads,
and CPU sections when available.

Use the stage breakdown and destination distributions before drilling into
profiles. They answer the basic shape questions quickly: whether time went into
table sync, producer runtime, drain, shutdown, row/event batch size, or
destination write duration.

The canonical `profile/table_copy.json` and `profile/table_streaming.json` files
are copied from the measured sample selected as the representative aggregate
report. For an odd number of samples this is the median sample by primary
throughput; for an even number, numeric aggregate fields may be averaged while
the canonical profile file still comes from one representative sample. The
hidden `.table_*_sample_*.json` files preserve every per-sample profile report
for spread and outlier analysis.

## CI-Sized Local Run

Use this before asking CI for a benchmark or when reproducing CI locally:

```bash
cargo xtask benchmark \
  --force-prepare \
  --warehouses 4 \
  --tpcc-threads 32 \
  --streaming-duration-seconds 60 \
  --streaming-drain-quiet-ms 1000 \
  --batch-max-fill-ms 1000 \
  --max-table-sync-workers 4 \
  --max-copy-connections-per-table 4 \
  --destination null \
  --samples 1 \
  --warmup-samples 0 \
  --output-dir target/bench-results-profile-ci-local \
  --hotpath \
  --hotpath-alloc \
  --hotpath-output-dir target/bench-results-profile-ci-local/profile
```

This uses all default replicated TPC-C tables. The default table set has more
tables than `--max-table-sync-workers 4`, so table-sync worker scheduling and
copy concurrency control are exercised. The pinned `--tpcc-threads 32` avoids
runner CPU count changing the generated workload shape. The 60-second streaming
window gives the apply loop, batching, Tokio scheduling, destination flush
tasks, SQL, and lock paths enough time to show stable hotspots. The 1s batch
fill gives batches room to grow, making memory growth and larger destination
flushes easier to inspect.

## Null Delay Sweep

Use fixed delay sweeps to separate core pipeline cost from destination flush
pressure:

```bash
for spec in no_delay:0:0 fixed_10ms:10:10 fixed_100ms:100:100 range_10_100ms:10:100; do
  IFS=: read -r name min_ms max_ms <<< "$spec"
  cargo xtask benchmark \
    --force-prepare \
    --warehouses 1 \
    --tpcc-threads 8 \
    --streaming-duration-seconds 10 \
    --streaming-drain-quiet-ms 1000 \
    --batch-max-fill-ms 1000 \
    --null-flush-delay-min-ms "$min_ms" \
    --null-flush-delay-max-ms "$max_ms" \
    --max-table-sync-workers 2 \
    --max-copy-connections-per-table 1 \
    --destination null \
    --samples 1 \
    --warmup-samples 0 \
    --output-dir "target/bench-results-null-delay/${name}"
done
```

Compare `table_streaming.json` fields `total_ms`, `ready_wait_ms`,
`end_to_end_with_shutdown_ms`, `destination_stats.event_batches`, and
`destination_stats.max_event_batch_size`. The no-delay run shows raw pipeline
cost; fixed 10ms is a light pressure profile; fixed 100ms and the 10-100ms
range are stress profiles for pending destination writes and scheduling.

## CPU And Flamegraph Run

Use CPU profiling after timing and allocation reports identify a suspicious
path:

```bash
cargo xtask benchmark \
  --force-prepare \
  --warehouses 1 \
  --streaming-duration-seconds 60 \
  --destination null \
  --samples 1 \
  --warmup-samples 0 \
  --benchmark-profile profiling \
  --output-dir target/bench-results-profile-cpu \
  --hotpath \
  --hotpath-alloc \
  --hotpath-cpu \
  --hotpath-output-dir target/bench-results-profile-cpu/profile
```

If CPU profiling succeeds, `benchmark_artifacts.md` lists copied CPU profiles
under `<output-dir>/cpu-profiles`, plus a text summary for agent review. Use
the summary first when you need a quick symbol-level view:

```bash
less target/bench-results-profile-cpu/cpu-profiles/<benchmark>-<run>-hp.json.sample-summary.md
```

Read the CPU sample summary from top to bottom. The ETL-focused tables only
show project symbols such as `etl::`, `etl_benchmarks::`, and benchmark binary
entrypoints. The filtered application tables hide common profiler,
symbolication, CLI, and sampling transport frames. The unfiltered application
and all-thread tables are kept below them for auditability.

Use the copied Samply profile for the full interactive UI:

```bash
samply load target/bench-results-profile-cpu/cpu-profiles/<benchmark>-<run>-hp.json.gz
```

Run that command to open the interactive call tree and flamegraph. The original
temporary Samply path is still preserved in JSON for debugging. The raw
Samply JSON is a Firefox Profiler profile and may contain addresses before UI
symbolication; the `*.sample-summary.*` files are the machine-readable
symbol-level summaries. If the `functions_cpu` section contains an error message
instead of a profile path, keep the timing and allocation data, then fix Samply
permissions before relying on CPU data.

## Long MCP Debug Run

MCP is most useful while the benchmark is still running. Use a longer streaming
duration so a human or agent has time to connect:

```bash
cargo xtask benchmark \
  --skip-prepare \
  --skip-table-copy \
  --tpcc-tables warehouse,district \
  --streaming-duration-seconds 60 \
  --streaming-drain-quiet-ms 1000 \
  --batch-max-fill-ms 1000 \
  --max-table-sync-workers 2 \
  --max-copy-connections-per-table 1 \
  --destination null \
  --samples 1 \
  --warmup-samples 0 \
  --output-dir target/bench-results-profile-mcp \
  --hotpath \
  --hotpath-alloc \
  --hotpath-mcp-benchmarks table_streaming \
  --hotpath-mcp-port 6771 \
  --hotpath-output-dir target/bench-results-profile-mcp/profile
```

Connect an MCP client to:

```text
http://localhost:6771/mcp
```

Example MCP client config:

```json
{
  "mcpServers": {
    "profile": {
      "type": "http",
      "url": "http://localhost:6771/mcp"
    }
  }
}
```

The server only exists while the selected benchmark binary is running. If the
connection is refused, wait until the `table_streaming` binary starts, or use a
longer streaming duration.

## MCP Lifetime Model

Profiling MCP is an in-process profiler endpoint. It does not keep the ETL
benchmark open until a human manually closes it, and it does not preserve a
queryable server after the benchmark exits.

The lifecycle is:

1. `cargo xtask benchmark` starts a benchmark binary such as `table_streaming`.
2. If that binary is selected by `--hotpath-mcp-benchmarks`, the benchmark
   starts an HTTP MCP server inside the benchmark process.
3. An LLM or human MCP client connects to `http://localhost:6771/mcp` while the
   benchmark is still running.
4. The client queries live profiler state such as timing, allocations, futures,
   Tokio runtime data, threads, mutexes, RwLocks, SQL, and profiler status.
5. When the benchmark binary finishes, the process exits and the MCP endpoint
   closes.
6. After exit, use the persisted artifacts instead: benchmark JSON, profile
   JSON, `benchmark_artifacts.md`, `benchmark_artifacts.json`, and copied CPU
   profiles under `cpu-profiles/` when available.

For interactive LLM debugging, make the benchmark long enough to inspect. The
recommended pattern is to enable MCP only for `table_streaming`, skip table copy
when appropriate, and set `--streaming-duration-seconds` to 60 seconds or more.
For very deep manual inspection, increase the duration further rather than
expecting MCP to pause the process.

## MCP Curl Probe

Use this when an agent needs to prove MCP is available without relying on a
desktop client. First initialize the session:

```bash
curl -i -sS http://localhost:6771/mcp \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  --data '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-06-18",
      "capabilities": {},
      "clientInfo": { "name": "etl-hotpath-debug", "version": "0.1.0" }
    }
  }'
```

Copy the `mcp-session-id` response header into `MCP_SESSION_ID`, then send the
initialized notification:

```bash
curl -sS http://localhost:6771/mcp \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $MCP_SESSION_ID" \
  --data '{
    "jsonrpc": "2.0",
    "method": "notifications/initialized"
  }'
```

List tools:

```bash
curl -sS http://localhost:6771/mcp \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $MCP_SESSION_ID" \
  --data '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }'
```

Call the most useful tools:

```bash
curl -sS http://localhost:6771/mcp \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $MCP_SESSION_ID" \
  --data '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "functions_timing",
      "arguments": {}
    }
  }'
```

Repeat the same request with these tool names:

- `profiler_status`
- `functions_timing`
- `functions_alloc`
- `futures`
- `tokio_runtime`
- `threads`
- `mutexes`
- `rw_locks`
- `sql`
- `function_timing_logs`
- `function_alloc_logs`
- `future_logs`

Not every tool will have rows in every run. Empty sections are useful signal:
they usually mean that code path did not execute, the feature was not enabled,
or the benchmark ended before enough data accumulated.
If `rw_locks` is not listed by the MCP server, use the persisted profile JSON
or profile HTTP JSON endpoint for that section instead.

## Reading Artifact JSON With jq

Use the Markdown report first, then use JSON for exact values:

```bash
jq '.benchmarks[] | {
  benchmark,
  benchmark_report_path,
  profile_report_path: .hotpath_report_path
}' \
  target/bench-results-profile-smoke/benchmark_artifacts.json
```

List throughput and destination counters:

```bash
jq '.benchmarks[] | {
  benchmark,
  throughput: .benchmark_report,
  destination_stats: .benchmark_report.destination_stats,
  tokio_runtime: .benchmark_report.tokio_runtime_stats
}' target/bench-results-profile-smoke/benchmark_artifacts.json
```

List available profile sections:

```bash
jq '.benchmarks[] | {
  benchmark,
  sections: (.hotpath_report.sections | keys)
}' target/bench-results-profile-smoke/benchmark_artifacts.json
```

Inspect top timing and allocation rows:

```bash
jq '.benchmarks[] | {
  benchmark,
  timing: .hotpath_report.sections.functions_timing.top_rows,
  alloc: .hotpath_report.sections.functions_alloc.top_rows
}' target/bench-results-profile-smoke/benchmark_artifacts.json
```

## Debugging Rubric

Use the same order for human review and LLM analysis:

1. Confirm the run shape. Check destination, warehouse count, table list,
   sample count, streaming duration, table-sync workers, copy connections, and
   enabled profiling modes.
2. Confirm row and event volume. A benchmark with too little data can miss
   regressions even if it completes successfully.
3. Confirm destination behavior. For the null destination, verify row and event
   batches were written and dropped after the configured simulated flush delay.
4. Check throughput and latency first. Compare table copy rows/sec and
   streaming events/sec against the base run or previous local run.
5. Check function timing. The top wall-clock rows identify where the system is
   waiting or working.
6. Check allocation rows. High allocation bytes or counts around encoding,
   event conversion, batch building, or destination writes usually point to
   memory churn.
7. Check Tokio runtime metrics. Look for live task count, global queue depth,
   worker busy time, and worker park counts.
8. Check futures. High poll counts or long busy times can indicate wake churn or
   a future doing too much per poll.
9. Check mutexes and RwLocks. High wait time or acquire count suggests
   contention, especially in table-sync state, table-sync pool, copy work
   queue, shared table cache, or task-set paths.
10. Check SQL. High SQL timing often points to metadata, slot, publication, or
    status-update bottlenecks rather than destination throughput.
11. Check threads and CPU. High wall-clock time with low CPU usually means
    waiting, backpressure, sleeps, IO, or scheduling. High CPU with matching
    hot functions means a compute-bound path.
12. Cross-check conclusions. Do not blame a function only because it is first in
    one table. Confirm with at least one adjacent signal such as allocation,
    CPU, futures, Tokio runtime, destination counters, or SQL timing.

## Regression Report Template

Use this structure when reporting a benchmark regression:

```text
Benchmark: table_copy_null or table_streaming_null
Run shape: warehouses, tables, samples, streaming duration, destination
Result: previous throughput -> current throughput, percent change
Primary signal: timing/allocation/CPU/futures/mutex/rw_lock/sql/tokio evidence
Secondary signal: corroborating artifact or MCP tool output
Likely bottleneck: short hypothesis with the responsible subsystem
Confidence: high/medium/low and why
Next action: one concrete code or measurement step
Artifacts: benchmark_artifacts.md/json, profile JSON, flamegraph path if any
```

## GitHub Actions

Benchmarks are intentionally label-gated. A pull request must have the
`benchmark` label before the benchmark workflow runs. The workflow also runs on
new commits to a labeled PR and through manual `workflow_dispatch`.

The split workflow design is intentional:

- `benchmark-ci.yml` runs the benchmark on a Blacksmith 16 vCPU Ubuntu 24.04
  ARM runner with read-only permissions and uploads
  `benchmark-profile-artifacts`.
- `benchmark-comment.yml` runs after the profiling workflow and posts comparison
  comments with pull-request write permission.

This keeps the default CI loop light and avoids giving benchmark execution the
same permissions as PR commenting.

To run from the GitHub UI:

1. Add the `benchmark` label to the PR, or open Actions and manually run
   **Benchmark CI**.
2. Wait for the `Benchmarks` job.
3. Open the step summary and inspect the concise head and base summaries.
4. Download `benchmark-profile-artifacts` if deeper JSON inspection is needed.
5. Read the PR comments posted by **Benchmark Comment** for profile comparisons.

To run manually with the GitHub CLI:

```bash
gh workflow run benchmark-ci.yml
```

## Agent Checklist

When an LLM agent runs or reviews a benchmark, follow this checklist exactly:

1. Run the smallest relevant local profiling command first.
2. Open `benchmark_artifacts.md`.
3. Parse `benchmark_artifacts.json` for exact values.
4. If MCP was requested, prove `tools/list` works and call at least
   `profiler_status`, `functions_timing`, `functions_alloc`, and
   `tokio_runtime`.
5. If CPU was requested, check whether `functions_cpu` has a usable profile
   path or only an error message.
6. Explain missing sections as data, not as a silent failure.
7. Compare against base or previous results before calling something a
   regression.
8. Include the run command, artifact paths, and profiler modes in the final
   report.

## Common Failure Modes

`connection refused` when connecting to MCP:

The selected benchmark binary has not started, has already exited, or MCP was
not enabled for that benchmark. Use a longer streaming duration and
`--hotpath-mcp-benchmarks table_streaming`.

No CPU flamegraph:

Samply could not attach or CPU profiling was not enabled. Verify
`--hotpath-cpu`, `--benchmark-profile profiling`, `hotpath-samply`, and local
profiler permissions.

Profile report exists but a section is empty:

The code path might not have run, the relevant profiling feature might not be
enabled, or the run was too short. Increase data volume or duration before
concluding the subsystem has no cost.

CI benchmark did not run for a PR:

Add the `benchmark` label, push a new commit, reopen the PR, or run
**Benchmark CI** manually from Actions.

PR comment did not appear:

Check whether **Benchmark CI** uploaded `benchmark-profile-artifacts` and
whether **Benchmark Comment** found `pr_number.txt`. Manual workflow runs are
useful for artifacts but do not always have PR metadata for comments.
