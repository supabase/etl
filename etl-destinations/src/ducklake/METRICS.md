# DuckLake Metrics

This directory exposes DuckLake-specific metrics to help tune write-path
behavior and the maintenance settings described in the DuckLake docs.

The metrics fall into four groups:

- write-path metrics: show how the ETL writer is batching, waiting, retrying,
  and flushing inline data.
- maintenance execution metrics: operation-level duration and skip metrics
  emitted by the background maintenance worker with the primary reason and
  outcome for each maintenance attempt.
- table-health samples: histograms recorded by a background sampler every
  30 seconds on a dedicated DuckDB pool of size 1. They describe the current
  shape of tables known to the current destination instance.
- catalog-backlog gauges: current global snapshot and deletion backlog in the
  attached DuckLake catalog.

These metrics intentionally avoid a `table_name` label. That keeps Prometheus
cardinality low. The tradeoff is that table-health metrics are sampled as
histograms over recently written tables rather than exported as one time series
per table.

## Metric groups

### Connection and blocking metrics

- `etl_ducklake_pool_size`
- `etl_ducklake_blocking_slot_wait_seconds`
- `etl_ducklake_pool_checkout_wait_seconds`
- `etl_ducklake_blocking_operation_duration_seconds`

Use these first to separate storage-shape problems from pure concurrency or pool
pressure. If these are high, maintenance may not be your real bottleneck.

### Batch and retry metrics

- `etl_ducklake_batch_commit_duration_seconds`
- `etl_ducklake_batch_prepared_mutations`
- `etl_ducklake_upsert_rows`
- `etl_ducklake_delete_predicates`
- `etl_ducklake_retries_total`
- `etl_ducklake_failed_batches_total`
- `etl_ducklake_replayed_batches_total`

These explain the pressure your writer is putting on DuckLake:

- larger `upsert_rows` usually means fewer, larger files.
- larger `delete_predicates` usually means more delete pressure and more need
  for `rewrite_data_files`.
- growing retries usually points to transaction conflicts or file-visibility
  issues before it points to bad maintenance thresholds.

### Inline flush metrics

- `etl_ducklake_inline_flush_rows`
- `etl_ducklake_inline_flush_duration_seconds`

The destination attaches DuckLake with `DATA_INLINING_ROW_LIMIT = 10000` and then
lets a background maintenance worker flush and checkpoint inlined data after
writes. Destination shutdown also runs one final best-effort inline flush sweep
for known tables. These metrics tell you whether that strategy is helping.

The `batch_kind` label on these metrics uses:

- `mutation` for background CDC flushes
- `shutdown` for the final best-effort shutdown sweep

How to read them:

- if `inline_flush_rows` is usually `0`, the workload is often bypassing inline
  storage already, so the current inline limit is not doing much.
- if `inline_flush_rows` is usually tiny, you are still materializing small
  files after commit. That points to upstream batch shape or flush cadence, and
  sometimes to an overly large `target_file_size`.
- if `inline_flush_rows` is often meaningfully larger than `upsert_rows`, the
  inlining limit is helping consolidate multiple atomic batches before files are
  materialized.
- if `batch_kind="shutdown"` shows meaningful work, the process is still
  relying on final shutdown cleanup to drain inline backlogs.

### Background maintenance metrics

- `etl_ducklake_maintenance_duration_seconds`
- `etl_ducklake_maintenance_skipped_total`

`etl_ducklake_maintenance_duration_seconds` is emitted once per background
maintenance operation that actually runs. Use the histogram `_count` as the
event count for non-skipped outcomes.

It carries these labels:

- `task`: `flush`, `targeted_maintenance`, or `checkpoint`
- `operation`: `flush_inlined_data`, `rewrite_data_files`,
  `merge_adjacent_files`, or `checkpoint`
- `reason`: the primary cause for the maintenance decision
- `outcome`: `applied`, `noop`, or `failed`

`etl_ducklake_maintenance_skipped_total` counts maintenance operations that
were skipped because the worker could not acquire the table-local write slot.
It is labeled by `task`, `operation`, and `reason`.

How to read it:

- `task="flush"` with `reason="pending_bytes_threshold"` means the flush was
  scheduled because estimated pre-compression inline bytes crossed the
  configured threshold. The current heuristic assumes a 4:1 raw-to-parquet
  compression ratio.
- `task="flush"` with `reason="pending_inserted_rows_threshold"` means the
  optional row-count threshold was enabled in code and fired before shutdown.
- `task="targeted_maintenance"` with
  `reason="idle_rewrite_metrics_threshold"` or
  `reason="emergency_rewrite_metrics_threshold"` means rewrite was selected
  from sampled delete pressure.
- `task="targeted_maintenance"` with
  `reason="idle_merge_metrics_threshold"` or
  `reason="emergency_merge_metrics_threshold"` means merge was selected from
  sampled small-file pressure.
- `task="targeted_maintenance"` may emit one duration series or two for a
  maintenance cycle, depending on which operations crossed their thresholds.
- rising `etl_ducklake_maintenance_skipped_total` means the worker wants to run
  maintenance but the table-local write slot is still busy.
- rising histogram `_count` for `outcome="failed"` points to maintenance
  execution issues, while many `outcome="noop"` samples usually mean
  maintenance is polling more often than work is actually accumulating.

### Table-health sampling metrics

- `etl_ducklake_table_active_data_files`
- `etl_ducklake_table_active_data_bytes`
- `etl_ducklake_table_active_data_file_avg_size_bytes`
- `etl_ducklake_table_small_file_ratio`
- `etl_ducklake_table_active_delete_files`
- `etl_ducklake_table_active_delete_bytes`
- `etl_ducklake_table_deleted_row_ratio`

These are sampled from DuckLake metadata by the periodic background sampler.

How to read them:

- high `active_data_files` with low `active_data_file_avg_size_bytes` means the
  table is fragmented.
- high `small_file_ratio` means many files are below 5 MiB. DuckLake recommends
  files be at least a few MiB, so this is the clearest signal that
  `merge_adjacent_files` or `target_file_size` needs attention.
- high `deleted_row_ratio` means a meaningful portion of active rows are being
  masked by delete files. If this stays elevated, lowering
  `rewrite_delete_threshold` or running `rewrite_data_files` more often is
  usually justified.

### Catalog backlog gauges

- `etl_ducklake_snapshots_total`
- `etl_ducklake_oldest_snapshot_age_seconds`
- `etl_ducklake_active_data_files_total`
- `etl_ducklake_files_scheduled_for_deletion_total`
- `etl_ducklake_files_scheduled_for_deletion_bytes`
- `etl_ducklake_oldest_scheduled_deletion_age_seconds`

These are global to the DuckLake catalog.

How to read them:

- `active_data_files_total` is the exact current total active data-file count
  in the catalog. Use it when you need a true total rather than the sampled
  per-table distribution from `etl_ducklake_table_active_data_files`.
- rising `snapshots_total` and `oldest_snapshot_age_seconds` indicate snapshot
  retention is growing. If this is not intentional for time travel or recovery,
  tighten `expire_snapshots` or run it more frequently.
- rising `files_scheduled_for_deletion_*` metrics mean cleanup is falling
  behind. If the oldest scheduled deletion age keeps growing, run cleanup more
  frequently or reduce `delete_older_than` when it is safe.

## Maintenance tuning playbook

### `merge_adjacent_files`

Watch:

- `etl_ducklake_table_active_data_files`
- `etl_ducklake_table_active_data_file_avg_size_bytes`
- `etl_ducklake_table_small_file_ratio`

Interpretation:

- if `small_file_ratio` stays high and average file size stays low, merge more
  aggressively or lower `target_file_size`.
- if average file size is already healthy and `small_file_ratio` is low, a more
  aggressive merge policy will likely buy little.

### `rewrite_data_files`

Watch:

- `etl_ducklake_table_active_delete_files`
- `etl_ducklake_table_active_delete_bytes`
- `etl_ducklake_table_deleted_row_ratio`

Interpretation:

- if `deleted_row_ratio` stays close to zero, the default threshold can stay
  conservative.
- if `deleted_row_ratio` frequently gets large before rewrite happens, lower
  `rewrite_delete_threshold` or run rewrite more frequently.

### `expire_snapshots`

Watch:

- `etl_ducklake_snapshots_total`
- `etl_ducklake_oldest_snapshot_age_seconds`

Interpretation:

- if both metrics grow without a matching retention need, snapshots are
  lingering too long.
- do not lower expiration purely from these metrics. The safe lower bound comes
  from your rollback, recovery, and time-travel requirements.

### `cleanup_of_files`

Watch:

- `etl_ducklake_files_scheduled_for_deletion_total`
- `etl_ducklake_files_scheduled_for_deletion_bytes`
- `etl_ducklake_oldest_scheduled_deletion_age_seconds`

Interpretation:

- if the backlog grows but `expire_snapshots` is healthy, cleanup cadence is too
  low.
- do not lower `delete_older_than` below the maximum expected reader or
  maintenance overlap window.

### `checkpoint`

`CHECKPOINT` bundles multiple maintenance steps. Use the table-health metrics
plus the catalog-backlog gauges to decide whether checkpoint cadence is too low:

- fragmented table metrics suggest merge and rewrite work is pending.
- growing snapshot and deletion backlogs suggest expire and cleanup work is
  pending.

## Practical advice

- Start by fixing pool or retry pressure before tuning maintenance knobs.
- Tune one maintenance setting at a time and watch the corresponding metrics for
  at least one full workload cycle.
- Use these metrics with table-specific logs when you need to identify the worst
  offenders. The metrics are intentionally low-cardinality.
