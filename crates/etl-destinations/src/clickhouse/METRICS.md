# ClickHouse Metrics

This directory exposes ClickHouse-specific metrics to answer "is this pipeline
healthy?" without reading logs line-by-line or shelling into ClickHouse.

The metrics fall into three groups:

- write-path metrics: row counts, byte volumes, statement counts, INSERT
  latency, and INSERT failures.
- DDL metrics: latency and failure counts for `CREATE` / `ALTER` /
  `DROP` / `RENAME` / `TRUNCATE` statements.
- probe metrics: latency of cheap server round-trips (`SELECT 1`, database
  existence) and schema lookups against `system.columns`.

All labels are intentionally low-cardinality. `source` is bounded to `copy`
(initial table sync) and `streaming` (CDC events). `kind` is bounded to the
fixed set of DDL operation types. `outcome` is bounded to `timeout` and
`failed`.

## Write path

- `etl_clickhouse_insert_duration_seconds`
- `etl_clickhouse_insert_rows`
- `etl_clickhouse_insert_bytes`
- `etl_clickhouse_insert_encoding_errors_total`
- `etl_clickhouse_insert_errors_total`
- `etl_clickhouse_statements_per_batch`

`etl_clickhouse_insert_duration_seconds` measures one committed RowBinary
INSERT statement, from the first write to the server acknowledgement. It is
recorded only on success.

`etl_clickhouse_insert_rows` and `etl_clickhouse_insert_bytes` are the
denominators for `insert_duration_seconds`. Without them, a slow statement
could be slow because of 10M rows or 10 rows; with them, the dashboard can
plot throughput (rows or bytes per second).

`etl_clickhouse_insert_errors_total` is the on-call signal: "did inserts start
failing in the last N minutes?". The `outcome` label distinguishes
`timeout` (client deadline fired) from `failed` (server returned an error
before the deadline). Specific error codes are in logs. It counts only
network/server failures of the INSERT round-trip.

`etl_clickhouse_insert_encoding_errors_total` is the complementary counter for
the pre-network step: rows rejected by the RowBinary encoder (type mismatch,
null in a non-nullable column, length mismatch). These are upstream data-shape
issues, not destination-side failures, and have a different remediation path,
so they are tracked separately. The specific reason is in logs.

`etl_clickhouse_statements_per_batch` shows how many INSERT statements were
committed for a single logical write batch. It exceeds 1 only when
`max_bytes_per_insert` forces a mid-batch flush. A persistently high value
points to oversized incoming batches that should be split upstream or to a
`max_bytes_per_insert` that is too low.

All write-path metrics carry the `source` label.

How to read them:

- low `insert_duration_seconds` with low `insert_rows` is fine; low duration
  with high rows is great.
- high `insert_duration_seconds` with low `insert_rows` is the worst-of-both
  shape and usually points to small-batch churn or server-side contention.
- a spike in `insert_errors_total{outcome="timeout"}` without a spike in
  `outcome="failed"` usually means the destination is saturated, not broken.
- `statements_per_batch > 1` consistently means batches are being chopped by
  the per-statement byte limit.

## DDL

- `etl_clickhouse_ddl_duration_seconds`
- `etl_clickhouse_ddl_errors_total`

`etl_clickhouse_ddl_duration_seconds` measures one DDL statement (CREATE /
ALTER / DROP / RENAME / TRUNCATE / view DDL). Recorded regardless of outcome
so latency is visible even on failure.

`etl_clickhouse_ddl_errors_total` counts DDL failures, labeled by `kind` and
`outcome`. This catches schema-sync incidents that previously required reading
logs.

Both metrics carry a `kind` label with one of:

- `create_table`, `create_view`
- `drop_table`, `drop_view`, `truncate_table`
- `add_column`, `drop_column`, `rename_column`

## Probes

- `etl_clickhouse_connectivity_check_duration_seconds`
- `etl_clickhouse_schema_query_duration_seconds`

`etl_clickhouse_connectivity_check_duration_seconds` captures `SELECT 1` and
the `system.databases` existence query. Recorded regardless of outcome.
Connectivity slowdowns usually lead INSERT slowdowns, so this is a useful
leading indicator on dashboards.

`etl_clickhouse_schema_query_duration_seconds` captures `system.columns`
lookups during schema sync. Recorded regardless of outcome.

Neither metric carries labels; both are intentionally global.

## Practical advice

- Start by checking `insert_errors_total` for a recent spike. If absent, the
  pipeline is healthy.
- If errors are present, split by `outcome` to decide whether to look at
  timeouts (capacity / network) or other failures (server logs).
- If errors are absent but latency is high, look at `insert_duration_seconds`
  vs. `insert_rows` / `insert_bytes` to see whether the issue is row volume,
  byte volume, or per-statement overhead.
- If DDL is failing repeatedly for one `kind`, the schema-sync path for that
  operation is the right place to investigate.
