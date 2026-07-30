# ClickHouse Destination

> **Status: Closed beta.** ClickHouse is a closed beta destination.
> Availability is limited while the integration stabilizes.

## Requirements

- ClickHouse **23.5 or newer** is required for the default
  `ReplacingMergeTree` engine.
- The source table replica identity must be primary key or full.
- The default `ReplacingMergeTree` engine requires a source primary key. The
  `MergeTree` engine works for source tables without a primary key.

## Running the example

For the repository's local services, copy `.env.example` to `.env` and load
it. Then initialize and seed the services:

```bash
source .env
cargo x init
cargo x seed
```

Run the ClickHouse example directly with Cargo:

```bash
cargo run -p etl-examples --bin clickhouse --features clickhouse -- \
    --db-host "$TESTS_DATABASE_HOST" \
    --db-port "$TESTS_DATABASE_PORT" \
    --db-name etl_testdata \
    --db-username "$TESTS_DATABASE_USERNAME" \
    --publication seed_pub
```

Both passwords come from the variables loaded from `.env`.

Alternatively, use the xtask wrapper. It reads the `TESTS_DATABASE_*` and
`TESTS_CLICKHOUSE_*` variables and supplies the local database and publication
defaults:

```bash
cargo x example clickhouse
```

## Table engines

The destination supports two layouts. Select one per pipeline with
`--clickhouse-engine`:

| Flag value                       | Engine               | Use it for                                              |
| -------------------------------- | -------------------- | ------------------------------------------------------- |
| `replacing_merge_tree` (default) | `ReplacingMergeTree` | Current-state replicas. Source must have a primary key. |
| `merge_tree`                     | `MergeTree`          | Append-only event log. Works for PK-less source tables. |

Table names derive from the Postgres schema and table name. They use
double-underscore escaping. For example, `public.orders` becomes
`public_orders`, and `my_schema.t` becomes `my__schema_t`.

### ReplacingMergeTree (default)

Each replicated table uses
`ReplacingMergeTree(_etl_version, _etl_deleted)`, keyed on the source primary
key. Two trailing columns control deduplication and tombstone handling:

- `_etl_version UInt128` -- the packed Postgres event sequence key:
  `(commit_lsn << 64) | tx_ordinal`. Higher values win during a `FINAL` merge.
  Thus, the latest event for each primary key wins. The commit LSN and the
  in-transaction ordinal give a total order for all events. This includes
  multiple row events that share a WAL record.
- `_etl_deleted UInt8` -- tombstone flag. `1` for DELETE events and `0` for
  other events.

The destination also creates a `<table>__current` view for each table. This
view hides the `ReplacingMergeTree` internals:

```sql
CREATE VIEW IF NOT EXISTS "public_orders__current" AS
SELECT <user columns>
FROM "public_orders" FINAL
WHERE _etl_deleted = 0
```

Read patterns:

- Use the `__current` view for current-state queries.
- Or query the base table directly:

  ```sql
  SELECT <user columns>
  FROM "public_orders" FINAL
  WHERE _etl_deleted = 0
  ```

`OPTIMIZE` guidance:

- The replicator never runs `OPTIMIZE ... FINAL CLEANUP`. Background merges
  collapse duplicates over time. Operators control physical tombstone removal.
- To reclaim deleted rows on disk, run
  `OPTIMIZE TABLE "<table>" FINAL CLEANUP` on a schedule that matches your
  retention requirements.

### MergeTree

Each replicated table uses `MergeTree() ORDER BY tuple()`. Two CDC metadata
columns follow each row:

- `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`.
- `cdc_lsn`: the Postgres commit LSN at the time of the change.

Read patterns:

- For current state by primary key, take the latest event by `cdc_lsn` with
  `LIMIT 1 BY`. Then filter out tombstones:

  ```sql
  SELECT <user columns> FROM (
      SELECT * FROM "public_orders"
      ORDER BY cdc_lsn DESC LIMIT 1 BY (id)
  )
  WHERE cdc_operation != 'DELETE'
  ```

- For event log queries, read the table directly. The table keeps every CDC
  event.

## Connection notes

For HTTPS connections, provide an `https://` URL. TLS uses webpki root
certificates automatically.

Set `TESTS_CLICKHOUSE_PASSWORD` when ClickHouse requires authentication. The
example reads this variable directly, so the secret does not appear in process
arguments. The `--clickhouse-password` flag remains available for one-off local
runs.

## CLI flags

| Flag                           | Default                | Description                                                   |
| ------------------------------ | ---------------------- | ------------------------------------------------------------- |
| `--db-host`                    | _(required)_           | Postgres host                                                 |
| `--db-port`                    | _(required)_           | Postgres port (`u16`)                                         |
| `--db-name`                    | _(required)_           | Postgres database name                                        |
| `--db-username`                | _(required)_           | Postgres user (must have REPLICATION)                         |
| `--db-password`                | _(optional)_           | Password; env: `TESTS_DATABASE_PASSWORD`                      |
| `--clickhouse-url`             | _(required)_           | HTTP(S) endpoint; env: `TESTS_CLICKHOUSE_URL`                 |
| `--clickhouse-user`            | _(required)_           | User name; env: `TESTS_CLICKHOUSE_USER`                       |
| `--clickhouse-password`        | _(optional)_           | Password; env: `TESTS_CLICKHOUSE_PASSWORD`                    |
| `--clickhouse-database`        | `default`              | Target database; env: `TESTS_CLICKHOUSE_DATABASE`             |
| `--clickhouse-engine`          | `replacing_merge_tree` | Table engine: `replacing_merge_tree` or `merge_tree`          |
| `--max-batch-fill-duration-ms` | `5000`                 | Max time to wait before flushing a batch                      |
| `--max-table-sync-workers`     | `4`                    | Concurrent workers during initial copy                        |
| `--publication`                | _(required)_           | Postgres publication name                                     |

## Metrics

See [`./METRICS.md`](./METRICS.md) for the metrics that the ClickHouse
destination emits.
