# ClickHouse Destination

> **Status: Private alpha.** ClickHouse is a private alpha destination.
> Access is limited while the integration stabilizes.

## Requirements

- ClickHouse **23.5 or newer** is required for the default
  `ReplacingMergeTree` engine.
- The source table replica identity must be `DEFAULT` with a primary key, or
  `FULL`. See [Update requirements](#update-requirements).
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
  select <user columns>
  from "public_orders" final
  where _etl_deleted = 0
  ```

`OPTIMIZE` guidance:

- The replicator never runs `OPTIMIZE ... FINAL CLEANUP`. Background merges
  collapse duplicates over time. Operators control physical tombstone removal.
- To reclaim deleted rows on disk, run
  `optimize table "<table>" final cleanup` on a schedule that matches your
  retention requirements.

### MergeTree

Each replicated table uses `MergeTree() ORDER BY tuple()`. Three CDC metadata
columns follow each row:

- `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`.
- `cdc_lsn`: the Postgres commit LSN at the time of the change.
- `cdc_tx_ordinal`: the zero-based event position within the Postgres
  transaction. Together with `cdc_lsn` it gives a total order over events,
  including multiple row events that share a WAL record.

Read patterns:

- For current state by primary key, take the latest event by `cdc_lsn` and
  `cdc_tx_ordinal` with `limit 1 by`. Then filter out tombstones:

  ```sql
  select <user columns> from (
      select * from "public_orders"
      order by cdc_lsn desc, cdc_tx_ordinal desc
      limit 1 by (id)
  )
  where cdc_operation != 'DELETE'
  ```

- For event log queries, read the table directly. The table keeps every CDC
  event.

## Update requirements

An update that changes a primary key writes a delete marker (tombstone) for the
old key, followed by the row under the new key. For example, changing `id` from
`1` to `2` must remove `1` from current-state queries rather than leave both rows
visible.

Postgres's replica identity controls which old values it sends. Use one of:

- `REPLICA IDENTITY DEFAULT` with a primary key: sends the old primary-key values
  when the key changes.
- `REPLICA IDENTITY FULL`: sends the old row, including its primary key.

An alternative identity, such as an index on `email`, may not provide the old
primary key. ETL rejects such updates and deletes with
`SourceReplicaIdentityError` rather than leaving stale primary-key rows.

The new row must contain a value for every replicated column, not just the
changed columns. Postgres can omit unchanged large (TOASTed) values from updates.
If ETL cannot reconstruct those values, it rejects the update rather than
replacing them with `NULL`.

## Upgrading existing tables

Adding `cdc_tx_ordinal` is a breaking layout change for existing `MergeTree`
tables (only those created during the private alpha). ETL does not add the
column automatically: the next write from a restarted destination fails before
inserting rows. `ReplacingMergeTree` keeps its `_etl_version UInt128` /
`_etl_deleted UInt8` layout and does not require this ALTER.

For a non-destructive MergeTree upgrade:

1. Stop all writers to every affected destination table.
2. Verify the physical table has the expected user columns followed by
   `cdc_operation String` and `cdc_lsn UInt64`, with no source column named
   `cdc_tx_ordinal`. Resolve other schema drift separately.
3. Append the new column to each physical table, using its actual ClickHouse
   database and escaped table name. For example:

   ```sql
   alter table default.public_orders
       add column cdc_tx_ordinal UInt64 default 0 after cdc_lsn;
   ```

4. Keep the existing ETL metadata, schema snapshots, and replication
   checkpoints. Start only the upgraded writer, and update current-state
   queries to order by both `cdc_lsn` and `cdc_tx_ordinal`.

Existing events receive ordinal `0`. This cannot reconstruct their ordering
within an old transaction. Neither this ALTER nor the new writer removes stale
old-key rows left by earlier primary-key-changing updates, including stale rows
in `ReplacingMergeTree`.

If an accurate current-state baseline is required, use ETL's table reset/re-copy
path instead. It drops and recreates the destination table and copies the current
source contents; **the previous append-only event history is lost**. Preserve
that history separately if needed. `TRUNCATE` alone is not a layout migration
and does not reset ETL checkpoints.

## Connection notes

For HTTPS connections, provide an `https://` URL. TLS uses webpki root
certificates automatically.

The standalone replicator can enforce a public-network policy
(`ClickHouseDestination::new_public`). In that mode the URL must use
`https://` and the host must resolve only to publicly routable addresses;
loopback, private, link-local, and other IANA special-purpose ranges are
rejected. The example binary does not enforce this policy, so
`http://localhost:8123` works locally.

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
