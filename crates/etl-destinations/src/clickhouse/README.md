# ClickHouse Destination

> **Private alpha.** The managed service admits a limited set of users; the
> open-source destination is unrestricted. Expect operational requirements to
> change.

## Requirements

- `ReplacingMergeTree` (the default) needs ClickHouse **23.5 or newer** and a
  source primary key. `MergeTree` has neither requirement.
- In either engine, publish every primary-key column if the source has a key.
- For tables with a primary key, updates and deletes need a matching replica
  identity or `FULL`. See [Update requirements](#update-requirements).

## Running the example

Complete the [development setup](../../../../DEVELOPMENT.md), then run these
commands from the repository root. Copy `.env.example` to `.env` if you have
not already done so:

```bash
source .env
cargo x init
cargo x seed
```

Run the example:

```bash
cargo x example clickhouse
```

The wrapper reads the exported Postgres connection variables and defaults to
database `etl_testdata` and publication `seed_pub`. The example reads
`TESTS_CLICKHOUSE_*` directly. The equivalent direct command is:

```bash
cargo run -p etl-examples --bin clickhouse --features clickhouse -- \
    --db-host "$TESTS_DATABASE_HOST" \
    --db-port "$TESTS_DATABASE_PORT" \
    --db-name etl_testdata \
    --db-username "$TESTS_DATABASE_USERNAME" \
    --publication seed_pub
```

## Table engines

Select a layout with the example's `--clickhouse-engine` flag or the standalone
replicator's `destination.engine` setting:

| Engine value                     | Engine               | Use it for                                       |
| -------------------------------- | -------------------- | ------------------------------------------------ |
| `replacing_merge_tree` (default) | `ReplacingMergeTree` | Current-state replica. Requires a primary key.   |
| `merge_tree`                     | `MergeTree`          | Append-only event log. No primary key required.  |

Table names are `<schema>_<table>` with underscores in either part doubled:
`public.orders` → `public_orders`, `my_schema.t` → `my__schema_t`.
Schema and table names cannot start or end with `_`, or contain `"` or `;`.

### ReplacingMergeTree (default)

`ReplacingMergeTree(_etl_version, _etl_deleted)` keeps the latest version for
each source primary key:

- `_etl_version UInt128`: `(commit_lsn << 64) | tx_ordinal`. Higher versions
  win for the same key.
- `_etl_deleted UInt8`: `1` marks a tombstone; `0` marks a live row.

Initial-copy rows use version `0`.

Query the `<table>__current` view for current state. It applies `final`,
filters tombstones, and returns only source columns:

```sql
select * from public_orders__current;
```

The replicator never runs `OPTIMIZE ... FINAL CLEANUP`. Background merges
collapse older versions but retain tombstones.

Cleanup removes tombstones. A row with an older version inserted afterward
becomes visible again. Restart replay can cause this: ETL replays every event
written after the persisted checkpoint, and insert deduplication can drop a
replayed tombstone whose block hash is still in the deduplication log while
accepting the live row in a differently batched block.

Run cleanup only when nothing can replay:

1. Stop the pipeline and wait for it to exit.
2. Confirm the checkpoint covers every write. In Postgres:

   ```sql
   select flush_lsn - '0/0'::pg_lsn
   from etl.replication_progress
   where pipeline_id = <pipeline id> and worker_type = 'apply'
     and table_id is null;
   ```

   In ClickHouse:

   ```sql
   select max(bitShiftRight(_etl_version, 64)) from "public_orders";
   ```

   The ClickHouse value must be lower. If it is not, start the pipeline, let
   it pass a commit boundary, and stop it again.
3. On replicated or ClickHouse Cloud tables, run
   `system sync replica "public_orders"`.
4. Run the statements below, then restart the pipeline.

See [ClickHouse's cleanup requirements](https://clickhouse.com/docs/concepts/features/operations/update/replacing-merge-tree#automatic-upserts-of-inserted-rows).

```sql
alter table "public_orders"
    modify setting allow_experimental_replacing_merge_with_cleanup = 1;
optimize table "public_orders" final cleanup;
```

### MergeTree

Each replicated table uses `MergeTree() ORDER BY tuple()`. Three CDC metadata
columns follow each row:

- `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`.
- `cdc_lsn`: the Postgres commit LSN at the time of the change.
- `cdc_tx_ordinal`: the zero-based event position within the Postgres
  transaction.

Initial-copy rows use `INSERT` with `cdc_lsn = 0` and `cdc_tx_ordinal = 0`.
A replicated source `TRUNCATE` clears the table and its event history; it is
not stored as a log entry.

The table is the event log. For current state per primary key, take the latest
event and drop tombstones:

```sql
select id, user_id, total, status, created_at from (
    select * from "public_orders"
    order by cdc_lsn desc, cdc_tx_ordinal desc
    limit 1 by (id)
)
where cdc_operation != 'DELETE';
```

## Update requirements

An update that changes a primary key writes a tombstone for the old key and
then the row under the new key, so current-state queries see only the new key.
Both rows share the source event's sequence.

Postgres's replica identity controls which old values it sends. Use one of:

- `REPLICA IDENTITY DEFAULT` with a primary key: sends the old primary-key values
  when the key changes.
- `REPLICA IDENTITY USING INDEX` with the same identity columns as the primary
  key: ETL treats it as a primary-key identity.
- `REPLICA IDENTITY FULL`: sends the entire old row.

For tables with a primary key, ETL rejects other index identities with
`SourceReplicaIdentityError` rather than risk leaving a stale row.

With `NOTHING`, or `DEFAULT` without a primary key, Postgres rejects updates
and deletes when the publication includes those operations. They fail at
the source, before ETL receives an event. Inserts do not require replica
identity.

Postgres omits unchanged TOASTed (large) values from update rows. ETL fills
them from the old row image, which only `REPLICA IDENTITY FULL` guarantees;
when it cannot, it rejects the update instead of writing `NULL`. Use `FULL` for
tables with large columns.

## Upgrading existing tables

Older `MergeTree` tables without `cdc_tx_ordinal` must be upgraded before ETL
can write to them. `ReplacingMergeTree` does not need this schema change.

For a non-destructive `MergeTree` upgrade:

1. Stop all writers to every affected destination table.
2. Confirm the table ends with `cdc_operation String, cdc_lsn UInt64` and has
   no user column named `cdc_tx_ordinal`.
3. Add the column after `cdc_lsn`:

   ```sql
   alter table default.public_orders
       add column cdc_tx_ordinal UInt64 default 0 after cdc_lsn;
   ```

4. Restart the pipeline. Do not reset ETL state. Update current-state queries
   to order by `cdc_lsn desc, cdc_tx_ordinal desc`.

Existing events receive ordinal `0`; their order within a transaction cannot
be recovered. Upgrading does not remove stale old-key rows left by earlier
primary-key changes in either engine.

For an exact current-state baseline, reset the table instead: ETL drops and
recreates it and re-copies the source. **This discards the event history**,
so export it first if you need it. `TRUNCATE` on its own neither
migrates the layout nor resets ETL checkpoints.

## Connection notes

For HTTPS connections, provide an `https://` URL. TLS uses `webpki` root
certificates automatically.

For HTTPS, the standalone replicator requires every resolved address to be
publicly routable. It rejects loopback, private, link-local, and other IANA
special-purpose ranges. Managed configurations also require HTTPS.
Standalone HTTP and the example binary allow local connections.

Set `TESTS_DATABASE_PASSWORD` and `TESTS_CLICKHOUSE_PASSWORD` rather than
passing password flags to keep secrets out of process arguments.

## Example CLI flags

| Flag                           | Default                | Description                                                   |
| ------------------------------ | ---------------------- | ------------------------------------------------------------- |
| `--db-host`                    | _(required)_           | Postgres host                                                 |
| `--db-port`                    | _(required)_           | Postgres port                                                 |
| `--db-name`                    | _(required)_           | Postgres database name                                        |
| `--db-username`                | _(required)_           | Postgres user (must have `REPLICATION`)                       |
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

See [Metrics](./METRICS.md).
