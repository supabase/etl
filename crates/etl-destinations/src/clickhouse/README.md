# ClickHouse Destination

> **Status: Private alpha.** Access is limited, and behavior may change before
> general availability.

## Requirements

- `ReplacingMergeTree` (the default) needs ClickHouse **23.5 or newer** and a
  source primary key. `MergeTree` has neither requirement.
- Replica identity must be `DEFAULT` with a primary key, or `FULL`. See
  [Update requirements](#update-requirements).

## Running the example

Copy `.env.example` to `.env`, then start and seed the local Postgres and
ClickHouse:

```bash
source .env
cargo x init
cargo x seed
```

Run the example:

```bash
cargo x example clickhouse
```

`cargo x` supplies the local database, the `seed_pub` publication, and the
`TESTS_*` variables. The equivalent direct invocation:

```bash
cargo run -p etl-examples --bin clickhouse --features clickhouse -- \
    --db-host "$TESTS_DATABASE_HOST" \
    --db-port "$TESTS_DATABASE_PORT" \
    --db-name etl_testdata \
    --db-username "$TESTS_DATABASE_USERNAME" \
    --publication seed_pub
```

## Table engines

The destination supports two layouts. Select one per pipeline with
`--clickhouse-engine`:

| Flag value                       | Engine               | Use it for                                       |
| -------------------------------- | -------------------- | ------------------------------------------------ |
| `replacing_merge_tree` (default) | `ReplacingMergeTree` | Current-state replica. Requires a primary key.   |
| `merge_tree`                     | `MergeTree`          | Append-only event log. No primary key required.  |

Table names are `<schema>_<table>` with underscores in either part doubled:
`public.orders` → `public_orders`, `my_schema.t` → `my__schema_t`.

### ReplacingMergeTree (default)

Each replicated table uses
`ReplacingMergeTree(_etl_version, _etl_deleted)`, keyed on the source primary
key. Two trailing columns drive the merge:

- `_etl_version UInt128` -- the packed event sequence key
  `(commit_lsn << 64) | tx_ordinal`. It totally orders every event, including
  rows that share a WAL record, so the latest event per primary key wins under
  `FINAL`.
- `_etl_deleted UInt8` -- tombstone flag. `1` for `DELETE` events and `0` for
  other events.

Each table gets a `<table>__current` view that hides the `ReplacingMergeTree`
columns:

```sql
CREATE VIEW IF NOT EXISTS "public_orders__current" AS
SELECT <user columns>
FROM "public_orders" FINAL
WHERE _etl_deleted = 0
```

Query the `__current` view for current state, or read the base table directly:

```sql
select <user columns>
from "public_orders" final
where _etl_deleted = 0
```

The replicator never runs `OPTIMIZE ... FINAL CLEANUP`; background merges
collapse duplicates, but deleted rows stay on disk until you run
`optimize table "<table>" final cleanup`.

### MergeTree

Each replicated table uses `MergeTree() ORDER BY tuple()`. Three CDC metadata
columns follow each row:

- `cdc_operation`: `INSERT`, `UPDATE`, or `DELETE`.
- `cdc_lsn`: the Postgres commit LSN at the time of the change.
- `cdc_tx_ordinal`: the zero-based event position within the Postgres
  transaction.

The table is the event log. For current state per primary key, take the latest
event and drop tombstones:

```sql
select <user columns> from (
    select * from "public_orders"
    order by cdc_lsn desc, cdc_tx_ordinal desc
    limit 1 by (id)
)
where cdc_operation != 'DELETE'
```

## Update requirements

An update that changes a primary key writes a tombstone for the old key and
then the row under the new key, so current-state queries see only the new key.

Postgres's replica identity controls which old values it sends. Use one of:

- `REPLICA IDENTITY DEFAULT` with a primary key: sends the old primary-key values
  when the key changes.
- `REPLICA IDENTITY FULL`: sends the old row, including its primary key.

Any other identity (`USING INDEX`, `NOTHING`) omits the old primary key, so ETL
rejects the update or delete with `SourceReplicaIdentityError` instead of
writing a stale row.

Postgres omits unchanged TOASTed (large) values from update rows. ETL fills
them from the old row image, which only `REPLICA IDENTITY FULL` guarantees;
when it cannot, it rejects the update instead of writing `NULL`. Use `FULL` for
tables with large columns.

## Upgrading existing tables

`MergeTree` tables created during the private alpha lack `cdc_tx_ordinal`. ETL
refuses to write to them until the column exists; `ReplacingMergeTree` tables
are unaffected.

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

Pre-existing events get ordinal `0`, so their order within a transaction is
lost. The `ALTER` also leaves any stale old-key rows from earlier primary-key
changes in place, in both engines.

For an exact current-state baseline, reset the table instead: ETL drops and
recreates it and re-copies the source. **This discards the append-only event
history**, so export it first if you need it. `TRUNCATE` on its own neither
migrates the layout nor resets ETL checkpoints.

## Connection notes

For HTTPS connections, provide an `https://` URL. TLS uses `webpki` root
certificates automatically.

The standalone replicator can enforce a public-network policy
(`ClickHouseDestination::new_public`), which requires an `https://` URL and a
host that resolves only to publicly routable addresses. It rejects loopback,
private, link-local, and other IANA special-purpose ranges. The example binary
skips this policy, so `http://localhost:8123` works locally.

The example reads `TESTS_CLICKHOUSE_PASSWORD` from the environment so the
secret stays out of process arguments; `--clickhouse-password` also works.

## CLI flags

| Flag                           | Default                | Description                                                   |
| ------------------------------ | ---------------------- | ------------------------------------------------------------- |
| `--db-host`                    | _(required)_           | Postgres host                                                 |
| `--db-port`                    | _(required)_           | Postgres port (`u16`)                                         |
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

See [`./METRICS.md`](./METRICS.md) for the metrics that the ClickHouse
destination emits.
