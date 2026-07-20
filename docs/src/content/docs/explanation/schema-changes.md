---
title: Schema Changes
description: How ETL handles DDL and evolving table schemas.
---

**How ETL handles DDL and evolving table schemas**

ETL supports schema changes, and this area is actively being improved. The
current implementation is intentionally conservative: the source-side event
trigger captures a rich PostgreSQL-shaped snapshot, while ETL currently models
well-understood column changes: **adds, drops, renames, and column default
changes**. Built-in destination support varies by destination DDL
capabilities. **BigQuery, ClickHouse, DuckLake, and Snowflake** apply supported
schema changes automatically; Iceberg is deprecated for new deployments and does
not support schema-change DDL.

## Short Version

For published permanent tables, ETL currently models these `ALTER TABLE`
changes:

| Source change | ETL interpretation |
|---------------|--------------------|
| Add a replicated column | Add column |
| Drop a replicated column | Drop column |
| Rename a replicated column | Rename column |
| Change a replicated column default | Column default modification |
| Drop a replicated column default | Column default removal |
| Drop `NOT NULL` from a replicated column | BigQuery relaxes an existing `REQUIRED` column to `NULLABLE`; other built-in destinations currently leave nullability unchanged |
| Set `NOT NULL` on a replicated column | Detected in the schema snapshot, but not applied to built-in destinations |
| Several of the above in one statement | One final schema snapshot, diffed into a minimal ordered transition |

When several attributes of the same logical column change at once, ETL groups
them into one column change with multiple modifications. For example, renaming a
column and changing its default in one `ALTER TABLE` statement is treated as a
single logical column change.

## How It Works

ETL installs a PostgreSQL `ddl_command_end` event trigger named
`supabase_etl_ddl_message_trigger`. When an `ALTER TABLE` statement affects a
published permanent table, the trigger emits a transactional logical message
with prefix `supabase_etl_ddl`.

That message is **internal plumbing**. Destinations do not receive it directly.
Instead, ETL:

1. Parses the schema-change message.
2. Stores a new versioned table schema using the message LSN as the schema
   snapshot id.
3. Invalidates the in-memory relation state for that table.
4. Waits for PostgreSQL pgoutput to emit a fresh `RELATION` message before the
   next row event for that table.
5. Sends destinations a public `Event::Relation` with the new
   `ReplicatedTableSchema`.

The important public boundary is:

```text
... -> internal DDL message -> Relation(new schema) -> Insert/Update/Delete ...
```

Destinations should treat `Event::Relation` as the point where the **active schema changes** for following row events.

`Relation` is an ordered event, not a batch boundary. ETL batches calls to
`write_events()` based on size and time, so a single destination batch may
contain zero, one, or many schema changes, including multiple relation events
for the same table.

### Snapshot Compression and Ordered Operations

The DDL message contains the complete table schema after an `ALTER TABLE`
statement finishes. ETL does not replicate the statement's individual
subcommands. If several DDL statements occur without intervening DML, ETL may
store each source snapshot, but pgoutput does not need to emit a `Relation`
between them. The destination then compares its previously applied schema
directly with the final schema preceding the next row event. If DML occurs
between two DDL statements, pgoutput emits the intervening `Relation`, so that
schema remains an observable destination boundary.

Schema diffing therefore works from the two endpoint snapshots rather than
trying to reconstruct source DDL history. Columns are matched by PostgreSQL
`attnum`, and the diff includes an ordered operation plan that:

1. Drops only columns whose names must be freed before an add or rename.
2. Applies rename chains from their free end toward their source.
3. Uses one collision-checked `supabase_etl_` temporary name per rename cycle,
   and only when that cycle has no free target.
4. Applies final-name column modifications and additions.
5. Defers unrelated drops until the end.

Consequently, transient add, drop, or rename operations absent from the final
snapshot do not create destination DDL. Each endpoint difference normally
produces one destination operation; a rename cycle requires exactly one extra
temporary rename.

## Destination-Specific DDL Behavior

ETL has one shared schema-change signal, but **DDL behavior is implemented per destination**. A destination may choose to apply DDL automatically, reject a schema change, or require operator handling.

| Destination | Current DDL behavior |
|-------------|----------------------|
| BigQuery | Supports add, drop, rename, `REQUIRED` to `NULLABLE` relaxation, and supported literal default metadata. BigQuery requires added columns to be nullable and does not backfill existing rows for `ADD COLUMN ... DEFAULT`. PostgreSQL remains responsible for enforcing later `SET NOT NULL` changes because BigQuery cannot tighten an existing column in place. |
| ClickHouse | Supports add, drop, rename, and supported literal defaults. `ReplacingMergeTree` rejects primary-key drops or renames because the ordering expression cannot be rewritten safely. ClickHouse default expressions are metadata-only unless explicitly materialized; ETL does not issue `MATERIALIZE COLUMN`. |
| DuckLake | Supports add, drop, rename, and supported literal defaults. DuckLake records supported add-time defaults as metadata without rewriting existing data files. |
| Snowflake | Supports add, drop, rename, create-table literal defaults, and literal add-column defaults. Literal defaults are included in `ADD COLUMN` so Snowflake can expose add-time default values for existing rows; non-literal defaults and later default changes are skipped with a warning. |
| Iceberg | Deprecated for now. Schema-change DDL is not a supported path for new deployments. |
| Custom destinations | Destination authors decide which `Event::Relation` changes to apply, reject, or handle manually. |

## Default Backfills

When a source table adds a replicated column with a default, PostgreSQL can make
pre-existing source rows read as though they already contain that default. ETL
deliberately avoids **physical destination backfills** for those existing rows.
Built-in destinations avoid operations such as `UPDATE`, `MERGE`, CTAS/swap
rewrites, or ClickHouse `MATERIALIZE COLUMN` because those operations can
rewrite large tables, block replication progress, and create
destination-specific cost spikes.

Instead, destinations apply the schema change in the cheapest safe form they
support:

- BigQuery adds the column as nullable, then sets supported literal default
  metadata for future writes.
- ClickHouse may expose default values for pre-existing rows through default
  metadata, but ETL does not issue `MATERIALIZE COLUMN`.
- DuckLake uses add-time initial default metadata for supported defaults; its
  schema evolution does not rewrite data files.
- Snowflake uses `ADD COLUMN ... DEFAULT` for supported literal defaults.
  Snowflake exposes default values for existing rows and does not document this
  as a physical row rewrite, but defaults created this way cannot later be
  dropped.

As a result, pre-existing destination rows might not match PostgreSQL's
historical `ADD COLUMN ... DEFAULT` view unless the destination has a
metadata-only initial-default mechanism and the source default is supported.
This does **not** mean future replicated tuples lose their default values:
PostgreSQL sends evaluated column values in row data after the relation change,
and ETL writes those values normally. The limitation is only that unsupported
defaults are not installed as destination schema default metadata, and existing
destination rows are not rewritten by ETL. A physical destination backfill mode
may be added in the future, but it needs explicit controls for batching,
throttling, observability, and how long the pipeline can safely pause or run
behind while the destination rewrite happens.

## Supported Column Defaults

Column defaults are **best-effort metadata translations**, not a PostgreSQL
expression evaluator. ETL reads the source default from PostgreSQL's
`pg_get_expr` output, parses only deterministic literal defaults, and asks each
destination whether that parsed default can be rendered safely in that
destination's SQL dialect.

If a default is not in the supported subset, ETL skips the destination default
metadata with a warning. Replication does not fail, and future tuples still
carry evaluated values from PostgreSQL. This is intentional: runtime-generated
defaults can evaluate at different times or under different session settings in
different systems, so installing a similar-looking destination default can create
silent mismatches. ETL can add support for specific additional defaults later
when their semantics can be preserved for the destination.

The shared parser currently recognizes only these source default shapes:

| Source default shape | Examples |
|----------------------|----------|
| String literals | `'pending'::text`, `('don''t'::text)` |
| Numeric literals | `42`, `-1`, `'42.10'::numeric(10,2)` |
| Boolean literals | `true`, `false`, `'true'::boolean` |
| Date/time/timestamp literals | `'2026-01-01'::date`, `'12:30:00'::time`, `'2026-01-01 12:30:00'::timestamp` |
| JSON literals | `'{}'::jsonb`, `'{"enabled": true}'::json` |
| UUID literals | `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid` |

The parser is intentionally conservative. These PostgreSQL defaults are
examples of **unsupported** expressions:

```sql
default nextval('users_id_seq')
default 'a' || 'b'
default lower('USER' || '_ID')
default concat('a', 'b')
default md5('x')
default random()
default clock_timestamp()
default now()
default current_timestamp
default current_user
default gen_random_uuid()
default uuid_generate_v4()
default timezone('UTC', now())
default array['a', 'b']
default (select 'x')
default current_setting('app.tenant_id')
default 1e6
default interval '1 day 2 hours'
```

Unsupported defaults are skipped because translating arbitrary PostgreSQL
expressions would require both a PostgreSQL parser and a destination-specific
expression translator. Most destinations do not accept arbitrary PostgreSQL
expressions as column defaults, and even similar-looking SQL can have different
volatility, time zone, type coercion, or evaluation semantics. Skipping the
destination schema default does not drop actual row values emitted by
PostgreSQL.

Destination support may be narrower than parser support:

| Destination | Supported default behavior |
|-------------|----------------------------|
| BigQuery | Supports compatible string, numeric, boolean, date, time, timestamp, JSON, and UUID literals. Added columns are created nullable and supported defaults are set afterward for future writes. |
| ClickHouse | Supports compatible string, numeric, boolean, date, time, timestamp, JSON, and UUID literals. Defaults are metadata only unless separately materialized. |
| DuckLake | Supports compatible string, numeric, date, time, timestamp, JSON, and UUID literals. Boolean defaults are currently skipped by the DuckLake destination. |
| Snowflake | `CREATE TABLE` supports compatible string, numeric, boolean, date, time, timestamp, JSON, and UUID literals. `ADD COLUMN` only receives the literal subset Snowflake allows for add-column defaults: string, numeric, and boolean literals. Later default changes on existing columns are skipped. |

When changing a default from one supported expression to an unsupported
expression, destinations that can safely remove defaults drop the old supported
default to avoid leaving stale destination behavior behind. Snowflake is the
exception for defaults introduced by `ADD COLUMN ... DEFAULT`, because Snowflake
does not allow those defaults to be dropped safely.

## Diff Semantics

ETL stores schemas in **PostgreSQL column ordinal order** (`pg_attribute.attnum`)
and computes destination schema diffs over replicated columns only.

The current diff rules are:

- Same ordinal position, different name: column rename.
- Same ordinal position, different default expression: column default change.
- Same ordinal position, different nullability: detected schema metadata change.
- Old ordinal position missing from the new schema: column drop.
- New ordinal position missing from the old schema: column add.

This matches PostgreSQL's normal behavior for simple column operations:
renames keep the same `attnum`, dropped columns disappear from the visible
schema, and newly added columns receive new ordinal positions.

## Destination Handling

Custom destinations should handle schema changes in `write_events()` by
watching for `Event::Relation`.

A practical flow is:

1. Iterate through the batch in order, treating each relation event as a
   possible schema transition for that table.
2. Flush any buffered rows/events for the old schema before processing the
   relation event.
3. Compare the old destination schema with the relation event's new
   `ReplicatedTableSchema`.
4. Mark destination metadata as `Applying` if the destination needs recovery
   bookkeeping for the DDL transition.
5. Apply supported destination DDL in the order supplied by the schema diff.
6. Mark destination metadata as `Applied` only after the destination schema is
   actually ready for following row events.
7. Process following row events with the new schema.

The built-in BigQuery, ClickHouse, DuckLake, and Snowflake destinations follow
this shape: they mark destination schema metadata as `Applying`, apply the
supported DDL operations, then mark the schema as `Applied`. Because destination
DDL is not always transactional, a crash while metadata is `Applying` may require
manual intervention.

Other destination modules may support a narrower schema-change surface. Treat
`Event::Relation` as the stable ETL contract, then check the destination's
status and implementation before relying on automatic destination DDL.

## Supported Scope

The source event trigger intentionally observes a broad schema snapshot, but
ETL currently supports the simplest safe cases:

- `ALTER TABLE ... ADD COLUMN` for replicated columns.
- `ALTER TABLE ... DROP COLUMN` for replicated columns.
- `ALTER TABLE ... RENAME COLUMN` for replicated columns.
- `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` where the destination supports
  setting compatible default metadata.
- `ALTER TABLE ... ALTER COLUMN ... DROP DEFAULT` where the destination supports
  removing default metadata safely.
- Multi-subcommand `ALTER TABLE` statements composed of those simple changes.
- Changes to published permanent tables only.

The trigger ignores temporary tables, unpublished tables, generated columns,
dropped-column catalog tombstones, extension-owned DDL, and non-logical-WAL
databases.

## Current Limitations

These behaviors are **not full destination DDL semantics** yet:

- Only `ALTER TABLE` is captured by the ETL DDL trigger today.
- Type changes, constraint changes, identity changes, and replica-identity
  changes may be visible in the emitted snapshot, but they are not yet
  interpreted as destination DDL operations.
- Table create/drop/rename operations are outside the current schema-change
  contract. Publication membership changes and table cleanup are handled by
  other pipeline logic.
- A drop and re-add is not treated as a rename. It becomes a drop plus an add
  because PostgreSQL assigns a new ordinal position to the new column.
- Destination defaults are best-effort metadata translations. Unsupported
  defaults are skipped with a warning instead of failing replication. This does
  not remove values PostgreSQL emits in future row events; it only means the
  destination schema default metadata is not set. If a previously supported
  default becomes unsupported, ETL removes the old destination default where the
  destination supports that operation so stale behavior is not left behind.
  Snowflake default changes on existing columns are skipped with a warning
  because `ALTER COLUMN SET DEFAULT` is documented only for existing sequence
  defaults, and defaults introduced by `ALTER TABLE ADD COLUMN ... DEFAULT`
  cannot be dropped safely.
- Runtime-generated defaults are intentionally unsupported as destination schema
  defaults. Examples include `now()`, `clock_timestamp()`, `gen_random_uuid()`,
  `random()`, sequence defaults, and session-dependent expressions such as
  `current_user`. Those expressions can evaluate at different times or under
  different session settings in each destination. PostgreSQL still sends
  evaluated values for future row events, but existing destination rows are not
  physically backfilled by ETL. Avoid runtime-generated defaults for replicated
  schema changes when exact historical values matter.
- `ADD COLUMN ... DEFAULT` semantics differ by destination. ETL intentionally
  avoids destination DDL that rewrites all existing rows. BigQuery leaves
  pre-existing destination rows null for newly added defaulted columns, while
  ClickHouse, DuckLake, and Snowflake can expose supported add-time defaults
  without ETL issuing a materialization rewrite. Snowflake only receives
  add-column defaults for source defaults that can be rendered as Snowflake
  literals.
- BigQuery applies streaming `DROP NOT NULL` changes so future source NULL values
  remain writable. BigQuery cannot change an existing `NULLABLE` column to
  `REQUIRED`, so PostgreSQL enforces later `SET NOT NULL` changes while the
  destination column remains nullable. Other built-in destinations currently
  leave streaming nullability changes unchanged. Newly added columns remain
  nullable where the destination requires that for historical rows.
- The trigger payload includes `current_query` for debugging only. It can
  contain literals and multiple statements, so it must not be treated as
  replayable DDL.
- Sessions can set `supabase_etl.skip_ddl_log = 'true'` as an emergency
  opt-out while recovering a system. DDL executed with that setting enabled is
  not logged for ETL.

Future work will implement broader behavior on top of the richer trigger
payload, but the current contract is deliberately limited to safe column-level
schema changes.

## Event Ordering

Schema changes are transactional logical messages, so they appear in WAL order
relative to row changes. ETL updates its stored schema when it decodes the DDL
message, then waits for the next `RELATION` message to rebuild the runtime
replication and identity masks for row decoding.

This matters for custom destinations: row events after a relation event should
be decoded and written using that relation event's schema. Row events before it
belong to the previous schema.
