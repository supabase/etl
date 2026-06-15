---
title: Schema Changes
description: How ETL handles DDL and evolving table schemas.
---

**How ETL handles DDL and evolving table schemas**

ETL supports schema changes, and this area is actively being improved. The
current implementation is intentionally conservative: the source-side event
trigger captures a rich PostgreSQL-shaped snapshot, while ETL currently models
well-understood column changes: **adds, drops, renames, nullability changes, and
column default changes**. Built-in destination support varies by destination DDL
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
| Drop `NOT NULL` from a replicated column | Nullability modification |
| Set `NOT NULL` on a replicated column | Destination-specific nullability modification |
| Several of the above in one statement | One schema snapshot, diffed into column additions, removals, and grouped column modifications |

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

## Destination-Specific DDL Behavior

ETL has one shared schema-change signal, but **DDL behavior is implemented per destination**. A destination may choose to apply DDL automatically, reject a schema change, or require operator handling.

| Destination | Current DDL behavior |
|-------------|----------------------|
| BigQuery | Supports add, drop, rename, supported default metadata, and dropping `NOT NULL`. BigQuery requires added columns to be nullable and does not backfill existing rows for `ADD COLUMN ... DEFAULT`. |
| ClickHouse | Supports add, drop, rename, supported defaults, and nullability changes. `ReplacingMergeTree` rejects primary-key drops or renames because the ordering expression cannot be rewritten safely. ClickHouse default expressions are metadata-only unless explicitly materialized; ETL does not issue `MATERIALIZE COLUMN`. |
| DuckLake | Supports add, drop, rename, supported defaults, and nullability changes. DuckLake records supported add-time defaults as metadata without rewriting existing data files. |
| Snowflake | Supports add, drop, rename, create-table defaults, literal add-column defaults, and dropping `NOT NULL`. Literal defaults are included in `ADD COLUMN` so Snowflake can expose add-time default values for existing rows; non-literal add-column defaults and later default changes are skipped with a warning. |
| Iceberg | Deprecated for now. Schema-change DDL is not a supported path for new deployments. |
| Custom destinations | Destination authors decide which `Event::Relation` changes to apply, reject, or handle manually. |

## Default Backfills

When a source table adds a replicated column with a default, ETL deliberately
avoids **physical destination backfills** for existing rows. Built-in
destinations avoid operations such as `UPDATE`, `MERGE`, CTAS/swap rewrites,
or ClickHouse `MATERIALIZE COLUMN` because those operations can rewrite large
tables, block replication progress, and create destination-specific cost spikes.

Instead, destinations apply the schema change in the cheapest safe form they
support:

- BigQuery adds the column as nullable, then sets supported default metadata for
  future writes.
- ClickHouse may expose default values for pre-existing rows through default
  metadata, but ETL does not issue `MATERIALIZE COLUMN`.
- DuckLake uses add-time initial default metadata for supported defaults; its
  schema evolution does not rewrite data files.
- Snowflake uses `ADD COLUMN ... DEFAULT` for the literal default subset allowed
  by Snowflake. Snowflake exposes default values for existing rows and does not
  document this as a physical row rewrite, but defaults created this way cannot
  later be dropped.

As a result, pre-existing destination rows might not match PostgreSQL's
historical `ADD COLUMN ... DEFAULT` view unless the destination has a
metadata-only initial-default mechanism. Future row events remain correct
because PostgreSQL sends the evaluated row values after the relation change. A
physical destination backfill mode may be added in the future, but it needs
explicit controls for batching, throttling, observability, and how long the
pipeline can safely pause or run behind while the destination rewrite happens.

## Supported Column Defaults

Column defaults are **best-effort metadata translations**, not a PostgreSQL
expression evaluator. ETL reads the source default from PostgreSQL's
`pg_get_expr` output, parses only a small portable subset, and asks each
destination whether that parsed default can be rendered safely in that
destination's SQL dialect.

If a default is not in the supported subset, ETL skips the destination default
with a warning. Replication does not fail because PostgreSQL still sends
evaluated values for future row events.

The shared parser currently recognizes only these source default shapes:

| Source default shape | Examples |
|----------------------|----------|
| String literals | `'pending'::text`, `('don''t'::text)` |
| Numeric literals | `42`, `-1`, `'42.10'::numeric(10,2)` |
| Boolean literals | `true`, `false`, `'true'::boolean` |
| Date/time/timestamp literals | `'2026-01-01'::date`, `'12:30:00'::time`, `'2026-01-01 12:30:00'::timestamp` |
| JSON literals | `'{}'::jsonb`, `'{"enabled": true}'::json` |
| UUID v4 generators | `gen_random_uuid()`, `uuid_generate_v4()` |
| Current user expressions | `current_user`, `session_user` |
| Current temporal expressions | `now()`, `transaction_timestamp()`, `current_timestamp`, `current_date`, `current_time`, `localtimestamp` |
| Simple current-temporal interval arithmetic | `now() + interval '30 days'`, `current_date - interval '7 days'` |
| Literal string case functions | `lower('USER'::text)`, `upper('user')` |
| Simple numeric arithmetic | `(10 + 5) * 2` |

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
volatility, time zone, type coercion, or evaluation semantics.

Destination support is narrower than parser support:

| Destination | Supported default behavior |
|-------------|----------------------------|
| BigQuery | Supports compatible literals, `GENERATE_UUID()` for UUID-as-string columns, `SESSION_USER()` for text columns, and current date/time/timestamp defaults for matching temporal columns. It does not render lower/upper functions, interval arithmetic, numeric arithmetic, string concatenation, or other expressions. Added columns are created nullable and supported defaults are set afterward for future writes. |
| ClickHouse | Supports compatible literals, UUID generation, current user for text columns, current date/timestamp defaults, simple current-temporal interval arithmetic, lower/upper over string literals, and simple numeric arithmetic for native numeric columns. Defaults are metadata only unless separately materialized. |
| DuckLake | Supports compatible string/numeric/date/time/timestamp/JSON literals, UUID generation, current user for text columns, current date/time/timestamp defaults, simple current-temporal interval arithmetic, lower/upper over string literals, and simple numeric arithmetic for native numeric columns. Boolean defaults are currently skipped. |
| Snowflake | `CREATE TABLE` supports compatible literals, JSON literals via `PARSE_JSON`, UUID generation, current user for text columns, current date/time/timestamp defaults, simple current-temporal interval arithmetic, lower/upper over string literals, and simple numeric arithmetic for native numeric columns. `ADD COLUMN` only receives the literal subset Snowflake allows for add-column defaults: string, numeric, and boolean literals. Later default changes on existing columns are skipped. |

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
- Same ordinal position, different nullability: column nullability change.
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
5. Apply supported destination DDL for adds, drops, renames, nullability changes,
   and default changes.
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
- `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL`.
- `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` where the destination can
  apply it safely.
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
- Destination defaults are best-effort translations. Unsupported defaults are
  skipped with a warning instead of failing replication. If a previously
  supported default becomes unsupported, ETL removes the old destination default
  where the destination supports that operation so stale behavior is not left
  behind. Snowflake default changes on existing columns are skipped with a
  warning because `ALTER COLUMN SET DEFAULT` is documented only for existing
  sequence defaults, and defaults introduced by
  `ALTER TABLE ADD COLUMN ... DEFAULT` cannot be dropped safely.
- Volatile defaults cannot be made historically identical without explicit
  source backfill. Examples include `now()`, `clock_timestamp()`,
  `gen_random_uuid()`, `random()`, sequence defaults, and session-dependent
  expressions such as `current_user`. Future row events remain correct because
  PostgreSQL sends the evaluated row values, but existing destination rows are
  not physically backfilled by ETL. Avoid volatile defaults for replicated
  schema changes when exact historical values matter.
- `ADD COLUMN ... DEFAULT` semantics differ by destination. ETL intentionally
  avoids destination DDL that rewrites all existing rows. BigQuery leaves
  pre-existing destination rows null for newly added defaulted columns, while
  ClickHouse, DuckLake, and Snowflake can expose supported add-time defaults
  without ETL issuing a materialization rewrite. Snowflake only receives
  add-column defaults for source defaults that can be rendered as Snowflake
  literals.
- Tightening nullability with `SET NOT NULL` is destination-specific and may
  fail if existing destination data violates the constraint. BigQuery and
  Snowflake currently automate only `DROP NOT NULL`.
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
