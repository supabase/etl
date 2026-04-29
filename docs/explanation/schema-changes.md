# Schema Changes

**How ETL handles DDL and evolving table schemas**

ETL supports schema changes, and this area is actively being improved. The
current implementation is intentionally conservative: the source-side event
trigger captures a rich PostgreSQL-shaped snapshot, while ETL currently models
only simple, well-understood column changes: adds, drops, and renames. Built-in
destination support varies: BigQuery applies these changes today, DuckLake is
in progress, and Iceberg is deprecated for now.

## Short Version

For published permanent tables, ETL currently models these `ALTER TABLE`
changes:

| Source change | ETL interpretation |
|---------------|--------------------|
| Add a replicated column | Add column |
| Drop a replicated column | Drop column |
| Rename a replicated column | Rename column |
| Several of the above in one statement | One schema snapshot, diffed into adds, drops, and renames |

Future behavior will expand from this base. The event trigger already emits
more catalog information than ETL consumes today so the system can grow without
redesigning the source message format.

## How It Works

ETL installs a PostgreSQL `ddl_command_end` event trigger named
`supabase_etl_ddl_message_trigger`. When an `ALTER TABLE` statement affects a
published permanent table, the trigger emits a transactional logical message
with prefix `supabase_etl_ddl`.

That message is internal plumbing. Destinations do not receive it directly.
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

Destinations should treat `Event::Relation` as the point where the active
schema changes for following row events.

## Destination-Specific DDL Behavior

ETL has one shared schema-change signal, but DDL behavior is implemented per
destination. A destination may choose to apply DDL automatically, reject a
schema change, or require operator handling.

| Destination | Current DDL behavior |
|-------------|----------------------|
| BigQuery | Stable support for simple add, drop, and rename column changes. |
| DuckLake | In progress. Do not rely on automatic DDL behavior yet. |
| Iceberg | Deprecated for now. Schema-change DDL is not a supported path for new deployments. |
| Custom destinations | Destination authors decide which `Event::Relation` changes to apply, reject, or handle manually. |

## Diff Semantics

ETL stores schemas in PostgreSQL column ordinal order (`pg_attribute.attnum`)
and computes destination schema diffs over replicated columns only.

The current diff rules are:

- Same ordinal position, different name: column rename.
- Old ordinal position missing from the new schema: column drop.
- New ordinal position missing from the old schema: column add.

This matches PostgreSQL's normal behavior for simple column operations:
renames keep the same `attnum`, dropped columns disappear from the visible
schema, and newly added columns receive new ordinal positions.

## Destination Handling

Custom destinations should handle schema changes in `write_events()` by
watching for `Event::Relation`.

A practical flow is:

1. Flush any buffered rows/events for the old schema before processing the
   relation event.
2. Compare the old destination schema with the relation event's new
   `ReplicatedTableSchema`.
3. Apply supported destination DDL for adds, drops, and renames.
4. Persist destination metadata only after the destination schema is applied.
5. Process following row events with the new schema.

The built-in BigQuery destination follows this shape: it marks destination
schema metadata as `Applying`, applies add/rename/drop operations, then marks
the schema as `Applied`. Because BigQuery DDL is not transactional, a crash
while metadata is `Applying` may require manual intervention.

Other destination modules may support a narrower schema-change surface. Treat
`Event::Relation` as the stable ETL contract, then check the destination's
status and implementation before relying on automatic destination DDL.

## Supported Scope

The source event trigger intentionally observes a broad schema snapshot, but
ETL currently supports the simplest safe cases:

- `ALTER TABLE ... ADD COLUMN` for replicated columns.
- `ALTER TABLE ... DROP COLUMN` for replicated columns.
- `ALTER TABLE ... RENAME COLUMN` for replicated columns.
- Multi-subcommand `ALTER TABLE` statements composed of those simple changes.
- Changes to published permanent tables only.

The trigger ignores temporary tables, unpublished tables, generated columns,
dropped-column catalog tombstones, extension-owned DDL, and non-logical-WAL
databases.

## Current Limitations

These behaviors are not full destination DDL semantics yet:

- Only `ALTER TABLE` is captured by the ETL DDL trigger today.
- Type changes, nullability changes, default changes, constraint changes,
  identity changes, and replica-identity changes may be visible in the emitted
  snapshot, but they are not yet interpreted as destination DDL operations.
- Table create/drop/rename operations are outside the current schema-change
  contract. Publication membership changes and table cleanup are handled by
  other pipeline logic.
- A drop and re-add is not treated as a rename. It becomes a drop plus an add
  because PostgreSQL assigns a new ordinal position to the new column.
- The trigger payload includes `current_query` for debugging only. It can
  contain literals and multiple statements, so it must not be treated as
  replayable DDL.
- Sessions can set `supabase_etl.skip_ddl_log = 'true'` as an emergency
  opt-out while recovering a system. DDL executed with that setting enabled is
  not logged for ETL.

Future work will implement broader behavior on top of the richer trigger
payload, but the current contract is deliberately limited to simple add, drop,
and rename handling.

## Event Ordering

Schema changes are transactional logical messages, so they appear in WAL order
relative to row changes. ETL updates its stored schema when it decodes the DDL
message, then waits for the next `RELATION` message to rebuild the runtime
replication and identity masks for row decoding.

This matters for custom destinations: row events after a relation event should
be decoded and written using that relation event's schema. Row events before it
belong to the previous schema.
