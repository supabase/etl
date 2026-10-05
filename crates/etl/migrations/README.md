# Replication migrations

Migrations stay beside the crate that embeds and applies them. This directory has
two independent sets:

| Directory | Owns | Applied by |
| --- | --- | --- |
| [`source/`](source/) | Source schema snapshots, DDL event triggers, and publication-aware schema-change messages. | `Pipeline::start` through `etl::postgres::migrations::run_source_migrations`, unless explicitly disabled. |
| [`postgres_store/`](postgres_store/) | Durable replication progress, table state, stored schemas, and destination metadata. | `etl::store::PostgresStore::new`. |

Every pipeline needs the source helpers, including pipelines using a custom state
store. Only `PostgresStore` needs the durable-store migrations. Maintenance
coordination has its own [migration set](../../etl-maintenance/migrations/README.md)
owned by `etl-maintenance`.

## Schema capture contract and limitations

Schema-change handling is in **public beta**. The source migrations install
`etl.describe_table_schema`, `etl.describe_table_identity`, and the event
trigger `supabase_etl_ddl_message_trigger`. At `ddl_command_end`, it calls
`etl.emit_schema_change_messages` to emit transactional `supabase_etl_ddl`
messages containing unprojected table-schema descriptions. A rollback discards
the message along with its DDL.

Ordinary `ALTER TABLE` messages apply to interested pipelines regardless of
publication. `ALTER PUBLICATION` messages carry a publication scope; pipelines
ignore other publications' messages. The trigger checks publication membership
to identify affected tables, but does not construct a separate projected schema
for every pipeline. Each pipeline selects the stored version in WAL order and
builds its replication mask from `RELATION` metadata. Supported column-list
changes therefore use the same destination schema-planning path as table DDL.

Do not treat the trigger's post-command description as a guaranteed current
catalog image under all interleavings:

- `etl.describe_table_schema` combines MVCC catalog rows with C helpers such as
  `format_type` and `pg_get_expr`. Internal catalog/cache lookups need not use
  the same visibility as the surrounding SQL. Publication expansion in the
  trigger also uses PostgreSQL helpers.
- `ddl_command_end` makes the command's own changes visible before commit. It
  does not refresh an already established `REPEATABLE READ` or `SERIALIZABLE`
  snapshot to include another transaction's later committed DDL. Transactional
  WAL emission orders the captured payload; it does not correct its contents.
- Relation masks cannot distinguish every missing column caused by intentional
  publication filtering from one caused by mismatched schema history. A
  successful subset match is not proof that capture was correct.

Initial-copy relation locks live in the replication client, not these triggers.
They protect table layout before the slot snapshot and throughout copying;
they do not resolve later trigger-visibility or relation-metadata limitations.
The public [schema-change documentation](../../../site/content/docs/explanation/schema-changes.mdx#current-limitations)
contains the A/B transaction example and operating guidance. The
[two-session reproductions](schema-visibility.md) demonstrate helper visibility
and incomplete committed WAL payloads on PostgreSQL 14 and 18, with a
read-committed control. Improvements here need focused concurrent-session
coverage and a new migration; keep applied SQL migrations immutable.

## Database history and compatibility

Both sets create objects in `etl` and record applied versions in
`etl._sqlx_migrations`. Each SQLx migrator uses `ignore_missing` so versions belonging
to another set can coexist in the history table. It still checks the checksums of
its own applied migrations. Keep migration version numbers unique across sets that
can share a database.

The directory split describes ownership, not separate database histories. Do not
flatten the directories, reset the history table, renumber applied versions, or
modify existing SQL to reorganize the repository. Add a new migration for a schema
change. Preserve matching `.up.sql`/`.down.sql` pairs and verify rollback compatibility
where a down migration is supported.

## Applying and testing

`cargo x migrate` creates the configured local database if necessary, applies the
Postgres store set, then the source set. `cargo x init` includes this step. The
command reads `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`,
and `POSTGRES_DB`; see the [development guide](../../../DEVELOPMENT.md).

Runtime source setup skips migration execution on physical standbys. Apply the
source migrations on the primary and allow them to replay before starting a pipeline
against a standby. Source migration setup and store construction preserve their
existing connection and locking behavior.

The migration integration tests live in [`tests/migrations.rs`](../tests/migrations.rs)
and exercise upgrades, down migrations, schema helpers, and shared history. With the local Postgres services running:

```bash
cargo nextest run --locked -p etl --all-features --test main -E 'test(migrations::)'
```
