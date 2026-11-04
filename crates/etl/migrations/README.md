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
records the PostgreSQL 14.24 and 18.6 reproductions, including a read-committed
control. Fixes need concurrent-session coverage and a new migration; keep
applied SQL migrations immutable.

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

## Pipeline identity backfill

Deploy the dual-writing release to all writers sharing a source database
before deploying the backfill migration. Store initialization applies it to all
four state tables, including historical rows and other pipelines in that database.
It fills only missing string IDs and requires them to match the existing numeric
IDs. Negative stored IDs or conflicting string IDs abort the entire transaction;
existing identities are not replaced.

The migration adds non-null checks and matching string-key indexes while retaining
numeric-key reads and indexes. The transaction locks metadata tables before
updating rows; active pipelines wait for commit before their metadata operations
continue. If that wait exceeds a writer's query timeout, the writer may need to
restart after the migration commits.
Rollback removes the new constraints and indexes but preserves populated strings.

## String-key cutover

After backfill, deploy API readers using `pipeline_id_v2` first. Its reset operation
must still populate the legacy integer for older replicators. Updating the API's
ETL dependency does not run state-store migrations. Keep creating numeric string
IDs until the string-key ETL image is the default for new pipelines.

Then deploy the string-key ETL release across the fleet. It makes the old column
nullable and reads and writes only V2. Once every replicator is upgraded, remove
the API's temporary legacy reset write. Drop the old column in a later release,
after all writers have stopped referencing it.

After V2-only writes begin, an older ETL image needs its legacy keys restored
before it can resume that pipeline. Use the new image for rollout retries.

## Legacy column removal

All readers must use V2 and all writers must stop populating legacy keys before
deploying this migration. This includes running pipelines, stopped pipelines
when they resume, and applications using the shared metadata helpers.

The first cleanup replicator sharing a database drops the four legacy columns
and their dependent indexes in one transaction. Other string-key replicators
continue using V2. The migration retains the V2 column names, identities, indexes,
and constraints, and does not rewrite rows or rebuild retained indexes. Metadata
operations wait while the migration holds its DDL locks.

Rollback restores nullable compatibility columns and indexes for the preceding
string-key release. The restored columns contain nulls; discarded legacy values
are not reconstructed. V2 data remains authoritative. Restore this schema before
reverting any writer to a version that populates legacy keys.

Snowflake ownership and retry identity compatibility is independent of these
Postgres columns and remains in place.
