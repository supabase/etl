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
