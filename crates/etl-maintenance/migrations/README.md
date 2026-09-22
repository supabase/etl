# Maintenance coordination migrations

[`postgres/`](postgres/) contains the SQL owned by the Postgres external-maintenance
store: shared state used to coordinate replicators and maintenance workers.
`PostgresExternalMaintenanceStore::ensure_schema` embeds and applies this set on the
database represented by its connection pool.

These migrations are separate from the engine's
[source helpers and replication state store](../../etl/migrations/README.md).
`cargo x migrate` initializes those engine sets; it does not provision maintenance
coordination. The maintenance store initializes its schema when its caller requests
it.

Objects and migration history live in `etl` and `etl._sqlx_migrations`. The migrator
uses `ignore_missing` so other migration sets can share that history table, while
checksums remain enforced for maintenance versions. Allocate unique version numbers
across colocated sets. Preserve existing filenames, SQL bytes, and up/down pairs;
add a new migration for a change to the deployed schema.
