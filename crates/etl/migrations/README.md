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

## Schema capture contract

Schema-change handling is in **public beta**. The `ddl_command_end` trigger
emits transactional `supabase_etl_ddl` messages for supported `ALTER TABLE` and
`ALTER PUBLICATION` commands. Messages roll back with their DDL and are assumed
to come from the trusted ETL source trigger.

Copy and CDC share three catalog helpers:

- `etl.describe_table_columns` returns non-generated columns in physical
  `attnum` order, including type OIDs, modifiers, defaults, and missing values.
- `etl.describe_table_identity` separates destination primary-key metadata from
  table-local protocol identity. Included index attributes and non-immediate
  indexes are not protocol keys; PostgreSQL's version-specific partition-index
  behavior is preserved.
- `etl.describe_table_publication_columns` resolves effective publication
  membership and column filters, including declarative partition roots.

The trigger captures these fields in one statement through stable helpers.
Publication filters use attribute numbers: `null` means all supported columns,
while an explicit list preserves its policy when columns are added. Dropped
column gaps do not change tuple order. Every snapshot carries the historical
filter needed for replay; current catalogs or a previous all-one mask cannot
recover that policy. Table DDL captures relevant publications together;
publication DDL is scoped to the changed publication.

Table DDL conservatively refreshes effective identities throughout its
containing declarative partition tree. Each table uses its own schema and
filters, including unchanged siblings and ancestors. Ordinary inheritance
expands downward only. `publication_table_candidates` bounds publication-wide
capture to members and declarative descendants; unpublished tables do not need
full metadata descriptions. Work still grows with tree size and publication
filters.

The payload omits submitted SQL and unused diagnostic metadata. The column
helper uses `pg_get_expr(adbin, 0)` because defaults contain no column references;
this avoids opening each table and rebuilding table-wide deparse contexts.
Generated expressions are excluded. The legacy `describe_table_schema` helper
remains available for older binaries.

The trigger runs in the customer's DDL session. `SECURITY DEFINER` supplies
privileges, not connection settings, so the column helper captures ETL's shared
date, interval, float, timezone, and bytea settings with `SET ... FROM CURRENT`
at installation. Manual installers must use the same settings. Changing those
shared defaults later requires a migration to refresh the helper. SQLx's separate
float-precision option is disabled so it cannot override the shared setting.

Helpers use qualified catalog references and `search_path = pg_catalog, pg_temp`.
Direct execution is revoked from PUBLIC; table owners execute the trigger with
its trusted owner's privileges. Keep the `etl` schema and helper owner trusted.

### Missing values and defaults

`missing_value_expression` preserves a supported fast-add value when it differs
from the current default. For example, `ADD COLUMN score integer DEFAULT 1,
ALTER COLUMN score SET DEFAULT 2` leaves existing source rows at `1` and future
inserts at `2`. A captured SQL NULL is the string `"null"`; an absent hint falls
back to the current default, preserving legacy behavior. Partitioned tables
store no rows, so they report no hint and use the current default.

Conversion is restricted to supported built-in scalar types so generic JSON
conversion cannot invoke custom casts as the trigger owner. Float literals
preserve negative zero. Custom types, arrays, and composites retain the existing
portability fallback; captured metadata cannot recover values already
materialized by rewrites or volatile defaults. Destinations retain their own
backfill policies, described in the
[schema-change documentation](../../../site/content/docs/explanation/schema-changes.mdx).

### Synthetic and native relations

A complete DDL snapshot installs the decoder and emits an ordinary
`Event::Relation` immediately, even without later rows. Synthetic and native
relations both reach destinations; no deduplication state or separate event
protocol is needed. A native relation refreshes publication and identity masks
against the connection's established snapshot, never a preloaded future one.
Column order, names, normalized types, and modifiers must match before that
decoder is installed. Unknown type OIDs retain the existing text fallback.

Native relations remain necessary for startup and invalidations outside the
trigger's command set, such as dropping an identity index. Legacy WAL without
publication metadata retains the pending-relation fallback. Legacy identity
fields are normalized during decoding; the emitted compatibility field for
explicit-index identity remains for older binaries.

Destinations advance durable snapshot metadata even when no physical DDL is
needed. Iceberg accepts only unchanged schema contents and publication masks,
ignoring add-time missing-value hints on existing columns; it still rejects
schema evolution. Its store therefore requires schema reads through
`DestinationStore`, with the existing schema-retention contract.

Ordering is local to each worker. Apply skips a table while its table-sync worker
owns it, so independent workers can deliver changes to different tables in a
different order. Copy completion alone is not handover. Cross-table rename/name
reuse and atomic multi-table destination DDL require controlled maintenance.

### Operating limits

Serialize source schema and publication DDL in short, fresh `READ COMMITTED`
transactions. Stable helpers share the capture statement's snapshot, but
`pg_get_expr` uses internal catalog caches, and an established `REPEATABLE READ`
or `SERIALIZABLE` snapshot can be stale. Transactional WAL orders the payload;
it cannot correct stale capture. Initial-copy locks protect copying, not later
DDL capture. Ordinary DML can continue.

Live publication membership, row-filter, and partition-topology changes require
resynchronization; emitted metadata does not move rows into or out of a
publication. See the documented
[current limitations](../../../site/content/docs/explanation/schema-changes.mdx#current-limitations).
PostgreSQL 15-only catalogs remain in separately planned branches so PostgreSQL
14 never references unavailable catalogs.

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

When source migrations are disabled, apply the latest source set before starting
this binary: initial copy requires the new column and publication helpers. The
upgrade preserves non-PUBLIC reader grants from `describe_table_schema`,
including grant options, and gives the retained trigger owner access to its new
private helper. Fresh replication roles need the grants in the
[configuration guide](../../../site/content/docs/guides/configure-postgres.mdx).

Queued legacy payloads remain readable without reconstructing missing history
from current catalogs. Older binaries ignore the added fields and retain their
relation-driven behavior; external consumers of removed diagnostic fields need
updating. Downgrade binaries before reverting the source migration, because the
new binary requires helpers removed by its down migration.

The store migration adds nullable `etl.table_columns.missing_value_expression`
without backfilling old snapshots. Older binaries tolerate this column, while
new binaries require it; downgrade binaries before reverting the store
migration. Rust callers using `ColumnSchema` struct literals must initialize the
new field; constructors retain the legacy `None` default.

Runtime source setup skips migration execution on physical standbys. Apply the
source migrations on the primary and allow them to replay before starting a
pipeline against a standby.

The integration tests in [`tests/migrations.rs`](../tests/migrations.rs) cover
upgrade/rollback compatibility, queued legacy messages, helper grants, and
shared history. With the local Postgres services running:

```bash
cargo nextest run --locked -p etl --all-features --test main -E 'test(migrations::)'
```
