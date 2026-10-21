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

## Change scope

| Change | Purpose and relationship to synthetic relations |
| --- | --- |
| Narrow shared column loader | Copy and CDC use the same column representation without formatted type names or unused metadata. Independently useful for consistency and cost. |
| Compact WAL payload | Removes submitted SQL and diagnostic fields. Independently reduces payload size and avoids copying unrelated query literals. |
| Publication attnums on table and publication DDL | Makes each new snapshot self-contained, distinguishing all columns from an explicit list. Required to synthesize reliable masks without querying newer catalogs. |
| Shared MVCC publication loader | Centralizes copy/CDC membership and filter rules. Uses PostgreSQL's normal-object OID boundary to exclude initdb objects; avoids current-cache publication expansion. Useful independently, and supplies synthetic metadata. |
| Bounded publication candidates and partition-tree refreshes | Avoids full descriptions of unrelated tables and duplicate inheritance paths. Table DDL refreshes every effective identity in its declarative partition tree, using each table's own schema. Improves capture coverage independently; synthetic relations make these refreshes immediate. |
| Separate protocol replica identity | Matches real relation keys, including deferrable/default identity and insert-only filters; ordinary inheritance no longer borrows a parent's primary key. Fixes copy semantics and enables correct synthetic identity. Existing INCLUDE-key exclusion is retained. |
| Captured missing-value literal | Preserves supported fast-add values when the current default differs. An independent value-correctness fix; it does not replace ordered relation barriers. |
| Immediate ordinary relation events | Installs the complete decoder and emits `Event::Relation` at each new DDL boundary, including DDL without later rows. This is the synthetic relation behavior. |
| Real relation refresh and exact local lineage | Retains PostgreSQL startup/invalidation handling, refreshes both masks, and preserves the connection's established snapshot despite preloaded future schemas. Required coexistence semantics; no deduplication is added. |
| Optional wire fields and nullable store migration | Keeps queued legacy messages and old stored schemas readable with their historical fallback. New metadata survives replay and destination recovery. |
| Reader grant migration | Carries existing non-PUBLIC role grants to the new helpers, including grant options. Independently avoids breaking admin-installed, non-owner pipelines. |
| Unchanged Iceberg snapshots | Allows metadata-only refreshes with identical source contents and publication masks, including conservative tree refreshes. Actual schema evolution remains unsupported. Needed to consume extra synthetic snapshots without failing unchanged tables. |
| Restricted missing-value conversion | Prevents newly introduced JSON conversion from invoking custom casts as the trigger owner. Existing pinned search paths, qualified catalog references, PUBLIC revocations, and transactional emission are preserved protections. |

## Schema capture contract and limitations

Schema-change handling is in **public beta**. The source migrations install
`etl.describe_table_columns`, `etl.describe_table_identity`,
`etl.describe_table_publication_columns`, and the event
trigger `supabase_etl_ddl_message_trigger`. At `ddl_command_end`, it calls
`etl.emit_schema_change_messages` to emit transactional `supabase_etl_ddl`
messages containing unprojected table-schema descriptions. A rollback discards
the message along with its DDL.

Ordinary `ALTER TABLE` messages apply to interested pipelines regardless of
publication. `ALTER PUBLICATION` messages carry a publication scope; pipelines
ignore other publications' messages. Every new snapshot includes the effective
column filters for each effective publication at that DDL boundary. A `null`
filter means all supported columns; explicit lists contain PostgreSQL column
attnums. The schema already supplies their names, so capture does not repeat name
lookups for every publication. Copy and CDC resolve these attnums against their
immutable table schema, including gaps left by dropped columns. The schema is
stored once, rather than repeated for every publication. Publication
DDL captures only its scoped publication. Each pipeline combines
its captured columns and identity with the stored schema and emits a destination
relation barrier immediately, preserving intermediate changes even without later
rows. Older WAL payloads without publication columns keep the relation-driven
fallback. Supported column-list changes use the same schema-planning path as
table DDL.

The decoder payload contains only routing, columns, identity, and publication
filters. It omits submitted SQL, command-address descriptions, formatted type
names, and other unused column annotations. A client query can contain DML as
well as DDL; copying that query would duplicate unrelated row literals into the
logical message. The older `describe_table_schema` helper remains available for
older binaries and source introspection. Current copy and CDC share the narrower
`describe_table_columns` query, avoiding `format_type` and the extra type-catalog
join. Column default expressions still require `pg_get_expr`.

The shared column helper also captures PostgreSQL's stored fast-add missing
value separately from the current default when the two differ. For example,
`ADD COLUMN score integer DEFAULT 1, ALTER COLUMN score SET DEFAULT 2` leaves
old source rows at `1` and future inserts at `2`, despite emitting only one
post-statement snapshot. The schema planner adds the column using the captured
value, then restores the current default according to destination capabilities.
Publication-only additions retain their existing no-backfill policy. Optional
`missing_value_expression` is omitted when unnecessary; a captured SQL NULL is
the string `"null"`, distinct from absent legacy metadata.

Conversion is restricted to supported built-in scalar types. Generic JSON
conversion of user-defined types can invoke their casts under the trigger
owner's privileges, so custom types, arrays, and composites retain the existing
default-portability fallback. JSON scalar strings and JSON null retain their
JSON representation. This metadata does not recover values materialized by
heap rewrites or volatile defaults; a post-statement catalog description is not
a complete record of every subcommand's effects.

Do not treat the trigger's post-command description as a guaranteed current
catalog image under all interleavings:

- `etl.describe_table_columns` combines MVCC catalog rows with `pg_get_expr`
  for default expressions. Internal catalog/cache lookups need not use
  the same visibility as the surrounding SQL. Schema, identity, and publication
  filters are captured by one SQL statement through stable helpers. Publication
  membership and filters use MVCC catalog SQL shared with initial copy, without
  current-cache expansion helpers or locks on unrelated tables.
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
control. These interleavings are outside the serialized-DDL contract; the tests
keep isolation guarantees explicit. Keep applied SQL migrations immutable.

## Synthetic and real relations

A synthetic relation uses the same full table schema, publication mask, and
identity mask as a real `pgoutput` relation at that catalog state. Copy and
synthetic relations share the identity loader and mask builder. Protocol identity
keys are captured separately from destination primary-key metadata: default
identity needs the table's own non-deferrable primary index, and included index
columns are not identity keys. PostgreSQL 14-17 require a valid primary index;
PostgreSQL 18 also selects an invalid partitioned primary index for DEFAULT.
Capture follows that version difference, checked against real relation flags by
`invalid_partitioned_primary_index_matches_real_identity`. Both DEFAULT and
USING INDEX require an immediate index. Attaching a replica-identity index to a
deferrable unique constraint can leave `indisreplident` set; the shared loader
excludes that index just as PostgreSQL does. The copy/synthetic/real mask matrix
covers this serialized DDL sequence. Full identity covers the replicated columns;
insert-only publication filters can omit key columns.

The runtime identity type contains only `replica_identity_attnums` for both
DEFAULT and USING INDEX. Deserialization normalizes legacy fields once; an
explicit empty resolved array remains authoritative. Legacy DEFAULT metadata
also excludes a known deferrable primary key. Missing both resolved and legacy
keys for USING INDEX is a decode error, not an empty identity.

The SQL payload retains `replica_identity_index_attnums` solely because the
previous binary requires that field. It is derived from the same resolved
aggregate for USING INDEX, without a second index-key query. FULL and NOTHING
continue to select their masks by mode. Removing the compatibility field would
break older readers of newly emitted WAL; it is not additional runtime state.
Old WAL without publication metadata keeps its relation-driven fallback.

Each worker emits ordinary `Event::Relation` events in its own WAL order. Both
synthetic and real events are forwarded, including duplicates. No deduplication
state or destination-specific event protocol is introduced. DDL-only transactions
now update the decoding state before SyncDone, so handover does not need a later
row to supply the masks.

Table DDL conservatively refreshes every effective publication identity in its
containing declarative partition tree: ancestors, intermediate roots, leaves,
and siblings. Each snapshot describes that table's own schema and filters;
a leaf-local default or identity change is never copied to a root. Root-only,
leaf-only, and mixed publications use the same membership query. This is a
snapshot refresh, not a claim that every table changed. Ordinary SQL inheritance
keeps downward-only capture, and publication DDL keeps its publication scope.
Attachment/detachment can change the tree before capture; these snapshots do not
synchronize rows entering or leaving a publication and do not make live topology
changes supported.

Schema-capable destinations plan each new snapshot even when no physical DDL is
needed. Iceberg accepts a newer snapshot only when the exact stored previous
schema's name and complete column metadata, and its publication mask, are
identical. It flushes preceding rows before advancing the applied endpoint;
actual schema or mask changes remain rejected. This requires `DestinationStore`
(state plus schema reads) for Iceberg instead of its former state-only bound.
Custom stores implementing only `SharedStateStore` must add `SchemaStore` to
use Iceberg. Its source schema versions must be retained under the existing
destination metadata retention contract.

This is not global schema ordering across workers. Apply skips a table's DDL
while its table-sync worker owns that table. The source copy lock is released
before catchup and handover finish; another ready table can therefore deliver
later DDL first. The failpoint regression
`synthetic_relation_order_is_local_to_each_worker` pauses a recopy after its
copy transaction commits and demonstrates this delivery order deterministically.
It uses the memory destination and does not implement or test native table
renames. Independent per-table schema changes do not acquire cross-table name
dependencies merely because they share a source transaction, and ETL does not
promise atomic multi-table destination DDL.

Table-rename implementation remains outside this branch. A future implementation
must either coordinate dependencies such as `a -> archived; b -> a` across
workers, or disallow cross-table name reuse while a participating table is
initially syncing or being recopied. Completion of copy alone is insufficient;
the relevant ownership boundary is handover to apply. Serializing source DDL
does not serialize delivery from independent workers. Ordinary rename to an
unoccupied name has no dependency on another table releasing that name.

Real relation handling remains necessary. The trigger captures supported ALTER
TABLE and ALTER PUBLICATION events, not every invalidation handled by pgoutput.
For example, DROP INDEX can remove the replica identity without emitting our DDL
snapshot. A real relation refreshes that identity against the exact schema
already established by this connection's synthetic or real relation. It rebuilds
both masks without selecting a future schema retained from another attempt.
The `real_relation_updates_identity_after_replica_index_drop` regression checks
this with a deliberately preloaded future snapshot. Reconnects, legacy WAL,
partition metadata, and future PostgreSQL changes also make removing real
relations a broader protocol change. PostgreSQL's [message callback](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/pgoutput/pgoutput.c)
forwards logical messages without invoking its relation-emission path; SQL cannot
force a real relation at each DDL boundary through pg_logical_emit_message.

Concurrent DDL is unsupported. Serialize schema and publication changes in short,
fresh READ COMMITTED transactions. The capture SELECT and its STABLE SQL/PLpgSQL
helpers already share one statement snapshot; transaction-wide repeatability is
not needed to align those reads. A fresh REPEATABLE READ transaction also sees
preceding committed DDL if it has not already acquired a snapshot, as the fresh
repeatable-read regression checks. However, an older repeatable-read snapshot
can be stale even when DDL statements do not overlap. The trigger cannot choose
an independent isolation level inside the customer's transaction, and stronger
SQL isolation does not change native catalog-cache visibility. Ordinary DML can
continue. Copy retains its existing locks; streaming capture adds no relation locks.

## Publication capture alternatives

| Approach | Causality and operational cost |
| --- | --- |
| Inline publication filters in the table snapshot (implemented) | Self-contained WAL replay; one schema plus relevant publication filters. Membership joins use the affected table and declarative ancestors, not publication-wide table expansion. Null avoids repeating unfiltered column names. |
| One message per publication | Same capture work, more repeated schema and WAL. Stock pgoutput forwards logical messages to every messages-enabled slot; publication scope is filtered by ETL. Splitting messages does not provide server-side routing. |
| Group identical explicit filters | Can further reduce wide repeated filters. Adds a grouped wire format and lookup logic; defer until measured payloads justify it. |
| Register only ETL publications | Bounds capture to active pipelines, but needs durable registration, ownership, cleanup, and safe bootstrap before any DDL can be missed. |
| Cache publication state and emit deltas | Avoids re-reading all filters on table DDL, but requires ordered membership/filter/topology history, a consistent initial baseline, persistence, and recovery. Reusing a previous mask alone misses added unfiltered columns. |
| Query catalogs during apply | A later query can observe a newer publication filter than the replayed DDL. Does not preserve the required historical boundary. |
| Custom output plugin | Could use decoding's historical catalog context to supply richer metadata, but replaces pgoutput and requires a server extension and a new compatibility surface. [PostgreSQL documents the plugin API and catalog-access restrictions](https://www.postgresql.org/docs/18/logicaldecoding-output-plugin.html). |
| Explicitly queued DDL | [pglogical's replicate_ddl_command](https://github.com/2ndQuadrant/pglogical#additional-functions) illustrates ordering commands with data. Requiring callers to use a DDL wrapper changes ETL's source contract and does not itself provide publication masks. |

### Why retaining the previous relation is insufficient

Synthetic and real relations can coexist without deduplication; that is the
implemented protocol. The additional question is whether ALTER TABLE can omit
publication metadata and immediately reuse the previous real relation. Local
pgoutput probes show why a mask alone is not a sufficient baseline:

- An all-column publication and an explicit list of every current column produce
  identical real relations. After ADD COLUMN, only the former includes the new
  column. Remapping old mask bits by attnum cannot recover this distinction.
- Initial copy supplies a complete decoder, and SyncDone persists one. Ready
  discards that handover decoder. After a Ready restart, table DDL can arrive
  before any real relation, including DDL for a table that receives no more rows.
- Reading the publication at startup supplies current state, not replay state.
  In the disconnected probe, two table DDL messages used a two-column filter;
  a later ALTER PUBLICATION exposed a third column before reconnect. The current
  catalog and the next real relation both described the later three-column
  filter. Neither could supply the earlier DDL barriers.

The first and third cases were reproduced on PostgreSQL 15-18. DDL-only messages
without real relations and identity refresh after DROP INDEX were reproduced on
PostgreSQL 14-18. Integration regressions cover live and Ready-restart ADD COLUMN
for both equal-mask policies, and replay preceding a later publication change.

A cache-based implementation is possible, but it must retain the filter policy
(`all columns` or an explicit set of attnums), recover it at the durable replay
position, and update identity from each table snapshot. A later real relation can
refresh actual columns and keys; it cannot establish the missing filter policy.
That adds durable source-decoding state rather than merely reusing masks.

ALTER TABLE can also affect publication membership: DROP COLUMN CASCADE removes
a table's explicit publication entry when that entry depends on the column.
SET SCHEMA and partition changes can affect effective membership too. Such live
membership/topology changes remain outside the supported contract and require
resynchronization. Serializing DDL does not eliminate these semantic effects or
the historical-state problem during replay.

### Keeping capture work bounded

The trigger and its helpers pin `search_path` to `pg_catalog` and qualify catalog
and ETL references. The trigger runs with its owner's privileges, while direct
helper execution is revoked from PUBLIC. The `etl` schema and helper owner must
remain trusted; do not grant untrusted roles CREATE on that schema. Tests execute
table-owner DDL with hostile search paths, shadow helper functions, and temporary
catalog names. They also check that the table owner has no direct helper access.

The trigger reads publication membership before describing the full table. An
unpublished table therefore needs no schema or identity load. Publication-wide
DDL starts from direct, schema, or all-table members and expands declarative
partitions through `publication_table_candidates`; it does not invoke the full
column helper for unrelated tables. The shared column helper still decides the
effective root/leaf targets and filters. Recursive table DDL visits each
target/publication-scope pair once, including multiple-inheritance diamonds.

Local PostgreSQL 18 measurements compared the actual `origin/main` trigger
with this implementation using a 100-column table with longer names. With three
publications, complete message payloads decreased from 26,982 bytes to
15,318 bytes for all-column policies, 15,324 for two-column lists, and 16,482
for explicit 100-column lists. These are payload sizes, not total WAL sizes.

Each timing sample averaged twenty ALTER COLUMN SET DEFAULT statements, with
five samples per case and commit latency excluded. In one comparison, main's
medians were about 1.6–3.4 ms. Current three-publication medians were about
1.1–1.4 ms; 100 explicit 100-column publications took about 4.1 ms and emitted
55,857 bytes. With 1,000 such publications, current probes ranged from about
19–45 ms and emitted 422,157 bytes. Main does not capture publication filters,
so its cost does not include this extra historical metadata. The new trigger
is not universally faster: work and payload grow with the number and width of
publication filters, and conservative tree capture also grows with tree size.
These local timings depend on planning, catalog statistics, and machine load;
they are not production guarantees.

A separate two-column partition probe measured leaf-local DDL with 5 and 50
leaves, using three samples of five statements each. For 50 leaves, root-only
publishing emitted one 587-byte snapshot in about 3.3 ms; leaf-only emitted
50 snapshots totaling about 29.5 KB in 7.6 ms; mixed root/leaf publishing emitted
51 snapshots totaling about 30.1 KB in 9.3 ms. Even root-only capture visits the
containing tree before membership filtering. These narrow-table measurements
demonstrate the conservative rule's scaling cost; wider schemas and more
publications add work.

Nested EXPLAIN ANALYZE confirmed targeted attribute/default lookups and
catalog-only publication discovery. A forced lateral lookup used fewer catalog
pages with 5,000 unrelated tables but was slower in that probe, so the ordinary
join remains. Query plans can still scan catalog rows; bounded helper calls do
not imply constant cost independent of database size.

Regression tests check that unrelated tables do not multiply full metadata
captures and that unpublished DDL loads no schema or identity. Recursive target
selection prevents inheritance paths from duplicating snapshots. A leaf DDL
visits the same effective partition-tree targets as a root DDL, including
unchanged siblings and ancestors. Publication-wide commands likewise describe
their scoped effective targets. The trigger does not parse submitted SQL or
maintain prior source-side state to guess which snapshots can be omitted.

### Product scope: static filters and resync

Keeping initial column selection while requiring resync for later filter changes
is a reasonable product boundary. [PeerDB reported column exclusion as a common
customer request](https://blog.peerdb.io/optimizing-postgres-replication-using-column-exclusion),
and [Airbyte reported substantial community demand](https://airbyte.com/blog/airbyte-column-selection-control-over-the-exact-data-to-sync).
Those vendor reports do not quantify usage of PostgreSQL publication column
lists or demand for live filter edits. [ClickPipes documents resync to include
previously excluded columns](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/clickpipes/postgres/faq.md#include-excluded-columns).

A static-filter policy would still need publication-aware masks and a distinction
between all columns and an explicit list of every current column. PostgreSQL
[adds future columns only for the former](https://www.postgresql.org/docs/18/logical-replication-col-lists.html).
Using all-one masks while accepting a filtered publication would misdecode its
row tuples. Rejecting filters entirely would remove a useful existing feature
without solving identity capture or DDL ordering. This implementation preserves
the existing filter behavior; a resync-only policy is a separate product change.

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
this binary: initial copy calls the new column and publication helpers. The
migration carries existing role EXECUTE grants on `describe_table_schema` to
both new reader helpers, including grant options and inherited-role access.
PUBLIC access remains revoked. For a fresh non-owner replication role, grant
USAGE on `etl` and EXECUTE on the reader helpers as described in the
[configuration guide](../../../site/content/docs/guides/configure-postgres.mdx).
Older binaries retain their broad schema helper, and their decoders ignore added JSON
fields and do not require the removed diagnostic fields. The compact payload is
not compatible with external consumers that depend on those diagnostic fields.
Downgrade binaries before reverting this migration; a newer binary cannot copy
through helpers removed by the down migration. Older binaries continue to use
their relation-driven behavior rather than the new immediate DDL barriers.

New readers accept queued legacy payloads and column descriptions without
`missing_value_expression`. Missing historical values keep their old fallback;
they must not be reconstructed from current catalogs. The separate store
migration adds a nullable `etl.table_columns.missing_value_expression`, without
backfilling old snapshots. Older binaries use explicit column lists and tolerate
that added column. New binaries require it; downgrade the binary before rolling
the store migration back. Persisting this field makes destination recovery use
the same captured value as uninterrupted streaming. Adding this public
`ColumnSchema` field requires Rust callers using struct literals to initialize
it; constructor and builder callers retain the legacy `None` default.

The upgrade regression `source_upgrade_replays_queued_legacy_and_synthetic_relations`
downgrades the source to its last pre-synthetic migration while a stopped
pipeline retains its slot, queues old-format DDL and rows, upgrades the helpers,
and queues new-format DDL before replay. It checks the historical schemas and
both masks at the old/new boundary without recopying the table, for both
DEFAULT and USING INDEX with a distinct identity column. The separate
`source_upgrade_from_main_preserves_queued_identity_messages` regression starts
with a fresh database and only the committed pre-synthetic source migrations,
queues legacy DDL, upgrades, and reads both formats from the retained slot. It
also checks the required identity fields consumed by the previous decoder.
Reader-grant tests start from that same fresh migration boundary. Legacy messages
retain legacy ordering and missing-value limitations; restarting cannot recover
information never recorded in WAL. A supported old payload is not a reason to
reject startup or request a restart. Genuinely malformed or undecodable messages
still fail rather than being skipped or acknowledged.

Runtime source setup skips migration execution on physical standbys. Apply the
source migrations on the primary and allow them to replay before starting a pipeline
against a standby. Source migration setup and store construction preserve their
existing connection and locking behavior.

The migration integration tests live in [`tests/migrations.rs`](../tests/migrations.rs)
and exercise upgrades, down migrations, schema helpers, and shared history. With the local Postgres services running:

```bash
cargo nextest run --locked -p etl --all-features --test main -E 'test(migrations::)'
```
