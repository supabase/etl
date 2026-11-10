# `etl-destinations`

Built-in destination implementations for [Supabase ETL](https://supabase.github.io/etl/).
Enable only the destination feature you need when embedding the `etl` crate or
building `etl-replicator`.

| Feature | Destination | Status |
| --- | --- | --- |
| `clickhouse` | ClickHouse | Public alpha |
| `bigquery` | Google BigQuery | Public alpha |
| `ducklake` | DuckLake | Public alpha |
| `snowflake` | Snowflake | Public alpha |

Public alpha describes the managed-service release stage. Local and self-hosted
use of the open-source destinations is unrestricted.

ClickHouse is the easiest destination to run locally. BigQuery is the most
mature cloud destination. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
for release status, requirements, and limitations.

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.

## DuckLake source namespaces

Set `destination.ducklake.table_namespace` to a stable source identifier for a
new destination. For example, `project_a` maps source `public.orders` to
`lake.project_a.public.orders`. These are nested schemas; dots and quotes inside
any identifier are preserved as part of that identifier. Parent schemas are
created before their children.

Omitting `table_namespace` retains the flat `lake.public.orders` layout,
existing serialized table identities, and replay keys. The namespace is durable
layout identity: changing, adding, or removing it on an existing pipeline is
rejected when stored table metadata is loaded. Create a new destination and
replication state and perform a fresh copy to change layouts.

Namespaced table metadata uses a separate JSON shape without top-level `schema`
and `table` fields. Pre-namespace replicators reject these persisted identities
instead of silently treating them as flat tables after an image rollback. Keep
using a namespace-capable image for these destinations; legacy flat metadata
remains byte-identical and readable by older binaries.

Namespaces require DuckDB 2.0 and DuckLake catalog format 1.1. Writers and external
maintenance check the format and never automatically upgrade a 1.0 catalog.
Existing flat destinations can continue using format 1.0. An explicit catalog
upgrade is irreversible; use the DuckLake migration procedure or recreate a
Warehouse catalog as part of a coordinated cutover.

Table discovery, schema evolution, replay identity, metrics, and per-table
maintenance include the namespace. Snapshot expiration, old-file cleanup,
writer options, and ETL helper tables remain catalog-wide. This feature does
not provide access control or independent maintenance ownership for multiple
pipelines sharing one catalog; that requires separate coordination work.
