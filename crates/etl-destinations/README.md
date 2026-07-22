# `etl` - Destinations

Destination implementations for the ETL system.

Enable the destination modules you need with crate features:

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `clickhouse` | ClickHouse | In progress |
| `ducklake` | DuckLake | In progress |
| `iceberg` | Apache Iceberg | Deprecated for now |
| `snowflake` | Snowflake | In progress |
| `postgres` | Postgres | In progress |

The Postgres destination creates current-state UPSERT tables (no CDC meta columns),
auto-applies portable schema changes, maps source `timetz` to destination `text`,
and accepts TLS through `PgConnectionConfig.tls` (API-created destinations currently
force TLS disabled).

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.
