# `etl` - Destinations

Destination implementations for the ETL system.

Enable the destination modules you need with crate features:

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `clickhouse` | ClickHouse | In progress |
| `ducklake` | DuckLake | In progress |
| `iceberg` | Apache Iceberg | Deprecated |
| `snowflake` | Snowflake | In progress |

BigQuery is the stable, recommended default. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
for each implementation's maturity, requirements, and limitations.

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.
