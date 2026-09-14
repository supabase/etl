# `etl-destinations`

Built-in destination implementations for [Supabase ETL](https://supabase.github.io/etl/).
Enable only the destination feature you need when embedding the `etl` crate or
building `etl-replicator`.

| Feature | Destination | Status |
| --- | --- | --- |
| `clickhouse` | ClickHouse | In progress |
| `bigquery` | Google BigQuery | Stable |
| `ducklake` | DuckLake | In progress |
| `snowflake` | Snowflake | In progress |
| `iceberg` | Apache Iceberg | Deprecated |

ClickHouse is the easiest destination to run locally. BigQuery is the most
mature cloud destination. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
for each implementation's maturity, requirements, and limitations.

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.
