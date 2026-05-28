# `etl` - Destinations

Destination implementations for the ETL system.

Enable the destination modules you need with crate features:

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `ducklake` | DuckLake | In progress |
| `iceberg` | Apache Iceberg | Deprecated for now |

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.
