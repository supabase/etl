# `etl` - Destinations

Destination implementations for the ETL system.

Enable the destination modules you need with crate features:

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `ducklake` | DuckLake | In progress |
| `iceberg` | Apache Iceberg | Experimental / unsupported |

The Iceberg module is unmaintained experimental code. It is not production
supported, has not been widely tested, and should only be used at your own
risk. It writes materialized Iceberg format v2 tables with equality-delete CDC,
but does not run compaction, snapshot expiration, manifest rewrites, or
orphan-file cleanup. CDC requires a replicated source primary key and
primary-key or full replica identity. BigQuery and DuckLake are the maintained
destination paths.

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.
