# `etl` - Destinations

Destination implementations for the ETL system.

Enable the destination modules you need with crate features:

| Feature | Destination | Status |
| --- | --- | --- |
| `bigquery` | Google BigQuery | Stable |
| `ducklake` | DuckLake | In progress |
| `iceberg` | Apache Iceberg | Experimental / unsupported |

The Iceberg module is unmaintained experimental code and is not production
supported. It now writes materialized Iceberg format v2 tables rather than
append-only changelog tables: table-copy rows are appended, inserts are
replay-safe upserts, updates delete the old key and write the new row, deletes
write equality-delete files, and truncates drop and recreate the table. Simple
add, drop, and rename column changes are supported; primary-key/identifier
changes, positional deletes, delete vectors, and full DDL support are not.

Iceberg CDC writes create data files, equality-delete files, manifests,
snapshots, and table metadata over time. This destination does not compact,
rewrite, expire, or clean up those files. Run catalog/table maintenance outside
ETL before considering the tables healthy or performant. Use the module at your
own risk; BigQuery and DuckLake are the maintained destination paths. If the
tables are stored in AWS S3 Tables, keep AWS table maintenance enabled and tune
it there; ETL does not enable, disable, or validate S3 Tables maintenance.

DuckLake external maintenance is configured at runtime with
`maintenance_mode`: `disabled`, `kubernetes`, or `postgres`. The default is
`disabled`. Kubernetes coordination expects
`ETL_DUCKLAKE_MAINTENANCE_CR_NAME` and
`ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE`. Postgres coordination uses the same
Postgres catalog connection as DuckLake and stores coordination state in the
`etl` schema.
