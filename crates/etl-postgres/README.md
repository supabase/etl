# `etl-postgres` - Shared Postgres Primitives

This crate contains Postgres-specific primitives shared by ETL workspace crates.
It is not the primary end-user pipeline API; most pipeline users should import
from `etl` and destination modules from `etl-destinations`.

The public modules here cover reusable boundaries such as schema identifiers,
Postgres value wrappers, replication slot naming, source database metadata, and
Postgres-backed store records. Runtime internals that are only needed by the
core pipeline stay inside the `etl` crate.
